/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.collection

import java.io._
import java.util.Comparator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import org.apache.spark.storage.{BlockId, DiskBlockObjectWriter}

/**
 * Sorts and potentially merges a number of key-value pairs of type (K, V) to produce key-combiner
 * pairs of type (K, C). Uses a Partitioner to first group the keys into partitions, and then
 * optionally sorts keys within each partition using a custom Comparator. Can output a single
 * partitioned file with a different byte range for each partition, suitable for shuffle fetches.
 *
 * If combining is disabled, the type C must equal V -- we'll cast the objects at the end.
 *
 * Note: Although ExternalSorter is a fairly generic sorter, some of its configuration is tied
 * to its use in sort-based shuffle (for example, its block compression is controlled by
 * `spark.shuffle.compress`).  We may need to revisit this if ExternalSorter is used in other
 * non-shuffle contexts where we might want to use different configuration settings.
 *
 * @param aggregator optional Aggregator with combine functions to use for merging data
 * @param partitioner optional Partitioner; if given, sort by partition ID and then key
 * @param ordering optional Ordering to sort keys within each partition; should be a total ordering
 * @param serializer serializer to use when spilling to disk
 *
 * Note that if an Ordering is given, we'll always sort using it, so only provide it if you really
 * want the output keys to be sorted. In a map task without map-side combine for example, you
 * probably want to pass None as the ordering to avoid extra sorting. On the other hand, if you do
 * want to do combining, having an Ordering is more efficient than not having it.
 *
 * Users interact with this class in the following way:
 *
 * 1. Instantiate an ExternalSorter.
 *
 * 2. Call insertAll() with a set of records.
 *
 * 3. Request an iterator() back to traverse sorted/aggregated records.
 *     - or -
 *    Invoke writePartitionedFile() to create a file containing sorted/aggregated outputs
 *    that can be used in Spark's sort shuffle.
 *
 * At a high level, this class works internally as follows:
 *
 *  - We repeatedly fill up buffers of in-memory data, using either a PartitionedAppendOnlyMap if
 *    we want to combine by key, or a PartitionedPairBuffer if we don't.
 *    Inside these buffers, we sort elements by partition ID and then possibly also by key.
 *    To avoid calling the partitioner multiple times with each key, we store the partition ID
 *    alongside each record.
 *
 *  - When each buffer reaches our memory limit, we spill it to a file. This file is sorted first
 *    by partition ID and possibly second by key or by hash code of the key, if we want to do
 *    aggregation. For each file, we track how many objects were in each partition in memory, so we
 *    don't have to write out the partition ID for every element.
 *
 *  - When the user requests an iterator or file output, the spilled files are merged, along with
 *    any remaining in-memory data, using the same sort order defined above (unless both sorting
 *    and aggregation are disabled). If we need to aggregate by key, we either use a total ordering
 *    from the ordering parameter, or read the keys with the same hash code and compare them with
 *    each other for equality to merge values.
 *
 *  - Users are expected to call stop() at the end to delete all the intermediate files.
 */
private[spark] class ExternalSorter[K, V, C](
    context: TaskContext,
    aggregator: Option[Aggregator[K, V, C]] = None,
    partitioner: Option[Partitioner] = None,
    ordering: Option[Ordering[K]] = None,
    serializer: Serializer = SparkEnv.get.serializer)
  extends Spillable[WritablePartitionedPairCollection[K, C]](context.taskMemoryManager())
  with Logging {

  private val conf = SparkEnv.get.conf

  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  private val shouldPartition = numPartitions > 1
  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

  private val blockManager = SparkEnv.get.blockManager
  private val diskBlockManager = blockManager.diskBlockManager
  private val serializerManager = SparkEnv.get.serializerManager
  private val serInstance = serializer.newInstance()

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // Size of object batches when reading/writing from serializers.
  //
  // Objects are written in batches, with each batch using its own serialization stream. This
  // cuts down on the size of reference-tracking maps constructed when deserializing a stream.
  //
  // NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
  // grow internal data structures by growing + copying every time the number of objects doubles.
  private val serializerBatchSize = conf.getLong("spark.shuffle.spill.batchSize", 10000)

  // 在我们执行spill之前，用于在内存中存储对象(数据)的数据结果。我们要么把对象存储到AppendOnlyMap中并对其合并，
  // 要么将对象存储到一个数组buffer中。这取决于我们是否设置了Aggregator。
  // Data structures to store in-memory objects before we spill. Depending on whether we have an
  // Aggregator set, we either put objects into an AppendOnlyMap where we combine them, or we
  // store them in an array buffer.
  @volatile private var map = new PartitionedAppendOnlyMap[K, C]
  @volatile private var buffer = new PartitionedPairBuffer[K, C]

  // Total spilling statistics
  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Peak size of the in-memory data structure observed so far, in bytes
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  @volatile private var isShuffleSort: Boolean = true
  private val forceSpillFiles = new ArrayBuffer[SpilledFile]
  @volatile private var readingIterator: SpillableIterator = null

  // A comparator for keys K that orders them within a partition to allow aggregation or sorting.
  // Can be a partial ordering by hash code if a total ordering is not provided through by the
  // user. (A partial ordering means that equal keys have comparator.compare(k, k) = 0, but some
  // non-equal keys also have this, so we need to do a later pass to find truly equal keys).
  // Note that we ignore this if no aggregator and no ordering are given.
  private val keyComparator: Comparator[K] = ordering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K): Int = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })

  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  // Information about a spilled file. Includes sizes in bytes of "batches" written by the
  // serializer as we periodically reset its stream, as well as number of elements in each
  // partition, used to efficiently keep track of partitions when merging.
  private[this] case class SpilledFile(
    file: File,
    blockId: BlockId,
    serializerBatchSizes: Array[Long],
    elementsPerPartition: Array[Long])

  private val spills = new ArrayBuffer[SpilledFile]

  /**
   * Number of files this sorter has spilled so far.
   * Exposed for testing.
   */
  private[spark] def numSpills: Int = spills.size

  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined

    // 使用map和buffer的一个重要不同是：使用map，会对具有相同key的健值对通过aggregator进行合并
    if (shouldCombine) { // 如果定义了map端的aggregator
      // Combine values in-memory first using our AppendOnlyMap
      val mergeValue = aggregator.get.mergeValue
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        // 如果有旧值，则合并value，反之，则创建一个新的combiner
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        addElementsRead()
        kv = records.next()
        // changeValue()会将拥有相同((getPartition(kv._1), kv._1))的健值对合并到一起，
        // 最后map中存储的是((getPartition(kv._1), kv._1), combiner)的形式.
        // 需要注意的是，map真正的底层实现是一个数组:data(i) = key, data(2 * i + 1) = value。
        // 详见AppendOnlyMap的实现。
        map.changeValue((getPartition(kv._1), kv._1), update)
        // 查看map占用的内存是否达到阈值（在记录插入到map的过程中，如果map已满，会自动扩增size），
        // 如果是，则spill map中的记录到磁盘
        maybeSpillCollection(usingMap = true)
      }
    } else {
      // 我们先看map端不需要combine的情况...
      // Stick values into our buffer
      while (records.hasNext) {
        addElementsRead()
        val kv = records.next()
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        // 如果最后一个record插入后，没有发生spill，那么，buffer(内存中)还是有元素的
        maybeSpillCollection(usingMap = false)
      }
    }
  }

  /**
   * 如果需要，则将当前内存中的集合（记录）spill到磁盘上
   * Spill the current in-memory collection to disk if needed.
   *
   * @param usingMap whether we're using a map or buffer as our current in-memory collection
   */
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    if (usingMap) { // 如果使用的是map作为内存中缓存数据的数据结构
      // 获取map在jvm中的估计大小
      estimatedSize = map.estimateSize()
      if (maybeSpill(map, estimatedSize)) {
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else {
      estimatedSize = buffer.estimateSize()
      if (maybeSpill(buffer, estimatedSize)) {
        // 如果成功spill，则重新创建新的buffer
        // 所以之前的buffer是因为没有了引用，而被gc自动回收吗???
        buffer = new PartitionedPairBuffer[K, C]
      }
    }

    // 更新buffer／map的内存使用峰值
    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }

  /**
   * Spill our in-memory collection to a sorted file that we can merge later.
   * We add this file into `spilledFiles` to find it later.
   *
   * @param collection whichever collection we're using (map or buffer)
   */
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    // 该inMemoryIterator的记录已经根据partitionID（或和Key）排好了序
    val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
    val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
    spills += spillFile
  }

  /**
   * Force to spilling the current in-memory collection to disk to release memory,
   * It will be called by TaskMemoryManager when there is not enough memory for the task.
   */
  override protected[this] def forceSpill(): Boolean = {
    if (isShuffleSort) {
      false
    } else {
      assert(readingIterator != null)
      val isSpilled = readingIterator.spill()
      if (isSpilled) {
        map = null
        buffer = null
      }
      isSpilled
    }
  }

  /**
   * Spark Spill策略：内存中的iterator(已经根据partitionID或和Key排好序)会依次将元素
   * 通过DiskBlockObjectWriter写入本地磁盘的临时文件中。每当写入的元素个数到达
   * serializerBatchSize(可通过spark.shuffle.spill.batchSize配置，默认10000)个时，就会把
   * 这serializerBatchSize个元素组成一个batch(FileSegment)。同一个partition中，最后不足
   * serializerBatchSize个的元素组成一个batch。最终，各个partition的所有batch即相关信息组成
   * 一个SpilledFile。
   * 需要注意是：1）一个SpilledFile中的一个partition可能会包含多个batch，详见L317-L321的注释
   *           2）元素会通过序列化流来写入磁盘。因此，当使用SpillReader来读取溢写至磁盘的元
   *              素时，又需要反序列之。
   *
   * 将内存中的元素溢写至磁盘中的临时文件
   * Spill contents of in-memory iterator to a temporary file on disk.
   */
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: WritablePartitionedIterator)
      : SpilledFile = {
    // Because these files may be read during shuffle, their compression must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more context.
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()

    // These variables are reset after each flush
    var objectsWritten: Long = 0
    val spillMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics
    // file指定了writer将往哪个文件写入内容
    val writer: DiskBlockObjectWriter =
      blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // How many elements we have in each partition
    val elementsPerPartition = new Array[Long](numPartitions)

    // Flush the disk writer's contents to disk, and update relevant variables.
    // The writer is committed at the end of this process.
    def flush(): Unit = {
      val segment = writer.commitAndGet()
      batchSizes += segment.length
      _diskBytesSpilled += segment.length
      objectsWritten = 0
    }

    var success = false
    try {
      // 注意，inMemoryIterator中的记录已经根据partitionID或和Key排好序
      while (inMemoryIterator.hasNext) {
        val partitionId = inMemoryIterator.nextPartition()
        require(partitionId >= 0 && partitionId < numPartitions,
          s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")
        inMemoryIterator.writeNext(writer)
        elementsPerPartition(partitionId) += 1
        objectsWritten += 1

        // 如果写入的元素个数达到serializerBatchSize(默认10000)个，则批量的flush元素
        // 存在一个可能是：一个partition包含多个batch(大小为serializerBatchSize)，如：
        // 在该partition中有25000个元素，那么，每10000个就会生成一个batch，然后最后5000个元素，
        // 又会生成一个batch(虽然最后只剩5000个元素，没达到10000个，但也不会和其它的partition
        // 的元素去凑成一个batch。因为，一个batch必须是同一个partition中的元素)
        if (objectsWritten == serializerBatchSize) {
          flush()
        }
      }
      if (objectsWritten > 0) {
        flush()
      } else {
        writer.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (success) {
        writer.close()
      } else {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        writer.revertPartialWritesAndClose()
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }
    // 返回该spill file（注意该BlockId是一个TempShuffleBlockId）
    // 该file的结构是：一个file中有存储了多个partition的数据，
    // 而每个partition又由多个batches组成。
    SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition)
  }

  /**
   * Merge a sequence of sorted files, giving an iterator over partitions and then over elements
   * inside each partition. This can be used to either write out a new file or return data to
   * the user.
   *
   * Returns an iterator over all the data written to this object, grouped by partition. For each
   * partition we then have an iterator over its contents, and these are expected to be accessed
   * in order (you can't "skip ahead" to one partition without reading the previous one).
   * Guaranteed to return a key-value pair for each partition, in order of partition ID.
   */
  private def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] = {
    val readers = spills.map(new SpillReader(_))
    val inMemBuffered = inMemory.buffered
    (0 until numPartitions).iterator.map { p =>
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)
      // _.readNextPartition()和inMemIterator都是属于p分区的iterator
      // iterators由两部分组成：一部分是各个SpilledFile中的p分区中的元素构成的各个iterator，
      // 另一部分是内存中的p分区的元素构成的iterator
      val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)
      // 如果定义了aggregator，需要将记录按key值combine
      if (aggregator.isDefined) {
        // 大致是读明白了...主要是spark实际运用经验不够丰富，很多场景没遇到
        // 乍一看，哇，这个函数真是复杂，但只要耐心的看，就发现其实也很简单
        // Perform partial aggregation across partitions
        (p, mergeWithAggregation(
          iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
      } else if (ordering.isDefined) { // 如果只是定义了ordering
        // No aggregator given, but we have an ordering (e.g. used by reduce tasks in sortByKey);
        // sort the elements without trying to merge them
        (p, mergeSort(iterators, ordering.get))
      } else {
        // 如果啥也没定义（太好了，执行效率最高啦）
        (p, iterators.iterator.flatten)
      }
    }
  }

  /**
   * 注意，我们已经保证这个Seq中的iterators都是一个partition的
   * Merge-sort a sequence of (K, C) iterators using a given a comparator for the keys.
   */
  private def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K])
      : Iterator[Product2[K, C]] =
  {
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
    type Iter = BufferedIterator[Product2[K, C]]
    val heap = new mutable.PriorityQueue[Iter]()(new Ordering[Iter] {
      // 之所以使用和comparator.compare相反的比较结果，是因为PriorityQueue优先出队优先级最大的元素
      // Use the reverse of comparator.compare because PriorityQueue dequeues the max
      // 貌似这里的大小比较，只关注了key，不关注partition（废话，这里的所有iter都已经是一个partition中的，
      // 还比较partition干啥？）
      override def compare(x: Iter, y: Iter): Int = -comparator.compare(x.head._1, y.head._1)
    })
    // _* 再次遇到，什么意思???理解不够透彻
    // _* 貌似是说可以有一个或多个参数，就像java的 ...
    // 这里，bufferedIters是一个包含多个元素的seq()。
    // 所以，enqueue()会依次遍历seq()中的每个元素（Iterator[Product2[K, C]]），并入队。
    heap.enqueue(bufferedIters: _*)  // Will contain only the iterators with hasNext = true
    new Iterator[Product2[K, C]] {
      override def hasNext: Boolean = !heap.isEmpty

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val firstBuf = heap.dequeue()
        val firstPair = firstBuf.next()
        // 如果该buffered iterator还有元素，则将其重新入队
        if (firstBuf.hasNext) {
          heap.enqueue(firstBuf)
        }
        firstPair
      }
    }
  }

  /**
   * Merge a sequence of (K, C) iterators by aggregating values for each key, assuming that each
   * iterator is sorted by key with a given comparator. If the comparator is not a total ordering
   * (e.g. when we sort objects by hash code and different keys may compare as equal although
   * they're not), we still merge them by doing equality tests for all keys that compare as equal.
   */
  private def mergeWithAggregation(
      iterators: Seq[Iterator[Product2[K, C]]],
      mergeCombiners: (C, C) => C,
      comparator: Comparator[K],
      totalOrder: Boolean)
      : Iterator[Product2[K, C]] =
  {
    // 所谓的totalOrder是指：如果我们定义的comparator，能让所有值不等的key在使用comparator比较之后同样不等(1)；
    // 而非totalOrder，例如以hash(key)比较值不等的key，则可能会有hash(key1) == hash(key2)。具有这种性质的
    // comparator排序就被认为是非totalOrder。
    // （但是，怎么看该方法的调用，好像如果定义了ordering，就是totalOrder了呢？如果用户定义的ordering不满足性质
    // (1)怎么办？？？）
    if (!totalOrder) {
      // 我们只拥有局部有序。例如：通过hash code对keys进行排序，这意味着多个不同的key，从ordering的角度来看，
      // 被认为是相等的。为了处理这个问题(因为combiner是合并那些key真正相等的pair，而不是说ordering认为它们
      // 相等，就可以合并。比如，有三个健值对(k1, v1), (k2, v2), (k1, v3),在ordering看来，可能k1,k2是相等
      // 的，但是combiner却不能合并(k1, v1)和(k2, v2), 因为它们的值并不真的相同)，我们需要读取所有ordering
      // 认为相等的key，然后进一步比较它们到底是否相等。
      // We only have a partial ordering, e.g. comparing the keys by hash code, which means that
      // multiple distinct keys might be treated as equal by the ordering. To deal with this, we
      // need to read all keys considered equal by the ordering at once and compare them.
      new Iterator[Iterator[Product2[K, C]]] {
        // 因为我们只有局部有序，所以在mergeSort()之后，记录的排序可能是这样的(假设我们的comparator是奇数key
        // 小于偶数key)：
        // (1, 5), (3, 5), (1, 4), (2, 1), (4, 2), (2, 5)
        // 所以，comparator就会认为key = 1和key = 3相等。但是，它们的实际值并不相等，于是乎，它们也不能combine。
        val sorted = mergeSort(iterators, comparator).buffered

        // Buffers reused across elements to decrease memory allocation
        val keys = new ArrayBuffer[K]
        val combiners = new ArrayBuffer[C]

        override def hasNext: Boolean = sorted.hasNext

        // next()返回的是一个iterator???
        override def next(): Iterator[Product2[K, C]] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          keys.clear()
          combiners.clear()
          // 从sorted iterator中取出第一个健值对
          val firstPair = sorted.next()
          // 因为第一个健值对肯定没在keys和combiners中出现过，
          // 所以，都需要添加进去
          keys += firstPair._1
          combiners += firstPair._2
          val key = firstPair._1
          // 找到所有ordering认为和key相等的健值对
          while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
            // 该pair的key在ordering看来和给定的key相等
            val pair = sorted.next()
            var i = 0
            var foundKey = false
            // 接下来要做的是：从已知的keys中去寻找，有没有和该pair的key真正(值)相等的key
            while (i < keys.size && !foundKey) {
              if (keys(i) == pair._1) {
                // 如果找到了一个真正(值)相等的key，则通过combiner合并
                combiners(i) = mergeCombiners(combiners(i), pair._2)
                // 说明该pair的key之前已经存在于keys中，不是第一次出现
                foundKey = true
              }
              i += 1
            }
            // 说明该pair的key是第一次出现，则添加到keys和combiners中去
            if (!foundKey) {
              keys += pair._1
              combiners += pair._2
            }
          }

          // zip()的结果也是返回一个iterator
          // Note that we return an iterator of elements since we could've had many keys marked
          // equal by the partial order; we flatten this below to get a flat iterator of (K, C).
          keys.iterator.zip(combiners.iterator)
        }
        // 这里的语法哦...晕...看不懂
      }.flatMap(i => i)
    } else {
      // 我们全局有序！
      // We have a total ordering, so the objects with the same key are sequential.
      new Iterator[Product2[K, C]] {
        // mergeSort()会根据指定的comparator，对key进行排序
        // 因为我们的comparator全局有序，所以mergeSort()之后记录排序会是这样的：
        // (1,5), (1, 4), (2, 4), (2,6), (2, 10), (3, 5)
        // 也就是说，值相等的key是连续出现的，而不像上面的局部有序。
        val sorted = mergeSort(iterators, comparator).buffered

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Product2[K, C] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          val elem = sorted.next()
          val k = elem._1
          var c = elem._2
          // 因为comparator是全局有序的，所以才可以这样循环合并
          // 把具有相同key的(key, value)都通过mergeCombiners()合并起来
          while (sorted.hasNext && sorted.head._1 == k) {
            val pair = sorted.next()
            c = mergeCombiners(c, pair._2)
          }
          (k, c)
        }
      }
    }
  }

  /**
   * 一个用于一个一个partition的读取spilled file的内部类。
   * An internal class for reading a spilled file partition by partition. Expects all the
   * partitions to be requested in order.
   */
  private[this] class SpillReader(spill: SpilledFile) {
    // scanLeft(v)(f)会以v为初始值，从左到右应用f到集合的元素上，并返回所有中间结果。
    // 例如：(1 to 10).scanLeft(0)(_ + _)返回的结果是：
    // List(0, 1, 3, 6, 10, 15, 21, 28, 36, 45, 55)

    // 由此，下面这行代码的意思也就好解释了：把原先记录各个partition的size(注意：该size只
    // 包含了一个SpillFile中的各个partition的元素，而不是整个map task中的各个partition
    // 的所有元素)的array，转换成一个记录各个partition偏移量的array(包含第一个partition
    // 的0偏移量).所以，最终，batchOffsets的size是serializerBatchSizes.length + 1
    // Serializer batch offsets; size will be batchSize.length + 1
    val batchOffsets = spill.serializerBatchSizes.scanLeft(0L)(_ + _)

    // Track which partition and which batch stream we're in. These will be the indices of
    // the next element we will read. We'll also store the last partition read so that
    // readNextPartition() can figure out what partition that was from.
    var partitionId = 0
    var indexInPartition = 0L
    var batchId = 0
    var indexInBatch = 0
    var lastPartitionId = 0

    // 初始化的时候调用该方法，只有一种可能的情况：
    // 如果第一个partition为空，则跳过该partition。
    // 而不可能说已经访问到该partition的末尾。
    skipToNextPartition()

    // Intermediate file and deserializer streams that read from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    var fileStream: FileInputStream = null
    var deserializeStream = nextBatchStream()  // Also sets fileStream

    var nextItem: (K, C) = null
    var finished = false

    /** Construct a stream that only reads from the next batch */
    def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      if (batchId < batchOffsets.length - 1) {
        // 关闭上一个batch的流
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        // 确定该batch的起始偏移位置
        val start = batchOffsets(batchId)
        fileStream = new FileInputStream(spill.file)
        fileStream.getChannel.position(start)
        batchId += 1

        // 确定该batch的终止偏移位置(也是下一个batch的起始偏移位置)
        val end = batchOffsets(batchId)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        // 封装文件流，创建缓存输入流
        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))

        // 封装缓存输入流，创建解压流
        val wrappedStream = serializerManager.wrapStream(spill.blockId, bufferedStream)
        // 封装压缩流，创建反序列化流
        serInstance.deserializeStream(wrappedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * 当我们访问到当前partition的末尾时，则更新partitionId，在该过程中，同时也有可能会跳过空的partition。
     * Update partitionId if we have reached the end of our current partition, possibly skipping
     * empty partitions on the way.
     */
    private def skipToNextPartition() {
      // 如果indexInPartition为0，且spill.elementsPerPartition(partitionId)(说明该partition为空)，
      // 则可以完美地跳过。
      while (partitionId < numPartitions &&
          indexInPartition == spill.elementsPerPartition(partitionId)) {
        partitionId += 1
        indexInPartition = 0L
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream and update partitionId,
     * indexInPartition, indexInBatch and such to match its location.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     */
    private def readNextItem(): (K, C) = {
      if (finished || deserializeStream == null) {
        return null
      }
      // 反序列化
      // 从反序列化流中分别读取key和value
      val k = deserializeStream.readKey().asInstanceOf[K]
      val c = deserializeStream.readValue().asInstanceOf[C]
      // 更新上一个访问的partitionId
      lastPartitionId = partitionId
      // Start reading the next batch if we're done with this one
      // 读取的元素在该batch中的索引位置
      indexInBatch += 1
      // 如果读取的元素个数达到了serializerBatchSize个，说明该batch的所有元素都读完了。
      // 则开始读取该partition中的下一个batch。
      // 这和我们spill是写入元素至磁盘文件时的batch size大小是对应的。
      if (indexInBatch == serializerBatchSize) {
        indexInBatch = 0
        deserializeStream = nextBatchStream()
      }
      // Update the partition location of the element we're reading
      // 读取的元素在该partition中的索引位置(一个partition可能包含多个batch)
      indexInPartition += 1
      // 检查是否访问完了该partition中的所有元素，如果是，则开始访问下一个partition
      skipToNextPartition()
      // 说明我们访问(读取)完了所有的partition(中的所有元素)
      // If we've finished reading the last partition, remember that we're done
      if (partitionId == numPartitions) {
        finished = true
        if (deserializeStream != null) {
          deserializeStream.close()
        }
      }
      (k, c)
    }

    var nextPartitionToRead = 0

    def readNextPartition(): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
      val myPartition = nextPartitionToRead
      nextPartitionToRead += 1

      override def hasNext: Boolean = {
        if (nextItem == null) {
          nextItem = readNextItem()
          if (nextItem == null) {
            return false
          }
        }
        // QUESTION: 为啥是 >= ？
        assert(lastPartitionId >= myPartition)
        // 相等的话，说明我们还在读取该myPartition中的记录
        // Check that we're still in the right partition; note that readNextItem will have returned
        // null at EOF above so we would've returned false there
        lastPartitionId == myPartition
      }

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val item = nextItem
        nextItem = null
        item
      }
    }

    // Clean up our open streams and put us in a state where we can't read any more data
    def cleanup() {
      batchId = batchOffsets.length  // Prevent reading any other batch
      val ds = deserializeStream
      deserializeStream = null
      // 文件流不用close吗???是不是会在deserializeStream的close()中close???
      fileStream = null
      if (ds != null) {
        ds.close()
      }
      // NOTE: We don't do file.delete() here because that is done in ExternalSorter.stop().
      // This should also be fixed in ExternalAppendOnlyMap.
    }
  }

  /**
   * Returns a destructive iterator for iterating over the entries of this map.
   * If this iterator is forced spill to disk to release memory when there is not enough memory,
   * it returns pairs from an on-disk map.
   */
  def destructiveIterator(memoryIterator: Iterator[((Int, K), C)]): Iterator[((Int, K), C)] = {
    // TODO read isShuffleSort什么意思???
    // 如果这是map端的shuffle，则isShuffleSort = true; 反之为false？？？
    if (isShuffleSort) {
      memoryIterator
    } else {
      readingIterator = new SpillableIterator(memoryIterator)
      readingIterator
    }
  }

  /**
   * Return an iterator over all the data written to this object, grouped by partition and
   * aggregated by the requested aggregator. For each partition we then have an iterator over its
   * contents, and these are expected to be accessed in order (you can't "skip ahead" to one
   * partition without reading the previous one). Guaranteed to return a key-value pair for each
   * partition, in order of partition ID.
   *
   * For now, we just merge all the spilled files in once pass, but this can be modified to
   * support hierarchical merging.
   * Exposed for testing.
   */
  def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    val usingMap = aggregator.isDefined
    // 如果定义了aggregator，则使用map，反之，使用buffer
    val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) map else buffer
    if (spills.isEmpty) {
      // Special case: if we have only in-memory data, we don't need to merge streams, and perhaps
      // we don't even need to sort by anything other than partition ID
      if (!ordering.isDefined) {
        // The user hasn't requested sorted keys, so only sort by partition ID, not key
        groupByPartition(destructiveIterator(collection.partitionedDestructiveSortedIterator(None)))
      } else {
        // We do need to sort by both partition ID and key
        groupByPartition(destructiveIterator(
          collection.partitionedDestructiveSortedIterator(Some(keyComparator))))
      }
    } else {
      // 合并溢写的和内存中的数据
      // Merge spilled and in-memory data
      merge(spills, destructiveIterator(
        collection.partitionedDestructiveSortedIterator(comparator)))
    }
  }

  /**
   * Return an iterator over all the data written to this object, aggregated by our aggregator.
   */
  def iterator: Iterator[Product2[K, C]] = {
    isShuffleSort = false
    partitionedIterator.flatMap(pair => pair._2)
  }

  /**
   * Write all the data added into this ExternalSorter into a file in the disk store. This is
   * called by the SortShuffleWriter.
   *
   * @param blockId block ID to write to. The index file will be blockId.name + ".index".
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker)
   */
  def writePartitionedFile(
      blockId: BlockId,
      outputFile: File): Array[Long] = {

    // Track location of each range in the output file
    val lengths = new Array[Long](numPartitions)
    val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
      context.taskMetrics().shuffleWriteMetrics)

    if (spills.isEmpty) {
      // spills为空，说明在这种情况下，我们只有在内存中的数据
      // Case where we only have in-memory data
      val collection = if (aggregator.isDefined) map else buffer
      // 对内存中的数据(按照partition ID或partitionID和key)排序，并返回WritablePartitionedIterator
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      // 由于it(至少)是按照partitionID排过序的，所以此时的records是按照partitionID连续出现的。
      // 例如：(p1, v11), (p1,v12), (p2, v21), (p3, v31), (p3, v32), (p3, v33)
      // 由此，下面的代码才变得清晰起来。
      while (it.hasNext) {
        val partitionId = it.nextPartition()
        while (it.hasNext && it.nextPartition() == partitionId) {
          it.writeNext(writer)
        }
        // 将同一个分区内的写入文件的内容提交，并生成一个FileSegment
        // 例如：(p1, v11)和(p1,v12)生成一个FileSegment；
        // (p2, v21)单独生成一个FileSegment;
        // (p3, v31), (p3, v32), (p3, v33)这三个生成一个FileSegment
        val segment = writer.commitAndGet()
        lengths(partitionId) = segment.length
      }
    } else {
      // 注意：merger-sort的步骤很重要，感觉也是spark在shuffle过程中很耗时的一个阶段
      // We must perform merge-sort; get an iterator by partition and write everything directly.
      for ((id, elements) <- this.partitionedIterator) {
        // elements是同一个id对应的分区内的所有经过merge sort后的元素
        if (elements.hasNext) {
          for (elem <- elements) {
            writer.write(elem._1, elem._2)
          }
          // 同样地，将同一个分区内的写入文件的内容提交，并生成一个FileSegment
          val segment = writer.commitAndGet()
          // 记录每个partition的记录的size
          lengths(id) = segment.length
        }
      }
    }

    writer.close()
    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)

    lengths
  }

  // spills在map端shuffle的时候用到了，但是forceSpillFiles好像没有在map端shuffle的时候用到。
  // 所以，forceSpillFiles是不是在reduce端shuffle的时候用到的？？？
  def stop(): Unit = {
    // 既然我们已经将spills对应的files合并成了一个MapOutput，则它们就可被删除了
    spills.foreach(s => s.file.delete())
    spills.clear()
    forceSpillFiles.foreach(s => s.file.delete())
    forceSpillFiles.clear()
    if (map != null || buffer != null) {
      map = null // So that the memory can be garbage-collected
      buffer = null // So that the memory can be garbage-collected
      // 是否execution memory
      releaseMemory()
    }
  }

  /**
   * Given a stream of ((partition, key), combiner) pairs *assumed to be sorted by partition ID*,
   * group together the pairs for each partition into a sub-iterator.
   *
   * @param data an iterator of elements, assumed to already be sorted by partition ID
   */
  private def groupByPartition(data: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] =
  {
    // BufferedIterator的作用是：原始iterator的next移动，会导致所有buffered iterator的next移动(
    // 就相当于一个软连接，希望你能明白我的意思)
    val buffered = data.buffered
    (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
  }

  /**
   * An iterator that reads only the elements for a given partition ID from an underlying buffered
   * stream, assuming this partition is the next one to be read. Used to make it easier to return
   * partitioned iterators from our in-memory collection.
   */
  private[this] class IteratorForPartition(partitionId: Int, data: BufferedIterator[((Int, K), C)])
    extends Iterator[Product2[K, C]]
  {
    override def hasNext: Boolean = data.hasNext && data.head._1._1 == partitionId

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      // elem((partitionId, key), value)
      val elem = data.next()
      (elem._1._2, elem._2)
    }
  }

  private[this] class SpillableIterator(var upstream: Iterator[((Int, K), C)])
    extends Iterator[((Int, K), C)] {

    private val SPILL_LOCK = new Object()

    private var nextUpstream: Iterator[((Int, K), C)] = null

    private var cur: ((Int, K), C) = readNext()

    private var hasSpilled: Boolean = false

    def spill(): Boolean = SPILL_LOCK.synchronized {
      if (hasSpilled) {
        false
      } else {
        val inMemoryIterator = new WritablePartitionedIterator {
          private[this] var cur = if (upstream.hasNext) upstream.next() else null

          def writeNext(writer: DiskBlockObjectWriter): Unit = {
            writer.write(cur._1._2, cur._2)
            cur = if (upstream.hasNext) upstream.next() else null
          }

          def hasNext(): Boolean = cur != null

          def nextPartition(): Int = cur._1._1
        }
        logInfo(s"Task ${context.taskAttemptId} force spilling in-memory map to disk and " +
          s" it will release ${org.apache.spark.util.Utils.bytesToString(getUsed())} memory")
        val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
        forceSpillFiles += spillFile
        val spillReader = new SpillReader(spillFile)
        nextUpstream = (0 until numPartitions).iterator.flatMap { p =>
          val iterator = spillReader.readNextPartition()
          iterator.map(cur => ((p, cur._1), cur._2))
        }
        hasSpilled = true
        true
      }
    }

    def readNext(): ((Int, K), C) = SPILL_LOCK.synchronized {
      if (nextUpstream != null) {
        upstream = nextUpstream
        nextUpstream = null
      }
      if (upstream.hasNext) {
        upstream.next()
      } else {
        null
      }
    }

    override def hasNext(): Boolean = cur != null

    override def next(): ((Int, K), C) = {
      val r = cur
      cur = readNext()
      r
    }
  }
}
