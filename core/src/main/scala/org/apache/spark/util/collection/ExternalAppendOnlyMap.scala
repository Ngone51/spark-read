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

import scala.collection.BufferedIterator
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, Serializer, SerializerManager}
import org.apache.spark.storage.{BlockId, BlockManager}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalAppendOnlyMap.HashComparator

/**
 * :: DeveloperApi ::
 *
 * 注意， ExternalAppendOnlyMap是一个MemoryConsumer
 * An append-only map that spills sorted content to disk when there is insufficient space for it
 * to grow.
 *
 * This map takes two passes over the data:
 *
 *   (1) Values are merged into combiners, which are sorted and spilled to disk as necessary
 *   (2) Combiners are read from disk and merged together
 *
 * The setting of the spill threshold faces the following trade-off: If the spill threshold is
 * too high, the in-memory map may occupy more memory than is available, resulting in OOM.
 * However, if the spill threshold is too low, we spill frequently and incur unnecessary disk
 * writes. This may lead to a performance regression compared to the normal case of using the
 * non-spilling AppendOnlyMap.
 */
@DeveloperApi
class ExternalAppendOnlyMap[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    serializer: Serializer = SparkEnv.get.serializer,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    context: TaskContext = TaskContext.get(),
    serializerManager: SerializerManager = SparkEnv.get.serializerManager)
  extends Spillable[SizeTracker](context.taskMemoryManager())
  with Serializable
  with Logging
  with Iterable[(K, C)] {

  if (context == null) {
    throw new IllegalStateException(
      "Spillable collections should not be instantiated outside of tasks")
  }

  // Backwards-compatibility constructor for binary compatibility
  def this(
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      serializer: Serializer,
      blockManager: BlockManager) {
    this(createCombiner, mergeValue, mergeCombiners, serializer, blockManager, TaskContext.get())
  }

  @volatile private var currentMap = new SizeTrackingAppendOnlyMap[K, C]
  private val spilledMaps = new ArrayBuffer[DiskMapIterator]
  private val sparkConf = SparkEnv.get.conf
  private val diskBlockManager = blockManager.diskBlockManager

  /**
   * Size of object batches when reading/writing from serializers.
   *
   * Objects are written in batches, with each batch using its own serialization stream. This
   * cuts down on the size of reference-tracking maps constructed when deserializing a stream.
   *
   * NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
   * grow internal data structures by growing + copying every time the number of objects doubles.
   */
  private val serializerBatchSize = sparkConf.getLong("spark.shuffle.spill.batchSize", 10000)

  // Number of bytes spilled in total
  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize =
    sparkConf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // Write metrics
  private val writeMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics()

  // Peak size of the in-memory map observed so far, in bytes
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  private val keyComparator = new HashComparator[K]
  private val ser = serializer.newInstance()

  @volatile private var readingIterator: SpillableIterator = null

  /**
   * Number of files this map has spilled so far.
   * Exposed for testing.
   */
  private[collection] def numSpills: Int = spilledMaps.size

  /**
   * Insert the given key and value into the map.
   */
  def insert(key: K, value: V): Unit = {
    insertAll(Iterator((key, value)))
  }

  /**
   * （该方法的代码和ExternalSorter的insertAll()简直一模一样啊。。。什么情况???）
   * Insert the given iterator of keys and values into the map.
   *
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *
   * （它自己也说了，这里有内存的使用是没有被MemoryManager所tracked的）
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   */
  def insertAll(entries: Iterator[Product2[K, V]]): Unit = {
    if (currentMap == null) {
      throw new IllegalStateException(
        "Cannot insert new elements into a map after calling iterator")
    }
    // An update function for the map that we reuse across entries to avoid allocating
    // a new closure each time
    var curEntry: Product2[K, V] = null
    val update: (Boolean, C) => C = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, curEntry._2) else createCombiner(curEntry._2)
    }

    while (entries.hasNext) {
      curEntry = entries.next()
      val estimatedSize = currentMap.estimateSize()
      if (estimatedSize > _peakMemoryUsedBytes) {
        _peakMemoryUsedBytes = estimatedSize
      }
      if (maybeSpill(currentMap, estimatedSize)) {
        currentMap = new SizeTrackingAppendOnlyMap[K, C]
      }
      // 注意：我们在AppendOnlyMap中插入记录(k, v),一定会对具有相同key值的记录（通过update方法）
      // 进行merge。因此，在currentMap被spill到磁盘的时候，磁盘中的记录也已经是经过merge的。
      currentMap.changeValue(curEntry._1, update)
      addElementsRead()
    }
  }

  /**
   * Insert the given iterable of keys and values into the map.
   *
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   */
  def insertAll(entries: Iterable[Product2[K, V]]): Unit = {
    insertAll(entries.iterator)
  }

  /**
   * 对该内存中的map的元素排序，并spill到磁盘上的一个临时文件中
   * Sort the existing contents of the in-memory map and spill them to a temporary file on disk.
   */
  override protected[this] def spill(collection: SizeTracker): Unit = {
    // keyComparator貌似默认就定死为HashComparator了（是不是因为在BlockStoreShuffleReader之后还要
    // （如果定义了ordering）按key进行排序？）
    // 现在，inMemoryIterator是已经根据key的hash值排过序的了（该方法会破坏currentMap的结构）
    val inMemoryIterator = currentMap.destructiveSortedIterator(keyComparator)
    // 将inMemoryIterator spill到磁盘上，返回DiskMapIterator。该iterator可以从磁盘有序（按
    // 上述keyComparator排序的结果）地读取（k，v）记录。
    val diskMapIterator = spillMemoryIteratorToDisk(inMemoryIterator)
    // 更新spilledMaps
    spilledMaps += diskMapIterator
  }

  /**
   * 强制将当前内存中的集合spill到磁盘上，以释放内存。
   * 该方法会在task所需的内存不足时，通过TaskMemoryManager强制调用(所噶。。。之前在TaskMemoryManager
   * 有些地方的处理看不明白，比如，一个task会对应多个consumer吗？现在是联系起来了。。。[说的啥玩意儿])
   * （2018.04.09）看到这里，我们需要再看看TaskMemoryManager！！！
   * Force to spilling the current in-memory collection to disk to release memory,
   * It will be called by TaskMemoryManager when there is not enough memory for the task.
   */
  override protected[this] def forceSpill(): Boolean = {
    if (readingIterator != null) {
      val isSpilled = readingIterator.spill()
      if (isSpilled) {
        currentMap = null
      }
      isSpilled
    } else if (currentMap.size > 0) {
      spill(currentMap)
      currentMap = new SizeTrackingAppendOnlyMap[K, C]
      true
    } else {
      false
    }
  }

  /**
   * Spill the in-memory Iterator to a temporary file on disk.
   */
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: Iterator[(K, C)])
      : DiskMapIterator = {
    val (blockId, file) = diskBlockManager.createTempLocalBlock()
    val writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, writeMetrics)
    var objectsWritten = 0

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // Flush the disk writer's contents to disk, and update relevant variables
    def flush(): Unit = {
      // 在这里我们并不需要保存该segment（有些地方要的）
      val segment = writer.commitAndGet()
      batchSizes += segment.length
      _diskBytesSpilled += segment.length
      objectsWritten = 0
    }

    var success = false
    try {
      while (inMemoryIterator.hasNext) {
        val kv = inMemoryIterator.next()
        writer.write(kv._1, kv._2)
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          flush()
        }
      }
      if (objectsWritten > 0) {
        flush()
        writer.close()
      } else {
        // 如果 objectsWritten = 0，也可以revert，
        // 只不过revert的内容为空，没有影响的。
        writer.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (!success) {
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

    new DiskMapIterator(file, blockId, batchSizes)
  }

  /**
   * 注意，调用该方式时，我们的currentMap的结构没有被破坏
   * Returns a destructive iterator for iterating over the entries of this map.
   * If this iterator is forced spill to disk to release memory when there is not enough memory,
   * it returns pairs from an on-disk map.
   */
  def destructiveIterator(inMemoryIterator: Iterator[(K, C)]): Iterator[(K, C)] = {
    // SpillableIterator：一个可用于在内存不足时，spill iterator到磁盘，并转而用DiskMapIterator来
    // 读取元素的iterator。
    readingIterator = new SpillableIterator(inMemoryIterator)
    readingIterator
  }

  /**
   * Return a destructive iterator that merges the in-memory map with the spilled maps.
   * If no spill has occurred, simply return the in-memory map's iterator.
   */
  override def iterator: Iterator[(K, C)] = {
    if (currentMap == null) {
      throw new IllegalStateException(
        "ExternalAppendOnlyMap.iterator is destructive and should only be called once.")
    }
    // 如果没有发生过spill，则直接返回in-memory map的iterator
    // (当然还需要额外的处理，比如让该iterator可以发生spill)
    if (spilledMaps.isEmpty) {
      // 首先调用currentMap#iterator()方法，该方法会返回currentMap的一个iterator用于遍历map中的元素，
      // 但是，该方法不会破坏currentMap的结构。而如果调用destructiveSortedIterator，也会返回遍历map元素的
      // iterator，但是会破坏map的结构。
      CompletionIterator[(K, C), Iterator[(K, C)]](
        destructiveIterator(currentMap.iterator), freeCurrentMap())
    } else {
      // 说明发生了spill，产生了spilled maps，且当前（in-memory）currentMap里也可能还有数据
      new ExternalIterator()
    }
  }

  private def freeCurrentMap(): Unit = {
    if (currentMap != null) {
      currentMap = null // So that the memory can be garbage-collected
      releaseMemory()
    }
  }

  /**
   * An iterator that sort-merges (K, C) pairs from the in-memory map and the spilled maps
   */
  private class ExternalIterator extends Iterator[(K, C)] {

    // A queue that maintains a buffer for each stream we are currently merging
    // This queue maintains the invariant that it only contains non-empty buffers
    private val mergeHeap = new mutable.PriorityQueue[StreamBuffer]

    // inputStreams由in-memory map和spilled maps on disk两部分共同构成。in-memory map就在现在开始排序，
    // 而spilled maps在写入磁盘的时候已经排好序。
    // Input streams are derived both from the in-memory map and spilled maps on disk
    // The in-memory map is sorted in place, while the spilled maps are already in sorted order
    private val sortedMap = CompletionIterator[(K, C), Iterator[(K, C)]](destructiveIterator(
      currentMap.destructiveSortedIterator(keyComparator)), freeCurrentMap())
    private val inputStreams = (Seq(sortedMap) ++ spilledMaps).map(it => it.buffered)

    inputStreams.foreach { it =>
      val kcPairs = new ArrayBuffer[(K, C)]
      readNextHashCode(it, kcPairs)
      if (kcPairs.length > 0) {
        mergeHeap.enqueue(new StreamBuffer(it, kcPairs))
      }
    }

    /**
     * Fill a buffer with the next set of keys with the same hash code from a given iterator. We
     * read streams one hash code at a time to ensure we don't miss elements when they are merged.
     *
     * Assumes the given iterator is in sorted order of hash code.
     *
     * @param it iterator to read from
     * @param buf buffer to write the results into
     */
    private def readNextHashCode(it: BufferedIterator[(K, C)], buf: ArrayBuffer[(K, C)]): Unit = {
      if (it.hasNext) {
        var kc = it.next()
        buf += kc
        // 之所以it的第一个元素就是minHash，是因为我们这里的it里的元素(k,v)都是已经根据key排过序了
        val minHash = hashKey(kc)
        // QUESTION： 如果it.head._1.hashCode() != minHash了，但是it还没遍历完呢？？？
        // 答：我们只是在这里先把每个iterator中hash值最小的元素都找出来，组成各自的buf。然后
        // 在这所有的buf中，再去找hash值最小的。一个buf里的元素的hash值都是一样的，所以在这组
        // buf里的元素都是最小的。然后，如果这个buf被遍历完了，则我们再去该buf的iterator中取
        // 元素，组成一个新的buf，然后塞入mergeHeap中。

        // 把hash值一样的k塞到buf中（知道hash(k)不一样为止，退出while循环。
        // 注意：此时it中还有没塞到buffer中的元素（k，v）。不急，我们之后还会继续来处理）
        // 注意，有可能k1和k2不一样，但是它们的hash值一样（hash冲突）
        while (it.hasNext && it.head._1.hashCode() == minHash) {
          kc = it.next()
          buf += kc
        }
      }
    }

    /**
     * If the given buffer contains a value for the given key, merge that value into
     * baseCombiner and remove the corresponding (K, C) pair from the buffer.
     */
    private def mergeIfKeyExists(key: K, baseCombiner: C, buffer: StreamBuffer): C = {
      var i = 0
      while (i < buffer.pairs.length) {
        val pair = buffer.pairs(i)
        if (pair._1 == key) {
          // 注意，对于给定的key值最多会有一个pair被merge。因为currentMap(AppendOnlyMap)在插入记录(k,v)的
          // 时候，就会对记录进行merge，所以spill到磁盘的记录已经是merge过了的。所以，当我们发现第一个key相等
          // 的pair就直接return是没有问题的。
          // Note that there's at most one pair in the buffer with a given key, since we always
          // merge stuff in a map before spilling, so it's safe to return after the first we find
          removeFromBuffer(buffer.pairs, i)
          return mergeCombiners(baseCombiner, pair._2)
        }
        i += 1
      }
      baseCombiner
    }

    /**
     * Remove the index'th element from an ArrayBuffer in constant time, swapping another element
     * into its place. This is more efficient than the ArrayBuffer.remove method because it does
     * not have to shift all the elements in the array over. It works for our array buffers because
     * we don't care about the order of elements inside, we just want to search them for a key.
     */
    private def removeFromBuffer[T](buffer: ArrayBuffer[T], index: Int): T = {
      val elem = buffer(index)
      buffer(index) = buffer(buffer.size - 1)  // This also works if index == buffer.size - 1
      buffer.reduceToSize(buffer.size - 1)
      elem
    }

    /**
     * Return true if there exists an input stream that still has unvisited pairs.
     */
    override def hasNext: Boolean = mergeHeap.nonEmpty

    /**
     * Select a key with the minimum hash, then combine all values with the same key from all
     * input streams.
     */
    override def next(): (K, C) = {
      if (mergeHeap.isEmpty) {
        throw new NoSuchElementException
      }
      // Select a key from the StreamBuffer that holds the lowest key hash
      val minBuffer = mergeHeap.dequeue()
      val minPairs = minBuffer.pairs
      val minHash = minBuffer.minKeyHash
      // 意思是随便取出一个???
      val minPair = removeFromBuffer(minPairs, 0)
      val minKey = minPair._1
      var minCombiner = minPair._2
      // 就是从minBuffer里取出来的，怎么会不相等呢???
      assert(hashKey(minPair) == minHash)

      // For all other streams that may have this key (i.e. have the same minimum key hash),
      // merge in the corresponding value (if any) from that stream
      val mergedBuffers = ArrayBuffer[StreamBuffer](minBuffer)
      // 注意：mergeHeap后面跟的是head
      while (mergeHeap.nonEmpty && mergeHeap.head.minKeyHash == minHash) {
        val newBuffer = mergeHeap.dequeue()
        // 一个newBuffer中，最多只合并一个pair
        minCombiner = mergeIfKeyExists(minKey, minCombiner, newBuffer)
        mergedBuffers += newBuffer
      }

      // Repopulate each visited stream buffer and add it back to the queue if it is non-empty
      mergedBuffers.foreach { buffer =>
        // 如果buffer中所有的pair都合并好了，则我们继续取该buffer对应的
        // iterator的中剩下的pair
        if (buffer.isEmpty) {
          readNextHashCode(buffer.iterator, buffer.pairs)
        }
        // 如果buffer中还有未合并的pair，则重新将其加入到mergeHeap队列中
        if (!buffer.isEmpty) {
          mergeHeap.enqueue(buffer)
        }
      }

      (minKey, minCombiner)
    }

    /**
     * A buffer for streaming from a map iterator (in-memory or on-disk) sorted by key hash.
     * Each buffer maintains all of the key-value pairs with what is currently the lowest hash
     * code among keys in the stream. There may be multiple keys if there are hash collisions.
     * Note that because when we spill data out, we only spill one value for each key, there is
     * at most one element for each key.
     *
     * StreamBuffers are ordered by the minimum key hash currently available in their stream so
     * that we can put them into a heap and sort that.
     */
    private class StreamBuffer(
        val iterator: BufferedIterator[(K, C)],
        val pairs: ArrayBuffer[(K, C)])
      extends Comparable[StreamBuffer] {

      def isEmpty: Boolean = pairs.length == 0

      // Invalid if there are no more pairs in this stream
      def minKeyHash: Int = {
        assert(pairs.length > 0)
        hashKey(pairs.head)
      }

      override def compareTo(other: StreamBuffer): Int = {
        // 注意，这里我们的优先级是反过来的！
        // descending order because mutable.PriorityQueue dequeues the max, not the min
        if (other.minKeyHash < minKeyHash) -1 else if (other.minKeyHash == minKeyHash) 0 else 1
      }
    }
  }

  /**
   * An iterator that returns (K, C) pairs in sorted order from an on-disk map
   */
  private class DiskMapIterator(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
    extends Iterator[(K, C)]
  {
    // 假设我们的batchSizes为[10, 15, 25, 40]，则调用scanLeft，
    // 返回的batchOffsets为[0, 10, 25, 50, 90]
    private val batchOffsets = batchSizes.scanLeft(0L)(_ + _)  // Size will be batchSize.length + 1
    assert(file.length() == batchOffsets.last,
      "File length is not equal to the last batch offset:\n" +
      s"    file length = ${file.length}\n" +
      s"    last batch offset = ${batchOffsets.last}\n" +
      s"    all batch offsets = ${batchOffsets.mkString(",")}"
    )

    private var batchIndex = 0  // Which batch we're in
    private var fileStream: FileInputStream = null

    // An intermediate stream that reads from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    private var deserializeStream: DeserializationStream = null
    private var nextItem: (K, C) = null
    private var objectsRead = 0

    /**
     * Construct a stream that reads only from the next batch.
     */
    private def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      if (batchIndex < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        val start = batchOffsets(batchIndex)
        fileStream = new FileInputStream(file)
        // 通过FileChannel设置，fileStream读取的开始位置
        fileStream.getChannel.position(start)
        batchIndex += 1

        val end = batchOffsets(batchIndex)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        // 首先将该文件输入流封装成了一个有限输入流(只能读取end-start范围大小的数据)
        // 然后将该有限输入流封装成一个缓冲输入流
        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
        // 然后再封装成解密输入流以及解压输入流
        val wrappedStream = serializerManager.wrapStream(blockId, bufferedStream)
        // 最终再封装成反序列化流
        ser.deserializeStream(wrappedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     */
    private def readNextItem(): (K, C) = {
      try {
        val k = deserializeStream.readKey().asInstanceOf[K]
        val c = deserializeStream.readValue().asInstanceOf[C]
        val item = (k, c)
        objectsRead += 1
        // 说明这个batch批次的items已经读完了
        // 我们需要再创建一个新的输入流来读取下一个batch的items
        if (objectsRead == serializerBatchSize) {
          objectsRead = 0
          deserializeStream = nextBatchStream()
        }
        item
      } catch {
        case e: EOFException =>
          cleanup()
          null
      }
    }

    override def hasNext: Boolean = {
      if (nextItem == null) {
        if (deserializeStream == null) {
          // In case of deserializeStream has not been initialized
          deserializeStream = nextBatchStream()
          if (deserializeStream == null) {
            return false
          }
        }
        nextItem = readNextItem()
      }
      nextItem != null
    }

    override def next(): (K, C) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val item = nextItem
      nextItem = null
      item
    }

    private def cleanup() {
      batchIndex = batchOffsets.length  // Prevent reading any other batch
      if (deserializeStream != null) {
        deserializeStream.close()
        deserializeStream = null
      }
      if (fileStream != null) {
        fileStream.close()
        fileStream = null
      }
      if (file.exists()) {
        if (!file.delete()) {
          logWarning(s"Error deleting ${file}")
        }
      }
    }

    // 在该task完成的时候，执行cleanup()：清理资源
    context.addTaskCompletionListener(context => cleanup())
  }

  private[this] class SpillableIterator(var upstream: Iterator[(K, C)])
    extends Iterator[(K, C)] {

    private val SPILL_LOCK = new Object()

    private var nextUpstream: Iterator[(K, C)] = null

    private var cur: (K, C) = readNext()

    private var hasSpilled: Boolean = false

    def spill(): Boolean = SPILL_LOCK.synchronized {
      if (hasSpilled) {
        false
      } else {
        logInfo(s"Task ${context.taskAttemptId} force spilling in-memory map to disk and " +
          s"it will release ${org.apache.spark.util.Utils.bytesToString(getUsed())} memory")
        nextUpstream = spillMemoryIteratorToDisk(upstream)
        hasSpilled = true
        true
      }
    }

    def readNext(): (K, C) = SPILL_LOCK.synchronized {
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

    override def next(): (K, C) = {
      val r = cur
      // 如果在该iterator遍历的过程中，内存充足，则readNext()会一直从传参进来的原始upstream
      // 读取元素。而如果内存不够了，则期间可能发生该iterator的spill。当该iterator spill发生
      // 后，我们就要通过spillMemoryIteratorToDisk()返回的DiskMapIterator去读取数据。（显然
      // 后者因为需要从磁盘中读取元素，会更慢。但这也是内存不够时的不得不做出的让步处理）
      cur = readNext()
      r
    }
  }

  /** Convenience function to hash the given (K, C) pair by the key. */
  private def hashKey(kc: (K, C)): Int = ExternalAppendOnlyMap.hash(kc._1)

  override def toString(): String = {
    this.getClass.getName + "@" + java.lang.Integer.toHexString(this.hashCode())
  }
}

private[spark] object ExternalAppendOnlyMap {

  /**
   * Return the hash code of the given object. If the object is null, return a special hash code.
   */
  private def hash[T](obj: T): Int = {
    if (obj == null) 0 else obj.hashCode()
  }

  /**
   * A comparator which sorts arbitrary keys based on their hash codes.
   */
  private class HashComparator[K] extends Comparator[K] {
    def compare(key1: K, key2: K): Int = {
      val hash1 = hash(key1)
      val hash2 = hash(key2)
      if (hash1 < hash2) -1 else if (hash1 == hash2) 0 else 1
    }
  }
}
