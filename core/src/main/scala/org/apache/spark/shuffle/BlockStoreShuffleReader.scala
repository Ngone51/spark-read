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

package org.apache.spark.shuffle

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockManager, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
 * requesting them from other nodes' block stores.
 */
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      // 注意该tracker是MapOutputTrackerWorker
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue),
      // 一个reduce task向一个主机正在拉取的远程blocks的最大个数（默认Int.MaxValue）
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      // 当该reduce task拉取的远程blocks所占的内存超过了该阈值，则这些blocks spill
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt", true))

    val serializerInstance = dep.serializer.newInstance()

    // 为什么这里使用flatMap,里面的case是这样的???
    // 答：因为flatMap会调用该对象的hasNext、next方法。而ShuffleBlockFetcherIterator继承
    // 了iterator，并且实现了自己的next、hasNext方法。在ShuffleBlockFetcherIterator自己
    // 实现的next方法中，就是返回(blockId, inputStream)这种形式的。
    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      // 封装反序列化流
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // TODO read context.taskMetrics.createTempShuffleReadMetrics
    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      // 当metricIter的元素遍历完成时，会合并shuffle read过程中的统计信息
      context.taskMetrics().mergeShuffleReadMetrics())

    // 为了支持task撤销(杀死)，我们需要在这里使用interruptible iterator
    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    // 首先，我们需要在这里明确一个事实：map端输出的map outputs中的某个partition中的数据(k, v)
    // 虽然被分到了同一个partition中，但是,它们的key并不一定一样。它们之所以被分到同一个partition，
    // 是key经过Partitioner(key)计算之后得到的对应的partition。比如，我们有一个Partition(key)
    // = {key % 2}。那么，key为奇数的(k, v)健值对就会被分配到同一个partition中，而key为偶数的则会
    // 被分配到另一个partition中。

    // 从下面的代码来看，如果ShuffleDependency定义了aggregator，则map端如果定义了mapSideCombine，
    // 才会执行combine values；而只要定义了aggregator，reduce端都会进行combine values。（具体怎么
    // 合并，应该跟设置的Combiner有关咯）
    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      // 如果在map端（？）定义了aggregator且mapSideCombine = true,
      // 则我们需要根据key合并values。（我们不是在map端写outputs的时候也合并过吗？然后现在把所有map
      // 端的outputs的某个partition的数据都读出来后，又要合并了？如果我们只读取一个partition呢？
      // 那岂不是都合并到一个combiner里去了???）
      // 答：在map端写output文件的时候，我们是根据key合并values，而在reduce端读的时候，
      // 我们是根据key合并combiners
      if (dep.mapSideCombine) {
        // 我们读取的values是已经在map端经过合并的
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // 如果未曾在map端指定过mapSideCombine，则在reduce端读取的时候，执行根据key进行合并
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      // 哼，啥都不合并！
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        // 可能会发生spill
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        // TODO read sort.iterator
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }
  }
}
