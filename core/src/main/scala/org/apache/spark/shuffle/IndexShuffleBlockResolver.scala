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

import java.io._
import java.nio.channels.Channels
import java.nio.file.Files

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.NioBufferedFileInputStream
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage._
import org.apache.spark.util.Utils

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
private[spark] class IndexShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager = null)
  extends ShuffleBlockResolver
  with Logging {

  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)

  // TODO read SparkTransportConf
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  // 创建或获取shuffle的数据文件的抽象（File对象，还没有真正创建该文件）
  def getDataFile(shuffleId: Int, mapId: Int): File = {
    // 注意：最后所有的partition files会合并成一个file，通过DiskBlockManager来存储。而该file对应的BlockId
    // 正是该ShuffleDataBlockId。注意ShuffleBlockId的第三个参数reduceId是0，意思是不指定哪个reduce partition
    // (而不是第0个partition啊)。这是因为，我们把所有分区的数据都当作一个Block来存储了，那就不用区分这是哪个分区的
    // Block了。但是可能之前spark的版本是分开来存储各个分区的Blocks，所以需要第三个参数reduceId来区分这是哪个partition
    // 的Block。然后，现在的spark版本保留该参数只是为了和之前的方法兼容。
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  // 创建shuffle的索引文件的抽象
  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    // 通过DiskBlockManager来获取或创建索引文件(File对象)
    // 根据shuffleId、mapId、NOOP_REDUCE_ID创建ShuffleIndexBlockId，来获取对应的索引文件
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
   * Remove data file and index file that contain the output data from one map.
   */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting data ${file.getPath()}")
      }
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting index ${file.getPath()}")
      }
    }
  }

  /**
   * 检查给定的索引文件和数据文件是否相互匹配。
   * 如果匹配，则返回数据文件中，该分区(map端某个分区)的大小。反之，返回null。
   * Check whether the given index and data files match each other.
   * If so, return the partition lengths in the data file. Otherwise return null.
   */
  private def checkIndexAndDataFile(index: File, data: File, blocks: Int): Array[Long] = {
    // 看writeIndexFileAndCommit()的L157的注释就知道为什么是这样的了。
    // the index file should have `block + 1` longs as offset.
    if (index.length() != (blocks + 1) * 8) {
      return null
    }
    val lengths = new Array[Long](blocks)
    // Read the lengths of blocks
    val in = try {
      new DataInputStream(new NioBufferedFileInputStream(index))
    } catch {
      case e: IOException =>
        return null
    }
    try {
      // 将偏移量转换成每个block的大小
      // Convert the offsets into lengths of each block
      var offset = in.readLong()
      // 因为第一个写入的offset就是0啊
      // 不等于0，很有可能说明，这是该task第一次执行，indexFile还为未初始化。
      // 错，如果是task的第一次attempt，则在该方法的第一行if()就应该返回null了
      if (offset != 0L) {
        return null
      }
      var i = 0
      while (i < blocks) {
        val off = in.readLong()
        lengths(i) = off - offset
        offset = off
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    // 数据文件的大小和索引文件记录的大小需要相互匹配！
    // the size of data file should match with index file
    if (data.length() == lengths.sum) {
      lengths
    } else {
      null
    }
  }

  /**
   * 将每个block的偏移量写入一个索引文件，并在文件末尾增加一个最终的偏移量。getBlockData将会使用该文件来定位
   * 每个block的起始位置。
   *
   * 该方法会以一个原子操作提交数据文件和索引文件，要么使用已经存在的那个，或者用新的替换。
   *
   * 注意：如果要使用已经存在的索引文件，则'lengths'会被更新为和已经存在的索引文件匹配。
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   */
  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: File): Unit = {
    // 创建获取索引文件的抽象（File对象）
    val indexFile = getIndexFile(shuffleId, mapId)
    // 再创建一个临时索引文件???(看完下面的代码，就知道为什么还要创建一个临时索引文件了)
    val indexTmp = Utils.tempFileWith(indexFile)
    try {
      // 这个I/O流为什么要这么封装???我是真的不懂，需要多多学习。
      // 答：BufferedOutputStream可以让从文件流中去读的数据先缓存到buffer中
      val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
      Utils.tryWithSafeFinally {
        // 我们依次遍历每个block的大小(字节), 并将其转换为偏移量(offsets).
        // We take in lengths of each block, need to convert it to offsets.
        var offset = 0L
        // writeLong()会用8个byte来存储一个offset(long型，在jvm中，long就是用8个字节存储的),
        // 所以，整个index文件最终的length是:
        // (lengths.length + 1) * 8
        // 之所以要+1，是因为一开始写入了offset = 0L(因为第一个block的偏移量肯定是0咯),也占用了8个字节。
        out.writeLong(offset)
        for (length <- lengths) {
          offset += length
          out.writeLong(offset)
        }
      } {
        out.close()
      }

      // 获取shuffle的数据文件（如果这是该task的attempt第一次调用writeIndexFileAndCommit()，则该
      // 数据文件没有任何数据；而如果有其它的attempts在之前调用了，则很有可能会有数据）
      val dataFile = getDataFile(shuffleId, mapId)
      // 每个executor上只有一个IndexShuffleBlockResolver（但是却会有多个tasks同时运行），
      // 所以，synchronized能够保证接下来的检查和重命名是原子操作。
      // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
      // the following check and rename are atomic.
      synchronized {
        // 检查数据文件和索引文件是否相互匹配
        // (注意：这里检查的是indexFile，不是indexTmp；是dataFile，不是dataTmp)
        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
        // 不为null，说明数据文件和索引文件相互匹配(一切ok)
        if (existingLengths != null) {
          // 说明之前有相同task的另一次尝试执行，已经成功地把map outputs写好了。所以，只需要使用
          // 已经存在的分区大小且删除我们临时的map outputs(指传进来的dataTmp)。
          // 也就该方法注释的note里提到的：the `lengths` will be updated to match the
          // existing index file if use the existing ones.
          // Another attempt for the same task has already written our map outputs successfully,
          // so just use the existing partition lengths and delete our temporary map outputs.
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
          // 删除临时的map outputs
          if (dataTmp != null && dataTmp.exists()) {
            dataTmp.delete()
          }
          // 删除临时的索引文件
          indexTmp.delete()
        } else {
          // 如果不匹配，说明这是该task第一次成功地尝试执行来写map outputs。则，用我们
          // 新的写入(indexTmp和dataTmp)覆盖任何已经存在的数据文件和索引文件。
          // This is the first successful attempt in writing the map outputs for this task,
          // so override any existing index and data files with the ones we wrote.
          if (indexFile.exists()) {
            indexFile.delete()
          }
          if (dataFile.exists()) {
            dataFile.delete()
          }
          // 将indexTmp重命名为indexFile(以实现覆盖)
          if (!indexTmp.renameTo(indexFile)) {
            throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
          }
          // 将dataTmp重命名为dataFile(以实现覆盖)
          if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
            throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
          }
        }
      }
    } finally {
      // 以防万一，try{}抛出异常，而indexTmp没有删除
      // 删除临时索引文件
      if (indexTmp.exists() && !indexTmp.delete()) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
      }
    }
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    // 首先获取该Map Output的索引文件
    val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)

    // SPARK-22982: if this FileInputStream's position is seeked forward by another piece of code
    // which is incorrectly using our file descriptor then this code will fetch the wrong offsets
    // (which may cause a reducer to be sent a different reducer's data). The explicit position
    // checks added here were a useful debugging aid during SPARK-22982 and may help prevent this
    // class of issue from re-occurring in the future which is why they are left here even though
    // SPARK-22982 is fixed.
    // 先从file获取该file对应的channel
    val channel = Files.newByteChannel(indexFile.toPath)
    // 为什么是blockId.reduceId * 8???
    // 答：首先，indexFile里面存储的是map端每个partition的偏移量(用于读取dataFile)。
    // 而该偏移量是用long来存储的，一个long需要8个字节。例如：reduceId = 0，则
    // position = 0，即该reducer可以从文件的第0个字节处读取第一个partition的偏移量。
    // 而如果reduceId = 1，则position = 8，即该reducer就要从该文件的第8个字节开始
    // 读取第二个偏移量。
    channel.position(blockId.reduceId * 8)
    // 再根据该channel获取inputStream，然后再封装成DataInputStream
    val in = new DataInputStream(Channels.newInputStream(channel))
    try {
      // 获取的offset对应DataFile开始读取对应partition的开始位置
      val offset = in.readLong()
      val nextOffset = in.readLong()
      val actualPosition = channel.position()
      // 因为offset和nextOffset读取了两个long，所以position会向前移动16个字节。
      val expectedPosition = blockId.reduceId * 8 + 16
      if (actualPosition != expectedPosition) {
        throw new Exception(s"SPARK-22982: Incorrect channel position after index file reads: " +
          s"expected $expectedPosition but actual position was $actualPosition.")
      }
      // FileSegmentManagedBuffer的长度是nextOffset - offset的大小,
      // 而这个区间对应一段连续的partitions（也有可能只是一个partition）。
      new FileSegmentManagedBuffer(
        transportConf,
        // 获取我们的Map Output数据文件
        getDataFile(blockId.shuffleId, blockId.mapId),
        // 从Map Output中读取数据的起始位置
        offset,
        // 从Map Output中读取数据的size
        nextOffset - offset)
    } finally {
      in.close()
    }
  }

  override def stop(): Unit = {}
}

private[spark] object IndexShuffleBlockResolver {
  // 一个No-op reduce ID，仅仅用于和disk store交互
  // 因为disk store目前存储的方式还是需要一对（map，reduce），而我们现在的sort shuffle outputs已经是组合了
  // 所有partitions的数据构成了一个单独的文件（这个文件不再是某个reduce的）。所以，使用No-op reduce ID只是为了
  // 兼容disk store的存储方式。
  // No-op reduce ID used in interactions with disk store.
  // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
  // shuffle outputs for several reduces are glommed into a single file.
  val NOOP_REDUCE_ID = 0
}
