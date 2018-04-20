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

package org.apache.spark.shuffle.sort;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import javax.annotation.Nullable;

import scala.None$;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.*;
import org.apache.spark.util.Utils;

/**
 * This class implements sort-based shuffle's hash-style shuffle fallback path. This write path
 * writes incoming records to separate files, one file per reduce partition, then concatenates these
 * per-partition files to form a single output file, regions of which are served to reducers.
 * Records are not buffered in memory. It writes output in a format
 * that can be served / consumed via {@link org.apache.spark.shuffle.IndexShuffleBlockResolver}.
 * <p>
 * This write path is inefficient for shuffles with large numbers of reduce partitions because it
 * simultaneously opens separate serializers and file streams for all partitions. As a result,
 * {@link SortShuffleManager} only selects this write path when
 * <ul>
 *    <li>no Ordering is specified,</li>
 *    <li>no Aggregator is specified, and</li>
 *    <li>the number of partitions is less than
 *      <code>spark.shuffle.sort.bypassMergeThreshold</code>.</li>
 * </ul>
 *
 * This code used to be part of {@link org.apache.spark.util.collection.ExternalSorter} but was
 * refactored into its own class in order to reduce code complexity; see SPARK-7855 for details.
 * <p>
 * There have been proposals to completely remove this code path; see SPARK-6026 for details.
 */
final class BypassMergeSortShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(BypassMergeSortShuffleWriter.class);

  private final int fileBufferSize;
  private final boolean transferToEnabled;
  private final int numPartitions;
  private final BlockManager blockManager;
  private final Partitioner partitioner;
  private final ShuffleWriteMetrics writeMetrics;
  private final int shuffleId;
  private final int mapId;
  private final Serializer serializer;
  private final IndexShuffleBlockResolver shuffleBlockResolver;

  /** Array of file writers, one for each partition */
  private DiskBlockObjectWriter[] partitionWriters;
  private FileSegment[] partitionWriterSegments;
  @Nullable private MapStatus mapStatus;
  private long[] partitionLengths;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  BypassMergeSortShuffleWriter(
      BlockManager blockManager,
      IndexShuffleBlockResolver shuffleBlockResolver,
      BypassMergeSortShuffleHandle<K, V> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf conf) {
    // 文件缓存大小，默认32k
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSize = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;
    // transferTo???啥玩意儿???
    this.transferToEnabled = conf.getBoolean("spark.file.transferTo", true);
    this.blockManager = blockManager;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.mapId = mapId;
    this.shuffleId = dep.shuffleId();
    // 注意: 这里是partitioner，不是partition，默认是HashPartitioner
    // 同时注意：partitioner是从ShuffleDependency中获取的
    this.partitioner = dep.partitioner();
    // 所以，numPartitions是reducer端partition的个数
    this.numPartitions = partitioner.numPartitions();
    this.writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
    this.serializer = dep.serializer();
    this.shuffleBlockResolver = shuffleBlockResolver;
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    assert (partitionWriters == null);
    // 如果records为空（话说， 有没有可能一个rdd的compute结果为空？）
    if (!records.hasNext()) {
      partitionLengths = new long[numPartitions];
      // 则dataTmp也为空
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, null);
      // TODO read what's this???
      mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
      return;
    }
    // 以下，records不为空
    // 创建序列化器实例对象
    final SerializerInstance serInstance = serializer.newInstance();
    final long openStartTime = System.nanoTime();
    // 初始化partitionWriters数组
    partitionWriters = new DiskBlockObjectWriter[numPartitions];
    // 初始化partitionWriterSegments数组
    partitionWriterSegments = new FileSegment[numPartitions];
    // 该map task会为reduce端的每个partition创建一个临时文件，用于写数据
    for (int i = 0; i < numPartitions; i++) {
      final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
        blockManager.diskBlockManager().createTempShuffleBlock();
      final File file = tempShuffleBlockIdPlusFile._2();
      final BlockId blockId = tempShuffleBlockIdPlusFile._1();
      // 所有的DiskBlockObjectWriter都共用同一个writeMetrics
      // 为每个partition创建一个file writer(DiskBlockObjectWriter)
      partitionWriters[i] =
        blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
    }
    // Creating the file to write to and creating a disk writer both involve interacting with
    // the disk, and can take a long time in aggregate when we open many files, so should be
    // included in the shuffle write time.
    writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

    while (records.hasNext()) {
      final Product2<K, V> record = records.next();
      final K key = record._1();
      // 调用partitioner的getPartition(key)方法，来确定将该记录存储到reduce端的哪个partition中
      // 比如，最经典的HashPartition，就是将key做hash计算的结果，作为reduce端的partition的index
      partitionWriters[partitioner.getPartition(key)].write(key, record._2());
    }

    for (int i = 0; i < numPartitions; i++) {
      // 获取每个partition的writer
      final DiskBlockObjectWriter writer = partitionWriters[i];
      // commit每个DiskBlockObjectWriter写入的records到文件中
      // 注意：在BypassMergeSortShuffleWriter中，每个writer写入到文件的records属于同一个(reduce端的)partition
      // NOTE：每个writer对应的file，都对应于DiskBlockManager里的一个TempShuffleBlock
      partitionWriterSegments[i] = writer.commitAndGet();
      writer.close();
    }

    // 在map端，mapId对应的task将所有的partition数据写入该DataFile
    // （之前，每个partition的数据存储在上述每个writer对应的文件中）
    // 注意：在调用writeIndexFileAndCommit()之前该output file一直是没数据的
    File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
    // 在与output的同一目录下，创建一个临时文件抽象tmp
    File tmp = Utils.tempFileWith(output);
    try {
      // writePartitionedFile()用于把各个分区的文件，串联成一个组合文件
      // 各个分区文件的内容会写入到tmp文件中，tmp文件即是组合成的文件
      partitionLengths = writePartitionedFile(tmp);
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
      }
    }
    // shuffleServerId在很多时候（没有启用外部shuffle服务的时候？）其实就是BlockManagerId
    // 创建该mapId／task对应的MapStatus
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }

  @VisibleForTesting
  long[] getPartitionLengths() {
    return partitionLengths;
  }

  /**
   * 把各个分区的文件，串联成一个组合文件
   * Concatenate all of the per-partition files into a single combined file.
   *
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
   */
  private long[] writePartitionedFile(File outputFile) throws IOException {
    // Track location of the partition starts in the output file
    final long[] lengths = new long[numPartitions];
    if (partitionWriters == null) {
      // We were passed an empty iterator
      return lengths;
    }

    final FileOutputStream out = new FileOutputStream(outputFile, true);
    final long writeStartTime = System.nanoTime();
    boolean threwException = true;
    try {
      // 逐一把每个partition的输出文件合并到一个文件outputFile
      for (int i = 0; i < numPartitions; i++) {
        final File file = partitionWriterSegments[i].file();
        if (file.exists()) {
          final FileInputStream in = new FileInputStream(file);
          boolean copyThrewException = true;
          try {
            // 从文件输入流in中读取内容拷贝到文件输出流out中，并返回从文件输入流in中读取的字节大小
            // TODO read copyStream
            lengths[i] = Utils.copyStream(in, out, false, transferToEnabled);
            copyThrewException = false;
          } finally {
            // 如果进入了finally，copyThrewException = true，说明在try{}中有异常抛出了。
            // 则我们在调用Closeables.close()时， 如果还有异常抛出，我们就忽略它。因为，try{}中已经抛
            // 出了更重要的异常。反之，如果此时copyThrewException = false，说明try{}中的代码
            // 顺利完成了。则如果在后续调用Closeables.close(), 即in.close()时，有IOException异常抛
            // 则我们需要关注一下该异常，将其抛出。
            Closeables.close(in, copyThrewException);
          }
          // QUESTION：删除该文件后，那DiskBlockManager中对应的TmpShuffleBlock怎么办？
          // 是不是因为Tmp的Block就不用管它了呢？
          // 答：其实在DiskBlockManager中，并没有存储任何Block相关的东西，只有一些文件目录。
          // 所以，在这里删除文件后，也不用管对应的Block会怎么样。
          if (!file.delete()) {
            logger.error("Unable to delete file for partition {}", i);
          }
        }
      }
      threwException = false;
    } finally {
      // 和上面一样的用法
      Closeables.close(out, threwException);
      // writeMetrics会统计整个shuffle写过程中的所有耗时
      writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
    }
    // 让gc回收该对象
    partitionWriters = null;
    return lengths;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (stopping) {
      return None$.empty();
    } else {
      stopping = true;
      if (success) {
        if (mapStatus == null) {
          throw new IllegalStateException("Cannot call stop(true) without having called write()");
        }
        return Option.apply(mapStatus);
      } else {
        // map task fail了，so 删除我们的output data
        // The map task failed, so delete our output data.
        if (partitionWriters != null) {
          try {
            for (DiskBlockObjectWriter writer : partitionWriters) {
              // This method explicitly does _not_ throw exceptions:
              File file = writer.revertPartialWritesAndClose();
              if (!file.delete()) {
                logger.error("Error while deleting file {}", file.getAbsolutePath());
              }
            }
          } finally {
            partitionWriters = null;
          }
        }
        return None$.empty();
      }
    }
  }
}
