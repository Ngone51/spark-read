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

import javax.annotation.Nullable;
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import scala.Option;
import scala.Product2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.*;
import org.apache.spark.annotation.Private;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TimeTrackingOutputStream;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.Utils;
import org.apache.spark.internal.config.package$;

@Private
public class UnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(UnsafeShuffleWriter.class);

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  @VisibleForTesting
  static final int DEFAULT_INITIAL_SORT_BUFFER_SIZE = 4096;
  static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  private final BlockManager blockManager;
  private final IndexShuffleBlockResolver shuffleBlockResolver;
  private final TaskMemoryManager memoryManager;
  private final SerializerInstance serializer;
  private final Partitioner partitioner;
  private final ShuffleWriteMetrics writeMetrics;
  private final int shuffleId;
  private final int mapId;
  private final TaskContext taskContext;
  private final SparkConf sparkConf;
  private final boolean transferToEnabled;
  private final int initialSortBufferSize;
  private final int inputBufferSizeInBytes;
  private final int outputBufferSizeInBytes;

  @Nullable private MapStatus mapStatus;
  @Nullable private ShuffleExternalSorter sorter;
  private long peakMemoryUsedBytes = 0;

  /** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
  private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
    MyByteArrayOutputStream(int size) { super(size); }
    public byte[] getBuf() { return buf; }
  }

  private MyByteArrayOutputStream serBuffer;
  private SerializationStream serOutputStream;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  private class CloseAndFlushShieldOutputStream extends CloseShieldOutputStream {

    CloseAndFlushShieldOutputStream(OutputStream outputStream) {
      super(outputStream);
    }

    @Override
    public void flush() {
      // do nothing
    }
  }

  public UnsafeShuffleWriter(
      BlockManager blockManager,
      IndexShuffleBlockResolver shuffleBlockResolver,
      TaskMemoryManager memoryManager,
      SerializedShuffleHandle<K, V> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf sparkConf) throws IOException {
    final int numPartitions = handle.dependency().partitioner().numPartitions();
    if (numPartitions > SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()) {
      throw new IllegalArgumentException(
        "UnsafeShuffleWriter can only be used for shuffles with at most " +
        SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE() +
        " reduce partitions");
    }
    this.blockManager = blockManager;
    this.shuffleBlockResolver = shuffleBlockResolver;
    this.memoryManager = memoryManager;
    this.mapId = mapId;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    this.serializer = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
    this.taskContext = taskContext;
    this.sparkConf = sparkConf;
    this.transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);
    this.initialSortBufferSize = sparkConf.getInt("spark.shuffle.sort.initialBufferSize",
                                                  DEFAULT_INITIAL_SORT_BUFFER_SIZE);
    this.inputBufferSizeInBytes =
      (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.outputBufferSizeInBytes =
      (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE()) * 1024;
    open();
  }

  private void updatePeakMemoryUsed() {
    // sorter can be null if this writer is closed
    if (sorter != null) {
      long mem = sorter.getPeakMemoryUsedBytes();
      if (mem > peakMemoryUsedBytes) {
        peakMemoryUsedBytes = mem;
      }
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  /**
   * This convenience method should only be called in test code.
   */
  @VisibleForTesting
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    write(JavaConverters.asScalaIteratorConverter(records).asScala());
  }

  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
    // Keep track of success so we know if we encountered an exception
    // We do this rather than a standard try/catch/re-throw to handle
    // generic throwables.
    boolean success = false;
    try {
      while (records.hasNext()) {
        insertRecordIntoSorter(records.next());
      }
      // 在这里会merge spill
      closeAndWriteOutput();
      success = true;
    } finally {
      if (sorter != null) {
        try {
          sorter.cleanupResources();
        } catch (Exception e) {
          // Only throw this error if we won't be masking another
          // error.
          if (success) {
            throw e;
          } else {
            logger.error("In addition to a failure during writing, we failed during " +
                         "cleanup.", e);
          }
        }
      }
    }
  }

  private void open() {
    assert (sorter == null);
    sorter = new ShuffleExternalSorter(
      memoryManager,
      blockManager,
      taskContext,
      initialSortBufferSize,
      partitioner.numPartitions(),
      sparkConf,
      writeMetrics);
    serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    serOutputStream = serializer.serializeStream(serBuffer);
  }

  @VisibleForTesting
  void closeAndWriteOutput() throws IOException {
    assert(sorter != null);
    updatePeakMemoryUsed();
    serBuffer = null;
    serOutputStream = null;
    // 会把当前还在内存（page）中的记录spill到磁盘，生成最后一个spill文件；
    // 因为接下来，我们要把所有的spill文件都合并起来，生成该task的map output。
    final SpillInfo[] spills = sorter.closeAndGetSpills();
    // let sorter = null
    sorter = null;
    final long[] partitionLengths;
    final File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
    final File tmp = Utils.tempFileWith(output);
    try {
      try {
        // 合并所有的spills文件(和SortShuffleWriter通过ExternalSorter来merge
        // 所以spill文件不一样的是，UnsafeShuffleWriter会自己来merge那些spill的
        // 文件，而不是通过ShuffleExternalSorter)
        partitionLengths = mergeSpills(spills, tmp);
      } finally {
        for (SpillInfo spill : spills) {
          if (spill.file.exists() && ! spill.file.delete()) {
            logger.error("Error while deleting spill file {}", spill.file.getPath());
          }
        }
      }
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
      }
    }
    // 创建该task的MapStatus
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }

  @VisibleForTesting
  void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
    assert(sorter != null);
    final K key = record._1();
    final int partitionId = partitioner.getPartition(key);
    // 注意这里的reset，这样，我们才能复用该serBuffer
    // （我们已经serBuffer流上封装了一层serOutputStream， 这样的话serOutputStream就能
    // 使用serBuffer流中的buffer来写数据。）
    serBuffer.reset();
    // 在java调用scala的方法(本来第二个参数在scala是implicit传递的, 在java变成显示传递)
    serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
    serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
    // flush可以让我们在buffer中写入的数据写到serBuffer的底层byte array中去。（因为下一个记录过来，
    // buffer需要reset，所以我们需要在数据丢失之前flush）
    serOutputStream.flush();

    // 获取刚刚在serBuffer写入的记录的byte size（其实就是serBuffer中的byte数组的len）
    final int serializedRecordSize = serBuffer.size();
    assert (serializedRecordSize > 0);

    sorter.insertRecord(
      serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
  }

  @VisibleForTesting
  void forceSorterToSpill() throws IOException {
    assert (sorter != null);
    sorter.spill();
  }

  /**
   * TODO read 貌似这个好复杂...想哭
   * Merge zero or more spill files together, choosing the fastest merging strategy based on the
   * number of spills and the IO compression codec.
   *
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpills(SpillInfo[] spills, File outputFile) throws IOException {
    // 是否启用压缩
    final boolean compressionEnabled = sparkConf.getBoolean("spark.shuffle.compress", true);
    // 获取配置的压缩器
    final CompressionCodec compressionCodec = CompressionCodec$.MODULE$.createCodec(sparkConf);
    // 是否启用快速merge
    final boolean fastMergeEnabled =
      sparkConf.getBoolean("spark.shuffle.unsafe.fastMergeEnabled", true);
    // 是否支持快速merge
    // TODO read supportsConcatenationOfSerializedStreams()
    final boolean fastMergeIsSupported = !compressionEnabled ||
      CompressionCodec$.MODULE$.supportsConcatenationOfSerializedStreams(compressionCodec);
    // 是否启用加密
    final boolean encryptionEnabled = blockManager.serializerManager().encryptionEnabled();
    try {
      if (spills.length == 0) {
        new FileOutputStream(outputFile).close(); // Create an empty file
        return new long[partitioner.numPartitions()];
      } else if (spills.length == 1) {
        // Here, we don't need to perform any metrics updates because the bytes written to this
        // output file would have already been counted as shuffle bytes written.
        Files.move(spills[0].file, outputFile);
        return spills[0].partitionLengths;
      } else {
        final long[] partitionLengths;
        // There are multiple spills to merge, so none of these spill files' lengths were counted
        // towards our shuffle write count or shuffle write time. If we use the slow merge path,
        // then the final output file's size won't necessarily be equal to the sum of the spill
        // files' sizes. To guard against this case, we look at the output file's actual size when
        // computing shuffle bytes written.
        //
        // We allow the individual merge methods to report their own IO times since different merge
        // strategies use different IO techniques.  We count IO during merge towards the shuffle
        // shuffle write time, which appears to be consistent with the "not bypassing merge-sort"
        // branch in ExternalSorter.
        if (fastMergeEnabled && fastMergeIsSupported) {
          // Compression is disabled or we are using an IO compression codec that supports
          // decompression of concatenated compressed streams, so we can perform a fast spill merge
          // that doesn't need to interpret the spilled bytes.
          if (transferToEnabled && !encryptionEnabled) {
            logger.debug("Using transferTo-based fast merge");
            partitionLengths = mergeSpillsWithTransferTo(spills, outputFile);
          } else {
            logger.debug("Using fileStream-based fast merge");
            partitionLengths = mergeSpillsWithFileStream(spills, outputFile, null);
          }
        } else {
          logger.debug("Using slow merge");
          partitionLengths = mergeSpillsWithFileStream(spills, outputFile, compressionCodec);
        }
        // When closing an UnsafeShuffleExternalSorter that has already spilled once but also has
        // in-memory records, we write out the in-memory records to a file but do not count that
        // final write as bytes spilled (instead, it's accounted as shuffle write). The merge needs
        // to be counted as shuffle write, but this will lead to double-counting of the final
        // SpillInfo's bytes.
        writeMetrics.decBytesWritten(spills[spills.length - 1].file.length());
        writeMetrics.incBytesWritten(outputFile.length());
        return partitionLengths;
      }
    } catch (IOException e) {
      if (outputFile.exists() && !outputFile.delete()) {
        logger.error("Unable to delete output file {}", outputFile.getPath());
      }
      throw e;
    }
  }

  /**
   * Merges spill files using Java FileStreams. This code path is typically slower than
   * the NIO-based merge, {@link UnsafeShuffleWriter#mergeSpillsWithTransferTo(SpillInfo[],
   * File)}, and it's mostly used in cases where the IO compression codec does not support
   * concatenation of compressed data, when encryption is enabled, or when users have
   * explicitly disabled use of {@code transferTo} in order to work around kernel bugs.
   * This code path might also be faster in cases where individual partition size in a spill
   * is small and UnsafeShuffleWriter#mergeSpillsWithTransferTo method performs many small
   * disk ios which is inefficient. In those case, Using large buffers for input and output
   * files helps reducing the number of disk ios, making the file merging faster.
   *
   * @param spills the spills to merge.
   * @param outputFile the file to write the merged data to.
   * @param compressionCodec the IO compression codec, or null if shuffle compression is disabled.
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpillsWithFileStream(
      SpillInfo[] spills,
      File outputFile,
      @Nullable CompressionCodec compressionCodec) throws IOException {
    assert (spills.length >= 2);
    final int numPartitions = partitioner.numPartitions();
    final long[] partitionLengths = new long[numPartitions];
    final InputStream[] spillInputStreams = new InputStream[spills.length];

    // outputFile就是最终要合并成的一个目标文件
    // 创建缓冲文件输出流，缓冲大小为outputBufferSizeInBytes(默认32k)
    // 缓冲区的设的越大，越能减少和磁盘的io次数。但是缺点也就是比较吃内存了。
    final OutputStream bos = new BufferedOutputStream(
            new FileOutputStream(outputFile),
            outputBufferSizeInBytes);
    // 再用记数输出流封装上面的缓冲文件输出流。
    // 用记数数流的好处是能够在不关闭底层文件的条件下，获取该文件的大小。
    // Use a counting output stream to avoid having to close the underlying file and ask
    // the file system for its size after each partition is written.
    final CountingOutputStream mergedFileOutputStream = new CountingOutputStream(bos);

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        // TODO read NioBufferedFileInputStream
        // 创建nio缓冲文件输入流(spark自定义的流，既支持nio，又支持缓冲)
        spillInputStreams[i] = new NioBufferedFileInputStream(
            spills[i].file,
            inputBufferSizeInBytes);
      }
      for (int partition = 0; partition < numPartitions; partition++) {
        // 初始目标文件长度，每次写入一个partition的记录后，就会增长
        final long initialFileLength = mergedFileOutputStream.getByteCount();
        // TODO read CloseAndFlushShieldOutputStream 到底啥意思???
        // Shield the underlying output stream from close() and flush() calls, so that we can close
        // the higher level streams to make sure all data is really flushed and internal state is
        // cleaned.
        OutputStream partitionOutput = new CloseAndFlushShieldOutputStream(
          new TimeTrackingOutputStream(writeMetrics, mergedFileOutputStream));
        // 用加密流封装CloseAndFlushShieldOutputStream流
        partitionOutput = blockManager.serializerManager().wrapForEncryption(partitionOutput);
        // 如果compressionCodec不为null，则再封装一层压缩流
        if (compressionCodec != null) {
          partitionOutput = compressionCodec.compressedOutputStream(partitionOutput);
        }
        // 从每个spill file中读取对应partition的记录（数据）
        for (int i = 0; i < spills.length; i++) {
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          if (partitionLengthInSpill > 0) {
            // 为每个spill file创建有限输入流
            // 注意：因为spill file中的记录已经经过partitionID排序，所以我们在通过inputStream读取的
            // 时候，并不需要指定该partition在该spill file中的offset。例如，假如我们在spill file中
            // 存储的记录是这样的：（1, 2),(1, 3),(3, 1), (3, 5), (3, 2), (4, 3)
            // 我们依次遍历上面这个spill file中的每个分区， 对于记录个数为0的分区，我们会过滤掉。
            InputStream partitionInputStream = new LimitedInputStream(spillInputStreams[i],
              partitionLengthInSpill, false);
            try {
              // 封装解密流
              partitionInputStream = blockManager.serializerManager().wrapForEncryption(
                partitionInputStream);
              // 封装解压流
              if (compressionCodec != null) {
                partitionInputStream = compressionCodec.compressedInputStream(partitionInputStream);
              }
              // 从partitionInputStream拷贝字节流到partitionOutput
              ByteStreams.copy(partitionInputStream, partitionOutput);
            } finally {
              partitionInputStream.close();
            }
          }
        }
        partitionOutput.flush();
        partitionOutput.close();
        // （在合并了多个spill file之后）统计该partition的大小
        partitionLengths[partition] = (mergedFileOutputStream.getByteCount() - initialFileLength);
      }
      threwException = false;
    } finally {
      // 为了避免掩盖在try{}中抛出的异常而致使我们提前(try{}还没结束)地进入finally{}，
      // 只有当threwException == false，我们才会在close资源的时候抛出异常。
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (InputStream stream : spillInputStreams) {
        Closeables.close(stream, threwException);
      }
      Closeables.close(mergedFileOutputStream, threwException);
    }
    return partitionLengths;
  }

  /**
   * Merges spill files by using NIO's transferTo to concatenate spill partitions' bytes.
   * This is only safe when the IO compression codec and serializer support concatenation of
   * serialized streams.
   *
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpillsWithTransferTo(SpillInfo[] spills, File outputFile) throws IOException {
    assert (spills.length >= 2);
    final int numPartitions = partitioner.numPartitions();
    final long[] partitionLengths = new long[numPartitions];
    final FileChannel[] spillInputChannels = new FileChannel[spills.length];
    // 相当于初始化每个spillInputChannelPositions[i]为0
    final long[] spillInputChannelPositions = new long[spills.length];
    FileChannel mergedFileOutputChannel = null;

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        spillInputChannels[i] = new FileInputStream(spills[i].file).getChannel();
      }
      // This file needs to opened in append mode in order to work around a Linux kernel bug that
      // affects transferTo; see SPARK-3948 for more details.
      mergedFileOutputChannel = new FileOutputStream(outputFile, true).getChannel();

      long bytesWrittenToMergedFile = 0;
      // 基于NIO，直接拷贝序列化的数据，避免反序列
      // 将所有的spill文件，合并到一个输出文件(outputFile)
      for (int partition = 0; partition < numPartitions; partition++) {
        for (int i = 0; i < spills.length; i++) {
          // 获取在spills[i](SpillInfo)中，partition分区的size
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          final FileChannel spillInputChannel = spillInputChannels[i];
          final long writeStartTime = System.nanoTime();
          // spillInputChannelPositions[i]初始化为0，之所以position能够默认从0开始，
          // 是因为每个SpillInfo对应的file里存储的记录已经按照partitionID排序好了。所以，
          // 当for循环从第0个partition开始的时候，刚好对上file中从position为0开始的分区
          // 数据。之后，只要调整spillInputChannelPositions[i]，让其读取下一个partition
          // 的数据即可。
          // copyFileStreamNIO：基于NIO的transferTo()
          Utils.copyFileStreamNIO(
            spillInputChannel,
            mergedFileOutputChannel,
            spillInputChannelPositions[i],
            partitionLengthInSpill);
          // 更新input file(spill file)的position，作为读取下一个分区的记录的起始地址
          spillInputChannelPositions[i] += partitionLengthInSpill;
          writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
          bytesWrittenToMergedFile += partitionLengthInSpill;
          partitionLengths[partition] += partitionLengthInSpill;
        }
      }
      // Check the position after transferTo loop to see if it is in the right position and raise an
      // exception if it is incorrect. The position will not be increased to the expected length
      // after calling transferTo in kernel version 2.6.32. This issue is described at
      // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.
      if (mergedFileOutputChannel.position() != bytesWrittenToMergedFile) {
        throw new IOException(
          "Current position " + mergedFileOutputChannel.position() + " does not equal expected " +
            "position " + bytesWrittenToMergedFile + " after transferTo. Please check your kernel" +
            " version to see if it is 2.6.32, as there is a kernel bug which will lead to " +
            "unexpected behavior when using transferTo. You can set spark.file.transferTo=false " +
            "to disable this NIO feature."
        );
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (int i = 0; i < spills.length; i++) {
        assert(spillInputChannelPositions[i] == spills[i].file.length());
        Closeables.close(spillInputChannels[i], threwException);
      }
      Closeables.close(mergedFileOutputChannel, threwException);
    }
    return partitionLengths;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          return Option.apply(null);
        }
      }
    } finally {
      if (sorter != null) {
        // If sorter is non-null, then this implies that we called stop() in response to an error,
        // so we need to clean up memory and spill files created by the sorter
        sorter.cleanupResources();
      }
    }
  }
}
