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
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TooLargePageException;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.FileSegment;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 * An external sorter that is specialized for sort-based shuffle.
 * <p>
 * Incoming records are appended to data pages. When all records have been inserted (or when the
 * current thread's shuffle memory limit is reached), the in-memory records are sorted according to
 * their partition ids (using a {@link ShuffleInMemorySorter}). The sorted records are then
 * written to a single output file (or multiple files, if we've spilled). The format of the output
 * files is the same as the format of the final output file written by
 * {@link org.apache.spark.shuffle.sort.SortShuffleWriter}: each output partition's records are
 * written as a single serialized, compressed stream that can be read with a new decompression and
 * deserialization stream.
 * <p>
 * Unlike {@link org.apache.spark.util.collection.ExternalSorter}, this sorter does not merge its
 * spill files. Instead, this merging is performed in {@link UnsafeShuffleWriter}, which uses a
 * specialized merge procedure that avoids extra serialization/deserialization.
 */
final class ShuffleExternalSorter extends MemoryConsumer {

  private static final Logger logger = LoggerFactory.getLogger(ShuffleExternalSorter.class);

  @VisibleForTesting
  static final int DISK_WRITE_BUFFER_SIZE = 1024 * 1024;

  private final int numPartitions;
  private final TaskMemoryManager taskMemoryManager;
  private final BlockManager blockManager;
  private final TaskContext taskContext;
  private final ShuffleWriteMetrics writeMetrics;

  /**
   * 当内存中存储的记录个数大于该阈值时，则强制执行spill
   * Force this sorter to spill when there are this many elements in memory.
   */
  private final int numElementsForSpillThreshold;

  /** The buffer size to use when writing spills using DiskBlockObjectWriter */
  private final int fileBufferSizeBytes;

  /** The buffer size to use when writing the sorted records to an on-disk file */
  private final int diskWriteBufferSize;

  /**
   * 用于存储记录进行排序(???)的内存页。当spilling执行时，在该列表中的内存页就可以被释放。虽然，从原则上讲，
   * 我们可以在spill执行过程中循环利用这些内存页(从另一方面说，如果我们让TaskMemoryManager自己维护一个可
   * 复用内存页池，并不是很有必要。)
   * Memory pages that hold the records being sorted. The pages in this list are freed when
   * spilling, although in principle we could recycle these pages across spills (on the other hand,
   * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
   * itself).
   */
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

  private final LinkedList<SpillInfo> spills = new LinkedList<>();

  /** Peak memory used by this sorter so far, in bytes. **/
  private long peakMemoryUsedBytes;

  // These variables are reset after spilling:
  @Nullable private ShuffleInMemorySorter inMemSorter;
  @Nullable private MemoryBlock currentPage = null;
  private long pageCursor = -1;

  ShuffleExternalSorter(
      TaskMemoryManager memoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      int initialSize,
      int numPartitions,
      SparkConf conf,
      ShuffleWriteMetrics writeMetrics) {
    super(memoryManager,
      // 哈???
      (int) Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
      memoryManager.getTungstenMemoryMode());
    this.taskMemoryManager = memoryManager;
    this.blockManager = blockManager;
    this.taskContext = taskContext;
    this.numPartitions = numPartitions;
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSizeBytes =
        (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.numElementsForSpillThreshold =
        (int) conf.get(package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD());
    this.writeMetrics = writeMetrics;
    this.inMemSorter = new ShuffleInMemorySorter(
      this, initialSize, conf.getBoolean("spark.shuffle.sort.useRadixSort", true));
    this.peakMemoryUsedBytes = getMemoryUsage();
    this.diskWriteBufferSize =
        (int) (long) conf.get(package$.MODULE$.SHUFFLE_DISK_WRITE_BUFFER_SIZE());
  }

  /**
   * Sorts the in-memory records and writes the sorted records to an on-disk file.
   * This method does not free the sort data structures.
   *
   * @param isLastFile if true, this indicates that we're writing the final output file and that the
   *                   bytes written should be counted towards shuffle spill metrics rather than
   *                   shuffle write metrics.
   */
  private void writeSortedFile(boolean isLastFile) {

    final ShuffleWriteMetrics writeMetricsToUse;

    // TODO read
    if (isLastFile) {
      // We're writing the final non-spill file, so we _do_ want to count this as shuffle bytes.
      writeMetricsToUse = writeMetrics;
    } else {
      // We're spilling, so bytes written should be counted towards spill rather than write.
      // Create a dummy WriteMetrics object to absorb these metrics, since we don't want to count
      // them towards shuffle bytes written.
      writeMetricsToUse = new ShuffleWriteMetrics();
    }

    // 采用基数排序或Tim排序，将records按partition ID排好序。
    // This call performs the actual sort.
    final ShuffleInMemorySorter.ShuffleSorterIterator sortedRecords =
      inMemSorter.getSortedIterator();

    // 缓存数据??? 可是下面分段写入记录的时候，不是也通过DiskBlockObjectWriter直接写了吗???
    // DiskBlockObjectWriter不是自带BufferOutputStream的吗??? 看不懂了....
    // Small writes to DiskBlockObjectWriter will be fairly inefficient. Since there doesn't seem to
    // be an API to directly transfer bytes from managed memory to the disk writer, we buffer
    // data through a byte array. This array does not need to be large enough to hold a single
    // record;
    final byte[] writeBuffer = new byte[diskWriteBufferSize];

    // TODO read comment
    // Because this output will be read during shuffle, its compression codec must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more details.
    final Tuple2<TempShuffleBlockId, File> spilledFileInfo =
      blockManager.diskBlockManager().createTempShuffleBlock();
    // 所以，最终写入该file的记录都是按partitionID有序的
    final File file = spilledFileInfo._2();
    final TempShuffleBlockId blockId = spilledFileInfo._1();
    final SpillInfo spillInfo = new SpillInfo(numPartitions, file, blockId);

    // Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
    // Our write path doesn't actually use this serializer (since we end up calling the `write()`
    // OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
    // around this, we pass a dummy no-op serializer.
    final SerializerInstance ser = DummySerializerInstance.INSTANCE;

    final DiskBlockObjectWriter writer =
      blockManager.getDiskWriter(blockId, file, ser, fileBufferSizeBytes, writeMetricsToUse);

    int currentPartition = -1;
    while (sortedRecords.hasNext()) {
      sortedRecords.loadNext();
      final int partition = sortedRecords.packedRecordPointer.getPartitionId();
      assert (partition >= currentPartition);
      if (partition != currentPartition) {
        // Switch to the new partition
        if (currentPartition != -1) {
          // 提交同一个partition中的records，并生成一个FileSegment
          final FileSegment fileSegment = writer.commitAndGet();
          spillInfo.partitionLengths[currentPartition] = fileSegment.length();
        }
        currentPartition = partition;
      }

      final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
      final Object recordPage = taskMemoryManager.getPage(recordPointer);
      final long recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer);
      // dataRemaining意为记录的长度。
      int dataRemaining = Platform.getInt(recordPage, recordOffsetInPage);
      long recordReadPosition = recordOffsetInPage + 4; // skip over record length
      // TODO read
      // 这个分段写一个记录什么意思，没整明白???
      // writeBuffer用作缓存???
      while (dataRemaining > 0) {
        // 为了分段写记录到disk中去
        final int toTransfer = Math.min(diskWriteBufferSize, dataRemaining);
        // copy 未知类型(maybe long[], byte[]) to byte[]
        // 把一个记录分段拷贝到一个较小的buffer中去
        Platform.copyMemory(
          recordPage, recordReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
        // 将buffer中的内容通过磁盘写入writer(可能并为写入磁盘，而是先通过writeBuffer缓存???)。
        writer.write(writeBuffer, 0, toTransfer);
        recordReadPosition += toTransfer;
        dataRemaining -= toTransfer;
      }
      // 写完一个完整的记录后，更新写统计信息
      writer.recordWritten();
    }

    // 提交最后一个partition，并生成FileSegment
    final FileSegment committedSegment = writer.commitAndGet();
    writer.close();
    // If `writeSortedFile()` was called from `closeAndGetSpills()` and no records were inserted,
    // then the file might be empty. Note that it might be better to avoid calling
    // writeSortedFile() in that case.
    if (currentPartition != -1) {
      spillInfo.partitionLengths[currentPartition] = committedSegment.length();
      // 添加该spillInfo至spills
      spills.add(spillInfo);
    }

    // TODO read
    if (!isLastFile) {  // i.e. this is a spill file
      // The current semantics of `shuffleRecordsWritten` seem to be that it's updated when records
      // are written to disk, not when they enter the shuffle sorting code. DiskBlockObjectWriter
      // relies on its `recordWritten()` method being called in order to trigger periodic updates to
      // `shuffleBytesWritten`. If we were to remove the `recordWritten()` call and increment that
      // counter at a higher-level, then the in-progress metrics for records written and bytes
      // written would get out of sync.
      //
      // When writing the last file, we pass `writeMetrics` directly to the DiskBlockObjectWriter;
      // in all other cases, we pass in a dummy write metrics to capture metrics, then copy those
      // metrics to the true write metrics here. The reason for performing this copying is so that
      // we can avoid reporting spilled bytes as shuffle write bytes.
      //
      // Note that we intentionally ignore the value of `writeMetricsToUse.shuffleWriteTime()`.
      // Consistent with ExternalSorter, we do not count this IO towards shuffle write time.
      // This means that this IO time is not accounted for anywhere; SPARK-3577 will fix this.
      writeMetrics.incRecordsWritten(writeMetricsToUse.recordsWritten());
      taskContext.taskMetrics().incDiskBytesSpilled(writeMetricsToUse.bytesWritten());
    }
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   */
  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (trigger != this || inMemSorter == null || inMemSorter.numRecords() == 0) {
      return 0L;
    }

    logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
      Thread.currentThread().getId(),
      Utils.bytesToString(getMemoryUsage()),
      spills.size(),
      spills.size() > 1 ? " times" : " time");

    // 对inMemSorter中数据进行排序，并通过DiskBlockObjectWriter写入磁盘
    writeSortedFile(false);
    // 感觉这里的freeMemory并不是spillSize(如果spiilSize指的是spill掉的记录的size的话)，
    // 因为这里free掉的memory是pages的占用的内存，而pages的内存并不是都存储了记录，也会有末尾
    // 零碎的内存没有被利用起来。
    final long spillSize = freeMemory();
    // spill之后，会reset inMemSorter。
    // 在reset中，会inMemSoter会释放当前的array(可能扩增过size)，
    // 然后重新申请大小initialSize的array
    inMemSorter.reset();
    // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
    // records. Otherwise, if the task is over allocated memory, then without freeing the memory
    // pages, we might not be able to get memory for the pointer array.
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
    return spillSize;
  }

  // ShuffleExternalSorter的内存使用由
  // 所有分配的pages和一个inMemSorter中的LongArray组成
  private long getMemoryUsage() {
    long totalPageSize = 0;
    for (MemoryBlock page : allocatedPages) {
      totalPageSize += page.size();
    }
    return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
  }

  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  // TODO read freeMemory
  private long freeMemory() {
    // 更新内存使用的峰值
    updatePeakMemoryUsed();
    long memoryFreed = 0;
    // 释放所有的pages??? So，这些pages是用来干嘛的呀???
    // 答：pages用来records的实际数据，而inMemSorter
    for (MemoryBlock block : allocatedPages) {
      memoryFreed += block.size();
      freePage(block);
    }
    allocatedPages.clear();
    currentPage = null;
    pageCursor = 0;
    // memoryFreed不包括inMemSorter的使用内存???
    // 答：可能的解释是，释放的内存指的只是记录实际数据占的内存。
    // 而且，inMemSorter占用的内存，后在该方法调用之后，调用reset()来释放。
    return memoryFreed;
  }

  /**
   * Force all memory and spill files to be deleted; called by shuffle error-handling code.
   */
  public void cleanupResources() {
    freeMemory();
    if (inMemSorter != null) {
      inMemSorter.free();
      inMemSorter = null;
    }
    for (SpillInfo spill : spills) {
      if (spill.file.exists() && !spill.file.delete()) {
        logger.error("Unable to delete spill file {}", spill.file.getPath());
      }
    }
  }

  /**
   * Checks whether there is enough space to insert an additional record in to the sort pointer
   * array and grows the array if additional space is required. If the required space cannot be
   * obtained, then the in-memory data will be spilled to disk.
   */
  private void growPointerArrayIfNecessary() throws IOException {
    assert(inMemSorter != null);
    // 如果inMemSorter没有更多的空间来存储记录
    if (!inMemSorter.hasSpaceForAnotherRecord()) {
      // 获取该inMemSorter已经使用的内存大小
      long used = inMemSorter.getMemoryUsage();
      LongArray array;
      try {
        // 增长array的size至原来的2倍(有可能触发的spill操作)
        // could trigger spilling
        array = allocateArray(used / 8 * 2);
      } catch (TooLargePageException e) {
        // 记录指针数组已经很大了，在单个page中已经放不下了，所以需要执行spill
        // The pointer array is too big to fix in a single page, spill.
        // TODO read ShuffleExternalSorter spill
        spill();
        // 直接return了???
        // 因为spill执行后，inMemSorter肯定会有额外的存储空间了啊(因为inMemSorter
        // 中的元素被spill到磁盘中去了呀)
        return;
      } catch (SparkOutOfMemoryError e) {
        // should have trigger spilling
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
          logger.error("Unable to grow the pointer array");
          throw e;
        }
        return;
      }
      // 如果没有异常抛出，仍有可能发生了spill。
      // 所以，先检查是否已经触发了spill
      // check if spilling is triggered or not
      if (inMemSorter.hasSpaceForAnotherRecord()) {
        // 如果inMemSorter现在有额外存储空间了，说明该inMemSorter执行了spill,
        // 则释放掉刚刚申请的array
        freeArray(array);
      } else {
        // 如果没有，则用新申请的array替换就的array
        inMemSorter.expandPointerArray(array);
      }
    }
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the memory manager and spill if the requested memory can not be obtained.
   *
   * @param required the required space in the data page, in bytes, including space for storing
   *                      the record size. This must be less than or equal to the page size (records
   *                      that exceed the page size are handled via a different code path which uses
   *                      special overflow pages).
   */
  private void acquireNewPageIfNecessary(int required) {
    // 如果从未申请过一个page或者当前page的剩余空间(小于required size)不足以存储当前的记录，
    // 则申请一个新的page
    if (currentPage == null ||
      pageCursor + required > currentPage.getBaseOffset() + currentPage.size() ) {
      // TODO: try to find space in previous pages
      // 设置currentPage为新申请的page
      // required大小为record的大小，而且申请的page的内存只会小于等于required，
      // 所以一个page最多只能存下一个记录咯。那上面的TODO为什么还要从previous中去寻找空间呢???
      // 但是从if()里的判断条件看，也是想在一个page中存多个record的意思啊，所以到底是怎么回事呢???
      currentPage = allocatePage(required);
      // 设置pageCursor为当前page的base offset
      pageCursor = currentPage.getBaseOffset();
      // 添加当前page至allocatedPages
      allocatedPages.add(currentPage);
    }
  }

  /**
   * Write a record to the shuffle sorter.
   */
  public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
    throws IOException {

    // for tests
    assert(inMemSorter != null);
    // 如果inMemSorter中的记录个数超过了设定的阈值，则强制执行spill
    if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
      logger.info("Spilling data because number of spilledRecords crossed the threshold " +
        numElementsForSpillThreshold);
      // 强制执行spill
      spill();
    }

    // 先看看是否需要扩增array的size
    growPointerArrayIfNecessary();
    // 需要额外的4个字节用于存储记录的长度(除了记录本身)
    // Need 4 bytes to store the record length.
    final int required = length + 4;
    // 再看看是否有必要申请一个新的page
    acquireNewPageIfNecessary(required);

    assert(currentPage != null);
    final Object base = currentPage.getBaseObject();
    // TODO read
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
    // 总感觉这样直接用Platform怪怪的，需要封装一下，所以有人提出了SPARK-10399(当然，该issue不止于此)
    // 先在该page中存储该记录的长度
    Platform.putInt(base, pageCursor, length);
    // pageCursor向右移动4个字节
    pageCursor += 4;
    // 再在该page中存储该记录(实际数据)
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
    // 再更新pageCursor
    pageCursor += length;
    // 再往inMemSorter中插入该记录的recordAddress和partition ID，用于之后的排序
    inMemSorter.insertRecord(recordAddress, partitionId);
  }

  /**
   * Close the sorter, causing any buffered data to be sorted and written out to disk.
   *
   * @return metadata for the spill files written by this sorter. If no records were ever inserted
   *         into this sorter, then this will return an empty array.
   * @throws IOException
   */
  public SpillInfo[] closeAndGetSpills() throws IOException {
    if (inMemSorter != null) {
      // Do not count the final file towards the spill count.
      writeSortedFile(true);
      freeMemory();
      inMemSorter.free();
      inMemSorter = null;
    }
    return spills.toArray(new SpillInfo[spills.size()]);
  }

}
