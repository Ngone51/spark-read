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

package org.apache.spark.storage

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}
import java.nio.channels.FileChannel

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.util.Utils

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block. For efficiency, it retains the underlying file channel across
 * multiple commits. This channel is kept open until close() is called. In case of faults,
 * callers should instead close with revertPartialWritesAndClose() to atomically revert the
 * uncommitted partial writes.
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 */
private[spark] class DiskBlockObjectWriter(
    val file: File,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    syncWrites: Boolean,
    // These write metrics concurrently shared with other active DiskBlockObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetrics,
    val blockId: BlockId = null)
  extends OutputStream
  with Logging {

  /**
   * Guards against close calls, e.g. from a wrapping stream.
   * Call manualClose to close the stream that was extended by this trait.
   * Commit uses this trait to close object streams without paying the
   * cost of closing and opening the underlying file.
   */
  private trait ManualCloseOutputStream extends OutputStream {
    abstract override def close(): Unit = {
      flush()
    }

    // TODO read 看不懂???
    def manualClose(): Unit = {
      super.close()
    }
  }

  /** The file channel, used for repositioning / truncating the file. */
  private var channel: FileChannel = null
  private var mcs: ManualCloseOutputStream = null
  private var bs: OutputStream = null
  private var fos: FileOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private var initialized = false
  private var streamOpen = false
  private var hasBeenClosed = false

  /**
   * Cursors used to represent positions in the file.
   *
   * xxxxxxxxxx|----------|-----|
   *           ^          ^     ^
   *           |          |    channel.position()
   *           |        reportedPosition
   *         committedPosition
   *
   * reportedPosition: Position at the time of the last update to the write metrics.
   * committedPosition: Offset after last committed write.
   * -----: Current writes to the underlying file.
   * xxxxx: Committed contents of the file.
   */
  private var committedPosition = file.length()
  private var reportedPosition = committedPosition

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   * And we reset it after every commitAndGet called.
   */
  private var numRecordsWritten = 0

  private def initialize(): Unit = {
    // 以追加写的方式创建文件输出流
    fos = new FileOutputStream(file, true)
    // 获取文件输入流的管道
    channel = fos.getChannel()
    ts = new TimeTrackingOutputStream(writeMetrics, fos)
    // 这个流的运用哟....???
    class ManualCloseBufferedOutputStream
      extends BufferedOutputStream(ts, bufferSize) with ManualCloseOutputStream
    mcs = new ManualCloseBufferedOutputStream
  }

  def open(): DiskBlockObjectWriter = {
    // 如果writer已经关闭，则抛出异常
    if (hasBeenClosed) {
      // 只允许被打开一次
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    // 如果writer还未初始化，则初始化之
    if (!initialized) {
      initialize()
      initialized = true
    }

    // 这个流的灵活使用，真是我的薄弱点，要多看、多想、多运用!!!
    // 之后要重点关注SerializerManager和SerializerInstance这两个类
    // 在流mcs上在封装一层压缩流，得到bs流
    bs = serializerManager.wrapStream(blockId, mcs)
    // 再在bs流上封装一层序列化流，得到objOut流
    objOut = serializerInstance.serializeStream(bs)
    streamOpen = true
    this
  }

  /**
   * Close and cleanup all resources.
   * Should call after committing or reverting partial writes.
   */
  private def closeResources(): Unit = {
    if (initialized) {
      Utils.tryWithSafeFinally {
        mcs.manualClose()
      } {
        channel = null
        mcs = null
        bs = null
        fos = null
        ts = null
        objOut = null
        initialized = false
        streamOpen = false
        hasBeenClosed = true
      }
    }
  }

  /**
   * commits任何遗留的partial writes（怎么翻译？），并关闭资源。
   * 显然，如果我们在调用close()之前，刚刚调用了commitAndGet()，并且在这之前未写入任何内容，
   * 则此时调用commitAndGet()返回的FileSegment的length为0。
   * Commits any remaining partial writes and closes resources.
   */
  override def close() {
    if (initialized) {
      Utils.tryWithSafeFinally {
        commitAndGet()
      } {
        closeResources()
      }
    }
  }

  /**
   * Flush the partial writes and commit them as a single atomic block.
   * A commit may write additional bytes to frame the atomic block.
   *
   * @return file segment with previous offset and length committed on this call.
   */
  def commitAndGet(): FileSegment = {
    if (streamOpen) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      objOut.close()
      streamOpen = false

      if (syncWrites) {
        // Force outstanding writes to disk and track how long it takes
        val start = System.nanoTime()
        // TODO read fos.getFD.sync()
        // 强制所有系统缓冲区与基础设备同步。该方法在此 FileDescriptor 的所有修改数据和属性都写入相关设备后返回。
        // 特别是，如果此 FileDescriptor 引用物理存储介质，比如文件系统中的文件，则一直要等到将与此 FileDesecriptor
        // 有关的缓冲区的所有内存中修改副本写入物理介质中，sync 方法才会返回。 sync 方法由要求物理存储（比如文件）
        // 处于某种已知状态下的代码使用。例如，提供简单事务处理设施的类可以使用 sync 来确保某个文件所有由给定事务造
        // 成的更改都记录在存储介质上。 sync 只影响此 FileDescriptor 的缓冲区下游。如果正通过应用程序（例如，通过
        // 一个 BufferedOutputStream 对象）实现内存缓冲，那么必须在数据受 sync 影响之前将这些缓冲区刷新，并转到
        // FileDescriptor 中（例如，通过调用 OutputStream.flush）。
        fos.getFD.sync()
        writeMetrics.incWriteTime(System.nanoTime() - start)
      }

      val pos = channel.position()
      // 如果是该writer第一次执行该方法，那么，committedPosition = 0(没毛病吧???)
      val fileSegment = new FileSegment(file, committedPosition, pos - committedPosition)
      committedPosition = pos
      // In certain compression codecs, more bytes are written after streams are closed
      writeMetrics.incBytesWritten(committedPosition - reportedPosition)
      reportedPosition = committedPosition
      numRecordsWritten = 0
      fileSegment
    } else {
      new FileSegment(file, committedPosition, 0)
    }
  }


  /**
   * Reverts writes that haven't been committed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   *
   * @return the file that this DiskBlockObjectWriter wrote to.
   */
  def revertPartialWritesAndClose(): File = {
    // Discard current writes. We do this by flushing the outstanding writes and then
    // truncating the file to its initial position.
    Utils.tryWithSafeFinally {
      if (initialized) {
        writeMetrics.decBytesWritten(reportedPosition - committedPosition)
        writeMetrics.decRecordsWritten(numRecordsWritten)
        streamOpen = false
        closeResources()
      }
    } {
      var truncateStream: FileOutputStream = null
      try {
        truncateStream = new FileOutputStream(file, true)
        // 清除file[0, committedPosition]之外写入的内容(就是把committedPosition后写入的，
        // 还未提交的内容清除)
        truncateStream.getChannel.truncate(committedPosition)
      } catch {
        case e: Exception =>
          logError("Uncaught exception while reverting partial writes to file " + file, e)
      } finally {
        if (truncateStream != null) {
          truncateStream.close()
          truncateStream = null
        }
      }
    }
    file
  }

  /**
   * Writes a key-value pair.
   */
  def write(key: Any, value: Any) {
    if (!streamOpen) {
      open()
    }

    // 注意调用的是writeKey()，writeKey()又会调用writerObject()
    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!streamOpen) {
      open()
    }

    bs.write(kvBytes, offs, len)
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  def recordWritten(): Unit = {
    // 写入record个数+1
    numRecordsWritten += 1
    // 更新writer的统计信息
    writeMetrics.incRecordsWritten(1)

    // 每个2^14个record，更新写入文件的bytes大小
    if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
    }
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private def updateBytesWritten() {
    // pos表示当前文件写入内容的大小
    val pos = channel.position()
    // pos - reportedPosition：当前大小减去上一次更新时的文件大小，
    // 表示其增加的内容大小
    writeMetrics.incBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }

  // For testing
  private[spark] override def flush() {
    objOut.flush()
    bs.flush()
  }
}
