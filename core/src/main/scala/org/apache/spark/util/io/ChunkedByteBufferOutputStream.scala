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

package org.apache.spark.util.io

import java.io.OutputStream
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.storage.StorageUtils

/**
 * 这部分代码也很牛逼啊。ChunkedByteBufferOutputStream能够以分散的多个chunk来构建ChunkedByteBuffer，
 * 而不是申请一个连续存储的Array[ByteBuffer]。Array[ByteBuffer]需要一片连续的内存空间，不仅对内存的
 * 占用多，而且利用率低。而用分散的多个chunk存储，可以有效提升内存的利用率。之所以说chunks(虽然它本身
 * 是一个Array)是分散的，是因为，我们再申请ByteBuffer不是一次性就申请好的，而是根据需要，一个一个申请
 * 的(see allocateNewChunkIfNeeded())。
 * An OutputStream that writes to fixed-size chunks of byte arrays.
 *
 * @param chunkSize size of each chunk, in bytes.
 */
private[spark] class ChunkedByteBufferOutputStream(
    chunkSize: Int,
    allocator: Int => ByteBuffer)
  extends OutputStream {

  private[this] var toChunkedByteBufferWasCalled = false

  private val chunks = new ArrayBuffer[ByteBuffer]

  /** Index of the last chunk. Starting with -1 when the chunks array is empty. */
  private[this] var lastChunkIndex = -1

  /**
   * Next position to write in the last chunk.
   *
   * If this equals chunkSize, it means for next write we need to allocate a new chunk.
   * This can also never be 0.
   */
  private[this] var position = chunkSize
  private[this] var _size = 0
  private[this] var closed: Boolean = false

  def size: Long = _size

  override def close(): Unit = {
    if (!closed) {
      super.close()
      closed = true
    }
  }

  // 把一个Int类型转换成一个Byte存储到ByteBuffer中去
  // 但是，一个Byte最多只能表示+127,不然就溢出了。是否需要assert(b < 128)呢???
  override def write(b: Int): Unit = {
    require(!closed, "cannot write to a closed ChunkedByteBufferOutputStream")
    allocateNewChunkIfNeeded()
    chunks(lastChunkIndex).put(b.toByte)
    // 当前chunk字节数+1
    position += 1
    // 总字节数+1
    _size += 1
  }

  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    require(!closed, "cannot write to a closed ChunkedByteBufferOutputStream")
    var written = 0
    while (written < len) {
      allocateNewChunkIfNeeded()
      // 在当前chunk的剩余可存储字节大小和bytes的未写入的字节数中取小的那一个
      val thisBatch = math.min(chunkSize - position, len - written)
      chunks(lastChunkIndex).put(bytes, written + off, thisBatch)
      written += thisBatch
      position += thisBatch
    }
    _size += len
  }

  @inline
  private def allocateNewChunkIfNeeded(): Unit = {
    // 如果position == chunkSize，则新申请一个chunkSize大小的ByteBuffer
    if (position == chunkSize) {
      chunks += allocator(chunkSize)
      lastChunkIndex += 1
      position = 0
    }
  }

  def toChunkedByteBuffer: ChunkedByteBuffer = {
    require(closed, "cannot call toChunkedByteBuffer() unless close() has been called")
    require(!toChunkedByteBufferWasCalled, "toChunkedByteBuffer() can only be called once")
    toChunkedByteBufferWasCalled = true
    if (lastChunkIndex == -1) {
      new ChunkedByteBuffer(Array.empty[ByteBuffer])
    } else {
      // Copy the first n-1 chunks to the output, and then create an array that fits the last chunk.
      // 哈，什么意思???
      // An alternative would have been returning an array of ByteBuffers, with the last buffer
      // bounded to only the last chunk's position. However, given our use case in Spark (to put
      // the chunks in block manager), only limiting the view bound of the buffer would still
      // require the block manager to store the whole chunk.
      val ret = new Array[ByteBuffer](chunks.size)
      for (i <- 0 until chunks.size - 1) {
        ret(i) = chunks(i)
        ret(i).flip()
      }
      // 如果最后一个chunk刚好是一个写满的chunk，那么，整个拷贝到ret(lastChunkIndex)中去
      if (position == chunkSize) {
        ret(lastChunkIndex) = chunks(lastChunkIndex)
        ret(lastChunkIndex).flip()
      } else {
        // 最后一个chunk未写满，为了节省存储空间，我们只申请
        // 实际空间占用大小的ByteBuffer，并拷贝给ret
        ret(lastChunkIndex) = allocator(position)
        chunks(lastChunkIndex).flip()
        ret(lastChunkIndex).put(chunks(lastChunkIndex))
        ret(lastChunkIndex).flip()
        // TODO 为什么最后一个chunk需要dispose,而其它的chunk不需要???
        StorageUtils.dispose(chunks(lastChunkIndex))
      }
      new ChunkedByteBuffer(ret)
    }
  }
}
