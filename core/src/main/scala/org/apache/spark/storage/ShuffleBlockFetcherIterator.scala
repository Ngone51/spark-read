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

import java.io.{File, InputStream, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, ShuffleClient, TempFileManager}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBufferOutputStream

/**
 * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
 * manager. For remote blocks, it fetches them using the provided BlockTransferService.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * The implementation throttles the remote fetches so they don't exceed maxBytesInFlight to avoid
 * using too much memory.
 *
 * @param context [[TaskContext]], used for metrics update
 * @param shuffleClient [[ShuffleClient]] for fetching remote blocks
 * @param blockManager [[BlockManager]] for reading local blocks
 * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require the size (in bytes as a long field) in
 *                        order to throttle the memory usage.
 * @param streamWrapper A function to wrap the returned input stream.
 * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
 * @param maxReqsInFlight max number of remote requests to fetch blocks at any given point.
 * @param maxBlocksInFlightPerAddress max number of shuffle blocks being fetched at any given point
 *                                    for a given remote host:port.
 * @param maxReqSizeShuffleToMem max size (in bytes) of a request that can be shuffled to memory.
 * @param detectCorrupt whether to detect any corruption in fetched blocks.
 */
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: ShuffleClient,
    blockManager: BlockManager,
    blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    maxReqSizeShuffleToMem: Long,
    detectCorrupt: Boolean)
  extends Iterator[(BlockId, InputStream)] with TempFileManager with Logging {

  import ShuffleBlockFetcherIterator._

  /**
   * 拉取的blocks总数。该总数可能会比blocksByAddress中的blocks总数小，因为我们在initialize()方法
   * 中过滤了大小为0的blocks。
   * blocks总数 = 本地blocks的总数 + 远程的blocks总数
   * Total number of blocks to fetch. This can be smaller than the total number of blocks
   * in [[blocksByAddress]] because we filter out zero-sized blocks in [[initialize]].
   *
   * This should equal localBlocks.size + remoteBlocks.size.
   */
  private[this] var numBlocksToFetch = 0

  /**
   * 调用者目前为止处理的blocks总数。当numBlocksProcessed == numBlocksToFetch时，
   * 该iterator也就迭代完毕了。
   * The number of blocks processed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
   */
  private[this] var numBlocksProcessed = 0

  private[this] val startTime = System.currentTimeMillis

  /**
   * 需要拉取的本地blocks，大小为0的blocks除外。
   * Local blocks to fetch, excluding zero-sized blocks.
   */
  private[this] val localBlocks = new ArrayBuffer[BlockId]()

  /**
   * 需要拉取的远程blocks，大小为0的blocks除外
   * Remote blocks to fetch, excluding zero-sized blocks.
   */
  private[this] val remoteBlocks = new HashSet[BlockId]()

  /**
   * 一个用于存储我们FetchResult的阻塞队列。该队列将由BlockTransferService提供的异步模型变成
   * 一个同步模型(iterator)。 哈???什么意思???
   * 答：解释一下。BlockTransferService在获取blocks数据时是异步执行的（发送完请求后，它就不管了，等sever
   * 把block传回来之后，自有listener去处理。而results是一个阻塞队列，它只能按队列中元素的顺序有序获取，并且
   * 当队列中没有元素时（server还没来得及返回响应消息），它会阻塞等待。 ）
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
   * 当前正在被处理的FetchResult。我们之所以追踪它，是为了如果在处理当前buffer的过程中万一抛出了运行时异常抛出，
   * 则可以释放当前的buffer。
   * Current [[FetchResult]] being processed. We track this so we can release the current buffer
   * in case of a runtime exception when processing the current buffer.
   */
  @volatile private[this] var currentResult: SuccessFetchResult = null

  /**
   * 等待发起的FetchRequest队列；我们会逐渐从该队列中取出FetchRequest，以保证正在传输(in flight???)中的
   * 字节总数不会超过maxBytesInFlight的限制。
   * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
   */
  private[this] val fetchRequests = new Queue[FetchRequest]

  /**
   * 用于存储那些出队之后，不能在第一时间(或推迟)发起请求的FetchRequest的队列。这些FetchRequest会在满足拉取的限制后，
   * 再次尝试发起。
   * Queue of fetch requests which could not be issued the first time they were dequeued. These
   * requests are tried again when the fetch constraints are satisfied.
   */
  private[this] val deferredFetchRequests = new HashMap[BlockManagerId, Queue[FetchRequest]]()

  /** Current bytes in flight from our requests */
  private[this] var bytesInFlight = 0L

  /** Current number of requests in flight */
  private[this] var reqsInFlight = 0

  /** Current number of blocks in flight per host:port */
  private[this] val numBlocksInFlightPerAddress = new HashMap[BlockManagerId, Int]()

  /**
   * 存储那些不能被成功解压的bloks，用于保证我们最多尝试一次拉取这些败坏(corrupted)的blocks。
   * The blocks that can't be decompressed successfully, it is used to guarantee that we retry
   * at most once for those corrupted blocks.
   */
  private[this] val corruptedBlocks = mutable.HashSet[BlockId]()

  private[this] val shuffleMetrics = context.taskMetrics().createTempShuffleReadMetrics()

  /**
   * Whether the iterator is still active. If isZombie is true, the callback interface will no
   * longer place fetched blocks into [[results]].
   */
  @GuardedBy("this")
  private[this] var isZombie = false

  /**
   * 一个用于存储用于shuffling远程超大的blocks的文件集合。在cleanup的时候，该集合中的文件会被发现(清除)。
   * 这是避免磁盘文件泄露的防护层。
   * A set to store the files used for shuffling remote huge blocks. Files in this set will be
   * deleted when cleanup. This is a layer of defensiveness against disk file leaks.
   */
  @GuardedBy("this")
  private[this] val shuffleFilesSet = mutable.HashSet[File]()

  // TODO read
  initialize()

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    if (currentResult != null) {
      currentResult.buf.release()
    }
    currentResult = null
  }

  override def createTempFile(): File = {
    blockManager.diskBlockManager.createTempLocalBlock()._2
  }

  override def registerTempFileToClean(file: File): Boolean = synchronized {
    if (isZombie) {
      false
    } else {
      shuffleFilesSet += file
      true
    }
  }

  /**
   * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
   */
  private[this] def cleanup() {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, address, _, buf, _) =>
          if (address != blockManager.blockManagerId) {
            shuffleMetrics.incRemoteBytesRead(buf.size)
            if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
              shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
            }
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          buf.release()
        case _ =>
      }
    }
    shuffleFilesSet.foreach { file =>
      if (!file.delete()) {
        logWarning("Failed to cleanup shuffle fetch temp file " + file.getAbsolutePath())
      }
    }
  }

  private[this] def sendRequest(req: FetchRequest) {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    // 更新正在传输的字节大小
    bytesInFlight += req.size
    // 更新正在传输的请求个数
    reqsInFlight += 1

    // so we can look up the size of each blockID
    val sizeMap = req.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
    val remainingBlocks = new HashSet[String]() ++= sizeMap.keys
    val blockIds = req.blocks.map(_._1.toString)
    val address = req.address

    val blockFetchingListener = new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // Only add the buffer to results queue if the iterator is not zombie,
        // i.e. cleanup() has not been called yet.
        ShuffleBlockFetcherIterator.this.synchronized {
          if (!isZombie) {
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            // TODO read
            // 类似于增加一个对该buf的"引用"。只有当"引用"个数为0的时候，该buf才能被成功释放。
            buf.retain()
            // 成功获取一个block，则剩下等待获取的blocks应该去除该成功获取的blcok
            remainingBlocks -= blockId
            // 这个sizeMap的key难道不是blockId.toString吗???
            // 哦，发现没，这个blockId是一个String类型的。
            // 将获取的结果封装成SuccessFetchResult，并存储到results中。
            results.put(new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf,
              remainingBlocks.isEmpty))
            logDebug("remainingBlocks: " + remainingBlocks)
          }
        }
        logTrace("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
      }

      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
        // 获取失败，则将获取的结果封装成一个FailureFetchResult，并同样存储到results中。
        results.put(new FailureFetchResult(BlockId(blockId), address, e))
      }
    }

    // Fetch remote shuffle blocks to disk when the request is too large. Since the shuffle data is
    // already encrypted and compressed over the wire(w.r.t. the related configs), we can just fetch
    // the data and write it to file directly.
    // 如果请求的blocks大小超过了可能在内存中存储的maxReqSizeShuffleToMem限制，则直接将其写入磁盘中
    if (req.size > maxReqSizeShuffleToMem) {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, this)
    } else {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, null)
    }
  }

  // 把本地blocks和远程blocks分开来
  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize
      + ", maxBlocksInFlightPerAddress: " + maxBlocksInFlightPerAddress)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    val remoteRequests = new ArrayBuffer[FetchRequest]

    // Tracks total number of blocks (including zero sized blocks)
    var totalBlocks = 0
    for ((address, blockInfos) <- blocksByAddress) {
      totalBlocks += blockInfos.size
      // executorId相等，说明是本地的
      if (address.executorId == blockManager.blockManagerId.executorId) {
        // 过滤size为0的blocks
        // Filter out zero-sized blocks
        localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
        numBlocksToFetch += localBlocks.size
      } else { // 远程的blocks
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        // 远程的blocks需要一个一个来组成FetchRequest，因为每增加一个block，
        // 就要检查targetRequestSize或maxBlocksInFlightPerAddress的限制
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          // Skip empty blocks
          if (size > 0) {
            curBlocks += ((blockId, size))
            remoteBlocks += blockId
            numBlocksToFetch += 1
            curRequestSize += size
          } else if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          }
          if (curRequestSize >= targetRequestSize ||
              curBlocks.size >= maxBlocksInFlightPerAddress) {
            // 一个FetchRequest可能包含多个blocks(不超过maxBlocksInFlightPerAddress限制)
            // 每一个FetchRequest会批量地从一个node上获取blocks
            // Add this FetchRequest
            remoteRequests += new FetchRequest(address, curBlocks)
            logDebug(s"Creating fetch request of $curRequestSize at $address "
              + s"with ${curBlocks.size} blocks")
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            curRequestSize = 0
          }
        }
        // Add in the final request
        if (curBlocks.nonEmpty) {
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }
    // totalBlocks是blocksByAddress包含的所有blocks总数，而numBlocksToFetch过滤了大小为0的blocks
    logInfo(s"Getting $numBlocksToFetch non-empty blocks out of $totalBlocks blocks")
    remoteRequests
  }

  /**
   * 在我们拉取远程块的同时，拉取本地块。这是可行的(不会发生内存的争用)，因为直到我们创建输入流时，
   * ManagedBuffer的内存才会真正被分配。在此之前，我们在ManagedBuffer上使用的内存只要ManagedBuffer
   * 的引用本身。
   * Fetch the local blocks while we are fetching remote blocks. This is ok because
   * `ManagedBuffer`'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
   */
  private[this] def fetchLocalBlocks() {
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val blockId = iter.next()
      try {
        val buf = blockManager.getBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()
        results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf, false))
      } catch {
        case e: Exception =>
          // If we see an exception, stop immediately.
          logError(s"Error occurred while fetching local blocks", e)
          results.put(new FailureFetchResult(blockId, blockManager.blockManagerId, e))
          return
      }
    }
  }

  private[this] def initialize(): Unit = {
    // TODO read cleanup()
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener(_ => cleanup())

    // 分开本地blocks和远程blocks
    // Split local and remote blocks.
    val remoteRequests = splitLocalRemoteBlocks()
    // 将远程请求以随机地顺序添加到我们的队列当中。
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // 发起我们对blocks的初始请求，直到maxBytesInFlight的限制
    // Send out initial requests for blocks, up to our maxBytesInFlight
    fetchUpToMaxBytes()

    // 已经发起的远程请求的个数（remoteRequests：所有远程请求数组；fetchRequests等待发起的远程请求队列）
    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
   * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
   * underlying each InputStream will be freed by the cleanup() method registered with the
   * TaskCompletionListener. However, callers should close() these InputStreams
   * as soon as they are no longer needed, in order to release memory as early as possible.
   *
   * Throws a FetchFailedException if the next block could not be fetched.
   */
  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }

    numBlocksProcessed += 1

    var result: FetchResult = null
    var input: InputStream = null
    // Take the next fetched result and try to decompress it to detect data corruption,
    // then fetch it one more time if it's corrupt, throw FailureFetchResult if the second fetch
    // is also corrupt, so the previous stage could be retried.
    // For local shuffle block, throw FailureFetchResult for the first IOException.
    while (result == null) {
      val startFetchWait = System.currentTimeMillis()
      // 当results中没有元素时，会发生阻塞（直到server发回响应数据）
      result = results.take()
      val stopFetchWait = System.currentTimeMillis()
      shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

      result match {
        case r @ SuccessFetchResult(blockId, address, size, buf, isNetworkReqDone) =>
          if (address != blockManager.blockManagerId) {
            // TODO 现在才减少该值会不会有点太晚？
            numBlocksInFlightPerAddress(address) = numBlocksInFlightPerAddress(address) - 1
            shuffleMetrics.incRemoteBytesRead(buf.size)
            if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
              shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
            }
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          bytesInFlight -= size
          // 如果该SuccessFetchResult是一个request中的最后一个拉取的block，
          // 那么意味这该request也就完成了所有拉取请求，则reqsInFlight减1。
          if (isNetworkReqDone) {
            reqsInFlight -= 1
            logDebug("Number of requests in flight " + reqsInFlight)
          }

          val in = try {
            // TODO read
            // buf创建input stream
            buf.createInputStream()
          } catch {
            // The exception could only be throwed by local shuffle block
            case e: IOException =>
              assert(buf.isInstanceOf[FileSegmentManagedBuffer])
              logError("Failed to create input stream from local block", e)
              buf.release()
              throwFetchFailedException(blockId, address, e)
          }

          // 封装压缩流
          input = streamWrapper(blockId, in)
          // Only copy the stream if it's wrapped by compression or encryption, also the size of
          // block is small (the decompressed block is smaller than maxBytesInFlight)
          if (detectCorrupt && !input.eq(in) && size < maxBytesInFlight / 3) {
            val originalInput = input
            val out = new ChunkedByteBufferOutputStream(64 * 1024, ByteBuffer.allocate)
            try {
              // Decompress the whole block at once to detect any corruption, which could increase
              // the memory usage tne potential increase the chance of OOM.
              // TODO: manage the memory used here, and spill it into disk in case of OOM.
              Utils.copyStream(input, out)
              out.close()
              input = out.toChunkedByteBuffer.toInputStream(dispose = true)
            } catch {
              // copyStream失败，就意味着corrupt了吗???
              case e: IOException =>
                buf.release()
                if (buf.isInstanceOf[FileSegmentManagedBuffer]
                  || corruptedBlocks.contains(blockId)) {
                  throwFetchFailedException(blockId, address, e)
                } else {
                  logWarning(s"got an corrupted block $blockId from $address, fetch again", e)
                  corruptedBlocks += blockId
                  // 重新获取的话，server端的chunk index还对的上来吗???
                  fetchRequests += FetchRequest(address, Array((blockId, size)))
                  result = null
                }
            } finally {
              // TODO: release the buf here to free memory earlier
              originalInput.close()
              in.close()
            }
          }

        case FailureFetchResult(blockId, address, e) =>
          throwFetchFailedException(blockId, address, e)
      }

      // 我们再次发起（可能多个）获取远程block的请求，放心这个过程是异步执行的
      // Send fetch requests up to maxBytesInFlight
      fetchUpToMaxBytes()
    }

    currentResult = result.asInstanceOf[SuccessFetchResult]
    (currentResult.blockId, new BufferReleasingInputStream(input, this))
  }

  private def fetchUpToMaxBytes(): Unit = {
    // 发送(blocks)拉取请求，直至maxBytesInFlight。如果你不能立刻从一个远程主机上获取blocks，则把它
    // 加入推迟请求队列，直到下次它能够被再次处理。
    // Send fetch requests up to maxBytesInFlight. If you cannot fetch from a remote host
    // immediately, defer the request until the next time it can be processed.

    // TODO read
    // 如果可能的话，处理任何outstanding(突出?优秀?)的推迟请求。
    // Process any outstanding deferred fetch requests if possible.
    if (deferredFetchRequests.nonEmpty) {
      for ((remoteAddress, defReqQueue) <- deferredFetchRequests) {
        while (isRemoteBlockFetchable(defReqQueue) &&
            !isRemoteAddressMaxedOut(remoteAddress, defReqQueue.front)) {
          val request = defReqQueue.dequeue()
          logDebug(s"Processing deferred fetch request for $remoteAddress with "
            + s"${request.blocks.length} blocks")
          send(remoteAddress, request)
          if (defReqQueue.isEmpty) {
            deferredFetchRequests -= remoteAddress
          }
        }
      }
    }

    // 如果可能的话，处理任何正常的拉取请求
    // Process any regular fetch requests if possible.
    // 检查是否满足发起一个远程拉取请求的条件
    while (isRemoteBlockFetchable(fetchRequests)) {
      val request = fetchRequests.dequeue()
      val remoteAddress = request.address
      // 检查同一个主机的blocks拉取数量是否已经达到上限
      if (isRemoteAddressMaxedOut(remoteAddress, request)) {
        logDebug(s"Deferring fetch request for $remoteAddress with ${request.blocks.size} blocks")
        // 如果达到上限了，就暂且将该请求放入延迟请求队列中
        val defReqQueue = deferredFetchRequests.getOrElse(remoteAddress, new Queue[FetchRequest]())
        defReqQueue.enqueue(request)
        deferredFetchRequests(remoteAddress) = defReqQueue
      } else {
        // 如果既满足发起远程请求的条件，又满足同一主机拉取条件，
        // 则发起远程拉取请求
        send(remoteAddress, request)
      }
    }

    def send(remoteAddress: BlockManagerId, request: FetchRequest): Unit = {
      // 发起(异步)请求
      sendRequest(request)
      // 更新numBlocksInFlightPerAddress
      numBlocksInFlightPerAddress(remoteAddress) =
        numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size
    }

    // 检查是否满足发起一个远程拉取请求的条件
    def isRemoteBlockFetchable(fetchReqQueue: Queue[FetchRequest]): Boolean = {
      fetchReqQueue.nonEmpty &&
        (bytesInFlight == 0 ||
          (reqsInFlight + 1 <= maxReqsInFlight &&
            bytesInFlight + fetchReqQueue.front.size <= maxBytesInFlight))
    }

    // 检查是否满足同一个远程主机blocks拉取数量不超过maxBlocksInFlightPerAddress的限制条件
    // Checks if sending a new fetch request will exceed the max no. of blocks being fetched from a
    // given remote address.
    def isRemoteAddressMaxedOut(remoteAddress: BlockManagerId, request: FetchRequest): Boolean = {
      numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size >
        maxBlocksInFlightPerAddress
    }
  }

  private def throwFetchFailedException(blockId: BlockId, address: BlockManagerId, e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }
}

/**
 * 一个用于确保当InputStream.close()被调用时，底层的ManagedBuffer能够被释放的辅助类。
 * Helper class that ensures a ManagedBuffer is released upon InputStream.close()
 */
private class BufferReleasingInputStream(
    private val delegate: InputStream,
    private val iterator: ShuffleBlockFetcherIterator)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[storage]
object ShuffleBlockFetcherIterator {

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of tuple, where the first element is the block id,
   *               and the second element is the estimated size, used to calculate bytesInFlight.
   */
  case class FetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)]) {
    // size是seq中所有blocks的size之和
    val size = blocks.map(_._2).sum
  }

  /**
   * Result of a fetch from a remote block.
   */
  private[storage] sealed trait FetchResult {
    val blockId: BlockId
    val address: BlockManagerId
  }

  /**
   * Result of a fetch from a remote block successfully.
   * @param blockId block id
   * @param address BlockManager that the block was fetched from.
   * @param size estimated size of the block, used to calculate bytesInFlight.
   *             Note that this is NOT the exact bytes.
   * @param buf `ManagedBuffer` for the content.
   * @param isNetworkReqDone Is this the last network request for this host in this fetch request.
   */
  private[storage] case class SuccessFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer,
      isNetworkReqDone: Boolean) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param address BlockManager that the block was attempted to be fetched from
   * @param e the failure exception
   */
  private[storage] case class FailureFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      e: Throwable)
    extends FetchResult
}
