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

package org.apache.spark.network.netty

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, OpenBlocks, StreamHandle, UploadBlock}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, StorageLevel}

/**
 * Serves requests to open blocks by simply registering one chunk per block requested.
 * Handles opening and uploading arbitrary（随意地？？？） BlockManager blocks.
 *
 * Opened blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one Spark-level shuffle block.
 * 注册了“一对一”策略的打开的Block，意为着每一个传输层的chunk对应着spark层的一个shuffle block
 */
class NettyBlockRpcServer(
    appId: String,
    serializer: Serializer,
    blockManager: BlockDataManager)
  extends RpcHandler with Logging {

  private val streamManager = new OneForOneStreamManager()

  // 处理由TransportRequestHandler#processRpcRequest()中传输过来的请求
  override def receive(
      client: TransportClient,
      rpcMessage: ByteBuffer,
      responseContext: RpcResponseCallback): Unit = {
    // 解析收到的消息类型
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    logTrace(s"Received request: $message")

    message match {
      // OpenBlocks： 读取（拉取）blocks的请求
      case openBlocks: OpenBlocks =>
        val blocksNum = openBlocks.blockIds.length
        // 通过blockManager的getBlockData()获取block
        val blocks = for (i <- (0 until blocksNum).view)
          yield blockManager.getBlockData(BlockId.apply(openBlocks.blockIds(i)))
        // 向streamManger注册一个新的stream(日后必有用...)
        // 解释一下"日后必有用"以及stream和blocks之间的关联：
        // client端会首先发送一个Rpc请求到Server端，该Rpc请求中包含了"OpenBlocks"的消息。
        // server端接收"OpenBlocks"的消息之后，最终会通过BlockManager来获取"OpenBlocks"
        // 请求中所有blockIds对应的blocks（注意：通过getBlockData获取的block会被封装成一个
        // ManagedBuffer）。然后，这些blocks又会被封装成一个stream。在stream中有个Iterator
        // 类型的buffers。buffers中的每一个元素被成为chunk。而这chunk其实是与block一一对应的。
        // （从blocks.iterator.asJava这行代码就看得出来）。只不过，chunk是面向与stream而言的，
        // 是网络传输过程中的概念。而block是在spark系统中的概念，是更高层的。而"OpenBlocks"请求
        // 并不会将该stream直接返回给client端，而是先暂存起来，返回的则是一个StreamHandle。
        // 所谓的（stream）"日后必有用"，是当client端收到由server端发送过来的StreamHandle时，
        // client就能根据该StreamHandle，再次向server端发送ChunkFetchRequest请求来获取对应
        // stream中的chunks。至于server为什么不直接发送给client整个stream呢？我觉得：
        // 一、类似于TCP三次握手协议，client和server之间要保证已经建立起了有效连接；
        // 二、一次传输stream可能会对网络资源造成极大压力，引发传输错误
        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava)
        logTrace(s"Registered streamId $streamId with $blocksNum buffers")
        responseContext.onSuccess(new StreamHandle(streamId, blocksNum).toByteBuffer)

      case uploadBlock: UploadBlock =>
        // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
        val (level: StorageLevel, classTag: ClassTag[_]) = {
          serializer
            .newInstance()
            .deserialize(ByteBuffer.wrap(uploadBlock.metadata))
            .asInstanceOf[(StorageLevel, ClassTag[_])]
        }
        val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))
        val blockId = BlockId(uploadBlock.blockId)
        blockManager.putBlockData(blockId, data, level, classTag)
        responseContext.onSuccess(ByteBuffer.allocate(0))
    }
  }

  override def getStreamManager(): StreamManager = streamManager
}
