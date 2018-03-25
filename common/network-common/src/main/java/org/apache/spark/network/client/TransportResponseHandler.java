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

package org.apache.spark.network.client;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.ResponseMessage;
import org.apache.spark.network.protocol.RpcFailure;
import org.apache.spark.network.protocol.RpcResponse;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.StreamFailure;
import org.apache.spark.network.protocol.StreamResponse;
import org.apache.spark.network.server.MessageHandler;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;
import org.apache.spark.network.util.TransportFrameDecoder;

/**
 * Handler that processes server responses, in response to requests issued from a
 * [[TransportClient]]. It works by tracking the list of outstanding requests (and their callbacks).
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
  private static final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

  private final Channel channel;

  private final Map<StreamChunkId, ChunkReceivedCallback> outstandingFetches;

  private final Map<Long, RpcResponseCallback> outstandingRpcs;

  private final Queue<Pair<String, StreamCallback>> streamCallbacks;
  private volatile boolean streamActive;

  /** Records the time (in system nanoseconds) that the last fetch or RPC request was sent. */
  private final AtomicLong timeOfLastRequestNs;

  public TransportResponseHandler(Channel channel) {
    this.channel = channel;
    this.outstandingFetches = new ConcurrentHashMap<>();
    this.outstandingRpcs = new ConcurrentHashMap<>();
    this.streamCallbacks = new ConcurrentLinkedQueue<>();
    this.timeOfLastRequestNs = new AtomicLong(0);
  }

  public void addFetchRequest(StreamChunkId streamChunkId, ChunkReceivedCallback callback) {
    updateTimeOfLastRequest();
    outstandingFetches.put(streamChunkId, callback);
  }

  public void removeFetchRequest(StreamChunkId streamChunkId) {
    outstandingFetches.remove(streamChunkId);
  }

  public void addRpcRequest(long requestId, RpcResponseCallback callback) {
    updateTimeOfLastRequest();
    outstandingRpcs.put(requestId, callback);
  }

  public void removeRpcRequest(long requestId) {
    outstandingRpcs.remove(requestId);
  }

  public void addStreamCallback(String streamId, StreamCallback callback) {
    timeOfLastRequestNs.set(System.nanoTime());
    streamCallbacks.offer(ImmutablePair.of(streamId, callback));
  }

  @VisibleForTesting
  public void deactivateStream() {
    streamActive = false;
  }

  /**
   * Fire the failure callback for all outstanding requests. This is called when we have an
   * uncaught exception or pre-mature connection termination.
   */
  private void failOutstandingRequests(Throwable cause) {
    for (Map.Entry<StreamChunkId, ChunkReceivedCallback> entry : outstandingFetches.entrySet()) {
      try {
        entry.getValue().onFailure(entry.getKey().chunkIndex, cause);
      } catch (Exception e) {
        logger.warn("ChunkReceivedCallback.onFailure throws exception", e);
      }
    }
    for (Map.Entry<Long, RpcResponseCallback> entry : outstandingRpcs.entrySet()) {
      try {
        entry.getValue().onFailure(cause);
      } catch (Exception e) {
        logger.warn("RpcResponseCallback.onFailure throws exception", e);
      }
    }
    for (Pair<String, StreamCallback> entry : streamCallbacks) {
      try {
        entry.getValue().onFailure(entry.getKey(), cause);
      } catch (Exception e) {
        logger.warn("StreamCallback.onFailure throws exception", e);
      }
    }

    // It's OK if new fetches appear, as they will fail immediately.
    outstandingFetches.clear();
    outstandingRpcs.clear();
    streamCallbacks.clear();
  }

  @Override
  public void channelActive() {
  }

  @Override
  public void channelInactive() {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
        numOutstandingRequests(), remoteAddress);
      failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"));
    }
  }

  @Override
  public void exceptionCaught(Throwable cause) {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
        numOutstandingRequests(), remoteAddress);
      failOutstandingRequests(cause);
    }
  }

  // 处理TransportRequestHandler#respone#channel.writeAndFlush()发送过来的消息
  @Override
  public void handle(ResponseMessage message) throws Exception {
    // 如果message是chunk拉取成功的消息
    if (message instanceof ChunkFetchSuccess) {
      ChunkFetchSuccess resp = (ChunkFetchSuccess) message;
      ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
      // 如果该请求不在outstandingFetches里面，则忽略它，但需要先释放buf
      if (listener == null) {
        logger.warn("Ignoring response for block {} from {} since it is not outstanding",
          resp.streamChunkId, getRemoteAddress(channel));
        resp.body().release();
      } else {
        outstandingFetches.remove(resp.streamChunkId);
        // 这里的listener应该对应OneForOneBlockFetcher里的chunkCallback，
        // chunkCallback继而有会将该消息传递给ShuffleBlockFetcherIterator里的blockFetchingListener。
        // 其实，这也是底层网络传输中callback和上层listener的一种一一对应，就像chunk和block对应一样。
        listener.onSuccess(resp.streamChunkId.chunkIndex, resp.body());
        // 释放buf占用的内存
        resp.body().release();
      }
    } else if (message instanceof ChunkFetchFailure) { // 拉取失败的消息
      ChunkFetchFailure resp = (ChunkFetchFailure) message;
      ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
      if (listener == null) {
        logger.warn("Ignoring response for block {} from {} ({}) since it is not outstanding",
          resp.streamChunkId, getRemoteAddress(channel), resp.errorString);
      } else {
        outstandingFetches.remove(resp.streamChunkId);
        // 这里的listener应该对应OneForOneBlockFetcher里的chunkCallback
        listener.onFailure(resp.streamChunkId.chunkIndex, new ChunkFetchFailureException(
          "Failure while fetching " + resp.streamChunkId + ": " + resp.errorString));
      }
    } else if (message instanceof RpcResponse) { // 响应的消息是"RpcResponse"
      RpcResponse resp = (RpcResponse) message;
      // 获取对应的rpc请求id，该id是client和server之间处理同一个rpc请求的凭据
      RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
      if (listener == null) {
        logger.warn("Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
          resp.requestId, getRemoteAddress(channel), resp.body().size());
      } else {
        outstandingRpcs.remove(resp.requestId);
        try {
          listener.onSuccess(resp.body().nioByteBuffer());
        } finally {
          resp.body().release();
        }
      }
    } else if (message instanceof RpcFailure) {
      RpcFailure resp = (RpcFailure) message;
      RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
      if (listener == null) {
        logger.warn("Ignoring response for RPC {} from {} ({}) since it is not outstanding",
          resp.requestId, getRemoteAddress(channel), resp.errorString);
      } else {
        outstandingRpcs.remove(resp.requestId);
        listener.onFailure(new RuntimeException(resp.errorString));
      }
    } else if (message instanceof StreamResponse) { // stream响应消息
      StreamResponse resp = (StreamResponse) message;
      // 使用poll直接从streamCallbacks队首取出callback（因为streamCallbacks中的callbacks在
      // 添加时是加锁的，已经保证了它的有序性）
      // TODO read 通过网络传输后，接收到的resp还是有序的吗？
      Pair<String, StreamCallback> entry = streamCallbacks.poll();
      if (entry != null) {
        StreamCallback callback = entry.getValue();
        // TODO read
        if (resp.byteCount > 0) {
          StreamInterceptor interceptor = new StreamInterceptor(this, resp.streamId, resp.byteCount,
            callback);
          try {
            TransportFrameDecoder frameDecoder = (TransportFrameDecoder)
              channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
            frameDecoder.setInterceptor(interceptor);
            streamActive = true;
          } catch (Exception e) {
            logger.error("Error installing stream handler.", e);
            deactivateStream();
          }
        } else {
          try {
            callback.onComplete(resp.streamId);
          } catch (Exception e) {
            logger.warn("Error in stream handler onComplete().", e);
          }
        }
      } else {
        logger.error("Could not find callback for StreamResponse.");
      }
    } else if (message instanceof StreamFailure) {
      StreamFailure resp = (StreamFailure) message;
      Pair<String, StreamCallback> entry = streamCallbacks.poll();
      if (entry != null) {
        StreamCallback callback = entry.getValue();
        try {
          callback.onFailure(resp.streamId, new RuntimeException(resp.error));
        } catch (IOException ioe) {
          logger.warn("Error in stream failure handler.", ioe);
        }
      } else {
        logger.warn("Stream failure with unknown callback: {}", resp.error);
      }
    } else {
      throw new IllegalStateException("Unknown response type: " + message.type());
    }
  }

  /** Returns total number of outstanding requests (fetch requests + rpcs) */
  public int numOutstandingRequests() {
    return outstandingFetches.size() + outstandingRpcs.size() + streamCallbacks.size() +
      (streamActive ? 1 : 0);
  }

  /** Returns the time in nanoseconds of when the last request was sent out. */
  public long getTimeOfLastRequestNs() {
    return timeOfLastRequestNs.get();
  }

  /** Updates the time of the last request to the current system time. */
  public void updateTimeOfLastRequest() {
    timeOfLastRequestNs.set(System.nanoTime());
  }

}
