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

package org.apache.spark.network.protocol;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encoder used by the server side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 */
@ChannelHandler.Sharable
public final class MessageEncoder extends MessageToMessageEncoder<Message> {

  private static final Logger logger = LoggerFactory.getLogger(MessageEncoder.class);

  public static final MessageEncoder INSTANCE = new MessageEncoder();

  private MessageEncoder() {}

  /***
   * Encodes a Message by invoking its encode() method. For non-data messages, we will add one
   * ByteBuf to 'out' containing the total frame length, the message type, and the message itself.
   * In the case of a ChunkFetchSuccess, we will also add the ManagedBuffer corresponding to the
   * data to 'out', in order to enable zero-copy transfer.
   */
  @Override
  public void encode(ChannelHandlerContext ctx, Message in, List<Object> out) throws Exception {
    Object body = null;
    long bodyLength = 0;
    boolean isBodyInFrame = false;

    // If the message has a body, take it out to enable zero-copy transfer for the payload.
    if (in.body() != null) {
      try {
        bodyLength = in.body().size();
        // 注意，前方高能！
        // convertToNetty会将我们的body(ManagedBuffer)转化成Netty中的ByteBuf或者FileRegion
        // （都用于之后的数据写入）
        body = in.body().convertToNetty();
        isBodyInFrame = in.isBodyInFrame();
      } catch (Exception e) {
        // 因为在convertToNetty的时候，会增加对body的引用，所以如果发生了异常，我们作为caller要及时释放
        in.body().release();
        if (in instanceof AbstractResponseMessage) {
          AbstractResponseMessage resp = (AbstractResponseMessage) in;
          // Re-encode this message as a failure response.
          String error = e.getMessage() != null ? e.getMessage() : "null";
          logger.error(String.format("Error processing %s for client %s",
            in, ctx.channel().remoteAddress()), e);
          encode(ctx, resp.createFailureResponse(error), out);
        } else {
          throw e;
        }
        return;
      }
    }

    // 获取该消息的类型
    Message.Type msgType = in.type();
    // 所有的消息都有frame（帧？） length，消息类型，以及消息本身。而frame length可能不包含body data的length，
    // 这取决于发送的消息有没有body。
    // All messages have the frame length, message type, and message itself. The frame length
    // may optionally include the length of the body data, depending on what message is being
    // sent.
    // "8"表示的是frameLength所用的字节数，因为frameLength是一个long类型的，而long是8个字节。
    int headerLength = 8 + msgType.encodedLength() + in.encodedLength();
    // 整个frame的length由headerLength和bodyLength（前提是消息in中有body）组成
    long frameLength = headerLength + (isBodyInFrame ? bodyLength : 0);
    // 申请一个headerLength大小的ByteBuf用于存储编码后的消息
    ByteBuf header = ctx.alloc().heapBuffer(headerLength);
    // 先编码整个frame的大小
    header.writeLong(frameLength);
    // 再编码消息类型
    msgType.encode(header);
    // 最后编码我们的消息（只编码消息本身（貌似只有消息的大小？包含body的大小），不包含body。
    // 因为我们这里编码的只是header（类似于消息头？））
    in.encode(header);
    // 写入header的data的size必须等于申请的ByteBuf的大小headerLength
    assert header.writableBytes() == 0;

    if (body != null) {
      // 如果该消息带有body，则我们发送MessageWithHeader
      // We transfer ownership of the reference on in.body() to MessageWithHeader.
      // This reference will be freed when MessageWithHeader.deallocate() is called.
      out.add(new MessageWithHeader(in.body(), header, body, bodyLength));
    } else {
      // 否则，直接发送header
      out.add(header);
    }
  }

}
