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

package org.apache.spark.network.server;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.client.TransportClient;

/**
 * {@link org.apache.spark.network.client.TransportClient} 发送的sendRPC()消息的处理程序。
 */
public abstract class RpcHandler {

  private static final RpcResponseCallback ONE_WAY_CALLBACK = new OneWayRpcCallback();

  /**
   * 收到一条RPC消息。在此方法中抛出的任何异常都将以字符串形式作为标准的RPC错误发送回客户端。
   *
   * Neither this method nor #receiveStream will be called in parallel for a single
   * TransportClient (i.e., channel).
   *
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param message The serialized bytes of the RPC.
   * @param callback Callback which should be invoked exactly once upon success or failure of the
   *                 RPC.
   */
  public abstract void receive(
      TransportClient client,
      ByteBuffer message,
      RpcResponseCallback callback);

  /**
   * 接收一条RPC消息，其中包含要作为流接收的数据。
   * 在此方法中抛出的任何异常都将以字符串形式作为标准的RPC错误发送回客户端。
   *
   * Neither this method nor #receive will be called in parallel for a single TransportClient
   * (i.e., channel).
   *
   * An error while reading data from the stream
   * ({@link org.apache.spark.network.client.StreamCallback#onData(String, ByteBuffer)})
   * will fail the entire channel.  A failure in "post-processing" the stream in
   * {@link org.apache.spark.network.client.StreamCallback#onComplete(String)} will result in an
   * rpcFailure, but the channel will remain active.
   *
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param messageHeader The serialized bytes of the header portion of the RPC.  This is in meant
   *                      to be relatively small, and will be buffered entirely in memory, to
   *                      facilitate how the streaming portion should be received.
   * @param callback Callback which should be invoked exactly once upon success or failure of the
   *                 RPC.
   * @return a StreamCallback for handling the accompanying streaming data
   */
  public StreamCallbackWithID receiveStream(
      TransportClient client,
      ByteBuffer messageHeader,
      RpcResponseCallback callback) {
    throw new UnsupportedOperationException();
  }

  /**
   * 返回StreamManager，其中包含有关TransportClient当前正在获取流的状态。
   */
  public abstract StreamManager getStreamManager();

  /**
   * Receives an RPC message that does not expect a reply. The default implementation will
   * call "{@link #receive(TransportClient, ByteBuffer, RpcResponseCallback)}" and log a warning if
   * any of the callback methods are called.
   *
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param message The serialized bytes of the RPC.
   */
  public void receive(TransportClient client, ByteBuffer message) {
    receive(client, message, ONE_WAY_CALLBACK);
  }

  /**
   * Invoked when the channel associated with the given client is active.
   */
  public void channelActive(TransportClient client) { }

  /**
   * Invoked when the channel associated with the given client is inactive.
   * No further requests will come from this client.
   */
  public void channelInactive(TransportClient client) { }

  public void exceptionCaught(Throwable cause, TransportClient client) { }

  private static class OneWayRpcCallback implements RpcResponseCallback {

    private static final Logger logger = LoggerFactory.getLogger(OneWayRpcCallback.class);

    @Override
    public void onSuccess(ByteBuffer response) {
      logger.warn("Response provided for one-way RPC.");
    }

    @Override
    public void onFailure(Throwable e) {
      logger.error("Error response provided for one-way RPC.", e);
    }

  }

}
