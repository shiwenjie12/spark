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

import io.netty.channel.Channel;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;

/**
 * StreamManager用于从流中获取单个块。
 * {@link TransportRequestHandler}中使用了此参数，以响应fetchChunk()请求。
 * 流的创建不在传输层的范围内，但是保证只能通过一个客户端连接读取给定的流，
 * 这意味着特定流的getChunk()将被串行调用，并且一旦与该流关联的连接关闭后，该流将不再使用。
 */
public abstract class StreamManager {
  /**
   * 在响应fetchChunk()请求时调用。返回的缓冲区将按原样传递给客户端。
   * 单个流将与单个TCP连接相关联，因此不会为特定流并行调用此方法。
   *
   * Chunks may be requested in any order, and requests may be repeated, but it is not required
   * that implementations support this behavior.
   *
   * The returned ManagedBuffer will be release()'d after being written to the network.
   *
   * @param streamId id of a stream that has been previously registered with the StreamManager.
   * @param chunkIndex 0-indexed chunk of the stream that's requested
   */
  public abstract ManagedBuffer getChunk(long streamId, int chunkIndex);

  /**
   * 在响应stream()请求时调用。返回的数据通过单个TCP连接流式传输到客户端。
   *
   * Note the <code>streamId</code> argument is not related to the similarly named argument in the
   * {@link #getChunk(long, int)} method.
   *
   * @param streamId id of a stream that has been previously registered with the StreamManager.
   * @return A managed buffer for the stream, or null if the stream was not found.
   */
  public ManagedBuffer openStream(String streamId) {
    throw new UnsupportedOperationException();
  }

  /**
   * 指示给定的频道已被终止。发生这种情况之后，我们保证不再从关联的流中读取数据，因此可以清除任何状态。
   */
  public void connectionTerminated(Channel channel) { }

  /**
   * 验证客户端是否有权从给定流中读取。
   *
   * @throws SecurityException If client is not authorized.
   */
  public void checkAuthorization(TransportClient client, long streamId) { }

  /**
   * 返回此StreamManager中正在传输但尚未完成的块数。
   */
  public long chunksBeingTransferred() {
    return 0;
  }

  /**
   * 开始发送块时调用。
   */
  public void chunkBeingSent(long streamId) { }

  /**
   * 开始发送流时调用。
   */
  public void streamBeingSent(String streamId) { }

  /**
   * 成功发送块时调用。
   */
  public void chunkSent(long streamId) { }

  /**
   * 流成功发送时调用。
   */
  public void streamSent(String streamId) { }

}
