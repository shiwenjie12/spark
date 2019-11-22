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

/**
 * 客户端连接到服务器后，将在TransportServer的客户端通道上执行的引导程序。
 * 这允许自定义客户端通道以允许诸如SASL身份验证之类的事情。
 */
public interface TransportServerBootstrap {
  /**
   * 如果需要，自定义渠道以包括新功能。
   *
   * @param channel The connected channel opened by the client.
   * @param rpcHandler The RPC handler for the server.
   * @return The RPC handler to use for the channel.
   */
  RpcHandler doBootstrap(Channel channel, RpcHandler rpcHandler);
}
