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

import io.netty.channel.Channel;

/**
 * 在将其返回给用户之前在TransportClient上执行的引导程序。
 * 这使得能够在每个连接一次的基础上初始交换信息（例如，SASL认证令牌）。
 *
 *
 * 由于连接（和TransportClients）被尽可能多地重用，因此执行昂贵的自举操作通常是合理的，因为它们通常与JVM本身共享寿命。
 */
public interface TransportClientBootstrap {
  /** Performs the bootstrapping operation, throwing an exception on failure. */
  void doBootstrap(TransportClient client, Channel channel) throws RuntimeException;
}
