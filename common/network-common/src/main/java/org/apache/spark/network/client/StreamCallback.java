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
import java.nio.ByteBuffer;

/**
 * 回调流数据。流数据将提供给
 * {@link #onData (String，ByteBuffer)}方法到达时。收到所有流数据后，
 * {@link #onComplete(String)}将被调用。
 * <p>
 * 网络库保证单个线程一次会调用这些方法，但是不同的线程可能会进行不同的调用。
 */
public interface StreamCallback {
  /** 在收到流数据时调用. */
  void onData(String streamId, ByteBuffer buf) throws IOException;

  /** 当接收到流中的所有数据时调用. */
  void onComplete(String streamId) throws IOException;

  /** 如果从流中读取数据时发生错误，则调用此方法. */
  void onFailure(String streamId, Throwable cause) throws IOException;
}
