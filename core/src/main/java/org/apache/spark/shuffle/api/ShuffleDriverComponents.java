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

package org.apache.spark.shuffle.api;

import java.util.Map;

import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * 用于为驱动程序构建随机播放支持模块的接口。
 */
@Private
public interface ShuffleDriverComponents {

  /**
   * 在驱动程序中调用一次以引导此特定于此应用程序的模块。
   * 在将执行程序请求提交给集群管理器之前，将调用此方法。
   *
   * This method should prepare the module with its shuffle components i.e. registering against
   * an external file servers or shuffle services, or creating tables in a shuffle
   * storage data database.
   *
   * @return additional SparkConf settings necessary for initializing the executor components.
   * This would include configurations that cannot be statically set on the application, like
   * the host:port of external services for shuffle storage.
   */
  Map<String, String> initializeApplication();

  /**
   * 在Spark应用程序的末尾调用一次，以清除任何现有的随机播放状态。
   */
  void cleanupApplication();

  /**
   * 在随机播放阶段首次生成随机播放ID时，每个随机播放ID调用一次。
   *
   * @param shuffleId 随机播放阶段的唯一标识符。
   */
  default void registerShuffle(int shuffleId) {}

  /**
   * 删除与给定随机播放关联的随机播放数据。
   *
   * @param shuffleId The unique identifier for the shuffle stage.
   * @param blocking Whether this call should block on the deletion of the data.
   */
  default void removeShuffle(int shuffleId, boolean blocking) {}
}
