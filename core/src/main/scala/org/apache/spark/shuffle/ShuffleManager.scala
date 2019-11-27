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

package org.apache.spark.shuffle

import org.apache.spark.{ShuffleDependency, TaskContext}

/**
 * 随机播放系统的可插拔接口。根据spark.shuffle.manager设置，
 * 在SparkEnv中的驱动程序和每个执行程序上创建一个ShuffleManager。
 * 驱动程序向其中注册洗牌，执行程序（或驱动程序本地运行的任务）可以要求读取和写入数据。
 *
 * NOTE: this will be instantiated by SparkEnv so its constructor can take a SparkConf and
 * boolean isDriver as parameters.
 */
private[spark] trait ShuffleManager {

  /**
   * 向管理员注册洗牌并获取一个句柄，以将其传递给任务。
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  /** 获取给定分区的作家。地图任务调用执行程序。 */
  def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V]

  /**
   * 获取有关一系列精简分区（包括startPartition到endPartition-1，包括首尾）的读者。
   * 通过减少任务来调用执行程序。
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C]

  /**
   * 获取有关一个特定映射器生成的一系列reduce分区（包括startPartition至endPartition-1，包括其中）的reader。
   * 通过减少任务来调用执行程序。
   */
  def getReaderForOneMapper[K, C](
      handle: ShuffleHandle,
      mapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C]

  /**
   * 从ShuffleManager中删除随机播放的元数据。
   * @return true if the metadata removed successfully, otherwise false.
   */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * 返回一个能够基于块坐标检索混洗块数据的解析器。
   */
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager. */
  def stop(): Unit
}
