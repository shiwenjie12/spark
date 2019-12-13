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

package org.apache.spark.scheduler

import org.apache.spark.SparkContext

/**
 * 插件外部调度程序的集群管理器界面。
 */
private[spark] trait ExternalClusterManager {

  /**
   * 检查此集群管理器实例是否可以为某个主URL创建调度程序组件。
   * @param masterURL the master URL
   * @return True if the cluster manager can create scheduler backend/
   */
  def canCreate(masterURL: String): Boolean

  /**
   * 为给定的SparkContext创建任务计划程序实例
   * @param sc SparkContext
   * @param masterURL the master URL
   * @return TaskScheduler that will be responsible for task handling
   */
  def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler

  /**
   * 为给定的SparkContext和调度程序创建调度程序后端。
   * 在使用“ ExternalClusterManager.createTaskScheduler()”创建任务计划程序之后调用该方法。
   * @param sc SparkContext
   * @param masterURL the master URL
   * @param scheduler TaskScheduler that will be used with the scheduler backend.
   * @return SchedulerBackend that works with a TaskScheduler
   */
  def createSchedulerBackend(sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend

  /**
   * 初始化任务计划程序和后端计划程序。创建调度程序组件后调用
   * @param scheduler TaskScheduler that will be responsible for task handling
   * @param backend SchedulerBackend that works with a TaskScheduler
   */
  def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit
}
