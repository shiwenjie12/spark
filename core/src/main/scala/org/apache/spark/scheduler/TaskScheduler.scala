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

import scala.collection.mutable.Map

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

/**
 * 低级任务计划程序接口，当前仅由[[org.apache.spark.scheduler.TaskSchedulerImpl]]实施。
 *
 * 该接口允许插入不同的任务计划程序。每个TaskScheduler为单个SparkContext计划任务。
 * 这些调度程序从DAGScheduler获取每个阶段提交给它们的任务集，并负责将任务发送到集群，运行它们，重试是否有故障以及减轻散乱。
 * 他们将事件返回给DAGScheduler。
 */
private[spark] trait TaskScheduler {

  private val appId = "spark-application-" + System.currentTimeMillis

  def rootPool: Pool

  def schedulingMode: SchedulingMode

  def start(): Unit

  // 在系统成功初始化之后调用（通常在spark上下文中）。
  // Yarn使用它来引导基于首选位置的资源分配，等待从属注册等。
  def postStartHook(): Unit = { }

  // Disconnect from the cluster.
  def stop(): Unit

  // 提交一系列要运行的任务。
  def submitTasks(taskSet: TaskSet): Unit

  // Kill all the tasks in a stage and fail the stage and all the jobs that depend on the stage.
  // Throw UnsupportedOperationException if the backend doesn't support kill tasks.
  def cancelTasks(stageId: Int, interruptThread: Boolean): Unit

  /**
   * Kills a task attempt.
   * Throw UnsupportedOperationException if the backend doesn't support kill a task.
   *
   * @return Whether the task was successfully killed.
   */
  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean

  // Kill all the running task attempts in a stage.
  // Throw UnsupportedOperationException if the backend doesn't support kill tasks.
  def killAllTaskAttempts(stageId: Int, interruptThread: Boolean, reason: String): Unit

  // Notify the corresponding `TaskSetManager`s of the stage, that a partition has already completed
  // and they can skip running tasks for it.
  def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit

  // Set the DAG scheduler for upcalls. This is guaranteed to be set before submitTasks is called.
  def setDAGScheduler(dagScheduler: DAGScheduler): Unit

  // Get the default level of parallelism to use in the cluster, as a hint for sizing jobs.
  def defaultParallelism(): Int

  /**
   * Update metrics for in-progress tasks and executor metrics, and let the master know that the
   * BlockManager is still alive. Return true if the driver knows about the given block manager.
   * Otherwise, return false, indicating that the block manager should re-register.
   */
  def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId,
      executorUpdates: Map[(Int, Int), ExecutorMetrics]): Boolean

  /**
   * 获取与作业关联的应用程序ID。
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * 处理丢失的执行者
   */
  def executorLost(executorId: String, reason: ExecutorLossReason): Unit

  /**
   * 处理被删除的worker
   */
  def workerRemoved(workerId: String, host: String, message: String): Unit

  /**
   * 获取与作业关联的应用程序的尝试ID。
   *
   * @return An application's Attempt ID
   */
  def applicationAttemptId(): Option[String]

}
