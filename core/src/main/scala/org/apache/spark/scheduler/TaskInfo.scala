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

import org.apache.spark.TaskState
import org.apache.spark.TaskState.TaskState
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * 有关TaskSet中正在运行的任务尝试的信息。
 */
@DeveloperApi
class TaskInfo(
    val taskId: Long,
    /**
     * 该任务在其任务集中的索引。不必与任务正在计算的RDD分区的ID相同。
     */
    val index: Int,
    val attemptNumber: Int,
    val launchTime: Long,
    val executorId: String,
    val host: String,
    val taskLocality: TaskLocality.TaskLocality,
    val speculative: Boolean) {

  /**
   * 任务开始远程获取结果的时间。如果任务完成后立即发送任务结果
   * （与发送IndirectTaskResult和稍后从块管理器获取结果相反），则不会设置。
   */
  var gettingResultTime: Long = 0

  /**
   * 在此任务期间对可累积物料进行中间更新。
   * 请注意，对于一个任务中的同一可累积量多次更新，
   * 或者对于一个任务中存在相同名称但具有不同ID的两个可累积量而言，这都是有效的。
   */
  def accumulables: Seq[AccumulableInfo] = _accumulables

  private[this] var _accumulables: Seq[AccumulableInfo] = Nil

  private[spark] def setAccumulables(newAccumulables: Seq[AccumulableInfo]): Unit = {
    _accumulables = newAccumulables
  }

  /**
   * The time when the task has completed successfully (including the time to remotely fetch
   * results, if necessary).
   */
  var finishTime: Long = 0

  var failed = false

  var killed = false

  private[spark] def markGettingResult(time: Long): Unit = {
    gettingResultTime = time
  }

  private[spark] def markFinished(state: TaskState, time: Long): Unit = {
    // finishTime should be set larger than 0, otherwise "finished" below will return false.
    assert(time > 0)
    finishTime = time
    if (state == TaskState.FAILED) {
      failed = true
    } else if (state == TaskState.KILLED) {
      killed = true
    }
  }

  def gettingResult: Boolean = gettingResultTime != 0

  def finished: Boolean = finishTime != 0

  def successful: Boolean = finished && !failed && !killed

  def running: Boolean = !finished

  def status: String = {
    if (running) {
      if (gettingResult) {
        "GET RESULT"
      } else {
        "RUNNING"
      }
    } else if (failed) {
      "FAILED"
    } else if (killed) {
      "KILLED"
    } else if (successful) {
      "SUCCESS"
    } else {
      "UNKNOWN"
    }
  }

  def id: String = s"$index.$attemptNumber"

  def duration: Long = {
    if (!finished) {
      throw new UnsupportedOperationException("duration() called on unfinished task")
    } else {
      finishTime - launchTime
    }
  }

  private[spark] def timeRunning(currentTime: Long): Long = currentTime - launchTime
}
