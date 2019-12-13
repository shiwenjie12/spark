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

import java.util.Properties

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, CallSite}

/**
 * DAGScheduler可以处理的事件类型。
 * DAGScheduler使用事件队列架构，其中任何线程都可以发布事件（例如任务完成或正在提交新作业），
 * 但是只有一个“逻辑”线程可以读取这些事件并做出决定。
 * 这大大简化了同步。
 */
private[scheduler] sealed trait DAGSchedulerEvent

/** 已针对目标RDD提交了一项result-yielding的工作 */
private[scheduler] case class JobSubmitted(
    jobId: Int,
    finalRDD: RDD[_],
    func: (TaskContext, Iterator[_]) => _,
    partitions: Array[Int],
    callSite: CallSite,
    listener: JobListener,
    properties: Properties = null)
  extends DAGSchedulerEvent

/** A map stage as submitted to run as a separate job */
private[scheduler] case class MapStageSubmitted(
  jobId: Int,
  dependency: ShuffleDependency[_, _, _],
  callSite: CallSite,
  listener: JobListener,
  properties: Properties = null)
  extends DAGSchedulerEvent

private[scheduler] case class StageCancelled(
    stageId: Int,
    reason: Option[String])
  extends DAGSchedulerEvent

private[scheduler] case class JobCancelled(
    jobId: Int,
    reason: Option[String])
  extends DAGSchedulerEvent

private[scheduler] case class JobGroupCancelled(groupId: String) extends DAGSchedulerEvent

private[scheduler] case object AllJobsCancelled extends DAGSchedulerEvent

private[scheduler]
case class BeginEvent(task: Task[_], taskInfo: TaskInfo) extends DAGSchedulerEvent

private[scheduler]
case class GettingResultEvent(taskInfo: TaskInfo) extends DAGSchedulerEvent

// 完成时间
private[scheduler] case class CompletionEvent(
    task: Task[_],
    reason: TaskEndReason,
    result: Any,
    accumUpdates: Seq[AccumulatorV2[_, _]],
    metricPeaks: Array[Long],
    taskInfo: TaskInfo)
  extends DAGSchedulerEvent

private[scheduler] case class ExecutorAdded(execId: String, host: String) extends DAGSchedulerEvent

private[scheduler] case class ExecutorLost(execId: String, reason: ExecutorLossReason)
  extends DAGSchedulerEvent

private[scheduler] case class WorkerRemoved(workerId: String, host: String, message: String)
  extends DAGSchedulerEvent

// 任务集失败
private[scheduler]
case class TaskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable])
  extends DAGSchedulerEvent

private[scheduler] case object ResubmitFailedStages extends DAGSchedulerEvent

private[scheduler]
case class SpeculativeTaskSubmitted(task: Task[_]) extends DAGSchedulerEvent

