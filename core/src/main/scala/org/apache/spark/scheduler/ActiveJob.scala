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

import org.apache.spark.util.CallSite

/**
 * A running job in the DAGScheduler. Jobs can be of two types: a result job, which computes a
 * ResultStage to execute an action, or a map-stage job, which computes the map outputs for a
 * ShuffleMapStage before any downstream stages are submitted. The latter is used for adaptive
 * query planning, to look at map output statistics before submitting later stages. We distinguish
 * between these two types of jobs using the finalStage field of this class.
 * DAGScheduler中的一项正在运行的工作。作业可以有两种类型：结果作业，它计算要执行操作的ResultStage；
 * 或者地图阶段作业，它在提交任何下游阶段之前计算ShuffleMapStage的地图输出。
 * 后者用于自适应查询计划，在提交后续阶段之前先查看地图输出统计信息。
 * 我们使用此类的finalStage字段来区分这两种类型的作业。
 *
 * Jobs are only tracked for "leaf" stages that clients directly submitted, through DAGScheduler's
 * submitJob or submitMapStage methods. However, either type of job may cause the execution of
 * other earlier stages (for RDDs in the DAG it depends on), and multiple jobs may share some of
 * these previous stages. These dependencies are managed inside DAGScheduler.
 * 通过DAGScheduler的SubmitJob或SubmitMapStage方法，仅跟踪客户直接提交的“leaf”阶段的作业。
 * 但是，任何一种作业都可能导致执行其他较早阶段（对于它依赖于DAG中的RDD），并且多个作业可能共享其中一些先前阶段。
 * 这些依赖关系在DAGScheduler中管理。
 *
 * @param jobId A unique ID for this job.
 * @param finalStage The stage that this job computes (either a ResultStage for an action or a
 *   ShuffleMapStage for submitMapStage).
 * @param callSite Where this job was initiated in the user's program (shown on UI).
 * @param listener A listener to notify if tasks in this job finish or the job fails.
 * @param properties Scheduling properties attached to the job, such as fair scheduler pool name.
 */
private[spark] class ActiveJob(
    val jobId: Int,
    val finalStage: Stage,
    val callSite: CallSite,
    val listener: JobListener,
    val properties: Properties) {

  /**
   * 我们需要为此工作计算的分区数。请注意，对于诸如first()和lookup()之类的操作，结果阶段可能不需要计算其目标RDD中的所有分区。
   */
  val numPartitions = finalStage match {
    case r: ResultStage => r.partitions.length
    case m: ShuffleMapStage => m.rdd.partitions.length
  }

  /** 该阶段的哪些分区已完成 */
  val finished = Array.fill[Boolean](numPartitions)(false)

  var numFinished = 0

  /** 在此阶段重置所有分区的状态，以便将其标记为未完成。 */
  def resetAllPartitions(): Unit = {
    (0 until numPartitions).foreach(finished.update(_, false))
    numFinished = 0
  }
}
