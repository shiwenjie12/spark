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

import scala.collection.mutable.HashSet

import org.apache.spark.{MapOutputTrackerMaster, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * ShuffleMapStages是执行DAG中的中间阶段，可为随机产生数据。
 * 它们在每个随机操作之前发生，并且可能在此之前包含多个流水线操作（例如，映射和过滤器）。
 * 在执行时，它们保存地图输出文件，以后可以通过reduce任务获取它们。
 * “ shuffleDep”字段描述了每个阶段所属的洗牌，
 * “ outputLocs”和“ numAvailableOutputs”之类的变量跟踪准备好多少地图输出。
 *
 * ShuffleMapStages也可以作为作业通过DAGScheduler.submitMapStage独立提交。
 * 对于这样的阶段，在`mapStageJobs`中跟踪提交它们的ActiveJobs。请注意，可能有多个ActiveJob尝试计算相同的随机映射阶段。
 */
private[spark] class ShuffleMapStage(
    id: Int,
    rdd: RDD[_],
    numTasks: Int,
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite,
    val shuffleDep: ShuffleDependency[_, _, _],
    mapOutputTrackerMaster: MapOutputTrackerMaster)
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite) {

  private[this] var _mapStageJobs: List[ActiveJob] = Nil

  /**
   * 尚未计算的分区，或已经丢失的执行器上已计算的分区，因此应重新计算。
   * DAGScheduler使用此变量来确定阶段何时完成。
   * 在该阶段的活动尝试中或在此阶段的较早尝试中，如果任务成功，都可能导致分区ID从PendingPartitions中删除。
   * 结果，此变量可能与该阶段的活动尝试与TaskSetManager中的待处理任务不一致
   * (此处存储的分区将始终是TaskSetManager认为待处理的分区的子集)。
   */
  val pendingPartitions = new HashSet[Int]

  override def toString: String = "ShuffleMapStage " + id

  /**
   * Returns the list of active jobs,
   * i.e. map-stage jobs that were submitted to execute this stage independently (if any).
   */
  def mapStageJobs: Seq[ActiveJob] = _mapStageJobs

  /** Adds the job to the active job list. */
  def addActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = job :: _mapStageJobs
  }

  /** Removes the job from the active job list. */
  def removeActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = _mapStageJobs.filter(_ != job)
  }

  /**
   * Number of partitions that have shuffle outputs.
   * When this reaches [[numPartitions]], this map stage is ready.
   */
  def numAvailableOutputs: Int = mapOutputTrackerMaster.getNumAvailableOutputs(shuffleDep.shuffleId)

  /**
   * Returns true if the map stage is ready, i.e. all partitions have shuffle outputs.
   */
  def isAvailable: Boolean = numAvailableOutputs == numPartitions

  /** 返回缺少的分区ID的序列(即需要计算)。 */
  override def findMissingPartitions(): Seq[Int] = {
    mapOutputTrackerMaster
      .findMissingPartitions(shuffleDep.shuffleId)
      .getOrElse(0 until numPartitions)
  }
}
