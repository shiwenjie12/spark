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

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.util.CallSite

/**
 * 阶段是一组并行任务，所有这些任务都计算需要作为Spark作业的一部分运行的相同功能，
 * 其中所有任务都具有相同的随机依赖关系。调度程序运行的每个DAG任务在发生混洗的边界处分为多个阶段，
 * 然后DAGScheduler以拓扑顺序运行这些阶段。
 *
 * 每个阶段可以是随机播放映射阶段(在这种情况下其任务的结果输入到其他阶段)，也可以是结果阶段(在这种情况下其任务直接计算Spark动作)\
 * (例如count()，save()等等)，方法是在RDD上运行函数。对于随机映射阶段，我们还跟踪每个输出分区所在的节点。
 *
 * 每个阶段还具有一个firstJobId，用于标识首先提交该阶段的作业。
 * 使用FIFO调度时，这允许首先计算较早作业的阶段，或者在出现故障时更快地恢复。
 *
 * 最后，由于故障恢复，可以多次尝试重新执行一个阶段。
 * 在这种情况下，Stage对象将跟踪多个StageInfo对象以传递给侦听器或Web UI。最新的将可以通过latestInfo访问。
 *
 * @param id 唯一的阶段ID
 * @param rdd RDD that this stage runs on: for a shuffle map stage, it's the RDD we run map tasks
 *   on, while for a result stage, it's the target RDD that we ran an action on
 * @param numTasks Total number of tasks in stage; result stages in particular may not need to
 *   compute all partitions, e.g. for first(), lookup(), and take().
 * @param parents List of stages that this stage depends on (through shuffle dependencies).
 * @param firstJobId ID of the first job this stage was part of, for FIFO scheduling.
 * @param callSite Location in the user program associated with this stage: either where the target
 *   RDD was created, for a shuffle map stage, or where the action for a result stage was called.
 */
private[scheduler] abstract class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val parents: List[Stage],
    val firstJobId: Int,
    val callSite: CallSite)
  extends Logging {

  val numPartitions = rdd.partitions.length

  /** 此阶段所属的一组作业。 */
  val jobIds = new HashSet[Int]

  /** 该阶段下一次新尝试使用的ID。 */
  private var nextAttemptId: Int = 0

  val name: String = callSite.shortForm
  val details: String = callSite.longForm

  /**
   * Pointer to the [[StageInfo]] object for the most recent attempt. This needs to be initialized
   * here, before any attempts have actually been created, because the DAGScheduler uses this
   * StageInfo to tell SparkListeners when a job starts (which happens before any stage attempts
   * have been created).
   */
  private var _latestInfo: StageInfo = StageInfo.fromStage(this, nextAttemptId)

  /**
   * Set of stage attempt IDs that have failed. We keep track of these failures in order to avoid
   * endless retries if a stage keeps failing.
   * We keep track of each attempt ID that has failed to avoid recording duplicate failures if
   * multiple tasks from the same stage attempt fail (SPARK-5945).
   */
  val failedAttemptIds = new HashSet[Int]

  private[scheduler] def clearFailures() : Unit = {
    failedAttemptIds.clear()
  }

  /** 通过创建具有新尝试ID的新StageInfo来为此阶段创建新尝试。 */
  def makeNewStageAttempt(
      numPartitionsToCompute: Int,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = {
    val metrics = new TaskMetrics
    metrics.register(rdd.sparkContext)
    _latestInfo = StageInfo.fromStage(
      this, nextAttemptId, Some(numPartitionsToCompute), metrics, taskLocalityPreferences)
    nextAttemptId += 1
  }

  /** Returns the StageInfo for the most recent attempt for this stage. */
  def latestInfo: StageInfo = _latestInfo

  override final def hashCode(): Int = id

  override final def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }

  /** 返回缺少的分区ID的序列（即需要计算）。 */
  def findMissingPartitions(): Seq[Int]

  def isIndeterminate: Boolean = {
    rdd.outputDeterministicLevel == DeterministicLevel.INDETERMINATE
  }
}
