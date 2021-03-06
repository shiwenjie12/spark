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

package org.apache.spark

import java.util.{Timer, TimerTask}
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.{LiveListenerBus, SparkListener, SparkListenerStageCompleted}

/**
 * 对于每个屏障阶段尝试，在任何时候最多只能激活一个barrier（）调用，
  * 因此我们可以使用（stageId，stageAttemptId）来标识barrier（）调用来自的阶段尝试。
 */
private case class ContextBarrierId(stageId: Int, stageAttemptId: Int) {
  override def toString: String = s"Stage $stageId (Attempt $stageAttemptId)"
}

/**
 * 一个协调器，处理来自BarrierTaskContext的所有全局同步请求。
 * 每个全局同步请求均由BarrierTaskContext.barrier（）生成，并由stageId + stageAttemptId + barrierEpoch标识。
 * 在收到对一组“ barrier（）”调用的所有请求后，答复所有阻塞的全局同步请求。
 * 如果协调器无法在配置的时间内收集足够的全局同步请求，请使所有请求失败并返回带有超时的异常消息。
 */
private[spark] class BarrierCoordinator(
    timeoutInSecs: Long,
    listenerBus: LiveListenerBus,
    override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {

  // TODO SPARK-25030 Create a Timer() in the mainClass submitted to SparkSubmit makes it unable to
  // fetch result, we shall fix the issue.
  private lazy val timer = new Timer("BarrierCoordinator barrier epoch increment timer")

  // 侦听StageCompleted事件，清除相应的ContextBarrierState。
  private val listener = new SparkListener {
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageInfo = stageCompleted.stageInfo
      val barrierId = ContextBarrierId(stageInfo.stageId, stageInfo.attemptNumber)
      // Clear ContextBarrierState from a finished stage attempt.
      cleanupBarrierStage(barrierId)
    }
  }

  // Record all active stage attempts that make barrier() call(s), and the corresponding internal
  // state.
  private val states = new ConcurrentHashMap[ContextBarrierId, ContextBarrierState]

  override def onStart(): Unit = {
    super.onStart()
    listenerBus.addToStatusQueue(listener)
  }

  override def onStop(): Unit = {
    try {
      states.forEachValue(1, clearStateConsumer)
      states.clear()
      listenerBus.removeListener(listener)
    } finally {
      super.onStop()
    }
  }

  /**
   * 提供barrier()调用的当前状态。当新的阶段尝试发出barrier()调用时会创建状态，并在完成阶段进行回收。
   *
   * @param barrierId 进行barrier()调用的barrier阶段的标识符。
   * @param numTasks 屏障阶段的任务数量，该阶段的所有barrier()调用均应收集成功的numTasks请求。
   */
  private class ContextBarrierState(
      val barrierId: ContextBarrierId,
      val numTasks: Int) {

    // There may be multiple barrier() calls from a barrier stage attempt, `barrierEpoch` is used
    // to identify each barrier() call. It shall get increased when a barrier() call succeeds, or
    // reset when a barrier() call fails due to timeout.
    private var barrierEpoch: Int = 0

    // An array of RPCCallContexts for barrier tasks that are waiting for reply of a barrier()
    // call.
    private val requesters: ArrayBuffer[RpcCallContext] = new ArrayBuffer[RpcCallContext](numTasks)

    // A timer task that ensures we may timeout for a barrier() call.
    private var timerTask: TimerTask = null

    // Init a TimerTask for a barrier() call.
    private def initTimerTask(state: ContextBarrierState): Unit = {
      timerTask = new TimerTask {
        override def run(): Unit = state.synchronized {
          // Timeout current barrier() call, fail all the sync requests.
          requesters.foreach(_.sendFailure(new SparkException("The coordinator didn't get all " +
            s"barrier sync requests for barrier epoch $barrierEpoch from $barrierId within " +
            s"$timeoutInSecs second(s).")))
          cleanupBarrierStage(barrierId)
        }
      }
    }

    // Cancel the current active TimerTask and release resources.
    private def cancelTimerTask(): Unit = {
      if (timerTask != null) {
        timerTask.cancel()
        timer.purge()
        timerTask = null
      }
    }

    // 处理全局同步请求。如果在配置的时间内收集了足够多的请求，则barrier()调用成功，否则将使所有未决请求失败。
    def handleRequest(requester: RpcCallContext, request: RequestToSync): Unit = synchronized {
      val taskId = request.taskAttemptId
      val epoch = request.barrierEpoch

      // 需要从BarrierTaskContext正确设置任务数。
      require(request.numTasks == numTasks, s"Number of tasks of $barrierId is " +
        s"${request.numTasks} from Task $taskId, previously it was $numTasks.")

      // 检查障碍任务的纪元是否与当前的barrierEpoch相匹配。
      logInfo(s"Current barrier epoch for $barrierId is $barrierEpoch.")
      if (epoch != barrierEpoch) {
        requester.sendFailure(new SparkException(s"The request to sync of $barrierId with " +
          s"barrier epoch $barrierEpoch has already finished. Maybe task $taskId is not " +
          "properly killed."))
      } else {
        // If this is the first sync message received for a barrier() call, start timer to ensure
        // we may timeout for the sync.
        if (requesters.isEmpty) {
          initTimerTask(this)
          timer.schedule(timerTask, timeoutInSecs * 1000)
        }
        // Add the requester to array of RPCCallContexts pending for reply.
        requesters += requester
        logInfo(s"Barrier sync epoch $barrierEpoch from $barrierId received update from Task " +
          s"$taskId, current progress: ${requesters.size}/$numTasks.")
        if (maybeFinishAllRequesters(requesters, numTasks)) {
          // Finished current barrier() call successfully, clean up ContextBarrierState and
          // increase the barrier epoch.
          logInfo(s"Barrier sync epoch $barrierEpoch from $barrierId received all updates from " +
            s"tasks, finished successfully.")
          barrierEpoch += 1
          requesters.clear()
          cancelTimerTask()
        }
      }
    }

    // Finish all the blocking barrier sync requests from a stage attempt successfully if we
    // have received all the sync requests.
    private def maybeFinishAllRequesters(
        requesters: ArrayBuffer[RpcCallContext],
        numTasks: Int): Boolean = {
      if (requesters.size == numTasks) {
        requesters.foreach(_.reply(()))
        true
      } else {
        false
      }
    }

    // Cleanup the internal state of a barrier stage attempt.
    def clear(): Unit = synchronized {
      // The global sync fails so the stage is expected to retry another attempt, all sync
      // messages come from current stage attempt shall fail.
      barrierEpoch = -1
      requesters.clear()
      cancelTimerTask()
    }
  }

  // Clean up the [[ContextBarrierState]] that correspond to a specific stage attempt.
  private def cleanupBarrierStage(barrierId: ContextBarrierId): Unit = {
    val barrierState = states.remove(barrierId)
    if (barrierState != null) {
      barrierState.clear()
    }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case request @ RequestToSync(numTasks, stageId, stageAttemptId, _, _) =>
      // Get or init the ContextBarrierState correspond to the stage attempt.
      val barrierId = ContextBarrierId(stageId, stageAttemptId)
      states.computeIfAbsent(barrierId,
        (key: ContextBarrierId) => new ContextBarrierState(key, numTasks))
      val barrierState = states.get(barrierId)

      barrierState.handleRequest(context, request)
  }

  private val clearStateConsumer = new Consumer[ContextBarrierState] {
    override def accept(state: ContextBarrierState) = state.clear()
  }
}

private[spark] sealed trait BarrierCoordinatorMessage extends Serializable

/**
 * 来自BarrierTaskContext的全局同步请求消息，通过“ barrier()”调用。每个请求都由stageId + stageAttemptId + barrierEpoch标识。
 *
 * @param numTasks BarrierCoordinator应收到的全局同步请求数
 * @param stageId ID of current stage
 * @param stageAttemptId ID of current stage attempt
 * @param taskAttemptId Unique ID of current task
 * @param barrierEpoch ID of the `barrier()` call, a task may consist multiple `barrier()` calls.
 */
private[spark] case class RequestToSync(
    numTasks: Int,
    stageId: Int,
    stageAttemptId: Int,
    taskAttemptId: Long,
    barrierEpoch: Int) extends BarrierCoordinatorMessage
