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

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * 创建一个心跳线程，该线程将以intervalMs的间隔调用指定的reportHeartbeat函数。
 *
 * @param reportHeartbeat the heartbeat reporting function to call.
 * @param name the thread name for the heartbeater.
 * @param intervalMs the interval between heartbeats.
 */
private[spark] class Heartbeater(
    reportHeartbeat: () => Unit,
    name: String,
    intervalMs: Long) extends Logging {
  // Executor for the heartbeat task
  private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor(name)

  /** Schedules a task to report a heartbeat. */
  def start(): Unit = {
    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartbeat())
    }
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }

  /** Stops the heartbeat thread. */
  def stop(): Unit = {
    heartbeater.shutdown()
    heartbeater.awaitTermination(10, TimeUnit.SECONDS)
  }
}
