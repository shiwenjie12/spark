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

package org.apache.spark.ui

import java.util.{Timer, TimerTask}

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.UI._
import org.apache.spark.status.api.v1.StageData

/**
 * ConsoleProgressBar在控制台的下一行显示阶段的进度。
 * 它会定期从应用程序状态存储中轮询活动阶段的状态，并且在阶段运行了至少500毫秒后才会显示进度条。
 * 如果多个阶段同时运行，则它们的状态将合并在一起，显示在一行中。
 */
private[spark] class ConsoleProgressBar(sc: SparkContext) extends Logging {
  // Carriage return
  private val CR = '\r'
  // Update period of progress bar, in milliseconds
  private val updatePeriodMSec = sc.getConf.get(UI_CONSOLE_PROGRESS_UPDATE_INTERVAL)
  // Delay to show up a progress bar, in milliseconds
  private val firstDelayMSec = 500L

  // The width of terminal
  private val TerminalWidth = sys.env.getOrElse("COLUMNS", "80").toInt

  private var lastFinishTime = 0L
  private var lastUpdateTime = 0L
  private var lastProgressBar = ""

  // Schedule a refresh thread to run periodically
  private val timer = new Timer("refresh progress", true)
  timer.schedule(new TimerTask{
    override def run(): Unit = {
      refresh()
    }
  }, firstDelayMSec, updatePeriodMSec)

  /**
   * 尝试在每个周期刷新进度条
   */
  private def refresh(): Unit = synchronized {
    val now = System.currentTimeMillis()
    if (now - lastFinishTime < firstDelayMSec) {
      return
    }
    val stages = sc.statusStore.activeStages()
      .filter { s => now - s.submissionTime.get.getTime() > firstDelayMSec }
    if (stages.length > 0) {
      show(now, stages.take(3))  // display at most 3 stages in same time
    }
  }

  /**
   * Show progress bar in console. The progress bar is displayed in the next line
   * after your last output, keeps overwriting itself to hold in one line. The logging will follow
   * the progress bar, then progress bar will be showed in next line without overwrite logs.
   */
  private def show(now: Long, stages: Seq[StageData]): Unit = {
    val width = TerminalWidth / stages.size
    val bar = stages.map { s =>
      val total = s.numTasks
      val header = s"[Stage ${s.stageId}:"
      val tailer = s"(${s.numCompleteTasks} + ${s.numActiveTasks}) / $total]"
      val w = width - header.length - tailer.length
      val bar = if (w > 0) {
        val percent = w * s.numCompleteTasks / total
        (0 until w).map { i =>
          if (i < percent) "=" else if (i == percent) ">" else " "
        }.mkString("")
      } else {
        ""
      }
      header + bar + tailer
    }.mkString("")

    // only refresh if it's changed OR after 1 minute (or the ssh connection will be closed
    // after idle some time)
    if (bar != lastProgressBar || now - lastUpdateTime > 60 * 1000L) {
      System.err.print(CR + bar)
      lastUpdateTime = now
    }
    lastProgressBar = bar
  }

  /**
   * Clear the progress bar if showed.
   */
  private def clear(): Unit = {
    if (!lastProgressBar.isEmpty) {
      System.err.printf(CR + " " * TerminalWidth + CR)
      lastProgressBar = ""
    }
  }

  /**
   * 将所有阶段标记为已完成，清除进度条（如果显示），则进度将不会与作业输出交织在一起。
   */
  def finishAll(): Unit = synchronized {
    clear()
    lastFinishTime = System.currentTimeMillis()
  }

  /**
   * Tear down the timer thread.  The timer thread is a GC root, and it retains the entire
   * SparkContext if it's not terminated.
   */
  def stop(): Unit = timer.cancel()
}
