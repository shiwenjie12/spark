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

package org.apache.spark.util

import java.io.File
import java.util.PriorityQueue

import scala.util.Try

import org.apache.hadoop.fs.FileSystem

import org.apache.spark.internal.Logging

/**
 * Spark使用的各种实用程序方法。
 */
private[spark] object ShutdownHookManager extends Logging {
  val DEFAULT_SHUTDOWN_PRIORITY = 100

  /**
   * SparkContext实例的关闭优先级。这低于默认优先级，因此默认情况下，在关闭上下文之前运行挂接。
   */
  val SPARK_CONTEXT_SHUTDOWN_PRIORITY = 50

  /**
   * 临时目录的关闭优先级必须低于SparkContext的关闭优先级。
   * 否则，在Spark作业运行时清理临时目录可能会在关机时引发意外错误。
   */
  val TEMP_DIR_SHUTDOWN_PRIORITY = 25

  private lazy val shutdownHooks = {
    val manager = new SparkShutdownHookManager()
    manager.install()
    manager
  }

  private val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()

  // Add a shutdown hook to delete the temp dirs when the JVM exits
  logDebug("Adding shutdown hook") // force eager creation of logger
  addShutdownHook(TEMP_DIR_SHUTDOWN_PRIORITY) { () =>
    logInfo("Shutdown hook called")
    // 我们需要具体化要删除的路径，因为deleteRecursive会在遍历它时从shutdownDeletePaths中删除项目。
    shutdownDeletePaths.toArray.foreach { dirPath =>
      try {
        logInfo("Deleting directory " + dirPath)
        Utils.deleteRecursively(new File(dirPath))
      } catch {
        case e: Exception => logError(s"Exception while deleting Spark temp dir: $dirPath", e)
      }
    }
  }

  // Register the path to be deleted via shutdown hook
  def registerShutdownDeleteDir(file: File): Unit = {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += absolutePath
    }
  }

  // Remove the path to be deleted via shutdown hook
  def removeShutdownDeleteDir(file: File): Unit = {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.remove(absolutePath)
    }
  }

  // Is the path already registered to be deleted via a shutdown hook ?
  def hasShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.contains(absolutePath)
    }
  }

  // Note: if file is child of some registered path, while not equal to it, then return true;
  // else false. This is to ensure that two shutdown hooks do not try to delete each others
  // paths - resulting in IOException and incomplete cleanup.
  def hasRootAsShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    val retval = shutdownDeletePaths.synchronized {
      shutdownDeletePaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
    if (retval) {
      logInfo("path = " + file + ", already present as root for deletion.")
    }
    retval
  }

  /**
   * Detect whether this thread might be executing a shutdown hook. Will always return true if
   * the current thread is a running a shutdown hook but may spuriously return true otherwise (e.g.
   * if System.exit was just called by a concurrent thread).
   *
   * Currently, this detects whether the JVM is shutting down by Runtime#addShutdownHook throwing
   * an IllegalStateException.
   */
  def inShutdown(): Boolean = {
    try {
      val hook = new Thread {
        override def run(): Unit = {}
      }
      // scalastyle:off runtimeaddshutdownhook
      Runtime.getRuntime.addShutdownHook(hook)
      // scalastyle:on runtimeaddshutdownhook
      Runtime.getRuntime.removeShutdownHook(hook)
    } catch {
      case ise: IllegalStateException => return true
    }
    false
  }

  /**
   * Adds a shutdown hook with default priority.
   *
   * @param hook The code to run during shutdown.
   * @return A handle that can be used to unregister the shutdown hook.
   */
  def addShutdownHook(hook: () => Unit): AnyRef = {
    addShutdownHook(DEFAULT_SHUTDOWN_PRIORITY)(hook)
  }

  /**
   * 添加具有给定优先级的关机钩子。具有较高优先级值的挂钩首先运行。
   *
   * @param hook The code to run during shutdown.
   * @return A handle that can be used to unregister the shutdown hook.
   */
  def addShutdownHook(priority: Int)(hook: () => Unit): AnyRef = {
    shutdownHooks.add(priority, hook)
  }

  /**
   * Remove a previously installed shutdown hook.
   *
   * @param ref A handle returned by `addShutdownHook`.
   * @return Whether the hook was removed.
   */
  def removeShutdownHook(ref: AnyRef): Boolean = {
    shutdownHooks.remove(ref)
  }

}

private [util] class SparkShutdownHookManager {

  private val hooks = new PriorityQueue[SparkShutdownHook]()
  @volatile private var shuttingDown = false

  /**
   * 安装一个在关机时运行的钩子，并依次运行所有已注册的钩子。
   */
  def install(): Unit = {
    val hookTask = new Runnable() {
      override def run(): Unit = runAll()
    }
    org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(
      hookTask, FileSystem.SHUTDOWN_HOOK_PRIORITY + 30)
  }

  def runAll(): Unit = {
    shuttingDown = true
    var nextHook: SparkShutdownHook = null
    while ({ nextHook = hooks.synchronized { hooks.poll() }; nextHook != null }) {
      Try(Utils.logUncaughtExceptions(nextHook.run()))
    }
  }

  def add(priority: Int, hook: () => Unit): AnyRef = {
    hooks.synchronized {
      if (shuttingDown) {
        throw new IllegalStateException("Shutdown hooks cannot be modified during shutdown.")
      }
      val hookRef = new SparkShutdownHook(priority, hook)
      hooks.add(hookRef)
      hookRef
    }
  }

  def remove(ref: AnyRef): Boolean = {
    hooks.synchronized { hooks.remove(ref) }
  }

}

private class SparkShutdownHook(private val priority: Int, hook: () => Unit)
  extends Comparable[SparkShutdownHook] {

  override def compareTo(other: SparkShutdownHook): Int = {
    other.priority - priority
  }

  def run(): Unit = hook()

}
