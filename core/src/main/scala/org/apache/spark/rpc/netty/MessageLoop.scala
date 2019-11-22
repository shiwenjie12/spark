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

package org.apache.spark.rpc.netty

import java.util.concurrent._

import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.EXECUTOR_ID
import org.apache.spark.internal.config.Network._
import org.apache.spark.rpc.{IsolatedRpcEndpoint, RpcEndpoint}
import org.apache.spark.util.ThreadUtils

/**
 * [[Dispatcher]]用于将消息传递到端点的消息循环。
 */
private sealed abstract class MessageLoop(dispatcher: Dispatcher) extends Logging {

  // 带有待处理邮件的收件箱列表，由邮件循环处理。
  private val active = new LinkedBlockingQueue[Inbox]()

  // Message loop task; should be run in all threads of the message loop's pool.
  protected val receiveLoopRunnable = new Runnable() {
    override def run(): Unit = receiveLoop()
  }

  protected val threadpool: ExecutorService

  private var stopped = false

  def post(endpointName: String, message: InboxMessage): Unit

  def unregister(name: String): Unit

  def stop(): Unit = {
    synchronized {
      if (!stopped) {
        setActive(MessageLoop.PoisonPill)
        threadpool.shutdown()
        stopped = true
      }
    }
    threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  protected final def setActive(inbox: Inbox): Unit = active.offer(inbox)

  private def receiveLoop(): Unit = {
    try {
      while (true) {
        try {
          val inbox = active.take()
          if (inbox == MessageLoop.PoisonPill) {
            // 放回PoisonPill，以便其他线程可以看到它。
            setActive(MessageLoop.PoisonPill)
            return
          }
          inbox.process(dispatcher)
        } catch {
          case NonFatal(e) => logError(e.getMessage, e)
        }
      }
    } catch {
      case _: InterruptedException => // exit
        case t: Throwable =>
          try {
            // 重新提交接收任务，以便如果UncaughtExceptionHandler决定不终止JVM，消息传递仍将起作用。
            threadpool.execute(receiveLoopRunnable)
          } finally {
            throw t
          }
    }
  }
}

private object MessageLoop {
  /** 指示消息循环的带毒收件箱应停止处理消息。 */
  val PoisonPill = new Inbox(null, null)
}

/**
 * 使用共享线程池为多个RPC端点提供服务的消息循环。
 */
private class SharedMessageLoop(
    conf: SparkConf,
    dispatcher: Dispatcher,
    numUsableCores: Int)
  extends MessageLoop(dispatcher) {

  private val endpoints = new ConcurrentHashMap[String, Inbox]()

  private def getNumOfThreads(conf: SparkConf): Int = {
    val availableCores =
      if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()

    val modNumThreads = conf.get(RPC_NETTY_DISPATCHER_NUM_THREADS)
      .getOrElse(math.max(2, availableCores))

    conf.get(EXECUTOR_ID).map { id =>
      val role = if (id == SparkContext.DRIVER_IDENTIFIER) "driver" else "executor"
      conf.getInt(s"spark.$role.rpc.netty.dispatcher.numThreads", modNumThreads)
    }.getOrElse(modNumThreads)
  }

  /** Thread pool used for dispatching messages. */
  override protected val threadpool: ThreadPoolExecutor = {
    val numThreads = getNumOfThreads(conf)
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
    for (i <- 0 until numThreads) {
      pool.execute(receiveLoopRunnable)
    }
    pool
  }

  override def post(endpointName: String, message: InboxMessage): Unit = {
    val inbox = endpoints.get(endpointName)
    inbox.post(message)
    setActive(inbox)
  }

  override def unregister(name: String): Unit = {
    val inbox = endpoints.remove(name)
    if (inbox != null) {
      inbox.stop()
      // Mark active to handle the OnStop message.
      setActive(inbox)
    }
  }

  def register(name: String, endpoint: RpcEndpoint): Unit = {
    val inbox = new Inbox(name, endpoint)
    endpoints.put(name, inbox)
    // Mark active to handle the OnStart message.
    setActive(inbox)
  }
}

/**
 * 专用于单个RPC端点的消息循环。
 */
private class DedicatedMessageLoop(
    name: String,
    endpoint: IsolatedRpcEndpoint,
    dispatcher: Dispatcher)
  extends MessageLoop(dispatcher) {

  private val inbox = new Inbox(name, endpoint)

  override protected val threadpool = if (endpoint.threadCount() > 1) {
    ThreadUtils.newDaemonCachedThreadPool(s"dispatcher-$name", endpoint.threadCount())
  } else {
    ThreadUtils.newDaemonSingleThreadExecutor(s"dispatcher-$name")
  }

  (1 to endpoint.threadCount()).foreach { _ =>
    threadpool.submit(receiveLoopRunnable)
  }

  // Mark active to handle the OnStart message.
  setActive(inbox)

  override def post(endpointName: String, message: InboxMessage): Unit = {
    require(endpointName == name)
    inbox.post(message)
    setActive(inbox)
  }

  override def unregister(endpointName: String): Unit = synchronized {
    require(endpointName == name)
    inbox.stop()
    // Mark active to handle the OnStop message.
    setActive(inbox)
    setActive(MessageLoop.PoisonPill)
    threadpool.shutdown()
  }
}
