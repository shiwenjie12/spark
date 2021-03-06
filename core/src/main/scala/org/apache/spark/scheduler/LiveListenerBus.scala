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

import java.util.{List => JList}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.DynamicVariable

import com.codahale.metrics.{Counter, MetricRegistry, Timer}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source

/**
 * 异步将SparkListenerEvents传递给已注册的SparkListeners。
 *
 * 在调用“ start()”之前，所有已发布的事件仅被缓冲。
  * 仅在此侦听器总线启动之后，事件才会实际传播到所有连接的侦听器。
  * 调用`stop()`时，此侦听器总线将停止，并且停止后它将丢弃其他事件。
 */
private[spark] class LiveListenerBus(conf: SparkConf) {

  import LiveListenerBus._

  private var sparkContext: SparkContext = _

  private[spark] val metrics = new LiveListenerBusMetrics(conf)

  // Indicate if `start()` is called
  private val started = new AtomicBoolean(false)
  // Indicate if `stop()` is called
  private val stopped = new AtomicBoolean(false)

  /** A counter for dropped events. It will be reset every time we log it. */
  private val droppedEventsCounter = new AtomicLong(0L)

  /** When `droppedEventsCounter` was logged last time in milliseconds. */
  @volatile private var lastReportTimestamp = 0L

  private val queues = new CopyOnWriteArrayList[AsyncEventQueue]()

  // Visible for testing.
  @volatile private[scheduler] var queuedEvents = new mutable.ListBuffer[SparkListenerEvent]()

  /** Add a listener to queue shared by all non-internal listeners. */
  def addToSharedQueue(listener: SparkListenerInterface): Unit = {
    addToQueue(listener, SHARED_QUEUE)
  }

  /** Add a listener to the executor management queue. */
  def addToManagementQueue(listener: SparkListenerInterface): Unit = {
    addToQueue(listener, EXECUTOR_MANAGEMENT_QUEUE)
  }

  /** 将侦听器添加到应用程序状态队列。 */
  def addToStatusQueue(listener: SparkListenerInterface): Unit = {
    addToQueue(listener, APP_STATUS_QUEUE)
  }

  /** Add a listener to the event log queue. */
  def addToEventLogQueue(listener: SparkListenerInterface): Unit = {
    addToQueue(listener, EVENT_LOG_QUEUE)
  }

  /**
   * Add a listener to a specific queue, creating a new queue if needed. Queues are independent
   * of each other (each one uses a separate thread for delivering events), allowing slower
   * listeners to be somewhat isolated from others.
   */
  private[spark] def addToQueue(
      listener: SparkListenerInterface,
      queue: String): Unit = synchronized {
    if (stopped.get()) {
      throw new IllegalStateException("LiveListenerBus is stopped.")
    }

    queues.asScala.find(_.name == queue) match {
      case Some(queue) =>
        queue.addListener(listener)

      case None =>
        val newQueue = new AsyncEventQueue(queue, conf, metrics, this)
        newQueue.addListener(listener)
        if (started.get()) {
          newQueue.start(sparkContext)
        }
        queues.add(newQueue)
    }
  }

  def removeListener(listener: SparkListenerInterface): Unit = synchronized {
    // Remove listener from all queues it was added to, and stop queues that have become empty.
    queues.asScala
      .filter { queue =>
        queue.removeListener(listener)
        queue.listeners.isEmpty()
      }
      .foreach { toRemove =>
        if (started.get() && !stopped.get()) {
          toRemove.stop()
        }
        queues.remove(toRemove)
      }
  }

  /** 将事件发布到所有队列。 */
  def post(event: SparkListenerEvent): Unit = {
    if (stopped.get()) {
      return
    }

    metrics.numEventsPosted.inc()

    // 如果事件缓冲区为null，则表示总线已启动，我们可以避免同步并将事件直接发布到队列中。
    // 这应该是总线生命周期中最常见的情况。
    if (queuedEvents == null) {
      postToQueues(event)
      return
    }

    // 否则，需要进行同步以检查总线是否已启动，以确保调用start（）的线程接收到新事件。
    synchronized {
      if (!started.get()) {
        queuedEvents += event
        return
      }
    }

    // 如果进行上述检查时总线已经启动，则直接将其发布到队列中。
    postToQueues(event)
  }

  private def postToQueues(event: SparkListenerEvent): Unit = {
    val it = queues.iterator()
    while (it.hasNext()) {
      it.next().post(event)
    }
  }

  /**
   * 开始向附加的侦听器发送事件。
   *
   * 它首先发送所有在此侦听器总线启动之前发布的缓冲事件，然后在侦听器总线仍在运行时异步侦听任何其他事件。
   * 这仅应调用一次。
   *
   * @param sc Used to stop the SparkContext in case the listener thread dies.
   */
  def start(sc: SparkContext, metricsSystem: MetricsSystem): Unit = synchronized {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("LiveListenerBus already started.")
    }

    this.sparkContext = sc
    queues.asScala.foreach { q =>
      q.start(sc)
      queuedEvents.foreach(q.post)
    }
    queuedEvents = null
    metricsSystem.registerSource(metrics)
  }

  /**
   * For testing only. Wait until there are no more events in the queue, or until the default
   * wait time has elapsed. Throw `TimeoutException` if the specified time elapsed before the queue
   * emptied.
   * Exposed for testing.
   */
  @throws(classOf[TimeoutException])
  private[spark] def waitUntilEmpty(): Unit = {
    waitUntilEmpty(TimeUnit.SECONDS.toMillis(10))
  }

  /**
   * For testing only. Wait until there are no more events in the queue, or until the specified
   * time has elapsed. Throw `TimeoutException` if the specified time elapsed before the queue
   * emptied.
   * Exposed for testing.
   */
  @throws(classOf[TimeoutException])
  def waitUntilEmpty(timeoutMillis: Long): Unit = {
    val deadline = System.currentTimeMillis + timeoutMillis
    queues.asScala.foreach { queue =>
      if (!queue.waitUntilEmpty(deadline)) {
        throw new TimeoutException(s"The event queue is not empty after $timeoutMillis ms.")
      }
    }
  }

  /**
   * Stop the listener bus. It will wait until the queued events have been processed, but drop the
   * new events after stopping.
   */
  def stop(): Unit = {
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop bus that has not yet started!")
    }

    if (!stopped.compareAndSet(false, true)) {
      return
    }

    synchronized {
      queues.asScala.foreach(_.stop())
      queues.clear()
    }
  }

  // For testing only.
  private[spark] def findListenersByClass[T <: SparkListenerInterface : ClassTag](): Seq[T] = {
    queues.asScala.flatMap { queue => queue.findListenersByClass[T]() }
  }

  // For testing only.
  private[spark] def listeners: JList[SparkListenerInterface] = {
    queues.asScala.flatMap(_.listeners.asScala).asJava
  }

  // For testing only.
  private[scheduler] def activeQueues(): Set[String] = {
    queues.asScala.map(_.name).toSet
  }

  // For testing only.
  private[scheduler] def getQueueCapacity(name: String): Option[Int] = {
    queues.asScala.find(_.name == name).map(_.capacity)
  }
}

private[spark] object LiveListenerBus {
  // Allows for Context to check whether stop() call is made within listener thread
  val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)

  private[scheduler] val SHARED_QUEUE = "shared"

  private[scheduler] val APP_STATUS_QUEUE = "appStatus"

  private[scheduler] val EXECUTOR_MANAGEMENT_QUEUE = "executorManagement"

  private[scheduler] val EVENT_LOG_QUEUE = "eventLog"
}

private[spark] class LiveListenerBusMetrics(conf: SparkConf)
  extends Source with Logging {

  override val sourceName: String = "LiveListenerBus"
  override val metricRegistry: MetricRegistry = new MetricRegistry

  /**
   * The total number of events posted to the LiveListenerBus. This is a count of the total number
   * of events which have been produced by the application and sent to the listener bus, NOT a
   * count of the number of events which have been processed and delivered to listeners (or dropped
   * without being delivered).
   */
  val numEventsPosted: Counter = metricRegistry.counter(MetricRegistry.name("numEventsPosted"))

  // Guarded by synchronization.
  private val perListenerClassTimers = mutable.Map[String, Timer]()

  /**
   * Returns a timer tracking the processing time of the given listener class.
   * events processed by that listener. This method is thread-safe.
   */
  def getTimerForListenerClass(cls: Class[_ <: SparkListenerInterface]): Option[Timer] = {
    synchronized {
      val className = cls.getName
      val maxTimed = conf.get(LISTENER_BUS_METRICS_MAX_LISTENER_CLASSES_TIMED)
      perListenerClassTimers.get(className).orElse {
        if (perListenerClassTimers.size == maxTimed) {
          logError(s"Not measuring processing time for listener class $className because a " +
            s"maximum of $maxTimed listener classes are already timed.")
          None
        } else {
          perListenerClassTimers(className) =
            metricRegistry.timer(MetricRegistry.name("listenerProcessingTime", className))
          perListenerClassTimers.get(className)
        }
      }
    }
  }

}
