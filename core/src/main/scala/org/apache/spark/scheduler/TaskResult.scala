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

import java.io._
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.BlockId
import org.apache.spark.util.{AccumulatorV2, Utils}

// 任务结果。还包含对累加器变量和执行器指标峰值的更新。
private[spark] sealed trait TaskResult[T]

/** 对已存储在工作程序的BlockManager中的DirectTaskResult的引用。 */
private[spark] case class IndirectTaskResult[T](blockId: BlockId, size: Int)
  extends TaskResult[T] with Serializable

/** 一个TaskResult，其中包含任务的返回值，累加器更新和指标峰值。 */
private[spark] class DirectTaskResult[T](
    var valueBytes: ByteBuffer,
    var accumUpdates: Seq[AccumulatorV2[_, _]],
    var metricPeaks: Array[Long])
  extends TaskResult[T] with Externalizable {

  private var valueObjectDeserialized = false
  private var valueObject: T = _

  def this() = this(null.asInstanceOf[ByteBuffer], null,
    new Array[Long](ExecutorMetricType.numMetrics))

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeInt(valueBytes.remaining)
    Utils.writeByteBuffer(valueBytes, out)
    out.writeInt(accumUpdates.size)
    accumUpdates.foreach(out.writeObject)
    out.writeInt(metricPeaks.length)
    metricPeaks.foreach(out.writeLong)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    val blen = in.readInt()
    val byteVal = new Array[Byte](blen)
    in.readFully(byteVal)
    valueBytes = ByteBuffer.wrap(byteVal)

    val numUpdates = in.readInt
    if (numUpdates == 0) {
      accumUpdates = Seq.empty
    } else {
      val _accumUpdates = new ArrayBuffer[AccumulatorV2[_, _]]
      for (i <- 0 until numUpdates) {
        _accumUpdates += in.readObject.asInstanceOf[AccumulatorV2[_, _]]
      }
      accumUpdates = _accumUpdates
    }

    val numMetrics = in.readInt
    if (numMetrics == 0) {
      metricPeaks = Array.empty
    } else {
      metricPeaks = new Array[Long](numMetrics)
      (0 until numMetrics).foreach { i =>
        metricPeaks(i) = in.readLong
      }
    }
    valueObjectDeserialized = false
  }

  /**
   * 第一次调用value()时，需要从valueBytes中反序列化valueObject。
   * 大型实例可能要花费数十秒。因此，在第一次调用“值”时，调用者应避免阻塞其他线程。
   *
   * 第一次之后，value（）很简单，只返回反序列化的valueObject。
   */
  def value(resultSer: SerializerInstance = null): T = {
    if (valueObjectDeserialized) {
      valueObject
    } else {
      // This should not run when holding a lock because it may cost dozens of seconds for a large
      // value
      val ser = if (resultSer == null) SparkEnv.get.serializer.newInstance() else resultSer
      valueObject = ser.deserialize(valueBytes)
      valueObjectDeserialized = true
      valueObject
    }
  }
}
