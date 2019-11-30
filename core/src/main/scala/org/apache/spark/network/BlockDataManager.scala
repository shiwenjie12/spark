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

package org.apache.spark.network

import scala.reflect.ClassTag

import org.apache.spark.TaskContext
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.storage.{BlockId, StorageLevel}

private[spark]
trait BlockDataManager {

  /**
   * 获取本地块数据的接口。如果找不到或无法成功读取该块，则引发异常。
   */
  def getBlockData(blockId: BlockId): ManagedBuffer

  /**
   * 使用给定的存储级别将块本地放置。
   *
   * 如果存储了该块，则返回true；如果放置操作失败或该块已存在，则返回false。
   */
  def putBlockData(
      blockId: BlockId,
      data: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Boolean

  /**
   * 将给定的块作为流接收。
   *
   * 调用此方法时，块数据本身不可用-它将传递给返回的StreamCallbackWithID。
   */
  def putBlockDataAsStream(
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_]): StreamCallbackWithID

  /**
   * 释放由[[putBlockData()]]和[[getBlockData()]]获取的锁。
   */
  def releaseLock(blockId: BlockId, taskContext: Option[TaskContext]): Unit
}
