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

import java.nio.ByteBuffer

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockStoreClient, DownloadFileManager}
import org.apache.spark.storage.{BlockId, EncryptedManagedBuffer, StorageLevel}
import org.apache.spark.util.ThreadUtils

/**
 * 用于一次获取一组块的BlockTransferService。每个BlockTransferService实例都包含客户端和服务器。
 */
private[spark] abstract class BlockTransferService extends BlockStoreClient with Logging {

  /**
   * 通过为其提供BlockDataManager初始化传输服务，该服务可用于获取本地块或放置本地块。
   * [[BlockStoreClient]]中的fetchBlocks方法也仅在调用后才可用。
   */
  def init(blockDataManager: BlockDataManager): Unit

  /**
   * 服务正在监听的端口号，仅在调用[[init]]之后可用。
   */
  def port: Int

  /**
   * 服务正在侦听的主机名，仅在调用[[init]]后可用。
   */
  def hostName: String

  /**
   * 将单个块上载到远程节点，仅在调用[[init]]后可用。
   */
  def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Future[Unit]

  /**
   * A special case of [[fetchBlocks]], as it fetches only one block and is blocking.
   *
   * It is also only available after [[init]] is invoked.
   */
  def fetchBlockSync(
      host: String,
      port: Int,
      execId: String,
      blockId: String,
      tempFileManager: DownloadFileManager): ManagedBuffer = {
    // A monitor for the thread to wait on.
    val result = Promise[ManagedBuffer]()
    fetchBlocks(host, port, execId, Array(blockId),
      new BlockFetchingListener {
        override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
          result.failure(exception)
        }
        override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
          data match {
            case f: FileSegmentManagedBuffer =>
              result.success(f)
            case e: EncryptedManagedBuffer =>
              result.success(e)
            case _ =>
              try {
                val ret = ByteBuffer.allocate(data.size.toInt)
                ret.put(data.nioByteBuffer())
                ret.flip()
                result.success(new NioManagedBuffer(ret))
              } catch {
                case e: Throwable => result.failure(e)
              }
          }
        }
      }, tempFileManager)
    ThreadUtils.awaitResult(result.future, Duration.Inf)
  }

  /**
   * 将单个块上载到远程节点，仅在调用[[init]]后可用。
   *
   * 此方法类似于[[uploadBlock]]，但此方法会阻塞线程，直到上传完成。
   */
  def uploadBlockSync(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Unit = {
    val future = uploadBlock(hostname, port, execId, blockId, blockData, level, classTag)
    ThreadUtils.awaitResult(future, Duration.Inf)
  }
}
