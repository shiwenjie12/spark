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

package org.apache.spark.network.netty

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, StreamCallbackWithID, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol._
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockId, StorageLevel}

/**
 * 通过简单地为每个请求的块注册一个块来服务于打开块的请求。
 * 处理打开和上传任意BlockManager块。
 *
 * Opened blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one Spark-level shuffle block.
 * 打开的块以“一对一”策略注册，这意味着每个传输层块均等同于一个Spark级别的洗牌块。
 */
class NettyBlockRpcServer(
    appId: String,
    serializer: Serializer,
    blockManager: BlockDataManager)
  extends RpcHandler with Logging {

  private val streamManager = new OneForOneStreamManager()

  override def receive(
      client: TransportClient,
      rpcMessage: ByteBuffer,
      responseContext: RpcResponseCallback): Unit = {
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    logTrace(s"Received request: $message")

    message match {
      case openBlocks: OpenBlocks =>
        val blocksNum = openBlocks.blockIds.length
        val blocks = for (i <- (0 until blocksNum).view)
          yield blockManager.getBlockData(BlockId.apply(openBlocks.blockIds(i)))
        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava,
          client.getChannel)
        logTrace(s"Registered streamId $streamId with $blocksNum buffers")
        responseContext.onSuccess(new StreamHandle(streamId, blocksNum).toByteBuffer)

      case fetchShuffleBlocks: FetchShuffleBlocks =>
        val blocks = fetchShuffleBlocks.mapIds.zipWithIndex.flatMap { case (mapId, index) =>
          if (!fetchShuffleBlocks.batchFetchEnabled) {
            fetchShuffleBlocks.reduceIds(index).map { reduceId =>
              blockManager.getBlockData(
                ShuffleBlockId(fetchShuffleBlocks.shuffleId, mapId, reduceId))
            }
          } else {
            val startAndEndId = fetchShuffleBlocks.reduceIds(index)
            if (startAndEndId.length != 2) {
              throw new IllegalStateException(s"Invalid shuffle fetch request when batch mode " +
                s"is enabled: $fetchShuffleBlocks")
            }
            Array(blockManager.getBlockData(
              ShuffleBlockBatchId(
                fetchShuffleBlocks.shuffleId, mapId, startAndEndId(0), startAndEndId(1))))
          }
        }

        val numBlockIds = if (fetchShuffleBlocks.batchFetchEnabled) {
          fetchShuffleBlocks.mapIds.length
        } else {
          fetchShuffleBlocks.reduceIds.map(_.length).sum
        }

        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava,
          client.getChannel)
        logTrace(s"Registered streamId $streamId with $numBlockIds buffers")
        responseContext.onSuccess(
          new StreamHandle(streamId, numBlockIds).toByteBuffer)

      case uploadBlock: UploadBlock =>
        // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
        val (level, classTag) = deserializeMetadata(uploadBlock.metadata)
        val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))
        val blockId = BlockId(uploadBlock.blockId)
        logDebug(s"Receiving replicated block $blockId with level ${level} " +
          s"from ${client.getSocketAddress}")
        blockManager.putBlockData(blockId, data, level, classTag)
        responseContext.onSuccess(ByteBuffer.allocate(0))
    }
  }

  override def receiveStream(
      client: TransportClient,
      messageHeader: ByteBuffer,
      responseContext: RpcResponseCallback): StreamCallbackWithID = {
    val message =
      BlockTransferMessage.Decoder.fromByteBuffer(messageHeader).asInstanceOf[UploadBlockStream]
    val (level, classTag) = deserializeMetadata(message.metadata)
    val blockId = BlockId(message.blockId)
    logDebug(s"Receiving replicated block $blockId with level ${level} as stream " +
      s"from ${client.getSocketAddress}")
    // 这将立即返回，但将在streamData上设置一个回调，该回调仍将执行netty线程中的所有处理。
    blockManager.putBlockDataAsStream(blockId, level, classTag)
  }

  private def deserializeMetadata[T](metadata: Array[Byte]): (StorageLevel, ClassTag[T]) = {
    serializer
      .newInstance()
      .deserialize(ByteBuffer.wrap(metadata))
      .asInstanceOf[(StorageLevel, ClassTag[T])]
  }

  override def getStreamManager(): StreamManager = streamManager
}
