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

package org.apache.spark.network.server;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;

/**
 * 允许注册Iterator<ManagedBuffer> 的StreamManager，客户端将其单独作为块获取。每个注册缓冲区是一个块。
 */
public class OneForOneStreamManager extends StreamManager {
  private static final Logger logger = LoggerFactory.getLogger(OneForOneStreamManager.class);

  private final AtomicLong nextStreamId;
  private final ConcurrentHashMap<Long, StreamState> streams;

  /** State of a single stream. */
  private static class StreamState {
    final String appId;
    final Iterator<ManagedBuffer> buffers;

    // 与流关联的频道
    final Channel associatedChannel;

    // 用于跟踪用户已检索到的缓冲区的索引，只是为了确保调用者一次仅请求每个块一个。
    int curChunk = 0;

    // 用于跟踪正在传输但尚未完成的块的数量。
    volatile long chunksBeingTransferred = 0L;

    StreamState(String appId, Iterator<ManagedBuffer> buffers, Channel channel) {
      this.appId = appId;
      this.buffers = Preconditions.checkNotNull(buffers);
      this.associatedChannel = channel;
    }
  }

  public OneForOneStreamManager() {
    // For debugging purposes, start with a random stream id to help identifying different streams.
    // This does not need to be globally unique, only unique to this class.
    nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
    streams = new ConcurrentHashMap<>();
  }

  @Override
  public ManagedBuffer getChunk(long streamId, int chunkIndex) {
    StreamState state = streams.get(streamId);
    if (chunkIndex != state.curChunk) {
      throw new IllegalStateException(String.format(
        "Received out-of-order chunk index %s (expected %s)", chunkIndex, state.curChunk));
    } else if (!state.buffers.hasNext()) {
      throw new IllegalStateException(String.format(
        "Requested chunk index beyond end %s", chunkIndex));
    }
    state.curChunk += 1;
    ManagedBuffer nextChunk = state.buffers.next();

    if (!state.buffers.hasNext()) {
      logger.trace("Removing stream id {}", streamId);
      streams.remove(streamId);
    }

    return nextChunk;
  }

  @Override
  public ManagedBuffer openStream(String streamChunkId) {
    Pair<Long, Integer> streamChunkIdPair = parseStreamChunkId(streamChunkId);
    return getChunk(streamChunkIdPair.getLeft(), streamChunkIdPair.getRight());
  }

  public static String genStreamChunkId(long streamId, int chunkId) {
    return String.format("%d_%d", streamId, chunkId);
  }

  // 将streamChunkId解析为流ID和块ID。当获取远程块作为流时使用。
  public static Pair<Long, Integer> parseStreamChunkId(String streamChunkId) {
    String[] array = streamChunkId.split("_");
    assert array.length == 2:
      "Stream id and chunk index should be specified.";
    long streamId = Long.valueOf(array[0]);
    int chunkIndex = Integer.valueOf(array[1]);
    return ImmutablePair.of(streamId, chunkIndex);
  }

  @Override
  public void connectionTerminated(Channel channel) {
    // Close all streams which have been associated with the channel.
    for (Map.Entry<Long, StreamState> entry: streams.entrySet()) {
      StreamState state = entry.getValue();
      if (state.associatedChannel == channel) {
        streams.remove(entry.getKey());

        // 释放所有剩余的缓冲区。
        while (state.buffers.hasNext()) {
          ManagedBuffer buffer = state.buffers.next();
          if (buffer != null) {
            buffer.release();
          }
        }
      }
    }
  }

  @Override
  public void checkAuthorization(TransportClient client, long streamId) {
    if (client.getClientId() != null) {
      StreamState state = streams.get(streamId);
      Preconditions.checkArgument(state != null, "Unknown stream ID.");
      if (!client.getClientId().equals(state.appId)) {
        throw new SecurityException(String.format(
          "Client %s not authorized to read stream %d (app %s).",
          client.getClientId(),
          streamId,
          state.appId));
      }
    }
  }

  @Override
  public void chunkBeingSent(long streamId) {
    StreamState streamState = streams.get(streamId);
    if (streamState != null) {
      streamState.chunksBeingTransferred++;
    }

  }

  @Override
  public void streamBeingSent(String streamId) {
    chunkBeingSent(parseStreamChunkId(streamId).getLeft());
  }

  @Override
  public void chunkSent(long streamId) {
    StreamState streamState = streams.get(streamId);
    if (streamState != null) {
      streamState.chunksBeingTransferred--;
    }
  }

  @Override
  public void streamSent(String streamId) {
    chunkSent(OneForOneStreamManager.parseStreamChunkId(streamId).getLeft());
  }

  @Override
  public long chunksBeingTransferred() {
    long sum = 0L;
    for (StreamState streamState: streams.values()) {
      sum += streamState.chunksBeingTransferred;
    }
    return sum;
  }

  /**
   * 注册一个ManagedBuffers流，一次作为单独的块提供给调用者。每个ManagedBuffer在网络上传输后都将被释放。
   * 如果在完全耗尽迭代器之前关闭了客户端连接，则其余的缓冲区将全部由release()处理。
   *
   * If an app ID is provided, only callers who've authenticated with the given app ID will be
   * allowed to fetch from this stream.
   *
   * This method also associates the stream with a single client connection, which is guaranteed
   * to be the only reader of the stream. Once the connection is closed, the stream will never
   * be used again, enabling cleanup by `connectionTerminated`.
   */
  public long registerStream(String appId, Iterator<ManagedBuffer> buffers, Channel channel) {
    long myStreamId = nextStreamId.getAndIncrement();
    streams.put(myStreamId, new StreamState(appId, buffers, channel));
    return myStreamId;
  }

  @VisibleForTesting
  public int numStreamStates() {
    return streams.size();
  }
}
