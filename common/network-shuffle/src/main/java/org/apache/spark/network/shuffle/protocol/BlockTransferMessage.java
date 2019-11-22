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

package org.apache.spark.network.shuffle.protocol;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.shuffle.ExternalBlockHandler;
import org.apache.spark.network.shuffle.protocol.mesos.RegisterDriver;
import org.apache.spark.network.shuffle.protocol.mesos.ShuffleServiceHeartbeat;

/**
 * 由{@link ExternalBlockHandler}或Spark的NettyBlockTransferService处理的消息。
 *
 * 在高层次上：
 *   -从逻辑上讲，OpenBlock仅由NettyBlockTransferService处理，但出于功能考虑
 *     对于旧版本的Spark，我们仍将其保留在外部洗牌服务中。
 *     它返回一个StreamHandle。
 *   -UploadBlock仅由NettyBlockTransferService处理。
 *   -RegisterExecutor仅由外部洗牌服务处理。
 *   -RemoveBlocks仅由外部随机播放服务处理。
 *   -两种服务都对FetchShuffleBlocks处理随机文件。它返回一个StreamHandle。
 */
public abstract class BlockTransferMessage implements Encodable {
  protected abstract Type type();

  /** Preceding every serialized message is its type, which allows us to deserialize it. */
  public enum Type {
    OPEN_BLOCKS(0), UPLOAD_BLOCK(1), REGISTER_EXECUTOR(2), STREAM_HANDLE(3), REGISTER_DRIVER(4),
    HEARTBEAT(5), UPLOAD_BLOCK_STREAM(6), REMOVE_BLOCKS(7), BLOCKS_REMOVED(8),
    FETCH_SHUFFLE_BLOCKS(9);

    private final byte id;

    Type(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.id = (byte) id;
    }

    public byte id() { return id; }
  }

  // NB: Java does not support static methods in interfaces, so we must put this in a static class.
  public static class Decoder {
    /** Deserializes the 'type' byte followed by the message itself. */
    public static BlockTransferMessage fromByteBuffer(ByteBuffer msg) {
      ByteBuf buf = Unpooled.wrappedBuffer(msg);
      byte type = buf.readByte();
      switch (type) {
        case 0: return OpenBlocks.decode(buf);
        case 1: return UploadBlock.decode(buf);
        case 2: return RegisterExecutor.decode(buf);
        case 3: return StreamHandle.decode(buf);
        case 4: return RegisterDriver.decode(buf);
        case 5: return ShuffleServiceHeartbeat.decode(buf);
        case 6: return UploadBlockStream.decode(buf);
        case 7: return RemoveBlocks.decode(buf);
        case 8: return BlocksRemoved.decode(buf);
        case 9: return FetchShuffleBlocks.decode(buf);
        default: throw new IllegalArgumentException("Unknown message type: " + type);
      }
    }
  }

  /** Serializes the 'type' byte followed by the message itself. */
  public ByteBuffer toByteBuffer() {
    // Allow room for encoded message, plus the type byte
    ByteBuf buf = Unpooled.buffer(encodedLength() + 1);
    buf.writeByte(type().id);
    encode(buf);
    assert buf.writableBytes() == 0 : "Writable bytes remain: " + buf.writableBytes();
    return buf.nioBuffer();
  }
}
