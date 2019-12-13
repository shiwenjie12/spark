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

package org.apache.spark.network.protocol;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;

/** 为简单类型提供一组规范的编码器。 */
public class Encoders {

  /** 字符串按其长度编码，后跟UTF-8字节。 */
  public static class Strings {
    public static int encodedLength(String s) {
      return 4 + s.getBytes(StandardCharsets.UTF_8).length;
    }

    public static void encode(ByteBuf buf, String s) {
      byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
      buf.writeInt(bytes.length);
      buf.writeBytes(bytes);
    }

    public static String decode(ByteBuf buf) {
      int length = buf.readInt();
      byte[] bytes = new byte[length];
      buf.readBytes(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  /** 字节数组按其长度后跟字节进行编码。 */
  public static class ByteArrays {
    public static int encodedLength(byte[] arr) {
      return 4 + arr.length;
    }

    public static void encode(ByteBuf buf, byte[] arr) {
      buf.writeInt(arr.length);
      buf.writeBytes(arr);
    }

    public static byte[] decode(ByteBuf buf) {
      int length = buf.readInt();
      byte[] bytes = new byte[length];
      buf.readBytes(bytes);
      return bytes;
    }
  }

  /** String arrays are encoded with the number of strings followed by per-String encoding. */
  public static class StringArrays {
    public static int encodedLength(String[] strings) {
      int totalLength = 4;
      for (String s : strings) {
        totalLength += Strings.encodedLength(s);
      }
      return totalLength;
    }

    public static void encode(ByteBuf buf, String[] strings) {
      buf.writeInt(strings.length);
      for (String s : strings) {
        Strings.encode(buf, s);
      }
    }

    public static String[] decode(ByteBuf buf) {
      int numStrings = buf.readInt();
      String[] strings = new String[numStrings];
      for (int i = 0; i < strings.length; i ++) {
        strings[i] = Strings.decode(buf);
      }
      return strings;
    }
  }

  /** Integer arrays are encoded with their length followed by integers. */
  public static class IntArrays {
    public static int encodedLength(int[] ints) {
      return 4 + 4 * ints.length;
    }

    public static void encode(ByteBuf buf, int[] ints) {
      buf.writeInt(ints.length);
      for (int i : ints) {
        buf.writeInt(i);
      }
    }

    public static int[] decode(ByteBuf buf) {
      int numInts = buf.readInt();
      int[] ints = new int[numInts];
      for (int i = 0; i < ints.length; i ++) {
        ints[i] = buf.readInt();
      }
      return ints;
    }
  }

  /** Long integer arrays are encoded with their length followed by long integers. */
  public static class LongArrays {
    public static int encodedLength(long[] longs) {
      return 4 + 8 * longs.length;
    }

    public static void encode(ByteBuf buf, long[] longs) {
      buf.writeInt(longs.length);
      for (long i : longs) {
        buf.writeLong(i);
      }
    }

    public static long[] decode(ByteBuf buf) {
      int numLongs = buf.readInt();
      long[] longs = new long[numLongs];
      for (int i = 0; i < longs.length; i ++) {
        longs[i] = buf.readLong();
      }
      return longs;
    }
  }
}
