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

package org.apache.spark.unsafe.array;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * 长值数组。与本地JVM阵列相比，这:
 * <ul>
 *   <li>支持同时使用堆上和堆外内存</li>
 *   <li>没有边界检查，因此当断言关闭时，可能会使JVM进程崩溃</li>
 * </ul>
 */
public final class LongArray {

  // 这很长，因此我们在计算偏移量时执行长乘法。
  private static final long WIDTH = 8;

  private final MemoryBlock memory;
  private final Object baseObj;
  private final long baseOffset;

  private final long length;

  public LongArray(MemoryBlock memory) {
    assert memory.size() < (long) Integer.MAX_VALUE * 8: "Array size >= Integer.MAX_VALUE elements";
    this.memory = memory;
    this.baseObj = memory.getBaseObject();
    this.baseOffset = memory.getBaseOffset();
    this.length = memory.size() / WIDTH;
  }

  public MemoryBlock memoryBlock() {
    return memory;
  }

  public Object getBaseObject() {
    return baseObj;
  }

  public long getBaseOffset() {
    return baseOffset;
  }

  /**
   * Returns the number of elements this array can hold.
   */
  public long size() {
    return length;
  }

  /**
   * Fill this all with 0L.
   */
  public void zeroOut() {
    for (long off = baseOffset; off < baseOffset + length * WIDTH; off += WIDTH) {
      Platform.putLong(baseObj, off, 0);
    }
  }

  /**
   * Sets the value at position {@code index}.
   */
  public void set(int index, long value) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < length : "index (" + index + ") should < length (" + length + ")";
    Platform.putLong(baseObj, baseOffset + index * WIDTH, value);
  }

  /**
   * Returns the value at position {@code index}.
   */
  public long get(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < length : "index (" + index + ") should < length (" + length + ")";
    return Platform.getLong(baseObj, baseOffset + index * WIDTH);
  }
}
