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

package org.apache.spark.memory

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Tests._
import org.apache.spark.storage.BlockId

/**
  * [[MemoryManager]]强制执行和存储之间的软边界，以便任何一方都可以从另一方借用内存。
  *
  * 执行和存储之间共享的区域是（总堆空间-300MB）的一小部分，可通过`spark.memory.fraction'（默认值为0.6）进行配置。
  * 边界在该空间内的位置进一步由“ spark.memory.storageFraction”（默认值为0.5）确定。
  * 这意味着默认情况下，存储区域的大小为堆空间的0.6 * 0.5 = 0.3。
  *
  * Storage can borrow as much execution memory as is free until execution reclaims its space.
  * When this happens, cached blocks will be evicted from memory until sufficient borrowed
  * memory is released to satisfy the execution memory request.
  *
  * Similarly, execution can borrow as much storage memory as is free. However, execution
  * memory is *never* evicted by storage due to the complexities involved in implementing this.
  * The implication is that attempts to cache blocks may fail if execution has already eaten
  * up most of the storage space, in which case the new blocks will be evicted immediately
  * according to their respective storage levels.
  *
  * @param onHeapStorageRegionSize 存储区域的大小，以字节为单位。
  * 该区域不是静态保留的。如有必要，执行可以从中借用。
  * 仅当实际存储内存使用量超出此区域时，才可以驱逐缓存的块。
  **/
private[spark] class UnifiedMemoryManager(
                                           conf: SparkConf,
                                           val maxHeapMemory: Long,
                                           onHeapStorageRegionSize: Long,
                                           numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    onHeapStorageRegionSize,
    maxHeapMemory - onHeapStorageRegionSize) {

  private def assertInvariants(): Unit = {
    assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)
    assert(
      offHeapExecutionMemoryPool.poolSize + offHeapStorageMemoryPool.poolSize == maxOffHeapMemory)
  }

  assertInvariants()

  override def maxOnHeapStorageMemory: Long = synchronized {
    maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed
  }

  override def maxOffHeapStorageMemory: Long = synchronized {
    maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed
  }

  /**
    * 尝试为当前任务获取最多“ numBytes”个执行内存，并返回获得的字节数，如果不能分配则返回0。
    *
    * 在某些情况下，此调用可能会阻塞，直到有足够的可用内存为止，以确保在强制执行之前，每个任务都有机会增加到总内存池的至少1 / 2N
    * （其中N是活动任务的数量）。溢。如果任务数量增加，但是较旧的任务已经有很多内存，则可能会发生这种情况。
    */
  override private[memory] def acquireExecutionMemory(
                                                       numBytes: Long,
                                                       taskAttemptId: Long,
                                                       memoryMode: MemoryMode): Long = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    val (executionPool, storagePool, storageRegionSize, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        onHeapStorageRegionSize,
        maxHeapMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        offHeapStorageMemory,
        maxOffHeapMemory)
    }

    /**
      * 通过逐出缓存的块来扩大执行池，从而缩小存储池。
      *
      * 当获取任务的内存时，执行池可能需要进行多次尝试。
      * 每次尝试都必须能够退出存储，以防其他任务跳入并在两次尝试之间缓存较大的块。每次尝试调用一次。
      */
    def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
      if (extraMemoryNeeded > 0) {
        // 执行池中没有足够的可用内存，因此请尝试从存储中回收内存。我们可以从存储池中回收任何可用内存。
        // 如果存储池变得大于“ storageRegionSize”，我们可以逐出存储块并回收从执行中借用的内存。
        val memoryReclaimableFromStorage = math.max(
          storagePool.memoryFree,
          storagePool.poolSize - storageRegionSize)
        if (memoryReclaimableFromStorage > 0) {
          // 仅回收必要和可用的空间：
          val spaceToReclaim = storagePool.freeSpaceToShrinkPool(
            math.min(extraMemoryNeeded, memoryReclaimableFromStorage))
          storagePool.decrementPoolSize(spaceToReclaim)
          executionPool.incrementPoolSize(spaceToReclaim)
        }
      }
    }

    /**
      * 退出存储内存后，执行池的大小。
      *
      * 执行内存池将此数量平均分配给活动任务，以限制每个任务的执行内存分配。
      * 重要的是要使其大于执行池的大小，因为执行池的大小不考虑可能因撤消存储空间而释放的潜在内存。
      * 否则，我们可能会打SPARK-12155。
      *
      * Additionally, this quantity should be kept below `maxMemory` to arbitrate fairness
      * in execution memory allocation across tasks, Otherwise, a task may occupy more than
      * its fair share of execution memory, mistakenly thinking that other tasks can acquire
      * the portion of storage memory that cannot be evicted.
      */
    def computeMaxExecutionPoolSize(): Long = {
      maxMemory - math.min(storagePool.memoryUsed, storageRegionSize)
    }

    executionPool.acquireMemory(
      numBytes, taskAttemptId, maybeGrowExecutionPool, computeMaxExecutionPoolSize
  }

  override def acquireStorageMemory(
                                     blockId: BlockId,
                                     numBytes: Long,
                                     memoryMode: MemoryMode): Boolean = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    val (executionPool, storagePool, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        maxOnHeapStorageMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        maxOffHeapStorageMemory)
    }
    if (numBytes > maxMemory) {
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxMemory bytes)")
      return false
    }
    if (numBytes > storagePool.memoryFree) {
      // There is not enough free memory in the storage pool, so try to borrow free memory from
      // the execution pool.
      val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree,
        numBytes - storagePool.memoryFree)
      executionPool.decrementPoolSize(memoryBorrowedFromExecution)
      storagePool.incrementPoolSize(memoryBorrowedFromExecution)
    }
    storagePool.acquireMemory(blockId, numBytes)
  }

  override def acquireUnrollMemory(
                                    blockId: BlockId,
                                    numBytes: Long,
                                    memoryMode: MemoryMode): Boolean = synchronized {
    acquireStorageMemory(blockId, numBytes, memoryMode)
  }
}

object UnifiedMemoryManager {

  // 为非存储，非执行目的预留一定数量的内存。
  // 这个功能类似于`spark.memory.fraction`，但是保证我们即使是很小的堆也为系统保留足够的内存。
  // 例如。如果我们有一个1GB的JVM，那么默认情况下，用于执行和存储的内存将为（1024-300）* 0.6 = 434MB。
  private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

  def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
    val maxMemory = getMaxMemory(conf)
    new UnifiedMemoryManager(
      conf,
      maxHeapMemory = maxMemory,
      onHeapStorageRegionSize =
        (maxMemory * conf.get(config.MEMORY_STORAGE_FRACTION)).toLong,
      numCores = numCores)
  }

  /**
    * 返回执行和存储之间共享的内存总量（以字节为单位）。
    */
  private def getMaxMemory(conf: SparkConf): Long = {
    val systemMemory = conf.get(TEST_MEMORY)
    // 保留内存
    val reservedMemory = conf.getLong(TEST_RESERVED_MEMORY.key,
      if (conf.contains(IS_TESTING)) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
    // 最小系统内存
    val minSystemMemory = (reservedMemory * 1.5).ceil.toLong
    if (systemMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"System memory $systemMemory must " +
        s"be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
        s"option or ${config.DRIVER_MEMORY.key} in Spark configuration.")
    }
    // SPARK-12759 Check executor memory to fail fast if memory is insufficient
    if (conf.contains(config.EXECUTOR_MEMORY)) {
      val executorMemory = conf.getSizeAsBytes(config.EXECUTOR_MEMORY.key)
      if (executorMemory < minSystemMemory) {
        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
          s"$minSystemMemory. Please increase executor memory using the " +
          s"--executor-memory option or ${config.EXECUTOR_MEMORY.key} in Spark configuration.")
      }
    }
    // 可用内存
    val usableMemory = systemMemory - reservedMemory
    val memoryFraction = conf.get(config.MEMORY_FRACTION)
    (usableMemory * memoryFraction).toLong
  }
}
