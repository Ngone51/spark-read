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
import org.apache.spark.storage.BlockId

/**
 * 一个实现了软边界的MemoryManager，能够让execution和storage互相借用对方的内存。
 * A [[MemoryManager]] that enforces a soft boundary between execution and storage such that
 * either side can borrow memory from the other.
 *
 * 在execution和storage间共享的内存大小 =  spark.memory.fraction(默认0.6) * （the total heap space - 300MB），
 * 在该共享内存中，进一步以spark.memory.storageFraction(默认0.5)划分storage的大小。
 * The region shared between execution and storage is a fraction of (the total heap space - 300MB)
 * configurable through `spark.memory.fraction` (default 0.6). The position of the boundary
 * within this space is further determined by `spark.memory.storageFraction` (default 0.5).
 * This means the size of the storage region is 0.6 * 0.5 = 0.3 of the heap space by default.
 *
 * Storage can borrow as much execution memory as is free until execution reclaims its space.
 * When this happens, cached blocks will be evicted from memory until sufficient borrowed
 * memory is released to satisfy the execution memory request.(只从借去的那部分内存里释放吗?如果借去的不够呢???)
 *
 * Similarly, execution can borrow as much storage memory as is free. However, execution
 * memory is *never* evicted by storage due to the complexities involved in implementing this.
 * The implication is that attempts to cache blocks may fail if execution has already eaten
 * up most of the storage space, in which case the new blocks will be evicted immediately
 * according to their respective storage levels.(最后一句什么意思???)
 * (如果有很多task占据了storage的绝大多数内存，而storage只剩下一点点的内存；当有一个新的block想要put到
 *  storage里去的时候怎么办???（该block的size大于storeage的剩余内存，且即使驱逐已经存在的blocks，还是不够）
 *  是不是就OOM了???)
 *
 * @param onHeapStorageRegionSize Size of the storage region, in bytes.
 *                          This region is not statically reserved; execution can borrow from
 *                         （但是onHeapStorageRegionSize的实际值是不变的吧，实际变化的是storage pool size???）
 *                          it if necessary. Cached blocks can be evicted only if actual
 *                          storage memory usage exceeds this region.
 */
private[spark] class UnifiedMemoryManager private[memory] (
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
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
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
     * Grow the execution pool by evicting cached blocks, thereby shrinking the storage pool.
     *
     * When acquiring memory for a task, the execution pool may need to make multiple
     * attempts. Each attempt must be able to evict storage in case another task jumps in
     * and caches a large block between the attempts. This is called once per attempt.
     */
    def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
      if (extraMemoryNeeded > 0) {
        // There is not enough free memory in the execution pool, so try to reclaim memory from
        // storage. We can reclaim any free memory from the storage pool. If the storage pool
        // has grown to become larger than `storageRegionSize`, we can evict blocks and reclaim
        // the memory that storage has borrowed from execution.
        // 当execution pool的空闲内存不够时，我们可以向storage pool借点内存。有两种情况（我的理解）：
        // 1. storage pool size <= storageRegionSize，那么，execution pool能借用的内存就是storage pool的
        // memoryFree(空闲内存);
        // 2. storage pool size > storageRegionSize，也就是说storage pool增长到比storageRegionSize还大的时候，
        // 那么，我们就驱逐超过storageRegionSize存储的那部分blocks。
        // memoryReclaimableFromStorage就是在上述两种情况中，取（execution pool）能借用内存最多的一种情况。
        // 但是我在想，有没有这样一种情况：storage pool size已经增长超过了storageRegionSize,但是storage pool还是
        // 有空闲的内存（memoryFree）???(应该没有的吧，因为只有在MemoryFree用光的时候，storage pool size才会增长
        // 超过storageRegionSize啊)
        val memoryReclaimableFromStorage = math.max(
          storagePool.memoryFree, // 1. storage pool有空闲内存，execution pool向它借内存
          storagePool.poolSize - storageRegionSize) // 2. storage pool之前借走了execution pool的内存，execution pool
                                                    // 驱逐这部分被借走的内存，回收利用之。
        // memoryReclaimableFromStorage = 0：此时，storage pool的空闲内存(MemoryFree)为0，且poolSize=storageRegionSize
        // memoryReclaimableFromStorage < 0：memoryFree = 0（storage pool的空闲内存也被用光了：自己用或者被
        // execution pool借用）且poolSize < storageRegionSize（storage pool的size被削减了，说明execution pool之前
        // 借用了storage pool的内存），这时，execution pool的size就不能再增长了。
        // memoryReclaimableFromStorage<=0的情况，会不会导致OOM呢???
        if (memoryReclaimableFromStorage > 0) {
          // Only reclaim as much space as is necessary and available:
          val spaceToReclaim = storagePool.freeSpaceToShrinkPool(
            math.min(extraMemoryNeeded, memoryReclaimableFromStorage))
          storagePool.decrementPoolSize(spaceToReclaim)
          executionPool.incrementPoolSize(spaceToReclaim)
        }
      }
    }

    /**
     * The size the execution pool would have after evicting storage memory.
     *
     * The execution memory pool divides this quantity among the active tasks evenly to cap
     * the execution memory allocation for each task. It is important to keep this greater
     * than the execution pool size, which doesn't take into account potential memory that
     * could be freed by evicting storage. Otherwise we may hit SPARK-12155.
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
      numBytes, taskAttemptId, maybeGrowExecutionPool, () => computeMaxExecutionPoolSize)
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
      // 如果numBytes - storagePool.memoryFree < executionPool.memoryFree，那么，从executionPool借来的内存已经足够；
      // 反之，借完executionPool的所有空闲内存，还是不够，那怎么办呢???
      // 答：驱逐已经缓存的blcoks
      val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree,
        numBytes - storagePool.memoryFree)
      executionPool.decrementPoolSize(memoryBorrowedFromExecution)
      storagePool.incrementPoolSize(memoryBorrowedFromExecution)
    }
    // storage pool和execution不一样的地方在于，如果storage借用了execution的内存后，
    // 还是不够用，那么storage就会驱逐自己已有的blocks(在这里，如果storage占用的内存没有超过onHeapStorageRegionSize，
    // 还是会发生驱逐blocks???问题是，storage都已经借用了execution的内存，那么，storage的占用内存
    // 还会小于onHeapStorageRegionSize吗???)。而对于execution，如果内存不够，它会反复调用
    // maybeGrowExecutionPool(借用或驱逐并回收利用storage内存)方法，直至足够的空闲内存
    storagePool.acquireMemory(blockId, numBytes)
  }

  // 所以unroll memory和storage memory的区别到底是啥???
  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    acquireStorageMemory(blockId, numBytes, memoryMode)
  }
}

object UnifiedMemoryManager {

  // 固定300M内存，用于除了storage、execution之外的目的
  // Set aside a fixed amount of memory for non-storage, non-execution purposes.
  // This serves a function similar to `spark.memory.fraction`, but guarantees that we reserve
  // sufficient memory for the system even for small heaps. E.g. if we have a 1GB JVM, then
  // the memory used for execution and storage will be (1024 - 300) * 0.6 = 434MB by default.
  private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

  def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
    val maxMemory = getMaxMemory(conf)
    new UnifiedMemoryManager(
      conf,
      maxHeapMemory = maxMemory,
      onHeapStorageRegionSize =
        (maxMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong,
      numCores = numCores)
  }

  /**
   * Return the total amount of memory shared between execution and storage, in bytes.
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    // 使用Runtime.getRuntime.maxMemory来获取JVM的内存大小(参数spark.testing.memory应该是用来单元测试的)
    val systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    // 获取系统保留内存大小，默认300M(如果是单元测试，则默认为0M)
    val reservedMemory = conf.getLong("spark.testing.reservedMemory",
      if (conf.contains("spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
    // 设置系统最小内存为1.5倍的系统保留内存大小
    val minSystemMemory = (reservedMemory * 1.5).ceil.toLong
    if (systemMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"System memory $systemMemory must " +
        s"be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
        s"option or spark.driver.memory in Spark configuration.")
    }
    // SPARK-12759 Check executor memory to fail fast if memory is insufficient
    if (conf.contains("spark.executor.memory")) {
      val executorMemory = conf.getSizeAsBytes("spark.executor.memory")
      if (executorMemory < minSystemMemory) {
        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
          s"$minSystemMemory. Please increase executor memory using the " +
          s"--executor-memory option or spark.executor.memory in Spark configuration.")
      }
    }
    // 真正可以使用的内存 = 系统(JVM)内存 - 保留内存
    val usableMemory = systemMemory - reservedMemory
    // 获取storage和execution在可用内存中的占比
    // 那么，剩下的可用内存是用来做什么的呢???(还有0.4的占比，可不小呢)
    val memoryFraction = conf.getDouble("spark.memory.fraction", 0.6)
    (usableMemory * memoryFraction).toLong
  }
}
