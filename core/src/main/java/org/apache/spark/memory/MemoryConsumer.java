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

package org.apache.spark.memory;

import java.io.IOException;

import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * A memory consumer of {@link TaskMemoryManager} that supports spilling.
 *
 * Note: this only supports allocation / spilling of Tungsten memory.
 */
public abstract class MemoryConsumer {

  protected final TaskMemoryManager taskMemoryManager;
  private final long pageSize;
  private final MemoryMode mode;
  protected long used;

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode) {
    this.taskMemoryManager = taskMemoryManager;
    this.pageSize = pageSize;
    this.mode = mode;
  }

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager) {
    this(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.ON_HEAP);
  }

  /**
   * Returns the memory mode, {@link MemoryMode#ON_HEAP} or {@link MemoryMode#OFF_HEAP}.
   */
  public MemoryMode getMode() {
    return mode;
  }

  /**
   * Returns the size of used memory in bytes.
   */
  protected long getUsed() {
    return used;
  }

  /**
   * Force spill during building.
   */
  public void spill() throws IOException {
    spill(Long.MAX_VALUE, this);
  }

  /**
   * Spill some data to disk to release memory, which will be called by TaskMemoryManager
   * when there is not enough memory for the task.
   *
   * This should be implemented by subclass.
   *
   * Note: In order to avoid possible deadlock, should not call acquireMemory() from spill().
   *
   * Note: today, this only frees Tungsten-managed pages.
   *
   * @param size the amount of memory should be released
   * @param trigger the MemoryConsumer that trigger this spilling
   * @return the amount of released memory in bytes
   * @throws IOException
   */
  public abstract long spill(long size, MemoryConsumer trigger) throws IOException;

  /**
   * Allocates a LongArray of `size`. Note that this method may throw `OutOfMemoryError` if Spark
   * doesn't have enough memory for this allocation, or throw `TooLargePageException` if this
   * `LongArray` is too large to fit in a single page. The caller side should take care of these
   * two exceptions, or make sure the `size` is small enough that won't trigger exceptions.
   *
   * @throws SparkOutOfMemoryError
   * @throws TooLargePageException
   */
  public LongArray allocateArray(long size) {
    long required = size * 8L;
    MemoryBlock page = taskMemoryManager.allocatePage(required, this);
    if (page == null || page.size() < required) {
      throwOom(page, required);
    }
    used += required;
    return new LongArray(page);
  }

  /**
   * Frees a LongArray.
   */
  public void freeArray(LongArray array) {
    freePage(array.memoryBlock());
  }

  /**
   * Allocate a memory block with at least `required` bytes.
   *
   * @throws OutOfMemoryError
   */
  protected MemoryBlock allocatePage(long required) {
    // pageSize是我们指定在分配一个page时的默认大小；
    // 如果一个记录的大小（required）小于pageSize，则我们就创建一个大小为pageSize的page；如果有多个大小
    // 都远比pageSize要的记录，则我们可以在一个page中存储多个记录；反之，如果一个记录的大小（required）大于
    // pageSize，则该记录就无法存放（溢出了）在默认申请大小为pageSize的page中了。此时，我们破格单独为该记录
    // 申请一个required大小的page。注意，该page只能存储该记录，也就是说只能存储一个记录。这样一来，该记录在
    // 该page的相对offset就只能是0。offser始终为0的好处是：我们的required可能会大于
    // PackedRecordPointer#MAXIMUM_PAGE_SIZE_BYTES(2^27 bytes = 128M)限制，但是我们不用担心在为该记录的
    // offset编码的时候溢出（假设我们的required为256M，如果我们在该page存储第二个记录，此时的offset可能会是
    // 130M（> 128M）,而PackedRecordPointer对offset的编码只支持27个bit， 130M已经超过27个bit所能表示的范围了），
    // 因为我们只在required page中存储一个记录，因而offset始终为0！
    MemoryBlock page = taskMemoryManager.allocatePage(Math.max(pageSize, required), this);
    if (page == null || page.size() < required) {
      throwOom(page, required);
    }
    used += page.size();
    return page;
  }

  /**
   * Free a memory block.
   */
  protected void freePage(MemoryBlock page) {
    used -= page.size();
    taskMemoryManager.freePage(page, this);
  }

  /**
   * Allocates memory of `size`.
   */
  public long acquireMemory(long size) {
    long granted = taskMemoryManager.acquireExecutionMemory(size, this);
    used += granted;
    return granted;
  }

  /**
   * Release N bytes of memory.
   */
  public void freeMemory(long size) {
    taskMemoryManager.releaseExecutionMemory(size, this);
    used -= size;
  }

  private void throwOom(final MemoryBlock page, final long required) {
    long got = 0;
    if (page != null) {
      got = page.size();
      taskMemoryManager.freePage(page, this);
    }
    taskMemoryManager.showMemoryUsage();
    throw new SparkOutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " +
      got);
  }
}
