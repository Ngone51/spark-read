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

package org.apache.spark.unsafe.memory;

import org.apache.spark.unsafe.Platform;

/**
 * A simple {@link MemoryAllocator} that uses {@code Unsafe} to allocate off-heap memory.
 */
public class UnsafeMemoryAllocator implements MemoryAllocator {

  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    // 通过Platform调用unsafe的allocateMemory()来
    // 分配size大小的堆外内存，并返回该内存块的起始地址
    long address = Platform.allocateMemory(size);
    // 从堆外内存申请的内存块，它的obj为null(和堆内内存申请的内存块的不同之处)，
    // 它的offset也直接对应来堆外内存的地址，而不是(堆内内存中的)偏移量
    MemoryBlock memory = new MemoryBlock(null, address, size);
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    }
    return memory;
  }

  @Override
  public void free(MemoryBlock memory) {
    assert (memory.obj == null) :
      "baseObject not null; are you trying to use the off-heap allocator to free on-heap memory?";
    assert (memory.pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) :
      "page has already been freed";
    assert ((memory.pageNumber == MemoryBlock.NO_PAGE_NUMBER)
            || (memory.pageNumber == MemoryBlock.FREED_IN_TMM_PAGE_NUMBER)) :
      "TMM-allocated pages must be freed via TMM.freePage(), not directly in allocator free()";

    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
    }
    // freeMemory只要给定内存块的起始值就能🈯️知道释放的内存块大小了???
    // (很有可能是因为，unsafe知道如何处理)
    Platform.freeMemory(memory.offset);
    // As an additional layer of defense against use-after-free bugs, we mutate the
    // MemoryBlock to reset its pointer.
    memory.offset = 0;
    // Mark the page as freed (so we can detect double-frees).
    memory.pageNumber = MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER;
  }
}
