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

import javax.annotation.concurrent.GuardedBy;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.spark.unsafe.Platform;

/**
 * A simple {@link MemoryAllocator} that can allocate up to 16GB using a JVM long primitive array.
 */
public class HeapMemoryAllocator implements MemoryAllocator {

  // 用于分配内存和释放内存时的pooling mechanism。
  // 从下面的代码来看，pooling mechanism大概就是对大块内存的复用机制。
  // 可能由于申请大块内存是比较overhead的，所以要复用。
  @GuardedBy("this")
  private final Map<Long, LinkedList<WeakReference<long[]>>> bufferPoolsBySize = new HashMap<>();

  // 1M??? 是的，没错，单位是byte，所以是1M
  private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;

  /**
   * pooling mechanism???啥玩意儿???内存池???
   * Returns true if allocations of the given size should go through the pooling mechanism and
   * false otherwise.
   */
  private boolean shouldPool(long size) {
    // Very small allocations are less likely to benefit from pooling.
    return size >= POOLING_THRESHOLD_BYTES;
  }

  // 分配size(字节)大小的内存块
  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    // 如果size超过了POOLING_THRESHOLD_BYTES(说明需要申请的内存块略大)，
    // 则启用pooling mechanism,从内存池找找看，有没有可复用的内存块。
    if (shouldPool(size)) {
      synchronized (this) {
        // 获取对应size的缓存内存块(弱引用存储)。
        final LinkedList<WeakReference<long[]>> pool = bufferPoolsBySize.get(size);
        if (pool != null) {
          while (!pool.isEmpty()) {
            // 🀄️注意：此处用pop()，复用的内存块，不能再被其它人复用啊。
            final WeakReference<long[]> arrayReference = pool.pop();
            // 获取到缓存的内存块(在释放内存时为了复用而保存起来的)，所以该内存块可能有
            // 之前使用过的遗留数据，是不干净的(或者说没有清零)
            final long[] array = arrayReference.get();
            if (array != null) {
              // 之所以要乘以8，是因为array是long[]数组，每个long是8个字节，
              // 所以array的总字节数是：array.length * 8L(字节)
              // 确保内存块的大小 >= 想要申请的大小
              assert (array.length * 8L >= size);
              // 创建MemoryBlock(内存块)，复用了之前的内存(array)
              MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
              if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
                // 用(byte)0xa5填充新申请的内存块
                memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
              }
              return memory;
            }
          }
          // 如果所有的缓存内存块都不可复用，则remove掉该size大小对应的那些缓存内存块
          // (可是既然我们都缓存了size大小的内存块，那么，这些内存块，又怎么会不满足复用的条件呢???
          // 可能的一个原因是，因为内存块(array)都是用弱引用缓存的，所以有可能被gc清理掉了，
          // 则array为null，也就没有可复用的内存块了)
          bufferPoolsBySize.remove(size);
        }
      }
    }
    // (size + 7) / 8是什么意思啊??? 取8的倍数???
    // 答：因为MemoryBlock是以long[] array数组来作为内存块存储数据的，而一个long的大小又为8个字节，
    // 也就是说，一个array中的元素就可以存储8个字节的数据。假设，我们现在需要存储n(1 <= n < 8)个字
    // 节的数据，显然，我们也只能至少申请1个size大小的long[] array，虽然会有几个字节的浪费。但是没办
    // 法，你又不可能把一个long拆开来。所以，这里的7就是用来做这个工作的。
    long[] array = new long[(int) ((size + 7) / 8)];
    MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    }
    return memory;
  }

  // 一个page必须先通过调用TaskMemoryManager.freePage()释放，
  // 然后再调用MemoryBlock.free()来释放被分配的内存
  @Override
  public void free(MemoryBlock memory) {
    // 如果MemoryBlock.obj为null，说明该MemoryBlock被分配的是off-heap memory(UnsafeMemoryAllocator)，
    // 反之，被分配的是on-heap memory(HeapMemoryAllocator)
    assert (memory.obj != null) :
      "baseObject was null; are you trying to use the on-heap allocator to free off-heap memory?";
    assert (memory.pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) :
      "page has already been freed";
    // 必须先调用TaskMemoryManager.freePage()
    assert ((memory.pageNumber == MemoryBlock.NO_PAGE_NUMBER)
            || (memory.pageNumber == MemoryBlock.FREED_IN_TMM_PAGE_NUMBER)) :
      "TMM-allocated pages must first be freed via TMM.freePage(), not directly in allocator " +
        "free()";

    final long size = memory.size();
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
    }

    // Mark the page as freed (so we can detect double-frees).
    memory.pageNumber = MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER;

    // 哈??? 什么意思???
    // As an additional layer of defense against use-after-free bugs, we mutate the
    // MemoryBlock to null out its reference to the long[] array.
    long[] array = (long[]) memory.obj;
    // 设置为null??? 解除该MemoryBlock对array的引用，以避免产生use-after-free的bug???
    memory.setObjAndOffset(null, 0);

    // 如果满足启用pooling mechanism机制的条件
    if (shouldPool(size)) {
      synchronized (this) {
        LinkedList<WeakReference<long[]>> pool = bufferPoolsBySize.get(size);
        if (pool == null) {
          pool = new LinkedList<>();
          bufferPoolsBySize.put(size, pool);
        }
        // 缓存array内存块，以复用
        pool.add(new WeakReference<>(array));
      }
    } else {
      // Do nothing
    }
  }
}
