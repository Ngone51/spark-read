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

package org.apache.spark.util.collection

import java.util.Comparator

import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.collection.WritablePartitionedPairCollection._

/**
 * Append-only buffer of key-value pairs, each with a corresponding partition ID, that keeps track
 * of its estimated size in bytes.
 *
 * The buffer can support up to 1073741819 elements.
 */
private[spark] class PartitionedPairBuffer[K, V](initialCapacity: Int = 64)
  extends WritablePartitionedPairCollection[K, V] with SizeTracker
{
  import PartitionedPairBuffer._

  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  // Basic growable array data structure. We use a single array of AnyRef to hold both the keys
  // and the values, so that we can sort them efficiently with KVArraySortDataFormat.
  private var capacity = initialCapacity
  private var curSize = 0
  private var data = new Array[AnyRef](2 * initialCapacity)

  /** Add an element into the buffer */
  def insert(partition: Int, key: K, value: V): Unit = {
    // 注意：data数组的当前大小、容量大小分别都是curSize、capacity的2倍！！！
    if (curSize == capacity) {
      // 双倍扩容
      growArray()
    }
    // 注意data数组的存储方式：两个两个连续存储，
    // 第一个位置存储(分区id，key)的pair对,
    // 第二个位置存储value
    data(2 * curSize) = (partition, key.asInstanceOf[AnyRef])
    data(2 * curSize + 1) = value.asInstanceOf[AnyRef]
    curSize += 1
    // 妙，实在是妙，这才是多态的正确打开方式！
    afterUpdate()
  }

  /** Double the size of the array because we've reached capacity */
  private def growArray(): Unit = {
    if (capacity >= MAXIMUM_CAPACITY) {
      throw new IllegalStateException(s"Can't insert more than ${MAXIMUM_CAPACITY} elements")
    }
    val newCapacity =
      if (capacity * 2 > MAXIMUM_CAPACITY) { // Overflow
        MAXIMUM_CAPACITY
      } else {
        capacity * 2
      }
    val newArray = new Array[AnyRef](2 * newCapacity)
    // 注意区分：data数组的实际大小是2*capacity，而newArray的实际大小是2*newCapacity
    System.arraycopy(data, 0, newArray, 0, 2 * capacity)
    data = newArray
    capacity = newCapacity
    // 妙，实在是妙，这才是多态的正确打开方式！
    resetSamples()
  }

  /** Iterate through the data in a given order. For this class this is not really destructive. */
  override def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    // 如果没有指定keyComparator，keyComparator为None，则map结果为None，会执行else，
    // 得到partitionComparator，该comparator只根据partition ID进行排序
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
    // 用Tim Sort算法对原始data数组按给定的comparator进行排序
    new Sorter(new KVArraySortDataFormat[(Int, K), AnyRef]).sort(data, 0, curSize, comparator)
    iterator
  }

  private def iterator(): Iterator[((Int, K), V)] = new Iterator[((Int, K), V)] {
    var pos = 0

    override def hasNext: Boolean = pos < curSize

    override def next(): ((Int, K), V) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val pair = (data(2 * pos).asInstanceOf[(Int, K)], data(2 * pos + 1).asInstanceOf[V])
      pos += 1
      pair
    }
  }
}

private object PartitionedPairBuffer {
  val MAXIMUM_CAPACITY: Int = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 2
}
