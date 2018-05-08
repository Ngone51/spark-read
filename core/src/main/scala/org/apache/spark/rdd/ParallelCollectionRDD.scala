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

package org.apache.spark.rdd

import java.io._

import scala.Serializable
import scala.collection.Map
import scala.collection.immutable.NumericRange
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.Utils

private[spark] class ParallelCollectionPartition[T: ClassTag](
    var rddId: Long,
    var slice: Int,
    var values: Seq[T]
  ) extends Partition with Serializable {

  def iterator: Iterator[T] = values.iterator

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: ParallelCollectionPartition[_] =>
      this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {

    val sfactory = SparkEnv.get.serializer

    // Treat java serializer with default action rather than going thru serialization, to avoid a
    // separate serialization header.

    sfactory match {
        // defaultWriteObject会默认把该类的非静态和非transient字段写入out流
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeLong(rddId)
        out.writeInt(slice)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser)(_.writeObject(values))
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {

    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        rddId = in.readLong()
        slice = in.readInt()

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser)(ds => values = ds.readObject[Seq[T]]())
    }
  }
}

private[spark] class ParallelCollectionRDD[T: ClassTag](
    sc: SparkContext,
    @transient private val data: Seq[T],
    numSlices: Int,
    locationPrefs: Map[Int, Seq[String]])
    extends RDD[T](sc, Nil) {
  // TODO: Right now, each split sends along its full data, even if later down the RDD chain it gets
  // cached. It might be worthwhile to write the data to a file in the DFS and read it in the split
  // instead.
  // UPDATE: A parallel collection can be checkpointed to HDFS, which achieves this goal.

  override def getPartitions: Array[Partition] = {
    // 先对传进来的数据分片
    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
    // 再对每个分片创建一个Partition（分区）
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
  }

  // 注意，这里的Iterator的元素还不是（K，V）形式的。
  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    new InterruptibleIterator(context, s.asInstanceOf[ParallelCollectionPartition[T]].iterator)
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    locationPrefs.getOrElse(s.index, Nil)
  }
}

private object ParallelCollectionRDD {
  /**
   * 对于Range collection，也用Range来表示每一个分片，有利于减少内存的开销。（我的理解，Range不用存储
   * 区间的所有数值，只需要存储开始值、结束值和步长即可 ）
   * Slice a collection into numSlices sub-collections. One extra thing we do here is to treat Range
   * collections specially, encoding the slices as other Ranges to minimize memory cost. This makes
   * it efficient to run Spark over RDDs representing large sets of numbers. And if the collection
   * is an inclusive Range, we use inclusive range for the last slice.
   */
  def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of partitions required")
    }
    // Sequences need to be sliced at the same set of index positions for operations
    // like RDD.zip() to behave as expected
    // 计算每一组分片的起始index和结束index
    // 假如length = 10， numSlice =3：
    // ----------- start ---------------------- end -----
    // I    (0 * 10) / 3 = 0             (1 * 10) / 3 = 3
    // II   (1 * 10) / 3 = 3             (2 * 10) / 3 = 6
    // III  (2 * 10) / 3 = 6             (3 * 10) / 3 = 10
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
    seq match {
      case r: Range =>
        positions(r.length, numSlices).zipWithIndex.map { case ((start, end), index) =>
          // If the range is inclusive, use inclusive range for the last slice
          // 如果range是inclusive（我理解是闭区间），那么，最后一个分片也是用闭区间的range
          if (r.isInclusive && index == numSlices - 1) {
            new Range.Inclusive(r.start + start * r.step, r.end, r.step)
          }
          else {
            // 其它的分片都是开区间的
            new Range(r.start + start * r.step, r.start + end * r.step, r.step)
          }
        }.toSeq.asInstanceOf[Seq[Seq[T]]]
        // 为什么Range和NumericRange的处理方式差别这么大？
        // 好吧，NumericRange并不是继承Range的
      case nr: NumericRange[_] =>
        // For ranges of Long, Double, BigInteger, etc
        val slices = new ArrayBuffer[Seq[T]](numSlices)
        var r = nr
        for ((start, end) <- positions(nr.length, numSlices)) {
          val sliceSize = end - start
          slices += r.take(sliceSize).asInstanceOf[Seq[T]]
          // 大概是把上个步骤以及分过片的那部分区间里的值给裁剪掉了
          r = r.drop(sliceSize)
        }
        slices
      case _ =>
        val array = seq.toArray // To prevent O(n^2) operations for List etc
        positions(array.length, numSlices).map { case (start, end) =>
            array.slice(start, end).toSeq
        }.toSeq
    }
  }
}
