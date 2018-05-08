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

import java.io.{IOException, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.reflect.ClassTag

import org.apache.spark.{Dependency, Partition, RangeDependency, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

/**
 * Partition for UnionRDD.
 *
 * @param idx index of the partition
 * @param rdd the parent RDD this partition refers to
 * @param parentRddIndex index of the parent RDD this partition refers to
 * @param parentRddPartitionIndex index of the partition within the parent RDD
 *                                this partition refers to
 */
private[spark] class UnionPartition[T: ClassTag](
    idx: Int,
    @transient private val rdd: RDD[T],
    val parentRddIndex: Int,
    @transient private val parentRddPartitionIndex: Int)
  extends Partition {

  var parentPartition: Partition = rdd.partitions(parentRddPartitionIndex)

  def preferredLocations(): Seq[String] = rdd.preferredLocations(parentPartition)

  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    parentPartition = rdd.partitions(parentRddPartitionIndex)
    oos.defaultWriteObject()
  }
}

object UnionRDD {
  private[spark] lazy val partitionEvalTaskSupport =
    new ForkJoinTaskSupport(new ForkJoinPool(8))
}

@DeveloperApi
class UnionRDD[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]])
  extends RDD[T](sc, Nil) {  // Nil since we implement getDependencies
                            // 之所以为Nil，因为我们已经在下面实现了getDependencies方法
  // visible for testing
  private[spark] val isPartitionListingParallel: Boolean =
    rdds.length > conf.getInt("spark.rdd.parallelListingThreshold", 10)

  override def getPartitions: Array[Partition] = {
    // isPartitionListingParallel??? 并发性???
    val parRDDs = if (isPartitionListingParallel) {
      val parArray = rdds.par
      parArray.tasksupport = UnionRDD.partitionEvalTaskSupport
      parArray
    } else {
      rdds
    }
    // 计算所有parent rdd的partitions个数的总和，作为Array[Partition]数组的大小
    // 这个数组将要存储的就是我们自己这个UnionRDD的Partition
    val array = new Array[Partition](parRDDs.map(_.partitions.length).seq.sum)
    var pos = 0
    // 注意语法：相当于两个for循环
    for ((rdd, rddIndex) <- rdds.zipWithIndex; split <- rdd.partitions) {
      // 就是说，我这个UnionPartition对应了第rddIndex个parent rdd的第split.index个分区
      // 有点绕，看看UnionPartition的注释就明白了
      // 注意：UnionRDD相当于是把所有的parent rdd当作一个整体的RDD来看待，不用管parent rdd的
      // 类型是否一样，因为不同类型的rdd又会有各自的compute()方法。UnionRDD的partition只需要把
      // 所有parent rdd的partition依次对应上即可。
      array(pos) = new UnionPartition(pos, rdd, rddIndex, split.index)
      pos += 1
    }
    array
  }

  override def getDependencies: Seq[Dependency[_]] = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {
      // 在[pos, pos+rdd.partitions.length)这个范围内，都是同一个parent rdd
      deps += new RangeDependency(rdd, 0, pos, rdd.partitions.length)
      pos += rdd.partitions.length
    }
    deps
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val part = s.asInstanceOf[UnionPartition[T]]
    parent[T](part.parentRddIndex).iterator(part.parentPartition, context)
  }

  override def getPreferredLocations(s: Partition): Seq[String] =
    s.asInstanceOf[UnionPartition[T]].preferredLocations()

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}
