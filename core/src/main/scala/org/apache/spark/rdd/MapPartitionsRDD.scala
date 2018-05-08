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

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {
  // 我们点进去看RDD[U](prev)，该方法的形参名字是：oneParent。说明，MapPartitionsRDD确实是只有一个
  // parent rdd。下面的QUESTION也就解决啦。
  // 同时，在父类RDD[U](prev)的构建过程中，会创建MapPartitionsRDD对prev RDD的OneToOneDependency依赖

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  // 我们以examples/SparkPi为例，MapPartitionsRDD的firstParent就是ParallelCollectionRDD.因此，我们现在
  // 就通过获取ParallelCollectionRDD的Partitions来作为MapPartitionsRDD自己的Partitions。（所以，我们也
  // 可以发现，MapPartitionsRDD没有自己实现的Partition类）
  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    // QUESTION： 其中，使用firstParent是不是意味着，MapPartitionsRDD只可能会有一个parent rdd???
    // ANSWER：是的

    // 这是一个类似递归的计算过程。以SparkPi为例，我们需要等到ParallelCollectionRDD的iterator计算完，
    // 才能在MapPartitionsRDD获取其计算结果，并在此处进行MapPartitionsRDD的计算。
    f(context, split.index, firstParent[T].iterator(split, context))

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
