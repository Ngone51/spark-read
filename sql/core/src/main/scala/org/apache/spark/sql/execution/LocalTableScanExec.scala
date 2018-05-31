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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.metric.SQLMetrics


/**
 * 一个用于遍历本地集合的数据的物理计划节点
 * Physical plan node for scanning data from a local collection.
 */
case class LocalTableScanExec(
    output: Seq[Attribute],
    @transient rows: Seq[InternalRow]) extends LeafExecNode {

  // 不同的exec应该有它们各自不同的metric，因为它们的关注点不一样啊。比如对于该exec，我们只关系该table的行数咯
  // TODO read SQLMetric extremely
  // SQLMetric是AccumulatorV2类型的。在这里，我们创建了一个名为"number of output rows"的SQLMetric，
  // 用于统计该table的输出行数。
  // 注意："numOutputRows"相当于是对该metric的一个别名，而不是该metric真正的名称。
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  @transient private lazy val unsafeRows: Array[InternalRow] = {
    if (rows.isEmpty) {
      Array.empty
    } else {
      // UnsafeProjection将rows（InternalRow）转换为UnsafeRow？可能我rows已经是UnsafeRow类型了呢？
      val proj = UnsafeProjection.create(output, output)
      rows.map(r => proj(r).copy()).toArray
    }
  }

  // 分区个数（在默认的cores个数和unsafeRows.length中抉择）
  private lazy val numParallelism: Int = math.min(math.max(unsafeRows.length, 1),
    sqlContext.sparkContext.defaultParallelism)

  private lazy val rdd = sqlContext.sparkContext.parallelize(unsafeRows, numParallelism)

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    rdd.map { r =>
      // 注意：numOutputRows是AccumulatorV2类型的对象，所以，该对象会在各个分区上做一次累加。
      // 最后，在driver端得到的结果就是各个分区累加后的总和。
      numOutputRows += 1
      r
    }
  }

  override protected def stringArgs: Iterator[Any] = {
    if (rows.isEmpty) {
      Iterator("<empty>", output)
    } else {
      Iterator(output)
    }
  }

  override def executeCollect(): Array[InternalRow] = {
    longMetric("numOutputRows").add(unsafeRows.size)
    // 因为是本地的table，所以直接可以返回结果unsafeRows，而不用发起spark job
    unsafeRows
  }

  override def executeTake(limit: Int): Array[InternalRow] = {
    val taken = unsafeRows.take(limit)
    longMetric("numOutputRows").add(taken.size)
    taken
  }
}
