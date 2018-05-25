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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

object SQLExecution {

  val EXECUTION_ID_KEY = "spark.sql.execution.id"

  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement

  private val executionIdToQueryExecution = new ConcurrentHashMap[Long, QueryExecution]()

  def getQueryExecution(executionId: Long): QueryExecution = {
    executionIdToQueryExecution.get(executionId)
  }

  private val testing = sys.props.contains("spark.testing")

  private[sql] def checkSQLExecutionId(sparkSession: SparkSession): Unit = {
    val sc = sparkSession.sparkContext
    // only throw an exception during tests. a missing execution ID should not fail a job.
    if (testing && sc.getLocalProperty(EXECUTION_ID_KEY) == null) {
      // Attention testers: when a test fails with this exception, it means that the action that
      // started execution of a query didn't call withNewExecutionId. The execution ID should be
      // set by calling withNewExecutionId in the action that begins execution, like
      // Dataset.collect or DataFrameWriter.insertInto.
      throw new IllegalStateException("Execution ID should be set")
    }
  }

  /**
   * 封装一个将要执行“queryExecution”的action来追踪在body代码块中的所有的spark jobs，这样我们就可以通过
   * 一个execution来与它们（jobs）连接（哈？）。
   * Wrap an action that will execute "queryExecution" to track all Spark jobs in the body so that
   * we can connect them with an execution.
   */
  def withNewExecutionId[T](
      sparkSession: SparkSession,
      queryExecution: QueryExecution)(body: => T): T = {
    val sc = sparkSession.sparkContext
    // 如果这是第一次发起一个queryExecution，那么，默认的oldExecutionId是什么呢？
    val oldExecutionId = sc.getLocalProperty(EXECUTION_ID_KEY)
    // 通过SQLExecution获取一个新的execution id
    val executionId = SQLExecution.nextExecutionId
    // QUESTION: 有没有可能一个queryExecution还没有执行完，然后又有一个新的queryExecution提交上来执行？
    // 在这种情况下，当前的spark.sql.execution.id就会被覆盖掉。如果当前的s.s.e.id在其它地方会被使用的话，
    // 那么，它就不应该被覆盖。那么，就不可能说在一个queryExecution还没有执行完的时候就有新的queryExecution提交。
    // ANSWER: queryExecution是由action触发的，而action又会触发spark core中job流程。在spark core中，只有一个job
    // 执行完成之后，才能再执行下一个job。那么，对于queryExecution是不是也是一样的呢？
    // 设置当前的spark.sql.execution.id为该executionId
    sc.setLocalProperty(EXECUTION_ID_KEY, executionId.toString)
    // 记录该新id和该queryExecution之间的映射
    executionIdToQueryExecution.put(executionId, queryExecution)
    try {
      // sparkContext.getCallSite() would first try to pick up any call site that was previously
      // set, then fall back to Utils.getCallSite(); call Utils.getCallSite() directly on
      // streaming queries would give us call site like "run at <unknown>:0"
      val callSite = sparkSession.sparkContext.getCallSite()

      // post SparkListenerSQLExecutionStar事件到spark的监听总线（注意：这里是sparkContext中的listenerBus哦）
      sparkSession.sparkContext.listenerBus.post(SparkListenerSQLExecutionStart(
        executionId, callSite.shortForm, callSite.longForm, queryExecution.toString,
        SparkPlanInfo.fromSparkPlan(queryExecution.executedPlan), System.currentTimeMillis()))
      try {
        // 执行body(真正开始执行一个action，会触发spark core的工作机制，即提交一个job)
        body
      } finally {
        // post SQLExecutionEnd事件
        sparkSession.sparkContext.listenerBus.post(SparkListenerSQLExecutionEnd(
          executionId, System.currentTimeMillis()))
      }
    } finally {
      // 该queryExecution执行完成，则从executionIdToQueryExecution中将其删除
      executionIdToQueryExecution.remove(executionId)
      // 恢复spark.sql.execution.id为原来的oldExecutionId
      sc.setLocalProperty(EXECUTION_ID_KEY, oldExecutionId)
    }
  }

  /**
   * Wrap an action with a known executionId. When running a different action in a different
   * thread from the original one, this method can be used to connect the Spark jobs in this action
   * with the known executionId, e.g., `BroadcastHashJoin.broadcastFuture`.
   */
  def withExecutionId[T](sc: SparkContext, executionId: String)(body: => T): T = {
    val oldExecutionId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    try {
      sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, executionId)
      body
    } finally {
      sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, oldExecutionId)
    }
  }
}
