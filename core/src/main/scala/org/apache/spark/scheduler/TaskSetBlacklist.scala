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
package org.apache.spark.scheduler

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.util.Clock

/**
 * Handles blacklisting executors and nodes within a taskset.  This includes blacklisting specific
 * (task, executor) / (task, nodes) pairs, and also completely blacklisting executors and nodes
 * for the entire taskset.
 *
 * It also must store sufficient information in task failures for application level blacklisting,
 * which is handled by [[BlacklistTracker]].  Note that BlacklistTracker does not know anything
 * about task failures until a taskset completes successfully.
 *
 * THREADING:  This class is a helper to [[TaskSetManager]]; as with the methods in
 * [[TaskSetManager]] this class is designed only to be called from code with a lock on the
 * TaskScheduler (e.g. its event handlers). It should not be called from other threads.
 */
private[scheduler] class TaskSetBlacklist(
    private val listenerBus: LiveListenerBus,
    val conf: SparkConf,
    val stageId: Int,
    val stageAttemptId: Int,
    val clock: Clock) extends Logging {

  // 同一个task在同一个executor上最大的失败（attempts）次数，默认1次
  private val MAX_TASK_ATTEMPTS_PER_EXECUTOR = conf.get(config.MAX_TASK_ATTEMPTS_PER_EXECUTOR)
  // 同一个task在同一个host上最大的尝试失败（attempts）次数
  private val MAX_TASK_ATTEMPTS_PER_NODE = conf.get(config.MAX_TASK_ATTEMPTS_PER_NODE)
  // 一个stage中的task在同一个executor上的最大失败个数（不包含attempts， 即如果有task0.0在该executor
  // 上执行失败，且task1.0，task1.1在该executor上执行失败，失败个数 = 2， 而不是3）（超过该值，
  // stage会把该executor加入自己的黑名单中）
  private val MAX_FAILURES_PER_EXEC_STAGE = conf.get(config.MAX_FAILURES_PER_EXEC_STAGE)
  // 同一个host上，已经被一个stage加入黑名单的executors的最大个数
  // （超过该值，stage会把该host也加入自己的黑名单中）
  private val MAX_FAILED_EXEC_PER_NODE_STAGE = conf.get(config.MAX_FAILED_EXEC_PER_NODE_STAGE)

  /**
   * A map from each executor to the task failures on that executor.  This is used for blacklisting
   * within this taskset, and it is also relayed（传达给） onto [[BlacklistTracker]] for app-level
   * blacklisting if this taskset completes successfully.
   */
  val execToFailures = new HashMap[String, ExecutorFailuresInTaskSet]()

  /**
   * Map from node to all executors on it with failures.  Needed because we want to know about
   * executors on a node even after they have died. (We don't want to bother tracking the
   * node -> execs mapping in the usual case when there aren't any failures).
   */
  private val nodeToExecsWithFailures = new HashMap[String, HashSet[String]]()
  private val nodeToBlacklistedTaskIndexes = new HashMap[String, HashSet[Int]]()
  // 被该TaskSet（或者说stage）加入黑名单的executors
  // (也就是说，该TaskSet/stage中的task不会再去这些executors上执行tasks)
  private val blacklistedExecs = new HashSet[String]()
  // 被该TaskSet（或者说stage）加入黑名单的hosts
  // (也就是说，该TaskSet/stage中的task不会再去这些hosts上执行tasks)
  private val blacklistedNodes = new HashSet[String]()

  private var latestFailureReason: String = null

  /**
   * Get the most recent failure reason of this TaskSet.
   * @return
   */
  def getLatestFailureReason: String = {
    latestFailureReason
  }

  /**
   * Return true if this executor is blacklisted for the given task.  This does *not*
   * need to return true if the executor is blacklisted for the entire stage, or blacklisted
   * for the entire application.  That is to keep this method as fast as possible in the inner-loop
   * of the scheduler, where those filters will have already been applied.
   */
  def isExecutorBlacklistedForTask(executorId: String, index: Int): Boolean = {
    execToFailures.get(executorId).exists { execFailures =>
      // 如果该task在该executor的失败次数超过阈值MAX_TASK_ATTEMPTS_PER_EXECUTOR（默认1次）时，
      // 才算blacklist了。
      execFailures.getNumTaskFailures(index) >= MAX_TASK_ATTEMPTS_PER_EXECUTOR
    }
  }

  def isNodeBlacklistedForTask(node: String, index: Int): Boolean = {
    nodeToBlacklistedTaskIndexes.get(node).exists(_.contains(index))
  }

  /**
   * Return true if this executor is blacklisted for the given stage.  Completely ignores whether
   * the executor is blacklisted for the entire application (or anything to do with the node the
   * executor is on).  That is to keep this method as fast as possible in the inner-loop of the
   * scheduler, where those filters will already have been applied.
   */
  def isExecutorBlacklistedForTaskSet(executorId: String): Boolean = {
    blacklistedExecs.contains(executorId)
  }

  def isNodeBlacklistedForTaskSet(node: String): Boolean = {
    blacklistedNodes.contains(node)
  }

  private[scheduler] def updateBlacklistForFailedTask(
      host: String,
      exec: String,
      index: Int,
      failureReason: String): Unit = {
    latestFailureReason = failureReason
    val execFailures = execToFailures.getOrElseUpdate(exec, new ExecutorFailuresInTaskSet(host))
    // 更新index对应的task的在该executor上的失败次数以及最近一次的失败时间
    execFailures.updateWithFailure(index, clock.getTimeMillis())

    // check if this task has also failed on other executors on the same host -- if its gone
    // over the limit, blacklist this task from the entire host.
    val execsWithFailuresOnNode = nodeToExecsWithFailures.getOrElseUpdate(host, new HashSet())
    execsWithFailuresOnNode += exec
    val failuresOnHost = execsWithFailuresOnNode.toIterator.flatMap { exec =>
      execToFailures.get(exec).map { failures =>
        // We count task attempts here, not the number of unique executors with failures.  This is
        // because jobs are aborted based on the number task attempts; if we counted unique
        // executors, it would be hard to config to ensure that you try another
        // node before hitting the max number of task failures.
        failures.getNumTaskFailures(index)
      }
    }.sum
    // 如果在该主机上的失败次数已经超过了阈值MAX_TASK_ATTEMPTS_PER_NODE，
    // 则将该主机加入到该index所对应的task的黑名单中（也就说，该task不会再去
    // 该主机上执行）
    if (failuresOnHost >= MAX_TASK_ATTEMPTS_PER_NODE) {
      nodeToBlacklistedTaskIndexes.getOrElseUpdate(host, new HashSet()) += index
    }

    // Check if enough tasks have failed on the executor to blacklist it for the entire stage.
    // 在该executor上所有失败的tasks个数（不包含attempts， 即如果有task0.0在该executor上执行失败，
    // task1.0，task1.1在该executor上执行失败，则numFailures = 2， 而不是3）
    val numFailures = execFailures.numUniqueTasksWithFailures
    if (numFailures >= MAX_FAILURES_PER_EXEC_STAGE) {
      // 将该executor加入该stage的黑名单（即该stage中的tasks不会再去该executor上执行）
      if (blacklistedExecs.add(exec)) {
        logInfo(s"Blacklisting executor ${exec} for stage $stageId")
        // This executor has been pushed into the blacklist for this stage.  Let's check if it
        // pushes the whole node into the blacklist.
        val blacklistedExecutorsOnNode =
          execsWithFailuresOnNode.filter(blacklistedExecs.contains(_))
        val now = clock.getTimeMillis()
        listenerBus.post(
          SparkListenerExecutorBlacklistedForStage(now, exec, numFailures, stageId, stageAttemptId))
        val numFailExec = blacklistedExecutorsOnNode.size
        // 如果在该主机上，已经被该stage加入黑名单的executors的个数超过该阈值，则把该host也加入自己的黑名单
        if (numFailExec >= MAX_FAILED_EXEC_PER_NODE_STAGE) {
          if (blacklistedNodes.add(host)) {
            logInfo(s"Blacklisting ${host} for stage $stageId")
            listenerBus.post(
              SparkListenerNodeBlacklistedForStage(now, host, numFailExec, stageId, stageAttemptId))
          }
        }
      }
    }
  }
}
