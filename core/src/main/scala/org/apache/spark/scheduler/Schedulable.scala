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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * 调度器接口。有两种调度器类型：Pool和TaskSetManager
 * An interface for schedulable entities.
 * there are two type of Schedulable entities(Pools and TaskSetManagers)
 */
private[spark] trait Schedulable {
  var parent: Pool
  // 注意：child是一个调度器队列
  // child queues
  def schedulableQueue: ConcurrentLinkedQueue[Schedulable]
  // 调度模式：FAIR、FIFO、None
  def schedulingMode: SchedulingMode
  def weight: Int
  def minShare: Int
  def runningTasks: Int
  def priority: Int
  // 注意：调度器和stage id相关联
  def stageId: Int
  def name: String

  def addSchedulable(schedulable: Schedulable): Unit
  def removeSchedulable(schedulable: Schedulable): Unit
  def getSchedulableByName(name: String): Schedulable
  def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit
  def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean
  def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager]
}
