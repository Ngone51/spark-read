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

import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.APP_CALLER_CONTEXT
import org.apache.spark.memory.{MemoryMode, TaskMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.util._

/**
 * A unit of execution. We have two kinds of Task's in Spark:
 *
 *  - [[org.apache.spark.scheduler.ShuffleMapTask]]
 *  - [[org.apache.spark.scheduler.ResultTask]]
 *
 * A Spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultTasks, while earlier stages consist of ShuffleMapTasks. A ResultTask executes the task
 * and sends the task output back to the driver application. A ShuffleMapTask executes the task
 * and divides the task output to multiple buckets (based on the task's partitioner).
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param partitionId index of the number in the RDD
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 */
private[spark] abstract class Task[T](
    val stageId: Int,
    val stageAttemptId: Int,
    val partitionId: Int,
    @transient var localProperties: Properties = new Properties,
    // The default value is only used in tests.
    serializedTaskMetrics: Array[Byte] =
      SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array(),
    val jobId: Option[Int] = None,
    val appId: Option[String] = None,
    val appAttemptId: Option[String] = None) extends Serializable {

  @transient lazy val metrics: TaskMetrics =
    SparkEnv.get.closureSerializer.newInstance().deserialize(ByteBuffer.wrap(serializedTaskMetrics))

  /**
   * Called by [[org.apache.spark.executor.Executor]] to run this task.
   *
   * taskAttemptId：在SparkContext中，该任务尝试的唯一id
   * attemptNumber：表示该任务第几次尝试(0表示第一次尝试)
   * @param taskAttemptId an identifier for this task attempt that is unique within a SparkContext.
   * @param attemptNumber how many times this task has been attempted (0 for the first attempt)
   * @return the result of the task along with updates of Accumulators.
   */
  final def run(
      taskAttemptId: Long,
      attemptNumber: Int,
      metricsSystem: MetricsSystem): T = {
    // 向BlockInfoManager注册该task
    SparkEnv.get.blockManager.registerTask(taskAttemptId)
    // 注意：非常重要！！！每个task都会有自己的一个context。然后每个执行task的线程
    // 都会将自己的context在TaskContext存一个副本。因为TaskContext的底层实现是用一
    // 个ThreadLocal变量来存储这些contexts。所以，当每个task通过TaskContext去过去
    // context时，它们都会获取自己的执行线程对应的那一个context！其实这就是ThreadLocal
    // 的作用的体现。其实再简单想一下就知道，ThreadLocal的底层实现会用一个map来存储每个线程
    // 和它们的contexts之间的映射。
    // 创建TaskContext
    // (在task真正执行之前，创建context。这样一来，可以在task执行期间调用context来
    // 设置task执行完成之后需要做的事情。比如，添加listener来在task执行完成后进行cleanup())
    context = new TaskContextImpl(
      stageId,
      stageAttemptId, // stageAttemptId and stageAttemptNumber are semantically equal
      partitionId,
      taskAttemptId,
      attemptNumber,
      taskMemoryManager,
      localProperties,
      metricsSystem,
      metrics)
    // (通过ThreadLocal变量)设置当前环境的TaskContext
    TaskContext.setTaskContext(context)
    // 当前任务运行线程
    taskThread = Thread.currentThread()

    // 如果_reasonIfKilled不为null，则kill该任务
    if (_reasonIfKilled != null) {
      kill(interruptThread = false, _reasonIfKilled)
    }

    // TODO read
    // 创建CallerContext(啥玩意儿???)，不知道它是干嘛的，可能只和hadoop有关???
    new CallerContext(
      "TASK",
      SparkEnv.get.conf.get(APP_CALLER_CONTEXT),
      appId,
      appAttemptId,
      jobId,
      Option(stageId),
      Option(stageAttemptId),
      Option(taskAttemptId),
      Option(attemptNumber)).setCurrentContext()

    try {
      // 不同的任务类型实现不同的runTask()来执行任务
      // remember， this func is very important!!!
      runTask(context)
    } catch {
      case e: Throwable =>
        // Catch all errors; run task failure callbacks, and rethrow the exception.
        try {
          // 标记该任务失败
          context.markTaskFailed(e)
        } catch {
          case t: Throwable =>
            e.addSuppressed(t)
        }
        // 以错误信息标记该task完成
        context.markTaskCompleted(Some(e))
        throw e
    } finally {
      try {
        // 正常标记该task完成，并执行注册在该task上的completion callbacks(listeners)，比如某些
        // listener会进行cleanup()的工作
        // Call the task completion callbacks. If "markTaskCompleted" is called twice, the second
        // one is no-op.
        context.markTaskCompleted(None)
      } finally {
        try {
          Utils.tryLogNonFatalError {
            // 释放该task占用的unroll memory（unroll memory是task在申请storage memory时申请的预留内存，
            // 可能会有部分残留（没有释放）。为什么说"可能"？因为我看到，当一个value完全unroll的时候，unroll的
            // memory是会全部释放掉的，或者说全部转换成了storage memory；而如果一个value unroll失败，则它部分
            // 占用的unroll内存，会在PartiallyUnrolledIterator的元素遍历结束时，自动释放。所以，我暂时还没发现
            // 哪些地方没有释放掉一些占用的unroll memory。）
            // Release memory used by this thread for unrolling blocks
            SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP)
            SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(
              MemoryMode.OFF_HEAP)
            // 唤醒其它等待着内存释放的tasks（也就是说，在一个executor会有多个tasks执行）
            // Notify any tasks waiting for execution memory to be freed to wake up and try to
            // acquire memory again. This makes impossible the scenario where a task sleeps forever
            // because there are no other tasks left to notify it. Since this is safe to do but may
            // not be strictly necessary, we should revisit whether we can remove this in the
            // future.
            val memoryManager = SparkEnv.get.memoryManager
            memoryManager.synchronized { memoryManager.notifyAll() }
          }
        } finally {
          // 哈??? 下面的注释什么意思???
          // Though we unset the ThreadLocal here, the context member variable itself is still
          // queried directly in the TaskRunner to check for FetchFailedExceptions.
          TaskContext.unset()
        }
      }
    }
  }

  private var taskMemoryManager: TaskMemoryManager = _

  def setTaskMemoryManager(taskMemoryManager: TaskMemoryManager): Unit = {
    this.taskMemoryManager = taskMemoryManager
  }

  def runTask(context: TaskContext): T

  def preferredLocations: Seq[TaskLocation] = Nil

  // Map output tracker epoch. Will be set by TaskSetManager.
  var epoch: Long = -1

  // Task context, to be initialized in run().
  @transient var context: TaskContextImpl = _

  // The actual Thread on which the task is running, if any. Initialized in run().
  @volatile @transient private var taskThread: Thread = _

  // If non-null, this task has been killed and the reason is as specified. This is used in case
  // context is not yet initialized when kill() is invoked.
  @volatile @transient private var _reasonIfKilled: String = null

  protected var _executorDeserializeTime: Long = 0
  protected var _executorDeserializeCpuTime: Long = 0

  /**
   * If defined, this task has been killed and this option contains the reason.
   */
  def reasonIfKilled: Option[String] = Option(_reasonIfKilled)

  /**
   * Returns the amount of time spent deserializing the RDD and function to be run.
   */
  def executorDeserializeTime: Long = _executorDeserializeTime
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime

  /**
   * Collect the latest values of accumulators used in this task. If the task failed,
   * filter out the accumulators whose values should not be included on failures.
   */
  def collectAccumulatorUpdates(taskFailed: Boolean = false): Seq[AccumulatorV2[_, _]] = {
    if (context != null) {
      // Note: internal accumulators representing task metrics always count failed values
      context.taskMetrics.nonZeroInternalAccums() ++
        // zero value external accumulators may still be useful, e.g. SQLMetrics, we should not
        // filter them out.
        // TODO read
        context.taskMetrics.externalAccums.filter(a => !taskFailed || a.countFailedValues)
    } else {
      Seq.empty
    }
  }

  /**
   * 通过设置中断标志为true来kill一个task。这（kill的行为）依赖与上层spark代码和用户代码正确的处理该
   * 标志。该方法是密等的，所以它能被多次调用。
   * 如果interruptThread = true，则我们也会调用真正执行task的线程的interrupt()方法。
   * （可以看到，该方法所有的动作只是打打标记，并没有让一个task真正的直接挂掉。）
   * Kills a task by setting the interrupted flag to true. This relies on the upper level Spark
   * code and user code to properly handle the flag. This function should be idempotent so it can
   * be called multiple times.
   * If interruptThread is true, we will also call Thread.interrupt() on the Task's executor thread.
   */
  def kill(interruptThread: Boolean, reason: String) {
    require(reason != null)
    // 设置该task被kill的理由
    _reasonIfKilled = reason
    if (context != null) {
      // 在context中记录该task被kill的理由
      context.markInterrupted(reason)
    }
    // 如果interruptThread，则调用该线程的interrupt()方法
    // 关于线程中断，我觉得可以看看这篇文章：http://www.cnblogs.com/w-wfy/p/6415005.html
    if (interruptThread && taskThread != null) {
      taskThread.interrupt()
    }
  }
}
