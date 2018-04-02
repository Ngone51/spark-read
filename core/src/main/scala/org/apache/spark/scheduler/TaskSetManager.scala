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

import java.io.NotSerializableException
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.math.max
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.scheduler.SchedulingMode._
import org.apache.spark.util.{AccumulatorV2, Clock, SystemClock, Utils}
import org.apache.spark.util.collection.MedianHeap

/**
 * 用于调度在TaskSchedulerImpl中的单个TaskSet。该类跟踪了每个任务，任务重试次数，以及通过延迟调度(?)来执行本地化调度。
 * 该类主要有两个接口，一个是resourceOffer，用于询问TaskSet，是否需要在某个节点上执行一个任务；另一个是statusUpdate，
 * 用于告诉TaskSet，其中的某个任务的状态发生了变化(例如：任务结束)。
 * Schedules the tasks within a single TaskSet in the TaskSchedulerImpl. This class keeps track of
 * each task, retries tasks if they fail (up to a limited number of times), and
 * handles locality-aware scheduling for this TaskSet via delay scheduling. The main interfaces
 * to it are resourceOffer, which asks the TaskSet whether it wants to run a task on one node,
 * and statusUpdate, which tells it that one of its tasks changed state (e.g. finished).
 *
 * THREADING: This class is designed to only be called from code with a lock on the
 * TaskScheduler (e.g. its event handlers). It should not be called from other threads.
 *
 * @param sched           the TaskSchedulerImpl associated with the TaskSetManager
 * @param taskSet         the TaskSet to manage scheduling for
 * @param maxTaskFailures if any particular task fails this number of times, the entire
 *                        task set will be aborted
 */
private[spark] class TaskSetManager(
    sched: TaskSchedulerImpl,
    val taskSet: TaskSet,
    val maxTaskFailures: Int,
    blacklistTracker: Option[BlacklistTracker] = None,
    clock: Clock = new SystemClock()) extends Schedulable with Logging {

  private val conf = sched.sc.conf

  // SPARK-21563 make a copy of the jars/files so they are consistent across the TaskSet
  // scala语法：_* ???
  private val addedJars = HashMap[String, Long](sched.sc.addedJars.toSeq: _*)
  private val addedFiles = HashMap[String, Long](sched.sc.addedFiles.toSeq: _*)

  // task推测执行策略相关参数
  // Quantile of tasks at which to start speculation
  val SPECULATION_QUANTILE = conf.getDouble("spark.speculation.quantile", 0.75)
  val SPECULATION_MULTIPLIER = conf.getDouble("spark.speculation.multiplier", 1.5)

  // Limit of bytes for total size of results (default is 1GB)
  val maxResultSize = Utils.getMaxResultSize(conf)

  // 是否开启任务的推测执行策略
  val speculationEnabled = conf.getBoolean("spark.speculation", false)

  // 这个closures到底是个啥???
  // Serializer for closures and tasks.
  val env = SparkEnv.get
  val ser = env.closureSerializer.newInstance()

  val tasks = taskSet.tasks
  val numTasks = tasks.length
  val copiesRunning = new Array[Int](numTasks)

  // 对于每个任务，记录是否有任务的某个副本执行成功。如果某个任务因fetch failure而执行失败，也有可能被
  // 标记为"succeeded"，在这种情况下，该任务不能被重新执行，因为我们需要先重新生成丢失的map数据。(什么意思???)
  // For each task, tracks whether a copy of the task has succeeded. A task will also be
  // marked as "succeeded" if it failed with a fetch failure, in which case it should not
  // be re-run because the missing map data needs to be regenerated first.
  val successful = new Array[Boolean](numTasks)
  private val numFailures = new Array[Int](numTasks)

  // Set the coresponding index of Boolean var when the task killed by other attempt tasks,
  // this happened while we set the `spark.speculation` to true. The task killed by others
  // should not resubmit while executor lost.
  private val killedByOtherAttempt: Array[Boolean] = new Array[Boolean](numTasks)

  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  private[scheduler] var tasksSuccessful = 0

  val weight = 1
  val minShare = 0
  // 对应JobId
  var priority = taskSet.priority
  var stageId = taskSet.stageId
  val name = "TaskSet_" + taskSet.id
  var parent: Pool = null
  // 统计目前已经获取的tasks的(序列化)结果的size
  private var totalResultSize = 0L
  // 统计当前已经处理的成功结束的tasks的个数
  private var calculatedTasks = 0

  private[scheduler] val taskSetBlacklistHelperOpt: Option[TaskSetBlacklist] = {
    blacklistTracker.map { _ =>
      new TaskSetBlacklist(sched.sc.listenerBus, conf, stageId, taskSet.stageAttemptId, clock)
    }
  }

  private[scheduler] val runningTasksSet = new HashSet[Long]

  override def runningTasks: Int = runningTasksSet.size

  def someAttemptSucceeded(tid: Long): Boolean = {
    successful(taskInfos(tid).index)
  }

  // 一旦该TaskSetManager没有其它任务发起，则isZombie会变为true。如果每个task至少有一次attempt执行成功了，或者，
  // TaskSet终止了(例如：被kill掉了)，那么，TaskSetManagers就会进入僵尸状态。TaskSetManagers会保持僵尸状态，
  // 直到所有的任务运行结束。我们之所以让TaskSetManagers一直保持在僵尸状态，是为了持续追踪且统计正在运行的任务。
  // True once no more tasks should be launched for this task set manager. TaskSetManagers enter
  // the zombie state once at least one attempt of each task has completed successfully, or if the
  // task set is aborted (for example, because it was killed).  TaskSetManagers remain in the zombie
  // state until all tasks have finished running; we keep TaskSetManagers that are in the zombie
  // state in order to continue to track and account for the running tasks.
  // TODO: We should kill any running task attempts when the task set manager becomes a zombie.
  private[scheduler] var isZombie = false

  // 存储每个executor对应的等待执行的(多个)任务。事实上，这些集合被当作栈来对待，新的
  // tasks会加入到ArrayBuffer的末端，然后又从末端删除。这样有利于快速地发现那些重复
  // 失败的任务，因为无论何时一个任务失败，该任务又会被压入栈顶。这些集合相互直接可能会
  // 包含重复的任务，基于以下两个理由：
  // 1）任务是惰性删除的：当一个任务发起的时候，除了发起该任务的pending栈，其它的pending
  // 栈上还会有该任务(因为惰性删除的原因嘛)（所以，一个任务会在所有executor上pending???）
  // 2) 由于任务执行失败的关系，任务可能会多次重复添加到这些pending栈中去。
  // 重复的任务会通过dequeueTaskFromList来处理，它能够保证一个任务在还没有开始运行时，才能
  // 被发起。
  // Set of pending tasks for each executor. These collections are actually
  // treated as stacks, in which new tasks are added to the end of the
  // ArrayBuffer and removed from the end. This makes it faster to detect
  // tasks that repeatedly fail because whenever a task failed, it is put
  // back at the head of the stack. These collections may contain duplicates
  // for two reasons:
  // (1): Tasks are only removed lazily; when a task is launched, it remains
  // in all the pending lists except the one that it was launched from.
  // (2): Tasks may be re-added to these lists multiple times as a result
  // of failures.
  // Duplicates are handled in dequeueTaskFromList, which ensures that a
  // task hasn't already started running before launching it.
  private val pendingTasksForExecutor = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each host. Similar to pendingTasksForExecutor,
  // but at host level.
  private val pendingTasksForHost = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each rack -- similar to the above.
  private val pendingTasksForRack = new HashMap[String, ArrayBuffer[Int]]

  // Set containing pending tasks with no locality preferences.
  private[scheduler] var pendingTasksWithNoPrefs = new ArrayBuffer[Int]

  // 也是被当作栈来看待，和上面一样
  // Set containing all pending tasks (also used as a stack, as above).
  private val allPendingTasks = new ArrayBuffer[Int]

  // Tasks that can be speculated. Since these will be a small fraction of total
  // tasks, we'll just hold them in a HashSet.
  private[scheduler] val speculatableTasks = new HashSet[Int]

  // Task index, start and finish time for each task attempt (indexed by task ID)
  private val taskInfos = new HashMap[Long, TaskInfo]

  // 用于记录所有成功执行的任务的运行时间的中位数
  // Use a MedianHeap to record durations of successful tasks so we know when to launch
  // speculative tasks. This is only used when speculation is enabled, to avoid the overhead
  // of inserting into the heap when the heap won't be used.
  val successfulTaskDurations = new MedianHeap()

  // How frequently to reprint duplicate exceptions in full, in milliseconds
  val EXCEPTION_PRINT_INTERVAL =
    conf.getLong("spark.logging.exceptionPrintInterval", 10000)

  // 记录最近的异常和异常重复次数以及上次打印完整异常时间之间的映射关系
  // Map of recent exceptions (identified by string representation and top stack frame) to
  // duplicate count (how many times the same exception has appeared) and time the full exception
  // was printed. This should ideally be an LRU map that can drop old exceptions automatically.
  // 那为什么不用LinkedHashMap呢???
  private val recentExceptions = HashMap[String, (Int, Long)]()

  // So，epoch的作用???
  // Figure out the current map output tracker epoch and set it on all tasks
  val epoch = sched.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  for (t <- tasks) {
    // 和mapOutputTracker的epoch对上了???
    t.epoch = epoch
  }

  // Add all our tasks to the pending lists. We do this in reverse order
  // of task index so that tasks with low indices get launched first.
  for (i <- (0 until numTasks).reverse) {
    addPendingTask(i)
  }

  /**
   * Track the set of locality levels which are valid given the tasks locality preferences and
   * the set of currently available executors.  This is updated as executors are added and removed.
   * This allows a performance optimization, of skipping levels that aren't relevant (eg., skip
   * PROCESS_LOCAL if no tasks could be run PROCESS_LOCAL for the current set of executors).
   */
  private[scheduler] var myLocalityLevels = computeValidLocalityLevels()

  // Time to wait at each level
  private[scheduler] var localityWaits = myLocalityLevels.map(getLocalityWait)

  // Delay scheduling variables: we keep track of our current locality level and the time we
  // last launched a task at that level, and move up a level when localityWaits[curLevel] expires.
  // We then move down if we manage to launch a "more local" task.
  private var currentLocalityIndex = 0 // Index of our current locality level in validLocalityLevels
  private var lastLaunchTime = clock.getTimeMillis()  // Time we last launched a task at this level

  override def schedulableQueue: ConcurrentLinkedQueue[Schedulable] = null

  override def schedulingMode: SchedulingMode = SchedulingMode.NONE

  private[scheduler] var emittedTaskSizeWarning = false

  // 将一个任务添加到对应的所有(注意：是对应的所有)pending队列中去
  /** Add a task to all the pending-task lists that it should be on. */
  private[spark] def addPendingTask(index: Int) {
    for (loc <- tasks(index).preferredLocations) {
      loc match {
        case e: ExecutorCacheTaskLocation =>
          // 如果是一个ExecutorCacheTaskLocation，则更新对应的executor的pending队列
          pendingTasksForExecutor.getOrElseUpdate(e.executorId, new ArrayBuffer) += index
        case e: HDFSCacheTaskLocation =>
          // 如果是一个HDFSCacheTaskLocation，则先根据主机的host地址，获取该主机上的所有executors
          val exe = sched.getExecutorsAliveOnHost(loc.host)
          exe match {
            case Some(set) =>
              for (e <- set) {
                // 把该任务添加到该主机(host)的所有executor的pending队列中(所以在
                // pendingTasksForExecutor中，任务是有可能重复的嘛)
                pendingTasksForExecutor.getOrElseUpdate(e, new ArrayBuffer) += index
              }
              logInfo(s"Pending task $index has a cached location at ${e.host} " +
                ", where there are executors " + set.mkString(","))
            case None => logDebug(s"Pending task $index has a cached location at ${e.host} " +
                ", but there are no executors alive there.")
          }
        case _ =>
      }
      // 更新对应的主机(host)的pending队列
      pendingTasksForHost.getOrElseUpdate(loc.host, new ArrayBuffer) += index
      // 如果sched的实现是TaskSchedulerImpl，那么，getRackForHost()总返回None
      // 如果有该主机对应的机架信息，则更新机架对应的pending队列
      for (rack <- sched.getRackForHost(loc.host)) {
        pendingTasksForRack.getOrElseUpdate(rack, new ArrayBuffer) += index
      }
    }

    // 对于LearningExample这个例子，preferredLocations就是Nil啊，所以就添加到
    // pendingTasksWithNoPrefs里去了??? 感觉是。
    if (tasks(index).preferredLocations == Nil) {
      pendingTasksWithNoPrefs += index
    }

    // 更新allPendingTasks
    allPendingTasks += index  // No point scanning this whole list to find the old task there
  }

  /**
   * Return the pending tasks list for a given executor ID, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForExecutor(executorId: String): ArrayBuffer[Int] = {
    pendingTasksForExecutor.getOrElse(executorId, ArrayBuffer())
  }

  /**
   * Return the pending tasks list for a given host, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForHost(host: String): ArrayBuffer[Int] = {
    pendingTasksForHost.getOrElse(host, ArrayBuffer())
  }

  /**
   * Return the pending rack-local task list for a given rack, or an empty list if
   * there is no map entry for that rack
   */
  private def getPendingTasksForRack(rack: String): ArrayBuffer[Int] = {
    pendingTasksForRack.getOrElse(rack, ArrayBuffer())
  }

  /**
   * Dequeue a pending task from the given list and return its index.
   * Return None if the list is empty.
   * This method also cleans up any tasks in the list that have already
   * been launched, since we want that to happen lazily.
   */
  private def dequeueTaskFromList(
      execId: String,
      host: String,
      list: ArrayBuffer[Int]): Option[Int] = {
    var indexOffset = list.size
    while (indexOffset > 0) {
      // 又是倒着来的，因为要模拟栈
      indexOffset -= 1
      val index = list(indexOffset)
      // 又要先确认executor或host没有加入黑名单
      if (!isTaskBlacklistedOnExecOrNode(index, execId, host)) {
        // 这里的remove相当于trimEnd(1),也就删除列表尾部的一个元素(因为模拟了栈)
        // This should almost always be list.trimEnd(1) to remove tail
        list.remove(indexOffset)
        // 这边又要检查(不重复??? 可能有些调用不是按我现在看的顺序，一路调用过来的(比如单元测试)，
        // 所以可能先前没有检查)
        if (copiesRunning(index) == 0 && !successful(index)) {
          return Some(index)
        }
      }
    }
    None
  }

  /** Check whether a task is currently running an attempt on a given host */
  private def hasAttemptOnHost(taskIndex: Int, host: String): Boolean = {
    taskAttempts(taskIndex).exists(_.host == host)
  }

  private def isTaskBlacklistedOnExecOrNode(index: Int, execId: String, host: String): Boolean = {
    taskSetBlacklistHelperOpt.exists { blacklist =>
      blacklist.isNodeBlacklistedForTask(host, index) ||
        blacklist.isExecutorBlacklistedForTask(execId, index)
    }
  }

  /**
   * Return a speculative task for a given executor if any are available. The task should not have
   * an attempt running on this host, in case the host is slow. In addition, the task should meet
   * the given locality constraint.
   */
  // Labeled as protected to allow tests to override providing speculative tasks if necessary
  protected def dequeueSpeculativeTask(execId: String, host: String, locality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value)] =
  {
    speculatableTasks.retain(index => !successful(index)) // Remove finished tasks from set

    def canRunOnHost(index: Int): Boolean = {
      !hasAttemptOnHost(index, host) &&
        !isTaskBlacklistedOnExecOrNode(index, execId, host)
    }

    if (!speculatableTasks.isEmpty) {
      // Check for process-local tasks; note that tasks can be process-local
      // on multiple nodes when we replicate cached blocks, as in Spark Streaming
      for (index <- speculatableTasks if canRunOnHost(index)) {
        val prefs = tasks(index).preferredLocations
        val executors = prefs.flatMap(_ match {
          case e: ExecutorCacheTaskLocation => Some(e.executorId)
          case _ => None
        });
        if (executors.contains(execId)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.PROCESS_LOCAL))
        }
      }

      // Check for node-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.NODE_LOCAL)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          val locations = tasks(index).preferredLocations.map(_.host)
          if (locations.contains(host)) {
            speculatableTasks -= index
            return Some((index, TaskLocality.NODE_LOCAL))
          }
        }
      }

      // Check for no-preference tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.NO_PREF)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          val locations = tasks(index).preferredLocations
          if (locations.size == 0) {
            speculatableTasks -= index
            return Some((index, TaskLocality.PROCESS_LOCAL))
          }
        }
      }

      // Check for rack-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
        for (rack <- sched.getRackForHost(host)) {
          for (index <- speculatableTasks if canRunOnHost(index)) {
            val racks = tasks(index).preferredLocations.map(_.host).flatMap(sched.getRackForHost)
            if (racks.contains(rack)) {
              speculatableTasks -= index
              return Some((index, TaskLocality.RACK_LOCAL))
            }
          }
        }
      }

      // Check for non-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.ANY))
        }
      }
    }

    None
  }

  /**
   * Dequeue a pending task for a given node and return its index and locality level.
   * Only search for tasks matching the given locality constraint.
   *
   * @return An option containing (task index within the task set, locality, is speculative?)
   */
  private def dequeueTask(execId: String, host: String, maxLocality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value, Boolean)] =
  {
    // 因为maxLocality最小就等于PROCESS_LOCAL，所以它肯定满足大于等于PROCESS_LOCAL，所以这边
    // 不需要isAllowed判断。
    for (index <- dequeueTaskFromList(execId, host, getPendingTasksForExecutor(execId))) {
      // 因为是从executor的pending队列中，出队的task，所以locality是PROCESS_LOCAL
      return Some((index, TaskLocality.PROCESS_LOCAL, false))
    }

    // 假设maxLocality=PROCESS_LOCAL，所以，我们肯定不能在Host的pending tasks中来出队一个task啊，
    // 因为它不满足PROCESS_LOCAL的需求。
    // isAllowed -> NODE_LOCAL <= maxLocality
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      for (index <- dequeueTaskFromList(execId, host, getPendingTasksForHost(host))) {
        return Some((index, TaskLocality.NODE_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
      // Look for noPref tasks after NODE_LOCAL for minimize cross-rack traffic
      for (index <- dequeueTaskFromList(execId, host, pendingTasksWithNoPrefs)) {
        return Some((index, TaskLocality.PROCESS_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.RACK_LOCAL)) {
      for {
        rack <- sched.getRackForHost(host)
        index <- dequeueTaskFromList(execId, host, getPendingTasksForRack(rack))
      } {
        return Some((index, TaskLocality.RACK_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.ANY)) {
      for (index <- dequeueTaskFromList(execId, host, allPendingTasks)) {
        return Some((index, TaskLocality.ANY, false))
      }
    }

    // TODO read
    // 如果所有其它任务都已经被调度过了，那么，尝试找一个推测执行的任务
    // find a speculative task if all others tasks have been scheduled
    dequeueSpeculativeTask(execId, host, maxLocality).map {
      case (taskIndex, allowedLocality) => (taskIndex, allowedLocality, true)}
  }

  /**
   * 该函数作用：为单个executor提供的offer(资源)，匹配一个任务。
   * Respond to an offer of a single executor from the scheduler by finding a task
   *
   * NOTE: this function is either called with a maxLocality which
   * would be adjusted by delay scheduling algorithm or it will be with a special
   * NO_PREF locality which will be not modified
   *
   * @param execId the executor Id of the offered resource
   * @param host  the host Id of the offered resource
   * @param maxLocality the maximum locality we want to schedule the tasks at
   */
  @throws[TaskNotSerializableException]
  def resourceOffer(
      execId: String,
      host: String,
      maxLocality: TaskLocality.TaskLocality)
    : Option[TaskDescription] =
  {
    // 从TaskSchedulerImpl.resourceOfferSingleTaskSet()过来的时候不是已经
    // 过滤黑名单了吗???（除非是为了测试，再过滤一遍）
    val offerBlacklisted = taskSetBlacklistHelperOpt.exists { blacklist =>
      blacklist.isNodeBlacklistedForTaskSet(host) ||
        blacklist.isExecutorBlacklistedForTaskSet(execId)
    }
    if (!isZombie && !offerBlacklisted) {
      // 准备launch任务的开始时间
      val curTime = clock.getTimeMillis()

      var allowedLocality = maxLocality

      // NO_PREF不需要被调整
      if (maxLocality != TaskLocality.NO_PREF) {
        allowedLocality = getAllowedLocalityLevel(curTime)
        // 说明之前的locality超时了
        if (allowedLocality > maxLocality) {
          // 我们不允许去搜索更远的任务???
          // We're not allowed to search for farther-away tasks
          allowedLocality = maxLocality
        }
      }

      // 在allowedLocality约束下，从task的pending队列中，
      // 取出一个task来，并生成该task的TaskDescription
      dequeueTask(execId, host, allowedLocality).map { case ((index, taskLocality, speculative)) =>
        // Found a task; do some bookkeeping and return a task description
        val task = tasks(index)
        val taskId = sched.newTaskId()
        // 该task的运行副本+1
        // Do various bookkeeping
        copiesRunning(index) += 1
        val attemptNum = taskAttempts(index).size
        // 注意区分这里的taskId和attemptNum：taskId应该是全局(整个application中的所有task)唯一的，
        // 而attemptNum是指在某个TaskSet中，同一个task的第attempt(从0开始)次执行
        val info = new TaskInfo(taskId, index, attemptNum, curTime,
          execId, host, taskLocality, speculative)
        taskInfos(taskId) = info
        // 从头部加入该taskAttempts队列(实现：整个队列依次往后移位???)
        taskAttempts(index) = info :: taskAttempts(index)
        // Update our locality level for delay scheduling
        // NO_PREF will not affect the variables related to delay scheduling
        if (maxLocality != TaskLocality.NO_PREF) {
          currentLocalityIndex = getLocalityIndex(taskLocality)
          lastLaunchTime = curTime
        }
        // 又要序列化task??? 这是哪里的task，我已经晕了...
        // 我猜到时候会把这个序列化的task对象发送到executor(TaskSetManager的
        // 所有准备工作应该是在driver端完成的)
        // Serialize and return the task
        val serializedTask: ByteBuffer = try {
          ser.serialize(task)
        } catch {
          // If the task cannot be serialized, then there's no point to re-attempt the task,
          // as it will always fail. So just abort the whole task-set.
          case NonFatal(e) =>
            val msg = s"Failed to serialize task $taskId, not attempting to retry it."
            logError(msg, e)
            abort(s"$msg Exception during serialization: $e")
            throw new TaskNotSerializableException(e)
        }
        // 检查单个task的size大小是否超过了建议的大小
        if (serializedTask.limit() > TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024 &&
          !emittedTaskSizeWarning) {
          emittedTaskSizeWarning = true
          logWarning(s"Stage ${task.stageId} contains a task of very large size " +
            s"(${serializedTask.limit() / 1024} KB). The maximum recommended task size is " +
            s"${TaskSetManager.TASK_SIZE_TO_WARN_KB} KB.")
        }
        // 为TaskSetManager添加一个running task
        addRunningTask(taskId)

        // 我们之前会记录一个任务序列化所花费的时间，但是任务序列化后的size已经是序列化时间很好的说明了。
        // We used to log the time it takes to serialize the task, but task size is already
        // a good proxy to task serialization time.
        // val timeTaken = clock.getTime() - startTime
        val taskName = s"task ${info.id} in stage ${taskSet.id}"
        logInfo(s"Starting $taskName (TID $taskId, $host, executor ${info.executorId}, " +
          s"partition ${task.partitionId}, $taskLocality, ${serializedTask.limit()} bytes)")

        // 通过dagScheduler通知ListenerBus，有新的task启动
        sched.dagScheduler.taskStarted(task, info)
        // 为该Task创建TaskDescription
        new TaskDescription(
          taskId,
          attemptNum,
          execId,
          taskName,
          index,
          addedFiles,
          addedJars,
          task.localProperties,
          serializedTask)
      }
    } else {
      None
    }
  }

  private def maybeFinishTaskSet() {
    if (isZombie && runningTasks == 0) {
      sched.taskSetFinished(this)
      if (tasksSuccessful == numTasks) {
        blacklistTracker.foreach(_.updateBlacklistForSuccessfulTaskSet(
          taskSet.stageId,
          taskSet.stageAttemptId,
          taskSetBlacklistHelperOpt.get.execToFailures))
      }
    }
  }

  /**
   * Get the level we can launch tasks according to delay scheduling, based on current wait time.
   */
  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    // Remove the scheduled or finished tasks lazily
    def tasksNeedToBeScheduledFrom(pendingTaskIds: ArrayBuffer[Int]): Boolean = {
      var indexOffset = pendingTaskIds.size
      while (indexOffset > 0) {
        // 到着来的发现没，这样pending tasks队列就像一个栈一样了
        indexOffset -= 1
        val index = pendingTaskIds(indexOffset)
        // 如果该task(index是task在TaskSet中的索引)的运行副本为0，且从未记录成功，
        // 说明有还未被调度执行的task，则返回true
        if (copiesRunning(index) == 0 && !successful(index)) {
          return true
        } else {
          // 反之，该task已经被调度执行了，或者已经执行成功了，则pending队列中删除(这里体现了惰性删除)
          pendingTaskIds.remove(indexOffset)
        }
      }
      false
    }
    // Walk through the list of tasks that can be scheduled at each location and returns true
    // if there are any tasks that still need to be scheduled. Lazily cleans up tasks that have
    // already been scheduled.
    def moreTasksToRunIn(pendingTasks: HashMap[String, ArrayBuffer[Int]]): Boolean = {
      val emptyKeys = new ArrayBuffer[String]
      val hasTasks = pendingTasks.exists {
        case (id: String, tasks: ArrayBuffer[Int]) =>
          if (tasksNeedToBeScheduledFrom(tasks)) {
            true
          } else {
            emptyKeys += id
            false
          }
      }
      // 说明该executor或host或rack对应的pending队列里的task都已经被调度执行或执行成功了
      // The key could be executorId, host or rackId
      emptyKeys.foreach(id => pendingTasks.remove(id))
      hasTasks
    }

    while (currentLocalityIndex < myLocalityLevels.length - 1) {
      val moreTasks = myLocalityLevels(currentLocalityIndex) match {
        case TaskLocality.PROCESS_LOCAL => moreTasksToRunIn(pendingTasksForExecutor)
        case TaskLocality.NODE_LOCAL => moreTasksToRunIn(pendingTasksForHost)
        case TaskLocality.NO_PREF => pendingTasksWithNoPrefs.nonEmpty
        case TaskLocality.RACK_LOCAL => moreTasksToRunIn(pendingTasksForRack)
      }
      if (!moreTasks) {
        // This is a performance optimization: if there are no more tasks that can
        // be scheduled at a particular locality level, there is no point in waiting
        // for the locality wait timeout (SPARK-4939).
        lastLaunchTime = curTime
        logDebug(s"No tasks for locality level ${myLocalityLevels(currentLocalityIndex)}, " +
          s"so moving to locality level ${myLocalityLevels(currentLocalityIndex + 1)}")
        currentLocalityIndex += 1
      } else if (curTime - lastLaunchTime >= localityWaits(currentLocalityIndex)) {
        // Jump to the next locality level, and reset lastLaunchTime so that the next locality
        // wait timer doesn't immediately expire
        lastLaunchTime += localityWaits(currentLocalityIndex)
        logDebug(s"Moving to ${myLocalityLevels(currentLocalityIndex + 1)} after waiting for " +
          s"${localityWaits(currentLocalityIndex)}ms")
        currentLocalityIndex += 1
      } else {
        return myLocalityLevels(currentLocalityIndex)
      }
    }
    myLocalityLevels(currentLocalityIndex)
  }

  /**
   * Find the index in myLocalityLevels for a given locality. This is also designed to work with
   * localities that are not in myLocalityLevels (in case we somehow get those) by returning the
   * next-biggest level we have. Uses the fact that the last value in myLocalityLevels is ANY.
   */
  def getLocalityIndex(locality: TaskLocality.TaskLocality): Int = {
    var index = 0
    while (locality > myLocalityLevels(index)) {
      index += 1
    }
    index
  }

  /**
   * Check whether the given task set has been blacklisted to the point that it can't run anywhere.
   *
   * It is possible that this taskset has become impossible to schedule *anywhere* due to the
   * blacklist.  The most common scenario would be if there are fewer executors than
   * spark.task.maxFailures. We need to detect this so we can fail the task set, otherwise the job
   * will hang.
   *
   * There's a tradeoff here: we could make sure all tasks in the task set are schedulable, but that
   * would add extra time to each iteration of the scheduling loop. Here, we take the approach of
   * making sure at least one of the unscheduled tasks is schedulable. This means we may not detect
   * the hang as quickly as we could have, but we'll always detect the hang eventually, and the
   * method is faster in the typical case. In the worst case, this method can take
   * O(maxTaskFailures + numTasks) time, but it will be faster when there haven't been any task
   * failures (this is because the method picks one unscheduled task, and then iterates through each
   * executor until it finds one that the task isn't blacklisted on).
   */
  private[scheduler] def abortIfCompletelyBlacklisted(
      hostToExecutors: HashMap[String, HashSet[String]]): Unit = {
    taskSetBlacklistHelperOpt.foreach { taskSetBlacklist =>
      val appBlacklist = blacklistTracker.get
      // Only look for unschedulable tasks when at least one executor has registered. Otherwise,
      // task sets will be (unnecessarily) aborted in cases when no executors have registered yet.
      if (hostToExecutors.nonEmpty) {
        // find any task that needs to be scheduled
        val pendingTask: Option[Int] = {
          // usually this will just take the last pending task, but because of the lazy removal
          // from each list, we may need to go deeper in the list.  We poll from the end because
          // failed tasks are put back at the end of allPendingTasks, so we're more likely to find
          // an unschedulable task this way.
          val indexOffset = allPendingTasks.lastIndexWhere { indexInTaskSet =>
            copiesRunning(indexInTaskSet) == 0 && !successful(indexInTaskSet)
          }
          if (indexOffset == -1) {
            None
          } else {
            Some(allPendingTasks(indexOffset))
          }
        }

        pendingTask.foreach { indexInTaskSet =>
          // try to find some executor this task can run on.  Its possible that some *other*
          // task isn't schedulable anywhere, but we will discover that in some later call,
          // when that unschedulable task is the last task remaining.
          val blacklistedEverywhere = hostToExecutors.forall { case (host, execsOnHost) =>
            // Check if the task can run on the node
            val nodeBlacklisted =
              appBlacklist.isNodeBlacklisted(host) ||
                taskSetBlacklist.isNodeBlacklistedForTaskSet(host) ||
                taskSetBlacklist.isNodeBlacklistedForTask(host, indexInTaskSet)
            if (nodeBlacklisted) {
              true
            } else {
              // Check if the task can run on any of the executors
              execsOnHost.forall { exec =>
                appBlacklist.isExecutorBlacklisted(exec) ||
                  taskSetBlacklist.isExecutorBlacklistedForTaskSet(exec) ||
                  taskSetBlacklist.isExecutorBlacklistedForTask(exec, indexInTaskSet)
              }
            }
          }
          if (blacklistedEverywhere) {
            val partition = tasks(indexInTaskSet).partitionId
            abort(s"""
              |Aborting $taskSet because task $indexInTaskSet (partition $partition)
              |cannot run anywhere due to node and executor blacklist.
              |Most recent failure:
              |${taskSetBlacklist.getLatestFailureReason}
              |
              |Blacklisting behavior can be configured via spark.blacklist.*.
              |""".stripMargin)
          }
        }
      }
    }
  }

  /**
   * Marks the task as getting result and notifies the DAG Scheduler
   */
  def handleTaskGettingResult(tid: Long): Unit = {
    val info = taskInfos(tid)
    // 标记该Task获取结果的开始时间
    info.markGettingResult(clock.getTimeMillis())
    // 同时通知DAG Scheduler该task正在获取结果的事件，DAG再把该事件
    // post到ListenerBus上，Listener继而将该事件分发给注册建听了该
    // 事件的listeners
    sched.dagScheduler.taskGettingResult(info)
  }

  /**
   * Check whether has enough quota to fetch the result with `size` bytes
   */
  def canFetchMoreResults(size: Long): Boolean = sched.synchronized {
    totalResultSize += size
    calculatedTasks += 1
    // 如果超过了maxResultSize（默认1G），则整个task set中止（why？maybe，超过
    // 最大限制，对网络传输有影响???）
    if (maxResultSize > 0 && totalResultSize > maxResultSize) {
      val msg = s"Total size of serialized results of ${calculatedTasks} tasks " +
        s"(${Utils.bytesToString(totalResultSize)}) is bigger than spark.driver.maxResultSize " +
        s"(${Utils.bytesToString(maxResultSize)})"
      logError(msg)
      // TODO read
      abort(msg)
      false
    } else {
      true
    }
  }

  /**
   * 一定要记得，该方法是受TaskScheduler加锁保护的！！！所以，有些地方我们才能够不用担心线程安全
   * 标记该task为成功状态，并且通知DAGScheduler这里有个task结束啦！！！（DAGScheduler继而又会
   * 通过ListenerBus post一个task结束的事件）
   * Marks a task as successful and notifies the DAGScheduler that the task has ended.
   */
  def handleSuccessfulTask(tid: Long, result: DirectTaskResult[_]): Unit = {
    val info = taskInfos(tid)
    val index = info.index
    // 标记该task结束，并记录结束时间
    info.markFinished(TaskState.FINISHED, clock.getTimeMillis())
    // 如果开启了task推荐执行的策略，当有新的task执行完成的时候，
    // 我们就要重新计算所有成功执行的tasks的运行耗时的中位数。
    // 因为推荐执行策略会根据该中位数来发起推测执行的任务。
    if (speculationEnabled) {
      // info.duration用于计算该task的执行耗时
      // 更新所有成功执行的tasks的运行耗时的中位数
      successfulTaskDurations.insert(info.duration)
    }
    // 从TaskSetManager#runnungTaskSet中移除该task(因为该task已经执行结束啦，
    // 它已经不是正在执行的tasks的一员啦)
    removeRunningTask(tid)

    // TODO：一个问题：是不是只有在task推测执行策略开启的情况下，才会有其它的attempts?
    // 因为我们查看killedByOtherAttempt这个数据结构上的注释说明了解到，只有当推测执行
    // 策略开启的时候，killedByOtherAttempt(index)才有可能设置为true。
    // kill掉该task的其它attempts(因为所有attempts里已经有一个成功啦，其它的就没必要继续执行啦！)
    // Kill any other attempts for the same task (since those are unnecessary now that one
    // attempt completed successfully).
    for (attemptInfo <- taskAttempts(index) if attemptInfo.running) {
      logInfo(s"Killing attempt ${attemptInfo.attemptNumber} for task ${attemptInfo.id} " +
        s"in stage ${taskSet.id} (TID ${attemptInfo.taskId}) on ${attemptInfo.host} " +
        s"as the attempt ${info.attemptNumber} succeeded on ${info.host}")
      killedByOtherAttempt(index) = true
      // 考虑到网络IO，能否批量kill掉tasks???
      // 答：显然不行的。原本推测执行的策略就是要让tasks在不同的executor上执行，所以怎么可能批量删除呢。
      // 通过backend kill一个task
      // （我们已经阅读了和kill task相关的代码，这部分代码大多是看起来只是给task打打被kill的标记
      // ，而没有真正执行杀死的操作。那么，task又是如何被真正杀死的呢???感觉和线程的interrupt知识
      // 相关）
      // TODO 更好地理解kill task
      sched.backend.killTask(
        attemptInfo.taskId,
        attemptInfo.executorId,
        interruptThread = true,
        reason = "another attempt succeeded")
    }
    if (!successful(index)) {
      tasksSuccessful += 1
      logInfo(s"Finished task ${info.id} in stage ${taskSet.id} (TID ${info.taskId}) in" +
        s" ${info.duration} ms on ${info.host} (executor ${info.executorId})" +
        s" ($tasksSuccessful/$numTasks)")
      // Mark successful and stop if all the tasks have succeeded.
      successful(index) = true
      // 如果tasksSuccessful等于numTasks，则说明该TaskSet中所以的tasks能执行成功咯，
      // 则我们可以让TaskSetManager自我毁灭啦。
      if (tasksSuccessful == numTasks) {
        isZombie = true
      }
    } else {
      // TODO QUESTION 在什么情况下，一个task会成功两次???
      logInfo("Ignoring task-finished event for " + info.id + " in stage " + taskSet.id +
        " because task " + index + " has already completed successfully")
    }
    // 原来如此，厉害啊！！！
    // This method is called by "TaskSchedulerImpl.handleSuccessfulTask" which holds the
    // "TaskSchedulerImpl" lock until exiting. To avoid the SPARK-7655 issue, we should not
    // "deserialize" the value when holding a lock to avoid blocking other threads. So we call
    // "result.value()" in "TaskResultGetter.enqueueSuccessfulTask" before reaching here.
    // Note: "result.value()" only deserializes the value when it's called at the first time, so
    // here "result.value()" just returns the value and won't block other threads.
    // TODO read taskEnded()
    sched.dagScheduler.taskEnded(tasks(index), Success, result.value(), result.accumUpdates, info)
    // TODO read maybeFinishTaskSet()
    maybeFinishTaskSet()
  }

  /**
   * Marks the task as failed, re-adds it to the list of pending tasks, and notifies the
   * DAG Scheduler.
   */
  def handleFailedTask(tid: Long, state: TaskState, reason: TaskFailedReason) {
    val info = taskInfos(tid)
    // 如果该task已经fail或kill了，则return(no-op)
    if (info.failed || info.killed) {
      return
    }
    // 删除该正在运行的task
    removeRunningTask(tid)
    // 标记该task为finish状态（Failed or Killed，或者什么都不是???）
    info.markFinished(state, clock.getTimeMillis())
    val index = info.index
    // 该task正在running的副本数减1
    copiesRunning(index) -= 1
    var accumUpdates: Seq[AccumulatorV2[_, _]] = Seq.empty
    val failureReason = s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid, ${info.host}," +
      s" executor ${info.executorId}): ${reason.toErrorString}"
    val failureException: Option[Throwable] = reason match {
      case fetchFailed: FetchFailed =>
        logWarning(failureReason)
        if (!successful(index)) {
          successful(index) = true
          tasksSuccessful += 1
        }
        isZombie = true

        if (fetchFailed.bmAddress != null) {
          blacklistTracker.foreach(_.updateBlacklistForFetchFailure(
            fetchFailed.bmAddress.host, fetchFailed.bmAddress.executorId))
        }

        None

      case ef: ExceptionFailure =>
        // ExceptionFailure's might have accumulator updates
        accumUpdates = ef.accums
        if (ef.className == classOf[NotSerializableException].getName) {
          // If the task result wasn't serializable, there's no point in trying to re-execute it.
          logError("Task %s in stage %s (TID %d) had a not serializable result: %s; not retrying"
            .format(info.id, taskSet.id, tid, ef.description))
          abort("Task %s in stage %s (TID %d) had a not serializable result: %s".format(
            info.id, taskSet.id, tid, ef.description))
          return
        }
        val key = ef.description
        val now = clock.getTimeMillis()
        val (printFull, dupCount) = {
          if (recentExceptions.contains(key)) {
            val (dupCount, printTime) = recentExceptions(key)
            if (now - printTime > EXCEPTION_PRINT_INTERVAL) {
              recentExceptions(key) = (0, now)
              (true, 0)
            } else {
              recentExceptions(key) = (dupCount + 1, printTime)
              (false, dupCount + 1)
            }
          } else {
            recentExceptions(key) = (0, now)
            (true, 0)
          }
        }
        if (printFull) {
          logWarning(failureReason)
        } else {
          logInfo(
            s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid) on ${info.host}, executor" +
              s" ${info.executorId}: ${ef.className} (${ef.description}) [duplicate $dupCount]")
        }
        ef.exception

      case e: ExecutorLostFailure if !e.exitCausedByApp =>
        logInfo(s"Task $tid failed because while it was being computed, its executor " +
          "exited for a reason unrelated to the task. Not counting this failure towards the " +
          "maximum number of failures for the task.")
        None

      case e: TaskFailedReason =>  // TaskResultLost, TaskKilled, and others
        logWarning(failureReason)
        None
    }
    // 通知dagSheduler taskend 事件，这样listener就会知道有个task结束了
    sched.dagScheduler.taskEnded(tasks(index), reason, null, accumUpdates, info)

    if (!isZombie && reason.countTowardsTaskFailures) {
      assert (null != failureReason)
      // TODO read
      taskSetBlacklistHelperOpt.foreach(_.updateBlacklistForFailedTask(
        info.host, info.executorId, index, failureReason))
      numFailures(index) += 1
      if (numFailures(index) >= maxTaskFailures) {
        logError("Task %d in stage %s failed %d times; aborting job".format(
          index, taskSet.id, maxTaskFailures))
        abort("Task %d in stage %s failed %d times, most recent failure: %s\nDriver stacktrace:"
          .format(index, taskSet.id, maxTaskFailures, failureReason), failureException)
        return
      }
    }

    if (successful(index)) {
      logInfo(s"Task ${info.id} in stage ${taskSet.id} (TID $tid) failed, but the task will not" +
        s" be re-executed (either because the task failed with a shuffle data fetch failure," +
        s" so the previous stage needs to be re-run, or because a different copy of the task" +
        s" has already succeeded).")
    } else {
      addPendingTask(index)
    }

    maybeFinishTaskSet()
  }

  // 终止该task set（这意味着一个stage的abort???）
  def abort(message: String, exception: Option[Throwable] = None): Unit = sched.synchronized {
    // TODO: Kill running tasks if we were not terminated due to a Mesos error
    sched.dagScheduler.taskSetFailed(taskSet, message, exception)
    isZombie = true
    maybeFinishTaskSet()
  }

  /** If the given task ID is not in the set of running tasks, adds it.
   *
   * Used to keep track of the number of running tasks, for enforcing scheduling policies.
   */
  def addRunningTask(tid: Long) {
    if (runningTasksSet.add(tid) && parent != null) {
      parent.increaseRunningTasks(1)
    }
  }

  /** If the given task ID is in the set of running tasks, removes it. */
  def removeRunningTask(tid: Long) {
    if (runningTasksSet.remove(tid) && parent != null) {
      parent.decreaseRunningTasks(1)
    }
  }

  override def getSchedulableByName(name: String): Schedulable = {
    null
  }

  override def addSchedulable(schedulable: Schedulable) {}

  override def removeSchedulable(schedulable: Schedulable) {}

  override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]()
    sortedTaskSetQueue += this
    sortedTaskSetQueue
  }

  /**
   * 当一个executor失联的时候被TaskScheduler调用，这样我们就可以将我们的tasks重新入队（之后就会
   * 被分配到其它的executor去咯～这应该就算是一种容错机制吧）
   * Called by TaskScheduler when an executor is lost so we can re-enqueue our tasks
   */
  override def executorLost(execId: String, host: String, reason: ExecutorLossReason) {
    // 对于shuffle map stage，我们需要将该stage中的所有在该executor上执行的tasks resubmit
    // （不管该task正在running，还是已经finish），因为会有下一个stage来读取上一个stage的tasks
    // 写出的partition数据。而此时，该tasks的executor已经失联了，所以下一个stage也就获取不到数据了。
    // 而对于其它的类型的task（比如，ResultTask），如果它已经finish了，则我们已经获取到了数据，就不用
    // resubmit了。而如果它的状态还是running，说明该task的结果是返回不回来了。所以，我们需要resubmit它。

    // Re-enqueue any tasks that ran on the failed executor if this is a shuffle map stage,
    // and we are not using an external shuffle server which could serve the shuffle outputs.
    // The reason is the next stage wouldn't be able to fetch the data from this dead executor
    // so we would need to rerun these tasks on other executors.
    // 如果该taskSet中的task是ShuffleMapTask，且我们没有启用外部的shuffle服务，且TaskSetManager处于活跃状态
    if (tasks(0).isInstanceOf[ShuffleMapTask] && !env.blockManager.externalShuffleServiceEnabled
        && !isZombie) {
      // （我以前总是在想，一个task在一个机器上已经挂了，怎么重新让它重新再
      // 到其他的机器上去运行呢，原来就是这样的：保存了一份TaskSet的meta信息。
      // 厉害啊！）
      // 从taskInfos中获取在该已经失联的executor上运行的task的信息
      for ((tid, info) <- taskInfos if info.executorId == execId) {
        val index = taskInfos(tid).index
        // 我们只处理已经成功的task。因为如果该task还没成功，到时候肯定会有其它地方来继续处理它。
        if (successful(index) && !killedByOtherAttempt(index)) {
          successful(index) = false
          copiesRunning(index) -= 1
          tasksSuccessful -= 1
          // 将该task加入到pending队列中
          addPendingTask(index)
          // Tell the DAGScheduler that this task was resubmitted so that it doesn't think our
          // stage finishes when a total of tasks.size tasks finish.
          sched.dagScheduler.taskEnded(
            tasks(index), Resubmitted, null, Seq.empty, info)
        }
      }
    }
    // 对于ResultTask，该task正在running，我们才能resubmit（原因见上述注释）
    for ((tid, info) <- taskInfos if info.running && info.executorId == execId) {
      // 判断executor的失联是否是由app造成的。
      val exitCausedByApp: Boolean = reason match {
        case exited: ExecutorExited => exited.exitCausedByApp
        case ExecutorKilled => false
        case _ => true
      }
      // 处理failed task
      handleFailedTask(tid, TaskState.FAILED, ExecutorLostFailure(info.executorId, exitCausedByApp,
        Some(reason.toString)))
    }
    // recalculate valid locality levels and waits when executor is lost
    recomputeLocality()
  }

  /**
   * Check for tasks to be speculated and return true if there are any. This is called periodically
   * by the TaskScheduler.
   *
   */
  override def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean = {
    // Can't speculate if we only have one task, and no need to speculate if the task set is a
    // zombie.
    if (isZombie || numTasks == 1) {
      return false
    }
    var foundTasks = false
    val minFinishedForSpeculation = (SPECULATION_QUANTILE * numTasks).floor.toInt
    logDebug("Checking for speculative tasks: minFinished = " + minFinishedForSpeculation)

    if (tasksSuccessful >= minFinishedForSpeculation && tasksSuccessful > 0) {
      val time = clock.getTimeMillis()
      val medianDuration = successfulTaskDurations.median
      val threshold = max(SPECULATION_MULTIPLIER * medianDuration, minTimeToSpeculation)
      // TODO: Threshold should also look at standard deviation of task durations and have a lower
      // bound based on that.
      logDebug("Task length threshold for speculation: " + threshold)
      for (tid <- runningTasksSet) {
        val info = taskInfos(tid)
        val index = info.index
        if (!successful(index) && copiesRunning(index) == 1 && info.timeRunning(time) > threshold &&
          !speculatableTasks.contains(index)) {
          logInfo(
            "Marking task %d in stage %s (on %s) as speculatable because it ran more than %.0f ms"
              .format(index, taskSet.id, info.host, threshold))
          speculatableTasks += index
          sched.dagScheduler.speculativeTaskSubmitted(tasks(index))
          foundTasks = true
        }
      }
    }
    foundTasks
  }

  private def getLocalityWait(level: TaskLocality.TaskLocality): Long = {
    val defaultWait = conf.get(config.LOCALITY_WAIT)
    val localityWaitKey = level match {
      case TaskLocality.PROCESS_LOCAL => "spark.locality.wait.process"
      case TaskLocality.NODE_LOCAL => "spark.locality.wait.node"
      case TaskLocality.RACK_LOCAL => "spark.locality.wait.rack"
      case _ => null
    }

    if (localityWaitKey != null) {
      conf.getTimeAsMs(localityWaitKey, defaultWait.toString)
    } else {
      0L
    }
  }

  /**
   * Compute the locality levels used in this TaskSet. Assumes that all tasks have already been
   * added to queues using addPendingTask.
   *
   */
  private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY}
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    if (!pendingTasksForExecutor.isEmpty &&
        pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive(_))) {
      levels += PROCESS_LOCAL
    }
    if (!pendingTasksForHost.isEmpty &&
        pendingTasksForHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) {
      levels += NODE_LOCAL
    }
    if (!pendingTasksWithNoPrefs.isEmpty) {
      levels += NO_PREF
    }
    if (!pendingTasksForRack.isEmpty &&
        pendingTasksForRack.keySet.exists(sched.hasHostAliveOnRack(_))) {
      levels += RACK_LOCAL
    }
    levels += ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray
  }

  def recomputeLocality() {
    val previousLocalityLevel = myLocalityLevels(currentLocalityIndex)
    myLocalityLevels = computeValidLocalityLevels()
    localityWaits = myLocalityLevels.map(getLocalityWait)
    currentLocalityIndex = getLocalityIndex(previousLocalityLevel)
  }

  def executorAdded() {
    recomputeLocality()
  }
}

private[spark] object TaskSetManager {
  // The user will be warned if any stages contain a task that has a serialized size greater than
  // this.
  val TASK_SIZE_TO_WARN_KB = 100
}
