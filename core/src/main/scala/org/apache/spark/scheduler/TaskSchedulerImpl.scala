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
import java.util.{Locale, Timer, TimerTask}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.Set
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, ThreadUtils, Utils}

/**
 * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
 * It can also work with a local setup by using a `LocalSchedulerBackend` and setting
 * isLocal to true. It handles common logic, like determining a scheduling order across jobs, waking
 * up to launch speculative tasks, etc.
 *
 * Clients should first call initialize() and start(), then submit task sets through the
 * runTasks method.
 *
 * THREADING: [[SchedulerBackend]]s and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * [[SchedulerBackend]]s synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.
 */
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false)
  extends TaskScheduler with Logging {

  import TaskSchedulerImpl._

  def this(sc: SparkContext) = {
    this(sc, sc.conf.get(config.MAX_TASK_FAILURES))
  }

  // 黑名单tracker ？？？
  // Lazily initializing blackListTrackOpt to avoid getting empty ExecutorAllocationClient,
  // because ExecutorAllocationClient is created after this TaskSchedulerImpl.
  private[scheduler] lazy val blacklistTrackerOpt = maybeCreateBlacklistTracker(sc)

  val conf = sc.conf

  // spark speculative策略：如果一个task执行时间超过了MIN_TIME_TO_SPECULATION（单位）设置的时间，就会在其它executor上启动一
  // 个相同的task，如果该task率先执行完毕，则原先的task就会被禁止继续执行。
  // How often to check for speculative tasks
  // 检测speculative task的时间间隔，默认100ms
  val SPECULATION_INTERVAL_MS = conf.getTimeAsMs("spark.speculation.interval", "100ms")

  // Duplicate copies of a task will only be launched if the original copy has been running for
  // at least this amount of time. This is to avoid the overhead of launching speculative copies
  // of tasks that are very short.
  val MIN_TIME_TO_SPECULATION = 100

  private val speculationScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")

  // Threshold above which we warn user initial TaskSet may be starved
  val STARVATION_TIMEOUT_MS = conf.getTimeAsMs("spark.starvation.timeout", "15s")

  // CPUs to request per task
  // 执行一个task需要的cpu个数
  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  // TODO read：TaskSetManager seems especially important
  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.
  // 一个stage可以对应多个attempt的TaskSet，但是处于active状态的taskSet只能有一个！
  // 第一个Int表示stage id，第二个Int表示stageAttemptId
  private val taskSetsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, TaskSetManager]]

  // Protected by `this`
  private[scheduler] val taskIdToTaskSetManager = new HashMap[Long, TaskSetManager]
  val taskIdToExecutorId = new HashMap[Long, String]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer(true)

  // Incrementing task IDs
  val nextTaskId = new AtomicLong(0)

  // IDs of the tasks running on each executor
  private val executorIdToRunningTaskIds = new HashMap[String, HashSet[Long]]

  // 返回每个executor上runningTask的个数
  def runningTasksByExecutors: Map[String, Int] = synchronized {
    executorIdToRunningTaskIds.toMap.mapValues(_.size)
  }

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn(怎么翻译？？？) is used to decide when we can attain data locality on a given host
  // 记录每个节点上的executor集合。可以用来计算hostsAlive，也可以用来判断是否可以在给定的节点上获取本地数据
  protected val hostToExecutors = new HashMap[String, HashSet[String]]

  // 一个机架上有哪些节点？？？
  protected val hostsByRack = new HashMap[String, HashSet[String]]

  // 每个executorId对应的节点
  protected val executorIdToHost = new HashMap[String, String]

  // 哈？
  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null

  var backend: SchedulerBackend = null

  // mapOutput跟踪器？
  val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]

  private var schedulableBuilder: SchedulableBuilder = null
  // scheduler调度策略
  // default scheduler is FIFO
  private val schedulingModeConf = conf.get(SCHEDULER_MODE_PROPERTY, SchedulingMode.FIFO.toString)
  val schedulingMode: SchedulingMode =
    try {
      SchedulingMode.withName(schedulingModeConf.toUpperCase(Locale.ROOT))
    } catch {
      case e: java.util.NoSuchElementException =>
        throw new SparkException(s"Unrecognized $SCHEDULER_MODE_PROPERTY: $schedulingModeConf")
    }

  val rootPool: Pool = new Pool("", schedulingMode, 0, 0)

  // This is a var so that we can reset it for testing purposes.
  // 反序列化和远程获取（如果有必要）task的结果
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  def initialize(backend: SchedulerBackend) {
    this.backend = backend
    // TODO read schedulableBuilder
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported $SCHEDULER_MODE_PROPERTY: " +
          s"$schedulingMode")
      }
    }
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  // 在local模式下，相当于只启动backend(start()会在SparkContext中被调用)
  override def start() {
    backend.start()

    // 如果是非local模式运行且设置了speculation策略，则开启speculationScheduler线程，用来定时检查是否有speculative tasks
    if (!isLocal && conf.getBoolean("spark.speculation", false)) {
      logInfo("Starting speculative execution thread")
      speculationScheduler.scheduleWithFixedDelay(new Runnable {
        override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
          checkSpeculatableTasks()
        }
      }, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }

  override def postStartHook() {
    waitBackendReady()
  }

  override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    this.synchronized {
      // 创建TaskSetManager
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      // 获取该taskSet对应的stage id
      val stage = taskSet.stageId
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
      stageTaskSets(taskSet.stageAttemptId) = manager
      val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
        ts.taskSet != taskSet && !ts.isZombie
      }
      // 在一个stage多个attempt的taskSet中，处于active状态的taskSet只能有一个
      if (conflictingTaskSet) {
        throw new IllegalStateException(s"more than one active taskSet for stage $stage:" +
          s" ${stageTaskSets.toSeq.map{_._2.taskSet.id}.mkString(",")}")
      }
      // 添加该TaskSetManager到调度树中
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      hasReceivedTask = true
    }
    backend.reviveOffers()
  }

  // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
  private[scheduler] def createTaskSetManager(
      taskSet: TaskSet,
      maxTaskFailures: Int): TaskSetManager = {
    new TaskSetManager(this, taskSet, maxTaskFailures, blacklistTrackerOpt)
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    // 从这里可知，一个stage可能有多个attempts同时在运行???
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        // There are two possible cases here:
        // 1. The task set manager has been created and some tasks have been scheduled.
        //    In this case, send a kill signal to the executors to kill the task and then abort
        //    the stage.
        // 2. The task set manager has been created but no tasks has been scheduled. In this case,
        //    simply abort the stage.
        // 如果该TaskSetManager中有正在运行的tasks，则先将它们kill掉
        tsm.runningTasksSet.foreach { tid =>
            taskIdToExecutorId.get(tid).foreach(execId =>
              backend.killTask(tid, execId, interruptThread, reason = "Stage cancelled"))
        }
        // 中止该TaskSetManager，相当于就是中止该stage
        tsm.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
  }

  override def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
    logInfo(s"Killing task $taskId: $reason")
    val execId = taskIdToExecutorId.get(taskId)
    if (execId.isDefined) {
      backend.killTask(taskId, execId.get, interruptThread, reason)
      true
    } else {
      logWarning(s"Could not kill task $taskId because no task with that ID was found.")
      false
    }
  }

  /**
   * Called to indicate that all task attempts (including speculated tasks) associated with the
   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
   * cleaned up.
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    taskSetsByStageIdAndAttempt.get(manager.taskSet.stageId).foreach { taskSetsForStage =>
      taskSetsForStage -= manager.taskSet.stageAttemptId
      if (taskSetsForStage.isEmpty) {
        taskSetsByStageIdAndAttempt -= manager.taskSet.stageId
      }
    }
    manager.parent.removeSchedulable(manager)
    logInfo(s"Removed TaskSet ${manager.taskSet.id}, whose tasks have all completed, from pool" +
      s" ${manager.parent.name}")
  }

  // 这个函数的作用：把一个TaskSet均衡地分布到各个offer上，
  // 但一次调用可能只有若干个任务成功分配，之后会多次调用该函数，
  // 来尝试任务分配(但应该不能保证TaskSet的所有任务都分配成功)
  private def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      tasks: IndexedSeq[ArrayBuffer[TaskDescription]]) : Boolean = {
    var launchedTask = false
    // 到此为止，整个应用中，加入黑名单的节点或executor已经过滤掉了
    // nodes and executors that are blacklisted for the entire application have already been
    // filtered out by this point

    // 依次遍历每个offer，尝试在该offer分配一个任务
    for (i <- 0 until shuffledOffers.size) {
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      // 该work offer的可用cpu核数至少大于等于CPUS_PER_TASK，
      // 这样可以保证至少能够分配一个任务
      if (availableCpus(i) >= CPUS_PER_TASK) {
        try {
          // 应该是...每次最多只返回一个Task，有可能返回空(比如,TaskSet中所有task都
          // 已经分配到了各自的offer，而还有多余的offers)
          // TODO 可不可以这样优化：当TaskSet已经分配完所有任务后，而offer还没遍历完，
          // 则提前跳出循环???
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetManager(tid) = taskSet
            taskIdToExecutorId(tid) = execId
            executorIdToRunningTaskIds(execId).add(tid)
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    } // end for: 一次for循环最多成功分配shuffledOffers.size个任务到每个offer上
    // 那么，如果TaskSet.size大于shuffledOffers.size(即还有未分配完的任务)...
    return launchedTask
  }

  /**
   * 集群管理者调用该方法来为slave节点提供资源。我们通过以任务的优先级顺序询问我们处于active状态的task sets来作
   * 为响应。我们通过轮询的方式，让每个节点都能分配到任务，以此来平衡整个集群的负载。
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  def resourceOffers(offers: IndexedSeq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each slave as alive and remember its hostname
    // Also track if new executor is added
    var newExecAvail = false
    for (o <- offers) {
      if (!hostToExecutors.contains(o.host)) {
        hostToExecutors(o.host) = new HashSet[String]()
      }
      // 如果executorIdToRunningTaskIds不包含该executorId，说明有
      // 新的executor("新"是相对于TaskSchedulerImpl来说的，并不是说
      // 该executor刚刚才创建)
      if (!executorIdToRunningTaskIds.contains(o.executorId)) {
        hostToExecutors(o.host) += o.executorId
        // 通知DAGScheduler有新的executor添加，并做相应处理
        executorAdded(o.executorId, o.host)
        executorIdToHost(o.executorId) = o.host
        executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
        // 确认有新的executor添加
        newExecAvail = true
      }
      // 根据host的机架信息，更新hostsByRack
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }

    // Before making any offers, remove any nodes from the blacklist whose blacklist has expired. Do
    // this here to avoid a separate thread and added synchronization overhead, and also because
    // updating the blacklist is only relevant when task offers are being made.
    // TODO read
    blacklistTrackerOpt.foreach(_.applyBlacklistTimeout())

    // 过滤黑名单中的节点或executor
    val filteredOffers = blacklistTrackerOpt.map { blacklistTracker =>
      offers.filter { offer =>
        !blacklistTracker.isNodeBlacklisted(offer.host) &&
          !blacklistTracker.isExecutorBlacklisted(offer.executorId)
      }
    }.getOrElse(offers)

    // 随机打乱offers，避免重复在同一个节点上分配任务，以达到集群整体的负载均衡
    val shuffledOffers = shuffleOffers(filteredOffers)
    // Build a list of tasks to assign to each worker.
    // o.cores / CPUS_PER_TASK表示每个节点最多能分配的任务个数(那么，有可能等于0咯)
    // So，tasks.size就是shuffledOffers的size
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores / CPUS_PER_TASK))
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    // sorted -> FIFO or FAIR
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      // 如果发现有新的executor可用，需要重新计算locality
      // TODO read
      if (newExecAvail) {
        taskSet.executorAdded()
      }
    }

    // 按照我们的调度顺序依次遍历每个TaskSet，然后以locality level的递增顺序来提供给它每个节点(这个翻译...还是
    // 看英文的吧...)，由此让它能有机会在所有节点上发起本地任务。
    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    // 首先按照调度策略(FIFO FAIR)遍历每个TaskSet
    for (taskSet <- sortedTaskSets) {
      var launchedAnyTask = false
      var launchedTaskAtCurrentMaxLocality = false
      // 然后遍历该TaskSet的myLocalityLevels
      // TODO 可能的优化：如果TaskSet已经在某个locality分配完了所有任务，
      // 就可以提前跳出循环了吧
      for (currentMaxLocality <- taskSet.myLocalityLevels) {
        do {
          // 在当前Locality下，resourceOfferSingleTaskSet尝试把TaskSet的任务
          // 分配到shuffledOffers上；如果在当前Locality下，已经没有可以分配的任务，
          // 则尝试下一个Locality，重复上一步操作。
          launchedTaskAtCurrentMaxLocality = resourceOfferSingleTaskSet(
            taskSet, currentMaxLocality, shuffledOffers, availableCpus, tasks)
          launchedAnyTask |= launchedTaskAtCurrentMaxLocality
        } while (launchedTaskAtCurrentMaxLocality)
      } // end for：这个for循环加里面的while循环，能保证成功分配完
      // TaskSet中的所有任务到offers上吗???(貌似不能啊)
      if (!launchedAnyTask) {
        taskSet.abortIfCompletelyBlacklisted(hostToExecutors)
      }
    }

    // tasks初始化后，它的size不就是大于0的吗???
    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }

  /**
   * Shuffle offers around to avoid always placing tasks on the same workers.  Exposed to allow
   * overriding in tests, so it can be deterministic.
   */
  protected def shuffleOffers(offers: IndexedSeq[WorkerOffer]): IndexedSeq[WorkerOffer] = {
    Random.shuffle(offers)
  }

  // 这个是TaskSchedulerImpl的statusUpdate()。（backend也有该方法）
  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var failedExecutor: Option[String] = None
    var reason: Option[ExecutorLossReason] = None
    synchronized {
      try {
        // 找到该task对应的TaskSetManager
        taskIdToTaskSetManager.get(tid) match {
          case Some(taskSet) =>
            if (state == TaskState.LOST) {
              // TaskState.LOST is only used by the deprecated Mesos fine-grained scheduling mode,
              // where each executor corresponds to a single task, so mark the executor as failed.
              val execId = taskIdToExecutorId.getOrElse(tid, throw new IllegalStateException(
                "taskIdToTaskSetManager.contains(tid) <=> taskIdToExecutorId.contains(tid)"))
              if (executorIdToRunningTaskIds.contains(execId)) {
                reason = Some(
                  SlaveLost(s"Task $tid was lost, so marking the executor as lost as well."))
                // 移除该executor
                removeExecutor(execId, reason.get)
                failedExecutor = Some(execId)
              }
            }
            // 该Task的状态标志属于结束中的一种（FINISHED, FAILED, KILLED, LOST）
            if (TaskState.isFinished(state)) {
              // 清除该task在该TaskScheduler中的信息（其实本质上就是删除
              // 几个map结构中的该id）
              cleanupTaskState(tid)
              // 从TaskSetManager的runningTasks中删除该task
              taskSet.removeRunningTask(tid)
              // 该task的状态为FINISHED，说明该task正常结束，
              // 则我们通过taskResultGetter来反序列化或远程获取该task的执行结果
              // （如果我们的serializedData是DirectTaskResult则直接反序列化；如果是
              // IndirectTaskResult，则通过BlockManager远程获取）
              if (state == TaskState.FINISHED) {
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                // 如果task的状态标记是FAILED、KILLED、LOST里的一种，
                // 就以FailedTask交由TaskResultGetter来处理
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                "likely the result of receiving duplicate task finished status updates) or its " +
                "executor has been marked as failed.")
                .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // TODO read 如果发现有failed executor
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    if (failedExecutor.isDefined) {
      assert(reason.isDefined)
      dagScheduler.executorLost(failedExecutor.get, reason.get)
      backend.reviveOffers()
    }
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  override def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId): Boolean = {
    // (taskId, stageId, stageAttemptId, accumUpdates)
    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = synchronized {
      accumUpdates.flatMap { case (id, updates) =>
        val accInfos = updates.map(acc => acc.toInfo(Some(acc.value), None))
        taskIdToTaskSetManager.get(id).map { taskSetMgr =>
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accInfos)
        }
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId)
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskResult: DirectTaskResult[_]): Unit = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  // 注意：TaskSchedulerImpl和TaskSetManager都有handleFailedTask()
  // 事实上，TaskSchedulerImpl的handleFailedTask()又把主要工作交给了
  // TaskSetManager的handleFailedTask()来处理了
  def handleFailedTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskState: TaskState,
      reason: TaskFailedReason): Unit = synchronized {
                                        // 注意这里是加同步锁的
    taskSetManager.handleFailedTask(tid, taskState, reason)
    // 如果taskSetManager还没死，且该task在任何attempts中还没有成功过
    if (!taskSetManager.isZombie && !taskSetManager.someAttemptSucceeded(tid)) {
      // TODO read
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  def error(message: String) {
    synchronized {
      if (taskSetsByStageIdAndAttempt.nonEmpty) {
        // Have each task set throw a SparkException with the error
        for {
          attempts <- taskSetsByStageIdAndAttempt.values
          manager <- attempts.values
        } {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        throw new SparkException(s"Exiting due to error from cluster scheduler: $message")
      }
    }
  }

  override def stop() {
    speculationScheduler.shutdown()
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    starvationTimer.cancel()
  }

  override def defaultParallelism(): Int = backend.defaultParallelism()

  // TODO mark to read
  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks() {
    //  revive: 复活（很形象的变量名）
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks(MIN_TIME_TO_SPECULATION)
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    var failedExecutor: Option[String] = None

    synchronized {
      if (executorIdToRunningTaskIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logExecutorLoss(executorId, hostPort, reason)
        removeExecutor(executorId, reason)
        failedExecutor = Some(executorId)
      } else {
        executorIdToHost.get(executorId) match {
          case Some(hostPort) =>
            // If the host mapping still exists, it means we don't know the loss reason for the
            // executor. So call removeExecutor() to update tasks running on that executor when
            // the real loss reason is finally known.
            logExecutorLoss(executorId, hostPort, reason)
            removeExecutor(executorId, reason)

          case None =>
            // We may get multiple executorLost() calls with different loss reasons. For example,
            // one may be triggered by a dropped connection from the slave while another may be a
            // report of executor termination from Mesos. We produce log messages for both so we
            // eventually report the termination reason.
            logError(s"Lost an executor $executorId (already removed): $reason")
        }
      }
    }
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get, reason)
      backend.reviveOffers()
    }
  }

  override def workerRemoved(workerId: String, host: String, message: String): Unit = {
    logInfo(s"Handle removed worker $workerId: $message")
    dagScheduler.workerRemoved(workerId, host, message)
  }

  private def logExecutorLoss(
      executorId: String,
      hostPort: String,
      reason: ExecutorLossReason): Unit = reason match {
    case LossReasonPending =>
      logDebug(s"Executor $executorId on $hostPort lost, but reason not yet known.")
    case ExecutorKilled =>
      logInfo(s"Executor $executorId on $hostPort killed by driver.")
    case _ =>
      logError(s"Lost executor $executorId on $hostPort: $reason")
  }

  /**
   * 总之，要把该task从TaskScheduler里删除干净
   * Cleans up the TaskScheduler's state for tracking the given task.
   */
  private def cleanupTaskState(tid: Long): Unit = {
    // 从taskIdToTaskSetManager中删除该task
    taskIdToTaskSetManager.remove(tid)
    // 从taskIdToExecutorId删除该task
    taskIdToExecutorId.remove(tid).foreach { executorId =>
      // 从executorIdToRunningTaskIds中删除该task
      executorIdToRunningTaskIds.get(executorId).foreach { _.remove(tid) }
    }
  }

  /**
   * 从我们所有的数据结构中删除该executor，并且标记它为失联。
   * Remove an executor from all our data structures and mark it as lost. If the executor's loss
   * reason is not yet known, do not yet remove its association with its host nor update the status
   * of any running tasks, since the loss reason defines whether we'll fail those tasks.
   */
  private def removeExecutor(executorId: String, reason: ExecutorLossReason) {
    // 因为executor已经失联，所以在该executor上的tasks不再会发送任何状态更新的信息，
    // 因此，它们可以在这里清除。
    // The tasks on the lost executor may not send any more status updates (because the executor
    // has been lost), so they should be cleaned up here.
    executorIdToRunningTaskIds.remove(executorId).foreach { taskIds =>
      logDebug("Cleaning up TaskScheduler state for tasks " +
        s"${taskIds.mkString("[", ",", "]")} on failed executor $executorId")
      // We do not notify the TaskSetManager of the task failures because that will
      // happen below in the rootPool.executorLost() call.
      // 从TaskSchedulerImpl中清理干净这些tasks
      taskIds.foreach(cleanupTaskState)
    }

    val host = executorIdToHost(executorId)
    val execs = hostToExecutors.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      hostToExecutors -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }

    // 如果executor失联的具体原因还不知道
    if (reason != LossReasonPending) {
      executorIdToHost -= executorId
      rootPool.executorLost(executorId, host, reason)
    }
    // TODO read
    blacklistTrackerOpt.foreach(_.handleRemovedExecutor(executorId))
  }

  def executorAdded(execId: String, host: String) {
    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    hostToExecutors.get(host).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    hostToExecutors.contains(host)
  }

  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.contains(rack)
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.contains(execId)
  }

  def isExecutorBusy(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.get(execId).exists(_.nonEmpty)
  }

  /**
   * Get a snapshot of the currently blacklisted nodes for the entire application.  This is
   * thread-safe -- it can be called without a lock on the TaskScheduler.
   */
  def nodeBlacklist(): scala.collection.immutable.Set[String] = {
    blacklistTrackerOpt.map(_.nodeBlacklist()).getOrElse(scala.collection.immutable.Set())
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None

  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    while (!backend.isReady) {
      // Might take a while for backend to be ready if it is waiting on resources.
      if (sc.stopped.get) {
        // For example: the master removes the application for some reason
        throw new IllegalStateException("Spark context stopped while waiting for backend")
      }
      synchronized {
        this.wait(100)
      }
    }
  }

  override def applicationId(): String = backend.applicationId()

  override def applicationAttemptId(): Option[String] = backend.applicationAttemptId()

  private[scheduler] def taskSetManagerForAttempt(
      stageId: Int,
      stageAttemptId: Int): Option[TaskSetManager] = {
    for {
      attempts <- taskSetsByStageIdAndAttempt.get(stageId)
      manager <- attempts.get(stageAttemptId)
    } yield {
      manager
    }
  }

}


private[spark] object TaskSchedulerImpl {

  val SCHEDULER_MODE_PROPERTY = "spark.scheduler.mode"

  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used. The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given {@literal <h1, [o1, o2, o3]>}, {@literal <h2, [o4]>} and
   * {@literal <h3, [o5, o6]>}, returns {@literal [o1, o5, o4, o2, o6, o3]}.
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.getOrElse(key, null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size) {
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }

  // TODO read
  private def maybeCreateBlacklistTracker(sc: SparkContext): Option[BlacklistTracker] = {
    if (BlacklistTracker.isBlacklistEnabled(sc.conf)) {
      val executorAllocClient: Option[ExecutorAllocationClient] = sc.schedulerBackend match {
        case b: ExecutorAllocationClient => Some(b)
        case _ => None
      }
      Some(new BlacklistTracker(sc, executorAllocClient))
    } else {
      None
    }
  }

}
