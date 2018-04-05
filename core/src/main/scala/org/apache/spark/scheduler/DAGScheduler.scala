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
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.Map
import scala.collection.mutable.{ArrayStack, HashMap, HashSet}
import scala.concurrent.duration._
import scala.language.existentials
import scala.language.postfixOps
import scala.util.control.NonFatal

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat
import org.apache.spark.util._

/**
 * The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of
 * stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a
 * minimal schedule to run the job. It then submits stages as TaskSets to an underlying
 * TaskScheduler implementation that runs them on the cluster. A TaskSet contains fully independent
 * tasks that can run right away based on the data that's already on the cluster (e.g. map output
 * files from previous stages), though it may fail if this data becomes unavailable.
 *
 * Spark stages are created by breaking the RDD graph at shuffle boundaries. RDD operations with
 * "narrow" dependencies, like map() and filter(), are pipelined together into one set of tasks
 * in each stage, but operations with shuffle dependencies require multiple stages (one to write a
 * set of map output files, and another to read those files after a barrier). In the end, every
 * stage will have only shuffle dependencies on other stages, and may compute multiple operations
 * inside it. The actual pipelining of these operations happens in the RDD.compute() functions of
 * various RDDs
 *
 * In addition to coming up with a DAG of stages, the DAGScheduler also determines the preferred
 * locations to run each task on, based on the current cache status, and passes these to the
 * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
 * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
 * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
 * a small number of times before cancelling the whole stage.
 *
 * When looking through this code, there are several key concepts:
 *
 *  - Jobs (represented by [[ActiveJob]]) are the top-level work items submitted to the scheduler.
 *    For example, when the user calls an action, like count(), a job will be submitted through
 *    submitJob. Each Job may require the execution of multiple stages to build intermediate data.
 *
 *  - Stages ([[Stage]]) are sets of tasks that compute intermediate results in jobs, where each
 *    task computes the same function on partitions of the same RDD. Stages are separated at shuffle
 *    boundaries, which introduce a barrier (where we must wait for the previous stage to finish to
 *    fetch outputs). There are two types of stages: [[ResultStage]], for the final stage that
 *    executes an action, and [[ShuffleMapStage]], which writes map output files for a shuffle.
 *    Stages are often shared across multiple jobs, if these jobs reuse the same RDDs.
 *
 *  - Tasks are individual units of work, each sent to one machine.
 *
 *  - Cache tracking: the DAGScheduler figures out which RDDs are cached to avoid recomputing them
 *    and likewise remembers which shuffle map stages have already produced output files to avoid
 *    redoing the map side of a shuffle.
 *
 *  - Preferred locations: the DAGScheduler also computes where to run each task in a stage based
 *    on the preferred locations of its underlying RDDs, or the location of cached or shuffle data.
 *
 *  - Cleanup: all data structures are cleared when the running jobs that depend on them finish,
 *    to prevent memory leaks in a long-running application.
 *
 * To recover from failures, the same stage might need to run multiple times, which are called
 * "attempts". If the TaskScheduler reports that a task failed because a map output file from a
 * previous stage was lost, the DAGScheduler resubmits that lost stage. This is detected through a
 * CompletionEvent with FetchFailed, or an ExecutorLost event. The DAGScheduler will wait a small
 * amount of time to see whether other nodes or tasks fail, then resubmit TaskSets for any lost
 * stage(s) that compute the missing tasks. As part of this process, we might also have to create
 * Stage objects for old (finished) stages where we previously cleaned up the Stage object. Since
 * tasks from the old attempt of a stage could still be running, care must be taken to map any
 * events received in the correct Stage object.
 *
 * Here's a checklist to use when making or reviewing changes to this class:
 *
 *  - All data structures should be cleared when the jobs involving them end to avoid indefinite
 *    accumulation of state in long-running programs.
 *
 *  - When adding a new data structure, update `DAGSchedulerSuite.assertDataStructuresEmpty` to
 *    include the new structure. This will help to catch memory leaks.
 */
private[spark]
class DAGScheduler(
    private[scheduler] val sc: SparkContext,
    private[scheduler] val taskScheduler: TaskScheduler,
    listenerBus: LiveListenerBus,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv,
    clock: Clock = new SystemClock())
  extends Logging {

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  private[spark] val metricsSource: DAGSchedulerSource = new DAGSchedulerSource(this)

  private[scheduler] val nextJobId = new AtomicInteger(0)
  private[scheduler] def numTotalJobs: Int = nextJobId.get()
  private val nextStageId = new AtomicInteger(0)

  // 一个job对应的所有stage
  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
  /**
   * Mapping from shuffle dependency ID to the ShuffleMapStage that will generate the data for
   * that dependency. Only includes stages that are part of currently running job (when the job(s)
   * that require the shuffle stage complete, the mapping will be removed, and the only record of
   * the shuffle data will be in the MapOutputTracker).
   */
  private[scheduler] val shuffleIdToMapStage = new HashMap[Int, ShuffleMapStage]
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]

  // Stages we need to run whose parents aren't done
  private[scheduler] val waitingStages = new HashSet[Stage]

  // Stages we are running right now
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  private[scheduler] val failedStages = new HashSet[Stage]

  private[scheduler] val activeJobs = new HashSet[ActiveJob]

  /**
   * Contains the locations that each RDD's partitions are cached on.  This map's keys are RDD ids
   * and its values are arrays indexed by partition numbers. Each array value is the set of
   * locations where that RDD partition is cached.
   *
   * All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).
   */
  private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  private val failedEpoch = new HashMap[String, Long]

  private [scheduler] val outputCommitCoordinator = env.outputCommitCoordinator

  // A closure serializer that we reuse.
  // This is only safe because DAGScheduler runs in a single thread.
  // 是JavaSerializerInstance类型的
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem. */
  private val disallowStageRetryForTest = sc.getConf.getBoolean("spark.test.noStageRetry", false)

  /**
   * Whether to unregister all the outputs on the host in condition that we receive a FetchFailure,
   * this is set default to false, which means, we only unregister the outputs related to the exact
   * executor(instead of the host) on a FetchFailure.
   */
  private[scheduler] val unRegisterOutputOnHostOnFetchFailure =
    sc.getConf.get(config.UNREGISTER_OUTPUT_ON_HOST_ON_FETCH_FAILURE)

  /**
   * 一个stage在aborted之前最多可连续失败的次数，默认4次
   * Number of consecutive stage attempts allowed before a stage is aborted.
   */
  private[scheduler] val maxConsecutiveStageAttempts =
    sc.getConf.getInt("spark.stage.maxConsecutiveAttempts",
      DAGScheduler.DEFAULT_MAX_CONSECUTIVE_STAGE_ATTEMPTS)

  private val messageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")

  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  taskScheduler.setDAGScheduler(this)

  /**
   * Called by the TaskSetManager to report task's starting.
   */
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  /**
   * Called by the TaskSetManager to report that a task has completed
   * and results are being fetched remotely.
   */
  def taskGettingResult(taskInfo: TaskInfo) {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }

  /**
   * Called by the TaskSetManager to report task completions or failures.
   */
  def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Seq[AccumulatorV2[_, _]],
      taskInfo: TaskInfo): Unit = {
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, taskInfo))
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  def executorHeartbeatReceived(
      execId: String,
      // (taskId, stageId, stageAttemptId, accumUpdates)
      accumUpdates: Array[(Long, Int, Int, Seq[AccumulableInfo])],
      blockManagerId: BlockManagerId): Boolean = {
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, accumUpdates))
    blockManagerMaster.driverEndpoint.askSync[Boolean](
      BlockManagerHeartbeat(blockManagerId), new RpcTimeout(600 seconds, "BlockManagerHeartbeat"))
  }

  /**
   * Called by TaskScheduler implementation when an executor fails.
   */
  def executorLost(execId: String, reason: ExecutorLossReason): Unit = {
    eventProcessLoop.post(ExecutorLost(execId, reason))
  }

  /**
   * Called by TaskScheduler implementation when a worker is removed.
   */
  def workerRemoved(workerId: String, host: String, message: String): Unit = {
    eventProcessLoop.post(WorkerRemoved(workerId, host, message))
  }

  /**
   * Called by TaskScheduler implementation when a host is added.
   */
  def executorAdded(execId: String, host: String): Unit = {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  /**
   * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
   * cancellation of the job itself.
   */
  def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit = {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
  }

  /**
   * Called by the TaskSetManager when it decides a speculative task is needed.
   */
  def speculativeTaskSubmitted(task: Task[_]): Unit = {
    eventProcessLoop.post(SpeculativeTaskSubmitted(task))
  }

  private[scheduler]
  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = cacheLocs.synchronized {
    // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
    if (!cacheLocs.contains(rdd.id)) {
      // 默认的StorageLevel是不是就是NONE??? see RDD#L1658
      // Note: if the storage level is NONE, we don't need to get locations from block manager.
      val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
        IndexedSeq.fill(rdd.partitions.length)(Nil)
      } else {
        // 创建RDDBlockId
        // RDDBlockId所包含的信息只是告知了这是哪个rdd，以及这是该rdd的哪个分片
        val blockIds =
          rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
        // 再通过请求master节点，获取location信息（这里的请求应该是master到master吧）
        blockManagerMaster.getLocations(blockIds).map { bms =>
          bms.map(bm => TaskLocation(bm.host, bm.executorId))
        }
      }
      cacheLocs(rdd.id) = locs
    }
    cacheLocs(rdd.id)
  }

  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()
  }

  /**
   * 如果一个shuffle map stage已经在shuffleIdToMapStage存在了，则直接返回。否则，
   * 会先为该ShuffleDependency所有的未在shuffleIdToMapStage中注册的祖先ShuffleDependencies
   * 创建shuffle map stage，然后再为该ShuffleDependency自己，创建shuffle map stage。
   * Gets a shuffle map stage if one exists in shuffleIdToMapStage. Otherwise, if the
   * shuffle map stage doesn't already exist, this method will create the shuffle map stage in
   * addition to any missing ancestor shuffle map stages.
   */
  private def getOrCreateShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) =>
        stage

      case None =>
        // Create stages for all missing ancestor shuffle dependencies.
        // 返回该rdd的所有祖先（不再是直接）ShuffleDependencies
        // 那shuffle dependency的遍历顺序是？？？可能又是类似于树的先序遍历？？？
        getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          // Even though getMissingAncestorShuffleDependencies only returns shuffle dependencies
          // that were not already in shuffleIdToMapStage, it's possible that by the time we
          // get to a particular dependency in the foreach loop, it's been added to
          // shuffleIdToMapStage by the stage creation process for an earlier dependency. See
          // SPARK-13902 for more information.
          if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
            createShuffleMapStage(dep, firstJobId)
          }
        }
        // Finally, create a stage for the given shuffle dependency.
        createShuffleMapStage(shuffleDep, firstJobId)
    }
  }

  /**
   * Creates a ShuffleMapStage that generates the given shuffle dependency's partitions. If a
   * previously run stage generated the same shuffle data, this function will copy the output
   * locations that are still available from the previous shuffle to avoid unnecessarily
   * regenerating data.
   */
  def createShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], jobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val numTasks = rdd.partitions.length
    // 虽然这一步又调用了getOrCreateParentStages(), 但是感觉在ResultStage创建时，调用的那一次，
    // 应该已经把所有的stage都创建好了。在这里只会有get的过程，没有create的过程了。不知道我理解的对不对？？？
    // [[理解的不对！]] “这里”其实属于创建ResultStage的一部分，所以，不可能所有的Stage创建好了
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ShuffleMapStage(
      id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep, mapOutputTracker)

    stageIdToStage(id) = stage
    shuffleIdToMapStage(shuffleDep.shuffleId) = stage
    // 到这一步，整个DAG图的所有Stage已经划分好了 --> 又不对！只能说，
    // 包括该rdd及该rdd所有祖先在内的构成的子DAG图的stage已经划分好了

    // 划分完之后，再更新stage和该JobId的依赖关系
    // 这样看来，在创建ResultStage的末尾再调用updateJobIdStageIdMaps，
    // 是不是其(result stage)父类的stage大都已经建立和JobId之间的依赖关系？？？
    // 或者说，其实最后只剩下了result stage（也就是final stage）自己还没建立和job的依赖关系？？？
    updateJobIdStageIdMaps(jobId, stage)

    if (!mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      // 向MapOutputTrackerMaster注册该shuffle
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    stage
  }

  /**
   * Create a ResultStage associated with the provided jobId.
   * 创建ResultStage，在一个完整的Job中，只有一个ResultStage，就是action触发阶段所在的stage，其它都是ShuffleMapStage
   */
  private def createResultStage(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage = {
    // 对于SparkPi，parents为Nil？？？因为只有OnetoOneDependency，
    // 没有ShuffleDependency啊（为什么debug断点进不来: 重新编译一下源码，恢复正常）
    // debug后发现，对于SparkPi，parents确实为Nil
    /**
     * getOrCreateParentStages()只会返回该rdd一层关系内(或直接)的stage依赖
     * 同时，getOrCreateParentStages()执行完后，所有Stage就划分好了
     */
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
    stageIdToStage(id) = stage
    // 这里update的是不是只有该Stage自己？？？
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

  /**
   * Get or create the list of parent stages for a given RDD.  The new Stages will be created with
   * the provided firstJobId.
   */
  private def getOrCreateParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    // getShuffleDependencies(rdd)只会获取和该rdd直接关联（详见该函数的说明）的ShuffleDependency
    getShuffleDependencies(rdd).map { shuffleDep =>
      getOrCreateShuffleMapStage(shuffleDep, firstJobId)
    }.toList
  }

  // 这个函数会通过getShuffleDependencies()一层一层往上找到所有还未在shuffleIdToMapStage里注册的ShuffleDependency
  /** Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet */
  private def getMissingAncestorShuffleDependencies(
      rdd: RDD[_]): ArrayStack[ShuffleDependency[_, _, _]] = {
    val ancestors = new ArrayStack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // 哪里manually了？这样就算是manually？
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ArrayStack[RDD[_]]
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.pop()
      if (!visited(toVisit)) {
        visited += toVisit
        /**
         * TODO 感觉这边可以把rdd之间的"直接shuffle dependency关系"保存起来(比如Map[id, HashSet[sdep]])，
         * 这样，在createShuffleMapStage#L359时，就不用再重新找直接地shuffleDependencies了。
         */
        getShuffleDependencies(toVisit).foreach { shuffleDep =>
          if (!shuffleIdToMapStage.contains(shuffleDep.shuffleId)) {
            ancestors.push(shuffleDep)
            waitingForVisit.push(shuffleDep.rdd)
          } // Otherwise, the dependency and its ancestors have already been registered.
        }
      }
    }
    ancestors
  }

  /**
   * 我觉得对于spark源码newbie，对于“immediate parents”的理解可能会有问题（我一开始就没理解对）
   * 假设rdd之间存在这样的依赖关系(shf: ShuffleDependency， oto: OneToOneDep)：
   *         a <-shf- b <- oto- <- c
   *                              /
   * f <-shf- <- e <-oto- d <- shf
   * 那么，最终，parents变量会返回ShuffleDependency(a), ShuffleDependency(d)。而不包括ShuffleDependency(f)。
   * 也就是说，OneToOneDependency并不影响"直接性"。
   *
   * Returns shuffle dependencies that are immediate parents of the given RDD.
   *
   * This function will not return more distant ancestors.  For example, if C has a shuffle
   * dependency on B which has a shuffle dependency on A:
   *
   * A <-- B <-- C
   *
   * calling this function with rdd C will only return the B <-- C dependency.
   *
   * This function is scheduler-visible for the purpose of unit testing.
   */
  private[scheduler] def getShuffleDependencies(
      rdd: RDD[_]): HashSet[ShuffleDependency[_, _, _]] = {
    // 虽然定义了HashSet，但是最多只会存储一个parent的吧，可能是为了之后使用算子操作（map）？？？不对！
    val parents = new HashSet[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new ArrayStack[RDD[_]]
    // BFS即视感
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.pop()
      if (!visited(toVisit)) {
        visited += toVisit
        toVisit.dependencies.foreach {
          case shuffleDep: ShuffleDependency[_, _, _] =>
            // 按照注释的意思，shuffleDep应该只有一个
            // 这样，我们是否可以认为，一个RDD最多只有一个ShuffleDependency呢？ 不对，理解有问题，看函数说明
            parents += shuffleDep
          case dependency =>
            waitingForVisit.push(dependency.rdd)
        }
      }
    }
    parents
  }

  // 获取未提交的parent stages
  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ArrayStack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        // TODO read
        // rdd有未被缓存的分区？？？
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        if (rddHasUncachedPartitions) {
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                // 为什么又是一个rdd一个rdd的往上找，而不是一个stage一个Stage的往上找？难道下面真的有可能创建新的Stage吗？
                // TODO：在这里还会有创建stage的情况吗？如果有，在什么情形下呢？
                val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
                // 如果该stage还未完成？？则加入missing
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  /**
   * Registers the given jobId among the jobs that need the given stage and
   * all of that stage's ancestors.
   */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    @tailrec
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parentsWithoutThisJobId = s.parents.filter { ! _.jobIds.contains(jobId) }
        /** 为什么要加tail？？？
         * 答：不熟悉scala语法导致迟迟看不懂代码逻辑。在scala中，List的tail表示去除第一个元素后的剩余元素List
         * 而last才是获取List的最后一个元素。
         * 所以这个函数的递归逻辑应该是这样的：
         * 假设有这样的一个DAG：D依赖C，C依赖B和G，B依赖A和F（字母表示一个stage）
         * A <- B <- C <- D
         *     /    /
         *   F    G
         * 那么，如果D先进来，处理完D，得到D的parentsWithoutThisJobId为{C}（假设这里所有的parents的jobIds都不包含
         * 传进来的jobId），那么下一个处理的Stage就是C，然后对应的parentsWithoutThisJobId为{B, G}。再接下来，处理B，
         * 得到对应的parentsWithoutThisJobId为{A, F, G}。以此类推，接下来分别是：A->{F, G}，F->{G}, G->{}。
         * 可以发现，如果把这个DAG图左旋90度当做一颗"树"来看的话，那么，上述处理Stage的过程就是对树做先序遍历的过程。
         * 最终的结果是，这个DAG图（或树）上的所有节点，都加入了该JobId。
         *
         * 如果在上述过程中，发现B已经包含了该jobId，然后B及其父节点（A，F）就不会被继续处理？
         * 那么，A和F是否会遗漏该jobId？？？
         * 结论是不会，我们可以从该函数的逻辑确定，如果子节点拥有某个JobId，那么，其父节点也必定会包含该jobId。
         */
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }

  /**
   * Removes state for job and any stages that are not needed by any other job.  Does not
   * handle cancelling tasks or notifying the SparkListener about finished jobs/stages/tasks.
   *
   * @param job The job whose state to cleanup.
   */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit = {
    // 获取该job的注册stages（其实就是由该job划分出来的stages）
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
              .format(job.jobId, stageId))
          } else {
            // 清理和该stage相关的数据结构
            def removeStage(stageId: Int) {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleIdToMapStage.find(_._2 == stage)) {
                  shuffleIdToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) { // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    // 清理和该job相关的数据结构
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage match {
      case r: ResultStage => r.removeActiveJob()
      case m: ShuffleMapStage => m.removeActiveJob(job)
    }
  }

  /**
   * Submit an action job to the scheduler.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @return a JobWaiter object that can be used to block until the job finishes executing
   *         or can be used to cancel the job.
   *
   * @throws IllegalArgumentException when partitions ids are illegal
   */
  def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    waiter
  }

  /**
   * Run an action job on the given RDD and pass all the results to the resultHandler function as
   * they arrive.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @note Throws `Exception` when the job fails
   */
  def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit = {
    val start = System.nanoTime
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    ThreadUtils.awaitReady(waiter.completionFuture, Duration.Inf)
    waiter.completionFuture.value.get match {
      case scala.util.Success(_) =>
        logInfo("Job %d finished: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case scala.util.Failure(exception) =>
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }

  /**
   * Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
   * as they arrive. Returns a partial result object from the evaluator.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param evaluator `ApproximateEvaluator` to receive the partial results
   * @param callSite where in the user program this job was called
   * @param timeout maximum time to wait for the job, in milliseconds
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: CallSite,
      timeout: Long,
      properties: Properties): PartialResult[R] = {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.partitions.length).toArray
    val jobId = nextJobId.getAndIncrement()
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions, callSite, listener, SerializationUtils.clone(properties)))
    listener.awaitResult()    // Will throw an exception if the job fails
  }

  /**
   * Submit a shuffle map stage to run independently and get a JobWaiter object back. The waiter
   * can be used to block until the job finishes executing or can be used to cancel the job.
   * This method is used for adaptive query planning, to run map stages and look at statistics
   * about their outputs before submitting downstream stages.
   *
   * @param dependency the ShuffleDependency to run a map stage for
   * @param callback function called with the result of the job, which in this case will be a
   *   single MapOutputStatistics object showing how much data was produced for each partition
   * @param callSite where in the user program this job was submitted
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
  def submitMapStage[K, V, C](
      dependency: ShuffleDependency[K, V, C],
      callback: MapOutputStatistics => Unit,
      callSite: CallSite,
      properties: Properties): JobWaiter[MapOutputStatistics] = {

    val rdd = dependency.rdd
    val jobId = nextJobId.getAndIncrement()
    if (rdd.partitions.length == 0) {
      throw new SparkException("Can't run submitMapStage on RDD with 0 partitions")
    }

    // We create a JobWaiter with only one "task", which will be marked as complete when the whole
    // map stage has completed, and will be passed the MapOutputStatistics for that stage.
    // This makes it easier to avoid race conditions between the user code and the map output
    // tracker that might result if we told the user the stage had finished, but then they queries
    // the map output tracker and some node failures had caused the output statistics to be lost.
    val waiter = new JobWaiter(this, jobId, 1, (i: Int, r: MapOutputStatistics) => callback(r))
    eventProcessLoop.post(MapStageSubmitted(
      jobId, dependency, callSite, waiter, SerializationUtils.clone(properties)))
    waiter
  }

  /**
   * Cancel a job that is running or waiting in the queue.
   */
  def cancelJob(jobId: Int, reason: Option[String]): Unit = {
    logInfo("Asked to cancel job " + jobId)
    eventProcessLoop.post(JobCancelled(jobId, reason))
  }

  /**
   * Cancel all jobs in the given job group ID.
   */
  def cancelJobGroup(groupId: String): Unit = {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessLoop.post(JobGroupCancelled(groupId))
  }

  /**
   * Cancel all jobs that are running or waiting in the queue.
   */
  def cancelAllJobs(): Unit = {
    eventProcessLoop.post(AllJobsCancelled)
  }

  private[scheduler] def doCancelAllJobs() {
    // Cancel all running jobs.
    runningStages.map(_.firstJobId).foreach(handleJobCancellation(_,
      Option("as part of cancellation of all jobs")))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
  }

  /**
   * Cancel all jobs associated with a running or scheduled stage.
   */
  def cancelStage(stageId: Int, reason: Option[String]) {
    eventProcessLoop.post(StageCancelled(stageId, reason))
  }

  /**
   * Kill a given task. It will be retried.
   *
   * @return Whether the task was successfully killed.
   */
  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
    taskScheduler.killTaskAttempt(taskId, interruptThread, reason)
  }

  /**
   * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
   * the last fetch failure.
   */
  private[scheduler] def resubmitFailedStages() {
    if (failedStages.size > 0) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
        submitStage(stage)
      }
    }
  }

  /**
   * Check for waiting stages which are now eligible for resubmission.
   * Submits stages that depend on the given parent stage. Called when the parent stage completes
   * successfully.
   */
  private def submitWaitingChildStages(parent: Stage) {
    logTrace(s"Checking if any dependencies of $parent are now runnable")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    // 有个问题：有没有一个stage有多个parents的??? 如果有，那么，只要它的parents
    // 其中之一个stage结束就能开始执行了么???
    // 答：首先，一个stage肯定会有多个parents，从stage的parents变量为List类型就可以看出；
    // 其次，只有部分几个parent stages结束，就能直接提交该stage开始执行吗？
    // 事实上，并不可以。虽然我们在下面调用了submitStage(stage)。但是，看该方法里的代码发现，
    // 还是会先寻找这个stage的missing（没有执行完成的） parent stages执行。只有当该stage的
    // 所有parent stages都执行完成的时候，才能轮到该stage去执行（其中的tasks）。
    // （所以说，这个机制真的很完美！太厉害了！）

    // 在waitingStages中，找出parent stages包含该stage的stages。
    val childStages = waitingStages.filter(_.parents.contains(parent)).toArray
    // 从waitingStages移除这些将被提交的stages
    // 如果在submitStage()发现该stage还有未执行完成的parent stages，那么会先
    // 执行它的parent stages。然后，该stage又会重新被加入到waitingStages中。
    waitingStages --= childStages
    // 根据job的先后顺序（体现FIFO的调度策略），提交stage
    for (stage <- childStages.sortBy(_.firstJobId)) {
      // 提交该stage，不一定会真的执行。如果该stage还有为执行完的parent stages，那么会先
      // 执行它的parent stages。且该stage会重新被加入到waitingStages中。
      submitStage(stage)
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
  private def activeJobForStage(stage: Stage): Option[Int] = {
    // jobsThatUseStage包含了所有拥有该Stage的jobId，且有序排列，以保证job的有序执行（id小的先执行）
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    // 找出最早提交（find找到第一个匹配就结束）的包含该Stage且处于active状态的jobId
    jobsThatUseStage.find(jobIdToActiveJob.contains)
  }

  private[scheduler] def handleJobGroupCancelled(groupId: String) {
    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    val activeInGroup = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists {
        _.getProperty(SparkContext.SPARK_JOB_GROUP_ID) == groupId
      }
    }
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_,
        Option("part of cancelled job group %s".format(groupId))))
  }

  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo) {
    // Note that there is a chance that this task is launched after the stage is cancelled.
    // In that case, we wouldn't have the stage anymore in stageIdToStage.
    val stageAttemptId =
      stageIdToStage.get(task.stageId).map(_.latestInfo.attemptNumber).getOrElse(-1)
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))
  }

  private[scheduler] def handleSpeculativeTaskSubmitted(task: Task[_]): Unit = {
    listenerBus.post(SparkListenerSpeculativeTaskSubmitted(task.stageId))
  }

  // 处理TaskSetFailed事件
  private[scheduler] def handleTaskSetFailed(
      taskSet: TaskSet,
      reason: String,
      exception: Option[Throwable]): Unit = {
    // 也就是要abort该task set对应的stage
    stageIdToStage.get(taskSet.stageId).foreach { abortStage(_, reason, exception) }
  }

  private[scheduler] def cleanUpAfterSchedulerStop() {
    for (job <- activeJobs) {
      val error =
        new SparkException(s"Job ${job.jobId} cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      // The `toArray` here is necessary so that we don't iterate over `runningStages` while
      // mutating it.
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo) {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
  }

  // 处理作业提交
  private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    var finalStage: ResultStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      // 从finalRDD开始，倒着向前，构建出第一个Stage（从Job执行的角
      // 度看是最后一个运行的stage，所以必定为ResultStage）
      finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    // 根据finalStage构建Job
    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    // TODO：read清理缓存的location？？
    clearCacheLocs()
    logInfo("Got job %s (%s) with %d output partitions".format(
      job.jobId, callSite.shortForm, partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    // 从最后一个stage开始提交（并不意味着先执行），而是该函数会递归向上寻找parent stage，直到第一个stage
    submitStage(finalStage)
  }

  private[scheduler] def handleMapStageSubmitted(jobId: Int,
      dependency: ShuffleDependency[_, _, _],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    // Submitting this map stage might still require the creation of some parent stages, so make
    // sure that happens.
    var finalStage: ShuffleMapStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = getOrCreateShuffleMapStage(dependency, jobId)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got map stage job %s (%s) with %d output partitions".format(
      jobId, callSite.shortForm, dependency.rdd.partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.addActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)

    // If the whole stage has already finished, tell the listener and remove it
    if (finalStage.isAvailable) {
      markMapStageJobAsFinished(job, mapOutputTracker.getStatistics(dependency))
    }
  }

  /** Submits stage, but first recursively submits any missing parents. */
  // 提交stage，但是会先[递归]地提交任何遗漏的parent stage
  private def submitStage(stage: Stage) {
    // 该jobId为所有需要该stage且处于active状态的Job里面最早被提交的Job
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      // 如果该Stage既不是在等待parent stage执行完，又不是正在running，且不是需要被resubmit的，
      // 就尝试提交它（不一定提交，因为还要递归检查它的parent stage是否已经提交）
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        // 获取未执行的parent stage
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        // 如果没有parent stage了，就直接提交该stage中tasks
        if (missing.isEmpty) {
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          submitMissingTasks(stage, jobId.get)
        } else {
          for (parent <- missing) {
            // 递归寻找
            submitStage(parent)
          }
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
  }

  // TODO 修改注释
  /** Called when stage's parents are（not吧？？？）available and we can now do its task. */
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    logDebug("submitMissingTasks(" + stage + ")")

    // First figure out the indexes of partition ids to compute.
    // 首先获取到需要计算的(map端)分区（index或者说id吧）
    // （因为有可能该stage是第二次或第三次重新提交，所以我们通过findMissingPartitions
    // 来获取在前几次stage执行完成之后没有计算成功的parttions）
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
    // 获取该job对应的属性设置
    val properties = jobIdToActiveJob(jobId).properties

    // 标记该stage为running stage
    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    // TODO read outputCommitCoordinator
    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }
    // TODO re-read very important here
    // 在LearningExample这个例子中，得到的taskIdLocation是{(1, List()), (2, List())}
    // 居然是空的？？？那么，task在哪里执行呢？
    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    // 创建一个新的stage attempt
    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)

    // If there are tasks to execute, record the submission time of the stage. Otherwise,
    // post the even without the submission time, which indicates that this stage was
    // skipped.
    // 如果该Stage有task需要执行（partitions不为空？），就记录该Stage的提交时间。否则，
    // 在提交stage submit事件时，如果没有提交时间，则意味着该Stage跳过执行。
    if (partitionsToCompute.nonEmpty) {
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    }
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>
          JavaUtils.bufferToArray(
            closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
        case stage: ResultStage =>
          JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
      }
      // 时隔近半个月，终于又回到了这里！！！(讲道理，上面看过的都有点忘了...还好打了注释)
      // 一个短短的sc.broadcast()暗藏了多少玄机啊！！！叹为观止！
      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        // TODO read abortStage()
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    // 根据不同的类型的stage生成ShuffleMapTask或ResultTask
    // task的个数等于map端partition的个数
    val tasks: Seq[Task[_]] = try {
      // 序列化Task的统计信息
      val serializedTaskMetrics = closureSerializer.serialize(stage.latestInfo.taskMetrics).array()
      stage match {
        case stage: ShuffleMapStage =>
          stage.pendingPartitions.clear()
          partitionsToCompute.map { id =>
            // taskIdToLocations可能为空啊
            // locs是集群节点的位置（host、port）信息
            val locs = taskIdToLocations(id)
            // part是rdd的某个partitions
            val part = stage.rdd.partitions(id)
            // 更新pendingPartitions，说明该id对于的part正在计算中
            stage.pendingPartitions += id
            // 构建ShuffleMapTask
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptNumber,
              taskBinary, part, locs, properties, serializedTaskMetrics, Option(jobId),
              Option(sc.applicationId), sc.applicationAttemptId)
          }

        case stage: ResultStage =>
          partitionsToCompute.map { id =>
            // 注意：stage.partitions???stage.rdd.partitions???
            val p: Int = stage.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocations(id)
            //构建ResultTask
            new ResultTask(stage.id, stage.latestInfo.attemptNumber,
              taskBinary, part, locs, id, properties, serializedTaskMetrics,
              Option(jobId), Option(sc.applicationId), sc.applicationAttemptId)
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    if (tasks.size > 0) {
      logInfo(s"Submitting ${tasks.size} missing tasks from $stage (${stage.rdd}) (first 15 " +
        s"tasks are for partitions ${tasks.take(15).map(_.partitionId)})")
      // 最终通过TaskScheduler提交Task(多个Task封装成一个TaskSet)
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptNumber, jobId, properties))
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      markStageAsFinished(stage, None)

      val debugString = stage match {
        case stage: ShuffleMapStage =>
          s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})"
        case stage : ResultStage =>
          s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
      }
      logDebug(debugString)
      // 如果没有task需要执行，且该stage结束，则检查其它需要被执行的stage
      submitWaitingChildStages(stage)
    }
  }

  /**
   * Merge local values from a task into the corresponding accumulators previously registered
   * here on the driver.
   *
   * Although accumulators themselves are not thread-safe, this method is called only from one
   * thread, the one that runs the scheduling loop. This means we only handle one task
   * completion event at a time so we don't need to worry about locking the accumulators.
   * This still doesn't stop the caller from updating the accumulator outside the scheduler,
   * but that's not our problem since there's nothing we can do about that.
   */
  private def updateAccumulators(event: CompletionEvent): Unit = {
    // 获取结束的task
    val task = event.task
    // 获取该task所属的stage
    val stage = stageIdToStage(task.stageId)
    try {
      event.accumUpdates.foreach { updates =>
        val id = updates.id
        // Find the corresponding accumulator on the driver and update it
        val acc: AccumulatorV2[Any, Any] = AccumulatorContext.get(id) match {
          case Some(accum) => accum.asInstanceOf[AccumulatorV2[Any, Any]]
          case None =>
            throw new SparkException(s"attempted to access non-existent accumulator $id")
        }
        acc.merge(updates.asInstanceOf[AccumulatorV2[Any, Any]])
        // 如果updates为zero，也就没有更新的内容，那我们就不去管它
        // To avoid UI cruft, ignore cases where value wasn't updated
        if (acc.name.isDefined && !updates.isZero) {
          // acc是merge之后的AccumulatorV2
          stage.latestInfo.accumulables(id) = acc.toInfo(None, Some(acc.value))
          // taskInfo的accumulables存储了该task执行期间的每个阶段的更新信息
          event.taskInfo.setAccumulables(
            acc.toInfo(Some(updates.value), Some(acc.value)) +: event.taskInfo.accumulables)
        }
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Failed to update accumulators for task ${task.partitionId}", e)
    }
  }

  private def postTaskEnd(event: CompletionEvent): Unit = {
    val taskMetrics: TaskMetrics =
      if (event.accumUpdates.nonEmpty) {
        try {
          TaskMetrics.fromAccumulators(event.accumUpdates)
        } catch {
          case NonFatal(e) =>
            val taskId = event.taskInfo.taskId
            logError(s"Error when attempting to reconstruct metrics for task $taskId", e)
            null
        }
      } else {
        null
      }

    // TODO read
    listenerBus.post(SparkListenerTaskEnd(event.task.stageId, event.task.stageAttemptId,
      Utils.getFormattedClassName(event.task), event.reason, event.taskInfo, taskMetrics))
  }

  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
   */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task
    val taskId = event.taskInfo.id
    val stageId = task.stageId
    val taskType = Utils.getFormattedClassName(task)

    // TODO read
    outputCommitCoordinator.taskCompleted(
      stageId,
      task.partitionId,
      event.taskInfo.attemptNumber, // this is a task attempt number
      event.reason)

    // 很有可能该stage已经finish了
    if (!stageIdToStage.contains(task.stageId)) {
      // The stage may have already finished when we get this event -- eg. maybe it was a
      // speculative task. It is important that we send the TaskEnd event in any case, so listeners
      // are properly notified and can chose to handle it. For instance, some listeners are
      // doing their own accounting and if they don't get the task end event they think
      // tasks are still running when they really aren't.
      postTaskEnd(event)

      // Skip all the actions if the stage has been cancelled.
      return
    }

    // 获取该task所属的stage
    val stage = stageIdToStage(task.stageId)

    // Make sure the task's accumulators are updated before any other processing happens, so that
    // we can post a task end event before any jobs or stages are updated. The accumulators are
    // only updated in certain cases.
    event.reason match {
        // task成功地执行结束
      case Success =>
        task match {
            // TODO read task的类型为ResultTask
          case rt: ResultTask[_, _] =>
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              case Some(job) =>
                // Only update the accumulator once for each result task.
                if (!job.finished(rt.outputId)) {
                  updateAccumulators(event)
                }
              case None => // Ignore update if task's job has finished.
            }
            // 否则，task的类型为ShuffleMapTask
          case _ =>
            // 更新accumulators
            updateAccumulators(event)
        }
      case _: ExceptionFailure => updateAccumulators(event)
      case _ =>
    }
    // 通过ListenerBus提交task end事件
    postTaskEnd(event)

    event.reason match {
      case Success =>
        task match {
          case rt: ResultTask[_, _] =>
            // Cast to ResultStage here because it's part of the ResultTask
            // TODO Refactor this out to a function that accepts a ResultStage
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  job.finished(rt.outputId) = true
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(resultStage)
                    cleanupStateForJobAndIndependentStages(job)
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  try {
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Exception =>
                      // TODO: Perhaps we want to mark the resultStage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }
          // task的类型为ShuffleMapTask
          case smt: ShuffleMapTask =>
            // 获取该task所属的ShuffleMapStage
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            // 对于ShuffleMapTask，它的返回结果就是MapStatus
            val status = event.result.asInstanceOf[MapStatus]
            // 获取该status所属的executorId（location其实就是BlockManagerId）
            // executorId意味着我们知道了该task是在哪里完成的
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            // 如果该task的stage attempt id正是该stage的attempt number（这里的id和number是一样的，
            // 都表示第几次尝试），说明该task正是该stage现在想要的
            if (stageIdToStage(task.stageId).latestInfo.attemptNumber == task.stageAttemptId) {
              // 哈??? 括号里的注释什么意思????
              // This task was for the currently running attempt of the stage. Since the task
              // completed successfully from the perspective of the TaskSetManager, mark it as
              // no longer pending (the TaskSetManager may consider the task complete even
              // when the output needs to be ignored because the task's epoch is too small below.
              // In this case, when pending partitions is empty, there will still be missing
              // output locations, which will cause the DAGScheduler to resubmit the stage below.)
              // 说明该task对应的partition的计算任务已经完成啦
              shuffleStage.pendingPartitions -= task.partitionId
            }
            // TODO read Epoch相关
            if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              logInfo(s"Ignoring possibly bogus $smt completion from executor $execId")
            } else {
              // 该task的epoch被认可了。现在我们可以注册该task的output信息到mapOutputTracker上了
              // The epoch of the task is acceptable (i.e., the task was launched after the most
              // recent failure we're aware of for the executor), so mark the task's output as
              // available.
              mapOutputTracker.registerMapOutput(
                shuffleStage.shuffleDep.shuffleId, smt.partitionId, status)
              // 从该stage等待的pending partitions去除该task对应的partition（因为该task已经成功地完成
              // 了任务，并将其对应的partition信息注册到了mapOutputTracker中）。虽然该partition可能已经
              // 在上面被去除过了，但是也有可能还没有去除（比如，该task是在一个更早的stage attempt中执行的）
              // 这能够让stage在每个task的任意一个副本执行成功的时候，标记为结束，即使说当前处于活跃状态的stage
              // 仍然还有正在运行的tasks。（显然，我已经有每个分区的map output信息了，虽然还有task在运行，那
              // 也是多余的。）
              // Remove the task's partition from pending partitions. This may have already been
              // done above, but will not have been done yet in cases where the task attempt was
              // from an earlier attempt of the stage (i.e., not the attempt that's currently
              // running).  This allows the DAGScheduler to mark the stage as complete when one
              // copy of each task has finished successfully, even if the currently active stage
              // still has tasks running.
              shuffleStage.pendingPartitions -= task.partitionId
            }

            // 如果该stage是正在运行的stage，且该stage的pending partitions已经空了
            // 有个问题：既然一个stage要在上一个stage完成之后才能运行，那么，runningStage中会有
            // 多个不同阶段的stage吗???还是说只可能会有同一个stage的不同attempts在同时运行???
            // 不，还有一种可能，就是两个stage之间是那种"关系"，这就不需要谁等谁，可以同时运行。
            if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {
              // 则标记该stage为结束状态
              markStageAsFinished(shuffleStage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)

              // TODO read epoch相关
              // This call to increment the epoch may not be strictly necessary, but it is retained
              // for now in order to minimize the changes in behavior from an earlier version of the
              // code. This existing behavior of always incrementing the epoch following any
              // successful shuffle map stage completion may have benefits by causing unneeded
              // cached map outputs to be cleaned up earlier on executors. In the future we can
              // consider removing this call, but this will require some extra investigation.
              // See https://github.com/apache/spark/pull/17955/files#r117385673 for more details.
              mapOutputTracker.incrementEpoch()

              // 清除缓存的location???
              clearCacheLocs()

              // 从上面的if条件中可以看到，pending partitions已经为空了，
              // 而shuffleStage.isAvailable为false意味这numAvailableOutputs还是小于numPartitions，
              // 这是什么情况???为什么会这样???pending partitions不就意味着所有的tasks都已经成功完成了吗。
              // 而所有tasks都成功完成就会在上面的mapOutputTracker.registerMapOutput注册它的map output
              // 信息。所有tasks注册完成后，不就会让numAvailableOutputs等于numPartitions吗???
              // 在什么情况下，它俩会不等呢??? 还是说真的是代码逻辑的问题???
              if (!shuffleStage.isAvailable) {
                // 某些tasks执行失败了，让我们来重新提交该stage
                // Some tasks had failed; let's resubmit this shuffleStage.
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
                  ") because some of its tasks had failed: " +
                  shuffleStage.findMissingPartitions().mkString(", "))
                // 重新提交该stage（虽然，我们在上面已经标记为该stage结束了???）
                submitStage(shuffleStage)
              } else {
                // 标记任何在等待该stage的map-stage jobs为结束状态.(map-stage job好像是一种特殊
                // 的job，它可以单独的提交一个stage（该stage没有任何的依赖）来执行。而该stage执行完成，
                // 则该job也就完成了)
                // Mark any map-stage jobs waiting on this stage as finished
                if (shuffleStage.mapStageJobs.nonEmpty) {
                  // 获取该stage的reduce端的每个partition需要从map端拉取的数据size大小
                  val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
                  for (job <- shuffleStage.mapStageJobs) {
                    // TODO read
                    // 标记每个map-stage job为结束状态
                    markMapStageJobAsFinished(job, stats)
                  }
                }
                // 提交等待该stage结束的child stages
                submitWaitingChildStages(shuffleStage)
              }
            }
        }

      case Resubmitted =>
        logInfo("Resubmitted " + task + ", so marking it as still running")
        stage match {
          case sms: ShuffleMapStage =>
            // resubmitted的本质就是这么简单，就是要等待该分区的数据。牛逼啊！
            sms.pendingPartitions += task.partitionId

          case _ =>
            // TaskSerManagers只能resummitted ShuffleMapStage里的tasks（为啥???）
            assert(false, "TaskSetManagers should only send Resubmitted task statuses for " +
              "tasks in ShuffleMapStages.")
        }

        // 该task因为FetchFailed而失败
      case FetchFailed(bmAddress, shuffleId, mapId, reduceId, failureMessage) =>
        // 获取该task所属的stage
        val failedStage = stageIdToStage(task.stageId)
        // 获取该shuffle dependency对应的ShuffleMapStage
        /**
         * 该mapStage是failedStage的上一个stage???如果failedStage有多个parent stages呢???
         * 那么，mapStage会是多个吗???
         * 答：不可能的。虽然一个stage可能会有多个parent stages（从rdd的角度来看，比如下图，rdd5
         * 所在的stage有三个parent stages，于是rdd5会存储3个ShuffleDependency依赖，每个依赖的rdd
         * 又分别是rdd4、rdd1、rdd2）。但是，一个shuffle它只能对应一个stage！所以我们在这里获得mapStage
         * 只可能有一个！rdd5所在的stage，比如生成了9个tasks。这9个tasks的计算方法是一样，但是它们获取的
         * 数据源却不一样，有的可能通过shuffle1获取rdd1所在的stage的输出数据，而其它的则可能通过shuffle2
         * 或shuffle3（至于怎么划分的，需要看看DAGScheduler提交task的过程）。而不同的shuffle注定只对应了
         * 一个上层stage。
         * rdd4           rdd1            rdd2
         *   |              |               |
         * shuffle3       shuffle1        shuffle2
         *   |\             |              /|
         *   | \------------|-------------/ |
         *   |              |               |
         *  rdd6           rdd5            rdd3
         *
         *  job2           job3             job1
         *
         */
        val mapStage = shuffleIdToMapStage(shuffleId)

        if (failedStage.latestInfo.attemptNumber != task.stageAttemptId) {
          // 如果该task的stage attempt id比最新的stage attempt id小，则直接忽略该失败的
          // task。因为我们可能已经有更新的task在执行了（说不定新的task意识到了之前的问题，就
          // 成功了呢）。
          logInfo(s"Ignoring fetch failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ${failedStage.latestInfo.attemptNumber}) running")
        } else {
          // It is likely that we receive multiple FetchFailed for a single stage (because we have
          // multiple tasks running concurrently on different executors). In that case, it is
          // possible the fetch failure has already been handled by the scheduler.
          if (runningStages.contains(failedStage)) {
            logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
              s"due to a fetch failure from $mapStage (${mapStage.name})")
            // 标记failedStage以failureMessage结束（fail掉该stage）
            markStageAsFinished(failedStage, Some(failureMessage))
          } else {
            // 因为stage中的tasks是在多个executor上并发执行的。所以，很有可能在其它executor上执行的task，
            // 早早地就返回了一个FetchFailure，并且fail掉了该stage。
            logDebug(s"Received fetch failure from $task, but its from $failedStage which is no " +
              s"longer running")
          }

          // 更新该stage的fetchFailedAttemptIds
          // （要知道，stage始终是同一个stage，但是它可以发起多次attempts）
          failedStage.fetchFailedAttemptIds.add(task.stageAttemptId)
          // QUESTION：fetchFailedAttemptIds里存储的attempt ids必定是连续的吗???
          // 这不废话嘛，提出这问题也是脑子broken了。难道fetchFailedAttemptIds会存储
          // 比如：[0, 1, 3, 5]。那第id = 2次的attempt怎么了呢？只可能成功了啊。而且stage
          // 的某次attempt成功了，也不会有之后的其它attempts了。
          // 不不不！！！！ 上面的理解不对！ [0, 1, 3, 5]说不定也是可能的！
          // 如果该stage的连续失败次数已经超过阈值maxConsecutiveStageAttempts（默认4次），
          // 则中止该stage
          val shouldAbortStage =
            failedStage.fetchFailedAttemptIds.size >= maxConsecutiveStageAttempts ||
            disallowStageRetryForTest

          if (shouldAbortStage) {
            val abortMessage = if (disallowStageRetryForTest) {
              "Fetch failure will not retry stage due to testing config"
            } else {
              s"""$failedStage (${failedStage.name})
                 |has failed the maximum allowable number of
                 |times: $maxConsecutiveStageAttempts.
                 |Most recent failure reason: $failureMessage""".stripMargin.replaceAll("\n", " ")
            }
            // 注意abort一个stage和mark stage as finished非常不同！abort意味这该stage不能再执行了；
            // 而finish则可以之后再执行。finish并一定意味着成功啊。比如，我们上面对failedStage标记为finish了
            // 但是该stage又没有执行成功，我们之后还会resubmit它的。

            // 中止该stage（连带反应：和该stage相关的jobs）
            abortStage(failedStage, abortMessage, None)
          } else { // update failedStages and make sure a ResubmitFailedStages event is enqueued
            // TODO: Cancel running tasks in the failed stage -- cf. SPARK-17064
            val noResubmitEnqueued = !failedStages.contains(failedStage)
            // 把failedStage和mapStage加入到failedStages中，等待resubmit
            failedStages += failedStage
            failedStages += mapStage
            if (noResubmitEnqueued) {
              // We expect one executor failure to trigger many FetchFailures in rapid succession,
              // but all of those task failures can typically be handled by a single resubmission of
              // the failed stage.  We avoid flooding the scheduler's event queue with resubmit
              // messages by checking whether a resubmit is already in the event queue for the
              // failed stage.  If there is already a resubmit enqueued for a different failed
              // stage, that event would also be sufficient to handle the current failed stage, but
              // producing a resubmit for each failed stage makes debugging and logging a little
              // simpler while not producing an overwhelming number of scheduler events.
              logInfo(
                s"Resubmitting $mapStage (${mapStage.name}) and " +
                s"$failedStage (${failedStage.name}) due to fetch failure"
              )
              messageScheduler.schedule(
                new Runnable {
                  override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
                },
                DAGScheduler.RESUBMIT_TIMEOUT,
                TimeUnit.MILLISECONDS
              )
            }
          }
          // 我们无法获取该map stage的output，则我们把它在mapOutputTracker的注册
          // 信息清理掉（反正这些信息也不能帮我们正确的获取到数据，而我们之后resubmit的stage
          // 会重新向mapOutputTracker注册我们新的mapStage的信息。
          // Mark the map whose fetch failed as broken in the map stage
          if (mapId != -1) {
            mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
          }

          // TODO: mark the executor as failed only if there were lots of fetch failures on it
          if (bmAddress != null) {
            // 如果我们启用了外部shuffle服务，并且配置了"当有fetch failure发生时，则清除该host上
            // 所有executor的output信息"为true
            val hostToUnregisterOutputs = if (env.blockManager.externalShuffleServiceEnabled &&
              unRegisterOutputOnHostOnFetchFailure) {
              // We had a fetch failure with the external shuffle service, so we
              // assume all shuffle data on the node is bad.
              Some(bmAddress.host)
            } else {
              // Unregister shuffle data just for one executor (we don't have any
              // reason to believe shuffle data has been lost for the entire host).
              None
            }
            // TODO read
            removeExecutorAndUnregisterOutputs(
              execId = bmAddress.executorId,
              fileLost = true,
              hostToUnregisterOutputs = hostToUnregisterOutputs,
              maybeEpoch = Some(task.epoch))
          }
        }

      case commitDenied: TaskCommitDenied =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits

      case exceptionFailure: ExceptionFailure =>
        // Nothing left to do, already handled above for accumulator updates.

      case TaskResultLost =>
        // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case _: ExecutorLostFailure | _: TaskKilled | UnknownReason =>
        // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
        // will abort the job.
    }
  }

  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * We will also assume that we've lost all shuffle blocks associated with the executor if the
   * executor serves its own blocks (i.e., we're not using external shuffle), the entire slave
   * is lost (likely including the shuffle service), or a FetchFailed occurred, in which case we
   * presume all shuffle data related to this executor to be lost.
   *
   * Optionally the epoch during which the failure was caught can be passed to avoid allowing
   * stray fetch failures from possibly retriggering the detection of a node as lost.
   */
  private[scheduler] def handleExecutorLost(
      execId: String,
      workerLost: Boolean): Unit = {
    // if the cluster manager explicitly tells us that the entire worker was lost, then
    // we know to unregister shuffle output.  (Note that "worker" specifically refers to the process
    // from a Standalone cluster, where the shuffle service lives in the Worker.)
    val fileLost = workerLost || !env.blockManager.externalShuffleServiceEnabled
    removeExecutorAndUnregisterOutputs(
      execId = execId,
      fileLost = fileLost,
      hostToUnregisterOutputs = None,
      maybeEpoch = None)
  }

  private def removeExecutorAndUnregisterOutputs(
      execId: String,
      fileLost: Boolean,
      hostToUnregisterOutputs: Option[String],
      maybeEpoch: Option[Long] = None): Unit = {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    // TODO epoch啊epoch
    // 如果failedEpoch已经包含了execId，说明该executor之前挂掉过。
    // 然后呢？它后来又恢复了，现在又挂掉了???
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      // 更新该executor对应的epoch number
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      // TODO read（居然是）通过blockManagerMaster删除该executor
      blockManagerMaster.removeExecutor(execId)
      if (fileLost) {
        hostToUnregisterOutputs match {
          case Some(host) =>
            logInfo("Shuffle files lost for host: %s (epoch %d)".format(host, currentEpoch))
            // 删除在该host上的所有output信息
            mapOutputTracker.removeOutputsOnHost(host)
          case None =>
            logInfo("Shuffle files lost for executor: %s (epoch %d)".format(execId, currentEpoch))
            // 删除在该executor上的所有output信息
            mapOutputTracker.removeOutputsOnExecutor(execId)
        }
        // TODO read clearCacheLocs
        clearCacheLocs()

      } else {
        logDebug("Additional executor lost message for %s (epoch %d)".format(execId, currentEpoch))
      }
    }
  }

  /**
   * Responds to a worker being removed. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use workerRemoved() to post a loss event from outside.
   *
   * We will assume that we've lost all shuffle blocks associated with the host if a worker is
   * removed, so we will remove them all from MapStatus.
   *
   * @param workerId identifier of the worker that is removed.
   * @param host host of the worker that is removed.
   * @param message the reason why the worker is removed.
   */
  private[scheduler] def handleWorkerRemoved(
      workerId: String,
      host: String,
      message: String): Unit = {
    logInfo("Shuffle files lost for worker %s on host %s".format(workerId, host))
    mapOutputTracker.removeOutputsOnHost(host)
    clearCacheLocs()
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
  }

  private[scheduler] def handleStageCancellation(stageId: Int, reason: Option[String]) {
    stageIdToStage.get(stageId) match {
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray
        jobsThatUseStage.foreach { jobId =>
          val reasonStr = reason match {
            case Some(originalReason) =>
              s"because $originalReason"
            case None =>
              s"because Stage $stageId was cancelled"
          }
          handleJobCancellation(jobId, Option(reasonStr))
        }
      case None =>
        logInfo("No active jobs to kill for Stage " + stageId)
    }
  }

  private[scheduler] def handleJobCancellation(jobId: Int, reason: Option[String]) {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(
        jobIdToActiveJob(jobId), "Job %d cancelled %s".format(jobId, reason.getOrElse("")))
    }
  }

  /**
   * 标记该stage为结束状态，并把它从running stages列表中删除
   * Marks a stage as finished and removes it from the list of running stages.
   */
  private def markStageAsFinished(stage: Stage, errorMessage: Option[String] = None): Unit = {
    // 计算该stage的运行耗时
    val serviceTime = stage.latestInfo.submissionTime match {
      case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      // 记录该stage的完成时间（所以，上面的serviceTime就被抛弃了吗???反正是一个失败的stage，
      // 没多大用处吧）
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())

      // 既然该stage已经执行成功了，那么，就删除该stage其它几次attempts失败的信息。
      // 我们只会对一个stage的连续的多次失败有限制，而如果是在一个长时间运行的job中，如果该
      // stage（断断续续地）尝试了多次，那么，这些无关的失败最终也不会造成该stage的aborted。
      // Clear failure count for this stage, now that it's succeeded.
      // We only limit consecutive failures of stage attempts,so that if a stage is
      // re-used many times in a long-running job, unrelated failures don't eventually cause the
      // stage to be aborted.
      stage.clearFailures()
    } else {
      // 虽然stage结束了，但是是非正常的结束。
      // 标记该stage最近一次失败的原因
      stage.latestInfo.stageFailed(errorMessage.get)
      logInfo(s"$stage (${stage.name}) failed in $serviceTime s due to ${errorMessage.get}")
    }

    // TODO read outputCommitCoordinator相关
    outputCommitCoordinator.stageEnd(stage.id)
    // 通过listener bus提交stage完成的事件
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
    // 从runningStages中移除该stag
    runningStages -= stage
  }

  /**
   * 中止所有依赖于该stage的（且处于活跃状态的）jobs。
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  private[scheduler] def abortStage(
      failedStage: Stage,
      reason: String,
      exception: Option[Throwable]): Unit = {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    // 通过调用stageDependsOn()，来判断该jobs是否依赖failedStage
    // (感觉stageDependsOn()是一个非常耗时的方法，而且要对每个jobs都调用，能否改进???)
    // 答：并不需要改进！而且这是非常必要的。在stageDependsOn()方法能为activeJobs中job
    // 划分好所有的stages。而该信息会在failJobAndIndependentStages()被用到（在调用
    // failJobAndIndependentStages()必须保证一个job的stages已经划分好）。
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    // 记录该failedStage的完成时间
    // （感觉这部分及后面相关的代码有点"神奇"或者说"绕"？说说我的想法：虽然该方法的名字是abortStage，
    // 但是，真正和abort stage有点关联的好像就像下面的设置该stage的完成时间。而设置好完成时间并不意味
    // 该abort的行为就结束了。
    // 显然，如果我们要abort一个stage，那些使用了该stage的jobs则同样需要被fail掉（因为这些jobs的
    // 的某个stage注定无法完成了，所以该job的最终结果也是失败的）。问题的关键是，当我要fail掉那些jobs
    // 时，比如job-A，我们就需要考虑，由job-A划分出来的stages，是否只和job-A有关；因为也有可能另一个
    // job-B（不一定属于上述的jobs）依赖了job-A的某个stage，比如stage-1。所以，我们在fail掉job-A时，
    // 只能abort（在fail job时，我们也需要abort stages）那些只有被job-A所使用的stages，而对于像
    // stage-1这样的就不能被abort）
    // 综上所述，本方法要abort一个stage，但是只在这里记录了完成时间，但是，当我们继续fail那些依赖
    // 该stage的jobs的时候，如果只有一个job依赖它，那么它会被进一步fail掉（比如，cancel tasks；mark
    // as finish）。而如果有其它jobs依赖，则不会被fail。
    // 这。。。。感觉好悖论啊。。。一个stage要被abort掉，但是发现有多个jobs依赖它，然后它就只被记录了完成时间，
    // 而没有其它的操作（比如 cancel tasks， etc.）。
    // 但是无论如何，有一点是必然的，那就是和该stage相关联的jobs都会被fail掉！！！
    // （详见failJobAndIndependentStages()）
    // 疑问解决了，没有问题的。详见failJobAndIndependentStages#jobsForStage那边的注释。
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
    for (job <- dependentJobs) {
      // fail掉job以及只和该job有关的stages
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason", exception)
    }
    if (dependentJobs.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  /** Fails a job and all stages that are only used by that job, and cleans up relevant state. */
  private def failJobAndIndependentStages(
      job: ActiveJob,
      failureReason: String,
      exception: Option[Throwable] = None): Unit = {
    val error = new SparkException(failureReason, exception.getOrElse(null))
    var ableToCancelStages = true

    val shouldInterruptThread =
      if (job.properties == null) false
      else job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false").toBoolean

    // 取消和其它jobs没有关联的（只和该方法传参进来的job有关联的）且正在运行的stages
    // Cancel all independent, running stages.

    // 先获取该job划分出来的所有stages。
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    }
    stages.foreach { stageId =>
      // 获取和该stage关联的所有jobs
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        // 可能的改进 如果该jobsForStage中的jobs是我们要被fail掉的jobs，那么，虽然
        // 该stage被多个jobs依赖，但是fail掉它也没关系吧???
        // 答：针对上述改进，我花了（2018.4.4）一下午的时间来改进这点并且增加单元测试(see spark fork
        // 项目中的分支WRONG-dev-stage-should-be-failed-when-shared-by-multiple-failing-jobs
        // -only)。但是，我后来发现这个改进点是没有必要的（虽然改进的方式是没有问题的）。由此，我们继续来
        // 说说从abortStage()到调用该方法再到调用cleanupStateForJobAndIndependentStages()时发生的
        // 事情。之前，一直觉得，在abortStage()中，只记录了我们想要abort的stage的完成时间，而没有其它多余
        // 的操作，好像该stage最终也没有fail掉，但其实不是这样的（虽然那个完成时间的设置确实没什么必要）。
        // 假设我们现在要abort stage0，且stage0是被job1和job2使用。然后我们看到该方法中来，假如传参进来
        // 的是job1，对于stage0，jobsForStage包含job1和job2，而这两个job又都是因为abort stage0而导致
        // 的fail。现在，jobsForStage大于1，显然我们不能fail任何stage0。但是，我们最终还是可以fail job1。
        // 而fail job1之后会调用方法cleanupStateForJobAndIndependentStages，该方法会清理和job1相关的
        // 数据结构。这会导致，当下一次该方法传参进来的时候是job2时，对于stage0，jobsForStage只包含了job2
        // （因为我们在job1的cleanupStateForJobAndIndependentStages方法中去除了stage0对于job1的依赖）
        // 而此时jobsForStage的size就等于1了。则我们最终会fail stage0！其它的stage也是一样道理的。所以，
        // 那些只属于fail掉的jobs的stage最终也是会被fail掉的。

        if (!stageIdToStage.contains(stageId)) {
          logError(s"Missing Stage for stage with id $stageId")
        } else {
          // 说明该stage只有我们将要中止的这个job和它关联，如果它处于running状态，那么我们就可以中止它
          // This is the only job that uses this stage, so fail the stage if it is running.
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            try { // cancelTasks will fail if a SchedulerBackend does not implement killTask
              // TODO read 删除该stage中的所有tasks
              taskScheduler.cancelTasks(stageId, shouldInterruptThread)
              // 标记该stage为结束状态
              markStageAsFinished(stage, Some(failureReason))
            } catch {
              case e: UnsupportedOperationException =>
                logInfo(s"Could not cancel tasks for stage $stageId", e)
              ableToCancelStages = false
            }
          }
        }
      }
    }

    // 当该job中所有只和它相关的stages被fail掉之后，我们就可以来fail掉该job了
    if (ableToCancelStages) {
      // 首先需要清理掉和那些刚刚被fail掉的stages相关的数据结构(e.g. stageIdToStage、runningStages)以及
      // 和该job相关的数据结构(e.g. jobIdToStageIds, activeJobs)
      // SPARK-15783 important to cleanup state first, just for tests where we have some asserts
      // against the state.  Otherwise we have a *little* bit of flakiness in the tests.
      cleanupStateForJobAndIndependentStages(job)
      // 清理完上述states后，我们通知job的listener，该job fail了
      job.listener.jobFailed(error)
      // 将该job end事件上传到监听总线
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  /** Return true if one of stage's ancestors is target. */
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {
      return true
    }
    val visitedRdds = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ArrayStack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
              if (!mapStage.isAvailable) {
                waitingForVisit.push(mapStage.rdd)
              }  // Otherwise there's no need to follow the dependency back
            case narrowDep: NarrowDependency[_] =>
              waitingForVisit.push(narrowDep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    visitedRdds.contains(target.rdd)
  }

  /**
   * 获取一个特定RDD的某个分片（分区,翻译有点乱了。。。）的位置(locality翻译为“位置”，而不是“本地化”)信息
   * Gets the locality information associated with a partition of a particular RDD.
   *
   * This method is thread-safe and is called from both DAGScheduler and SparkContext.
   *
   * @param rdd whose partitions are to be looked at
   * @param partition to lookup locality information for
   * @return list of machines that are preferred by the partition
   */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /**
   * Recursive implementation for getPreferredLocs.
   *
   * This method is thread-safe because it only accesses DAGScheduler state through thread-safe
   * methods (getCacheLocs()); please be careful when modifying this method, because any new
   * DAGScheduler state accessed by it may require additional synchronization.
   */
  private def getPreferredLocsInternal(
      rdd: RDD[_],
      partition: Int,
      visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    if (!visited.add((rdd, partition))) {
      // Nil has already been returned for previously visited partitions.
      return Nil
    }
    // If the partition is cached, return the cache locations
    // getCacheLocs会返回所有分区的所有TaskLocation信息，然后再取partition这个分区的所有TaskLocation信息
    // 注意，后面一个括号就是一个数组的取值操作
    val cached = getCacheLocs(rdd)(partition)
    if (cached.nonEmpty) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (rddPrefs.nonEmpty) {
      return rddPrefs.map(TaskLocation(_))
    }

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }

      case _ =>
    }

    Nil
  }

  /** Mark a map stage job as finished with the given output stats, and report to its listener. */
  def markMapStageJobAsFinished(job: ActiveJob, stats: MapOutputStatistics): Unit = {
    // 这里的"task"貌似和那个task的含义不一样。
    // In map stage jobs, we only create a single "task", which is to finish all of the stage
    // (including reusing any previous map outputs, etc); so we just mark task 0 as done
    job.finished(0) = true
    job.numFinished += 1
    job.listener.taskSucceeded(0, stats)
    cleanupStateForJobAndIndependentStages(job)
    listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
  }

  def stop() {
    messageScheduler.shutdownNow()
    eventProcessLoop.stop()
    taskScheduler.stop()
  }

  eventProcessLoop.start()
}

private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
   * The main event loop of the DAG scheduler.
   */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    // 处理新提交的job
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

    case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

    case StageCancelled(stageId, reason) =>
      dagScheduler.handleStageCancellation(stageId, reason)

    case JobCancelled(jobId, reason) =>
      dagScheduler.handleJobCancellation(jobId, reason)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId, reason) =>
      val workerLost = reason match {
        case SlaveLost(_, true) => true
        case _ => false
      }
      dagScheduler.handleExecutorLost(execId, workerLost)

    case WorkerRemoved(workerId, host, message) =>
      dagScheduler.handleWorkerRemoved(workerId, host, message)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case SpeculativeTaskSubmitted(task) =>
      dagScheduler.handleSpeculativeTaskSubmitted(task)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    // task结束事件
    case completion: CompletionEvent =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stopInNewThread()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200

  // Number of consecutive stage attempts allowed before a stage is aborted
  val DEFAULT_MAX_CONSECUTIVE_STAGE_ATTEMPTS = 4
}
