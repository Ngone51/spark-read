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

import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.spark.{ExecutorAllocationClient, SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.util.{Clock, SystemClock, Utils}

/**
 * BlacklistTracker is designed to track problematic executors and nodes.  It supports blacklisting
 * executors and nodes across an entire application (with a periodic expiry).  TaskSetManagers add
 * additional blacklisting of executors and nodes for individual tasks and stages which works in
 * concert with the blacklisting here.
 *
 * The tracker needs to deal with a variety of workloads, eg.:
 *
 *  * bad user code --  this may lead to many task failures, but that should not count against
 *      individual executors
 *  * many small stages -- this may prevent a bad executor for having many failures within one
 *      stage, but still many failures over the entire application
 *  * "flaky" executors -- they don't fail every task, but are still faulty enough to merit
 *      blacklisting
 *
 * See the design doc on SPARK-8425 for a more in-depth discussion.
 *
 * THREADING: As with most helpers of TaskSchedulerImpl, this is not thread-safe.  Though it is
 * called by multiple threads, callers must already have a lock on the TaskSchedulerImpl.  The
 * one exception is [[nodeBlacklist()]], which can be called without holding a lock.
 */
private[scheduler] class BlacklistTracker (
    private val listenerBus: LiveListenerBus,
    conf: SparkConf,
    allocationClient: Option[ExecutorAllocationClient],
    clock: Clock = new SystemClock()) extends Logging {

  def this(sc: SparkContext, allocationClient: Option[ExecutorAllocationClient]) = {
    this(sc.listenerBus, sc.conf, allocationClient)
  }

  // 验证Blacklist配置的有效性
  BlacklistTracker.validateBlacklistConfs(conf)
  // 在一个executor上task失败的最大个数（默认为2个）
  // （所谓的个数是指在TaskSet中的唯一的task的个数，而不是task的attempts次数）
  private val MAX_FAILURES_PER_EXEC = conf.get(config.MAX_FAILURES_PER_EXEC)
  // 一个主机上，被app加入黑名单的executors的个数的最大值（默认为2个）
  //（即如果已经有2个executors被加入该app的黑名单，则executors所在的主机也会被该app加入黑名单。
  // 一个主机被加入app的黑名单意味着该app不再会选择该主机执行tasks）
  private val MAX_FAILED_EXEC_PER_NODE = conf.get(config.MAX_FAILED_EXEC_PER_NODE)
  // 默认1个小时
  val BLACKLIST_TIMEOUT_MILLIS = BlacklistTracker.getBlacklistTimeout(conf)
  // 是否启用FetchFailure的黑名单策略，默认false
  private val BLACKLIST_FETCH_FAILURE_ENABLED = conf.get(config.BLACKLIST_FETCH_FAILURE_ENABLED)

  /**
   * A map from executorId to information on task failures.  Tracks the time of each task failure,
   * so that we can avoid blacklisting executors due to failures that are very far apart.  We do not
   * actively remove from this as soon as tasks hit their timeouts, to avoid the time it would take
   * to do so.  But it will not grow too large, because as soon as an executor gets too many
   * failures, we blacklist the executor and remove its entry here.
   */
  private val executorIdToFailureList = new HashMap[String, ExecutorFailureList]()
  val executorIdToBlacklistStatus = new HashMap[String, BlacklistedExecutor]()
  val nodeIdToBlacklistExpiryTime = new HashMap[String, Long]()
  /**
   * An immutable copy of the set of nodes that are currently blacklisted.  Kept in an
   * AtomicReference to make [[nodeBlacklist()]] thread-safe.
   */
  private val _nodeBlacklist = new AtomicReference[Set[String]](Set())
  /**
   * 超过这个时间的executor或host将会从黑名单中移除（重新加入白名单）
   * Time when the next blacklist will expire.  Used as a
   * shortcut to avoid iterating over all entries in the blacklist when none will have expired.
   */
  var nextExpiryTime: Long = Long.MaxValue
  /**
   * Mapping from nodes to all of the executors that have been blacklisted on that node. We do *not*
   * remove from this when executors are removed from spark, so we can track when we get multiple
   * successive blacklisted executors on one node.  Nonetheless, it will not grow too large because
   * there cannot be many blacklisted executors on one node, before we stop requesting more
   * executors on that node, and we clean up the list of blacklisted executors once an executor has
   * been blacklisted for BLACKLIST_TIMEOUT_MILLIS.
   */
  val nodeToBlacklistedExecs = new HashMap[String, HashSet[String]]()

  /**
   * 将被加入黑名单时间超过BLACKLIST_TIMEOUT_MILLIS的executor和node从黑名单中移除，
   * 重新加入白名单（"哎，都把你拉黑这么久了，毛病找到了吧？你现在已经恢复好了吧？"）
   * Un-blacklists executors and nodes that have been blacklisted for at least
   * BLACKLIST_TIMEOUT_MILLIS
   */
  def applyBlacklistTimeout(): Unit = {
    val now = clock.getTimeMillis()
    // quickly check if we've got anything to expire from blacklist -- if not, avoid doing any work
    if (now > nextExpiryTime) {
      // 如果expiryTime < now，则可以将executor移出黑名单
      // Apply the timeout to blacklisted nodes and executors
      val execsToUnblacklist = executorIdToBlacklistStatus.filter(_._2.expiryTime < now).keys
      if (execsToUnblacklist.nonEmpty) {
        // Un-blacklist any executors that have been blacklisted longer than the blacklist timeout.
        logInfo(s"Removing executors $execsToUnblacklist from blacklist because the blacklist " +
          s"for those executors has timed out")
        execsToUnblacklist.foreach { exec =>
          // 将该executor从黑名单中移除
          val status = executorIdToBlacklistStatus.remove(exec).get
          val failedExecsOnNode = nodeToBlacklistedExecs(status.node)
          listenerBus.post(SparkListenerExecutorUnblacklisted(now, exec))
          // 将该executor从其对应的node中移除
          failedExecsOnNode.remove(exec)
          if (failedExecsOnNode.isEmpty) {
            nodeToBlacklistedExecs.remove(status.node)
          }
        }
      }
      // 可以从黑名单中移出的nodes
      val nodesToUnblacklist = nodeIdToBlacklistExpiryTime.filter(_._2 < now).keys
      if (nodesToUnblacklist.nonEmpty) {
        // Un-blacklist any nodes that have been blacklisted longer than the blacklist timeout.
        logInfo(s"Removing nodes $nodesToUnblacklist from blacklist because the blacklist " +
          s"has timed out")
        nodesToUnblacklist.foreach { node =>
          // 将该node从黑名单中移出
          // （注意：node从黑名单中移出，不代表该node上的executors也都移出了黑名单）
          nodeIdToBlacklistExpiryTime.remove(node)
          listenerBus.post(SparkListenerNodeUnblacklisted(now, node))
        }
        // 更新_nodeBlacklist
        _nodeBlacklist.set(nodeIdToBlacklistExpiryTime.keySet.toSet)
      }
      // 更新NextExpiryTime
      updateNextExpiryTime()
    }
  }

  // 更新下一次的ExpiryTime（我们只取最小的一个）
  private def updateNextExpiryTime(): Unit = {
    val execMinExpiry = if (executorIdToBlacklistStatus.nonEmpty) {
      executorIdToBlacklistStatus.map{_._2.expiryTime}.min
    } else {
      Long.MaxValue
    }
    val nodeMinExpiry = if (nodeIdToBlacklistExpiryTime.nonEmpty) {
      nodeIdToBlacklistExpiryTime.values.min
    } else {
      Long.MaxValue
    }
    nextExpiryTime = math.min(execMinExpiry, nodeMinExpiry)
  }

  private def killBlacklistedExecutor(exec: String): Unit = {
    // 如果启用了kill掉加入黑名单的executors的策略，则kill掉该executor
    if (conf.get(config.BLACKLIST_KILL_ENABLED)) {
      allocationClient match {
        case Some(a) =>
          logInfo(s"Killing blacklisted executor id $exec " +
            s"since ${config.BLACKLIST_KILL_ENABLED.key} is set.")
          a.killExecutors(Seq(exec), true, true)
        case None =>
          logWarning(s"Not attempting to kill blacklisted executor id $exec " +
            s"since allocation client is not defined.")
      }
    }
  }

  private def killExecutorsOnBlacklistedNode(node: String): Unit = {
    // 如果启用了kill掉加入黑名单的executors的策略，则kill掉该（主机上的）executors
    if (conf.get(config.BLACKLIST_KILL_ENABLED)) {
      allocationClient match {
        case Some(a) =>
          logInfo(s"Killing all executors on blacklisted host $node " +
            s"since ${config.BLACKLIST_KILL_ENABLED.key} is set.")
          if (a.killExecutorsOnHost(node) == false) {
            logError(s"Killing executors on node $node failed.")
          }
        case None =>
          logWarning(s"Not attempting to kill executors on blacklisted host $node " +
            s"since allocation client is not defined.")
      }
    }
  }

  def updateBlacklistForFetchFailure(host: String, exec: String): Unit = {
    // 如果启用了FetchFailure的Blacklist策略
    if (BLACKLIST_FETCH_FAILURE_ENABLED) {
      // If we blacklist on fetch failures, we are implicitly saying that we believe the failure is
      // non-transient, and can't be recovered from (even if this is the first fetch failure,
      // stage is retried after just one failure, so we don't always get a chance to collect
      // multiple fetch failures).
      // If the external shuffle-service is on, then every other executor on this node would
      // be suffering from the same issue, so we should blacklist (and potentially kill) all
      // of them immediately.

      val now = clock.getTimeMillis()
      val expiryTimeForNewBlacklists = now + BLACKLIST_TIMEOUT_MILLIS

      // 如果启用了外部shuffle服务
      if (conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
        if (!nodeIdToBlacklistExpiryTime.contains(host)) {
          logInfo(s"blacklisting node $host due to fetch failure of external shuffle service")

          nodeIdToBlacklistExpiryTime.put(host, expiryTimeForNewBlacklists)
          listenerBus.post(SparkListenerNodeBlacklisted(now, host, 1))
          _nodeBlacklist.set(nodeIdToBlacklistExpiryTime.keySet.toSet)
          killExecutorsOnBlacklistedNode(host)
          updateNextExpiryTime()
        }
      } else if (!executorIdToBlacklistStatus.contains(exec)) {
        // 由于FetchFailure，将该executor加入黑名单
        logInfo(s"Blacklisting executor $exec due to fetch failure")

        // 将该executor加入executorIdToBlacklistStatus中
        executorIdToBlacklistStatus.put(exec, BlacklistedExecutor(host, expiryTimeForNewBlacklists))
        // We hardcoded number of failure tasks to 1 for fetch failure, because there's no
        // reattempt for such failure.
        listenerBus.post(SparkListenerExecutorBlacklisted(now, exec, 1))
        // 更新下一次的ExpiryTime
        updateNextExpiryTime()
        // 只有启用了kill掉加入黑名单的executor的策略，才会kill掉该executor
        killBlacklistedExecutor(exec)

        // FIXME are you kidding me??? exec???
        val blacklistedExecsOnNode = nodeToBlacklistedExecs.getOrElseUpdate(exec, HashSet[String]())
        blacklistedExecsOnNode += exec
      }
    }
  }

  // 注意：一定是为Successful的TaskSet更新app的黑名单状态
  def updateBlacklistForSuccessfulTaskSet(
      stageId: Int,
      stageAttemptId: Int,
      failuresByExec: HashMap[String, ExecutorFailuresInTaskSet]): Unit = {
    // if any tasks failed, we count them towards the overall failure count for the executor at
    // this point.
    val now = clock.getTimeMillis()
    failuresByExec.foreach { case (exec, failuresInTaskSet) =>
      val appFailuresOnExecutor =
        executorIdToFailureList.getOrElseUpdate(exec, new ExecutorFailureList)
      // 更新在该executor上的failure tasks（这些tasks都是在一个TaskSet中，且都选择了
      // 在该executor上执行）的信息
      appFailuresOnExecutor.addFailures(stageId, stageAttemptId, failuresInTaskSet)
      // 检查当前时间now是否已经使得该executor上的一些failure task的expiry time失效了
      // （所谓的失效，怎么说呢...看刚方法的注释吧，强烈要求看该方法里的中文注释）
      appFailuresOnExecutor.dropFailuresWithTimeoutBefore(now)
      // 统计现在在该executor上failed的task总个数
      val newTotal = appFailuresOnExecutor.numUniqueTaskFailures

      // 该executor的expiry time（注意：要从现在算起）
      val expiryTimeForNewBlacklists = now + BLACKLIST_TIMEOUT_MILLIS
      // If this pushes the total number of failures over the threshold, blacklist the executor.
      // If its already blacklisted, we avoid "re-blacklisting" (which can happen if there were
      // other tasks already running in another taskset when it got blacklisted), because it makes
      // some of the logic around expiry times a little more confusing.  But it also wouldn't be a
      // problem to re-blacklist, with a later expiry time.
      if (newTotal >= MAX_FAILURES_PER_EXEC && !executorIdToBlacklistStatus.contains(exec)) {
        // 注意：必须是在successful的task sets里的统计task failure
        logInfo(s"Blacklisting executor id: $exec because it has $newTotal" +
          s" task failures in successful task sets")
        val node = failuresInTaskSet.node
        // 将该executor加入（app的）黑名单
        // （注意，这里是加入到app的黑名单中了，而不是TaskSet/Stage的黑名单）
        executorIdToBlacklistStatus.put(exec, BlacklistedExecutor(node, expiryTimeForNewBlacklists))
        listenerBus.post(SparkListenerExecutorBlacklisted(now, exec, newTotal))
        // 既然该executor当前已经被加入到黑名单之中了，我们就不需要ExecutorFailureList信息了
        // 因为该信息只是用来决测一个executor是否需要加入黑名单中。而在该executor加入黑名单的这段时间
        // 内，该信息也不会再更新了。等到该executor重新移出黑名单，我们再重新记录该信息即可。
        executorIdToFailureList.remove(exec)
        // 更新Expiry Time
        updateNextExpiryTime()
        killBlacklistedExecutor(exec)

        // 除了把executor加入黑名单，我们也会更新在主机（node／host）上的failures数据，并且如果可能的话，
        // 也将整个主机加入到(整个app的)黑名单中。
        // In addition to blacklisting the executor, we also update the data for failures on the
        // node, and potentially put the entire node into a blacklist as well.
        val blacklistedExecsOnNode = nodeToBlacklistedExecs.getOrElseUpdate(node, HashSet[String]())
        blacklistedExecsOnNode += exec
        // If the node is already in the blacklist, we avoid adding it again with a later expiry
        // time.
        if (blacklistedExecsOnNode.size >= MAX_FAILED_EXEC_PER_NODE &&
            !nodeIdToBlacklistExpiryTime.contains(node)) {
          logInfo(s"Blacklisting node $node because it has ${blacklistedExecsOnNode.size} " +
            s"executors blacklisted: ${blacklistedExecsOnNode}")
          // 将该主机（node）即其ExpiryTime加入app的黑名单中
          nodeIdToBlacklistExpiryTime.put(node, expiryTimeForNewBlacklists)
          listenerBus.post(SparkListenerNodeBlacklisted(now, node, blacklistedExecsOnNode.size))
          _nodeBlacklist.set(nodeIdToBlacklistExpiryTime.keySet.toSet)
          killExecutorsOnBlacklistedNode(node)
        }
      }
    }
  }

  def isExecutorBlacklisted(executorId: String): Boolean = {
    executorIdToBlacklistStatus.contains(executorId)
  }

  /**
   * Get the full set of nodes that are blacklisted.  Unlike other methods in this class, this *IS*
   * thread-safe -- no lock required on a taskScheduler.
   */
  def nodeBlacklist(): Set[String] = {
    _nodeBlacklist.get()
  }

  def isNodeBlacklisted(node: String): Boolean = {
    nodeIdToBlacklistExpiryTime.contains(node)
  }

  def handleRemovedExecutor(executorId: String): Unit = {
    // We intentionally do not clean up executors that are already blacklisted in
    // nodeToBlacklistedExecs, so that if another executor on the same node gets blacklisted, we can
    // blacklist the entire node.  We also can't clean up executorIdToBlacklistStatus, so we can
    // eventually remove the executor after the timeout.  Despite not clearing those structures
    // here, we don't expect they will grow too big since you won't get too many executors on one
    // node, and the timeout will clear it up periodically in any case.
    executorIdToFailureList -= executorId
  }


  /**
   * Tracks all failures for one executor (that have not passed the timeout).
   *
   * In general we actually expect this to be extremely small, since it won't contain more than the
   * maximum number of task failures before an executor is failed (default 2).
   */
  private[scheduler] final class ExecutorFailureList extends Logging {

    private case class TaskId(stage: Int, stageAttempt: Int, taskIndex: Int)

    /**
     * All failures on this executor in successful task sets.
     */
    private var failuresAndExpiryTimes = ArrayBuffer[(TaskId, Long)]()
    /**
     * As an optimization, we track the min expiry time over all entries in failuresAndExpiryTimes
     * so its quick to tell if there are any failures with expiry before the current time.
     */
    private var minExpiryTime = Long.MaxValue

    def addFailures(
        stage: Int,
        stageAttempt: Int,
        failuresInTaskSet: ExecutorFailuresInTaskSet): Unit = {
      failuresInTaskSet.taskToFailureCountAndFailureTime.foreach {
        case (taskIdx, (_, failureTime)) =>
          // 为每个task设置Expiry Time
          val expiryTime = failureTime + BLACKLIST_TIMEOUT_MILLIS
          failuresAndExpiryTimes += ((TaskId(stage, stageAttempt, taskIdx), expiryTime))
          // 这挺好
          if (expiryTime < minExpiryTime) {
            minExpiryTime = expiryTime
          }
      }
    }

    /**
     * The number of unique tasks that failed on this executor.  Only counts failures within the
     * timeout, and in successful tasksets.
     */
    def numUniqueTaskFailures: Int = failuresAndExpiryTimes.size

    def isEmpty: Boolean = failuresAndExpiryTimes.isEmpty

    /**
     * Apply the timeout to individual tasks.  This is to prevent one-off failures that are very
     * spread out in time (and likely have nothing to do with problems on the executor) from
     * triggering blacklisting.  However, note that we do *not* remove executors and nodes from
     * the blacklist as we expire individual task failures -- each have their own timeout.  Eg.,
     * suppose:
     *  * timeout = 10, maxFailuresPerExec = 2
     *  * Task 1 fails on exec 1 at time 0
     *  * Task 2 fails on exec 1 at time 5
     * -->  exec 1 is blacklisted from time 5 - 15.
     * This is to simplify the implementation, as well as keep the behavior easier to understand
     * for the end user.
     */
    def dropFailuresWithTimeoutBefore(dropBefore: Long): Unit = {
      // 我们以上面的例子为例，解释一下该函数的意思：
      // 假设现在failuresAndExpiryTimes = {(Task 1, time 0), (Task 2, time 5)},
      // dropBefore = 12， BLACKLIST_TIMEOUT_MILLIS = 10，
      // 且minExpiryTime = time 0 + BLACKLIST_TIMEOUT_MILLIS = 10；
      // 所以如果我们根据Task 1的expiry time来把该executor加入黑名单的话，那么，在时间[0, 10]之内
      // 该executor被加入黑名单；同样地，对于Task 2, 在时间[5, 15]之内，该executor被加入黑名单。
      // 而此时dropBefore = 12， 此时该executor还没加入黑名单（显然的，因为我们在该executor执行了task
      // 才能统计它的failure task并更新黑名单状态啊）。所以，既然现在的dropBefore都已经大于minExpiryTime了,
      // 就说明，在failuresAndExpiryTimes中记录的某些task已经处于executor变为白名单的时间之内了。比如说，
      // 对于Task 1，它认定的executor的黑名单时间是[0, 10]，而现在的时间已经是dropBefore = 12。因此，对于
      // Task 1来说，该executor已经从黑名单移到白名单了。所以该task不能做为在该executor上fail的task的总个数。
      // 而在[dropBefore, dropBefore + BLACKLIST_TIMEOUT_MILLIS] = [12, 22]这段时间内的task应该被统计为
      // 在该executor上fail的task的总个数，比如Task 2。
      // 所以，我们现在需要更新failuresAndExpiryTimes， 把expiryTime < dropBefore的task从中移除。
      // 移除这些Expiry Time失效的tasks对于之后裁决该executor是否需要加入黑名单也有决定作用！同时，更新minExpiryTime。
      // QUESTION：能否只记录ExpiryTime最大的task呢???
      // 答：不不不，我们之后还有统计failuresAndExpiryTimes的size，用于裁决该executor是否需要加入黑名单

      if (minExpiryTime < dropBefore) {
        var newMinExpiry = Long.MaxValue
        val newFailures = new ArrayBuffer[(TaskId, Long)]
        failuresAndExpiryTimes.foreach { case (task, expiryTime) =>
          if (expiryTime >= dropBefore) {
            newFailures += ((task, expiryTime))
            if (expiryTime < newMinExpiry) {
              newMinExpiry = expiryTime
            }
          }
        }
        failuresAndExpiryTimes = newFailures
        minExpiryTime = newMinExpiry
      }
    }

    override def toString(): String = {
      s"failures = $failuresAndExpiryTimes"
    }
  }

}

private[scheduler] object BlacklistTracker extends Logging {

  private val DEFAULT_TIMEOUT = "1h"

  /**
   * Returns true if the blacklist is enabled, based on checking the configuration in the following
   * order:
   * 1. Is it specifically enabled or disabled?
   * 2. Is it enabled via the legacy timeout conf?
   * 3. Default is off
   */
  def isBlacklistEnabled(conf: SparkConf): Boolean = {
    conf.get(config.BLACKLIST_ENABLED) match {
      case Some(enabled) =>
        enabled
      case None =>
        // if they've got a non-zero setting for the legacy conf, always enable the blacklist,
        // otherwise, use the default.
        val legacyKey = config.BLACKLIST_LEGACY_TIMEOUT_CONF.key
        conf.get(config.BLACKLIST_LEGACY_TIMEOUT_CONF).exists { legacyTimeout =>
          if (legacyTimeout == 0) {
            logWarning(s"Turning off blacklisting due to legacy configuration: $legacyKey == 0")
            false
          } else {
            logWarning(s"Turning on blacklisting due to legacy configuration: $legacyKey > 0")
            true
          }
        }
    }
  }

  def getBlacklistTimeout(conf: SparkConf): Long = {
    conf.get(config.BLACKLIST_TIMEOUT_CONF).getOrElse {
      conf.get(config.BLACKLIST_LEGACY_TIMEOUT_CONF).getOrElse {
        Utils.timeStringAsMs(DEFAULT_TIMEOUT)
      }
    }
  }

  /**
   * Verify that blacklist configurations are consistent; if not, throw an exception.  Should only
   * be called if blacklisting is enabled.
   *
   * The configuration for the blacklist is expected to adhere to a few invariants.  Default
   * values follow these rules of course, but users may unwittingly change one configuration
   * without making the corresponding adjustment elsewhere.  This ensures we fail-fast when
   * there are such misconfigurations.
   */
  def validateBlacklistConfs(conf: SparkConf): Unit = {

    def mustBePos(k: String, v: String): Unit = {
      throw new IllegalArgumentException(s"$k was $v, but must be > 0.")
    }

    Seq(
      config.MAX_TASK_ATTEMPTS_PER_EXECUTOR,
      config.MAX_TASK_ATTEMPTS_PER_NODE,
      config.MAX_FAILURES_PER_EXEC_STAGE,
      config.MAX_FAILED_EXEC_PER_NODE_STAGE,
      config.MAX_FAILURES_PER_EXEC,
      config.MAX_FAILED_EXEC_PER_NODE
    ).foreach { config =>
      val v = conf.get(config)
      if (v <= 0) {
        mustBePos(config.key, v.toString)
      }
    }

    // 默认1小时
    val timeout = getBlacklistTimeout(conf)
    if (timeout <= 0) {
      // first, figure out where the timeout came from, to include the right conf in the message.
      conf.get(config.BLACKLIST_TIMEOUT_CONF) match {
        case Some(t) =>
          mustBePos(config.BLACKLIST_TIMEOUT_CONF.key, timeout.toString)
        case None =>
          mustBePos(config.BLACKLIST_LEGACY_TIMEOUT_CONF.key, timeout.toString)
      }
    }

    // 显然，一个task在一个node上最多的失败次数要比该该task所有情况的失败次数要小才行
    val maxTaskFailures = conf.get(config.MAX_TASK_FAILURES)
    val maxNodeAttempts = conf.get(config.MAX_TASK_ATTEMPTS_PER_NODE)

    if (maxNodeAttempts >= maxTaskFailures) {
      throw new IllegalArgumentException(s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key} " +
        s"( = ${maxNodeAttempts}) was >= ${config.MAX_TASK_FAILURES.key} " +
        s"( = ${maxTaskFailures} ).  Though blacklisting is enabled, with this configuration, " +
        s"Spark will not be robust to one bad node.  Decrease " +
        s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key}, increase ${config.MAX_TASK_FAILURES.key}, " +
        s"or disable blacklisting with ${config.BLACKLIST_ENABLED.key}")
    }
  }
}

private final case class BlacklistedExecutor(node: String, expiryTime: Long)
