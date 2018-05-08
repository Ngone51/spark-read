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

package org.apache.spark.executor

import java.io.{File, NotSerializableException}
import java.lang.Thread.UncaughtExceptionHandler
import java.lang.management.ManagementFactory
import java.net.{URI, URL}
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent._
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, Map}
import scala.util.control.NonFatal

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{SparkOutOfMemoryError, TaskMemoryManager}
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler.{DirectTaskResult, IndirectTaskResult, Task, TaskDescription}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Spark executor, backed by a threadpool to run tasks.
 *
 * This can be used with Mesos, YARN, and the standalone scheduler.
 * An internal RPC interface is used for communication with the driver,
 * except in the case of Mesos fine-grained mode.
 */
private[spark] class Executor(
    executorId: String,
    executorHostname: String,
    env: SparkEnv,
    userClassPath: Seq[URL] = Nil,
    isLocal: Boolean = false,
    uncaughtExceptionHandler: UncaughtExceptionHandler = new SparkUncaughtExceptionHandler)
  extends Logging {

  logInfo(s"Starting executor ID $executorId on host $executorHostname")

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private val conf = env.conf

  // executorHostname只需要包含主机名，不需要包含端口号，不用ip地址
  // No ip or host:port - just hostname
  Utils.checkHost(executorHostname)
  // 不能指定端口号
  // must not have port specified.
  assert (0 == Utils.parseHostPort(executorHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(executorHostname)

  // TODO read setDefaultUncaughtExceptionHandler()
  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler)
  }

  // 启动工作线程池
  // Start worker thread pool
  private val threadPool = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("Executor task launch worker-%d")
      .setThreadFactory(new ThreadFactory {
        override def newThread(r: Runnable): Thread =
          // 使用UninterruptibleThread来运行任务，这样我们就能避免在运行代码时，被Thread.interrupt()中断。有些
          // 问题，如KAFKA-1894, HADOOP-10622，会因为一些方法被中断而一直挂起。
          // Use UninterruptibleThread to run tasks so that we can allow running codes without being
          // interrupted by `Thread.interrupt()`. Some issues, such as KAFKA-1894, HADOOP-10622,
          // will hang forever if some methods are interrupted.
          new UninterruptibleThread(r, "unused") // thread name will be set by ThreadFactoryBuilder
      })
      .build()
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }
  private val executorSource = new ExecutorSource(threadPool, executorId)
  // 一个用于监控任务杀死或撤销的线程池
  // Pool used for threads that supervise task killing / cancellation
  private val taskReaperPool = ThreadUtils.newDaemonCachedThreadPool("Task reaper")
  // For tasks which are in the process of being killed, this map holds the most recently created
  // TaskReaper. All accesses to this map should be synchronized on the map itself (this isn't
  // a ConcurrentHashMap because we use the synchronization for purposes other than simply guarding
  // the integrity of the map's internal state). The purpose of this map is to prevent the creation
  // of a separate TaskReaper for every killTask() of a given task. Instead, this map allows us to
  // track whether an existing TaskReaper fulfills the role of a TaskReaper that we would otherwise
  // create. The map key is a task id.
  // Reaper: 死神、收割者
  // TaskReaper是一个Runnable对象
  private val taskReaperForTask: HashMap[Long, TaskReaper] = HashMap[Long, TaskReaper]()

  // 如果不是本地模式运行spark
  if (!isLocal) {
    // 每个Executor需要initialize BlockManager(还记得么??? very important)
    // 本地模式会在SparkContext中初始化driver端的BlockManager，所以此处不需要再初始化
    env.blockManager.initialize(conf.getAppId)
    // TODO read
    // 向统计系统注册executor source
    env.metricsSystem.registerSource(executorSource)
    // 向统计系统注册shuffle metrics source
    env.metricsSystem.registerSource(env.blockManager.shuffleMetricsSource)
  }

  // 是否在加载Spark自带的jar包的classes之前加载用户的jar包中的classes
  // Whether to load classes in user jars before those in Spark jars
  private val userClassPathFirst = conf.getBoolean("spark.executor.userClassPathFirst", false)

  // 是否监控被杀死或被中断的任务
  // Whether to monitor killed / interrupted tasks
  private val taskReaperEnabled = conf.getBoolean("spark.task.reaper.enabled", false)

  // Create our ClassLoader
  // do this after SparkEnv creation so can access the SecurityManager
  // TODO 注意：在这里也会创建URLClassLoader，可能和SPARK-12216、SPARK-8333有关(windows下temp文件无法删除)
  private val urlClassLoader = createClassLoader()
  // TODO read
  private val replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)

  // Set the classloader for serializer
  env.serializer.setDefaultClassLoader(replClassLoader)
  // SPARK-21928.  SerializerManager's internal instance of Kryo might get used in netty threads
  // for fetching remote cached RDD blocks, so need to make sure it uses the right classloader too.
  env.serializerManager.setDefaultClassLoader(replClassLoader)

  // Max size of direct result. If task result is bigger than this, we use the block manager
  // to send the result back.
  private val maxDirectResultSize = Math.min(
    conf.getSizeAsBytes("spark.task.maxDirectResultSize", 1L << 20),
    RpcUtils.maxMessageSizeBytes(conf))

  // Limit of bytes for total size of results (default is 1GB)
  private val maxResultSize = Utils.getMaxResultSize(conf)

  // 维护一个正在运行的tasks的列表（实现结构是ConcurrentHashMap）
  // Maintains the list of running tasks.
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  // 用于执行心跳任务的executor
  // Executor for the heartbeat task.
  private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")

  // must be initialized before running startDriverHeartbeat()
  private val heartbeatReceiverRef =
    RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)

  /**
   * 心跳发送最多失败次数，默认60，大约10分钟。如果一个executor在尝试60次心跳发送失败后，就会kill掉自己。
   * When an executor is unable to send heartbeats to the driver more than `HEARTBEAT_MAX_FAILURES`
   * times, it should kill itself. The default value is 60. It means we will retry to send
   * heartbeats about 10 minutes because the heartbeat interval is 10s.
   */
  private val HEARTBEAT_MAX_FAILURES = conf.getInt("spark.executor.heartbeat.maxFailures", 60)

  /**
   * Count the failure times of heartbeat. It should only be accessed in the heartbeat thread. Each
   * successful heartbeat will reset it to 0.
   */
  private var heartbeatFailures = 0

  // 启动一个心跳发送任务，用于向driver发送心跳(包含一些task的统计信息)
  startDriverHeartbeater()

  // 该executor上，正在运行的任务的个数
  private[executor] def numRunningTasks: Int = runningTasks.size()

  // 发起一个任务(by TaskRunner)
  def launchTask(context: ExecutorBackend, taskDescription: TaskDescription): Unit = {
    // 每次发起一个新的task，都要新建一个TaskRunner。所以，它俩是一一对应的。
    val tr = new TaskRunner(context, taskDescription)
    runningTasks.put(taskDescription.taskId, tr)
    threadPool.execute(tr)
  }

  // 在executor上杀死一个正在运行的task
  def killTask(taskId: Long, interruptThread: Boolean, reason: String): Unit = {
    // 获取该task对应的TaskRunner
    val taskRunner = runningTasks.get(taskId)
    if (taskRunner != null) {
      // 如果启用了监控被杀死或被中断的任务的机制
      if (taskReaperEnabled) {
        // 注意taskReaperForTask由synchronized所修饰
        val maybeNewTaskReaper: Option[TaskReaper] = taskReaperForTask.synchronized {
          val shouldCreateReaper = taskReaperForTask.get(taskId) match {
            case None => true
              // 如果existingReaper的interruptThread = true, 则不管新来的interruptThread是什么，
              // 我们都不需要重新创建Reaper；而如果existingReaper的interruptThread = false，则
              // 如果新来的interruptThread = false，则我们不用重复创建Reaper，而如果新来的
              // interruptThread = true，则我们需要新建一个Reaper。
              // 综上所述，我们的目标就是最好创建interruptThread = true的Reaper
            case Some(existingReaper) => interruptThread && !existingReaper.interruptThread
          }
          if (shouldCreateReaper) {
            // 好了，我们终于可以创建一个新的Reaper了
            val taskReaper = new TaskReaper(
              taskRunner, interruptThread = interruptThread, reason = reason)
            taskReaperForTask(taskId) = taskReaper
            Some(taskReaper)
          } else {
            None
          }
        }
        // 在同步代码块外面执行TaskReaper
        // Execute the TaskReaper from outside of the synchronized block.
        // TODO read
        maybeNewTaskReaper.foreach(taskReaperPool.execute)
      } else {
        // 否则，通过TaskRunner kill掉该task
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
      }
    }
  }

  /**
   * Function to kill the running tasks in an executor.
   * This can be called by executor back-ends to kill the
   * tasks instead of taking the JVM down.
   * @param interruptThread whether to interrupt the task thread
   */
  def killAllTasks(interruptThread: Boolean, reason: String) : Unit = {
    runningTasks.keys().asScala.foreach(t =>
      killTask(t, interruptThread = interruptThread, reason = reason))
  }

  def stop(): Unit = {
    env.metricsSystem.report()
    heartbeater.shutdown()
    heartbeater.awaitTermination(10, TimeUnit.SECONDS)
    threadPool.shutdown()
    if (!isLocal) {
      env.stop()
    }
  }

  /** Returns the total amount of time this JVM process has spent in garbage collection. */
  private def computeTotalGcTime(): Long = {
    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
  }

  class TaskRunner(
      execBackend: ExecutorBackend,
      private val taskDescription: TaskDescription)
    extends Runnable {

    val taskId = taskDescription.taskId
    val threadName = s"Executor task launch worker for task $taskId"
    private val taskName = taskDescription.name

    /** If specified, this task has been killed and this option contains the reason. */
    @volatile private var reasonIfKilled: Option[String] = None

    @volatile private var threadId: Long = -1

    def getThreadId: Long = threadId

    /** Whether this task has been finished. */
    @GuardedBy("TaskRunner.this")
    private var finished = false

    def isFinished: Boolean = synchronized { finished }

    /** How much the JVM process has spent in GC when the task starts to run. */
    @volatile var startGCTime: Long = _

    /**
     * 需要执行的任务。该变量会在run()中，通过反序列化来自driver端task binary来赋值。一旦被赋值，就不会被改变。
     * 注意：这里的task binary和DAGScheduler里的taskBinary(broadcast变量)不是一回事！这里的task binary
     * 应该就是指在TaskSetManager中序列化的Task对象(详见TaskSetManager#resourceOffers#L546)。
     * The task to run. This will be set in run() by deserializing the task binary coming
     * from the driver. Once it is set, it will never be changed.
     */
    @volatile var task: Task[Any] = _

    // TaskRunner#kill()
    def kill(interruptThread: Boolean, reason: String): Unit = {
      logInfo(s"Executor is trying to kill $taskName (TID $taskId), reason: $reason")
      reasonIfKilled = Some(reason)
      if (task != null) {
        synchronized {
          if (!finished) {
            // 再通过Task自己来kill掉自己
            // 还记得我们怎么到这里的吗？TaskSetManager#handleSuccessfulTask() ->
            // backend.killTask() -> executor#killTask() -> TaskRunner#killTask() ->
            // Task.killTask()
            task.kill(interruptThread, reason)
          }
        }
      }
    }

    /**
     * Set the finished flag to true and clear the current thread's interrupt status
     */
    private def setTaskFinishedAndClearInterruptStatus(): Unit = synchronized {
      this.finished = true
      // SPARK-14234 - Reset the interrupted status of the thread to avoid the
      // ClosedByInterruptException during execBackend.statusUpdate which causes
      // Executor to crash
      Thread.interrupted()
      // Notify any waiting TaskReapers. Generally there will only be one reaper per task but there
      // is a rare corner-case where one task can have two reapers in case cancel(interrupt=False)
      // is followed by cancel(interrupt=True). Thus we use notifyAll() to avoid a lost wakeup:
      notifyAll()
    }

    override def run(): Unit = {
      threadId = Thread.currentThread.getId
      Thread.currentThread.setName(threadName)
      val threadMXBean = ManagementFactory.getThreadMXBean
      // 创建任务内存管理器
      val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
      val deserializeStartTime = System.currentTimeMillis()
      val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
        threadMXBean.getCurrentThreadCpuTime
      } else 0L
      Thread.currentThread.setContextClassLoader(replClassLoader)
      val ser = env.closureSerializer.newInstance()
      logInfo(s"Running $taskName (TID $taskId)")
      // 更新该任务的状态为RUNNING，会通过endpoint通知driver端的TaskSchedulerImpl
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var taskStart: Long = 0
      var taskStartCpu: Long = 0
      // 计算gc耗时
      startGCTime = computeTotalGcTime()

      try {
        // Must be set before updateDependencies() is called, in case fetching dependencies
        // requires access to properties contained within (e.g. for access control).
        Executor.taskDeserializationProps.set(taskDescription.properties)

        // TODO read
        updateDependencies(taskDescription.addedFiles, taskDescription.addedJars)
        // 反序列化task!!!
        // 注意：这里反序列化的是在TaskSetManager中被序列化的Task对象，反序列化得到Task对象
        task = ser.deserialize[Task[Any]](
          taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)
        // 设置task的localProperties(来自driver端通过user配置)
        task.localProperties = taskDescription.properties
        // 设置任务的内存管理器
        task.setTaskMemoryManager(taskMemoryManager)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        val killReason = reasonIfKilled
        if (killReason.isDefined) {
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException(killReason.get)
        }

        // The purpose of updating the epoch here is to invalidate executor map output status cache
        // in case FetchFailures have occurred. In local mode `env.mapOutputTracker` will be
        // MapOutputTrackerMaster and its cache invalidation is not based on epoch numbers so
        // we don't need to make any special calls here.
        if (!isLocal) {
          logDebug("Task " + taskId + "'s epoch is " + task.epoch)
          env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].updateEpoch(task.epoch)
        }

        // Run the actual task and measure its runtime.
        taskStart = System.currentTimeMillis()
        taskStartCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L
        var threwException = true
        val value = try {
          // 调用task的run()方法
          // 这个taskAttemptId和attemptNumber真是乱啊...(见run()方法的注释，
          // 感觉说的还是很明确的)
          // taskAttemptId是taskId，在SparkContext中全局唯一
          // attemptNumber表示是该task的第几次尝试(从0开始)
          val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = taskDescription.attemptNumber,
            metricsSystem = env.metricsSystem)
          threwException = false
          res
        } finally {
          // 释放该task持有的全部锁（包括读锁和写锁）
          // releasedLocks：虽然变量名字是locks，但它实际上是刚刚被释放了读写锁的那些个blocks，
          // 确切地说，是blockId
          val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
          // 释放所有分配给该task的运行内存（先释放真正分配的堆内外内存，
          // 再释放由MemoryManager管理的execution memory）
          // （那个storage memory和task是什么关系啊???）
          // 答：详见MemoryStore#releaseUnrollMemoryForThisTask()上的注释
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()

          // 理解了一下变量threwException，意思是不是说：如果代码运行到finally，且threwException为false。
          // 说明，task.run()正常结束了。task正常结束，意味着它已经自己释放了所有的内存和blocks的读写锁(是
          // 这样的吗???)。然后，在finally中理应releasedLocks和freedMemory为空或0才对。而如果不为空或为0，
          // 说明该task虽然正常结束了，但是没有处理好这两个东西（一个是blocks的锁没有被释放，另一个是有内存泄露）
          // 这样理解的话，变量threwException的作用才能说的通。
          // 所以，我上面的理解对吗???关键就是，task在结束时会不会自己处理那些事情呢???
          if (freedMemory > 0 && !threwException) {
            val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
            if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logWarning(errMsg)
            }
          }

          if (releasedLocks.nonEmpty && !threwException) {
            val errMsg =
              s"${releasedLocks.size} block locks were not released by TID = $taskId:\n" +
                releasedLocks.mkString("[", ", ", "]")
            if (conf.getBoolean("spark.storage.exceptionOnPinLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logInfo(errMsg)
            }
          }
        }
        task.context.fetchFailed.foreach { fetchFailure =>
          // uh-oh.  it appears the user code has caught the fetch-failure without throwing any
          // other exceptions.  Its *possible* this is what the user meant to do (though highly
          // unlikely).  So we will log an error and keep going.
          logError(s"TID ${taskId} completed successfully though internally it encountered " +
            s"unrecoverable fetch failures!  Most likely this means user code is incorrectly " +
            s"swallowing Spark's internal ${classOf[FetchFailedException]}", fetchFailure)
        }
        val taskFinish = System.currentTimeMillis()
        val taskFinishCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L

        // 会抛出异常
        // If the task has been killed, let's fail it.
        task.context.killTaskIfInterrupted()

        // 至此，task全部正常，准备处理"后事"
        val resultSer = env.serializer.newInstance()
        val beforeSerialization = System.currentTimeMillis()
        // 序列化task的运行结果(如果是ShuffleMapTask则结果的类型是MapStatus；如果是ResultTask，
        // 则结果类型由用户编写的代码决定)
        val valueBytes = resultSer.serialize(value)
        val afterSerialization = System.currentTimeMillis()

        // 记录executor反序列化耗时
        // Deserialization happens in two parts: first, we deserialize a Task object, which
        // includes the Partition. Second, Task.run() deserializes the RDD and function to be run.
        task.metrics.setExecutorDeserializeTime(
          (taskStart - deserializeStartTime) + task.executorDeserializeTime)
        // 记录executor反序列化cpu耗时
        task.metrics.setExecutorDeserializeCpuTime(
          (taskStartCpu - deserializeStartCpuTime) + task.executorDeserializeCpuTime)
        // 记录executor的执行耗时（我们需要减去Task.run()的反序列化耗时(这部分耗时已经
        // 在上面统计过了)以避免重复计算）
        // We need to subtract Task.run()'s deserialization time to avoid double-counting
        task.metrics.setExecutorRunTime((taskFinish - taskStart) - task.executorDeserializeTime)
        // 记录executor的cpu执行耗时
        task.metrics.setExecutorCpuTime(
          (taskFinishCpu - taskStartCpu) - task.executorDeserializeCpuTime)
        // 记录GC耗时(还能记录gc耗时啊)
        task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
        // 记录序列化task执行结果的耗时
        task.metrics.setResultSerializationTime(afterSerialization - beforeSerialization)

        // 更新executor的统计信息
        // Expose task metrics using the Dropwizard metrics system.
        // Update task metrics counters
        executorSource.METRIC_CPU_TIME.inc(task.metrics.executorCpuTime)
        executorSource.METRIC_RUN_TIME.inc(task.metrics.executorRunTime)
        executorSource.METRIC_JVM_GC_TIME.inc(task.metrics.jvmGCTime)
        executorSource.METRIC_DESERIALIZE_TIME.inc(task.metrics.executorDeserializeTime)
        executorSource.METRIC_DESERIALIZE_CPU_TIME.inc(task.metrics.executorDeserializeCpuTime)
        executorSource.METRIC_RESULT_SERIALIZE_TIME.inc(task.metrics.resultSerializationTime)
        executorSource.METRIC_SHUFFLE_FETCH_WAIT_TIME
          .inc(task.metrics.shuffleReadMetrics.fetchWaitTime)
        executorSource.METRIC_SHUFFLE_WRITE_TIME.inc(task.metrics.shuffleWriteMetrics.writeTime)
        executorSource.METRIC_SHUFFLE_TOTAL_BYTES_READ
          .inc(task.metrics.shuffleReadMetrics.totalBytesRead)
        executorSource.METRIC_SHUFFLE_REMOTE_BYTES_READ
          .inc(task.metrics.shuffleReadMetrics.remoteBytesRead)
        executorSource.METRIC_SHUFFLE_REMOTE_BYTES_READ_TO_DISK
          .inc(task.metrics.shuffleReadMetrics.remoteBytesReadToDisk)
        executorSource.METRIC_SHUFFLE_LOCAL_BYTES_READ
          .inc(task.metrics.shuffleReadMetrics.localBytesRead)
        executorSource.METRIC_SHUFFLE_RECORDS_READ
          .inc(task.metrics.shuffleReadMetrics.recordsRead)
        executorSource.METRIC_SHUFFLE_REMOTE_BLOCKS_FETCHED
          .inc(task.metrics.shuffleReadMetrics.remoteBlocksFetched)
        executorSource.METRIC_SHUFFLE_LOCAL_BLOCKS_FETCHED
          .inc(task.metrics.shuffleReadMetrics.localBlocksFetched)
        executorSource.METRIC_SHUFFLE_BYTES_WRITTEN
          .inc(task.metrics.shuffleWriteMetrics.bytesWritten)
        executorSource.METRIC_SHUFFLE_RECORDS_WRITTEN
          .inc(task.metrics.shuffleWriteMetrics.recordsWritten)
        executorSource.METRIC_INPUT_BYTES_READ
          .inc(task.metrics.inputMetrics.bytesRead)
        executorSource.METRIC_INPUT_RECORDS_READ
          .inc(task.metrics.inputMetrics.recordsRead)
        executorSource.METRIC_OUTPUT_BYTES_WRITTEN
          .inc(task.metrics.outputMetrics.bytesWritten)
        executorSource.METRIC_OUTPUT_RECORDS_WRITTEN
          .inc(task.metrics.inputMetrics.recordsRead)
        executorSource.METRIC_RESULT_SIZE.inc(task.metrics.resultSize)
        executorSource.METRIC_DISK_BYTES_SPILLED.inc(task.metrics.diskBytesSpilled)
        executorSource.METRIC_MEMORY_BYTES_SPILLED.inc(task.metrics.memoryBytesSpilled)

        // Note: accumulator updates must be collected after TaskMetrics is updated
        val accumUpdates = task.collectAccumulatorUpdates()
        // 创建DirectTaskResult对象，该对象包含了task的返回值以及accumlator的更新信息
        // TODO: do not serialize value twice
        val directResult = new DirectTaskResult(valueBytes, accumUpdates)
        // 序列化directResult对象
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit()

        // directSend = sending directly back to the driver
        val serializedResult: ByteBuffer = {
          // maxResultSize默认1GB
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            // 如果该task的serializedDirectResult大小比maxResultSize还大，
            // 则我们不直接发送该task的serializedDirectResult了，而是发送该serializedDirectResult
            // 的一个引用（即IndirectTaskResult）。
            // 所以，是被抛弃了吗???（根据logWarning里的"dropping"的提示）因为，它也不像下面的，
            // 存到了BlockManager里去啊，这样之后executor才能访问该BlockManager来获取啊。
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize > maxDirectResultSize) {
            // 如果该task的serializedDirectResult大小比maxDirectResultSize还大，
            // 则我们将其存储到本地的BlockManager中，后续通过BlockManager来传送（我估计
            // 会通过BlockTransferService来传输啦～～～）
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId,
              new ChunkedByteBuffer(serializedDirectResult.duplicate()),
              StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
            serializedDirectResult
          }
        }
        // 标记该task为结束状态，并且清除Interrupt状态
        setTaskFinishedAndClearInterruptStatus()
        // 注意：此时的serializedResult可能有三种情况，详见上面的代码
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)

      } catch {
        // 该task执行过程中发生了Fetch Failure
        case t: Throwable if hasFetchFailure && !Utils.isFatalError(t) =>
          val reason = task.context.fetchFailed.get.toTaskFailedReason
          if (!t.isInstanceOf[FetchFailedException]) {
            // 在task的执行过程中产生了fetch failure，但是某些用户代码将该异常包装了起来并且抛出了
            // 其它的异常。不管它了，我们就把该异常当作fetch failure来处理。
            // there was a fetch failure in the task, but some user code wrapped that exception
            // and threw something else.  Regardless, we treat it as a fetch failure.
            val fetchFailedCls = classOf[FetchFailedException].getName
            logWarning(s"TID ${taskId} encountered a ${fetchFailedCls} and " +
              s"failed, but the ${fetchFailedCls} was hidden by another " +
              s"exception.  Spark is handling this like a fetch failure and ignoring the " +
              s"other exception: $t")
          }
          // 标记该task为结束状态，并且清除中断标志
          setTaskFinishedAndClearInterruptStatus()
          // 调用execBackend的statusUpdate()，注意在这里，TaskState为FAILED，
          // 并且reason是一个FetchFailedException（reason会先序列化，因为需要rpc传输）
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case t: TaskKilledException =>
          logInfo(s"Executor killed $taskName (TID $taskId), reason: ${t.reason}")
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled(t.reason)))

        case _: InterruptedException | NonFatal(_) if
            task != null && task.reasonIfKilled.isDefined =>
          val killReason = task.reasonIfKilled.getOrElse("unknown reason")
          logInfo(s"Executor interrupted and killed $taskName (TID $taskId), reason: $killReason")
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(
            taskId, TaskState.KILLED, ser.serialize(TaskKilled(killReason)))

        case CausedBy(cDE: CommitDeniedException) =>
          val reason = cDE.toTaskCommitDeniedReason
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(reason))

        case t: Throwable =>
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(s"Exception in $taskName (TID $taskId)", t)

          // SPARK-20904: Do not report failure to driver if if happened during shut down. Because
          // libraries may set up shutdown hooks that race with running tasks during shutdown,
          // spurious failures may occur and can result in improper accounting in the driver (e.g.
          // the task failure would not be ignored if the shutdown happened because of premption,
          // instead of an app issue).
          if (!ShutdownHookManager.inShutdown()) {
            // Collect latest accumulator values to report back to the driver
            val accums: Seq[AccumulatorV2[_, _]] =
              if (task != null) {
                task.metrics.setExecutorRunTime(System.currentTimeMillis() - taskStart)
                task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
                task.collectAccumulatorUpdates(taskFailed = true)
              } else {
                Seq.empty
              }

            val accUpdates = accums.map(acc => acc.toInfo(Some(acc.value), None))

            val serializedTaskEndReason = {
              try {
                ser.serialize(new ExceptionFailure(t, accUpdates).withAccums(accums))
              } catch {
                case _: NotSerializableException =>
                  // t is not serializable so just send the stacktrace
                  ser.serialize(new ExceptionFailure(t, accUpdates, false).withAccums(accums))
              }
            }
            setTaskFinishedAndClearInterruptStatus()
            execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)
          } else {
            logInfo("Not reporting error to driver during JVM shutdown.")
          }

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (!t.isInstanceOf[SparkOutOfMemoryError] && Utils.isFatalError(t)) {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t)
          }
      } finally {
        // 最后，从executor中的runningTasks中删除该task／TaskRunner
        runningTasks.remove(taskId)
      }
    }

    private def hasFetchFailure: Boolean = {
      task != null && task.context != null && task.context.fetchFailed.isDefined
    }
  }

  /**
   * Supervises the killing / cancellation of a task by sending the interrupted flag, optionally
   * sending a Thread.interrupt(), and monitoring the task until it finishes.
   *
   * Spark's current task cancellation / task killing mechanism is "best effort" because some tasks
   * may not be interruptable or may not respond to their "killed" flags being set. If a significant
   * fraction of a cluster's task slots are occupied by tasks that have been marked as killed but
   * remain running then this can lead to a situation where new jobs and tasks are starved of
   * resources that are being used by these zombie tasks.
   *
   * The TaskReaper was introduced in SPARK-18761 as a mechanism to monitor and clean up zombie
   * tasks. For backwards-compatibility / backportability this component is disabled by default
   * and must be explicitly enabled by setting `spark.task.reaper.enabled=true`.
   *
   * A TaskReaper is created for a particular task when that task is killed / cancelled. Typically
   * a task will have only one TaskReaper, but it's possible for a task to have up to two reapers
   * in case kill is called twice with different values for the `interrupt` parameter.
   *
   * Once created, a TaskReaper will run until its supervised task has finished running. If the
   * TaskReaper has not been configured to kill the JVM after a timeout (i.e. if
   * `spark.task.reaper.killTimeout < 0`) then this implies that the TaskReaper may run indefinitely
   * if the supervised task never exits.
   */
  private class TaskReaper(
      taskRunner: TaskRunner,
      val interruptThread: Boolean,
      val reason: String)
    extends Runnable {

    private[this] val taskId: Long = taskRunner.taskId

    private[this] val killPollingIntervalMs: Long =
      conf.getTimeAsMs("spark.task.reaper.pollingInterval", "10s")

    private[this] val killTimeoutMs: Long = conf.getTimeAsMs("spark.task.reaper.killTimeout", "-1")

    private[this] val takeThreadDump: Boolean =
      conf.getBoolean("spark.task.reaper.threadDump", true)

    override def run(): Unit = {
      val startTimeMs = System.currentTimeMillis()
      // 哈??? 好吧，注意这是一个def，不是一个val
      def elapsedTimeMs = System.currentTimeMillis() - startTimeMs
      // 是否超时
      def timeoutExceeded(): Boolean = killTimeoutMs > 0 && elapsedTimeMs > killTimeoutMs
      try {
        // Only attempt to kill the task once. If interruptThread = false then a second kill
        // attempt would be a no-op and if interruptThread = true then it may not be safe or
        // effective to interrupt multiple times:
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
        // Monitor the killed task until it exits. The synchronization logic here is complicated
        // because we don't want to synchronize on the taskRunner while possibly taking a thread
        // dump, but we also need to be careful to avoid races between checking whether the task
        // has finished and wait()ing for it to finish.
        var finished: Boolean = false
        while (!finished && !timeoutExceeded()) {
          taskRunner.synchronized {
            // We need to synchronize on the TaskRunner while checking whether the task has
            // finished in order to avoid a race where the task is marked as finished right after
            // we check and before we call wait().
            if (taskRunner.isFinished) {
              finished = true
            } else {
              taskRunner.wait(killPollingIntervalMs)
            }
          }
          if (taskRunner.isFinished) {
            finished = true
          } else {
            logWarning(s"Killed task $taskId is still running after $elapsedTimeMs ms")
            if (takeThreadDump) {
              try {
                // TODO read getThreadDumpForThread()
                Utils.getThreadDumpForThread(taskRunner.getThreadId).foreach { thread =>
                  if (thread.threadName == taskRunner.threadName) {
                    logWarning(s"Thread dump from task $taskId:\n${thread.stackTrace}")
                  }
                }
              } catch {
                case NonFatal(e) =>
                  logWarning("Exception thrown while obtaining thread dump: ", e)
              }
            }
          }
        }

        if (!taskRunner.isFinished && timeoutExceeded()) {
          if (isLocal) {
            logError(s"Killed task $taskId could not be stopped within $killTimeoutMs ms; " +
              "not killing JVM because we are running in local mode.")
          } else {
            // In non-local-mode, the exception thrown here will bubble up to the uncaught exception
            // handler and cause the executor JVM to exit.
            throw new SparkException(
              s"Killing executor JVM because killed task $taskId could not be stopped within " +
                s"$killTimeoutMs ms.")
          }
        }
      } finally {
        // Clean up entries in the taskReaperForTask map.
        taskReaperForTask.synchronized {
          taskReaperForTask.get(taskId).foreach { taskReaperInMap =>
            if (taskReaperInMap eq this) {
              taskReaperForTask.remove(taskId)
            } else {
              // This must have been a TaskReaper where interruptThread == false where a subsequent
              // killTask() call for the same task had interruptThread == true and overwrote the
              // map entry.
            }
          }
        }
      }
    }
  }

  /**
   * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
   * created by the interpreter to the search path
   */
  private def createClassLoader(): MutableURLClassLoader = {
    // Bootstrap the list of jars with the user class path.
    val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    if (userClassPathFirst) {
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  /**
   * If the REPL is in use, add another ClassLoader that will read
   * new classes defined by the REPL as the user types code
   */
  private def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader = {
    val classUri = conf.get("spark.repl.class.uri", null)
    if (classUri != null) {
      logInfo("Using REPL class URI: " + classUri)
      try {
        val _userClassPathFirst: java.lang.Boolean = userClassPathFirst
        val klass = Utils.classForName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[SparkConf], classOf[SparkEnv],
          classOf[String], classOf[ClassLoader], classOf[Boolean])
        constructor.newInstance(conf, env, classUri, parent, _userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      parent
    }
  }

  /**
   * Download any missing dependencies if we receive a new set of files and JARs from the
   * SparkContext. Also adds any new JARs we fetched to the class loader.
   */
  private def updateDependencies(newFiles: Map[String, Long], newJars: Map[String, Long]) {
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    synchronized {
      // Fetch missing dependencies
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        // Fetch file with useCache mode, close cache for local mode.
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
          env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
        currentFiles(name) = timestamp
      }
      for ((name, timestamp) <- newJars) {
        val localName = new URI(name).getPath.split("/").last
        val currentTimeStamp = currentJars.get(name)
          .orElse(currentJars.get(localName))
          .getOrElse(-1L)
        if (currentTimeStamp < timestamp) {
          logInfo("Fetching " + name + " with timestamp " + timestamp)
          // Fetch file with useCache mode, close cache for local mode.
          Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
            env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
          currentJars(name) = timestamp
          // Add it to our class loader
          val url = new File(SparkFiles.getRootDirectory(), localName).toURI.toURL
          if (!urlClassLoader.getURLs().contains(url)) {
            logInfo("Adding " + url + " to class loader")
            urlClassLoader.addURL(url)
          }
        }
      }
    }
  }

  // TODO read 发送心跳
  /** Reports heartbeat and metrics for active tasks to the driver. */
  private def reportHeartBeat(): Unit = {
    // list of (task id, accumUpdates) to send back to the driver
    val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]()
    val curGCTime = computeTotalGcTime()

    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner.task != null) {
        taskRunner.task.metrics.mergeShuffleReadMetrics()
        taskRunner.task.metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)
        accumUpdates += ((taskRunner.taskId, taskRunner.task.metrics.accumulators()))
      }
    }

    val message = Heartbeat(executorId, accumUpdates.toArray, env.blockManager.blockManagerId)
    try {
      // 心跳发送是阻塞的？
      val response = heartbeatReceiverRef.askSync[HeartbeatResponse](
          message, RpcTimeout(conf, "spark.executor.heartbeatInterval", "10s"))
      // 说明我们需要重新向BlockManagerMaster注册我们的BlockManager
      if (response.reregisterBlockManager) {
        logInfo("Told to re-register on heartbeat")
        env.blockManager.reregister()
      }
      heartbeatFailures = 0
    } catch {
      case NonFatal(e) =>
        logWarning("Issue communicating with driver in heartbeater", e)
        heartbeatFailures += 1
        // 因为该executor的已经联系不到driver了，所以就不需要再通知driver说什么"啊，我要退出啦"
        // 因为即使你想通知，你也联系不上driver啊。在driver很久没有收到该executor的消息后，自然会
        // 处理它。
        if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
          logError(s"Exit as unable to send heartbeats to driver " +
            s"more than $HEARTBEAT_MAX_FAILURES times")
          System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
        }
    }
  }

  /**
   * 安排一个任务用于向driver发送心跳和一些处于活跃状态的任务的部分统计信息
   * Schedules a task to report heartbeat and partial metrics for active tasks to driver.
   */
  private def startDriverHeartbeater(): Unit = {
    // 获取心跳发送时间周期
    val intervalMs = conf.getTimeAsMs("spark.executor.heartbeatInterval", "10s")

    // 第一次执行心跳任务需要等待的时间
    // 时间周期加上一个随机的时间，这样心跳就不会在同步中结束???啥意思???(这样是不是就可以
    // 错开各个executor向driver发送心跳的时机，以让driver减轻并发处理的压力???)
    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    // 执行心跳发送的线程
    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartBeat())
    }
    // 以固定的时间周期，执行心跳任务
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }
}

private[spark] object Executor {
  // This is reserved for internal use by components that need to read task properties before a
  // task is fully deserialized. When possible, the TaskContext.getLocalProperty call should be
  // used instead.
  val taskDeserializationProps: ThreadLocal[Properties] = new ThreadLocal[Properties]
}
