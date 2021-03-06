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

package org.apache.spark.scheduler.local

import java.io.File
import java.net.URL
import java.nio.ByteBuffer

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TaskState}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo

private case class ReviveOffers()

private case class StatusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer)

private case class KillTask(taskId: Long, interruptThread: Boolean, reason: String)

private case class StopExecutor()

/**
 * 在LocalSchedulerBackend上的调用，都会通过LocalEndpoint来序列化(该调用)。使用一个RpcEndpoint能
 * 够使得在LocalSchedulerBackend上的调用异步化，这是防止在LocalSchedulerBackend和TaskSchedulerImpl
 * 之间发生死锁的必要举措。
 * Calls to [[LocalSchedulerBackend]] are all serialized through LocalEndpoint. Using an
 * RpcEndpoint makes the calls on [[LocalSchedulerBackend]] asynchronous, which is necessary
 * to prevent deadlock between [[LocalSchedulerBackend]] and the [[TaskSchedulerImpl]].
 */
private[spark] class LocalEndpoint(
    override val rpcEnv: RpcEnv,
    userClassPath: Seq[URL],
    scheduler: TaskSchedulerImpl,
    executorBackend: LocalSchedulerBackend,
    private val totalCores: Int)
  extends ThreadSafeRpcEndpoint with Logging {

  private var freeCores = totalCores

  // 本地的executor的id是"driver"
  val localExecutorId = SparkContext.DRIVER_IDENTIFIER
  val localExecutorHostname = "localhost"

  // 创建一个Executor!!!(第一次接触到Executor的代码!)
  private val executor = new Executor(
    localExecutorId, localExecutorHostname, SparkEnv.get, userClassPath, isLocal = true)

  override def receive: PartialFunction[Any, Unit] = {
    case ReviveOffers =>
      reviveOffers()

    case StatusUpdate(taskId, state, serializedData) =>
      scheduler.statusUpdate(taskId, state, serializedData)
      // 如果该task已经结束（不管它是正常finish，还是failed、killed、lost），
      // 则释放该task之前占用的cpu资源
      if (TaskState.isFinished(state)) {
        freeCores += scheduler.CPUS_PER_TASK
        reviveOffers()
      }

    case KillTask(taskId, interruptThread, reason) =>
      executor.killTask(taskId, interruptThread, reason)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case StopExecutor =>
      executor.stop()
      context.reply(true)
  }

  def reviveOffers() {
    // 由于是本地运行spark，所以只有一个WorkerOffer
    val offers = IndexedSeq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores))
    for (task <- scheduler.resourceOffers(offers).flatten) {
      // 每执行一个任务，可用资源(cpu核数)就减少CPUS_PER_TASK个
      freeCores -= scheduler.CPUS_PER_TASK
      // 整一个resourceOffers的过程都是在申请task(多个从pending队列取出来的tasks)运行的资源，
      // 直到launchTask，才是真正的开始执行task
      executor.launchTask(executorBackend, task)
    }
  }
}

/**
 * Used when running a local version of Spark where the executor, backend, and master all run in
 * the same JVM. It sits behind a [[TaskSchedulerImpl]] and handles launching tasks on a single
 * Executor (created by the [[LocalSchedulerBackend]]) running locally.
 */
private[spark] class LocalSchedulerBackend(
    conf: SparkConf,
    scheduler: TaskSchedulerImpl,
    val totalCores: Int)
  extends SchedulerBackend with ExecutorBackend with Logging {

  // 这个属性么有override的吗???
  private val appId = "local-" + System.currentTimeMillis
  private var localEndpoint: RpcEndpointRef = null
  private val userClassPath = getUserClasspath(conf)
  private val listenerBus = scheduler.sc.listenerBus
  // TODO 先不管这个LauncherBackend
  // TODO read 跟Launcher有关，现在还不了解Launcher
  private val launcherBackend = new LauncherBackend() {
    override def conf: SparkConf = LocalSchedulerBackend.this.conf
    override def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  /**
   * Returns a list of URLs representing the user classpath.
   *
   * @param conf Spark configuration.
   */
  def getUserClasspath(conf: SparkConf): Seq[URL] = {
    // 索噶，原来会在这边导入spark.executor.extraClassPath参数配置的class
    val userClassPathStr = conf.getOption("spark.executor.extraClassPath")
    userClassPathStr.map(_.split(File.pathSeparator)).toSeq.flatten.map(new File(_).toURI.toURL)
  }

  launcherBackend.connect()

  override def start() {
    val rpcEnv = SparkEnv.get.rpcEnv
    // (为driver)创建一个本地的EndPoint(server)，相当于是driver和executor通信的桥梁
    // 在创建LocalEndpoint的里面会创建一个Executor，请看源码!!!
    val executorEndpoint = new LocalEndpoint(rpcEnv, userClassPath, scheduler, this, totalCores)
    localEndpoint = rpcEnv.setupEndpoint("LocalSchedulerBackendEndpoint", executorEndpoint)
    // 添加executor事件（此时，app还没start啊，但是此时bus也还启动啊，事件只是缓存在eventQueue里面，So，这个
    // 添加事件的先后顺序有关系吗？？？）：其实这里是创建了一个driver，但是driver也被spark内部视为executor来
    // 对待(或者是因为local模式运行，所以既是driver，又是executor???对的，应该这样理解。driver其实不用创建的。)
    // 会通过先前创建的AppStatusListener存储该executor的信息
    listenerBus.post(SparkListenerExecutorAdded(
      System.currentTimeMillis,
      executorEndpoint.localExecutorId, // 'driver'
      new ExecutorInfo(executorEndpoint.localExecutorHostname, totalCores, Map.empty)))
    // 我记得其它地方也有setAppId...
    launcherBackend.setAppId(appId)
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  override def stop() {
    stop(SparkAppHandle.State.FINISHED)
  }

  override def reviveOffers() {
    localEndpoint.send(ReviveOffers)
  }

  override def defaultParallelism(): Int =
    scheduler.conf.getInt("spark.default.parallelism", totalCores)

  override def killTask(
      taskId: Long, executorId: String, interruptThread: Boolean, reason: String) {
    // 如果你是本地kill task，那就不需要executorId了。因为本地就只有一个executor...
    localEndpoint.send(KillTask(taskId, interruptThread, reason))
  }

  override def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer) {
    localEndpoint.send(StatusUpdate(taskId, state, serializedData))
  }

  override def applicationId(): String = appId

  private def stop(finalState: SparkAppHandle.State): Unit = {
    localEndpoint.ask(StopExecutor)
    try {
      launcherBackend.setState(finalState)
    } finally {
      launcherBackend.close()
    }
  }

}
