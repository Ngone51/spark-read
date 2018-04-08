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

package org.apache.spark.rdd

import java.io.{FileNotFoundException, IOException}
import java.util.concurrent.TimeUnit

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.CHECKPOINT_COMPRESS
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * An RDD that reads from checkpoint files previously written to reliable storage.
 */
private[spark] class ReliableCheckpointRDD[T: ClassTag](
    sc: SparkContext,
    val checkpointPath: String,
    _partitioner: Option[Partitioner] = None
  ) extends CheckpointRDD[T](sc) {

  @transient private val hadoopConf = sc.hadoopConfiguration
  @transient private val cpath = new Path(checkpointPath)
  @transient private val fs = cpath.getFileSystem(hadoopConf)
  private val broadcastedConf = sc.broadcast(new SerializableConfiguration(hadoopConf))

  // Fail fast if checkpoint directory does not exist
  require(fs.exists(cpath), s"Checkpoint directory does not exist: $checkpointPath")

  /**
   * Return the path of the checkpoint directory this RDD reads data from.
   */
  override val getCheckpointFile: Option[String] = Some(checkpointPath)

  override val partitioner: Option[Partitioner] = {
    _partitioner.orElse {
      ReliableCheckpointRDD.readCheckpointedPartitionerFile(context, checkpointPath)
    }
  }

  /**
   * Return partitions described by the files in the checkpoint directory.
   *
   * Since the original RDD may belong to a prior application, there is no way to know a
   * priori the number of partitions to expect. This method assumes that the original set of
   * checkpoint files are fully preserved in a reliable storage across application lifespans.
   */
  protected override def getPartitions: Array[Partition] = {
    // listStatus can throw exception if path does not exist.
    val inputFiles = fs.listStatus(cpath)
      .map(_.getPath)
      .filter(_.getName.startsWith("part-"))
      .sortBy(_.getName.stripPrefix("part-").toInt)
    // Fail fast if input files are invalid
    inputFiles.zipWithIndex.foreach { case (path, i) =>
      if (path.getName != ReliableCheckpointRDD.checkpointFileName(i)) {
        throw new SparkException(s"Invalid checkpoint file: $path")
      }
    }
    Array.tabulate(inputFiles.length)(i => new CheckpointRDDPartition(i))
  }

  /**
   * Return the locations of the checkpoint file associated with the given partition.
   */
  protected override def getPreferredLocations(split: Partition): Seq[String] = {
    val status = fs.getFileStatus(
      new Path(checkpointPath, ReliableCheckpointRDD.checkpointFileName(split.index)))
    val locations = fs.getFileBlockLocations(status, 0, status.getLen)
    locations.headOption.toList.flatMap(_.getHosts).filter(_ != "localhost")
  }

  /**
   * 读取给定的partition相关的checkpoint文件的内容
   * Read the content of the checkpoint file associated with the given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val file = new Path(checkpointPath, ReliableCheckpointRDD.checkpointFileName(split.index))
    ReliableCheckpointRDD.readCheckpointFile(file, broadcastedConf, context)
  }

}

private[spark] object ReliableCheckpointRDD extends Logging {

  /**
   * 最终checkpoint文件的目录形式可能是这样的：
   * hdfs://c1a51ee9-1daf-4169-991e-b290f88bac20/rdd-0/part-00000
   * 第一个是（高可靠）文件系统的schema；
   * 第二个是uuid
   * 第三个是由字符串"rdd"和该rdd的id组成
   * 第四个是由字符串"part"和分区的id组成
   *
   * 注意：本方法完成了第四步；第二步由SparkContext的setCheckpointDirectory完成；
   * 第三步由ReliableRDDCheckpointData的checkpointPath完成；
   * Return the checkpoint file name for the given partition.
   */
  private def checkpointFileName(partitionIndex: Int): String = {
    "part-%05d".format(partitionIndex)
  }

  private def checkpointPartitionerFileName(): String = {
    "_partitioner"
  }

  /**
   * Write RDD to checkpoint files and return a ReliableCheckpointRDD representing the RDD.
   */
  def writeRDDToCheckpointDirectory[T: ClassTag](
      originalRDD: RDD[T],
      checkpointDir: String,
      blockSize: Int = -1): ReliableCheckpointRDD[T] = {
    val checkpointStartTimeNs = System.nanoTime()

    val sc = originalRDD.sparkContext

    // Create the output path for the checkpoint
    val checkpointDirPath = new Path(checkpointDir)
    val fs = checkpointDirPath.getFileSystem(sc.hadoopConfiguration)
    // 之前在SparkContext里setCheckpointDirectory的时候，不是已经mkdir过了吗???
    if (!fs.mkdirs(checkpointDirPath)) {
      throw new SparkException(s"Failed to create checkpoint path $checkpointDirPath")
    }

    // Save to file, and reload it as an RDD
    // 将hadoopConfiguration封装成一个broadcast变量（因为到时候需要在不同的executor上读取???）
    val broadcastedConf = sc.broadcast(
      new SerializableConfiguration(sc.hadoopConfiguration))
    // ********** 注意 ***********
    // 因为runJob是分布式执行的，实际的chcekpoint过程是，每个executor将originalRDD的某个partition
    // 的数据写入一个hdfs文件中（比如hdfs://c1a51ee9-1daf-4169-991e-b290f88bac20/rdd-0/part-00000
    // 它们的rdd的id是一样的，但是part的id不一样）
    // 而对于下面的writePartitionerToCheckpointDir()，貌似并没有按分布式的存，而是直接存到目录
    // 比如hdfs://c1a51ee9-1daf-4169-991e-b290f88bac20/rdd-0/_partitioner下
    // 当然hdfs本身是分布式的，这个我们不考虑。

    // 下面runjob()的第二个参数是要在final rdd(originalRDD)上运行的函数，除了该函数现在
    // 给出的两个参数，还有（TaskContext，Iterator）两个参数。当该函数应用在final rdd上时，
    // Iterator代表的就是final rdd的计算结果。然后，该函数就会把rdd的计算结果（iterator）写入
    // 到checkpoint文件中。
    // TODO: This is expensive because it computes the RDD again unnecessarily (SPARK-8582)
    sc.runJob(originalRDD,
      writePartitionToCheckpointFile[T](checkpointDirPath.toString, broadcastedConf) _)

    // 如果该rdd有自己的partitioner的话，那么，也把该partitioner写入checkpoint文件中
    if (originalRDD.partitioner.nonEmpty) {
      writePartitionerToCheckpointDir(sc, originalRDD.partitioner.get, checkpointDirPath)
    }

    val checkpointDurationMs =
      TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - checkpointStartTimeNs)
    logInfo(s"Checkpointing took $checkpointDurationMs ms.")

    val newRDD = new ReliableCheckpointRDD[T](
      sc, checkpointDirPath.toString, originalRDD.partitioner)
    if (newRDD.partitions.length != originalRDD.partitions.length) {
      throw new SparkException(
        "Checkpoint RDD has a different number of partitions from original RDD. Original " +
          s"RDD [ID: ${originalRDD.id}, num of partitions: ${originalRDD.partitions.length}]; " +
          s"Checkpoint RDD [ID: ${newRDD.id}, num of partitions: " +
          s"${newRDD.partitions.length}].")
    }
    newRDD
  }

  /**
   * Write an RDD partition's data to a checkpoint file.
   */
  def writePartitionToCheckpointFile[T: ClassTag](
      path: String,
      broadcastedConf: Broadcast[SerializableConfiguration],
      blockSize: Int = -1)(ctx: TaskContext, iterator: Iterator[T]) {
    val env = SparkEnv.get
    val outputDir = new Path(path)
    val fs = outputDir.getFileSystem(broadcastedConf.value.value)

    // 完成这一步，checkpoint的文件路径目前为止为
    // hdfs://c1a51ee9-1daf-4169-991e-b290f88bac20/rdd-0/part-00000
    val finalOutputName = ReliableCheckpointRDD.checkpointFileName(ctx.partitionId())
    // 创建最后写入数据的checkpoint文件的"句柄"???
    // (创建一个Path对象，没有真正创建该文件，只有通过fs调用create()之后，该文件
    // 才被真正创建。)
    val finalOutputPath = new Path(outputDir, finalOutputName)
    // 创建临时输出文件路径
    val tempOutputPath =
      new Path(outputDir, s".$finalOutputName-attempt-${ctx.attemptNumber()}")

    val bufferSize = env.conf.getInt("spark.buffer.size", 65536)

    val fileOutputStream = if (blockSize < 0) {
      // 创建临时文件，我们先把Checkpoint的输出数据写入该临时文件，到时候我们
      // 还会把该临时文件重命名为我们的finalOutputPath（spark经常这样做啊）
      val fileStream = fs.create(tempOutputPath, false, bufferSize)
      // 是否压缩rdd的checkpoint文件
      if (env.conf.get(CHECKPOINT_COMPRESS)) {
        CompressionCodec.createCodec(env.conf).compressedOutputStream(fileStream)
      } else {
        fileStream
      }
    } else {
      // This is mainly for testing purpose
      fs.create(tempOutputPath, false, bufferSize,
        fs.getDefaultReplication(fs.getWorkingDirectory), blockSize)
    }
    val serializer = env.serializer.newInstance()
    // 封装上面的文件输出流为序列化流
    val serializeStream = serializer.serializeStream(fileOutputStream)
    Utils.tryWithSafeFinally {
      serializeStream.writeAll(iterator)
    } {
      serializeStream.close()
    }

    // 将tempOutputPath重命名为我们的finalOutputPath；

    // 如果重命名失败
    if (!fs.rename(tempOutputPath, finalOutputPath)) {
      // 如果不存在文件finalOutputPath，说明保存失败
      if (!fs.exists(finalOutputPath)) {
        logInfo(s"Deleting tempOutputPath $tempOutputPath")
        fs.delete(tempOutputPath, false)
        // TODO 这里应该不是attemptNumber吧，我觉得应该是taskAttemptId()
        throw new IOException("Checkpoint failed: failed to save output of task: " +
          s"${ctx.attemptNumber()} and final output path does not exist: $finalOutputPath")
      } else {
        // 如果存在文件finalOutputPath，说明该task的另一个运行副本已经在我们之前完成重命名了。
        // Some other copy of this task must've finished before us and renamed it
        logInfo(s"Final output path $finalOutputPath already exists; not overwriting it")
        if (!fs.delete(tempOutputPath, false)) {
          logWarning(s"Error deleting ${tempOutputPath}")
        }
      }
    } // else：该rdd的checkpoint文件保存成功
  }

  /**
   * 保存该rdd的Partitioner到该rdd的checkpoint文件中
   * Write a partitioner to the given RDD checkpoint directory. This is done on a best-effort
   * basis; any exception while writing the partitioner is caught, logged and ignored.
   */
  private def writePartitionerToCheckpointDir(
    sc: SparkContext, partitioner: Partitioner, checkpointDirPath: Path): Unit = {
    try {
      val partitionerFilePath = new Path(checkpointDirPath, checkpointPartitionerFileName)
      val bufferSize = sc.conf.getInt("spark.buffer.size", 65536)
      val fs = partitionerFilePath.getFileSystem(sc.hadoopConfiguration)
      val fileOutputStream = fs.create(partitionerFilePath, false, bufferSize)
      // 获取一个序列化器实力
      val serializer = SparkEnv.get.serializer.newInstance()
      // 封装文件输出流为序列化流
      val serializeStream = serializer.serializeStream(fileOutputStream)
      Utils.tryWithSafeFinally {
        serializeStream.writeObject(partitioner)
      } {
        serializeStream.close()
      }
      logDebug(s"Written partitioner to $partitionerFilePath")
    } catch {
      // 只要是NonFatal的异常，我们都忽略它。
      // QUESTION：那万一partition没有写入成功呢???之后rdd恢复过程中没有该partition没关系吗???
      case NonFatal(e) =>
        logWarning(s"Error writing partitioner $partitioner to $checkpointDirPath")
    }
  }


  /**
   * Read a partitioner from the given RDD checkpoint directory, if it exists.
   * This is done on a best-effort basis; any exception while reading the partitioner is
   * caught, logged and ignored.
   */
  private def readCheckpointedPartitionerFile(
      sc: SparkContext,
      checkpointDirPath: String): Option[Partitioner] = {
    try {
      val bufferSize = sc.conf.getInt("spark.buffer.size", 65536)
      val partitionerFilePath = new Path(checkpointDirPath, checkpointPartitionerFileName)
      val fs = partitionerFilePath.getFileSystem(sc.hadoopConfiguration)
      val fileInputStream = fs.open(partitionerFilePath, bufferSize)
      val serializer = SparkEnv.get.serializer.newInstance()
      val partitioner = Utils.tryWithSafeFinally {
        val deserializeStream = serializer.deserializeStream(fileInputStream)
        Utils.tryWithSafeFinally {
          deserializeStream.readObject[Partitioner]
        } {
          deserializeStream.close()
        }
      } {
        fileInputStream.close()
      }

      logDebug(s"Read partitioner from $partitionerFilePath")
      Some(partitioner)
    } catch {
      case e: FileNotFoundException =>
        logDebug("No partitioner file", e)
        None
      case NonFatal(e) =>
        logWarning(s"Error reading partitioner from $checkpointDirPath, " +
            s"partitioner will not be recovered which may lead to performance loss", e)
        None
    }
  }

  /**
   * 读取指定checkpoint文件的内容
   * Read the content of the specified checkpoint file.
   */
  def readCheckpointFile[T](
      path: Path,
      broadcastedConf: Broadcast[SerializableConfiguration],
      context: TaskContext): Iterator[T] = {
    val env = SparkEnv.get
    val fs = path.getFileSystem(broadcastedConf.value.value)
    val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
    val fileInputStream = {
      val fileStream = fs.open(path, bufferSize)
      // 如果配置了压缩rdd的checkpoint文件内容，
      // 则封装文件流为解压输入流
      if (env.conf.get(CHECKPOINT_COMPRESS)) {
        CompressionCodec.createCodec(env.conf).compressedInputStream(fileStream)
      } else {
        fileStream
      }
    }
    val serializer = env.serializer.newInstance()
    // 封装解压输入流为反序列化流
    val deserializeStream = serializer.deserializeStream(fileInputStream)

    // 注册一个task结束时的callback：当该task结束时，关闭该输入流
    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener(context => deserializeStream.close())

    deserializeStream.asIterator.asInstanceOf[Iterator[T]]
  }

}
