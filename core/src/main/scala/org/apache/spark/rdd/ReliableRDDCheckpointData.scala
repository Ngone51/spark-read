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

import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.internal.Logging

/**
 * An implementation of checkpointing that writes the RDD data to reliable storage.
 * This allows drivers to be restarted on failure with previously computed state.
 */
private[spark] class ReliableRDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends RDDCheckpointData[T](rdd) with Logging {

  // The directory to which the associated RDD has been checkpointed to
  // This is assumed to be a non-local path that points to some reliable storage
  private val cpDir: String =
    // 完成这一步，checkpoint的文件路径目前为止为
    // hdfs://c1a51ee9-1daf-4169-991e-b290f88bac20/rdd-0/
    ReliableRDDCheckpointData.checkpointPath(rdd.context, rdd.id)
      .map(_.toString)
      .getOrElse { throw new SparkException("Checkpoint dir must be specified.") }

  /**
   * Return the directory to which this RDD was checkpointed.
   * If the RDD is not checkpointed yet, return None.
   */
  def getCheckpointDir: Option[String] = RDDCheckpointData.synchronized {
    if (isCheckpointed) {
      Some(cpDir.toString)
    } else {
      None
    }
  }

  /**
   * Materialize this RDD and write its content to a reliable DFS.
   * This is called immediately after the first action invoked on this RDD has completed.
   */
  protected override def doCheckpoint(): CheckpointRDD[T] = {
    val newRDD = ReliableCheckpointRDD.writeRDDToCheckpointDirectory(rdd, cpDir)

    // Optionally clean our checkpoint files if the reference is out of scope
    if (rdd.conf.getBoolean("spark.cleaner.referenceTracking.cleanCheckpoints", false)) {
      // TODO read SparkContext cleaner
      rdd.context.cleaner.foreach { cleaner =>
        cleaner.registerRDDCheckpointDataForCleanup(newRDD, rdd.id)
      }
    }

    logInfo(s"Done checkpointing RDD ${rdd.id} to $cpDir, new parent is RDD ${newRDD.id}")
    newRDD
  }

}

private[spark] object ReliableRDDCheckpointData extends Logging {

  /**
   * 返回该rdd的checkpoint data将要写入的目录。
   * 最终checkpoint文件的目录形式可能是这样的：
   * hdfs://c1a51ee9-1daf-4169-991e-b290f88bac20/rdd-0/part-00000
   * 第一个是（高可靠）文件系统的schema；
   * 第二个是uuid
   * 第三个是由字符串"rdd"和该rdd的id组成
   * 第四个是由字符串"part"和分区的id组成
   *
   * 注意：本方法完成了第三步；第二步由SparkContext的setCheckpointDirectory完成；
   * 第四步由ReliableCheckpointRDD的checkpointFileName()完成；
   * Return the path of the directory to which this RDD's checkpoint data is written.
   */
  def checkpointPath(sc: SparkContext, rddId: Int): Option[Path] = {
    sc.checkpointDir.map { dir => new Path(dir, s"rdd-$rddId") }
  }

  /** Clean up the files associated with the checkpoint data for this RDD. */
  def cleanCheckpoint(sc: SparkContext, rddId: Int): Unit = {
    checkpointPath(sc, rddId).foreach { path =>
      path.getFileSystem(sc.hadoopConfiguration).delete(path, true)
    }
  }
}
