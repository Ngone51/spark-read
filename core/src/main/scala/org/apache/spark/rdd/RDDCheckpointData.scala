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

import org.apache.spark.Partition

/**
 * Enumeration to manage state transitions of an RDD through checkpointing
 *
 * [ Initialized --{@literal >} checkpointing in progress --{@literal >} checkpointed ]
 */
private[spark] object CheckpointState extends Enumeration {
  type CheckpointState = Value
  val Initialized, CheckpointingInProgress, Checkpointed = Value
}

/**
 * 这个类包含了所有和RDD检查点相关的信息。该类的每个实例关联了一个RDD。该类能够管理关联RDD的检查点流程，以及，
 * 通过提供检查点RDD的更新分区、迭代器和首选的存储位置，来管理提交的检查点的状态。
 * This class contains all the information related to RDD checkpointing. Each instance of this
 * class is associated with an RDD. It manages process of checkpointing of the associated RDD,
 * as well as, manages the post-checkpoint state by providing the updated partitions,
 * iterator and preferred locations of the checkpointed RDD.
 */
private[spark] abstract class RDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends Serializable {

  import CheckpointState._

  // The checkpoint state of the associated RDD.
  protected var cpState = Initialized

  // The RDD that contains our checkpointed data
  private var cpRDD: Option[CheckpointRDD[T]] = None

  // TODO: are we sure we need to use a global lock in the following methods?

  /**
   * 返回该RDD的检查点数据是否已经持久化
   * Return whether the checkpoint data for this RDD is already persisted.
   */
  def isCheckpointed: Boolean = RDDCheckpointData.synchronized {
    cpState == Checkpointed
  }

  /**
   * 具体化该RDD，并持久化它的内容。
   * 这个方法的调用发生在该RDD上第一个action被调用完成后
   * Materialize this RDD and persist its content.
   * This is called immediately after the first action invoked on this RDD has completed.
   */
  final def checkpoint(): Unit = {
    // Guard against multiple threads checkpointing the same RDD by
    // atomically flipping the state of this RDDCheckpointData
    RDDCheckpointData.synchronized {
      if (cpState == Initialized) {
        cpState = CheckpointingInProgress
      } else {
        return
      }
    }

    val newRDD = doCheckpoint()

    // Update our state and truncate the RDD lineage
    RDDCheckpointData.synchronized {
      // checkpoint完成，则设置cpRDD为保存了该rdd结果的newRDD
      cpRDD = Some(newRDD)
      // 标记cpState为Checkpointed
      cpState = Checkpointed
      // 标记该rdd已经完成checkpoint，则清除它的dependencies、partitions
      rdd.markCheckpointed()
    }
  }

  /**
   * Materialize this RDD and persist its content.
   *
   * Subclasses should override this method to define custom checkpointing behavior.
   * @return the checkpoint RDD created in the process.
   */
  protected def doCheckpoint(): CheckpointRDD[T]

  /**
   * Return the RDD that contains our checkpointed data.
   * This is only defined if the checkpoint state is `Checkpointed`.
   */
  def checkpointRDD: Option[CheckpointRDD[T]] = RDDCheckpointData.synchronized { cpRDD }

  /**
   * Return the partitions of the resulting checkpoint RDD.
   * For tests only.
   */
  def getPartitions: Array[Partition] = RDDCheckpointData.synchronized {
    cpRDD.map(_.partitions).getOrElse { Array.empty }
  }

}

/**
 * Global lock for synchronizing checkpoint operations.
 */
private[spark] object RDDCheckpointData
