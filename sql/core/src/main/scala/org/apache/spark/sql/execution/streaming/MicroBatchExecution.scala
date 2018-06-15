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

package org.apache.spark.sql.execution.streaming

import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Map => MutableMap}

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, CurrentBatchTimestamp, CurrentDate, CurrentTimestamp}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.v2.{StreamingDataSourceV2Relation, WriteToDataSourceV2}
import org.apache.spark.sql.sources.v2.DataSourceV2Options
import org.apache.spark.sql.sources.v2.streaming.{MicroBatchReadSupport, MicroBatchWriteSupport}
import org.apache.spark.sql.sources.v2.streaming.reader.{MicroBatchReader, Offset => OffsetV2}
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, Trigger}
import org.apache.spark.util.{Clock, Utils}

class MicroBatchExecution(
    sparkSession: SparkSession,
    name: String,
    checkpointRoot: String,
    analyzedPlan: LogicalPlan,
    sink: BaseStreamingSink,
    trigger: Trigger,
    triggerClock: Clock,
    outputMode: OutputMode,
    extraOptions: Map[String, String],
    deleteCheckpointOnStop: Boolean)
  extends StreamExecution(
    sparkSession, name, checkpointRoot, analyzedPlan, sink,
    trigger, triggerClock, outputMode, deleteCheckpointOnStop) {

  @volatile protected var sources: Seq[BaseStreamingSource] = Seq.empty

  private val triggerExecutor = trigger match {
    case t: ProcessingTime => ProcessingTimeExecutor(t, triggerClock)
    case OneTimeTrigger => OneTimeExecutor()
    case _ => throw new IllegalStateException(s"Unknown type of trigger: $trigger")
  }

  override lazy val logicalPlan: LogicalPlan = {
    assert(queryExecutionThread eq Thread.currentThread,
      "logicalPlan must be initialized in QueryExecutionThread " +
        s"but the current thread was ${Thread.currentThread}")
    var nextSourceId = 0L
    val toExecutionRelationMap = MutableMap[StreamingRelation, StreamingExecutionRelation]()
    val v2ToExecutionRelationMap = MutableMap[StreamingRelationV2, StreamingExecutionRelation]()
    // We transform each distinct streaming relation into a StreamingExecutionRelation, keeping a
    // map as we go to ensure each identical relation gets the same StreamingExecutionRelation
    // object. For each microbatch, the StreamingExecutionRelation will be replaced with a logical
    // plan for the data within that batch.
    // Note that we have to use the previous `output` as attributes in StreamingExecutionRelation,
    // since the existing logical plan has already used those attributes. The per-microbatch
    // transformation is responsible for replacing attributes with their final values.
    val _logicalPlan = analyzedPlan.transform {
      case streamingRelation@StreamingRelation(dataSource, _, output) =>
        toExecutionRelationMap.getOrElseUpdate(streamingRelation, {
          // 具体化source，以避免在每个batch中创建它
          // Materialize source to avoid creating it in every batch
          val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
          val source = dataSource.createSource(metadataPath)
          nextSourceId += 1
          StreamingExecutionRelation(source, output)(sparkSession)
        })
      case s @ StreamingRelationV2(source: MicroBatchReadSupport, _, options, output, _) =>
        v2ToExecutionRelationMap.getOrElseUpdate(s, {
          // Materialize source to avoid creating it in every batch
          val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
          val reader = source.createMicroBatchReader(
            Optional.empty(), // user specified schema
            metadataPath,
            new DataSourceV2Options(options.asJava))
          nextSourceId += 1
          StreamingExecutionRelation(reader, output)(sparkSession)
        })
      case s @ StreamingRelationV2(_, sourceName, _, output, v1Relation) =>
        v2ToExecutionRelationMap.getOrElseUpdate(s, {
          // Materialize source to avoid creating it in every batch
          val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
          if (v1Relation.isEmpty) {
            throw new UnsupportedOperationException(
              s"Data source $sourceName does not support microbatch processing.")
          }
          val source = v1Relation.get.dataSource.createSource(metadataPath)
          nextSourceId += 1
          StreamingExecutionRelation(source, output)(sparkSession)
        })
    }
    sources = _logicalPlan.collect { case s: StreamingExecutionRelation => s.source }
    uniqueSources = sources.distinct
    _logicalPlan
  }

  /**
   * 当数据到达，反复地尝试去执行batches
   * Repeatedly attempts to run batches as data arrives.
   */
  protected def runActivatedStream(sparkSessionForStream: SparkSession): Unit = {
    triggerExecutor.execute(() => {
      startTrigger()

      if (isActive) {
        reportTimeTaken("triggerExecution") {
          if (currentBatchId < 0) {
            // We'll do this initialization only once
            populateStartOffsets(sparkSessionForStream)
            // 注意，此时currentBatchId已经等于0（表示第一个batch）了，而不是-1
            sparkSession.sparkContext.setJobDescription(getBatchDescriptionString)
            logDebug(s"Stream running from $committedOffsets to $availableOffsets")
          } else {
            constructNextBatch()
          }
          if (dataAvailable) {
            currentStatus = currentStatus.copy(isDataAvailable = true)
            updateStatusMessage("Processing new data")
            // 执行当前的batch
            runBatch(sparkSessionForStream)
          }
        }
        // Report trigger as finished and construct progress object.
        finishTrigger(dataAvailable)
        if (dataAvailable) {
          // Update committed offsets.
          commitLog.add(currentBatchId)
          committedOffsets ++= availableOffsets
          logDebug(s"batch ${currentBatchId} committed")
          // We'll increase currentBatchId after we complete processing current batch's data
          currentBatchId += 1
          sparkSession.sparkContext.setJobDescription(getBatchDescriptionString)
        } else {
          currentStatus = currentStatus.copy(isDataAvailable = false)
          updateStatusMessage("Waiting for data to arrive")
          Thread.sleep(pollingDelayMs)
        }
      }
      updateStatusMessage("Waiting for next trigger")
      isActive
    })
  }

  /**
   * Populate the start offsets to start the execution at the current offsets stored in the sink
   * (i.e. avoid reprocessing data that we have already processed). This function must be called
   * before any processing occurs and will populate the following fields:
   *  - currentBatchId
   *  - committedOffsets
   *  - availableOffsets
   *  The basic structure of this method is as follows:
   *
   *  Identify (from the offset log) the offsets used to run the last batch
   *  IF last batch exists THEN
   *    Set the next batch to be executed as the last recovered batch
   *    Check the commit log to see which batch was committed last
   *    IF the last batch was committed THEN
   *      Call getBatch using the last batch start and end offsets
   *      // ^^^^ above line is needed since some sources assume last batch always re-executes
   *      Setup for a new batch i.e., start = last batch end, and identify new end
   *    DONE
   *  ELSE
   *    Identify a brand new batch
   *  DONE
   */
  private def populateStartOffsets(sparkSessionToRunBatches: SparkSession): Unit = {
    offsetLog.getLatest() match {
      case Some((latestBatchId, nextOffsets)) =>
        /* First assume that we are re-executing the latest known batch
         * in the offset log */
        currentBatchId = latestBatchId
        availableOffsets = nextOffsets.toStreamProgress(sources)
        /* Initialize committed offsets to a committed batch, which at this
         * is the second latest batch id in the offset log. */
        if (latestBatchId != 0) {
          val secondLatestBatchId = offsetLog.get(latestBatchId - 1).getOrElse {
            throw new IllegalStateException(s"batch ${latestBatchId - 1} doesn't exist")
          }
          committedOffsets = secondLatestBatchId.toStreamProgress(sources)
        }

        // update offset metadata
        nextOffsets.metadata.foreach { metadata =>
          OffsetSeqMetadata.setSessionConf(metadata, sparkSessionToRunBatches.conf)
          offsetSeqMetadata = OffsetSeqMetadata(
            metadata.batchWatermarkMs, metadata.batchTimestampMs, sparkSessionToRunBatches.conf)
        }

        /* identify the current batch id: if commit log indicates we successfully processed the
         * latest batch id in the offset log, then we can safely move to the next batch
         * i.e., committedBatchId + 1 */
        commitLog.getLatest() match {
          case Some((latestCommittedBatchId, _)) =>
            if (latestBatchId == latestCommittedBatchId) {
              /* The last batch was successfully committed, so we can safely process a
               * new next batch but first:
               * Make a call to getBatch using the offsets from previous batch.
               * because certain sources (e.g., KafkaSource) assume on restart the last
               * batch will be executed before getOffset is called again. */
              availableOffsets.foreach {
                case (source: Source, end: Offset) =>
                  val start = committedOffsets.get(source)
                  source.getBatch(start, end)
                case nonV1Tuple =>
                  // The V2 API does not have the same edge case requiring getBatch to be called
                  // here, so we do nothing here.
              }
              currentBatchId = latestCommittedBatchId + 1
              committedOffsets ++= availableOffsets
              // Construct a new batch be recomputing availableOffsets
              constructNextBatch()
            } else if (latestCommittedBatchId < latestBatchId - 1) {
              logWarning(s"Batch completion log latest batch id is " +
                s"${latestCommittedBatchId}, which is not trailing " +
                s"batchid $latestBatchId by one")
            }
          case None => logInfo("no commit log present")
        }
        logDebug(s"Resuming at batch $currentBatchId with committed offsets " +
          s"$committedOffsets and available offsets $availableOffsets")
      case None => // We are starting this stream for the first time.
        // 第一次启动该stream
        logInfo(s"Starting new streaming query.")
        // 设置第一个batchId = 0
        currentBatchId = 0
        constructNextBatch()
    }
  }

  /**
   * 如果有任何新数据到达，则返回true。
   * Returns true if there is any new data available to be processed.
   */
  private def dataAvailable: Boolean = {
    availableOffsets.exists {
      case (source, available) =>
        // QUESTION：为什么不是committed < available？？？
        committedOffsets
          .get(source)
          .map(committed => committed != available)
          .getOrElse(true)
    }
  }

  /**
   * 询问所有的sources来查看是否有新的数据到达。如果有新的数据到达，batchId计数器就会增加，并且一条新的带有
   * 最新的offset的日志记录会被写入（WAL）。
   * Queries all of the sources to see if any new data is available. When there is new data the
   * batchId counter is incremented and a new log entry is written with the newest offsets.
   */
  private def constructNextBatch(): Unit = {
    // Check to see what new data is available.
    val hasNewData = {
      // 上锁
      awaitProgressLock.lock()
      try {
        // Generate a map from each unique source to the next available offset.
        val latestOffsets: Map[BaseStreamingSource, Option[Offset]] = uniqueSources.map {
          case s: Source =>
            updateStatusMessage(s"Getting offsets from $s")
            reportTimeTaken("getOffset") {
              (s, s.getOffset)
            }
          case s: MicroBatchReader =>
            // TODO read MicroBatchReader
            updateStatusMessage(s"Getting offsets from $s")
            reportTimeTaken("getOffset") {
            // Once v1 streaming source execution is gone, we can refactor this away.
            // For now, we set the range here to get the source to infer the available end offset,
            // get that offset, and then set the range again when we later execute.
            s.setOffsetRange(
              toJava(availableOffsets.get(s).map(off => s.deserializeOffset(off.json))),
              Optional.empty())

              (s, Some(s.getEndOffset))
            }
        }.toMap
        // 注意，latestOffsets是Map[BaseStreamingSource, Option[Offset]]类型的，mapValues(_.get)会遍历map的
        // values，然后把Option中的值取出来，所以最后返回的是GenTraversableOnce[(BaseStreamingSource, Offset)]类型
        availableOffsets ++= latestOffsets.filter { case (_, o) => o.nonEmpty }.mapValues(_.get)

        if (dataAvailable) {
          true
        } else {
          noNewData = true
          false
        }
      } finally {
        // 释放锁
        awaitProgressLock.unlock()
      }
    }
    if (hasNewData) { // 如果有新数据
      var batchWatermarkMs = offsetSeqMetadata.batchWatermarkMs
      // TODO read lastExecution != null
      // Update the eventTime watermarks if we find any in the plan.
      if (lastExecution != null) {
        lastExecution.executedPlan.collect {
          case e: EventTimeWatermarkExec => e
        }.zipWithIndex.foreach {
          case (e, index) if e.eventTimeStats.value.count > 0 =>
            logDebug(s"Observed event time stats $index: ${e.eventTimeStats.value}")
            val newWatermarkMs = e.eventTimeStats.value.max - e.delayMs
            val prevWatermarkMs = watermarkMsMap.get(index)
            if (prevWatermarkMs.isEmpty || newWatermarkMs > prevWatermarkMs.get) {
              watermarkMsMap.put(index, newWatermarkMs)
            }

          // Populate 0 if we haven't seen any data yet for this watermark node.
          case (_, index) =>
            if (!watermarkMsMap.isDefinedAt(index)) {
              watermarkMsMap.put(index, 0)
            }
        }

        // Update the global watermark to the minimum of all watermark nodes.
        // This is the safest option, because only the global watermark is fault-tolerant. Making
        // it the minimum of all individual watermarks guarantees it will never advance past where
        // any individual watermark operator would be if it were in a plan by itself.
        if(!watermarkMsMap.isEmpty) {
          val newWatermarkMs = watermarkMsMap.minBy(_._2)._2
          if (newWatermarkMs > batchWatermarkMs) {
            logInfo(s"Updating eventTime watermark to: $newWatermarkMs ms")
            batchWatermarkMs = newWatermarkMs
          } else {
            logDebug(
              s"Event time didn't move: $newWatermarkMs < " +
                s"$batchWatermarkMs")
          }
        }
      }
      offsetSeqMetadata = offsetSeqMetadata.copy(
        batchWatermarkMs = batchWatermarkMs,
        batchTimestampMs = triggerClock.getTimeMillis()) // Current batch timestamp in milliseconds

      updateStatusMessage("Writing offsets to log")
      reportTimeTaken("walCommit") {
        // QUESTION：也就是说，一个batch可以同时对应多个sources？？？
        assert(offsetLog.add(
          currentBatchId,
          availableOffsets.toOffsetSeq(sources, offsetSeqMetadata)),
          s"Concurrent update to the log. Multiple streaming jobs detected for $currentBatchId")
        logInfo(s"Committed offsets for batch $currentBatchId. " +
          s"Metadata ${offsetSeqMetadata.toString}")

        // NOTE: The following code is correct because runStream() processes exactly one
        // batch at a time. If we add pipeline parallelism (multiple batches in flight at
        // the same time), this cleanup logic will need to change.

        // 既然我们已经更新了scheduler的持久化的检查点，那么对于sources来说，抛弃来自上一个batch的数据就是安全的。
        // Now that we've updated the scheduler's persistent checkpoint, it is safe for the
        // sources to discard data from the previous batch.
        if (currentBatchId != 0) { // 说明存在上一个batch（currentBatchId = 0说明是第一个batch，则不存在上一个batch）
          val prevBatchOff = offsetLog.get(currentBatchId - 1)
          if (prevBatchOff.isDefined) {
            prevBatchOff.get.toStreamProgress(sources).foreach {
              case (src: Source, off) => src.commit(off)
              case (reader: MicroBatchReader, off) =>
                reader.commit(reader.deserializeOffset(off.json))
            }
          } else {
            throw new IllegalStateException(s"batch $currentBatchId doesn't exist")
          }
        }

        // FIXME 如果currentBatchId已经大于minLogEntriesToMaintain了，而currentBatchId只会最近增大，
        // 那么，岂不是每次进来都会purge，且每次purge的只有先前的一个batch file。
        // It is now safe to discard the metadata beyond the minimum number to retain.
        // Note that purge is exclusive, i.e. it purges everything before the target ID.
        if (minLogEntriesToMaintain < currentBatchId) {
          offsetLog.purge(currentBatchId - minLogEntriesToMaintain)
          commitLog.purge(currentBatchId - minLogEntriesToMaintain)
        }
      }
    } else {
      awaitProgressLock.lock()
      try {
        // Wake up any threads that are waiting for the stream to progress.
        awaitProgressLockCondition.signalAll()
      } finally {
        awaitProgressLock.unlock()
      }
    }
  }

  /**
   * Processes any data available between `availableOffsets` and `committedOffsets`.
   * @param sparkSessionToRunBatch Isolated [[SparkSession]] to run this batch with.
   */
  private def runBatch(sparkSessionToRunBatch: SparkSession): Unit = {
    // Request unprocessed data from all sources.
    newData = reportTimeTaken("getBatch") {
      availableOffsets.flatMap {
        case (source: Source, available)
          if committedOffsets.get(source).map(_ != available).getOrElse(true) => // 确定Else是true？？？
          // current：上一个batch的结束offset + 1是当前batch的开始offset。
          // available： 则是当前batch的结束offset
          val current = committedOffsets.get(source)
          // 从source获取当前的batch（包含数据）。注意，此时该batch的offset信息已经写入WAL日志中
          val batch = source.getBatch(current, available)
          assert(batch.isStreaming,
            s"DataFrame returned by getBatch from $source did not have isStreaming=true\n" +
              s"${batch.queryExecution.logical}")
          logDebug(s"Retrieving data from $source: $current -> $available")
          // 返回一个map的Entry(source, logicalPlan)
          Some(source -> batch.logicalPlan)
        case (reader: MicroBatchReader, available)
          if committedOffsets.get(reader).map(_ != available).getOrElse(true) =>
          val current = committedOffsets.get(reader).map(off => reader.deserializeOffset(off.json))
          reader.setOffsetRange(
            toJava(current),
            Optional.of(available.asInstanceOf[OffsetV2]))
          logDebug(s"Retrieving data from $reader: $current -> $available")
          Some(reader ->
            new StreamingDataSourceV2Relation(reader.readSchema().toAttributes, reader))
        case _ => None
      }
    }

    // A list of attributes that will need to be updated.
    val replacements = new ArrayBuffer[(Attribute, Attribute)]
    // Replace sources in the logical plan with data that has arrived since the last batch.
    val newBatchesPlan = logicalPlan transform {
      case StreamingExecutionRelation(source, output) =>
        newData.get(source).map { dataPlan =>
          assert(output.size == dataPlan.output.size,
            s"Invalid batch: ${Utils.truncatedString(output, ",")} != " +
              s"${Utils.truncatedString(dataPlan.output, ",")}")
          replacements ++= output.zip(dataPlan.output)
          dataPlan
        }.getOrElse {
          LocalRelation(output, isStreaming = true)
        }
    }

    // Rewire the plan to use the new attributes that were returned by the source.
    val replacementMap = AttributeMap(replacements)
    val newAttributePlan = newBatchesPlan transformAllExpressions {
      case a: Attribute if replacementMap.contains(a) =>
        replacementMap(a).withMetadata(a.metadata)
      case ct: CurrentTimestamp =>
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          ct.dataType)
      case cd: CurrentDate =>
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          cd.dataType, cd.timeZoneId)
    }

    val triggerLogicalPlan = sink match {
      case _: Sink => newAttributePlan
      case s: MicroBatchWriteSupport =>
        val writer = s.createMicroBatchWriter(
          s"$runId",
          currentBatchId,
          newAttributePlan.schema,
          outputMode,
          new DataSourceV2Options(extraOptions.asJava))
        assert(writer.isPresent, "microbatch writer must always be present")
        WriteToDataSourceV2(writer.get, newAttributePlan)
      case _ => throw new IllegalArgumentException(s"unknown sink type for $sink")
    }

    reportTimeTaken("queryPlanning") {
      lastExecution = new IncrementalExecution(
        sparkSessionToRunBatch,
        triggerLogicalPlan,
        outputMode,
        checkpointFile("state"),
        runId,
        currentBatchId,
        offsetSeqMetadata)
      lastExecution.executedPlan // Force the lazy generation of execution plan
    }

    val nextBatch =
      new Dataset(sparkSessionToRunBatch, lastExecution, RowEncoder(lastExecution.analyzed.schema))

    reportTimeTaken("addBatch") {
      SQLExecution.withNewExecutionId(sparkSessionToRunBatch, lastExecution) {
        sink match {
          case s: Sink => s.addBatch(currentBatchId, nextBatch)
          case s: MicroBatchWriteSupport =>
            // This doesn't accumulate any data - it just forces execution of the microbatch writer.
            nextBatch.collect()
        }
      }
    }

    awaitProgressLock.lock()
    try {
      // Wake up any threads that are waiting for the stream to progress.
      awaitProgressLockCondition.signalAll()
    } finally {
      awaitProgressLock.unlock()
    }
  }

  private def toJava(scalaOption: Option[OffsetV2]): Optional[OffsetV2] = {
    Optional.ofNullable(scalaOption.orNull)
  }
}
