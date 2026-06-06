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

package org.apache.spark.shuffle.streaming

import java.io.{ByteArrayOutputStream, IOException}
import java.util.zip.Checksum

import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}

/**
 * Map-side [[ShuffleWriter]] for the opt-in streaming shuffle engine. It is constructed by
 * `StreamingShuffleManager.getWriter` ONLY for shuffles marked with a [[StreamingShuffleHandle]];
 * every other handle continues to flow to the composed `SortShuffleManager` (the default + the
 * graceful-degradation fallback), so the streaming writer and the sort-based `SortShuffleWriter`
 * run side by side and the sort path is wholly unaffected by this class.
 *
 * Unlike `SortShuffleWriter`, which materializes a single merged, sorted output file per map task,
 * this writer partitions records into bounded per-partition in-memory buffers and pipelines them to
 * consumers as small (<=2MB) blocks as soon as they fill, removing the disk-materialization latency
 * inherent in sort-based shuffle (the design targets a 30-50% end-to-end latency reduction for
 * shuffle-bound 10GB+/100+ partition workloads). The behaviors required by the design are:
 *
 *  1. Bounded per-partition buffering: records are routed by `dep.partitioner.getPartition(key)`
 *     into per-partition buffers sized from the documented budget
 *     `(executorMemory * bufferSizePercent / 100) / numPartitions`, capped by the 2MB pipelined
 *     block size so each emitted block is at most
 *     [[BackpressureProtocol.MAX_PIPELINED_BLOCK_BYTES]].
 *  2. Memory-safe spill coordination: every emitted block is registered with the shared
 *     [[MemorySpillManager]], which accounts the bytes against the EXISTING
 *     [[org.apache.spark.memory.MemoryManager]] and spills the least-recently-used partitions to
 *     disk once the configured utilization threshold is crossed. This reuses the executor memory
 *     model unchanged; this writer never redesigns memory accounting.
 *  3. Flow control: block emission is paced through the shared [[BackpressureProtocol]]
 *     token-bucket rate limiter, and producer throughput is fed back to its heartbeat loop so a
 *     sustained consumer slowdown can trigger graceful fallback to sort-based shuffle.
 *  4. CRC32C integrity: each block carries a checksum computed with the EXISTING
 *     [[ShuffleChecksumHelper]] using the configured `spark.shuffle.checksum.algorithm` (CRC32C
 *     when so configured), the same facility sort-based shuffle uses, so the
 *     [[StreamingShuffleReader]] can validate every block and request retransmission on
 *     corruption. No new checksum implementation is introduced.
 *
 * Output discovery: on a successful write the map task emits a standard [[MapStatus]] built through
 * the `object MapStatus` factory, so the UNMODIFIED `MapOutputTracker` and DAG scheduler locate
 * streaming outputs EXACTLY as for sort-based shuffle. The writer never touches the scheduler
 * or the task lifecycle.
 *
 * Output lifetime & cleanup: [[stop]] is idempotent (mirrors `SortShuffleWriter`). On a SUCCESSFUL
 * stop the emitted blocks are RETAINED in the [[MemorySpillManager]] -- [[MapStatus]] advertises
 * them to reducers, so they must outlive the map task and are reclaimed only when a consumer
 * acknowledges them (through the [[StreamingBlockExchange]]) or the manager is torn down; releasing
 * them on success would delete advertised outputs before reducers could read them. On a FAILED stop
 * (or a producer crash) the writer reverts its buffered metrics, notifies subscribed readers via
 * [[StreamingBlockExchange.producerFailed]] so they invalidate and recompute upstream, and frees
 * every block this map registered via [[MemorySpillManager.unregisterMap]], so a failure leaks no
 * buffer memory -- the property gated by the 2-hour stress test in the suite.
 *
 * Coexistence strategy: this writer is instantiated ONLY on the streaming path. The default
 * `SortShuffleManager` fallback never constructs it, so the sort-based shuffle carries zero
 * streaming overhead. Being `private[spark]` and confined to the streaming package enforces the
 * zero-cross-contamination rule: existing components neither import nor depend on this class.
 *
 * Threading: a single map task thread drives `write`; the shared [[BackpressureProtocol]] and
 * [[MemorySpillManager]] are themselves thread-safe. Verbose logging is gated behind
 * `spark.shuffle.streaming.debug` to keep per-executor log volume under the mandated 10MB/hour.
 *
 * @param handle the [[StreamingShuffleHandle]] selecting the streaming path; supplies the
 *               `shuffleId` and the [[org.apache.spark.ShuffleDependency]]
 * @param mapId the unique id of this map task, forwarded verbatim into the emitted [[MapStatus]]
 * @param context the active [[TaskContext]] for this map task
 * @param writeMetrics the shuffle write metrics reporter updated as blocks and records are emitted
 * @param conf the active [[SparkConf]] supplying `spark.shuffle.*` settings
 * @param backpressure the shared producer/consumer flow-control + token-bucket rate limiter
 * @param spillManager the shared spill coordinator that bounds buffer memory via the MemoryManager
 * @param exchange the in-process producer->consumer data path that delivers emitted blocks to the
 *                 streaming reader and carries acknowledgments/retransmission requests back
 * @param metricsSource the shared JMX metrics source for streaming-shuffle telemetry
 * @tparam K the type of the keys being written
 * @tparam V the type of the values being written
 */
private[spark] class StreamingShuffleWriter[K, V](
    handle: StreamingShuffleHandle[K, V, _],
    mapId: Long,
    context: TaskContext,
    writeMetrics: ShuffleWriteMetricsReporter,
    conf: SparkConf,
    backpressure: BackpressureProtocol,
    spillManager: MemorySpillManager,
    exchange: StreamingBlockExchange,
    metricsSource: StreamingShuffleSource)
  extends ShuffleWriter[K, V] with Logging {

  import StreamingShuffleWriter._

  // The ShuffleDependency carrying the partitioner and serializer for this shuffle (mirrors
  // SortShuffleWriter). Read once so the streaming write path matches the sort-based writer.
  private val dep = handle.dependency

  // The shuffle id this writer produces output for; the spill-registry key and the block id base.
  private val shuffleId: Int = dep.shuffleId

  // The live SparkEnv for this executor. The writer always runs inside a task with a running env,
  // exactly like SortShuffleWriter, which dereferences SparkEnv.get.blockManager directly.
  private val env = SparkEnv.get

  // Block manager whose shuffleServerId becomes the MapStatus location, so the UNMODIFIED
  // MapOutputTracker locates streaming outputs exactly as for sort-based shuffle (mirrors
  // SortShuffleWriter L39).
  private val blockManager = env.blockManager

  // This map task's partition index in its stage (== context.partitionId()). Carried with every
  // emitted block so the reader can attribute coverage and populate FetchFailedException for the
  // right producer, matching SortShuffleWriter's use of the map index for output location.
  private val mapIndex: Int = context.partitionId()

  // This producer's block-manager address, advertised in the MapStatus and carried with each block
  // so the reader can populate FetchFailedException for the right producer on invalidation.
  private val bmAddress: BlockManagerId = blockManager.shuffleServerId

  // Coexistence (serialization): blocks are wrapped with the EXISTING SerializerManager so the
  // compression/encryption applied here EXACTLY mirrors the unwrap the StreamingShuffleReader does
  // via wrapStream(blockId, in). Reuse only -- no new serialization or transport stack.
  private val serializerManager = env.serializerManager

  // Number of reduce partitions for this shuffle; sizes every per-partition data structure below.
  private val numPartitions: Int = dep.partitioner.numPartitions

  // Percent of executor memory budgeted for streaming buffers (spark.shuffle.streaming.
  // bufferSizePercent, validated 1-50, default 20). READ-ONLY; this file never defines the entry.
  private val bufferPercent: Int = conf.get(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT)

  // Checksum algorithm for per-block integrity (CRC32C when so configured). Reuses the EXISTING
  // shuffle checksum config rather than introducing a new one; passed verbatim to
  // ShuffleChecksumHelper.
  private val checksumAlgorithm: String = conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM)

  // Verbose debug-logging gate (spark.shuffle.streaming.debug). Held off the hot path so
  // per-executor log volume stays under the mandated 10MB/hour budget.
  private val debug: Boolean = conf.get(config.STREAMING_SHUFFLE_DEBUG)

  // Performance note: each partition flushes a block once its buffered (compressed) size reaches
  // this threshold, derived from the documented per-partition budget
  //   (executorMemory * bufferSizePercent / 100) / numPartitions
  // and capped by the 2MB pipelined block size. Streaming small blocks as soon as they fill --
  // rather than materializing one merged file at the end -- removes the disk-materialization
  // latency and targets the 30-50% end-to-end latency reduction for shuffle-bound (10GB+/100+
  // partition) workloads.
  private val blockFlushThresholdBytes: Long = computeBlockFlushThreshold()

  // Per-partition buffers, created lazily on first record and recreated after each block flush.
  // A null entry means the partition currently has no open buffer. Touched only by the task thread.
  private val buffers = new Array[PartitionBuffer](numPartitions)

  // Monotonic per-partition block sequence number; combined with (shuffleId, mapId, partitionId) it
  // forms the addressable BlockKey so every emitted block is uniquely identifiable for delivery,
  // acknowledgment, and retransmission.
  private val blockSeq = new Array[Long](numPartitions)

  // Count of blocks emitted per reduce partition; reported to the exchange via completeMap so the
  // reader can verify it has received every expected block before completing (no truncated reads).
  private val blockCounts = new Array[Long](numPartitions)

  // Bytes emitted per reduce partition; returned by getPartitionLengths() and put in MapStatus.
  private val partitionLengths = new Array[Long](numPartitions)

  // Aggregated per-block CRC32C fingerprint (summation is order-independent and deterministic for a
  // given input), passed as the MapStatus checksum so map-output changes across retries surface.
  private var aggregatedChecksum: Long = 0L

  // Running totals used to revert buffered write metrics on a failed stop (decBytesWritten/
  // decRecordsWritten), so task metrics never over-report after a producer failure.
  private var totalBytesWritten: Long = 0L
  private var totalRecordsWritten: Long = 0L

  // Idempotency flag: map tasks may call stop(success = true) and then stop(success = false) on a
  // later exception, so we must avoid releasing buffers twice (mirrors SortShuffleWriter L46).
  private var stopping = false

  // Ensures the finally-block cleanup runs exactly once even across repeated stop() calls.
  private var released = false

  // The MapStatus produced by a successful write(); remains null until write() completes.
  private var mapStatus: MapStatus = null

  if (debug) {
    logDebug(s"StreamingShuffleWriter created for shuffle $shuffleId map $mapId " +
      s"(task ${context.taskAttemptId()}, stage ${context.stageId()}) with $numPartitions " +
      s"partitions; blockFlushThreshold=$blockFlushThresholdBytes bytes")
  }

  /**
   * Writes this map task's records into per-partition buffers and pipelines them to consumers as
   * <=2MB blocks, then builds the [[MapStatus]] consumed by the unchanged MapOutputTracker.
   * Mirrors `SortShuffleWriter.write` in shape (partition, buffer, account, emit `MapStatus`).
   */
  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val writeStartNanos = System.nanoTime()
    while (records.hasNext) {
      val record = records.next()
      val partitionId = dep.partitioner.getPartition(record._1)
      val buffer = bufferFor(partitionId)
      writePair(buffer.serStream, record._1, record._2)
      buffer.recordsInBlock += 1L
      // Bound each pipelined block to <=2MB: flush once the buffered (compressed) size reaches
      // the threshold so consumers receive in-progress data with minimal latency.
      if (buffer.out.size() >= blockFlushThresholdBytes) {
        flushBlock(buffer)
        buffers(partitionId) = null
      }
    }
    // Final flush: emit the tail block of every partition that still has buffered records.
    var partitionId = 0
    while (partitionId < numPartitions) {
      val buffer = buffers(partitionId)
      if (buffer != null && buffer.open) {
        flushBlock(buffer)
        buffers(partitionId) = null
      }
      partitionId += 1
    }
    writeMetrics.incWriteTime(System.nanoTime() - writeStartNanos)
    // Coexistence (coverage): tell the exchange this map finished and how many blocks it sent per
    // reduce partition, so each reader verifies block coverage before completing -- there is no
    // unconditional end-of-stream marker that could let a truncated read look like success.
    exchange.completeMap(shuffleId, mapId, mapIndex, blockCounts.clone())
    // Coexistence (output discovery): emit a STANDARD MapStatus via the `object MapStatus` factory
    // (MapStatus is a sealed trait, so `new` is impossible/incorrect). This keeps the UNMODIFIED
    // MapOutputTracker and DAG scheduler locating streaming outputs EXACTLY as for sort-based
    // shuffle -- the streaming engine changes how bytes move, never how outputs are discovered.
    mapStatus = MapStatus(bmAddress, partitionLengths, mapId, aggregatedChecksum)
  }

  /**
   * Closes this writer, returning the [[MapStatus]] on success. Idempotent (mirrors
   * `SortShuffleWriter`): map tasks may call stop(true) and then stop(false) on a later exception,
   * so the body runs at most once.
   *
   * CRITICAL output lifetime: on SUCCESS the emitted blocks are RETAINED in the spill manager (the
   * returned [[MapStatus]] advertises them to reducers, which reclaim them on ack), so we
   * must NOT release them here. On FAILURE we revert metrics, notify readers, and free every block
   * this map registered -- separating failure cleanup from successful output lifetime.
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      None
    } else {
      stopping = true
      if (success) {
        // Success: outputs remain in the spill manager until consumers acknowledge them (via the
        // exchange) or the manager is torn down. Releasing here would delete advertised outputs.
        Option(mapStatus)
      } else {
        // Failure cleanup, charged to shuffle write time like SortShuffleWriter L106-108.
        val cleanupStartNanos = System.nanoTime()
        try {
          // Revert buffered write metrics so a failed task does not over-report bytes/records.
          if (totalBytesWritten > 0L) {
            writeMetrics.decBytesWritten(totalBytesWritten)
          }
          if (totalRecordsWritten > 0L) {
            writeMetrics.decRecordsWritten(totalRecordsWritten)
          }
          abortOnFailure()
        } finally {
          writeMetrics.incWriteTime(System.nanoTime() - cleanupStartNanos)
        }
        None
      }
    }
  }

  override def getPartitionLengths(): Array[Long] = partitionLengths

  // Returns the open buffer for `partitionId`, creating a fresh one (a new, independently
  // deserializable block stream) when none is open. Each block is its own serialized stream so the
  // StreamingShuffleReader can deserialize blocks independently as they stream in.
  private def bufferFor(partitionId: Int): PartitionBuffer = {
    val existing = buffers(partitionId)
    if (existing != null && existing.open) {
      existing
    } else {
      val created = new PartitionBuffer(partitionId)
      buffers(partitionId) = created
      created
    }
  }

  // Serialize one key/value pair into a block stream. The serializer's writeKey/writeValue are
  // ClassTag-bounded; routing values through Any-typed parameters supplies ClassTag[Any] exactly
  // as DiskBlockObjectWriter.write(key: Any, value: Any) does, since K/V carry no ClassTag here.
  private def writePair(stream: SerializationStream, key: Any, value: Any): Unit = {
    stream.writeKey(key)
    stream.writeValue(value)
  }

  /**
   * Finalizes one partition's current block, computes its CRC32C, hands it to the spill manager for
   * memory-bounded buffering, paces its emission through backpressure, and accounts it against the
   * partition length and the write metrics.
   */
  private def flushBlock(buffer: PartitionBuffer): Unit = {
    if (buffer.open) {
      buffer.open = false
      // Close the serialization (and wrapped compression) stream so the block is a complete,
      // independently deserializable unit; ByteArrayOutputStream.close() is a safe no-op.
      buffer.serStream.close()
      val bytes = buffer.out.toByteArray
      if (bytes.length > 0) {
        emitBlock(buffer.partitionId, bytes, buffer.recordsInBlock)
      }
    }
  }

  // Emit a single finalized block for `partitionId` over the streaming data path. Order matters for
  // correctness: NOTHING is published or accounted unless the block is fully admitted, so an
  // oversize record, a sustained-slow-consumer fallback, or a task interruption aborts the block
  // cleanly with no partial state and no advertised-but-missing output.
  private def emitBlock(partitionId: Int, bytes: Array[Byte], records: Long): Unit = {
    val numBytes = bytes.length.toLong
    // Hard 2MB cap (MAJOR): a single record whose serialized block exceeds the pipelined cap cannot
    // be split into <=2MB blocks. Rather than emit an oversized streaming block, signal fallback to
    // sort-based shuffle (the manager composes SortShuffleManager for exactly this case).
    if (records <= 1L && numBytes > BackpressureProtocol.MAX_PIPELINED_BLOCK_BYTES) {
      throw new StreamingShuffleFallbackException(
        s"Streaming shuffle record for shuffle $shuffleId partition $partitionId serialized to " +
          s"$numBytes bytes, over the ${BackpressureProtocol.MAX_PIPELINED_BLOCK_BYTES}-byte " +
          "pipelined block cap and cannot be split; falling back to sort-based shuffle")
    }
    // Coexistence (integrity): compute the block checksum with the EXISTING ShuffleChecksumHelper
    // (CRC32C when so configured) over the EXACT bytes the reader validates -- the same facility
    // sort-based shuffle uses; no new checksum implementation is introduced.
    val checksum: Checksum = ShuffleChecksumHelper.getChecksumByAlgorithm(checksumAlgorithm)
    checksum.update(bytes, 0, bytes.length)
    val checksumValue = checksum.getValue
    // Flow control BEFORE emit: a sustained consumer slowdown (the >60s, 2x-slower signal) means
    // streaming can no longer keep up, so fall back to sort rather than emit into a stalled path.
    if (backpressure.shouldFallback) {
      throw new StreamingShuffleFallbackException(
        s"Streaming shuffle consumer is sustained-slow for shuffle $shuffleId; falling back to " +
          "sort-based shuffle")
    }
    // Pace emission through the shared token-bucket limiter, which also honors the producer-pause
    // and fallback signals. A false return means the block was NOT admitted: distinguish a task
    // interruption (abort the write) from a terminal backpressure/pause (fall back to sort). In
    // either case we publish nothing and update no metrics, so an unadmitted block is never
    // advertised as written (MAJOR interruption fix).
    if (!backpressure.acquireBlocking(numBytes)) {
      if (Thread.currentThread().isInterrupted) {
        throw new IOException(
          s"Streaming shuffle producer interrupted while pacing a $numBytes-byte block for " +
            s"shuffle $shuffleId partition $partitionId")
      } else {
        throw new StreamingShuffleFallbackException(
          s"Streaming shuffle could not admit a $numBytes-byte block for shuffle $shuffleId " +
            "within the backpressure deadline; falling back to sort-based shuffle")
      }
    }
    // Admitted: assign the block's sequence number and publish it over the in-process path. The
    // exchange registers the bytes with the spill manager (memory accounting + spill via the
    // EXISTING MemoryManager) and routes them to the owning reducer; a false return is a memory
    // backpressure hint (the block is still stored and routed), recorded as a backpressure event.
    val seq = blockSeq(partitionId)
    blockSeq(partitionId) = seq + 1L
    val key = MemorySpillManager.BlockKey(shuffleId, mapId, partitionId, seq)
    val meta = StreamingBlockExchange.BlockMeta(
      key, bmAddress, ShuffleBlockId(shuffleId, mapId, partitionId), mapIndex, checksumValue,
      numBytes)
    val granted = exchange.publishBlock(meta, bytes)
    if (!granted) {
      metricsSource.incBackpressureEvents()
      if (debug) {
        logDebug(s"Streaming shuffle buffer budget exhausted for shuffle $shuffleId partition " +
          s"$partitionId; spill scheduled, throttling producer")
      }
    }
    // Feed producer throughput so the heartbeat loop can compare it against consumer throughput and
    // trigger graceful fallback to sort-based shuffle on a sustained consumer slowdown.
    backpressure.recordProducerProgress(numBytes)
    // Account the admitted block exactly once, AFTER it has been published.
    aggregatedChecksum += checksumValue
    blockCounts(partitionId) += 1L
    partitionLengths(partitionId) += numBytes
    totalBytesWritten += numBytes
    totalRecordsWritten += records
    writeMetrics.incBytesWritten(numBytes)
    writeMetrics.incRecordsWritten(records)
  }

  // Compute the per-partition block-flush threshold in bytes from the documented budget
  //   (executorMemory * bufferSizePercent / 100) / numPartitions
  // capped by the 2MB pipelined block size and floored so progress is always possible. Double math
  // avoids Long overflow when maxOnHeapStorageMemory is the "unlimited" Long.MaxValue sentinel.
  private def computeBlockFlushThreshold(): Long = {
    val executorMemory = env.memoryManager.maxOnHeapStorageMemory.toDouble
    val parts = math.max(1, numPartitions).toDouble
    val perPartition = executorMemory * bufferPercent / PERCENT_SCALE / parts
    val capped = math.min(perPartition, BackpressureProtocol.MAX_PIPELINED_BLOCK_BYTES.toDouble)
    math.max(MIN_BLOCK_FLUSH_BYTES, capped.toLong)
  }

  // Failure cleanup (idempotent via `released`): close every open serialization stream, drop every
  // per-partition buffer, free ALL of this map's streaming blocks from the spill manager in one
  // shot (memory + any spill files), and tell readers this producer failed so they invalidate
  // partial reads and let the EXISTING FetchFailedException path recompute the upstream stage. This
  // runs ONLY on stop(success = false); successful output is retained until consumers ack it.
  private def abortOnFailure(): Unit = {
    if (!released) {
      released = true
      var partitionId = 0
      while (partitionId < numPartitions) {
        val buffer = buffers(partitionId)
        if (buffer != null) {
          closeQuietly(buffer)
          buffers(partitionId) = null
        }
        partitionId += 1
      }
      // One call frees memory and deletes any spill files for every block this map registered, so
      // there is no buffer-memory or disk leak on failure.
      spillManager.unregisterMap(shuffleId, mapId)
      // Tell any waiting readers this producer's output is gone; they atomically invalidate partial
      // reads and throw FetchFailedException, which the unmodified scheduler turns into upstream
      // stage recomputation -- preserving the lineage/fault-recovery model with zero data loss.
      exchange.producerFailed(shuffleId, mapId, mapIndex, bmAddress,
        s"Streaming shuffle map $mapId (index $mapIndex) for shuffle $shuffleId failed before " +
          "completion", null)
    }
  }

  // Close one buffer's serialization stream, swallowing any I/O error so cleanup of the remaining
  // partitions always proceeds (no partial-cleanup leak). Aborting an in-flight block is safe: the
  // consumer re-derives missing data through the existing FetchFailedException recomputation path.
  private def closeQuietly(buffer: PartitionBuffer): Unit = {
    if (buffer.open) {
      buffer.open = false
      try {
        buffer.serStream.close()
      } catch {
        case NonFatal(e) =>
          if (debug) {
            logDebug(s"Streaming shuffle failed to close buffer stream for shuffle $shuffleId", e)
          }
      }
    }
  }

  /**
   * One partition's in-progress block: a [[ByteArrayOutputStream]] wrapped with the EXISTING
   * SerializerManager (so compression/encryption mirror the reader's unwrap) feeding a fresh
   * [[SerializationStream]]. A new instance is created per block so each block is independently
   * deserializable by [[StreamingShuffleReader]]. Touched only by the single map task thread.
   */
  private final class PartitionBuffer(val partitionId: Int) {
    // Backing byte sink for this block; `size()` reports buffered bytes used for flush gating.
    val out = new ByteArrayOutputStream()
    // The block id used to wrap the stream; matches the id the reader passes to wrapStream so the
    // compress/encrypt decision is identical on both sides.
    private val blockId = ShuffleBlockId(shuffleId, mapId, partitionId)
    // Serialization stream for this block: serializerManager.wrapStream applies the compression/
    // encryption the reader unwraps; reuse only -- no new serialization stack.
    val serStream: SerializationStream =
      dep.serializer.newInstance().serializeStream(serializerManager.wrapStream(blockId, out))
    // Number of records serialized into the current block; folded into write metrics on flush.
    var recordsInBlock: Long = 0L
    // True while the stream is open; set false once flushed/closed so cleanup never double-closes.
    var open: Boolean = true
  }
}

/**
 * Constants for [[StreamingShuffleWriter]]. The pipelined block-size cap itself lives on
 * [[BackpressureProtocol]] so the producer and the rate limiter agree on the maximum block size.
 */
private[spark] object StreamingShuffleWriter {

  /** Scale used for percentage math when computing the per-partition buffer budget. */
  private val PERCENT_SCALE: Double = 100.0

  /** Floor for the per-partition block-flush threshold so a tiny budget still makes progress. */
  private val MIN_BLOCK_FLUSH_BYTES: Long = 1024L
}

/**
 * Signals that a record/block cannot be emitted over the streaming data path and the shuffle must
 * fall back to the sort-based engine. The streaming writer throws this for an unsplittable record
 * that exceeds the 2MB pipelined block cap, or when the backpressure protocol reports a terminal
 * fallback condition (a sustained-slow consumer or an admission deadline that is not an interrupt).
 *
 * Coexistence: [[FallbackShuffleWriter]] (which `StreamingShuffleManager.getWriter` wraps around
 * every streaming writer) CATCHES this exception during `write` and transparently re-runs the map
 * through the composed `SortShuffleManager`, replaying the buffered record sequence so the map
 * produces ordinary sort-based output and a real `MapStatus`. Graceful degradation is therefore
 * realized WITHOUT failing the task and without touching the scheduler or the sort path -- the
 * exception is an internal control signal that never escapes the fallback writer.
 */
private[spark] class StreamingShuffleFallbackException(message: String)
  extends SparkException(message)
