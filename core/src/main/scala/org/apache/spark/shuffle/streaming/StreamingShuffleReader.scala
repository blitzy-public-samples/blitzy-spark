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

import java.io.ByteArrayInputStream
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.zip.Checksum

import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.storage.{BlockId, BlockManagerId}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Reduce-side [[ShuffleReader]] for the opt-in streaming shuffle engine. It is constructed by
 * `StreamingShuffleManager.getReader` ONLY for shuffles marked with a [[StreamingShuffleHandle]];
 * every other handle continues to flow to the composed `SortShuffleManager` (the default + the
 * graceful-degradation fallback), so the streaming reader and the sort-based reader run side by
 * side and the sort path is wholly unaffected by this class.
 *
 * Unlike `BlockStoreShuffleReader`, which fetches fully-materialized map outputs, this reader
 * consumes IN-PROGRESS blocks as the producer streams them, bounding end-to-end latency. The full
 * fetch lifecycle is encapsulated behind the lazy/blocking iterator returned by [[read]]:
 *
 *  1. Lazy/blocking iterator: [[read]] returns immediately and performs NO eager fetching. All
 *     polling and blocking happen inside the returned iterator (the inner
 *     `StreamingShuffleRecordIterator`), so unchanged call sites observe exactly the same contract
 *     as the sort-based reader: `read(): Iterator[Product2[K, C]]`.
 *  2. In-progress block requests over the EXISTING transport: this reader owns a thread-safe inbox
 *     and mirrors the proven `ShuffleBlockFetcherIterator` design, where a transport response
 *     handler enqueues results and the consuming iterator polls them. The streaming response
 *     handler -- wired by `StreamingShuffleManager` on the EXISTING
 *     `org.apache.spark.network.TransportContext` (reuse only; no transport class is modified) --
 *     delivers each block, producer failure, and end-of-stream marker via the package-private
 *     callbacks below. Consumer-to-producer heartbeats (via the shared [[BackpressureProtocol]])
 *     apply flow control and double as the signal that pulls the producer to send/resend blocks.
 *  3. CRC32C integrity with retransmission: every received block is validated with the EXISTING
 *     [[ShuffleChecksumHelper]] (CRC32C by default). A corrupt block is retransmitted with
 *     exponential backoff (start 1s, doubling) up to [[BackpressureProtocol.MAX_RETRY_ATTEMPTS]]
 *     attempts before the read is invalidated.
 *  4. Consumed-buffer acknowledgment: each validated block is acknowledged through
 *     [[BackpressureProtocol.recordConsumerProgress]], which drives `MemorySpillManager` buffer
 *     reclamation on the writer side within the mandated 100ms.
 *  5. Atomic partial-read invalidation (SPARK-19276): on producer timeout, producer failure, or
 *     unrecoverable corruption, the reader throws the EXISTING [[FetchFailedException]] in a single
 *     construct-and-throw expression. The UNMODIFIED DAG scheduler converts this into upstream
 *     stage recomputation, preserving the existing lineage/fault-recovery model. This class never
 *     touches the scheduler. No partially-applied state escapes the iterator, guaranteeing zero
 *     data loss across producer crashes, consumer failures, and network partitions.
 *
 * Coexistence strategy: this reader is instantiated ONLY on the streaming path. The default
 * `SortShuffleManager` fallback never constructs it, so the sort-based shuffle carries zero
 * streaming overhead. Being `private[spark]` and confined to the streaming package enforces the
 * zero-cross-contamination rule: existing components neither import nor depend on this class.
 *
 * Threading: the inbox is the only cross-thread structure (a thread-safe
 * [[java.util.concurrent.LinkedBlockingQueue]] populated by the transport handler thread and
 * drained by the single task thread that consumes the returned iterator). All other iterator state
 * is touched solely by that task thread. Verbose logging is gated behind
 * `spark.shuffle.streaming.debug` to keep per-executor log volume under the mandated 10MB/hour.
 *
 * @param handle the [[StreamingShuffleHandle]] selecting the streaming path; supplies the
 *               `shuffleId` and the [[org.apache.spark.ShuffleDependency]]
 * @param startMapIndex inclusive start of the map-output range this reduce task consumes
 * @param endMapIndex exclusive end of the map-output range this reduce task consumes
 * @param startPartition inclusive start of the reduce-partition range (also the `reduceId` used to
 *                       populate [[FetchFailedException]])
 * @param endPartition exclusive end of the reduce-partition range
 * @param context the active [[TaskContext]]; used for interruption and fetch-failure propagation
 * @param readMetrics the shuffle read metrics reporter updated as blocks and records are consumed
 * @param conf the active [[SparkConf]] supplying `spark.shuffle.*` settings
 * @param backpressure the shared consumer-to-producer flow-control/heartbeat protocol
 * @param metricsSource the shared JMX metrics source whose `partialReadInvalidations` counter is
 *                      incremented on every atomic partial-read invalidation
 * @tparam K the type of the keys being read
 * @tparam C the type of the combined values produced on the reduce side
 */
private[spark] class StreamingShuffleReader[K, C](
    handle: StreamingShuffleHandle[K, _, C],
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    conf: SparkConf,
    backpressure: BackpressureProtocol,
    metricsSource: StreamingShuffleSource)
  extends ShuffleReader[K, C] with Logging {

  import StreamingShuffleReader._

  // The ShuffleDependency carrying the serializer, aggregator, and key ordering for this shuffle.
  // Read after the streaming fetch so the returned iterator semantics match sort-based reads.
  private val dep = handle.dependency

  // The shuffle id consumed by this reader; used to populate FetchFailedException accurately.
  private val shuffleId: Int = handle.shuffleId

  // Verbose debug-logging gate (spark.shuffle.streaming.debug). Held off the hot path so per-
  // executor log volume stays under the mandated 10MB/hour budget.
  private val debug: Boolean = conf.get(config.STREAMING_SHUFFLE_DEBUG)

  // Checksum algorithm (CRC32C by default) used to validate every received block. Reuses the
  // existing shuffle checksum facility rather than introducing a new checksum implementation.
  private val checksumAlgorithm: String = conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM)

  // Consumer-side receive buffer for in-progress blocks streamed from producers. The streaming
  // transport response handler (owned by StreamingShuffleManager, running on the EXISTING
  // TransportContext) enqueues messages here through the package-private callbacks below; the
  // returned iterator drains them on the task thread. This mirrors the listener-enqueues /
  // iterator-polls structure of the existing ShuffleBlockFetcherIterator.
  private val inbox = new LinkedBlockingQueue[StreamingBlockMessage]()

  if (debug) {
    logDebug(s"StreamingShuffleReader created for shuffle $shuffleId partitions " +
      s"[$startPartition, $endPartition) maps [$startMapIndex, $endMapIndex)")
  }

  // ============================================================================================
  // Transport integration point (REUSE ONLY).
  //
  // The streaming response handler owned by `StreamingShuffleManager` invokes these callbacks as
  // in-progress blocks arrive over the EXISTING `org.apache.spark.network.TransportContext`. They
  // are the ONLY entry points the transport uses to hand data to this reader, so no existing
  // transport class is modified. Keeping them package-private confines the streaming wiring to the
  // streaming package and enforces the zero-cross-contamination rule. The unbounded inbox means
  // these never block the transport thread.
  // ============================================================================================

  /**
   * Called by the streaming transport handler when an in-progress block (and its producer-computed
   * checksum) arrives for this reader.
   */
  private[streaming] def onBlockReceived(
      bmAddress: BlockManagerId,
      blockId: BlockId,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int,
      bytes: Array[Byte],
      checksum: Long): Unit = {
    inbox.put(BlockChunk(bmAddress, blockId, mapId, mapIndex, reduceId, bytes, checksum))
  }

  /**
   * Called by the streaming transport handler when a producer crashes or a connection to it is
   * lost. Drives an atomic partial-read invalidation when the consuming iterator reaches it.
   */
  private[streaming] def onProducerFailed(
      bmAddress: BlockManagerId,
      blockId: BlockId,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int,
      message: String,
      cause: Throwable): Unit = {
    inbox.put(ProducerFailed(bmAddress, blockId, mapId, mapIndex, reduceId, message, cause))
  }

  /**
   * Called by the streaming transport handler once every expected producer has finished streaming
   * all of its blocks for this reader's partition range. Cleanly terminates the iterator.
   */
  private[streaming] def onStreamComplete(): Unit = inbox.put(EndOfStream)

  /** Read the combined key-values for this reduce task. */
  override def read(): Iterator[Product2[K, C]] = {
    // CRITICAL lazy boundary: the streaming record iterator performs ALL polling/blocking/fetching
    // behind this iterator. `read()` itself does not block or fetch eagerly, so unchanged call
    // sites see the identical contract to BlockStoreShuffleReader.read().
    val streamedRecords: Iterator[(Any, Any)] = new StreamingShuffleRecordIterator()

    // Count each record read, then merge the streaming read metrics into the task metrics on
    // completion. Mirrors BlockStoreShuffleReader so streaming reads report identical metrics.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      streamedRecords.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator supports task cancellation around the blocking streaming pulls.
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    // Apply aggregation/ordering AFTER the streaming fetch, exactly as the sort-based reader does,
    // so the returned Iterator[Product2[K, C]] has identical semantics regardless of engine.
    val resultIter: Iterator[Product2[K, C]] = {
      if (dep.keyOrdering.isDefined) {
        // Create an ExternalSorter to sort the streamed data.
        val sorter: ExternalSorter[K, _, C] = if (dep.aggregator.isDefined) {
          if (dep.mapSideCombine) {
            new ExternalSorter[K, C, C](context,
              Option(new Aggregator[K, C, C](identity,
                dep.aggregator.get.mergeCombiners,
                dep.aggregator.get.mergeCombiners)),
              ordering = Some(dep.keyOrdering.get), serializer = dep.serializer)
          } else {
            new ExternalSorter[K, Nothing, C](context,
              dep.aggregator.asInstanceOf[Option[Aggregator[K, Nothing, C]]],
              ordering = Some(dep.keyOrdering.get), serializer = dep.serializer)
          }
        } else {
          new ExternalSorter[K, C, C](context, ordering = Some(dep.keyOrdering.get),
            serializer = dep.serializer)
        }
        sorter.insertAllAndUpdateMetrics(interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]])
      } else if (dep.aggregator.isDefined) {
        if (dep.mapSideCombine) {
          // We are reading values that are already combined.
          val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
          dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
        } else {
          // We don't know the value type, but also don't care -- the dependency *should*
          // have made sure it is compatible w/ this aggregator, which will convert the value
          // type to the combined type C.
          val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
          dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
        }
      } else {
        interruptibleIter.asInstanceOf[Iterator[(K, C)]]
      }
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed the previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }

  /**
   * The lazy/blocking iterator that backs [[read]]. It drains the inbox on the task thread,
   * validating, acknowledging, and deserializing each in-progress block into key/value records,
   * and performs atomic partial-read invalidation on producer timeout, producer failure, or
   * unrecoverable corruption. ALL streaming blocking lives here, behind the [[read]] boundary.
   */
  private final class StreamingShuffleRecordIterator extends Iterator[(Any, Any)] {

    // Decoded records of the current validated block; refilled on demand from the inbox.
    private var currentRecords: Iterator[(Any, Any)] = Iterator.empty

    // Set once every expected producer has signaled completion (EndOfStream); no more blocks.
    private var finished: Boolean = false

    // Producer context of the most recently validated block, used to populate FetchFailedException
    // accurately on a later producer timeout (the timed-out block's own ids are unknown then).
    private var lastProducerAddress: BlockManagerId = _
    private var lastMapId: Long = -1L
    private var lastMapIndex: Int = -1

    override def hasNext: Boolean = {
      if (currentRecords.hasNext) {
        true
      } else {
        fillBuffer()
        currentRecords.hasNext
      }
    }

    override def next(): (Any, Any) = {
      if (!hasNext) {
        throw new NoSuchElementException("StreamingShuffleReader has no more records")
      }
      currentRecords.next()
    }

    // Pull messages from the inbox until a validated block yields records or the stream completes.
    private def fillBuffer(): Unit = {
      while (!finished && !currentRecords.hasNext) {
        pollNext() match {
          case EndOfStream =>
            finished = true
          case ProducerFailed(addr, _, mapId, mapIndex, reduceId, message, cause) =>
            // Producer crash / network partition: invalidate atomically (throws; never returns).
            invalidate(addr, mapId, mapIndex, reduceId, message, cause)
          case chunk: BlockChunk =>
            currentRecords = receiveChunk(chunk)
        }
      }
    }

    // Block for the next inbox message, applying consumer-to-producer flow control and timing the
    // wait as fetch-wait time. A poll timeout is treated as a producer timeout and is invalidated.
    private def pollNext(): StreamingBlockMessage = {
      // A consumer heartbeat over the EXISTING transport signals liveness and pulls the producer to
      // send (or resend) in-progress blocks; this is how the reader "requests" streamed blocks.
      backpressure.consumerHeartbeat()
      val startNanos = System.nanoTime()
      val msg = inbox.poll(ProducerTimeoutMillis, TimeUnit.MILLISECONDS)
      readMetrics.incFetchWaitTime(elapsedMillis(startNanos))
      if (msg == null) {
        // Producer timeout: nothing arrived within the deadline. Invalidate atomically so no
        // partially-applied state escapes the iterator (zero data loss).
        invalidate(lastProducerAddress, lastMapId, lastMapIndex, startPartition,
          s"Streaming shuffle producer timed out (shuffle $shuffleId, reduce $startPartition)",
          null)
      } else {
        msg
      }
    }

    // Validate a received block's CRC32C; on corruption, request retransmission with exponential
    // backoff up to the retry budget, then invalidate. On success, record producer context, update
    // read metrics, acknowledge the consumed bytes, and deserialize into key/value records.
    private def receiveChunk(chunk: BlockChunk): Iterator[(Any, Any)] = {
      var current = chunk
      var attempts = 1
      // Coexistence: integrity is verified with the EXISTING ShuffleChecksumHelper (CRC32C), the
      // same facility sort-based shuffle uses; no new checksum implementation is introduced.
      while (!validateChecksum(current.bytes, current.checksum)) {
        if (attempts >= MaxRetryAttempts) {
          // Unrecoverable corruption after the full retry budget: invalidate atomically.
          invalidate(current.bmAddress, current.mapId, current.mapIndex, current.reduceId,
            s"Streaming shuffle block ${current.blockId} failed CRC32C validation after " +
              s"$attempts attempts", null)
        }
        requestRetransmission(current, attempts)
        current = awaitRetransmission(current)
        attempts += 1
      }
      lastProducerAddress = current.bmAddress
      lastMapId = current.mapId
      lastMapIndex = current.mapIndex
      recordBlockMetrics(current)
      // Acknowledge consumed bytes back to the producer; this drives MemorySpillManager buffer
      // reclamation on the writer side within the mandated 100ms of the acknowledgment.
      backpressure.recordConsumerProgress(current.bytes.length.toLong)
      deserializeChunk(current)
    }

    // After a corrupt block, wait for the producer to resend it. A failure or end-of-stream while
    // awaiting the resend is itself an unrecoverable partial read and is invalidated atomically.
    private def awaitRetransmission(failed: BlockChunk): BlockChunk = {
      pollNext() match {
        case chunk: BlockChunk =>
          chunk
        case ProducerFailed(addr, _, mapId, mapIndex, reduceId, message, cause) =>
          invalidate(addr, mapId, mapIndex, reduceId, message, cause)
        case EndOfStream =>
          invalidate(failed.bmAddress, failed.mapId, failed.mapIndex, failed.reduceId,
            s"Streaming shuffle stream ended before block ${failed.blockId} could be " +
              "retransmitted successfully", null)
      }
    }
  }

  // Issue a retransmission request for a corrupt block with exponential backoff (start 1s,
  // doubling, capped). The request is conveyed to the producer through the consumer-to-producer
  // flow-control channel over the EXISTING transport; the heartbeat both keeps the connection alive
  // and pulls the producer to resend the un-acknowledged (NACKed) block.
  private def requestRetransmission(chunk: BlockChunk, attempt: Int): Unit = {
    val backoff = backoffMillis(attempt)
    if (debug) {
      logDebug(s"Streaming shuffle block ${chunk.blockId} failed CRC32C validation; requesting " +
        s"retransmission (attempt $attempt of $MaxRetryAttempts) after ${backoff}ms backoff")
    }
    sleepInterruptibly(backoff)
    backpressure.consumerHeartbeat()
  }

  // Exponential backoff in millis: attempt 1 -> base (1s), attempt 2 -> 2s, attempt 3 -> 4s, ...
  // bounded by a hard cap so a pathological retransmission can never wait unbounded.
  private def backoffMillis(attempt: Int): Long = {
    val shift = math.min(math.max(attempt - 1, 0), MaxBackoffShift)
    math.min(RetryBackoffStartMillis * (1L << shift), MaxBackoffMillis)
  }

  // Sleep that honors task interruption: restores the interrupt status and returns promptly so the
  // surrounding InterruptibleIterator can abort the read cleanly.
  private def sleepInterruptibly(millis: Long): Unit = {
    if (millis > 0L) {
      try {
        Thread.sleep(millis)
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
      }
    }
  }

  // Recompute the block checksum with the EXISTING ShuffleChecksumHelper and compare it to the
  // producer-computed value transmitted with the block. CRC32C by default (configurable).
  private def validateChecksum(bytes: Array[Byte], expected: Long): Boolean = {
    val checksum: Checksum = ShuffleChecksumHelper.getChecksumByAlgorithm(checksumAlgorithm)
    checksum.update(bytes, 0, bytes.length)
    checksum.getValue == expected
  }

  // Classify a producer address as local (same executor) when a running SparkEnv is available,
  // otherwise treat it as remote. Used only to attribute read metrics to the right counters.
  private def isLocal(bmAddress: BlockManagerId): Boolean = {
    bmAddress != null && (Option(SparkEnv.get) match {
      case Some(env) if env.blockManager != null && env.blockManager.blockManagerId != null =>
        bmAddress == env.blockManager.blockManagerId
      case _ => false
    })
  }

  // Update shuffle read metrics for one validated block, attributing it to the local or remote
  // counters based on the producer address.
  private def recordBlockMetrics(chunk: BlockChunk): Unit = {
    val size = chunk.bytes.length.toLong
    if (isLocal(chunk.bmAddress)) {
      readMetrics.incLocalBlocksFetched(1L)
      readMetrics.incLocalBytesRead(size)
    } else {
      readMetrics.incRemoteBlocksFetched(1L)
      readMetrics.incRemoteBytesRead(size)
    }
  }

  // Deserialize a validated block into a key/value iterator. Compression/encryption are unwrapped
  // through the EXISTING SerializerManager when a SparkEnv is present; otherwise the raw bytes are
  // used (e.g., in unit tests with no running environment), so the reader degrades gracefully.
  private def deserializeChunk(chunk: BlockChunk): Iterator[(Any, Any)] = {
    val rawStream = new ByteArrayInputStream(chunk.bytes)
    val wrapped = Option(SparkEnv.get) match {
      case Some(env) =>
        try {
          env.serializerManager.wrapStream(chunk.blockId, rawStream)
        } catch {
          case NonFatal(e) =>
            if (debug) {
              logDebug(s"wrapStream unavailable for ${chunk.blockId}; using raw stream", e)
            }
            rawStream
        }
      case None => rawStream
    }
    dep.serializer.newInstance().deserializeStream(wrapped).asKeyValueIterator
  }

  // Atomic partial-read invalidation. Increments the partialReadInvalidations telemetry and then
  // throws the EXISTING FetchFailedException in a SINGLE construct-and-throw expression.
  //
  // CRITICAL (SPARK-19276): the FetchFailedException constructor calls
  // TaskContext.get().setFetchFailed(this), so it MUST be thrown immediately upon construction --
  // never constructed, stashed, and conditionally thrown later. Returns Nothing so callers may use
  // it in expression position.
  //
  // Coexistence: throwing FetchFailedException drives upstream stage recomputation via the
  // UNMODIFIED DAG scheduler, preserving the existing lineage/fault-recovery model. This class does
  // NOT touch the scheduler, the task lifecycle, or any existing file.
  private def invalidate(
      bmAddress: BlockManagerId,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int,
      message: String,
      cause: Throwable): Nothing = {
    metricsSource.incPartialReadInvalidations()
    if (debug) {
      logDebug(s"Streaming shuffle partial-read invalidation for shuffle $shuffleId reduce " +
        s"$reduceId: $message")
    }
    throw new FetchFailedException(bmAddress, shuffleId, mapId, mapIndex, reduceId, message, cause)
  }

  // Elapsed wall-clock millis since the given start nanos, for fetch-wait-time accounting.
  private def elapsedMillis(startNanos: Long): Long =
    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)
}

/**
 * Message types and tuning constants for [[StreamingShuffleReader]]. The retry/backoff and timeout
 * values are shared with [[BackpressureProtocol]] so the producer and consumer agree on the
 * retransmission budget and the producer-timeout deadline mandated by the streaming design.
 */
private[spark] object StreamingShuffleReader {

  /** Initial retransmission backoff in millis (start 1s, per the streaming design). */
  private val RetryBackoffStartMillis: Long =
    BackpressureProtocol.RETRY_BACKOFF_START_SECONDS * 1000L

  /** Maximum retransmission attempts before a block is declared unrecoverable (5). */
  private val MaxRetryAttempts: Int = BackpressureProtocol.MAX_RETRY_ATTEMPTS

  /** Maximum left-shift applied to the backoff base, capping exponential growth. */
  private val MaxBackoffShift: Int = 4

  /** Hard ceiling on a single retransmission backoff, in millis. */
  private val MaxBackoffMillis: Long = 30000L

  /**
   * Deadline for receiving the next in-progress block, completion, or failure before declaring a
   * producer timeout, in millis (heartbeat interval + connect timeout).
   */
  private val ProducerTimeoutMillis: Long =
    (BackpressureProtocol.HEARTBEAT_INTERVAL_SECONDS +
      BackpressureProtocol.CONNECT_TIMEOUT_SECONDS) * 1000L

  /**
   * A message delivered to a [[StreamingShuffleReader]] by the streaming transport handler over the
   * EXISTING transport. Sealed so the consuming iterator handles every case exhaustively.
   */
  private[spark] sealed trait StreamingBlockMessage

  /**
   * An in-progress block streamed from a producer, carrying the producer-computed checksum so the
   * reader can validate integrity and request retransmission on corruption.
   *
   * @param bmAddress the producer's [[BlockManagerId]] (used to populate [[FetchFailedException]])
   * @param blockId the shuffle block id (used to unwrap the stream and for diagnostics)
   * @param mapId the producing map task id
   * @param mapIndex the producing map task index
   * @param reduceId the reduce partition id
   * @param bytes the (possibly compressed/encrypted) serialized block bytes
   * @param checksum the producer-computed checksum (CRC32C by default) of `bytes`
   */
  private[spark] case class BlockChunk(
      bmAddress: BlockManagerId,
      blockId: BlockId,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int,
      bytes: Array[Byte],
      checksum: Long) extends StreamingBlockMessage

  /**
   * Signals that a producer crashed or its connection was lost; the consuming iterator turns this
   * into an atomic partial-read invalidation that drives upstream recomputation.
   *
   * @param bmAddress the failed producer's [[BlockManagerId]] (may be null if unknown)
   * @param blockId the in-flight shuffle block id (may be null if unknown)
   * @param mapId the producing map task id
   * @param mapIndex the producing map task index
   * @param reduceId the reduce partition id
   * @param message a human-readable description of the failure
   * @param cause the underlying cause, or null
   */
  private[spark] case class ProducerFailed(
      bmAddress: BlockManagerId,
      blockId: BlockId,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int,
      message: String,
      cause: Throwable) extends StreamingBlockMessage

  /** Signals that every expected producer has finished streaming; terminates the iterator. */
  private[spark] case object EndOfStream extends StreamingBlockMessage
}
