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
import java.util.concurrent.{ConcurrentLinkedQueue, LinkedBlockingQueue, Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.Checksum

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.streaming.StreamingBlockExchange.{BlockMeta, StreamingBlockConsumer}
import org.apache.spark.storage.BlockManagerId
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
 *  2. In-progress block requests via the [[StreamingBlockExchange]]: this reader subscribes to the
 *     exchange (the producer->consumer data path) and mirrors the proven
 *     `ShuffleBlockFetcherIterator` design, where a delivery callback enqueues results and the
 *     consuming iterator polls them. The exchange delivers each block, per-map completion-coverage
 *     signal, and producer failure through the [[StreamingBlockConsumer]] callbacks below. When the
 *     producer is co-located the hand-off is in-process; when it is on another executor
 *     the exchange routes over the EXISTING `org.apache.spark.network.TransportContext`
 *     (reuse only; no transport class is modified) -- identical callbacks either way.
 *     Consumer-to-producer heartbeats (via the shared [[BackpressureProtocol]]) apply
 *     flow control. The inbox is BOUNDED by message count and block bytes, so a fast
 *     producer blocks in the delivery callback (backpressure) rather than accumulating
 *     unbounded arrays.
 *  3. CRC32C integrity with block-specific retransmission: every received block is validated with
 *     the EXISTING [[ShuffleChecksumHelper]] (CRC32C). A corrupt block is re-requested by
 *     its addressable key via [[StreamingBlockExchange.requestResend]] with exponential backoff
 *     (start 1s, doubling) up to [[BackpressureProtocol.MAX_RETRY_ATTEMPTS]] attempts before the
 *     read is invalidated.
 *  4. Consumed-buffer acknowledgment: each validated block is acknowledged through
 *     [[StreamingBlockExchange.ack]] (which drives `MemorySpillManager` buffer reclamation on the
 *     writer side within the mandated 100ms) and [[BackpressureProtocol.recordConsumerProgress]].
 *  5. Atomic partial-read invalidation (SPARK-19276): on producer timeout BEFORE full per-map
 *     coverage, producer failure, or unrecoverable corruption, the reader throws the EXISTING
 *     [[FetchFailedException]] in a single construct-and-throw expression. Completion is derived
 *     from per-map coverage (every expected map completed AND every block it reported received), so
 *     a truncated stream can NEVER be reported as success. The UNMODIFIED scheduler converts the
 *     exception into upstream stage recomputation, preserving the existing lineage/fault-recovery
 *     model. This class never touches the scheduler. No partially-applied state escapes the
 *     iterator, guaranteeing zero data loss across producer crashes, consumer failures, and network
 *     partitions.
 *
 * Coexistence strategy: this reader is instantiated ONLY on the streaming path. The default
 * `SortShuffleManager` fallback never constructs it, so the sort-based shuffle carries zero
 * streaming overhead. Being `private[spark]` and confined to the streaming package enforces the
 * zero-cross-contamination rule: existing components neither import nor depend on this class.
 *
 * Threading: the inbox is the only cross-thread structure (a BOUNDED thread-safe
 * [[java.util.concurrent.LinkedBlockingQueue]] populated by the exchange's delivery threads and
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
 * @param exchange the producer->consumer data path this reader subscribes to
 *                 (in-process when the producer is co-located, over the existing
 *                 TransportContext when remote); it routes writer-emitted blocks to
 *                 [[onBlockReceived]], map-completion coverage to [[onMapComplete]], and
 *                 producer failures to [[onProducerFailed]], and carries acknowledgments
 *                 ([[StreamingBlockExchange.ack]]) and block-specific retransmission
 *                 requests ([[StreamingBlockExchange.requestResend]]) back
 * @param inboxCapacity the maximum number of undelivered messages buffered in the inbox before
 *                      producer callbacks block (count bound; see also `maxInboxBytes`)
 * @param maxInboxBytes the maximum number of undelivered block bytes buffered in the inbox before
 *                      producer callbacks block (byte bound; a single block larger than this is
 *                      still admitted alone so the reader never deadlocks)
 * @param fallbackOnSilence when true, a producer timeout or producer failure that occurs BEFORE any
 *                          record has been yielded raises a [[StreamingReadFallbackException]]
 *                          instead of a [[FetchFailedException]], letting the manager-owned
 *                          [[FallbackShuffleReader]] degrade this read to the composed
 *                          `SortShuffleManager` cleanly (no duplicate output). Once a record has
 *                          been yielded a clean swap is impossible, so invalidation reverts to the
 *                          standard [[FetchFailedException]] path regardless of this flag. Defaults
 *                          to false so direct (non-wrapped) construction keeps the original
 *                          invalidate-only semantics.
 * @param producerTimeoutMillis the millis deadline for the next in-progress block, map
 *                              completion, or producer failure before a producer timeout is
 *                              declared. A constructor seam (default
 *                              [[StreamingShuffleReader.DefaultProducerTimeoutMillis]]) letting
 *                              tests drive deterministic timeouts with no real waits
 * @param retryBackoffStartMillis the initial millis backoff before the first block-specific
 *                                retransmission, doubling thereafter (capped). A constructor seam
 *                                (default `DefaultRetryBackoffStartMillis`) letting tests exercise
 *                                the retry budget with instant (zero) backoff
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
    metricsSource: StreamingShuffleSource,
    exchange: StreamingBlockExchange,
    inboxCapacity: Int = StreamingShuffleReader.DefaultInboxCapacity,
    maxInboxBytes: Int = StreamingShuffleReader.DefaultMaxInboxBytes,
    fallbackOnSilence: Boolean = false,
    producerTimeoutMillis: Long = StreamingShuffleReader.DefaultProducerTimeoutMillis,
    retryBackoffStartMillis: Long = StreamingShuffleReader.DefaultRetryBackoffStartMillis)
  extends ShuffleReader[K, C] with StreamingBlockConsumer with Logging {

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

  // Consumer-side receive buffer for in-progress blocks streamed from producers. The
  // StreamingBlockExchange (the producer->consumer data path; in-process for co-located producers,
  // over the EXISTING TransportContext for remote ones) delivers blocks/coverage/failures through
  // the StreamingBlockConsumer callbacks below; the returned iterator drains them on the
  // task thread. This mirrors the listener-enqueues / iterator-polls structure of
  // ShuffleBlockFetcherIterator.
  //
  // BOUNDED for memory safety + backpressure (R4): the inbox is bounded by BOTH message count
  // (`inboxCapacity`) and buffered block bytes (`maxInboxBytes`). A producer callback that would
  // exceed either bound BLOCKS until the consumer drains -- and because publishBlock invokes the
  // callback on the producer's own thread, that blocking IS the end-to-end backpressure that
  // throttles a fast producer instead of letting unbounded byte arrays accumulate and risk OOM.
  private val inbox = new LinkedBlockingQueue[StreamingBlockMessage](math.max(1, inboxCapacity))

  // Byte budget guarding the inbox. A block acquires min(size, maxInboxBytes) permits before being
  // enqueued and releases them when drained, so buffered bytes stay bounded while a single block
  // larger than the whole budget is still admitted alone (it acquires every permit) rather than
  // deadlocking forever.
  private val inboxBytes = new Semaphore(math.max(1, maxInboxBytes))

  // Re-entrant resend channel. requestResend re-delivers a block by calling onBlockReceived on THIS
  // (the consumer) thread; routing such deliveries here -- not the bounded inbox -- means the
  // consumer never blocks on its own inbox while servicing a retransmission (deadlock-free). It is
  // bounded in practice by the per-block retry budget, since only the iterator thread requests it.
  private val resendInbox = new ConcurrentLinkedQueue[BlockChunk]()

  // The task thread that drains the iterator, captured on first poll. Used only to distinguish a
  // re-entrant resend delivery (this thread) from a producer-thread delivery (any other thread).
  private val consumerThread = new AtomicReference[Thread]()

  // Number of producer map tasks this reduce task expects an `onMapComplete` from. Completion (no
  // truncated read) requires a completion signal from EVERY one of these maps, plus every block
  // each of them reports having emitted for this reader's partition range (R1).
  private val expectedMapCount: Int = math.max(0, endMapIndex - startMapIndex)

  // Runtime fallback gate (F1). Set true the instant the returned iterator hands a record to the
  // caller. A clean swap to the sort reader is only possible while this is false (nothing has been
  // observed downstream yet); once a record escapes, invalidateOrFallback can no longer fall back
  // without risking duplicate output, so it reverts to the standard FetchFailedException path.
  // Touched solely by the single task thread that drains the iterator, so it needs no
  // synchronization.
  private var anyRecordYielded: Boolean = false

  if (debug) {
    logDebug(s"StreamingShuffleReader created for shuffle $shuffleId partitions " +
      s"[$startPartition, $endPartition) maps [$startMapIndex, $endMapIndex); inbox bound " +
      s"$inboxCapacity msgs / $maxInboxBytes bytes")
  }

  // Subscribe to the exchange as the LAST construction step (every field above is initialized, so
  // publishing `this` is safe). The exchange immediately replays any blocks/coverage that arrived
  // before this reader subscribed, and routes all future ones here.
  exchange.registerReader(
    shuffleId, startMapIndex, endMapIndex, startPartition, endPartition, this)

  // Unsubscribe on task completion (success/failure/cancel) so the exchange never retains
  // routing state for a finished reduce task. Reuses the EXISTING TaskContext lifecycle; no
  // scheduler or task code is modified.
  context.addTaskCompletionListener[Unit](_ => exchange.unregisterReader(shuffleId, this))

  // ============================================================================================
  // Data-path integration point (REUSE ONLY).
  //
  // These StreamingBlockConsumer callbacks are the ONLY entry points the exchange uses
  // to hand data to this reader. publishBlock/producerFailed invoke them on a PRODUCER thread (so a
  // full inbox blocks the producer = backpressure); requestResend invokes onBlockReceived on the
  // CONSUMER thread (routed to the re-entrant resend channel to stay deadlock-free). Keeping them
  // confined to the streaming package enforces the zero-cross-contamination rule.
  // ============================================================================================

  /**
   * Delivers an in-progress block (and its metadata, including the producer-computed checksum) to
   * this reader. On a producer thread this blocks when the bounded inbox is full (backpressure); on
   * the consumer thread (a resend re-delivery) it routes to the non-blocking resend channel.
   */
  override def onBlockReceived(meta: BlockMeta, bytes: Array[Byte]): Unit = {
    if (Thread.currentThread() eq consumerThread.get()) {
      resendInbox.add(BlockChunk(meta, bytes))
    } else {
      // Producer thread: acquire the byte budget (blocking) then enqueue (blocking when full),
      // propagating backpressure to the writer's publishBlock on its own map thread. If interrupted
      // after acquiring but before enqueue, release the permits so the budget never leaks.
      val permits = permitsFor(bytes)
      var acquired = false
      try {
        if (permits > 0) {
          inboxBytes.acquire(permits)
          acquired = true
        }
        inbox.put(BlockChunk(meta, bytes))
      } catch {
        case e: InterruptedException =>
          if (acquired) {
            inboxBytes.release(permits)
          }
          Thread.currentThread().interrupt()
          throw e
      }
    }
  }

  /**
   * Reports that a producing map finished and how many blocks it emitted per reduce partition, so
   * the consuming iterator can verify FULL coverage of its partition range before completing (R1).
   */
  override def onMapComplete(mapId: Long, mapIndex: Int, blockCounts: Array[Long]): Unit = {
    // Control message: it carries no block bytes, so it bypasses the byte budget and takes only a
    // count slot. The continuously-draining iterator guarantees it is admitted promptly.
    inbox.put(MapCompleted(mapId, mapIndex, blockCounts))
  }

  /**
   * Reports that a producing map failed; the consuming iterator turns this into an atomic
   * partial-read invalidation that drives upstream recomputation through `FetchFailedException`.
   */
  override def onProducerFailed(
      bmAddress: BlockManagerId,
      mapId: Long,
      mapIndex: Int,
      message: String,
      cause: Throwable): Unit = {
    inbox.put(ProducerFailed(bmAddress, mapId, mapIndex, message, cause))
  }

  // Byte permits a block of `bytes` consumes in the inbox budget: its size, capped by the whole
  // budget so an oversize block can still be admitted alone (acquiring every permit) rather than
  // requesting more than the semaphore can ever grant.
  private def permitsFor(bytes: Array[Byte]): Int = {
    val size = if (bytes == null) 0 else bytes.length
    math.min(math.max(size, 0), math.max(1, maxInboxBytes))
  }

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

    // Set once FULL coverage is verified -- every map completed AND every block it reported
    // for this reader's range has been received and validated; only then are there no more
    // blocks. There is NO unconditional end-of-stream marker, so a truncated stream cannot finish.
    private var finished: Boolean = false

    // Producer context of the most recently validated block, used to populate FetchFailedException
    // accurately on a later producer timeout (the timed-out block's own ids are unknown then).
    private var lastProducerAddress: BlockManagerId = _
    private var lastMapId: Long = -1L
    private var lastMapIndex: Int = -1

    // Coverage tracking (R1), touched only by this single task thread (no synchronization needed).
    // completedMaps: mapIndex -> number of blocks that map reported emitting for THIS reader's
    // [startPartition, endPartition) range (via onMapComplete). receivedPerMap: mapIndex -> number
    // of those blocks actually received and validated so far. The read is complete only when every
    // expected map has completed and received >= expected for each.
    private val completedMaps = new mutable.HashMap[Int, Long]()
    private val receivedPerMap = new mutable.HashMap[Int, Long]()

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
      val record = currentRecords.next()
      // Runtime fallback gate (F1): a record is now leaving this iterator. From here on a clean
      // swap to the sort reader is no longer possible (this record may already have been observed
      // or fed into a downstream sorter/aggregator), so any later invalidation must use the
      // standard FetchFailedException recompute path rather than StreamingReadFallbackException.
      anyRecordYielded = true
      record
    }

    // Pull messages until a validated block yields records or FULL coverage is verified. A poll
    // timeout while coverage is still incomplete is a truncated stream and is invalidated (R1).
    private def fillBuffer(): Unit = {
      while (!finished && !currentRecords.hasNext) {
        pollNext() match {
          case null =>
            // Inbox timed out. If full coverage was already verified we are done; otherwise a
            // producer stalled and this is a truncated read, which we NEVER report as success.
            // Route through invalidateOrFallback (F1): if nothing has been yielded yet and this
            // reader opted into silence fallback, degrade cleanly to sort; otherwise invalidate
            // atomically so the existing FetchFailedException path recomputes upstream.
            if (isComplete) {
              finished = true
            } else {
              invalidateOrFallback(lastProducerAddress, lastMapId, lastMapIndex, startPartition,
                s"Streaming shuffle producer timed out before full coverage (shuffle $shuffleId, " +
                  s"reduce $startPartition): ${completedMaps.size}/$expectedMapCount " +
                  "expected maps", null)
            }
          case mc: MapCompleted =>
            noteMapCompleted(mc.mapIndex, mc.blockCounts)
            if (isComplete) {
              finished = true
            }
          case pf: ProducerFailed =>
            // Producer crash / network partition. Route through invalidateOrFallback (F1): degrade
            // cleanly to sort when nothing has been yielded and fallback was requested, otherwise
            // invalidate atomically (throws; never returns).
            invalidateOrFallback(
              pf.bmAddress, pf.mapId, pf.mapIndex, startPartition, pf.message, pf.cause)
          case chunk: BlockChunk =>
            currentRecords = receiveChunk(chunk)
            // Finish as soon as coverage completes, even with records pending: `finished` only
            // gates this loop, and hasNext drains `currentRecords` first, so no end-of-read poll
            // stalls for the producer-timeout deadline.
            if (isComplete) {
              finished = true
            }
        }
      }
    }

    // True once every expected map has reported completion AND every block each reported for this
    // reader's partition range has been received and validated (R1: no truncated read can satisfy).
    private def isComplete: Boolean = {
      completedMaps.size >= expectedMapCount &&
        completedMaps.forall { case (mapIndex, expected) =>
          receivedPerMap.getOrElse(mapIndex, 0L) >= expected
        }
    }

    // Record a map's completion: sum the blocks it reported for THIS reader's [startPartition,
    // endPartition) range (index = reduce partition id), guarding the array bounds defensively.
    private def noteMapCompleted(mapIndex: Int, blockCounts: Array[Long]): Unit = {
      var expected = 0L
      if (blockCounts != null) {
        var p = startPartition
        val end = math.min(endPartition, blockCounts.length)
        while (p < end) {
          expected += blockCounts(p)
          p += 1
        }
      }
      completedMaps(mapIndex) = expected
    }

    // Count one received-and-validated block toward this map's coverage.
    private def noteBlockReceived(mapIndex: Int): Unit = {
      receivedPerMap(mapIndex) = receivedPerMap.getOrElse(mapIndex, 0L) + 1L
    }

    // Block for the next message, draining re-entrant resends first (no byte budget), then
    // the bounded inbox. Returns null on a producer-timeout deadline so the caller can decide,
    // based on coverage, whether that is normal completion or a truncated read (R1). Releases the
    // byte budget a drained block held so a producer blocked in onBlockReceived can proceed (R4).
    private def pollNext(): StreamingBlockMessage = {
      consumerThread.compareAndSet(null, Thread.currentThread())
      val resent = resendInbox.poll()
      if (resent != null) {
        resent
      } else {
        // A consumer heartbeat signals liveness and feeds consumer throughput to the flow-control
        // protocol; this is how the reader paces and "pulls" streamed blocks.
        backpressure.consumerHeartbeat()
        val startNanos = System.nanoTime()
        val msg = inbox.poll(producerTimeoutMillis, TimeUnit.MILLISECONDS)
        readMetrics.incFetchWaitTime(elapsedMillis(startNanos))
        msg match {
          case bc: BlockChunk =>
            inboxBytes.release(permitsFor(bc.bytes))
            bc
          case other =>
            other
        }
      }
    }

    // Validate a received block's CRC32C; on corruption, issue a block-specific NACK to resend THIS
    // exact block over the exchange with backoff up to the retry budget, then invalidate
    // if it can no longer be retransmitted. On success, count it toward coverage, record producer
    // context, update read metrics, acknowledge it (driving writer-side reclaim), and deserialize.
    private def receiveChunk(chunk: BlockChunk): Iterator[(Any, Any)] = {
      var current = chunk
      var attempts = 1
      // Coexistence: integrity is verified with the EXISTING ShuffleChecksumHelper (CRC32C), the
      // same facility sort-based shuffle uses; no new checksum implementation is introduced.
      while (!validateChecksum(current.bytes, current.meta.checksum)) {
        if (attempts >= MaxRetryAttempts) {
          // Unrecoverable corruption after the full retry budget: invalidate atomically.
          invalidate(current.meta.bmAddress, current.meta.mapId, current.meta.mapIndex,
            current.meta.reduceId,
            s"Streaming shuffle block ${current.meta.blockId} failed CRC32C validation after " +
              s"$attempts attempts", null)
        }
        // Block-specific NACK (R3): ask the exchange to resend THIS block by its addressable key,
        // after exponential backoff. If it is no longer retained, the read is unrecoverable.
        backoffBeforeResend(current.meta, attempts)
        if (!exchange.requestResend(current.meta)) {
          invalidate(current.meta.bmAddress, current.meta.mapId, current.meta.mapIndex,
            current.meta.reduceId,
            s"Streaming shuffle block ${current.meta.blockId} is corrupt and can no longer be " +
              s"retransmitted (attempt $attempts)", null)
        }
        current = awaitRetransmission(current)
        attempts += 1
      }
      lastProducerAddress = current.meta.bmAddress
      lastMapId = current.meta.mapId
      lastMapIndex = current.meta.mapIndex
      noteBlockReceived(current.meta.mapIndex)
      recordBlockMetrics(current)
      // Acknowledge the consumed block over the exchange (R2): this drives MemorySpillManager
      // .reclaim of the writer-side buffer within the mandated 100ms. The reader holds its OWN copy
      // of the bytes, so reclaiming the producer-side buffer never affects deserialization below.
      exchange.ack(current.meta)
      backpressure.recordConsumerProgress(current.bytes.length.toLong)
      deserializeChunk(current)
    }

    // After a corrupt block, wait for the resend (delivered via the re-entrant resend channel). A
    // producer failure, a competing completion, or a producer timeout while awaiting the resend is
    // itself an unrecoverable partial read and is invalidated atomically.
    private def awaitRetransmission(failed: BlockChunk): BlockChunk = {
      pollNext() match {
        case chunk: BlockChunk =>
          chunk
        case pf: ProducerFailed =>
          invalidate(pf.bmAddress, pf.mapId, pf.mapIndex, startPartition, pf.message, pf.cause)
        case mc: MapCompleted =>
          invalidate(failed.meta.bmAddress, failed.meta.mapId, failed.meta.mapIndex,
            failed.meta.reduceId,
            s"Streaming shuffle map ${mc.mapId} completed before corrupt block " +
              s"${failed.meta.blockId} could be retransmitted", null)
        case _ =>
          invalidate(failed.meta.bmAddress, failed.meta.mapId, failed.meta.mapIndex,
            failed.meta.reduceId,
            s"Streaming shuffle producer timed out before corrupt block ${failed.meta.blockId} " +
              "could be retransmitted", null)
      }
    }
  }

  // Exponential backoff before a block-specific resend request (start 1s, doubling, capped). A
  // consumer heartbeat keeps flow-control state fresh across the wait. The actual NACK is issued by
  // the caller via `exchange.requestResend`, which re-delivers the retained block by its key.
  private def backoffBeforeResend(meta: BlockMeta, attempt: Int): Unit = {
    val backoff = backoffMillis(attempt)
    if (debug) {
      logDebug(s"Streaming shuffle block ${meta.blockId} failed CRC32C validation; requesting " +
        s"resend (attempt $attempt of $MaxRetryAttempts) after ${backoff}ms backoff")
    }
    sleepInterruptibly(backoff)
    backpressure.consumerHeartbeat()
  }

  // Exponential backoff in millis: attempt 1 -> base (1s), attempt 2 -> 2s, attempt 3 -> 4s, ...
  // bounded by a hard cap so a pathological retransmission can never wait unbounded.
  private def backoffMillis(attempt: Int): Long = {
    val shift = math.min(math.max(attempt - 1, 0), MaxBackoffShift)
    math.min(retryBackoffStartMillis * (1L << shift), MaxBackoffMillis)
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
    if (isLocal(chunk.meta.bmAddress)) {
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
          env.serializerManager.wrapStream(chunk.meta.blockId, rawStream)
        } catch {
          case NonFatal(e) =>
            if (debug) {
              logDebug(s"wrapStream unavailable for ${chunk.meta.blockId}; using raw stream", e)
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

  // Runtime fallback dispatch (F1) for producer silence/failure. When the reader was built with
  // fallbackOnSilence=true AND no record has yet escaped the iterator, a clean degrade to the
  // composed SortShuffleManager is still possible, so raise StreamingReadFallbackException for the
  // manager-owned FallbackShuffleReader to handle -- this does NOT touch the
  // partialReadInvalidations counter or the scheduler. Otherwise (a record was already yielded, or
  // fallback was not requested) defer to invalidate(), which throws FetchFailedException to drive
  // the existing upstream recompute path. Returns Nothing so callers may use it in expression
  // position.
  private def invalidateOrFallback(
      bmAddress: BlockManagerId,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int,
      message: String,
      cause: Throwable): Nothing = {
    if (fallbackOnSilence && !anyRecordYielded) {
      if (debug) {
        logDebug(s"Streaming shuffle silence before first record for shuffle $shuffleId reduce " +
          s"$reduceId; signalling clean fallback to sort: $message")
      }
      throw new StreamingReadFallbackException(message)
    }
    invalidate(bmAddress, mapId, mapIndex, reduceId, message, cause)
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

  /**
   * Default initial retransmission backoff in millis (start 1s, per the streaming design). Used as
   * the default for the `retryBackoffStartMillis` constructor seam; tests pass 0 for instant retry.
   */
  private[streaming] val DefaultRetryBackoffStartMillis: Long =
    BackpressureProtocol.RETRY_BACKOFF_START_SECONDS * 1000L

  /** Maximum retransmission attempts before a block is declared unrecoverable (5). */
  private val MaxRetryAttempts: Int = BackpressureProtocol.MAX_RETRY_ATTEMPTS

  /** Maximum left-shift applied to the backoff base, capping exponential growth. */
  private val MaxBackoffShift: Int = 4

  /** Hard ceiling on a single retransmission backoff, in millis. */
  private val MaxBackoffMillis: Long = 30000L

  /**
   * Default deadline for receiving the next in-progress block, completion, or failure before
   * declaring a producer timeout, in millis (heartbeat interval + connect timeout). Used as the
   * default for the `producerTimeoutMillis` constructor seam; tests pass a tiny value to force a
   * deterministic timeout.
   */
  private[streaming] val DefaultProducerTimeoutMillis: Long =
    (BackpressureProtocol.HEARTBEAT_INTERVAL_SECONDS +
      BackpressureProtocol.CONNECT_TIMEOUT_SECONDS) * 1000L

  /** Default maximum number of undelivered messages buffered in a reader's inbox (count bound). */
  private[streaming] val DefaultInboxCapacity: Int = 128

  /**
   * Default maximum number of undelivered block bytes buffered in a reader's inbox (byte bound),
   * 64MB -- headroom for many in-flight <=2MB pipelined blocks while still bounding memory (R4).
   */
  private[streaming] val DefaultMaxInboxBytes: Int = 64 * 1024 * 1024

  /**
   * A message delivered to a [[StreamingShuffleReader]] by the [[StreamingBlockExchange]] through
   * its [[StreamingBlockExchange.StreamingBlockConsumer]] callbacks. Sealed so the consuming
   * iterator handles every case exhaustively. There is deliberately NO end-of-stream message:
   * completion is derived from per-map coverage so a truncated stream can never finish (R1).
   */
  private[spark] sealed trait StreamingBlockMessage

  /**
   * An in-progress block streamed from a producer, paired with its end-to-end [[BlockMeta]] (which
   * carries the addressable block key, producer address, block id, and producer-computed checksum)
   * so the reader can validate integrity and request a block-specific resend on corruption.
   *
   * @param meta the block metadata carried end to end (key/address/blockId/checksum/size)
   * @param bytes the (possibly compressed/encrypted) serialized block bytes
   */
  private[spark] case class BlockChunk(
      meta: BlockMeta,
      bytes: Array[Byte]) extends StreamingBlockMessage

  /**
   * Signals that a producing map task finished, reporting how many blocks it emitted for each
   * reduce partition (index = reduce partition id), so the reader can verify full coverage (R1).
   *
   * @param mapId the completed map task id
   * @param mapIndex the completed map task index
   * @param blockCounts per-reduce-partition block counts emitted by this map
   */
  private[spark] case class MapCompleted(
      mapId: Long,
      mapIndex: Int,
      blockCounts: Array[Long]) extends StreamingBlockMessage

  /**
   * Signals that a producer crashed or its connection was lost; the consuming iterator turns this
   * into an atomic partial-read invalidation that drives upstream recomputation.
   *
   * @param bmAddress the failed producer's [[BlockManagerId]] (may be null if unknown)
   * @param mapId the producing map task id
   * @param mapIndex the producing map task index
   * @param message a human-readable description of the failure
   * @param cause the underlying cause, or null
   */
  private[spark] case class ProducerFailed(
      bmAddress: BlockManagerId,
      mapId: Long,
      mapIndex: Int,
      message: String,
      cause: Throwable) extends StreamingBlockMessage
}
