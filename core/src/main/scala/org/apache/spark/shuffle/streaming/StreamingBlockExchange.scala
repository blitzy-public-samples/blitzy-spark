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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.storage.{BlockId, BlockManagerId}

/**
 * Producer-to-consumer data path for the opt-in streaming shuffle engine: the component that
 * actually carries finalized block bytes from a [[StreamingShuffleWriter]] to the
 * [[StreamingShuffleReader]] callbacks, and carries consumer acknowledgments / retransmission
 * requests back. The bytes it routes are owned and bounded by the shared [[MemorySpillManager]], so
 * this class never holds block bytes itself and adds no memory of its own beyond small per-shuffle
 * bookkeeping.
 *
 * Local and remote routing (coexistence strategy): the streaming engine deliberately confines
 * itself to the `ShuffleManager` abstraction boundary and reuses, rather than modifies,
 * the existing transport, scheduler, and memory subsystems. The producer (map) side emits
 * blocks through [[publishBlock]]; the consumer (reduce) side subscribes through
 * [[registerReader]] and receives
 * blocks on the [[StreamingBlockConsumer]] callbacks it already exposes. When the producer and
 * consumer are co-located the hand-off is in-process. When they are on different executors, the
 * exchange routes over the EXISTING `org.apache.spark.network.TransportContext` via a bound
 * [[StreamingShuffleTransport]] (see [[bindTransport]]): a remote consumer is registered as an
 * ordinary [[StreamingBlockConsumer]] proxy ([[RemoteReaderProxy]]) so the SAME routing logic
 * delivers to it, except "deliver" serializes the callback and ships it over the network. The
 * [[StreamingBlockConsumer]] contract is identical whether a consumer is local or remote, so the
 * writer and reader need no change. If no transport is bound (e.g. a unit test), the exchange is
 * purely in-process exactly as before. This keeps the streaming wiring fully inside the streaming
 * package (`private[spark]`), enforcing the zero-cross-contamination rule.
 *
 * Coverage and zero-loss semantics: the exchange forwards each producing map's per-reduce block
 * counts (via [[completeMap]]) so the consumer can verify it has received EVERY expected block from
 * EVERY expected map before completing -- there is no unconditional end-of-stream marker that could
 * let a truncated read masquerade as success. Producer failures are forwarded verbatim so the
 * consumer can invalidate and drive upstream recomputation through the existing
 * `FetchFailedException` path.
 *
 * Reclamation and retransmission: a consumer acknowledges a consumed block with [[ack]], which
 * reclaims its buffer through [[MemorySpillManager.reclaim]] within the mandated 100ms; a consumer
 * that detects corruption requests a block-specific resend with [[requestResend]], which re-reads
 * the exact block (from memory or its spill file) and re-delivers it. Both operate on a precise
 * [[BlockMeta]] so the right block -- and only that block -- is reclaimed or resent.
 *
 * Backpressure-safe delivery: every consumer callback is invoked OUTSIDE the per-shuffle lock, so a
 * consumer whose bounded inbox is full may block the delivering producer thread (the desired
 * backpressure) without ever stalling other shuffles or blocking acknowledgment/registration. Block
 * routing, pending buffering, and the persistent completion/failure logs are the only state guarded
 * by the lock, and that critical section performs no blocking work.
 *
 * Thread-safety: per-shuffle state is in a [[ConcurrentHashMap]] and each shuffle's mutations are
 * serialized on that shuffle's own monitor, so concurrent producers, consumers, and registrations
 * for different shuffles never contend. Verbose logging is gated behind
 * `spark.shuffle.streaming.debug` to keep per-executor log volume under the mandated 10MB/hour.
 *
 * @param conf the active [[SparkConf]] supplying `spark.shuffle.streaming.debug`
 * @param spillManager the shared spill coordinator that owns block bytes and provides read-back,
 *                     reclamation, and spill-aware retransmission
 */
private[spark] class StreamingBlockExchange(
    conf: SparkConf,
    spillManager: MemorySpillManager)
  extends Logging with StreamingShuffleTransport.StreamingTransportListener {

  import StreamingBlockExchange._

  // Verbose debug-logging gate; held off the hot path to honor the 10MB/hour log-volume budget.
  private val debug: Boolean = conf.get(config.STREAMING_SHUFFLE_DEBUG)

  // Per-shuffle routing state, keyed by shuffle id. Concurrent across shuffles; each value's
  // mutations are serialized on the value's own monitor (see ShuffleState).
  private val shuffles = new ConcurrentHashMap[Int, ShuffleState]()

  // Set once stop() runs; short-circuits further publish/complete/resend after teardown begins.
  @volatile private var stopped = false

  // The cross-executor network plane, bound by the engine via bindTransport AFTER construction (the
  // transport needs this exchange as its receive listener, so the two are wired in two steps).
  // Stays null for in-process-only use (every existing unit test), where all routing is local
  // and remote helpers below are never reached. Read on the hot path, so @volatile for safe
  // publication.
  @volatile private var transport: StreamingShuffleTransport = _

  // Source endpoint (producer streaming host, port) for blocks that arrived over the network, keyed
  // by block key. Consulted by ack/requestResend so a consumer sends its acknowledgment / resend
  // request back to the EXACT producer executor that owns the bytes (that executor's
  // MemorySpillManager holds them), rather than reclaiming/resending locally. Entries are
  // removed on ack.
  private val remoteSources =
    new ConcurrentHashMap[MemorySpillManager.BlockKey, (String, Int)]()

  // Per-shuffle bookkeeping. `readers` holds the currently-subscribed reducers. `pendingBlocks`
  // holds blocks published before their single owning reducer subscribed (each is delivered exactly
  // once, then removed). `mapCompletions` / `producerFailures` are PERSISTENT logs: they are
  // delivered live to subscribed readers AND replayed to any reader that subscribes later, so a
  // late-registering reducer still observes full coverage and any failures. Guarded by `this`.
  private final class ShuffleState {
    val readers = new ArrayBuffer[ReaderRegistration]()
    val pendingBlocks = new ArrayBuffer[BlockMeta]()
    val mapCompletions = new ArrayBuffer[MapCompletion]()
    val producerFailures = new ArrayBuffer[ProducerFailure]()
  }

  private def stateFor(shuffleId: Int): ShuffleState =
    shuffles.computeIfAbsent(shuffleId, _ => new ShuffleState)

  /**
   * Subscribes a reduce-side consumer to a shuffle for the given map-index and reduce-partition
   * ranges. Any blocks already published for this reducer (buffered because it had not yet
   * subscribed) are delivered immediately, and every recorded map-completion / producer-failure for
   * its map range is replayed, so a consumer that starts after its producers still observes full
   * coverage. All delivery happens OUTSIDE the lock.
   *
   * @param shuffleId the shuffle to subscribe to
   * @param startMapIndex inclusive start of the consumed map-index range
   * @param endMapIndex exclusive end of the consumed map-index range
   * @param startPartition inclusive start of the consumed reduce-partition range
   * @param endPartition exclusive end of the consumed reduce-partition range
   * @param consumer the reduce-side callback target
   */
  def registerReader(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      consumer: StreamingBlockConsumer): Unit = {
    val reg = new ReaderRegistration(
      startMapIndex, endMapIndex, startPartition, endPartition, consumer)
    val state = stateFor(shuffleId)
    val blocksToReplay = new ArrayBuffer[BlockMeta]()
    val completionsToReplay = new ArrayBuffer[MapCompletion]()
    val failuresToReplay = new ArrayBuffer[ProducerFailure]()
    state.synchronized {
      state.readers += reg
      // Claim matching pending blocks: each belongs to exactly one reducer (by reduce partition),
      // so remove it from the pending list as we hand it to this reader.
      var i = 0
      while (i < state.pendingBlocks.length) {
        val meta = state.pendingBlocks(i)
        if (reg.matchesBlock(meta)) {
          blocksToReplay += meta
          state.pendingBlocks.remove(i)
        } else {
          i += 1
        }
      }
      // Completions/failures persist; replay the ones in this reader's map range to it alone.
      state.mapCompletions.foreach(c => if (reg.matchesMap(c.mapIndex)) completionsToReplay += c)
      state.producerFailures.foreach(f => if (reg.matchesMap(f.mapIndex)) failuresToReplay += f)
    }
    // Deliver outside the lock. Completions first so the consumer knows expected counts, then any
    // already-published blocks, then any failures it must act on.
    completionsToReplay.foreach(c => consumer.onMapComplete(c.mapId, c.mapIndex, c.blockCounts))
    blocksToReplay.foreach { meta =>
      spillManager.read(meta.key).foreach(bytes => deliverBlock(consumer, meta, bytes))
    }
    failuresToReplay.foreach { f =>
      consumer.onProducerFailed(f.bmAddress, f.mapId, f.mapIndex, f.message, f.cause)
    }
    if (debug) {
      logDebug(s"Streaming shuffle reader subscribed to shuffle $shuffleId maps " +
        s"[$startMapIndex, $endMapIndex) partitions [$startPartition, $endPartition); replayed " +
        s"${blocksToReplay.length} block(s), ${completionsToReplay.length} completion(s)")
    }
  }

  /**
   * Unsubscribes a consumer (for example, when its reduce task completes or is cancelled). Safe to
   * call for a consumer that was never registered.
   *
   * @param shuffleId the shuffle to unsubscribe from
   * @param consumer the consumer to remove
   */
  def unregisterReader(shuffleId: Int, consumer: StreamingBlockConsumer): Unit = {
    val state = shuffles.get(shuffleId)
    if (state != null) {
      state.synchronized {
        var i = 0
        while (i < state.readers.length) {
          if (state.readers(i).consumer eq consumer) {
            state.readers.remove(i)
          } else {
            i += 1
          }
        }
      }
    }
  }

  /**
   * Publishes one finalized block from a producer. The bytes are handed to the shared
   * [[MemorySpillManager]] (which accounts and may spill them while keeping them readable), then
   * routed to the single reducer that owns the block's reduce partition. If that reducer has not
   * subscribed yet, the block is buffered (its bytes stay safe in the spill manager) and delivered
    * when the reducer registers. Delivery is OUTSIDE the lock so a full consumer inbox throttles
   * this producer thread without blocking other shuffles.
   *
   * @param meta the block's identity (key, producer address, block id, map index, checksum, size)
   * @param bytes the finalized, integrity-checked block bytes (ownership transfers to the manager)
   * @return true if the block was stored and routed; false if it was NOT stored -- a hard memory
   *         -admission failure (the streaming writer must fall back to sort) or the exchange has
   *         stopped -- in which case nothing was buffered, delivered, or advertised
   */
  def publishBlock(meta: BlockMeta, bytes: Array[Byte]): Boolean = {
    if (stopped) {
      false
    } else {
      val stored = spillManager.register(meta.key, bytes)
      if (!stored) {
        // Hard memory-admission failure (group B): the spill manager could not admit the bytes even
        // after a spill pass, so the block was NOT stored. Do not buffer it for replay or route it
        // to a reader -- its bytes are not retained and could not be re-read. Returning false makes
        // the streaming writer fall back to sort BEFORE any output is advertised; the fallback
        // replays the consumed record prefix through sort, so there is zero data loss.
        false
      } else {
        val state = stateFor(meta.shuffleId)
        val target = state.synchronized {
          val reader = state.readers.find(_.matchesBlock(meta))
          if (reader.isEmpty) {
            state.pendingBlocks += meta
          }
          reader
        }
        target.foreach(reader => deliverBlock(reader.consumer, meta, bytes))
        true
      }
    }
  }

  /**
   * Records that a producing map task has finished and how many blocks it emitted for each reduce
   * partition. The completion is logged persistently (so readers that subscribe later still learn
   * the expected counts) and delivered to every currently-subscribed reducer whose map range
   * includes this map. Consumers use these counts to verify full block coverage before completing.
   *
   * @param shuffleId the shuffle the map belongs to
   * @param mapId the completed map task id
   * @param mapIndex the completed map task index
   * @param blockCounts per-reduce-partition block counts (index = reduce partition id)
   */
  def completeMap(shuffleId: Int, mapId: Long, mapIndex: Int, blockCounts: Array[Long]): Unit = {
    if (!stopped) {
      val completion = new MapCompletion(mapId, mapIndex, blockCounts)
      val state = stateFor(shuffleId)
      val targets = state.synchronized {
        state.mapCompletions += completion
        state.readers.filter(_.matchesMap(mapIndex)).toList
      }
      targets.foreach(_.consumer.onMapComplete(mapId, mapIndex, blockCounts))
      if (debug) {
        logDebug(s"Streaming shuffle map $mapIndex (id $mapId) completed for shuffle $shuffleId; " +
          s"notified ${targets.length} reader(s)")
      }
    }
  }

  /**
   * Reports that a producing map task failed (crash or lost connection). The failure is logged
   * persistently and delivered to every subscribed reducer whose map range includes this map, each
   * of which performs an atomic partial-read invalidation that drives upstream recomputation
   * through the existing `FetchFailedException` path.
   *
   * @param shuffleId the shuffle the map belongs to
   * @param mapId the failed map task id
   * @param mapIndex the failed map task index
   * @param bmAddress the failed producer's block-manager address (may be null if unknown)
   * @param message a human-readable description of the failure
   * @param cause the underlying cause, or null
   */
  def producerFailed(
      shuffleId: Int,
      mapId: Long,
      mapIndex: Int,
      bmAddress: BlockManagerId,
      message: String,
      cause: Throwable): Unit = {
    if (!stopped) {
      val failure = new ProducerFailure(bmAddress, mapId, mapIndex, message, cause)
      val state = stateFor(shuffleId)
      val targets = state.synchronized {
        state.producerFailures += failure
        state.readers.filter(_.matchesMap(mapIndex)).toList
      }
      targets.foreach(_.consumer.onProducerFailed(bmAddress, mapId, mapIndex, message, cause))
    }
  }

  /**
   * Acknowledges that a consumer has fully consumed a block, reclaiming its buffer through the
   * shared [[MemorySpillManager]] within the mandated 100ms (releasing heap memory and deleting any
   * spill file). This is the ack-to-reclamation path that bounds writer-side buffer lifetime.
   *
   * @param meta the consumed block's metadata
   */
  def ack(meta: BlockMeta): Unit = {
    val source = remoteSources.remove(meta.key)
    if (source != null && transport != null) {
      // The block was streamed from another executor, whose MemorySpillManager owns the bytes. Send
      // the ack there so it -- not this consumer -- reclaims the buffer within the mandated 100ms.
      transport.send(source._1, source._2, StreamingShuffleTransport.encodeAck(meta))
      if (debug) {
        logDebug(s"Streaming shuffle consumer acknowledged remote block ${meta.key} to " +
          s"${source._1}:${source._2}")
      }
    } else {
      // In-process (co-located producer): reclaim directly through the shared spill manager.
      spillManager.reclaim(meta.key)
      if (debug) {
        logDebug(s"Streaming shuffle consumer acknowledged block ${meta.key}; buffer reclaimed")
      }
    }
  }

  /**
   * Re-delivers a specific block on a consumer's request (e.g. after a CRC32C mismatch). The
   * exact block is re-read from the shared [[MemorySpillManager]] -- from memory or, if it was
   * spilled, from its retained spill file -- and re-delivered to its owning reducer. Because the
   * block is addressed by its precise [[BlockMeta.key]], only that block is resent.
   *
   * @param meta the block to resend
   * @return true if the block was found and re-delivered; false if it is no longer retained (e.g.
   *         already acknowledged) or the exchange has stopped
   */
  def requestResend(meta: BlockMeta): Boolean = {
    if (stopped) {
      false
    } else if (remoteSources.containsKey(meta.key) && transport != null) {
      // The block came from another executor; forward the resend request to that producer, which
      // re-reads the exact block from its spill manager and re-delivers it over the transport. The
      // remoteSources entry is left in place (only ack removes it) so the resend can be
      // acked later.
      val source = remoteSources.get(meta.key)
      transport.send(source._1, source._2, StreamingShuffleTransport.encodeResend(meta))
      if (debug) {
        logDebug(s"Streaming shuffle requested remote resend of block ${meta.key} from " +
          s"${source._1}:${source._2}")
      }
      true
    } else {
      spillManager.read(meta.key) match {
        case Some(bytes) =>
          val state = stateFor(meta.shuffleId)
          val target = state.synchronized {
            state.readers.find(_.matchesBlock(meta))
          }
          target.foreach(reader => deliverBlock(reader.consumer, meta, bytes))
          if (debug) {
            logDebug(s"Streaming shuffle resent block ${meta.key} on consumer request")
          }
          target.isDefined
        case None =>
          if (debug) {
            logDebug(s"Streaming shuffle cannot resend block ${meta.key}; no longer retained")
          }
          false
      }
    }
  }

  /**
   * Tears down the exchange, dropping all per-shuffle routing state. Does NOT stop the shared
   * [[MemorySpillManager]] or the bound [[StreamingShuffleTransport]], whose lifetimes are owned by
   * the engine that created them. Idempotent.
   */
  def stop(): Unit = {
    stopped = true
    shuffles.clear()
    remoteSources.clear()
  }

  /**
   * Binds the cross-executor network plane. Called once by the engine AFTER both this exchange and
   * the transport are constructed (the transport takes this exchange as its receive listener,
   * so the two are necessarily wired in two steps). Until this is called the exchange routes
   * purely in-process; afterwards remote consumers/producers are reachable over the network.
   * Idempotent rebinding is not supported (and never attempted by the engine).
   *
   * @param t the streaming transport for this executor (server + client factory over
   *          TransportContext)
   */
  def bindTransport(t: StreamingShuffleTransport): Unit = {
    transport = t
  }

  /**
   * Consumer-side entry point: tell a producer executor that this consumer wants blocks for the
   * given ranges, by sending a SUBSCRIBE over the transport. The producer registers a
   * [[RemoteReaderProxy]] pointing back at this executor's transport endpoint and begins streaming
   * matching blocks here. No-op if no transport is bound.
   *
   * Note on rendezvous: discovering WHICH executor hosts a given producer (and thus the
   * `producerHost`/`producerPort` to pass here) is resolved by the driver-side
   * [[StreamingShuffleEndpointCoordinator]], which maps each executor id to its
   * streaming-transport endpoint. The manager wires the reader to query that coordinator and
   * call this method for each remote peer, so cross-executor subscription is auto-driven WITHOUT
   * modifying the scheduler or `MapOutputTracker` (AAP section 0.6.2 excludes only modifying those
   * subsystems, not querying executor metadata). The data path itself crosses executor boundaries.
   *
   * @param producerHost the producer executor's streaming-transport host
   * @param producerPort the producer executor's streaming-transport port
   * @param shuffleId the shuffle to subscribe to
   * @param startMapIndex inclusive start of the consumed map-index range
   * @param endMapIndex exclusive end of the consumed map-index range
   * @param startPartition inclusive start of the consumed reduce-partition range
   * @param endPartition exclusive end of the consumed reduce-partition range
   */
  def subscribeRemote(
      producerHost: String,
      producerPort: Int,
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Unit = {
    val t = transport
    if (t != null) {
      t.send(producerHost, producerPort, StreamingShuffleTransport.encodeSubscribe(
        shuffleId, startMapIndex, endMapIndex, startPartition, endPartition, t.host, t.port))
    }
  }

  // ---------------------------------------------------------------------------------------------
  // StreamingTransportListener: receive-side callbacks invoked by the transport's RpcHandler when a
  // message arrives from a peer executor. They translate network messages into the SAME local
  // routing primitives used for in-process shuffle, so the producer/consumer contracts are
  // identical whether a peer is local or remote.
  // ---------------------------------------------------------------------------------------------

  /**
   * Producer side: a remote consumer subscribed. Register a [[RemoteReaderProxy]] for its ranges
   * through the ordinary [[registerReader]] path, so all existing routing (including replay of
   * already-published blocks and completion/failure logs) delivers to it -- except delivery
   * serializes the callback and ships it back to the consumer's endpoint.
   */
  override def onRemoteSubscribe(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      consumerHost: String,
      consumerPort: Int): Unit = {
    val proxy = new RemoteReaderProxy(shuffleId, consumerHost, consumerPort)
    registerReader(shuffleId, startMapIndex, endMapIndex, startPartition, endPartition, proxy)
    if (debug) {
      logDebug(s"Streaming shuffle registered remote reader $consumerHost:$consumerPort for " +
        s"shuffle $shuffleId maps [$startMapIndex, $endMapIndex) partitions " +
        s"[$startPartition, $endPartition)")
    }
  }

  /**
   * Consumer side: a block arrived from a remote producer. In the common case the local reader is
   * already subscribed (the reader registers locally BEFORE it subscribes remotely), so we remember
   * the producer endpoint -- so a later ack/resend for this exact block goes back to the owning
   * executor -- and deliver the bytes straight to the reader. The bytes stay owned by the
   * producer's spill manager until our ack frees them.
   *
   * Rare race (block arrives before the local reader subscribes): take local ownership instead --
   * store the bytes in THIS executor's spill manager and buffer the meta so [[registerReader]]
   * replays it, and ack the producer immediately so it frees its copy. The block is now locally
   * owned (absent from `remoteSources`), so the reader's later ack/resend reclaims/re-reads
   * locally.
   */
  override def onRemoteBlock(
      meta: BlockMeta,
      bytes: Array[Byte],
      producerHost: String,
      producerPort: Int): Unit = {
    if (!stopped) {
      val state = stateFor(meta.shuffleId)
      var admitted = true
      val target = state.synchronized {
        val reader = state.readers.find(_.matchesBlock(meta))
        if (reader.isEmpty) {
          // Pre-subscribe race: take LOCAL ownership of the bytes and buffer for replay -- but ONLY
          // if storage memory is admitted. On a hard admission failure (group B: consumer-side
          // memory pressure) we neither buffer nor ack below, so the producer keeps its copy and
          // its resend/timeout machinery re-delivers later; nothing is lost and an OOM is avoided.
          admitted = spillManager.register(meta.key, bytes)
          if (admitted) {
            state.pendingBlocks += meta
          }
        }
        reader
      }
      if (target.isDefined) {
        remoteSources.put(meta.key, (producerHost, producerPort))
        deliverBlock(target.get.consumer, meta, bytes)
      } else if (admitted) {
        // Ownership transferred locally above; release the producer's copy.
        val t = transport
        if (t != null) {
          t.send(producerHost, producerPort, StreamingShuffleTransport.encodeAck(meta))
        }
      }
    }
  }

  /**
   * Consumer side: a remote producer completed a map. Route it through the local completion path.
   */
  override def onRemoteComplete(
      shuffleId: Int,
      mapId: Long,
      mapIndex: Int,
      blockCounts: Array[Long]): Unit = {
    completeMap(shuffleId, mapId, mapIndex, blockCounts)
  }

  /** Consumer side: a remote producer failed. Route it through the local failure path. */
  override def onRemoteFailure(
      shuffleId: Int,
      mapId: Long,
      mapIndex: Int,
      bmAddress: BlockManagerId,
      message: String): Unit = {
    producerFailed(shuffleId, mapId, mapIndex, bmAddress, message, null)
  }

  /** Producer side: a remote consumer acknowledged a block. Reclaim its buffer here (we own it). */
  override def onRemoteAck(meta: BlockMeta): Unit = {
    spillManager.reclaim(meta.key)
    if (debug) {
      logDebug(s"Streaming shuffle reclaimed block ${meta.key} on remote consumer ack")
    }
  }

  /**
   * Producer side: a remote consumer requested a resend. Re-read and re-deliver (back over net).
   */
  override def onRemoteResend(meta: BlockMeta): Unit = {
    requestResend(meta)
  }

   // Hand one block to a consumer. Invoked only OUTSIDE the lock; the consumer's callback
   // may block on its own bounded inbox (intended backpressure) without holding any exchange lock.
  private def deliverBlock(
      consumer: StreamingBlockConsumer,
      meta: BlockMeta,
      bytes: Array[Byte]): Unit = {
    consumer.onBlockReceived(meta, bytes)
  }

  /**
   * A [[StreamingBlockConsumer]] that forwards every callback to a remote consumer executor
   * over the bound [[StreamingShuffleTransport]]. The producer-side exchange registers one of
   * these per remote SUBSCRIBE, so the ordinary routing in
   * [[publishBlock]]/[[completeMap]]/[[producerFailed]] delivers to it exactly as to a local
   * reader -- but "deliver" serializes the message and ships it to `consumerHost:consumerPort`.
   * The producer's own transport endpoint is stamped into each BLOCK so the consumer can
   * ack/resend back to the right executor.
   */
  private final class RemoteReaderProxy(shuffleId: Int, consumerHost: String, consumerPort: Int)
    extends StreamingBlockConsumer {

    override def onBlockReceived(meta: BlockMeta, bytes: Array[Byte]): Unit = {
      val t = transport
      if (t != null) {
        t.send(consumerHost, consumerPort,
          StreamingShuffleTransport.encodeBlock(t.host, t.port, meta, bytes))
      }
    }

    // The shuffle id is captured at registration (a proxy is created per remote SUBSCRIBE, which
    // carries it) because onMapComplete/onProducerFailed do not -- a local consumer already
    // knows it from its own registration, but the remote wire form must carry it.
    override def onMapComplete(mapId: Long, mapIndex: Int, blockCounts: Array[Long]): Unit = {
      val t = transport
      if (t != null) {
        t.send(consumerHost, consumerPort,
          StreamingShuffleTransport.encodeComplete(shuffleId, mapId, mapIndex, blockCounts))
      }
    }

    override def onProducerFailed(
        bmAddress: BlockManagerId,
        mapId: Long,
        mapIndex: Int,
        message: String,
        cause: Throwable): Unit = {
      val t = transport
      if (t != null) {
        t.send(consumerHost, consumerPort,
          StreamingShuffleTransport.encodeFailure(shuffleId, mapId, mapIndex, bmAddress, message))
      }
    }
  }
}

/**
 * Public contracts for [[StreamingBlockExchange]]: the per-block metadata carried end to end, the
 * reduce-side consumer callback interface, and the internal routing records. Confined to the
 * streaming package so existing components never depend on it.
 */
private[spark] object StreamingBlockExchange {

  /**
   * Identity and integrity metadata for a single streaming block, carried with the bytes from
   * producer to consumer and retained by the consumer so it can acknowledge ([[ack]]) or request a
   * resend ([[requestResend]]) of exactly this block. The reduce partition, shuffle id, and map id
   * are derived from the addressable [[MemorySpillManager.BlockKey]] to avoid duplicated state.
   *
   * @param key the spill-manager block key `(shuffleId, mapId, partitionId, seq)`
   * @param bmAddress the producer's [[BlockManagerId]] (used to populate `FetchFailedException`)
   * @param blockId the shuffle block id (used to unwrap the serialized stream and for diagnostics)
   * @param mapIndex the producing map task index
   * @param checksum the producer-computed checksum (CRC32C by default) of `bytes`
   * @param sizeBytes the serialized size of the block in bytes
   */
  case class BlockMeta(
      key: MemorySpillManager.BlockKey,
      bmAddress: BlockManagerId,
      blockId: BlockId,
      mapIndex: Int,
      checksum: Long,
      sizeBytes: Long) {

    /** The shuffle this block belongs to. */
    def shuffleId: Int = key.shuffleId

    /** The producing map task id. */
    def mapId: Long = key.mapId

    /** The target reduce partition (equal to the block key's partition id). */
    def reduceId: Int = key.partitionId

    /** The monotonic per-(shuffle, map, partition) block sequence number. */
    def seq: Long = key.seq
  }

  /**
   * Reduce-side callback interface a [[StreamingShuffleReader]] implements to receive streamed
   * blocks, map-completion coverage signals, and producer-failure notifications. Implementations
   * must be safe to call from producer threads and must apply their own backpressure (for example,
   * by bounding the inbox they enqueue into) since the exchange invokes these directly.
   */
  trait StreamingBlockConsumer {

    /**
     * Delivers a block and its metadata. The consumer validates integrity, acknowledges via
     * [[StreamingBlockExchange.ack]] once consumed, and may request a resend via
     * [[StreamingBlockExchange.requestResend]] on corruption.
     */
    def onBlockReceived(meta: BlockMeta, bytes: Array[Byte]): Unit

    /**
     * Signals that a producing map finished, reporting how many blocks it emitted for each reduce
     * partition (index = reduce partition id), so the consumer can verify full coverage.
     */
    def onMapComplete(mapId: Long, mapIndex: Int, blockCounts: Array[Long]): Unit

    /**
     * Signals that a producing map failed; the consumer invalidates and drives upstream
     * recomputation through the existing `FetchFailedException` path.
     */
    def onProducerFailed(
        bmAddress: BlockManagerId,
        mapId: Long,
        mapIndex: Int,
        message: String,
        cause: Throwable): Unit
  }

  /**
   * A subscribed reducer and its consumed ranges, used to route blocks (by reduce partition) and
   * map signals (by map index) to the right consumer.
   */
  private final class ReaderRegistration(
      val startMapIndex: Int,
      val endMapIndex: Int,
      val startPartition: Int,
      val endPartition: Int,
      val consumer: StreamingBlockConsumer) {

    /** True if the given block falls within both this reader's map range and partition range. */
    def matchesBlock(meta: BlockMeta): Boolean =
      meta.mapIndex >= startMapIndex && meta.mapIndex < endMapIndex &&
        meta.reduceId >= startPartition && meta.reduceId < endPartition

    /** True if the given map index falls within this reader's map range. */
    def matchesMap(mapIndex: Int): Boolean =
      mapIndex >= startMapIndex && mapIndex < endMapIndex
  }

  /** A recorded map completion with its per-reduce-partition block counts. */
  private final class MapCompletion(
      val mapId: Long,
      val mapIndex: Int,
      val blockCounts: Array[Long])

  /** A recorded producer failure. */
  private final class ProducerFailure(
      val bmAddress: BlockManagerId,
      val mapId: Long,
      val mapIndex: Int,
      val message: String,
      val cause: Throwable)
}
