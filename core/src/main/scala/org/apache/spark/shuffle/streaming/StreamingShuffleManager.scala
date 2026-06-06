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

import scala.util.control.NonFatal

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager

/**
 * Entry point of the OPT-IN streaming shuffle engine. It streams intermediate shuffle data directly
 * from producer (map) tasks to consumer (reduce) tasks through bounded in-memory buffers governed
 * by a backpressure protocol, removing the disk-materialization latency of sort-based shuffle for
 * shuffle-bound workloads.
 *
 * ==Coexistence strategy (the central design contract of this class)==
 * Streaming shuffle is two-fold opt-in: it is selected only when `spark.shuffle.manager=streaming`
 * (which makes `object ShuffleManager.create` resolve this class) AND the feature flag
 * `spark.shuffle.streaming.enabled=true` is set. The sort-based [[SortShuffleManager]] remains the
 * DEFAULT and the FALLBACK; this manager never modifies or replaces it. Instead it COMPOSES a
 * private `SortShuffleManager` instance and DELEGATES to it whenever:
 *
 *  - the streaming feature flag is disabled, or
 *  - the shuffle dependency is unsupported by the streaming writer (see [[canUseStreaming]] --
 *    e.g. map-side combine), or
 *  - a runtime fallback condition fires: the consumer is sustained 2x slower than the producer for
 *    more than 60s, memory pressure risks OOM during buffer allocation, the network link saturates
 *    above 90% capacity, or a producer/consumer version mismatch is detected.
 *
 * Concretely, every shuffle that streaming cannot (or should not) handle is registered with -- and
 * handled end-to-end by -- the composed sort engine. Dispatch is purely handle-based, mirroring how
 * [[SortShuffleManager]] dispatches on `SerializedShuffleHandle`/`BypassMergeSortShuffleHandle`:
 * [[registerShuffle]] returns a [[StreamingShuffleHandle]] only on the streaming path, and
 * [[getWriter]]/[[getReader]] route a [[StreamingShuffleHandle]] to the streaming writer/reader
 * while routing every other handle type to the composed sort engine. Because the streaming
 * components are constructed ONLY on the streaming path, the sort-based shuffle carries zero
 * streaming overhead and is wholly unaffected by this class.
 *
 * ==Integration discipline==
 * All changes for this feature are confined to the `ShuffleManager` abstraction boundary. The
 * scheduler, task lifecycle, user-facing APIs, the executor memory model, the network transport,
 * and the block-manager storage contracts are reused through their existing interfaces and are
 * never modified. Output discovery is unchanged: the streaming writer emits a standard `MapStatus`
 * so the unmodified `MapOutputTracker`/DAG scheduler locate streaming outputs exactly as for sort.
 * Fault recovery is unchanged: the streaming reader throws the existing `FetchFailedException` on
 * partial-read invalidation, which the unmodified scheduler converts into upstream recomputation.
 *
 * ==Constructor contract==
 * `object ShuffleManager.create` instantiates the configured manager via
 * `Utils.instantiateSerializerOrShuffleManager(className, conf, isDriver)`, so this class MUST
 * expose the 2-arg `(conf: SparkConf, isDriver: Boolean)` constructor. `SparkEnv` is never modified
 * -- registering the `"streaming"` short name in `object ShuffleManager.shortShuffleMgrNames` (a
 * separate one-line edit) is sufficient for dynamic resolution.
 *
 * @param conf the active [[SparkConf]] supplying `spark.shuffle.*` and `spark.shuffle.streaming.*`
 * @param isDriver whether this manager instance is the driver-side instance (executors create their
 *                 own); retained for parity with the [[ShuffleManager]] instantiation contract
 */
private[spark] class StreamingShuffleManager(conf: SparkConf, isDriver: Boolean)
  extends ShuffleManager with Logging {

  // Coexistence: the sort-based engine is composed (never modified) and used as the
  // graceful-degradation fallback path. Its 1-arg (conf) constructor is used verbatim, and its
  // single IndexShuffleBlockResolver is reused both for spilled streaming blocks and as this
  // manager's shuffleBlockResolver, so the block-manager storage contract is preserved unchanged.
  private[this] val sortShuffleManager = new SortShuffleManager(conf)

  // The streaming feature flag (spark.shuffle.streaming.enabled). When false, EVERY shuffle is
  // delegated to the composed sort engine at registration time, so streaming is fully inert even
  // though `spark.shuffle.manager=streaming` selected this class. Config entries live in
  // internal/config/package.scala; this file only READS them.
  private[this] val streamingEnabled: Boolean = conf.get(config.STREAMING_SHUFFLE_ENABLED)

  // Verbose debug-logging gate (spark.shuffle.streaming.debug). Held off the hot path so the
  // per-executor log volume stays under the mandated 10MB/hour budget.
  private[this] val debug: Boolean = conf.get(config.STREAMING_SHUFFLE_DEBUG)

  // Active streaming shuffle ids on this executor/driver, tracked exactly like
  // SortShuffleManager.taskIdMapsForShuffle uses a ConcurrentHashMap. Used for two purposes:
  //   1. its size() is the by-name `numConcurrentShuffles` divisor of the BackpressureProtocol
  //      token-bucket (per-shuffle bandwidth share tracks the live degree of concurrency), and
  //   2. cleanup bookkeeping in unregisterShuffle.
  // A shuffle id is present iff it is being handled on the streaming path. The value is an unused
  // presence marker; the map is used purely as a concurrent set.
  private[this] val streamingShuffles = new ConcurrentHashMap[Int, java.lang.Boolean]()

  // Lazily-constructed bundle of the shared streaming primitives (metrics source, spill manager,
  // block exchange, backpressure protocol). Built on first STREAMING getWriter/getReader rather
  // than in the constructor because the executor MemoryManager (SparkEnv.memoryManager, required by
  // MemorySpillManager) is initialized AFTER the ShuffleManager during SparkEnv bring-up. This
  // mirrors SortShuffleManager's `lazy val shuffleExecutorComponents`, which defers SparkEnv.get
  // for the same reason. A null value means streaming has not been activated yet, so stop() can
  // tear down without ever forcing construction. Guarded by double-checked locking on `this`.
  @volatile private[this] var engine: StreamingEngine = _

  if (debug) {
    logInfo(s"StreamingShuffleManager initialized (isDriver=$isDriver, " +
      s"streamingEnabled=$streamingEnabled); sort-based shuffle remains the default and fallback")
  }

  /**
   * Returns the shared streaming primitives, constructing them on first use under double-checked
   * locking. Only the streaming dispatch paths call this, so the sort fallback never triggers
   * construction and never pays for the streaming daemon threads (heartbeat/poller).
   */
  private def streamingEngine: StreamingEngine = {
    val existing = engine
    if (existing != null) {
      existing
    } else {
      synchronized {
        if (engine == null) {
          engine = new StreamingEngine
        }
        engine
      }
    }
  }

  /**
   * Gating helper mirroring `SortShuffleManager.canUseSerializedShuffle`: decides whether a shuffle
   * dependency can be handled by the streaming writer. The streaming writer serializes raw
   * (key, value) pairs and performs NO map-side aggregation, so a dependency requesting map-side
   * combine MUST fall back to the sort engine (which performs the aggregation). Returning false
   * here is the registration-time half of graceful degradation: such shuffles are registered with,
   * and handled end-to-end by, the composed [[SortShuffleManager]].
   *
   * @param dependency the shuffle dependency being registered
   * @return true if streaming can handle the dependency; false to fall back to sort-based shuffle
   */
  private def canUseStreaming(dependency: ShuffleDependency[_, _, _]): Boolean = {
    if (dependency.mapSideCombine) {
      if (debug) {
        logDebug(s"Shuffle ${dependency.shuffleId} needs map-side combine, which the streaming " +
          "writer does not perform; falling back to sort-based shuffle")
      }
      false
    } else {
      true
    }
  }

  /**
   * Obtains a [[ShuffleHandle]] to pass to tasks. On the streaming path this returns a
   * [[StreamingShuffleHandle]] (the dispatch marker that routes the writer/reader to the streaming
   * engine); otherwise it delegates to the composed [[SortShuffleManager]].
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (streamingEnabled && canUseStreaming(dependency)) {
      // Streaming path: mark the shuffle active so the BackpressureProtocol concurrency divisor and
      // unregisterShuffle cleanup see it, then return the marker handle that getWriter/getReader
      // dispatch on. This is the ONLY place a StreamingShuffleHandle is created.
      streamingShuffles.put(shuffleId, java.lang.Boolean.TRUE)
      if (debug) {
        logDebug(s"Registering shuffle $shuffleId on the streaming path " +
          s"(${dependency.partitioner.numPartitions} partitions)")
      }
      new StreamingShuffleHandle[K, V, C](shuffleId, dependency)
    } else {
      // Coexistence: streaming is disabled OR the dependency is unsupported -> delegate
      // to the composed SortShuffleManager, which remains the default and the fallback. The sort
      // engine returns its own handle type and handles this shuffle end-to-end; it is never
      // modified.
      sortShuffleManager.registerShuffle(shuffleId, dependency)
    }
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      case streamingHandle: StreamingShuffleHandle[K @unchecked, V @unchecked, _] =>
        // Streaming dispatch: track the id (keeps the executor-side concurrency divisor accurate
        // even though registerShuffle ran on the driver) and build the streaming writer over the
        // shared primitives. Constructing the engine here defers SparkEnv.memoryManager access to
        // task time, when it is guaranteed initialized.
        streamingShuffles.put(handle.shuffleId, java.lang.Boolean.TRUE)
        val e = streamingEngine
        new StreamingShuffleWriter[K, V](
          streamingHandle, mapId, context, metrics, conf,
          e.backpressure, e.spillManager, e.exchange, e.metricsSource)
      case other =>
        // Coexistence: any non-streaming handle flows to the composed SortShuffleManager (the
        // default + fallback). The streaming writer is never constructed for it, so sort-based
        // shuffle carries zero streaming overhead.
        sortShuffleManager.getWriter(other, mapId, context, metrics)
    }
  }

  /**
   * Get a reader for a range of reduce partitions to read from a range of map outputs. This
   * overrides the NON-final 7-arg `getReader`; the 5-arg overload on [[ShuffleManager]] is `final`
   * and already delegates here, so it must NOT be overridden.
   *
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    handle match {
      case streamingHandle: StreamingShuffleHandle[K @unchecked, _, C @unchecked] =>
        // Streaming dispatch: build the streaming reader over the shared primitives. The reader's
        // last two ctor args (inbox bounds) default, so only the 11 required args are passed.
        streamingShuffles.put(handle.shuffleId, java.lang.Boolean.TRUE)
        val e = streamingEngine
        new StreamingShuffleReader[K, C](
          streamingHandle, startMapIndex, endMapIndex, startPartition, endPartition,
          context, metrics, conf, e.backpressure, e.metricsSource, e.exchange)
      case _ =>
        // Coexistence: any non-streaming handle flows to the composed SortShuffleManager's reader
        // (the default + fallback), passing the map/partition ranges through unchanged.
        sortShuffleManager.getReader(
          handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
    }
  }

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
   *
   * Coexistence: this returns the composed [[SortShuffleManager]]'s single
   * `IndexShuffleBlockResolver` rather than creating a second resolver. Reusing ONE resolver
   * preserves the block-manager storage contract and is exactly the resolver the
   * [[MemorySpillManager]] writes spilled streaming blocks through, so streaming and sort-based
   * blocks share one consistent on-disk storage path.
   */
  override def shuffleBlockResolver: ShuffleBlockResolver = sortShuffleManager.shuffleBlockResolver

  /**
   * Remove a shuffle's metadata from the ShuffleManager.
   *
   * @return true if the metadata removed successfully, otherwise false.
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    // Streaming-side bookkeeping: drop the id from the active streaming set so the live concurrency
    // count (the token-bucket bandwidth divisor) stays accurate. Buffered streaming blocks for this
    // shuffle are reclaimed through the consumer-ack path and freed on writer failure, and any
    // spilled blocks (written through the shared resolver) are removed by the delegated call below.
    streamingShuffles.remove(shuffleId)
    // Coexistence: ALWAYS delegate to the composed SortShuffleManager. It owns the single
    // IndexShuffleBlockResolver and removes this shuffle's per-map on-disk data exactly as for
    // sort-based shuffle, whether the shuffle was handled by streaming or by sort. Return its
    // result so callers observe identical semantics to the default manager.
    sortShuffleManager.unregisterShuffle(shuffleId)
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    // Coexistence: tear down streaming lifecycles first (backpressure heartbeat thread, spill
    // poller thread, exchange routing state) -- but ONLY if the streaming engine was ever activated
    // (lazy on first streaming use); a sort-only run never constructed it. Then ALWAYS stop the
    // composed SortShuffleManager in the finally block so the fallback engine and the shared
    // resolver are shut down even if streaming teardown throws.
    try {
      val e = engine
      if (e != null) {
        e.stop()
      }
    } finally {
      sortShuffleManager.stop()
    }
  }

  /**
   * The bundle of shared, per-executor streaming primitives, constructed lazily on first streaming
   * use by [[streamingEngine]]. Keeping the wiring in one inner class isolates all streaming
   * collaborator construction to this package (the zero-cross-contamination rule) and gives
   * [[stop]] a single, ordered teardown entry point. Field initialization order matters: the
   * metrics source is built (and registered) first, then the spill manager (which the exchange and
   * backpressure reference), then the exchange, then the backpressure protocol.
   */
  private final class StreamingEngine {

    // The single, canonical holder of the four streaming JMX metrics. Registered with the EXISTING
    // MetricsSystem using the same Dropwizard MetricRegistry pattern as ExecutorSource -- no new
    // metrics framework. Its gauges read pre-computed atomic holders, so telemetry overhead stays
    // well under the mandated 1% CPU budget.
    val metricsSource: StreamingShuffleSource = new StreamingShuffleSource

    // Coexistence (telemetry): register the source with the running MetricsSystem when one is
    // available. Guarded by Option(SparkEnv.get) and NonFatal so a missing env (e.g. a bare unit
    // test) or a duplicate-registration warning never breaks the streaming data path; metrics are
    // best-effort observability, not a correctness dependency.
    Option(SparkEnv.get).foreach { env =>
      try {
        env.metricsSystem.registerSource(metricsSource)
      } catch {
        case NonFatal(e) =>
          logWarning("Failed to register the streaming shuffle metrics source; continuing " +
            "without JMX telemetry for streaming shuffle", e)
      }
    }

    // Spill coordinator that bounds buffer memory through the EXISTING MemoryManager and writes
    // spilled blocks through the composed sort engine's single IndexShuffleBlockResolver (reuse
    // only -- no new memory model, no second resolver, no new storage path). SparkEnv.memoryManager
    // is read here, at first streaming use, when it is guaranteed initialized.
    val spillManager: MemorySpillManager = new MemorySpillManager(
      conf, SparkEnv.get.memoryManager, sortShuffleManager.shuffleBlockResolver, metricsSource)

    // In-process producer->consumer data path that delivers writer-emitted blocks to subscribed
    // readers and carries acks/retransmission requests back. It holds no block bytes itself (the
    // spill manager owns them); a later checkpoint backs it with the EXISTING TransportContext
    // without changing the writer/reader contract.
    val exchange: StreamingBlockExchange = new StreamingBlockExchange(conf, spillManager)

    // Flow control + token-bucket rate limiter shared by the streaming writers/readers. The
    // by-name `numConcurrentShuffles` argument is the live count of active streaming shuffles, so
    // each shuffle's bandwidth share (maxBandwidthMBps / numConcurrentShuffles) tracks the current
    // degree of concurrency. It also exposes the sustained-slow-consumer `shouldFallback` signal
    // the writer consults to degrade gracefully to sort-based shuffle.
    val backpressure: BackpressureProtocol =
      new BackpressureProtocol(conf, streamingShuffles.size(), metricsSource)

    /**
     * Stops the streaming lifecycles in reverse dependency order. Each collaborator's `stop()` is
     * idempotent; the nested try/finally guarantees the spill manager (which owns the daemon poller
     * thread and any spill files) is always stopped even if an earlier teardown throws. The shared
     * IndexShuffleBlockResolver is owned by the composed SortShuffleManager and is stopped by its
     * `stop()`, not here.
     */
    def stop(): Unit = {
      try {
        backpressure.stop()
      } finally {
        try {
          exchange.stop()
        } finally {
          spillManager.stop()
        }
      }
    }
  }
}
