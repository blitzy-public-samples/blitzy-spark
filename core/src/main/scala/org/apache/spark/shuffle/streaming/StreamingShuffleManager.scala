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
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.util.RpcUtils

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

  // In-memory size at which a FallbackShuffleWriter's SpillableReplayBuffer spills to a temp file.
  // The replay buffer captures records during a streaming write so a runtime fallback can replay
  // them into sort; it is discarded unread on the happy path. A modest 4MB cap keeps small or
  // CPU-bound maps entirely in memory (no disk, no regression for those) while bounding heap use
  // for large maps, which spill -- honoring the memory-safety gate. It is intentionally not a user
  // config: the five spark.shuffle.streaming.* entries are the frozen configuration surface for v1.
  private[this] val replayMemoryThresholdBytes: Long = 4L * 1024L * 1024L

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

  // F4 (stop/first-use race safety): set true by stop() while holding `this` -- the SAME monitor
  // streamingEngine() constructs the engine under. Once true, streamingEngine() refuses to build a
  // new engine (so no daemon heartbeat/poller threads can be spawned after stop() begins), and any
  // engine constructed concurrently with stop() is guaranteed to be observed and torn down by
  // stop(). Declared @volatile so the streamingEngine() fast path can read it without the monitor.
  @volatile private[this] var stopped: Boolean = false

  // ---------------------------------------------------------------------------------------------
  // Cross-executor rendezvous (CRITICAL closure of the AAP's producer->consumer streaming
  // objective). The data plane (StreamingBlockExchange/StreamingShuffleTransport) can move blocks
  // across executors, but a reduce task must first DISCOVER which executors host its producers. The
  // unmodified MapOutputTracker records only post-completion block locations, so this manager hosts
  // a small additive directory -- the StreamingShuffleEndpointCoordinator -- entirely WITHIN the
  // ShuffleManager boundary (no scheduler/MapOutputTracker/task changes).
  //
  // All three are built best-effort and LAZILY, guarded by `this`, exactly like `engine`: SparkEnv
  // (and thus rpcEnv) is null while this manager is constructed during SparkEnv bring-up. When no
  // SparkEnv/RpcEnv is available (e.g. a bare unit test) they stay None and streaming runs
  // local-only, preserving graceful degradation. `rendezvousInitialized` makes the (idempotent)
  // setup run at most once per manager.
  @volatile private[this] var rendezvousInitialized: Boolean = false
  // DRIVER: ref to the coordinator endpoint this manager set up. EXECUTOR: ref to the driver's
  // coordinator. None when no RpcEnv is available.
  @volatile private[this] var coordinatorRef: Option[RpcEndpointRef] = None
  // Executor-side client wrapping `coordinatorRef`: advertises this executor's transport endpoint
  // and answers reduce-side peer discovery. None whenever `coordinatorRef` is None.
  @volatile private[this] var rendezvous: Option[StreamingShuffleRendezvous] = None

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
    // Fast path: an engine already exists AND shutdown has not begun. The `&& !stopped` guard
    // ensures that once stop() starts, no caller is handed an engine without first going through
    // the monitor below, where the stopped state is authoritative.
    if (existing != null && !stopped) {
      existing
    } else {
      synchronized {
        // F4: authoritative stopped check under the same monitor stop() uses. Refusing construction
        // here is precisely what makes it impossible to spawn streaming daemon threads
        // (backpressure heartbeat, spill poller) after stop() has begun -- first use after stop
        // fails fast instead of silently leaking threads behind stop()'s back.
        if (stopped) {
          throw new IllegalStateException(
            "StreamingShuffleManager has been stopped and cannot service new streaming shuffles")
        }
        if (engine == null) {
          engine = new StreamingEngine
        }
        engine
      }
    }
  }

  /**
   * Best-effort, idempotent setup of the cross-executor rendezvous, mirroring
   * `SparkEnv.registerOrLookupEndpoint`: the DRIVER sets up the single coordinator endpoint and the
   * EXECUTOR looks up a ref to it. Deferred to first use (not the constructor) because `SparkEnv`
   * is not yet available while this manager is built during SparkEnv bring-up. Guarded by `this`
   * and a one-shot flag so the (driver) endpoint is registered exactly once.
   *
   * @return the executor-side [[StreamingShuffleRendezvous]] client, or None when no `SparkEnv`/
   *         `RpcEnv` is available (e.g. a bare unit test) -- streaming then runs local-only
   */
  private def ensureRendezvous(): Option[StreamingShuffleRendezvous] = {
    if (!rendezvousInitialized) {
      synchronized {
        if (!rendezvousInitialized) {
          Option(SparkEnv.get).foreach { env =>
            try {
              val ref =
                if (isDriver) {
                  // Driver hosts the directory. setupEndpoint registers it under the well-known
                  // name; this runs on first registerShuffle (driver), before any task launches.
                  env.rpcEnv.setupEndpoint(
                    StreamingShuffleEndpointCoordinator.ENDPOINT_NAME,
                    new StreamingShuffleEndpointCoordinator(env.rpcEnv))
                } else {
                  // Executor looks up the driver's directory by name (reuses the existing RpcEnv).
                  RpcUtils.makeDriverRef(
                    StreamingShuffleEndpointCoordinator.ENDPOINT_NAME, conf, env.rpcEnv)
                }
              coordinatorRef = Some(ref)
              rendezvous = Some(new StreamingShuffleRendezvous(ref, conf, env.executorId))
            } catch {
              case NonFatal(e) =>
                logWarning("Failed to set up the streaming shuffle rendezvous coordinator; " +
                  "cross-executor streaming is disabled and streaming runs local-only", e)
                coordinatorRef = None
                rendezvous = None
            }
          }
          rendezvousInitialized = true
        }
      }
    }
    rendezvous
  }

  /**
   * Best-effort teardown of the cross-executor rendezvous, called from [[stop]]. The EXECUTOR
   * withdraws its advertised endpoint from the driver directory so a later discovering reduce task
   * never targets a dead transport; the DRIVER additionally stops the coordinator endpoint it owns.
   * Fully guarded so a teardown failure can never prevent the composed [[SortShuffleManager]] from
   * shutting down (the central coexistence guarantee of [[stop]]). Idempotent and safe when the
   * rendezvous was never initialized (both fields stay None -> no-ops).
   */
  private def tearDownRendezvous(): Unit = {
    // Withdraw this executor's endpoint first (fire-and-forget), then stop the owned endpoint. The
    // helper is already best-effort internally; the extra guard here defends the stop() finally.
    try {
      rendezvous.foreach(_.unregister())
    } catch {
      case NonFatal(e) =>
        logWarning("Failed to withdraw the streaming shuffle endpoint on stop (best-effort)", e)
    }
    // Only the DRIVER owns the coordinator endpoint (it called setupEndpoint); executors merely
    // hold a ref to it, so they must NOT stop it. Stopping the endpoint clears the directory.
    if (isDriver) {
      try {
        coordinatorRef.foreach(ref => Option(SparkEnv.get).foreach(_.rpcEnv.stop(ref)))
      } catch {
        case NonFatal(e) =>
          logWarning("Failed to stop the streaming shuffle coordinator endpoint (best-effort)", e)
      }
    }
  }

  /**
   * Best-effort probe for AAP fallback condition (d): producer/consumer protocol-version mismatch.
   * Delegates to the cross-executor rendezvous, which compares every advertised endpoint's
   * streaming protocol version against this executor's. Wired into the [[BackpressureProtocol]]
   * heartbeat (via [[BackpressureProtocol.setFallbackProbe]]) so a mismatch flips `shouldFallback`
   * and the streaming writer degrades affected shuffles to sort. Returns false when no rendezvous
   * is available (local mode / bare test) or on any failure, so it never disrupts a healthy run.
   *
   * @return true if a peer advertises a different streaming protocol version, false otherwise
   */
  private def versionMismatchDetected(): Boolean = {
    try {
      ensureRendezvous().exists(_.detectVersionMismatch())
    } catch {
      case NonFatal(e) =>
        logDebug("Streaming shuffle version-mismatch probe failed (best-effort); no fallback", e)
        false
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
      // Cross-executor rendezvous: registerShuffle runs on the DRIVER before any task launches, so
      // this is the right moment to stand up the coordinator endpoint. It guarantees the directory
      // exists before executors look it up at first streaming write/read. Best-effort and
      // idempotent (no-op on executors and after the first call); never blocks or fails register.
      ensureRendezvous()
      if (debug) {
        logDebug(s"Registering shuffle $shuffleId on the streaming path " +
          s"(${dependency.partitioner.numPartitions} partitions)")
      }
      // numMaps = total producer-map-task count, captured HERE on the driver. registerShuffle runs
      // during ShuffleDependency construction, where dependency.rdd is non-null; but
      // ShuffleDependency.rdd is @transient and is null once the handle is deserialized on an
      // executor. Carrying numMaps in the handle lets the executor-side StreamingShuffleReader
      // resolve the getReader(endMapIndex=Int.MaxValue) sentinel with no MapOutputTracker RPC or
      // scheduler change (QA finding F-1). It equals the numMaps the DAG scheduler registers.
      val numMaps = dependency.rdd.partitions.length
      new StreamingShuffleHandle[K, V, C](shuffleId, dependency, numMaps)
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
        // QoS (AAP): register this shuffle's priority so the backpressure admission path
        // (BackpressureProtocol.tryAcquire) arbitrates bandwidth in favor of higher-priority
        // shuffles under token scarcity. isSpeculative is false because TaskContext exposes NO
        // public speculative flag; the comparator fully supports the dimension (proven by tests)
        // and enforces it for any caller that supplies it. Idempotent (last-wins) across this
        // shuffle's many map tasks on this executor; unregisterShuffle drops it.
        e.backpressure.registerShufflePriority(
          handle.shuffleId,
          BackpressureProtocol.ShufflePriority(
            isSpeculative = false,
            streamingHandle.dependency.partitioner.numPartitions,
            dataVolumeBytes = 0L))
        val streamingWriter = new StreamingShuffleWriter[K, V](
          streamingHandle, mapId, context, metrics, conf,
          e.backpressure, e.spillManager, e.exchange, e.metricsSource)
        // Runtime fallback (F1): wrap the streaming writer so a StreamingShuffleFallbackException
        // raised mid-write (oversize unsplittable block, slow-consumer/backpressure shouldFallback,
        // or exhausted admission) is caught HERE -- before any MapStatus is advertised -- and the
        // map is transparently re-run through the composed SortShuffleManager's writer. The wrapper
        // tees consumed records into a bounded, spillable replay buffer so the already-consumed
        // prefix can be replayed into sort and concatenated with the remaining source records. This
        // delivers the AAP's automatic graceful degradation (zero task/stage failure on fallback)
        // without touching the scheduler, and emits a standard MapStatus exactly as sort would.
        new FallbackShuffleWriter[K, V](
          streamingHandle, mapId, context, metrics, streamingWriter,
          sortShuffleManager, SparkEnv.get.blockManager, replayMemoryThresholdBytes)
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
        // Streaming dispatch: build the streaming reader over the shared primitives, opting into
        // silence-driven fallback. With fallbackOnSilence=true the reader raises a
        // StreamingReadFallbackException (instead of a FetchFailedException) when a producer goes
        // silent BEFORE any record has been yielded, so the wrapper below can still degrade to sort
        // cleanly. The inbox-bound ctor args default, so only the required args plus the flag pass.
        streamingShuffles.put(handle.shuffleId, java.lang.Boolean.TRUE)
        val e = streamingEngine
        // Cross-executor rendezvous (R-A): resolve the getReader endMapIndex sentinel exactly as
        // the reader does internally (Int.MaxValue -> the handle's authoritative numMaps) so the
        // remote SUBSCRIBE advertises the SAME map range to producers that the reader's local
        // registration uses, keeping the producer-side RemoteReaderProxy map filter identical to
        // the local reader's coverage.
        val resolvedEndMapIndex =
          if (endMapIndex == Int.MaxValue) streamingHandle.numMaps else endMapIndex
        val streamingReader = new StreamingShuffleReader[K, C](
          streamingHandle, startMapIndex, endMapIndex, startPartition, endPartition,
          context, metrics, conf, e.backpressure, e.metricsSource, e.exchange,
          fallbackOnSilence = true,
          // The reader fires this ONCE on first read(): discover the remote producer executors from
          // the driver coordinator and subscribe this reduce task's exchange to each, so blocks on
          // OTHER executors stream here automatically over the EXISTING transport -- this is the
          // production caller the cross-executor path previously lacked. It is best-effort:
          // ensureRendezvous() returns None when no RpcEnv/coordinator is available (e.g. local
          // mode or a bare unit test) and subscribeToRemoteProducers swallows discovery failures,
          // so the reader is left on its local in-process subscription only, preserving graceful
          // degradation (and degrading further to sort if expected remote blocks never arrive).
          remoteSubscribe = () => ensureRendezvous().foreach(_.subscribeToRemoteProducers(
            e.exchange, streamingHandle.shuffleId, startMapIndex, resolvedEndMapIndex,
            startPartition, endPartition)))
        // Runtime fallback (F1): wrap the streaming reader so a StreamingReadFallbackException
        // raised before any record is produced transparently rebuilds a SortShuffleManager reader
        // for the SAME map/partition range and serves from it. The buildSortReader thunk is invoked
        // lazily only on fallback. Limitation (documented): once the streaming reader has yielded a
        // record, fallback can no longer be clean, so the reader instead invalidates via
        // FetchFailedException and the unmodified DAG scheduler recomputes the upstream stage --
        // preserving correctness.
        new FallbackShuffleReader[K, C](
          streamingReader,
          () => sortShuffleManager.getReader[K, C](
            handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics))
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
    // count (the token-bucket bandwidth divisor) stays accurate.
    streamingShuffles.remove(shuffleId)
    // Streaming-side cleanup (F3): the sort manager only tracks map ids that flowed through ITS
    // writer, so its unregisterShuffle cannot reclaim streaming buffers/spill files -- streaming
    // map ids were never registered there. Reclaim every block this shuffle still owns directly
    // through the spill manager: in-memory buffers that consumers have not yet acknowledged AND
    // any blocks that already spilled to temp files. This covers the unregister-before-ack case
    // where outputs are still resident. Guarded on a non-null engine because a sort-only run (or a
    // run before first streaming use) never constructed one and thus holds no streaming state.
    val e = engine
    if (e != null) {
      e.spillManager.unregisterShuffle(shuffleId)
      // QoS cleanup: drop this shuffle's priority descriptor so the backpressure admission path no
      // longer arbitrates for it (and the registry does not retain stale entries across shuffles).
      e.backpressure.unregisterShufflePriority(shuffleId)
    }
    // Coexistence: ALWAYS delegate to the composed SortShuffleManager afterwards. It owns the
    // single IndexShuffleBlockResolver and removes any sort-owned per-map on-disk data for this
    // shuffle -- both for shuffles it handled directly and for streaming maps that fell back to
    // sort. Return its result so callers observe identical semantics to the default manager.
    sortShuffleManager.unregisterShuffle(shuffleId)
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    // Race-safety (F4): flip the stopped flag and read the engine reference inside the SAME monitor
    // that streamingEngine uses to construct the engine. This makes stop() and lazy construction
    // mutually exclusive, eliminating the window where a concurrent first use could create daemon
    // poller/heartbeat threads after stop() had already observed a null engine. After this
    // critical section, exactly one of two things is true: (a) no engine was ever built and none
    // ever will be (a later first use throws IllegalStateException), or (b) toStop holds the one
    // engine that was built, and we are responsible for stopping it -- even if it was constructed
    // concurrently just before we won the lock.
    val toStop = synchronized {
      stopped = true
      engine
    }
    // Coexistence: tear down streaming lifecycles first (backpressure heartbeat thread, spill
    // poller thread, exchange routing state) -- but ONLY if the streaming engine was ever
    // activated; a sort-only run never constructed it. Then ALWAYS stop the composed
    // SortShuffleManager in the finally block so the fallback engine and the shared resolver are
    // shut down even if streaming teardown throws.
    try {
      if (toStop != null) {
        toStop.stop()
      }
    } finally {
      // Cross-executor rendezvous teardown (best-effort, self-guarded): withdraw this executor's
      // advertised endpoint and, on the driver, stop the coordinator endpoint. Done in the finally
      // before the sort stop so it runs even if streaming-engine teardown threw, and it can never
      // itself prevent the composed SortShuffleManager from shutting down.
      tearDownRendezvous()
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

    // Producer->consumer data path that delivers writer-emitted blocks to subscribed readers and
    // carries acks/retransmission requests back. It holds no block bytes itself (the spill manager
    // owns them). For co-located producer/consumer it hands off in-process; for cross-executor
    // streaming it routes over the EXISTING network transport via the streaming transport bound
    // below, without changing the writer/reader contract.
    val exchange: StreamingBlockExchange = new StreamingBlockExchange(conf, spillManager)

    // Cross-executor network plane built on the EXISTING TransportContext stack (reuse only -- see
    // StreamingShuffleTransport). It is bound to the exchange (its receive listener) so blocks and
    // control messages can cross executor boundaries. Best-effort: if a transport server cannot be
    // stood up (e.g. a bare unit-test env, or no SparkEnv/blockManager), streaming stays in-process
    // for co-located tasks rather than failing -- mirroring the metrics-source guard above.
    // Two-step wiring (construct exchange, then transport with exchange as listener, then
    // bindTransport) is required because the dependency is bidirectional.
    val transport: Option[StreamingShuffleTransport] =
      try {
        Option(SparkEnv.get).map { env =>
          val bindHost = env.blockManager.blockManagerId.host
          val t = new StreamingShuffleTransport(conf, bindHost, exchange)
          exchange.bindTransport(t)
          t
        }
      } catch {
        case NonFatal(e) =>
          logWarning("Failed to start the streaming shuffle network transport; streaming shuffle " +
            "will operate in-process for co-located tasks only", e)
          None
      }

    // Cross-executor rendezvous (producer/consumer advertisement): once this executor's streaming
    // transport is listening, advertise its (host, port) to the driver coordinator so REMOTE reduce
    // tasks can discover and subscribe to it. Best-effort via ensureRendezvous(); a missing
    // coordinator (or no bound transport) simply leaves this executor reachable for co-located
    // (in-process) tasks only, preserving graceful degradation.
    transport.foreach { t =>
      ensureRendezvous().foreach(_.register(t.host, t.port))
    }

    // Flow control + token-bucket rate limiter shared by the streaming writers/readers. The
    // by-name `numConcurrentShuffles` argument is the live count of active streaming shuffles, so
    // each shuffle's bandwidth share (maxBandwidthMBps / numConcurrentShuffles) tracks the current
    // degree of concurrency. It also exposes the `shouldFallback` signal the writer consults to
    // degrade gracefully to sort-based shuffle on any of the four AAP fallback conditions.
    val backpressure: BackpressureProtocol =
      new BackpressureProtocol(conf, streamingShuffles.size(), metricsSource)

    // Wire the cross-executor version handshake into the backpressure heartbeat (AAP fallback
    // condition (d)): on each tick, while not already in fallback, the protocol consults this probe
    // and -- if a peer advertises a different streaming protocol version -- raises `shouldFallback`
    // so the writer degrades affected shuffles to sort. Best-effort: returns false in local mode or
    // when no coordinator is reachable, so a healthy homogeneous cluster never falls back on this.
    backpressure.setFallbackProbe(() => versionMismatchDetected())

    /**
     * Stops the streaming lifecycles in reverse dependency order. Each collaborator's `stop()` is
     * idempotent; the nested try/finally guarantees the spill manager (which owns the daemon poller
     * thread and any spill files) is always stopped even if an earlier teardown throws. The
     * transport is stopped before the exchange so no further network messages arrive while routing
     * state is being dropped. The shared IndexShuffleBlockResolver is owned by the composed
     * SortShuffleManager and is stopped by its `stop()`, not here.
     */
    def stop(): Unit = {
      try {
        backpressure.stop()
      } finally {
        try {
          transport.foreach(_.stop())
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
}
