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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashSet

/**
 * Streaming shuffle manager implementing the [[ShuffleManager]] trait as an opt-in
 * alternative to the default sort-based shuffle.
 *
 * This manager introduces a producer-to-consumer data pipeline that eliminates shuffle
 * materialization latency by streaming buffered data directly from map tasks to reduce
 * tasks with integrated backpressure flow control, memory-spill coordination, and
 * fault-tolerant partial-read invalidation.
 *
 * ==Activation==
 * Enabled via `spark.shuffle.manager=streaming` with `spark.shuffle.streaming.enabled=true`.
 * When streaming is disabled (the default), [[registerShuffle]] falls back to creating a
 * [[BaseShuffleHandle]] which the sort-based path handles transparently. This ensures
 * coexistence with [[org.apache.spark.shuffle.sort.SortShuffleManager]] as the
 * production-stable default.
 *
 * ==Factory Methods==
 * - [[registerShuffle]]: Returns [[StreamingShuffleHandle]] when streaming is enabled,
 *   carrying buffer sizing and spill threshold metadata for executor-side configuration.
 * - [[getWriter]]: Creates [[StreamingShuffleWriter]] instances that buffer records in
 *   per-partition memory regions and pipeline them via the Netty transport layer.
 * - [[getReader]]: Creates [[StreamingShuffleReader]] instances that poll producers for
 *   available blocks before shuffle completion with CRC32C checksum validation.
 *
 * ==Fallback Support==
 * Runtime conditions (consumer slowdown, memory pressure, network saturation) are
 * monitored via [[shouldFallback]]. When sustained degradation is detected, subsequent
 * shuffle registrations fall back to [[BaseShuffleHandle]] for sort-based handling.
 * In-flight streaming shuffles complete gracefully or spill to disk before fallback.
 *
 * ==Telemetry==
 * Operational metrics are exposed via [[StreamingShuffleMetricsSource]] (Dropwizard gauges)
 * for buffer utilization, spill counts, backpressure events, and partial read invalidations.
 * Metrics registration is lazy and only occurs when streaming shuffle is actually used.
 *
 * ==Thread Safety==
 * All mutable state uses thread-safe primitives: [[ConcurrentHashMap]] for task tracking,
 * [[AtomicBoolean]]/[[AtomicLong]] for fallback state, and `@volatile` for metrics source.
 * The [[OpenHashSet]] per-shuffle task ID set is accessed under `synchronized` blocks.
 *
 * ==Isolation==
 * All streaming logic is self-contained within the `org.apache.spark.shuffle.streaming`
 * package. There are zero imports from `org.apache.spark.shuffle.sort` -- the sort-based
 * implementation is completely unaffected by this class.
 *
 * @param conf SparkConf for accessing streaming shuffle configuration parameters
 * @param isDriver true when this manager is instantiated on the driver, false on executors.
 *                 As documented in the [[ShuffleManager]] trait Scaladoc: "This will be
 *                 instantiated by SparkEnv so its constructor can take a SparkConf and
 *                 boolean isDriver as parameters."
 */
private[spark] class StreamingShuffleManager(conf: SparkConf, isDriver: Boolean)
  extends ShuffleManager with Logging {

  // ===========================================================================
  // Internal State -- Task ID Tracking
  // ===========================================================================

  /**
   * Thread-safe mapping from shuffle IDs to the set of map task IDs producing output
   * for those shuffles. Used during [[unregisterShuffle]] to clean up per-map-task
   * resources (spilled blocks, cached data).
   *
   * Same pattern as `SortShuffleManager.taskIdMapsForShuffle` -- the
   * [[ConcurrentHashMap]] provides safe concurrent put/remove from multiple executor
   * threads, while individual [[OpenHashSet]] operations are protected by
   * `synchronized` blocks on the set instance.
   */
  private[this] val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()

  // ===========================================================================
  // Internal State -- Metrics Source
  // ===========================================================================

  /**
   * Streaming shuffle metrics source, lazily initialized when the first streaming
   * shuffle is registered. The `@volatile` annotation ensures visibility of the
   * `Some(source)` assignment across executor threads without explicit synchronization
   * for the common read path.
   */
  @volatile private var streamingMetricsSource: Option[StreamingShuffleMetricsSource] = None

  // ===========================================================================
  // Internal State -- Fallback Condition Tracking
  // ===========================================================================

  /**
   * Whether the automatic fallback to sort-based shuffle has been activated.
   * Once set to `true`, all subsequent [[registerShuffle]] calls return
   * [[BaseShuffleHandle]] instead of [[StreamingShuffleHandle]]. This is a
   * one-way flag -- once activated, fallback persists for the executor's lifetime
   * (configuration changes require executor restart per AAP v1 constraint).
   */
  private val fallbackActivated = new AtomicBoolean(false)

  /**
   * Timestamp (nanos) when sustained consumer slowdown was first detected.
   * Used to enforce the 60-second duration threshold before activating fallback.
   * Reset whenever the consumer-to-producer speed ratio drops below the threshold.
   */
  @volatile private var consumerSlowdownStartNanos: Long = System.nanoTime()

  /**
   * Current consumer-to-producer speed ratio encoded as a long via
   * [[java.lang.Double.doubleToLongBits]] for atomic read/write.
   * A ratio >= 2.0 means the consumer is at least 2x slower than the producer.
   * Updated by streaming components via [[reportConsumerSlowdown]].
   */
  private val currentConsumerSlowdownRatio =
    new AtomicLong(java.lang.Double.doubleToLongBits(0.0))

  /**
   * Current network saturation ratio (0.0 to 1.0) encoded as a long.
   * A value > 0.9 indicates network saturation exceeding the 90% threshold.
   * Updated by streaming components via [[reportNetworkSaturation]].
   */
  private val currentNetworkSaturation =
    new AtomicLong(java.lang.Double.doubleToLongBits(0.0))

  /**
   * Whether memory pressure has been detected that prevents buffer allocation.
   * Set by [[MemorySpillManager]] when execution memory pool is exhausted and
   * buffer allocation would risk OOM. Updated via [[reportMemoryPressure]].
   */
  private val memoryPressureDetected = new AtomicBoolean(false)

  // ===========================================================================
  // Block Resolver
  // ===========================================================================

  /**
   * Streaming block resolver providing block data resolution from in-memory
   * buffers and spilled disk files. Returns [[StreamingShuffleBlockResolver]]
   * instead of `IndexShuffleBlockResolver`.
   *
   * Follows the same pattern as `SortShuffleManager.shuffleBlockResolver`.
   */
  override val shuffleBlockResolver: StreamingShuffleBlockResolver =
    new StreamingShuffleBlockResolver(conf)

  /**
   * Lazily-initialized fallback [[ShuffleManager]] (SortShuffleManager) for handling
   * [[BaseShuffleHandle]] instances when streaming shuffle is disabled or falls back.
   *
   * Created via reflection to avoid any direct import dependency on the
   * `org.apache.spark.shuffle.sort` package, maintaining zero cross-contamination
   * as required by the architectural isolation constraint.
   *
   * The fallback manager reuses the same SparkConf and isDriver flag, ensuring
   * consistent configuration between the streaming and sort-based code paths.
   */
  @volatile private var fallbackSortInitialized: Boolean = false

  private lazy val fallbackSortManager: ShuffleManager = {
    val mgr = Utils.instantiateSerializerOrShuffleManager[ShuffleManager](
      "org.apache.spark.shuffle.sort.SortShuffleManager", conf, isDriver)
    fallbackSortInitialized = true
    // Wire the fallback manager's block resolver into the streaming resolver so
    // that blocks written by the sort-based path can be resolved when
    // BlockManager calls our shuffleBlockResolver.getBlockData().
    shuffleBlockResolver.setFallbackResolver(mgr.shuffleBlockResolver)
    mgr
  }

  // ===========================================================================
  // ShuffleManager Trait Implementation
  // ===========================================================================

  /**
   * Registers a shuffle and returns a [[ShuffleHandle]] for task dispatch.
   *
   * When streaming shuffle is enabled and no fallback condition is active, returns a
   * [[StreamingShuffleHandle]] populated with buffer sizing configuration from
   * [[StreamingShuffleConfig]]. When streaming is disabled or fallback is active,
   * returns a [[BaseShuffleHandle]] for sort-based shuffle handling.
   *
   * The returned handle is serialized to executors and used by [[getWriter]] and
   * [[getReader]] for type-based dispatch to the appropriate writer/reader.
   *
   * @param shuffleId unique identifier for this shuffle, assigned by the DAG scheduler
   * @param dependency the shuffle dependency containing partitioner, serializer, and
   *                   aggregator configuration
   * @return StreamingShuffleHandle when streaming is active, BaseShuffleHandle otherwise
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (StreamingShuffleConfig.isStreamingEnabled(conf) && !shouldFallback()) {
      val bufferPercent = StreamingShuffleConfig.getBufferSizePercent(conf)
      val spillThreshold = StreamingShuffleConfig.getSpillThreshold(conf)
      val partitionCount = dependency.partitioner.numPartitions
      if (StreamingShuffleConfig.isDebugEnabled(conf)) {
        logInfo(s"Registering streaming shuffle $shuffleId with $partitionCount partitions, " +
          s"buffer=$bufferPercent%, spillThreshold=$spillThreshold%")
      }
      new StreamingShuffleHandle[K, V, C](
        shuffleId, dependency, bufferPercent, spillThreshold, partitionCount)
    } else {
      // Fallback: create BaseShuffleHandle for sort-based path.
      // This branch is taken when streaming is disabled (the default) or when
      // runtime fallback conditions have been detected.
      if (fallbackActivated.get() && StreamingShuffleConfig.isDebugEnabled(conf)) {
        logInfo(s"Registering shuffle $shuffleId with BaseShuffleHandle " +
          "(streaming fallback activated due to runtime degradation)")
      } else if (!fallbackActivated.get()) {
        logDebug(s"Registering shuffle $shuffleId with BaseShuffleHandle " +
          "(streaming shuffle disabled)")
      }
      new BaseShuffleHandle(shuffleId, dependency)
    }
  }

  /**
   * Creates a [[ShuffleWriter]] for a given partition on an executor.
   *
   * Tracks the map task ID in [[taskIdMapsForShuffle]] for cleanup during
   * [[unregisterShuffle]], then pattern-matches on the handle type to construct
   * the appropriate writer. For [[StreamingShuffleHandle]], creates a
   * [[StreamingShuffleWriter]] that buffers records in per-partition memory regions.
   *
   * @param handle the shuffle handle returned by [[registerShuffle]]
   * @param mapId unique identifier for this map task
   * @param context task context for metrics and lifecycle management
   * @param metrics write metrics reporter for standard and streaming-specific counters
   * @return StreamingShuffleWriter for streaming handles
   * @throws SparkException if an unexpected handle type is received
   */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    // Record this map task ID for cleanup during unregisterShuffle.
    // Same pattern as SortShuffleManager.getWriter (lines 150-152).
    val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
      handle.shuffleId, _ => new OpenHashSet[Long](16))
    mapTaskIds.synchronized { mapTaskIds.add(mapId) }

    handle match {
      case streamingHandle: StreamingShuffleHandle[K @unchecked, V @unchecked, _] =>
        new StreamingShuffleWriter[K, V](
          streamingHandle,
          mapId,
          context,
          metrics,
          conf)
      case base: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        // Fallback path: delegate to the sort-based SortShuffleManager for
        // BaseShuffleHandle instances. This occurs when streaming shuffle is
        // disabled or when runtime fallback conditions have been detected.
        logDebug(s"Delegating getWriter for shuffle ${handle.shuffleId} to " +
          "fallback SortShuffleManager (BaseShuffleHandle)")
        fallbackSortManager.getWriter(base, mapId, context, metrics)
      case other =>
        throw new SparkException(
          s"StreamingShuffleManager.getWriter received unexpected ShuffleHandle type: " +
          s"${other.getClass.getName}. Expected StreamingShuffleHandle or " +
          s"BaseShuffleHandle for shuffle ${handle.shuffleId}.")
    }
  }

  /**
   * Creates a [[ShuffleReader]] for a range of reduce partitions from a range of
   * map outputs.
   *
   * Queries [[MapOutputTracker.getMapSizesByExecutorId]] for shuffle block locations
   * (same pattern as `SortShuffleManager.getReader`) and constructs a
   * [[StreamingShuffleReader]] that polls producers for available blocks before
   * shuffle completion. Streaming shuffle does not support push-based shuffle merging.
   *
   * Note: The 5-parameter `getReader` overload is `final` in the [[ShuffleManager]]
   * trait and delegates to this 7-parameter version. We only override this version.
   *
   * @param handle the shuffle handle for this read operation
   * @param startMapIndex start of the map output range (inclusive)
   * @param endMapIndex end of the map output range (exclusive); Int.MaxValue for all
   * @param startPartition start of the reduce partition range (inclusive)
   * @param endPartition end of the reduce partition range (exclusive)
   * @param context task context for metrics and lifecycle management
   * @param metrics read metrics reporter for standard and streaming-specific counters
   * @return StreamingShuffleReader for streaming shuffle data consumption
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
        // Query MapOutputTracker for shuffle block locations.
        // Same pattern as SortShuffleManager.getReader (lines 134-136).
        // Streaming shuffle does NOT use getPushBasedShuffleMapSizesByExecutorId
        // since it operates on a completely separate data path from push-based shuffle.
        val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
          handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
        new StreamingShuffleReader[K, C](
          streamingHandle,
          blocksByAddress,
          context,
          metrics,
          conf)
      case _ =>
        // Fallback path: delegate to the sort-based SortShuffleManager for
        // BaseShuffleHandle instances (when streaming is disabled or fell back).
        logDebug(s"Delegating getReader for shuffle ${handle.shuffleId} to " +
          "fallback SortShuffleManager")
        fallbackSortManager.getReader(
          handle, startMapIndex, endMapIndex, startPartition, endPartition,
          context, metrics)
    }
  }

  /**
   * Removes a shuffle's metadata and cached data from this manager.
   *
   * Iterates through all map task IDs registered for the given shuffle and
   * delegates per-map-task cleanup to [[StreamingShuffleBlockResolver.removeDataByMap]],
   * which removes both in-memory cached blocks and tracking state.
   *
   * Same pattern as `SortShuffleManager.unregisterShuffle` (lines 179-188).
   *
   * @param shuffleId the shuffle to unregister
   * @return true after cleanup completes (always succeeds)
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(taskIdMapsForShuffle.remove(shuffleId)).foreach { mapTaskIds =>
      mapTaskIds.synchronized {
        val iter = mapTaskIds.iterator
        while (iter.hasNext) {
          val mapTaskId = iter.next()
          // Delegate per-map-task block cleanup to the streaming block resolver.
          // This removes both in-memory cached blocks and shuffle-map tracking entries.
          shuffleBlockResolver.removeDataByMap(shuffleId, mapTaskId)
        }
      }
    }
    // Also delegate to the fallback sort manager if it has been initialized,
    // to clean up any sort-based shuffle state for BaseShuffleHandle shuffles.
    // The fallbackSortInitialized flag avoids triggering lazy initialization
    // of fallbackSortManager when it was never used.
    if (fallbackSortInitialized) {
      fallbackSortManager.unregisterShuffle(shuffleId)
    }
    logDebug(s"Unregistered streaming shuffle $shuffleId")
    true
  }

  /**
   * Shuts down this shuffle manager, releasing all held resources.
   *
   * Delegates to [[StreamingShuffleBlockResolver.stop]] to clear the in-memory
   * block cache and tracking state. Same pattern as `SortShuffleManager.stop()`.
   */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
    logInfo("StreamingShuffleManager stopped")
  }

  // ===========================================================================
  // Fallback Condition Detection
  // ===========================================================================

  /**
   * Checks whether runtime conditions warrant fallback from streaming to
   * sort-based shuffle.
   *
   * Evaluates four fallback conditions per the AAP specification:
   *  1. '''Memory pressure''': Buffer allocation would risk OOM
   *  2. '''Network saturation''': Link capacity exceeds 90% threshold
   *  3. '''Consumer slowdown''': Consumer sustained 2x slower than producer
   *     for more than 60 seconds continuously
   *  4. '''Already activated''': A previous check triggered fallback
   *
   * This method is called by [[StreamingShuffleWriter]] before each write batch
   * to determine whether to continue streaming or fall back to sort-based behavior.
   * Once fallback is activated, it remains active for the executor's lifetime
   * (per AAP v1 no-dynamic-reconfiguration constraint).
   *
   * Thread safety: All state reads use atomic operations. The fallback flag is
   * a one-way transition from false to true, preventing TOCTOU races.
   *
   * @return true if fallback to sort-based shuffle should be activated
   */
  private[streaming] def shouldFallback(): Boolean = {
    // Fast path: if fallback is already activated, return immediately
    if (fallbackActivated.get()) {
      return true
    }

    // Condition 1: Memory pressure prevents buffer allocation (OOM risk)
    if (memoryPressureDetected.get()) {
      logError("Streaming shuffle fallback triggered: memory pressure prevents " +
        "buffer allocation. Subsequent shuffle registrations will use sort-based shuffle.")
      fallbackActivated.set(true)
      streamingMetricsSource.foreach(_.incrementBackpressureEvents())
      return true
    }

    // Condition 2: Network saturation exceeds 90% link capacity
    val networkRatio = java.lang.Double.longBitsToDouble(currentNetworkSaturation.get())
    if (networkRatio > StreamingShuffleManager.FALLBACK_NETWORK_SATURATION_THRESHOLD) {
      logError(s"Streaming shuffle fallback triggered: network saturation " +
        f"${networkRatio * 100}%.1f%% exceeds " +
        f"${StreamingShuffleManager.FALLBACK_NETWORK_SATURATION_THRESHOLD * 100}%.0f%% " +
        "threshold. Subsequent shuffle registrations will use sort-based shuffle.")
      fallbackActivated.set(true)
      streamingMetricsSource.foreach(_.incrementBackpressureEvents())
      return true
    }

    // Condition 3: Consumer sustained 2x slower than producer for >60 seconds
    val slowdownRatio = java.lang.Double.longBitsToDouble(
      currentConsumerSlowdownRatio.get())
    if (slowdownRatio >= StreamingShuffleManager.FALLBACK_CONSUMER_SLOWDOWN_THRESHOLD) {
      val elapsedNanos = System.nanoTime() - consumerSlowdownStartNanos
      val elapsedSeconds = elapsedNanos / 1000000000L
      if (elapsedSeconds > StreamingShuffleManager.FALLBACK_CONSUMER_SLOWDOWN_DURATION_S) {
        logError(s"Streaming shuffle fallback triggered: consumer " +
          f"${slowdownRatio}%.1fx slower than producer for ${elapsedSeconds}s " +
          s"(threshold: ${StreamingShuffleManager.FALLBACK_CONSUMER_SLOWDOWN_THRESHOLD}x " +
          s"for >${StreamingShuffleManager.FALLBACK_CONSUMER_SLOWDOWN_DURATION_S}s). " +
          "Subsequent shuffle registrations will use sort-based shuffle.")
        fallbackActivated.set(true)
        streamingMetricsSource.foreach(_.incrementBackpressureEvents())
        return true
      }
    } else {
      // Consumer speed is acceptable -- reset the slowdown timer
      consumerSlowdownStartNanos = System.nanoTime()
    }

    false
  }

  // ===========================================================================
  // Runtime Condition Reporting -- Called by Streaming Components
  // ===========================================================================

  /**
   * Reports the current consumer-to-producer speed ratio for fallback evaluation.
   *
   * Called by [[BackpressureProtocol]] or [[StreamingShuffleWriter]] when monitoring
   * consumer acknowledgment rates. A ratio >= 2.0 indicates the consumer is at least
   * 2x slower than the producer.
   *
   * When the ratio first crosses the 2.0 threshold, the slowdown timer starts.
   * If it remains above threshold for 60 continuous seconds, [[shouldFallback]]
   * returns true.
   *
   * @param ratio consumer-to-producer speed ratio (>= 0.0)
   */
  private[streaming] def reportConsumerSlowdown(ratio: Double): Unit = {
    val prevRatio = java.lang.Double.longBitsToDouble(currentConsumerSlowdownRatio.get())
    currentConsumerSlowdownRatio.set(java.lang.Double.doubleToLongBits(ratio))
    // Start the slowdown timer when ratio first exceeds the threshold
    if (ratio >= StreamingShuffleManager.FALLBACK_CONSUMER_SLOWDOWN_THRESHOLD &&
        prevRatio < StreamingShuffleManager.FALLBACK_CONSUMER_SLOWDOWN_THRESHOLD) {
      consumerSlowdownStartNanos = System.nanoTime()
      if (StreamingShuffleConfig.isDebugEnabled(conf)) {
        logInfo(s"Consumer slowdown detected: ratio=${ratio}x, starting 60s timer")
      }
    }
  }

  /**
   * Reports the current network saturation ratio for fallback evaluation.
   *
   * Called by [[BackpressureProtocol]] based on bandwidth utilization monitoring.
   * A value > 0.9 indicates network saturation exceeding the 90% threshold.
   *
   * @param ratio network utilization ratio (0.0 to 1.0)
   */
  private[streaming] def reportNetworkSaturation(ratio: Double): Unit = {
    currentNetworkSaturation.set(java.lang.Double.doubleToLongBits(ratio))
  }

  /**
   * Reports whether memory pressure has been detected that prevents buffer allocation.
   *
   * Called by [[MemorySpillManager]] when the execution memory pool is exhausted
   * and further buffer allocation would risk OOM.
   *
   * @param underPressure true if memory pressure is detected, false when resolved
   */
  private[streaming] def reportMemoryPressure(underPressure: Boolean): Unit = {
    memoryPressureDetected.set(underPressure)
  }

  // ===========================================================================
  // Metrics Source Management
  // ===========================================================================

  /**
   * Returns the current streaming shuffle metrics source, if initialized.
   *
   * This accessor is used by the Executor during metrics registration and by
   * streaming components that need to report telemetry. Returns `None` if
   * [[initMetricsSource]] has not been called yet.
   *
   * @return Optional StreamingShuffleMetricsSource instance
   */
  private[streaming] def getMetricsSource: Option[StreamingShuffleMetricsSource] =
    streamingMetricsSource

  /**
   * Lazily initializes and returns the streaming shuffle metrics source.
   *
   * Uses double-checked locking to ensure thread-safe singleton initialization:
   * the `@volatile` field guarantees visibility of the `Some(source)` assignment,
   * while the `synchronized` block prevents duplicate creation under contention.
   *
   * After creation, the source is available via [[getMetricsSource]] for
   * registration with the Spark [[org.apache.spark.metrics.MetricsSystem MetricsSystem]]
   * by the Executor during initialization.
   *
   * @return the StreamingShuffleMetricsSource singleton for this manager
   */
  private[streaming] def initMetricsSource(): StreamingShuffleMetricsSource = {
    streamingMetricsSource match {
      case Some(source) => source
      case None =>
        synchronized {
          streamingMetricsSource match {
            case Some(source) => source
            case None =>
              val source = new StreamingShuffleMetricsSource()
              streamingMetricsSource = Some(source)
              logInfo("Initialized StreamingShuffleMetricsSource for streaming shuffle telemetry")
              source
          }
        }
    }
  }

  // ===========================================================================
  // Metrics Delegation -- Facade for Streaming Components
  // ===========================================================================

  /**
   * Updates the buffer utilization percentage in the metrics source.
   *
   * Convenience method for [[MemorySpillManager]] to report buffer occupancy
   * without needing a direct reference to the metrics source.
   *
   * @param percent current buffer utilization as a percentage (0-100)
   */
  private[streaming] def updateBufferUtilization(percent: Long): Unit = {
    streamingMetricsSource.foreach(_.updateBufferUtilizationPercent(percent))
  }

  /**
   * Records a disk spill event in the metrics source.
   *
   * Convenience method for [[MemorySpillManager]] to report spill events
   * without needing a direct reference to the metrics source.
   */
  private[streaming] def reportSpillEvent(): Unit = {
    streamingMetricsSource.foreach(_.incrementSpillCount())
  }

  /**
   * Records a backpressure event in the metrics source.
   *
   * Convenience method for [[BackpressureProtocol]] to report rate limiting
   * events without needing a direct reference to the metrics source.
   */
  private[streaming] def reportBackpressureEvent(): Unit = {
    streamingMetricsSource.foreach(_.incrementBackpressureEvents())
  }

  /**
   * Records a partial read invalidation event in the metrics source.
   *
   * Convenience method for [[StreamingShuffleReader]] to report producer failure
   * detections without needing a direct reference to the metrics source.
   * This increments the JMX gauge `shuffle.streaming.partialReadInvalidations`
   * for external monitoring integration (Prometheus, Grafana).
   */
  private[streaming] def reportPartialReadInvalidation(): Unit = {
    streamingMetricsSource.foreach(_.incrementPartialReadInvalidations())
  }
}


/**
 * Companion object for [[StreamingShuffleManager]] containing streaming shuffle
 * protocol constants derived from the AAP specification.
 *
 * These constants define the streaming shuffle's wire protocol parameters,
 * timeout thresholds, and fallback condition triggers. They are used by
 * multiple streaming components ([[StreamingShuffleWriter]], [[StreamingShuffleReader]],
 * [[BackpressureProtocol]]) for consistent protocol behavior.
 *
 * All values are immutable and thread-safe for concurrent access.
 */
private[spark] object StreamingShuffleManager {

  /**
   * Maximum block size for streaming shuffle data pipelining.
   * 2MB blocks optimize TCP throughput while maintaining fine-grained
   * backpressure granularity per the AAP network transfer constraints.
   */
  val STREAMING_SHUFFLE_BLOCK_SIZE: Long = 2L * 1024L * 1024L

  /**
   * Consumer liveness heartbeat timeout in milliseconds.
   * When a consumer fails to send heartbeat acknowledgments within this window,
   * the producer considers the consumer failed and buffers unacknowledged data
   * for potential retransmission or spill.
   */
  val CONSUMER_HEARTBEAT_TIMEOUT_MS: Long = 10000L

  /**
   * Producer connection timeout in milliseconds for failure detection.
   * When a consumer cannot establish or maintain a connection to a producer
   * within this window, the producer is considered failed and a
   * [[FetchFailedException]] is thrown to trigger DAG recomputation.
   */
  val PRODUCER_CONNECTION_TIMEOUT_MS: Long = 5000L

  /**
   * Consumer-to-producer speed ratio threshold for fallback activation.
   * When the consumer is consistently this many times slower than the producer
   * for the duration specified by [[FALLBACK_CONSUMER_SLOWDOWN_DURATION_S]],
   * streaming shuffle falls back to sort-based shuffle.
   */
  val FALLBACK_CONSUMER_SLOWDOWN_THRESHOLD: Double = 2.0

  /**
   * Network saturation ratio threshold for fallback activation.
   * When network link capacity utilization exceeds this ratio (0.9 = 90%),
   * streaming shuffle falls back to sort-based shuffle to avoid
   * competing with other cluster traffic.
   */
  val FALLBACK_NETWORK_SATURATION_THRESHOLD: Double = 0.9

  /**
   * Duration in seconds that consumer slowdown must be sustained before
   * triggering fallback. The consumer must be at least
   * [[FALLBACK_CONSUMER_SLOWDOWN_THRESHOLD]]x slower than the producer
   * for this continuous duration to activate fallback.
   */
  private[streaming] val FALLBACK_CONSUMER_SLOWDOWN_DURATION_S: Long = 60L
}
