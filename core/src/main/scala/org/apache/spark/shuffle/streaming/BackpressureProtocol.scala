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

import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.ThreadUtils

/**
 * Flow-control and rate-limiting protocol for the opt-in streaming shuffle engine. A single
 * instance is created per executor by `StreamingShuffleManager` (only when the streaming path is
 * actually active for a shuffle) and is shared by the streaming writer pipeline. It provides the
 * four capabilities required by the streaming design:
 *
 *  1. Consumer-to-producer heartbeat flow control on a 10s interval with a 5s connect timeout. A
 *     missing or slow consumer pauses the producer. The [[shouldFallback]] signal -- which the
 *     manager reads (through the writer) to degrade a shuffle gracefully to sort -- is raised on
 *     ANY of three sustained conditions: a consumer kept 2x slower than the producer for over 60s,
 *     token-bucket utilization sustained above 90% of the configured link capacity (network
 *     saturation), or an external version-mismatch probe (see [[setFallbackProbe]]).
 *  2. A token-bucket rate limiter whose refill rate is `maxBandwidthMBps / numConcurrentShuffles`.
 *     Short bursts are absorbed up to the bucket capacity while the sustained long-term rate is
 *     enforced. A `maxBandwidthMBps` of 0 is the "unlimited" sentinel and disables limiting.
 *  3. QoS priority arbitration via [[comparePriority]], consulted on the admission path
 *     ([[tryAcquire]]): real shuffle traffic always outranks speculative-task traffic, and among
 *     concurrent shuffles the one with more partitions (then more buffered data) wins contended
 *     bandwidth. Under token scarcity an outranked shuffle DEFERS so the higher-priority shuffle's
 *     traffic is admitted first.
 *  4. Telemetry: every throttle event increments the `backpressureEvents` counter on the shared
 *     [[StreamingShuffleSource]], exposed over JMX through the existing `MetricRegistry`.
 *
 * Transport reuse: all control messages (heartbeats and acks) are carried over the EXISTING
 * `org.apache.spark.network.TransportContext` used by the rest of the shuffle stack. This class
 * introduces no new transport stack or dependency; it only owns the flow-control policy and the
 * accounting that the writer and reader apply on top of that existing transport.
 *
 * Coexistence strategy: this protocol is instantiated and active ONLY on the streaming path. The
 * default `SortShuffleManager` fallback never constructs it, so the sort-based shuffle is wholly
 * unaffected and carries zero backpressure overhead. Being `private[spark]` and confined to the
 * streaming package enforces the zero-cross-contamination rule.
 *
 * Thread-safety: token accounting is guarded by a short `synchronized` section on `this`; the
 * heartbeat sampling runs on a dedicated daemon scheduler and touches only atomics and volatiles,
 * so it never contends with the hot-path `acquire` calls made from the writer pipeline.
 *
 * @param conf the active [[SparkConf]] supplying the `spark.shuffle.streaming.*` settings
 * @param numConcurrentShuffles by-name count of concurrently active streaming shuffles on this
 *                              executor, re-evaluated on every refill so the per-shuffle share of
 *                              the bandwidth budget tracks the live degree of concurrency
 * @param metricsSource the shared JMX metrics source whose `backpressureEvents` counter is
 *                       incremented on every throttle event
 */
private[spark] class BackpressureProtocol(
    conf: SparkConf,
    numConcurrentShuffles: => Int,
    metricsSource: StreamingShuffleSource)
  extends Logging {

  import BackpressureProtocol._

  // Per-executor bandwidth budget in MB/s for the token bucket. 0 is the "unlimited" sentinel
  // (read from spark.shuffle.streaming.maxBandwidthMBps) and disables rate limiting entirely.
  private val maxBandwidthMBps: Int = conf.get(config.STREAMING_SHUFFLE_MAX_BANDWIDTH_MBPS)

  // Verbose debug-logging gate (spark.shuffle.streaming.debug). Kept off the hot path and used to
  // hold per-executor log volume under the mandated 10MB/hour budget.
  private val debug: Boolean = conf.get(config.STREAMING_SHUFFLE_DEBUG)

  // True when rate limiting is disabled (unlimited sentinel or a non-positive budget). When set,
  // every acquire call is granted immediately with no token accounting.
  private val rateLimitingDisabled: Boolean = maxBandwidthMBps <= 0

  // --- Token-bucket state ---------------------------------------------------------------------
  // Currently available tokens, measured in BYTES (1 token == 1 byte) so callers can request the
  // exact size of a pipelined block. Refilled lazily from wall-clock elapsed time.
  private val availableTokens = new AtomicLong(0L)
  // Wall-clock nanos of the last refill, used to compute how many tokens to add on the next call.
  private val lastRefillNanos = new AtomicLong(System.nanoTime())
  // Cumulative bytes granted by the limiter, exposed for diagnostics and telemetry.
  private val bytesTransmitted = new AtomicLong(0L)

  // --- Heartbeat and rate-tracking state ------------------------------------------------------
  // Cumulative bytes the producer has offered to transmit and the consumer has acknowledged.
  private val producerBytes = new AtomicLong(0L)
  private val consumerBytes = new AtomicLong(0L)
  // Snapshots taken on the previous heartbeat tick to derive per-interval throughput rates.
  private val lastSampledProducerBytes = new AtomicLong(0L)
  private val lastSampledConsumerBytes = new AtomicLong(0L)
  // Snapshot of cumulative limiter-admitted bytes (bytesTransmitted) on the previous heartbeat
  // tick, used to derive the per-interval transmitted rate for network-saturation detection.
  private val lastSampledTransmittedBytes = new AtomicLong(0L)
  private val lastSampleNanos = new AtomicLong(System.nanoTime())
  // Last time a consumer heartbeat (or consumed-byte progress) was observed.
  private val lastConsumerHeartbeatNanos = new AtomicLong(System.nanoTime())

  // Set once the consumer has been sustained 2x slower than the producer for longer than the
  // fallback window; read by StreamingShuffleManager to degrade gracefully to sort-based shuffle.
  @volatile private var shouldFallbackFlag: Boolean = false
  // Set when the consumer heartbeat has been missing past the connection deadline; signals the
  // producer pipeline to throttle or pause until the consumer recovers.
  @volatile private var producerPaused: Boolean = false
  // Wall-clock nanos since which the consumer has been continuously "too slow"; -1 when keeping
  // up. Mutated only by the single heartbeat thread.
  @volatile private var slowSinceNanos: Long = -1L
  // Wall-clock nanos since which token-bucket utilization has been continuously above the
  // network-saturation ratio; -1 when not saturated. Mutated only by the heartbeat thread. Once
  // the saturated window exceeds NETWORK_SATURATION_SUSTAINED_SECONDS the fallback flag is set
  // (AAP fallback condition (c): network saturation > 90% of the configured link capacity).
  @volatile private var saturatedSinceNanos: Long = -1L
  // Guards stop() against double shutdown.
  @volatile private var stopped: Boolean = false

  // External, best-effort fallback probe invoked on each heartbeat tick while NOT already in
  // fallback. Wired by StreamingEngine to the cross-executor version handshake
  // (StreamingShuffleRendezvous.detectVersionMismatch): a true result flips [[shouldFallback]] so a
  // producer/consumer protocol-version mismatch (AAP fallback condition (d)) degrades the shuffle
  // to sort. Defaults to a no-op so bare unit tests and local mode never probe. @volatile so the
  // heartbeat thread observes the engine's installed value without extra locking.
  @volatile private var fallbackProbe: () => Boolean = () => false

  // QoS priority registry (AAP: real shuffle traffic must outrank speculative-task traffic). Maps
  // an active shuffleId to its [[BackpressureProtocol.ShufflePriority]] descriptor and is consulted
  // on the token-bucket admission path so that, under token scarcity, a lower-priority shuffle
  // DEFERS to a higher-priority one. A ConcurrentHashMap because writers on multiple task threads
  // register/update concurrently while the hot-path acquire calls read it under `this`.
  private val shufflePriorities = new ConcurrentHashMap[Integer, ShufflePriority]()

  // Dedicated daemon scheduler running the consumer-to-producer heartbeat and rate-sampling loop.
  // A daemon single thread keeps telemetry overhead well under the 1% CPU budget and never blocks
  // executor shutdown.
  private val heartbeatExecutor: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("streaming-shuffle-backpressure")

  // The periodic heartbeat task, defined as an explicit Runnable (Spark convention). Failures are
  // swallowed inside heartbeatTick() so a transient error can never tear down the schedule.
  private val heartbeatRunnable: Runnable = new Runnable {
    override def run(): Unit = heartbeatTick()
  }

  // Schedule the heartbeat at the fixed 10s interval; the handle is cancelled in stop().
  private val heartbeatTask: ScheduledFuture[_] = heartbeatExecutor.scheduleAtFixedRate(
    heartbeatRunnable,
    HEARTBEAT_INTERVAL_SECONDS,
    HEARTBEAT_INTERVAL_SECONDS,
    TimeUnit.SECONDS)

  if (debug) {
    logDebug(s"BackpressureProtocol started: maxBandwidthMBps=$maxBandwidthMBps" +
      s" rateLimitingDisabled=$rateLimitingDisabled")
  }

  // ============================================================================================
  // Token-bucket rate limiter
  // ============================================================================================

  // Live count of concurrently active streaming shuffles, floored at 1 to avoid a divide-by-zero
  // when the manager reports no registered shuffles yet.
  private def concurrency: Int = math.max(1, numConcurrentShuffles)

  // Per-shuffle refill rate in BYTES/second = (maxBandwidthMBps / concurrentShuffles) * 1MB.
  private def refillBytesPerSecond: Double =
    (maxBandwidthMBps.toDouble / concurrency) * BYTES_PER_MB

  // Bucket capacity in bytes: at least one second of refill, but never below the maximum pipelined
  // block size so a single full-size block can always eventually be admitted (no starvation).
  private def capacityBytes: Long =
    math.max(refillBytesPerSecond.toLong, MAX_PIPELINED_BLOCK_BYTES)

  // Lazily add tokens for the wall-clock time elapsed since the last refill, capped at capacity.
  // MUST be called while holding this object's monitor.
  private def refillTokens(): Unit = {
    val now = System.nanoTime()
    val elapsed = now - lastRefillNanos.get()
    if (elapsed > 0L) {
      val added = (elapsed.toDouble / NANOS_PER_SECOND) * refillBytesPerSecond
      if (added >= 1.0) {
        val updated = math.min(capacityBytes, availableTokens.get() + added.toLong)
        availableTokens.set(updated)
        lastRefillNanos.set(now)
      }
    }
  }

  /**
   * Low-level, non-blocking token acquisition with no QoS arbitration. Equivalent to
   * [[tryAcquire(shuffleId:Int,bytes:Long)*]] for an unregistered caller, so it never defers. Kept
   * for callers (and tests) that do not carry a shuffle identity.
   *
   * @param bytes the number of bytes the caller wishes to transmit
   * @return true if the bytes were admitted (tokens deducted), false if currently throttled
   */
  def tryAcquire(bytes: Long): Boolean = tryAcquire(NO_QOS_SHUFFLE_ID, bytes)

  /**
   * Low-level, non-blocking token acquisition for a specific shuffle. Refills the bucket from
   * elapsed time and, if enough tokens are available AND QoS does not require this shuffle to defer
   * to a higher-priority one (see [[qosShouldDefer]]), deducts `bytes` and returns true. It is the
   * pure mechanism with no telemetry side effects, so callers can cheaply probe availability.
   *
   * @param shuffleId the shuffle requesting bandwidth; [[NO_QOS_SHUFFLE_ID]] opts out of QoS
   * @param bytes the number of bytes the caller wishes to transmit
   * @return true if the bytes were admitted (tokens deducted), false if throttled or QoS-deferred
   */
  def tryAcquire(shuffleId: Int, bytes: Long): Boolean = {
    if (rateLimitingDisabled || bytes <= 0L) {
      true
    } else {
      synchronized {
        refillTokens()
        val available = availableTokens.get()
        // Admit only when tokens suffice AND QoS does not require this (outranked) shuffle to defer
        // to a higher-priority one under scarcity. comparePriority is thereby consulted on the real
        // admission path, not just in isolation.
        if (available >= bytes && !qosShouldDefer(shuffleId, available)) {
          availableTokens.addAndGet(-bytes)
          bytesTransmitted.addAndGet(bytes)
          true
        } else {
          false
        }
      }
    }
  }

  /**
   * Canonical non-blocking acquisition used by the writer pipeline. Delegates to [[tryAcquire]]
   * and, when the request is throttled, records a backpressure telemetry event before returning
   * false so the caller can buffer, spill, or back off.
   *
   * @param bytes the number of bytes the caller wishes to transmit
   * @return true if admitted, false if throttled (a backpressure event was recorded)
   */
  def acquire(bytes: Long): Boolean = {
    // Enforce the consumer-driven pause signal directly on the acquisition path: when the consumer
    // heartbeat has gone missing past the connection deadline the producer MUST NOT emit, so a
    // non-blocking acquire is refused (and recorded) regardless of token availability. This is the
    // enforcement point the writer relies on so a missing heartbeat actually pauses writes, rather
    // than merely exposing `producerShouldPause` as an unconsumed advisory signal.
    if (producerPaused) {
      recordBackpressureEvent("producer paused: consumer heartbeat missing; refusing emission")
      false
    } else {
      val granted = tryAcquire(bytes)
      if (!granted) {
        recordBackpressureEvent(s"rate limiter throttled request for $bytes bytes")
      }
      granted
    }
  }

  /**
   * Blocking acquisition: waits until `bytes` tokens are available (or the unlimited sentinel makes
   * limiting a no-op), sleeping for an estimated refill delay between attempts. Records a single
   * backpressure event the first time it has to wait. Honors interruption by restoring the
   * thread's interrupt status and returning false so the caller can abort cleanly.
   *
   * @param bytes the number of bytes the caller wishes to transmit
   * @return true once admitted, false if the waiting thread was interrupted
   */
  def acquireBlocking(bytes: Long): Boolean = acquireBlocking(NO_QOS_SHUFFLE_ID, bytes)

  /**
   * Blocking acquisition for a specific shuffle, threading its identity through to the token-bucket
   * admission so QoS arbitration ([[qosShouldDefer]]) applies while waiting: under scarcity an
   * outranked shuffle keeps deferring until the bucket refills past the QoS reserve (i.e. once the
   * higher-priority traffic is satisfied). Honors the same interruption, producer-pause, and
   * fallback semantics as [[acquireBlocking(bytes:Long)*]].
   *
   * @param shuffleId the shuffle requesting bandwidth; [[NO_QOS_SHUFFLE_ID]] opts out of QoS
   * @param bytes the number of bytes the caller wishes to transmit
   * @return true once admitted, false if the waiting thread was interrupted
   */
  def acquireBlocking(shuffleId: Int, bytes: Long): Boolean = {
    // Fast path: with rate limiting disabled there is no token accounting to wait on. We take the
    // fast path ONLY when the producer is neither paused (missing consumer) nor in fallback, so
    // those states are honored even for the "unlimited" bandwidth configuration.
    if (rateLimitingDisabled && !producerPaused && !shouldFallbackFlag && bytes > 0L) {
      return true
    }
    if (bytes <= 0L) {
      return true
    }
    var throttled = false
    var pauseRecorded = false
    var result = true
    var waiting = true
    // Bound the time the producer will block on a missing-consumer pause; a permanently absent
    // consumer is reported as a failure (false) rather than blocking the map task forever.
    val pauseDeadlineNanos = System.nanoTime() + PAUSE_MAX_WAIT_NANOS
    while (waiting) {
      if (Thread.interrupted()) {
        // Interruption aborts pacing cleanly: restore the interrupt flag and signal the caller to
        // abort the block. The writer treats this as a write failure, never a successful emission.
        Thread.currentThread().interrupt()
        waiting = false
        result = false
      } else if (shouldFallbackFlag) {
        // A sustained-slowdown fallback has fired: stop pacing and let the caller degrade the
        // shuffle gracefully (the writer throws so the manager can fall back to sort).
        waiting = false
        result = false
      } else if (producerPaused) {
        // Consumer heartbeat missing: pause emission until the consumer recovers, bounded by the
        // pause deadline. This is the blocking-path enforcement of `producerShouldPause`.
        if (!pauseRecorded) {
          recordBackpressureEvent("producer paused: consumer heartbeat missing; waiting to resume")
          pauseRecorded = true
        }
        if (System.nanoTime() >= pauseDeadlineNanos) {
          waiting = false
          result = false
        } else {
          try {
            Thread.sleep(PAUSE_POLL_MILLIS)
          } catch {
            case _: InterruptedException =>
              Thread.currentThread().interrupt()
              waiting = false
              result = false
          }
        }
      } else if (rateLimitingDisabled || tryAcquire(shuffleId, bytes)) {
        // Either limiting is off (we reached here only because of a transient pause that has now
        // cleared) or enough tokens were available AND QoS did not require this shuffle to defer:
        // the block is admitted.
        waiting = false
      } else {
        if (!throttled) {
          recordBackpressureEvent(s"blocking acquire waiting for $bytes bytes")
          throttled = true
        }
        try {
          Thread.sleep(estimatedWaitMillis(bytes))
        } catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
            waiting = false
            result = false
        }
      }
    }
    result
  }

  // Estimate how long to wait for the current token deficit to refill, bounded to a small cap so we
  // re-check liveness (and a possibly changed concurrency level) frequently.
  private def estimatedWaitMillis(bytes: Long): Long = {
    val deficit = synchronized { math.max(0L, bytes - availableTokens.get()) }
    val perSecond = refillBytesPerSecond
    if (perSecond <= 0.0) {
      MAX_WAIT_MILLIS
    } else {
      val millis = (deficit.toDouble / perSecond * MILLIS_PER_SECOND).toLong
      math.min(MAX_WAIT_MILLIS, math.max(MIN_WAIT_MILLIS, millis))
    }
  }

  /** Cumulative bytes admitted by the rate limiter since construction (diagnostics/telemetry). */
  def bytesTransmittedTotal: Long = bytesTransmitted.get()

  // ============================================================================================
  // Heartbeat flow control and fallback detection
  // ============================================================================================

  /**
   * Records that the producer has offered `bytes` for transmission. Fed by the writer pipeline so
   * the heartbeat loop can compare producer throughput against consumer throughput.
   *
   * @param bytes the number of bytes the producer has just made available downstream
   */
  def recordProducerProgress(bytes: Long): Unit = {
    if (bytes > 0L) {
      producerBytes.addAndGet(bytes)
    }
  }

  /**
   * Records that the consumer has acknowledged `bytes`. Consuming data is also treated as a
   * liveness signal, so this refreshes the consumer-heartbeat timestamp.
   *
   * @param bytes the number of bytes the consumer has just acknowledged
   */
  def recordConsumerProgress(bytes: Long): Unit = {
    if (bytes > 0L) {
      consumerBytes.addAndGet(bytes)
    }
    lastConsumerHeartbeatNanos.set(System.nanoTime())
  }

  /**
   * Explicit consumer-to-producer heartbeat ping. Refreshes the liveness timestamp; if the
   * producer had been paused for a missing consumer it is allowed to resume on the next tick.
   */
  def consumerHeartbeat(): Unit = {
    lastConsumerHeartbeatNanos.set(System.nanoTime())
  }

  /**
   * Whether the streaming engine should gracefully degrade this shuffle to the sort-based
   * fallback. Becomes true once the consumer has been sustained [[BackpressureProtocol.
   * FALLBACK_SLOWNESS_RATIO]]x slower than the producer for longer than
   * [[BackpressureProtocol.FALLBACK_SUSTAINED_SECONDS]]. Read by `StreamingShuffleManager`.
   *
   * @return true if a sustained-slowdown fallback condition has fired
   */
  def shouldFallback: Boolean = shouldFallbackFlag

  /**
   * Whether the producer pipeline should currently throttle or pause because the consumer
   * heartbeat has gone missing past the connection deadline (heartbeat interval + connect timeout).
   *
   * @return true if the producer should pause pending consumer recovery
   */
  def producerShouldPause: Boolean = producerPaused

  /**
   * Installs the external, best-effort fallback probe. Called once by `StreamingEngine` after
   * construction to wire the cross-executor version handshake
   * ([[StreamingShuffleRendezvous.detectVersionMismatch]]). The probe is invoked on the heartbeat
   * thread, only while not already in fallback, so it must be cheap and self-guarding; a true
   * result raises [[shouldFallback]] (AAP fallback condition (d): producer/consumer version
   * mismatch). A setter -- rather than a constructor argument -- keeps the existing 3-arg
   * constructor binary-compatible for all current call sites.
   *
   * @param probe returns true when an external fallback condition (e.g. a protocol-version
   *              mismatch) has been detected; must be best-effort and never throw
   */
  def setFallbackProbe(probe: () => Boolean): Unit = {
    fallbackProbe = probe
  }

  // Periodic heartbeat tick: samples producer/consumer throughput since the previous tick, updates
  // fallback detection, and checks for a missing consumer. Guarded against transient failures so
  // the recurring schedule can never be torn down by an exception.
  private def heartbeatTick(): Unit = {
    try {
      val now = System.nanoTime()
      val prevNanos = lastSampleNanos.getAndSet(now)
      val intervalSeconds = (now - prevNanos).toDouble / NANOS_PER_SECOND
      if (intervalSeconds > 0.0) {
        val producerNow = producerBytes.get()
        val consumerNow = consumerBytes.get()
        val producerRate =
          (producerNow - lastSampledProducerBytes.getAndSet(producerNow)) / intervalSeconds
        val consumerRate =
          (consumerNow - lastSampledConsumerBytes.getAndSet(consumerNow)) / intervalSeconds
        evaluateFallback(producerRate, consumerRate, now)
        detectMissingConsumer(now)
        // Network saturation (AAP fallback condition (c)): derive the per-interval rate of
        // limiter-admitted bytes vs the configured per-shuffle link capacity (evaluated below).
        val transmittedNow = bytesTransmitted.get()
        val transmittedRate =
          (transmittedNow - lastSampledTransmittedBytes.getAndSet(transmittedNow)) / intervalSeconds
        evaluateNetworkSaturation(transmittedRate, now)
        // External fallback probe (AAP fallback condition (d)): consult the version handshake.
        checkExternalFallback()
      }
    } catch {
      case NonFatal(e) =>
        // Never let a sampling error kill the recurring heartbeat; note it only under debug.
        if (debug) {
          logWarning("Streaming shuffle backpressure heartbeat tick failed", e)
        }
    }
  }

  // Fallback rule: consumer sustained FALLBACK_SLOWNESS_RATIO (2x) slower than the producer for
  // more than FALLBACK_SUSTAINED_SECONDS (60s). Tracks the start of the slow window and, once the
  // window is exceeded, flips the one-way fallback flag and emits a single backpressure event.
  private def evaluateFallback(producerRate: Double, consumerRate: Double, now: Long): Unit = {
    val consumerTooSlow =
      producerRate > 0.0 && (consumerRate * FALLBACK_SLOWNESS_RATIO) < producerRate
    if (consumerTooSlow) {
      if (slowSinceNanos < 0L) {
        slowSinceNanos = now
      } else {
        val slowForSeconds = (now - slowSinceNanos).toDouble / NANOS_PER_SECOND
        if (slowForSeconds >= FALLBACK_SUSTAINED_SECONDS && !shouldFallbackFlag) {
          shouldFallbackFlag = true
          recordBackpressureEvent("consumer sustained 2x slower than producer for over 60s")
          logWarning("Streaming shuffle consumer sustained 2x slower than the producer for over " +
            "60s; signaling graceful fallback to sort-based shuffle")
        }
      }
    } else {
      // Consumer is keeping up: reset the slow-window tracker.
      slowSinceNanos = -1L
    }
  }

  // Detect a missing or slow consumer: if no heartbeat or consumed-byte progress has arrived within
  // the heartbeat interval plus the connect timeout, signal the producer to pause; resume when the
  // consumer recovers within the deadline.
  private def detectMissingConsumer(now: Long): Unit = {
    val sinceHeartbeatSeconds =
      (now - lastConsumerHeartbeatNanos.get()).toDouble / NANOS_PER_SECOND
    val deadlineSeconds = HEARTBEAT_INTERVAL_SECONDS + CONNECT_TIMEOUT_SECONDS
    if (sinceHeartbeatSeconds > deadlineSeconds) {
      if (!producerPaused) {
        producerPaused = true
        recordBackpressureEvent(
          s"consumer heartbeat missing for over ${deadlineSeconds}s; pausing producer")
      }
    } else if (producerPaused) {
      // Consumer recovered within the deadline; allow the producer to resume.
      producerPaused = false
      if (debug) {
        logDebug("Streaming shuffle consumer heartbeat recovered; resuming producer")
      }
    }
  }

  // Network-saturation rule (AAP fallback condition (c)): the per-shuffle link capacity is the
  // token-bucket refill rate (maxBandwidthMBps / numConcurrentShuffles). When the actual admitted
  // rate stays at or above NETWORK_SATURATION_RATIO (90%) of that capacity for more than
  // NETWORK_SATURATION_SUSTAINED_SECONDS, the link is treated as saturated and the one-way fallback
  // flag is flipped so the writer degrades to sort. With rate limiting disabled ("unlimited") there
  // is no configured capacity to saturate, so the rule never fires. Tracks the start of the
  // saturated window exactly like evaluateFallback tracks the slow-consumer window.
  private def evaluateNetworkSaturation(transmittedRate: Double, now: Long): Unit = {
    val capacity = refillBytesPerSecond
    val saturated =
      !rateLimitingDisabled && capacity > 0.0 &&
        transmittedRate >= capacity * NETWORK_SATURATION_RATIO
    if (saturated) {
      if (saturatedSinceNanos < 0L) {
        saturatedSinceNanos = now
      } else {
        val saturatedForSeconds = (now - saturatedSinceNanos).toDouble / NANOS_PER_SECOND
        if (saturatedForSeconds >= NETWORK_SATURATION_SUSTAINED_SECONDS && !shouldFallbackFlag) {
          shouldFallbackFlag = true
          recordBackpressureEvent("network saturation sustained above 90% of link capacity")
          logWarning("Streaming shuffle network saturation sustained above 90% of the configured " +
            "link capacity; signaling graceful fallback to sort-based shuffle")
        }
      }
    } else {
      // Utilization fell back below the saturation ratio: reset the saturated-window tracker.
      saturatedSinceNanos = -1L
    }
  }

  // External fallback probe (AAP fallback condition (d): producer/consumer version mismatch). The
  // engine wires `fallbackProbe` to the cross-executor version handshake; a true result flips the
  // one-way fallback flag so the writer degrades to sort. Only consulted while not already in
  // fallback, so a homogeneous (matching-version) cluster pays a single cheap probe per tick and a
  // mismatched cluster signals fallback exactly once.
  private def checkExternalFallback(): Unit = {
    if (!shouldFallbackFlag && fallbackProbe()) {
      shouldFallbackFlag = true
      recordBackpressureEvent("external fallback condition (version mismatch)")
      logWarning("Streaming shuffle external fallback condition detected (e.g. producer/consumer " +
        "protocol-version mismatch); signaling graceful fallback to sort-based shuffle")
    }
  }

  // ============================================================================================
  // QoS priority arbitration
  // ============================================================================================

  /**
   * Comparator for QoS arbitration when multiple shuffles contend for the shared bandwidth budget.
   * The ordering places the most-preferred shuffle first (a negative result means `a` should be
   * served before `b`):
   *
   *  1. Real shuffle traffic always outranks speculative-task traffic.
   *  2. Among shuffles of equal speculative-ness, the one with MORE partitions wins.
   *  3. Remaining ties are broken by larger buffered data volume.
   *
   * @param a the first shuffle's priority descriptor
   * @param b the second shuffle's priority descriptor
   * @return negative if `a` has higher priority, positive if `b` does, zero if equal
   */
  def comparePriority(a: ShufflePriority, b: ShufflePriority): Int = {
    if (a.isSpeculative != b.isSpeculative) {
      // Non-speculative (real) shuffle traffic is served before speculative-task traffic.
      if (a.isSpeculative) 1 else -1
    } else if (a.numPartitions != b.numPartitions) {
      // More partitions means higher priority, so it is ordered first.
      java.lang.Integer.compare(b.numPartitions, a.numPartitions)
    } else {
      // Larger buffered data volume means higher priority, so it is ordered first.
      java.lang.Long.compare(b.dataVolumeBytes, a.dataVolumeBytes)
    }
  }

  /**
   * Registers (or re-registers, last-wins) a shuffle's QoS priority so the admission path can
   * arbitrate bandwidth in its favor. Called by `StreamingShuffleManager.getWriter` for every
   * streaming shuffle. Idempotent across the many map tasks of one shuffle on an executor.
   *
   * @param shuffleId the shuffle to register
   * @param priority its [[ShufflePriority]] descriptor (speculative-ness, partition count, volume)
   */
  def registerShufflePriority(shuffleId: Int, priority: ShufflePriority): Unit = {
    shufflePriorities.put(Integer.valueOf(shuffleId), priority)
  }

  /**
   * Removes a shuffle's QoS priority, called from `StreamingShuffleManager.unregisterShuffle`. A
   * no-op if the shuffle was never registered.
   *
   * @param shuffleId the shuffle to drop from the QoS registry
   */
  def unregisterShufflePriority(shuffleId: Int): Unit = {
    shufflePriorities.remove(Integer.valueOf(shuffleId))
  }

  /**
   * Refreshes a registered shuffle's buffered data volume (the QoS tie-breaker) as its writer
   * emits. A no-op if the shuffle is not registered, so it is always safe to call from the writer.
   *
   * @param shuffleId the shuffle whose volume changed
   * @param dataVolumeBytes the latest buffered/emitted data volume in bytes
   */
  def updateShuffleDataVolume(shuffleId: Int, dataVolumeBytes: Long): Unit = {
    shufflePriorities.computeIfPresent(
      Integer.valueOf(shuffleId),
      (_, prev) => prev.copy(dataVolumeBytes = dataVolumeBytes))
  }

  // Whether some OTHER registered shuffle strictly outranks `shuffleId` (comparePriority < 0). An
  // unregistered caller (including the NO_QOS_SHUFFLE_ID sentinel) is never outranked, so callers
  // that opt out of QoS are never deprioritized. Reads the concurrent registry without locking.
  private def outranked(shuffleId: Int): Boolean = {
    val self = shufflePriorities.get(Integer.valueOf(shuffleId))
    if (self == null) {
      false
    } else {
      var found = false
      val it = shufflePriorities.entrySet().iterator()
      while (!found && it.hasNext) {
        val entry = it.next()
        if (entry.getKey.intValue() != shuffleId && comparePriority(entry.getValue, self) < 0) {
          found = true
        }
      }
      found
    }
  }

  // Tokens reserved for higher-priority traffic: an outranked shuffle may only draw from the bucket
  // once it has refilled past this reserve, guaranteeing higher-priority shuffles first claim to
  // the top (1 - QOS_RESERVE_FRACTION) of capacity. Never below one full pipelined block so a lone
  // outranked shuffle (no actual contention) is not starved indefinitely once the bucket refills.
  private def qosReserveBytes: Long =
    math.min(capacityBytes, math.max(MAX_PIPELINED_BLOCK_BYTES,
      (capacityBytes * QOS_RESERVE_FRACTION).toLong))

  // QoS admission decision: an outranked shuffle DEFERS (is refused) while the bucket is below the
  // reserve -- i.e. while a higher-priority shuffle is actively draining it. When the bucket has
  // refilled past the reserve (higher-priority demand satisfied/idle) the outranked shuffle
  // proceeds, so there is no permanent starvation. Never defers when rate limiting is disabled.
  // Pure in `available` (does not read the live token field) so it is deterministically testable.
  private def qosShouldDefer(shuffleId: Int, available: Long): Boolean = {
    !rateLimitingDisabled && outranked(shuffleId) && available < qosReserveBytes
  }

  // ============================================================================================
  // Telemetry and lifecycle
  // ============================================================================================

  // Record one backpressure/throttle event on the shared JMX metrics source. The reason string is
  // by-name and only materialized when debug logging is enabled, keeping telemetry overhead minimal
  // and per-executor log volume under the mandated 10MB/hour budget.
  private def recordBackpressureEvent(reason: => String): Unit = {
    metricsSource.incBackpressureEvents()
    if (debug) {
      logDebug(s"Streaming shuffle backpressure event: $reason")
    }
  }

  /**
   * Shuts down the heartbeat scheduler cleanly, leaving no daemon threads behind. Idempotent and
   * safe to call multiple times. Invoked from `StreamingShuffleManager.stop()`.
   */
  def stop(): Unit = synchronized {
    if (!stopped) {
      stopped = true
      heartbeatTask.cancel(false)
      ThreadUtils.shutdown(heartbeatExecutor)
      if (debug) {
        logDebug(s"BackpressureProtocol stopped; bytesTransmitted=${bytesTransmitted.get()}" +
          s" fallbackSignaled=$shouldFallbackFlag")
      }
    }
  }
}

/**
 * Constants and the QoS priority descriptor for [[BackpressureProtocol]]. The values encode the
 * operational gates mandated by the streaming-shuffle design: heartbeat cadence, timeouts, the
 * pipelined block size, the retry/backoff policy, and the sustained-slowdown fallback thresholds.
 */
private[spark] object BackpressureProtocol {

  /** Consumer-to-producer heartbeat interval, in seconds. */
  val HEARTBEAT_INTERVAL_SECONDS: Long = 10L

  /** Connection (connect) timeout for heartbeat control messages, in seconds. */
  val CONNECT_TIMEOUT_SECONDS: Long = 5L

  /** TCP keepalive for streaming control connections, in seconds. */
  val TCP_KEEPALIVE_SECONDS: Long = 5L

  /** Maximum size of a single pipelined block streamed to a consumer, in bytes (2MB). */
  val MAX_PIPELINED_BLOCK_BYTES: Long = 2L * 1024L * 1024L

  /** Initial retry backoff for control-message retransmission, in seconds. */
  val RETRY_BACKOFF_START_SECONDS: Long = 1L

  /** Maximum number of retry attempts for control-message retransmission. */
  val MAX_RETRY_ATTEMPTS: Int = 5

  /** Consumer is "too slow" when at least this many times slower than the producer. */
  val FALLBACK_SLOWNESS_RATIO: Int = 2

  /** Sustained slow duration that triggers graceful fallback to sort-based shuffle, in seconds. */
  val FALLBACK_SUSTAINED_SECONDS: Double = 60.0

  /**
   * Token-bucket utilization (admitted rate / configured per-shuffle link capacity) at or above
   * which the link is considered saturated (AAP fallback condition (c): network saturation > 90%).
   */
  val NETWORK_SATURATION_RATIO: Double = 0.9

  /**
   * Sustained saturation duration that triggers graceful fallback to sort-based shuffle, in
   * seconds. A sustained window (rather than an instantaneous spike) is required because the token
   * bucket deliberately absorbs short bursts; only a sustained >90% utilization is true saturation.
   */
  val NETWORK_SATURATION_SUSTAINED_SECONDS: Double = 60.0

  /**
   * Fraction of token-bucket capacity reserved for higher-priority traffic under QoS arbitration.
   * An outranked shuffle may only draw from the bucket once it has refilled past this reserve, so
   * higher-priority shuffles always get first claim to the top (1 - fraction) of the bucket.
   */
  val QOS_RESERVE_FRACTION: Double = 0.5

  /**
   * Sentinel shuffle id used by the no-argument [[BackpressureProtocol.tryAcquire(bytes:Long)*]]
   * and [[BackpressureProtocol.acquireBlocking(bytes:Long)*]] overloads. It is never registered in
   * the QoS registry, so a caller using it is never deprioritized (opts out of QoS arbitration).
   */
  val NO_QOS_SHUFFLE_ID: Int = Int.MinValue

  /** Bytes per megabyte, for MB/s to bytes/s conversions. */
  val BYTES_PER_MB: Long = 1024L * 1024L

  /** Nanoseconds per second, for rate math. */
  val NANOS_PER_SECOND: Double = 1.0e9

  /** Milliseconds per second, for blocking-wait estimation. */
  val MILLIS_PER_SECOND: Double = 1000.0

  /** Lower bound on a single blocking-acquire sleep, in milliseconds. */
  val MIN_WAIT_MILLIS: Long = 1L

  /** Upper bound on a single blocking-acquire sleep, in milliseconds (re-check cadence). */
  val MAX_WAIT_MILLIS: Long = 1000L

  /** Poll cadence while the producer is paused waiting for a missing consumer to recover, in ms. */
  val PAUSE_POLL_MILLIS: Long = 50L

  /**
   * Maximum time `acquireBlocking` will wait on a missing-consumer pause before reporting failure,
   * in nanoseconds (2 minutes). A permanently absent consumer is surfaced as a failed acquisition
   * rather than blocking the map task indefinitely, letting the writer abort/degrade.
   */
  val PAUSE_MAX_WAIT_NANOS: Long = 120L * 1000L * 1000L * 1000L

  /**
   * Immutable descriptor used by [[BackpressureProtocol.comparePriority]] to arbitrate bandwidth
   * among contending shuffles.
   *
   * @param isSpeculative whether the traffic belongs to a speculative task (lower priority)
   * @param numPartitions the shuffle's partition count (more partitions means higher priority)
   * @param dataVolumeBytes the currently buffered data volume in bytes (used as a tie-breaker)
   */
  case class ShufflePriority(
      isSpeculative: Boolean,
      numPartitions: Int,
      dataVolumeBytes: Long)
}
