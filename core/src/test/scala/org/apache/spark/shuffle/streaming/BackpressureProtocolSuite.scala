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

import org.mockito.Mockito.{atLeastOnce, mock, verify}
import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.internal.config

/**
 * Unit tests for [[BackpressureProtocol]], the streaming-shuffle flow-control and rate-limiting
 * protocol. The suite is intentionally lightweight, mirroring
 * `org.apache.spark.shuffle.sort.SortShuffleManagerSuite`, and covers the documented behaviors:
 * token-bucket rate limiting (refill = maxBandwidthMBps / numConcurrentShuffles), the "unlimited"
 * bypass, consumer-acknowledgment flow control, sustained-slow-consumer fallback signaling, QoS
 * priority arbitration, the backpressure telemetry side effect, and a clean, idempotent stop().
 *
 * Determinism: the time-driven logic (the 60s sustained-slowdown fallback and heartbeat-timeout
 * detection) is exercised through testable seams with injected timestamps rather than wall-clock
 * sleeps, so the tests stay fast and reliable.
 *
 * Same-package access: this suite lives in the streaming package, so it can directly construct the
 * private[spark] protocol and reference the package-private StreamingShuffleSource and the
 * ShufflePriority descriptor.
 */
class BackpressureProtocolSuite extends SparkFunSuite with Matchers with PrivateMethodTester
  with Eventually {

  import BackpressureProtocol.ShufflePriority

  /**
   * Constructs a [[BackpressureProtocol]] and guarantees its heartbeat scheduler thread is shut
   * down via stop(), so no test leaks the daemon executor created in the constructor.
   */
  private def withProtocol(
      conf: SparkConf,
      numConcurrentShuffles: => Int,
      source: StreamingShuffleSource)(body: BackpressureProtocol => Unit): Unit = {
    val bp = new BackpressureProtocol(conf, numConcurrentShuffles, source)
    try {
      body(bp)
    } finally {
      bp.stop()
    }
  }

  /**
   * Reads the cumulative `shuffle.streaming.backpressureEvents` gauge value from a real
   * [[StreamingShuffleSource]] (registered under MetricRegistry.name("shuffle", "streaming",
   * "backpressureEvents")). Used to assert the telemetry side effect of a throttle event.
   */
  private def backpressureEvents(source: StreamingShuffleSource): Long = {
    source.metricRegistry.getGauges.get("shuffle.streaming.backpressureEvents")
      .getValue.asInstanceOf[Long]
  }

  /** Converts whole seconds to nanoseconds for injecting timestamps into the protocol's seams. */
  private def secondsToNanos(seconds: Long): Long = seconds * 1000L * 1000L * 1000L

  test("token bucket refill rate equals maxBandwidthMBps / numConcurrentShuffles") {
    // 100 MB/s budget shared across 2 concurrent shuffles => 50 MB/s per-shuffle refill.
    val conf = new SparkConf(false).set(config.STREAMING_SHUFFLE_MAX_BANDWIDTH_MBPS, 100)
    val source = new StreamingShuffleSource
    withProtocol(conf, numConcurrentShuffles = 2, source) { bp =>
      // Refill rate (bytes/s) == (maxBandwidthMBps / numConcurrentShuffles) * 1MB == 50 * 1048576.
      val refillRate = PrivateMethod[Double](Symbol("refillBytesPerSecond"))
      bp.invokePrivate(refillRate()) mustBe (50.0 * 1024 * 1024)

      // A request larger than the bucket capacity (~50MB) can never be admitted -> throttled.
      bp.tryAcquire(1024L * 1024L * 1024L) mustBe false
      // Zero/negative byte requests take the no-op fast path and are always admitted.
      bp.tryAcquire(0L) mustBe true

      // As wall-clock time elapses the bucket refills from empty, so a small request is admitted.
      eventually {
        bp.tryAcquire(1024L) mustBe true
      }
      // Successful acquisitions are accounted in the cumulative transmitted-bytes counter.
      bp.bytesTransmittedTotal must be >= 1024L
    }
  }

  test("unlimited bandwidth bypasses rate limiting") {
    // Default maxBandwidthMBps is 0 ("unlimited"), which disables the token bucket entirely.
    val conf = new SparkConf(false)
    val source = new StreamingShuffleSource
    withProtocol(conf, numConcurrentShuffles = 4, source) { bp =>
      // Every acquire is granted immediately regardless of size when limiting is disabled.
      bp.tryAcquire(Long.MaxValue) mustBe true
      bp.acquire(1024L * 1024L * 1024L) mustBe true
      bp.acquireBlocking(Long.MaxValue) mustBe true
      // No throttling occurred, so no backpressure telemetry was recorded.
      backpressureEvents(source) mustBe 0L
    }
  }

  test("consumer acknowledgment drives reclamation / flow-control state") {
    val conf = new SparkConf(false)
    val source = new StreamingShuffleSource
    withProtocol(conf, numConcurrentShuffles = 1, source) { bp =>
      val detectMissingConsumer = PrivateMethod[Unit](Symbol("detectMissingConsumer"))
      // A fresh protocol does not pause the producer.
      bp.producerShouldPause mustBe false

      // Simulate the consumer heartbeat going missing well past the connect deadline (15s): the
      // producer is paused. We inject a future timestamp instead of sleeping.
      bp.invokePrivate(detectMissingConsumer(System.nanoTime() + secondsToNanos(16)))
      bp.producerShouldPause mustBe true

      // The consumer acknowledges progress and pings a heartbeat, refreshing liveness; the next
      // detection within the deadline resumes the producer (flow control reopens).
      bp.recordConsumerProgress(4096L)
      bp.consumerHeartbeat()
      bp.invokePrivate(detectMissingConsumer(System.nanoTime()))
      bp.producerShouldPause mustBe false
    }
  }

  test("detects sustained slow consumer and signals fallback") {
    val conf = new SparkConf(false)
    val source = new StreamingShuffleSource
    withProtocol(conf, numConcurrentShuffles = 1, source) { bp =>
      val evaluateFallback = PrivateMethod[Unit](Symbol("evaluateFallback"))
      // A fresh protocol has not signaled fallback.
      bp.shouldFallback mustBe false

      // Consumer at 10 MB/s vs producer at 100 MB/s = 10x slower (>= the 2x threshold). The first
      // sample only starts the slow window; fallback must not fire yet.
      val windowStart = System.nanoTime()
      bp.invokePrivate(evaluateFallback(100.0, 10.0, windowStart))
      bp.shouldFallback mustBe false

      // Once the slow condition has been sustained for more than 60s, fallback is signaled. We
      // inject a timestamp 61s later rather than sleeping.
      bp.invokePrivate(evaluateFallback(100.0, 10.0, windowStart + secondsToNanos(61)))
      bp.shouldFallback mustBe true
    }
  }

  test("a consumer that keeps up resets the slow-window and prevents fallback") {
    val conf = new SparkConf(false)
    val source = new StreamingShuffleSource
    withProtocol(conf, numConcurrentShuffles = 1, source) { bp =>
      val evaluateFallback = PrivateMethod[Unit](Symbol("evaluateFallback"))
      val t0 = System.nanoTime()
      // Start a slow window (consumer 10x slower than the producer)...
      bp.invokePrivate(evaluateFallback(100.0, 10.0, t0))
      // ...then the consumer catches up (equal rate), which resets the slow window.
      bp.invokePrivate(evaluateFallback(100.0, 100.0, t0 + secondsToNanos(30)))
      // A single later slow sample restarts the window from scratch, so the earlier 30s of
      // slowness does NOT count: fallback must still be false.
      bp.invokePrivate(evaluateFallback(100.0, 10.0, t0 + secondsToNanos(61)))
      bp.shouldFallback mustBe false
    }
  }

  test("QoS arbitration prioritizes shuffle traffic over speculative tasks") {
    val conf = new SparkConf(false)
    withProtocol(conf, numConcurrentShuffles = 1, new StreamingShuffleSource) { bp =>
      // ShufflePriority(isSpeculative, numPartitions, dataVolumeBytes). comparePriority returns a
      // negative value when its FIRST argument has the higher priority (is served first).
      val realShuffle = ShufflePriority(false, 4, 1L)
      val speculative = ShufflePriority(true, 100, 9L)
      // Real shuffle traffic outranks speculative traffic even with fewer partitions / less volume.
      bp.comparePriority(realShuffle, speculative) must be < 0
      bp.comparePriority(speculative, realShuffle) must be > 0

      // Among non-speculative contenders, the one with more partitions wins.
      val manyParts = ShufflePriority(false, 10, 0L)
      val fewParts = ShufflePriority(false, 5, 0L)
      bp.comparePriority(manyParts, fewParts) must be < 0

      // With equal speculative-ness and partition count, the larger buffered volume wins.
      val bigVol = ShufflePriority(false, 5, 2048L)
      val smallVol = ShufflePriority(false, 5, 512L)
      bp.comparePriority(bigVol, smallVol) must be < 0

      // Identical descriptors are equal in priority.
      bp.comparePriority(bigVol, bigVol) mustBe 0
    }
  }

  test("backpressure throttling increments the backpressureEvents metric") {
    val conf = new SparkConf(false)
      .set(config.STREAMING_SHUFFLE_MAX_BANDWIDTH_MBPS, 100)
      .set(config.STREAMING_SHUFFLE_DEBUG, true)
    val source = new StreamingShuffleSource
    withProtocol(conf, numConcurrentShuffles = 2, source) { bp =>
      // No throttling yet -> the gauge starts at zero.
      backpressureEvents(source) mustBe 0L
      // An over-capacity request is throttled, which records a backpressure event on the source.
      bp.acquire(1024L * 1024L * 1024L) mustBe false
      backpressureEvents(source) must be >= 1L
    }
  }

  test("backpressure events are reported to the metrics source (mock verification)") {
    val conf = new SparkConf(false).set(config.STREAMING_SHUFFLE_MAX_BANDWIDTH_MBPS, 100)
    val source = mock(classOf[StreamingShuffleSource])
    withProtocol(conf, numConcurrentShuffles = 2, source) { bp =>
      // Throttling an over-capacity request must notify the metrics source.
      bp.acquire(1024L * 1024L * 1024L) mustBe false
      verify(source, atLeastOnce()).incBackpressureEvents()
    }
  }

  test("stop() releases scheduled threads cleanly and is idempotent") {
    val conf = new SparkConf(false)
    val bp = new BackpressureProtocol(conf, numConcurrentShuffles = 1, new StreamingShuffleSource)
    // stop() shuts down the heartbeat scheduler without throwing...
    noException must be thrownBy bp.stop()
    // ...and is guarded so repeated calls are safe (no double-shutdown error).
    noException must be thrownBy bp.stop()
  }

}
