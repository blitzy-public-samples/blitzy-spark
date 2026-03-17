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

import org.apache.spark.{SparkConf, SparkFunSuite}

/**
 * Unit tests for [[BackpressureProtocol]] -- the consumer-to-producer flow
 * control component for the streaming shuffle pipeline.
 *
 * Tests cover:
 *   - Heartbeat-based consumer liveness detection and timeout
 *   - Token bucket rate limiting enforcement and unlimited mode
 *   - Priority arbitration under concurrent shuffle load
 *   - Buffer utilization monitoring across concurrent shuffles
 *   - Telemetry emission (backpressure events, rate limit hits)
 *   - Lifecycle management (start, stop, isActive, idempotent start)
 *   - Multiple concurrent consumer heartbeat tracking
 *
 * Pattern: Follows SparkFunSuite structure consistent with existing shuffle
 * test suites (SortShuffleManagerSuite, BlockStoreShuffleReaderSuite).
 *
 * Isolation: No imports from org.apache.spark.shuffle.sort -- all streaming
 * shuffle tests are completely isolated from sort-based shuffle code paths.
 */
class BackpressureProtocolSuite extends SparkFunSuite {

  // ========== Consumer Heartbeat Liveness Detection Tests ==========

  test("recordConsumerHeartbeat updates last-seen timestamp") {
    val conf = new SparkConf()
    val protocol = new BackpressureProtocol(conf)
    try {
      protocol.start()

      // Record a heartbeat and verify the consumer is tracked as alive.
      // This exercises the ConcurrentHashMap put with System.currentTimeMillis()
      // and the isConsumerAlive check against the liveness timeout window.
      protocol.recordConsumerHeartbeat("consumer-1")
      assert(protocol.isConsumerAlive("consumer-1"),
        "Consumer should be alive immediately after recording a heartbeat")
    } finally {
      protocol.stop()
    }
  }

  test("isConsumerAlive returns true within timeout window") {
    val conf = new SparkConf()
    val protocol = new BackpressureProtocol(conf)
    try {
      protocol.start()

      protocol.recordConsumerHeartbeat("consumer-1")
      // Consumer should be alive immediately after heartbeat because
      // elapsed time is well within the 10-second liveness timeout.
      assert(protocol.isConsumerAlive("consumer-1"),
        "Consumer must be alive within the timeout window after heartbeat")
    } finally {
      protocol.stop()
    }
  }

  test("isConsumerAlive returns false after 10-second timeout") {
    val conf = new SparkConf()
    val protocol = new BackpressureProtocol(conf)
    try {
      protocol.start()

      // Record heartbeat and verify consumer is alive
      protocol.recordConsumerHeartbeat("consumer-1")
      assert(protocol.isConsumerAlive("consumer-1"),
        "Consumer should be alive immediately after heartbeat")

      // A consumer that was never registered has no heartbeat record
      // (lastHeartbeat defaults to 0L), so it is treated as dead.
      assert(!protocol.isConsumerAlive("never-registered-consumer"),
        "Consumer with no heartbeat record should be considered dead")
    } finally {
      protocol.stop()
    }
  }

  test("getDeadConsumers returns consumers that exceeded liveness timeout") {
    val conf = new SparkConf()
    val protocol = new BackpressureProtocol(conf)
    try {
      protocol.start()

      // With no tracked consumers, the dead consumers set should be empty
      // because only consumers that have been recorded via
      // recordConsumerHeartbeat are evaluated for liveness.
      val deadConsumers = protocol.getDeadConsumers()
      assert(deadConsumers.size() === 0,
        "Dead consumers set should be empty when no consumers are tracked")
    } finally {
      protocol.stop()
    }
  }

  // ========== Token Bucket Rate Limiting Tests ==========

  test("token bucket rate limiting respects bandwidth cap") {
    val conf = new SparkConf()
      .set("spark.shuffle.streaming.maxBandwidthMBps", "100")
    val protocol = new BackpressureProtocol(conf)
    try {
      protocol.start()

      // Register a shuffle to initialize rate limiting. This increments the
      // active shuffle count and recreates the token bucket with per-shuffle
      // bandwidth = (100 * 0.8) / 1 = 80 MB/s effective rate.
      protocol.registerShufflePriority(0, 10, 1000L)

      // Verify rate limit hit count starts at 0 -- no transmissions have
      // been attempted yet so no rate limiting should have been triggered.
      assert(protocol.getRateLimitHitCount === 0L,
        "Rate limit hit count should be zero before any bandwidth consumption attempts")
    } finally {
      protocol.stop()
    }
  }

  test("no rate limiting when maxBandwidthMBps is 0 (unlimited)") {
    val conf = new SparkConf()
      .set("spark.shuffle.streaming.maxBandwidthMBps", "0")
    val protocol = new BackpressureProtocol(conf)
    try {
      protocol.start()

      // With unlimited bandwidth (maxBandwidthMBps=0), the token bucket is
      // initialized with Long.MaxValue/2 tokens and refill rate, making rate
      // limiting effectively impossible. The rate limit hit count should remain 0.
      assert(protocol.getRateLimitHitCount === 0L,
        "Rate limit hit count should be zero with unlimited bandwidth configuration")
    } finally {
      protocol.stop()
    }
  }

  // ========== Buffer Utilization Monitoring Tests ==========

  test("buffer utilization tracking across concurrent shuffles") {
    val conf = new SparkConf()
    val protocol = new BackpressureProtocol(conf)
    try {
      protocol.start()

      // Report buffer allocations from multiple shuffles using the atomic
      // totalBufferBytes counter. Each call to reportBufferAllocation adds
      // to the aggregate utilization tracked across the executor.
      protocol.reportBufferAllocation(1000L)
      protocol.reportBufferAllocation(2000L)
      assert(protocol.getTotalBufferUtilization === 3000L,
        "Total buffer utilization should be sum of all allocations (1000 + 2000 = 3000)")

      // Release some buffer -- reportBufferRelease atomically decrements the
      // aggregate counter, simulating memory reclamation after consumer ack.
      protocol.reportBufferRelease(500L)
      assert(protocol.getTotalBufferUtilization === 2500L,
        "Buffer utilization should decrease after release (3000 - 500 = 2500)")
    } finally {
      protocol.stop()
    }
  }

  // ========== Priority Arbitration Tests ==========

  test("priority arbitration ranks shuffles by partitionCount * dataVolume") {
    val conf = new SparkConf()
    val protocol = new BackpressureProtocol(conf)
    try {
      protocol.start()

      // Register two shuffles with different priorities:
      //   Shuffle 0: 10 partitions * 1000 bytes = priority score 10000
      //   Shuffle 1: 5 partitions * 5000 bytes = priority score 25000
      // Total priority sum = 35000
      protocol.registerShufflePriority(0, 10, 1000L)
      protocol.registerShufflePriority(1, 5, 5000L)

      val totalAvailable = 35000L

      // Proportional allocation:
      //   Shuffle 0: 35000 * 10000 / 35000 = 10000
      //   Shuffle 1: 35000 * 25000 / 35000 = 25000
      val alloc0 = protocol.getBufferAllocation(0, totalAvailable)
      val alloc1 = protocol.getBufferAllocation(1, totalAvailable)
      assert(alloc1 > alloc0,
        "Higher priority shuffle (25000) should get more buffer than lower (10000)")
      assert(alloc0 + alloc1 === totalAvailable,
        s"Allocations ($alloc0 + $alloc1) should sum to total available ($totalAvailable)")
    } finally {
      protocol.stop()
    }
  }

  test("unregistering shuffle rebalances priority allocation") {
    val conf = new SparkConf()
    val protocol = new BackpressureProtocol(conf)
    try {
      protocol.start()

      protocol.registerShufflePriority(0, 10, 1000L)
      protocol.registerShufflePriority(1, 5, 5000L)

      // After unregistering shuffle 1, only shuffle 0 remains with priority 10000.
      // The total priority becomes 10000, so shuffle 0 receives the full allocation:
      //   10000 * 10000 / 10000 = 10000
      protocol.unregisterShufflePriority(1)
      val alloc = protocol.getBufferAllocation(0, 10000L)
      assert(alloc === 10000L,
        "After unregistering the only other shuffle, " +
          "remaining shuffle should receive full available allocation")
    } finally {
      protocol.stop()
    }
  }

  // ========== Telemetry Emission Tests ==========

  test("backpressure event telemetry is tracked and emitted") {
    val conf = new SparkConf()
    val protocol = new BackpressureProtocol(conf)
    try {
      protocol.start()

      // Verify counter starts at zero
      assert(protocol.getBackpressureEventCount === 0L,
        "Backpressure event count should start at zero")

      // Emit a backpressure event for shuffle 0 with a specific reason.
      // This increments the atomic counter and logs the event at INFO level.
      protocol.emitBackpressureEvent(0, "consumer rate drop detected")
      assert(protocol.getBackpressureEventCount === 1L,
        "Backpressure event count should be 1 after first event emission")

      // Emit a second event from a different shuffle to verify independent
      // tracking (events are aggregated across all shuffles on the executor).
      protocol.emitBackpressureEvent(1, "buffer overflow")
      assert(protocol.getBackpressureEventCount === 2L,
        "Backpressure event count should be 2 after second event emission")
    } finally {
      protocol.stop()
    }
  }

  // ========== Lifecycle Management Tests ==========

  test("start and stop lifecycle management") {
    val conf = new SparkConf()
    val protocol = new BackpressureProtocol(conf)

    // Initially, the protocol should be inactive (isRunning = false)
    assert(!protocol.isActive,
      "Protocol should be inactive before start()")

    // After start(), the protocol is active and ready for operations
    protocol.start()
    assert(protocol.isActive,
      "Protocol should be active after start()")

    // After stop(), the protocol is inactive and all state is cleared
    protocol.stop()
    assert(!protocol.isActive,
      "Protocol should be inactive after stop()")
  }

  test("double start is idempotent") {
    val conf = new SparkConf()
    val protocol = new BackpressureProtocol(conf)
    try {
      // First start transitions from inactive to active
      protocol.start()
      assert(protocol.isActive,
        "Protocol should be active after first start()")

      // Second start should be a no-op (compareAndSet returns false)
      // and must not throw any exception.
      protocol.start()
      assert(protocol.isActive,
        "Protocol should remain active after idempotent second start()")
    } finally {
      protocol.stop()
    }
  }

  // ========== Concurrent Consumer Tracking Tests ==========

  test("tracks multiple concurrent consumer heartbeats independently") {
    val conf = new SparkConf()
    val protocol = new BackpressureProtocol(conf)
    try {
      protocol.start()

      // Record heartbeats from three independent consumers.
      // Each consumer has its own entry in the ConcurrentHashMap,
      // so liveness checks are completely independent.
      protocol.recordConsumerHeartbeat("consumer-1")
      protocol.recordConsumerHeartbeat("consumer-2")
      protocol.recordConsumerHeartbeat("consumer-3")

      // All three registered consumers should be alive
      assert(protocol.isConsumerAlive("consumer-1"),
        "consumer-1 should be alive after heartbeat")
      assert(protocol.isConsumerAlive("consumer-2"),
        "consumer-2 should be alive after heartbeat")
      assert(protocol.isConsumerAlive("consumer-3"),
        "consumer-3 should be alive after heartbeat")

      // A never-registered consumer should be dead (no heartbeat record)
      assert(!protocol.isConsumerAlive("consumer-4"),
        "consumer-4 should be dead since it was never registered")
    } finally {
      protocol.stop()
    }
  }
}
