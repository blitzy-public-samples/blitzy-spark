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

import com.codahale.metrics.{Counter, Gauge, Meter, MetricRegistry, Timer}

import org.apache.spark.metrics.source.Source

/**
 * JMX metrics source for streaming shuffle operational visibility implementing Dropwizard Metrics
 * Source interface. Exposes real-time gauges (buffer utilization percentage), counters (spill
 * events, backpressure incidents, partial read invalidations, checksum mismatches), meters
 * (bytes streamed, blocks transferred), and timers (block stream latency, spill latency,
 * acknowledgment latency) per shuffle ID.
 *
 * This metrics source is registered with Spark MetricsSystem for monitoring dashboard integration
 * and operational debugging. Critical for production troubleshooting per Section 0.9 telemetry
 * requirements.
 *
 * Thread-Safety: All metrics are thread-safe and designed for concurrent access from multiple
 * StreamingShuffleWriter and StreamingShuffleReader instances.
 *
 * Performance: Telemetry overhead limited to <1% CPU utilization per Section 0.2 requirements.
 *
 * @param shuffleId The unique identifier for this streaming shuffle operation
 */
private[spark] class StreamingShuffleMetricsSource(shuffleId: Int) extends Source {

  /**
   * Source name for JMX identification with per-shuffle granularity.
   * Format: StreamingShuffle.$shuffleId
   */
  override val sourceName: String = s"StreamingShuffle.$shuffleId"

  /**
   * Dropwizard MetricRegistry containing all registered metrics for this shuffle.
   * Exposed via JMX when registered with Spark MetricsSystem.
   */
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  // ==================================================================================
  // Real-Time Gauges
  // ==================================================================================

  /**
   * Current buffer utilization as a percentage (0.0 to 100.0).
   * Updated every 100ms by the MemorySpillManager monitoring thread.
   * Used to trigger automatic disk spill at 80% threshold (configurable 50-95%).
   */
  @volatile private var currentBufferUtilization: Double = 0.0

  metricRegistry.register("bufferUtilization", new Gauge[Double] {
    override def getValue: Double = currentBufferUtilization
  })

  /**
   * Updates the current buffer utilization percentage.
   * Called by MemorySpillManager every 100ms during monitoring.
   *
   * @param utilizationPercent Buffer utilization as a percentage (0.0 to 100.0)
   */
  def updateBufferUtilization(utilizationPercent: Double): Unit = {
    require(utilizationPercent >= 0.0 && utilizationPercent <= 100.0,
      s"Buffer utilization must be between 0.0 and 100.0, got $utilizationPercent")
    currentBufferUtilization = utilizationPercent
  }

  // ==================================================================================
  // Event Counters
  // ==================================================================================

  /**
   * Total number of disk spill events triggered when buffer utilization exceeds threshold.
   * Incremented by MemorySpillManager when automatic spill occurs.
   * High spill count indicates insufficient buffer memory or heavy shuffle load.
   */
  val spillCount: Counter = metricRegistry.counter("spillCount")

  /**
   * Total bytes spilled to disk across all spill events for this shuffle.
   * Tracks cumulative volume of data written to BlockManager storage.
   * Used to assess memory pressure and buffer sizing effectiveness.
   */
  val spillBytes: Counter = metricRegistry.counter("spillBytes")

  /**
   * Total number of backpressure events where consumer rate limiting was applied.
   * Incremented when token bucket enforces rate limiting due to consumer slowdown.
   * Indicates consumer throughput bottleneck relative to producer rate.
   */
  val backpressureEvents: Counter = metricRegistry.counter("backpressureEvents")

  /**
   * Total number of partial read invalidations due to producer failures.
   * Incremented when StreamingShuffleReader detects connection timeout (5 seconds)
   * and triggers discard of all partial reads from failed producer.
   * Non-zero value indicates producer executor failures requiring upstream recomputation.
   */
  val partialReadInvalidations: Counter = metricRegistry.counter("partialReadInvalidations")

  /**
   * Total number of checksum validation failures (CRC32C mismatches).
   * Incremented when StreamingShuffleReader detects data corruption on block receive.
   * Non-zero value indicates network corruption or disk corruption requiring investigation.
   * Triggers automatic retransmission up to 5 attempts before fallback.
   */
  val checksumMismatches: Counter = metricRegistry.counter("checksumMismatches")

  /**
   * Total number of automatic fallback events to sort-based shuffle.
   * Incremented when streaming conditions fail: memory pressure, consumer slowdown,
   * network saturation, or version mismatch.
   * Tracks frequency of degradation scenarios requiring operational attention.
   */
  val fallbackCount: Counter = metricRegistry.counter("fallbackCount")

  // ==================================================================================
  // Throughput Meters
  // ==================================================================================

  /**
   * Rate of bytes streamed directly from producers to consumers (not spilled to disk).
   * Measured in bytes per second with exponentially-weighted moving averages (1min, 5min, 15min).
   * High streaming rate indicates effective buffer utilization and minimal spill overhead.
   */
  val bytesStreamed: Meter = metricRegistry.meter("bytesStreamed")

  /**
   * Rate of shuffle blocks successfully transferred from producers to consumers.
   * Measured in blocks per second with exponentially-weighted moving averages.
   * Tracks streaming shuffle throughput and network transfer efficiency.
   */
  val blocksTransferred: Meter = metricRegistry.meter("blocksTransferred")

  // ==================================================================================
  // Performance Timers
  // ==================================================================================

  /**
   * Latency distribution for streaming individual blocks from producer to consumer.
   * Measures time from StreamingShuffleWriter.uploadStream() call to consumer acknowledgment.
   * Tracks min, max, mean, percentiles (p50, p75, p95, p99) for performance monitoring.
   * High latency indicates network congestion or consumer processing bottleneck.
   */
  val blockStreamLatency: Timer = metricRegistry.timer("blockStreamLatency")

  /**
   * Latency distribution for disk spill operations when buffer threshold exceeded.
   * Measures time from spill trigger to BlockManager.putBytes() completion.
   * Tracks I/O subsystem performance and disk bandwidth availability.
   * High latency indicates slow disk or I/O contention requiring tuning.
   */
  val spillLatency: Timer = metricRegistry.timer("spillLatency")

  /**
   * Latency distribution for consumer acknowledgment processing and buffer reclamation.
   * Measures time from StreamingShuffleAcknowledgment receipt to memory release.
   * Must complete within 100ms per Section 0.2 memory safety requirements.
   * High latency indicates memory management bottleneck or GC pressure.
   */
  val acknowledgmentLatency: Timer = metricRegistry.timer("acknowledgmentLatency")

  // ==================================================================================
  // Utility Methods
  // ==================================================================================

  /**
   * Returns a formatted string representation of current metrics state for debugging.
   * Includes all counters, meters, and timer statistics.
   *
   * @return Human-readable metrics summary
   */
  override def toString: String = {
    s"""StreamingShuffleMetricsSource(shuffleId=$shuffleId):
       |  bufferUtilization: $currentBufferUtilization%
       |  spillCount: ${spillCount.getCount}
       |  spillBytes: ${spillBytes.getCount}
       |  backpressureEvents: ${backpressureEvents.getCount}
       |  partialReadInvalidations: ${partialReadInvalidations.getCount}
       |  checksumMismatches: ${checksumMismatches.getCount}
       |  fallbackCount: ${fallbackCount.getCount}
       |  bytesStreamed: ${bytesStreamed.getCount} (rate: ${bytesStreamed.getMeanRate}/s)
       |  blocksTransferred: ${blocksTransferred.getCount}
       |    (rate: ${blocksTransferred.getMeanRate}/s)
       |  blockStreamLatency: count=${blockStreamLatency.getCount},
       |    mean=${blockStreamLatency.getSnapshot.getMean}ms
       |  spillLatency: count=${spillLatency.getCount},
       |    mean=${spillLatency.getSnapshot.getMean}ms
       |  acknowledgmentLatency: count=${acknowledgmentLatency.getCount},
       |    mean=${acknowledgmentLatency.getSnapshot.getMean}ms
       |""".stripMargin
  }
}
