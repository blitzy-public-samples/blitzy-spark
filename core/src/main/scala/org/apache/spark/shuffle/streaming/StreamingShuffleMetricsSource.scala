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

import java.util.concurrent.atomic.AtomicLong

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source

/**
 * Dropwizard metrics source for streaming shuffle operational telemetry.
 *
 * Exposes real-time gauges for buffer utilization, spill counts, backpressure events,
 * and partial read invalidations via JMX and configured Spark metrics sinks.
 *
 * Follows the [[org.apache.spark.executor.ExecutorMetricsSource]] pattern for consistent
 * integration with Spark's metrics infrastructure. All gauge callbacks read from
 * [[AtomicLong]] counters ensuring O(1) lock-free access with less than 1% CPU overhead.
 *
 * Coexistence note: This metrics source is only registered when streaming shuffle is
 * enabled (`spark.shuffle.streaming.enabled=true`). It does not affect sort-based shuffle
 * metrics or any other existing metrics sources.
 *
 * Registered gauges:
 *  - `shuffle.streaming.bufferUtilizationPercent` — real-time buffer occupancy percentage
 *  - `shuffle.streaming.spillCount` — cumulative disk spill events
 *  - `shuffle.streaming.backpressureEvents` — consumer rate limiting incidents
 *  - `shuffle.streaming.partialReadInvalidations` — producer failure detection count
 */
private[spark] class StreamingShuffleMetricsSource extends Source {

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  override val sourceName: String = "StreamingShuffleMetrics"

  // Thread-safe atomic counters backing all gauge callbacks.
  // AtomicLong provides O(1) lock-free reads via get(), ensuring gauge polling
  // introduces negligible CPU overhead (<1% target per AAP specification).
  private val _bufferUtilizationPercent: AtomicLong = new AtomicLong(0L)
  private val _spillCount: AtomicLong = new AtomicLong(0L)
  private val _backpressureEvents: AtomicLong = new AtomicLong(0L)
  private val _partialReadInvalidations: AtomicLong = new AtomicLong(0L)

  /**
   * A gauge that reads its current value from an [[AtomicLong]] counter.
   *
   * Follows the `ExecutorMetricGauge` pattern from
   * [[org.apache.spark.executor.ExecutorMetricsSource]]. Each gauge instance is bound
   * to a specific atomic counter at construction time and returns the counter's current
   * value on every poll cycle from the Dropwizard metrics reporter.
   */
  private class StreamingShuffleGauge(counter: AtomicLong) extends Gauge[Long] {
    override def getValue: Long = counter.get()
  }

  /**
   * Registers all streaming shuffle metrics gauges with the Dropwizard [[MetricRegistry]]
   * and registers this source with the Spark [[MetricsSystem]].
   *
   * Called by the Executor during initialization when streaming shuffle is enabled.
   * This follows the same registration pattern as
   * [[org.apache.spark.executor.ExecutorMetricsSource.register]].
   *
   * After registration, all four gauges are accessible via JMX under the configured
   * metrics namespace and via any configured Spark metrics sinks (CSV, Graphite,
   * Prometheus, etc.).
   *
   * @param metricsSystem the Spark MetricsSystem instance to register this source with
   */
  def register(metricsSystem: MetricsSystem): Unit = {
    // Register buffer utilization gauge — updated by MemorySpillManager
    metricRegistry.register(
      MetricRegistry.name("shuffle", "streaming", "bufferUtilizationPercent"),
      new StreamingShuffleGauge(_bufferUtilizationPercent))

    // Register spill count gauge — incremented by MemorySpillManager on each spill event
    metricRegistry.register(
      MetricRegistry.name("shuffle", "streaming", "spillCount"),
      new StreamingShuffleGauge(_spillCount))

    // Register backpressure events gauge — incremented by BackpressureProtocol
    metricRegistry.register(
      MetricRegistry.name("shuffle", "streaming", "backpressureEvents"),
      new StreamingShuffleGauge(_backpressureEvents))

    // Register partial read invalidations gauge — incremented by StreamingShuffleReader
    metricRegistry.register(
      MetricRegistry.name("shuffle", "streaming", "partialReadInvalidations"),
      new StreamingShuffleGauge(_partialReadInvalidations))

    // Register this source with the Spark MetricsSystem for sink polling
    metricsSystem.registerSource(this)
  }

  // ---------------------------------------------------------------------------
  // Metric update methods — called by streaming shuffle components
  // ---------------------------------------------------------------------------

  /**
   * Updates the buffer utilization percentage to the given value.
   * Called by [[MemorySpillManager]] during its 100ms polling cycle to reflect
   * the current aggregate buffer occupancy across all active streaming shuffles
   * on this executor.
   *
   * @param value buffer utilization as a percentage (0–100)
   */
  def updateBufferUtilizationPercent(value: Long): Unit = {
    _bufferUtilizationPercent.set(value)
  }

  /**
   * Increments the cumulative spill count by one.
   * Called by [[MemorySpillManager]] each time a partition buffer is evicted
   * from memory and persisted to disk via BlockManager.
   */
  def incrementSpillCount(): Unit = {
    _spillCount.incrementAndGet()
  }

  /**
   * Increments the backpressure events counter by one.
   * Called by [[BackpressureProtocol]] when a consumer rate limit is triggered
   * or a token bucket throttle event occurs.
   */
  def incrementBackpressureEvents(): Unit = {
    _backpressureEvents.incrementAndGet()
  }

  /**
   * Increments the partial read invalidations counter by one.
   * Called by [[StreamingShuffleReader]] when a producer failure is detected
   * (via connection timeout) and all partial reads from that producer are
   * atomically discarded.
   */
  def incrementPartialReadInvalidations(): Unit = {
    _partialReadInvalidations.incrementAndGet()
  }

  // ---------------------------------------------------------------------------
  // Accessor methods — for testing, telemetry, and programmatic inspection
  // ---------------------------------------------------------------------------

  /** Returns the current cumulative disk spill count. */
  def getSpillCount: Long = _spillCount.get()

  /** Returns the current cumulative backpressure event count. */
  def getBackpressureEvents: Long = _backpressureEvents.get()

  /** Returns the current cumulative partial read invalidation count. */
  def getPartialReadInvalidations: Long = _partialReadInvalidations.get()

  /** Returns the current buffer utilization percentage (0–100). */
  def getBufferUtilizationPercent: Long = _bufferUtilizationPercent.get()
}
