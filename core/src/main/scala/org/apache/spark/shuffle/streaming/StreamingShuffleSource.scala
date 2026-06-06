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

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

/**
 * Dropwizard [[Source]] that publishes the four streaming-shuffle telemetry metrics over JMX
 * through the standard `MetricsSystem`, reusing the exact `MetricRegistry`/`Gauge` registration
 * pattern of [[org.apache.spark.executor.ExecutorSource]] (no new metrics framework, no new
 * dependency). This is the single, canonical, thread-safe holder of the streaming metric state:
 * the streaming engine's components (`MemorySpillManager`, `BackpressureProtocol` and
 * `StreamingShuffleReader`) mutate the counters here through the public updater methods, and the
 * registered gauges sample those holders lazily when scraped over JMX.
 *
 * Coexistence strategy: this Source is created and registered (by `StreamingShuffleManager`) only
 * when the opt-in streaming engine is actually running for a shuffle. It is never instantiated on
 * the sort-based path, so it adds zero overhead to the default `SortShuffleManager` fallback. The
 * gauges read pre-computed atomic holders, keeping the telemetry overhead well under the mandated
 * 1% CPU budget. Being `private[spark]` and confined to the streaming package enforces the
 * zero-cross-contamination rule: existing components neither import nor depend on it.
 */
private[spark] class StreamingShuffleSource extends Source {

  // The registry that backs every gauge below; exposed to the standard MetricsSystem for JMX.
  override val metricRegistry = new MetricRegistry()

  // Logical name under which the streaming metrics appear in the MetricsSystem namespace.
  override val sourceName = "streamingShuffle"

  // Canonical, thread-safe metric state mutated concurrently by the streaming components. These
  // holders are declared before the gauge registrations so they are fully initialized by the time
  // any gauge could be sampled.
  private val spillCountValue = new AtomicLong(0L)
  private val backpressureEventsValue = new AtomicLong(0L)
  private val partialReadInvalidationsValue = new AtomicLong(0L)
  private val bufferUtilizationPercentValue = new AtomicInteger(0)

  /** Records that a buffered partition was spilled to disk (called by `MemorySpillManager`). */
  def incSpillCount(): Unit = spillCountValue.incrementAndGet()

  /** Records that the producer was throttled by backpressure (called by `BackpressureProtocol`). */
  def incBackpressureEvents(): Unit = backpressureEventsValue.incrementAndGet()

  /** Records an atomic partial-read invalidation (called by `StreamingShuffleReader`). */
  def incPartialReadInvalidations(): Unit = partialReadInvalidationsValue.incrementAndGet()

  /** Sets the current buffer-utilization percentage (0-100) sampled by `MemorySpillManager`. */
  def setBufferUtilizationPercent(p: Int): Unit = bufferUtilizationPercentValue.set(p)

  // Gauge for the current per-executor buffer-utilization percentage driving spill decisions.
  metricRegistry.register(MetricRegistry.name("shuffle", "streaming", "bufferUtilizationPercent"),
    new Gauge[Int] {
      override def getValue: Int = bufferUtilizationPercentValue.get()
    })

  // Gauge for the cumulative number of buffer spills to disk.
  metricRegistry.register(MetricRegistry.name("shuffle", "streaming", "spillCount"),
    new Gauge[Long] {
      override def getValue: Long = spillCountValue.get()
    })

  // Gauge for the cumulative number of backpressure throttling events.
  metricRegistry.register(MetricRegistry.name("shuffle", "streaming", "backpressureEvents"),
    new Gauge[Long] {
      override def getValue: Long = backpressureEventsValue.get()
    })

  // Gauge for the cumulative number of atomic partial-read invalidations.
  metricRegistry.register(MetricRegistry.name("shuffle", "streaming", "partialReadInvalidations"),
    new Gauge[Long] {
      override def getValue: Long = partialReadInvalidationsValue.get()
    })
}
