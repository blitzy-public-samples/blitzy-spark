---
layout: global
displayTitle: Streaming Shuffle
title: Streaming Shuffle
description: Architecture, configuration, and operations guide for the opt-in streaming shuffle engine in Spark SPARK_VERSION_SHORT
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

Streaming Shuffle is an **opt-in** shuffle engine, new in Spark 4.1.0, that streams intermediate
shuffle data directly from map (producer) tasks to reduce (consumer) tasks through bounded
in-memory buffers governed by a backpressure protocol, instead of materializing the intermediate
data to disk the way the default sort-based shuffle does. By avoiding the disk round-trip for
intermediate data, it shortens the critical path of shuffle-heavy stages.

**Performance objective.** The engine targets a **30-50% end-to-end latency reduction** for
shuffle-bound workloads (10GB+ of shuffle data, 100+ partitions) and a **5-10%** improvement for
CPU-bound workloads.

**Safety objective.** Streaming shuffle **coexists with, and never replaces, the default
`SortShuffleManager`**. Sort-based shuffle remains both the default engine and the automatic
fallback, so workloads that do not benefit from streaming see zero regression. Whenever streaming
is disabled, unsupported for a given shuffle, or a runtime fallback condition fires, the affected
stage transparently uses sort-based shuffle.

Activation is **two-fold** (see the [Configuration](#configuration) section below and the
[Shuffle Behavior](configuration.html#shuffle-behavior) rows of the configuration guide): you must
select the streaming manager AND set the streaming feature flag. If either is missing, Spark uses
the default sort-based shuffle.

# Architecture

All streaming logic lives in a new, **isolated package `org.apache.spark.shuffle.streaming`** (a
sibling of `org.apache.spark.shuffle.sort`), with zero cross-contamination of existing components:
no existing class imports or depends on the streaming classes. The feature plugs into existing
Spark machinery **entirely within the `ShuffleManager` abstraction boundary**. There are only two
integration points:

1. The `ShuffleManager` factory map, which resolves `spark.shuffle.manager=streaming` to
   `StreamingShuffleManager`.
2. The configuration registry, which exposes the `spark.shuffle.streaming.*` properties.

Everything else -- `SparkEnv`, the DAG scheduler, the task lifecycle, the user-facing
RDD/DataFrame/Dataset APIs, the executor memory model, and the network transport -- is unchanged
and reused through its existing interfaces.

## Components

The engine is composed of six core classes, all in `org.apache.spark.shuffle.streaming`:

- `StreamingShuffleManager` -- implements the `ShuffleManager` trait (constructor
  `(conf, isDriver)`). It dispatches the streaming writer and reader and **composes a
  `SortShuffleManager` instance for graceful fallback**, delegating to it whenever streaming is
  disabled or a fallback condition fires.
- `StreamingShuffleHandle` -- extends `BaseShuffleHandle`; a lightweight dispatch marker whose
  presence makes the manager route a shuffle to the streaming writer/reader. It mirrors how
  `SerializedShuffleHandle` and `BypassMergeSortShuffleHandle` mark sort-path variants; any other
  handle type is delegated to the composed `SortShuffleManager`.
- `StreamingShuffleWriter` -- extends `ShuffleWriter`; partitions records into bounded
  per-partition in-memory buffers, pipelines blocks to consumers over the existing transport,
  computes CRC32C integrity checks, coordinates backpressure and spill, and emits a standard
  `MapStatus`.
- `StreamingShuffleReader` -- implements `ShuffleReader`; returns a lazy/blocking iterator that
  requests in-progress blocks, validates CRC32C (with retransmission on corruption), acknowledges
  consumed buffers, and performs atomic partial-read invalidation when a producer cannot be read.
- `BackpressureProtocol` -- provides consumer-to-producer heartbeat flow control, a token-bucket
  rate limiter, and QoS priority arbitration (shuffle traffic is prioritized over speculative
  tasks).
- `MemorySpillManager` -- polls buffer-memory utilization and, when the spill threshold is
  crossed, spills the largest buffered partitions (selected by LRU) to disk via the block manager,
  reclaiming buffers promptly after consumer acknowledgment.

## Integration points

Streaming shuffle reuses the following existing subsystems **without modification**:

- **Instantiation** -- the manager is created through the dynamic `ShuffleManager` factory, so
  `SparkEnv` is untouched; registering the `streaming` name in the factory map is sufficient.
- **Fallback** -- `StreamingShuffleManager` composes a `SortShuffleManager` and delegates
  `registerShuffle`, `getWriter`, and `getReader` to it for the sort path; the sort engine is never
  modified.
- **Memory accounting** -- buffer allocation is tracked through the existing `MemoryManager`
  interface; the executor memory model is not redesigned.
- **Networking** -- blocks are streamed over the existing `TransportContext`; no new transport
  stack is introduced.
- **Checksums** -- CRC32C generation and validation reuse the existing `ShuffleChecksumUtils`
  facility.
- **Output discovery** -- the writer emits a standard `MapStatus`, so `MapOutputTracker` and the
  DAG scheduler locate outputs exactly as they do for sort-based shuffle.
- **Fault recovery** -- on partial-read invalidation the reader throws the existing
  `FetchFailedException`, which the unmodified DAG scheduler converts into upstream stage
  recomputation, preserving the lineage and fault-recovery model.

# Coexistence and Graceful Fallback

Streaming shuffle is designed so that the proven sort-based shuffle is always available.
**Sort-based shuffle is the default**, and streaming shuffle runs alongside it -- it never replaces
the default engine.

**Two-fold activation.** To use streaming shuffle you must set BOTH:

- `spark.shuffle.manager=streaming` -- selects `StreamingShuffleManager` through the
  shuffle-manager factory map, and
- `spark.shuffle.streaming.enabled=true` -- the master opt-in feature flag.

If either is unset, Spark uses the default sort-based shuffle. Selecting the streaming manager
without enabling the feature flag (or enabling the flag without selecting the manager) does not
activate streaming.

**Composition, not replacement.** `StreamingShuffleManager` **composes a `SortShuffleManager`
instance and delegates to it** whenever streaming is disabled, the shuffle dependency is
unsupported, or a runtime fallback condition fires. The sort engine is never modified; it is reused
exactly as-is. Because the streaming writer still emits a standard `MapStatus` and the streaming
reader signals failures through the existing `FetchFailedException`, the DAG scheduler cannot tell
the two engines apart.

## Fallback conditions

In addition to the two-fold opt-in, the engine automatically reverts a shuffle to sort-based
shuffle when any of the following runtime conditions is detected:

1. The consumer is sustained **2x slower** than the producer for **more than 60 seconds**.
2. **Memory pressure** that would prevent buffer allocation (OOM risk).
3. **Network saturation** above **90%** of link capacity.
4. **Producer/consumer version mismatch**.

On any of these conditions, the affected stage transparently uses sort-based shuffle, preserving
correctness and avoiding regression. Fallback is per-shuffle and requires no user intervention.

# Configuration

Streaming shuffle adds five properties under the `spark.shuffle.streaming.*` namespace. The
**Since Version** for all five is **4.1.0**. These properties take effect only when
`spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=true` (see
[Coexistence and Graceful Fallback](#coexistence-and-graceful-fallback)).

| Property Name | Default | Meaning | Since Version |
|---------------|---------|---------|---------------|
| `spark.shuffle.streaming.enabled` | `false` | Master opt-in flag for the streaming shuffle engine. Must be `true` AND `spark.shuffle.manager=streaming` for streaming to activate; otherwise the default sort-based shuffle is used. | 4.1.0 |
| `spark.shuffle.streaming.bufferSizePercent` | `20` | Percent (range `1-50`) of executor memory reserved for per-partition streaming buffers. Per-partition buffer = (executorMemory * bufferSizePercent / 100) / numPartitions. | 4.1.0 |
| `spark.shuffle.streaming.spillThreshold` | `80` | Buffer-utilization percent (range `50-95`) at which the spill manager begins spilling the largest buffered partitions (LRU) to disk via the block manager. | 4.1.0 |
| `spark.shuffle.streaming.maxBandwidthMBps` | `0` | Per-executor streaming rate limit in MB/s enforced by the token-bucket limiter. A value of `0` means unlimited (no rate limiting). | 4.1.0 |
| `spark.shuffle.streaming.debug` | `false` | Enable verbose streaming-shuffle debug logging. Keep log volume below 10MB/hour per executor. | 4.1.0 |

**Configuration changes require an executor restart.** There is no dynamic reconfiguration in this
version; changing any `spark.shuffle.streaming.*` property takes effect only after executors are
restarted.

For related settings, see the [Shuffle Behavior](configuration.html#shuffle-behavior) section of
the configuration guide and the [Streaming Shuffle tuning](tuning.html#streaming-shuffle) guidance.

# Operational Details

This section describes the runtime behavior and failure-tolerance mechanisms of the streaming
engine.

**Buffer sizing.** Each shuffle reserves a slice of executor memory for streaming buffers,
partitioned evenly across the shuffle's output partitions:

```
per-partition buffer = (executorMemory * bufferSizePercent / 100) / numPartitions
```

`bufferSizePercent` defaults to `20` and must be within `1-50`. Buffer memory is tracked through the
existing `MemoryManager`, so streaming buffers participate in the normal executor memory accounting.

**Spill.** The `MemorySpillManager` polls buffer-memory utilization and, when utilization crosses
the configurable `spillThreshold` (default **80%**, range `50-95`), spills the largest buffered
partitions to disk through the block manager. Spill selection is **LRU**, and the spill response is
designed to complete in **sub-100ms**. Spilling frees buffer memory without losing data: spilled
blocks are served from disk and reclaimed once consumed.

**Integrity.** Every streamed block is protected with a **CRC32C** checksum computed and validated
through the existing `ShuffleChecksumUtils` facility (no new checksum implementation). If a block
fails validation, it is retransmitted using **exponential backoff (start 1s, max 5 attempts)**.

**Atomic partial-read invalidation.** If a producer times out or a block is corrupt beyond
recovery, the reader performs an atomic partial-read invalidation: it discards any partially
consumed state for the affected partition and throws the existing `FetchFailedException`. The
unmodified DAG scheduler converts that exception into upstream stage recomputation. This guarantees
**zero data loss** across producer crashes, consumer failures, and network partitions, because no
reduce task ever observes a partially read partition as complete.

**Flow control.** Backpressure is driven by consumer-to-producer signaling with the following
parameters:

- Heartbeat interval: **10s**
- Connection timeout: **5s**
- TCP keepalive: **5s**
- Pipelined block size: limited to **2MB**

**Rate limiting.** A **token-bucket** limiter caps per-executor streaming bandwidth. Its refill rate
is computed as **`maxBandwidthMBps / numConcurrentShuffles`**, dividing the configured budget fairly
across concurrent shuffles. Setting `maxBandwidthMBps=0` (the default) disables rate limiting
entirely.

**Quality of service.** Shuffle traffic is prioritized over speculative tasks, so speculative
execution cannot starve in-progress streaming shuffles of bandwidth or buffer memory.

# Monitoring and Metrics

Streaming-shuffle telemetry uses the **existing Spark metrics system** -- the Dropwizard
`MetricRegistry` pattern used by other Spark metric sources -- and is exposed over **JMX** (and any
other configured metrics sink; see the [monitoring guide](monitoring.html)). The metrics are
published by a metrics source named **`streamingShuffle`**, which is registered only when the
streaming engine is actually running for a shuffle, so it adds no overhead on the sort-based
fallback path.

The source publishes four metrics:

| Metric | Type | Meaning |
|--------|------|---------|
| `shuffle.streaming.bufferUtilizationPercent` | gauge | Current buffer utilization, as a percentage (0-100). |
| `shuffle.streaming.spillCount` | gauge (cumulative count) | Number of spill events. |
| `shuffle.streaming.backpressureEvents` | gauge (cumulative count) | Number of backpressure/throttle events. |
| `shuffle.streaming.partialReadInvalidations` | gauge (cumulative count) | Number of atomic partial-read invalidations. |

All four are registered as Dropwizard gauges; the three event metrics report monotonically
increasing cumulative counts backed by atomic counters.

**Telemetry budget.** Metric collection is designed to add **less than 1% CPU** overhead, and
streaming-shuffle log volume is kept **below 10MB/hour per executor**. Verbose debug logging is
gated behind `spark.shuffle.streaming.debug` (default `false`).

## Dashboards

Because the metrics are standard JMX gauges and counters, they can be scraped into monitoring
dashboards -- for example through the JMX sink into Prometheus/Grafana, or any sink documented in
the [monitoring guide](monitoring.html). Recommended dashboard panels:

- **Buffer utilization** (`shuffle.streaming.bufferUtilizationPercent`) -- watch for sustained
  values near the spill threshold, which indicate impending or frequent spilling.
- **Spill count** (`shuffle.streaming.spillCount`) -- a rising rate signals memory pressure.
- **Backpressure events** (`shuffle.streaming.backpressureEvents`) -- elevated values indicate slow
  consumers or a saturated network.
- **Partial-read invalidations** (`shuffle.streaming.partialReadInvalidations`) -- nonzero values
  indicate producer/consumer failures or network partitions driving recomputation.

# Troubleshooting

The list below maps common symptoms to corrective actions. The relevant metrics are described in
[Monitoring and Metrics](#monitoring-and-metrics).

- **Streaming shuffle does not take effect.** Verify that BOTH `spark.shuffle.manager=streaming`
  and `spark.shuffle.streaming.enabled=true` are set, and that executors were restarted after the
  configuration change (configuration is not applied dynamically).
- **Frequent spilling** (high `shuffle.streaming.spillCount`, with
  `shuffle.streaming.bufferUtilizationPercent` sustained near the threshold). On memory-rich
  executors, increase `spark.shuffle.streaming.bufferSizePercent` or
  `spark.shuffle.streaming.spillThreshold` (within their ranges of `1-50` and `50-95`), or increase
  parallelism so each partition buffer holds less data.
- **High `shuffle.streaming.backpressureEvents` / slow consumers.** Check whether the
  consumer-2x-slower-than-producer fallback is triggering; on a saturated network, consider setting
  a finite `spark.shuffle.streaming.maxBandwidthMBps` to smooth bursts.
- **Frequent `shuffle.streaming.partialReadInvalidations` or `FetchFailedException`s.** These
  indicate producer/consumer failures or network partitions driving recomputation; investigate
  executor stability and network health.
- **Unexpected fallback to sort-based shuffle.** Review the four
  [fallback conditions](#fallback-conditions). Enable `spark.shuffle.streaming.debug` temporarily to
  capture details, keeping the log-volume budget (10MB/hour per executor) in mind.

# Migration Guide

Streaming shuffle is opt-in and safe to adopt incrementally because sort-based shuffle remains the
unchanged default and automatic fallback.

**Opt-in procedure.**

1. Start in a test/staging environment using a representative shuffle-bound job (10GB+ of shuffle
   data, 100+ partitions).
2. Set `spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=true`, then restart
   executors.
3. Monitor the four [streaming metrics](#monitoring-and-metrics) and end-to-end latency, and tune
   `spark.shuffle.streaming.bufferSizePercent`, `spark.shuffle.streaming.spillThreshold`, and
   `spark.shuffle.streaming.maxBandwidthMBps` as needed.
4. Roll out gradually to broader workloads once the staging results are satisfactory.

Example `spark-submit` configuration to enable streaming shuffle:

```
--conf spark.shuffle.manager=streaming \
--conf spark.shuffle.streaming.enabled=true \
--conf spark.shuffle.streaming.bufferSizePercent=20 \
--conf spark.shuffle.streaming.spillThreshold=80
```

**Rollback procedure.** To revert to sort-based shuffle, either set
`spark.shuffle.streaming.enabled=false` or set `spark.shuffle.manager=sort` (the default), then
restart executors. Rollback is safe because sort-based shuffle is the unchanged default engine; no
data or state migration is required.

# Compatibility Matrix

Streaming shuffle **introduces no new dependencies**. It reuses the existing Netty transport
(`TransportContext`) and the existing Dropwizard metrics registry, and requires no build manifest
changes. The read-only build baseline for this feature is:

| Component | Version |
|-----------|---------|
| Apache Spark | 4.1.0-SNAPSHOT |
| Scala | 2.13.17 (binary 2.13) |
| Java | 17 |
| Hadoop | 3.4.2 |
| Netty | 4.2.7.Final |
| Dropwizard Metrics | 4.2.33 |

Because the feature is additive and confined to the `org.apache.spark.shuffle.streaming` package
plus two small integration touchpoints (the shuffle-manager factory map and the configuration
registry), it does not change any existing dependency, transport, or metrics infrastructure.
