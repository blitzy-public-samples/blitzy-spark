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

> **Availability at this release.** This release ships the streaming-shuffle **data-plane
> components** and an **in-process data path**; the cluster-wide **activation surface is staged for
> a later checkpoint**. Concretely:
>
> - **Implemented now:** the streaming `ShuffleWriter` and `ShuffleReader`, the
>   `StreamingShuffleHandle` dispatch marker, the `BackpressureProtocol`, the `MemorySpillManager`
>   (with on-disk spill and read-back), the `StreamingShuffleSource` metrics source, and a
>   package-private `StreamingBlockExchange` that connects writer block emission to reader
>   consumption -- providing per-partition buffering, CRC32C integrity, block-specific
>   retransmission, consumer-acknowledged buffer reclamation, and coverage-based partial-read
>   invalidation.
> - **Planned for a later checkpoint:** the `StreamingShuffleManager` itself and its registration in
>   the shuffle-manager factory map (so `spark.shuffle.manager=streaming` is **not yet selectable**),
>   and the distributed `TransportContext`-backed wire transport that will carry blocks across
>   executors. The current `StreamingBlockExchange` is the in-process realization that the
>   distributed transport will replace behind the same consumer contract.
>
> Until the manager and its factory registration land, setting `spark.shuffle.manager=streaming`
> has no effect and Spark continues to use the default sort-based shuffle. The configuration keys,
> metric names, and class names documented below are final and match the code in this release;
> sections that describe **manager selection** or **distributed transport** are marked as planned
> accordingly.

**Performance objective.** The engine targets a **30-50% end-to-end latency reduction** for
shuffle-bound workloads (10GB+ of shuffle data, 100+ partitions) and a **5-10%** improvement for
CPU-bound workloads.

**Safety objective.** Streaming shuffle is designed to **coexist with, and never replace, the
default `SortShuffleManager`**. Sort-based shuffle remains the default engine and is the designated
automatic fallback, so workloads that do not benefit from streaming see zero regression. The
per-shuffle automatic fallback is performed by the `StreamingShuffleManager` by delegating to a
composed `SortShuffleManager`; because the manager is staged for a later checkpoint (see the
**Availability** note above), the fallback *signals* described below (slow-consumer detection,
oversize-record and admission fallback) are computed in this release, while the manager-level revert
that consumes them lands with the manager.

Activation is **two-fold** (see the [Configuration](#configuration) section below and the
[Shuffle Behavior](configuration.html#shuffle-behavior) rows of the configuration guide): you must
select the streaming manager AND set the streaming feature flag. Manager selection
(`spark.shuffle.manager=streaming`) becomes available with the later-checkpoint manager; until then,
neither setting activates streaming and Spark uses the default sort-based shuffle.

# Architecture

All streaming logic lives in a new, **isolated package `org.apache.spark.shuffle.streaming`** (a
sibling of `org.apache.spark.shuffle.sort`), with zero cross-contamination of existing components:
no existing class imports or depends on the streaming classes. The feature plugs into existing
Spark machinery **entirely within the `ShuffleManager` abstraction boundary**. There are only two
integration points:

1. The `ShuffleManager` factory map, which (in the later checkpoint that adds the manager) resolves
   `spark.shuffle.manager=streaming` to `StreamingShuffleManager`. This release does not yet modify
   the factory map, so the name is not yet selectable.
2. The configuration registry, which exposes the `spark.shuffle.streaming.*` properties. These
   entries are present in this release.

Everything else -- `SparkEnv`, the DAG scheduler, the task lifecycle, the user-facing
RDD/DataFrame/Dataset APIs, and the executor memory model -- is unchanged and reused through its
existing interfaces. The network transport is likewise reused without modification; in this release
the producer-to-consumer data path is the in-process `StreamingBlockExchange`, and the distributed
`TransportContext`-backed transport is added in a later checkpoint behind the same consumer
contract.

## Components

The engine is composed of the following classes, all in `org.apache.spark.shuffle.streaming`. All
except `StreamingShuffleManager` are present in this release; the manager is staged for a later
checkpoint (see the **Availability** note in the [Overview](#overview)).

- `StreamingShuffleManager` *(planned, later checkpoint)* -- implements the `ShuffleManager` trait
  (constructor `(conf, isDriver)`). It dispatches the streaming writer and reader and **composes a
  `SortShuffleManager` instance for graceful fallback**, delegating to it whenever streaming is
  disabled or a fallback condition fires. This class is not present in this release, so streaming is
  not yet selectable as a shuffle manager.
- `StreamingShuffleHandle` -- extends `BaseShuffleHandle`; a lightweight dispatch marker whose
  presence makes the manager route a shuffle to the streaming writer/reader. It mirrors how
  `SerializedShuffleHandle` and `BypassMergeSortShuffleHandle` mark sort-path variants; any other
  handle type is delegated to the composed `SortShuffleManager`.
- `StreamingShuffleWriter` -- extends `ShuffleWriter`; partitions records into bounded
  per-partition in-memory buffers, publishes blocks to consumers through the in-process
  `StreamingBlockExchange` (the distributed `TransportContext`-backed transport replaces the
  exchange in a later checkpoint), computes CRC32C integrity checks, coordinates backpressure and
  spill, and emits a standard `MapStatus`.
- `StreamingShuffleReader` -- implements `ShuffleReader`; returns a lazy/blocking iterator that
  consumes in-progress blocks from a bounded inbox, validates CRC32C (requesting a block-specific
  retransmission on corruption), acknowledges consumed buffers (which reclaims the producer-side
  buffer), and performs atomic partial-read invalidation -- via `FetchFailedException` -- whenever a
  producer cannot be read or a stream completes before all expected blocks arrive.
- `StreamingBlockExchange` -- the package-private in-process producer-to-consumer data path used in
  this release. It connects writer block emission to reader callbacks, delegates block-byte storage,
  reclamation, and read-back to the `MemorySpillManager`, tracks per-map coverage so a reader only
  completes once all expected blocks are delivered, and serves block-specific resend requests. A
  later checkpoint backs this contract with the distributed `TransportContext` transport.
- `BackpressureProtocol` -- provides consumer-to-producer heartbeat flow-control state, a
  token-bucket rate limiter, slow-consumer/admission fallback signals, and QoS priority arbitration
  (shuffle traffic is prioritized over speculative tasks).
- `MemorySpillManager` -- polls buffer-memory utilization and, when the spill threshold is
  crossed, spills the largest buffered partitions among the LRU-eligible set to disk, serving and
  reclaiming those blocks by key and reclaiming buffers promptly after consumer acknowledgment.
- `StreamingShuffleSource` -- the Dropwizard metrics source (`sourceName = "streamingShuffle"`) that
  publishes the four streaming gauges described in [Monitoring and Metrics](#monitoring-and-metrics).

## Integration points

Streaming shuffle reuses the following existing subsystems **without modification**:

- **Instantiation** *(planned, later checkpoint)* -- the manager is created through the dynamic
  `ShuffleManager` factory, so `SparkEnv` is untouched; registering the `streaming` name in the
  factory map is sufficient. This release does not yet add that map entry.
- **Fallback** *(planned, later checkpoint)* -- `StreamingShuffleManager` composes a
  `SortShuffleManager` and delegates `registerShuffle`, `getWriter`, and `getReader` to it for the
  sort path; the sort engine is never modified. The fallback signals are computed in this release;
  the delegating manager that acts on them lands later.
- **Memory accounting** -- buffer allocation is tracked through the existing `MemoryManager`
  interface; the executor memory model is not redesigned.
- **Networking** -- no new transport stack is introduced. In this release the data path is the
  in-process `StreamingBlockExchange`; a later checkpoint streams blocks over the existing
  `TransportContext` behind the same consumer contract.
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

**Two-fold activation** *(manager selection lands in a later checkpoint).* The intended activation
requires BOTH:

- `spark.shuffle.manager=streaming` -- selects `StreamingShuffleManager` through the
  shuffle-manager factory map, and
- `spark.shuffle.streaming.enabled=true` -- the master opt-in feature flag.

Because `StreamingShuffleManager` and its factory-map registration are staged for a later checkpoint
(see the **Availability** note in the [Overview](#overview)), `spark.shuffle.manager=streaming` is
not yet selectable: in this release Spark uses the default sort-based shuffle regardless of these
settings. The configuration keys above are final and accepted by this release; they will gate
streaming once the manager lands.

**Composition, not replacement** *(planned, later checkpoint).* `StreamingShuffleManager` will
**compose a `SortShuffleManager` instance and delegate to it** whenever streaming is disabled, the
shuffle dependency is unsupported, or a runtime fallback condition fires. The sort engine is never
modified; it is reused exactly as-is. Because the streaming writer already emits a standard
`MapStatus` and the streaming reader signals failures through the existing `FetchFailedException`,
the DAG scheduler cannot tell the two engines apart.

## Fallback conditions

The design reverts a shuffle to sort-based shuffle when any of the following runtime conditions is
detected. In this release the **detection signals** are computed by the `BackpressureProtocol` and
the writer (which raises a `StreamingShuffleFallbackException` on oversize records and on admission
fallback); the **automatic per-shuffle revert** that consumes these signals is performed by the
`StreamingShuffleManager` and therefore lands with the manager in a later checkpoint.

1. The consumer is sustained **2x slower** than the producer for **more than 60 seconds**.
2. **Memory pressure** that would prevent buffer allocation (OOM risk).
3. **Network saturation** above **90%** of link capacity.
4. **Producer/consumer version mismatch**.

Once the manager lands, any of these conditions causes the affected stage to transparently use
sort-based shuffle, preserving correctness and avoiding regression. Fallback is per-shuffle and
requires no user intervention.

# Configuration

Streaming shuffle adds five properties under the `spark.shuffle.streaming.*` namespace. The
**Since Version** for all five is **4.1.0**, and all five are present and validated in this release.
They will gate streaming once `spark.shuffle.manager=streaming` becomes selectable with the
later-checkpoint manager and `spark.shuffle.streaming.enabled=true` is set (see
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

**Spill.** The `MemorySpillManager` polls buffer-memory utilization roughly every **100ms** and,
when utilization crosses the configurable `spillThreshold` (default **80%**, range `50-95`), spills
the **largest buffered blocks among the LRU-eligible set** to a local spill file (located via the
shuffle block resolver, falling back to a JVM temporary file when no `SparkEnv` is available). The
spill response is designed to complete in **sub-100ms**. Spilling frees buffer memory without losing
data: spilled blocks remain addressable by key, are read back from disk on demand (including for
retransmission), and are reclaimed once consumed.

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
- TCP keepalive: **5s** -- a transport-level setting that applies once the distributed
  `TransportContext`-backed transport lands in a later checkpoint; the constant is defined in this
  release but the in-process data path does not use a TCP socket.
- Pipelined block size: limited to **2MB** -- enforced at emission time; a single record that would
  exceed this cap raises a `StreamingShuffleFallbackException` rather than emitting an oversized
  block.

**Rate limiting.** A **token-bucket** limiter caps per-executor streaming bandwidth. Its refill rate
is computed as **`maxBandwidthMBps / numConcurrentShuffles`**, dividing the configured budget fairly
across concurrent shuffles. Setting `maxBandwidthMBps=0` (the default) disables rate limiting
entirely.

**Quality of service.** Shuffle traffic is prioritized over speculative tasks, so speculative
execution cannot starve in-progress streaming shuffles of bandwidth or buffer memory.

# Monitoring and Metrics

Streaming-shuffle telemetry uses the **existing Spark metrics system** -- the Dropwizard
`MetricRegistry` pattern used by other Spark metric sources. The metrics are published by a metrics
source named **`streamingShuffle`** (the `StreamingShuffleSource` class), which is present in this
release and updated live by the streaming data-plane components, so it adds no overhead on the
sort-based fallback path. Registration of the source into the cluster metrics system -- and thus
exposure over **JMX** and any other configured metrics sink (see the
[monitoring guide](monitoring.html)) -- is wired together with the `StreamingShuffleManager` in a
later checkpoint.

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

- **Streaming shuffle does not take effect.** In this release this is expected: the
  `StreamingShuffleManager` and its factory-map registration are staged for a later checkpoint, so
  `spark.shuffle.manager=streaming` is not yet selectable and Spark uses the default sort-based
  shuffle (see the **Availability** note in the [Overview](#overview)). Once the manager lands,
  verify that BOTH `spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=true` are
  set and that executors were restarted after the configuration change (configuration is not applied
  dynamically).
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

Streaming shuffle is opt-in and is designed to be safe to adopt incrementally because sort-based
shuffle remains the unchanged default and the designated automatic fallback.

> **Note.** Manager selection (`spark.shuffle.manager=streaming`) is not available in this release;
> it lands with the `StreamingShuffleManager` in a later checkpoint (see the **Availability** note
> in the [Overview](#overview)). The procedure below is the **target opt-in procedure** for that
> later checkpoint. In this release the streaming engine cannot be activated as the cluster shuffle
> manager, so no migration action is required yet.

**Opt-in procedure (applies once the manager lands).**

1. Start in a test/staging environment using a representative shuffle-bound job (10GB+ of shuffle
   data, 100+ partitions).
2. Set `spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=true`, then restart
   executors.
3. Monitor the four [streaming metrics](#monitoring-and-metrics) and end-to-end latency, and tune
   `spark.shuffle.streaming.bufferSizePercent`, `spark.shuffle.streaming.spillThreshold`, and
   `spark.shuffle.streaming.maxBandwidthMBps` as needed.
4. Roll out gradually to broader workloads once the staging results are satisfactory.

Example `spark-submit` configuration to enable streaming shuffle (once the manager lands):

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

Streaming shuffle **introduces no new dependencies**. It reuses the existing Dropwizard metrics
registry, and the distributed data path (later checkpoint) reuses the existing Netty transport
(`TransportContext`); no build manifest changes are required. The read-only build baseline for this
feature is:

| Component | Version |
|-----------|---------|
| Apache Spark | 4.1.0-SNAPSHOT |
| Scala | 2.13.17 (binary 2.13) |
| Java | 17 |
| Hadoop | 3.4.2 |
| Netty | 4.2.7.Final |
| Dropwizard Metrics | 4.2.33 |

Because the feature is additive and confined to the `org.apache.spark.shuffle.streaming` package
plus two small integration touchpoints (the configuration registry in this release, and the
shuffle-manager factory map in a later checkpoint), it does not change any existing dependency,
transport, or metrics infrastructure.
