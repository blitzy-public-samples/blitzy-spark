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

> **Availability at this release.** `spark.shuffle.manager=streaming` is now **selectable**: this
> release ships the full control plane and an end-to-end data path. Concretely:
>
> - **Implemented now:** the `StreamingShuffleManager` and its registration in the shuffle-manager
>   factory map (so `spark.shuffle.manager=streaming` selects it); the two-fold opt-in and the
>   manager-driven **automatic runtime fallback** to a composed `SortShuffleManager`; the streaming
>   `ShuffleWriter` and `ShuffleReader`; the `StreamingShuffleHandle` dispatch marker; the
>   `BackpressureProtocol`; the `MemorySpillManager` (with on-disk spill and read-back); the
>   `StreamingShuffleSource` metrics source; the package-private `StreamingBlockExchange` data path
>   (per-partition buffering, CRC32C integrity, block-specific retransmission, consumer-acknowledged
>   buffer reclamation, and coverage-based partial-read invalidation); and a
>   `TransportContext`-backed `StreamingShuffleTransport` that carries blocks, acks, resends, and
>   completion/failure signals across executor boundaries by reusing Spark's existing Netty stack.
> - **Cross-executor rendezvous (automatic):** a driver-side `StreamingShuffleEndpointCoordinator`
>   RPC endpoint maintains an `executorId -> (host, port)` registry of streaming-transport endpoints.
>   Each executor's streaming engine advertises its `StreamingShuffleTransport` listen address when it
>   first registers a streaming shuffle, and reduce readers resolve the set of remote peers from the
>   coordinator and **auto-subscribe** to remote producers through the exchange's subscribe API. This
>   rendezvous lives entirely within the `ShuffleManager` boundary and reuses Spark's existing RPC
>   env; it does **not** modify the DAG scheduler or `MapOutputTracker`. When no driver coordinator is
>   reachable (for example in a single-JVM unit test) the reader transparently serves co-located
>   producers in-process, so the engine degrades safely rather than failing.
> - **Automatic fallback triggers:** all four documented fallback conditions are wired to fire
>   automatically -- a sustained-slow consumer (2x slower for >60s), a memory-pressure admission
>   failure (OOM risk), sustained link saturation (>=90% of the configured per-shuffle bandwidth for
>   >60s), and a producer/consumer protocol-version mismatch -- each degrades the affected shuffle to
>   the composed sort-based engine before any `MapStatus` output is advertised. See
>   [Fallback conditions](#fallback-conditions) below for the detection mechanism of each.
> - **Current limitations (v1):** the reader-side switch to sort is duplication-free only before the
>   first record is yielded to the consumer; a later switch falls back via the existing
>   `FetchFailedException` recomputation path. Link-saturation detection is scoped to the per-executor
>   token-bucket rate limiter, so it is meaningful only when `spark.shuffle.streaming.maxBandwidthMBps`
>   is set (an unlimited budget has no capacity to saturate). Configuration changes require an executor
>   restart (no dynamic reconfiguration in this version).
>
> The configuration keys, metric names, and class names documented below are final and match the
> code in this release.

**Performance objective.** The engine targets a **30-50% end-to-end latency reduction** for
shuffle-bound workloads (10GB+ of shuffle data, 100+ partitions) and a **5-10%** improvement for
CPU-bound workloads.

**Safety objective.** Streaming shuffle is designed to **coexist with, and never replace, the
default `SortShuffleManager`**. Sort-based shuffle remains the default engine and is the designated
automatic fallback, so workloads that do not benefit from streaming see zero regression. The
per-shuffle automatic fallback is performed by the `StreamingShuffleManager` by delegating to a
composed `SortShuffleManager`: at registration time when streaming is disabled or the shuffle is
unsupported (for example it requires map-side combine), and at run time when a fallback signal fires
(a sustained-slow consumer, an admission/memory-pressure timeout, or an oversize unsplittable block).
In every case the affected task still produces a standard `MapStatus`, so the scheduler and
`MapOutputTracker` are unaffected.

Activation is **two-fold** (see the [Configuration](#configuration) section below and the
[Shuffle Behavior](configuration.html#shuffle-behavior) rows of the configuration guide): you must
select the streaming manager (`spark.shuffle.manager=streaming`) AND set the streaming feature flag
(`spark.shuffle.streaming.enabled=true`). Selecting the manager without the flag delegates every
shuffle to the composed sort-based engine, so streaming is never engaged until both are set.

# Architecture

All streaming logic lives in a new, **isolated package `org.apache.spark.shuffle.streaming`** (a
sibling of `org.apache.spark.shuffle.sort`), with zero cross-contamination of existing components:
no existing class imports or depends on the streaming classes. The feature plugs into existing
Spark machinery **entirely within the `ShuffleManager` abstraction boundary**. There are only two
integration points:

1. The `ShuffleManager` factory map, which resolves `spark.shuffle.manager=streaming` to
   `StreamingShuffleManager`. This single map entry is the only production change outside the
   streaming package besides the configuration keys.
2. The configuration registry, which exposes the `spark.shuffle.streaming.*` properties.

Everything else -- `SparkEnv`, the DAG scheduler, the task lifecycle, the user-facing
RDD/DataFrame/Dataset APIs, and the executor memory model -- is unchanged and reused through its
existing interfaces. The network transport is likewise reused without modification: blocks move
across executors over a `StreamingShuffleTransport` that is built on Spark's existing
`TransportContext`, and when no transport is bound (for example in a single-JVM unit test) the
same `StreamingBlockExchange` contract serves co-located producers and consumers in-process.

## Components

The engine is composed of the following classes, all in `org.apache.spark.shuffle.streaming`, and
all present in this release.

- `StreamingShuffleManager` -- implements the `ShuffleManager` trait (constructor
  `(conf, isDriver)`). It dispatches the streaming writer and reader and **composes a
  `SortShuffleManager` instance for graceful fallback**, delegating to it whenever streaming is
  disabled, the shuffle is unsupported, or a runtime fallback condition fires. Selecting
  `spark.shuffle.manager=streaming` resolves to this class.
- `StreamingShuffleHandle` -- extends `BaseShuffleHandle`; a lightweight dispatch marker whose
  presence makes the manager route a shuffle to the streaming writer/reader. It mirrors how
  `SerializedShuffleHandle` and `BypassMergeSortShuffleHandle` mark sort-path variants; any other
  handle type is delegated to the composed `SortShuffleManager`.
- `StreamingShuffleWriter` -- extends `ShuffleWriter`; partitions records into bounded
  per-partition in-memory buffers, publishes blocks to consumers through the `StreamingBlockExchange`
  (which routes to co-located readers in-process or to remote readers over the
  `TransportContext`-backed transport), computes CRC32C integrity checks, coordinates backpressure
  and spill, and emits a standard `MapStatus`. When a fallback condition fires it raises an internal
  signal that the manager catches to degrade the task to sort-based shuffle.
- `StreamingShuffleReader` -- implements `ShuffleReader`; returns a lazy/blocking iterator that
  consumes in-progress blocks from a bounded inbox, validates CRC32C (requesting a block-specific
  retransmission on corruption), acknowledges consumed buffers (which reclaims the producer-side
  buffer), and performs atomic partial-read invalidation -- via `FetchFailedException` -- whenever a
  producer cannot be read or a stream completes before all expected blocks arrive.
- `StreamingBlockExchange` -- the package-private producer-to-consumer data path. It connects writer
  block emission to reader callbacks, delegates block-byte storage, reclamation, and read-back to the
  `MemorySpillManager`, tracks per-map coverage so a reader only completes once all expected blocks
  are delivered, and serves block-specific resend requests. The same routing serves both co-located
  readers (in-process) and remote readers: when a `StreamingShuffleTransport` is bound, a remote
  subscriber is registered as an ordinary consumer proxy, so block delivery, acks, resends, and
  completion/failure signals cross executor boundaries with no change to the writer or reader.
- `StreamingShuffleTransport` -- the cross-executor wire transport. It builds on Spark's existing
  `TransportContext` (server, client factory, and `RpcHandler`) through their public interfaces
  only -- no transport internals are modified -- and carries the streaming protocol's BLOCK,
  COMPLETE, FAILED, ACK, RESEND, and SUBSCRIBE messages between executors' streaming endpoints.
- `StreamingShuffleEndpointCoordinator` -- the driver-side rendezvous registry. It is a
  `ThreadSafeRpcEndpoint` that maps each executor's id to its `StreamingShuffleTransport` listen
  address (host/port) and protocol version. Executors register their endpoint when they first set up
  the streaming engine, and reduce readers query it to discover remote producers and auto-subscribe
  to them through the exchange. It reuses Spark's existing RPC env and is best-effort: when it is
  unreachable the reader serves only co-located producers. The advertised protocol version is what
  the version-mismatch fallback probe compares.
- `BackpressureProtocol` -- provides consumer-to-producer heartbeat flow-control state, a
  token-bucket rate limiter, slow-consumer/admission fallback signals, and QoS priority arbitration
  (shuffle traffic is prioritized over speculative tasks).
- `MemorySpillManager` -- polls buffer-memory utilization and, when the spill threshold is
  crossed, spills the largest buffered partitions among the LRU-eligible set to disk, serving and
  reclaiming those blocks by key and reclaiming buffers promptly after consumer acknowledgment.
- `FallbackShuffleWriter` -- the manager-owned wrapper that runs the streaming writer and, if it
  raises a `StreamingShuffleFallbackException`, transparently re-writes the task's records through
  the composed `SortShuffleManager`'s writer -- before any `MapStatus` is advertised.
- `FallbackShuffleReader` -- the manager-owned wrapper that runs the streaming reader and, on a
  streaming read fallback, switches to the composed sort reader (duplication-free before the first
  record is yielded; otherwise the existing `FetchFailedException` recomputation path applies).
- `SpillableReplayBuffer` -- a bounded, optionally disk-backed buffer that retains a consumed prefix
  so a mid-stream fallback can replay already-read records correctly without data loss.
- `StreamingShuffleSource` -- the Dropwizard metrics source (`sourceName = "streamingShuffle"`) that
  publishes the four streaming gauges described in [Monitoring and Metrics](#monitoring-and-metrics).

### Source layout

The complete streaming-shuffle feature comprises the files below. The production engine lives in
`core/src/main/scala/org/apache/spark/shuffle/streaming/`: the six core components named in the
design (`StreamingShuffleManager`, `StreamingShuffleHandle`, `StreamingShuffleWriter`,
`StreamingShuffleReader`, `BackpressureProtocol`, `MemorySpillManager`), plus the runtime support
classes that complete the data path and fallback machinery (`StreamingBlockExchange`,
`StreamingShuffleTransport`, `StreamingShuffleEndpointCoordinator`, `FallbackShuffleWriter`,
`FallbackShuffleReader`, `SpillableReplayBuffer`) and the `StreamingShuffleSource` metrics source.

The only production changes outside the package are the `streaming` entry in the `ShuffleManager`
factory map, the five `spark.shuffle.streaming.*` entries in the configuration registry, and an
additive generic `computeChecksum` overload on the existing `ShuffleChecksumUtils` (reusing the
established CRC32C facility rather than introducing a new one).

The matching test suites live in `core/src/test/scala/org/apache/spark/shuffle/streaming/`
(`StreamingShuffleManagerSuite`, `StreamingShuffleWriterSuite`, `StreamingShuffleReaderSuite`,
`BackpressureProtocolSuite`, `MemorySpillManagerSuite`, `StreamingBlockExchangeSuite`,
`StreamingShuffleTransportSuite`, `StreamingShuffleEndpointCoordinatorSuite`, and the end-to-end
`StreamingShuffleIntegrationSuite`), alongside the `StreamingShufflePerformanceBenchmark`.

## Integration points

Streaming shuffle reuses the following existing subsystems **without modification**:

- **Instantiation** -- the manager is created through the dynamic `ShuffleManager` factory, so
  `SparkEnv` is untouched; registering the `streaming` name in the factory map is sufficient.
- **Fallback** -- `StreamingShuffleManager` composes a `SortShuffleManager` and delegates
  `registerShuffle`, `getWriter`, and `getReader` to it for the sort path; the sort engine is never
  modified. At registration time it falls back for disabled/unsupported shuffles, and at run time a
  manager-owned fallback writer/reader catches a streaming fallback signal, discards partial
  streaming state, and re-routes the task through the composed sort engine before any output is
  advertised.
- **Memory accounting** -- buffer allocation is tracked through the existing `MemoryManager`
  interface; the executor memory model is not redesigned.
- **Networking** -- no new transport stack is introduced. Blocks stream over a
  `StreamingShuffleTransport` built on the existing `TransportContext`; co-located producers and
  consumers (or a transport-less unit test) use the same `StreamingBlockExchange` contract
  in-process.
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

**Two-fold activation.** Activation requires BOTH:

- `spark.shuffle.manager=streaming` -- selects `StreamingShuffleManager` through the
  shuffle-manager factory map, and
- `spark.shuffle.streaming.enabled=true` -- the master opt-in feature flag.

`spark.shuffle.manager=streaming` is selectable in this release. With the manager selected but the
feature flag left at its default `false`, the manager registers every shuffle with the composed
`SortShuffleManager`, so Spark behaves exactly as it does with the default sort-based shuffle until
you also set `spark.shuffle.streaming.enabled=true`.

**Composition, not replacement.** `StreamingShuffleManager` **composes a `SortShuffleManager`
instance and delegates to it** whenever streaming is disabled, the shuffle dependency is unsupported,
or a runtime fallback condition fires. The sort engine is never modified; it is reused exactly
as-is. Because the streaming writer already emits a standard `MapStatus` and the streaming reader
signals failures through the existing `FetchFailedException`, the DAG scheduler cannot tell the two
engines apart.

## Fallback conditions

The manager reverts a task to sort-based shuffle when a runtime fallback condition is detected. The
**detection signals** are computed by the `BackpressureProtocol` and the writer, which raises an
internal `StreamingShuffleFallbackException` on an oversize unsplittable block, on a sustained-slow
consumer, on a hard memory-admission failure (the spill manager could not admit the bytes even after
a synchronous spill pass), on sustained link saturation, and on a producer/consumer protocol-version
mismatch. A manager-owned fallback writer catches that signal, discards partial streaming state, and
re-writes the task's records through the composed `SortShuffleManager` -- and it does so **before any
`MapStatus` is advertised**, so no partial streaming output is ever published.

1. The consumer is sustained **2x slower** than the producer for **more than 60 seconds** (slow-
   consumer signal) -- **implemented** as an automatic runtime fallback.
2. **Memory pressure that prevents buffer allocation** (OOM risk) -- a hard memory-admission failure
   that persists after the spill manager attempts a synchronous spill-and-retry pass -- **implemented**
   as an automatic runtime fallback. The block is neither stored nor routed, and the writer falls back
   to sort before any output is advertised.
3. A block whose serialized size exceeds the pipelined block-size cap (`2 MB`) and so cannot be
   streamed -- enforced for both single-record and multi-record blocks -- **implemented** as an
   automatic runtime fallback.
4. **Sustained network saturation** at or above **90%** of the configured per-shuffle link capacity
   (`maxBandwidthMBps / numConcurrentShuffles`) for **more than 60 seconds**, measured from the
   token-bucket transmitted-byte accounting -- **implemented** as an automatic runtime fallback. With
   an unlimited bandwidth budget there is no configured capacity to saturate, so this trigger is inert.
5. A **producer/consumer protocol-version mismatch**, detected through the streaming rendezvous
   handshake (each `StreamingShuffleTransport` endpoint advertises its protocol version, and the
   reader/manager side probes the registry for any peer on a different version) -- **implemented** as
   an automatic runtime fallback.

For the implemented conditions, the affected task transparently produces sort-based output with no
record loss and without surfacing a task failure, preserving correctness and avoiding regression.
Fallback is per-task and requires no user intervention. Note that the reader-side fallback is
duplication-free only before the first record has been yielded to the consumer; once consumption has
begun, a producer failure or coverage shortfall instead triggers the existing `FetchFailedException`
upstream-recomputation path.

# Configuration

Streaming shuffle adds five properties under the `spark.shuffle.streaming.*` namespace. The
**Since Version** for all five is **4.1.0**, and all five are present and validated in this release.
They gate streaming when `spark.shuffle.manager=streaming` is selected and
`spark.shuffle.streaming.enabled=true` is set (see
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
- TCP keepalive: **5s** -- a transport-level setting that applies to the `TransportContext`-backed
  `StreamingShuffleTransport` when blocks stream across executors; co-located (in-process) transfers
  through the `StreamingBlockExchange` do not use a TCP socket.
- Pipelined block size: limited to **2MB** -- enforced at emission time for **every** block, whether
  single-record or multi-record. The writer flushes the serialization stream and checks the exact
  serialized size before publishing; any block that would exceed the cap and cannot be split raises a
  `StreamingShuffleFallbackException` rather than emitting an oversized block.

**Rate limiting.** A **token-bucket** limiter caps per-executor streaming bandwidth. Its refill rate
is computed as **`maxBandwidthMBps / numConcurrentShuffles`**, dividing the configured budget fairly
across concurrent shuffles. Setting `maxBandwidthMBps=0` (the default) disables rate limiting
entirely.

**Quality of service.** Shuffle traffic is prioritized over speculative tasks, so speculative
execution cannot starve in-progress streaming shuffles of bandwidth or buffer memory.

# Monitoring and Metrics

Streaming-shuffle telemetry uses the **existing Spark metrics system** -- the Dropwizard
`MetricRegistry` pattern used by other Spark metric sources. The metrics are published by a metrics
source named **`streamingShuffle`** (the `StreamingShuffleSource` class), which is updated live by
the streaming data-plane components, so it adds no overhead on the sort-based fallback path. The
`StreamingShuffleManager` registers the source with the running `MetricsSystem` when one is
available (the registration is best-effort and guarded, so a missing `SparkEnv` or a duplicate
registration never affects the data path), exposing the metrics over **JMX** and any other
configured metrics sink (see the [monitoring guide](monitoring.html)).

The source publishes four metrics:

| Metric | Type | Meaning |
|--------|------|---------|
| `shuffle.streaming.bufferUtilizationPercent` | gauge | Current buffer utilization, as a percentage (0-100). |
| `shuffle.streaming.spillCount` | gauge (cumulative count) | Number of spill events. |
| `shuffle.streaming.backpressureEvents` | gauge (cumulative count) | Number of backpressure/throttle events. |
| `shuffle.streaming.partialReadInvalidations` | gauge (cumulative count) | Number of atomic partial-read invalidations. |

All four are registered as Dropwizard gauges; the three event metrics report monotonically
increasing cumulative counts backed by atomic counters.

## Safety gates and acceptance methodology

Streaming shuffle is held to four quantitative safety gates. Because they are workload- and
cluster-dependent, they are validated through performance/stress acceptance runs rather than asserted
in the CI unit suites (the CI integration suite logs end-to-end timing for visibility but asserts no
wall-clock SLA). The targets and how to verify each:

- **Telemetry overhead < 1% CPU.** Metric updates are atomic counter/gauge writes on the data path,
  and the `streamingShuffle` source adds nothing on the sort-based fallback path (it is only updated
  by the streaming data-plane components). To verify, run an identical shuffle-bound workload twice --
  once with `spark.shuffle.streaming.enabled=true` and once on the sort manager -- and compare the
  `executorCpuTime` task metric; the streaming-attributable telemetry delta must stay under 1% of
  executor CPU.
- **Log volume < 10MB/hour per executor.** All verbose per-block logging is gated behind
  `spark.shuffle.streaming.debug` (default `false`); at the default level the engine logs only
  lifecycle and fallback events. To verify, run a sustained shuffle workload for one hour at the
  default log level and confirm per-executor streaming log output stays under the 10MB/hour budget.
- **Zero memory leaks over a 2-hour stress test.** Buffers are registered with the
  `MemorySpillManager` on publish and reclaimed on consumer acknowledgment (or freed on producer
  failure / `stop`). To verify, drive continuous streaming shuffles for two hours and confirm the
  spill manager's `trackedBytesTotal` and `reservedBytesTotal` counters return to zero between stages
  and that heap usage is flat across the run (no monotonic growth).
- **Spill threshold response.** Buffer utilization is polled and a spill is triggered at the
  configured threshold (default 80%), with reclamation completing within ~100ms of acknowledgment.

The `StreamingShufflePerformanceBenchmark` (under
`core/src/test/scala/org/apache/spark/shuffle/streaming/`) provides the baseline sort-vs-streaming
comparison harness for latency, memory, spill, and bandwidth, and is the recommended starting point
for capturing these acceptance measurements on a given cluster.

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
  and `spark.shuffle.streaming.enabled=true` are set: selecting the manager alone leaves every
  shuffle on the composed sort-based engine. Confirm the shuffle is supported (a shuffle that
  requires map-side combine is delegated to sort by design), and that executors were restarted
  after the configuration change (configuration is not applied dynamically). If streaming is active
  but a stage still runs on sort, a runtime fallback condition may have fired -- check
  `shuffle.streaming.backpressureEvents` (see the **Fallback conditions** above).
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

> **Note.** `spark.shuffle.manager=streaming` is selectable in this release and the procedure below
> applies as written. Cross-executor streaming is auto-driven: producers are discovered through the
> driver-side `StreamingShuffleEndpointCoordinator` and reduce readers auto-subscribe to remote
> producers, and all four documented fallback conditions fire automatically (see the
> **Availability** note in the [Overview](#overview) and [Fallback conditions](#fallback-conditions)).
> The one v1 limitation to keep in mind when planning a rollout is that the reader-side switch to sort
> is duplication-free only before the first record is yielded to the consumer (a later switch uses the
> existing `FetchFailedException` recomputation path). Because sort-based shuffle is the unchanged
> default and the automatic fallback, adopting streaming is safe to do incrementally.

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

Streaming shuffle **introduces no new dependencies**. It reuses the existing Dropwizard metrics
registry, and the cross-executor data path reuses the existing Netty transport (`TransportContext`)
through its public interfaces; no build manifest changes are required. The read-only build baseline
for this feature is:

| Component | Version |
|-----------|---------|
| Apache Spark | 4.1.0-SNAPSHOT |
| Scala | 2.13.17 (binary 2.13) |
| Java | 17 |
| Hadoop | 3.4.2 |
| Netty | 4.2.7.Final |
| Dropwizard Metrics | 4.2.33 |

Because the feature is additive and confined to the `org.apache.spark.shuffle.streaming` package
plus two small integration touchpoints (the configuration registry and the shuffle-manager factory
map entry), it does not change any existing dependency, transport, or metrics infrastructure.
