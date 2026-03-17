---
layout: global
displayTitle: Streaming Shuffle Guide
title: Streaming Shuffle
description: Configuration reference, architecture, and performance tuning guide for the Spark streaming shuffle SPARK_VERSION_SHORT
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

The streaming shuffle is an **opt-in** alternative to the default sort-based shuffle in Apache Spark.
Instead of materializing all shuffle data to disk before reduce tasks can begin reading, the
streaming shuffle streams buffered data directly from map tasks (producers) to reduce tasks
(consumers) through a producer-to-consumer data pipeline. This eliminates shuffle materialization
latency and enables pipelining of the map and reduce phases.

## When to Use Streaming Shuffle

The streaming shuffle is designed for **shuffle-heavy workloads** where shuffle I/O is the primary
bottleneck:

- **Large data volumes**: Workloads shuffling 10 GB or more of data across 100 or more partitions
  benefit the most, achieving **30–50% end-to-end latency reduction**.
- **CPU-bound workloads**: Even workloads that are primarily CPU-bound can see **5–10% improvement**
  through reduced scheduler overhead.
- **Memory-bound workloads**: The streaming shuffle introduces **zero performance regression** for
  memory-bound workloads, thanks to automatic fallback to the sort-based shuffle when memory
  pressure is detected.

## Transparent to Application Code

The streaming shuffle requires **no changes** to RDD, DataFrame, or Dataset APIs. It is activated
purely through configuration. Existing application code continues to work without modification.

## Coexistence with Sort-Based Shuffle

The existing `SortShuffleManager` remains the **default and production-stable** shuffle
implementation. Both the sort-based and streaming shuffle implementations are pluggable via the
`ShuffleManager` trait. Setting `spark.shuffle.manager=streaming` activates the streaming manager;
the default `sort` manager continues to operate as before when the streaming shuffle is not enabled.
The `SortShuffleManager` is completely untouched and serves as the automatic fallback under
degradation conditions.

## Quick Start

To enable the streaming shuffle, set the following two properties in your Spark configuration
(e.g., in `spark-defaults.conf` or via `SparkConf`):

    spark.shuffle.manager=streaming
    spark.shuffle.streaming.enabled=true

Both properties are required. Setting `spark.shuffle.manager=streaming` selects the
`StreamingShuffleManager` class, and `spark.shuffle.streaming.enabled=true` activates the streaming
data pipeline within that manager.

# Configuration Reference

The following table lists all configuration parameters for the streaming shuffle feature. All
properties use the `spark.shuffle.streaming.*` namespace.

<table>
  <thead>
    <tr>
      <th style="width:35%">Property Name</th>
      <th style="width:12%">Default</th>
      <th style="width:8%">Type</th>
      <th style="width:10%">Range</th>
      <th>Description</th>
    </tr>
  </thead>
  <tr>
    <td><code>spark.shuffle.manager</code></td>
    <td><code>sort</code></td>
    <td>String</td>
    <td>N/A</td>
    <td>
      Set to <code>streaming</code> to activate the streaming shuffle manager. Alternatively, use
      the full class name
      <code>org.apache.spark.shuffle.streaming.StreamingShuffleManager</code>.
    </td>
  </tr>
  <tr>
    <td><code>spark.shuffle.streaming.enabled</code></td>
    <td><code>false</code></td>
    <td>Boolean</td>
    <td>N/A</td>
    <td>
      Master toggle for the streaming shuffle. Must be <code>true</code> together with
      <code>spark.shuffle.manager=streaming</code> to activate the streaming data pipeline.
    </td>
  </tr>
  <tr>
    <td><code>spark.shuffle.streaming.bufferSizePercent</code></td>
    <td><code>20</code></td>
    <td>Int</td>
    <td>[1, 50]</td>
    <td>
      Percentage of executor memory allocated for streaming shuffle buffers. Per-partition buffer
      size is calculated as:
      <code>(executorMemory &times; bufferSizePercent) / numPartitions</code>.
    </td>
  </tr>
  <tr>
    <td><code>spark.shuffle.streaming.spillThreshold</code></td>
    <td><code>80</code></td>
    <td>Int</td>
    <td>[50, 95]</td>
    <td>
      Buffer occupancy percentage threshold that triggers automatic disk spill via LRU partition
      eviction. When aggregate buffer utilization across all active streaming shuffles on an
      executor exceeds this threshold, the largest buffered partition is spilled to disk.
    </td>
  </tr>
  <tr>
    <td><code>spark.shuffle.streaming.maxBandwidthMBps</code></td>
    <td><code>0</code> (unlimited)</td>
    <td>Int</td>
    <td>[0, &infin;)</td>
    <td>
      Per-executor bandwidth cap in MB/s for streaming shuffle data transfer. <code>0</code> means
      unlimited. Rate limiting uses a token bucket algorithm with refill rate =
      <code>maxBandwidthMBps / numConcurrentShuffles</code>.
    </td>
  </tr>
  <tr>
    <td><code>spark.shuffle.streaming.debug</code></td>
    <td><code>false</code></td>
    <td>Boolean</td>
    <td>N/A</td>
    <td>
      Enable verbose debug logging for streaming shuffle operations. When enabled, expected log
      volume is &lt;10 MB/hour per executor.
    </td>
  </tr>
</table>

## Configuration Notes

- **Restart Required**: All configuration changes require an executor restart. Dynamic
  reconfiguration is not supported in this version.
- **Memory Safety**: The `bufferSizePercent` value is validated to be within [1, 50] at startup to
  prevent memory exhaustion. Buffer allocation is tracked via the existing `MemoryManager` interface.
- **Spill Safety**: The `spillThreshold` value is validated to be within [50, 95]. Setting the
  threshold too high (above 90) risks out-of-memory conditions under bursty workloads. Setting it
  too low (below 60) causes excessive disk spills and degrades performance.
- **Telemetry Overhead**: Streaming shuffle metric collection adds less than 1% CPU utilization
  overhead. Debug logging is disabled by default and should only be enabled for troubleshooting.

# Architecture

## Producer-to-Consumer Data Pipeline

The streaming shuffle replaces the traditional write-to-disk-then-read model with a buffered
streaming pipeline. The data flow proceeds as follows:

1. **Map Task (Producer)**: The `StreamingShuffleWriter` partitions incoming records by key and
   buffers them in per-partition memory regions. Each partition buffer is sized at
   `(executorMemory × bufferSizePercent) / numPartitions`.

2. **Memory Buffers**: Per-partition buffers are allocated from the executor's execution memory pool
   via the existing `MemoryManager` interface. Memory usage is continuously monitored by the
   `MemorySpillManager`.

3. **Network Streaming**: Once a buffer reaches the 2 MB block size threshold, the buffered data is
   pipelined directly to the consumer executor via the existing Netty-based transport layer
   (`TransportClient` / `TransportServer`). This eliminates the need to write all shuffle data to
   disk before the reduce phase can start.

4. **Reduce Task (Consumer)**: The `StreamingShuffleReader` polls producers for available data
   blocks before the shuffle is fully complete (in-progress block requests). This allows the reduce
   task to begin processing data as soon as it becomes available.

5. **Acknowledgment**: The consumer sends acknowledgment positions (byte offsets) back to the
   producer after successfully receiving and validating each block. The producer uses these
   acknowledgments to reclaim buffer memory.

6. **Data Integrity**: A CRC32C checksum is generated for each 2 MB block on the producer side.
   The consumer validates the checksum upon receipt and requests retransmission if a mismatch is
   detected.

## Backpressure Protocol

The streaming shuffle includes a consumer-to-producer flow control mechanism to prevent fast
producers from overwhelming slow consumers:

- **Heartbeat-based flow control**: The consumer sends position update messages every 5 seconds to
  the producer, indicating how much data has been consumed. The producer uses these updates to
  regulate its send rate.

- **Token bucket rate limiting**: Each executor enforces a per-executor bandwidth cap using a token
  bucket algorithm. The refill rate is `maxBandwidthMBps / numConcurrentShuffles`, ensuring fair
  bandwidth sharing across concurrent shuffles.

- **Buffer utilization monitoring**: The `BackpressureProtocol` tracks aggregate memory usage across
  all active streaming shuffles on the executor. When utilization approaches the spill threshold, it
  signals the `MemorySpillManager` to begin evicting partitions.

- **Priority arbitration**: When multiple shuffles compete for memory, the protocol ranks them by
  `partitionCount × dataVolumeEstimate` and allocates buffer memory proportionally. Higher-priority
  shuffles receive a larger share of available memory.

- **Consumer liveness detection**: If a consumer fails to send a heartbeat within 10 seconds, the
  producer marks the consumer as potentially failed and begins retaining unacknowledged data for
  retransmission.

## Memory Spill Management

The `MemorySpillManager` prevents out-of-memory conditions by automatically spilling buffer data to
disk when memory pressure builds:

- **Polling**: The spill manager polls the execution memory pool utilization every 100 milliseconds.

- **Spill Trigger**: When aggregate buffer occupancy exceeds the configured spill threshold (default
  80%), a spill operation is initiated.

- **Eviction Policy**: LRU (Least Recently Used) eviction selects the largest buffered partition for
  disk spill. This prioritizes reclaiming the most memory in a single eviction.

- **Disk Persistence**: Spilled partition data is persisted via the existing `BlockManager` disk
  storage subsystem (`DiskBlockManager`). The spilled files use the same temporary file allocation
  mechanism as the sort-based shuffle.

- **Buffer Reclamation**: Memory is released within 100 milliseconds of receiving a consumer
  acknowledgment for the corresponding data. This ensures prompt memory recycling.

- **Atomic Spill**: A partition's data is fully written to disk before the corresponding memory is
  released, preventing data loss during the spill operation.

## Component Interaction Diagram

The following diagram shows how the streaming shuffle components interact across the driver and
executor processes:

```
Driver
├── SparkContext
│   └── SparkEnv.initializeShuffleManager()
│       └── StreamingShuffleManager.create(conf, isDriver)
└── MapOutputTrackerMaster (shuffle location tracking)

Producer Executor
├── ShuffleWriteProcessor.write()
│   └── StreamingShuffleManager.getWriter()
│       └── StreamingShuffleWriter
│           ├── Memory Buffers (per-partition)
│           ├── BackpressureProtocol (flow control)
│           ├── MemorySpillManager (spill to disk)
│           └── TransportClient (network streaming)
└── StreamingShuffleBlockResolver (block resolution)

Consumer Executor
├── StreamingShuffleManager.getReader()
│   └── StreamingShuffleReader
│       ├── In-progress Block Polling
│       ├── CRC32C Checksum Validation
│       ├── Acknowledgment Protocol
│       └── ExternalSorter (aggregation/ordering)
└── MapOutputTrackerWorker (location queries)

Network Layer (Netty-based)
    TransportClient ←→ TransportServer
```

The driver initializes the `StreamingShuffleManager` during `SparkEnv` creation. Producer executors
use the manager to obtain `StreamingShuffleWriter` instances, which buffer and stream partition data
to consumer executors. Consumer executors obtain `StreamingShuffleReader` instances that poll
producers for in-progress data, validate checksums, and send acknowledgments. The
`MapOutputTracker` coordinates shuffle block locations between producers and consumers.

# Protocol Specification

## Timing Parameters

The following timing parameters govern the streaming shuffle protocol:

<table>
  <thead>
    <tr>
      <th style="width:40%">Parameter</th>
      <th style="width:15%">Value</th>
      <th>Purpose</th>
    </tr>
  </thead>
  <tr>
    <td>Consumer-to-producer heartbeat interval</td>
    <td>5 seconds</td>
    <td>Flow control position updates from consumer to producer</td>
  </tr>
  <tr>
    <td>Consumer liveness heartbeat timeout</td>
    <td>10 seconds</td>
    <td>Failure detection for unresponsive consumers</td>
  </tr>
  <tr>
    <td>Producer failure connection timeout</td>
    <td>5 seconds</td>
    <td>Detection of crashed producer executors</td>
  </tr>
  <tr>
    <td>Block size</td>
    <td>2 MB</td>
    <td>Pipelining efficiency for network transfers</td>
  </tr>
  <tr>
    <td>Retry base interval</td>
    <td>1 second</td>
    <td>Exponential backoff starting point for retransmission</td>
  </tr>
  <tr>
    <td>Max retry attempts</td>
    <td>5</td>
    <td>Maximum retransmission attempts per block before failure</td>
  </tr>
  <tr>
    <td>Memory spill polling interval</td>
    <td>100 ms</td>
    <td>Buffer utilization monitoring frequency</td>
  </tr>
  <tr>
    <td>Buffer reclamation target</td>
    <td>100 ms</td>
    <td>Time to release memory after consumer acknowledgment</td>
  </tr>
  <tr>
    <td>Rate limiting capacity</td>
    <td>80% link capacity</td>
    <td>Token bucket fill rate ceiling</td>
  </tr>
</table>

## Block Transfer Protocol

The streaming shuffle uses the following protocol for transferring data blocks from producer to
consumer:

1. **Block Construction**: The producer serializes records into 2 MB blocks. A CRC32C checksum is
   computed over the block payload and appended to the block header.

2. **Block Transmission**: Blocks are transmitted via the existing Netty-based transport layer using
   `TransportClient.sendRpc()` or stream upload. The transport layer handles framing, connection
   management, and TCP-level reliability.

3. **Checksum Validation**: Upon receipt, the consumer recomputes the CRC32C checksum over the
   received payload and compares it with the producer's checksum. If the checksums do not match, the
   consumer requests retransmission of the corrupted block.

4. **Acknowledgment**: After successful validation, the consumer sends an acknowledgment message
   containing the byte offset of the last successfully received position. The producer uses this
   offset to reclaim buffer memory for the acknowledged data.

5. **Connection Health**: TCP keepalive is enabled with a 5-second interval to detect connection
   failures promptly.

## Retry Policy

The streaming shuffle uses exponential backoff for retransmission and reconnection:

- **Backoff schedule**: 1s, 2s, 4s, 8s, 16s (doubling with each attempt, up to 5 attempts maximum)
- **Applies to**: Block retransmission after checksum failure, and connection re-establishment after
  timeout
- **Terminal failure**: After 5 consecutive failed attempts, the reader throws a
  `FetchFailedException` which propagates to the `DAGScheduler` to trigger recomputation of the
  upstream `ShuffleMapStage`

# Failure Handling

The streaming shuffle guarantees **zero data loss** under all failure scenarios. The following
sections describe the handling flow for each failure type.

## Producer Failure

When a producer executor crashes or becomes unreachable during a streaming shuffle:

1. The `StreamingShuffleReader` detects a connection timeout (5 seconds) on the Netty
   `TransportClient` channel to the failed producer.
2. The reader atomically invalidates all partial reads from the failed producer and discards any
   buffered data that has not been fully validated.
3. The reader throws a `FetchFailedException`, which the `TaskContext` captures via
   `setFetchFailed()`.
4. The `DAGScheduler` receives the `FetchFailed` event and triggers recomputation of the upstream
   `ShuffleMapStage`.
5. The recomputed map task uses the configured shuffle manager (streaming or fallback) to rewrite
   the shuffle output.

**Zero data loss guarantee**: All partial data is safely discarded. Complete recomputation of the
upstream stage ensures correctness.

## Consumer Failure

When a consumer executor crashes or becomes unresponsive:

1. The `StreamingShuffleWriter` detects missing acknowledgment heartbeats after a 10-second timeout.
2. The writer retains all unacknowledged data in memory buffers.
3. If the buffer exceeds the spill threshold, the `MemorySpillManager` triggers a disk spill via
   `BlockManager` to prevent memory exhaustion on the producer.
4. When the consumer reconnects (after the `DAGScheduler` resubmits the reduce task), the writer
   retransmits unacknowledged blocks from memory or spilled disk files.
5. If the consumer permanently fails, the `DAGScheduler` resubmits the reduce task to a different
   executor.

**Zero data loss guarantee**: Unacknowledged data is preserved in memory or on disk until
successfully consumed by the resubmitted reduce task.

## Network Partition

When a network partition separates the producer and consumer:

1. Both the producer and consumer detect connection loss via TCP keepalive timeout.
2. The producer retains all buffered data and spills to disk if memory pressure builds during the
   partition.
3. The consumer throws a `FetchFailedException` after the 5-second connection timeout.
4. The `DAGScheduler` triggers upstream recomputation of the affected `ShuffleMapStage`.
5. If the network partition persists, the automatic fallback mechanism may activate for subsequent
   shuffle registrations (see [Automatic Fallback](#automatic-fallback)).

**Automatic fallback**: Sustained network issues lasting more than 60 seconds trigger a per-shuffle
fallback to the sort-based shuffle.

## Memory Exhaustion

When memory pressure threatens executor stability:

1. The `MemorySpillManager` detects buffer utilization exceeding the configured spill threshold.
2. LRU eviction selects the largest buffered partition for disk spill.
3. If spilling cannot free sufficient memory (e.g., all partitions are already spilled), the
   streaming shuffle signals a fallback condition to the `StreamingShuffleManager`.
4. The `StreamingShuffleManager` routes subsequent shuffle registrations to sort-based shuffle
   behavior, preventing further memory pressure.

**Zero OOM guarantee**: The combination of automatic spill and fallback prevents executor crashes
due to memory exhaustion from streaming shuffle buffers.

# Performance Tuning

## Expected Performance

The streaming shuffle targets the following performance improvements over the default sort-based
shuffle:

- **Shuffle-heavy workloads** (10 GB+ data, 100+ partitions): **30–50% end-to-end latency
  reduction** by eliminating shuffle materialization to disk and pipelining map and reduce phases.
- **CPU-bound workloads**: **5–10% improvement** through reduced scheduler overhead and fewer disk
  I/O operations.
- **Memory-bound workloads**: **Zero performance regression** via automatic detection of memory
  pressure and fallback to the sort-based shuffle.

These are target performance envelopes. Actual improvement depends on cluster configuration,
data characteristics, and workload patterns.

## Buffer Sizing Recommendations

The `spark.shuffle.streaming.bufferSizePercent` parameter controls how much executor memory is
allocated for streaming shuffle buffers. The optimal value depends on your workload:

- **Large shuffles (&gt;10 GB)**: Use `bufferSizePercent=30` to `40` with sufficient executor memory
  (16 GB or more). Larger buffers reduce the frequency of network round trips and disk spills.
- **Medium shuffles (1–10 GB)**: The default `bufferSizePercent=20` is usually optimal and balances
  buffer capacity with memory available for other operations (caching, aggregation).
- **Many concurrent shuffles**: Lower `bufferSizePercent` to `10` to `15` to share memory across
  concurrent shuffles and prevent contention.
- **Formula reminder**: Per-partition buffer size =
  `(executorMemory × bufferSizePercent%) / numPartitions`. For example, with 8 GB executor memory,
  `bufferSizePercent=20`, and 200 partitions: `(8 GB × 0.20) / 200 = 8 MB` per partition.

## Spill Threshold Optimization

The `spark.shuffle.streaming.spillThreshold` parameter controls when buffer data is spilled to
disk:

- **Default (80%)**: Conservative and appropriate for most workloads. Provides a reasonable safety
  margin while minimizing spill overhead.
- **Low-memory environments**: Lower to `60` to `70` to provide a wider safety margin against
  out-of-memory conditions.
- **High-memory environments**: Raise to `85` to `90` to reduce spill frequency and maximize
  in-memory throughput.
- **Bursty workloads**: Keep at the default `80` or lower to handle sudden memory spikes caused by
  uneven partition sizes.
- **CAUTION**: Never set above `95`. Values above 90 risk out-of-memory conditions under concurrent
  memory pressure from multiple shuffles, caching, and task execution.

## Network Bandwidth Tuning

The `spark.shuffle.streaming.maxBandwidthMBps` parameter controls per-executor bandwidth usage:

- **Default (`0`, unlimited)**: Appropriate when the network is dedicated to the Spark cluster or
  when network bandwidth is not a constraint.
- **Shared networks**: Set to approximately 80% of the available per-executor bandwidth to leave
  headroom for non-shuffle traffic (heartbeats, broadcast, task scheduling).
- **Token bucket fairness**: The token bucket algorithm ensures fair bandwidth sharing across
  concurrent shuffles. Each shuffle receives a rate of
  `maxBandwidthMBps / numConcurrentShuffles`.
- **Example**: With `maxBandwidthMBps=800` and 4 concurrent shuffles, each shuffle receives a
  200 MB/s rate limit.

# Automatic Fallback

The streaming shuffle automatically falls back to sort-based shuffle behavior when runtime
conditions indicate that streaming is not beneficial or risks stability. Fallback is evaluated
per-shuffle, not globally — other concurrent shuffles may continue streaming if their conditions
are healthy.

## Fallback Conditions

The following conditions trigger automatic fallback to the sort-based shuffle:

1. **Consumer Rate Degradation**: The consumer processing rate is sustained at 2x slower than the
   producer rate for more than 60 seconds. This indicates that streaming is not providing a latency
   benefit because the consumer cannot keep up with the producer.

2. **Memory Pressure**: Memory pressure prevents buffer allocation, indicating an out-of-memory
   risk. The `MemorySpillManager` has exhausted its ability to free memory through spills.

3. **Network Saturation**: Network utilization exceeds 90% of link capacity. At this level,
   additional streaming traffic would degrade overall cluster communication including heartbeats
   and task scheduling.

4. **Version Mismatch**: The producer and consumer are running incompatible versions of the
   streaming shuffle protocol. This can occur during rolling upgrades.

## Fallback Behavior

- Fallback is **per-shuffle**, not global. Other shuffles on the same executor may continue using
  the streaming path if their conditions are healthy.
- In-flight streaming shuffles complete gracefully or spill all buffered data to disk before the
  fallback takes effect for new shuffle registrations.
- Fallback events are logged at **WARN** level. Check executor logs for messages containing
  `"Falling back to sort-based shuffle"` to identify when fallback is activated.
- See the [Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html) guide for
  resolution steps when fallback is triggered.

# See Also

- [Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html) — Common issues,
  telemetry interpretation, debugging procedures, and known limitations
- [Spark Configuration](configuration.html) — General Spark configuration reference
- [Tuning Spark](tuning.html) — General performance tuning guide
- [Monitoring and Instrumentation](monitoring.html) — Metrics system and JMX configuration for
  monitoring streaming shuffle telemetry
