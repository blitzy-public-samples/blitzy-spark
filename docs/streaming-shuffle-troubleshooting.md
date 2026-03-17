---
layout: global
displayTitle: Streaming Shuffle Troubleshooting
title: Streaming Shuffle Troubleshooting
description: Troubleshooting guide for Spark streaming shuffle SPARK_VERSION_SHORT
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

This guide helps operators and developers diagnose and resolve issues when using the Spark
streaming shuffle feature. The streaming shuffle is an opt-in alternative to the default
sort-based shuffle that eliminates shuffle materialization latency by streaming buffered data
directly from map tasks to reduce tasks. It is activated by setting
`spark.shuffle.manager=streaming` and `spark.shuffle.streaming.enabled=true`.

The streaming shuffle coexists with the default sort-based shuffle (`SortShuffleManager`).
When streaming shuffle is not enabled or when automatic fallback conditions are triggered,
Spark seamlessly uses the sort-based shuffle path. All streaming shuffle logic resides in the
dedicated `org.apache.spark.shuffle.streaming` package and does not affect existing shuffle
code paths.

# Common Issues and Resolution

## Streaming Shuffle Not Activating

The streaming shuffle requires **both** of the following configuration properties to be set:

- `spark.shuffle.manager=streaming`
- `spark.shuffle.streaming.enabled=true`

If either property is missing or set incorrectly, Spark falls back to the default sort-based
shuffle manager without any error message.

**Important:** Configuration changes require an executor restart. There is no dynamic
reconfiguration support in v1 — you cannot toggle streaming shuffle on or off without
stopping and restarting all executors.

### Verification Checklist

1. **Confirm `spark.shuffle.manager` is set to `streaming`** (not `sort` or any other value).
   Check your `spark-defaults.conf`, `SparkConf` object, or `--conf` command-line arguments.

2. **Confirm `spark.shuffle.streaming.enabled` is `true`.**
   This property defaults to `false`. It must be explicitly set to `true`.

3. **Confirm executors were restarted after the configuration change.**
   If you changed configuration on a running cluster, executors must be restarted for the new
   settings to take effect.

4. **Check SparkContext logs for initialization messages.**
   When streaming shuffle activates successfully, the executor logs will contain:
   ```
   INFO StreamingShuffleManager: StreamingShuffleManager initialized
   ```
   If you do not see this message, the streaming shuffle manager was not loaded.

## Excessive Disk Spills

Disk spills occur when the in-memory streaming buffer occupancy exceeds the configurable spill
threshold. By default, spills are triggered when buffer utilization reaches **80%** of the
allocated streaming buffer memory.

### Symptoms

- Elevated `shuffle.streaming.spillCount` metric (see [Telemetry Interpretation](#telemetry-interpretation))
- Increased disk I/O on executor nodes
- Degraded shuffle performance approaching sort-based shuffle latency

### Resolution

1. **Increase buffer allocation.** Tune `spark.shuffle.streaming.bufferSizePercent` (range: 1–50,
   default: 20) to allocate a larger portion of executor memory to streaming buffers. For example,
   setting it to `30` allocates 30% of executor memory for streaming shuffle buffers.

2. **Raise the spill threshold.** Tune `spark.shuffle.streaming.spillThreshold` (range: 50–95,
   default: 80) to allow higher buffer utilization before triggering a spill. Setting it to `90`
   or `95` is appropriate for workloads with bursty memory patterns where brief spikes are normal.

3. **Increase executor memory.** If the workload consistently requires more buffer space, consider
   increasing `spark.executor.memory` to provide a larger absolute buffer size.

4. **Reduce partition count.** Fewer partitions means larger per-partition buffer allocations
   (formula: `executorMemory × bufferSizePercent / numPartitions`), which reduces spill frequency.

> **Caution:** Setting `spillThreshold` too high (e.g., 95) increases the risk of
> out-of-memory errors if buffer utilization spikes suddenly. Monitor the
> `shuffle.streaming.bufferUtilizationPercent` gauge closely when adjusting this parameter.

## Backpressure Throttling

Backpressure activates when the consumer (reduce task) processing rate drops significantly
relative to the producer (map task) output rate. The streaming shuffle uses a token bucket
algorithm to enforce per-executor rate limiting, preventing producers from overwhelming slow
consumers.

### Symptoms

- Elevated `shuffle.streaming.backpressureEvents` counter
- Map tasks appear to stall or slow down despite available CPU
- Network bandwidth between executors is not fully utilized

### Resolution

1. **Check network bandwidth between executors.** Backpressure may indicate genuine network
   congestion. Use system-level tools (`iftop`, `sar`, `nload`) to verify available bandwidth.

2. **Review the number of concurrent shuffles.** The token bucket rate is calculated as
   `maxBandwidthMBps / numConcurrentShuffles`. When many shuffles compete for bandwidth,
   each gets a smaller allocation. Reduce job parallelism or stagger shuffle-heavy stages.

3. **Set an explicit bandwidth limit.** Configure `spark.shuffle.streaming.maxBandwidthMBps` to
   control per-executor bandwidth allocation. The default value of `0` means unlimited, but
   setting an explicit value helps prevent network saturation across concurrent shuffles.

4. **Reduce partition count.** Fewer partitions reduces per-partition data volume and the
   overall streaming throughput demand, alleviating backpressure conditions.

5. **Check consumer-side bottlenecks.** If reduce tasks perform expensive aggregations or
   transformations, they may be inherently slower than producers. Consider optimizing the
   reduce-side logic or increasing reduce-side parallelism.

## Checksum Failures and Data Integrity

The streaming shuffle uses CRC32C checksums to validate the integrity of every data block
transferred between producers and consumers. Each 2MB block is checksummed by the writer
and validated by the reader upon receipt.

### Symptoms

- Elevated `shuffle.streaming.checksumFailures` counter (if exposed in custom telemetry)
- Log messages: `"Checksum mismatch for block X"` at WARN level
- Automatic retransmission requests from the reader to the writer

### Resolution

1. **Check network health.** Checksum failures most commonly indicate network-level packet
   corruption. Inspect switch error counters, NIC statistics (`ethtool -S`), and cable integrity.

2. **Verify executor JVM stability.** Long GC pauses can corrupt in-flight memory buffers if
   the JVM is under severe memory pressure. Review GC logs for stop-the-world pauses exceeding
   100ms during active shuffle periods.

3. **Understand the retry mechanism.** Blocks with failed checksums are automatically
   retransmitted using exponential backoff (starting at 1 second, maximum 5 attempts). Transient
   corruption is typically resolved by retransmission without operator intervention.

4. **If persistent, fall back to sort-based shuffle.** If checksum failures persist after
   addressing network and JVM issues, switch back to `spark.shuffle.manager=sort` to use the
   disk-materialized shuffle path, which is not subject to in-flight memory corruption.

## Automatic Fallback Triggering Unexpectedly

The streaming shuffle includes automatic fallback logic that reverts individual shuffles to
sort-based behavior when runtime conditions degrade. Fallback is **per-shuffle** — other
concurrent shuffles may continue using the streaming path if their conditions are healthy.

### Fallback Conditions

The following conditions trigger automatic fallback:

1. **Consumer sustained 2× slower than producer for >60 seconds.** When the consumer
   consistently processes data at less than half the producer's output rate for more than one
   minute, the shuffle falls back to avoid unbounded buffer growth.

2. **Memory pressure prevents buffer allocation (OOM risk).** When the executor's execution
   memory pool cannot satisfy streaming buffer allocation requests, the shuffle falls back to
   disk-based materialization.

3. **Network saturation exceeds 90% link capacity.** When network utilization on the executor's
   link exceeds 90%, additional streaming shuffles fall back to prevent packet loss and
   retransmission storms.

4. **Producer/consumer version mismatch.** When executors running different Spark versions
   attempt a streaming shuffle, a compatibility check triggers fallback to the universally
   supported sort-based path.

### Resolution

1. **Review the `shuffle.streaming.bufferUtilizationPercent` metric.** If it is consistently
   near 100%, increase `spark.shuffle.streaming.bufferSizePercent` to provide more buffer room.

2. **Review executor memory allocation.** Ensure the execution memory pool has sufficient
   capacity. Check `spark.memory.fraction` and `spark.memory.storageFraction` to verify
   adequate execution memory is available for streaming buffers.

3. **Understand that fallback is per-shuffle, not global.** Other shuffles in the same
   application may continue streaming normally. There is no need to disable streaming shuffle
   globally unless fallback is triggered for every shuffle.

4. **Check logs for fallback messages.** Search executor logs for:
   ```
   WARN StreamingShuffleManager: Falling back to sort-based shuffle
   ```
   The log message includes the shuffle ID and the specific condition that triggered fallback.

# Telemetry Interpretation

The streaming shuffle exposes operational metrics via JMX through the Spark metrics system
(Dropwizard Metrics). These metrics provide real-time visibility into streaming shuffle health
and performance.

## JMX Metrics Reference

| Metric Name | Type | Description | Normal Range | Concerning Threshold |
|---|---|---|---|---|
| `shuffle.streaming.bufferUtilizationPercent` | Gauge | Real-time buffer occupancy as a percentage of allocated streaming buffer memory | 20–70% | >85% sustained |
| `shuffle.streaming.spillCount` | Counter | Cumulative disk spill events triggered by memory pressure | 0–10 per shuffle | >50 per shuffle |
| `shuffle.streaming.backpressureEvents` | Counter | Consumer rate limiting incidents where the producer was throttled | 0–5 per shuffle | >20 per shuffle |
| `shuffle.streaming.partialReadInvalidations` | Counter | Producer failure detection events where partial reads were discarded | 0 | Any non-zero value |

## Monitoring via Prometheus and Grafana

All streaming shuffle metrics are exposed through JMX and are accessible via the Spark metrics
system's sink infrastructure. To integrate with Prometheus and Grafana:

1. **Configure the Prometheus sink.** Spark supports a Prometheus servlet sink that exposes
   metrics in Prometheus format. See the [Monitoring and Instrumentation](monitoring.html) guide
   for detailed sink configuration instructions.

2. **Suggested Grafana dashboards:**
   - **Buffer Utilization Over Time** — Plot the `shuffle.streaming.bufferUtilizationPercent`
     gauge to visualize memory pressure trends across executors.
   - **Spill Rate** — Use the rate (derivative) of `shuffle.streaming.spillCount` to identify
     spill frequency spikes correlated with specific stages or workloads.
   - **Backpressure Event Frequency** — Use the rate of `shuffle.streaming.backpressureEvents`
     to detect sustained consumer-side bottlenecks.
   - **Partial Read Invalidations** — Alert on any non-zero value of
     `shuffle.streaming.partialReadInvalidations`, as this indicates producer executor failures.

## Interpreting Metric Patterns

### Healthy Operation

A well-tuned streaming shuffle exhibits the following metric patterns:

- **Buffer utilization fluctuates between 20–70%.** This indicates adequate buffer headroom
  without memory waste. Utilization naturally varies with data distribution across partitions.
- **Spills are infrequent** — fewer than 10 per shuffle stage. Occasional spills are normal
  during peak data bursts and do not indicate a problem.
- **Backpressure events are rare** — fewer than 5 per shuffle stage. Occasional backpressure
  during concurrent shuffle overlap is expected.
- **Zero partial read invalidations.** This metric should remain at zero under normal
  operation, indicating no producer executor failures.

### Concerning Patterns

The following patterns warrant investigation and tuning:

- **Buffer utilization consistently above 85%.** The streaming buffers are under sustained
  pressure. **Action:** Increase `spark.shuffle.streaming.bufferSizePercent` or reduce the
  partition count to increase per-partition buffer allocation.

- **Spill count exceeding 50 per shuffle.** Frequent spills erode the latency advantage of
  streaming shuffle. **Action:** Increase executor memory allocation, increase
  `spark.shuffle.streaming.bufferSizePercent`, or raise `spark.shuffle.streaming.spillThreshold`
  to reduce spill frequency.

- **Frequent backpressure events (>20 per shuffle).** This indicates a sustained throughput
  mismatch between producers and consumers. **Action:** Check network bandwidth, reduce
  concurrent shuffle count, or investigate consumer-side processing bottlenecks.

- **Any partial read invalidations (>0).** This indicates at least one producer executor
  failed during an active shuffle. **Action:** Investigate executor stability — check for
  OOM kills, hardware failures, or preemption events. The streaming shuffle automatically
  handles these failures via `FetchFailedException` and DAG recomputation, but frequent
  invalidations indicate an infrastructure problem.

# Debugging Procedures

## Enabling Debug Logging

Verbose debug logging for the streaming shuffle is controlled by the
`spark.shuffle.streaming.debug` configuration property:

```
spark.shuffle.streaming.debug=true
```

Debug logging is **disabled by default** to minimize log volume. When enabled, the expected
log output volume is less than **10MB per hour per executor**, keeping operational overhead
manageable.

> **Note:** Streaming shuffle telemetry (including metrics collection and debug logging)
> adds less than 1% CPU overhead per executor.

### What Debug Logging Captures

When debug logging is enabled, the following additional information is written to executor logs:

- **Per-block transfer events** with timestamps, block sizes, and destination executor IDs
- **Backpressure protocol heartbeat messages** including consumer position updates and
  token bucket refill events
- **Memory spill trigger decisions** with current utilization percentages and selected
  partition IDs for eviction
- **Checksum computation and validation results** for every transferred block
- **Consumer acknowledgment positions** showing buffer reclamation progress

## Key Log Messages Reference

The following log messages are emitted by the streaming shuffle components. Use these
messages to diagnose specific conditions:

| Log Message | Level | Component | Meaning |
|---|---|---|---|
| `StreamingShuffleManager initialized` | INFO | StreamingShuffleManager | Streaming shuffle manager loaded and activated successfully |
| `Buffer utilization exceeded threshold` | WARN | MemorySpillManager | Buffer occupancy exceeded the configured `spillThreshold`; disk spill initiated |
| `Backpressure activated for shuffle <id>` | INFO | BackpressureProtocol | Consumer rate limiting engaged for a specific shuffle; producer output rate throttled |
| `Producer failure detected for executor <id>` | WARN | StreamingShuffleReader | Connection timeout after 5 seconds; producer executor presumed failed |
| `Partial read invalidated for shuffle <id>, map <id>` | WARN | StreamingShuffleReader | Incomplete data from a failed producer discarded; triggers upstream recomputation |
| `Falling back to sort-based shuffle` | WARN | StreamingShuffleManager | Automatic fallback condition met; this shuffle reverts to sort-based behavior |
| `Checksum mismatch for block <id>` | WARN | StreamingShuffleReader | CRC32C validation failed for a received block; retransmission requested |

## Step-by-Step Debugging Workflow

Follow this workflow to diagnose streaming shuffle issues:

1. **Enable debug logging.**
   Set `spark.shuffle.streaming.debug=true` in your Spark configuration and restart executors.

2. **Verify streaming shuffle activation.**
   Check executor logs for the `"StreamingShuffleManager initialized"` message. If this
   message is absent, review the [Streaming Shuffle Not Activating](#streaming-shuffle-not-activating)
   section above.

3. **Monitor JMX metrics for anomalies.**
   Connect to executor JMX endpoints or review Prometheus/Grafana dashboards for the four
   key streaming shuffle metrics described in [Telemetry Interpretation](#telemetry-interpretation).

4. **Investigate excessive spills.**
   If the `shuffle.streaming.spillCount` counter is elevated, check the
   `shuffle.streaming.bufferUtilizationPercent` gauge. Sustained utilization above 85%
   indicates the need for buffer tuning.

5. **Investigate frequent backpressure.**
   If `shuffle.streaming.backpressureEvents` is elevated, verify network bandwidth between
   executors and review the number of concurrent shuffles sharing the bandwidth allocation.

6. **Investigate failures.**
   If the application experiences shuffle failures, search logs for `"Producer failure detected"`
   or `"Partial read invalidated"` messages. These indicate executor-level failures that
   trigger DAG recomputation via `FetchFailedException`.

7. **Collect heap dump if memory leak is suspected.**
   The streaming shuffle is designed for zero memory leaks under all failure scenarios. If a
   memory leak is suspected, collect a heap dump using `jmap` or JMX and analyze retained
   objects in the `org.apache.spark.shuffle.streaming` package.

# Known Limitations

The following limitations apply to the streaming shuffle feature in the current version:

1. **No Dynamic Reconfiguration.**
   All streaming shuffle configuration changes require an executor restart. You cannot toggle
   streaming shuffle on or off, or adjust parameters like `bufferSizePercent` or
   `spillThreshold`, without stopping and restarting executors. Dynamic reconfiguration is
   not supported in v1.

2. **No Push-Based Shuffle Support.**
   Streaming shuffle mode does not support push-based shuffle. The `ShuffleBlockPusher` code
   path is skipped for `StreamingShuffleHandle` instances. Push-based shuffle remains fully
   available when using the default sort-based shuffle manager
   (`spark.shuffle.manager=sort`).

3. **No Merge Support for Streaming Blocks.**
   The `StreamingShuffleBlockResolver` does not support merged shuffle block operations.
   Calls to `getMergedBlockData()` and `getMergedBlockMeta()` return empty results. Merged
   block functionality is available only with the sort-based shuffle manager.

4. **Executor Memory Ceiling for Buffers.**
   Streaming buffers are limited to a configurable percentage of executor memory, with a
   range of 1–50% (default: 20%). Workloads that require buffer allocation beyond this
   ceiling must either increase executor memory (`spark.executor.memory`) or use the
   sort-based shuffle. The per-partition buffer size follows the formula:
   `(executorMemory × bufferSizePercent) / numPartitions`.

5. **Automatic Fallback Is Per-Shuffle.**
   When automatic fallback conditions are triggered, only the specific shuffle experiencing
   issues reverts to sort-based behavior. Other concurrent shuffles in the same application
   may continue using the streaming path. This means mixed-mode operation (some shuffles
   streaming, some sort-based) is possible within a single application run.

6. **Telemetry Overhead.**
   Streaming shuffle telemetry collection (metrics gauges, counters, and optional debug
   logging) adds less than 1% CPU overhead per executor. While negligible for most workloads,
   this overhead is non-zero and should be accounted for in performance-sensitive environments.

# Related Documentation

For additional information on configuring, monitoring, and tuning the streaming shuffle,
refer to the following documentation:

- **[Streaming Shuffle Guide](streaming-shuffle-guide.html)** — Comprehensive configuration
  reference, architecture design document, protocol specification, and performance tuning
  recommendations.
- **[Spark Configuration](configuration.html)** — General Spark configuration reference,
  including all `spark.shuffle.*` properties.
- **[Monitoring and Instrumentation](monitoring.html)** — Spark metrics system setup,
  Prometheus sink configuration, and JMX monitoring.
- **[Tuning Spark](tuning.html)** — General performance tuning guide covering memory
  management, serialization, data locality, and shuffle optimization.
