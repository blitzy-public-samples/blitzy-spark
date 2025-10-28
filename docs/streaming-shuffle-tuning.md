---
layout: global
title: Streaming Shuffle Performance Tuning
displayTitle: Streaming Shuffle Performance Tuning Guide
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

* This will become a table of contents (this text will be scrapped).
{:toc}

# Streaming Shuffle Performance Tuning Guide

## Introduction

The streaming shuffle feature in Apache Spark 4.1.0+ enables shuffle data to flow directly from map tasks to reduce tasks without full materialization to disk, providing 30-50% latency reduction for shuffle-heavy workloads. However, optimal performance requires careful tuning of memory buffers, spill thresholds, and network bandwidth parameters based on your specific workload characteristics.

This guide provides concrete recommendations for tuning streaming shuffle configuration to achieve optimal performance while maintaining stability and resource efficiency. The key configuration parameters are:

- `spark.shuffle.streaming.bufferSizePercent`: Percentage of executor memory allocated to streaming buffers (1-50%, default 20%)
- `spark.shuffle.streaming.spillThreshold`: Buffer utilization percentage that triggers disk spill (50-95%, default 80%)
- `spark.shuffle.streaming.maxBandwidthMBps`: Maximum network bandwidth for shuffle transfers (optional rate limiting)

### When to Tune vs Accept Defaults

**Accept defaults** when:
- Standard production workloads with 1-10GB shuffles and 100-500 partitions
- Executor memory is adequate (8GB+ per executor)
- Network bandwidth is not a constraint
- Initial deployment and performance validation phase

**Tune parameters** when:
- Performance metrics indicate frequent spills or memory pressure
- Network saturation detected (>80% link utilization)
- Workload characteristics significantly differ from standard patterns
- Specific latency or throughput targets must be met

Always start with defaults and tune incrementally based on observed metrics from your actual workload.

## Buffer Sizing Recommendations

Streaming shuffle allocates per-partition memory buffers to stream data directly to consumers. The buffer size per partition is calculated as:

```
Buffer per partition = (executor memory × bufferSizePercent / 100) / numPartitions
```

### Small Shuffles (<1GB, <100 partitions)

**Recommended configuration:**
```properties
spark.shuffle.streaming.bufferSizePercent=10-15
```

**Rationale**: Small shuffles benefit minimally from streaming but consume memory that could be used for computation. Lower buffer allocation minimizes overhead while still enabling streaming for qualifying operations.

**Example**: 4GB executor, 50 partitions
- bufferSizePercent=10: 4GB × 0.10 / 50 = 8.2MB per partition
- Total buffer memory: 410MB (10% of executor memory)

**Expected performance**: 15-25% latency reduction, minimal memory impact

### Medium Shuffles (1-10GB, 100-500 partitions)

**Recommended configuration:**
```properties
spark.shuffle.streaming.bufferSizePercent=20
```

**Rationale**: This is the default and optimal for most production workloads. It provides sufficient buffering to absorb network latency variations while leaving adequate memory for task execution and other Spark operations.

**Example**: 8GB executor, 200 partitions
- bufferSizePercent=20: 8GB × 0.20 / 200 = 8.2MB per partition
- Total buffer memory: 1.6GB (20% of executor memory)

**Expected performance**: 30-40% latency reduction, balanced memory usage

### Large Shuffles (>10GB, >500 partitions)

**Recommended configuration:**
```properties
spark.shuffle.streaming.bufferSizePercent=25-30
```

**Rationale**: Large shuffles with many partitions benefit most from streaming. Higher buffer allocation reduces spill frequency, improving overall throughput. The larger executor memory typical of these workloads can accommodate higher buffer percentages.

**Example**: 16GB executor, 1000 partitions
- bufferSizePercent=30: 16GB × 0.30 / 1000 = 4.9MB per partition
- Total buffer memory: 4.8GB (30% of executor memory)

**Expected performance**: 40-50% latency reduction, fewer spills

### Memory-Constrained Executors

**Recommended configuration:**
```properties
spark.shuffle.streaming.bufferSizePercent=5-10
spark.shuffle.streaming.spillThreshold=65
```

**Rationale**: When executor memory is limited (<4GB) or shared with memory-intensive operations (e.g., caching large datasets, ML model training), reduce buffer allocation and enable more aggressive spilling to prevent out-of-memory errors.

**Example**: 2GB executor, 100 partitions
- bufferSizePercent=8: 2GB × 0.08 / 100 = 1.6MB per partition
- Total buffer memory: 160MB (8% of executor memory)

**Expected performance**: 10-20% latency reduction, frequent but manageable spills

### Partition Count Considerations

**Rule of thumb**: Aim for 4-16MB per partition buffer for optimal streaming performance.

- **<4MB per partition**: Increase bufferSizePercent or reduce partition count (repartition)
- **4-16MB per partition**: Optimal range, no tuning needed
- **>16MB per partition**: Consider reducing bufferSizePercent to free memory for other operations

**Validation query**:
```scala
val executorMemory = sc.getConf.get("spark.executor.memory")
val bufferPercent = sc.getConf.get("spark.shuffle.streaming.bufferSizePercent", "20").toInt
val numPartitions = df.rdd.getNumPartitions
val bufferPerPartition = (executorMemory * bufferPercent / 100.0) / numPartitions
println(s"Buffer per partition: ${bufferPerPartition}MB")
```

### Executor Memory Sizing Guidance

For streaming shuffle workloads, allocate executor memory according to:

```
Recommended executor memory = (baseline memory) + (shuffle buffer memory) + (20% safety margin)
```

**Example calculation**:
- Baseline for task execution: 4GB
- Shuffle buffer (20% for 500 partitions): 2GB
- Safety margin: 1.2GB
- **Total recommended**: 8GB per executor

Monitor `shuffle.streaming.bufferUtilization` metrics to verify buffers are not chronically underutilized (indicating over-allocation) or constantly at threshold (indicating under-allocation).

## Spill Threshold Optimization

The spill threshold determines when streaming shuffle buffers are persisted to disk to prevent memory exhaustion. Tuning this parameter balances memory safety against spill overhead.

### Conservative (spillThreshold: 60-70%)

**Recommended configuration:**
```properties
spark.shuffle.streaming.spillThreshold=65
```

**Use when:**
- Executor memory is limited or variable across the cluster
- Workload has unpredictable shuffle data volumes
- High-priority jobs that must not fail due to OOM
- Initial deployment phase with unknown characteristics

**Trade-off**: More frequent spills increase I/O overhead, reducing throughput by 10-15% compared to higher thresholds. However, significantly reduces risk of executor failure due to memory pressure.

**Monitoring**: If `shuffle.streaming.spillCount` exceeds 5 spills per shuffle per executor, and memory utilization stays below 70%, consider increasing threshold.

### Balanced (spillThreshold: 80%, default)

**Recommended configuration:**
```properties
spark.shuffle.streaming.spillThreshold=80
```

**Use when:**
- Standard production workloads with predictable patterns
- Adequate executor memory (8GB+)
- Memory allocation is stable across executors
- Default starting point for most users

**Trade-off**: Optimal balance between memory safety and performance. Allows buffers to utilize most allocated space before spilling while maintaining safety margin for memory spikes.

**Monitoring**: Ideal state shows 1-3 spills per shuffle per executor under normal load. If zero spills consistently, threshold can be increased. If >5 spills, decrease threshold or increase bufferSizePercent.

### Aggressive (spillThreshold: 85-90%)

**Recommended configuration:**
```properties
spark.shuffle.streaming.spillThreshold=88
```

**Use when:**
- Abundant executor memory (16GB+ per executor)
- Consistent, well-understood workload patterns
- Maximum performance is priority
- Memory monitoring and alerting infrastructure in place

**Trade-off**: Minimizes spill overhead, maximizing streaming throughput. However, smaller safety margin increases risk of memory pressure during workload spikes or when coexisting with other memory-intensive operations.

**Monitoring**: Watch `executor.memory.onHeapExecutionMemory` and trigger alerts if >90% utilization. Be prepared to reduce threshold if OOM errors occur.

### Impact on Latency and Throughput

Disk spills introduce latency penalties that compound with shuffle size:

| Spill Frequency | Latency Impact | Throughput Impact |
|-----------------|----------------|-------------------|
| 0 spills        | Baseline       | Baseline          |
| 1-2 spills      | +5-10%         | -2-5%             |
| 3-5 spills      | +15-25%        | -10-15%           |
| >5 spills       | +30-50%        | -20-30%           |

**Optimization goal**: Minimize spills to 0-2 per shuffle per executor for optimal performance.

### Monitoring Spill Frequency

Access streaming shuffle spill metrics via:

```scala
// In Spark application
val metrics = spark.sparkContext.env.metricsSystem
  .getSourcesByName("StreamingShuffle.*")
  .map(_.metricRegistry.counter("spillCount").getCount)
```

Or via JMX:
```
org.apache.spark.executor:type=StreamingShuffle.<shuffleId>,name=spillCount
```

### Adjusting Based on Metrics

**Decision matrix**:

| Observed Behavior | Action |
|-------------------|--------|
| spillCount=0, bufferUtilization<60% | Increase spillThreshold to 85-90% or reduce bufferSizePercent |
| spillCount=0, bufferUtilization=60-75% | Increase spillThreshold to 85% |
| spillCount=1-3, bufferUtilization=75-85% | Optimal configuration, no change needed |
| spillCount>5, bufferUtilization>80% | Decrease spillThreshold to 70-75% OR increase bufferSizePercent |
| spillCount>5, bufferUtilization<70% | Partition count mismatch, check partition skew |

## Network Bandwidth Tuning

Streaming shuffle transfers data over the network while map tasks are still executing. Proper bandwidth management prevents network saturation while maximizing throughput.

### Unlimited Bandwidth (maxBandwidthMBps: unset, default)

**Recommended configuration:**
```properties
# spark.shuffle.streaming.maxBandwidthMBps not set
```

**Use when:**
- Dedicated network infrastructure for Spark cluster
- Single-tenant environment with no competing traffic
- Network bandwidth significantly exceeds shuffle data rate
- Maximum performance is priority

**Best performance scenario**: Streaming shuffle achieves maximum latency reduction (40-50%) with no artificial throttling. However, risk of network saturation if multiple large shuffles execute concurrently.

**Monitoring**: Watch network interface utilization. If consistently >85%, consider rate limiting to prevent saturation and TCP congestion.

### Rate-Limited (maxBandwidthMBps: explicit value)

**Recommended configuration:**
```properties
spark.shuffle.streaming.maxBandwidthMBps=<calculated_value>
```

**Use when:**
- Shared network infrastructure
- Multi-tenant Spark clusters
- Network saturation observed during shuffle-heavy workloads
- Predictable, consistent performance required

**Calculation formula**:
```
maxBandwidthMBps = (total network bandwidth MB/s × 0.8) / estimated concurrent shuffles
```

**Example**: 10 Gbps (1250 MB/s) network, 5 concurrent jobs with shuffles
- Total bandwidth: 1250 MB/s
- Usable bandwidth (80%): 1000 MB/s
- Per-shuffle limit: 1000 / 5 = 200 MB/s
- Configuration: `spark.shuffle.streaming.maxBandwidthMBps=200`

**Trade-off**: Prevents network saturation and ensures fair sharing among concurrent shuffles. However, adds artificial cap that may reduce throughput below network capacity during periods of low concurrency.

### Network QoS Integration

When network infrastructure supports Quality of Service (QoS), coordinate streaming shuffle bandwidth limits with QoS policies:

**QoS Priority Classes**:
1. **High Priority** (40% bandwidth): Interactive queries, critical production jobs
2. **Medium Priority** (40% bandwidth): Batch ETL, standard analytics
3. **Low Priority** (20% bandwidth): Speculative execution, background tasks

Configure per-job bandwidth limits aligned with QoS class:
```properties
# High priority job
spark.shuffle.streaming.maxBandwidthMBps=500

# Medium priority job
spark.shuffle.streaming.maxBandwidthMBps=200

# Low priority job
spark.shuffle.streaming.maxBandwidthMBps=100
```

### Bandwidth Monitoring and Tuning

Monitor network utilization and streaming shuffle throughput:

```bash
# Network utilization
sar -n DEV 1 10 | grep eth0

# Streaming shuffle throughput (via metrics)
curl http://<executor>:4040/metrics/json | jq '.gauges["StreamingShuffle.*.bytesStreamed"]'
```

**Tuning indicators**:

| Network Utilization | Streaming Performance | Action |
|---------------------|----------------------|--------|
| <60% | Optimal | No change needed |
| 60-80% | Optimal | Monitor for growth |
| 80-90% | Degrading | Implement rate limiting at 80% of current bandwidth |
| >90% | Severely degraded | Reduce maxBandwidthMBps by 30-40% or add network capacity |

### TCP Keepalive Settings

Streaming shuffle uses 5-second TCP keepalive intervals for connection health monitoring. This is not configurable but important to understand for network planning:

- **Keepalive interval**: 5 seconds
- **Failure detection**: 3 missed keepalives (15 seconds total)
- **Network impact**: ~100 bytes per connection every 5 seconds (negligible)

Ensure network infrastructure (firewalls, load balancers) allows TCP keepalive packets and maintains connection state for at least 20 seconds of idle time.

## Workload-Specific Configuration Patterns

Different Spark workload types have distinct shuffle characteristics that benefit from tailored configuration.

### Pattern 1: Batch ETL Workloads

**Characteristics**:
- Large data volumes (100GB-10TB per job)
- Infrequent execution (daily, weekly)
- High partition counts (1000-10000 partitions)
- Latency tolerance: medium (minutes acceptable)
- Priority: Maximize throughput, minimize resource costs

**Recommended configuration**:
```properties
spark.shuffle.streaming.enabled=true
spark.shuffle.streaming.bufferSizePercent=30
spark.shuffle.streaming.spillThreshold=85
# spark.shuffle.streaming.maxBandwidthMBps not set
```

**Configuration rationale**:
- **High buffer allocation (30%)**: Large executors (16-32GB) in batch ETL can dedicate more memory to shuffle buffers
- **Aggressive spill threshold (85%)**: Maximize buffer utilization to reduce spill frequency and I/O overhead
- **Unlimited bandwidth**: Batch jobs often run during off-peak hours with minimal network contention

**Expected performance**:
- Latency reduction: 35-45% for shuffle-heavy transformations
- Spill rate: 1-2 spills per shuffle per executor
- Memory utilization: 75-85% of executor memory under peak load

**Example workload**: Daily aggregation of 5TB log data with multiple groupBy operations
```
Before streaming shuffle: 2.5 hours total runtime
After tuning: 1.6 hours total runtime (36% improvement)
```

### Pattern 2: Streaming Analytics

**Characteristics**:
- Continuous micro-batch processing (seconds to minutes per batch)
- Predictable data rates and volumes per batch
- Medium partition counts (100-500 partitions)
- Latency tolerance: low (sub-second to seconds)
- Priority: Consistent latency, no resource starvation

**Recommended configuration**:
```properties
spark.shuffle.streaming.enabled=true
spark.shuffle.streaming.bufferSizePercent=20
spark.shuffle.streaming.spillThreshold=75
spark.shuffle.streaming.maxBandwidthMBps=300
```

**Configuration rationale**:
- **Balanced buffer allocation (20%)**: Leaves memory for long-running streaming state and aggregations
- **Conservative spill threshold (75%)**: Prevents memory buildup across continuous micro-batches
- **Rate limiting (300 MB/s example)**: Ensures predictable network usage and prevents interference between concurrent streaming queries

**Expected performance**:
- Latency reduction: 25-35% per micro-batch
- Spill rate: 0-1 spills per micro-batch
- Memory utilization: 60-70% stable utilization

**Example workload**: Real-time sessionization of clickstream data with 10-second micro-batches
```
Before streaming shuffle: 8 seconds average batch processing time
After tuning: 5.5 seconds average batch processing time (31% improvement)
```

### Pattern 3: Interactive Queries

**Characteristics**:
- Small to medium shuffle sizes (1-100GB)
- Ad-hoc, unpredictable execution patterns
- Low to medium partition counts (50-200 partitions)
- Latency tolerance: very low (seconds)
- Priority: Fast response time, efficient resource use

**Recommended configuration**:
```properties
spark.shuffle.streaming.enabled=true
spark.shuffle.streaming.bufferSizePercent=15
spark.shuffle.streaming.spillThreshold=70
spark.shuffle.streaming.maxBandwidthMBps=400
```

**Configuration rationale**:
- **Moderate buffer allocation (15%)**: Fast query execution with minimal memory footprint
- **Conservative spill threshold (70%)**: Quick spills prevent memory buildup during multiple concurrent queries
- **High rate limit (400 MB/s example)**: Interactive queries benefit from burst bandwidth to minimize user wait time

**Expected performance**:
- Latency reduction: 30-40% for shuffle operations
- Spill rate: 2-4 spills per query (acceptable for short-lived queries)
- Memory utilization: 50-60% transient spikes

**Example workload**: SQL query with aggregation over 10GB dataset with 100 partitions
```
Before streaming shuffle: 45 seconds query time
After tuning: 28 seconds query time (38% improvement)
```

### Pattern 4: Machine Learning Training

**Characteristics**:
- Iterative shuffles (same shuffle repeated 10-1000 times)
- Medium to large shuffle sizes (10-500GB per iteration)
- Memory-intensive beyond shuffle (model parameters, gradients)
- Latency tolerance: medium (iterations may take minutes)
- Priority: Coexistence with ML memory requirements

**Recommended configuration**:
```properties
spark.shuffle.streaming.enabled=true
spark.shuffle.streaming.bufferSizePercent=10
spark.shuffle.streaming.spillThreshold=65
spark.shuffle.streaming.maxBandwidthMBps=250
```

**Configuration rationale**:
- **Low buffer allocation (10%)**: ML training requires large memory for model state; shuffle buffers kept minimal
- **Very conservative spill threshold (65%)**: Early spilling prevents competition with ML memory allocation
- **Moderate rate limiting (250 MB/s example)**: Balanced network usage across training iterations

**Expected performance**:
- Latency reduction: 15-25% per iteration (lower due to memory constraints)
- Spill rate: 3-6 spills per iteration (acceptable given memory prioritization)
- Memory utilization: 80-90% total (including ML operations)

**Example workload**: Distributed gradient descent with 50 iterations over 100GB training data
```
Before streaming shuffle: 3.2 hours total training time
After tuning: 2.5 hours total training time (22% improvement)
```

## Performance Trade-offs and Decision Matrix

Streaming shuffle configuration involves balancing competing objectives. Understanding these trade-offs enables informed tuning decisions.

### Memory vs Latency

**Relationship**: Higher buffer allocation (bufferSizePercent) reduces shuffle latency but increases memory pressure.

| bufferSizePercent | Latency Reduction | Memory Pressure | Spill Frequency |
|-------------------|-------------------|-----------------|-----------------|
| 5-10%             | 15-25%            | Low             | High (5-8 spills) |
| 15-20%            | 30-40%            | Medium          | Medium (2-4 spills) |
| 25-30%            | 40-50%            | High            | Low (0-2 spills) |
| 35-50%            | 45-50%            | Very High       | Very Low (0-1 spills) |

**Decision guideline**:
- Choose lower values (5-15%) when executor memory is constrained or shared with memory-intensive operations
- Choose higher values (25-35%) when large executors dedicated to shuffle-heavy workloads

### Spill Threshold vs Safety

**Relationship**: Higher spill threshold (spillThreshold) maximizes buffer utilization but increases OOM risk.

| spillThreshold | Buffer Utilization | OOM Risk | Performance |
|----------------|-------------------|----------|-------------|
| 50-60%         | 55-65%            | Minimal  | -15% throughput |
| 65-75%         | 70-80%            | Low      | -5% throughput |
| 80-85%         | 85-90%            | Medium   | Optimal |
| 90-95%         | 92-97%            | High     | +2-3% throughput |

**Decision guideline**:
- Choose lower values (65-75%) for production-critical workloads that must not fail
- Choose higher values (85-90%) when performance is critical and memory monitoring is robust

### Bandwidth Limits vs Throughput

**Relationship**: Rate limiting (maxBandwidthMBps) prevents network saturation but caps maximum throughput.

| Configuration | Network Utilization | Throughput | Latency Consistency |
|---------------|---------------------|------------|---------------------|
| Unlimited     | Variable (can reach 100%) | Maximum | Variable under contention |
| Rate-limited at 80% capacity | Stable <85% | 15-20% below peak | Consistent |
| Rate-limited at 60% capacity | Stable <65% | 35-40% below peak | Very consistent |

**Decision guideline**:
- Use unlimited for dedicated, single-tenant clusters with adequate network capacity
- Use rate limiting in shared environments or when consistent performance is more important than peak throughput

### Configuration Decision Matrix

Use this matrix to select initial configuration based on primary optimization goal:

| Primary Goal | bufferSizePercent | spillThreshold | maxBandwidthMBps | Expected Trade-off |
|--------------|-------------------|----------------|------------------|-------------------|
| **Maximum latency reduction** | 30 | 88 | Unset | High memory usage, potential OOM |
| **Balanced performance** | 20 | 80 | Unset | Default, optimal for most |
| **Memory efficiency** | 10 | 70 | 200 | More spills, moderate latency reduction |
| **Consistent performance** | 15 | 75 | 300 | Predictable but not maximum performance |
| **Resource safety** | 8 | 65 | 150 | Conservative, minimal risk |

## See Also

- [Streaming Shuffle Architecture](streaming-shuffle-architecture.html) - Detailed design and protocol specification
- [Streaming Shuffle Configuration](configuration.html#streaming-shuffle) - Complete configuration reference
- [Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html) - Common issues and debugging procedures
- [Monitoring and Metrics](monitoring.html) - Streaming shuffle metrics and JMX integration
