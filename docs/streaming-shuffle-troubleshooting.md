---
layout: global
title: Streaming Shuffle Troubleshooting
displayTitle: Streaming Shuffle Troubleshooting Guide
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

# Streaming Shuffle Troubleshooting Guide

## Overview

This guide provides comprehensive troubleshooting procedures for Apache Spark's streaming shuffle feature. Streaming shuffle eliminates shuffle materialization latency by streaming data directly from map tasks to reduce tasks with memory buffering and backpressure control.

### Purpose and Scope

This troubleshooting guide covers:
- JMX metrics interpretation for operational visibility
- Common issues with step-by-step resolutions
- Log analysis patterns for identifying root causes
- Debugging procedures for connection timeouts and failures
- Memory pressure diagnostics and mitigation strategies
- Data integrity validation and checksum troubleshooting

### Prerequisites

Before troubleshooting streaming shuffle issues, ensure:
- Streaming shuffle is enabled via `spark.shuffle.manager=streaming`
- JMX metrics are accessible via monitoring dashboard or JConsole
- Executor logs are centralized and searchable (e.g., via ELK stack)
- Debug logging can be enabled dynamically if needed

### Related Documentation

- [Streaming Shuffle Configuration](configuration.html#streaming-shuffle-configuration)
- [Streaming Shuffle Architecture](streaming-shuffle-architecture.html)
- [Streaming Shuffle Performance Tuning](streaming-shuffle-tuning.html)
- [Monitoring Dashboard Setup](monitoring/streaming-shuffle-dashboard.json)

---

## JMX Metrics Reference

Streaming shuffle exposes comprehensive metrics via JMX under the `StreamingShuffle.<shuffleId>` namespace. These metrics provide real-time visibility into shuffle behavior and health.

### shuffle.streaming.bufferUtilizationPercent

**Type**: Gauge (Double)  
**Description**: Real-time percentage of streaming shuffle buffer memory in use

**Interpretation**:
- **Normal Range**: 40-70% - Healthy buffer utilization with headroom for spikes
- **Warning Range**: 70-85% - Buffer approaching capacity, spills likely
- **Critical Range**: >85% - Imminent memory pressure, frequent spills occurring

**Action Items by Range**:
- **40-70%**: No action required, system operating normally
- **70-85%**: Monitor spill frequency, consider increasing `spark.shuffle.streaming.bufferSizePercent`
- **>85%**: Immediate action - increase buffer size or reduce spill threshold to prevent memory exhaustion

**JMX Access Example**:
```bash
# Via JConsole
Service:jmx:rmi:///jndi/rmi://<executor-host>:9999/jmxrmi
MBean: metrics:name=StreamingShuffle.0.bufferUtilization
Attribute: Value

# Via curl (if JMX HTTP endpoint enabled)
curl http://<executor-host>:4040/metrics/json | jq '.gauges["StreamingShuffle.0.bufferUtilization"]'

### shuffle.streaming.spillCount

**Type**: Counter (Long)  
**Description**: Cumulative count of disk spill events during shuffle execution

**Interpretation**:
- **Expected Baseline**: <5% of total partitions should spill in normal operation
- **Investigation Threshold**: >20% spill rate indicates undersized buffers or memory pressure
- **Critical Threshold**: >50% spill rate signals severe memory constraints

**Action Items**:
- Calculate spill rate: `spillCount / totalPartitions`
- If >20%: Increase `spark.shuffle.streaming.bufferSizePercent` from default 20% to 30-40%
- If >50%: Consider increasing executor memory or reducing concurrent shuffle operations

**Related Metrics**: Compare with `shuffle.streaming.spillBytes` to understand spill volume

### shuffle.streaming.backpressureEvents

**Type**: Counter (Long)  
**Description**: Number of times consumer rate limiting was triggered due to slow consumption

**Interpretation**:
- **Rare Events**: <10 per shuffle indicates occasional slowdowns, acceptable
- **Frequent Events**: >100 per shuffle suggests sustained consumer bottleneck
- **Continuous Events**: Backpressure on every block indicates severe consumer underprovisioning

**Root Cause Analysis**:
- Consumer tasks slower than producer tasks (CPU-bound reduce operations)
- Insufficient consumer parallelism (reduce partition count too low)
- Network bandwidth saturation preventing acknowledgment delivery
- Consumer executor resource starvation (CPU, memory)

**Resolution Strategy**:
1. Increase reduce task parallelism via `spark.sql.shuffle.partitions`
2. Increase `spark.shuffle.streaming.maxBandwidthMBps` if bandwidth-limited
3. Scale consumer executor resources (cores, memory)
4. Adjust spill threshold to buffer producer output during slowdowns

### shuffle.streaming.partialReadInvalidations

**Type**: Counter (Long)  
**Description**: Number of partial reads invalidated due to producer failures

**Interpretation**:
- **Healthy Clusters**: Should be zero or near-zero in stable environments
- **Occasional Failures**: 1-5 per job may occur due to spot instance preemption or transient issues
- **Frequent Invalidations**: >10 per job indicates systemic stability problems

**Indicators of Underlying Issues**:
- Network instability causing connection timeouts (5-second threshold)
- Producer executor crashes or OOM errors
- Task preemption on oversubscribed clusters
- Hardware failures on producer nodes

**Investigation Steps**:
1. Correlate invalidation timestamps with executor loss events in Spark UI
2. Check producer executor logs for OOM, SIGKILL, or network exceptions
3. Analyze cluster resource utilization during invalidation windows
4. Review infrastructure monitoring for node health issues

### shuffle.streaming.checksumMismatches

**Type**: Counter (Long)  
**Description**: Number of CRC32C checksum validation failures during block transfers

**Critical Indicator**: Should be **zero** in healthy systems. Non-zero indicates:
- Hardware memory corruption (ECC errors)
- Network packet corruption (damaged cables, faulty switches)
- Disk corruption during spill operations
- Software bugs in serialization/deserialization

**Immediate Actions for Non-Zero Values**:
1. **Stop using affected executors** - Drain and remove from cluster
2. **Run hardware diagnostics** - Memory test (memtest86), disk SMART checks
3. **Inspect network infrastructure** - Cable integrity, switch errors, packet loss
4. **Collect evidence** - Heap dumps, thread dumps, system logs for support escalation

**Validation Procedure**:
```bash
# Check for hardware memory errors
dmesg | grep -i 'memory error\|ecc'

# Verify disk health
smartctl -a /dev/sda | grep -i 'reallocated\|pending\|uncorrectable'

# Test network stability
iperf3 -c <remote-host> -t 300 -i 10  # 5-minute throughput test
```

### Accessing Metrics

**Via Spark UI** (Driver only):
- Navigate to `http://<driver-host>:4040/metrics/json`
- Filter for `StreamingShuffle.*` metrics

**Via JMX Console**:
```bash
jconsole <executor-host>:9999
# Navigate to MBeans > metrics > StreamingShuffle.<shuffleId>
```

**Via Prometheus/Grafana** (if configured):
- Use provided dashboard template: `docs/monitoring/streaming-shuffle-dashboard.json`
- Metrics automatically scraped with `spark_` prefix

---

## Common Issues and Resolutions

### Issue 1: High Buffer Utilization (>85%)

**Symptoms**:
- `bufferUtilizationPercent` metric consistently above 85%
- Frequent disk spills (high `spillCount`)
- Degraded shuffle performance compared to baseline
- Executor GC pressure from buffer churn

**Diagnosis**:
```bash
# Check current buffer utilization
curl -s http://<executor>:4040/metrics/json | \
  jq '.gauges | with_entries(select(.key | contains("bufferUtilization")))'

# Correlate with spill frequency
curl -s http://<executor>:4040/metrics/json | \
  jq '.counters | with_entries(select(.key | contains("spillCount")))'
```

**Root Cause**:
- Buffer size undersized for workload data volume
- Too many concurrent shuffles competing for buffer pool
- Spill threshold set too high (default 80%), preventing timely spills

**Resolution**:
1. **Increase buffer size** (immediate mitigation):
   ```properties
   spark.shuffle.streaming.bufferSizePercent=30  # Up from default 20%
   ```

2. **Lower spill threshold** (faster spill triggering):
   ```properties
   spark.shuffle.streaming.spillThreshold=70  # Down from default 80%
   ```

3. **Reduce concurrent shuffles** (resource contention):
   ```properties
   spark.sql.adaptive.coalescePartitions.enabled=true
   spark.sql.adaptive.advisoryPartitionSizeInBytes=128m  # Reduce partition count
   ```

4. **Increase executor memory** (long-term solution):
   ```properties
   spark.executor.memory=16g  # Up from 8g
   ```

**Prevention**:
- Right-size buffers during initial deployment using tuning guide
- Monitor buffer utilization trends and adjust proactively
- Set up alerts for sustained >75% utilization

### Issue 2: Connection Timeouts

**Symptoms**:
- Frequent `partialReadInvalidations` events
- Executor logs showing "Producer timeout after 5000ms"
- Task failures with `FetchFailedException`
- Upstream task recomputation storms

**Diagnosis**:
```bash
# Check producer executor logs for timeout patterns
grep -A 5 "Producer timeout" /var/log/spark/executor-*.log

# Identify affected shuffle IDs
grep "Streaming shuffle producer failure" /var/log/spark/executor-*.log | \
  awk '{print $NF}' | sort | uniq -c
```

**Root Cause**:
- Network latency spikes exceeding 5-second timeout threshold
- Producer executor overload (CPU saturation, GC pauses)
- Network partition isolating producer from consumers
- Executor crashes mid-shuffle without graceful shutdown

**Resolution**:
1. **Investigate network stability**:
   ```bash
   # Test latency to affected executors
   ping -c 100 <producer-host> | tail -1
   
   # Check for packet loss
   mtr --report --report-cycles 100 <producer-host>
   ```

2. **Analyze producer executor health**:
   ```bash
   # Check CPU utilization
   top -H -p $(pgrep -f 'CoarseGrainedExecutorBackend')
   
   # Inspect GC logs for long pauses
   grep "Full GC" /var/log/spark/gc.log | awk '{print $NF}'
   ```

3. **Review executor resource allocation**:
   - Ensure adequate CPU cores: `spark.executor.cores >= 4`
   - Verify memory headroom: `spark.executor.memoryOverhead >= 10% of executor.memory`
   - Check for resource contention with other workloads

4. **Enable heartbeat monitoring** (if not already):
   ```properties
   spark.shuffle.streaming.debug=true  # Verbose logging for diagnostics
   ```

**Prevention**:
- Deploy on stable network infrastructure with <1ms latency
- Provision executors with sufficient resources for workload
- Implement cluster-level health monitoring and auto-remediation
- Use dedicated network for shuffle traffic if possible

### Issue 3: Memory Pressure and OOM Errors

**Symptoms**:
- Executor crashes with `java.lang.OutOfMemoryError: Java heap space`
- Automatic fallback to sort-based shuffle
- Log messages: "Insufficient memory for streaming shuffle buffer allocation"
- Frequent full GC cycles consuming >50% of task execution time

**Diagnosis**:
```bash
# Analyze heap dumps for memory leaks
jmap -dump:live,format=b,file=/tmp/heap.hprof <executor-pid>
jhat -J-Xmx4g /tmp/heap.hprof

# Review GC logs for pressure indicators
grep "Full GC" /var/log/spark/gc.log | \
  awk '{print $1, $NF}' | \
  awk '{sum+=$2; count++} END {print "Avg Full GC:", sum/count "ms"}'

# Check buffer allocation attempts
grep "acquireStreamingShuffleMemory" /var/log/spark/executor-*.log | \
  grep -c "failed"
```

**Root Cause**:
- Buffer size percentage too high relative to available executor memory
- Memory fragmentation preventing large contiguous allocations
- Insufficient executor memory for workload data volume
- Memory leaks in application code consuming heap space

**Resolution**:
1. **Reduce buffer allocation** (immediate mitigation):
   ```properties
   spark.shuffle.streaming.bufferSizePercent=10  # Down from default 20%
   ```

2. **Increase executor memory** (primary solution):
   ```properties
   spark.executor.memory=32g  # Up from 16g
   spark.executor.memoryOverhead=3200m  # 10% of executor memory
   ```

3. **Enable aggressive GC** (reduce fragmentation):
   ```properties
   spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35
   ```

4. **Disable streaming shuffle temporarily** (fallback):
   ```properties
   spark.shuffle.manager=sort  # Revert to stable sort-based shuffle
   ```

**Prevention**:
- Size executor memory with 2x headroom above minimum requirements
- Monitor heap utilization trends via JMX or monitoring tools
- Perform heap dump analysis during development to identify leaks
- Use `spark.sql.adaptive.enabled=true` to reduce memory pressure

### Issue 4: Backpressure Events Degrading Performance

**Symptoms**:
- High `backpressureEvents` counter (>100 per shuffle)
- Producer tasks complete quickly but overall shuffle duration is slow
- Consumer acknowledgments delayed in logs
- Network utilization below capacity despite slow shuffle

**Diagnosis**:
```bash
# Compare producer vs consumer task durations
grep "Finished task" /var/log/spark/executor-*.log | \
  awk '/ShuffleMapTask/ {sum+=$NF; count++} END {print "Avg map task:", sum/count "ms"}'
grep "Finished task" /var/log/spark/executor-*.log | \
  awk '/ResultTask/ {sum+=$NF; count++} END {print "Avg reduce task:", sum/count "ms"}'

# Check consumer CPU utilization
mpstat -P ALL 1 10 | awk '/Average/ && $2 ~ /[0-9]/ {print $2, $3+$5}'
```

**Root Cause**:
- Consumer tasks computationally expensive (CPU-bound operations)
- Reduce partition count too low, limiting parallelism
- Consumer executors under-provisioned compared to producers
- Data skew causing few partitions to process majority of data

**Resolution**:
1. **Increase reduce parallelism**:
   ```properties
   spark.sql.shuffle.partitions=400  # Up from 200
   ```

2. **Scale consumer resources**:
   ```properties
   spark.executor.cores=8  # More CPU for consumer tasks
   spark.dynamicAllocation.enabled=true  # Scale executors dynamically
   spark.dynamicAllocation.maxExecutors=100
   ```

3. **Address data skew**:
   ```properties
   spark.sql.adaptive.skewJoin.enabled=true
   spark.sql.adaptive.skewJoin.skewedPartitionFactor=5
   ```

4. **Adjust rate limiting** (if bandwidth-constrained):
   ```properties
   spark.shuffle.streaming.maxBandwidthMBps=1000  # Increase from 500
   ```

**Prevention**:
- Balance producer and consumer resource allocation
- Use adaptive query execution to handle skew automatically
- Profile consumer operations to identify bottlenecks
- Monitor backpressure events and adjust before degradation

### Issue 5: Checksum Mismatches Indicating Corruption

**Symptoms**:
- Non-zero `checksumMismatches` counter
- Logs showing "CRC32C checksum validation failed"
- Block retransmission attempts followed by task failures
- Intermittent data corruption affecting shuffle correctness

**Diagnosis**:
```bash
# Identify affected executors
grep "checksum validation failed" /var/log/spark/executor-*.log | \
  awk '{print $1}' | sort | uniq -c | sort -rn

# Hardware diagnostics
# Memory ECC errors
sudo edac-util -v

# Disk errors
sudo smartctl -H /dev/sda
sudo dmesg | grep -i 'i/o error\|disk error'

# Network diagnostics
ethtool -S eth0 | grep -i 'error\|drop'
```

**Root Cause**:
- Hardware memory errors (single-bit or multi-bit ECC failures)
- Disk corruption during spill operations (bad sectors)
- Network transmission errors (cable damage, switch issues)
- Software bugs in serialization or buffer management

**Resolution**:
1. **Quarantine affected executors** (immediate action):
   ```bash
   # Mark executors as unhealthy in resource manager
   yarn node -status <node-id>
   yarn rmadmin -refreshNodes
   ```

2. **Run hardware diagnostics**:
   ```bash
   # Memory test (requires reboot)
   sudo memtest86+ --passes=4
   
   # Disk surface scan
   sudo badblocks -sv /dev/sda
   
   # Network cable test
   ethtool eth0 | grep 'Link detected'
   ```

3. **Replace faulty hardware**:
   - RMA memory modules showing ECC errors
   - Replace disks with reallocated sectors
   - Replace damaged network cables or NICs

4. **Enable software workarounds** (temporary):
   ```properties
   spark.shuffle.streaming.debug=true  # Verbose logging
   # Consider disabling streaming shuffle on affected nodes
   ```

**Prevention**:
- Deploy on hardware with ECC memory
- Implement proactive hardware health monitoring
- Regular cable and switch maintenance
- Use redundant network paths where possible

### Issue 6: Unexpected Fallback to Sort Shuffle

**Symptoms**:
- Streaming shuffle not engaged despite `spark.shuffle.manager=streaming`
- Logs showing "Streaming shuffle conditions not met, using sort-based"
- Performance not improved over baseline sort shuffle
- No streaming shuffle metrics appearing in JMX

**Diagnosis**:
```bash
# Check fallback reasons in driver logs
grep "Streaming shuffle" /var/log/spark/driver.log | grep -i "fallback\|not met"

# Verify configuration
spark-submit --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  --verbose my-app.jar 2>&1 | grep shuffle

# Check shuffle characteristics
grep "ShuffleDependency" /var/log/spark/driver.log
```

**Root Cause**:
- Feature flag not enabled: `spark.shuffle.streaming.enabled=false`
- Partition count below threshold (<100 partitions)
- Map-side combine enabled (not supported in v1)
- Serializer doesn't support relocation
- Insufficient memory for buffer allocation

**Resolution**:
1. **Enable feature flag explicitly**:
   ```properties
   spark.shuffle.manager=streaming
   spark.shuffle.streaming.enabled=true
   ```

2. **Verify shuffle prerequisites**:
   ```properties
   # Ensure sufficient partitions
   spark.sql.shuffle.partitions=200  # Must be >=100
   
   # Disable map-side combine if present
   spark.sql.adaptive.enabled=true
   spark.sql.adaptive.coalescePartitions.enabled=false
   ```

3. **Check serializer compatibility**:
   ```scala
   // Use Kryo serializer (supports relocation)
   spark.serializer=org.apache.spark.serializer.KryoSerializer
   ```

4. **Ensure adequate memory**:
   ```properties
   spark.executor.memory=16g  # Minimum 8g recommended
   spark.shuffle.streaming.bufferSizePercent=20
   ```

**Prevention**:
- Validate configuration before deployment using verbose mode
- Monitor fallback events via JMX metrics
- Document workload prerequisites for streaming shuffle
- Implement configuration validation in deployment pipeline

---

## Log Analysis Guidelines

### Producer Executor Logs

**Key Patterns to Monitor**:

1. **Buffer Allocation Success**:
   ```
   INFO StreamingShuffleWriter: Allocated 2048MB buffer for shuffle 0, partition 42
   ```
   - Indicates successful buffer acquisition
   - Check allocation size matches configuration

2. **Spill Trigger Events**:
   ```
   WARN MemorySpillManager: Buffer utilization 82%, triggering spill for shuffle 0, partitions [15, 23, 87]
   ```
   - Normal if occasional (<5% of partitions)
   - Frequent spills indicate undersized buffers

3. **Acknowledgment Delays**:
   ```
   WARN BackpressureProtocol: Consumer acknowledgment delayed 12000ms for shuffle 0, partition 5
   ```
   - Indicates consumer slowdown
   - May trigger backpressure if sustained

4. **Producer Failure Detection**:
   ```
   ERROR StreamingShuffleWriter: Failed to stream partition 10, attempting spill
   IOException: Connection reset by peer
   ```
   - Network connectivity issues
   - Should trigger retry or fallback

### Consumer Executor Logs

**Key Patterns to Monitor**:

1. **Connection Establishment**:
   ```
   INFO StreamingShuffleReader: Connected to producer executor-3:7337 for shuffle 0
   ```
   - Confirms network connectivity
   - Log producer host for failure correlation

2. **Heartbeat Status**:
   ```
   DEBUG BackpressureProtocol: Sent heartbeat to producer executor-3, position=5242880
   ```
   - Normal every 10 seconds
   - Missing heartbeats indicate consumer failure

3. **Partial Read Invalidation**:
   ```
   ERROR StreamingShuffleReader: Producer timeout after 5000ms, invalidating partial reads from map task 42
   WARN DAGScheduler: Streaming shuffle producer failure detected, triggering recomputation
   ```
   - Critical event indicating producer failure
   - Correlate with producer executor loss events

4. **Checksum Validation**:
   ```
   WARN StreamingShuffleReader: Checksum mismatch for shuffle 0, partition 8 (expected=0x3a7f21bc, actual=0x4b2c98df), retrying
   ERROR StreamingShuffleReader: Checksum validation failed after 5 retries, failing task
   ```
   - Data corruption detected
   - Requires hardware investigation

### DAGScheduler Driver Logs

**Key Patterns to Monitor**:

1. **Shuffle Registration**:
   ```
   INFO ShuffleManager: Registered streaming shuffle 0 with 200 partitions, 4096MB buffer pool
   ```
   - Confirms streaming shuffle activation
   - Verify buffer pool size matches configuration

2. **Fallback Events**:
   ```
   WARN StreamingShuffleManager: Consumer sustained 2x slower than producer for 65 seconds, triggering fallback to sort shuffle
   ```
   - Automatic degradation detected
   - Investigate consumer performance

3. **Failure Handling**:
   ```
   INFO DAGScheduler: Handling streaming shuffle partial read invalidation for shuffle 0, map 15
   INFO MapOutputTracker: Unregistered map output for shuffle 0, map 15
   ```
   - Partial read invalidation flow
   - Should trigger upstream recomputation

### Log Level Recommendations

**Normal Operations** (production):
```properties
log4j.logger.org.apache.spark.shuffle.streaming=INFO
```

**Performance Investigation**:
```properties
log4j.logger.org.apache.spark.shuffle.streaming=DEBUG
log4j.logger.org.apache.spark.shuffle.streaming.BackpressureProtocol=TRACE
```

**Troubleshooting Failures**:
```properties
log4j.logger.org.apache.spark.shuffle.streaming=TRACE
log4j.logger.org.apache.spark.network=DEBUG
spark.shuffle.streaming.debug=true
```

**Log Rotation Configuration** (prevent disk exhaustion):
```properties
log4j.appender.file.MaxFileSize=100MB
log4j.appender.file.MaxBackupIndex=10
```

---

## Debugging Procedures

### Procedure 1: Connection Timeout Investigation

**Objective**: Diagnose why consumers cannot connect to producers within 5-second timeout

**Steps**:

1. **Enable Debug Logging**:
   ```properties
   spark.shuffle.streaming.debug=true
   log4j.logger.org.apache.spark.shuffle.streaming=DEBUG
   ```

2. **Capture Thread Dumps During Timeout**:
   ```bash
   # On producer executor
   jstack <producer-pid> > /tmp/producer-threads.txt
   
   # On consumer executor
   jstack <consumer-pid> > /tmp/consumer-threads.txt
   ```

3. **Network Diagnostics**:
   ```bash
   # Test connectivity
   telnet <producer-host> 7337
   
   # Measure latency
   ping -c 100 <producer-host> | tail -1
   
   # Check routing
   traceroute <producer-host>
   
   # Verify firewall rules
   sudo iptables -L -n | grep 7337
   ```

4. **Executor Resource Check**:
   ```bash
   # CPU saturation
   top -H -p <producer-pid> -n 1 | head -20
   
   # Memory pressure
   jstat -gc <producer-pid> 1000 10
   
   # Thread contention
   jstack <producer-pid> | grep -A 5 "BLOCKED"
   ```

5. **Correlate with Infrastructure Events**:
   - Check cloud provider health dashboards
   - Review network maintenance windows
   - Inspect hypervisor resource contention (if virtualized)

**Expected Outcomes**:
- Network latency <5ms for healthy connections
- CPU utilization <80% on producer executors
- No thread deadlocks or long GC pauses
- Firewall rules allow shuffle port traffic

### Procedure 2: Memory Pressure Diagnosis

**Objective**: Identify root cause of OOM errors and memory exhaustion

**Steps**:

1. **Capture Heap Dump**:
   ```bash
   # Automatic on OOM
   spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError \
     -XX:HeapDumpPath=/tmp/heap-${EXECUTOR_ID}.hprof
   
   # Manual capture
   jmap -dump:format=b,file=/tmp/heap.hprof <executor-pid>
   ```

2. **Analyze Heap Dump**:
   ```bash
   # Using MAT (Memory Analyzer Tool)
   mat -nosplash -application org.eclipse.mat.api.parse ParseHeapDump \
     /tmp/heap.hprof org.eclipse.mat.api:suspects org.eclipse.mat.api:overview
   
   # Using jhat
   jhat -J-Xmx4g /tmp/heap.hprof
   # Navigate to http://localhost:7000/
   ```

3. **GC Log Analysis**:
   ```bash
   # Enable GC logging
   spark.executor.extraJavaOptions=-Xlog:gc*:file=/tmp/gc-${EXECUTOR_ID}.log
   
   # Analyze GC patterns
   grep "Pause Full" /tmp/gc-*.log | \
     awk '{print $NF}' | \
     awk '{sum+=$1; count++; if($1>max) max=$1} END {print "Avg:", sum/count "ms, Max:", max "ms"}'
   ```

4. **Buffer Allocation Tracking**:
   ```bash
   # Track streaming shuffle allocations
   grep "acquireStreamingShuffleMemory" /var/log/spark/executor-*.log | \
     awk '{sum+=$NF} END {print "Total allocated:", sum/1024/1024 "MB"}'
   ```

5. **Memory Leak Detection**:
   - Compare heap dumps over time for retained heap growth
   - Use jvisualvm profiler to track allocation hotspots
   - Review application code for unclosed resources

**Expected Outcomes**:
- Streaming shuffle buffers <20% of executor heap
- Full GC frequency <1 per minute
- No retained heap growth indicating leaks
- Adequate headroom (30%+) for task execution

### Procedure 3: Performance Degradation Analysis

**Objective**: Diagnose why streaming shuffle is slower than expected

**Steps**:

1. **Baseline Comparison**:
   ```bash
   # Run with sort shuffle
   spark-submit --conf spark.shuffle.manager=sort my-app.jar
   
   # Run with streaming shuffle
   spark-submit --conf spark.shuffle.manager=streaming my-app.jar
   
   # Compare metrics
   diff <(curl -s http://driver1:4040/metrics/json) \
        <(curl -s http://driver2:4040/metrics/json)
   ```

2. **Spill Rate Analysis**:
   ```bash
   # Calculate spill percentage
   SPILLS=$(curl -s http://driver:4040/metrics/json | jq '.counters["shuffle.streaming.spillCount"].count')
   PARTITIONS=$(curl -s http://driver:4040/api/v1/applications/app-1/stages | jq '.[0].numTasks')
   echo "Spill rate: $(( SPILLS * 100 / PARTITIONS ))%"
   ```

3. **Network Bandwidth Monitoring**:
   ```bash
   # Monitor network utilization
   iftop -i eth0 -t -s 60 > /tmp/network-usage.txt
   
   # Analyze throughput
   awk '/Total send rate/ {print $4, $5}' /tmp/network-usage.txt
   ```

4. **Task Execution Breakdown**:
   ```bash
   # Analyze shuffle read/write times from Spark UI
   curl -s http://driver:4040/api/v1/applications/app-1/stages/0/0/taskSummary | \
     jq '.shuffleWriteMetrics.writeTime, .shuffleReadMetrics.fetchWaitTime'
   ```

5. **Flame Graph Generation**:
   ```bash
   # CPU profiling with async-profiler
   ./profiler.sh -d 60 -f /tmp/flamegraph.svg <executor-pid>
   
   # Identify hotspots in shuffle code
   ```

**Expected Outcomes**:
- 30-50% latency reduction vs sort shuffle for 10GB+ shuffles
- Spill rate <5% in normal operation
- Network utilization aligned with configured bandwidth limits
- CPU time dominated by serialization/deserialization, not I/O

---

## Advanced Diagnostics

### Flame Graph Generation for CPU Profiling

**Purpose**: Identify CPU hotspots in streaming shuffle code paths

**Tools**: async-profiler, perf, FlameGraph

**Procedure**:
```bash
# Download async-profiler
wget https://github.com/async-profiler/async-profiler/releases/latest/download/async-profiler-linux-x64.tar.gz
tar xzf async-profiler-linux-x64.tar.gz

# Profile executor for 60 seconds
./async-profiler/profiler.sh -d 60 -f /tmp/flamegraph.svg -e cpu <executor-pid>

# Analyze flamegraph
# Look for unexpected hotspots in shuffle code
# Common issues: excessive serialization overhead, lock contention
```

**Interpretation**:
- Wide bars indicate time-consuming functions
- Tall stacks indicate deep call chains (potential inefficiency)
- Look for streaming shuffle methods: `streamPartitionData`, `processHeartbeat`, `validateChecksum`

### Network Packet Capture for Protocol Analysis

**Purpose**: Debug protocol-level issues with acknowledgments or heartbeats

**Procedure**:
```bash
# Capture shuffle traffic
sudo tcpdump -i eth0 -w /tmp/shuffle.pcap port 7337

# Analyze with Wireshark
wireshark /tmp/shuffle.pcap

# Filter for streaming shuffle messages
# tcp.port == 7337 && tcp.len > 0

# Verify acknowledgment round-trip times
# Check for packet loss or retransmissions
```

**Red Flags**:
- Acknowledgment RTT >100ms (slow consumer)
- TCP retransmissions >1% (network issues)
- Missing heartbeats (consumer failure)

### Stress Testing with Synthetic Workloads

**Purpose**: Reproduce issues in controlled environment

**Example Workload**:
```scala
// Generate 10GB shuffle with 200 partitions
val data = spark.range(0, 1000000000)
  .selectExpr("id", "id % 200 as key", "md5(cast(id as string)) as value")
  .repartition(200, col("key"))
  .groupBy("key")
  .agg(count("*"), collect_list("value"))
  .write.parquet("/tmp/output")
```

**Inject Failures**:
```bash
# Kill random executors mid-shuffle
while true; do
  sleep $(( RANDOM % 60 + 30 ))
  EXECUTOR_PID=$(pgrep -f CoarseGrainedExecutorBackend | shuf -n 1)
  kill -9 $EXECUTOR_PID
done
```

**Monitor Metrics**:
- Partial read invalidations should match executor kills
- No data loss or correctness issues
- Automatic recovery via recomputation

### A/B Testing: Streaming vs Sort Shuffle

**Purpose**: Quantify performance improvement for specific workload

**Procedure**:
```bash
# Run sort shuffle baseline
spark-submit --conf spark.shuffle.manager=sort \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/tmp/sort-logs \
  my-app.jar

# Run streaming shuffle
spark-submit --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/tmp/streaming-logs \
  my-app.jar

# Compare event logs
spark-history-server
# Navigate to each application and compare stage durations
```

**Metrics to Compare**:
- Total shuffle write time
- Total shuffle read time
- Executor memory usage
- Network I/O volume
- GC time percentage

### Escalation to Spark Community

**When to Escalate**:
- Suspected bugs in streaming shuffle implementation
- Correctness issues (data loss or corruption)
- Performance regression not explained by configuration
- Crashes or hangs with stack traces in shuffle code

**Information to Provide**:
1. Spark version and build details
2. Configuration (full `spark-defaults.conf`)
3. Executor logs with stack traces
4. JMX metrics snapshot during issue
5. Heap dumps (if memory-related)
6. Minimal reproducible example

**Community Channels**:
- Apache Spark JIRA: https://issues.apache.org/jira/browse/SPARK
- Spark mailing lists: dev@spark.apache.org
- Stack Overflow: Tag `apache-spark` with `streaming-shuffle`

---

## Summary

This troubleshooting guide provides comprehensive procedures for diagnosing and resolving streaming shuffle issues in Apache Spark. Key takeaways:

- **Monitor JMX metrics proactively** to detect issues before impact
- **High buffer utilization** (>85%) indicates need for configuration tuning
- **Connection timeouts** typically stem from network or executor resource issues
- **Memory pressure** requires balancing buffer size with executor memory
- **Checksum mismatches** (non-zero) indicate hardware problems requiring immediate action
- **Fallback to sort shuffle** has specific prerequisites that must be validated

For additional guidance:
- [Configuration Reference](configuration.html#streaming-shuffle-configuration) - Detailed parameter documentation
- [Architecture Design](streaming-shuffle-architecture.html) - Protocol and failure handling specifications
- [Performance Tuning](streaming-shuffle-tuning.html) - Optimization guidelines for production deployments
- [Migration Guide](streaming-shuffle-migration.html) - Staged rollout procedures

**Need Help?** File issues at [Apache Spark JIRA](https://issues.apache.org/jira/browse/SPARK) with tag `streaming-shuffle`.
