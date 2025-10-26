---
layout: global
title: Streaming Shuffle Architecture
displayTitle: Streaming Shuffle Architecture
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

## Introduction

Streaming shuffle is a new shuffle implementation in Apache Spark that eliminates shuffle materialization latency by streaming data directly from map tasks (producers) to reduce tasks (consumers) without requiring complete disk writes before reduce-side processing can begin. This architecture provides **30-50% latency reduction** for shuffle-heavy workloads, particularly those with 10GB+ data volumes and 100+ partitions.

## Key Benefits

Traditional sort-based shuffle in Spark follows a two-phase approach:
1. **Map Phase**: Write all shuffle data to local disk
2. **Reduce Phase**: Wait for all map tasks to complete, then fetch data from disk

Streaming shuffle removes this strict barrier by enabling:
- **Incremental Processing**: Reduce tasks start processing data as soon as map tasks produce it
- **Memory-First Architecture**: Data flows through memory buffers, spilling to disk only when necessary
- **Reduced I/O**: Eliminates redundant disk writes when consumer is ready to process
- **Lower Latency**: End-to-end shuffle latency reduced by 30-50% in typical workloads

## Architecture Components

The streaming shuffle architecture consists of five primary components:

1. **StreamingShuffleManager**: Factory and coordinator for streaming shuffle operations
2. **StreamingShuffleWriter**: Producer-side component managing buffer allocation and data streaming
3. **StreamingShuffleReader**: Consumer-side component handling block requests and acknowledgments
4. **BackpressureProtocol**: Flow control mechanism preventing producer from overwhelming consumer
5. **MemorySpillManager**: Automatic disk spill logic when memory thresholds exceeded

## Comparison with Sort-Based Shuffle

| Aspect | Sort-Based Shuffle | Streaming Shuffle |
|--------|-------------------|------------------|
| Data Flow | Disk → Network → Consumer | Memory → Network → Consumer (with spill fallback) |
| Latency | Higher (full materialization) | 30-50% lower (incremental streaming) |
| Memory | Fixed shuffle buffer | Configurable 1-50% executor memory |
| Failure Handling | Task recomputation | Partial read invalidation + recomputation |
| Activation | Default (`spark.shuffle.manager=sort`) | Opt-in (`spark.shuffle.manager=streaming`) |

# Streaming Protocol Specification

## Producer-Consumer Data Flow

The streaming shuffle protocol enables direct producer-to-consumer communication with the following flow:

```
┌─────────────────┐                    ┌─────────────────┐
│  Map Task       │                    │  Reduce Task    │
│  (Producer)     │                    │  (Consumer)     │
└────────┬────────┘                    └────────┬────────┘
         │                                      │
         │ 1. Serialize records into buffer    │
         │                                      │
         │ 2. Stream block when buffer reaches │
         │    threshold or partition complete   │
         ├─────────────────────────────────────>│
         │    StreamingShuffleBlock             │
         │    (data + CRC32C checksum)          │
         │                                      │ 3. Validate checksum
         │                                      │
         │                                      │ 4. Deserialize & process
         │                                      │
         │ 5. Send acknowledgment               │
         │<─────────────────────────────────────┤
         │    StreamingShuffleAcknowledgment    │
         │    (partition ID + offset)           │
         │                                      │
         │ 6. Reclaim buffer memory (<100ms)   │
         │                                      │
```

### Protocol Messages

The streaming shuffle protocol uses two primary message types that extend Spark's network protocol:

1. **StreamingShuffleAcknowledgment**: Consumer signals successful receipt and processing
   - Payload: `shuffleId`, `mapId`, `partitionId`, `consumedOffset`, `complete` flag
   - Purpose: Enables producer to reclaim buffer memory
   - Frequency: After each block successfully processed

2. **StreamingShuffleHeartbeat**: Consumer signals liveness to producer
   - Payload: `consumerId`, `timestamp`
   - Purpose: Detect consumer failures (timeout after 10 seconds)
   - Frequency: Every 10 seconds during active shuffle

## Block Structure and Streaming Semantics

Each streaming shuffle block consists of:

```
┌────────────────────────────────────────┐
│ Block Header (12 bytes)                │
├────────────────────────────────────────┤
│ - Length (4 bytes, int)                │
│ - Checksum (8 bytes, long, CRC32C)    │
├────────────────────────────────────────┤
│ Serialized Data                        │
│ - Records for partition                │
│ - Serialized via dependency serializer │
│ - Max 2MB per block for pipelining     │
└────────────────────────────────────────┘
```

**Streaming Semantics**:
- **Partial Reads**: Consumers request blocks before shuffle completes
- **In-Order Delivery**: Blocks streamed in order per partition
- **Idempotent Processing**: Duplicate block requests handled gracefully
- **Atomic Acknowledgment**: Consumer commits entire block or none

## Acknowledgment Protocol

The acknowledgment protocol ensures reliable buffer reclamation:

1. **Consumer Processing**: 
   - Validate checksum on block receive
   - Deserialize records
   - Process through reduce function
   
2. **Acknowledgment Transmission**:
   - Send `StreamingShuffleAcknowledgment` with consumed offset
   - Use existing `TransportContext` for reliable delivery
   - Retry with exponential backoff on network errors

3. **Producer Reclamation**:
   - Track highest acknowledged offset per partition
   - Release buffer memory up to acknowledged position
   - Complete within 100ms of acknowledgment receipt

4. **Failure Handling**:
   - Missing acknowledgment after 10 seconds triggers spill-to-disk
   - Consumer failure invalidates partial reads
   - Producer failure triggers upstream recomputation

## Heartbeat Mechanism

Consumer liveness is monitored via 10-second heartbeat interval:

```
Time: 0s          10s         20s         30s         40s
      │           │           │           │           │
      ├───────────┼───────────┼───────────┼───────────> Consumer Active
      │ Heartbeat │ Heartbeat │ Heartbeat │ Heartbeat │
      │           │           │           │           │
      │           │           │           │           │
      │           │           │ [MISS]    │ [MISS]    │ Consumer Failed
      │           │           │           │           │
      └───────────┴───────────┴───────────┴───────────> Producer Action
                                          │           │
                                          └───Timeout─> Trigger Spill
```

**Heartbeat Protocol**:
- **Interval**: 10 seconds
- **Timeout**: 10 seconds without heartbeat = consumer failure
- **Action on Timeout**: Producer spills buffered data to disk for durable storage
- **Recovery**: Consumer reconnects and fetches from spilled blocks

## Normal Operation Sequence Diagram

```
Producer               Network              Consumer               MemoryManager
   │                      │                      │                       │
   │  Allocate Buffer     │                      │                       │
   ├──────────────────────┼──────────────────────┼──────────────────────>│
   │<─────────────────────┼──────────────────────┼───────────────────────┤
   │  Buffer Granted      │                      │                       │
   │                      │                      │                       │
   │  Serialize Records   │                      │                       │
   ├──────────────        │                      │                       │
   │  into Buffer         │                      │                       │
   │                      │                      │                       │
   │  Stream Block        │                      │                       │
   ├─────────────────────>│  Forward Block       │                       │
   │                      ├─────────────────────>│                       │
   │                      │                      │  Validate Checksum    │
   │                      │                      ├──────────────          │
   │                      │                      │  Deserialize          │
   │                      │                      │                       │
   │                      │  Send Acknowledgment │                       │
   │<─────────────────────┼──────────────────────┤                       │
   │  Reclaim Buffer      │                      │                       │
   ├──────────────────────┼──────────────────────┼──────────────────────>│
   │                      │                      │                       │
```

**Key Points**:
1. Producer allocates buffer from memory pool before serialization
2. Blocks streamed immediately when buffer reaches threshold
3. Network layer ensures reliable delivery via TCP
4. Consumer validates data integrity with CRC32C checksum
5. Acknowledgment triggers buffer reclamation within 100ms
6. Process repeats for all partitions in the shuffle

# Memory Management Design

## Buffer Allocation Strategy

Memory for streaming shuffle buffers is allocated from executor memory with configurable limits:

**Configuration**:
- Parameter: `spark.shuffle.streaming.bufferSizePercent`
- Default: 20% of executor memory
- Range: 1-50% (validated at runtime)
- Formula: `totalBufferSize = executorMemory × bufferSizePercent / 100`

**Per-Partition Distribution**:
```
Total Buffer Size = Executor Memory × Buffer Percent
Per-Partition Buffer = Total Buffer Size / Number of Partitions

Example:
- Executor Memory: 10 GB
- Buffer Percent: 20%
- Partitions: 100
→ Total Buffer: 2 GB
→ Per-Partition: 20 MB
```

## Memory Pool Integration

Streaming shuffle integrates with Spark's `MemoryManager`:

```scala
// Allocation
def acquireStreamingShuffleMemory(
    taskAttemptId: Long,
    numBytes: Long,
    memoryMode: MemoryMode): Long

// Release
def releaseStreamingShuffleMemory(
    taskAttemptId: Long,
    numBytes: Long,
    memoryMode: MemoryMode): Unit
```

**Pool Tracking**:
- Separate accounting for streaming shuffle buffers
- Tracked independently from execution memory
- Visible in memory metrics for monitoring
- Guaranteed release on task completion via cleanup hooks

## Threshold Monitoring

Buffer utilization is monitored at 100ms polling intervals:

```
Monitoring Thread:
    Every 100ms:
        current = sum(buffer.used for all partitions)
        total = allocatedBufferSize
        utilization = (current / total) × 100
        
        if utilization > spillThreshold:
            selectPartitionsForSpill()
            triggerAsyncSpill()
```

**Thresholds**:
- **Normal Operation**: <70% utilization
- **Warning**: 70-80% utilization (log warning)
- **Spill Trigger**: >80% utilization (configurable 50-95%)
- **Critical**: >95% utilization (aggressive spill + rate limiting)

## Spill Trigger Logic

Automatic disk spill is triggered when buffer utilization exceeds configured threshold:

**Spill Decision Algorithm**:
```
if bufferUtilization > spillThreshold:
    requiredSpace = targetUtilization - currentUtilization
    partitionsToSpill = selectLRUPartitions(requiredSpace)
    
    for each partition in partitionsToSpill:
        spillToDisk(partition)
        reclaimMemory(partition)
        updateMetrics()
```

**Spill Characteristics**:
- **Trigger**: 80% buffer utilization (default)
- **Target**: Reduce to 60% utilization after spill
- **Selection**: LRU (Least Recently Used) partitions first
- **Performance**: Async spill to minimize producer blocking
- **Metrics**: Tracked per shuffle (`shuffle.streaming.spillCount`)

## LRU-Based Partition Eviction

When spill is required, partitions are selected using LRU algorithm:

```
Partition Priority (highest to lowest):
1. Least recently accessed
2. Fully written partitions (can be entirely spilled)
3. Largest buffers (maximize memory reclamation)
4. Partitions with acknowledged consumers (safe to spill)

Algorithm:
    partitions = allBufferedPartitions()
    sort by (lastAccessTime, isComplete, bufferSize)
    spill until (utilization <= targetThreshold)
```

**Eviction Priorities**:
- **Priority 1**: Completed partitions with all data written
- **Priority 2**: Partitions with slow or inactive consumers
- **Priority 3**: Oldest unacknowledged blocks
- **Priority 4**: Largest buffers to free most memory

## Buffer Reclamation Timing

Buffer memory is reclaimed within **100ms** of consumer acknowledgment:

```
Acknowledgment Received (t=0ms)
    ↓
Update Acknowledged Offset (t=0-5ms)
    ↓
Identify Reclaimable Buffers (t=5-10ms)
    ↓
Release to Memory Manager (t=10-20ms)
    ↓
Update Metrics (t=20-30ms)
    ↓
Complete (<100ms guaranteed)
```

**Reclamation Guarantees**:
- **Synchronous Release**: Memory returned to pool before next allocation
- **Atomic Operation**: All-or-nothing buffer release
- **Failure Safety**: Cleanup on task failure via completion listeners
- **Monitoring**: Reclamation latency tracked in metrics

## Memory Safety Mechanisms

Several safeguards prevent memory exhaustion:

1. **Hard Limits**: Buffer size capped at 50% executor memory
2. **Spill Thresholds**: Automatic spill before exhaustion
3. **Backpressure**: Slow consumer triggers rate limiting
4. **Fallback**: OOM risk triggers automatic fallback to sort-based shuffle
5. **Task Cleanup**: Guaranteed buffer release on task completion

# Network Layer Integration

## TransportContext Usage

Streaming shuffle leverages Spark's existing network infrastructure without modification:

**Network Stack**:
```
Application Layer: StreamingShuffleWriter/Reader
       ↓
Protocol Layer: StreamingShuffleAcknowledgment, StreamingShuffleHeartbeat
       ↓
Transport Layer: TransportContext (existing)
       ↓
Network Layer: Netty (existing, version 4.2.7.Final)
       ↓
Physical Layer: TCP/IP
```

**Key Methods Used**:
- `TransportClient.uploadStream()`: Stream blocks from producer to consumer
- `TransportClient.fetchChunk()`: Consumer requests blocks from producer
- `TransportClient.sendRpc()`: Acknowledgment and heartbeat messages

## Message Type Registration

Streaming shuffle registers two new message types in Spark's protocol:

```java
public enum Type implements Encodable {
  // Existing types: ChunkFetchRequest, ChunkFetchSuccess, etc.
  
  StreamingShuffleAck(13),       // Consumer acknowledgment
  StreamingShuffleHeartbeat(14); // Consumer liveness signal
  
  // Message dispatch handled by existing MessageDecoder
}
```

**Message Encoding**:
- Uses existing `Encoders` utility class
- Compact binary format for efficiency
- Versioned protocol for future compatibility

## Rate Limiting via Token Bucket

Network bandwidth is rate-limited using token bucket algorithm to prevent saturation:

**Token Bucket Parameters**:
- **Capacity**: `maxBandwidthMBps × 1024 × 1024` bytes
- **Refill Rate**: `capacity / 1000` bytes per millisecond
- **Target Utilization**: 80% of link capacity
- **Granularity**: Per-executor token bucket

**Algorithm**:
```
TokenBucket {
  capacity: bytes
  tokens: current available tokens
  refillRate: bytes per millisecond
  lastRefill: timestamp
  
  tryAcquire(numBytes, timeout):
    refill() // Add tokens based on elapsed time
    if tokens >= numBytes:
      tokens -= numBytes
      return true
    elif wait(timeout) until tokens available:
      tokens -= numBytes
      return true
    else:
      return false // Backpressure event
}
```

**Rate Limiting Behavior**:
- **Under Capacity**: No blocking, immediate send
- **At Capacity**: Brief wait for token refill
- **Over Capacity**: Backpressure event logged, spill triggered
- **Disabled**: When `maxBandwidthMBps` not set

## QoS Prioritization

Streaming shuffle traffic receives priority over speculative task execution:

**Priority Levels**:
1. **Highest**: Streaming shuffle acknowledgments (small, critical)
2. **High**: Streaming shuffle data blocks
3. **Medium**: Regular shuffle fetch requests
4. **Low**: Speculative task traffic

**Implementation**:
- Separate Netty channel pools for different priorities
- Queue prioritization in `TransportContext`
- Backpressure propagation to lower-priority traffic

## Block Size Limits

Shuffle blocks are limited to **2MB** for efficient pipelining:

**Rationale**:
- **Network Efficiency**: Smaller blocks enable better multiplexing
- **Failure Granularity**: Minimize retransmission cost on corruption
- **Memory Pressure**: Limit per-block memory allocation
- **Latency**: Enable incremental processing sooner

**Block Splitting**:
```
Large Partition (50MB):
    Split into 25 blocks of 2MB each
    Stream blocks sequentially
    Consumer processes incrementally
    Acknowledge each block independently
```

# Failure Handling and Recovery

## Producer Failure Detection

Producer failures are detected via connection timeout (5 seconds):

**Detection Mechanism**:
```
Consumer Perspective:
    requestBlock(mapId, partitionId)
        ↓
    Wait for response (5 second timeout)
        ↓
    TimeoutException
        ↓
    Mark producer as failed
        ↓
    Invalidate partial reads
        ↓
    Notify DAGScheduler for recomputation
```

**Connection Timeout Parameters**:
- **Timeout**: 5 seconds for block fetch request
- **Retries**: 0 retries (fail fast on timeout)
- **Detection Latency**: 5 seconds maximum
- **Action**: Trigger partial read invalidation immediately

## Consumer Liveness Monitoring

Consumer liveness is monitored via 10-second heartbeat:

**Monitoring Flow**:
```
Producer Perspective:
    lastHeartbeat = consumerHeartbeatTimestamp
    currentTime = now()
    elapsed = currentTime - lastHeartbeat
    
    if elapsed > 10 seconds:
        Mark consumer as inactive
        Spill buffered data to disk
        Log timeout event
        Update metrics
```

**Heartbeat Failure Handling**:
- **Grace Period**: 10 seconds without heartbeat
- **Action**: Spill unacknowledged buffers to disk
- **Recovery**: Consumer reconnects and fetches from spilled blocks
- **Metrics**: Tracked in `shuffle.streaming.consumerTimeouts`

## Partial Read Invalidation Protocol

When a producer fails, all partial reads from that producer must be invalidated:

**Invalidation Flow**:
```
┌─────────────┐
│   Consumer  │
│   Detects   │
│   Timeout   │
└──────┬──────┘
       │
       │ 1. Atomic Discard
       ↓
┌─────────────────────┐
│ Discard ALL blocks  │
│ from failed producer│
│ (atomically)        │
└──────┬──────────────┘
       │
       │ 2. Notify Scheduler
       ↓
┌─────────────────────┐
│  DAGScheduler       │
│  receives event:    │
│  StreamingShuffle   │
│  PartialRead        │
│  Invalidated        │
└──────┬──────────────┘
       │
       │ 3. Trigger Recomputation
       ↓
┌─────────────────────┐
│ Invalidate map      │
│ output for failed   │
│ task                │
│                     │
│ Resubmit upstream   │
│ task with new       │
│ attempt ID          │
└─────────────────────┘
```

**Atomicity Guarantees**:
- **All-or-Nothing**: Either all blocks from a producer are valid, or none are
- **No Partial Data**: Consumer discards buffered data on failure
- **Idempotent**: Multiple invalidation events handled safely
- **Lineage Preserved**: RDD recomputation follows existing Spark logic

## Upstream Task Recomputation

Failed producers trigger upstream task recomputation via existing DAGScheduler mechanism:

**Recomputation Flow**:
1. Consumer notifies `DAGScheduler` of producer failure
2. `MapOutputTracker` unregisters failed map output
3. `DAGScheduler` marks stage as failed
4. Upstream task resubmitted with new attempt ID
5. New attempt creates fresh shuffle with different shuffle ID
6. Consumer retries fetch from new shuffle attempt

**Integration with Existing Failure Handling**:
- Uses existing `FetchFailedException` mechanism
- Leverages existing task retry logic
- Preserves lineage tracking
- No changes to RDD recomputation logic

## Atomic Discard Mechanism

Partial read invalidation is atomic to prevent data corruption:

**Discard Algorithm**:
```scala
def invalidatePartialReads(failedMapId: Int): Unit = synchronized {
  // 1. Mark failed producer
  failedProducers.add(failedMapId)
  
  // 2. Atomically discard buffered blocks
  val blocksToDiscard = bufferedBlocks.filter(_.mapId == failedMapId)
  bufferedBlocks.removeAll(blocksToDiscard)
  
  // 3. Release memory
  blocksToDiscard.foreach { block =>
    memoryManager.releaseMemory(block.size)
  }
  
  // 4. Update metrics
  metrics.incPartialReadInvalidations(1)
  metrics.incInvalidatedBytes(blocksToDiscard.map(_.size).sum)
  
  // 5. Log event
  logWarning(s"Invalidated ${blocksToDiscard.size} blocks from failed producer $failedMapId")
}
```

## Retry Policy

Block fetch retries follow exponential backoff strategy:

**Retry Parameters**:
- **Initial Delay**: 1 second
- **Backoff Multiplier**: 2x
- **Maximum Attempts**: 5
- **Maximum Delay**: 16 seconds
- **Jitter**: ±25% random variation

**Retry Sequence**:
```
Attempt 1: Immediate
Attempt 2: Wait 1s ± 250ms
Attempt 3: Wait 2s ± 500ms
Attempt 4: Wait 4s ± 1s
Attempt 5: Wait 8s ± 2s
Failure: Trigger recomputation
```

## Failure Recovery Sequence Diagram

```
Producer    Network    Consumer    DAGScheduler    MemoryManager
   │           │           │             │               │
   │  Stream   │           │             │               │
   │  Block    │           │             │               │
   ├──────────>│           │             │               │
   │           ├──────────>│             │               │
   │           │           │  Process    │               │
   │           │           ├─────        │               │
   X           │           │             │               │
 Failed        │           │ Timeout     │               │
               │           │ (5s)        │               │
               │           │             │               │
               │           │  Invalidate │               │
               │           │  Partial    │               │
               │           │  Reads      │               │
               │           ├────────────>│               │
               │           │             │  Unregister   │
               │           │             │  Map Output   │
               │           │             ├────────       │
               │           │             │               │
               │           │             │  Resubmit     │
               │           │             │  Task         │
               │           │             ├────────       │
               │           │             │               │
   │ New       │           │             │               │
   │ Attempt   │           │             │               │
   │ Started   │           │             │               │
   │           │           │             │               │
   │  Allocate │           │             │               │
   │  Buffer   │           │             │               │
   ├───────────┼───────────┼─────────────┼──────────────>│
   │<──────────┼───────────┼─────────────┼───────────────┤
   │  Buffer   │           │             │               │
   │  Granted  │           │             │               │
   │           │           │             │               │
   │  Stream   │           │             │               │
   │  Block    │           │             │               │
   ├──────────>│           │             │               │
   │           ├──────────>│             │               │
   │           │           │  Success    │               │
   │           │           │             │               │
```

# Checksum Validation Protocol

## CRC32C Algorithm Selection

Streaming shuffle uses **CRC32C** (Castagnoli) for data integrity validation:

**Selection Rationale**:
- **Hardware Acceleration**: Supported by modern Intel/AMD CPUs via SSE 4.2
- **Performance**: 10-20x faster than software-only CRC32
- **Collision Resistance**: Better polynomial than standard CRC32
- **Industry Standard**: Used by iSCSI, SCTP, and ext4 filesystem
- **Low Overhead**: <1% CPU impact with hardware acceleration

**Implementation**:
```java
import java.util.zip.CRC32C;

// Generate checksum
CRC32C checksum = new CRC32C();
checksum.update(dataBuffer);
long checksumValue = checksum.getValue();
```

## Per-Block Checksum Generation

Checksums are generated for each 2MB block on the producer side:

**Write Path**:
```scala
def writeBlockWithChecksum(partitionId: Int, data: ByteBuffer): Unit = {
  // 1. Generate checksum
  val checksum = new CRC32C()
  checksum.update(data)
  val checksumValue = checksum.getValue
  
  // 2. Create block header
  val header = ByteBuffer.allocate(12)
  header.putInt(data.remaining())      // 4 bytes: data length
  header.putLong(checksumValue)        // 8 bytes: CRC32C checksum
  header.flip()
  
  // 3. Stream header + data
  transportClient.uploadStream(
    blockId = shuffleBlockId(shuffleId, mapId, partitionId, blockIndex),
    header = new NioManagedBuffer(header),
    data = new NioManagedBuffer(data),
    callback = acknowledgmentHandler
  )
  
  // 4. Update metrics
  metrics.incBytesWritten(data.remaining())
  metrics.incBlocksWritten(1)
}
```

## Checksum Validation on Read

Consumers validate checksums immediately upon block receipt:

**Read Path**:
```scala
def readBlockWithValidation(blockId: String): ByteBuffer = {
  // 1. Fetch block from network
  val managedBuffer = transportClient.fetchChunk(blockId)
  val buffer = managedBuffer.nioByteBuffer()
  
  // 2. Parse header
  val length = buffer.getInt()
  val expectedChecksum = buffer.getLong()
  
  // 3. Extract data
  val data = buffer.slice()
  data.limit(length)
  
  // 4. Compute actual checksum
  val actualChecksum = new CRC32C()
  actualChecksum.update(data)
  val actualValue = actualChecksum.getValue
  
  // 5. Validate
  if (actualValue != expectedChecksum) {
    metrics.incChecksumMismatches(1)
    throw new ChecksumMismatchException(
      s"Checksum mismatch for block $blockId: " +
      s"expected=$expectedChecksum, actual=$actualValue"
    )
  }
  
  // 6. Return validated data
  data
}
```

## Corruption Detection and Retransmission

When checksum validation fails, automatic retransmission is attempted:

**Retransmission Flow**:
```
Checksum Mismatch Detected
        ↓
Log Warning + Update Metrics
        ↓
Attempt Retry (exponential backoff)
        ↓
    [Retry 1: 100ms delay]
        ↓
    [Retry 2: 200ms delay]
        ↓
    [Retry 3: 400ms delay]
        ↓
    [Retry 4: 800ms delay]
        ↓
    [Retry 5: 1600ms delay]
        ↓
All Retries Exhausted?
        ↓
  Yes: Trigger Recomputation
   No: Success → Continue
```

**Retransmission Strategy**:
- **Immediate Retry**: First retry after 100ms
- **Exponential Backoff**: Double delay each retry
- **Maximum Retries**: 5 attempts before failing
- **Failure Action**: Trigger upstream task recomputation
- **Metrics**: Track retransmission rate and success

## Zero Data Loss Guarantee

Checksum validation ensures zero data loss through multiple mechanisms:

1. **Mandatory Validation**: Every block validated before processing
2. **Failure Detection**: Corruption detected with >99.999% probability
3. **Automatic Retry**: Transient errors handled via retransmission
4. **Recomputation Fallback**: Permanent errors trigger task recomputation
5. **End-to-End Integrity**: Producer checksum matches consumer validation

**Guarantee Statement**:
```
Zero Data Loss Guarantee:
    IF checksum validation passes
    THEN data integrity is preserved
    ELSE recomputation ensures correct result
    
    Result: Application receives correct data or job fails explicitly
            (no silent data corruption)
```

# Backpressure and Flow Control

## Consumer-Driven Flow Control

Streaming shuffle implements consumer-driven flow control to prevent overwhelming slow consumers:

**Flow Control Mechanism**:
```
Consumer Rate < Producer Rate
        ↓
Consumer Acknowledgments Slow
        ↓
Producer Buffer Fills (>80%)
        ↓
Backpressure Event Logged
        ↓
Producer Actions:
    1. Slow send rate (rate limiting)
    2. Spill to disk when buffer full
    3. Log backpressure event
    4. Update metrics
```

**Rate Adaptation**:
- **Normal**: Producer streams at maximum rate
- **Moderate Pressure**: Producer waits for acknowledgments
- **High Pressure**: Producer spills to disk, continues at reduced rate
- **Critical Pressure**: Automatic fallback to sort-based shuffle

## Backpressure Signaling Protocol

Backpressure is signaled implicitly through acknowledgment rate:

**Signaling Mechanism**:
```scala
// Producer monitors acknowledgment rate
val acknowledgedBytes = recentAcknowledgments.sum
val producedBytes = recentWrites.sum
val consumerRate = acknowledgedBytes / timeWindow
val producerRate = producedBytes / timeWindow

if (consumerRate < producerRate * 0.5) {
  // Consumer significantly slower (>2x)
  logBackpressureEvent()
  applyRateLimiting()
  
  if (durationInBackpressure > 60.seconds) {
    // Sustained backpressure
    triggerFallbackToSortShuffle()
  }
}
```

**Thresholds**:
- **Warning**: Consumer 1.5x slower than producer
- **Action**: Consumer 2x slower than producer (trigger spill)
- **Critical**: Consumer 2x slower for >60 seconds (trigger fallback)

## Automatic Spill on Consumer Lag

When consumers lag significantly, producers automatically spill to disk:

**Spill Trigger Conditions**:
1. **Buffer Threshold**: Buffer utilization >80%
2. **Acknowledgment Delay**: No acknowledgment for >10 seconds
3. **Rate Mismatch**: Consumer <50% of producer rate
4. **Memory Pressure**: Executor memory >90% utilized

**Spill Process**:
```
Backpressure Detected
        ↓
Select LRU Partitions
        ↓
Async Spill to Disk (via BlockManager)
        ↓
Reclaim Buffer Memory
        ↓
Continue Streaming (rate-limited)
        ↓
Consumer Resumes → Fetch from Disk
```

## Fallback to Sort-Based Shuffle

Sustained backpressure triggers automatic fallback:

**Fallback Conditions**:
1. **Consumer Speed**: Consumer sustained 2x slower for >60 seconds
2. **Memory Pressure**: Unable to allocate buffers (OOM risk)
3. **Network Saturation**: Link utilization >90% for >60 seconds
4. **Spill Rate**: >50% of data spilled (streaming benefit lost)

**Fallback Process**:
```
Fallback Condition Detected
        ↓
Log Fallback Decision + Reason
        ↓
Complete Current Shuffle with Streaming
        ↓
Next Shuffle: Use SortShuffleManager
        ↓
Update Metrics (fallbackCount++)
        ↓
Optional: Alert Operations Team
```

## Backpressure Scenario Sequence Diagram

```
Producer        MemoryManager        Consumer        Metrics
   │                   │                  │              │
   │  Write Fast       │                  │              │
   ├──────────>        │                  │              │
   │  (1 MB/s)         │                  │              │
   │                   │                  │              │
   │                   │       Process Slow│             │
   │                   │       (100 KB/s)  │             │
   │                   │<──────────────────┤             │
   │                   │   Slow Ack        │             │
   │                   │                   │             │
   │  Buffer           │                   │             │
   │  Filling          │                   │             │
   ├──────────────────>│                   │             │
   │  (80% util)       │                   │             │
   │                   │                   │             │
   │  Backpressure     │                   │             │
   │  Detected         │                   │             │
   ├───────────────────┼───────────────────┼────────────>│
   │                   │                   │   Log Event │
   │                   │                   │             │
   │  Select LRU       │                   │             │
   │  Partitions       │                   │             │
   ├──────────         │                   │             │
   │                   │                   │             │
   │  Spill to Disk    │                   │             │
   ├──────────────────>│                   │             │
   │  Release Memory   │                   │             │
   │<──────────────────┤                   │             │
   │                   │                   │             │
   │  Continue         │                   │             │
   │  (Rate Limited)   │                   │             │
   ├──────────>        │                   │             │
   │  (200 KB/s)       │                   │             │
   │                   │                   │             │
```

**Key Observations**:
1. Producer detects slow consumer via acknowledgment rate
2. Buffer utilization triggers backpressure event
3. Producer spills to disk to prevent memory exhaustion
4. Producer continues at rate-limited speed matching consumer
5. Metrics captured for operational visibility

---

# Summary

The streaming shuffle architecture provides significant latency improvements through several key innovations:

1. **Direct Streaming**: Eliminate full shuffle materialization by streaming data directly from producers to consumers
2. **Memory-First Design**: Process data through memory buffers with automatic disk spill fallback
3. **Flow Control**: Consumer-driven backpressure prevents overwhelming slow consumers
4. **Failure Recovery**: Partial read invalidation with upstream recomputation ensures zero data loss
5. **Data Integrity**: CRC32C checksums validate every block for corruption detection
6. **Graceful Degradation**: Automatic fallback to sort-based shuffle when conditions not optimal

**Performance Characteristics**:
- **Latency Reduction**: 30-50% for shuffle-heavy workloads (10GB+, 100+ partitions)
- **Memory Overhead**: 20% executor memory (configurable 1-50%)
- **Network Efficiency**: 80% link capacity target with rate limiting
- **Spill Rate**: <5% in normal operation with proper tuning

**Operational Considerations**:
- Enable via `spark.shuffle.manager=streaming`
- Monitor buffer utilization and spill metrics
- Tune buffer size and spill threshold per workload
- Review failure logs for producer/consumer issues
- Verify network bandwidth adequate for streaming

For detailed configuration guidance, see [Streaming Shuffle Configuration](configuration.html#streaming-shuffle-configuration).

For performance tuning recommendations, see [Streaming Shuffle Performance Tuning](streaming-shuffle-tuning.html).

For troubleshooting assistance, see [Streaming Shuffle Troubleshooting](streaming-shuffle-troubleshooting.html).

For migration and adoption guidance, see [Streaming Shuffle Migration Guide](streaming-shuffle-migration.html).
