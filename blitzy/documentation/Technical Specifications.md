# Technical Specification

# 0. Agent Action Plan

## 0.1 Intent Clarification

### 0.1.1 Core Feature Objective

Based on the prompt, the Blitzy platform understands that the new feature requirement is to **add a streaming shuffle capability to the Apache Spark (v4.1.0-SNAPSHOT) core runtime** as an opt-in alternative to the existing sort-based shuffle. The implementation introduces a producer-to-consumer data pipeline that eliminates shuffle materialization latency by streaming buffered data directly from map tasks to reduce tasks with integrated backpressure flow control, memory-spill coordination, and fault-tolerant partial-read invalidation.

The feature requirements, restated with enhanced technical clarity:

- **Streaming Shuffle Manager**: Create a new `StreamingShuffleManager` class implementing the existing `org.apache.spark.shuffle.ShuffleManager` trait (defined in `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala`). This manager is activated via the configuration key `spark.shuffle.manager=streaming` and coexists with the default `SortShuffleManager` as a pluggable alternative.

- **Streaming Shuffle Writer**: Create a `StreamingShuffleWriter` extending the abstract `ShuffleWriter[K, V]` class (defined in `core/src/main/scala/org/apache/spark/shuffle/ShuffleWriter.scala`). This writer allocates per-partition memory buffers capped at a configurable percentage of executor memory (default 20%) and pipelines buffered data directly to consumer executors through the existing Netty-based transport layer (`common/network-common/`), rather than materializing to disk as `SortShuffleWriter` does.

- **Backpressure Protocol**: Create a `BackpressureProtocol` component implementing heartbeat-based consumer-to-producer flow control with 5-second timeout, per-executor rate limiting via token bucket algorithm at 80% link capacity, buffer utilization monitoring across concurrent shuffles, priority arbitration by partition count and data volume, and telemetry emission for operational visibility.

- **Streaming Shuffle Reader**: Create a `StreamingShuffleReader` implementing the `ShuffleReader[K, C]` trait (defined in `core/src/main/scala/org/apache/spark/shuffle/ShuffleReader.scala`). This reader polls producers for available data before shuffle completion (in-progress block requests), detects producer failure via connection timeout (5 seconds), triggers upstream DAG recomputation on failure, sends acknowledgment positions for buffer reclamation, and validates block integrity via CRC32C checksums.

- **Memory Spill Manager**: Create a `MemorySpillManager` component that monitors buffer utilization at 100ms polling intervals, triggers automatic disk spill via LRU partition eviction when buffer occupancy exceeds 80% (configurable 50-95%), integrates with the existing `BlockManager` disk storage for persistence, and reclaims memory within 100ms of consumer acknowledgment.

- **Performance Targets**: Achieve 30-50% end-to-end latency reduction for shuffle-heavy workloads (10GB+ data, 100+ partitions), 5-10% improvement for CPU-bound workloads through reduced scheduler overhead, and zero performance regression for memory-bound workloads via automatic fallback.

- **Fault Tolerance**: Guarantee zero data loss under all failure scenarios including producer crashes, consumer failures, network partitions, and memory exhaustion with automatic fallback to sort-based shuffle under sustained degradation conditions.

**Implicit requirements detected:**

- The `ShuffleManager.create()` factory method in `ShuffleManager` companion object (line 106–109 of `ShuffleManager.scala`) must be extended to recognize the `"streaming"` short name and map it to the `StreamingShuffleManager` class name.
- The `SHUFFLE_MANAGER` configuration entry in `core/src/main/scala/org/apache/spark/internal/config/package.scala` (line 1730–1734) currently defaults to `"sort"` — new streaming-specific configuration keys must be added to this same config package.
- The `ShuffleWriteProcessor` (in `core/src/main/scala/org/apache/spark/shuffle/ShuffleWriteProcessor.scala`) invokes `SparkEnv.get.shuffleManager.getWriter()` (line 51–52) and then calls the push-based shuffle block mechanism via `IndexShuffleBlockResolver`. The streaming writer must conform to this lifecycle without breaking the push-based shuffle codepath for the sort-based manager.
- The existing `ShuffleWriteMetricsReporter` and `ShuffleReadMetricsReporter` interfaces (in `core/src/main/scala/org/apache/spark/shuffle/metrics.scala`) must be extended or supplemented with streaming-specific telemetry counters.
- Streaming shuffle blocks must be addressable via the existing `BlockId` taxonomy and compatible with `MapOutputTracker` for shuffle location tracking.

### 0.1.2 Special Instructions and Constraints

**Architectural Preservation Directives:**

- **Zero modification** to RDD/DataFrame/Dataset user-facing APIs — the streaming shuffle must be transparent to application code
- **Zero modification** to DAG scheduler, task scheduling algorithms, executor lifecycle management, lineage tracking, or fault recovery model
- **Coexistence**: The existing `SortShuffleManager` implementation (in `core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala`) must remain completely untouched and continue operating as the production-stable fallback
- **Isolation**: All streaming logic must reside in dedicated classes within a new package (`org.apache.spark.shuffle.streaming`) with zero cross-contamination into existing shuffle code paths
- **Block Manager interface contracts** must be preserved — the streaming implementation integrates with existing disk storage mechanisms rather than replacing them
- **Task serialization/deserialization protocols** must remain unchanged

**Configuration Constraints:**

- Streaming shuffle is **opt-in only** — `spark.shuffle.streaming.enabled` defaults to `false`
- Configuration changes require executor restart (no dynamic reconfiguration in v1)
- Telemetry overhead limited to <1% CPU utilization
- Log volume capped at <10MB/hour per executor
- Debug logging disabled by default, enabled via `spark.shuffle.streaming.debug=true`

**Memory Management Constraints:**

- Streaming buffers limited to 20% of executor memory (configurable 1-50%)
- Per-partition buffer size formula: `(executorMemory * bufferPercent) / numPartitions`
- Buffer allocation tracked via the existing `MemoryManager` interface (`core/src/main/scala/org/apache/spark/memory/MemoryManager.scala`)
- Spill trigger enforced at 80% utilization (configurable 50-95%)
- Zero memory leaks under failure scenarios (validated via 2-hour stress test)

**Network Transfer Constraints:**

- Leverage existing `org.apache.spark.network.TransportContext` (`common/network-common/src/main/java/org/apache/spark/network/TransportContext.java`) for streaming
- Rate limiting via token bucket: refill rate = `maxBandwidthMBps / numConcurrentShuffles`
- TCP keepalive with 5-second interval
- Block size limited to 2MB for pipelining efficiency
- Connection timeout: 5 seconds for producer failure detection
- Heartbeat interval: 10 seconds for consumer liveness monitoring
- Retry policy: exponential backoff starting 1 second, max 5 attempts

**Automatic Fallback Conditions:**

- Consumer sustained 2x slower than producer for >60 seconds
- Memory pressure prevents buffer allocation (OOM risk)
- Network saturation exceeds 90% link capacity
- Producer/consumer version mismatch (compatibility check)

### 0.1.3 Technical Interpretation

These feature requirements translate to the following technical implementation strategy:

- **To implement the streaming shuffle manager**, we will create a new `StreamingShuffleManager` class in `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala` that implements the `ShuffleManager` trait, and modify the `ShuffleManager` companion object's `shortShuffleMgrNames` map in `ShuffleManager.scala` (line 112–114) to register the `"streaming"` shorthand.

- **To implement the streaming writer**, we will create `StreamingShuffleWriter` extending `ShuffleWriter[K, V]` that replaces disk-materialization with memory-buffered network streaming, while still producing a valid `MapStatus` compatible with `MapOutputTracker` expectations.

- **To implement the streaming reader**, we will create `StreamingShuffleReader` implementing the `ShuffleReader[K, C]` trait that polls producers for in-progress data availability, replacing the `ShuffleBlockFetcherIterator` pattern used by the existing `BlockStoreShuffleReader`.

- **To implement backpressure**, we will create `BackpressureProtocol` as a standalone component leveraging the existing Netty `TransportClient`/`TransportServer` infrastructure from `common/network-common/` for consumer-to-producer signaling.

- **To implement memory spill management**, we will create `MemorySpillManager` that integrates with the existing `BlockManager` (specifically its `DiskStore` subsystem in `core/src/main/scala/org/apache/spark/storage/`) for disk persistence and with `MemoryManager` for allocation tracking.

- **To add configuration support**, we will add new `ConfigBuilder` entries in `core/src/main/scala/org/apache/spark/internal/config/package.scala` following the existing `spark.shuffle.*` naming convention.

- **To add streaming-specific telemetry**, we will create a `StreamingShuffleMetricsSource` class following the pattern of `ExecutorMetricsSource` (in `core/src/main/scala/org/apache/spark/executor/ExecutorMetricsSource.scala`) that registers Dropwizard gauges and counters for buffer utilization, spill counts, backpressure events, and partial read invalidations.

- **To validate correctness and performance**, we will create comprehensive test suites under `core/src/test/scala/org/apache/spark/shuffle/streaming/` following the established testing patterns in `core/src/test/scala/org/apache/spark/shuffle/sort/`.

## 0.2 Repository Scope Discovery

### 0.2.1 Comprehensive File Analysis

The Apache Spark monorepo (version 4.1.0-SNAPSHOT) is a Maven-based multi-module project structured around the `core/` module for runtime, `common/` for shared network and utility libraries, and domain-specific modules for SQL, streaming, and ML. The shuffle subsystem spans both `core/` (Scala/Java shuffle abstractions and sort-based implementation) and `common/network-common/` plus `common/network-shuffle/` (Netty-based transport and external shuffle service).

#### Existing Modules to Modify

| File Path | Purpose | Modification Type |
|-----------|---------|-------------------|
| `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` | ShuffleManager trait and factory companion object | Add `"streaming"` to `shortShuffleMgrNames` map (line 112-114) |
| `core/src/main/scala/org/apache/spark/internal/config/package.scala` | Central Spark configuration definitions | Add new `spark.shuffle.streaming.*` ConfigBuilder entries alongside existing shuffle configs (after line ~1734) |
| `core/src/main/scala/org/apache/spark/shuffle/metrics.scala` | ShuffleWriteMetricsReporter and ShuffleReadMetricsReporter interfaces | Add streaming-specific metric methods (buffer utilization, spill count, backpressure events) |
| `core/src/main/scala/org/apache/spark/executor/ShuffleReadMetrics.scala` | Accumulator-based shuffle read metrics collection | Add streaming shuffle read metric accumulators and merge logic |
| `core/src/main/scala/org/apache/spark/executor/ShuffleWriteMetrics.scala` | Accumulator-based shuffle write metrics collection | Add streaming shuffle write metric accumulators |
| `core/src/main/scala/org/apache/spark/executor/Executor.scala` | Executor process — registers metric sources | Register `StreamingShuffleMetricsSource` alongside existing metrics (around line 163-166) |
| `core/src/main/scala/org/apache/spark/shuffle/ShuffleWriteProcessor.scala` | Controls ShuffleWriter lifecycle per map task | Add awareness of streaming shuffle handle type to avoid push-based shuffle path for streaming writes (lines 73-86) |

#### Integration Point Discovery

**API Endpoints That Connect to the Feature:**
- `SparkEnv.initializeShuffleManager()` (line 223-227 in `SparkEnv.scala`) — where `ShuffleManager.create()` is called; streaming manager instantiation flows through this path
- `SparkContext` (line 589) — calls `_env.initializeShuffleManager()` during driver initialization
- `ShuffleWriteProcessor.write()` (line 43-101 in `ShuffleWriteProcessor.scala`) — obtains writer from manager and orchestrates the map task write lifecycle
- `SortShuffleManager.getReader()` (line 119-142 in `SortShuffleManager.scala`) — pattern to follow for streaming reader construction

**Memory Management Subsystem:**
- `core/src/main/scala/org/apache/spark/memory/MemoryManager.scala` — abstract memory manager with execution and storage pools
- `core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala` — concrete implementation with dynamic memory allocation between pools
- `core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala` — execution memory tracking for shuffle buffers

**Network Transport Layer:**
- `common/network-common/src/main/java/org/apache/spark/network/TransportContext.java` — creates transport servers and client factories
- `common/network-common/src/main/java/org/apache/spark/network/client/TransportClient.java` — sends RPC requests and stream fetches
- `common/network-common/src/main/java/org/apache/spark/network/server/TransportServer.java` — accepts incoming connections
- `common/network-common/src/main/java/org/apache/spark/network/client/TransportClientFactory.java` — manages connection pooling
- `common/network-common/src/main/java/org/apache/spark/network/protocol/StreamRequest.java` — existing stream protocol messages
- `common/network-common/src/main/java/org/apache/spark/network/protocol/StreamResponse.java` — stream response framing
- `core/src/main/scala/org/apache/spark/network/netty/NettyBlockTransferService.scala` — Spark-specific block transfer using transport layer

**Block Storage Subsystem:**
- `core/src/main/scala/org/apache/spark/storage/BlockManager.scala` — central storage manager for block put/get operations and shuffle data access
- `core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala` — existing iterator for fetching shuffle blocks from remote executors
- `core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala` — sort-based shuffle block data/index file resolution

**MapOutput Tracking:**
- `core/src/main/scala/org/apache/spark/MapOutputTracker.scala` — `MapOutputTrackerMaster` (driver) and `MapOutputTrackerWorker` (executor) for tracking shuffle output locations

**Checksum Infrastructure:**
- `core/src/main/scala/org/apache/spark/shuffle/ShuffleChecksumUtils.scala` — existing checksum utilities for shuffle block integrity
- `core/src/main/java/org/apache/spark/shuffle/checksum/ShuffleChecksumSupport.java` — checksum support interface

**Existing Test Suites (for pattern reference):**
- `core/src/test/scala/org/apache/spark/shuffle/sort/SortShuffleManagerSuite.scala` — manager test patterns
- `core/src/test/scala/org/apache/spark/shuffle/sort/SortShuffleWriterSuite.scala` — writer test patterns
- `core/src/test/scala/org/apache/spark/shuffle/BlockStoreShuffleReaderSuite.scala` — reader test patterns
- `core/src/test/scala/org/apache/spark/shuffle/sort/BypassMergeSortShuffleWriterSuite.scala` — alternative writer test patterns

### 0.2.2 New File Requirements

#### New Source Files to Create

| File Path | Purpose | Estimated Size |
|-----------|---------|----------------|
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala` | Main ShuffleManager implementation that factory-creates streaming writer/reader, manages shuffle registration/unregistration, and handles graceful fallback to sort-based shuffle | ~300 lines |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriter.scala` | Per-partition memory buffer management, network streaming pipeline to consumers, backpressure-triggered spill coordination, CRC32C checksum generation per block | ~500 lines |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReader.scala` | In-progress block polling from producers, partial read invalidation on producer failure, consumer acknowledgment protocol, checksum validation on receive | ~400 lines |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureProtocol.scala` | Heartbeat-based flow control, token bucket rate limiting, buffer utilization monitoring, priority arbitration for concurrent shuffles, telemetry emission | ~350 lines |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/MemorySpillManager.scala` | 100ms polling of memory manager, LRU eviction of largest partitions, BlockManager disk persistence integration, buffer reclamation on ack, spill metrics | ~400 lines |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandle.scala` | Opaque handle subclassing `ShuffleHandle` for streaming shuffle identification, carrying streaming-specific metadata | ~30 lines |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleBlockResolver.scala` | Block resolution for streaming shuffle data, integrating with BlockManager for spilled blocks | ~150 lines |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetricsSource.scala` | Dropwizard metrics source exposing streaming shuffle gauges and counters via JMX and Spark metrics system | ~100 lines |
| `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleConfig.scala` | Centralized configuration constants and validation for all `spark.shuffle.streaming.*` parameters | ~80 lines |

#### New Test Files to Create

| File Path | Purpose | Estimated Size |
|-----------|---------|----------------|
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriterSuite.scala` | Unit tests: buffer allocation, partition-level memory tracking, spill trigger at 80% threshold, checksum generation, producer failure cleanup | ~400 lines |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/BackpressureProtocolSuite.scala` | Unit tests: consumer ack processing, token bucket rate limiting, timeout detection, priority arbitration under concurrent shuffle load | ~350 lines |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReaderSuite.scala` | Unit tests: in-progress block request, producer failure detection via connection timeout, partial read invalidation, checksum validation and retransmission | ~350 lines |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/MemorySpillManagerSuite.scala` | Unit tests: threshold monitoring, LRU eviction selection, buffer reclamation timing, spill metrics accuracy | ~250 lines |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala` | Unit tests: manager registration/unregistration, writer/reader factory methods, fallback condition detection | ~200 lines |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationTest.scala` | Integration tests: 10GB shuffle with 100 partitions, producer failure mid-shuffle, consumer slowdown spill trigger, network partition fallback, 5-concurrent-shuffle memory arbitration | ~500 lines |
| `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShufflePerformanceBenchmark.scala` | Performance benchmark: baseline sort vs streaming for groupByKey on 10GB dataset, 100 partitions; latency, memory utilization, spill frequency, bandwidth metrics | ~200 lines |

#### New Documentation Files to Create

| File Path | Purpose |
|-----------|---------|
| `docs/streaming-shuffle-guide.md` | Configuration reference for all `spark.shuffle.streaming.*` parameters, architecture design document with protocol specification, failure handling flows, and performance tuning guide |
| `docs/streaming-shuffle-troubleshooting.md` | Common issues, telemetry interpretation, debugging procedures, and known limitations |

### 0.2.3 Web Search Research Conducted

No external web research was required for this implementation. All architectural decisions and integration patterns are fully derivable from the existing Apache Spark codebase analysis:

- The `ShuffleManager` pluggable interface pattern is well-established and documented in the codebase
- The Netty-based transport layer (`common/network-common/`) provides all required streaming primitives (stream requests, responses, chunked transfers)
- The memory management subsystem (`core/src/main/scala/org/apache/spark/memory/`) provides the execution memory pool interface for buffer tracking
- CRC32C checksum support is already validated in the config at `core/src/main/scala/org/apache/spark/internal/config/package.scala` line 1668
- Token bucket rate limiting and backpressure patterns are standard distributed systems techniques requiring no external research

## 0.3 Dependency Inventory

### 0.3.1 Private and Public Packages

The streaming shuffle implementation leverages exclusively existing dependencies already present in the Apache Spark monorepo. No new external dependencies are required — the feature is built entirely within the existing `spark-core` module boundary using the transport, memory, and metrics subsystems that Spark already includes.

| Package Registry | Package Name | Version | Purpose in Streaming Shuffle |
|-----------------|-------------|---------|------------------------------|
| Maven Central | `org.apache.spark:spark-core_2.13` | 4.1.0-SNAPSHOT | Host module — all new streaming shuffle classes reside here |
| Maven Central | `org.apache.spark:spark-network-common_2.13` | 4.1.0-SNAPSHOT | TransportContext, TransportClient, TransportServer — provides the Netty-based streaming transport layer for producer-to-consumer data pipeline |
| Maven Central | `org.apache.spark:spark-network-shuffle_2.13` | 4.1.0-SNAPSHOT | ExternalBlockHandler, BlockTransferListener — shuffle block resolution and external shuffle service integration |
| Maven Central | `org.apache.spark:spark-unsafe_2.13` | 4.1.0-SNAPSHOT | Platform-level memory operations, MemoryAllocator, MemoryBlock — low-level buffer allocation for streaming buffers |
| Maven Central | `io.netty:netty-all` | 4.2.7.Final | Asynchronous I/O framework — underlying transport for streaming data channels, TCP keepalive, connection management |
| Maven Central | `io.dropwizard.metrics:metrics-core` | 4.2.33 | Gauge, Counter, Histogram metric primitives — streaming shuffle telemetry (buffer utilization, spill counts, backpressure events) |
| Maven Central | `io.dropwizard.metrics:metrics-jmx` | 4.2.33 | JMX metric exposure — external monitoring integration for streaming shuffle operational metrics |
| Maven Central | `com.esotericsoftware:kryo-shaded` | 4.0.3 | Object serialization — serializing shuffle records in streaming buffers |
| Maven Central | `com.fasterxml.jackson.core:jackson-databind` | 2.20.0 | JSON serialization — configuration and metadata exchange for backpressure protocol |
| Maven Central | `org.roaringbitmap:RoaringBitmap` | (parent-managed) | Compressed bitmap operations — partition tracking in streaming shuffle block resolver |
| Maven Central | `org.scala-lang:scala-library` | 2.13.17 | Scala standard library — all streaming shuffle Scala source files |
| Maven Central | `org.scalatest:scalatest_2.13` | 3.2.19 | Test framework — all streaming shuffle unit and integration test suites |
| Maven Central | `org.scalatestplus:mockito-5-12_2.13` | (parent-managed) | Mocking framework — mock BlockManager, MemoryManager, TransportClient in unit tests |
| Maven Central | `org.junit.jupiter:junit-jupiter` | 6.0.0 | JUnit 5 test runner — Java-based test support for any Java-layer tests |

### 0.3.2 Dependency Updates

#### Import Updates

The streaming shuffle feature introduces a new package `org.apache.spark.shuffle.streaming` but does not require changes to existing import structures across the broader codebase. The modifications are contained to specific integration points:

**Files requiring new import additions:**

- `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` — Add import for the streaming manager class reference in `shortShuffleMgrNames`:
  - New: `import org.apache.spark.shuffle.streaming.StreamingShuffleManager`

- `core/src/main/scala/org/apache/spark/shuffle/ShuffleWriteProcessor.scala` — Add import for streaming handle type detection:
  - New: `import org.apache.spark.shuffle.streaming.StreamingShuffleHandle`

- `core/src/main/scala/org/apache/spark/executor/Executor.scala` — Add import for streaming metrics source registration:
  - New: `import org.apache.spark.shuffle.streaming.StreamingShuffleMetricsSource`

**New files' internal imports** will follow existing patterns:

- All streaming shuffle classes import from `org.apache.spark.shuffle._` for base traits
- Network integration imports from `org.apache.spark.network.client._` and `org.apache.spark.network.server._`
- Memory integration imports from `org.apache.spark.memory._`
- Storage integration imports from `org.apache.spark.storage._`
- Configuration imports from `org.apache.spark.internal.config`
- Metrics imports from `com.codahale.metrics.{Gauge, MetricRegistry}` and `org.apache.spark.metrics.source.Source`

#### External Reference Updates

| File Pattern | Update Type |
|-------------|------------|
| `core/pom.xml` | No changes — all dependencies already declared |
| `pom.xml` (parent) | No changes — no new version properties needed |
| `docs/configuration.md` | Document new `spark.shuffle.streaming.*` configuration parameters |
| `docs/monitoring.md` | Document new streaming shuffle metrics and JMX beans |
| `README.md` | No changes — feature is internal shuffle optimization |

## 0.4 Integration Analysis

### 0.4.1 Existing Code Touchpoints

#### Direct Modifications Required

**`core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` (lines 106-118) — ShuffleManager Factory Registration**

The companion object's `shortShuffleMgrNames` map must include the `"streaming"` alias. Currently (lines 112-114):

```scala
val shortShuffleMgrNames = Map(
  "sort" -> classOf[SortShuffleManager].getName,
  "tungsten-sort" -> classOf[SortShuffleManager].getName)
```

This map is extended to add the `"streaming"` entry pointing to `StreamingShuffleManager`. The `getShuffleManagerClassName` method (line 111-118) performs case-insensitive lookup against this map, so the streaming manager will be resolved identically to how `"sort"` resolves to `SortShuffleManager`.

**`core/src/main/scala/org/apache/spark/internal/config/package.scala` (after line ~1734) — Configuration Entries**

New `ConfigBuilder` entries for all streaming shuffle parameters must be added in the shuffle configuration block, following the existing naming convention `spark.shuffle.*`:

- `spark.shuffle.streaming.enabled` — BooleanConf, default `false`
- `spark.shuffle.streaming.bufferSizePercent` — IntConf with range validation [1, 50], default `20`
- `spark.shuffle.streaming.spillThreshold` — IntConf with range validation [50, 95], default `80`
- `spark.shuffle.streaming.maxBandwidthMBps` — IntConf, default `0` (unlimited)
- `spark.shuffle.streaming.debug` — BooleanConf, default `false`

These follow the same pattern as `SHUFFLE_CHECKSUM_ENABLED` (line 1651) and `SHUFFLE_CHECKSUM_ALGORITHM` (line 1662).

**`core/src/main/scala/org/apache/spark/shuffle/metrics.scala` — Metrics Reporter Extension**

The `ShuffleWriteMetricsReporter` trait (lines 56-62) and `ShuffleReadMetricsReporter` trait (lines 28-46) must be extended with streaming-specific metric methods:

- Write metrics: `incStreamingBufferBytes(v: Long)`, `incStreamingSpillCount(v: Long)`, `incBackpressureEvents(v: Long)`
- Read metrics: `incPartialReadInvalidations(v: Long)`, `incStreamingBlocksReceived(v: Long)`, `incChecksumFailures(v: Long)`

These additions use the same `private[spark]` visibility modifier pattern as existing methods.

**`core/src/main/scala/org/apache/spark/executor/ShuffleReadMetrics.scala` — Read Metric Accumulators**

New `LongAccumulator` fields must be added alongside existing fields (lines 32-48) for streaming-specific read counters. The `TempShuffleReadMetrics` class (lines 246-300) must be updated in parallel to track the same streaming counters. The `setMergeValues` method (line 200) must incorporate the new counters.

**`core/src/main/scala/org/apache/spark/executor/ShuffleWriteMetrics.scala` — Write Metric Accumulators**

Parallel to the read metrics, new write metric accumulators for streaming buffer bytes, spill counts, and backpressure events must be added.

**`core/src/main/scala/org/apache/spark/shuffle/ShuffleWriteProcessor.scala` (lines 73-86) — Streaming Handle Awareness**

The write processor's push-based shuffle block path currently pattern-matches on `IndexShuffleBlockResolver`:

```scala
manager.shuffleBlockResolver match {
  case resolver: IndexShuffleBlockResolver => ...
  case _ =>
}
```

The streaming shuffle uses its own `StreamingShuffleBlockResolver`, so this existing `case _ =>` branch naturally handles the streaming case (no push-based block pushing for streaming). However, the streaming handle must be compatible with the `mapStatus.get` return on line 88.

**`core/src/main/scala/org/apache/spark/executor/Executor.scala` (lines 163-166) — Metrics Source Registration**

The streaming shuffle metrics source must be registered with the Spark `MetricsSystem` alongside existing sources. The registration follows the same pattern as `executorMetricsSource.foreach(_.register(env.metricsSystem))` on line 165.

#### Dependency Injections

**`SparkEnv.initializeShuffleManager()` (line 223-227 in `SparkEnv.scala`) — Manager Instantiation**

The `ShuffleManager.create(conf, isDriver)` call at line 226 invokes `Utils.instantiateSerializerOrShuffleManager` which uses reflection to instantiate the manager class. The `StreamingShuffleManager` must have a constructor matching the expected signature `(SparkConf, Boolean)` — `SparkConf` for configuration and `Boolean` for the `isDriver` flag.

**`BlockManager` (in `core/src/main/scala/org/apache/spark/storage/BlockManager.scala`) — Disk Storage for Spills**

The `MemorySpillManager` integrates with `BlockManager`'s existing disk store capabilities. Spilled partition data is persisted via `BlockManager.putBlockData()` or directly through `DiskBlockManager` for temporary shuffle files, following the same spill-to-disk pattern used by `ExternalSorter`.

**`MemoryManager` (in `core/src/main/scala/org/apache/spark/memory/MemoryManager.scala`) — Buffer Allocation Tracking**

Streaming buffer memory is allocated from the execution memory pool via the `acquireExecutionMemory()` interface. The `MemorySpillManager` monitors the execution memory pool utilization to trigger spills. This uses the same interface that `ExternalSorter` and `ShuffleExternalSorter` use for spill decisions.

**`MapOutputTracker` (in `core/src/main/scala/org/apache/spark/MapOutputTracker.scala`) — Shuffle Location Tracking**

The `StreamingShuffleWriter` must report a valid `MapStatus` upon completion, containing partition sizes and executor location. The `StreamingShuffleReader` queries `MapOutputTracker.getMapSizesByExecutorId()` for shuffle block locations, identical to the pattern in `SortShuffleManager.getReader()` (lines 128-137).

#### Database/Schema Updates

No database or persistent schema changes are required. The streaming shuffle is a runtime-only feature with the following transient state:

- In-memory partition buffers managed by `MemorySpillManager`
- Spilled partition files in the executor's local scratch space (same temp directory used by sort-based shuffle)
- Streaming shuffle metadata tracked in `MapOutputTrackerMaster` via standard `MapStatus` entries

### 0.4.2 Integration Architecture Diagram

```mermaid
graph TB
    subgraph Driver
        SC[SparkContext]
        SE[SparkEnv]
        MOTM[MapOutputTrackerMaster]
        DS[DAGScheduler]
    end

    subgraph "Executor (Producer)"
        SWP[ShuffleWriteProcessor]
        SSM[StreamingShuffleManager]
        SSW[StreamingShuffleWriter]
        BP[BackpressureProtocol]
        MSM[MemorySpillManager]
        MM[MemoryManager]
        BM1[BlockManager]
        TC1[TransportClient]
    end

    subgraph "Executor (Consumer)"
        SSR[StreamingShuffleReader]
        ACK[Acknowledgment Protocol]
        CV[Checksum Validator]
        TC2[TransportClient]
        BM2[BlockManager]
    end

    SC -->|initializeShuffleManager| SE
    SE -->|create| SSM
    DS -->|submit ShuffleMapTask| SWP
    SWP -->|getWriter| SSM
    SSM -->|factory| SSW
    SSW -->|allocate buffers| MM
    SSW -->|stream data| TC1
    SSW -->|backpressure signals| BP
    BP -->|spill trigger| MSM
    MSM -->|disk persist| BM1
    MSM -->|monitor| MM
    SSW -->|report MapStatus| MOTM

    TC1 -->|network stream| TC2
    TC2 -->|deliver blocks| SSR
    SSR -->|validate| CV
    SSR -->|send ack| ACK
    ACK -->|reclaim buffer| BP
    SSR -->|query locations| MOTM
    SSR -->|fallback fetch| BM2
end
```

### 0.4.3 Failure Handling Integration

**Producer Failure Detection Flow (integrates with DAGScheduler):**

- `StreamingShuffleReader` detects connection timeout (5 seconds) on the Netty `TransportClient` channel
- Reader invalidates all partial reads from the failed producer and discards buffered data
- Reader throws `FetchFailedException` (from `core/src/main/scala/org/apache/spark/shuffle/FetchFailedException.scala`) which the `TaskContext` captures via `setFetchFailed()`
- `DAGScheduler` receives the `FetchFailed` event and triggers recomputation of the upstream `ShuffleMapStage`
- On recomputation, the upstream task uses the configured shuffle manager (streaming or fallback) to rewrite shuffle output

**Consumer Failure Detection Flow (integrates with BlockManager):**

- `StreamingShuffleWriter` detects missing acknowledgments after 10-second heartbeat timeout
- Writer buffers unacknowledged data in memory
- If buffer exceeds 80% threshold, `MemorySpillManager` triggers disk spill via `BlockManager` integration
- When consumer reconnects, writer retransmits unacknowledged blocks from spill or memory
- If consumer permanently fails, the DAGScheduler resubmits the reduce task

**Automatic Fallback Integration:**

- `StreamingShuffleManager` monitors runtime conditions (consumer rate, memory pressure, network saturation)
- When fallback conditions are detected, the manager logs a warning and routes subsequent shuffle registrations to `SortShuffleManager` behavior
- In-flight streaming shuffles complete gracefully or spill to disk before fallback activation
- Fallback is per-shuffle, not global — other shuffles may continue streaming if conditions permit

## 0.5 Technical Implementation

### 0.5.1 File-by-File Execution Plan

Every file listed below MUST be created or modified as specified. Files are grouped by functional concern to enable parallel development within each group.

#### Group 1 — Core Feature Files (New)

- **CREATE: `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManager.scala`**
  - Implement `ShuffleManager` trait with constructor signature `(SparkConf, Boolean)`
  - Provide `registerShuffle()` returning `StreamingShuffleHandle` instances
  - Factory `getWriter()` returning `StreamingShuffleWriter` instances, consuming `ShuffleWriteMetricsReporter`
  - Factory `getReader()` returning `StreamingShuffleReader` instances with `MapOutputTracker` location queries
  - Maintain `ConcurrentHashMap[Int, OpenHashSet[Long]]` for shuffle-to-task-id tracking (same pattern as `SortShuffleManager`)
  - Implement `unregisterShuffle()` for cleanup of streaming buffers and spilled files
  - Implement fallback condition detection: consumer rate monitoring, memory pressure checks, network saturation checks
  - Provide `shuffleBlockResolver` returning `StreamingShuffleBlockResolver`
  - Implement `stop()` for orderly shutdown of streaming resources

- **CREATE: `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriter.scala`**
  - Extend `ShuffleWriter[K, V]` abstract class
  - Implement `write(records: Iterator[Product2[K, V]])`: partition records by key, buffer in per-partition memory regions limited to `(executorMemory * bufferPercent) / numPartitions`
  - Pipeline buffered data to consumer executors via `TransportClient.sendRpc()` or stream upload using existing Netty channel infrastructure
  - Monitor consumer acknowledgment rate; signal `BackpressureProtocol` when rate drops below threshold
  - Delegate to `MemorySpillManager` when buffer utilization exceeds configured threshold
  - Generate CRC32C checksums per 2MB block for integrity validation
  - Implement `stop(success: Boolean)`: return `Option[MapStatus]` with partition sizes; clean up resources on failure
  - Implement `getPartitionLengths()`: return cumulative partition byte counts

- **CREATE: `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReader.scala`**
  - Implement `ShuffleReader[K, C]` trait
  - Implement `read()`: return `Iterator[Product2[K, C]]` by polling producers for available blocks before shuffle completion
  - Manage in-progress block requests using `TransportClient` connections to producer executors
  - Detect producer failure via 5-second connection timeout on Netty channel
  - On failure: atomically discard all partial reads from failed producer, throw `FetchFailedException`
  - Send acknowledgment positions to producer for buffer reclamation via `BackpressureProtocol`
  - Validate CRC32C checksums on received blocks; request retransmission on mismatch
  - Support aggregation and sorting as `BlockStoreShuffleReader` does (leverage `ExternalSorter` for key ordering)

- **CREATE: `core/src/main/scala/org/apache/spark/shuffle/streaming/BackpressureProtocol.scala`**
  - Heartbeat-based flow control: consumer sends position updates every 5 seconds to producer
  - Token bucket rate limiter: per-executor bandwidth cap at `maxBandwidthMBps / numConcurrentShuffles`
  - Buffer utilization monitoring: track aggregate memory usage across all active streaming shuffles on executor
  - Priority arbitration: rank concurrent shuffles by `partitionCount * dataVolumeEstimate` for memory allocation
  - Telemetry emission: emit `backpressureEvent`, `rateLimitHit`, `priorityRebalance` log entries at INFO level
  - 10-second consumer liveness heartbeat detection for failure signaling

- **CREATE: `core/src/main/scala/org/apache/spark/shuffle/streaming/MemorySpillManager.scala`**
  - 100ms polling loop monitoring execution memory pool utilization via `MemoryManager`
  - Spill trigger: when buffer occupancy >= configured threshold (default 80%), select largest buffered partition via LRU policy
  - Disk persistence: write selected partition buffers to local disk via `BlockManager.diskBlockManager` temporary file allocation
  - Buffer reclamation: release memory within 100ms of consumer acknowledgment signal
  - Spill coordination: ensure atomic spill — partition data is fully written before memory is released
  - Metrics: track spill frequency, volume (bytes), and latency per spill event

#### Group 2 — Supporting Infrastructure (New)

- **CREATE: `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleHandle.scala`**
  - Extend `ShuffleHandle(shuffleId)` abstract class
  - Carry `ShuffleDependency[K, V, C]` reference (same pattern as `BaseShuffleHandle`)
  - Add streaming-specific metadata: buffer configuration, expected partition count

- **CREATE: `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleBlockResolver.scala`**
  - Implement `ShuffleBlockResolver` trait
  - Resolve streaming shuffle blocks from in-memory buffers or spilled disk files
  - Implement `getBlockData(blockId, dirs)` for both active streams and spilled data
  - Implement `getMergedBlockData()` and `getMergedBlockMeta()` (return empty for streaming — no merge support)
  - Implement `stop()` for cleanup of resolver state

- **CREATE: `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleMetricsSource.scala`**
  - Implement `org.apache.spark.metrics.source.Source` trait
  - Register Dropwizard `Gauge[Long]` metrics for:
    - `shuffle.streaming.bufferUtilizationPercent` — real-time buffer occupancy
    - `shuffle.streaming.spillCount` — cumulative disk spill events
    - `shuffle.streaming.backpressureEvents` — consumer rate limiting incidents
    - `shuffle.streaming.partialReadInvalidations` — producer failure detection count
  - Register with `MetricsSystem` following `ExecutorMetricsSource` pattern

- **CREATE: `core/src/main/scala/org/apache/spark/shuffle/streaming/StreamingShuffleConfig.scala`**
  - Define configuration key constants: `STREAMING_ENABLED`, `BUFFER_SIZE_PERCENT`, `SPILL_THRESHOLD`, `MAX_BANDWIDTH_MBPS`, `DEBUG_ENABLED`
  - Centralize validation logic for range checks and defaults
  - Provide helper methods: `isStreamingEnabled(conf: SparkConf)`, `getBufferSizePercent(conf: SparkConf)`

#### Group 3 — Existing File Modifications

- **MODIFY: `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala`** (line 112-114)
  - Add `"streaming" -> classOf[StreamingShuffleManager].getName` to `shortShuffleMgrNames` map

- **MODIFY: `core/src/main/scala/org/apache/spark/internal/config/package.scala`** (after line ~1734)
  - Add `ConfigBuilder` entries for all `spark.shuffle.streaming.*` parameters with proper versioning, documentation strings, type constraints, and defaults

- **MODIFY: `core/src/main/scala/org/apache/spark/shuffle/metrics.scala`**
  - Add streaming metric methods to `ShuffleWriteMetricsReporter` and `ShuffleReadMetricsReporter` traits with default no-op implementations to maintain backward compatibility

- **MODIFY: `core/src/main/scala/org/apache/spark/executor/ShuffleReadMetrics.scala`**
  - Add `LongAccumulator` fields for streaming read metrics
  - Update `TempShuffleReadMetrics` with matching fields
  - Update `setMergeValues()` to incorporate streaming metrics

- **MODIFY: `core/src/main/scala/org/apache/spark/executor/ShuffleWriteMetrics.scala`**
  - Add `LongAccumulator` fields for streaming write metrics (buffer bytes, spill count, backpressure events)

- **MODIFY: `core/src/main/scala/org/apache/spark/executor/Executor.scala`** (around line 163-166)
  - Register `StreamingShuffleMetricsSource` with the metrics system when streaming shuffle is enabled

#### Group 4 — Test Suites (New)

- **CREATE: `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleManagerSuite.scala`**
  - Test manager instantiation via `ShuffleManager.create()` with `spark.shuffle.manager=streaming`
  - Test `registerShuffle()` returns `StreamingShuffleHandle`
  - Test `getWriter()` and `getReader()` factory methods
  - Test `unregisterShuffle()` cleanup
  - Test fallback condition detection

- **CREATE: `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleWriterSuite.scala`**
  - Test buffer allocation respects per-partition memory limits
  - Test spill trigger at configurable threshold with timing validation (<100ms)
  - Test CRC32C checksum generation per block
  - Test resource cleanup on producer failure
  - Test `MapStatus` generation with correct partition sizes

- **CREATE: `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleReaderSuite.scala`**
  - Test in-progress block request and partial data consumption
  - Test producer failure detection via 5-second connection timeout
  - Test partial read invalidation and `FetchFailedException` propagation
  - Test CRC32C checksum validation and retransmission request
  - Test aggregation and sorting integration with `ExternalSorter`

- **CREATE: `core/src/test/scala/org/apache/spark/shuffle/streaming/BackpressureProtocolSuite.scala`**
  - Test consumer acknowledgment processing and buffer reclamation signaling
  - Test token bucket rate limiting enforcement
  - Test 10-second timeout detection and failure signaling
  - Test priority arbitration under concurrent shuffle load

- **CREATE: `core/src/test/scala/org/apache/spark/shuffle/streaming/MemorySpillManagerSuite.scala`**
  - Test 80% threshold monitoring with 100ms polling interval
  - Test LRU eviction selection of largest buffered partition
  - Test buffer reclamation within 100ms of acknowledgment
  - Test spill metrics recording accuracy

- **CREATE: `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShuffleIntegrationTest.scala`**
  - Test complete 10GB shuffle with 100 partitions — verify 30% latency reduction
  - Test producer failure injection mid-shuffle — validate partial read invalidation
  - Test consumer slowdown (50% rate) — validate automatic spill trigger
  - Test network partition — validate timeout and fallback behavior
  - Test 5 concurrent shuffles — validate buffer allocation arbitration

- **CREATE: `core/src/test/scala/org/apache/spark/shuffle/streaming/StreamingShufflePerformanceBenchmark.scala`**
  - Baseline: sort-based shuffle for `groupByKey` on 10GB dataset, 100 partitions
  - Streaming: same workload with streaming shuffle enabled
  - Metrics captured: end-to-end latency, memory utilization, spill frequency, network bandwidth

#### Group 5 — Documentation (New)

- **CREATE: `docs/streaming-shuffle-guide.md`**
  - Configuration reference for all `spark.shuffle.streaming.*` parameters
  - Architecture design document with streaming protocol specification
  - Failure handling flow diagrams
  - Performance tuning guide (buffer sizing recommendations, spill threshold optimization)

- **CREATE: `docs/streaming-shuffle-troubleshooting.md`**
  - Common issues and resolution steps
  - Telemetry interpretation guide
  - Debugging procedures with `spark.shuffle.streaming.debug=true`
  - Known limitations and workarounds

### 0.5.2 Implementation Approach per File

The implementation follows a bottom-up approach establishing infrastructure before feature logic:

- **Establish feature foundation** by creating `StreamingShuffleConfig.scala` (configuration constants) and `StreamingShuffleHandle.scala` (shuffle handle type) first — these are leaf dependencies with no upstream requirements
- **Build memory management** with `MemorySpillManager.scala` integrating against `MemoryManager` and `BlockManager` — this component is independently testable
- **Implement backpressure** with `BackpressureProtocol.scala` using `TransportClient` for signaling — independently testable with mock transport
- **Create the writer** with `StreamingShuffleWriter.scala` composing `MemorySpillManager` and `BackpressureProtocol` — depends on both infrastructure components
- **Create the reader** with `StreamingShuffleReader.scala` consuming from `TransportClient` with checksum validation — independently testable with mock producers
- **Wire the manager** with `StreamingShuffleManager.scala` orchestrating writer/reader creation, fallback logic, and metrics — final composition layer
- **Modify integration points** in `ShuffleManager.scala`, `config/package.scala`, `metrics.scala`, and `Executor.scala` — minimal surgical changes
- **Create metrics source** with `StreamingShuffleMetricsSource.scala` following `ExecutorMetricsSource` pattern
- **Build block resolver** with `StreamingShuffleBlockResolver.scala` for spilled block retrieval
- **Write tests** for each component, culminating in integration and performance tests
- **Author documentation** covering configuration, architecture, tuning, and troubleshooting

## 0.6 Scope Boundaries

### 0.6.1 Exhaustively In Scope

**All new feature source files:**
- `core/src/main/scala/org/apache/spark/shuffle/streaming/**/*.scala` — entire streaming shuffle package including `StreamingShuffleManager`, `StreamingShuffleWriter`, `StreamingShuffleReader`, `BackpressureProtocol`, `MemorySpillManager`, `StreamingShuffleHandle`, `StreamingShuffleBlockResolver`, `StreamingShuffleMetricsSource`, `StreamingShuffleConfig`

**All feature tests:**
- `core/src/test/scala/org/apache/spark/shuffle/streaming/**/*.scala` — all unit, integration, and performance test suites

**Integration points (surgical modifications to existing files):**
- `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` — add `"streaming"` entry to `shortShuffleMgrNames` map in companion object (lines 112-114)
- `core/src/main/scala/org/apache/spark/internal/config/package.scala` — add `ConfigBuilder` entries for `spark.shuffle.streaming.*` parameters (after line ~1734)
- `core/src/main/scala/org/apache/spark/shuffle/metrics.scala` — extend `ShuffleWriteMetricsReporter` and `ShuffleReadMetricsReporter` traits with streaming metric methods
- `core/src/main/scala/org/apache/spark/executor/ShuffleReadMetrics.scala` — add streaming metric accumulators and merge logic
- `core/src/main/scala/org/apache/spark/executor/ShuffleWriteMetrics.scala` — add streaming metric accumulators
- `core/src/main/scala/org/apache/spark/executor/Executor.scala` — register `StreamingShuffleMetricsSource` (around line 163-166)
- `core/src/main/scala/org/apache/spark/shuffle/ShuffleWriteProcessor.scala` — ensure streaming handle compatibility with write lifecycle (lines 73-86)

**Configuration files:**
- `core/src/main/scala/org/apache/spark/internal/config/package.scala` — new `spark.shuffle.streaming.enabled`, `spark.shuffle.streaming.bufferSizePercent`, `spark.shuffle.streaming.spillThreshold`, `spark.shuffle.streaming.maxBandwidthMBps`, `spark.shuffle.streaming.debug`

**Documentation:**
- `docs/streaming-shuffle-guide.md` — configuration reference, architecture document, performance tuning guide
- `docs/streaming-shuffle-troubleshooting.md` — common issues, telemetry interpretation, debugging procedures

**Transitive read-only dependencies (leveraged but not modified):**
- `common/network-common/src/main/java/org/apache/spark/network/**/*.java` — TransportContext, TransportClient, TransportServer, TransportClientFactory, protocol messages
- `core/src/main/scala/org/apache/spark/memory/**/*.scala` — MemoryManager, ExecutionMemoryPool, UnifiedMemoryManager
- `core/src/main/scala/org/apache/spark/storage/BlockManager.scala` — disk store for spill persistence
- `core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala` — reference pattern for block fetching
- `core/src/main/scala/org/apache/spark/MapOutputTracker.scala` — shuffle location tracking interface
- `core/src/main/scala/org/apache/spark/shuffle/FetchFailedException.scala` — failure propagation to DAGScheduler
- `core/src/main/scala/org/apache/spark/shuffle/ShuffleChecksumUtils.scala` — checksum utility reference
- `core/src/main/scala/org/apache/spark/network/netty/NettyBlockTransferService.scala` — network transfer pattern reference
- `core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala` — reference implementation for ShuffleManager trait
- `core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleWriter.scala` — reference implementation for ShuffleWriter class
- `core/src/main/scala/org/apache/spark/shuffle/BlockStoreShuffleReader.scala` — reference implementation for ShuffleReader trait

### 0.6.2 Explicitly Out of Scope

**User-Facing APIs — ZERO MODIFICATIONS:**
- `core/src/main/scala/org/apache/spark/rdd/**/*.scala` — RDD API remains unchanged
- `sql/core/src/main/scala/org/apache/spark/sql/**/*.scala` — DataFrame/Dataset APIs untouched
- `python/pyspark/**/*.py` — PySpark interface not affected
- `R/pkg/**/*.R` — SparkR interface not affected

**Scheduler and Task Lifecycle — ZERO MODIFICATIONS:**
- `core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala` — DAG scheduling algorithms unchanged
- `core/src/main/scala/org/apache/spark/scheduler/TaskScheduler*.scala` — task scheduling unchanged
- `core/src/main/scala/org/apache/spark/executor/CoarseGrainedExecutorBackend.scala` — executor lifecycle unchanged

**Existing Shuffle Implementation — ZERO MODIFICATIONS:**
- `core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala` — coexists as fallback, not modified
- `core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleWriter.scala` — not modified
- `core/src/main/java/org/apache/spark/shuffle/sort/UnsafeShuffleWriter.java` — not modified
- `core/src/main/java/org/apache/spark/shuffle/sort/BypassMergeSortShuffleWriter.java` — not modified
- `core/src/main/java/org/apache/spark/shuffle/sort/ShuffleExternalSorter.java` — not modified
- `core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala` — not modified
- `core/src/main/java/org/apache/spark/shuffle/sort/io/**/*.java` — LocalDisk shuffle I/O not modified

**Lineage and Fault Recovery — ZERO MODIFICATIONS:**
- RDD lineage tracking mechanisms unchanged
- Checkpointing system unchanged
- Task speculation unchanged

**Block Manager Interface Contracts — ZERO MODIFICATIONS:**
- `core/src/main/scala/org/apache/spark/storage/BlockManager.scala` — storage interface preserved (streaming uses existing APIs only)
- `core/src/main/scala/org/apache/spark/network/BlockDataManager.scala` — data manager interface preserved

**Task Serialization — ZERO MODIFICATIONS:**
- Task serialization and deserialization protocols unchanged
- Closure serialization unchanged

**Deployment Infrastructure — ZERO MODIFICATIONS:**
- `resource-managers/**/*` — YARN, Kubernetes, Standalone cluster managers unchanged
- `.github/workflows/**/*` — CI/CD pipelines unchanged
- `Dockerfile*`, `docker-compose*` — container configurations unchanged
- `assembly/pom.xml` — packaging unchanged

**Other Modules Not Affected:**
- `sql/**/*` — Spark SQL, Catalyst optimizer, Hive integration
- `streaming/**/*` — DStream legacy streaming
- `mllib/**/*` — Machine learning library
- `graphx/**/*` — Graph processing
- `connector/**/*` — Data source connectors
- `python/**/*` — PySpark
- `R/**/*` — SparkR

**Explicitly Out of Scope Feature Work:**
- DAG optimization heuristics — no changes to query planning or stage optimization
- Query planning modifications — Catalyst optimizer untouched
- Executor memory model redesign — existing `UnifiedMemoryManager` model preserved
- External system integrations — no new external dependencies or services
- Dynamic reconfiguration — configuration changes require executor restart in v1
- Performance optimizations beyond streaming shuffle — no speculative execution changes, no broadcast optimization
- Refactoring of existing shuffle code — `SortShuffleManager` and related classes remain untouched

## 0.7 Rules for Feature Addition

### 0.7.1 Implementation Discipline Rules

The user has explicitly emphasized the following implementation discipline constraints that must be honored throughout all code artifacts:

- **Minimal modification principle**: Make only changes necessary to implement streaming shuffle capability within the `ShuffleManager` abstraction boundary. Every modification to existing files must be justified as essential for streaming shuffle integration.

- **Fallback preservation**: Preserve the existing sort-based shuffle as the production-stable fallback. The `SortShuffleManager` must remain fully operational with zero behavioral changes when `spark.shuffle.streaming.enabled=false` (the default).

- **DAG scheduler isolation**: Never modify the DAG scheduler, task lifecycle, or user-facing APIs. The streaming shuffle must be completely transparent to the computation graph and application code layer.

- **Least-impact integration**: When implementation choices exist, select the approach requiring the least modification to the executor memory model and network transport layer. Prefer composition over modification — streaming classes should consume existing infrastructure APIs rather than extending or altering them.

- **Zero cross-contamination**: Isolate all streaming logic in dedicated classes within the `org.apache.spark.shuffle.streaming` package. No streaming-specific logic should leak into existing shuffle code paths in `org.apache.spark.shuffle.sort` or `org.apache.spark.shuffle`.

- **Coexistence documentation**: Document all integration points with clear comments explaining the coexistence strategy between streaming and sort-based shuffle.

### 0.7.2 Performance and Reliability Rules

- **Target performance envelope**: 30-50% latency reduction for shuffle-heavy workloads (10GB+ data, 100+ partitions), 5-10% improvement for CPU-bound workloads, zero regression for memory-bound workloads
- **Memory safety**: Streaming buffers must never exceed the configured percentage of executor memory (default 20%, max 50%). Buffer allocation must be tracked via the existing `MemoryManager` interface.
- **Spill response time**: Memory exhaustion prevention via 80% threshold spill trigger must respond within <100ms
- **Zero data loss**: Under all failure scenarios including producer crashes, consumer failures, network partitions — validated by 10 failure injection test scenarios
- **Memory leak prevention**: Zero retained heap after 2-hour continuous stress test with 1000 concurrent tasks and 500 concurrent shuffles

### 0.7.3 Configuration and Operational Rules

- **Opt-in only**: The feature is disabled by default (`spark.shuffle.streaming.enabled=false`). Users must explicitly enable it.
- **No dynamic reconfiguration**: All configuration changes require executor restart in v1
- **Telemetry overhead**: <1% CPU utilization for streaming shuffle metric collection
- **Log volume**: <10MB/hour per executor for streaming events. Debug logging disabled by default, enabled only via `spark.shuffle.streaming.debug=true`
- **JMX exposure**: All streaming shuffle metrics must be accessible via JMX for external monitoring integration (Prometheus, Grafana)

### 0.7.4 Testing and Quality Gate Rules

- **Unit test coverage**: >85% line coverage for all new components
- **Integration test stability**: All integration tests must pass with zero flakiness over 10 consecutive runs
- **Performance validation**: Benchmark must demonstrate >30% improvement for the target workload (10GB groupByKey, 100 partitions)
- **Failure validation**: All 10 failure injection scenarios (producer crash, consumer crash, network partition, memory exhaustion, disk failure, checksum mismatch, connection timeout, GC pause, concurrent producer failures, consumer reconnect) must validate zero data loss
- **Stress test validation**: 2-hour continuous run with random 1% task failure injection rate must show <5% throughput degradation and zero memory leaks (validated via heap dump analysis)

### 0.7.5 Rollback Criteria

Immediate disable of streaming shuffle if any of the following are observed in production:

- Data loss incident reported and confirmed
- Memory exhaustion causing executor crashes (>5% increase over baseline)
- Performance regression >10% for any workload type
- Checksum failure rate >0.1% indicating data corruption
- Sustained backpressure events exceeding operational thresholds

## 0.8 References

### 0.8.1 Codebase Files and Folders Searched

The following files and folders were comprehensively analyzed to derive the conclusions, integration points, and file mappings in this Agent Action Plan:

**Root-Level Configuration:**
- `pom.xml` — Parent Maven POM: Java 17, Scala 2.13.17, Netty 4.2.7.Final, Dropwizard Metrics 4.2.33, Kryo 4.0.3, Jackson 2.20.0, Hadoop 3.4.2, project version 4.1.0-SNAPSHOT
- `scalastyle-config.xml` — Scala coding standards

**Core Shuffle Subsystem (primary analysis target):**
- `core/src/main/scala/org/apache/spark/shuffle/ShuffleManager.scala` — ShuffleManager trait and factory companion object (119 lines)
- `core/src/main/scala/org/apache/spark/shuffle/ShuffleWriter.scala` — ShuffleWriter abstract class (37 lines)
- `core/src/main/scala/org/apache/spark/shuffle/ShuffleReader.scala` — ShuffleReader trait (33 lines)
- `core/src/main/scala/org/apache/spark/shuffle/ShuffleHandle.scala` — ShuffleHandle base class (28 lines)
- `core/src/main/scala/org/apache/spark/shuffle/BaseShuffleHandle.scala` — BaseShuffleHandle implementation (28 lines)
- `core/src/main/scala/org/apache/spark/shuffle/ShuffleBlockResolver.scala` — ShuffleBlockResolver trait (67 lines)
- `core/src/main/scala/org/apache/spark/shuffle/ShuffleWriteProcessor.scala` — Write lifecycle processor (102 lines)
- `core/src/main/scala/org/apache/spark/shuffle/ShuffleDataIOUtils.scala` — Shuffle data I/O plugin loading (42 lines)
- `core/src/main/scala/org/apache/spark/shuffle/ShuffleChecksumUtils.scala` — Checksum utilities
- `core/src/main/scala/org/apache/spark/shuffle/metrics.scala` — Metrics reporter interfaces (62 lines)
- `core/src/main/scala/org/apache/spark/shuffle/FetchFailedException.scala` — Failure propagation
- `core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala` — Sort-based block resolver
- `core/src/main/scala/org/apache/spark/shuffle/ShuffleBlockPusher.scala` — Push-based shuffle support

**Sort-Based Shuffle Implementation (reference patterns):**
- `core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala` — Existing manager implementation (278 lines)
- `core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleWriter.scala` — Existing writer implementation (127 lines)
- `core/src/main/scala/org/apache/spark/shuffle/BlockStoreShuffleReader.scala` — Existing reader implementation (160 lines)

**Java Shuffle API Layer:**
- `core/src/main/java/org/apache/spark/shuffle/api/ShuffleDataIO.java`
- `core/src/main/java/org/apache/spark/shuffle/api/ShuffleExecutorComponents.java`
- `core/src/main/java/org/apache/spark/shuffle/api/ShuffleMapOutputWriter.java`
- `core/src/main/java/org/apache/spark/shuffle/sort/UnsafeShuffleWriter.java`
- `core/src/main/java/org/apache/spark/shuffle/sort/BypassMergeSortShuffleWriter.java`
- `core/src/main/java/org/apache/spark/shuffle/sort/io/LocalDiskShuffleDataIO.java`
- `core/src/main/java/org/apache/spark/shuffle/checksum/ShuffleChecksumSupport.java`

**SparkEnv and SparkContext Integration:**
- `core/src/main/scala/org/apache/spark/SparkEnv.scala` — ShuffleManager initialization (line 223-227)
- `core/src/main/scala/org/apache/spark/SparkContext.scala` — initializeShuffleManager call (line 589)

**Configuration Infrastructure:**
- `core/src/main/scala/org/apache/spark/internal/config/package.scala` — SHUFFLE_MANAGER config (lines 1730-1734), SHUFFLE_CHECKSUM_ENABLED (line 1651), SHUFFLE_CHECKSUM_ALGORITHM (line 1662-1668)

**Memory Management Subsystem:**
- `core/src/main/scala/org/apache/spark/memory/MemoryManager.scala` — Abstract memory manager (execution and storage pools)
- `core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala` — Concrete memory manager
- `core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala` — Execution memory tracking

**Executor Metrics Infrastructure:**
- `core/src/main/scala/org/apache/spark/executor/Executor.scala` — Metrics source registration (lines 163-166)
- `core/src/main/scala/org/apache/spark/executor/ExecutorMetricsSource.scala` — Metrics source pattern (64 lines)
- `core/src/main/scala/org/apache/spark/executor/ShuffleReadMetrics.scala` — Read metric accumulators (300 lines)
- `core/src/main/scala/org/apache/spark/executor/ShuffleWriteMetrics.scala` — Write metric accumulators

**Network Transport Layer:**
- `common/network-common/src/main/java/org/apache/spark/network/TransportContext.java` — Transport context factory
- `common/network-common/src/main/java/org/apache/spark/network/client/TransportClient.java` — Client for RPC and stream requests
- `common/network-common/src/main/java/org/apache/spark/network/client/TransportClientFactory.java` — Connection pooling
- `common/network-common/src/main/java/org/apache/spark/network/server/TransportServer.java` — Server for incoming connections
- `common/network-common/src/main/java/org/apache/spark/network/protocol/StreamRequest.java` — Stream protocol messages
- `common/network-common/src/main/java/org/apache/spark/network/protocol/StreamResponse.java` — Stream response framing
- `core/src/main/scala/org/apache/spark/network/netty/NettyBlockTransferService.scala` — Spark-specific block transfer
- `core/src/main/scala/org/apache/spark/network/BlockTransferService.scala` — Transfer service abstraction

**Storage Subsystem:**
- `core/src/main/scala/org/apache/spark/storage/BlockManager.scala` — Block storage management
- `core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala` — Shuffle block fetching

**MapOutput Tracking:**
- `core/src/main/scala/org/apache/spark/MapOutputTracker.scala` — Master/Worker shuffle location tracking

**Existing Test Suites (for pattern reference):**
- `core/src/test/scala/org/apache/spark/shuffle/sort/SortShuffleManagerSuite.scala`
- `core/src/test/scala/org/apache/spark/shuffle/sort/SortShuffleWriterSuite.scala`
- `core/src/test/scala/org/apache/spark/shuffle/sort/BypassMergeSortShuffleWriterSuite.scala`
- `core/src/test/scala/org/apache/spark/shuffle/sort/IndexShuffleBlockResolverSuite.scala`
- `core/src/test/scala/org/apache/spark/shuffle/BlockStoreShuffleReaderSuite.scala`
- `core/src/test/scala/org/apache/spark/shuffle/ShuffleBlockPusherSuite.scala`

**Folder-Level Analysis:**
- Root (`/`) — Complete repository structure with all 30+ top-level children
- `core/` — Module POM, src, and benchmarks
- `common/` — All 10 submodules including network-common, network-shuffle, unsafe
- `common/network-common/` — 102 Java files across client, server, protocol, crypto, and util packages

### 0.8.2 Tech Spec Sections Referenced

- **Section 1.1 Executive Summary** — Project overview (Spark 4.1.0-SNAPSHOT), core business problem, stakeholder analysis, ecosystem integration details (Hadoop 3.4.2, Kafka 3.9.1, Parquet 1.16.0, Arrow 18.3.0)
- **Section 2.1 Feature Catalog** — F-001 (RDD API), F-009 (Caching/Persistence), F-010 (Fault Tolerance) — context for shuffle integration with BlockManager, DAGScheduler, and lineage-based recovery
- **Section 3.1 Programming Languages** — Scala 2.13.17 as primary implementation language, Java 17+ minimum runtime, toolchain constraints
- **Section 5.1 High-Level Architecture** — Master-worker architecture, shuffle data flow description (map phase write, reduce phase read), pluggable component architecture, memory-disk boundaries, BlockManager integration patterns

### 0.8.3 Attachments and External Metadata

- **User-provided attachments**: None (0 attachments provided)
- **Figma URLs**: None specified
- **Environment files**: None in `/tmp/environments_files/`
- **Setup instructions**: None provided by user
- **Environment variables**: None specified
- **Secrets**: None specified

