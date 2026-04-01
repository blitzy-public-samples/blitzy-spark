# Detect Backpressure Conditions to Enable Proactive Stream Processing Bottleneck Identification

## User Story

**As a** DevOps engineer monitoring production Structured Streaming pipelines,
**I want to** automatically detect backpressure conditions in Structured Streaming queries when the processing time exceeds a configurable percentage (default 90%) of the trigger interval for 3 or more consecutive micro-batches, or when the input row rate (`inputRowsPerSecond` from `StreamingQueryProgress`) exceeds the processing row rate (`processedRowsPerSecond`) by a configurable threshold (default 20%),
**So that** pipeline degradation is identified within 3 trigger intervals instead of being discovered hours later through downstream data staleness, reducing mean-time-to-detection from an average of 45 minutes to under 2 minutes and preventing silent data delivery delays that affect downstream consumers.

**Secondary Persona:** As a streaming application developer, I need programmatic access to backpressure events through the `StreamingQueryListener` bus so that I can build automated remediation workflows triggered by backpressure conditions.

## Acceptance Criteria

### AC-1: Input Validation — Threshold Configuration Acceptance

- **Given** a streaming query with `spark.streaming.backpressure.triggerThreshold` set to a value between 0.1 and 1.0
- **When** the configuration is loaded at query startup
- **Then** the system accepts the threshold and applies it for backpressure evaluation on each micro-batch

### AC-2: Expected Output — Processing Time Backpressure Detection

- **Given** a running Structured Streaming query with a trigger interval of 10 seconds and a trigger threshold of 0.9
- **When** the `triggerExecution` duration reported in `StreamingQueryProgress` exceeds 9000 milliseconds for 3 consecutive micro-batches
- **Then** a backpressure event is emitted containing the query ID, query name, trigger interval, observed processing times for the 3 batches, and the threshold value

### AC-3: Expected Output — Rate Imbalance Backpressure Detection

- **Given** a running Structured Streaming query with `spark.streaming.backpressure.rateImbalanceThreshold` set to 0.2
- **When** `inputRowsPerSecond` exceeds `processedRowsPerSecond` by more than 20% for 3 consecutive micro-batches
- **Then** a rate-imbalance backpressure event is emitted containing the query ID, input rates, processing rates, and the imbalance ratio for each of the 3 batches

### AC-4: Error Handling — Null or Zero Progress Data

- **Given** a Structured Streaming query where `StreamingQueryProgress` returns null or contains zero values for `inputRowsPerSecond` and `processedRowsPerSecond`
- **When** the backpressure detector evaluates the batch
- **Then** the detector skips the evaluation for that batch without throwing an exception and logs a warning message with the query ID and batch ID

### AC-5: Edge Case — Continuous Processing Mode

- **Given** a Structured Streaming query running in continuous processing mode (`Trigger.Continuous`) where `triggerExecution` duration is not applicable
- **When** the backpressure detector is invoked
- **Then** the detector uses only the rate-imbalance method (`inputRowsPerSecond` vs `processedRowsPerSecond`) and ignores the trigger-time-based detection

### AC-6: Kafka-Specific Backpressure — Consumer Lag Detection

- **Given** a Structured Streaming query reading from a Kafka source via the kafka-0-10 connector
- **When** the consumer lag (difference between latest offset and committed offset) exceeds a configurable threshold of 10000 offsets per partition
- **Then** a source-specific backpressure event is emitted containing the topic, partition, current lag, and threshold value

### AC-7: Recovery — Backpressure Resolved Event

- **Given** a backpressure condition that was previously detected
- **When** the processing time drops below the trigger threshold for 3 consecutive micro-batches AND the rate imbalance drops below the configured threshold
- **Then** a backpressure-resolved event is emitted containing the query ID, recovery timestamp, and the number of batches that were in backpressure state

## Sub-Tasks

- **ST-1:** Implement `BackpressureDetector` class in `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/` that consumes `StreamingQueryProgress` events and evaluates backpressure conditions per micro-batch
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/ProgressReporter.scala` — per-batch progress reporting with `triggerExecution` duration, `inputRowsPerSecond`, `processedRowsPerSecond`
- **ST-2:** Add configurable thresholds via `SparkConf` properties:
  - `spark.streaming.backpressure.triggerThreshold` (default: 0.9, valid range: [0.1, 1.0])
  - `spark.streaming.backpressure.rateImbalanceThreshold` (default: 0.2, valid range: [0.0, 1.0])
  - `spark.streaming.backpressure.consecutiveBatches` (default: 3, valid range: [1, 100])
  - `spark.streaming.backpressure.kafka.lagThreshold` (default: 10000, valid range: [1, MAX_LONG])
- **ST-3:** Integrate with `ProgressReporter` to receive per-batch progress updates and feed them into the `BackpressureDetector` evaluation pipeline
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/ProgressReporter.scala` — `updateProgress()` method and `progressBuffer` queue
- **ST-4:** Create `BackpressureEvent` case class hierarchy:
  - `BackpressureDetected` — emitted when trigger-time or rate-imbalance threshold is breached for the configured consecutive batch count
  - `BackpressureResolved` — emitted when conditions return to normal for the configured consecutive batch count
  - `RateImbalanceDetected` — emitted specifically for input-rate-to-processing-rate imbalance scenarios
  - `KafkaLagBackpressureDetected` — emitted for Kafka consumer lag threshold breaches
- **ST-5:** Integrate with `MetricsReporter` to expose backpressure state as a Dropwizard gauge metric (`backpressure-detected`, `backpressure-duration-ms`, `rate-imbalance-ratio`)
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetricsReporter.scala` — existing Dropwizard gauge metrics: `inputRate-total`, `processingRate-total`, `latency` (triggerExecution duration)
- **ST-6:** Add Kafka-specific backpressure detection using consumer lag from `connector/kafka-0-10/` offset tracking, comparing latest available offset against committed offset per partition
  - `Source: connector/kafka-0-10/` — Kafka source offset tracking for consumer lag
- **ST-7:** Register backpressure events with the `StreamingQueryListener` bus for downstream consumption by application developers and monitoring integrations
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala` — `StreamExecution.lastProgress` accessor and query listener integration
- **ST-8:** Write unit tests for threshold boundary conditions, consecutive batch counting logic, event emission correctness, and null/zero input handling
- **ST-9:** Write integration tests for Kafka source backpressure detection with a test Kafka cluster, covering lag threshold breaches and recovery scenarios

## Edge Cases

- **Empty/Null Input:** `StreamingQueryProgress` is null or contains null fields for `inputRowsPerSecond`, `processedRowsPerSecond`, or `durationMs` — the detector must skip evaluation for that batch and log a warning message containing the query ID and batch ID without throwing exceptions or corrupting the consecutive batch counter state
- **Boundary Values:** Processing time is at the exact threshold boundary (e.g., `triggerExecution` = 9000ms with a threshold of 9000ms derived from a 10-second trigger interval and 0.9 trigger threshold) — the system must treat the boundary as NOT in backpressure, using a strictly-greater-than comparison to avoid false-positive detections at the threshold edge
- **Invalid Input:** `spark.streaming.backpressure.triggerThreshold` is set to a value outside the valid range (e.g., -0.5, 0.0, 1.5) — the system must reject the configuration at query startup with a clear error message specifying the valid range [0.1, 1.0] and prevent the streaming query from starting with an invalid configuration
- **Rapid Recovery and Re-detection:** Backpressure is detected, resolves for 2 batches (below the configured recovery threshold of 3 consecutive non-backpressure batches), then re-occurs — the consecutive batch counter for recovery must not reset until a full recovery of 3 consecutive non-backpressure batches is observed, preventing premature backpressure-resolved events during intermittent fluctuations
- **Zero Processing Rate:** `processedRowsPerSecond` is 0.0 while `inputRowsPerSecond` is positive — the detector must emit a backpressure event without division-by-zero errors, treating a zero processing rate as an infinite imbalance ratio and immediately counting the batch toward the consecutive backpressure threshold

## Dependencies

- **FEATURE-005-01: Stream Health Monitoring** — Provides the underlying processing rate and batch duration metrics that the backpressure detection logic consumes for threshold evaluation
  - Link: [FEATURE-005-01-stream-health-monitoring.md](../../FEATURE-005-01-stream-health-monitoring.md)
- **StreamingQueryProgress API** (`sql/core/`) — Provides `inputRowsPerSecond`, `processedRowsPerSecond`, and `durationMs.triggerExecution` fields used as primary input signals for backpressure detection
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/ProgressReporter.scala`
- **ProgressReporter** — Source of per-batch progress events that feed into the `BackpressureDetector` evaluation loop
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/ProgressReporter.scala`
- **MetricsReporter** — Integration point for exposing backpressure gauge metrics through the Dropwizard MetricRegistry to all configured sinks (Prometheus, JMX, Graphite, Slf4j, CSV)
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetricsReporter.scala`
- **Kafka Connector** (`connector/kafka-0-10/`) — Kafka consumer group offset lag metrics and per-partition offset tracking used for source-specific backpressure detection in Kafka-sourced streaming queries
- **SparkConf** — Configuration property framework for defining and validating backpressure threshold settings at query startup
- **Dropwizard Metrics Sinks** — PrometheusServlet, JmxSink, GraphiteSink, Slf4jSink used to export backpressure gauge metrics
  - `Source: docs/monitoring.md`

## Story Estimation Guidance

- **Story Points:** 8 (Fibonacci scale)
- **Justification:** This story involves implementing a new detection component with multiple detection strategies (trigger-time-based, rate-imbalance-based, and Kafka-specific lag-based), integration with two existing systems (`ProgressReporter` for input data and `MetricsReporter` for output metrics), handling of continuous processing mode as a distinct edge case, event state machine logic (detected → resolved transitions with consecutive batch counting), and comprehensive test coverage including Kafka integration tests with a test cluster.
- **Complexity Factors:**
  - Multiple backpressure detection algorithms with independent thresholds and evaluation logic
  - Event state machine managing detected-to-resolved transitions with hysteresis (consecutive batch counting prevents premature state transitions)
  - Kafka-specific integration requiring access to consumer group offset lag data from the kafka-0-10 connector
  - Boundary condition handling for threshold comparisons (strictly-greater-than semantics) and zero/null input protection
  - Concurrent access safety for the consecutive batch counter and backpressure state, given that progress updates may arrive from the streaming execution thread while metrics are read from monitoring threads
- **Sprint Fit:** Completable within a single 2-week sprint by one engineer with Structured Streaming internals experience

## Definition of Done

- `BackpressureDetector` class is implemented in `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/` and integrated with the streaming execution runtime
- All 7 acceptance criteria (AC-1 through AC-7) pass with automated tests
- Backpressure detection works for both micro-batch and continuous processing modes, with continuous mode using only rate-imbalance detection
- Kafka-specific offset lag detection is functional with the kafka-0-10 connector, emitting source-specific backpressure events when per-partition lag exceeds the configured threshold
- Backpressure events (`BackpressureDetected`, `BackpressureResolved`, `RateImbalanceDetected`, `KafkaLagBackpressureDetected`) are published to the `StreamingQueryListener` bus for downstream consumption
- Backpressure state is exposed as Dropwizard gauge metrics (`backpressure-detected`, `backpressure-duration-ms`, `rate-imbalance-ratio`) accessible via all configured metric sinks (Prometheus, JMX, Graphite, Slf4j, CSV)
- Configuration properties (`spark.streaming.backpressure.triggerThreshold`, `spark.streaming.backpressure.rateImbalanceThreshold`, `spark.streaming.backpressure.consecutiveBatches`, `spark.streaming.backpressure.kafka.lagThreshold`) are documented with valid ranges and default values
- Unit tests cover all 5 edge cases (null/zero input, boundary values, invalid configuration, rapid recovery/re-detection, zero processing rate) with explicit assertions
- Integration tests validate end-to-end backpressure detection and recovery with a running Structured Streaming query, including Kafka source lag simulation
- Code review is complete with no unresolved comments
