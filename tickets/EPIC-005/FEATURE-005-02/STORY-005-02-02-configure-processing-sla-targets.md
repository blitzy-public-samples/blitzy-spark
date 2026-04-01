# Configure Processing SLA Targets to Define Measurable Performance Expectations for Streaming Queries

## User Story

**As a** data platform administrator,
**I want to** configure per-streaming-query SLA targets consisting of a maximum processing latency (in milliseconds) and a minimum throughput (in rows per second) via SparkConf properties (`spark.streaming.sla.<queryName>.maxLatencyMs` and `spark.streaming.sla.<queryName>.minThroughputRowsPerSec`), with support for global defaults via `spark.streaming.sla.default.maxLatencyMs` and `spark.streaming.sla.default.minThroughputRowsPerSec`,
**so that** data platform teams can define explicit, measurable performance contracts for each streaming pipeline, replacing ad-hoc latency monitoring with formal SLA targets that enable automated breach detection and reduce SLA violation response time by up to 70%.

### Source References

- `Source: docs/configuration.md` — Spark configuration property patterns, time/byte unit formats, precedence rules (SparkConf, spark-defaults.conf, --conf flags)
- `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala` — Streaming query execution lifecycle, SparkSession and SparkConf access during query startup
- `Source: sql/core/src/main/scala/org/apache/spark/sql/classic/StreamingQuery.scala` — StreamingQuery API providing access to query name and progress reporting
- `Source: sql/core/src/main/scala/org/apache/spark/sql/classic/StreamingQueryManager.scala` — StreamingQueryManager managing active queries and StreamingQueryProgress reporting
- `Source: docs/monitoring.md` — Monitoring and instrumentation guide covering metrics sinks and REST API endpoints

---

## Acceptance Criteria

### AC-1: Input Validation — Per-Query Configuration Loading

- **Given** a SparkConf with property `spark.streaming.sla.myQuery.maxLatencyMs` set to `5000`
- **When** a streaming query named "myQuery" starts
- **Then** the SLA manager loads the maximum latency target of 5000 milliseconds for that specific query and uses it for SLA evaluation on every completed micro-batch

### AC-2: Input Validation — Negative Value Rejection

- **Given** a SparkConf with `spark.streaming.sla.default.maxLatencyMs` set to a negative value (`-100`)
- **When** the streaming query attempts to start
- **Then** the system throws an `IllegalArgumentException` with a message containing the property name `spark.streaming.sla.default.maxLatencyMs` and specifying that the value must be a positive integer greater than zero

### AC-3: Expected Output — Latency SLA Violation Detection

- **Given** a streaming query "ordersStream" with `spark.streaming.sla.ordersStream.maxLatencyMs` set to `3000`
- **When** a micro-batch completes with triggerExecution duration of 3500 milliseconds (reported by `StreamingQueryProgress.durationMs`)
- **Then** the SLA evaluation marks this batch as a latency SLA violation and records the query name "ordersStream", the target of 3000ms, the observed value of 3500ms, and the batch ID

### AC-4: Expected Output — Throughput SLA Violation Detection

- **Given** a streaming query "eventsStream" with `spark.streaming.sla.eventsStream.minThroughputRowsPerSec` set to `10000`
- **When** a micro-batch completes with `processedRowsPerSecond` of 8500 as reported by `StreamingQueryProgress`
- **Then** the SLA evaluation marks this batch as a throughput SLA violation and records the query name "eventsStream", the target of 10000 rows/sec, the observed value of 8500 rows/sec, and the batch ID

### AC-5: Error Handling — Missing Configuration Graceful Degradation

- **Given** a streaming query "analyticsStream" with no per-query SLA properties and no default SLA properties configured in SparkConf
- **When** the query starts and completes micro-batches
- **Then** no SLA evaluation is performed for that query, no errors are thrown, and the SLA status is reported as "unconfigured" in the query status metadata

### AC-6: Edge Case — Per-Query Override Takes Precedence Over Global Default

- **Given** `spark.streaming.sla.default.maxLatencyMs` set to `5000` and `spark.streaming.sla.myQuery.maxLatencyMs` set to `2000`
- **When** a streaming query named "myQuery" starts
- **Then** the per-query value of 2000ms takes precedence over the default of 5000ms for SLA evaluation of the "myQuery" streaming query

### AC-7: Edge Case — Runtime Reconfiguration Without Query Restart

- **Given** a running streaming query with `spark.streaming.sla.myQuery.maxLatencyMs` set to `5000`
- **When** the property is updated via `sparkSession.conf.set("spark.streaming.sla.myQuery.maxLatencyMs", "3000")` while the query is running
- **Then** the SLA target is updated to 3000ms starting from the next micro-batch without requiring a query restart

---

## Sub-Tasks

- Define SLA configuration property schema: `spark.streaming.sla.<queryName>.maxLatencyMs` (Long, positive), `spark.streaming.sla.<queryName>.minThroughputRowsPerSec` (Long, positive), with `default` as a reserved query name for global defaults
- Implement `SlaConfigManager` class that reads and validates SLA properties from SparkConf, resolving per-query overrides against global defaults
- Add configuration validation in `SlaConfigManager`: reject negative values, zero values, and non-numeric values with descriptive error messages including the offending property name and value
- Integrate `SlaConfigManager` with `StreamExecution` to load SLA targets at query startup and support runtime reconfiguration via session-level conf changes
- Implement `SlaEvaluator` class that compares `StreamingQueryProgress` metrics (`triggerExecution` duration from `durationMs`, `processedRowsPerSecond`) against loaded SLA targets after each micro-batch
- Create `SlaViolation` data class containing query name, SLA type (latency or throughput), target value, observed value, batch ID, and timestamp
- Register SLA configuration properties in the Spark configuration documentation pattern following `docs/configuration.md` conventions (property name, default, description)
- Write unit tests for configuration parsing, validation, precedence resolution (per-query over default), and runtime reconfiguration
- Write unit tests for SLA evaluation logic with boundary conditions (observed value equal to target, observed value exceeding target, observed value below target)

---

## Edge Cases

- **Empty/Null Input**: No SLA properties are set in SparkConf — the system must operate in "unconfigured" mode with no SLA evaluation and no errors, reporting SLA status as "unconfigured" in query status metadata
- **Boundary Values**: Observed latency is exactly equal to the configured `maxLatencyMs` (e.g., both are 5000ms) — the system must treat this as NOT a violation (a violation requires the observed value to strictly exceed the target); similarly, observed throughput exactly equal to `minThroughputRowsPerSec` is NOT a violation
- **Invalid Input**: `spark.streaming.sla.default.maxLatencyMs` is set to a non-numeric string (e.g., "abc") — the system must throw a `NumberFormatException` at query startup with a message containing the property name and the invalid value
- **Partial Configuration**: Only `maxLatencyMs` is configured without `minThroughputRowsPerSec` — the system must evaluate only the latency SLA and skip throughput evaluation without error; the converse (only throughput configured) must also be supported
- **Query Name with Special Characters**: A streaming query name contains dots or spaces (e.g., "my.query" or "my query") — the system must use URL-encoding or replacement characters (e.g., underscores) in the property key lookup to avoid conflicts with the Spark property namespace delimiter

---

## Dependencies

- **SparkConf Configuration Framework** (`docs/configuration.md`) — Property loading, validation, and precedence rules; supports SparkConf programmatic API, `spark-defaults.conf` file, and `--conf` command-line flags
- **StreamingQueryProgress API** (`sql/core/`) — Provides `durationMs` map (including `triggerExecution` key) and `processedRowsPerSecond` for SLA comparison after each micro-batch
- **StreamExecution** (`sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala`) — Integration point for loading SLA configuration at query startup; provides access to `sparkSession.sessionState.conf` for runtime reconfiguration
- **STORY-005-02-01** (Detect Backpressure Conditions) — Backpressure detection provides complementary signals; SLA violations may coincide with backpressure conditions and both features share the streaming metrics pipeline
- **FEATURE-005-01** (Stream Health Monitoring) — Provides the underlying health metrics infrastructure (processing rates, watermark progression, state store size, processing lag, checkpoint status) that SLA evaluation depends on for metric availability

---

## Story Estimation Guidance

- **Story Points**: 5 (Fibonacci)
- **Justification**: Involves new configuration schema definition, configuration parsing and validation logic, SLA evaluation engine, and runtime reconfiguration support. Moderate complexity with well-defined boundaries — the primary work is configuration management and comparison logic without complex algorithms
- **Complexity Factors**:
  - Configuration precedence resolution (per-query values override global defaults)
  - Runtime reconfiguration without query restart (session-level conf change detection)
  - Special character handling in query names for property key lookup
  - Integration with existing SparkConf framework and StreamExecution lifecycle
  - Boundary condition handling (exact-match is not a violation)

---

## Definition of Done

- SLA configuration properties are loadable via SparkConf, `spark-defaults.conf`, and `--conf` flags
- Per-query and global default SLA targets are supported with per-query values taking precedence over global defaults
- SLA evaluation compares each micro-batch against configured targets and produces `SlaViolation` records containing query name, SLA type, target value, observed value, batch ID, and timestamp
- Invalid configuration values (negative, zero, non-numeric) are rejected at query startup with descriptive error messages that include the offending property name and value
- Runtime reconfiguration via `sparkSession.conf.set()` updates SLA targets without requiring a query restart, taking effect starting from the next micro-batch
- Queries with no SLA configuration operate in "unconfigured" mode with no evaluation performed and no errors thrown
- Unit tests cover all 5 edge cases (empty/null, boundary, invalid, partial, special characters) and all 7 acceptance criteria
- Configuration properties follow the naming conventions documented in `docs/configuration.md` (`spark.streaming.sla.<queryName>.<metric>`)
- Code review is complete with no unresolved comments
