# Generate Structured Observability Reports to Provide Comprehensive Stream Health Documentation

## User Story

**As a** data engineer,
**I want to** generate structured observability reports in JSON and CSV formats containing four sections — (1) backpressure events log with timestamps, query IDs, detection method, and duration; (2) SLA compliance summary with per-query target versus observed values, violation count, and compliance percentage; (3) resource utilization summary with executor count, CPU utilization, memory utilization, and state store sizes over the reporting window; and (4) stream health snapshots with processing rates, watermark progression, and checkpoint status — accessible via REST API at `/api/v1/applications/[app-id]/streaming/observability/report` with query parameters for format (`json` or `csv`), time range (`startTime`, `endTime`), and query filter (`queryName`),
**so that** data engineering teams can generate on-demand and scheduled compliance reports for stakeholders without manual data collection, reducing weekly reporting effort from 4 hours of manual metric gathering to under 30 seconds of API calls and enabling audit-ready documentation of streaming pipeline health.

### Source Context

- `Source: docs/monitoring.md` — REST API `/api/v1` endpoints (`/applications`, `/applications/[app-id]/jobs`, `/applications/[app-id]/streaming`), event logging, metrics sinks (Prometheus, Graphite, StatsD, JMX, CSV, Console)
- `Source: core/src/main/scala/org/apache/spark/status/AppStatusStore.scala` — KVStore-backed historical data providing executor summaries (`executorList`), task metrics, stage data, and application info used to populate the resource utilization section of the report
- `Source: core/src/main/scala/org/apache/spark/status/api/v1/ApiRootResource.scala` — REST API root resource at `@Path("/v1")` with JAX-RS endpoint registration patterns and JacksonMessageWriter for JSON serialization
- `Source: core/src/main/scala/org/apache/spark/status/api/v1/OneApplicationResource.scala` — Per-application REST resource pattern with `@GET`, `@Path`, `@QueryParam` annotations and `withUI(_.store.*)` data access pattern
- `Source: core/src/main/scala/org/apache/spark/status/api/v1/JacksonMessageWriter.scala` — Jackson JSON serialization for REST API responses
- `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/ProgressReporter.scala` — `StreamingQueryProgress` reporting with `progressBuffer`, ISO8601 timestamp formatting (`yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`), and per-batch metrics (inputRowsPerSecond, processedRowsPerSecond, durationMs, watermark)

## Acceptance Criteria

**AC-1 (Input Validation — Format Parameter):**
- **Given** a REST API request to `/api/v1/applications/[app-id]/streaming/observability/report?format=json`
- **When** the `format` parameter is `json` or `csv`
- **Then** the server accepts the request and returns the report in the specified format with Content-Type `application/json` or `text/csv` respectively

**AC-2 (Input Validation — Time Range):**
- **Given** a REST API request with `startTime=2024-01-01T00:00:00Z&endTime=2024-01-01T23:59:59Z`
- **When** `startTime` is before `endTime` and both are valid ISO8601 timestamps
- **Then** the report includes only events and metrics that fall within the specified time range

**AC-3 (Expected Output — JSON Report):**
- **Given** a streaming query `ordersStream` that experienced 3 backpressure events and 5 SLA violations in the last hour
- **When** a JSON format report is requested
- **Then** the response contains a JSON object with four top-level keys:
  - `backpressureEvents` — array of 3 objects each with fields: `timestamp`, `queryId`, `queryName`, `detectionMethod`, `durationMs`
  - `slaCompliance` — object with fields: `queryName`, `latencyTarget`, `latencyP99`, `throughputTarget`, `throughputP50`, `violationCount` (value: 5), `compliancePercentage`
  - `resourceUtilization` — object with fields: `executorCount`, `avgCpuPercent`, `avgMemoryPercent`, `peakMemoryPercent`, `stateStoreSizeBytes`
  - `streamHealth` — object with fields: `avgInputRowsPerSec`, `avgProcessedRowsPerSec`, `currentWatermark`, `lastCheckpointTimestamp`, `lastCheckpointDurationMs`

**AC-4 (Expected Output — CSV Report):**
- **Given** the same streaming query data as AC-3
- **When** a CSV format report is requested
- **Then** the response contains a CSV file with a header row and data rows, where each section is separated by a blank line and a section header row (e.g., `# Backpressure Events`, `# SLA Compliance`, `# Resource Utilization`, `# Stream Health`), and each row within a section uses comma-separated values matching the JSON field names as column headers

**AC-5 (Error Handling — Invalid Format):**
- **Given** a REST API request with `format=xml`
- **When** the `format` parameter is not `json` or `csv`
- **Then** the server returns HTTP 400 Bad Request with a JSON error body containing the message `Invalid format parameter. Accepted values: json, csv` and the invalid value received

**AC-6 (Error Handling — No Data Available):**
- **Given** a streaming application that has just started with no completed micro-batches
- **When** an observability report is requested
- **Then** the server returns HTTP 200 with an empty report structure (empty arrays and zero-valued metrics) rather than HTTP 404 or 500, and each section includes a `status` field set to `no_data_available`

**AC-7 (Edge Case — Multi-Query Report):**
- **Given** 3 streaming queries running in the same application
- **When** a report is requested without a `queryName` filter
- **Then** the report aggregates data from all 3 queries, with the `backpressureEvents` array containing events from all queries sorted by timestamp descending, `slaCompliance` containing an entry per query, and `resourceUtilization` showing application-level aggregates

**AC-8 (Edge Case — Large Time Range):**
- **Given** a report request spanning 7 days with over 100,000 completed micro-batches
- **When** the report is generated
- **Then** the response is returned within 30 seconds, summary metrics are computed using pre-aggregated data from `AppStatusStore` rather than re-scanning all individual batch records, and the report includes a `metadata` section with `generationTimeMs` and `batchesAnalyzed` count

## Sub-Tasks

- Design `ObservabilityReport` data model with four sections: `BackpressureEventsSection`, `SlaComplianceSection`, `ResourceUtilizationSection`, `StreamHealthSection`
- Implement `ObservabilityReportGenerator` class that collects data from `AppStatusStore` (executor data, streaming progress), backpressure event store (from STORY-005-02-01), and SLA violation records (from STORY-005-02-02 and STORY-005-02-03)
- Implement JSON serialization using Jackson (version 2.20.0 per `pom.xml`) for the report data model, following the serialization patterns in `JacksonMessageWriter`
- Implement CSV serialization with section headers (`# Backpressure Events`, `# SLA Compliance`, `# Resource Utilization`, `# Stream Health`) and escaping for values containing commas or newlines
- Create REST API endpoint at `/api/v1/applications/[app-id]/streaming/observability/report` following the existing JAX-RS endpoint patterns in `core/src/main/scala/org/apache/spark/status/api/v1/` (using `@GET`, `@Path`, `@QueryParam`, `@Produces` annotations)
- Implement query parameter handling: `format` (default: `json`), `startTime` and `endTime` (optional ISO8601), `queryName` (optional filter)
- Implement time-range filtering for all report sections using ISO8601 timestamp parsing consistent with `ProgressReporter`'s `DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")`
- Implement multi-query aggregation for application-level reports when no `queryName` filter is specified
- Optimize report generation for large time ranges using pre-aggregated `AppStatusStore` data accessed via `KVStore` views
- Add report generation metadata section containing `generationTimeMs`, `batchesAnalyzed`, and `reportVersion`
- Write unit tests for report generation with mock data, JSON and CSV serialization, time filtering, and multi-query aggregation
- Write integration tests for the REST API endpoint with parameter combinations including valid formats, invalid formats, time ranges, query filters, empty data, and large datasets

## Edge Cases

- **Empty/Null Input**: No streaming queries have completed any micro-batches — the report must return a valid empty structure with empty arrays, zero-valued metrics, and a `status` field set to `no_data_available` in each section; the system must not return null values or throw exceptions
- **Boundary Values**: Time range `startTime` equals `endTime` (zero-width window) — the system must return a report containing only events with a timestamp matching that exact instant, or an empty report if no events match that timestamp
- **Invalid Input**: `startTime` is after `endTime` in the query parameters — the system must return HTTP 400 with a descriptive error message `startTime must be before endTime` and include the received values in the error response body
- **Large Report**: Report spans thousands of batches across multiple queries generating a response exceeding 10 MB — the system must apply pagination or streaming response (chunked transfer encoding) and include a `truncated` flag set to `true` if the response exceeds a configurable maximum size (default 50 MB)
- **Concurrent Report Generation**: Multiple report requests arrive simultaneously for the same application — the system must handle concurrent access to `AppStatusStore` without data corruption or deadlocks, using read-only access patterns consistent with `AppStatusStore`'s thread-safe `KVStore` views

## Dependencies

- **STORY-005-02-01** (Detect Backpressure Conditions) — provides backpressure event data (timestamps, query IDs, detection methods, durations) for the backpressure events section of the report
- **STORY-005-02-02** (Configure Processing SLA Targets) — provides SLA target values (`maxLatencyMs`, `minThroughputRowsPerSec`) for compliance percentage calculations in the SLA compliance section
- **STORY-005-02-03** (Alert on SLA Breaches) — provides SLA violation records and cumulative breach counts for the SLA compliance section
- **STORY-005-02-04** (Recommend Resource Scaling) — resource scaling recommendations may be included as an optional section in the report
- **FEATURE-005-01** (Stream Health Monitoring) — provides stream health metrics (processing rates, watermark progression, state store size, checkpoint status) for the stream health section
- **AppStatusStore** (`core/src/main/scala/org/apache/spark/status/AppStatusStore.scala`) — historical executor summaries, stage data, and application info via `KVStore` for the resource utilization section
- **REST API Infrastructure** (`core/src/main/scala/org/apache/spark/status/api/v1/`) — endpoint registration via `ApiRootResource`, request handling via `OneApplicationResource` pattern, and JSON serialization via `JacksonMessageWriter`
- **Jackson JSON Library** (version 2.20.0 per `pom.xml`) — JSON serialization and deserialization for the report data model

## Story Estimation Guidance

- **Story Points**: 8 (Fibonacci)
- **Justification**: Involves designing a multi-section report data model, implementing dual-format serialization (JSON and CSV), creating a REST API endpoint with JAX-RS annotations, building time-range filtering with ISO8601 parsing, implementing multi-query aggregation logic, and optimizing for large-dataset performance. High complexity due to the breadth of data sources (AppStatusStore, backpressure events, SLA violations, stream health metrics) and the performance requirement of 30-second response time for 7-day time ranges.
- **Complexity Factors**:
  - Multi-section report data model design (4 sections with distinct schemas)
  - Dual serialization formats (JSON via Jackson, CSV with section headers and escaping)
  - REST API endpoint creation following existing JAX-RS patterns
  - Time-range filtering with ISO8601 timestamp parsing and validation
  - Multi-query aggregation for application-level reports
  - Large-dataset performance optimization using pre-aggregated `AppStatusStore` data
  - Concurrent access handling with thread-safe read-only `KVStore` views

## Definition of Done

- Observability reports are generated in both JSON and CSV formats via the REST API endpoint at `/api/v1/applications/[app-id]/streaming/observability/report`
- Reports contain all four sections: backpressure events, SLA compliance, resource utilization, and stream health
- Time-range filtering works with ISO8601 timestamp parameters (`startTime`, `endTime`)
- Multi-query aggregation produces application-level summaries when no `queryName` filter is specified
- Per-query filtering returns query-specific data when the `queryName` parameter is provided
- Report generation completes within 30 seconds for time ranges spanning up to 7 days
- Invalid parameters (unsupported format, reversed time range) return descriptive HTTP 400 responses
- Empty or newly started applications return valid empty report structures without errors
- Unit tests cover all report sections, both serialization formats, time filtering, and edge cases
- Integration tests validate the REST API endpoint with parameter combinations including valid formats, invalid formats, time ranges, query filters, and empty data scenarios
- Report metadata includes `generationTimeMs` and `batchesAnalyzed` fields
- Code review is complete with no unresolved comments
