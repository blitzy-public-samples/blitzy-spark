# Display Stream Processing Rates to Enable Real-Time Throughput Monitoring for Structured Streaming Queries

## User Story

**As a** streaming application developer,
**I want to** view input rows per second and processed rows per second metrics for each active Structured Streaming query in the Spark Web UI Structured Streaming tab, with both per-batch values and rolling trend charts,
**So that** I can monitor query throughput in real time and identify processing bottlenecks within 30 seconds of occurrence, reducing mean-time-to-detection of throughput degradation by at least 50% compared to manual log inspection.

## Acceptance Criteria

### AC1 — Input Validation

```gherkin
Given a Structured Streaming query is running with at least one configured source
When the streaming application developer opens the Structured Streaming tab in the Spark Web UI
Then the UI displays the current inputRowsPerSecond metric for each active query, rounded to 2 decimal places
And the metric value matches the inputRowsPerSecond field from the latest StreamingQueryProgress event within a 1-second tolerance
```

`Source: sql/api/src/main/scala/org/apache/spark/sql/streaming/progress.scala` — `StreamingQueryProgress.inputRowsPerSecond` is computed as the sum of all `SourceProgress.inputRowsPerSecond` values.

### AC2 — Expected Output (Processing Rate)

```gherkin
Given a Structured Streaming query has processed at least 3 micro-batches
When the streaming application developer views the processing rate panel
Then the UI displays processedRowsPerSecond as a numeric value with 2 decimal places
And a trend chart shows processedRowsPerSecond values for the last 100 completed batches plotted against batch timestamps
```

`Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/ProgressReporter.scala` — `progressBuffer` (Queue[StreamingQueryProgress]) stores recent progress events that feed the trend chart.

### AC3 — Per-Batch Display

```gherkin
Given a Structured Streaming query has completed batch ID N
When the streaming application developer selects batch N in the batch history table
Then the UI shows inputRowsPerSecond and processedRowsPerSecond specific to that batch
And the batch duration in milliseconds is displayed alongside the rate metrics
```

`Source: sql/api/src/main/scala/org/apache/spark/sql/streaming/progress.scala` — `StreamingQueryProgress` exposes `batchId`, `batchDuration`, `inputRowsPerSecond`, and `processedRowsPerSecond` per trigger.

### AC4 — Multiple Query Support

```gherkin
Given 3 or more Structured Streaming queries are running concurrently in the same SparkSession
When the streaming application developer views the Structured Streaming tab
Then each query is listed separately with its own inputRowsPerSecond and processedRowsPerSecond values
And the total aggregate inputRowsPerSecond across all queries is displayed in a summary row
```

### AC5 — Error Handling (No Data)

```gherkin
Given a Structured Streaming query has been started but no data has arrived from the source
When the streaming application developer views the processing rate panel
Then the inputRowsPerSecond displays 0.00 and processedRowsPerSecond displays 0.00
And a status indicator shows "Awaiting Data" instead of displaying an error message
```

`Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetricsReporter.scala` — The `registerGauge` method defaults to `0.0` when `stream.lastProgress` is null, confirming that a zero-state is the expected baseline before data arrives.

### AC6 — Edge Case (Query Restart)

```gherkin
Given a Structured Streaming query was stopped and restarted from a checkpoint
When the streaming application developer views the Structured Streaming tab after restart
Then the trend chart starts fresh from the first batch after restart
And the previous run's processing rate history is not mixed with the current run's data
```

`Source: sql/api/src/main/scala/org/apache/spark/sql/streaming/progress.scala` — Each `StreamingQueryProgress` carries a `runId` (UUID unique per start/restart) that distinguishes runs of the same persistent query `id`.

## Sub-Tasks

- **Sub-task 1:** Extend the Structured Streaming tab REST API endpoint (`/api/v1/applications/[app-id]/streaming/statistics`) to include per-query `inputRowsPerSecond` and `processedRowsPerSecond` in the JSON response
- **Sub-task 2:** Add a processing rate panel component to the Structured Streaming tab UI that renders numeric rate values from `StreamingQueryProgress` data
- **Sub-task 3:** Implement a trend chart component that stores the last 100 batch rate values in a circular buffer and renders them as a time-series line chart
- **Sub-task 4:** Add per-batch rate display to the batch detail view with `inputRowsPerSecond`, `processedRowsPerSecond`, and `batchDuration` fields
- **Sub-task 5:** Implement aggregate summary row logic for multi-query display, summing `inputRowsPerSecond` across all active queries
- **Sub-task 6:** Handle zero-data and query-restart edge cases with "Awaiting Data" status indicator and fresh trend chart initialization
- **Sub-task 7:** Register `inputRate-total` and `processingRate-total` as Dropwizard gauge metrics via `MetricsReporter` for external monitoring system consumption
- **Sub-task 8:** Write unit tests for rate computation, trend buffer management, and multi-query aggregation logic
- **Sub-task 9:** Write integration tests validating end-to-end rate display with a rate source streaming query

## Edge Cases

- **Edge Case 1 (Empty/Null Input):** When `StreamingQueryProgress` is null (query just started, no progress yet), display "N/A" for both rate metrics and disable the trend chart until the first progress event arrives.
- **Edge Case 2 (Boundary Values):** When `inputRowsPerSecond` exceeds 1,000,000 rows/sec, format the display using SI notation (e.g., "1.25M rows/sec") to prevent UI overflow; when `processedRowsPerSecond` is exactly 0.00 for more than 10 consecutive batches, display a warning badge.
- **Edge Case 3 (Invalid Input):** When `StreamingQueryProgress` reports `NaN` or `Infinity` for `inputRowsPerSecond` (division by zero in rate calculation), display "—" as placeholder text and log a warning-level message with the query ID and batch ID.
- **Edge Case 4 (Rapid Query Start/Stop):** When a query runs for fewer than 2 batches before stopping, display the available batch data without rendering the trend chart (minimum 2 data points required for trend line).
- **Edge Case 5 (Concurrent Query Termination):** When one of multiple active queries terminates while the UI is open, remove that query's row from the display within the next UI refresh cycle (default 1 second) without disrupting the remaining queries' metrics.

## Dependencies

- **FEATURE-005-01:** Stream Health Monitoring (parent feature) — [FEATURE-005-01-stream-health-monitoring.md](../FEATURE-005-01-stream-health-monitoring.md)
- **EPIC-005:** Enhanced Real-Time Stream Observability (parent epic) — [EPIC-005-enhanced-stream-observability.md](../../EPIC-005-enhanced-stream-observability.md)
- **F-003:** Real-Time Stream Processing (Structured Streaming must be functional)
- **StreamingQueryProgress API:** `sql/api/src/main/scala/org/apache/spark/sql/streaming/progress.scala` — provides `inputRowsPerSecond`, `processedRowsPerSecond`, `batchDuration`, `batchId`, `numInputRows`
- **MetricsReporter:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetricsReporter.scala` — existing Dropwizard gauge registration for `inputRate-total`, `processingRate-total`, `latency`, `eventTime-watermark`, `states-rowsTotal`, `states-usedBytes`
- **ProgressReporter:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/ProgressReporter.scala` — `progressBuffer` (Queue[StreamingQueryProgress]), progress computation and reporting logic
- **Spark Web UI:** `docs/web-ui.md` — Structured Streaming tab (existing UI extension point)
- **Dropwizard Metrics 4.2.33:** `core/src/main/scala/org/apache/spark/metrics/MetricsSystem.scala` — metrics sink infrastructure (Prometheus, JMX, Graphite, StatsD, CSV, Console)

## Story Estimation Guidance

- **Story Points:** 5 (Fibonacci scale)
- **Rationale:** Involves extending the existing Web UI Structured Streaming tab with new visual components (rate panel and trend chart), REST API modifications, multi-query aggregation logic, edge case handling, and integration with the Dropwizard metrics pipeline. Moderate complexity due to UI rendering and circular buffer management, but leverages existing `StreamingQueryProgress` data structures. Completable within a single sprint.

## Definition of Done

- `inputRowsPerSecond` and `processedRowsPerSecond` are displayed in the Structured Streaming tab for each active query
- Trend chart renders the last 100 batch rate values as a time-series line chart
- Per-batch rate detail view shows rate metrics alongside batch duration
- Multi-query aggregate summary row displays total `inputRowsPerSecond`
- Zero-data state shows 0.00 rates with "Awaiting Data" indicator
- Query restart initializes a fresh trend chart without stale data
- `NaN`/`Infinity` rate values display "—" placeholder with logged warning
- `inputRate-total` and `processingRate-total` Dropwizard gauge metrics are registered and emitted to all configured sinks
- Unit tests achieve at least 90% line coverage for rate display logic
- Integration test validates end-to-end display with a running rate source query
- Code reviewed and merged to the feature branch
