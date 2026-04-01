# Detect Processing Lag to Surface Streaming Pipeline Delays via Web UI and Metrics System

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

**As a** DevOps engineer,
**I want** to see the processing lag — defined as the difference between the current wall-clock time and the event time of the most recently processed record — for each active Structured Streaming query, displayed in the Spark Web UI and exposed as a Dropwizard metric,
**So that** I can detect when a streaming pipeline falls behind real-time processing within 2 minutes and trigger scaling or alerting actions, reducing the risk of data delivery SLA breaches by at least 60%.

## Acceptance Criteria

### AC1 — Input Validation (Lag Computation)

```gherkin
Given a Structured Streaming query is running and has completed at least 1 micro-batch with event-time data
When the DevOps engineer opens the Structured Streaming tab in the Spark Web UI
Then the UI displays the processing lag in seconds, calculated as (current wall-clock UTC epoch milliseconds minus the maximum event time from the latest StreamingQueryProgress.eventTime["max"]) divided by 1000
And the lag value updates on each UI refresh cycle (default 1 second)
```

> `Source: sql/api/src/main/scala/org/apache/spark/sql/streaming/progress.scala` — `StreamingQueryProgress.eventTime` is a `ju.Map[String, String]` containing keys "max", "min", "avg", "watermark" with ISO8601 UTC timestamps (lines 130–137, field declaration at line 154)

### AC2 — Expected Output (Lag Trend Chart)

```gherkin
Given a Structured Streaming query has completed at least 10 micro-batches
When the DevOps engineer views the processing lag panel
Then a trend chart displays the computed processing lag values for the last 100 batches plotted against batch timestamps
And the Y-axis label reads "Processing Lag (seconds)" and the X-axis label reads "Batch Timestamp"
```

### AC3 — Dropwizard Metric Exposure

```gherkin
Given the Dropwizard MetricsSystem is configured with at least one sink (Prometheus, JMX, CSV, or Graphite)
When a Structured Streaming query updates its processing lag after each batch
Then a Dropwizard gauge metric named "processingLag-seconds" is registered and published to all configured metric sinks
And the gauge value equals the latest processing lag in seconds as a Long value
```

> `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetricsReporter.scala` — Existing gauge registration pattern at lines 42–55 registers `inputRate-total`, `processingRate-total`, `latency`, `eventTime-watermark`, `states-rowsTotal`, and `states-usedBytes` via `registerGauge()` (lines 66–75)

### AC4 — Lag Severity Classification

```gherkin
Given a Structured Streaming query has a computed processing lag value
When the processing lag is less than 60 seconds
Then the lag indicator displays a green "Healthy" badge
When the processing lag is between 60 and 300 seconds (inclusive)
Then the lag indicator displays a yellow "Warning" badge
When the processing lag exceeds 300 seconds
Then the lag indicator displays a red "Critical" badge
```

### AC5 — Error Handling (No Event Time)

```gherkin
Given a Structured Streaming query is processing data without event-time semantics (no timestamp column, no watermark)
When the DevOps engineer views the processing lag section
Then the lag panel displays "Event Time Not Available" as a text label
And no lag metric is registered in the Dropwizard MetricsSystem for that query
```

### AC6 — Edge Case (Clock Skew)

```gherkin
Given a Structured Streaming query is running and the maximum event time from the latest batch exceeds the current wall-clock time (negative lag due to clock skew or future-dated events)
When the DevOps engineer views the processing lag display
Then the processing lag displays 0 seconds (clamped to non-negative)
And a "Clock Skew Detected" informational badge is shown next to the lag value
```

## Sub-Tasks

- **ST-1: Implement processing lag calculation** — Compute lag as `max(0, System.currentTimeMillis() - parseISO8601ToEpochMillis(StreamingQueryProgress.eventTime["max"])) / 1000`, returning seconds as a Long. Reuse the `convertStringDateToMillis` parsing pattern from `MetricsReporter.scala` (line 57–64) which applies the ISO8601 `DateTimeFormatter` with UTC zone ID.
- **ST-2: Add processing lag display to the Structured Streaming tab UI** — Render the numeric lag value with a severity badge (Healthy / Warning / Critical) based on configurable thresholds. Place the lag indicator alongside the existing Input Rate and Process Rate displays.
- **ST-3: Implement lag trend chart component** — Store the last 100 computed lag values in a circular buffer and render as a time-series line chart with Y-axis "Processing Lag (seconds)" and X-axis "Batch Timestamp".
- **ST-4: Register "processingLag-seconds" Dropwizard gauge metric** — Add a new `registerGauge("processingLag-seconds", ...)` call in `MetricsReporter` alongside the existing `inputRate-total` and `processingRate-total` gauges (lines 42–44), following the same `registerGauge[T]` pattern (lines 66–75).
- **ST-5: Implement severity classification logic with configurable thresholds** — Read thresholds from SparkConf properties: `spark.sql.streaming.lag.warningThresholdSeconds` (default 60) and `spark.sql.streaming.lag.criticalThresholdSeconds` (default 300). Classify lag as Healthy (< warning), Warning (>= warning and <= critical), or Critical (> critical).
- **ST-6: Handle "Event Time Not Available" fallback** — Check whether `StreamingQueryProgress.eventTime` is null or does not contain the "max" key before computing lag. If absent, display "Event Time Not Available" and skip the Dropwizard gauge update for that query.
- **ST-7: Implement clock skew detection** — When the computed raw lag value is negative (event time exceeds wall-clock time), clamp the displayed lag to 0 seconds and set a clock-skew flag that triggers a "Clock Skew Detected" informational badge in the UI.
- **ST-8: Expose processing lag via REST API** — Include processing lag in the JSON response of the REST API endpoint `/api/v1/applications/[app-id]/streaming/statistics` as a new field `"processingLagSeconds"`.
- **ST-9: Write unit tests** — Cover lag computation (positive, zero, negative/clamped), severity classification at all threshold boundaries (59, 60, 300, 301), clock skew clamping, missing event time fallback, and SparkConf threshold configuration parsing.
- **ST-10: Write integration test** — Use a `rate` source streaming query to validate end-to-end lag detection, Dropwizard gauge publication, severity badge display, and REST API response inclusion.

## Edge Cases

- **Edge Case 1 — Empty/Null Input:** When `StreamingQueryProgress.eventTime` map is null or does not contain the "max" key (no event-time data in the current batch), display "N/A" for the lag value and skip the Dropwizard gauge update for that batch, retaining the last known lag value.
- **Edge Case 2 — Boundary Values:** When processing lag is exactly 60 seconds, classify as "Warning" (inclusive lower bound). When processing lag is exactly 300 seconds, classify as "Warning" (inclusive upper bound of Warning range, consistent with AC4's 60–300 inclusive definition). When the processing lag is 301 seconds, classify as "Critical". When lag exceeds 86,400 seconds (24 hours), display in "N days, Xh Ym" format instead of raw seconds.
- **Edge Case 3 — Invalid Input:** When the "max" event-time string in `StreamingQueryProgress.eventTime` is not a valid ISO8601 timestamp, log an error-level message with the malformed value and query ID, display "Parse Error" as the lag value, and do not update the Dropwizard gauge metric.
- **Edge Case 4 — Batch Processing Time Spikes:** When a single batch takes more than 10x the average batch duration (outlier), the lag computation still reflects the wall-clock time difference at the point of UI refresh — not the batch's end time — to provide an accurate real-time lag measurement.
- **Edge Case 5 — Continuous Processing Mode:** When the streaming query runs in continuous processing mode (`Trigger.Continuous`) instead of micro-batch mode, adapt the lag computation to use the continuous epoch's latest processed event time rather than batch-based event time, and note "Continuous Mode" in the lag display.

## Dependencies

- **FEATURE-005-01:** Stream Health Monitoring (parent feature) — [FEATURE-005-01-stream-health-monitoring.md](../FEATURE-005-01-stream-health-monitoring.md)
- **EPIC-005:** Enhanced Real-Time Stream Observability (parent epic) — [EPIC-005-enhanced-stream-observability.md](../../EPIC-005-enhanced-stream-observability.md)
- **F-003:** Real-Time Stream Processing — Structured Streaming engine must be functional for event-time processing and micro-batch/continuous execution
- **StreamingQueryProgress.eventTime:** `Source: sql/api/src/main/scala/org/apache/spark/sql/streaming/progress.scala` — `eventTime` map (type `ju.Map[String, String]`) with keys "max", "min", "avg", "watermark" containing ISO8601 UTC timestamps; also provides `durationMs`, `batchId`, `inputRowsPerSecond`, `processedRowsPerSecond`
- **OffsetSeqLog:** `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/OffsetSeqLog.scala` — Offset tracking for per-batch progress, logs offset ranges to persistent HDFS files
- **CommitLog:** `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/CommitLog.scala` — Batch commit log recording successful batch completions before processing the next batch
- **MetricsReporter:** `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetricsReporter.scala` — Existing Dropwizard gauge registration pattern for `inputRate-total`, `processingRate-total`, `latency`, `eventTime-watermark`, `states-rowsTotal`, `states-usedBytes`; uses `registerGauge[T]` helper (lines 66–75)
- **MetricsSystem:** `Source: core/src/main/scala/org/apache/spark/metrics/MetricsSystem.scala` — Dropwizard metrics sink infrastructure managing `MetricRegistry`, `Source`, and `Sink` abstractions; supports Prometheus (`PrometheusServlet`), JMX, Graphite, StatsD, CSV, Console, and Slf4j sinks
- **Spark Web UI:** `Source: docs/web-ui.md` — Structured Streaming tab serves as the display surface for processing rates, watermark, state, and lag metrics
- **Monitoring Guide:** `Source: docs/monitoring.md` — REST API endpoints (`/api/v1/applications/[app-id]/streaming/statistics`), metrics sink configuration, and event logging

## Story Estimation Guidance

- **Story Points:** 5 (Fibonacci scale)
- **Rationale:** This story involves processing lag computation from existing `StreamingQueryProgress` event-time data, severity classification with configurable thresholds via SparkConf, a new Dropwizard gauge registration following the established `MetricsReporter` pattern, trend chart rendering for the last 100 batches, clock skew clamping, and REST API extension. Moderate complexity — the implementation leverages the existing progress reporting pipeline and Dropwizard metrics infrastructure without requiring new framework integrations. Completable within a single sprint.

## Definition of Done

- Processing lag is displayed in seconds in the Structured Streaming tab for each active query with event-time data
- Lag severity badges (Healthy / Warning / Critical) are shown with configurable thresholds (`spark.sql.streaming.lag.warningThresholdSeconds` default 60, `spark.sql.streaming.lag.criticalThresholdSeconds` default 300)
- Lag trend chart shows computed lag values for the last 100 batches with labeled axes
- `processingLag-seconds` Dropwizard gauge metric is registered in `MetricsReporter` and published to all configured sinks (Prometheus, JMX, Graphite, CSV, Console)
- "Event Time Not Available" is displayed for queries without event-time semantics
- Negative lag (clock skew) is clamped to 0 seconds with a "Clock Skew Detected" informational badge
- Processing lag is included in the REST API JSON response at `/api/v1/applications/[app-id]/streaming/statistics` as the `processingLagSeconds` field
- Unit tests achieve at least 90% line coverage for lag computation, severity classification, clock skew clamping, and threshold configuration parsing
- Integration test validates end-to-end lag detection with a `rate` source streaming query, confirming metric publication and UI rendering
- Code reviewed and merged to the feature branch
