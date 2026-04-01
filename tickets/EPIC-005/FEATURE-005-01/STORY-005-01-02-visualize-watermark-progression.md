# Visualize Watermark Progression to Track Event-Time Advancement Across Structured Streaming Queries

## User Story

**As a** data engineer,
**I want to** view the current event-time watermark value, watermark advancement rate (milliseconds advanced per batch), and the gap between wall-clock time and watermark for each active Structured Streaming query in the Spark Web UI,
**so that** I can detect stalled or slow-advancing watermarks within 60 seconds, preventing late-data accumulation that would otherwise increase state store size by an estimated 20-40% and degrade query performance.

## Acceptance Criteria

### AC1 — Input Validation (Watermark Enabled)

```gherkin
Given a Structured Streaming query is running with an event-time watermark defined via withWatermark()
When the data engineer opens the Structured Streaming tab in the Spark Web UI
Then the UI displays the current watermark value as an ISO8601 UTC timestamp (e.g., "2024-01-15T10:30:00.000Z")
And the watermark value matches the "watermark" field from the latest StreamingQueryProgress.eventTime map
```

`Source: sql/api/src/main/scala/org/apache/spark/sql/streaming/progress.scala` — StreamingQueryProgress.eventTime map contains "watermark", "max", "min", "avg" keys as ISO8601 UTC timestamps

### AC2 — Expected Output (Advancement Rate)

```gherkin
Given a Structured Streaming query has completed at least 5 micro-batches with advancing watermark values
When the data engineer views the watermark progression panel
Then the UI displays the watermark advancement rate as the average milliseconds advanced per batch over the last 10 batches
And a watermark progression chart plots watermark values against batch IDs for the last 100 batches
```

### AC3 — Event-Time Gap Display

```gherkin
Given a Structured Streaming query is actively processing data with event-time watermark
When the data engineer views the watermark details section
Then the UI displays the event-time gap calculated as (current wall-clock time in UTC minus current watermark value) in hours, minutes, and seconds format (e.g., "2h 15m 30s")
And the gap value updates on each UI refresh cycle
```

### AC4 — Multiple Watermark Operators

```gherkin
Given a Structured Streaming query contains 2 or more stateful operators each with independent watermark tracking
When the data engineer views the watermark progression panel
Then the UI displays the global watermark value (minimum across all operators) in the query summary
And per-operator watermark values are listed in an expandable detail section with operator names
```

`Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/WatermarkPropagator.scala` — WatermarkPropagator interface provides propagate(), getInputWatermarkForLateEvents(), getInputWatermarkForEviction() for per-operator watermark tracking

### AC5 — Error Handling (No Watermark Defined)

```gherkin
Given a Structured Streaming query is running without a withWatermark() definition
When the data engineer views the Structured Streaming tab
Then the watermark section displays "No Watermark Configured" as a text label
And the watermark progression chart and advancement rate fields are hidden for that query
```

### AC6 — Edge Case (Watermark Stall)

```gherkin
Given a Structured Streaming query's watermark has not advanced for 5 or more consecutive batches
When the data engineer views the watermark progression panel
Then a "Watermark Stalled" warning badge is displayed next to the watermark value
And the stall duration (time since last watermark advancement) is shown in seconds
```

## Sub-Tasks

- **Sub-task 1:** Extract the watermark value from `StreamingQueryProgress.eventTime` map ("watermark" key) and expose it via the Structured Streaming tab REST API endpoint
  - `Source: sql/api/src/main/scala/org/apache/spark/sql/streaming/progress.scala` — eventTime is `ju.Map[String, String]` with keys "watermark", "max", "min", "avg"
- **Sub-task 2:** Compute watermark advancement rate by comparing consecutive batch watermark values and averaging over the last 10 batches
- **Sub-task 3:** Implement event-time gap calculation as `(System.currentTimeMillis() - watermark epoch millis)` with human-readable formatting (hours, minutes, seconds)
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetricsReporter.scala` — reuse `convertStringDateToMillis` helper for ISO8601-to-epoch conversion
- **Sub-task 4:** Build watermark progression chart component rendering watermark values over the last 100 batches as a time-series line chart
- **Sub-task 5:** Add per-operator watermark display for queries with multiple stateful operators, using `WatermarkPropagator` per-operator tracking
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/WatermarkPropagator.scala` — `PropagateWatermarkSimulator` maintains per-operator input watermarks via `inputWatermarks: mutable.Map[Long, Map[Long, Option[Long]]]`
- **Sub-task 6:** Implement "No Watermark Configured" fallback for queries without `withWatermark()` by checking if the `eventTime` map contains the "watermark" key
- **Sub-task 7:** Add watermark stall detection logic (no advancement for 5 or more consecutive batches) with visual warning badge
- **Sub-task 8:** Register `eventTime-watermark` as a Dropwizard gauge metric via `MetricsReporter` (already exists as gauge — verify and extend with advancement-rate gauge)
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetricsReporter.scala` — existing `registerGauge("eventTime-watermark", ...)` gauge uses `convertStringDateToMillis(progress.eventTime.get("watermark"))`
- **Sub-task 9:** Write unit tests for advancement rate calculation, gap formatting, and stall detection logic
- **Sub-task 10:** Write integration test validating watermark progression display with a windowed aggregation streaming query

## Edge Cases

### Edge Case 1 — Empty/Null Input

When the `StreamingQueryProgress.eventTime` map is empty or the "watermark" key is null (query started but no event-time data has arrived yet), display "Pending" for the watermark value and disable the progression chart until the first non-null watermark value is received.

### Edge Case 2 — Boundary Values

When the watermark value is at epoch zero (`1970-01-01T00:00:00.000Z`), which indicates the watermark has not been initialized, display "Not Initialized" instead of the epoch timestamp. When the watermark advancement rate is exactly 0 for a single batch (no new data arrived), exclude that batch from the rolling average calculation.

### Edge Case 3 — Invalid Input

When the "watermark" string in the `eventTime` map is not a valid ISO8601 timestamp (malformed string), display "Parse Error" as the watermark value and log an error-level message with the malformed string and query ID.

### Edge Case 4 — Large Event-Time Gap

When the event-time gap exceeds 24 hours, switch the display format from "Xh Ym Zs" to "N days, Xh Ym" for readability, and display a critical alert badge indicating the watermark is significantly behind wall-clock time.

### Edge Case 5 — Query With No Stateful Operators

When a streaming query has no stateful operators (purely stateless transformations) but has a watermark defined, display the watermark value but note "No Stateful Operators" in the operator detail section.

## Dependencies

- **FEATURE-005-01:** Stream Health Monitoring (parent feature)
- **EPIC-005:** Enhanced Real-Time Stream Observability (parent epic)
- **STORY-005-01-01:** Display Stream Processing Rates — shared UI framework for the Structured Streaming tab (optional — can be developed in parallel)
- **F-003:** Real-Time Stream Processing (Structured Streaming must be functional with watermark support)
- **StreamingQueryProgress.eventTime:** `sql/api/src/main/scala/org/apache/spark/sql/streaming/progress.scala` — eventTime map with "watermark", "max", "min", "avg" keys
- **WatermarkPropagator:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/WatermarkPropagator.scala` — per-operator watermark propagation (propagate, getInputWatermarkForLateEvents, getInputWatermarkForEviction)
- **MetricsReporter:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetricsReporter.scala` — existing eventTime-watermark Dropwizard gauge registration
- **Spark Web UI:** `docs/web-ui.md` — Structured Streaming tab with existing watermark metrics display (Global Watermark Gap, state rows, memory usage)

## Story Estimation Guidance

- **Story Points:** 5 (Fibonacci scale)
- **Rationale:** Involves watermark extraction from `StreamingQueryProgress`, advancement rate computation with rolling average, event-time gap calculation with formatting, chart rendering, multi-operator display, and stall detection. Moderate complexity — leverages existing `eventTime` map in progress events but requires new UI components and computation logic. Completable within a single sprint.

## Definition of Done

- Current watermark value is displayed as ISO8601 UTC timestamp for each active watermark-enabled query
- Watermark advancement rate (average ms/batch over last 10 batches) is computed and displayed
- Event-time gap (wall-clock minus watermark) is displayed in human-readable format (hours, minutes, seconds)
- Watermark progression chart shows watermark values for the last 100 batches
- Per-operator watermark values are shown in an expandable detail section for multi-operator queries
- "No Watermark Configured" is displayed for queries without `withWatermark()`
- "Watermark Stalled" warning appears after 5 consecutive non-advancing batches
- Dropwizard gauge metric for watermark is verified and advancement-rate gauge is registered
- Unit tests achieve at least 90% line coverage for watermark display and computation logic
- Integration test validates watermark progression display with a windowed aggregation query
- Code reviewed and merged to the feature branch
