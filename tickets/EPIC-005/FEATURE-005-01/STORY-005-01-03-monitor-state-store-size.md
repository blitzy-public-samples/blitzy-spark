# Monitor State Store Size to Detect Unbounded State Growth and Prevent Out-of-Memory Failures

## User Story

**As a** data platform administrator,
**I want to** monitor per-operator state store metrics including numKeys, memoryUsedBytes, numRowsUpdated, numRowsRemoved, and state growth trends for each Structured Streaming query, with configurable size threshold alerts,
**So that** I can detect unbounded state growth before it causes out-of-memory executor failures, reducing unplanned streaming query crashes by at least 70% and avoiding the 2-4 hours of recovery time that each OOM failure typically requires.

## Acceptance Criteria

### AC1 — Input Validation (State Store Exists)

```gherkin
Given a Structured Streaming query is running with at least one stateful operator (e.g., groupBy().agg(), stream-stream join, flatMapGroupsWithState)
When the data platform administrator opens the Structured Streaming tab in the Spark Web UI
Then the UI displays a state store metrics panel listing each stateful operator by name
And each operator row shows numKeys, memoryUsedBytes (formatted in KB/MB/GB), numRowsUpdated, and numRowsRemoved from the latest StateOperatorProgress
```

### AC2 — Expected Output (Size Trend)

```gherkin
Given a Structured Streaming query has completed at least 10 micro-batches with stateful operations
When the data platform administrator views the state store trend panel
Then a chart displays memoryUsedBytes values for each stateful operator over the last 100 batches
And a second chart displays numKeys values for each stateful operator over the last 100 batches
```

### AC3 — Threshold Alert Configuration

```gherkin
Given the data platform administrator has configured a state store size threshold via SparkConf property spark.sql.streaming.stateStore.alertThresholdBytes with a value of 1073741824 (1 GB)
When any stateful operator's memoryUsedBytes exceeds 1073741824 bytes
Then a "State Size Threshold Exceeded" alert is emitted as a Dropwizard gauge metric with value 1
And a visual alert badge is displayed next to the affected operator in the Web UI
```

### AC4 — Per-Operator Detail View

```gherkin
Given a Structured Streaming query contains 3 stateful operators
When the data platform administrator clicks on a specific operator in the state store metrics panel
Then the detail view displays: operatorName, numKeys, memoryUsedBytes, numRowsUpdated, numRowsRemoved, allUpdatesTimeMs, allRemovalsTimeMs, commitTimeMs, numShufflePartitions, and numStateStoreInstances
And any customMetrics reported by the state store provider are displayed in a separate "Custom Metrics" subsection
```

### AC5 — Error Handling (No Stateful Operators)

```gherkin
Given a Structured Streaming query is running with only stateless transformations (filter, map, select)
When the data platform administrator views the Structured Streaming tab
Then the state store metrics panel displays "No Stateful Operators" as a text label
And no state store charts or threshold alert controls are rendered for that query
```

### AC6 — Dropwizard Metrics Exposure

```gherkin
Given a Structured Streaming query is running with stateful operators and the Dropwizard MetricsSystem is configured with at least one sink (Prometheus, JMX, CSV, or Graphite)
When the state store metrics are updated after each batch
Then states-rowsTotal and states-usedBytes gauge metrics are published to all configured metric sinks
And the metric values match the sum of numRowsTotal and memoryUsedBytes across all stateful operators respectively
```

### AC7 — Edge Case (State Store Provider Switch)

```gherkin
Given a Structured Streaming query was previously running with HDFSBackedStateStoreProvider and is restarted with RocksDBStateStoreProvider
When the data platform administrator views the state store metrics after restart
Then the metrics reflect the current provider's StateStoreMetrics (numKeys, memoryUsedBytes, customMetrics specific to RocksDB)
And no stale metrics from the previous provider are displayed
```

## Sub-Tasks

- **Sub-task 1:** Extract StateOperatorProgress data (numKeys, memoryUsedBytes, numRowsUpdated, numRowsRemoved, allUpdatesTimeMs, allRemovalsTimeMs, commitTimeMs, customMetrics) from StreamingQueryProgress and expose via the Structured Streaming REST API
  - `Source: sql/api/src/main/scala/org/apache/spark/sql/streaming/progress.scala` — StateOperatorProgress class fields
- **Sub-task 2:** Build per-operator state store metrics panel in the Structured Streaming tab UI with operator name, key count, memory usage (formatted KB/MB/GB), and row churn metrics
  - `Source: docs/web-ui.md` — Structured Streaming tab layout reference
- **Sub-task 3:** Implement state size trend charts storing memoryUsedBytes and numKeys for the last 100 batches per operator in a circular buffer
- **Sub-task 4:** Add configurable threshold alerting via SparkConf property (spark.sql.streaming.stateStore.alertThresholdBytes) that triggers a Dropwizard gauge metric and UI badge when exceeded
  - `Source: core/src/main/scala/org/apache/spark/metrics/MetricsSystem.scala` — MetricRegistry gauge registration
- **Sub-task 5:** Build per-operator detail view with full StateOperatorProgress fields including customMetrics subsection
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateStoreProvider.scala` — RocksDB-specific custom metrics (SST file size, get/put times, flush/compact/checkpoint latencies)
- **Sub-task 6:** Handle "No Stateful Operators" case by checking stateOperators array length in StreamingQueryProgress
- **Sub-task 7:** Verify existing states-rowsTotal and states-usedBytes Dropwizard gauge metrics in MetricsReporter and ensure they publish to configured sinks
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetricsReporter.scala` — states-rowsTotal and states-usedBytes gauge registrations (lines 54-55)
- **Sub-task 8:** Handle state store provider switch by clearing cached trend data on query restart when the provider class changes
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/HDFSBackedStateStoreProvider.scala` — HDFS-specific metrics for comparison
- **Sub-task 9:** Write unit tests for threshold alerting logic, memory formatting, trend buffer management, and provider switch handling
- **Sub-task 10:** Write integration tests with both HDFSBackedStateStoreProvider and RocksDBStateStoreProvider validating metric accuracy

## Edge Cases

- **Edge Case 1 (Empty/Null Input):** When the StateOperatorProgress array is empty (query has no stateful operators) or contains an operator with memoryUsedBytes = 0 and numKeys = 0 (operator initialized but no state yet), display the operator row with zero values and an "Initializing" label rather than hiding it.

- **Edge Case 2 (Boundary Values):** When memoryUsedBytes exceeds 10 GB (10,737,418,240 bytes), format as "XX.XX GB" with 2 decimal places. When numKeys exceeds 1 billion (1,000,000,000), format using SI notation ("1.00B keys"). When the alert threshold is set to 0, treat it as "always alert" and display a persistent warning.

- **Edge Case 3 (Invalid Input):** When the threshold configuration value (spark.sql.streaming.stateStore.alertThresholdBytes) is set to a negative number or non-numeric string, log an error-level message and fall back to the default threshold of Long.MaxValue (no alerting). Display "Invalid Threshold Configuration" in the alert settings section of the UI.

- **Edge Case 4 (High Partition Count):** When a stateful operator has more than 1,000 state store instances (numStateStoreInstances > 1000), aggregate the per-partition metrics into a summary view showing total, average, min, and max memoryUsedBytes across partitions, rather than listing each partition individually.

- **Edge Case 5 (Custom Metrics Overflow):** When a state store provider (e.g., RocksDB) reports more than 20 custom metrics, display the first 10 in the default view and provide an "Expand All" toggle to show the complete list, preventing UI layout overflow.

## Dependencies

- **FEATURE-005-01:** Stream Health Monitoring (parent feature) — [FEATURE-005-01-stream-health-monitoring.md](../FEATURE-005-01-stream-health-monitoring.md)
- **EPIC-005:** Enhanced Real-Time Stream Observability (parent epic) — [EPIC-005-enhanced-stream-observability.md](../../EPIC-005-enhanced-stream-observability.md)
- **F-003:** Real-Time Stream Processing (Structured Streaming with stateful operations)
- **StateStore API:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/StateStore.scala` — StateStoreMetrics (numKeys, memoryUsedBytes, customMetrics, instanceMetrics), StateStoreCustomMetric (StateStoreCustomSumMetric, StateStoreCustomSizeMetric, StateStoreCustomTimingMetric), StateStoreInstanceMetric
- **StateOperatorProgress:** `sql/api/src/main/scala/org/apache/spark/sql/streaming/progress.scala` — operatorName, numRowsTotal, numRowsUpdated, allUpdatesTimeMs, numRowsRemoved, allRemovalsTimeMs, commitTimeMs, memoryUsedBytes, numShufflePartitions, numStateStoreInstances, customMetrics
- **MetricsReporter:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/MetricsReporter.scala` — existing states-rowsTotal and states-usedBytes Dropwizard gauges
- **RocksDBStateStoreProvider:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateStoreProvider.scala` — RocksDB-specific custom metrics (SST file size, get/put times and counts, flush/compact/checkpoint/fileSync latencies, bytes/files copied and reused, block cache metrics)
- **HDFSBackedStateStoreProvider:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/HDFSBackedStateStoreProvider.scala` — HDFS-specific state store metrics
- **Spark Web UI:** `docs/web-ui.md` — Structured Streaming tab with state metrics display

## Story Estimation Guidance

- **Story Points: 8** (Fibonacci scale)
- **Rationale:** Involves multi-operator state store metric extraction from StreamingQueryProgress, configurable threshold alerting with SparkConf integration, trend charting for two metrics (memoryUsedBytes and numKeys) over 100-batch windows, per-operator detail view with custom metrics subsection, memory formatting logic (KB/MB/GB/SI notation), and Dropwizard metric verification across all configured sinks. Higher complexity due to provider-agnostic design (HDFSBackedStateStoreProvider vs RocksDBStateStoreProvider produce different custom metrics), threshold configuration validation with fallback handling, circular buffer management for trend data, and extensive edge case handling (empty state, high partition counts, custom metrics overflow, provider switch). Completable within a single sprint with focused effort.

## Definition of Done

- Per-operator state store metrics (numKeys, memoryUsedBytes, numRowsUpdated, numRowsRemoved) are displayed in the Structured Streaming tab of the Spark Web UI
- State size trend charts show memoryUsedBytes and numKeys over the last 100 batches per operator
- Configurable threshold alert (spark.sql.streaming.stateStore.alertThresholdBytes) triggers a Dropwizard gauge metric and a UI badge when exceeded
- Per-operator detail view shows all StateOperatorProgress fields including customMetrics in a dedicated subsection
- "No Stateful Operators" text label is displayed for queries with only stateless transformations
- states-rowsTotal and states-usedBytes Dropwizard gauges publish to all configured metric sinks (Prometheus, JMX, Graphite, CSV)
- Provider switch on query restart clears stale trend data and displays the current provider's metrics
- Unit tests achieve at least 90% line coverage for state store monitoring logic (threshold alerting, memory formatting, trend buffer management, provider switch handling)
- Integration tests validate metric accuracy with both HDFSBackedStateStoreProvider and RocksDBStateStoreProvider
- Code reviewed and merged to the feature branch
