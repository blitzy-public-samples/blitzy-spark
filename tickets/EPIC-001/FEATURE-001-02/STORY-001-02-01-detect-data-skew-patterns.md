# Detect Data Skew Patterns in Shuffle Operations to Surface Partition Imbalance Diagnostics

## User Story

**As a** data engineer, **I want to** view a data skew analysis report that identifies shuffle partitions whose data size exceeds a configurable skew factor relative to the median partition size for each shuffle stage in a completed or running SQL query, **so that** I can pinpoint the root cause of straggler tasks that disproportionately slow down query execution, reducing skew-related debugging time from hours to minutes and enabling targeted repartitioning or salting fixes that can improve query runtime by 40-70% for skew-affected workloads.

## Acceptance Criteria

### AC-1: Expected Output — Skew Detection

- **Given** a completed SQL query with shuffle stages where at least one partition has data size greater than `spark.sql.adaptive.skewJoin.skewedPartitionFactor` (default: 5) multiplied by the median partition size AND greater than `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` (default: 256MB)
- **When** the data skew analysis runs on the query execution profile
- **Then** the system identifies each skewed partition and reports: stage ID, partition index, partition size in bytes, median partition size in bytes, skew ratio (partition size divided by median size), and the shuffle operation type (e.g., SortMergeJoin, HashAggregate)

> Source: `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/OptimizeSkewedJoin.scala` — `getSkewThreshold` method (lines 64-67) computes the skew threshold as `SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.max(medianSize * SKEW_JOIN_SKEWED_PARTITION_FACTOR)`

### AC-2: Input Validation — Threshold Configuration

- **Given** a data engineer configures `spark.sql.insights.skew.factor` with a value less than 1.0 or a non-numeric value
- **When** the configuration is applied
- **Then** the system rejects the configuration with an error message stating the minimum allowed value is 1.0 and the expected data type is a decimal number

### AC-3: Expected Output — Skew Severity Classification

- **Given** the skew analysis has identified skewed partitions
- **When** the skew ratio for a partition is between 5x and 10x the median, it is classified as "medium" severity; when the skew ratio exceeds 10x the median, it is classified as "high" severity
- **Then** each skewed partition entry in the report includes its severity classification ("medium" or "high") alongside the numeric skew ratio

### AC-4: Error Handling — Missing Shuffle Metrics

- **Given** a query has completed but the shuffle stage metrics are unavailable due to executor loss or garbage collection during metric collection
- **When** the data skew analysis attempts to process the query
- **Then** the system returns a partial report listing analyzed stages with results and flags the unavailable stages with status "metrics_unavailable" and an explanation that executor metrics were lost

### AC-5: Edge Case — Uniform Distribution

- **Given** a completed SQL query where all shuffle partitions have data sizes within 2x of the median partition size
- **When** the data skew analysis runs
- **Then** the system returns a report indicating "no skew detected" for each shuffle stage, with the maximum observed skew ratio and the total number of partitions analyzed

### AC-6: Edge Case — Single Partition

- **Given** a shuffle stage that produces exactly one partition (e.g., a global aggregation)
- **When** the data skew analysis runs on this stage
- **Then** the system skips skew analysis for that stage and reports it as "not applicable — single partition" since median-based skew detection requires at least 2 partitions

## Sub-Tasks

- **Sub-task 1: Implement `SkewDetector` component** — Build the core detection component that reads shuffle partition sizes from `MapOutputStatistics` (the same data structure used by `OptimizeSkewedJoin.getSkewThreshold`) and computes median, skew factor, and skew threshold per shuffle stage
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/OptimizeSkewedJoin.scala` — reference for `getSkewThreshold` logic and `Utils.median()` usage (lines 64-67, 123-124)

- **Sub-task 2: Implement skew severity classification logic** — Classify detected skewed partitions into severity levels: "medium" (skew ratio between 5x and 10x median) and "high" (skew ratio exceeding 10x median), with configurable thresholds via `spark.sql.insights.skew.factor` and `spark.sql.insights.skew.thresholdInBytes`

- **Sub-task 3: Create `SkewAnalysisResult` data model** — Define a structured data model containing: stage ID, partition index, partition size in bytes, median size in bytes, skew ratio, severity classification, and shuffle operation type (e.g., SortMergeJoin, ShuffledHashJoin, HashAggregate)

- **Sub-task 4: Integrate skew detection into the query profiling pipeline** — Wire the `SkewDetector` into the query execution lifecycle so that results are computed after query completion and stored in `SQLAppStatusStore` alongside existing execution data
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala` — execution data persistence layer

- **Sub-task 5: Add REST API endpoint** — Expose skew analysis results via `/api/v1/applications/[app-id]/sql/[execution-id]/skew` returning a JSON array of `SkewAnalysisResult` objects

- **Sub-task 6: Add Spark Web UI display panel** — Render skew analysis results in the SQL execution detail page as a sortable table with columns: Stage ID, Partition Index, Size (bytes), Median Size (bytes), Skew Ratio, Severity, Operation
  - `Source: docs/web-ui.md` — existing SQL tab structure and metrics display patterns

- **Sub-task 7: Write unit tests for `SkewDetector`** — Create test cases covering: uniform data distribution (no skew), single skewed partition, multiple skewed partitions across stages, single partition stage (skipped), empty partition stage, missing metrics, and boundary threshold values

## Edge Cases

### Edge Case 1: Empty/Null Input — All Partitions Empty

A shuffle stage has zero bytes across all partitions (e.g., after filter pushdown eliminates all rows). The system reports "no data — all partitions empty" and does not attempt skew calculation to avoid division by zero when the median equals 0.

### Edge Case 2: Boundary Values — Partition Size Exactly at Threshold

A partition size is exactly equal to `skewedPartitionFactor * median` (i.e., at the threshold boundary). The system classifies this as "medium" severity since the condition uses greater-than-or-equal semantics, consistent with the `.max()` comparison logic in `OptimizeSkewedJoin.getSkewThreshold`.

> Source: `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/OptimizeSkewedJoin.scala` — `getSkewThreshold` uses `.max()` comparison (line 65)

### Edge Case 3: Invalid Input — Negative Threshold Configuration

A negative value is provided for `spark.sql.insights.skew.thresholdInBytes`. The system rejects the configuration with an explicit error message specifying the minimum allowed value is 0 bytes.

### Edge Case 4: Large Partition Count — More Than 10,000 Partitions

A shuffle stage has more than 10,000 partitions. The system computes the median using sampling (every Nth partition) rather than sorting all partition sizes, to maintain sub-second analysis time for high-partition-count stages.

> Source: `sql/core/src/main/scala/org/apache/spark/sql/execution/ShuffledRowRDD.scala` — partition spec structures (`CoalescedPartitionSpec`, `PartialReducerPartitionSpec`) define how shuffle partitions are represented

### Edge Case 5: Concurrent Query Execution — Isolation per Execution ID

Multiple queries execute simultaneously with shuffle stages. The skew detection isolates analysis per execution ID and does not cross-contaminate results between concurrent queries. Each execution ID maps to its own independent set of `SkewAnalysisResult` entries.

## Dependencies

- **FEATURE-001-01 — Query Execution Profiling**: Provides execution plan and stage metrics data that skew detection analyzes as its primary input
  - `Source: tickets/EPIC-001/FEATURE-001-01-query-execution-profiling.md`
- **FEATURE-001-02 — Automated Optimization Recommendations**: Parent feature encompassing this story and four sibling stories for join strategy, partition optimization, cache identification, and summary reporting
  - `Source: tickets/EPIC-001/FEATURE-001-02-automated-optimization-recommendations.md`
- **EPIC-001 — Adaptive Query Performance Insights Engine**: Parent epic defining the overarching goal of reducing manual SQL optimization effort through automated profiling and recommendations
  - `Source: tickets/EPIC-001-adaptive-query-performance-insights.md`
- **F-002 — SQL Query Processing Engine**: Catalyst optimizer and AQE framework providing shuffle stage statistics, query plan structures, and runtime re-optimization infrastructure
  - `Source: sql/catalyst/` — query plan classes and optimizer rules
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/OptimizeSkewedJoin.scala` — reference implementation for skew threshold calculation (uses `SKEW_JOIN_SKEWED_PARTITION_FACTOR` default 5, `SKEW_JOIN_SKEWED_PARTITION_THRESHOLD` default 256MB)
- **Shuffle Metrics Infrastructure**: Partition-level shuffle read metrics providing the raw data (bytes per partition) for skew analysis
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ShuffledRowRDD.scala` — `ShufflePartitionSpec` definitions and partition-level data structures
- **SQLAppStatusStore**: Execution data persistence layer used to store and retrieve skew analysis results alongside query execution data
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala`
- **Spark Web UI SQL Tab**: Display surface for rendering skew analysis results within the SQL execution detail page
  - `Source: docs/web-ui.md`
- **Spark REST API**: Programmatic access layer for exposing skew analysis results to external tooling and automation
- **Performance Tuning Configuration**: Existing AQE skew join configuration properties that inform default threshold values
  - `Source: docs/sql-performance-tuning.md` — AQE configuration for `spark.sql.adaptive.skewJoin.*` properties

## Story Estimation Guidance

- **Story Points**: 5 (Fibonacci scale)
- **Justification**: Moderate complexity — the core skew detection algorithm mirrors the existing `OptimizeSkewedJoin` logic (threshold computation via `getSkewThreshold`, median calculation via `Utils.median()`, partition size comparison from `MapOutputStatistics.bytesByPartitionId`) but requires new data models (`SkewAnalysisResult`), a REST API endpoint, Web UI integration, and comprehensive test coverage for edge cases. The algorithm logic is well-understood from the existing AQE implementation, reducing uncertainty, but integration with the profiling pipeline and UI adds implementation effort. Fits within a single sprint for one experienced engineer.
- **Complexity Factors**:
  - Algorithm: Low — reuses established skew detection logic from `OptimizeSkewedJoin.scala`
  - Data model: Low — straightforward `SkewAnalysisResult` structure
  - API integration: Medium — new REST endpoint with JSON serialization
  - UI integration: Medium — new sortable table panel in SQL execution detail page
  - Testing: Medium — 7+ test scenarios including edge cases and boundary conditions

## Definition of Done

- Skew detection identifies partitions exceeding the configured skew factor (default 5x) relative to the median partition size AND exceeding the absolute threshold (default 256MB)
- Skew severity is classified as "medium" (5x-10x) or "high" (>10x) for each identified partition
- Results are persisted in `SQLAppStatusStore` and accessible via REST API endpoint `/api/v1/applications/[app-id]/sql/[execution-id]/skew`
- Spark Web UI SQL execution detail page displays skew analysis in a sortable table with columns: Stage ID, Partition Index, Size (bytes), Median Size (bytes), Skew Ratio, Severity, Operation
- Configuration parameters (`spark.sql.insights.skew.factor`, `spark.sql.insights.skew.thresholdInBytes`) are validated on application with clear error messages for invalid values
- Unit tests cover: uniform distribution (no skew), single skewed partition, multiple skewed partitions across stages, single partition stage (skipped), empty partition stage, missing metrics, boundary threshold values
- Integration test validates end-to-end skew detection for a query with a known skewed join (10x imbalance across partitions)
- No forbidden terms appear in any acceptance criteria text
- Story is demo-able: a data engineer can run a skewed query, navigate to the SQL execution page, and view the skew analysis report with severity classifications
