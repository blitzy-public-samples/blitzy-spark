# Track Data Quality Trends Across Pipeline Executions to Detect Gradual Data Degradation Over Time

## User Story

As a **data engineer**, I want to track and query historical quality metric trends across pipeline executions, persisted in the AppStatusStore using the KVStore backend, so that I can identify gradual data degradation patterns — such as a column's non-null ratio declining from 0.98 to 0.85 over 20 pipeline runs — before they breach alerting thresholds, reducing unplanned pipeline failures by up to 30%.

## Acceptance Criteria

- **AC-1 (Input Validation):** Given a pipeline has executed quality validation on a DataFrame named "orders", When the pipeline completes, Then the quality metrics (non-null ratio, distinct count, null count per column, overall quality score, timestamp, and pipeline run identifier) are persisted as a `QualityTrendEntry` entity in the AppStatusStore's KVStore with a composite index on (dataframe_name, timestamp)
- **AC-2 (Expected Output):** Given 15 pipeline executions have been completed for DataFrame "orders", When I query the quality trend API with `getQualityTrend("orders", limit=10)`, Then the API returns the 10 most recent `QualityTrendEntry` records ordered by timestamp descending, each containing the full per-column quality metrics snapshot for that execution
- **AC-3 (Error Handling):** Given a DataFrame name that has never been validated (e.g., "nonexistent_df"), When I query the quality trend API with `getQualityTrend("nonexistent_df")`, Then the API returns an empty sequence with zero entries and does not throw an exception
- **AC-4 (Retention Policy):** Given `spark.quality.history.retainedRuns` is configured to 100 (default), When a 101st quality validation result is persisted for the same DataFrame, Then the oldest entry is removed by the ElementTrackingStore trigger mechanism, maintaining exactly 100 entries for that DataFrame
- **AC-5 (Edge Case):** Given a pipeline writes quality metrics for 3 distinct DataFrames ("orders", "customers", "products") across 50 executions each, When I query `listTrackedDataFrames()`, Then the API returns exactly 3 distinct DataFrame names and each DataFrame retains its independent trend history limited by the per-DataFrame retention configuration
- **AC-6 (Time Range Query):** Given 30 quality entries exist for DataFrame "orders" spanning a 30-day period, When I query `getQualityTrend("orders", startTime=dayMinus7, endTime=now)`, Then only entries with timestamps within the specified 7-day window are returned, ordered by timestamp descending

## Sub-Tasks

- Define `QualityTrendEntry` wrapper class in `storeTypes.scala` pattern with KVIndex annotations: primary index on unique `trendId` (composite of dataframeName + timestamp), secondary indices on `dataframeName` and `timestamp`, following the wrapper class patterns in `core/src/main/scala/org/apache/spark/status/storeTypes.scala`
- Define `QualityTrendEntry` fields: trendId (string), dataframeName (string), timestamp (long — epoch millis), pipelineRunId (string), overallQualityScore (double), columnMetrics (serialized JSON or nested structure with per-column nonNullRatio, nullCount, distinctCount)
- Implement `QualityStatusListener` extending `SparkListener` that captures quality validation completion events, constructs `QualityTrendEntry` objects, and writes them to the `ElementTrackingStore`, following the listener pattern in `core/src/main/scala/org/apache/spark/status/AppStatusListener.scala`
- Register a retention trigger on the `ElementTrackingStore` for `QualityTrendEntry` class using `addTrigger` with threshold derived from `spark.quality.history.retainedRuns` (default 100), following the trigger-based cleanup pattern in `core/src/main/scala/org/apache/spark/status/ElementTrackingStore.scala`
- Implement `QualityTrendStore` read-side wrapper class providing `getQualityTrend(dataframeName, limit)`, `getQualityTrend(dataframeName, startTime, endTime)`, and `listTrackedDataFrames()` methods using KVStore view/index queries, following the `AppStatusStore` pattern in `core/src/main/scala/org/apache/spark/status/AppStatusStore.scala`
- Add configuration parameter `spark.quality.history.retainedRuns` with default value 100 and validation (minimum 1, maximum 10000)
- Write unit tests for `QualityTrendEntry` persistence, retrieval, retention trigger, time-range queries, and edge cases
- Write integration test validating end-to-end trend tracking across 20 simulated pipeline executions

## Edge Cases

- **Edge Case 1 (Empty/Null Input):** No quality validation has ever been performed — `listTrackedDataFrames()` returns an empty sequence and `getQualityTrend()` for any DataFrame name returns an empty sequence
- **Edge Case 2 (Boundary Values):** Retention limit is set to 1 (`spark.quality.history.retainedRuns=1`) — only the most recent quality entry is retained per DataFrame; querying returns exactly 1 entry
- **Edge Case 3 (Invalid Input):** `spark.quality.history.retainedRuns` is set to 0 or a negative number — the system logs a warning and uses the default value of 100
- **Edge Case 4 (Concurrent Writes):** Two pipeline threads write quality metrics for the same DataFrame simultaneously — the ElementTrackingStore's synchronized write mechanism ensures both entries are persisted without data corruption, each with distinct timestamps
- **Edge Case 5 (KVStore Backend Failure):** The underlying KVStore (LevelDB or RocksDB) encounters a disk I/O error during write — the system catches the exception, logs an error with the DataFrame name and timestamp, and does not crash the Spark application

## Dependencies

- **STORY-002-02-01** (Compute Completeness Metrics) — provides the completeness metric values (non-null ratio, distinct count, null count) stored in trend entries
- **FEATURE-002-01** (DataFrame Quality Rules Engine) — provides quality validation events consumed by the QualityStatusListener
- **FEATURE-002-02** (Quality Metrics and Reporting) — parent feature defining the overall metrics and reporting scope
- **EPIC-002** (Declarative Data Quality Validation Framework) — parent epic governing the data quality initiative
- **AppStatusStore pattern:** `core/src/main/scala/org/apache/spark/status/AppStatusStore.scala` — read-side KVStore wrapper providing programmatic accessors for persisted data using KVStore views and indices
- **AppStatusListener pattern:** `core/src/main/scala/org/apache/spark/status/AppStatusListener.scala` — SparkListener implementation that consumes events and persists state into ElementTrackingStore with configurable retention
- **Store types pattern:** `core/src/main/scala/org/apache/spark/status/storeTypes.scala` — KVIndex-annotated wrapper classes for KVStore serialization with compact index keys and Jackson annotations
- **ElementTrackingStore:** `core/src/main/scala/org/apache/spark/status/ElementTrackingStore.scala` — KVStore wrapper with addTrigger for count-based retention, onFlush callbacks, doAsync execution, synchronized write with trigger evaluation
- **KVStore utilities:** `core/src/main/scala/org/apache/spark/status/KVUtils.scala` — KVStore creation, serializer selection, metadata validation, and recovery utilities

## Story Estimation Guidance

- **Story Points:** 8 (Fibonacci)
- **Rationale:** High complexity — requires defining new KVStore entity classes with proper index annotations, implementing a SparkListener for event capture, building a read-side store wrapper with multiple query methods, configuring retention triggers, and validating concurrency behavior. The KVStore and listener patterns are established but require careful index design for composite queries. Estimated 5-7 days of development effort for one engineer.
- **Risk Factors:** KVStore index design for composite (dataframeName + timestamp) queries requires performance validation with large entry counts; retention trigger timing may interact with concurrent writes; serialization of per-column metrics within the KVStore entry requires careful schema design for forward compatibility

## Definition of Done

- Quality metrics are persisted as `QualityTrendEntry` entities in the KVStore after each quality validation execution
- `getQualityTrend(dataframeName, limit)` returns the N most recent entries ordered by timestamp descending
- `getQualityTrend(dataframeName, startTime, endTime)` returns entries within the specified time window
- `listTrackedDataFrames()` returns distinct DataFrame names with stored quality trends
- Retention trigger removes oldest entries when count exceeds `spark.quality.history.retainedRuns` (default 100)
- Concurrent writes produce distinct entries without data corruption
- Unit tests cover persistence, retrieval, retention, time-range queries, empty state, and boundary conditions
- Integration test validates 20+ pipeline executions with trend retrieval and retention enforcement
- API documentation with Scaladoc describes query methods, retention configuration, and usage patterns
- Code review completed and merged to feature branch
- No forbidden terms used in any acceptance criteria or documentation
