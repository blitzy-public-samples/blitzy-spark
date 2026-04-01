# Display Executor CPU and Memory Utilization Metrics per Query in the Spark Web UI

## User Story

**As a** platform engineer,
**I want to** view executor-level CPU utilization percentage and memory usage — including JVM heap memory, JVM off-heap memory, on-heap execution memory, off-heap execution memory, on-heap storage memory, off-heap storage memory, direct pool memory, and mapped pool memory — for each SQL query execution in the Spark Web UI,
**so that** I can identify resource-intensive queries that consume disproportionate cluster resources, enabling targeted capacity planning and query-level resource governance that reduces overall cluster cost by up to 25% through right-sizing executor allocations.

## Acceptance Criteria

### AC-1: Per-Executor CPU Utilization Display (Expected Output)

- **Given** a completed SQL query that ran across 4 executors
- **When** the platform engineer navigates to the SQL Execution Detail page for that query
- **Then** the page displays a "Resource Utilization" panel showing per-executor CPU utilization as a percentage value between 0% and 100%, calculated as `(executorCpuTime / executorRunTime) * 100` from task-level `TaskMetrics`, with one row displayed for each executor that participated in the query

### AC-2: Per-Executor Memory Metrics Display (Expected Output)

- **Given** a completed SQL query
- **When** the platform engineer views the "Resource Utilization" panel on the SQL Execution Detail page
- **Then** the panel displays per-executor memory metrics in a table with columns for each of the following `ExecutorMetricType` values: JVM heap memory used (bytes), JVM off-heap memory used (bytes), on-heap execution memory (bytes), off-heap execution memory (bytes), on-heap storage memory (bytes), off-heap storage memory (bytes), direct pool memory (bytes), and mapped pool memory (bytes), with each value rendered in human-readable format (e.g., "1.5 GiB") in the Web UI and available as exact byte values through the REST API

`Source: core/src/main/scala/org/apache/spark/metrics/ExecutorMetricType.scala` — defines JVMHeapMemory, JVMOffHeapMemory, OnHeapExecutionMemory, OffHeapExecutionMemory, OnHeapStorageMemory, OffHeapStorageMemory, DirectPoolMemory, MappedPoolMemory

### AC-3: REST API Input Validation (Input Validation)

- **Given** a request to the REST API endpoint `GET /api/v1/applications/[app-id]/sql/[execution-id]/resources`
- **When** the `app-id` parameter contains a non-existent application identifier
- **Then** the API returns an HTTP 404 response with a JSON body containing `{"error": "Application [app-id] not found"}` and does not expose internal stack traces or implementation details in the response body

### AC-4: Lost Executor Handling (Error Handling)

- **Given** a query that completed but one or more executors were lost (decommissioned or crashed) during execution
- **When** the platform engineer views the "Resource Utilization" panel for that query
- **Then** the panel displays metric data for all executors that reported metrics before being lost, marks each lost executor with a "LOST" label in the status column, and shows the last known metric values alongside a timestamp indicating when metrics were last received from that executor

### AC-5: Disabled Metrics Collection (Edge Case)

- **Given** a query executed via Spark Connect where executor metrics collection is disabled (configuration `spark.executor.processTreeMetrics.enabled` set to `false`)
- **When** the platform engineer views the "Resource Utilization" panel
- **Then** the panel displays the message "Executor resource metrics are not available for this query" with a hyperlink to the Spark monitoring configuration documentation for enabling metrics collection, and does not render empty or zero-valued metric tables

### AC-6: Aggregate Summary Row (Expected Output)

- **Given** a query that executed across 10 executors
- **When** the platform engineer views the "Resource Utilization" panel
- **Then** the panel includes a summary row at the bottom of the executor metrics table showing: total CPU time across all executors in milliseconds, peak JVM heap memory across all executors in bytes, total shuffle read in bytes, total shuffle write in bytes, and peak on-heap execution memory across all executors in bytes

## Sub-Tasks

- [ ] **ST-1**: Extend the SQL Execution Detail page (`sql/core/src/main/scala/org/apache/spark/sql/execution/ui/ExecutionPage.scala`) with a new "Resource Utilization" collapsible panel section below the existing execution plan visualization
- [ ] **ST-2**: Create a per-query executor metric aggregation service that correlates executor heartbeat metrics with SQL execution IDs using the existing `SQLAppStatusListener` stage-to-execution mapping and `LiveStageMetrics` task metric tracking
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusListener.scala` — `onExecutorMetricsUpdate`, `onTaskEnd`, stage-to-execution ID correlation
- [ ] **ST-3**: Map executor metrics from the `ExecutorMetricType` registry to per-query resource consumption, extracting values for: `JVMHeapMemory`, `JVMOffHeapMemory`, `OnHeapExecutionMemory`, `OffHeapExecutionMemory`, `OnHeapStorageMemory`, `OffHeapStorageMemory`, `DirectPoolMemory`, `MappedPoolMemory`
  - `Source: core/src/main/scala/org/apache/spark/metrics/ExecutorMetricType.scala` — `metricGetters` indexed sequence with offset mapping via `metricToOffset`
- [ ] **ST-4**: Calculate CPU utilization percentage per executor using the formula `(executorCpuTime / executorRunTime) * 100` derived from task-level `TaskMetrics` aggregated per executor, handling division-by-zero when `executorRunTime` is zero
- [ ] **ST-5**: Add a new REST API endpoint `GET /api/v1/applications/[app-id]/sql/[execution-id]/resources` returning a JSON response containing a `perExecutor` array (each entry with executor ID, CPU utilization percentage, and all 8 memory metric byte values) and an `aggregateSummary` object (total CPU time, peak heap, total shuffle read/write, peak execution memory)
  - `Source: docs/monitoring.md` — existing REST API endpoint patterns under `/api/v1/applications/[app-id]`
- [ ] **ST-6**: Implement handling for lost or removed executors: persist last-known metric values in the `SQLAppStatusStore` KVStore with a "LOST" status annotation and the timestamp of the last received heartbeat
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala` — KVStore-backed persistence layer
- [ ] **ST-7**: Render memory metrics in human-readable format in the Web UI (e.g., "1.5 GiB" for 1,610,612,736 bytes) while preserving exact byte values in all JSON API responses
- [ ] **ST-8**: Add Prometheus-compatible metric exports for per-query resource utilization using the existing `PrometheusServlet` sink pattern registered through the `MetricsSystem`
  - `Source: core/src/main/scala/org/apache/spark/metrics/MetricsSystem.scala` — `prometheusServlet` registration, `registerSource`, `buildRegistryName`
- [ ] **ST-9**: Write unit tests covering CPU utilization calculation edge cases: zero `executorRunTime` (return 0% instead of NaN/Infinity), negative metric values (clamp to zero), single-task executor, and multi-task executor aggregation
- [ ] **ST-10**: Write integration tests for the REST API endpoint validating: successful response with mock executor metric data, HTTP 404 for non-existent application ID, HTTP 400 for negative execution ID, and correct aggregate summary computation

## Edge Cases

### EC-1: Driver-Only Query Execution (Empty/Null Input)

When a query completes entirely on the driver with zero executor participation (e.g., `SHOW DATABASES`, `DESCRIBE TABLE`, DDL commands such as `CREATE TABLE` or `DROP VIEW`), the "Resource Utilization" panel must display the message "This query executed on the driver only — no executor resource metrics available" and render driver-side JVM metrics (heap memory, off-heap memory) from the driver's `MemoryMXBean` instead of executor metrics. The panel must not display an empty executor metrics table or throw a rendering error.

### EC-2: High Memory Usage at Allocation Limit (Boundary Values)

When an executor's JVM heap memory usage reaches or exceeds 90% of the configured maximum (`spark.executor.memory`), the memory utilization cell in the Web UI table must display with a visual warning indicator — a red background color (#FF4444) for usage at or above 90% and an amber background color (#FFA500) for usage between 75% and 89%. At 100% allocation (executor at the `spark.executor.memory` limit), the percentage display must show "100%" and not exceed that value even if transient GC overhead causes momentary spikes in reported memory.

### EC-3: Negative or Invalid Execution ID in REST API (Invalid Input)

A REST API request targeting a negative execution ID (e.g., `GET /api/v1/applications/app-123/sql/-1/resources`) must return an HTTP 400 response with the JSON body `{"error": "Execution ID must be a non-negative integer"}`. The API handler must validate the execution ID parameter before performing any KVStore lookup, and must not log stack traces at ERROR level for client input validation failures — only at DEBUG level.

### EC-4: Large Executor Count with Pagination (Boundary Values)

When a query runs across 1,000 or more executors, the "Resource Utilization" panel must render the per-executor metrics table with server-side pagination at 50 executors per page. The aggregate summary row must remain visible on every page and must compute across all executors (not just the current page). The aggregate computation must complete within 2 seconds for up to 1,000 executors. The pagination controls must display total executor count, current page number, and total page count.

### EC-5: Null or Zero Metric Values from Metric Collection Failure (Null Metrics)

When an executor reports null or zero values for all 8 memory metric types simultaneously (indicating a metric collection infrastructure failure rather than genuinely zero usage), the system must display "N/A" for each affected metric cell instead of "0 bytes". The system must log a single WARNING-level message per affected executor in the format `"Executor [executor-id]: all memory metrics returned zero/null — metric collection may have failed"` and must not repeat the warning for subsequent queries on the same executor within a 5-minute window.

## Dependencies

- **EPIC-001** — [Adaptive Query Performance Insights Engine](../../EPIC-001-adaptive-query-performance-insights.md): Parent epic defining the overall scope of the performance insights initiative
- **FEATURE-001-01** — [Query Execution Profiling](../FEATURE-001-01-query-execution-profiling.md): Parent feature encompassing plan capture, timeline visualization, resource metrics display, report export, and plan comparison
- **F-002 — SQL Query Processing Engine**: Provides the `QueryExecution` lifecycle, physical plan operators, and runtime statistics that establish the query execution context for resource metric correlation
- **`core/src/main/scala/org/apache/spark/metrics/ExecutorMetricType.scala`**: Defines the canonical executor metric type registry containing `JVMHeapMemory`, `JVMOffHeapMemory`, `OnHeapExecutionMemory`, `OffHeapExecutionMemory`, `OnHeapStorageMemory`, `OffHeapStorageMemory`, `DirectPoolMemory`, `MappedPoolMemory`, `OnHeapUnifiedMemory`, `OffHeapUnifiedMemory`, `ProcessTreeMetrics` (6 sub-metrics), and `GarbageCollectionMetrics` (7 sub-metrics) — with `metricGetters` providing indexed access and `metricToOffset` mapping metric names to array positions
- **`core/src/main/scala/org/apache/spark/metrics/MetricsSystem.scala`**: Provides the Dropwizard-based metrics system with `MetricRegistry` management, `Source`/`Sink` lifecycle, `PrometheusServlet` and `MetricsServlet` integration, and instance-based metric naming (`driver`, `executor`, `master`, `worker`) via `buildRegistryName`
- **`sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusListener.scala`**: SQL execution event listener that maps stages to execution IDs, processes `SparkListenerExecutorMetricsUpdate` and `SparkListenerTaskEnd` events, tracks live stage metrics via `LiveStageMetrics`, and aggregates per-task accumulator values into per-execution metrics
- **`sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala`**: KVStore access layer providing `execution()`, `executionsList()`, `executionMetrics()`, and `planGraph()` methods for reading persisted `SQLExecutionUIData` records that contain job status, stage sets, and metric values
- **`sql/core/src/main/scala/org/apache/spark/sql/execution/ui/ExecutionPage.scala`**: Current SQL Execution Detail page to be extended with the new "Resource Utilization" panel
- **Spark REST API (`/api/v1`)**: Existing REST API infrastructure documented in `docs/monitoring.md` with endpoints for applications, jobs, stages, and executors — to be extended with the `/sql/[execution-id]/resources` endpoint
- **`docs/web-ui.md`**: Documents the Executors tab (memory/disk/cores, storage memory, GC metrics) and Stages tab (task-level metrics, shuffle statistics) used as reference for resource metric display patterns
- **`docs/monitoring.md`**: Documents REST API endpoint structure, metrics sink configurations (ConsoleSink, CSVSink, JmxSink, PrometheusServlet), and executor metric types

## Story Estimation Guidance

**Story Points: 8** (Fibonacci scale)

| Factor | Complexity | Notes |
|--------|-----------|-------|
| Cross-cutting integration | High | Correlating executor-level heartbeat metrics with per-query SQL execution IDs requires bridging the metrics subsystem (`core/`) with the SQL execution tracking layer (`sql/core/`) through the `SQLAppStatusListener` stage-to-execution mapping |
| New REST API endpoint | Medium | Adding `GET /api/v1/applications/[app-id]/sql/[execution-id]/resources` with input validation, JSON serialization, and error handling follows established REST API patterns but introduces new response schemas |
| Web UI panel development | Medium | Extending `ExecutionPage.scala` with a collapsible "Resource Utilization" panel, per-executor table rendering, human-readable formatting, and color-coded threshold indicators |
| Lost executor handling | Medium | Persisting last-known metrics with "LOST" status annotation and timestamp requires state management for executor lifecycle events |
| Aggregate metric computation | Low-Medium | Computing totals and peaks across executors is straightforward but must handle edge cases (zero executors, null metrics, very large executor counts) |
| Testing | Medium | Unit tests for CPU calculation edge cases and integration tests for REST API responses and Web UI rendering |

**Estimated development effort**: 5–8 developer days, accounting for cross-module integration complexity, REST API contract definition, UI rendering, edge case handling, and comprehensive test coverage.

## Definition of Done

- [ ] Per-executor CPU utilization percentage (0%–100%) is displayed for each completed SQL query on the SQL Execution Detail page in the Spark Web UI
- [ ] Per-executor memory metrics for all 8 `ExecutorMetricType` values (JVMHeapMemory, JVMOffHeapMemory, OnHeapExecutionMemory, OffHeapExecutionMemory, OnHeapStorageMemory, OffHeapStorageMemory, DirectPoolMemory, MappedPoolMemory) are displayed in a tabular format on the "Resource Utilization" panel
- [ ] An aggregate summary row shows total CPU time (ms), peak JVM heap memory (bytes), total shuffle read (bytes), total shuffle write (bytes), and peak on-heap execution memory (bytes) across all participating executors
- [ ] Lost executors are displayed with a "LOST" label in the status column, showing last-known metric values and the timestamp of the last received heartbeat
- [ ] Queries with no executor metrics (driver-only execution or disabled metrics collection) display an informative message instead of empty or zero-valued tables
- [ ] REST API endpoint `GET /api/v1/applications/[app-id]/sql/[execution-id]/resources` returns per-query resource utilization as JSON with per-executor detail arrays and aggregate summary objects
- [ ] REST API validates input parameters and returns structured error responses: HTTP 404 for non-existent application ID, HTTP 400 for negative or non-integer execution ID
- [ ] Memory values are rendered in human-readable format (e.g., "1.5 GiB") in the Web UI and as exact byte values in all API JSON responses
- [ ] Unit tests cover CPU utilization calculation edge cases: zero `executorRunTime`, negative values, single-task and multi-task executor aggregation
- [ ] Integration tests validate REST API endpoint responses with mock executor metric data, including success, 404, and 400 scenarios
- [ ] Resource utilization panel loads and renders within 2 seconds for queries that executed across up to 500 executors
- [ ] Code review approved by at least 2 Apache Spark committers
- [ ] Documentation updated to cover "Resource Utilization" panel navigation in the Web UI guide and REST API endpoint usage in the monitoring guide
