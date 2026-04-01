# Export Query Profiling Data in JSON and CSV Formats for External Analysis

## User Story

**As a** data scientist,
**I want to** export query profiling data — including execution plans, stage metrics, operator-level metrics, and resource utilization — in both JSON (structured, machine-readable) and CSV (tabular, spreadsheet-compatible) formats,
**So that** I can import profiling data into external analysis tools (Jupyter notebooks, pandas DataFrames, Excel, custom dashboards) to perform statistical analysis on query performance patterns, identify recurring bottlenecks across hundreds of queries, and produce performance trend reports that reduce the time spent on manual performance analysis by up to 50%.

## Acceptance Criteria

**AC-1: JSON Export Returns Complete Profiling Data (Expected Output)**

- **Given** a completed SQL query with execution ID 42 persisted in the `SQLAppStatusStore` KVStore
- **When** the data scientist sends a GET request to `/api/v1/applications/[app-id]/sql/42/profile?format=json`
- **Then** the API returns an HTTP 200 response with Content-Type `application/json` containing a JSON object with the following fields:
  - `executionId` (long) — the unique SQL execution identifier
  - `description` (string) — the SQL query text or description from `SQLExecutionUIData.description`
  - `physicalPlanDescription` (string) — the full physical plan text from `SQLExecutionUIData.physicalPlanDescription`
  - `submissionTime` (string, ISO 8601 timestamp) — the query submission time
  - `completionTime` (string, ISO 8601 timestamp) — the query completion time
  - `duration` (long, milliseconds) — the elapsed time between submission and completion
  - `status` (string, one of: `RUNNING`, `COMPLETED`, `FAILED`) — derived from `SQLExecutionUIData.executionStatus`
  - `stages` (array) — an array of stage objects, each containing `stageId` (int), `stageAttemptId` (int), `duration` (long, milliseconds), `inputRows` (long), `outputRows` (long), `shuffleReadBytes` (long), and `shuffleWriteBytes` (long)
  - `planMetrics` (array) — an array of `SQLPlanMetric` objects, each containing `name` (string), `accumulatorId` (long), and `metricType` (string)
  - `executorResources` (array) — an array of per-executor objects, each containing `executorId` (string), `cpuPercentage` (double), and `memoryBytesUsed` (long)

> `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala` — `SQLExecutionUIData`, `SQLPlanMetric`, `SparkPlanGraphWrapper`

**AC-2: CSV Export Returns Tabular Operator-Level Metrics (Expected Output)**

- **Given** a completed SQL query with execution ID 42 persisted in the `SQLAppStatusStore` KVStore
- **When** the data scientist sends a GET request to `/api/v1/applications/[app-id]/sql/42/profile?format=csv`
- **Then** the API returns an HTTP 200 response with Content-Type `text/csv` and a Content-Disposition header set to `attachment; filename="query-42-profile.csv"`, containing a CSV file with one header row and one data row per physical operator in the execution plan, with the following columns:
  - `operatorId` — the unique node ID from `SparkPlanGraphWrapper.nodes`
  - `operatorName` — the operator name (e.g., `HashAggregate`, `SortMergeJoin`, `Exchange`)
  - `outputRows` — the number of rows produced by this operator
  - `outputDataSize` — the output data size in bytes
  - `executionTimeMs` — the operator execution time in milliseconds
  - `peakMemoryBytes` — peak memory consumed by this operator
  - `shuffleReadBytes` — bytes read during shuffle (0 if not a shuffle operator)
  - `shuffleWriteBytes` — bytes written during shuffle (0 if not a shuffle operator)
  - `spillMemoryBytes` — in-memory spill size in bytes
  - `spillDiskBytes` — on-disk spill size in bytes

> `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala` — `SparkPlanGraphWrapper`, `SparkPlanGraphNodeWrapper`

**AC-3: Unsupported Format Parameter Returns HTTP 400 (Input Validation)**

- **Given** a profiling export request with an unsupported format parameter value (e.g., `?format=xml` or `?format=pdf`)
- **When** the API processes the request
- **Then** the API returns an HTTP 400 response with a JSON body containing `{"error": "Unsupported export format 'xml'. Supported formats: json, csv"}` and does not generate any export file or stream any partial data to the client

**AC-4: Running Query Export Returns Partial Data with Status Flag (Error Handling)**

- **Given** a profiling export request for a query whose `SQLExecutionUIData.completionTime` is `None` (still in RUNNING status)
- **When** the data scientist requests the export via GET to `/api/v1/applications/[app-id]/sql/[execution-id]/profile?format=json`
- **Then** the API returns an HTTP 200 response with partial profiling data containing all metrics collected up to the current point (retrieved from the live `SQLAppStatusListener` merged with persisted data from `SQLAppStatusStore`), includes a `"status": "RUNNING"` field in the JSON response, and includes a `"partial": true` flag indicating the data is incomplete

> `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusListener.scala` — `liveExecutionMetrics()` method for in-flight metric retrieval

**AC-5: Bulk Export Handles 500+ Queries Using Streaming Output (Edge Case)**

- **Given** a SparkSession with 500 completed queries stored in the `SQLAppStatusStore` KVStore
- **When** the data scientist sends a GET request to `/api/v1/applications/[app-id]/sql/profile?format=json` (bulk export without a specific execution ID)
- **Then** the API returns profiling data for all 500 queries within 10 seconds, the response uses streaming JSON output (JSON Lines format or a JSON array emitted incrementally via Jackson `JsonGenerator`) to avoid loading all query data into memory simultaneously, and the response includes a `Content-Length` header or uses chunked transfer encoding for responses exceeding 10 MB

**AC-6: Exported Metrics Match All Physical Operators in Plan Tree (Expected Output)**

- **Given** a query that included join operators, aggregation operators, and window function operators across 3 stages
- **When** the data scientist exports the profiling report in JSON format via the `/api/v1/applications/[app-id]/sql/[execution-id]/profile?format=json` endpoint
- **Then** the exported data includes operator-level metrics for every physical operator in the plan tree — including join operators with rows output and hash/sort metrics, aggregate operators with number of groups, and window operators with window frame size — and the total number of metric entries in the `planMetrics` array matches the number of `SQLPlanMetric` records in the `SparkPlanGraphWrapper` for that execution ID

> `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala` — `planGraph()` method returning `SparkPlanGraph` from `SparkPlanGraphWrapper`

## Sub-Tasks

1. **Design the profiling report data model** — Define a unified export schema that consolidates execution metadata (`executionId`, `description`, `submissionTime`, `completionTime`, `duration`, `status`), physical plan information (`physicalPlanDescription`, `planMetrics`), stage-level metrics (`stages` array with timing and I/O counters), operator-level metrics (per-node metrics from `SparkPlanGraphWrapper`), and executor resource utilization (`executorResources` array with CPU and memory) into a single serializable data structure.

2. **Implement JSON serializer using Jackson (version 2.20.0)** — Create a JSON serialization module that uses the Jackson `ObjectMapper` (already a Spark dependency at version 2.20.0 per `pom.xml`) to produce the full profiling report JSON output with all fields specified in AC-1, including nested stage objects and plan metric arrays.

3. **Implement CSV serializer for operator-level metrics** — Create a CSV serialization module that flattens the operator-level metrics from the `SparkPlanGraph` node list into a tabular format with the columns specified in AC-2 (`operatorId`, `operatorName`, `outputRows`, `outputDataSize`, `executionTimeMs`, `peakMemoryBytes`, `shuffleReadBytes`, `shuffleWriteBytes`, `spillMemoryBytes`, `spillDiskBytes`), using proper CSV escaping for operator names containing commas or quotes.

4. **Register new REST API endpoints** — Add two new endpoints to the Spark REST API infrastructure (following the patterns in `docs/monitoring.md`):
   - `GET /api/v1/applications/[app-id]/sql/[execution-id]/profile?format={json|csv}` for single-query export
   - `GET /api/v1/applications/[app-id]/sql/profile?format={json|csv}` for bulk export of all queries

5. **Add Content-Type and Content-Disposition headers for CSV downloads** — Set `Content-Type: text/csv; charset=UTF-8` and `Content-Disposition: attachment; filename="query-[execution-id]-profile.csv"` headers on CSV export responses to trigger browser download behavior.

6. **Implement streaming JSON output for bulk exports** — Use Jackson's `JsonGenerator` with streaming write mode to emit JSON array elements incrementally for bulk export requests, preventing memory exhaustion when exporting 500+ queries.

7. **Add "Download Report" button to the SQL Execution Detail page in the Web UI** — Add a dropdown button to the execution detail page (extending `ExecutionPage.scala`) that offers JSON and CSV format selection and triggers the corresponding export endpoint URL.

8. **Handle partial data export for running queries** — Implement logic that reads current metrics from the live `SQLAppStatusListener.liveExecutionMetrics()` and merges them with persisted data from `SQLAppStatusStore.executionMetrics()` to produce a partial profiling report with `"partial": true` flag for in-flight queries.

9. **Add rate limiting for bulk export requests** — Implement a concurrency limit of 1 concurrent bulk export per application using an `AtomicBoolean` guard to prevent resource exhaustion from simultaneous bulk export requests.

10. **Write unit tests for JSON and CSV serializers** — Create unit tests covering JSON and CSV serialization for sample profiling data, including simple single-stage queries, complex multi-stage queries with joins and aggregations, queries with zero metrics, and queries containing Unicode characters in descriptions.

11. **Write integration tests for export endpoints** — Create integration tests that validate REST API response status codes, Content-Type headers, Content-Disposition headers, JSON schema structure, CSV column layout, and data completeness against known test query executions.

## Edge Cases

1. **Empty/Null Input — Query with No Operator Metrics**: Exporting a profiling report for a query that has no operator metrics (e.g., a `SHOW TABLES` command or a `SET` command with zero `SQLPlanMetric` entries in the `SparkPlanGraphWrapper`) must return a valid JSON response with an empty `planMetrics` array (`"planMetrics": []`) and an empty `stages` array, or a valid CSV response containing only the header row with all 10 column names and zero data rows, rather than returning an error or a null response body.

2. **Boundary Values — Very Large Execution Plan (5,000+ Operators)**: Exporting a profiling report for a query with 5,000 or more physical operators in the `SparkPlanGraph` must produce a complete export without truncation; the JSON response must contain entries for all operators in the `planMetrics` array, and the CSV file must contain one data row per operator (5,000+ data rows plus the header row) with no row limit or pagination applied.

3. **Invalid Input — Execution ID Exceeding Long.MAX_VALUE**: Requesting an export with an execution ID that exceeds the `Long.MAX_VALUE` range (e.g., `/api/v1/applications/[app-id]/sql/99999999999999999999/profile?format=json`) must return an HTTP 400 response with a JSON body containing `{"error": "Execution ID exceeds maximum value"}` rather than throwing an unhandled `NumberFormatException` or returning an HTTP 500 response.

4. **Boundary Values — Unicode Characters in Query Descriptions**: Queries with Unicode characters in table names, column aliases, or descriptions (e.g., Chinese characters, Japanese characters, emoji sequences, or right-to-left script) must produce valid UTF-8 encoded JSON output with Unicode escapes where required by the JSON specification, and valid UTF-8 encoded CSV output with fields containing special characters enclosed in double quotes per RFC 4180.

5. **Concurrent Exports — 10 Simultaneous Requests for Different Queries**: When 10 users simultaneously request profiling exports for 10 different query execution IDs, each request must return the complete and accurate profiling data for its specific requested execution ID with no data mixing, no partial responses caused by thread contention, and no `ConcurrentModificationException` from shared `KVStore` access.

## Dependencies

- **EPIC-001** — [Adaptive Query Performance Insights Engine](../../EPIC-001-adaptive-query-performance-insights.md) — parent epic defining the overall scope of the performance insights initiative
- **FEATURE-001-01** — [Query Execution Profiling](../FEATURE-001-01-query-execution-profiling.md) — parent feature encompassing plan capture, timeline visualization, resource metrics display, report export, and plan comparison
- **STORY-001-01-01** — Capture Query Execution Plans — plan data must be captured and persisted in the `SQLAppStatusStore` KVStore before export is possible; this story depends on plan persistence being implemented first
- **STORY-001-01-03** — Display Resource Utilization Metrics — the per-executor CPU and memory utilization data structure used in the `executorResources` field of the JSON export is defined and populated by this story
- **Spark REST API Infrastructure** — The existing `/api/v1` REST API infrastructure documented in `docs/monitoring.md` provides the endpoint registration pattern, JSON serialization conventions, and HTTP response handling that the new profile export endpoints extend
  - `Source: docs/monitoring.md` — REST API endpoints `/applications/[app-id]/sql` and `/applications/[app-id]/sql/[execution-id]`, JSON format conventions, metrics sinks (ConsoleSink, CSVSink, JmxSink, PrometheusServlet)
- **`SQLAppStatusStore`** — Provides KVStore read access for `SQLExecutionUIData` (execution metadata, status, metrics), `SparkPlanGraphWrapper` (physical plan graph with node-level metrics), and `SQLPlanMetric` (individual metric definitions with name, accumulatorId, and metricType)
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala`
- **`SQLAppStatusListener`** — Provides live execution metrics for in-flight queries through the `liveExecutionMetrics()` method, enabling partial data export for queries in RUNNING status
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusListener.scala`
- **Jackson JSON Library (version 2.20.0)** — JSON serialization library already included as a Spark dependency (per `pom.xml`), used for producing JSON export output and streaming JSON generation via `JsonGenerator`
- **Spark Web UI** — The SQL Execution Detail page where the "Download Report" button is added
  - `Source: docs/web-ui.md` — SQL tab documentation covering query DAG visualization, SQL metrics display, logical and physical plan rendering

## Story Estimation Guidance

- **Story Points: 5** (Fibonacci scale)
- **Rationale**: The core infrastructure for profiling data collection is provided by STORY-001-01-01 (plan capture and persistence) and STORY-001-01-03 (resource utilization data structures). This story focuses on serialization of existing data into two output formats and REST API endpoint registration. JSON serialization leverages the existing Jackson library (version 2.20.0) already used throughout Spark's REST API. CSV generation is a straightforward tabular flattening of operator metrics. The bulk export streaming output adds moderate complexity. The Web UI button is a minor frontend addition. Estimated effort: 3–5 development days for a mid-level engineer familiar with the Spark REST API and Web UI codebase.

## Definition of Done

- [ ] JSON export endpoint (`GET /api/v1/applications/[app-id]/sql/[execution-id]/profile?format=json`) returns complete profiling data with all specified fields (`executionId`, `description`, `physicalPlanDescription`, `submissionTime`, `completionTime`, `duration`, `status`, `stages`, `planMetrics`, `executorResources`) for any completed query
- [ ] CSV export endpoint (`GET /api/v1/applications/[app-id]/sql/[execution-id]/profile?format=csv`) returns tabular operator-level metrics with all 10 columns and a `Content-Disposition: attachment; filename="query-[id]-profile.csv"` header
- [ ] Unsupported format parameters (anything other than `json` or `csv`) return HTTP 400 with a descriptive error message specifying the unsupported format name and listing the supported formats
- [ ] Running queries return partial profiling data with `"status": "RUNNING"` and `"partial": true` flag, sourcing live metrics from `SQLAppStatusListener`
- [ ] Bulk export for 500+ queries completes within 10 seconds using streaming JSON output via Jackson `JsonGenerator`, with chunked transfer encoding for large responses
- [ ] Queries with zero `SQLPlanMetric` entries return valid JSON (empty `planMetrics` array) and valid CSV (header row only) responses
- [ ] Unicode content in query descriptions and column aliases is exported with valid UTF-8 encoding in both JSON and CSV formats
- [ ] "Download Report" button is visible on the SQL Execution Detail page in the Spark Web UI with a dropdown offering JSON and CSV format selection
- [ ] Unit tests cover JSON and CSV serialization for: single-stage queries, multi-stage queries with joins and aggregations, queries with zero metrics, and queries with Unicode content
- [ ] Integration tests validate REST API responses including HTTP status codes, Content-Type headers, Content-Disposition headers, JSON schema structure, and CSV column completeness
- [ ] Load test confirms bulk export of 500 queries completes without `OutOfMemoryError` or memory exhaustion
- [ ] Code review approved by at least 2 Apache Spark committers
- [ ] Documentation covers export API endpoint URLs, supported format parameter values, JSON response schema, CSV column definitions, and usage examples for curl and PySpark
