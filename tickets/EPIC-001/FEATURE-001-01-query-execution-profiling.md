# Implement Query Execution Profiling to Capture and Visualize SQL Performance Data

## Feature Summary

Data engineers and data scientists currently rely on manual inspection of the Spark Web UI SQL tab and Stages tab to understand query performance characteristics, a process that is time-consuming and lacks structured repeatability. This feature introduces automated query execution plan capture, stage execution timeline visualization, executor resource utilization metrics display, profiling report export in JSON and CSV formats, and side-by-side execution plan comparison — providing a structured, repeatable query profiling workflow that integrates directly into the existing Spark development experience.

The feature scope encompasses five capabilities: (1) capturing and persisting both logical and physical query execution plans through the `QueryExecution` lifecycle (`analyzed → optimized → sparkPlan → executedPlan → toRdd`), (2) visualizing stage execution timelines with per-stage duration, input/output row counts, and shuffle read/write metrics, (3) displaying per-query executor CPU percentage and memory usage in bytes, (4) enabling export of profiling data in JSON (structured, machine-readable) and CSV (tabular, spreadsheet-compatible) formats, and (5) allowing side-by-side comparison of two execution plans with highlighted differences. This feature extends the existing query execution infrastructure managed by `QueryExecution` (`sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala`), leverages `ExplainUtils` and `ExplainMode` for plan rendering, and builds on the SQL tab in Spark Web UI (`sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLTab.scala`, `AllExecutionsPage.scala`, `ExecutionPage.scala`) and the Dropwizard Metrics system (`core/src/main/scala/org/apache/spark/metrics/MetricsSystem.scala`).

## User Stories Index

| Story ID | Story Name | Phase | Description | Link |
|----------|-----------|-------|-------------|------|
| STORY-001-01-01 | Capture Query Execution Plans | Backend | Capture and persist logical and physical execution plans for SQL queries | [STORY-001-01-01-capture-query-execution-plans](./FEATURE-001-01/STORY-001-01-01-capture-query-execution-plans.md) |
| STORY-001-01-02 | Visualize Stage Execution Timeline | Frontend | Display a timeline view of query stage execution with per-stage duration and metrics | [STORY-001-01-02-visualize-stage-execution-timeline](./FEATURE-001-01/STORY-001-01-02-visualize-stage-execution-timeline.md) |
| STORY-001-01-03 | Display Resource Utilization Metrics | Frontend | Show executor CPU and memory utilization metrics for each query execution | [STORY-001-01-03-display-resource-utilization-metrics](./FEATURE-001-01/STORY-001-01-03-display-resource-utilization-metrics.md) |
| STORY-001-01-04 | Export Profiling Reports | Backend | Export query profiling data in JSON and CSV formats for external analysis | [STORY-001-01-04-export-profiling-reports](./FEATURE-001-01/STORY-001-01-04-export-profiling-reports.md) |
| STORY-001-01-05 | Compare Execution Plans | Frontend | Provide side-by-side comparison of query execution plans to identify optimization impacts | [STORY-001-01-05-compare-execution-plans](./FEATURE-001-01/STORY-001-01-05-compare-execution-plans.md) |

## Dependencies

- **EPIC-001 — Adaptive Query Performance Insights Engine**: Parent epic that defines the overall scope of the performance insights initiative, encompassing both query execution profiling (this feature) and automated optimization recommendations (FEATURE-001-02).
  - `Source: tickets/EPIC-001-adaptive-query-performance-insights.md`
- **F-002 — SQL Query Processing Engine**: The Catalyst optimizer, Spark SQL execution engine, and the `QueryExecution` lifecycle provide the query plan representation, physical plan operators, and runtime statistics that this profiling feature captures and visualizes.
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala` — query lifecycle phases (analyzed → optimized → sparkPlan → executedPlan → toRdd)
  - `Source: sql/catalyst/` — logical and physical plan tree structures, optimizer rules
- **Spark Web UI**: The existing Web UI infrastructure serves as the primary display surface for profiling data, extending the SQL tab with profiling panels and integrating with the Stages tab and Executors tab for timeline and resource views.
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLTab.scala` — SQL tab registration and page management
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/AllExecutionsPage.scala` — all SQL executions listing page
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/ExecutionPage.scala` — single execution detail page with plan visualization
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SparkPlanGraph.scala` — graph-based plan representation for UI rendering
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala` — SQL execution status persistence
  - `Source: core/src/main/scala/org/apache/spark/ui/SparkUI.scala` — main Web UI application hosting all tabs
  - `Source: docs/web-ui.md` — Web UI documentation covering Jobs, Stages, SQL, and Executors tabs
- **Dropwizard Metrics System (version 4.2.33)**: The underlying metrics collection framework provides the instrumentation layer for capturing executor resource utilization (CPU percentage, memory usage in bytes) and query-level performance counters used by the profiling engine.
  - `Source: core/src/main/scala/org/apache/spark/metrics/MetricsSystem.scala` — MetricsSystem lifecycle, Source trait, Sink implementations
  - `Source: docs/monitoring.md` — monitoring guide covering REST API, metrics sinks (Console, CSV, JMX, Graphite, Prometheus), event logging
- **QueryExecution API**: Provides the lazy evaluation phases for capturing logical plans (analyzed, optimized) and physical plans (sparkPlan, executedPlan) at each stage of the query compilation pipeline.
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala`
- **ExplainUtils and ExplainMode**: Provide plan text rendering capabilities and explain output modes (simple, extended, codegen, cost, formatted) used to generate human-readable plan representations for profiling reports.
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ExplainUtils.scala` — plan text rendering and operator ID assignment
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ExplainMode.scala` — explain output mode enumeration
- **SparkPlan and SparkPlanInfo**: The physical plan base class with per-operator metrics and the serializable plan representation used by the Web UI for plan graph rendering.
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlan.scala` — physical plan base class with metrics and execute methods
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlanInfo.scala` — serializable plan representation for UI transport
- **Spark Connect (gRPC)**: Enables remote access to profiling data for clients connected through the Spark Connect protocol, supporting headless and notebook-based workflows where the Web UI may not be directly accessible.
  - `Source: sql/connect/` — SparkConnectService proto definitions, client-server gRPC architecture
- **Existing Performance Tuning Infrastructure**: Current tuning capabilities documented in `docs/sql-performance-tuning.md` — including Adaptive Query Execution (AQE), join strategy hints (BROADCAST, MERGE, SHUFFLE_HASH, SHUFFLE_REPLICATE_NL), partition tuning, and caching configuration — provide the baseline context against which profiling data is interpreted.
  - `Source: docs/sql-performance-tuning.md` — AQE configuration, join strategies, partition tuning, caching, statistics

## Definition of Done

- All 5 user stories (STORY-001-01-01 through STORY-001-01-05) are complete and pass their respective acceptance criteria
- Query execution plans (both logical and physical) are captured and persisted for every completed query, storing the analyzed plan, optimized plan, spark plan, and executed plan representations
- Stage execution timeline is rendered in the Spark Web UI with per-stage duration in milliseconds, input row count, output row count, shuffle read bytes, and shuffle write bytes
- Executor resource utilization (CPU percentage as a decimal between 0.0 and 100.0, and memory usage in bytes) is displayed per-query in the Spark Web UI Executors section
- Profiling reports are exportable in JSON format (structured, machine-readable with nested plan nodes) and CSV format (tabular, one row per operator with columns for operator name, duration, input rows, output rows, and memory usage)
- Execution plan comparison displays two selected plans side-by-side with differences highlighted, categorizing changes as added operators, removed operators, or modified operators (changed metrics or parameters)
- Profiling data is accessible via the Spark REST API `/api/v1` endpoints, returning JSON-formatted profiling results for programmatic consumption
- Integration tests validate profiling data capture for queries using joins (broadcast and sort-merge), aggregations (GROUP BY, HAVING), and window functions (ROW_NUMBER, RANK)
- Performance overhead of profiling capture is measured and documented, confirming that profiling adds less than 5% latency to standard TPCDS benchmark queries when enabled
- Documentation covers profiling activation (configuration parameters), Web UI navigation for profiling panels, report export procedures, plan comparison workflow, and REST API endpoint reference
