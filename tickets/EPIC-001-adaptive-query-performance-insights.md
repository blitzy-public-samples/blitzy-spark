# Implement Adaptive Query Performance Insights Engine to Reduce Manual SQL Optimization Effort

## Epic Summary

Data engineers spend significant time manually tuning SQL queries through trial-and-error adjustments to join strategies, partition counts, and caching configurations; an automated profiling and recommendations engine reduces this optimization effort by up to 60% by surfacing actionable insights directly within the development workflow. This epic extends the Catalyst optimizer with user-facing diagnostics delivered through the Spark Web UI and Spark Connect, covering both query execution profiling and automated optimization recommendations. The scope is limited to read-only analysis and recommendation generation — this epic does NOT implement automatic query rewriting or auto-tuning behavior.

## Features Index

| Feature ID | Feature Name | Description | Link |
|---|---|---|---|
| FEATURE-001-01 | Query Execution Profiling | Captures and visualizes query execution plans, stage timelines, and resource utilization metrics to provide data engineers with detailed insight into SQL query behavior and performance bottlenecks. | [FEATURE-001-01-query-execution-profiling](./EPIC-001/FEATURE-001-01-query-execution-profiling.md) |
| FEATURE-001-02 | Automated Optimization Recommendations | Analyzes query execution patterns to detect data skew, recommend join strategy changes, suggest partition optimizations, identify missing cache opportunities, and generate consolidated optimization reports. | [FEATURE-001-02-automated-optimization-recommendations](./EPIC-001/FEATURE-001-02-automated-optimization-recommendations.md) |

## Dependencies

- **F-002 — SQL Query Processing Engine**: The Catalyst optimizer and Spark SQL execution engine provide the query plan representation, physical plan operators, and runtime statistics that this epic's profiling and recommendation features analyze.
  - `Source: sql/catalyst/` — query plan representation, logical and physical plan trees, optimizer rules
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/` — physical execution operators, QueryExecution lifecycle, ExplainUtils, SparkPlan metrics, Adaptive Query Execution (AQE) framework
- **Spark Web UI**: The existing Web UI infrastructure serves as the primary display surface for profiling data and optimization recommendations, extending the SQL tab and Stages tab with new performance insight panels.
  - `Source: docs/web-ui.md` — SQL tab (query DAG visualization, SQL metrics, logical/physical plan display), Stages tab (task metrics, shuffle statistics, executor aggregation), Executors tab (resource utilization)
- **Dropwizard Metrics System (version 4.2.33)**: The underlying metrics collection framework provides the instrumentation layer for capturing query-level performance counters, executor resource utilization, and shuffle statistics used by the profiling engine.
  - `Source: core/src/main/scala/org/apache/spark/metrics/` — MetricsSystem lifecycle manager, MetricsConfig loader, ExecutorMetricType definitions, Source trait, Sink implementations (Console, CSV, JMX, Graphite, Statsd, Prometheus)
- **Spark Connect (gRPC)**: Enables remote access to profiling data and optimization recommendations for clients connected through the Spark Connect protocol, supporting headless and notebook-based workflows.
  - `Source: sql/connect/` — SparkConnectService proto definitions, client-server architecture, gRPC service layer
- **Existing Performance Tuning Infrastructure**: Current tuning capabilities including Adaptive Query Execution (AQE), join strategy hints, partition coalescing, skew join optimization, and statistics-based planning provide the foundation that the insights engine builds upon.
  - `Source: docs/sql-performance-tuning.md` — AQE configuration (coalesce partitions, skew join splitting, broadcast join conversion), join strategy hints (BROADCAST, MERGE, SHUFFLE_HASH, SHUFFLE_REPLICATE_NL), partition tuning, caching configuration, statistics leveraging (data source, catalog, runtime)

## Definition of Done

- All child features (FEATURE-001-01: Query Execution Profiling and FEATURE-001-02: Automated Optimization Recommendations) are complete, code-reviewed, and integrated into the main branch
- Integration tests pass for end-to-end query profiling workflows, covering plan capture, timeline visualization, and resource metrics display
- Integration tests pass for recommendation generation workflows, covering skew detection, join strategy analysis, partition optimization, cache opportunity identification, and summary report generation
- The Spark Web UI displays a dedicated Performance Insights panel within the SQL tab, showing profiling data and optimization recommendations for each executed query
- Profiling data is accessible through the Spark Connect API for remote clients, returning structured profiling results over gRPC
- Documentation is published covering:
  - How to enable and configure query profiling (including sampling rate and retention settings)
  - How to interpret profiling results (execution plans, stage timelines, resource utilization)
  - How to read and act on optimization recommendations
  - API reference for programmatic access to profiling and recommendation data
- Performance overhead of the profiling engine is measured and documented, confirming that profiling adds less than 5% latency to typical query execution when enabled
- All optimization recommendations reference specific, actionable configuration changes or query rewrites with before/after comparison data
- The feature degrades gracefully when profiling is disabled — no performance impact on query execution when the insights engine is turned off
