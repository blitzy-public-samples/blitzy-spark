# Deliver Automated Optimization Recommendations to Eliminate Manual SQL Query Tuning

## Feature Summary

Data engineers currently invest significant time diagnosing and resolving SQL query performance issues — identifying data skew, selecting optimal join strategies, tuning partition counts, and determining caching candidates. This feature automates pattern detection across query execution data and generates specific, actionable recommendations that reduce manual SQL optimization effort by up to 60%. The recommendation engine detects data skew patterns across shuffle partitions, recommends join strategy changes (broadcast vs sort-merge vs shuffle-hash), suggests optimal partition counts based on data distribution, identifies datasets that benefit from caching to avoid redundant recomputation, and consolidates all findings into a prioritized summary optimization report.

This feature extends the Catalyst optimizer's Adaptive Query Execution (AQE) framework (`sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/`), which already performs runtime re-optimization including skew join handling via `OptimizeSkewedJoin.scala`, dynamic join selection via `DynamicJoinSelection.scala`, and partition coalescing via `CoalesceShufflePartitions.scala`. The recommendation engine analyzes the same runtime statistics collected by AQE but surfaces actionable recommendations to users rather than applying automatic changes. It builds on `SparkOptimizer` and `SparkStrategies` for join strategy awareness, `Partitioner.scala` for partition logic, `CacheManager.scala` for cache analysis, and the Dropwizard Metrics system (version 4.2.33) for performance data collection.

> **Source context:** `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala` (AQE main execution path), `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/OptimizeSkewedJoin.scala` (existing skew handling), `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/DynamicJoinSelection.scala` (existing dynamic join selection), `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/CoalesceShufflePartitions.scala` (partition coalescing logic), `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkOptimizer.scala` (optimization rules registry), `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala` (join strategy selection: broadcast, sort-merge, shuffle-hash), `core/src/main/scala/org/apache/spark/Partitioner.scala` (HashPartitioner, RangePartitioner logic), `sql/core/src/main/scala/org/apache/spark/sql/execution/CacheManager.scala` (cache state management), `core/src/main/scala/org/apache/spark/storage/BlockManager.scala` (storage block management and memory utilization), `docs/sql-performance-tuning.md` (existing AQE config, join hints, partition tuning), `docs/tuning.md` (memory management, serialization tuning, GC guidance)

## User Stories Index

| Story ID | Story Name | Phase | Description | Link |
|----------|-----------|-------|-------------|------|
| STORY-001-02-01 | Detect Data Skew Patterns | Backend | Identify and report partition-level data skew in shuffle operations during query execution | [STORY-001-02-01-detect-data-skew-patterns](./FEATURE-001-02/STORY-001-02-01-detect-data-skew-patterns.md) |
| STORY-001-02-02 | Recommend Join Strategy Changes | Backend | Analyze join operations and recommend optimal join strategies based on runtime data sizes | [STORY-001-02-02-recommend-join-strategy-changes](./FEATURE-001-02/STORY-001-02-02-recommend-join-strategy-changes.md) |
| STORY-001-02-03 | Suggest Partition Optimization | Backend | Recommend optimal partition counts and repartitioning strategies based on data distribution | [STORY-001-02-03-suggest-partition-optimization](./FEATURE-001-02/STORY-001-02-03-suggest-partition-optimization.md) |
| STORY-001-02-04 | Identify Missing Cache Opportunities | Backend | Detect repeatedly computed datasets and recommend caching to avoid redundant recomputation | [STORY-001-02-04-identify-missing-cache-opportunities](./FEATURE-001-02/STORY-001-02-04-identify-missing-cache-opportunities.md) |
| STORY-001-02-05 | Generate Optimization Summary Report | Frontend | Produce a consolidated report of all detected issues and recommendations for a query or session | [STORY-001-02-05-generate-optimization-summary-report](./FEATURE-001-02/STORY-001-02-05-generate-optimization-summary-report.md) |

## Dependencies

- **EPIC-001 — Adaptive Query Performance Insights Engine**: Parent epic that defines the overarching goal of reducing manual SQL optimization effort through automated profiling and recommendations
  - `Source: tickets/EPIC-001-adaptive-query-performance-insights.md`
- **FEATURE-001-01 — Query Execution Profiling**: Sibling feature that captures query execution plans, stage timelines, and resource utilization metrics; profiling data serves as the primary input for recommendation analysis
  - `Source: tickets/EPIC-001/FEATURE-001-01-query-execution-profiling.md`
- **F-002 — SQL Query Processing Engine**: Catalyst optimizer, execution engine, and query plan structure provide the foundation for analyzing query behavior and identifying optimization opportunities
  - `Source: sql/catalyst/` — query plan classes, logical and physical plan trees, optimizer rules
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/SparkOptimizer.scala` — optimization rules registry
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala` — join strategy selection (broadcast, sort-merge, shuffle-hash)
- **Adaptive Query Execution (AQE) Framework**: Runtime statistics collection and re-optimization infrastructure that the recommendation engine leverages for skew detection, join conversion analysis, and partition coalescing insights
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala` — AQE main execution path
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/OptimizeSkewedJoin.scala` — existing skew handling (reference pattern for skew detection)
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/DynamicJoinSelection.scala` — existing dynamic join selection logic
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/CoalesceShufflePartitions.scala` — partition coalescing logic (reference pattern for partition recommendations)
- **Spark Web UI**: SQL tab serves as the primary display surface for rendering optimization recommendations alongside query execution details
  - `Source: docs/web-ui.md` — SQL tab, Stages tab, Executors tab documentation
- **Dropwizard Metrics System (version 4.2.33)**: Shuffle statistics, executor resource utilization, and task-level performance counters used as input data for recommendation analysis
  - `Source: core/src/main/scala/org/apache/spark/metrics/` — MetricsSystem, ExecutorMetricType, Source trait, Sink implementations
- **CacheManager**: Cache state inspection for identifying datasets that are recomputed multiple times and would benefit from explicit caching
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/CacheManager.scala` — cache state management, cached plan tracking
- **Partitioner**: Partition count analysis and data distribution evaluation for generating partition optimization recommendations
  - `Source: core/src/main/scala/org/apache/spark/Partitioner.scala` — HashPartitioner, RangePartitioner logic
- **Configuration References**:
  - `Source: docs/sql-performance-tuning.md` — existing AQE configuration (`spark.sql.adaptive.enabled`, `spark.sql.adaptive.coalescePartitions.*`, `spark.sql.adaptive.skewJoin.*`), join hints (BROADCAST, MERGE, SHUFFLE_HASH), `spark.sql.autoBroadcastJoinThreshold`
  - `Source: docs/tuning.md` — memory management, serialization tuning, GC guidance

## Definition of Done

- All 5 user stories (STORY-001-02-01 through STORY-001-02-05) are complete and pass their acceptance criteria
- Data skew detection identifies partitions that exceed 5x the median partition size (matching the `spark.sql.adaptive.skewJoin.skewedPartitionFactor` default of 5.0) and reports affected stages with partition size distribution details
- Join strategy recommendations compare the current join type against the optimal join type based on runtime data sizes, using the broadcast threshold defined by `spark.sql.autoBroadcastJoinThreshold` (default 10MB) and the adaptive broadcast threshold `spark.sql.adaptive.autoBroadcastJoinThreshold`
- Partition optimization suggests specific target partition counts based on data volume and the `spark.sql.adaptive.advisoryPartitionSizeInBytes` configuration (default 64MB), with recommendations for both increasing and decreasing partition counts
- Cache opportunity detection identifies DataFrames computed more than once within a session and estimates memory savings from caching, referencing `spark.sql.inMemoryColumnarStorage.compressed` and `spark.sql.inMemoryColumnarStorage.batchSize` configuration
- Optimization summary report consolidates all findings with severity classification (high, medium, low), estimated performance impact percentage, and actionable code snippets or configuration changes for each recommendation
- Recommendations are displayed in the Spark Web UI SQL tab alongside query execution details, with each recommendation linked to the specific query operator it applies to
- Recommendations are accessible through the Spark REST API (`/api/v1` endpoints), returning structured JSON responses with recommendation type, severity, affected operators, and suggested changes
- Integration tests validate recommendation accuracy for known data skew scenarios, suboptimal join strategy selections, unnecessary partition counts, and cache-miss patterns
- No false positives are generated for optimally-configured queries in the integration test suite — the recommendation engine produces zero findings when queries are already using the best available strategies
- Documentation covers recommendation interpretation (what each recommendation type means), threshold configuration (how to adjust sensitivity), and manual override guidance (how to suppress or modify recommendations)
- All recommendation thresholds are configurable through Spark configuration properties, with defaults aligned to existing AQE thresholds documented in `docs/sql-performance-tuning.md`
