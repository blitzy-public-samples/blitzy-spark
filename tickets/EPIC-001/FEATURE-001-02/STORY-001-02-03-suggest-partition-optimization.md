# Suggest Partition Optimization Based on Data Volume and Distribution to Minimize Shuffle Overhead

## User Story

**As a** data engineer, **I want to** receive partition optimization recommendations that analyze the actual data volume and partition size distribution for each shuffle stage and suggest specific target partition counts, coalesce opportunities, and repartitioning strategies including the SQL or DataFrame API syntax to apply the change, **so that** I can replace manual trial-and-error partition tuning (repeatedly adjusting `spark.sql.shuffle.partitions` from its default of 200) with data-driven recommendations, reducing partition-related performance issues such as too-small partitions (excessive task scheduling overhead) or too-large partitions (memory pressure and spills) and achieving 20–50% runtime improvement for partition-misconfigured workloads.

## Acceptance Criteria

### AC-1: Expected Output — Partition Count Recommendation (Optimal)

- **Given** a completed SQL query where a shuffle stage has total data size of 12.8 GB across 200 partitions (current `spark.sql.shuffle.partitions` default), resulting in average partition size of 64 MB matching `spark.sql.adaptive.advisoryPartitionSizeInBytes` (default: 64 MB)
- **When** the partition analysis runs on the query execution profile
- **Then** the system reports "partition count is optimal" for this stage, showing: current partition count (200), total data size (12.8 GB), average partition size (64 MB), target partition size (64 MB), and recommended partition count (200 — no change)

### AC-2: Expected Output — Too Many Small Partitions

- **Given** a shuffle stage has total data size of 500 MB across 200 partitions, resulting in average partition size of 2.5 MB (well below the 64 MB advisory size)
- **When** the partition analysis runs
- **Then** the system recommends reducing the partition count to 8 (calculated as ceil(500 MB / 64 MB)), provides the coalesce hint syntax (`SELECT /*+ COALESCE(8) */ ...` and `df.coalesce(8)`), and classifies this as "medium" impact since small partitions cause task scheduling overhead but do not cause memory failures

### AC-3: Expected Output — Too Few Large Partitions

- **Given** a shuffle stage has total data size of 100 GB across 200 partitions, resulting in average partition size of 512 MB (8x the 64 MB advisory size)
- **When** the partition analysis runs
- **Then** the system recommends increasing the partition count to 1563 (calculated as ceil(100 GB / 64 MB)), provides the repartition hint syntax (`SELECT /*+ REPARTITION(1563) */ ...` and `df.repartition(1563)`), and classifies this as "high" impact since oversized partitions cause memory pressure, GC pauses, and potential disk spills

### AC-4: Input Validation — Advisory Partition Size

- **Given** a data engineer configures `spark.sql.adaptive.advisoryPartitionSizeInBytes` with a value of 0 bytes or a negative number
- **When** the configuration is applied
- **Then** the system rejects the configuration with an error message stating the minimum allowed value is 1 byte and the parameter must be a positive integer representing bytes

### AC-5: Error Handling — Unavailable Partition Metrics

- **Given** a shuffle stage where partition size metrics are not available because the stage was skipped by AQE (e.g., via `AQEPropagateEmptyRelation`) or the query was cancelled before shuffle materialization
- **When** the partition analysis attempts to evaluate this stage
- **Then** the system returns a report entry for the stage with status "analysis incomplete — shuffle not materialized" and does not produce a partition count recommendation for that stage

### AC-6: Edge Case — Single Partition Output

- **Given** a shuffle stage that produces a single output partition (e.g., due to a `coalesce(1)` or global sort) with data size of 5 GB
- **When** the partition analysis runs
- **Then** the system flags this as "high" impact, recommends repartitioning to 79 partitions (ceil(5 GB / 64 MB)), and notes that the single partition was likely caused by an explicit coalesce or global ordering operation that should be reviewed for necessity

## Sub-Tasks

1. **Implement `PartitionAnalyzer` component** — Build a component that reads partition sizes from `MapOutputStatistics.bytesByPartitionId` for each shuffle stage and computes total size, average size, min size, max size, standard deviation, and partition count.
   > Source: `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/CoalesceShufflePartitions.scala` — reference for reading `mapStats.bytesByPartitionId`

2. **Implement optimal partition count calculation** — Calculate `targetPartitions = ceil(totalDataSize / advisoryPartitionSize)` where advisory size defaults to `spark.sql.adaptive.advisoryPartitionSizeInBytes` (64 MB), bounded by `spark.sql.adaptive.coalescePartitions.minPartitionSize` (default 1 MB) as the lower per-partition bound, and constrained by `spark.sql.adaptive.coalescePartitions.initialPartitionNum` as upper bound if set.
   > Source: `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/CoalesceShufflePartitions.scala` — `advisoryPartitionSize` method and `ADVISORY_PARTITION_SIZE_IN_BYTES` configuration

3. **Implement partition distribution uniformity analysis** — Detect partition size variance by computing the coefficient of variation (stddev / mean). Flag when the coefficient of variation exceeds 1.0, indicating a non-uniform distribution where `REPARTITION` (hash-based, uniform) would address the problem better than `COALESCE` (merge-only, preserves locality).
   > Source: `core/src/main/scala/org/apache/spark/Partitioner.scala` — `HashPartitioner` (hash-based uniform distribution) and `RangePartitioner` (range-based for sorted output)

4. **Implement hint syntax generator** — Produce SQL hints (`COALESCE(n)`, `REPARTITION(n)`, `REPARTITION_BY_RANGE(n, col)`, `REBALANCE(n)`) and DataFrame API equivalents (`df.coalesce(n)`, `df.repartition(n)`, `df.repartitionByRange(n, col)`).
   > Source: `docs/sql-performance-tuning.md` — partition hint syntax documentation (coalesce, repartition, repartitionByRange, rebalance)

5. **Create `PartitionRecommendation` data model** — Define a structured result containing: stage ID, current partition count, total data size, average partition size, recommended partition count, recommended operation (coalesce / repartition / repartition_by_range / no change), impact category (critical / high / medium / low / none), and hint syntax strings for both SQL and DataFrame API formats.

6. **Integrate partition analysis into the profiling pipeline** — Persist partition analysis results in `SQLAppStatusStore` and expose them via a REST API endpoint at `/api/v1/applications/[app-id]/sql/[execution-id]/partition-recommendations`.

7. **Write unit and integration tests** — Cover the following scenarios: optimal partitions (no change), too many small partitions (coalesce), too few large partitions (repartition), single partition, non-uniform distribution, unavailable metrics, boundary advisory size values. Include an end-to-end integration test validating partition recommendations for a query with 200 partitions processing 500 MB (expecting a recommendation to coalesce to 8 partitions).

## Edge Cases

1. **Empty/Null Input** — A shuffle stage has all partitions with zero bytes (e.g., a query with no matching rows). The system reports "no data — all partitions empty" and recommends reviewing upstream filter conditions rather than adjusting partition counts, since repartitioning empty data has no performance impact.

2. **Boundary Values** — Total data size divided by advisory partition size results in exactly 1.0 (e.g., 64 MB data with 64 MB advisory size). The system recommends 1 partition and notes that coalescing to 1 partition eliminates shuffle overhead but creates a single-threaded bottleneck for downstream operations.

3. **Invalid Input** — `spark.sql.shuffle.partitions` is configured with a value of 0 or negative. The system rejects the value with an error message stating the minimum allowed value is 1 and the parameter must be a positive integer.

4. **Extreme Partition Count** — A stage has 100,000 partitions with total data of 100 MB (average 1 KB per partition). The system recommends aggressive coalescing to 2 partitions (ceil(100 MB / 64 MB)) and flags the current configuration as "critical" impact due to task scheduling overhead dominating actual computation.

5. **Mixed Partition Sizes (Bimodal Distribution)** — A stage has a bimodal partition distribution (half at 1 MB, half at 500 MB). The system detects the high coefficient of variation, recommends `REPARTITION` (full shuffle for uniformity) rather than `COALESCE` (merge only), and notes the data distribution pattern suggesting a potential upstream skew issue.

## Dependencies

- **FEATURE-001-01 — Query Execution Profiling**: Provides execution plan data and shuffle stage metrics that serve as input for partition analysis.
  - `Source: tickets/EPIC-001/FEATURE-001-01-query-execution-profiling.md`
- **FEATURE-001-02 — Automated Optimization Recommendations**: Parent feature defining the overall recommendation engine scope and integration points.
  - `Source: tickets/EPIC-001/FEATURE-001-02-automated-optimization-recommendations.md`
- **EPIC-001 — Adaptive Query Performance Insights Engine**: Parent epic establishing the overarching objective of reducing manual SQL optimization effort.
  - `Source: tickets/EPIC-001-adaptive-query-performance-insights.md`
- **F-002 — SQL Query Processing Engine**: Catalyst optimizer and AQE partition coalescing framework that provides runtime statistics and configuration parameters.
- **`core/src/main/scala/org/apache/spark/Partitioner.scala`**: Defines `HashPartitioner` (hash-based uniform distribution) and `RangePartitioner` (range-based for sorted output) — the two core partition strategies that recommendations reference.
  - `Source: core/src/main/scala/org/apache/spark/Partitioner.scala`
- **`sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/CoalesceShufflePartitions.scala`**: AQE partition coalescing logic using `advisoryPartitionSizeInBytes` (default 64 MB), `coalescePartitions.minPartitionSize` (default 1 MB), and `coalescePartitions.parallelismFirst` — the algorithm model for calculating target partition counts.
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/CoalesceShufflePartitions.scala`
- **`docs/sql-performance-tuning.md`**: Partition tuning documentation covering `spark.sql.shuffle.partitions` (default 200), `spark.sql.files.maxPartitionBytes` (default 128 MB), and coalesce/repartition/repartitionByRange/rebalance hint syntax.
  - `Source: docs/sql-performance-tuning.md`
- **STORY-001-02-01 — Detect Data Skew Patterns**: Skew detection results inform whether `REPARTITION` (full shuffle) is recommended over `COALESCE` (merge only) for non-uniform distributions.
- **SQLAppStatusStore**: Execution data persistence layer for storing and retrieving partition recommendations.
- **Spark REST API**: Programmatic access to partition recommendations via `/api/v1/applications/[app-id]/sql/[execution-id]/partition-recommendations`.

## Story Estimation Guidance

- **Story Points**: 5 (Fibonacci scale)
- **Justification**: Moderate complexity — the core partition count calculation is straightforward (`totalSize / advisorySize`) but the analysis must handle multiple edge cases (empty stages, single partition, extreme counts, non-uniform distribution), generate hint syntax for 4 partition operations across SQL and DataFrame APIs, and integrate the distribution uniformity analysis (coefficient of variation). The reference implementation in `CoalesceShufflePartitions.scala` provides a clear algorithm model. Fits within a single sprint for one experienced engineer.

## Definition of Done

- [ ] Partition analyzer evaluates all shuffle stages in a query and recommends optimal partition counts based on `spark.sql.adaptive.advisoryPartitionSizeInBytes` (default 64 MB)
- [ ] Recommendations distinguish between coalesce (reduce partitions, no full shuffle), repartition (redistribute for uniformity), and repartition_by_range (range-based redistribution)
- [ ] Each recommendation includes: stage ID, current partition count, recommended partition count, operation type, impact category (critical / high / medium / low / none), and hint syntax in both SQL and DataFrame API formats
- [ ] Distribution uniformity analysis detects non-uniform partition sizes (coefficient of variation > 1.0) and recommends REPARTITION over COALESCE when redistribution is needed
- [ ] Results are persisted in SQLAppStatusStore and accessible via REST API endpoint `/api/v1/applications/[app-id]/sql/[execution-id]/partition-recommendations`
- [ ] Unit tests cover: optimal configuration (no change), too-small partitions (coalesce), too-large partitions (repartition), single partition, empty partitions, non-uniform distribution, extreme partition counts, boundary advisory size
- [ ] Integration test validates end-to-end partition recommendation for a query with 200 partitions processing 500 MB (should recommend coalescing to 8 partitions)
- [ ] No forbidden terms appear in any acceptance criteria text
- [ ] Story is demo-able: a data engineer can run a query with default 200 shuffle partitions on a small dataset, navigate to the SQL execution page, and see the recommendation to reduce partition count with specific coalesce syntax
