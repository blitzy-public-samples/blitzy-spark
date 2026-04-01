# Identify Missing Cache Opportunities for Repeatedly Computed Datasets to Eliminate Redundant Processing

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

**As a** data scientist, **I want to** receive automated caching recommendations that identify DataFrames or tables computed more than once within the current SparkSession, including the estimated memory footprint for caching, the number of redundant computations detected, and the specific `dataFrame.cache()` or `spark.catalog.cacheTable()` API call to apply, **so that** I can avoid unknowingly recomputing expensive transformations (such as joins, aggregations, or ML feature pipelines) that can consume 30–60% of total query runtime in iterative workloads, enabling me to cache strategic datasets and reduce end-to-end notebook execution time by 2–5x for iterative exploration patterns.

## Acceptance Criteria

### AC-1: Duplicate Computation Detection (Expected Output)

- **Given** a SparkSession where the same DataFrame logical plan (after normalization per `QueryExecution.normalized`) has been executed 3 times within the session without being cached
- **When** the cache opportunity analysis runs
- **Then** the system identifies this DataFrame as a caching candidate and reports: the normalized logical plan hash, execution count (3), total cumulative execution time across all runs in milliseconds, estimated cache memory footprint in bytes (based on the output data size of the most recent execution), and the recommended API call (`dataFrame.cache()` for programmatic access or `spark.catalog.cacheTable("tableName")` for named tables)

### AC-2: Memory Savings Estimate (Expected Output)

- **Given** a caching candidate DataFrame with output size of 500 MB that was computed 4 times with average execution time of 30 seconds per run
- **When** the cache opportunity recommendation is generated
- **Then** the system estimates: memory required for caching as 500 MB (in-memory columnar format), cumulative time saved by caching as 90 seconds (3 redundant runs × 30 seconds), and the storage level recommendation (MEMORY_AND_DISK if estimated size exceeds 60% of available executor memory, MEMORY_ONLY otherwise)

### AC-3: Minimum Recomputation Threshold (Input Validation)

- **Given** a data scientist configures `spark.sql.insights.cache.minRecomputations` with a value of 0 or a negative integer
- **When** the configuration is applied
- **Then** the system rejects the value with an error message stating the minimum allowed value is 2 (a dataset must be computed at least twice to qualify as a caching candidate) and the parameter must be a positive integer greater than or equal to 2

### AC-4: Insufficient Memory for Caching (Error Handling)

- **Given** a caching candidate DataFrame with estimated cache size of 10 GB but the total available executor storage memory across the cluster is 8 GB
- **When** the recommendation is generated
- **Then** the system still recommends caching with MEMORY_AND_DISK storage level, includes a warning that the cached dataset exceeds available storage memory and will spill to disk, and provides the estimated disk spill amount (2 GB)

### AC-5: Already Cached Dataset (Edge Case)

- **Given** a DataFrame that has been computed multiple times but is already cached in the `CacheManager` (verified via `lookupCachedData`)
- **When** the cache opportunity analysis runs
- **Then** the system excludes this DataFrame from the caching recommendation list and instead reports it in a "currently cached" summary showing: table/DataFrame name, storage level, cached size in bytes, and cache hit count during the session

### AC-6: Trivial Computation (Edge Case)

- **Given** a DataFrame that has been computed 5 times but each execution completes in less than 100 milliseconds (e.g., a simple scan of a small file or a filter on a cached parent)
- **When** the cache opportunity analysis runs
- **Then** the system excludes this DataFrame from the "recommended" list and places it in an "informational" category, noting that caching overhead (serialization, memory allocation) may exceed the benefit for sub-100ms computations

## Sub-Tasks

1. **Implement `CacheOpportunityDetector` component** — Build a session-scoped component that tracks executed DataFrame logical plans (using the normalized plan from `QueryExecution.normalized`) across a SparkSession, counting execution occurrences and cumulative execution times per unique plan hash.
   - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/CacheManager.scala` (reference for plan normalization and `CachedData` model)

2. **Implement plan matching logic using `sameResult` semantics** — Compare normalized plans using the `sameResult` method (the same pattern used by `CacheManager.lookupCachedData`) to detect logically equivalent computations even when DataFrame variable names differ.
   - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/CacheManager.scala` (lines 353–361, `lookupCachedDataInternal` uses `plan.sameResult(cd.plan)`)

3. **Implement memory footprint estimation** — Calculate the estimated cache memory footprint using output data size from the most recent execution's `SQLMetrics` (rows output × average row size) and compare against available executor storage memory from `BlockManager.memoryStore.memoryUsed` and `BlockManager.memoryStore.maxMemory`.
   - `Source: core/src/main/scala/org/apache/spark/storage/BlockManager.scala` (lines 233–245, `memoryStore`, `maxOnHeapMemory`, `maxOffHeapMemory`)

4. **Implement storage level recommendation logic** — Recommend MEMORY_ONLY if estimated size is less than 60% of available storage memory; recommend MEMORY_AND_DISK if estimated size is 60% or more of available storage memory or exceeds total available storage memory; include disk spill estimation when applicable.
   - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/CacheManager.scala` (line 38, imports `StorageLevel.MEMORY_AND_DISK` as default)

5. **Create `CacheRecommendation` data model** — Define a structured data model containing: plan hash, plan description (truncated to 200 characters), execution count, cumulative execution time in milliseconds, estimated cache size in bytes, recommended storage level, estimated time savings in milliseconds, and the API syntax (`dataFrame.cache()` or `spark.catalog.cacheTable("name")`).
   - `Source: docs/sql-performance-tuning.md` (lines 28–31, caching API reference: `spark.catalog.cacheTable("tableName")`, `dataFrame.cache()`, `dataFrame.unpersist()`)

6. **Integrate detection into query lifecycle and UI surfaces** — Hook the `CacheOpportunityDetector` into the post-query completion listener to persist results in `SQLAppStatusStore`; add a REST API endpoint at `/api/v1/applications/[app-id]/cache-recommendations` returning a JSON array of `CacheRecommendation` objects; add a Web UI panel in the Storage tab showing cache recommendations alongside the current cache state.
   - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/CacheManager.scala` (cache state data used for "currently cached" summary)

7. **Write unit and integration tests** — Cover the following scenarios: single computation (no recommendation), duplicate computation (recommendation generated), already-cached dataset (excluded from recommendations), trivial computation (informational category only), memory-constrained scenario (MEMORY_AND_DISK recommendation), plan normalization (different variable names producing the same logical plan), streaming query exclusion, and concurrent sessions (isolation between SparkSessions).

## Edge Cases

### 1. Empty/Null Input — No Completed Queries in Session

A session has no completed queries (a new SparkSession with no actions executed). The system returns an empty recommendation list with a message "no query executions recorded in current session" and no error. The REST API endpoint returns an empty JSON array `[]` with HTTP 200, and the Web UI panel displays a placeholder message indicating no recommendations are available.

### 2. Boundary Values — Minimum Threshold and Trivial Computation Cutoff

A DataFrame is computed exactly 2 times (the minimum threshold for recommendation) with execution time of exactly 100 milliseconds each. The system includes it in the recommendation list since it meets the minimum recomputation threshold (2) and is at the boundary of the trivial computation cutoff (100 ms). The recommendation is flagged with a "low" impact classification, indicating marginal benefit from caching.

### 3. Invalid Input — Memory Percentage Configuration Out of Range

The `spark.sql.insights.cache.maxMemoryPercentage` parameter (controlling the 60% memory threshold for storage level selection) is set to a value greater than 100 or less than 0. The system rejects the configuration with an error message stating the allowed range is 1 to 100 (representing a percentage of available storage memory). Values of exactly 1 and exactly 100 are accepted as valid boundary inputs.

### 4. Large Session with Many Candidates — Ranking and Pagination

A notebook session has executed 500 distinct DataFrame plans with 50 qualifying as caching candidates. The system ranks recommendations by estimated time savings (highest first) and returns the top 20 by default (configurable via `spark.sql.insights.cache.maxRecommendations`). The REST API supports pagination parameters (`offset` and `limit`) to allow retrieval of the complete recommendation set. The Web UI displays the top 20 with a "Show All" toggle.

### 5. Streaming Query DataFrames — Exclusion from Recommendations

A DataFrame used in a Structured Streaming query is detected as repeatedly computed (per micro-batch). The system excludes streaming query plans from caching recommendations since Structured Streaming has its own state management and caching would interfere with checkpoint recovery. Streaming query plans are identified by the presence of `StreamingExecutionRelation` or `StreamingDataSourceV2ScanRelation` nodes in the logical plan tree.

## Dependencies

- **FEATURE-001-01 — Query Execution Profiling**: Provides execution time and output size metrics per query that feed into the cache opportunity detector's recomputation tracking and memory footprint estimation
  - `Source: tickets/EPIC-001/FEATURE-001-01-query-execution-profiling.md`
- **FEATURE-001-02 — Automated Optimization Recommendations**: Parent feature encompassing all five optimization recommendation stories, including this cache opportunity detection story
  - `Source: tickets/EPIC-001/FEATURE-001-02-automated-optimization-recommendations.md`
- **EPIC-001 — Adaptive Query Performance Insights Engine**: Parent epic defining the overarching goal of automated profiling and recommendations to reduce manual SQL optimization effort by up to 60%
  - `Source: tickets/EPIC-001-adaptive-query-performance-insights.md`
- **F-002 — SQL Query Processing Engine**: QueryExecution lifecycle, normalized plan representation, and `sameResult` semantics used for plan matching
  - `Source: sql/catalyst/` — logical plan trees, plan normalization
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/` — QueryExecution, physical plan operators
- **CacheManager**: Cache state management including `lookupCachedData` (check if a plan is cached), `cacheQuery` (cache a plan), `recacheByPlan` / `recacheByPath` (refresh cache); the `CachedData` model containing a normalized plan and `InMemoryRelation`
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/CacheManager.scala`
- **BlockManager**: Storage block management providing `memoryStore.memoryUsed`, `memoryStore.maxMemory`, `maxOnHeapMemory`, `maxOffHeapMemory`, and `diskStore` for disk spill estimation
  - `Source: core/src/main/scala/org/apache/spark/storage/BlockManager.scala`
- **Caching Documentation**: Caching API reference covering `spark.catalog.cacheTable()`, `dataFrame.cache()`, in-memory columnar storage, `dataFrame.unpersist()`, `spark.sql.inMemoryColumnarStorage.compressed`, and `spark.sql.inMemoryColumnarStorage.batchSize`
  - `Source: docs/sql-performance-tuning.md`
- **SQLAppStatusStore**: Execution data persistence layer for tracking per-query metrics and storing cache recommendation results
- **Spark Web UI Storage Tab**: Display surface for cache recommendations alongside current cache state information

## Story Estimation Guidance

- **Story Points**: 8 (Fibonacci scale)
- **Justification**: This story carries higher complexity than the data skew detection (STORY-001-02-01) and partition optimization (STORY-001-02-03) stories because:
  1. Tracking plan executions across a session requires a persistent in-memory registry with plan normalization and hash comparison using `sameResult` semantics
  2. Memory footprint estimation involves querying `BlockManager` metrics (`memoryStore.memoryUsed`, `memoryStore.maxMemory`) and performing capacity calculations against available executor storage memory
  3. Distinguishing between trivial and significant computations requires time-based thresholds with configurable cutoff values
  4. Handling already-cached datasets requires integration with the existing `CacheManager` via `lookupCachedData` to avoid duplicate recommendations
  5. Streaming query exclusion adds conditional logic to filter out plans containing `StreamingExecutionRelation` nodes
  6. The `CacheManager` provides a reference implementation for plan matching (`sameResult` semantics in `lookupCachedDataInternal`) but the session-scoped tracking and recommendation generation logic is new
- **Fit**: Completable within a single sprint by one engineer with familiarity in the Spark SQL execution layer

## Definition of Done

- [ ] Cache opportunity detector identifies DataFrames and tables computed 2 or more times (configurable via `spark.sql.insights.cache.minRecomputations`) within a SparkSession
- [ ] Each recommendation includes: plan description, execution count, cumulative execution time, estimated cache size, recommended storage level (MEMORY_ONLY or MEMORY_AND_DISK), estimated time savings, and API syntax (`dataFrame.cache()` or `spark.catalog.cacheTable("tableName")`)
- [ ] Already-cached datasets (verified via `CacheManager.lookupCachedData`) are excluded from recommendations and reported in a separate "currently cached" summary showing storage level, cached size, and cache hit count
- [ ] Trivial computations (each execution completing in less than 100 milliseconds) are categorized as "informational" rather than "recommended"
- [ ] Memory-constrained scenarios where estimated cache size exceeds available executor storage memory recommend MEMORY_AND_DISK with disk spill estimates
- [ ] Streaming query plans (containing `StreamingExecutionRelation` or `StreamingDataSourceV2ScanRelation`) are excluded from caching recommendations
- [ ] Results are persisted in `SQLAppStatusStore` and accessible via REST API endpoint at `/api/v1/applications/[app-id]/cache-recommendations`
- [ ] Spark Web UI Storage tab includes a cache recommendations panel showing ranked candidates sorted by estimated time savings
- [ ] Unit tests cover: no executions, single execution, duplicate execution, already-cached dataset, trivial computation, memory constraint, plan normalization, streaming exclusion, and concurrent session isolation
- [ ] Integration test validates end-to-end detection for a notebook-style workflow with 3 executions of the same join query, verifying recommendation accuracy against expected plan hash, execution count, and time savings
- [ ] No forbidden terms appear in any acceptance criteria text (all 13 prohibited terms verified absent from AC-1 through AC-6)
- [ ] Story is demo-able: a data scientist can run the same DataFrame computation 3 times in a notebook, navigate to the Storage tab, and see the caching recommendation with the specific API call syntax and estimated time savings
