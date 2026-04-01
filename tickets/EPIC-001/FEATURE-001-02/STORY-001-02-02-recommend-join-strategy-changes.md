# Recommend Join Strategy Changes Based on Runtime Data Sizes to Reduce Shuffle and I/O Overhead

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

**As a** data engineer, **I want to** receive automated join strategy recommendations that compare the current join strategy used for each join operation in a query against the optimal strategy determined from actual runtime data sizes, including specific join hint syntax to apply the recommended change, **so that** I can eliminate manual join strategy tuning that requires deep knowledge of Spark internals and data size estimation, reducing query optimization cycles from days to minutes and achieving 2-10x performance improvement for queries with suboptimal join strategies (e.g., SortMergeJoin used when data fits within the 10MB broadcast threshold).

## Acceptance Criteria

### AC-1: Expected Output — Join Recommendation

- **Given** a completed SQL query that uses a SortMergeJoin for a join operation where one side's runtime data size is less than `spark.sql.autoBroadcastJoinThreshold` (default: 10485760 bytes / 10MB)
- **When** the join strategy analysis runs on the query execution profile
- **Then** the system recommends switching to BroadcastHashJoin and provides: the join node ID, current strategy (SortMergeJoin), recommended strategy (BroadcastHashJoin), left side data size in bytes, right side data size in bytes, estimated performance gain category ("high" for broadcast-eligible joins), and the specific SQL hint syntax (`/*+ BROADCAST(table_name) */`)

### AC-2: Expected Output — Multiple Join Analysis

- **Given** a query execution plan containing 3 or more join operations with different join strategies
- **When** the join strategy analysis runs
- **Then** the system evaluates each join independently and returns a list of recommendations sorted by estimated performance impact (highest impact first), including joins where the current strategy is already optimal (marked as "no change recommended")

### AC-3: Input Validation — Broadcast Threshold

- **Given** a data engineer sets `spark.sql.autoBroadcastJoinThreshold` to a value of -2 (values below -1 are invalid; -1 disables broadcasting, 0+ sets threshold in bytes)
- **When** the configuration is applied
- **Then** the system rejects the value with an error message stating that the minimum allowed value is -1 (which disables broadcast joins) and values must be -1 or a non-negative integer representing bytes

### AC-4: Error Handling — Incomplete Runtime Statistics

- **Given** a join operation in a query where one side's runtime data size is unknown because AQE was disabled (`spark.sql.adaptive.enabled = false`) and no statistics are collected
- **When** the join strategy analysis attempts to evaluate this join
- **Then** the system reports the join as "analysis incomplete — runtime statistics unavailable" and recommends enabling AQE or running `ANALYZE TABLE` to collect table statistics for more accurate recommendations

### AC-5: Expected Output — Join Hint Syntax Generation

- **Given** a recommended join strategy change from SortMergeJoin to ShuffledHashJoin for a join between tables "orders" and "customers"
- **When** the recommendation is generated
- **Then** the system includes the exact SQL hint syntax: `SELECT /*+ SHUFFLE_HASH(customers) */ ... FROM orders JOIN customers ...` and the equivalent DataFrame API hint: `orders.join(customers.hint("shuffle_hash"), ...)` where the hint target is the smaller side of the join

### AC-6: Edge Case — Self-Join

- **Given** a query contains a self-join where the same table appears on both sides of the join
- **When** the join strategy analysis runs
- **Then** the system analyzes the runtime sizes of both sides independently (which may differ due to preceding filters or projections) and provides a recommendation based on the actual intermediate data sizes, not the raw table size

### AC-7: Edge Case — Cross Join

- **Given** a query contains a cross join (Cartesian product) between two tables
- **When** the join strategy analysis runs
- **Then** the system reports the cross join with a warning that cross joins produce output size equal to the product of both sides' row counts, flags it as a potential performance concern, and does not recommend a strategy change since cross joins have limited strategy options (BroadcastNestedLoopJoin if one side fits broadcast threshold, CartesianProduct otherwise)

## Sub-Tasks

- [ ] **Sub-task 1: Implement JoinStrategyAnalyzer component** — Build a component that extracts all join nodes from a physical execution plan, including join type (Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti, Cross), current strategy class (BroadcastHashJoinExec, SortMergeJoinExec, ShuffledHashJoinExec, BroadcastNestedLoopJoinExec), and both sides' runtime data sizes from `MapOutputStatistics`.
  > Source: `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/DynamicJoinSelection.scala` — reference for extracting `MapOutputStatistics` from `ShuffleQueryStageExec`

- [ ] **Sub-task 2: Implement optimal strategy determination logic** — Mirror the `SparkStrategies.JoinSelection` decision tree to determine the optimal join strategy for each join node: check broadcast threshold (`spark.sql.autoBroadcastJoinThreshold` default 10MB), check sort-merge preference (`spark.sql.join.preferSortMergeJoin`), evaluate shuffled hash join viability based on `SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD`, and fall through to BroadcastNestedLoopJoin or CartesianProduct for non-equi-joins.
  > Source: `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala` — `JoinSelection` strategy (lines 178–326) containing the full join type decision tree: BroadcastHashJoin → ShuffledHashJoin → SortMergeJoin → CartesianProduct → BroadcastNestedLoopJoin

- [ ] **Sub-task 3: Implement join hint syntax generator** — Produce both SQL hint format (`/*+ BROADCAST(table) */`, `/*+ MERGE(table) */`, `/*+ SHUFFLE_HASH(table) */`, `/*+ SHUFFLE_REPLICATE_NL(table) */`) and DataFrame API hint format (`df.hint("broadcast")`, `df.hint("merge")`, `df.hint("shuffle_hash")`, `df.hint("shuffle_replicate_nl")`) for each recommendation.
  > Source: `docs/sql-performance-tuning.md` — join strategy hints documentation (BROADCAST, MERGE, SHUFFLE_HASH, SHUFFLE_REPLICATE_NL), also accepts aliases BROADCASTJOIN and MAPJOIN

- [ ] **Sub-task 4: Create JoinRecommendation data model** — Define a structured data model containing: join node ID, join type (Inner, LeftOuter, etc.), current strategy name, recommended strategy name, left side data size in bytes, right side data size in bytes, estimated impact category (high, medium, low, none), applicable SQL hint syntax string, and applicable DataFrame API hint syntax string.

- [ ] **Sub-task 5: Integrate join analysis into the query profiling pipeline** — Connect the `JoinStrategyAnalyzer` to the query profiling pipeline so that join recommendations are computed after query execution completes, and persist results in `SQLAppStatusStore` alongside existing execution metadata.

- [ ] **Sub-task 6: Add REST API endpoint for join recommendations** — Implement the endpoint `/api/v1/applications/[app-id]/sql/[execution-id]/join-recommendations` returning a JSON array of `JoinRecommendation` objects, sorted by estimated performance impact (highest first).

- [ ] **Sub-task 7: Write unit and integration tests** — Cover the following scenarios: broadcast-eligible join missed (SortMergeJoin when data is under 10MB), sort-merge vs shuffled-hash comparison, self-join analysis with differing intermediate sizes, cross-join handling, multi-join query with 3+ joins, AQE-disabled fallback with missing statistics, and all 4 join hint format generations (BROADCAST, MERGE, SHUFFLE_HASH, SHUFFLE_REPLICATE_NL). Include one integration test that validates end-to-end recommendation for a query using SortMergeJoin where one side is under the broadcast threshold.

## Edge Cases

### 1. Empty/Null Input — Zero Rows After Predicate Pushdown

A join operation has zero rows on one or both sides after predicate pushdown. The system reports "one or both sides are empty after filtering" and recommends reviewing filter conditions rather than changing join strategy, since any strategy completes in negligible time for empty inputs.

### 2. Boundary Values — Data Size at Exact Broadcast Threshold

One side of a join has runtime data size exactly equal to `spark.sql.autoBroadcastJoinThreshold` (10485760 bytes / 10MB by default). The system recommends BroadcastHashJoin since the threshold comparison uses less-than-or-equal semantics, and notes that the dataset is at the broadcast boundary and may exceed the threshold with data growth.

### 3. Invalid Input — Unrecognized Execution Strategy Class

A query plan node is identified as a join but has an unrecognized execution strategy class (e.g., from a custom `SparkStrategy` extension). The system reports the join with current strategy as "custom/unknown" and skips recommendation generation for that specific join while continuing analysis of other joins in the plan.

### 4. Large Number of Joins — Star-Schema Queries with 20+ Joins

A query plan contains more than 20 join operations (common in star-schema queries). The system analyzes all joins and groups recommendations by impact category, providing a summary count (e.g., "3 high-impact, 5 medium-impact, 12 no-change") at the top of the results.

### 5. Broadcast Timeout Risk — Dataset Near Threshold

A recommendation to switch to BroadcastHashJoin is generated for a dataset close to the broadcast threshold. The system includes a note about `spark.sql.broadcastTimeout` (default 300 seconds) and warns that broadcast may timeout for datasets near the threshold on resource-constrained clusters.

## Dependencies

- **FEATURE-001-01** (Query Execution Profiling) — provides execution plan data and runtime statistics that serve as input for join strategy analysis
  - `Source: tickets/EPIC-001/FEATURE-001-01-query-execution-profiling.md`
- **FEATURE-001-02** (Automated Optimization Recommendations) — parent feature defining the recommendation engine scope
  - `Source: tickets/EPIC-001/FEATURE-001-02-automated-optimization-recommendations.md`
- **EPIC-001** (Adaptive Query Performance Insights Engine) — parent epic
  - `Source: tickets/EPIC-001-adaptive-query-performance-insights.md`
- **F-002** (SQL Query Processing Engine) — Catalyst optimizer and SparkStrategies for join selection logic
- **SparkStrategies.JoinSelection** — contains the join type decision tree that determines strategy selection order: BroadcastHashJoin → ShuffledHashJoin → SortMergeJoin → BroadcastNestedLoopJoin → CartesianProduct
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala`
- **DynamicJoinSelection** — runtime join re-selection logic using `MapOutputStatistics` to apply NO_BROADCAST_HASH, PREFER_SHUFFLE_HASH, and SHUFFLE_HASH hints based on partition statistics
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/DynamicJoinSelection.scala`
- **SQL Performance Tuning Documentation** — join strategy hints (BROADCAST, MERGE, SHUFFLE_HASH, SHUFFLE_REPLICATE_NL), `spark.sql.autoBroadcastJoinThreshold` (default 10485760 bytes / 10MB), `spark.sql.broadcastTimeout` (default 300 seconds)
  - `Source: docs/sql-performance-tuning.md`
- **SQLAppStatusStore** — execution data persistence layer for storing and retrieving join recommendations
- **Spark Web UI SQL tab** — primary display surface for rendering join strategy recommendations

## Story Estimation Guidance

- **Story Points:** 5 (Fibonacci scale)
- **Justification:** Moderate complexity — the join strategy recommendation logic mirrors the existing `JoinSelection` and `DynamicJoinSelection` decision trees, reducing algorithmic uncertainty. The main effort is in extracting runtime sizes from materialized shuffle stages via `MapOutputStatistics`, generating correct hint syntax for all 4 join strategy types across both SQL and DataFrame APIs, handling edge cases (self-joins, cross-joins, unknown strategies, empty inputs, threshold boundaries), and writing comprehensive tests. Integration with the profiling pipeline and the REST API follows patterns established by STORY-001-02-01 (Detect Data Skew Patterns). This story fits within a single sprint for one experienced engineer familiar with the Catalyst optimizer and physical plan structure.

## Definition of Done

- [ ] Join strategy analyzer evaluates all join nodes in a query execution plan and compares the current strategy against the optimal strategy based on runtime data sizes
- [ ] Recommendations include the specific join hint syntax in both SQL hint format (`/*+ BROADCAST(table) */`, `/*+ MERGE(table) */`, `/*+ SHUFFLE_HASH(table) */`, `/*+ SHUFFLE_REPLICATE_NL(table) */`) and DataFrame API format (`df.hint("broadcast")`, `df.hint("merge")`, `df.hint("shuffle_hash")`, `df.hint("shuffle_replicate_nl")`)
- [ ] Results are persisted in SQLAppStatusStore and accessible via the REST API endpoint `/api/v1/applications/[app-id]/sql/[execution-id]/join-recommendations`
- [ ] Each recommendation includes: join node ID, current strategy, recommended strategy, both sides' data sizes in bytes, estimated impact category (high, medium, low, none), and applicable hint syntax
- [ ] Edge cases handled: empty join sides (zero rows after filtering), boundary threshold values (data size exactly at `spark.sql.autoBroadcastJoinThreshold`), self-joins (independent intermediate size analysis), cross-joins (warning with limited strategy options), unknown strategy classes (reported as "custom/unknown" and skipped), broadcast timeout warnings (for datasets near threshold)
- [ ] Unit tests cover all 4 join strategies (BroadcastHashJoin, SortMergeJoin, ShuffledHashJoin, BroadcastNestedLoopJoin) with both sub-threshold and over-threshold data sizes
- [ ] Integration test validates end-to-end join recommendation for a query using SortMergeJoin where one side is under the 10MB broadcast threshold
- [ ] No forbidden terms appear in any acceptance criteria text
- [ ] Story is demo-able: a data engineer can run a query with a suboptimal join, navigate to the SQL execution page, and see the join recommendation with the correct hint syntax to apply
