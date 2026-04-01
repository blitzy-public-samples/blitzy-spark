# Capture and Persist Query Execution Plans for Completed SQL Queries

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

**As a** data engineer,
**I want to** capture and persist the logical and physical execution plans for every completed SQL query,
**so that** I can review the complete plan hierarchy (analyzed → optimized → physical → executed) after query completion, reducing post-mortem debugging time by up to 40% and enabling systematic query optimization without re-running queries.

## Acceptance Criteria

### AC-1: Plan Capture on Query Completion (Expected Output)

- **Given** a SparkSession with query profiling enabled via `spark.sql.profiling.enabled=true`
- **When** a SQL query completes execution
- **Then** the system persists both the logical plan (analyzed, optimized) and the physical plan (sparkPlan, executedPlan) to the SQLAppStatusStore within 2 seconds of query completion

### AC-2: Empty Query Input Rejection (Input Validation)

- **Given** a SQL query string submitted for execution
- **When** the query string is empty or contains only whitespace characters
- **Then** the system rejects the query with an `AnalysisException` before plan capture is attempted and no empty plan record is persisted to the store

### AC-3: Plan Rendering Across All Explain Modes (Expected Output)

- **Given** a completed query with a persisted execution plan
- **When** a data engineer requests the plan via the `QueryExecution.explainString(mode)` API with mode set to one of the 5 supported `ExplainMode` values (`simple`, `extended`, `codegen`, `cost`, `formatted`)
- **Then** the system returns a text representation that includes operator IDs, operator names, and input/output column attributes for each node in the plan tree

### AC-4: Partial Plan Persistence on Analysis Failure (Error Handling)

- **Given** a SQL query that fails during the analysis phase with an `AnalysisException`
- **When** the system attempts to capture the execution plan
- **Then** the system persists a partial plan record containing the analyzed logical plan up to the point of failure, sets the execution status to `FAILED`, records the error message, and does not persist any physical plan fields

### AC-5: AQE-Modified Plan Capture (Edge Case)

- **Given** a query executing with Adaptive Query Execution enabled (`spark.sql.adaptive.enabled=true`)
- **When** the physical plan is modified at runtime by AQE (e.g., join strategy change from SortMergeJoin to BroadcastHashJoin, partition coalescing)
- **Then** the system captures and persists the final `executedPlan` reflecting all AQE modifications rather than the initial `sparkPlan`, and both the initial and final plans are available for retrieval

### AC-6: Retrieval Performance and Plan Completeness (Expected Output)

- **Given** 100 completed queries in a single SparkSession
- **When** the data engineer retrieves the plan for query ID N (where 1 ≤ N ≤ 100)
- **Then** the system returns the plan within 500 milliseconds and the plan includes the complete operator tree with node names, `simpleString` representations, child relationships, metadata maps, and `SQLPlanMetric` sequences matching the original `SparkPlanInfo` structure

## Sub-Tasks

- Extend `QueryExecution` to emit a plan-captured event upon transition from `executedPlan` to `toRdd` phase, including serialized `SparkPlanInfo` for both logical and physical plans
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala`
- Implement a plan persistence adapter in `SQLAppStatusStore` that writes `SparkPlanGraphWrapper` records keyed by execution ID to the KVStore backend
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala`
- Add a plan retrieval API method `getPlanDetails(executionId: Long): Option[PlanDetailsRecord]` that returns logical plan text, physical plan text, and `SparkPlanGraph` for a given query
- Integrate plan capture with the existing `SQLAppStatusListener` to intercept `SparkListenerSQLExecutionEnd` events and trigger persistence
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusListener.scala`
- Add support for all 5 `ExplainMode` rendering formats (`simple`, `extended`, `codegen`, `cost`, `formatted`) in the plan retrieval API
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ExplainMode.scala`
- Handle AQE plan evolution by capturing the final `executedPlan` from `AdaptiveSparkPlanExec` after all AQE stages complete
- Write unit tests for plan capture with simple queries (`SELECT`), join queries, aggregate queries, and subqueries
- Write integration tests validating plan persistence survives SparkSession restart via History Server replay

## Edge Cases

### Empty/Null Input

A query that resolves to an `EmptyRelation` (e.g., `SELECT * FROM table WHERE 1=0`) must still produce a valid, persisted plan with an `EmptyRelationExec` node and zero output rows in metrics. No `NullPointerException` or missing record is permitted for queries returning zero rows.

### Boundary Values

A query plan with more than 1,000 operator nodes (deeply nested subqueries or large `UNION ALL` chains) must be captured and persisted without truncation; the `SparkPlanInfo` tree must contain all nodes with assigned operator IDs and no nodes silently dropped.

### Invalid Input

A syntactically invalid SQL string (e.g., `SELEC * FORM table`) must be rejected during parsing before any plan capture occurs, and no plan record is written to the `SQLAppStatusStore` for that execution ID. The parser error message must be propagated to the caller.

### Large Plan Metadata

A file scan query reading from 10,000+ partitioned files must capture the complete metadata map (file paths, partition filters, data filters) without exceeding the KVStore value size limit; if metadata exceeds 10 MB, it is truncated with a `[truncated]` marker appended to the metadata value string.

### Concurrent Queries

When 50 queries execute concurrently in the same SparkSession, each query's plan is captured independently with the assigned execution ID and no plan data is mixed between queries. Thread-safe access to the KVStore must be validated under concurrent write loads.

## Dependencies

- **EPIC-001** — Adaptive Query Performance Insights Engine (parent epic)
  - `Source: tickets/EPIC-001-adaptive-query-performance-insights.md`
- **FEATURE-001-01** — Query Execution Profiling (parent feature)
  - `Source: tickets/EPIC-001/FEATURE-001-01-query-execution-profiling.md`
- **F-002** — SQL Query Processing Engine: provides the `QueryExecution` lifecycle, Catalyst optimizer, `ExplainUtils`, and `ExplainMode`
- **QueryExecution** — query lifecycle phases: `analyzed` → `optimized` → `sparkPlan` → `executedPlan` → `toRdd`
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala`
- **ExplainUtils** — operator ID assignment, plan text rendering with codegen stage annotations
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ExplainUtils.scala`
- **ExplainMode** — 5 explain modes: `simple`, `extended`, `codegen`, `cost`, `formatted`
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ExplainMode.scala`
- **SparkPlanInfo** — serializable plan representation with `nodeName`, `simpleString`, `children`, `metadata`, `metrics`
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlanInfo.scala`
- **SQLAppStatusStore** — KVStore persistence layer for `SQLExecutionUIData` and `SparkPlanGraphWrapper`
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala`
- **SQLAppStatusListener** — listener for SQL execution events (`SparkListenerSQLExecutionStart`, `SparkListenerSQLExecutionEnd`)
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusListener.scala`
- **SparkPlanGraph** — graph-based plan model for visualization in the Spark Web UI
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SparkPlanGraph.scala`

## Story Estimation Guidance

- **Story Points:** 8 (Fibonacci scale)
- **Rationale:** Requires modifications to the core `QueryExecution` lifecycle (high complexity), a new persistence adapter in `SQLAppStatusStore`, integration with the existing `SQLAppStatusListener` infrastructure, AQE plan evolution handling via `AdaptiveSparkPlanExec`, and comprehensive test coverage across multiple plan types (simple, join, aggregate, subquery, AQE). Estimated 5–8 development days for a senior engineer familiar with Catalyst internals.
- **Complexity Factors:**
  - Core lifecycle modification in `QueryExecution` (analyzed → optimized → sparkPlan → executedPlan → toRdd)
  - Thread-safe persistence to KVStore under concurrent query execution
  - AQE runtime plan evolution requires capturing the final `executedPlan` after all stages complete
  - 5 distinct `ExplainMode` rendering paths must be validated

## Definition of Done

- Logical and physical execution plans are captured for every completed SQL query when profiling is enabled via `spark.sql.profiling.enabled=true`
- Plans are persisted to the KVStore via `SQLAppStatusStore` and retrievable by execution ID
- All 5 `ExplainMode` formats (`simple`, `extended`, `codegen`, `cost`, `formatted`) produce valid output for captured plans
- AQE-modified plans capture the final `executedPlan` with all runtime optimizations applied, and both the initial `sparkPlan` and final `executedPlan` are stored
- Failed queries persist partial plan records with `FAILED` execution status and recorded error messages
- Unit tests cover: simple `SELECT` queries, join queries (broadcast and sort-merge), aggregate queries (`GROUP BY`), subqueries (scalar and correlated), `EmptyRelation` queries, and syntactically invalid SQL strings
- Integration tests validate plan capture with AQE enabled and AQE disabled configurations
- Performance benchmark confirms plan capture overhead is less than 5% for standard TPC-DS queries
- Code review approved by at least 2 committers
- Documentation updated with plan capture configuration parameters and API usage examples
