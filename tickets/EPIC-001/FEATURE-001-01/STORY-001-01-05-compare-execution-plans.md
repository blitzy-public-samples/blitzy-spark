# Compare Two SQL Execution Plans Side-by-Side to Identify Optimization Impacts

## User Story

**As a** data engineer,

**I want to** select two SQL query execution plans and view them side-by-side in the Spark Web UI, with differences in operators, metrics, and plan structure highlighted visually — added operators in green, removed operators in red, and modified operators in amber,

**so that** I can measure the impact of query optimizations (such as adding broadcast hints, changing join strategies, or modifying partition counts) by comparing execution plans before and after changes, reducing the optimization validation cycle from multiple hours of manual plan inspection to under 10 minutes per comparison.

---

## Acceptance Criteria

### AC-1: Side-by-Side Plan Display

- **Given** two completed SQL queries with execution IDs 10 and 20 are stored in the `SQLAppStatusStore`
- **When** the data engineer navigates to the plan comparison page at `/sql/compare?left=10&right=20`
- **Then** the Web UI displays two execution plan DAG visualizations side-by-side, with the left panel showing the plan for execution ID 10 and the right panel showing the plan for execution ID 20, each rendered using the existing `SparkPlanGraph` DOT-based visualization via `makeDotFile`

> `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SparkPlanGraph.scala` — `SparkPlanGraph.makeDotFile(metrics)` generates DOT graph output for rendering

### AC-2: Structural Difference Highlighting

- **Given** two execution plans where the left plan uses a `SortMergeJoin` operator and the right plan uses a `BroadcastHashJoin` operator for the same logical join
- **When** the comparison page renders both plans
- **Then** the changed join operator is highlighted in amber (#FFC107) in both panels, and a differences summary panel below the visualizations lists each structural change with the following columns: change type (ADDED, REMOVED, or MODIFIED), operator name, operator ID, and for MODIFIED operators the specific attribute that changed (e.g., "Join type changed from SortMergeJoin to BroadcastHashJoin")

> `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SparkPlanGraph.scala` — `SparkPlanGraphNode` fields `id`, `name`, and `desc` (simpleString) are used for structural matching

### AC-3: Input Validation for Identical Execution IDs

- **Given** a comparison request where both `left` and `right` parameters reference the same execution ID (e.g., `/sql/compare?left=10&right=10`)
- **When** the page processes the request
- **Then** the page displays a validation message stating "Left and right execution IDs must be different to perform a comparison" and does not render the comparison layout

### AC-4: Error Handling for Missing Execution ID

- **Given** a comparison request where one of the execution IDs does not exist in the `SQLAppStatusStore` (e.g., left=10 exists, right=999 does not)
- **When** the page processes the request
- **Then** the page displays an error message "Execution ID 999 not found" identifying the missing execution, renders the available plan (ID 10) in a single panel, and provides a link back to the All Executions page at `/sql/` to select a valid query

> `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala` — `execution(executionId: Long): Option[SQLExecutionUIData]` returns `None` when the execution ID does not exist

### AC-5: Identical Plans with No Structural Differences

- **Given** two queries that produce structurally identical execution plans (same operators, same order, same join types)
- **When** the data engineer views the comparison
- **Then** the comparison page displays both plans with no highlighted differences, and the differences summary panel shows the message "No structural differences detected between the two execution plans" with an option to toggle metric value comparison (e.g., execution time, rows processed) instead

### AC-6: Metric Comparison with Percentage Delta

- **Given** two execution plans displayed side-by-side with the "Compare Metrics" toggle enabled
- **When** the data engineer activates the "Compare Metrics" toggle
- **Then** each operator node in both plans displays its key metrics (output rows, data size in bytes, execution time in milliseconds) and operators where the right plan's metric value differs from the left plan's value by more than 10% are annotated with a delta indicator showing the percentage change (e.g., "+45% output rows", "−30% execution time")

> `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SparkPlanGraph.scala` — `SQLPlanMetric(name, accumulatorId, metricType)` stores per-operator metric definitions used for metric comparison computation

---

## Sub-Tasks

1. **Design plan comparison algorithm**: Implement a structural diff algorithm that compares two `SparkPlanGraph` instances by matching nodes based on operator name, position in the tree, and parent relationships using the `SparkPlanGraphNode` fields `id`, `name`, and `desc` (simpleString). Use heuristic matching by operator name and tree depth to avoid O(N²) worst-case complexity.

   > `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SparkPlanGraph.scala`

2. **Implement PlanDiffResult data structure**: Create a `PlanDiffResult` case class containing three lists — `addedOperators`, `removedOperators`, and `modifiedOperators` — where each entry stores the operator name, operator ID, and a human-readable change description string.

3. **Create PlanComparisonPage**: Build a new Web UI page `PlanComparisonPage` registered under the SQL tab at the path `/sql/compare` that accepts `left` and `right` query parameters for execution IDs. Follow the pattern established by `ExecutionPage` which extends `WebUIPage` and uses `SQLAppStatusStore` for data retrieval.

   > `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/ExecutionPage.scala` — reference implementation for single-plan rendering page structure

4. **Extend AllExecutionsPage with comparison selection UI**: Add checkboxes for selecting two queries in the `AllExecutionsPage` execution listing table and a "Compare Plans" button that constructs the comparison URL and navigates to `/sql/compare?left=[id1]&right=[id2]`.

   > `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/ExecutionPage.scala` — existing execution listing used as the selection surface

5. **Render side-by-side plan DAGs**: Display two instances of the existing D3/dagre-d3 DOT renderer using `SparkPlanGraph.makeDotFile` output, with CSS color overlays applied to nodes based on diff results: green (#4CAF50) for ADDED nodes, red (#F44336) for REMOVED nodes, and amber (#FFC107) for MODIFIED nodes.

6. **Implement differences summary table**: Render a summary table below the DAG panels listing all structural changes with columns: Change Type, Operator Name, Operator ID, and Change Detail.

7. **Add metric comparison mode**: Implement a toggle that computes percentage differences between corresponding operator metrics using `SQLPlanMetric` data from each `SparkPlanGraphWrapper`. Annotate operators where the metric delta exceeds 10% with formatted percentage indicators.

   > `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala` — `executionMetrics(executionId)` returns `Map[Long, String]` of metric values

8. **Add REST API endpoint**: Create a `GET /api/v1/applications/[app-id]/sql/compare?left=[id]&right=[id]` endpoint returning a JSON response containing the structural diff results and optional metric comparison data.

9. **Handle AQE-evolved plans**: When comparing a pre-AQE initial plan with the final executed plan from the same query, retrieve the initial plan from `AdaptiveSparkPlanExec.initialPlan` and the executed plan from `AdaptiveSparkPlanExec.executedPlan`, and label each detected change as "AQE Optimization" in the change description.

   > `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlanInfo.scala` — handles `AdaptiveSparkPlanExec` by extracting `executedPlan` children

10. **Write unit tests for plan diff algorithm**: Cover the following cases — identical plans (zero differences), one added operator, one removed operator, changed join type (SortMergeJoin → BroadcastHashJoin), changed partition count, and AQE-evolved plan comparison.

11. **Write Selenium/WebUI integration tests**: Validate that the comparison page renders two DAG panels, that difference highlighting colors are applied to the correct nodes, that the differences summary table contains the expected entries, and that the metric comparison toggle functions as specified.

---

## Edge Cases

### 1. Empty/Null Input — Query with No Physical Plan

Comparing a completed query plan with a query that has no physical plan (e.g., a failed query that did not reach the `sparkPlan` phase in the `QueryExecution` lifecycle) must display the available plan on one side and a message "No physical plan available for execution ID [id] — query failed during analysis" on the other side, without throwing an unhandled exception.

> `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlanInfo.scala` — plan info is generated from `SparkPlan`; a failed query before the physical planning phase will not have a `SparkPlanInfo` representation

### 2. Boundary Values — Very Different Plan Sizes

Comparing two plans where one has 5 operators and the other has 500 operators must render both plans with independent vertical scrolling per panel. The diff algorithm must complete the structural comparison within 5 seconds for plans with up to 500 operators each and must not generate more than N² node comparisons — instead using heuristic matching by operator name and tree depth to maintain linear-time performance in the common case.

### 3. Invalid Input — Non-Numeric Execution IDs

Providing non-numeric values in the comparison URL (e.g., `/sql/compare?left=abc&right=def`) must return an HTTP 400 response with the message "Invalid execution ID format: left and right parameters must be numeric" rather than producing an unhandled `NumberFormatException` or server error page.

### 4. Boundary Values — AQE Initial vs Final Plan Comparison

When comparing a query's initial plan (before Adaptive Query Execution) with its final executed plan (after AQE), the diff algorithm must detect AQE-specific changes such as converted join types (SortMergeJoin → BroadcastHashJoin), coalesced partition counts, and skew-handled shuffle stages. Each AQE-induced change must be labeled as "AQE Optimization" in the change description column of the differences summary table, distinguishing AQE changes from user-initiated modifications.

> `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ExplainUtils.scala` — operator ID assignment and plan text rendering support AQE plan analysis

### 5. Concurrent Access — Simultaneous Comparison Requests

When two data engineers simultaneously access comparison pages for different execution ID pairs (e.g., one views `/sql/compare?left=10&right=20` while another views `/sql/compare?left=30&right=40`), each page must render the correct pair of plans without cross-contamination of comparison state. The comparison computation must be stateless per request, with no shared mutable state between concurrent page renders.

---

## Dependencies

### Parent References

- **EPIC-001** — [Adaptive Query Performance Insights Engine](../../EPIC-001-adaptive-query-performance-insights.md): Parent epic defining the overall scope of the performance insights initiative
- **FEATURE-001-01** — [Query Execution Profiling](../FEATURE-001-01-query-execution-profiling.md): Parent feature encompassing plan capture, timeline visualization, resource metrics, report export, and plan comparison
- **STORY-001-01-01** — [Capture Query Execution Plans](./STORY-001-01-01-capture-query-execution-plans.md): Plans must be captured and persisted in the `SQLAppStatusStore` before comparison is possible; this story is a prerequisite

### Feature Dependencies

- **F-002 — SQL Query Processing Engine**: Provides the query plan structure, Catalyst optimizer rules, physical plan operators, and the `QueryExecution` lifecycle that produces the plans being compared

### Source Code Dependencies

| Source File | Key Components Used | Purpose |
|---|---|---|
| `sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SparkPlanGraph.scala` | `SparkPlanGraph`, `SparkPlanGraphNode` (id, name, desc, metrics), `SparkPlanGraphCluster`, `SparkPlanGraphEdge` (fromId, toId), `makeDotFile`, `SQLPlanMetric` | Graph model for plan DAG rendering and structural node matching |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlanInfo.scala` | `SparkPlanInfo` (nodeName, simpleString, children, metadata, metrics), `fromSparkPlan`, `AdaptiveSparkPlanExec` handling | Serializable plan representation used for structural comparison input |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/ExplainUtils.scala` | `localIdMap`, `processPlanSkippingSubqueries`, operator ID assignment | Operator ID assignment and plan text rendering for textual diff fallback |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala` | `planGraph(executionId)`, `execution(executionId)`, `executionMetrics(executionId)`, `SparkPlanGraphWrapper` | Plan graph retrieval and execution data lookup from the KVStore |
| `sql/core/src/main/scala/org/apache/spark/sql/execution/ui/ExecutionPage.scala` | `ExecutionPage` extends `WebUIPage("execution")`, `SQLTab`, plan rendering pattern | Reference implementation for single-plan page structure and rendering approach |

### Client-Side Dependencies

- **D3.js / dagre-d3**: Client-side DAG visualization libraries bundled with the Spark Web UI, used to render the DOT graph output from `SparkPlanGraph.makeDotFile` in dual-panel layout

---

## Story Estimation Guidance

**Story Points: 13** (Fibonacci scale)

**Rationale:** This is the most complex story in the FEATURE-001-01 feature set. Implementation requires:

- A novel plan diff algorithm performing structural tree comparison between two `SparkPlanGraph` instances, with heuristic matching by operator name and tree depth to avoid O(N²) performance degradation
- A new Web UI page (`PlanComparisonPage`) with dual-panel DAG rendering using two independent D3/dagre-d3 graph instances
- Color-coded difference highlighting with CSS overlay management for ADDED (green), REMOVED (red), and MODIFIED (amber) nodes
- A metric comparison mode computing percentage deltas across corresponding operator metrics from `SQLPlanMetric` data
- A REST API endpoint returning structured JSON comparison results
- Special handling for AQE plan evolution (initial plan vs executed plan) with detection of AQE-specific optimizations
- Input validation, error handling, and concurrent access safety for stateless request processing

**Estimated effort:** 8–13 development days for a senior engineer experienced with tree algorithms, Spark Web UI architecture, and D3.js visualization. The plan diff algorithm design and dual-panel rendering integration represent the highest-risk components.

---

## Definition of Done

- [ ] Comparison page renders two execution plans side-by-side at `/sql/compare?left=[id]&right=[id]` with each plan displayed as a DAG visualization
- [ ] Structural differences are highlighted with distinct colors: ADDED operators in green (#4CAF50), REMOVED operators in red (#F44336), and MODIFIED operators in amber (#FFC107)
- [ ] Differences summary panel below the visualizations lists all changes with columns: change type, operator name, operator ID, and change detail description
- [ ] Metric comparison mode (activated via "Compare Metrics" toggle) shows percentage delta annotations for operators where the metric value differs by more than 10% between left and right plans
- [ ] Same-ID comparison (e.g., `left=10&right=10`) is rejected with the validation message "Left and right execution IDs must be different to perform a comparison"
- [ ] Missing execution ID displays an informative error message ("Execution ID [id] not found") with the available plan rendered in a single panel and a navigation link back to the All Executions page
- [ ] Identical plans display "No structural differences detected between the two execution plans" with an option to compare metric values
- [ ] REST API endpoint `GET /api/v1/applications/[app-id]/sql/compare?left=[id]&right=[id]` returns comparison data in JSON format including structural diff and metric comparison results
- [ ] AQE plan evolution comparison (initial vs final plan) is supported with each AQE-induced change labeled as "AQE Optimization" in the differences summary
- [ ] `AllExecutionsPage` includes selection UI with checkboxes for choosing two queries and a "Compare Plans" button that navigates to the comparison page
- [ ] Plan diff algorithm completes within 5 seconds for plans with up to 500 operators each, using heuristic matching to avoid O(N²) worst-case complexity
- [ ] Unit tests cover the diff algorithm for the following cases: identical plans, added operator, removed operator, modified join type, changed partition count, and AQE-evolved plans
- [ ] Selenium integration tests validate comparison page rendering, difference highlighting, differences summary table content, and metric comparison toggle interaction
- [ ] Code review approved by at least 2 committers
- [ ] Documentation covers comparison page navigation, URL parameter format, interpretation of diff results (color codes and change types), and metric comparison usage
