# Generate Optimization Summary Report Consolidating All Query Performance Recommendations

## User Story

**As a** data engineer, **I want to** generate a consolidated optimization summary report for a specific SQL query execution (or for all queries in a session) that aggregates all detected issues and recommendations — including data skew patterns, join strategy changes, partition optimization suggestions, and cache opportunities — into a single structured report with severity rankings, estimated performance impact percentages, and prioritized actionable steps, **so that** I obtain a single-view performance improvement plan instead of reviewing multiple individual analyses, reducing the optimization review process from examining 4 separate recommendation categories to a single prioritized action list, and enabling team-wide sharing of optimization findings through exportable JSON and CSV formats.

## Acceptance Criteria

### AC-1: Consolidated Report Structure (Expected Output)

**Given** a completed SQL query execution that has been analyzed by all 4 recommendation engines (skew detection, join strategy, partition optimization, cache opportunities)
**When** the optimization summary report is generated for that execution ID
**Then** the report contains: (1) an executive summary with total issue count by severity (high, medium, low), estimated total performance improvement percentage, and top 3 prioritized actions; (2) a skew analysis section listing all skewed partitions with severity and affected stages; (3) a join strategy section listing all join recommendations with current and recommended strategies; (4) a partition optimization section listing all partition count recommendations; (5) a cache opportunities section listing all caching candidates with estimated time savings; and (6) a configuration changes section listing all recommended `spark.sql.*` parameter adjustments

### AC-2: JSON Export Format (Expected Output)

**Given** a generated optimization summary report
**When** the report is exported in JSON format via the REST API endpoint `/api/v1/applications/[app-id]/sql/[execution-id]/optimization-report`
**Then** the JSON response contains a structured object with fields: `executionId` (long), `queryText` (string, truncated to 1000 characters), `timestamp` (ISO 8601), `summary` (object with `totalIssues`, `highSeverity`, `mediumSeverity`, `lowSeverity`, `estimatedImprovementPercent`), `recommendations` (array of objects each containing `category`, `severity`, `description`, `currentState`, `recommendedAction`, `estimatedImpact`, and `actionSyntax`), and `configurationChanges` (array of objects each containing `parameter`, `currentValue`, `recommendedValue`, `rationale`)

### AC-3: CSV Export Format (Expected Output)

**Given** a generated optimization summary report
**When** the report is exported in CSV format via the REST API endpoint `/api/v1/applications/[app-id]/sql/[execution-id]/optimization-report?format=csv`
**Then** the CSV file contains one row per recommendation with columns: ExecutionID, Category, Severity, Description, CurrentState, RecommendedAction, EstimatedImpact, ActionSyntax — and uses RFC 4180 compliant formatting with proper escaping for fields containing commas or newlines

### AC-4: Execution ID Validation (Input Validation)

**Given** a data engineer requests an optimization report for a non-existent execution ID (e.g., execution ID 99999 when only IDs 1-10 exist)
**When** the report generation is invoked
**Then** the system returns an HTTP 404 response with a JSON error body containing `errorCode: "EXECUTION_NOT_FOUND"` and `message: "No SQL execution found with ID 99999"`

### AC-5: Partial Analysis Results Handling (Error Handling)

**Given** a query execution where the skew analysis and join analysis completed but the partition analysis failed due to missing shuffle metrics and the cache analysis was not applicable (first execution in session)
**When** the optimization summary report is generated
**Then** the report includes the skew and join sections with full results, the partition section with status "analysis incomplete — shuffle metrics unavailable", and the cache section with status "not applicable — no repeated computations detected", and the executive summary reflects only the issues from completed analyses

### AC-6: Session-Level Aggregated Report (Expected Output)

**Given** a SparkSession with 5 completed SQL query executions, each with individual optimization analyses
**When** the data engineer requests a session-level summary report via `/api/v1/applications/[app-id]/optimization-report`
**Then** the report aggregates findings across all 5 executions, groups recommendations by category and severity, de-duplicates configuration change suggestions (e.g., a single recommendation for `spark.sql.shuffle.partitions` even if detected in multiple queries), and includes a per-query breakdown section

### AC-7: No Issues Detected Scenario (Edge Case)

**Given** a query execution where all 4 recommendation engines report no issues (no skew, optimal joins, optimal partitions, no caching candidates)
**When** the optimization summary report is generated
**Then** the report contains an executive summary stating "no optimization opportunities detected" with `totalIssues: 0` and `estimatedImprovementPercent: 0`, and each section reports its clean status (e.g., "all partitions within optimal range")

## Sub-Tasks

- [ ] **Sub-task 1:** Implement `OptimizationReportGenerator` component that queries `SQLAppStatusStore` for the execution's profiling data and collects results from the 4 recommendation engines (SkewDetector, JoinStrategyAnalyzer, PartitionAnalyzer, CacheOpportunityDetector)
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala`
- [ ] **Sub-task 2:** Implement severity aggregation and prioritization logic — rank recommendations by severity (high > medium > low), then by estimated impact percentage within each severity level; calculate total estimated improvement as weighted sum of individual impacts (accounting for non-additive effects where a skew fix may also reduce partition issues)
- [ ] **Sub-task 3:** Implement configuration change consolidation — extract all recommended Spark configuration parameter changes from individual recommendations, de-duplicate by parameter name (keeping the most impactful recommendation), and produce a unified configuration change list
  - `Source: docs/sql-performance-tuning.md` — AQE configuration parameters, join thresholds, partition tuning settings
  - `Source: docs/tuning.md` — serialization tuning, memory management, GC configuration
- [ ] **Sub-task 4:** Implement JSON report serialization using Jackson (version 2.20.0 per pom.xml) producing a structured JSON object with the schema defined in AC-2; ensure ISO 8601 timestamps, null handling for missing sections, and UTF-8 encoding
- [ ] **Sub-task 5:** Implement CSV report serialization following RFC 4180 with escaping; flatten the hierarchical recommendation structure into tabular rows with one recommendation per line
- [ ] **Sub-task 6:** Add REST API endpoints: (a) per-execution report at `/api/v1/applications/[app-id]/sql/[execution-id]/optimization-report` with `format` query parameter (json/csv, default json); (b) session-level report at `/api/v1/applications/[app-id]/optimization-report` aggregating all executions
  - `Source: docs/monitoring.md` — REST API endpoint patterns, metrics sinks (ConsoleSink, CSVSink, JmxSink, MetricsServlet, GraphiteSink)
- [ ] **Sub-task 7:** Add Spark Web UI panel in the SQL execution detail page showing the optimization summary report as an expandable card with severity-colored indicators and a "Download Report" button for JSON/CSV export
- [ ] **Sub-task 8:** Write unit tests covering: complete report with all 4 categories, partial report (some analyses incomplete), no-issues report, session-level aggregation, JSON schema validation, CSV RFC 4180 compliance, non-existent execution ID, and empty session

## Edge Cases

### 1. Empty/Null Input — No Query Executions in Session

A newly started SparkSession with no query executions — the session-level report endpoint returns an empty report with `totalExecutions: 0`, `totalIssues: 0`, and a message "no SQL executions recorded in the current application." The REST API responds with HTTP 200 and the empty report structure rather than an error code, since an empty session is a valid state.

### 2. Boundary Values — Exactly One Recommendation Per Category

A query execution has exactly 1 recommendation from each of the 4 categories (4 total recommendations) — the report includes all 4 with individual severity and impact, and the executive summary calculates the combined estimated improvement without double-counting overlapping benefits (e.g., if a skew fix and a partition fix target the same stage, their impact is not summed but uses the maximum of the two).

### 3. Invalid Input — Unsupported Export Format

The `format` query parameter in the REST API is set to an unsupported value (e.g., `format=xml`) — the system returns an HTTP 400 response with error message "unsupported format 'xml'; supported formats are 'json' and 'csv'." The error response body uses the same JSON error structure as AC-4 with `errorCode: "UNSUPPORTED_FORMAT"`.

### 4. Very Large Report — High-Volume Session Aggregation

A session with 100 query executions each generating 20 recommendations (2000 total recommendations) — the report includes pagination parameters (`offset`, `limit`) for the REST API response, with a default limit of 100 recommendations per page and a `totalCount` field indicating the full result count. The session-level configuration de-duplication reduces the configuration changes section to unique parameter recommendations regardless of total recommendation volume.

### 5. Concurrent Report Generation — Simultaneous Access

Two data engineers request the same execution's optimization report simultaneously — the system serves both requests from the persisted `SQLAppStatusStore` data without re-running the analyses, ensuring consistent results across concurrent reads. The `SQLAppStatusStore` is designed as a read-only view over the KVStore, supporting concurrent access without locking or serialization conflicts.

> `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala` — "Provides a view of a KVStore with methods that make it easy to query SQL-specific state. There's no state kept in this class, so it's ok to have multiple instances of it in an application."

## Dependencies

| Dependency | Type | Description |
|---|---|---|
| [STORY-001-02-01 — Detect Data Skew Patterns](./STORY-001-02-01-detect-data-skew-patterns.md) | Story | Provides skew analysis results (skewed partitions, severity, affected stages) for aggregation into the consolidated report |
| [STORY-001-02-02 — Recommend Join Strategy Changes](./STORY-001-02-02-recommend-join-strategy-changes.md) | Story | Provides join recommendation results (current vs recommended strategy, estimated impact) for aggregation into the consolidated report |
| [STORY-001-02-03 — Suggest Partition Optimization](./STORY-001-02-03-suggest-partition-optimization.md) | Story | Provides partition recommendation results (current vs recommended partition counts, data distribution analysis) for aggregation into the consolidated report |
| [STORY-001-02-04 — Identify Missing Cache Opportunities](./STORY-001-02-04-identify-missing-cache-opportunities.md) | Story | Provides cache recommendation results (caching candidates, estimated time savings, memory impact) for aggregation into the consolidated report |
| [FEATURE-001-01 — Query Execution Profiling](../FEATURE-001-01-query-execution-profiling.md) | Feature | Provides profiling data and execution metrics as the foundation for all analyses consumed by this report |
| [FEATURE-001-02 — Automated Optimization Recommendations](../FEATURE-001-02-automated-optimization-recommendations.md) | Feature | Parent feature defining the scope and context for this story |
| [EPIC-001 — Adaptive Query Performance Insights Engine](../../EPIC-001-adaptive-query-performance-insights.md) | Epic | Parent epic establishing the overarching goal of reducing manual SQL optimization effort |
| `SQLAppStatusStore` | Code | Execution data persistence layer — provides `executionsList()`, `execution()`, `executionMetrics()`, and `planGraph()` methods for retrieving query execution data used to populate report content. `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala` |
| `docs/tuning.md` | Documentation | Performance tuning reference content for contextualizing recommendations — serialization tuning (Kryo vs Java serialization), memory management (`spark.memory.fraction`, `spark.memory.storageFraction`), and GC guidance. `Source: docs/tuning.md` |
| `docs/sql-performance-tuning.md` | Documentation | SQL-specific tuning reference for configuration parameter recommendations — AQE settings (`spark.sql.adaptive.enabled`, `spark.sql.adaptive.coalescePartitions.*`, `spark.sql.adaptive.skewJoin.*`), join strategies (`spark.sql.autoBroadcastJoinThreshold`), partitioning (`spark.sql.shuffle.partitions`), and caching configuration (`spark.sql.inMemoryColumnarStorage.*`). `Source: docs/sql-performance-tuning.md` |
| `docs/monitoring.md` | Documentation | REST API documentation for `/api/v1/applications/[app-id]/...` endpoints used as the pattern for optimization report endpoints; metrics sinks (ConsoleSink, CSVSink, JmxSink, MetricsServlet, GraphiteSink) for report distribution. `Source: docs/monitoring.md` |
| Jackson JSON library (version 2.20.0) | External Library | JSON serialization for structured report export via the REST API |
| Spark Web UI | Infrastructure | Display surface for the optimization summary panel within the SQL execution detail page |

## Story Estimation Guidance

| Attribute | Value |
|---|---|
| **Story Points** | 8 |
| **Fibonacci Scale** | 1, 2, 3, 5, **8**, 13 |
| **Sprint Fit** | Completable within a single sprint with focused effort |

### Justification

This story is estimated at **8 story points** based on the following complexity factors:

1. **Aggregation across 4 recommendation categories** requires a unified data model that normalizes different recommendation types (skew, join, partition, cache) into a common format with consistent severity, impact, and action fields
2. **Severity-based prioritization with non-additive impact estimation** requires careful algorithmic design to avoid double-counting overlapping benefits when multiple recommendations target the same execution stage
3. **Two export formats** (JSON with Jackson, CSV with RFC 4180 compliance) each require dedicated serialization logic with format-specific handling for nested data structures, null values, and special characters
4. **Two REST API endpoints** (per-execution report and session-level aggregate report) with format query parameter, pagination support, and structured error handling
5. **Session-level aggregation** with de-duplication of configuration change suggestions across multiple query executions adds significant consolidation logic
6. **Web UI panel** with expandable sections, severity-colored indicators, and download functionality requires frontend integration work

The `SQLAppStatusStore` provides a solid persistence foundation with read-only access patterns, but the consolidation, prioritization, and multi-format presentation logic is entirely new.

## Definition of Done

- [ ] Optimization summary report aggregates results from all 4 recommendation engines (skew, join, partition, cache) into a single structured document
- [ ] Executive summary includes: total issue count by severity, estimated total performance improvement percentage, and top 3 prioritized actions
- [ ] Report is exportable in JSON format (structured object with schema per AC-2) and CSV format (RFC 4180 compliant per AC-3)
- [ ] REST API endpoints operational: per-execution report at `/api/v1/applications/[app-id]/sql/[execution-id]/optimization-report` and session-level aggregate report at `/api/v1/applications/[app-id]/optimization-report` with `format` query parameter
- [ ] Partial analysis results are handled: completed analyses are included, incomplete analyses show their status with a descriptive message
- [ ] Configuration change recommendations are de-duplicated across categories and queries (session-level)
- [ ] Non-existent execution IDs return HTTP 404 with structured error response containing `errorCode` and `message` fields
- [ ] No-issues scenarios produce a clean report stating zero optimization opportunities with `totalIssues: 0`
- [ ] Spark Web UI SQL execution detail page includes an optimization summary panel with severity indicators and a download button for JSON/CSV export
- [ ] Unit tests cover: full report, partial report, no-issues report, session aggregation, JSON schema validation, CSV compliance, invalid execution ID, unsupported format, empty session, large report pagination
- [ ] Integration test validates end-to-end report generation for a query with known skew, suboptimal join, and excess partitions — verifying all 3 issues appear in the consolidated report with the expected severity
- [ ] No forbidden terms appear in any acceptance criteria text
- [ ] Story is demo-able: a data engineer can run a query, navigate to the SQL execution detail page, view the consolidated optimization summary, and download it as JSON or CSV for sharing with the team
