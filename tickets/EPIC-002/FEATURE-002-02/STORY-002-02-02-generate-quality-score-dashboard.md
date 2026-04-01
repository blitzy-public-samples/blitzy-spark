# Generate Quality Score Dashboard in Spark Web UI to Visualize Data Quality Status Across DataFrames

## Implementation Phase

**Phase:** Frontend

This is a **Phase 2 (Frontend)** story. It must not begin development until all Phase 1 (Backend) stories in this epic have passed acceptance testing. Frontend stories consume data and APIs produced by backend stories.

## User Story

As a **data platform administrator**, I want to view a dedicated Data Quality tab in the Spark Web UI that displays per-DataFrame quality scores with color-coded indicators (green for scores at or above the configured threshold, red for scores below the threshold), so that I can monitor data quality status across all active and completed DataFrames in a single centralized view without switching to external tools, reducing mean time to detect data quality degradation from hours to minutes.

## Acceptance Criteria

### AC-1: Input Validation — Data Quality Tab Activation via Configuration

- **Given** a Spark application with `spark.quality.ui.enabled` set to `true` in SparkConf
- **When** the Spark Web UI loads
- **Then** a "Data Quality" tab appears in the top navigation bar between the existing tabs (after the "Executors" tab) and is accessible via URL path `/quality/`

### AC-2: Expected Output — Dashboard Displays Per-DataFrame Quality Summary

- **Given** 3 DataFrames have been validated with quality rules during the application
- **When** I navigate to the Data Quality tab
- **Then** the dashboard displays a table with columns: DataFrame Name, Total Columns, Columns Passing, Columns Failing, Overall Quality Score (percentage), and Status Indicator — with one row per validated DataFrame

### AC-3: Edge Case — Color-Coded Status Badges Based on Threshold

- **Given** the quality threshold is configured as `spark.quality.ui.threshold` with a default value of 0.8 (80%)
- **When** a DataFrame has an overall quality score of 0.92
- **Then** its status indicator displays a green badge; and when another DataFrame has a quality score of 0.65, its status indicator displays a red badge

### AC-4: Error Handling — Empty State When No Validations Exist

- **Given** no DataFrames have been validated with quality rules during the application
- **When** I navigate to the Data Quality tab
- **Then** the dashboard displays an informational message stating "No quality validation results available" instead of an empty table or an error page

### AC-5: Expected Output — Drill-Down Per-Column Quality Metrics

- **Given** the Data Quality tab displays 5 validated DataFrames
- **When** I click on a specific DataFrame row
- **Then** the view expands to show per-column quality metrics including column name, non-null ratio, distinct count, rule violations count, and individual column pass/fail status

### AC-6: Expected Output — Auto-Refresh Displays Newly Validated DataFrames

- **Given** new DataFrames are validated while the Data Quality tab is open
- **When** the page auto-refreshes at the interval configured by `spark.ui.retainedDeadExecutors` (default behavior)
- **Then** newly validated DataFrames appear in the dashboard table without requiring a full page reload

### AC-7: Error Handling — ACL Enforcement on Data Quality Tab

- **Given** Spark ACLs are enabled via `spark.acls.enable=true`
- **When** a user without view permissions accesses the Data Quality tab URL
- **Then** the server returns HTTP 403 Forbidden status consistent with other Spark UI tab access control behavior

## Sub-Tasks

- Create `DataQualityTab` class extending `SparkUITab` in `org.apache.spark.ui.quality` package, following the pattern of existing tabs like `JobsTab`, `StagesTab`, and `StorageTab` as implemented in `core/src/main/scala/org/apache/spark/ui/SparkUI.scala`
- Create `DataQualityPage` class extending `WebUIPage` that renders the quality dashboard HTML using `UIUtils.headerSparkPage` and `UIUtils.listingTable` helper methods consistent with existing page patterns in `core/src/main/scala/org/apache/spark/ui/UIUtils.scala`
- Implement a `QualityStore` reader class that retrieves quality validation results from the AppStatusStore (or a dedicated quality KVStore section) following the `AppStatusStore` pattern in `core/src/main/scala/org/apache/spark/status/AppStatusStore.scala`
- Register the Data Quality tab in `SparkUI.initialize()` method, gated by the `spark.quality.ui.enabled` configuration flag, following the conditional tab registration pattern used for `DriverLogTab` and `PrometheusServlet` in `core/src/main/scala/org/apache/spark/ui/SparkUI.scala` (lines 105–118)
- Implement color-coded status badges using inline CSS classes: green (`badge-success`) for scores >= threshold, red (`badge-danger`) for scores < threshold
- Implement drill-down per-DataFrame detail page showing per-column metrics (column name, non-null ratio, distinct count, rule violations count, individual column pass/fail status)
- Add configuration parameter `spark.quality.ui.threshold` with default value 0.8 for the pass/fail boundary
- Integrate with `HttpSecurityFilter` and `SecurityManager` for access control consistency, matching the ACL enforcement pattern in `core/src/main/scala/org/apache/spark/ui/HttpSecurityFilter.scala`
- Write unit tests for `DataQualityTab`, `DataQualityPage` rendering, and `QualityStore` data retrieval
- Write Selenium-based UI integration test following existing patterns in `sql/core/src/test/`

## Edge Cases

- **Edge Case 1 (Empty/Null Input):** No DataFrames have been validated during the application — the dashboard displays the message "No quality validation results available" instead of rendering an empty table or throwing an error
- **Edge Case 2 (Boundary Values):** Quality score is exactly at the threshold (e.g., 0.80 when the threshold is 0.80) — the status indicator displays a green badge because the threshold comparison is "at or above" (>=)
- **Edge Case 3 (Invalid Input):** `spark.quality.ui.threshold` is set to a value outside the [0.0, 1.0] range (e.g., 1.5 or -0.1) — the system logs a warning and falls back to the default threshold of 0.8
- **Edge Case 4 (Large Volume):** Application has validated 500+ DataFrames — the dashboard paginates results (default 100 rows per page) following the `PagedTable` pattern in `core/src/main/scala/org/apache/spark/ui/PagedTable.scala` to prevent excessive page load times
- **Edge Case 5 (Feature Disabled):** `spark.quality.ui.enabled` is set to `false` — the Data Quality tab is not registered in the Web UI navigation, and the `/quality/` URL returns HTTP 404

## Dependencies

- **STORY-002-02-01** (Compute Completeness Metrics) — provides the completeness metrics data (non-null ratio, distinct count, null count) displayed in the dashboard columns and drill-down view
- **FEATURE-002-01** (DataFrame Quality Rules Engine) — provides quality rule validation results (pass/fail per rule, violation counts) that determine the Overall Quality Score and Status Indicator
- **FEATURE-002-02** (Quality Metrics and Reporting) — parent feature defining the scope and integration requirements for this story
- **EPIC-002** (Declarative Data Quality Validation Framework) — parent epic governing the overall data quality initiative
- **Spark Web UI framework** (`Source: core/src/main/scala/org/apache/spark/ui/SparkUI.scala`) — UI tab registration via `attachTab()`, Jetty handler mounting, conditional tab registration gated by SparkConf flags (e.g., `DRIVER_LOG_LOCAL_DIR`, `UI_PROMETHEUS_ENABLED`)
- **Spark Web UI base classes** (`Source: core/src/main/scala/org/apache/spark/ui/WebUI.scala`) — `WebUI`, `SparkUITab`, `WebUIPage` base class hierarchy for tab and page implementation
- **UI utilities** (`Source: core/src/main/scala/org/apache/spark/ui/UIUtils.scala`) — `headerSparkPage`, `listingTable`, `makeProgressBar`, date/number formatters for rendering dashboard HTML
- **Pagination** (`Source: core/src/main/scala/org/apache/spark/ui/PagedTable.scala`) — `PagedDataSource` and `PagedTable` for paginated table rendering when validated DataFrame count exceeds the page size
- **Security** (`Source: core/src/main/scala/org/apache/spark/ui/HttpSecurityFilter.scala`) — HTTP security headers and ACL enforcement for the Data Quality tab URL
- **Status store** (`Source: core/src/main/scala/org/apache/spark/status/AppStatusStore.scala`) — KVStore-backed data retrieval pattern for quality dashboard data source
- **Web UI documentation** (`Source: docs/web-ui.md`) — existing Web UI tab descriptions and navigation patterns used as reference for Data Quality tab placement and design

## Story Estimation Guidance

- **Story Points:** 8 (Fibonacci)
- **Rationale:** High complexity — requires creating a new Web UI tab with custom page rendering, integrating with the Jetty server via `SparkUI` tab registration, implementing drill-down navigation to per-column metrics, handling pagination for large volumes of validated DataFrames, integrating with the security filter for ACL enforcement, and writing both unit tests and Selenium-based UI integration tests. The Web UI tab registration pattern is established (as seen in `SparkUI.initialize()`) but requires multiple new classes: tab, page, and store reader. Estimated 5–7 days of development effort for one engineer.
- **Risk Factors:**
  - Jetty handler integration requires careful ordering during `SparkUI` initialization to avoid handler conflicts
  - Selenium test infrastructure may exhibit flakiness in CI environments due to browser driver version dependencies
  - Color-coded badge rendering must be verified for cross-browser compatibility (Chrome, Firefox, Safari)

## Definition of Done

- Data Quality tab appears in the Spark Web UI navigation when `spark.quality.ui.enabled=true`
- Dashboard table displays DataFrame name, total columns, columns passing, columns failing, overall quality score (percentage), and color-coded status indicators for all validated DataFrames
- Color coding uses green (`badge-success`) for scores >= threshold and red (`badge-danger`) for scores < threshold, with threshold configurable via `spark.quality.ui.threshold` (default 0.8)
- Drill-down view shows per-column quality metrics (column name, non-null ratio, distinct count, rule violations count, individual column pass/fail status) when clicking a DataFrame row
- Empty state displays "No quality validation results available" message when no DataFrames have been validated
- Pagination activates when validated DataFrame count exceeds 100, following the `PagedTable` pattern
- Access control is enforced via the existing `SecurityManager` ACL mechanism, returning HTTP 403 for unauthorized users
- Unit tests cover tab registration, page rendering, color-coding logic, empty state handling, threshold boundary behavior (score == threshold), and invalid threshold fallback
- Selenium integration test validates end-to-end tab visibility, table content rendering, and drill-down navigation
- Code review completed and merged to feature branch
- No forbidden terms used in any acceptance criteria or documentation
