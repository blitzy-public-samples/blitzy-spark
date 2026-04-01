# Export Data Quality Reports in JSON and CSV Formats to Enable Automated Quality Gate Integration

## Implementation Phase

**Phase:** Frontend

This is a **Phase 2 (Frontend)** story. It must not begin development until all Phase 1 (Backend) stories in this epic have passed acceptance testing. Frontend stories consume data and APIs produced by backend stories.

## User Story

As a **data engineer**, I want to export data quality reports in both JSON and CSV formats via a programmatic DataFrame API method and a REST API endpoint (`/api/v1/quality/reports`), so that I can integrate quality validation results into CI/CD pipelines, data cataloging tools, and stakeholder reporting workflows, eliminating manual report generation that consumes up to 2 hours per pipeline release cycle.

## Acceptance Criteria

### AC-1: Input Validation — Programmatic JSON Export Writes Structured Report

- **Given** a validated DataFrame with quality metrics computed
- **When** I call `qualityReport.export(format="json", path="/output/report.json")`
- **Then** the system writes a JSON file to the specified path containing an array of objects with fields: `dataframeName`, `timestamp`, `overallQualityScore`, `columnCount`, `columnsPassingCount`, `columnsFailingCount`, and a `columnDetails` array with per-column metrics (`columnName`, `nonNullRatio`, `nullCount`, `distinctCount`, `ruleViolations`)

### AC-2: Expected Output — CSV Export Produces Per-Column Tabular Data

- **Given** a validated DataFrame "orders" with 12 columns
- **When** I call `qualityReport.export(format="csv", path="/output/report.csv")`
- **Then** the system writes a CSV file with a header row (`dataframe_name`, `column_name`, `non_null_ratio`, `null_count`, `distinct_count`, `rule_violations`, `quality_score`) and exactly 12 data rows (one per column), with all numeric values formatted to 4 decimal places for ratios and integers for counts

### AC-3: Error Handling — Invalid Export Format Raises IllegalArgumentException

- **Given** a quality report export request with `format="xml"`
- **When** the export method is invoked
- **Then** the system raises an `IllegalArgumentException` with message `"Unsupported export format: xml. Supported formats are: json, csv"`

### AC-4: Error Handling — Invalid Output Path Raises IOException

- **Given** a quality report export request with `path="/nonexistent/directory/report.json"`
- **When** the export method is invoked and the parent directory does not exist
- **Then** the system raises an `IOException` with a message identifying the invalid output path

### AC-5: Expected Output — REST API Returns JSON Quality Report

- **Given** the Spark application has quality validation results
- **When** I send a GET request to `/api/v1/quality/reports?format=json`
- **Then** the endpoint returns HTTP 200 with Content-Type `application/json` and a JSON body containing quality report data for all validated DataFrames in the current application

### AC-6: Expected Output — REST API Returns CSV Quality Report

- **Given** the Spark application has quality validation results
- **When** I send a GET request to `/api/v1/quality/reports?format=csv`
- **Then** the endpoint returns HTTP 200 with Content-Type `text/csv` and a CSV body with headers and one row per column per validated DataFrame

### AC-7: Expected Output — REST API Supports DataFrame Filtering

- **Given** 5 DataFrames have been validated
- **When** I send a GET request to `/api/v1/quality/reports?format=json&dataframe=orders`
- **Then** the endpoint returns quality report data only for the "orders" DataFrame, excluding all other DataFrames

## Sub-Tasks

- Implement `QualityReportExporter` class with methods: `exportAsJson(path: String): Unit`, `exportAsCsv(path: String): Unit`, and a unified `export(format: String, path: String): Unit` method with format validation
- Implement JSON export using Jackson `ObjectMapper` (already a Spark dependency, version 2.20.0 — `Source: pom.xml`) to serialize quality report data as a structured JSON array with nested column details
- Implement CSV export using `univocity-parsers` CSV writer (already a Spark SQL dependency) to produce header-row CSV output with escaping for column names containing special characters per RFC 4180
- Add `qualityReport` accessor method on the quality-validated DataFrame (or on the quality result object) that returns a `QualityReportExporter` instance
- Implement REST API endpoint `/api/v1/quality/reports` as a JAX-RS resource following the pattern of existing v1 API endpoints in `core/src/main/scala/org/apache/spark/status/api/v1/` directory, supporting query parameters: `format` (json|csv, default json) and `dataframe` (optional filter)
- Register the quality reports REST endpoint in the API root resource alongside existing endpoints (`Source: core/src/main/scala/org/apache/spark/status/api/v1/ApiRootResource.scala`), following the REST API patterns documented in `docs/monitoring.md` (existing `/api/v1/` endpoints for applications, jobs, stages, executors, sql, streaming, environment)
- Implement response content-type negotiation: `application/json` for JSON format, `text/csv` for CSV format
- Write unit tests for JSON and CSV export content validation, format validation, and path error handling
- Write integration test for REST API endpoint with HTTP client assertions on status code, content type, and response body structure

## Edge Cases

- **Edge Case 1 (Empty/Null Input)**: No DataFrames have been validated — programmatic export writes an empty JSON array `[]` or a CSV file with only the header row; REST API returns HTTP 200 with an empty array or header-only CSV body
- **Edge Case 2 (Boundary Values)**: DataFrame with a single column and a single row — export produces exactly one data record in JSON/CSV with all metrics computed for that single column
- **Edge Case 3 (Invalid Input)**: Column names containing commas, quotes, or newlines — CSV export wraps such column names in double quotes with escaping per RFC 4180
- **Edge Case 4 (Large Report)**: Quality report with 500 DataFrames and 50 columns each (25,000 total column rows) — export completes without memory overflow by using streaming write (Jackson streaming generator for JSON, buffered writer for CSV) rather than collecting all data in memory
- **Edge Case 5 (Concurrent REST Requests)**: Two simultaneous GET requests to `/api/v1/quality/reports` — both requests receive consistent, complete response data without data corruption or partial results

## Dependencies

- **STORY-002-02-01** (Compute Completeness Metrics) — provides per-column completeness metrics (non-null ratio, null count, distinct count) included in the exported report data
- **STORY-002-02-03** (Track Quality Trends Over Time) — provides historical trend data accessible for export via the `QualityTrendStore` query API
- **FEATURE-002-01** (DataFrame Quality Rules Engine) — provides rule violation data included in report column details
- **FEATURE-002-02** (Quality Metrics and Reporting) — parent feature; see [FEATURE-002-02-quality-metrics-and-reporting.md](../FEATURE-002-02-quality-metrics-and-reporting.md)
- **EPIC-002** (Declarative Data Quality Validation Framework) — parent epic; see [EPIC-002-declarative-data-quality-framework.md](../../EPIC-002-declarative-data-quality-framework.md)
- **REST API infrastructure** (`Source: docs/monitoring.md`) — documents existing `/api/v1/` endpoints (applications, jobs, stages, executors, sql, streaming, environment) and REST API patterns for Spark's status API
- **Status API v1** (`Source: core/src/main/scala/org/apache/spark/status/api/v1/`) — JAX-RS/Jersey resource implementations for the v1 REST surface, including `ApiRootResource.scala` (root resource with sub-resource locators), `JacksonMessageWriter.scala` (JSON serialization), and `OneApplicationResource.scala` (application-scoped endpoints)
- **AppStatusStore** (`Source: core/src/main/scala/org/apache/spark/status/AppStatusStore.scala`) — read-side KVStore wrapper providing data retrieval pattern for programmatic API and REST endpoint data access
- **Jackson** (version 2.20.0, `Source: pom.xml`) — JSON serialization library already included as a Spark dependency for `ObjectMapper`-based JSON export
- **univocity-parsers** — CSV parsing and writing library already included as a Spark SQL dependency for RFC 4180-compliant CSV export

## Story Estimation Guidance

- **Story Points**: 5 (Fibonacci)
- **Rationale**: Moderate complexity — JSON and CSV export using existing libraries (Jackson, univocity-parsers) are well-established patterns within the Spark codebase. The REST API endpoint follows existing v1 API patterns implemented in `core/src/main/scala/org/apache/spark/status/api/v1/`. Main effort is in implementing the streaming write for large reports, RFC 4180-compliant CSV escaping, and integration test infrastructure for REST endpoints. Estimated 3–4 days of development effort for one engineer.
- **Risk Factors**: Streaming JSON/CSV write for large reports requires careful memory management to avoid collecting all column rows in memory; REST API integration into the existing v1 resource hierarchy requires alignment with the JAX-RS configuration in `ApiRootResource.scala`; CSV RFC 4180 compliance for edge-case column names (containing commas, quotes, newlines) requires thorough testing

## Definition of Done

- Programmatic API exports quality reports in JSON format with a structured array of per-DataFrame and per-column metrics
- Programmatic API exports quality reports in CSV format with a header row and one row per column per DataFrame
- REST API endpoint `/api/v1/quality/reports` returns JSON (default) or CSV based on the `format` query parameter
- REST API supports the `dataframe` query parameter for filtering reports to a specific DataFrame
- Invalid export format raises `IllegalArgumentException` with supported formats listed in the error message
- Invalid output path raises `IOException` with the path identified in the error message
- Empty state (no validated DataFrames) produces an empty JSON array or a header-only CSV without errors
- CSV export handles special characters in column names per RFC 4180
- Unit tests validate JSON structure, CSV content, error handling, and empty state behavior
- Integration test validates REST API responses (status code, content type, and response body structure)
- Code review completed and merged to feature branch
- No forbidden terms used in any acceptance criteria or documentation
