# Visualize Stage Execution Timeline for SQL Queries in the Spark Web UI

## User Story

**As a** data engineer,
**I want to** view a visual timeline of all stages within a SQL query execution in the Spark Web UI, showing each stage's start time, end time, duration, and key metrics (task deserialization time, executor run time, JVM GC time, result serialization time, scheduler delay, peak execution memory, shuffle read size, shuffle write size, memory spill size, and disk spill size),
**so that** I can identify the longest-running stages and bottlenecks in a query's execution pipeline, reducing mean time to root-cause performance issues from hours to minutes and enabling targeted optimization of the most impactful stages.

## Acceptance Criteria

### AC-1: Timeline Rendering for Completed Multi-Stage Queries (Expected Output)

- **Given** a completed SQL query that executed across 5 or more stages
- **When** the data engineer navigates to the SQL tab → Execution Detail page for that query in the Spark Web UI
- **Then** the page displays a horizontal timeline chart where each stage is represented as a colored bar positioned on a time axis, with the bar's left edge at the stage start time and right edge at the stage end time, and the bar width is proportional to the stage duration

### AC-2: Stage Metrics Display on Interaction (Expected Output)

- **Given** a stage bar displayed in the execution timeline
- **When** the data engineer hovers over or clicks on the stage bar
- **Then** a tooltip or detail panel displays the following metrics: task deserialization time (ms), executor run time (ms), JVM GC time (ms), result serialization time (ms), scheduler delay (ms), peak execution memory (bytes), shuffle read size (bytes), shuffle write size (bytes), memory spill size (bytes), and disk spill size (bytes)

### AC-3: Non-Existent Execution ID Handling (Input Validation)

- **Given** a query execution ID submitted via the Web UI URL parameter
- **When** the execution ID does not exist in the SQLAppStatusStore (e.g., ID = -1 or ID = 999999999)
- **Then** the Web UI displays an error message stating "Execution ID [id] not found" and does not render a timeline

`Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala`

### AC-4: Failed or Cancelled Query Display (Error Handling)

- **Given** a query that was cancelled or failed before any stages completed
- **When** the data engineer views the execution detail page for that query
- **Then** the timeline displays an empty state with a message indicating "No stage data available for this execution" and shows the query status as FAILED or CANCELLED with the recorded error message

### AC-5: Single Stage Query Timeline (Edge Case)

- **Given** a query that executes entirely within a single stage (e.g., a `SELECT * FROM table LIMIT 10` with no shuffle)
- **When** the data engineer views the timeline for this query
- **Then** the timeline displays one stage bar spanning the full query execution duration, and the stage metrics are visible without requiring horizontal scrolling

### AC-6: Parallel Stage Swim-Lane Visualization (Expected Output)

- **Given** a query with stages that execute in parallel (e.g., two independent scan stages feeding a join)
- **When** the data engineer views the timeline
- **Then** parallel stages are displayed on separate rows (swim lanes) with overlapping time ranges visible, and a vertical cursor line shows the current hover position across all swim lanes simultaneously

## Sub-Tasks

- Design the timeline visualization component for the SQL Execution Detail page, extending `ExecutionPage` to include a new timeline section below the existing DAG visualization
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/ExecutionPage.scala`
- Create a JavaScript/D3-based timeline renderer that reads stage data from the SQL execution REST API endpoint `/api/v1/applications/[app-id]/sql/[execution-id]`
- Extend `SQLAppStatusListener` to capture per-stage start/end timestamps and aggregate task-level metrics (deserialization time, execution duration, GC time, scheduler delay, peak execution memory, shuffle read/write, spill) into stage-level summaries
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusListener.scala`
- Add a new REST API endpoint or extend the existing endpoint to return stage timeline data: `GET /api/v1/applications/[app-id]/sql/[execution-id]/stages` returning a JSON array of stage objects with timing and metric fields
- Implement swim-lane layout logic for parallel stages that assigns each concurrent stage group to a separate row
- Add hover tooltip component that displays the 10 stage-level metrics listed in AC-2
- Handle edge cases: empty stages (no tasks completed), single-stage queries, queries with 100+ stages (add horizontal scrolling and zoom controls)
- Add CSS styles for timeline bars including color coding by stage status (completed = green, failed = red, running = blue, skipped = grey)
- Write Selenium/WebUI integration tests validating timeline rendering for multi-stage and single-stage queries
- Write unit tests for stage metric aggregation logic in `SQLAppStatusListener`

## Edge Cases

- **Empty/Null Input — Zero-Stage Execution**: A query that completes with zero stages (e.g., a DDL command like `CREATE TABLE`) must display the execution detail page without a timeline section, showing only the command description and execution status
- **Boundary Values — More Than 200 Stages**: A query with more than 200 stages (e.g., a large `UNION ALL` across many tables) must render the timeline with horizontal scrolling enabled; each stage bar must remain at least 2 pixels wide to be clickable, and a zoom control allows expanding the time axis
- **Invalid Input — Non-Numeric Execution ID**: Accessing the execution detail page with a non-numeric execution ID in the URL (e.g., `/sql/execution?id=abc`) must return an HTTP 400 response with a message "Invalid execution ID format" instead of an unhandled exception
- **Boundary Values — Sub-Millisecond Stage Duration**: Stages completing in under 1 millisecond must be displayed with a minimum bar width of 2 pixels and a tooltip showing "< 1 ms" for the duration value
- **Concurrent Query Isolation**: When multiple queries are running simultaneously, the timeline for each query must show only the stages belonging to that specific query execution ID, with no stage data leaking from other concurrent queries

## Dependencies

- **EPIC-001** — Adaptive Query Performance Insights Engine (parent epic)
  - `Source: tickets/EPIC-001-adaptive-query-performance-insights.md`
- **FEATURE-001-01** — Query Execution Profiling (parent feature)
  - `Source: tickets/EPIC-001/FEATURE-001-01-query-execution-profiling.md`
- **STORY-001-01-01** — Capture Query Execution Plans: execution plan data must be captured before the timeline can reference plan stages
- **F-002** — SQL Query Processing Engine: the query execution lifecycle provides stage data, job-to-stage mappings, and runtime statistics consumed by the timeline
- **Spark Web UI Framework**: page rendering infrastructure for hosting the timeline component
  - `Source: core/src/main/scala/org/apache/spark/ui/SparkUI.scala`
- **SQL Tab**: tab registration and page management for the SQL execution detail view
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLTab.scala`
- **ExecutionPage**: the existing execution detail page that will be extended with the timeline section
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/ExecutionPage.scala`
- **SQLAppStatusListener**: the event listener that captures per-stage metrics and execution events for aggregation
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusListener.scala`
- **SQLAppStatusStore**: the data store providing stage execution data and query status for timeline rendering
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/ui/SQLAppStatusStore.scala`
- **D3.js / dagre-d3**: client-side visualization libraries already bundled with the Spark Web UI, used for rendering the timeline chart and swim-lane layout
- **Web UI Documentation**: documents the existing Jobs tab event timeline and Stages tab metrics (deserialization time, execution duration, GC time, scheduler delay, peak execution memory, shuffle read/write, spill sizes) that inform the timeline's metric set
  - `Source: docs/web-ui.md`

## Story Estimation Guidance

- **Story Points: 8** (Fibonacci scale)
- **Rationale**: This story requires new UI component development (horizontal timeline visualization with swim lanes), JavaScript/D3 rendering logic, REST API extension for stage timeline data, stage-level metric aggregation in `SQLAppStatusListener`, parallel stage layout algorithm, edge case handling for zero-stage/single-stage/200+ stage scenarios, and comprehensive UI integration testing. The front-end and back-end integration across the Web UI rendering layer (`ExecutionPage.scala`), data layer (`SQLAppStatusStore`), and event processing layer (`SQLAppStatusListener`) increases coordination complexity. Estimated 5–8 development days for an engineer experienced with the Spark Web UI and D3.js visualization.

## Definition of Done

- Timeline visualization renders on the SQL Execution Detail page for all completed queries with one or more stages
- Each stage is displayed as a proportionally-sized bar on a horizontal time axis with color coding by status (completed = green, failed = red, running = blue, skipped = grey)
- Stage metrics (all 10 specified metrics: task deserialization time, executor run time, JVM GC time, result serialization time, scheduler delay, peak execution memory, shuffle read size, shuffle write size, memory spill size, disk spill size) are accessible via hover tooltip or click detail panel
- Parallel stages are displayed in separate swim lanes with temporal overlap between concurrent stages visible
- Empty or missing stage scenarios display informative messages ("No stage data available for this execution") instead of rendering errors
- Non-existent execution IDs display "Execution ID [id] not found" without rendering a timeline
- Non-numeric execution IDs in the URL return an HTTP 400 response with "Invalid execution ID format"
- REST API endpoint returns stage timeline data in JSON format at `/api/v1/applications/[app-id]/sql/[execution-id]/stages`
- Timeline supports horizontal scrolling and zoom controls for queries with more than 200 stages
- Selenium integration tests validate timeline rendering for 1-stage, 5-stage, and 50-stage query scenarios
- Unit tests validate stage metric aggregation logic with 100% branch coverage for the aggregation code paths
- Performance: the timeline page loads within 3 seconds for queries with up to 500 stages
- Code review approved by at least 2 committers
- Documentation updated in `docs/web-ui.md` with the timeline feature description, UI navigation instructions, and metric definitions
