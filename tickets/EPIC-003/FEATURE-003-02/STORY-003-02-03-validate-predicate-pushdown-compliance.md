# Validate Predicate Pushdown Compliance for DataSource V2 Connectors to Ensure Filter Optimization Meets API Contract

## User Story

**As a** data engineer,
**I want** a predicate pushdown compliance validator that tests equality filters, range filters (`>`, `>=`, `<`, `<=`), in-set filters, is-null/is-not-null filters, and verifies that non-pushable predicates fall back to Spark-side filtering by inspecting the `StructFilters.pushedFilters` behavior and the executed query plan's `BatchScanExec` node,
**so that** I can confirm that my custom connector's filter pushdown implementation meets the DataSource V2 API contract, reducing data scan volumes by up to 80% for filtered queries and preventing silent correctness bugs where filters are silently dropped.

## Acceptance Criteria

### AC1 — Input Validation: Connector Pushdown Interface Verification

- **Given** a connector implementation that extends the pushdown compliance test class
- **When** the test harness initializes
- **Then** it validates that the connector's `ScanBuilder` implements the `pushDataFilters` method and that the filter pushdown configuration flag is enabled (e.g., `avroFilterPushDown` in `AvroScanBuilder.scala` line 49)

> `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroScanBuilder.scala` — Lines 48–54: `pushDataFilters` implementation using `StructFilters.pushedFilters` with `avroFilterPushDown` config check

### AC2 — Expected Output: Equality Filter Pushdown

- **Given** a test dataset with 10,000 rows and a column `id` with values 1 through 10,000
- **When** the test applies a filter `WHERE id = 5000`
- **Then** the executed plan's `BatchScanExec` node contains a non-empty `dataFilters` array including the equality predicate, and the result DataFrame contains exactly 1 row with `id = 5000`

### AC3 — Expected Output: Range Filter Pushdown

- **Given** the same test dataset with 10,000 rows and a column `id` with values 1 through 10,000
- **When** the test applies range filters (`WHERE id > 9000`, `WHERE id >= 9000`, `WHERE id < 100`, `WHERE id <= 100`)
- **Then** each filter is present in the `BatchScanExec`'s `dataFilters` and the result row count matches the expected count for each predicate: 999 rows for `id > 9000`, 1,000 rows for `id >= 9000`, 99 rows for `id < 100`, 100 rows for `id <= 100`

### AC4 — Expected Output: In-Set Filter Pushdown

- **Given** the test dataset with 10,000 rows
- **When** the test applies a filter `WHERE id IN (1, 500, 1000, 5000, 10000)`
- **Then** the in-set filter is pushed down to the connector and the result DataFrame contains exactly 5 rows matching those specific `id` values

### AC5 — Expected Output: Is-Null and Is-Not-Null Filter Pushdown

- **Given** a dataset where column `name` has 20% null values (e.g., 2,000 nulls in a 10,000-row dataset)
- **When** the test applies `WHERE name IS NULL` and `WHERE name IS NOT NULL`
- **Then** the is-null filter returns only null rows (2,000 rows) and the is-not-null filter returns only non-null rows (8,000 rows), and both filters appear in the pushed filters list within the `BatchScanExec` node

### AC6 — Non-Pushable Fallback: Spark-Side Filter Evaluation

- **Given** a filter expression that uses a UDF or complex expression not supported by the connector's pushdown implementation
- **When** the test applies this non-pushable filter
- **Then** the filter does NOT appear in the `BatchScanExec`'s pushed `dataFilters` and instead appears in the query plan as a Spark-side `Filter` node above the scan, and the result is still computed by Spark-side evaluation producing the expected output

### AC7 — Error Handling: Pushdown Disabled via Configuration

- **Given** a connector where filter pushdown is explicitly disabled via configuration (e.g., setting `avroFilterPushDown` to `false`)
- **When** the pushdown compliance test executes
- **Then** zero filters appear in the pushed filters list, the result is still computed by Spark applying all filters post-scan, and the test logs a warning indicating pushdown is disabled

### AC8 — Edge Case: Partition Filter vs Data Filter Separation

- **Given** a partitioned dataset with both partition columns (`p1`, `p2`) and data columns (`value`)
- **When** a combined filter predicate includes both partition filters and data filters (e.g., `WHERE p1 = 1 AND p2 = 2 AND value != 'a'` as in `AvroSuite` line 3214)
- **Then** partition filters are separated from data filters: `partitionFilters` is non-empty, `dataFilters` is non-empty, the partition filter columns (`p1`, `p2`) do not appear in the residual Spark-side `Filter` condition, and only matching partitions are scanned (as validated in `AvroSuite` lines 3216–3231)

> `Source: connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroSuite.scala` — Lines 3202–3268: Partition pruning and data filter pushdown test patterns including `BatchScanExec` extraction, `Filter` condition inspection, and `partitionFilters`/`dataFilters` validation

## Sub-Tasks

- [ ] Create abstract test class `ConnectorPushdownComplianceTest` extending `QueryTest` with `SharedSparkSession`
- [ ] Define abstract method `def connectorFormat: String` returning the DataSource format identifier (e.g., `"avro"`)
- [ ] Define abstract method `def pushdownConfigKey: String` returning the `SQLConf` key that enables or disables pushdown for the connector
- [ ] Implement helper method to extract `BatchScanExec` from the executed plan (based on `AvroSuite` lines 3226–3228: `df.queryExecution.executedPlan collectFirst { case BatchScanExec(_, f: Scan, ...) => f }`)
- [ ] Implement helper method to extract the `Filter` condition from the optimized plan (based on `AvroSuite` lines 3216–3218: `df.queryExecution.optimizedPlan.collectFirst { case f: Filter => f.condition }`)
- [ ] Implement equality filter pushdown test (`WHERE id = 5000`)
- [ ] Implement range filter pushdown tests (`>`, `>=`, `<`, `<=`) with expected row count assertions
- [ ] Implement in-set filter pushdown test (`WHERE id IN (...)`)
- [ ] Implement is-null and is-not-null filter pushdown tests with null-percentage assertions
- [ ] Implement non-pushable predicate fallback test using a UDF-based filter expression
- [ ] Implement partition-vs-data filter separation test with `partitionFilters` and `dataFilters` assertions
- [ ] Implement pushdown-disabled configuration test using `withSQLConf` to disable the pushdown flag
- [ ] Write concrete unit tests using the Avro connector as the reference implementation to validate the base class
- [ ] Document usage and extension points in Scaladoc comments on the abstract test class

## Edge Cases

### 1. Empty/Null Input — Filter on Empty Dataset

Applying a pushdown filter on an empty dataset (zero rows) must return an empty DataFrame without errors. The filter must still appear in the `BatchScanExec`'s pushed `dataFilters` list regardless of data presence, confirming that pushdown decisions are schema-driven and not data-driven.

### 2. Boundary Values — Min/Max and Beyond-Range Filters

Pushing down a filter on the minimum value (e.g., `WHERE id = 1` for the first row) and maximum value (e.g., `WHERE id = 10000` for the last row) must return exactly 1 row each. Pushing `WHERE id > 10000` must return zero rows without error. Pushing `WHERE id < 1` must return zero rows without error.

### 3. Invalid Input — Non-Existent Column Reference

Applying a filter on a column name that does not exist in the schema (e.g., `WHERE nonexistent_col = 1`) must throw an `AnalysisException` before reaching the pushdown phase, with a message indicating the unresolved column reference. The pushdown compliance validator must not mask this error.

### 4. Compound Predicates — AND/OR Logic with Mixed Pushability

Pushing down a compound predicate with AND/OR logic (e.g., `WHERE id > 100 AND id < 200 OR name IS NULL`) must handle operator precedence and push down each supported sub-predicate independently. Non-pushable sub-predicates within the compound expression must fall back to Spark-side evaluation while pushable sub-predicates are still pushed to the connector.

### 5. Type Coercion Filters — Implicit Type Conversion

Applying a filter with implicit type coercion (e.g., filtering an integer column with a string literal `WHERE id = '5000'`) must either push down the coerced filter after type resolution or fall back to Spark-side filtering without data corruption. The compliance test must verify that the result set is identical regardless of whether the coerced filter is pushed or evaluated Spark-side.

## Dependencies

- **FEATURE-003-02** (Connector Testing Framework) — parent feature providing the testing framework scope and integration context
- **STORY-003-02-01** (Provide Test Harness for Read Operations) — pushdown tests build on the read path infrastructure and test dataset preparation methods
- **F-008** (Data Source Connectors) — DataSource V2 pushdown API, specifically `ScanBuilder.pushDataFilters` contract
- **`org.apache.spark.sql.catalyst.StructFilters`** — `pushedFilters` utility method for determining which filters are pushable to the data source
- **`org.apache.spark.sql.execution.datasources.v2.BatchScanExec`** — executed plan node containing pushed `dataFilters` and `partitionFilters` arrays
- **`connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroScanBuilder.scala`** — Lines 48–54: Reference `pushDataFilters` implementation using `StructFilters.pushedFilters` with `avroFilterPushDown` config check
- **`connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroSuite.scala`** — Lines 3202–3268: Partition pruning and data filter pushdown test patterns (`BatchScanExec` extraction, `Filter` condition inspection, `partitionFilters`/`dataFilters` validation)

## Story Estimation Guidance

**Story Points: 5** (Fibonacci scale)

**Justification:** This story requires implementing 7+ filter type tests (equality, range with 4 operators, in-set, is-null, is-not-null, non-pushable fallback), query plan inspection helpers for `BatchScanExec` and `Filter` node extraction, and configuration-dependent behavior validation. Moderate complexity arises from plan traversal logic (extracting `BatchScanExec` and `Filter` nodes from both executed and optimized plans) and partition-vs-data filter separation assertions. The implementation leverages well-established patterns from `AvroSuite` pushdown tests (lines 3202–3268) and the `StructFilters.pushedFilters` utility. Completable within a single sprint by a data engineer familiar with the Catalyst query plan structure.

## Definition of Done

- [ ] Pushdown compliance test class validates all 6 filter types: equality, range (4 operators), in-set, is-null, is-not-null, and non-pushable fallback
- [ ] Plan inspection helpers extract the `BatchScanExec` node and its `dataFilters`/`partitionFilters` from the executed plan
- [ ] Partition-vs-data filter separation test confirms that partition filters are routed to `partitionFilters` and data filters to `dataFilters`
- [ ] Configuration-disabled test validates graceful degradation with all filters applied Spark-side when pushdown is turned off
- [ ] At least one concrete test validates the Avro connector's `pushDataFilters` implementation as a reference integration
- [ ] All tests pass in CI with both JDK 17 and JDK 21
- [ ] No forbidden terms appear in any acceptance criteria text
