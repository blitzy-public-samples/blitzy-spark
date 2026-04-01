# Provide Reusable Read Operation Test Harness to Standardize Connector Read Validation Across All Custom DataSource V2 Implementations

## User Story

**As a** platform engineer,
**I want** a reusable abstract base test class for read operations that extends `QueryTest` with `SharedSparkSession` and provides parameterized tests for single-column reads, multi-column reads, partition discovery, schema merging, and null handling,
**so that** I can validate any custom DataSource V2 connector's read path against a standardized test suite in under 30 minutes of integration effort, reducing read-path test development time by 70% compared to writing connector-specific test suites from scratch.

## Acceptance Criteria

1. **AC1 — Input Validation**
   - **Given** a connector implementation that extends the abstract read test base class and provides a data source format name via the `connectorFormat` method,
   - **When** the test harness is instantiated,
   - **Then** the harness validates that the format name is a non-empty string and that the test data directory exists on the local filesystem before executing any test cases, and throws an `IllegalArgumentException` with a message containing the invalid format name if validation fails.

2. **AC2 — Expected Output: Single-Column Read**
   - **Given** a test data file containing 1,000 rows with a single integer column named `id`,
   - **When** the single-column read test executes using `spark.read.format(connectorFormat).load(path)`,
   - **Then** the resulting DataFrame contains exactly 1,000 rows and the `checkAnswer` assertion confirms that all integer values match the expected dataset row by row.

3. **AC3 — Expected Output: Multi-Column Read**
   - **Given** a test data file containing rows with 5 columns of types integer, string, double, boolean, and long,
   - **When** the multi-column read test executes using `spark.read.format(connectorFormat).load(path)`,
   - **Then** all 5 columns are present in the resulting DataFrame schema with matching Spark SQL data types (`IntegerType`, `StringType`, `DoubleType`, `BooleanType`, `LongType`) and `checkAnswer` validates row-level equality against the expected dataset.

4. **AC4 — Partition Discovery**
   - **Given** a directory structure with Hive-style partitioning using the pattern `year=2024/month=01/`,
   - **When** the partition discovery test reads the partitioned data and applies a filter predicate `year = 2024 AND month = 1`,
   - **Then** the resulting DataFrame includes `year` and `month` as partition columns in the schema and returns only the rows that match the specified partition filter predicate, with zero rows from non-matching partitions.

5. **AC5 — Schema Merging**
   - **Given** two data files where File A has columns `(id: integer, name: string)` and File B has columns `(id: integer, email: string)`,
   - **When** the schema merging test reads both files with the `mergeSchema` option set to `true`,
   - **Then** the resulting DataFrame schema contains all three columns `(id: integer, name: string, email: string)` and rows originating from File A have `null` values in the `email` column while rows from File B have `null` values in the `name` column.

6. **AC6 — Null Handling**
   - **Given** a test data file containing 100 rows where 50 rows have a `null` value in a nullable string column named `description`,
   - **When** the null handling read test executes,
   - **Then** the resulting DataFrame contains exactly 50 rows with non-null `description` values and 50 rows with null `description` values, and no `NullPointerException` is thrown during the read operation.

7. **AC7 — Error Handling**
   - **Given** an empty directory that contains no data files,
   - **When** the read test harness attempts to read from the empty directory path using `spark.read.format(connectorFormat).load(emptyPath)`,
   - **Then** the harness throws an `AnalysisException` with a message that contains the text "Path does not exist" or "Unable to infer schema," and the test captures this expected exception using `intercept[AnalysisException]` without crashing the test runner.

8. **AC8 — Edge Case: Zero-Row File**
   - **Given** a data file that contains a valid schema header defining columns `(id: integer, name: string)` but zero data rows,
   - **When** the read test executes using `spark.read.format(connectorFormat).load(path)`,
   - **Then** the resulting DataFrame has a row count of exactly 0 and the schema contains exactly 2 fields matching the column definitions `id: IntegerType` and `name: StringType`.

## Sub-Tasks

- Create abstract base class `ConnectorReadTestBase` extending `QueryTest with SharedSparkSession`
- Define abstract method `def connectorFormat: String` that subclasses must implement to specify the data source format name
- Implement `beforeAll()` validation that asserts `connectorFormat` is non-empty and test data directory exists on disk
- Implement `withSQLConf` / `withTempPath` / `withTable` test fixture patterns modeled after the `AvroSuite` test infrastructure
  - `Source: connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroSuite.scala` — Lines 57–80: `QueryTest` + `SharedSparkSession` base class, `checkReloadMatchesSaved`, `checkAnswer` pattern
- Implement parameterized single-column read test that generates a test file with 1,000 integer rows and validates via `checkAnswer`
- Implement parameterized multi-column read test covering `IntegerType`, `StringType`, `DoubleType`, `BooleanType`, and `LongType` columns
- Implement partition discovery test that creates a Hive-style partitioned directory structure and validates partition column inference and filter predicate pushdown
- Implement schema merging test that writes two files with overlapping but non-identical schemas and reads them with `mergeSchema` set to `true`
- Implement null handling test that generates a test file with 50% null values in a nullable column and validates null/non-null counts
- Implement error handling test for empty directory reads using `intercept[AnalysisException]`
- Implement zero-row file test that creates a schema-only file and validates the resulting DataFrame has zero rows with the expected schema
- Write a concrete reference test class that extends `ConnectorReadTestBase` for the Avro connector to confirm the base class works end-to-end
  - `Source: connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroScanSuite.scala` — Lines 23–30: `FileScanSuiteBase` with `ScanBuilder` parameterization pattern
- Document all public methods and extension points in Scaladoc, including usage examples for extending the base class with a custom connector
  - `Source: connector/protobuf/src/test/scala/org/apache/spark/sql/protobuf/ProtobufTestBase.scala` — Lines 29–51: Shared test base trait extending `SQLTestUtils` with reusable utility methods

## Edge Cases

- **Empty/Null Input**: Reading from a directory that contains zero data files but includes metadata files such as `_SUCCESS` must return an empty DataFrame with the inferred or user-specified schema instead of throwing an unhandled exception; if the format does not support schema inference from metadata alone, the harness must accept a user-provided schema parameter and return an empty DataFrame matching that schema
- **Boundary Values**: Reading a single data file that contains exactly 1 row must return a DataFrame with exactly 1 row and all column values intact; reading a file with the maximum supported column count of 1,000 columns (as tested in the Avro wide-table read benchmark) must complete without stack overflow or schema truncation
- **Invalid Input**: Providing a non-existent file path to the read harness must produce a clear `AnalysisException` with a message identifying the missing path, not a generic `FileNotFoundException` or `NullPointerException`
- **Corrupt File**: Reading a data file with corrupted content (such as a truncated binary payload) must either throw a `SparkException` with a descriptive error message when parse mode is set to `FAILFAST`, or skip the corrupt record and continue processing when parse mode is set to `PERMISSIVE`
- **Schema Mismatch**: Reading a file where the actual data types differ from the user-specified schema (for example, a column declared as `IntegerType` containing string data) must produce a readable error message that identifies the mismatched column name and the conflicting types

## Dependencies

- **FEATURE-003-02 (Connector Testing Framework)** — Parent feature defining the overall testing framework scope and integration requirements
- **FEATURE-003-01 (Connector Scaffolding Generator)** — Generated connector projects must be structurally compatible with this read test harness
- **F-008 (Data Source Connectors)** — DataSource V2 read API interfaces (`Table`, `ScanBuilder`, `Scan`, `Batch`, `PartitionReaderFactory`) that define the read contracts validated by this harness
- `org.apache.spark.sql.QueryTest` — Base test class providing the `checkAnswer` assertion method used for DataFrame equality verification
- `org.apache.spark.sql.test.SharedSparkSession` — Shared `SparkSession` lifecycle management trait that initializes and tears down the session across test suites
- `connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroSuite.scala` — Reference read test patterns including `withSQLConf`, `withTempPath`, `withTable`, and `checkAnswer` usage
- `connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroScanSuite.scala` — `FileScanSuiteBase` pattern for scan-level read tests with `ScanBuilder` parameterization
- `connector/protobuf/src/test/scala/org/apache/spark/sql/protobuf/ProtobufTestBase.scala` — Shared test base class trait pattern extending `SQLTestUtils` with reusable descriptor resolution and DDL parsing utilities

## Story Estimation Guidance

- **Story Points: 5** (Fibonacci scale)
- **Justification**: Requires designing an abstract base class with 6+ parameterized test methods, integrating with the `QueryTest`/`SharedSparkSession` lifecycle, implementing test fixture management (`withTempPath`, `withSQLConf`, `withTable`), and validating against at least one existing connector (Avro) as a reference implementation. Moderate complexity arises from partition discovery setup, schema merging logic, and error handling edge cases. Completable within a single sprint by one engineer familiar with the Spark testing infrastructure.

## Definition of Done

- Abstract base class `ConnectorReadTestBase` is implemented extending `QueryTest with SharedSparkSession` with an abstract `connectorFormat` method
- All 6 read test categories (single-column read, multi-column read, partition discovery, schema merging, null handling, zero-row file read) have passing parameterized test methods in the base class
- Error handling tests confirm expected `AnalysisException` for empty directory and non-existent path scenarios
- Corrupt file tests confirm expected `SparkException` in `FAILFAST` mode and record skipping in `PERMISSIVE` mode
- At least one concrete test class extends `ConnectorReadTestBase` for the Avro connector and passes all inherited test methods
- Scaladoc documents all public methods, abstract methods, and extension points with usage examples
- All tests pass in CI on both JDK 17 and JDK 21 runtime environments
- No forbidden terms (approximately, several, various, adequate, appropriate, properly, correctly, efficiently, quickly, easily, user-friendly, reasonable, sufficient) appear in any acceptance criteria text
