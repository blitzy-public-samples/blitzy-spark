# Provide Reusable Write Operation Test Harness to Validate Connector Write Paths Including Save Modes and Codec Support

## User Story

**As a** platform engineer,

**I want** a reusable abstract base test class for write operations that tests single-column writes, multi-column writes, all four save modes (Overwrite, Append, ErrorIfExists, Ignore), codec sweeps (SNAPPY, ZSTANDARD, DEFLATE, BZIP2, XZ, UNCOMPRESSED), and output format round-trip validation using the `checkAnswer` pattern from `QueryTest`,

**So that** I can validate any custom DataSource V2 connector's write path against a comprehensive, standardized test suite in under 30 minutes of integration effort, reducing write-path test development time by 70% and ensuring consistent write-path quality across all custom connectors.

## Acceptance Criteria

- **AC1 — Input Validation:**
  - **Given** a connector implementation that extends the abstract write test base class
  - **When** the test harness is instantiated with a `connectorFormat` value
  - **Then** the harness validates that the format name is a non-empty string and that the write output directory is writable before executing any test cases

- **AC2 — Expected Output (Single-Column Write):**
  - **Given** a source DataFrame containing 1,000 rows with a single integer column
  - **When** the single-column write test writes the DataFrame using `df.write.format(connectorFormat).save(path)` and reads it back
  - **Then** the `checkAnswer` assertion confirms the read-back DataFrame matches the original 1,000 rows exactly

- **AC3 — Expected Output (Multi-Column Write):**
  - **Given** a source DataFrame with 5 columns (integer, string, double, boolean, long)
  - **When** the multi-column write test executes a write-then-read round trip
  - **Then** all 5 columns are preserved with matching data types and the `checkAnswer` assertion confirms row-level equality

- **AC4 — Save Modes:**
  - **Given** a pre-existing data file at the target path
  - **When** the save mode test executes with each of the four modes (Overwrite replaces data, Append adds to data, ErrorIfExists throws `AnalysisException`, Ignore skips write)
  - **Then** each mode produces the documented behavior: Overwrite yields only new rows, Append yields old plus new rows, ErrorIfExists throws an exception containing "already exists", Ignore retains only original rows

- **AC5 — Codec Sweep:**
  - **Given** a source DataFrame with 100,000 rows
  - **When** the codec sweep test writes the data with each supported compression codec (SNAPPY, ZSTANDARD, DEFLATE, BZIP2, XZ, UNCOMPRESSED)
  - **Then** each written file can be read back and the `checkAnswer` assertion confirms data integrity for every codec

- **AC6 — Error Handling:**
  - **Given** an invalid compression codec name "INVALID_CODEC"
  - **When** the write test attempts to write with this codec
  - **Then** the harness captures an `IllegalArgumentException` or `SparkException` with a message identifying the unsupported codec name

- **AC7 — Edge Case (Empty DataFrame):**
  - **Given** an empty DataFrame with a defined schema but zero rows
  - **When** the write test writes and reads back the empty DataFrame
  - **Then** the resulting DataFrame has zero rows and the schema matches the original DataFrame schema

- **AC8 — SerDe Round-Trip:**
  - **Given** a DataFrame with complex nested types (struct containing an array of maps)
  - **When** the write test serializes and deserializes the data through a write-read round trip
  - **Then** the nested structure is preserved and the `checkAnswer` assertion confirms field-level equality including nested values

## Sub-Tasks

- Create abstract base class `ConnectorWriteTestBase` extending `QueryTest with SharedSparkSession`
- Define abstract method `def connectorFormat: String` for subclass implementation
- Define abstract method `def supportedCodecs: Seq[String]` returning connector-specific compression codecs
- Implement `withTempPath` / `withTable` test fixture patterns based on AvroSuite write test patterns
  - `Source: connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroSuite.scala` — Lines 585–756: write tests using `write.format("avro").save`, compression configs, and codec options
- Implement single-column write-then-read round-trip test with `checkAnswer`
- Implement multi-column write-then-read test covering integer, string, double, boolean, and long columns
- Implement save mode tests for Overwrite, Append, ErrorIfExists, and Ignore modes
- Implement codec sweep test iterating over `supportedCodecs` with write-read-verify cycle
  - `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroWriteBenchmark.scala` — Lines 40–107: codec sweep patterns using `AvroCompressionCodec.values`, level parameters, and ZSTANDARD buffer pool
- Implement SerDe round-trip test for complex nested types based on AvroSerdeSuite patterns
  - `Source: connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroSerdeSuite.scala` — Lines 32–48: serialize-then-deserialize-then-assert-equality pattern
- Implement error handling tests for invalid codec and write-to-read-only-path scenarios
- Write unit tests for the base class using the Avro connector as reference implementation
- Document usage instructions and extension points in Scaladoc

## Edge Cases

- **Empty/Null Input:** Writing an empty DataFrame (zero rows, valid schema) must produce a valid output that can be read back as an empty DataFrame with the same schema, not throw an exception or produce a corrupt file
- **Boundary Values:** Writing a DataFrame with exactly 1 row must produce a valid single-record file; writing a DataFrame with 1,000 columns (as tested in `AvroWriteBenchmark` wide-column benchmark at line 46: `val width = 1000`) must complete without memory errors or schema serialization failures
  - `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroWriteBenchmark.scala` — Line 46
- **Invalid Input:** Specifying a save mode string that does not match any of the four valid modes (Overwrite, Append, ErrorIfExists, Ignore) must produce a clear error message identifying the invalid mode value
- **Null Column Values:** Writing a DataFrame where an entire column consists of null values must preserve the column in the output with all null values intact, and the schema must retain the original column data type
- **Concurrent Write Conflict:** Writing to the same output path from two simultaneous test cases with Overwrite mode must not corrupt data — the last write wins and produces a valid, readable output

## Dependencies

- **FEATURE-003-02** (Connector Testing Framework) — parent feature providing the overall testing framework context and integration requirements
- **STORY-003-02-01** (Read Test Harness) — write tests depend on read-back validation using the read harness to confirm written data integrity
- **FEATURE-003-01** (Connector Scaffolding Generator) — generated connector projects must be testable with this write harness
- **F-008** (Data Source Connectors) — DataSource V2 write API interfaces (`FileWrite`, `OutputWriterFactory`) that define the write contracts validated by this harness
- `org.apache.spark.sql.QueryTest` — base test class providing the `checkAnswer` assertion method used in all write-read round-trip validations
- `org.apache.spark.sql.test.SharedSparkSession` — shared `SparkSession` lifecycle management for test execution isolation
- `connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroSuite.scala` — reference write test patterns including save modes, compression codec configuration, and write-read round-trip validation (Lines 585–756)
- `connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroSerdeSuite.scala` — SerDe round-trip test patterns demonstrating serializer/deserializer assertion methodology (Lines 32–48)
- `connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroWriteBenchmark.scala` — codec sweep patterns including `AvroCompressionCodec.values` iteration, compression level parameters, and ZSTANDARD buffer pool configuration (Lines 40–107)

## Story Estimation Guidance

- **Story Points:** 5 (Fibonacci scale)
- **Justification:** Requires designing an abstract base class, implementing 7+ parameterized test methods covering write operations, save modes, codec sweeps, and SerDe round-trips. Moderate complexity due to save mode state management (pre-existing files) and codec compatibility verification. Uses established patterns from `AvroSuite` and `AvroSerdeSuite`. Completable within a single sprint by one engineer.

## Definition of Done

- Abstract base class `ConnectorWriteTestBase` is implemented extending `QueryTest with SharedSparkSession`
- All write test categories (single-column, multi-column, save modes, codec sweeps, SerDe round-trip, empty DataFrame) have passing parameterized test methods
- At least one concrete test class extends `ConnectorWriteTestBase` for the Avro connector and passes all tests
- Error handling tests confirm expected exceptions for invalid codec and invalid save mode scenarios
- Save mode tests validate all four modes: Overwrite, Append, ErrorIfExists, Ignore
- Scaladoc documents all public methods and extension points
- All tests pass in CI with both JDK 17 and JDK 21
- No forbidden terms appear in any acceptance criteria text
