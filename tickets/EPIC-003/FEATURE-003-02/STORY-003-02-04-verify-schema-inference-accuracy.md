# Verify Schema Inference Accuracy for DataSource V2 Connectors to Ensure Inferred Types Match Expected Spark SQL Data Types

## User Story

**As a** data engineer,

**I want** a schema inference accuracy validator that tests all Spark primitive types (BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, BinaryType), complex types (ArrayType, MapType, StructType), decimal types with configurable precision and scale, date/timestamp types, and nested structures to confirm that connector-inferred schemas match expected Spark data types,

**So that** I can detect schema inference bugs before they reach production data pipelines, preventing silent data corruption caused by type mismatches that affect up to 15% of custom connector deployments based on community-reported issues.

## Acceptance Criteria

### AC1 — Input Validation: Test Fixture Prerequisite Check

**Given** a connector implementation that extends the schema inference test class and provides test data files with known schemas,
**When** the test initializes,
**Then** it validates that at least one test data file exists in the configured `testDataPath` directory and that the `expectedSchema` definition returned by the abstract method is non-null and contains at least one `StructField` before executing inference tests.

### AC2 — Expected Output (Primitive Types): Exact Type Matching for All Nine Spark Primitive Types

**Given** test data files containing one column for each of the 9 Spark primitive types — BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, and BinaryType,
**When** the schema inference test calls `spark.read.format(connectorFormat).load(testDataPath).schema`,
**Then** each inferred column's `DataType` matches the expected Spark SQL type defined in the test fixture with zero type substitutions — for example, an IntegerType column is not inferred as LongType.

### AC3 — Expected Output (Complex Types): Matching Element Types and Nested Field Definitions

**Given** test data files containing columns of ArrayType(IntegerType), MapType(StringType, IntegerType), and StructType with two nested fields (name: StringType, value: IntegerType),
**When** schema inference executes via `spark.read.format(connectorFormat).load(testDataPath).schema`,
**Then** the inferred schema includes complex types with matching element types for ArrayType, matching key and value types for MapType, and matching field names and data types for StructType — each validated by `StructType` equality assertion.

### AC4 — Decimal Types: Precision and Scale Preservation

**Given** test data containing decimal values with precision 10 and scale 2,
**When** schema inference executes,
**Then** the inferred `DecimalType` has precision equal to 10 and scale equal to 2, not a default DecimalType(10,0) or a rounded precision value — confirmed by asserting `DecimalType(10, 2)` equality on the inferred field.

### AC5 — Date and Timestamp Types: Correct Temporal Type Inference

**Given** test data containing date values (conforming to DateType) and timestamp values (conforming to TimestampType),
**When** schema inference executes,
**Then** date columns are inferred as `DateType` and timestamp columns are inferred as `TimestampType` — not as StringType or LongType — confirmed by exact `DataType` comparison on each temporal column.

### AC6 — Nested Structures: Three-Level Nesting Preservation

**Given** test data with a 3-level nested structure — a top-level StructType containing an ArrayType whose elements are StructType with two fields (id: IntegerType, label: StringType),
**When** schema inference executes,
**Then** all 3 nesting levels are preserved in the inferred schema with matching field names and data types at each level, validated by recursive `StructType` comparison from the root to the innermost struct fields.

### AC7 — Error Handling: Clear Exception for Unsupported Types

**Given** a data file containing an unsupported or ambiguous data type that cannot be mapped to any Spark SQL type,
**When** schema inference executes,
**Then** the validator throws a `SparkException` or `AnalysisException` with a message that identifies both the unsupported type name and the column name where inference failed — not a generic `NullPointerException` or `ClassCastException`.

### AC8 — Edge Case (Nullable Inference): All-Null Column Handling

**Given** a data file where a specific column contains only null values across all rows,
**When** schema inference executes,
**Then** the inferred type for that column defaults to `NullType` or `StringType` (depending on connector-specific behavior) and the column's `StructField` has `nullable` set to `true` in its metadata.

## Sub-Tasks

1. **Create abstract test class `ConnectorSchemaInferenceTest`** extending `QueryTest` with `SharedSparkSession`, following the pattern established in `AvroSuite` (`Source: connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroSuite.scala` — lines 57–61)
2. **Define abstract method `def connectorFormat: String`** returning the DataSource V2 format identifier string (e.g., `"avro"`, `"parquet"`)
3. **Define abstract method `def testDataPath: String`** returning the filesystem path to the directory containing test data files with known schemas
4. **Define abstract method `def expectedSchema: StructType`** returning the expected `StructType` that the connector's schema inference must produce for the test data
5. **Implement primitive type test** covering all 9 Spark primitive types (BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, BinaryType), based on the Avro schema parsing and StructType comparison patterns in `AvroSuite` (`Source: connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroSuite.scala` — lines 83–100: `Schema.Parser` and `StructType` equality assertions)
6. **Implement complex type test** for ArrayType, MapType, and StructType — verifying element types, key/value types, and nested field definitions
7. **Implement decimal type test** with configurable precision and scale, asserting exact `DecimalType(precision, scale)` equality
8. **Implement date/timestamp type test** asserting DateType and TimestampType inference (not StringType or LongType fallback)
9. **Implement nested structure test** with 3-level nesting — StructType containing ArrayType containing StructType — validating recursive field name and type preservation
10. **Implement nullable inference test** for all-null columns — confirming `nullable=true` in the inferred `StructField`
11. **Implement error handling test** for unsupported types — confirming `SparkException` or `AnalysisException` with descriptive error messages
12. **Implement schema comparison utility method** (`def compareSchemas(inferred: StructType, expected: StructType): Seq[String]`) that produces a detailed diff output listing each mismatched field name, expected type, and inferred type
13. **Write concrete validation test using the Avro connector** as the reference implementation, extending `ConnectorSchemaInferenceTest` with `connectorFormat = "avro"` and leveraging the `inferSchema` method in `AvroTable` (`Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroTable.scala` — line 42: `AvroUtils.inferSchema(sparkSession, options.asScala.toMap, files)`)
14. **Document usage and extension points in Scaladoc** — including class-level documentation explaining how to extend the base class for a new connector and method-level documentation for each abstract method

## Edge Cases

### 1. Empty/Null Input — Empty Directory with No Files

Attempting schema inference on an empty directory containing no data files must either return `None` (empty schema) or throw an `AnalysisException` with a message indicating that no files were found for schema inference — it must not produce a `NullPointerException`, `NoSuchElementException`, or any unhandled runtime error.

### 2. Boundary Values — Minimal and Wide Schemas

Inferring schema from a file with exactly 1 column and 1 row must produce a valid single-field `StructType` with the correct data type. Inferring schema from a file with 1,000 columns (matching the wide-table pattern used in `AvroReadBenchmark`) must produce a `StructType` containing all 1,000 fields without truncation or field loss.

### 3. Invalid Input — Corrupt Data File

Providing a corrupt file that cannot be parsed for schema information (e.g., a truncated binary file or a file with an invalid magic number) must produce an exception with a message identifying the specific file path and the nature of the corruption — not a generic `IOException` with no context.

### 4. Schema Evolution — Merged Schema from Multiple Files

Inferring schema from two data files where the second file contains additional columns not present in the first must produce a merged schema containing all columns from both files when the `mergeSchema` option is set to `true`. When `mergeSchema` is `false`, only the schema from the first file (or a representative file) must be returned.

### 5. Type Widening — Integer-to-Long Column Conflict Across Files

Inferring schema from two data files where the same column is stored as IntegerType in one file and LongType in another must produce `LongType` (the wider type) in the merged schema when `mergeSchema` is enabled — the validator must not throw a type conflict error or silently select the narrower type.

## Dependencies

- **FEATURE-003-02 (Connector Testing Framework)** — Parent feature providing the overall testing framework scope and shared test infrastructure; this story delivers one of the five testing harness components
  - `Link: [FEATURE-003-02-connector-testing-framework.md](../FEATURE-003-02-connector-testing-framework.md)`
- **EPIC-003 (Unified Connector Development SDK)** — Grandparent epic establishing the unified SDK scope
  - `Link: [EPIC-003-unified-connector-development-sdk.md](../../EPIC-003-unified-connector-development-sdk.md)`
- **STORY-003-02-01 (Read Test Harness)** — Schema inference validation is an extension of the read path; the read test harness provides the shared `SparkSession` lifecycle and data loading patterns that this story builds upon
- **F-008 (Data Source Connectors)** — DataSource V2 `inferSchema` interface defined in `FileTable.inferSchema` that all file-based connectors implement
- **`org.apache.spark.sql.types.*`** — All Spark SQL data types: BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, BinaryType, DateType, TimestampType, DecimalType, ArrayType, MapType, StructType, NullType, and StructField
- **Source reference: `connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroTable.scala`** — Line 42: `inferSchema` implementation calling `AvroUtils.inferSchema(sparkSession, options.asScala.toMap, files)`, serving as the reference connector for concrete test validation
- **Source reference: `connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroSuite.scala`** — Lines 83–160: Schema validation test patterns including `checkAvroSchemaEquals` using `Schema.Parser`, `StructType.fromDDL`, and `StructType` equality assertions that inform the test design
- **Source reference: `connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroScanSuite.scala`** — `FileScanSuiteBase` extension and `ScanBuilder` parameterization pattern used for scan-level schema validation

## Story Estimation Guidance

**Story Points: 5** (Fibonacci scale)

**Justification:** This story requires implementing type-specific test methods for 9 Spark primitive types, 3 complex types (ArrayType, MapType, StructType), decimal with configurable precision/scale, date/timestamp types, and 3-level nested structures — each requiring test data fixture creation and schema comparison assertions. Additional complexity arises from handling type coercion edge cases (type widening, nullable inference, schema evolution with mergeSchema), implementing a detailed schema diff utility, and writing the concrete Avro connector validation. The scope is moderate — no external integrations, no UI components, and the test patterns are well-established in the existing Avro test suite. Completable within a single sprint by one engineer.

## Definition of Done

- Schema inference test class (`ConnectorSchemaInferenceTest`) is implemented as an abstract base class extending `QueryTest` with `SharedSparkSession`
- Abstract methods `connectorFormat`, `testDataPath`, and `expectedSchema` are defined and documented
- Primitive type test validates all 9 Spark primitive types (BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, BinaryType) with exact type matching
- Complex type tests validate ArrayType, MapType, and StructType with matching element types, key/value types, and field definitions
- Decimal type test confirms precision and scale preservation (e.g., DecimalType(10, 2) not DecimalType(10, 0))
- Date and timestamp tests confirm correct temporal type inference — DateType and TimestampType, not StringType or LongType fallback
- Nested structure test validates 3-level nesting preservation with recursive field name and type verification
- Error handling test validates that unsupported types produce `SparkException` or `AnalysisException` with descriptive messages identifying the column name and unsupported type
- Schema comparison utility method produces readable diff output listing field-by-field mismatches between inferred and expected schemas
- At least one concrete test class validates the Avro connector schema inference by extending `ConnectorSchemaInferenceTest` with `connectorFormat = "avro"`
- All edge cases (empty directory, minimal/wide schemas, corrupt files, schema evolution, type widening) are covered by dedicated test methods
- All tests pass in CI with both JDK 17 and JDK 21 runtime environments
- Scaladoc documentation covers class-level usage, abstract method contracts, and extension guidance for new connectors
- Zero forbidden vague terms appear in any acceptance criteria text — all criteria use precise, measurable language
- Code review completed and approved by at least one maintainer
