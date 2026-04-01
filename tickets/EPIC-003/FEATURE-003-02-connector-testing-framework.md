# Build Connector Testing Framework to Standardize DataSource V2 Connector Validation and Benchmarking

## Feature Summary

Connector developers lack standardized test harnesses for validating DataSource V2 implementations, forcing each team to write custom test suites from scratch and resulting in inconsistent quality coverage across the connector ecosystem. The Connector Testing Framework provides reusable abstract base test classes for read operations, write operations, predicate pushdown compliance, schema inference accuracy, and performance benchmarks — reducing test development effort by up to 70% and ensuring uniform validation standards across all custom connectors. Modeled after the proven test patterns in the Apache Spark repository — including `AvroSuite` (extending `QueryTest` with `SharedSparkSession`, using `checkAnswer`/`withSQLConf`/`withTempPath`/`withTable` patterns), `AvroScanSuite` (extending `FileScanSuiteBase` for scan-level validation), `AvroSerdeSuite` (extending `SparkFunSuite` for serialization round-trip verification), `ProtobufTestBase` (shared trait extending `SQLTestUtils`), and `AvroReadBenchmark`/`AvroWriteBenchmark` (extending `SqlBasedBenchmark`/`DataSourceWriteBenchmark` for JDK17/JDK21 performance profiling) — the framework abstracts these patterns into configurable, connector-agnostic base classes that any DataSource V2 implementation can extend.

## User Stories Index

| Story ID | Story Name | Description | Link |
|---|---|---|---|
| STORY-003-02-01 | Provide Test Harness for Read Operations | Delivers an abstract base test class with parameterized tests covering single-column reads, multi-column reads, partition discovery, schema merging, and filter pushdown during scan operations | [STORY-003-02-01-provide-test-harness-for-read-operations.md](./FEATURE-003-02/STORY-003-02-01-provide-test-harness-for-read-operations.md) |
| STORY-003-02-02 | Provide Test Harness for Write Operations | Delivers an abstract base test class with parameterized tests covering single-column writes, multi-column writes, all four save modes (overwrite, append, error-if-exists, ignore), and compression codec sweeps | [STORY-003-02-02-provide-test-harness-for-write-operations.md](./FEATURE-003-02/STORY-003-02-02-provide-test-harness-for-write-operations.md) |
| STORY-003-02-03 | Validate Predicate Pushdown Compliance | Provides a compliance validator that confirms filter pushdown is invoked for supported filter types (equality, range, in-set, is-null) and that non-pushable predicates fall back to Spark-side filtering | [STORY-003-02-03-validate-predicate-pushdown-compliance.md](./FEATURE-003-02/STORY-003-02-03-validate-predicate-pushdown-compliance.md) |
| STORY-003-02-04 | Verify Schema Inference Accuracy | Provides a schema inference validator that confirms inferred schemas match expected Spark data types for all primitive types, complex types (array, map, struct), and nested structures | [STORY-003-02-04-verify-schema-inference-accuracy.md](./FEATURE-003-02/STORY-003-02-04-verify-schema-inference-accuracy.md) |
| STORY-003-02-05 | Run Performance Benchmarks | Delivers a benchmark harness that produces Spark benchmark-format output with Best Time(ms), Avg Time(ms), Stdev(ms), Rate(M/s), Per Row(ns), and Relative columns for JDK17 and JDK21 runtimes | [STORY-003-02-05-run-performance-benchmarks.md](./FEATURE-003-02/STORY-003-02-05-run-performance-benchmarks.md) |

## Dependencies

- **EPIC-003 (Unified Connector Development SDK)** — Parent epic that defines the overall SDK scope encompassing both scaffolding generation and testing framework capabilities
- **FEATURE-003-01 (Connector Scaffolding Generator)** — Generated connector projects from the scaffolding generator serve as the primary input for the testing framework; test base classes must be compatible with the project structure and build configuration produced by the generator
- **F-008 (Data Source Connectors)** — DataSource V2 API interfaces (`Table`, `ScanBuilder`, `Scan`, `Batch`, `PartitionReaderFactory`, `WriteBuilder`, `BatchWrite`, `DataWriter`, `DataSourceRegister`) that define the contracts all connectors must implement and that the testing framework validates against
- **Existing test infrastructure:**
  - `SparkFunSuite`, `QueryTest`, `SharedSparkSession` — Core test base classes used by all Spark test suites for session lifecycle management, DataFrame assertion utilities, and SQL configuration control
    - `Source: connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroSuite.scala`
  - `FileScanSuiteBase` — Scan-level test base class for validating file-based DataSource V2 scan implementations
    - `Source: connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroScanSuite.scala`
  - `SQLTestUtils` — Utility trait providing `withTempPath`, `withTempView`, `withTable`, and other test helper methods
    - `Source: connector/protobuf/src/test/scala/org/apache/spark/sql/protobuf/ProtobufTestBase.scala`
- **Existing benchmark infrastructure:**
  - `SqlBasedBenchmark` — Base class for SQL-oriented read benchmarks that manages Spark session lifecycle and benchmark output formatting
    - `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroReadBenchmark.scala`
  - `DataSourceWriteBenchmark` — Base class for data source write benchmarks with codec sweep and compression parameter sweep support
    - `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroWriteBenchmark.scala`
  - Spark `Benchmark` class — Core benchmark runner producing tabular output with Best Time(ms), Avg Time(ms), Stdev(ms), Rate(M/s), Per Row(ns), and Relative columns
    - `Source: sql/core/src/test/scala/org/apache/spark/sql/execution/benchmark/`
- **Existing connector test suites (reference patterns):**
  - `AvroSuite` — Integration test suite demonstrating `checkAnswer`, `withSQLConf`, `withTempPath`, and `withTable` patterns for end-to-end read/write validation
    - `Source: connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroSuite.scala`
  - `AvroSerdeSuite` — Serialization/deserialization round-trip test suite demonstrating `SparkFunSuite` extension with field-match-type parameterized tests
    - `Source: connector/avro/src/test/scala/org/apache/spark/sql/avro/AvroSerdeSuite.scala`
  - `ProtobufTestBase` — Shared test base trait demonstrating reusable descriptor file resolution, DDL parsing utilities, and test fixture management
    - `Source: connector/protobuf/src/test/scala/org/apache/spark/sql/protobuf/ProtobufTestBase.scala`
  - `ProtobufFunctionsSuite` — Functions test suite demonstrating `QueryTest with SharedSparkSession with ProtobufTestBase` composition for schema-driven connector testing
    - `Source: connector/protobuf/src/test/scala/org/apache/spark/sql/protobuf/ProtobufFunctionsSuite.scala`

## Definition of Done

- Read test harness provides an abstract base class extending `QueryTest` with `SharedSparkSession` that includes parameterized tests for single-column reads, multi-column reads, partition discovery, schema merging, and filtered scan operations — connector developers implement only format-specific data preparation methods
- Write test harness provides an abstract base class extending `QueryTest` with `SharedSparkSession` that includes parameterized tests for single-column writes, multi-column writes, all four save modes (overwrite, append, error-if-exists, ignore), and compression codec sweeps — following the `withTempPath`/`withSQLConf` patterns established by `AvroSuite`
- Predicate pushdown compliance validator executes a defined set of filter pushdown assertions that confirm supported filter types (equality, range, in-set, is-null) are pushed to the connector and that non-pushable predicates fall back to Spark-side filtering — producing a pass/fail compliance report per filter type
- Schema inference accuracy validator compares connector-inferred schemas against expected schemas for all Spark primitive types (`BooleanType`, `ByteType`, `ShortType`, `IntegerType`, `LongType`, `FloatType`, `DoubleType`, `StringType`, `BinaryType`, `DateType`, `TimestampType`, `DecimalType`), complex types (`ArrayType`, `MapType`, `StructType`), and nested structures up to three levels deep
- Performance benchmark harness extends `SqlBasedBenchmark` and produces Spark benchmark-format tabular output with Best Time(ms), Avg Time(ms), Stdev(ms), Rate(M/s), Per Row(ns), and Relative columns for both JDK17 and JDK21 runtime environments — covering read throughput, write throughput, and codec-specific write throughput benchmarks
- All test base classes extend `SparkFunSuite` or `QueryTest` with `SharedSparkSession`, following the same session lifecycle management and assertion patterns used by existing connector test suites
- Framework documentation covers: how to extend base test classes for a new connector, how to configure parameterized test dimensions (data types, file sizes, partition counts), how to run compliance validators in isolation, and how to integrate benchmark harnesses with CI/CD pipelines
- At least one existing connector (Avro) is validated using the framework's base test classes and benchmark harnesses, confirming that the framework produces equivalent test coverage to the existing `AvroSuite`, `AvroScanSuite`, and `AvroReadBenchmark`/`AvroWriteBenchmark` test suites
