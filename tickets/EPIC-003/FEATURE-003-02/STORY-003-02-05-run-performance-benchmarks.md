# Run Performance Benchmarks on DataSource V2 Connectors to Produce Standardized Throughput and Latency Reports for JDK17 and JDK21

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

**As a** platform engineer,

**I want** a performance benchmark harness that extends Spark's BenchmarkBase framework and produces standardized benchmark output with Best Time(ms), Avg Time(ms), Stdev(ms), Rate(M/s), Per Row(ns), and Relative columns for both JDK 17 and JDK 21 environments, covering read benchmarks (single-column scan, multi-column scan, wide-table scan with 1,000 columns), write benchmarks (single-column output, multi-column output, partitioned output), and compression codec sweeps (SNAPPY, ZSTANDARD, DEFLATE, BZIP2, XZ with level parameter sweeps),

**So that** I can establish baseline performance metrics for any custom connector and detect performance regressions across JDK versions, enabling data-driven optimization decisions and maintaining throughput SLAs with less than 10% variance between benchmark runs.

## Acceptance Criteria

### AC1: Input Validation — Benchmark Harness Initialization

- **Given** a connector implementation that extends the benchmark harness and provides a valid connector format name and test data configuration
- **When** the benchmark harness initializes
- **Then** it validates that the connector format name is non-empty, the output directory for benchmark results is writable, and the `SPARK_GENERATE_BENCHMARK_FILES` environment variable is checked to determine whether to write output files

> `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroReadBenchmark.scala` — Line 37: `SPARK_GENERATE_BENCHMARK_FILES=1` controls output file generation

### AC2: Expected Output — Single-Column Numeric Read Benchmark

- **Given** test data containing 15,728,640 rows (matching the AvroReadBenchmark row count of `1024 * 1024 * 15`)
- **When** the read benchmark suite executes single-column numeric scan tests for TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE data types
- **Then** each benchmark produces a results table with columns Best Time(ms), Avg Time(ms), Stdev(ms), Rate(M/s), Per Row(ns), Relative, and the output follows the exact format: environment header (JVM version, OS, CPU), benchmark title header with separator lines, and labeled result rows (e.g., "Sum")

> `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroReadBenchmark.scala` — Lines 60–78: `numericScanBenchmark` method iterating over six data types
>
> `Source: connector/avro/benchmarks/AvroReadBenchmark-results.txt` — Lines 1–9: Output format reference with environment header, metric column header, separator, and "Sum" result row

### AC3: Expected Output — Write Benchmark

- **Given** a source DataFrame with 100,000 rows
- **When** the write benchmark suite executes single-column output, multi-column output, and partitioned output tests
- **Then** each benchmark produces a results table with the same 6-column metric header (Best Time(ms), Avg Time(ms), Stdev(ms), Rate(M/s), Per Row(ns), Relative) and the output file is named `<ConnectorName>WriteBenchmark-results.txt`

> `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroWriteBenchmark.scala` — Lines 40–107: `AvroWriteBenchmark` extending `DataSourceWriteBenchmark` with `runDataSourceBenchmark("Avro")` entry point
>
> `Source: connector/avro/benchmarks/AvroWriteBenchmark-results.txt` — Lines 1–9: Write benchmark output showing Output Single Int Column, Output Single Double Column, Output Int and String Column, Output Partitions, and Output Buckets result rows

### AC4: Codec Sweep — Compression Codec Iteration

- **Given** the write benchmark test data
- **When** the codec sweep benchmark iterates over compression codecs (SNAPPY, ZSTANDARD, DEFLATE, BZIP2, XZ, UNCOMPRESSED) and parameter levels (1, 3, 5, 7, 9 for codecs that support compression levels)
- **Then** each codec and level combination produces a labeled result row in the benchmark output, and ZSTANDARD additionally tests `bufferPool.enabled` variants (true and false) as done in AvroWriteBenchmark lines 88–94

> `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroWriteBenchmark.scala` — Lines 76–98: Codec sweep iterating `AvroCompressionCodec.values()`, filtering codecs that support compression levels, and adding ZSTANDARD `bufferPool.enabled` variant benchmarks
>
> `Source: connector/avro/benchmarks/AvroWriteBenchmark-results.txt` — Lines 13–55: Codec comparison output showing BZIP2, DEFLATE, UNCOMPRESSED, SNAPPY, XZ, ZSTANDARD rows, followed by per-codec level sweep tables with `bufferPool.enabled` variants for ZSTANDARD

### AC5: JDK Dual Output — JDK-Version-Specific File Naming

- **Given** the `SPARK_GENERATE_BENCHMARK_FILES=1` environment variable is set
- **When** the benchmark harness detects JDK 17 runtime
- **Then** output is written to `<ConnectorName>ReadBenchmark-results.txt` and `<ConnectorName>WriteBenchmark-results.txt`
- **When** the harness detects JDK 21 runtime
- **Then** output is written to `<ConnectorName>ReadBenchmark-jdk21-results.txt` and `<ConnectorName>WriteBenchmark-jdk21-results.txt`

> `Source: connector/avro/benchmarks/AvroReadBenchmark-results.txt` — Line 5: `OpenJDK 64-Bit Server VM 17.0.16+8-LTS` environment header for JDK 17 output
>
> `Source: connector/avro/benchmarks/AvroReadBenchmark-jdk21-results.txt` — Line 6: `OpenJDK 64-Bit Server VM 21.0.8+9-LTS` environment header for JDK 21 output

### AC6: Wide-Table Benchmark — High Column Count Scan

- **Given** a DataFrame with 1,000 columns and 100,000 rows (matching AvroWriteBenchmark lines 46–47)
- **When** the wide-table read benchmark executes select-all and single-column-from-wide-table tests (100, 200, 300 columns)
- **Then** each test produces metrics in the standard output format with Per Row(ns) values

> `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroReadBenchmark.scala` — Lines 195–217: `wideColumnsBenchmark` method scanning 1,000-column tables with `Select of all columns` test case
>
> `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroReadBenchmark.scala` — Lines 172–193: `columnsBenchmark` method scanning 100, 200, 300 wide-column tables with `Sum of single column` test case

### AC7: Error Handling — Benchmark Exception Isolation

- **Given** a connector that throws an exception during read or write within a benchmark case
- **When** the benchmark framework encounters the exception
- **Then** the benchmark reports the failure for that specific case with the exception message and continues executing remaining benchmark cases without aborting the entire suite

### AC8: Edge Case — Filter Pushdown Benchmark Comparison

- **Given** the read benchmark dataset
- **When** filter pushdown benchmark tests execute with three configurations (no filter, pushdown disabled, pushdown enabled)
- **Then** each configuration produces separate result rows and the Relative column for the pushdown-enabled case shows a numeric speedup factor compared to the no-filter baseline

> `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroReadBenchmark.scala` — Lines 220–272: `filtersPushdownBenchmark` method with `w/o filters`, `pushdown disabled`, and `w/ filters` test cases using `SQLConf.AVRO_FILTER_PUSHDOWN_ENABLED`

## Sub-Tasks

1. **Create abstract class `ConnectorBenchmarkBase` extending `SqlBasedBenchmark`**
   - Model after `object AvroReadBenchmark extends SqlBasedBenchmark` (AvroReadBenchmark line 41)
   - Define abstract method `def connectorFormat: String` returning the connector's DataSource V2 format identifier
   - Define abstract method `def supportedCodecs: Seq[String]` returning the list of compression codec names supported by the connector
   - Implement `withTempTable(tableNames: String*)(f: => Unit)` helper for temp view lifecycle management (AvroReadBenchmark lines 44–46)
   - Implement `prepareTable(dir: File, df: DataFrame, partition: Option[String])` helper for writing test data in the connector's format and registering a temp view (AvroReadBenchmark lines 48–58)

2. **Implement read benchmark methods**
   - `numericScanBenchmark(values: Int, dataType: DataType)` — single-column scan for TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE using `sum(id)` aggregation (AvroReadBenchmark lines 60–78)
   - `intStringScanBenchmark(values: Int)` — combined int+string column scan using `sum(c1), sum(length(c2))` aggregation (AvroReadBenchmark lines 80–99)
   - `partitionTableScanBenchmark(values: Int)` — partitioned table scan testing data column, partition column, and both columns (AvroReadBenchmark lines 101–127)
   - `repeatedStringScanBenchmark(values: Int)` — repeated string scan using `sum(length(c1))` aggregation (AvroReadBenchmark lines 129–145)
   - `stringWithNullsScanBenchmark(values: Int, fractionOfNulls: Double)` — null-rate sensitivity tests at 0%, 50%, 95% null fractions (AvroReadBenchmark lines 147–170)
   - `columnsBenchmark(values: Int, width: Int)` — single-column scan from wide tables with 100, 200, 300 columns (AvroReadBenchmark lines 172–193)
   - `wideColumnsBenchmark(values: Int, width: Int, files: Int)` — select-all from 1,000-column table repartitioned across multiple files (AvroReadBenchmark lines 195–217)
   - `filtersPushdownBenchmark(rowsNum: Int, numIters: Int)` — three-configuration filter pushdown comparison: no filter, pushdown disabled, pushdown enabled (AvroReadBenchmark lines 220–272)

3. **Implement write benchmark methods**
   - Single-column, multi-column, partitioned, and bucketed output benchmarks via `runDataSourceBenchmark(connectorFormat)` delegating to `DataSourceWriteBenchmark` (AvroWriteBenchmark line 104)
   - Codec sweep using `supportedCodecs` with write-to-temp-path pattern and `addBenchmark` helper (AvroWriteBenchmark lines 58–80)
   - Compression level parameter sweeps for codecs supporting compression levels, iterating levels 1, 3, 5, 7, 9 (AvroWriteBenchmark lines 82–97)
   - ZSTANDARD `bufferPool.enabled` variant benchmarks toggling the buffer pool setting for each level (AvroWriteBenchmark lines 88–94)
   - Wide-column write benchmark with 1,000 columns and 100,000 rows repartitioned across 12 files (AvroWriteBenchmark lines 41–101)

4. **Implement JDK version detection for output file naming**
   - Detect JDK major version from `System.getProperty("java.version")` or `java.specification.version`
   - For JDK 17: use `<ConnectorName>ReadBenchmark-results.txt` and `<ConnectorName>WriteBenchmark-results.txt`
   - For JDK 21: use `<ConnectorName>ReadBenchmark-jdk21-results.txt` and `<ConnectorName>WriteBenchmark-jdk21-results.txt`
   - Default to non-suffixed file names if JDK version cannot be determined

5. **Implement `runBenchmarkSuite(mainArgs: Array[String])` entry point**
   - Orchestrate all read benchmarks in sequence: numeric scans (6 types), int+string, partition, repeated string, string with nulls (3 rates), wide-column select-all, single-column from wide columns (100, 200, 300), filters pushdown (AvroReadBenchmark lines 274–307)
   - Orchestrate all write benchmarks in sequence: data source write benchmark, wide-column codec sweep (AvroWriteBenchmark lines 103–106)

6. **Write integration test executing benchmark harness with Avro connector**
   - Create `AvroConnectorBenchmarkTest` extending `ConnectorBenchmarkBase` with `connectorFormat = "avro"` and `supportedCodecs` matching `AvroCompressionCodec.values()`
   - Validate that output matches the format of existing `AvroReadBenchmark-results.txt` and `AvroWriteBenchmark-results.txt`

7. **Document usage**
   - How to run benchmarks: `build/sbt "<module>/Test/runMain <BenchmarkClass>"`
   - How to generate output files: `SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "<module>/Test/runMain <BenchmarkClass>"`
   - How to interpret results: column definitions (Best Time, Avg Time, Stdev, Rate, Per Row, Relative), environment header meaning, JDK version output distinction

## Edge Cases

### Edge Case 1: Empty Dataset — Zero Rows

Running a benchmark on an empty dataset (zero rows) must produce a valid benchmark output with all metric columns showing 0 or near-zero values. The harness must not throw a division-by-zero error in Rate(M/s) or Per Row(ns) calculations. The output file must contain the standard environment header and metric column header even when no meaningful data was processed.

### Edge Case 2: Boundary Values — Single Iteration and Maximum Row Count

Running a benchmark with exactly 1 iteration must produce valid Best Time(ms) and Avg Time(ms) values that are identical, with Stdev(ms) showing 0. Running a benchmark with the maximum row count used in Avro benchmarks (15,728,640 rows per AvroReadBenchmark, computed as `1024 * 1024 * 15`) must complete within the JVM heap limit without triggering an `OutOfMemoryError`. The Relative column must display `1.0X` for the first benchmark case in every benchmark group.

> `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroReadBenchmark.scala` — Line 277: Row count `1024 * 1024 * 15` = 15,728,640 rows for numeric scan benchmarks

### Edge Case 3: Invalid Input — Unsupported Compression Codec

Specifying a compression codec not supported by the connector (e.g., passing `"LZ4"` to a connector that only supports SNAPPY, DEFLATE, and ZSTANDARD) must produce a clear error message in the benchmark output for that specific codec case. The error must include the unsupported codec name and the list of codecs the connector supports. Remaining benchmark cases for other codecs must continue executing without interruption.

### Edge Case 4: JDK Version Detection Failure

If the JDK version cannot be determined from `System.getProperty("java.version")` or `System.getProperty("java.specification.version")` (e.g., the property returns `null`), the benchmark must default to the non-suffixed output file name (e.g., `ConnectorReadBenchmark-results.txt` instead of `ConnectorReadBenchmark-jdk21-results.txt`) and log a warning message indicating the JDK version could not be resolved.

### Edge Case 5: Disk Space Exhaustion During Write Benchmark

If the temp directory runs out of disk space during a write benchmark (e.g., when writing a 1,000-column table with compression), the benchmark must catch the `IOException` and report the failure for that specific write benchmark case with the exception details. The entire benchmark suite must not crash, and remaining benchmark cases must continue executing.

## Dependencies

- **FEATURE-003-02 (Connector Testing Framework)** — Parent feature defining the overall testing framework scope
  - `Source: tickets/EPIC-003/FEATURE-003-02-connector-testing-framework.md`
- **STORY-003-02-01 (Read Test Harness)** — Benchmark reads leverage the same read path patterns and `withTempTable`/`prepareTable` utilities established by the read test harness
- **STORY-003-02-02 (Write Test Harness)** — Benchmark writes leverage the same write path patterns, save mode utilities, and codec configuration established by the write test harness
- **F-008 (Data Source Connectors)** — DataSource V2 read/write API interfaces (`Table`, `ScanBuilder`, `WriteBuilder`, `DataSourceRegister`) that connectors under benchmark must implement
- **`org.apache.spark.benchmark.Benchmark`** — Spark's core benchmark framework class that produces the tabular output with Best Time(ms), Avg Time(ms), Stdev(ms), Rate(M/s), Per Row(ns), and Relative columns
- **`org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark`** — SQL-specific benchmark base class managing SparkSession lifecycle and benchmark output stream
  - `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroReadBenchmark.scala` — Line 41: `object AvroReadBenchmark extends SqlBasedBenchmark`
- **`org.apache.spark.sql.execution.benchmark.DataSourceWriteBenchmark`** — Write benchmark base class providing `runDataSourceBenchmark` method for standardized write throughput tests
  - `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroWriteBenchmark.scala` — Line 40: `object AvroWriteBenchmark extends DataSourceWriteBenchmark`
- **Avro benchmark reference implementations:**
  - `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroReadBenchmark.scala` — Lines 41–308: Complete read benchmark patterns including `numericScanBenchmark`, `intStringScanBenchmark`, `partitionTableScanBenchmark`, `repeatedStringScanBenchmark`, `stringWithNullsScanBenchmark`, `columnsBenchmark`, `wideColumnsBenchmark`, `filtersPushdownBenchmark`
  - `Source: connector/avro/src/test/scala/org/apache/spark/sql/execution/benchmark/AvroWriteBenchmark.scala` — Lines 40–107: Complete write benchmark patterns including `wideColumnsBenchmark`, `addBenchmark` helper, codec sweep, compression level sweeps, ZSTANDARD `bufferPool.enabled` variants
- **Avro benchmark output format references:**
  - `Source: connector/avro/benchmarks/AvroReadBenchmark-results.txt` — Read benchmark output format (JDK 17 environment header, 6-column metric table, result rows)
  - `Source: connector/avro/benchmarks/AvroReadBenchmark-jdk21-results.txt` — Read benchmark output format (JDK 21 environment header)
  - `Source: connector/avro/benchmarks/AvroWriteBenchmark-results.txt` — Write benchmark and codec sweep output format (JDK 17)
  - `Source: connector/avro/benchmarks/AvroWriteBenchmark-jdk21-results.txt` — Write benchmark and codec sweep output format (JDK 21)

## Story Estimation Guidance

| Parameter | Value |
|---|---|
| **Story Points** | **8** (Fibonacci scale) |
| **Complexity** | High — most complex story in the feature |
| **Justification** | Requires implementing 10+ benchmark methods across read, write, codec sweep, filter pushdown, and wide-table categories. Includes JDK version detection logic, precise output file formatting matching Spark's benchmark standard, ZSTANDARD `bufferPool.enabled` variant testing, and compression level parameter sweeps. Uses well-established patterns from `AvroReadBenchmark` and `AvroWriteBenchmark` as reference implementations. Completable within a single sprint but requires focused effort across benchmark framework integration, output formatting, and integration test validation. |
| **Key Effort Drivers** | Breadth of benchmark categories (6 numeric scan types, int+string, partition, repeated string, null-sensitivity at 3 rates, wide-column, filter pushdown, single-column/multi-column/partitioned/bucketed writes, codec sweep with level parameters); JDK dual-output file naming; integration test with Avro connector |
| **Risk Factors** | Benchmark output format must match existing Spark conventions precisely; codec support varies per connector requiring dynamic iteration; JVM heap constraints with large row counts (15,728,640 rows) |

## Definition of Done

- Benchmark harness produces output matching the Spark benchmark format with the 6-column metric header: Best Time(ms), Avg Time(ms), Stdev(ms), Rate(M/s), Per Row(ns), Relative
- Read benchmarks cover all categories: single-column numeric scans (TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE), int+string scan, partition scan, repeated string, null-rate sensitivity (0%, 50%, 95%), wide-table scan (1,000 columns), and filter pushdown comparison (no filter, pushdown disabled, pushdown enabled)
- Write benchmarks cover all categories: single-column output, multi-column output, partitioned output, bucketed output, and codec sweep across all supported codecs
- Codec sweep iterates over all supported codecs (SNAPPY, ZSTANDARD, DEFLATE, BZIP2, XZ, UNCOMPRESSED) and compression level parameter sweeps (levels 1, 3, 5, 7, 9) for codecs that support compression levels
- ZSTANDARD benchmark includes `bufferPool.enabled` variant tests (true and false) for each compression level
- JDK 17 and JDK 21 produce separate output files with the naming convention: `<ConnectorName>ReadBenchmark-results.txt` / `<ConnectorName>WriteBenchmark-results.txt` for JDK 17, and `<ConnectorName>ReadBenchmark-jdk21-results.txt` / `<ConnectorName>WriteBenchmark-jdk21-results.txt` for JDK 21
- `SPARK_GENERATE_BENCHMARK_FILES` environment variable controls whether benchmark results are written to output files
- At least one concrete benchmark validates the Avro connector, producing output that matches the format of existing `AvroReadBenchmark-results.txt` and `AvroWriteBenchmark-results.txt`
- All benchmarks execute without unhandled exceptions; failures in individual benchmark cases are reported with exception messages and do not abort the entire suite
- Zero terms from the project-defined forbidden vocabulary list appear in any acceptance criteria text
