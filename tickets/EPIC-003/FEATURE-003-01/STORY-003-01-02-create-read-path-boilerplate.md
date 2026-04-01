# Generate Read Path Boilerplate Code to Provide DataSource V2 File-Based Connector Read Foundation

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

**As a** platform engineer, **I want to** run the scaffolding generator and receive five generated Scala source files implementing the DataSource V2 read path — (1) a DataSourceV2 class extending `FileDataSourceV2` with `fallbackFileFormat`, `shortName()`, and `getTable` methods, (2) a Table case class extending `FileTable` with `newScanBuilder` and `inferSchema` methods, (3) a ScanBuilder case class extending `FileScanBuilder` with `build()` and `pushDataFilters` methods, (4) a Scan case class extending `FileScan` with `isSplitable` and `createReaderFactory` methods, and (5) a PartitionReaderFactory case class extending `FilePartitionReaderFactory` with a `buildReader` method stub, **so that** the connector has a complete, compilable read pipeline foundation that follows the established Spark DataSource V2 contract, reducing read path implementation effort from 3–5 days of API discovery to under 4 hours of format-specific logic implementation.

## Acceptance Criteria

- **AC1 — DataSourceV2 Class Generation (Expected Output):**
  - **Given** the generator receives connector name `"myformat"` and base package `"com.example.spark.sql.v2.myformat"`
  - **When** the read path boilerplate is generated
  - **Then** a `MyformatDataSourceV2.scala` file is created containing a class extending `FileDataSourceV2` with a `fallbackFileFormat` method returning the connector's `FileFormat` class, a `shortName()` method returning the connector name string, and two `getTable` overloads — one accepting `CaseInsensitiveStringMap` options only, and one accepting both `CaseInsensitiveStringMap` options and a `StructType` schema — matching the AvroDataSourceV2 pattern
  - `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroDataSourceV2.scala, lines 26–45`

- **AC2 — Table Class Generation (Expected Output):**
  - **Given** the generator runs with connector name `"myformat"`
  - **When** the Table class is generated
  - **Then** `MyformatTable.scala` contains a case class extending `FileTable` with: a `newScanBuilder(options: CaseInsensitiveStringMap): MyformatScanBuilder` method, an `inferSchema(files: Seq[FileStatus]): Option[StructType]` method returning `None` as a stub, a `supportsDataType(dataType: DataType): Boolean` method returning `true` as a stub, and a `formatName: String` property returning the connector name in uppercase — matching the AvroTable structure
  - `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroTable.scala, lines 31–55`

- **AC3 — ScanBuilder Class Generation (Expected Output):**
  - **Given** the generator runs with connector name `"myformat"`
  - **When** the ScanBuilder class is generated
  - **Then** `MyformatScanBuilder.scala` contains a case class extending `FileScanBuilder` with a `build(): MyformatScan` method that constructs and returns a `MyformatScan` instance with the session, file index, schemas, options, pushed filters, partition filters, and data filters, and a `pushDataFilters(dataFilters: Array[Filter]): Array[Filter]` method returning `Array.empty[Filter]` by default — matching the AvroScanBuilder pattern
  - `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroScanBuilder.scala, lines 27–55`

- **AC4 — Scan and PartitionReaderFactory Class Generation (Expected Output):**
  - **Given** the generator runs with connector name `"myformat"`
  - **When** the Scan and PartitionReaderFactory classes are generated
  - **Then** `MyformatScan.scala` contains a case class extending `FileScan` with `isSplitable(path: Path): Boolean` returning `true`, `createReaderFactory(): PartitionReaderFactory` returning a new `MyformatPartitionReaderFactory` instance, `equals(obj: Any): Boolean` with type-checked comparison, `hashCode(): Int` delegating to `super.hashCode()`, and `getMetaData(): Map[String, String]` returning metadata entries; and `MyformatPartitionReaderFactory.scala` contains a case class extending `FilePartitionReaderFactory` with a `buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow]` method containing a `throw new UnsupportedOperationException` placeholder with a comment directing the developer to implement format-specific deserialization
  - `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroScan.scala, lines 35–76`
  - `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroPartitionReaderFactory.scala, lines 48–121`

- **AC5 — Compilation Verification (Expected Output):**
  - **Given** all five read path files are generated into the project skeleton produced by STORY-003-01-01
  - **When** the project is compiled with `mvn compile`
  - **Then** compilation succeeds with zero errors against the `spark-sql_${scala.binary.version}` provided-scope dependency and all five classes are present in the `target/scala-${scala.binary.version}/classes` output directory

- **AC6 — Empty Connector Name Rejection (Input Validation):**
  - **Given** the connector name parameter is an empty string or `null`
  - **When** the generator attempts to produce read path files
  - **Then** the generator returns an error message stating `"Connector name is required for read path generation"` and does not create any files

- **AC7 — File Conflict Detection (Error Handling):**
  - **Given** the base package path resolves to a directory that already contains a file with a name matching one of the five generated file names
  - **When** the generator runs
  - **Then** it returns a warning listing each conflicting file name and its absolute path, and does not overwrite any existing file without the developer providing explicit `--overwrite` confirmation

- **AC8 — Hyphenated Connector Name Conversion (Edge Case):**
  - **Given** the connector name contains a hyphen (e.g., `"my-format"`)
  - **When** the read path classes are generated
  - **Then** all class names use PascalCase conversion — `MyFormatDataSourceV2`, `MyFormatTable`, `MyFormatScanBuilder`, `MyFormatScan`, `MyFormatPartitionReaderFactory` — and file names match the class names with the `.scala` extension (e.g., `MyFormatDataSourceV2.scala`)

## Sub-Tasks

- Create a DataSourceV2 class template extending `FileDataSourceV2` with `fallbackFileFormat` returning the connector's `FileFormat` class, `shortName()` returning the connector name string, and two `getTable` overloads — one with `CaseInsensitiveStringMap` options only calling `getPaths`, `getTableName`, `getOptionsWithoutPaths`, and constructing the Table instance, and one with both `CaseInsensitiveStringMap` options and `StructType` schema wrapping the schema in `Some(schema)`
- Create a Table case class template extending `FileTable` with constructor parameters (`name`, `sparkSession`, `options`, `paths`, `userSpecifiedSchema`, `fallbackFileFormat`), a `newScanBuilder` method returning the connector's ScanBuilder, an `inferSchema` stub returning `None`, a `newWriteBuilder` placeholder method, a `supportsDataType` stub returning `true`, and a `formatName` property returning the connector name in uppercase
- Create a ScanBuilder case class template extending `FileScanBuilder` with constructor parameters (`sparkSession`, `fileIndex`, `schema`, `dataSchema`, `options`), a `build()` method constructing and returning the connector's Scan instance with all required parameters, and a `pushDataFilters` method returning `Array.empty[Filter]` by default
- Create a Scan case class template extending `FileScan` with constructor parameters (`sparkSession`, `fileIndex`, `dataSchema`, `readDataSchema`, `readPartitionSchema`, `options`, `pushedFilters`, `partitionFilters`, `dataFilters`), `isSplitable` returning `true`, `createReaderFactory` returning a new PartitionReaderFactory instance, `equals` with type-checked pattern matching, `hashCode` delegating to `super.hashCode()`, and `getMetaData` returning `super.getMetaData()` merged with pushed filter metadata
- Create a PartitionReaderFactory case class template extending `FilePartitionReaderFactory` with constructor parameters (`sqlConf`, `broadcastedConf`, `dataSchema`, `readDataSchema`, `partitionSchema`) and a `buildReader` method that throws `UnsupportedOperationException` with a comment directing the developer to implement format-specific record deserialization logic
- Add import statements to each generated file referencing the required Spark packages: `org.apache.spark.sql.execution.datasources.v2.*`, `org.apache.spark.sql.execution.datasources.*`, `org.apache.spark.sql.connector.catalog.Table`, `org.apache.spark.sql.connector.read.PartitionReaderFactory`, `org.apache.spark.sql.types.*`, `org.apache.spark.sql.util.CaseInsensitiveStringMap`, `org.apache.spark.sql.sources.Filter`, `org.apache.hadoop.fs.Path`, `org.apache.hadoop.fs.FileStatus`
- Implement connector-name-to-class-name conversion logic: split on hyphens, capitalize the first letter of each segment, and concatenate (e.g., `"my-format"` → `"MyFormat"`, `"x"` → `"X"`, `"myformat"` → `"Myformat"`)
- Write unit tests verifying: (a) all five files are generated with the expected file names, (b) each class extends the correct Spark base class, (c) all required method signatures are present, (d) generated code compiles against `spark-sql` provided-scope dependency, (e) PascalCase conversion handles single characters, hyphens, and multi-word names

## Edge Cases

- **Empty/Null Input:** Connector name is `null` or an empty string — the generator must reject the input with an error message `"Connector name is required for read path generation"` rather than producing files with empty or malformed class names
- **Single-Character Boundary:** Connector name is a single character such as `"x"` — the generator must produce valid class names `XDataSourceV2`, `XTable`, `XScanBuilder`, `XScan`, and `XPartitionReaderFactory` with corresponding file names `XDataSourceV2.scala`, `XTable.scala`, `XScanBuilder.scala`, `XScan.scala`, and `XPartitionReaderFactory.scala`
- **Malformed Base Package:** Base package name contains consecutive dots (e.g., `"com..example"`), starts with a digit (e.g., `"1com.example"`), or contains whitespace (e.g., `"com. example"`) — the generator must validate the package name and reject the input with a specific error message identifying the exact malformation detected
- **Reserved Word Collision:** Connector name matches a Scala or Java reserved word (e.g., `"class"`, `"object"`, `"type"`, `"import"`) — the generator must detect the collision and reject the input with an error message stating `"Connector name '<name>' conflicts with a reserved keyword and cannot be used as a class name prefix"`
- **Pre-Existing File Conflict:** Target source directory already contains one or more files from a prior generation run (e.g., an existing `MyformatDataSourceV2.scala`) — the generator must detect each conflicting file, list the conflicting file paths in the warning output, and skip generation for those files unless the `--overwrite` flag is explicitly provided

## Dependencies

- **STORY-003-01-01** (Generate Connector Project Skeleton) — provides the Maven project directory structure, `pom.xml` with `spark-sql` provided-scope dependency, and `src/main/scala/<package-path>/` source directory into which the five read path files are generated
  - `Link:` [STORY-003-01-01-generate-connector-project-skeleton.md](./STORY-003-01-01-generate-connector-project-skeleton.md)
- **FEATURE-003-01** (Connector Scaffolding Generator) — parent feature defining the overall scaffolding scope including read path, write path, service registration, and documentation template generation
  - `Link:` [FEATURE-003-01-connector-scaffolding-generator.md](../FEATURE-003-01-connector-scaffolding-generator.md)
- **EPIC-003** (Unified Connector Development SDK) — parent epic defining the SDK scope and the DataSource V2 contracts that generated connectors must implement
  - `Link:` [EPIC-003-unified-connector-development-sdk.md](../../EPIC-003-unified-connector-development-sdk.md)
- **F-008 (Data Source Connectors)** — `FileDataSourceV2`, `FileTable`, `FileScanBuilder`, `FileScan`, and `FilePartitionReaderFactory` base classes in `sql/core/` that define the inheritance contracts for all five generated read path classes
- **Source Reference:** `connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroDataSourceV2.scala` — DataSourceV2 class extending `FileDataSourceV2` (line 26), `fallbackFileFormat` returning `classOf[AvroFileFormat]` (line 28), `shortName()` returning `"avro"` (line 30), `getTable` overload with options (lines 32–37), `getTable` overload with options and schema (lines 39–44)
- **Source Reference:** `connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroTable.scala` — Table case class with constructor parameters (lines 31–37), extending `FileTable` (line 38), `newScanBuilder` returning `AvroScanBuilder` (lines 39–40), `inferSchema` calling `AvroUtils.inferSchema` (lines 42–43), `supportsDataType` (line 52), `formatName` returning `"AVRO"` (line 54)
- **Source Reference:** `connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroScanBuilder.scala` — ScanBuilder case class with constructor parameters (lines 27–32), extending `FileScanBuilder` (line 33), `build()` constructing `AvroScan` (lines 35–46), `pushDataFilters` with filter pushdown logic (lines 48–54)
- **Source Reference:** `connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroScan.scala` — Scan case class with constructor parameters (lines 35–44), extending `FileScan`, `isSplitable` returning `true` (line 45), `createReaderFactory` constructing `AvroPartitionReaderFactory` (lines 47–63), `equals` (lines 65–69), `hashCode` (line 71), `getMetaData` (lines 73–75)
- **Source Reference:** `connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroPartitionReaderFactory.scala` — PartitionReaderFactory case class with constructor parameters (lines 48–55), extending `FilePartitionReaderFactory` (line 55), `buildReader` implementing Avro-specific deserialization (lines 58–121)

## Story Estimation Guidance

- **Story Points:** 5 (Fibonacci scale)
- **Rationale:** This is the largest story in FEATURE-003-01 — it requires generating five interrelated Scala classes with specific inheritance hierarchies (`FileDataSourceV2`, `FileTable`, `FileScanBuilder`, `FileScan`, `FilePartitionReaderFactory`), each with precise method signatures and import chains matching the Spark DataSource V2 SPI contract. The five classes must compose into a compilable read pipeline where the DataSourceV2 creates Tables, Tables create ScanBuilders, ScanBuilders create Scans, and Scans create PartitionReaderFactories. Complexity stems from the breadth of the DataSource V2 read surface, the need for cross-class type consistency (e.g., `MyformatScanBuilder.build()` must return `MyformatScan`), and the PascalCase conversion logic with edge case handling for single characters and hyphens.
- **Sprint Fit:** Completable within a single sprint but represents the largest single work item in FEATURE-003-01. Requires STORY-003-01-01 (project skeleton) to be completed first so that the generated files have a target project to be placed into.

## Definition of Done

- Five Scala source files are generated into the connector project's source directory: `<Name>DataSourceV2.scala`, `<Name>Table.scala`, `<Name>ScanBuilder.scala`, `<Name>Scan.scala`, and `<Name>PartitionReaderFactory.scala`
- `<Name>DataSourceV2` class extends `FileDataSourceV2` and contains `fallbackFileFormat`, `shortName()`, and two `getTable` overloads (options-only and options-with-schema)
- `<Name>Table` case class extends `FileTable` and contains `newScanBuilder`, `inferSchema` (stub returning `None`), `supportsDataType` (stub returning `true`), and `formatName` property
- `<Name>ScanBuilder` case class extends `FileScanBuilder` and contains `build()` returning the connector's Scan instance and `pushDataFilters` returning `Array.empty[Filter]`
- `<Name>Scan` case class extends `FileScan` and contains `isSplitable` returning `true`, `createReaderFactory` returning the connector's PartitionReaderFactory instance, `equals`, `hashCode`, and `getMetaData`
- `<Name>PartitionReaderFactory` case class extends `FilePartitionReaderFactory` and contains `buildReader` with an `UnsupportedOperationException` placeholder directing the developer to implement format-specific logic
- Generated project compiles with `mvn compile` against `spark-sql_${scala.binary.version}` provided-scope dependency with zero errors
- Connector name to PascalCase conversion handles single characters (e.g., `"x"` → `X`), hyphens (e.g., `"my-format"` → `MyFormat`), and plain lowercase names (e.g., `"myformat"` → `Myformat`)
- Input validation rejects empty, null, and malformed connector names and package names with specific error messages
- File conflict detection warns about existing files and prevents silent overwriting
- Unit tests cover all 8 acceptance criteria and all 5 edge cases
- Story is demo-able: running the generator with connector name `"myformat"` and base package `"com.example.spark.sql.v2.myformat"` produces 5 inspectable Scala source files that compile and whose class hierarchy matches the Avro connector reference implementation
