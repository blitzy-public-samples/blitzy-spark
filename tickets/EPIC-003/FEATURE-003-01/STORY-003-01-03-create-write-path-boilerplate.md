# Generate Write Path Boilerplate Code to Accelerate Custom Connector Data Output Implementation

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

**As a** platform engineer, **I want to** run the scaffolding generator and receive generated write path source files containing: (1) a Write case class extending `FileWrite` with a `prepareWrite` method stub that accepts `SQLConf`, `Job`, `options Map`, and `dataSchema StructType` and returns an `OutputWriterFactory`, (2) a `newWriteBuilder` method added to the Table class that creates a `WriteBuilder` returning the Write instance, and (3) an `OutputWriterFactory` stub class with a `newInstance` method placeholder, **so that** the connector has a compilable write path foundation that follows the established Spark DataSource V2 write contract, reducing write path implementation time from days to hours by eliminating the need to manually discover and implement the FileWrite → OutputWriterFactory → OutputWriter class hierarchy.

## Acceptance Criteria

- **AC1 — Write Case Class Generation (Expected Output):**
  - **Given** the generator receives connector name `"myformat"` and base package `"com.example.spark.sql.v2.myformat"`
  - **When** the write path boilerplate is generated
  - **Then** a `MyformatWrite.scala` file is created as a case class extending `FileWrite` with constructor parameters `paths: Seq[String]`, `formatName: String`, `supportsDataType: DataType => Boolean`, and `info: LogicalWriteInfo` — matching the AvroWrite pattern
  - `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroWrite.scala, lines 28–32`

- **AC2 — prepareWrite Method Stub (Expected Output):**
  - **Given** the Write class is generated
  - **When** inspected
  - **Then** it contains a `prepareWrite` method override with signature `(sqlConf: SQLConf, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory` that returns a stub `OutputWriterFactory` with a TODO comment indicating where format-specific write logic should be implemented — matching the AvroWrite.prepareWrite pattern
  - `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroWrite.scala, lines 33–39`

- **AC3 — Table Class newWriteBuilder Integration (Expected Output):**
  - **Given** the Table class exists from STORY-003-01-02
  - **When** the write path boilerplate is generated
  - **Then** the Table class is updated with a `newWriteBuilder(info: LogicalWriteInfo): WriteBuilder` method that returns a `WriteBuilder` whose `build()` method instantiates the generated Write class — matching the AvroTable.newWriteBuilder pattern
  - `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroTable.scala, lines 45–49`

- **AC4 — Compilation Verification (Expected Output):**
  - **Given** the write path is generated into the project skeleton from STORY-003-01-01
  - **When** the entire generated project is compiled with `mvn compile`
  - **Then** compilation succeeds with zero errors against the `spark-sql_${scala.binary.version}` provided-scope dependency

- **AC5 — Empty Connector Name Rejection (Input Validation):**
  - **Given** the connector name parameter is null or empty
  - **When** the generator attempts to produce write path files
  - **Then** the generator returns an error message stating `"Connector name is required for write path generation"` and does not create any files

- **AC6 — Missing Source Directory Detection (Error Handling):**
  - **Given** the generated project directory does not contain the expected package directory structure (`src/main/scala/<package-path>`)
  - **When** the write path generator runs
  - **Then** it returns an error message stating `"Expected source directory not found: src/main/scala/<package-path>"` and does not create any partial output

- **AC7 — Read-Only Connector Skip Logic (Edge Case):**
  - **Given** the generator is configured to produce a read-only connector (write support disabled)
  - **When** the generator runs
  - **Then** no Write class, no OutputWriterFactory stub, and no newWriteBuilder method are generated, and the Table class omits the `WriteBuilder` import and method

## Sub-Tasks

- Create Write case class template extending `FileWrite` with constructor parameters (`paths: Seq[String]`, `formatName: String`, `supportsDataType: DataType => Boolean`, `info: LogicalWriteInfo`) and a `prepareWrite` method stub returning a placeholder `OutputWriterFactory`
- Create `OutputWriterFactory` stub class with a `newInstance` placeholder method that throws `UnsupportedOperationException` with a comment directing the developer to implement format-specific serialization logic
- Add `newWriteBuilder` method to the Table class template (from STORY-003-01-02) that creates an anonymous `WriteBuilder` whose `build()` method returns the generated Write instance — following the pattern at `AvroTable.scala` lines 45–49
- Add required imports to the Write class template: `org.apache.hadoop.mapreduce.Job`, `org.apache.spark.sql.connector.write.LogicalWriteInfo`, `org.apache.spark.sql.execution.datasources.OutputWriterFactory`, `org.apache.spark.sql.execution.datasources.v2.FileWrite`, `org.apache.spark.sql.internal.SQLConf`, `org.apache.spark.sql.types._`
- Add WriteBuilder-related imports to the Table class template: `org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}`
- Implement conditional generation logic: skip all write path files and omit WriteBuilder-related code from the Table class when the write support configuration flag is set to disabled
- Write unit tests verifying Write class structure, `prepareWrite` method signature, `newWriteBuilder` integration in the Table class, `OutputWriterFactory` stub presence, and conditional generation behavior for read-only connectors

## Edge Cases

- **Empty/Null Input:** Connector name is null or an empty string — the generator must reject the input with a descriptive error message stating `"Connector name is required for write path generation"` rather than producing files with empty or malformed class names
- **Boundary — Write-Only Connector:** Read support is disabled but write support is enabled — the generator produces the Write class and OutputWriterFactory stub, and the Table class contains `newWriteBuilder` without `newScanBuilder`; the generated project must still compile with zero errors
- **Invalid Base Package:** Base package name contains characters that are not valid in Java package identifiers (e.g., spaces, hyphens in package segments like `"com.my-company"`, or leading digits like `"1com.example"`) — the generator must validate the package name and reject with an error specifying the valid Java package naming rules: lowercase letters, digits, and dots as separators with no leading digits in any segment
- **Boundary — Read-Only Connector:** Write support is explicitly disabled in the generator configuration — the generator must skip all write path file creation (Write class, OutputWriterFactory stub) and omit the `newWriteBuilder` method and `WriteBuilder` imports from the Table class entirely
- **Pre-Existing Write Class File:** Output directory already contains a Write class file from a previous generation run — the generator must detect the existing file and warn that it will be overwritten, requiring the `--overwrite` flag to proceed; without the flag, the generator must exit without modifying existing files

## Dependencies

- **STORY-003-01-01** (Generate Connector Project Skeleton) — provides the Maven project directory structure, `pom.xml` with `spark-sql` provided-scope dependency, and `src/main/scala/<package-path>/` source directory into which the write path files are generated
  - `Link:` [STORY-003-01-01-generate-connector-project-skeleton.md](./STORY-003-01-01-generate-connector-project-skeleton.md)
- **STORY-003-01-02** (Create Read Path Boilerplate) — the Table class is generated in the read path story and extended with the `newWriteBuilder` method in this story
  - `Link:` [STORY-003-01-02-create-read-path-boilerplate.md](./STORY-003-01-02-create-read-path-boilerplate.md)
- **FEATURE-003-01** (Connector Scaffolding Generator) — parent feature defining the overall scaffolding scope including read path, write path, service registration, and documentation template generation
  - `Link:` [FEATURE-003-01-connector-scaffolding-generator.md](../FEATURE-003-01-connector-scaffolding-generator.md)
- **EPIC-003** (Unified Connector Development SDK) — parent epic defining the SDK scope and the DataSource V2 contracts that generated connectors must implement
  - `Link:` [EPIC-003-unified-connector-development-sdk.md](../../EPIC-003-unified-connector-development-sdk.md)
- **F-008 (Data Source Connectors)** — `FileWrite` and `OutputWriterFactory` base classes in `sql/core/` that define the write path inheritance contracts for generated connector code
- **Source Reference:** `connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroWrite.scala` — Write case class pattern (lines 28–40): extends `FileWrite`, constructor parameters (`paths`, `formatName`, `supportsDataType`, `info`), `prepareWrite` returns `OutputWriterFactory` via `AvroUtils.prepareWrite`
- **Source Reference:** `connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroTable.scala` — `newWriteBuilder` method pattern (lines 45–49): creates anonymous `WriteBuilder` whose `build()` method returns `AvroWrite` instance with `paths`, `formatName`, `supportsDataType`, and `mergedWriteInfo(info)` arguments

## Story Estimation Guidance

- **Story Points:** 3 (Fibonacci scale)
- **Rationale:** Moderate complexity — generating the Write class and OutputWriterFactory stub is straightforward following the AvroWrite pattern, but integrating the `newWriteBuilder` method into the existing Table class (produced by STORY-003-01-02) requires coordination between two template outputs. The conditional generation logic (read-only vs read-write connectors) adds design complexity for configuration branching and Table class template variants. The write path surface is smaller than the read path (STORY-003-01-02 at 5 points covers five classes) since this story produces two classes (Write, OutputWriterFactory) plus one method addition to an existing class.
- **Sprint Fit:** Completable within a single sprint. Depends on STORY-003-01-01 (project skeleton) and STORY-003-01-02 (read path with Table class) being completed first.

## Definition of Done

- Write case class is generated extending `FileWrite` with constructor parameters (`paths: Seq[String]`, `formatName: String`, `supportsDataType: DataType => Boolean`, `info: LogicalWriteInfo`) matching the AvroWrite pattern
- `prepareWrite` method stub is present with the exact parameter types `(sqlConf: SQLConf, job: Job, options: Map[String, String], dataSchema: StructType)` and return type `OutputWriterFactory`
- `OutputWriterFactory` stub class is generated with a placeholder `newInstance` method that throws `UnsupportedOperationException` directing the developer to implement format-specific logic
- Table class includes a `newWriteBuilder(info: LogicalWriteInfo): WriteBuilder` method that returns an anonymous `WriteBuilder` whose `build()` method instantiates the generated Write class
- Write class template includes all required imports: `org.apache.hadoop.mapreduce.Job`, `org.apache.spark.sql.connector.write.LogicalWriteInfo`, `org.apache.spark.sql.execution.datasources.OutputWriterFactory`, `org.apache.spark.sql.execution.datasources.v2.FileWrite`, `org.apache.spark.sql.internal.SQLConf`, `org.apache.spark.sql.types._`
- Table class template includes WriteBuilder-related imports: `org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}`
- Generated project compiles with `mvn compile` against `spark-sql_${scala.binary.version}` provided-scope dependency with zero errors
- Read-only connector configuration skips all write path generation: no Write class, no OutputWriterFactory stub, no `newWriteBuilder` method, and no WriteBuilder imports in the Table class
- Input validation rejects empty, null, and invalid connector names and package names with specific error messages
- Unit tests cover all 7 acceptance criteria and all 5 edge cases
- Story is demo-able: running the generator with connector name `"myformat"` and base package `"com.example.spark.sql.v2.myformat"` produces inspectable Write class and OutputWriterFactory stub that compile alongside the read path classes from STORY-003-01-02
