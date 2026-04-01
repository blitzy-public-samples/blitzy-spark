# Implement Connector Scaffolding Generator to Automate Custom DataSource V2 Project Creation

## Feature Summary

Platform engineers and connector developers spend weeks understanding Spark DataSource V2 internals — `Table`, `ScanBuilder`, `WriteBuilder`, `DataSourceRegister`, and `PartitionReaderFactory` — before writing a single line of connector-specific code; the scaffolding generator eliminates this ramp-up by producing a compilable Maven or SBT connector project with read and write path boilerplate, DataSource V2 service registration, and documentation templates in a single command. The generator codifies the proven patterns from existing connectors — `connector/avro/` (`AvroDataSourceV2` extending `FileDataSourceV2`, `AvroTable` extending `FileTable`, `AvroScanBuilder` extending `FileScanBuilder`, `AvroWrite` extending `FileWrite`) and `connector/protobuf/` (shade plugin configuration, schema-based serialization) — into parameterized templates that produce a project structure matching the Maven POM layout of `connector/avro/pom.xml` (parent `spark-parent_2.13:4.1.0-SNAPSHOT`, `spark-sql` provided scope, Scala cross-build output directories). This feature covers five capabilities: project skeleton generation, read path boilerplate creation, write path boilerplate creation, DataSource V2 service registration generation, and connector documentation template production.

## User Stories Index

| Story ID | Story Name | Phase | Description | Link |
|---|---|---|---|---|
| STORY-003-01-01 | Generate Connector Project Skeleton | Backend | Generates a complete Maven/SBT project directory structure with POM configuration, Scala source layout, test layout, and resource directories matching existing connector modules | [STORY-003-01-01-generate-connector-project-skeleton.md](./FEATURE-003-01/STORY-003-01-01-generate-connector-project-skeleton.md) |
| STORY-003-01-02 | Create Read Path Boilerplate | Backend | Produces DataSource V2 read-side classes extending `FileDataSourceV2`, `FileTable`, and `FileScanBuilder` with stub schema inference and partition reader logic | [STORY-003-01-02-create-read-path-boilerplate.md](./FEATURE-003-01/STORY-003-01-02-create-read-path-boilerplate.md) |
| STORY-003-01-03 | Create Write Path Boilerplate | Backend | Produces DataSource V2 write-side classes extending `FileWrite` with `OutputWriterFactory` implementation and configurable save mode support | [STORY-003-01-03-create-write-path-boilerplate.md](./FEATURE-003-01/STORY-003-01-03-create-write-path-boilerplate.md) |
| STORY-003-01-04 | Generate DataSource V2 Registration | Backend | Creates the `META-INF/services` registration file and `DataSourceRegister` implementation class with a configurable connector short name | [STORY-003-01-04-generate-datasource-v2-registration.md](./FEATURE-003-01/STORY-003-01-04-generate-datasource-v2-registration.md) |
| STORY-003-01-05 | Produce Connector Documentation Template | Frontend | Generates a structured README and data source documentation template following the style of existing Spark data source guides with deployment, configuration, and type conversion sections | [STORY-003-01-05-produce-connector-documentation-template.md](./FEATURE-003-01/STORY-003-01-05-produce-connector-documentation-template.md) |

## Dependencies

- **EPIC-003 (Unified Connector Development SDK)** — Parent epic that defines the overall SDK scope, including this scaffolding generator and the companion connector testing framework (FEATURE-003-02)
- **F-008 (Data Source Connectors)** — Existing DataSource V2 API interfaces in `sql/core/` that define the contracts all generated connectors must implement: `Table`, `ScanBuilder`, `WriteBuilder`, `DataSourceRegister`, `PartitionReaderFactory`, `OutputWriterFactory`
  - `Source: sql/core/` — DataSource V2 registration and read/write path APIs
- **Existing connector reference implementations:**
  - `Source: connector/avro/pom.xml` — Maven POM structure for connector modules (parent `spark-parent_2.13:4.1.0-SNAPSHOT`, `spark-sql` provided scope, Scala cross-build output directories)
  - `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroDataSourceV2.scala` — DataSourceV2 registration pattern (extends `FileDataSourceV2`, `shortName()` returns `"avro"`, `getTable()` factory methods)
  - `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroTable.scala` — Table implementation pattern (extends `FileTable`, implements `newScanBuilder()`, `newWriteBuilder()`, `inferSchema()`, `supportsDataType()`)
  - `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroScanBuilder.scala` — Scan builder pattern (extends `FileScanBuilder`, implements `pushDataFilters()` for filter pushdown)
  - `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroWrite.scala` — Write path pattern (extends `FileWrite`, implements `prepareWrite()` returning `OutputWriterFactory`)
  - `Source: connector/protobuf/pom.xml` — Shade plugin configuration for dependency relocation in connectors with external library dependencies
  - `Source: connector/kafka-0-10/` — Reference streaming connector pattern with consumer strategy, partition-level read, and offset management
- **Maven/SBT build tooling** — For generated project build files following the POM structure from `connector/avro/pom.xml`
- **DataSource V2 API** — Core interfaces that generated connectors extend: `Table`, `ScanBuilder`, `WriteBuilder`, `DataSourceRegister`, `PartitionReaderFactory`, `OutputWriterFactory`

## Definition of Done

- Scaffolding generator produces a compilable Maven project with a `pom.xml` referencing `spark-parent_2.13` as parent and `spark-sql_2.13` as a provided-scope dependency
- Generated project directory structure matches the layout of existing connectors: `pom.xml`, `src/main/scala/`, `src/test/scala/`, `src/main/resources/META-INF/services/`, and `README.md`
- Generated read path boilerplate includes classes extending `FileDataSourceV2`, `FileTable`, and `FileScanBuilder` with implementations for `shortName()`, `getTable()`, `newScanBuilder()`, `inferSchema()`, and `pushDataFilters()`
- Generated write path boilerplate includes a class extending `FileWrite` with a `prepareWrite()` implementation returning an `OutputWriterFactory`
- DataSource V2 service registration file at `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` is generated containing the fully qualified class name of the connector's `DataSourceRegister` implementation, using the developer-specified short name
- Generated documentation template follows the section structure of `docs/sql-data-sources.md` including: deployment instructions, load and save function examples, configuration options, and supported type conversion matrices
  - `Source: docs/sql-data-sources.md` — Documentation style reference for connector guides
- At least one end-to-end test validates that the generated project compiles against `spark-parent_2.13:4.1.0-SNAPSHOT` and passes basic read and write integration tests using `spark.read.format("short-name")` and `df.write.format("short-name")` paths
- All generated Scala source files compile without warnings under the project's `scalac` settings
- Generated project includes a test class extending `SparkFunSuite` for the connector developer to expand with format-specific test cases
