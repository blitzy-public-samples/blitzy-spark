# Create Unified Connector Development SDK to Accelerate Custom Data Source Integration

## Epic Summary

Custom connector development for Apache Spark demands deep knowledge of Spark internals and the DataSource V2 API, causing organizations to spend weeks of engineering effort integrating each proprietary data source; a standardized SDK reduces that development time to days by providing project scaffolding, code generation, and pre-built testing harnesses. This epic extends the existing DataSource V2 API — used by built-in connectors such as Avro, Protobuf, Kafka, and Kinesis — with a unified toolkit that generates compilable connector project skeletons, read and write path boilerplate, service registration code, and documentation templates. The scope covers two features: a connector scaffolding generator and a connector testing framework; it does NOT implement specific connectors but instead provides the tooling and patterns that connector developers use to build, test, and publish their own integrations.

## Features Index

| Feature ID | Feature Name | Description | Link |
|---|---|---|---|
| FEATURE-003-01 | Connector Scaffolding Generator | Generates complete, compilable connector project skeletons with DataSource V2 registration, read path boilerplate, write path boilerplate, and documentation templates following the structure of existing connectors | [FEATURE-003-01-connector-scaffolding-generator.md](./EPIC-003/FEATURE-003-01-connector-scaffolding-generator.md) |
| FEATURE-003-02 | Connector Testing Framework | Provides reusable base test classes and test harnesses for validating read operations, write operations, predicate pushdown compliance, schema inference accuracy, and performance benchmarks for custom connectors | [FEATURE-003-02-connector-testing-framework.md](./EPIC-003/FEATURE-003-02-connector-testing-framework.md) |

## Execution Strategy

### Parallel Epic Execution

This epic has **no cross-epic dependencies** and can be executed simultaneously with EPIC-001, EPIC-002, EPIC-004, and EPIC-005. Each epic operates on independent Spark subsystems with distinct module boundaries, enabling all five epics to run in parallel with separate development teams.

### Implementation Phases — Backend Before Frontend

All stories within this epic are organized into two sequential phases. **Phase 1 (Backend)** must be completed and fully tested before **Phase 2 (Frontend)** begins, ensuring that all code generation logic, test harnesses, validation engines, and benchmark infrastructure are stable before documentation template and report output work starts.

#### Phase 1 — Backend (Code Generation, Test Harnesses, and Validation Engine)

Backend stories establish the scaffolding generator, DataSource V2 integration, read/write test harnesses, and benchmark execution infrastructure. These must be implemented and pass all unit and integration tests before Phase 2 begins.

| Story ID | Story Name | Feature | Rationale |
|----------|-----------|---------|-----------|
| STORY-003-01-01 | Generate Connector Project Skeleton | FEATURE-003-01 | Core project generation engine — foundation for all scaffolding |
| STORY-003-01-02 | Create Read Path Boilerplate | FEATURE-003-01 | DataSource V2 read code generation — backend scaffolding |
| STORY-003-01-03 | Create Write Path Boilerplate | FEATURE-003-01 | DataSource V2 write code generation — backend scaffolding |
| STORY-003-01-04 | Generate DataSource V2 Registration | FEATURE-003-01 | Service registration code generation — backend integration layer |
| STORY-003-02-01 | Provide Test Harness for Read Operations | FEATURE-003-02 | Read test base class — backend testing framework |
| STORY-003-02-02 | Provide Test Harness for Write Operations | FEATURE-003-02 | Write test base class — backend testing framework |
| STORY-003-02-03 | Validate Predicate Pushdown Compliance | FEATURE-003-02 | Pushdown verification engine — backend compliance checks |
| STORY-003-02-04 | Verify Schema Inference Accuracy | FEATURE-003-02 | Schema inference test suite — backend validation |
| STORY-003-02-05 | Run Performance Benchmarks | FEATURE-003-02 | Benchmark execution framework — backend performance measurement |

#### Phase 2 — Frontend (Documentation Templates and Output Formatting)

Frontend stories produce user-facing documentation and report output. These must not begin until all Phase 1 stories pass acceptance testing.

| Story ID | Story Name | Feature | Rationale |
|----------|-----------|---------|-----------|
| STORY-003-01-05 | Produce Connector Documentation Template | FEATURE-003-01 | Auto-generated README and usage guide — depends on scaffolding structure from Phase 1 |

### Phase Gate Criteria

- **Phase 1 → Phase 2 Gate**: All 9 backend stories must have passing unit tests, passing integration tests, and code review approval before any Phase 2 story begins development
- **Sprint Planning**: Phase 1 stories should be prioritized in Sprints 1–3; Phase 2 story should be planned for Sprint 4 after Phase 1 gate is passed

## Dependencies

- **F-008 (Data Source Connectors)** — DataSource V2 API interfaces including `Table`, `ScanBuilder`, `WriteBuilder`, and `DataSourceRegister` that define the contract all connectors must implement
  - `Source: sql/core/` — DataSource V2 registration and read/write path APIs
- **Existing connector implementations (reference patterns):**
  - `Source: connector/avro/` — Reference implementation for file-format-based connectors with schema conversion, partition reader factories, and SQL function integration (`to_avro`, `from_avro`)
  - `Source: connector/protobuf/` — Reference implementation for schema-driven connectors with Protobuf serializer/deserializer, Catalyst type mapping, and protoc build integration
  - `Source: connector/kafka-0-10/` — Reference implementation for streaming connectors with consumer strategy, partition-level read, offset management, and backpressure support
  - `Source: connector/kinesis-asl/` — Reference implementation for cloud streaming connectors with Kinesis client library integration and initial position strategies
- **SQL module** — `sql/core/` for the DataSource V2 read and write APIs, schema inference interfaces, and predicate pushdown contracts
- **Build tooling** — Maven project structure (`pom.xml` parent POM with `spark-parent_2.13:4.1.0-SNAPSHOT` coordinates) and SBT cross-build conventions used by all existing connector modules
- **Data source documentation** — Existing data source guides serve as the documentation pattern for generated connector README files
  - `Source: docs/sql-data-sources.md` — Data source overview and navigation structure
  - `Source: docs/sql-data-sources-avro.md` — Reference documentation structure for a format-based connector (deploying, load/save functions, configuration, type conversion matrices)

## Definition of Done

- All features (FEATURE-003-01 and FEATURE-003-02) are complete and integrated into a single SDK package
- The scaffolding generator produces compilable, runnable connector projects that include both read and write paths, DataSource V2 service registration, and Maven/SBT build configuration
- Generated connector projects follow the same directory structure, naming conventions, and build patterns as existing connectors (`connector/avro/`, `connector/protobuf/`)
- The testing framework provides base test classes for read operation validation, write operation validation, predicate pushdown compliance checks, and schema inference accuracy verification
- Performance benchmark harnesses are included in the testing framework, enabling connector developers to measure throughput and latency against defined baselines
- At least one sample connector is generated using the scaffolding generator and validated end-to-end using the testing framework to confirm the SDK produces functional output
- SDK documentation includes a usage guide covering scaffolding commands and options, an API reference for test base classes and harness utilities, and a step-by-step tutorial for creating a custom connector from scratch
- All generated code compiles against `spark-parent_2.13:4.1.0-SNAPSHOT` without modification and passes the testing framework's full validation suite
- Integration tests confirm that generated connectors can be loaded by `SparkSession` via the standard `spark.read.format("custom-format")` and `df.write.format("custom-format")` paths
