# Generate DataSource V2 Service Registration to Enable Short-Name Connector Discovery in Spark SQL

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

**As a** platform engineer, **I want to** run the scaffolding generator and receive a `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` file populated with the fully qualified class name of the generated DataSourceV2 class, along with a `shortName()` method override in the DataSourceV2 class returning the connector's registered short name, **so that** the connector is discoverable by Spark SQL's service loader mechanism, allowing end-users to reference the connector using `spark.read.format("<shortName>")` instead of the fully qualified class name, matching the registration pattern used by built-in connectors like Avro where `shortName()` returns `"avro"`.

> `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroDataSourceV2.scala` — `shortName()` returns `"avro"` (line 30), class extends `FileDataSourceV2` (line 26)
> `Source: connector/avro/src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` — contains `org.apache.spark.sql.v2.avro.AvroDataSourceV2` as the registered class (line 18)

## Acceptance Criteria

- **AC1 — Service Registration File Content (Expected Output):**
  - **Given** the generator receives connector short name `"myformat"` and base package `"com.example.spark.sql.v2.myformat"`
  - **When** the registration files are generated
  - **Then** the file `src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` is created containing exactly one line with the fully qualified class name: `com.example.spark.sql.v2.myformat.MyformatDataSourceV2`

- **AC2 — shortName() Method Generation (Expected Output):**
  - **Given** the generator receives connector short name `"myformat"`
  - **When** the DataSourceV2 class is generated
  - **Then** the class contains a `shortName()` method that returns the string `"myformat"` — matching the pattern in `AvroDataSourceV2.shortName()` which returns `"avro"`
  - `Source: connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroDataSourceV2.scala, line 30`

- **AC3 — Uppercase Character Rejection (Input Validation):**
  - **Given** the connector short name parameter contains uppercase characters (e.g., `"MyFormat"`)
  - **When** the generator validates the input
  - **Then** the generator returns an error message stating `"Connector short name must contain only lowercase alphanumeric characters and hyphens; received: MyFormat"`

- **AC4 — Empty or Null Short Name Rejection (Input Validation):**
  - **Given** the connector short name parameter is an empty string or null
  - **When** the generator validates the input
  - **Then** the generator returns an error message stating `"Connector short name is required and must be a non-empty string"`

- **AC5 — Service File Format Compliance (Expected Output):**
  - **Given** the generator creates the `META-INF/services` file
  - **When** the file is inspected
  - **Then** it contains the Apache License 2.0 header comment block (lines 1–16, using `#` comment syntax) followed by one fully qualified class name on a single line — matching the format of the Avro connector's service file
  - `Source: connector/avro/src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` — Apache License header (lines 1–16), fully qualified class name on line 18

- **AC6 — Filesystem Write Failure Handling (Error Handling):**
  - **Given** the `META-INF/services` directory path cannot be created due to filesystem permission restrictions
  - **When** the generator attempts to write the registration file
  - **Then** the generator returns an error message specifying the absolute path that could not be created and the underlying filesystem error description

- **AC7 — Hyphenated Short Name PascalCase Conversion (Edge Case):**
  - **Given** the connector short name contains a hyphen (e.g., `"my-format"`)
  - **When** the registration files are generated
  - **Then** the generated DataSourceV2 class name converts the short name to PascalCase (e.g., `"MyFormatDataSourceV2"`) while the `shortName()` method returns the original hyphenated string `"my-format"`, and the service registration file references the PascalCase class name `com.example.spark.sql.v2.myformat.MyFormatDataSourceV2`

## Sub-Tasks

- Create the `src/main/resources/META-INF/services/` directory structure within the generated project skeleton produced by STORY-003-01-01
- Generate the `org.apache.spark.sql.sources.DataSourceRegister` service file with the Apache License 2.0 header (using `#` comment syntax matching the Avro connector's service file format) and the fully qualified DataSourceV2 class name on a single line after the header
- Add the `shortName()` override method to the generated DataSourceV2 class returning the connector's short name string literal
- Implement input validation for the short name parameter: reject empty strings, null values, strings containing uppercase characters, and strings containing characters outside the allowed set of lowercase alphanumeric characters and hyphens
- Implement short-name-to-PascalCase class name conversion logic: split on hyphens, capitalize the first letter of each segment, and concatenate (e.g., `"my-format"` → `"MyFormat"`, `"x"` → `"X"`, `"myformat"` → `"Myformat"`)
- Write unit tests verifying: service file content matches the expected format, `shortName()` return value matches the input short name, input validation rejects all invalid inputs with the specified error messages, and PascalCase conversion handles single characters, hyphens, and multi-segment names

## Edge Cases

- **Empty/Null Input:** Connector short name is null or an empty string — the generator must reject the input with the error message `"Connector short name is required and must be a non-empty string"` rather than producing a service file with an empty or malformed fully qualified class name
- **Invalid Special Characters:** Short name contains characters outside the allowed set (e.g., `"my_format!"`, `"my.format"`, `"@connector"`) — the generator must reject the input with a character validation error specifying that only lowercase alphanumeric characters and hyphens are permitted
- **Single-Character Boundary:** Short name is a single character `"x"` — the generator must produce a valid service registration file referencing class name `XDataSourceV2` and the `shortName()` method must return `"x"`
- **Multi-Hyphen Boundary:** Short name contains multiple hyphens (e.g., `"my-custom-format"`) — the generator must convert to PascalCase class name `MyCustomFormatDataSourceV2`, keep the original hyphenated short name `"my-custom-format"` in the `shortName()` return value, and reference the PascalCase class name in the service registration file
- **Built-In Name Conflict:** Short name matches a built-in Spark data source name (`"avro"`, `"parquet"`, `"json"`, `"csv"`, `"orc"`, `"text"`, `"jdbc"`) — the generator must emit a warning message stating that the short name may conflict with a built-in Spark data source, allowing the developer to proceed or choose a different name

## Dependencies

- **STORY-003-01-01** (Generate Connector Project Skeleton) — provides the project directory structure including `src/main/resources/META-INF/services/` where the registration file is placed
  - `Link:` [STORY-003-01-01-generate-connector-project-skeleton.md](./STORY-003-01-01-generate-connector-project-skeleton.md)
- **STORY-003-01-02** (Create Read Path Boilerplate) — the DataSourceV2 class must be generated before service registration can reference its fully qualified class name
  - `Link:` [STORY-003-01-02-create-read-path-boilerplate.md](./STORY-003-01-02-create-read-path-boilerplate.md)
- **FEATURE-003-01** (Connector Scaffolding Generator) — parent feature defining the overall scaffolding scope including this registration generation capability
  - `Link:` [FEATURE-003-01-connector-scaffolding-generator.md](../FEATURE-003-01-connector-scaffolding-generator.md)
- **EPIC-003** (Unified Connector Development SDK) — parent epic defining the SDK scope and DataSource V2 contracts
  - `Link:` [EPIC-003-unified-connector-development-sdk.md](../../EPIC-003-unified-connector-development-sdk.md)
- **F-008 (Data Source Connectors)** — `DataSourceRegister` SPI interface in `sql/core/` that defines the service provider contract all connectors must implement for short-name discovery
- **Java SPI ServiceLoader mechanism** — standard `java.util.ServiceLoader` discovery pattern used by Spark to locate and instantiate `DataSourceRegister` implementations at runtime
- **Source Reference:** `connector/avro/src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` — contains `org.apache.spark.sql.v2.avro.AvroDataSourceV2` as the registered class (line 18), preceded by Apache License 2.0 header (lines 1–16)
- **Source Reference:** `connector/avro/src/main/scala/org/apache/spark/sql/v2/avro/AvroDataSourceV2.scala` — `shortName()` returns `"avro"` (line 30), class extends `FileDataSourceV2` (line 26)

## Story Estimation Guidance

- **Story Points:** 2 (Fibonacci scale)
- **Rationale:** Small, well-defined scope — generating two artifacts (the `META-INF/services` registration file and the `shortName()` method override) with well-understood patterns directly observed in the Avro connector reference implementation. Input validation for the short name parameter adds minor complexity. The `META-INF/services` pattern is a standard Java SPI mechanism with no ambiguity in its structure. The PascalCase conversion logic is shared with STORY-003-01-02 and can be reused.
- **Sprint Fit:** Completable within a single sprint. This story depends on STORY-003-01-01 (project skeleton) and STORY-003-01-02 (DataSourceV2 class generation) but is independently testable — the registration file content and `shortName()` return value can be verified in isolation.

## Definition of Done

- `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` file is generated at the path `src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` within the project skeleton
- Service file contains the Apache License 2.0 header using `#` comment syntax (lines 1–16) followed by the fully qualified DataSourceV2 class name on a single line
- DataSourceV2 class contains a `shortName()` method override returning the connector's registered short name as a string literal
- Input validation rejects empty, null, uppercase, and special-character short names with specific, descriptive error messages
- Hyphenated short names are converted to PascalCase for class naming (e.g., `"my-format"` → `MyFormatDataSourceV2`) while preserving the original hyphenated short name in the `shortName()` return value
- Built-in name conflicts (`"avro"`, `"parquet"`, `"json"`, `"csv"`, `"orc"`, `"text"`, `"jdbc"`) trigger a warning message
- Unit tests cover all 7 acceptance criteria and all 5 edge cases
- Story is demo-able: running the generator with short name `"myformat"` and base package `"com.example.spark.sql.v2.myformat"` produces a valid `META-INF/services` registration file and a DataSourceV2 class with `shortName()` returning `"myformat"`, enabling Spark's `ServiceLoader` to discover the connector via `spark.read.format("myformat")`
