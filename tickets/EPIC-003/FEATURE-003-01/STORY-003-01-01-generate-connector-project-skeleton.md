# Generate Connector Project Skeleton to Bootstrap Maven-Based DataSource V2 Connector Modules

## User Story

**As a** platform engineer, **I want to** run the scaffolding generator with a connector name and base package and receive a complete Maven project directory containing: (1) a `pom.xml` with parent reference to `org.apache.spark:spark-parent_2.13:4.1.0-SNAPSHOT` (`relativePath` set to `../../pom.xml`), a `spark-sql_${scala.binary.version}` dependency in provided scope, test-jar dependencies for `spark-core_${scala.binary.version}`, `spark-catalyst_${scala.binary.version}`, and `spark-sql_${scala.binary.version}` in test scope, output directories set to `target/scala-${scala.binary.version}/classes` and `target/scala-${scala.binary.version}/test-classes`, and an `sbt.project.name` property set to the connector name; (2) a standard directory layout including `src/main/scala/<package-path>/`, `src/test/scala/<package-path>/`, `src/main/resources/META-INF/services/`, and a `README.md` file, **so that** the connector project is immediately compilable with `mvn compile` and structurally identical to existing Spark connectors (Avro, Protobuf), eliminating 2–3 days of initial project setup and Maven POM configuration trial-and-error.

## Acceptance Criteria

- **AC1 — POM Parent Reference (Expected Output):**
  - **Given** the generator receives connector name `"myformat"` and base package `"com.example.spark.sql.v2.myformat"`
  - **When** the project skeleton is generated
  - **Then** a `pom.xml` file is created with a `<parent>` element containing `<groupId>org.apache.spark</groupId>`, `<artifactId>spark-parent_2.13</artifactId>`, `<version>4.1.0-SNAPSHOT</version>`, and `<relativePath>../../pom.xml</relativePath>` — matching the Avro connector POM structure
  - `Source: connector/avro/pom.xml, lines 21–26`

- **AC2 — POM Dependency Declarations (Expected Output):**
  - **Given** the project skeleton is generated
  - **When** the `pom.xml` dependencies section is inspected
  - **Then** it contains a `<dependency>` for `org.apache.spark:spark-sql_${scala.binary.version}` with `<scope>provided</scope>`, and three test-jar dependencies — `spark-core_${scala.binary.version}`, `spark-catalyst_${scala.binary.version}`, and `spark-sql_${scala.binary.version}` — each with `<type>test-jar</type>` and `<scope>test</scope>`, plus `scalacheck_${scala.binary.version}` with `<scope>test</scope>`, `spark-tags_${scala.binary.version}`, and `scala-parallel-collections_${scala.binary.version}`
  - `Source: connector/avro/pom.xml, lines 37–76`

- **AC3 — POM Build Configuration, ArtifactId, and Properties (Expected Output):**
  - **Given** the project skeleton is generated with connector name `"myformat"`
  - **When** the `pom.xml` `<build>` and `<properties>` sections are inspected
  - **Then** `<outputDirectory>` is set to `target/scala-${scala.binary.version}/classes`, `<testOutputDirectory>` is set to `target/scala-${scala.binary.version}/test-classes`, the `<properties>` section contains `<sbt.project.name>myformat</sbt.project.name>`, and the `<artifactId>` is set to `spark-myformat_2.13` — matching the naming convention where connector name `"avro"` maps to `artifactId` `spark-avro_2.13` and `sbt.project.name` `avro`
  - `Source: connector/avro/pom.xml, lines 28–30, 82–84`

- **AC4 — Directory Structure (Expected Output):**
  - **Given** the project skeleton is generated with connector name `"myformat"` and base package `"com.example.spark.sql.v2.myformat"`
  - **When** the directory structure is inspected
  - **Then** the following directories exist: `src/main/scala/com/example/spark/sql/v2/myformat/`, `src/test/scala/com/example/spark/sql/v2/myformat/`, and `src/main/resources/META-INF/services/`; and a `README.md` file exists at the generated project root

- **AC5 — Empty Connector Name Validation (Input Validation):**
  - **Given** the connector name parameter is an empty string
  - **When** the generator is invoked
  - **Then** it returns an error message stating `"Connector name must be a non-empty string containing only lowercase alphanumeric characters and hyphens"`

- **AC6 — Invalid Base Package Validation (Input Validation):**
  - **Given** the base package parameter contains invalid Java package characters (e.g., `"com.my company"` with a space)
  - **When** the generator validates input
  - **Then** it returns an error message stating `"Base package must be a valid Java package name (lowercase letters, digits, dots as separators)"`

- **AC7 — Non-Writable Output Directory (Error Handling):**
  - **Given** the target output directory is not writable
  - **When** the generator attempts to create the project skeleton
  - **Then** it returns an error message specifying the absolute directory path and the underlying permission error, without creating any partial output

- **AC8 — Single-Character Connector Name (Edge Case):**
  - **Given** the connector name parameter is the single character `"x"` and the base package is `"org.apache.spark.sql.v2.x"`
  - **When** the generator creates the project skeleton
  - **Then** the generated `pom.xml` contains `<artifactId>spark-x_2.13</artifactId>` and `<sbt.project.name>x</sbt.project.name>`, the directory structure includes `src/main/scala/org/apache/spark/sql/v2/x/` and `src/test/scala/org/apache/spark/sql/v2/x/`, and `mvn validate` succeeds on the generated POM without errors

## Sub-Tasks

- Create Maven POM template with parameterized `<parent>` reference (`spark-parent_2.13:4.1.0-SNAPSHOT`), `<artifactId>` (`spark-<name>_2.13`), `<name>` (`Spark <Name>`), and `<sbt.project.name>` property
- Create dependency section template containing `spark-sql` in provided scope, `spark-core`/`spark-catalyst`/`spark-sql` test-jar entries in test scope, `scalacheck` in test scope, `spark-tags`, and `scala-parallel-collections`
- Create `<build>` section template with Scala cross-build output directories (`target/scala-${scala.binary.version}/classes` and `target/scala-${scala.binary.version}/test-classes`)
- Implement directory structure generator that creates `src/main/scala/<package-path>/`, `src/test/scala/<package-path>/`, and `src/main/resources/META-INF/services/` based on the provided base package
- Create `README.md` template populated with connector name, build instructions (`mvn compile`, `mvn test`, `sbt compile`, `sbt test`), and links to Apache Spark DataSource V2 documentation
- Implement input validation for connector name (must be non-empty, contain only lowercase alphanumeric characters and hyphens, must not start or end with a hyphen) and base package (must be a valid Java package name: lowercase letters, digits, and dots as separators, no leading/trailing/consecutive dots)
- Write unit tests verifying generated POM structure (parent, dependencies, build config, properties), directory layout, `README.md` content, and all input validation rules

## Edge Cases

- **Empty/Null Input:** Connector name is `null` or an empty string — the generator must reject the input with a descriptive error message stating the allowed format rather than producing a malformed `pom.xml` with an empty `<artifactId>` or blank `<sbt.project.name>`
- **Invalid Characters in Connector Name:** Connector name contains characters outside the allowed set (e.g., `"my_format!"`, `"@connector"`, `"My-Format"` with uppercase) — the generator must validate and reject the input, specifying that only lowercase alphanumeric characters and hyphens are permitted
- **Single-Character Boundary:** Connector name is a single character such as `"x"` — the generator must produce a valid `pom.xml` with `<artifactId>spark-x_2.13</artifactId>` and `<sbt.project.name>x</sbt.project.name>`, and the directory structure must be created without error
- **Malformed Base Package:** Base package is empty, contains leading dots (e.g., `".com.example"`), trailing dots (e.g., `"com.example."`), or consecutive dots (e.g., `"com..example"`) — the generator must validate and reject the input with a specific error describing the exact malformation detected
- **Pre-Existing Output Directory:** Target output directory already exists and contains files — the generator must detect the existing content and fail with an error message listing the conflicting file paths rather than silently overwriting existing artifacts

## Dependencies

- **EPIC-003** — Unified Connector Development SDK (parent epic defining overall SDK scope and boundaries)
- **FEATURE-003-01** — Connector Scaffolding Generator (parent feature containing this story and four sibling stories)
  - `Link:` [FEATURE-003-01-connector-scaffolding-generator.md](../FEATURE-003-01-connector-scaffolding-generator.md)
- **F-008 (Data Source Connectors)** — Existing DataSource V2 connector pattern that defines the structural conventions the generated project must follow
- **Apache Maven 3.x** — Generated POM must be compatible with Maven 3.x build lifecycle; `mvn compile` must succeed on the generated project with an empty source tree
- **Source Reference:** `connector/avro/pom.xml` — Complete reference POM with parent declaration (lines 21–26), dependency section (lines 36–81), build configuration (lines 82–85), `artifactId` (line 28), and `sbt.project.name` (line 30)
- **Source Reference:** `connector/protobuf/pom.xml` — Alternative reference POM demonstrating shade plugin configuration for connectors requiring external dependency relocation
- **Source Reference:** `connector/protobuf/README.md` — Build instruction reference pattern showing `mvn` and `sbt` build commands and environment variable configuration (`SPARK_PROTOC_EXEC_PATH`)

## Story Estimation Guidance

- **Story Points:** 5 (Fibonacci scale)
- **Rationale:** Generating a complete, compilable Maven project requires precise POM structure matching against the existing Avro/Protobuf connector POMs, directory layout creation based on parameterized package paths, and robust input validation covering connector name format, Java package name format, and filesystem permission checks. The POM must reference the exact parent version (`4.1.0-SNAPSHOT`), declare dependencies with the exact scope and type conventions, and configure Scala cross-build output paths. While the core output is template-based, the input validation logic and edge case handling for malformed inputs, single-character names, and pre-existing directories add material complexity.
- **Sprint Fit:** Completable within a single sprint. This is the foundational story for FEATURE-003-01 — subsequent stories (read path boilerplate, write path boilerplate, service registration, documentation template) depend on the project skeleton this story produces.

## Definition of Done

- Generated `pom.xml` contains a `<parent>` element referencing `org.apache.spark:spark-parent_2.13:4.1.0-SNAPSHOT` with `<relativePath>../../pom.xml</relativePath>`
- Generated `pom.xml` includes `spark-sql_${scala.binary.version}` with `<scope>provided</scope>`, test-jar dependencies for `spark-core`, `spark-catalyst`, and `spark-sql` with `<scope>test</scope>`, `scalacheck_${scala.binary.version}` with `<scope>test</scope>`, `spark-tags_${scala.binary.version}`, and `scala-parallel-collections_${scala.binary.version}`
- Generated `pom.xml` `<build>` section sets `<outputDirectory>` to `target/scala-${scala.binary.version}/classes` and `<testOutputDirectory>` to `target/scala-${scala.binary.version}/test-classes`
- All required directories exist after generation: `src/main/scala/<package>/`, `src/test/scala/<package>/`, `src/main/resources/META-INF/services/`
- A `README.md` file exists at the generated project root containing the connector name and build instructions for both Maven and SBT
- `<artifactId>` follows the `spark-<name>_2.13` naming convention (e.g., connector name `"myformat"` produces `spark-myformat_2.13`)
- `<sbt.project.name>` property value matches the provided connector name exactly
- Generated project passes `mvn validate` and `mvn compile` with an empty source tree (POM structure is valid and all parent references resolve)
- Input validation rejects empty, null, and invalid connector names with descriptive error messages specifying allowed characters
- Input validation rejects empty, malformed, and invalid Java package names with descriptive error messages specifying the detected issue
- Unit tests cover all 8 acceptance criteria and all 5 edge cases
- Story is demo-able: running the generator with connector name `"myformat"` and base package `"com.example.spark.sql.v2.myformat"` produces a browsable, compilable Maven project that a reviewer can inspect and build
