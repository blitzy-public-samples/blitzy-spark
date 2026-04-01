# Generate Connector Documentation Template to Standardize Data Source Usage Guides

## User Story

**As a** platform engineer, **I want to** run the scaffolding generator and receive a pre-populated README.md documentation template that includes sections for deploying the connector (`--packages` instructions), a configuration options table, read and write API examples in Scala, Python, and SQL, and supported type conversion tables, **so that** connector documentation follows a consistent structure across all custom connectors, reducing documentation authoring time by 70% and ensuring end-users can onboard to any custom connector with a familiar guide format.

## Acceptance Criteria

- **AC1 — Deploying Section Generation (Expected Output):**
  - **Given** the generator has produced a connector project with Maven coordinates `org.apache.spark:spark-myformat_2.13:4.1.0-SNAPSHOT`
  - **When** the documentation template is generated
  - **Then** the README.md file contains a `## Deploying` section with `spark-submit --packages org.apache.spark:spark-myformat_2.13:4.1.0-SNAPSHOT` and `spark-shell --packages org.apache.spark:spark-myformat_2.13:4.1.0-SNAPSHOT` command examples, matching the deployment instructions pattern in the Avro data source guide
  - `Source: docs/sql-data-sources-avro.md` — Deploying section (lines 26–38)

- **AC2 — Configuration Options Table Generation (Expected Output):**
  - **Given** the generator receives the connector short name and a list of supported configuration options with metadata (property name, default value, description, since version)
  - **When** the documentation template is generated
  - **Then** the README.md includes a `## Configuration Options` section with a Markdown table containing columns: Property Name, Default, Description, and Since Version — with one row per provided configuration option
  - `Source: docs/sql-data-sources-avro.md` — Data Source Option section (lines 248–363)

- **AC3 — Read and Write API Examples Generation (Expected Output):**
  - **Given** the connector short name `"myformat"` is provided
  - **When** the README.md is generated
  - **Then** it includes a `## Load and Save Functions` section containing read and write API code examples in three languages: a Scala block using `spark.read.format("myformat").load("path")` and `df.write.format("myformat").save("path")`, a Python block using `spark.read.format("myformat").load("path")` and `df.write.format("myformat").save("path")`, and a SQL block using `CREATE TEMPORARY VIEW` with `USING myformat` syntax
  - `Source: docs/sql-data-sources-avro.md` — Load and Save Functions section (lines 40–84)

- **AC4 — Empty Short Name Rejection (Input Validation):**
  - **Given** the connector short name parameter is an empty string
  - **When** the generator attempts to produce the documentation template
  - **Then** the generator returns an error message stating `"Connector short name must be a non-empty string containing only lowercase alphanumeric characters and hyphens"`

- **AC5 — Zero Configuration Options Placeholder (Edge Case):**
  - **Given** no custom configuration options are defined for the connector
  - **When** the documentation template is generated
  - **Then** the Configuration Options section displays a placeholder row stating `"No custom configuration options. Inherits standard Spark SQL data source options."`

- **AC6 — Missing Output Directory Handling (Error Handling):**
  - **Given** the target output directory does not exist
  - **When** the generator attempts to write the README.md
  - **Then** the generator creates the directory and writes the file, or returns an error message specifying the absolute path that could not be created and the underlying filesystem error description

- **AC7 — Supported Types Tables Generation (Expected Output):**
  - **Given** the connector supports both read and write operations
  - **When** the documentation template is generated
  - **Then** it includes a `## Supported Types` section with two Markdown tables: one titled `Source Format → Spark SQL` containing source-format-to-Spark-SQL type mappings, and one titled `Spark SQL → Source Format` containing Spark-SQL-to-source-format type mappings — matching the bidirectional type conversion table structure in the Avro data source guide
  - `Source: docs/sql-data-sources-avro.md` — Supported types sections (lines 479–535, 576–600+)

## Sub-Tasks

- Implement README.md template engine that accepts connector metadata inputs: short name, Maven coordinates (`groupId:artifactId:version`), configuration options list (each with property name, default, description, since version), and supported type mappings (source-to-Spark and Spark-to-source)
- Create Deploying section template with `spark-submit --packages` and `spark-shell --packages` command patterns using the connector's Maven coordinates, matching the structure at `docs/sql-data-sources-avro.md` lines 26–38
  - `Source: connector/avro/pom.xml` — Maven coordinates pattern (`org.apache.spark:spark-avro_2.13:4.1.0-SNAPSHOT`)
- Create Configuration Options section template with Markdown table generation containing columns (Property Name, Default, Description, Since Version) populated from the connector's configuration options metadata
- Create Load and Save Functions section template with Scala, Python, and SQL code blocks using the connector's short name in `spark.read.format("<shortName>")` and `df.write.format("<shortName>")` call patterns
- Create Supported Types section template with two bidirectional type conversion tables: `Source Format → Spark SQL` for read conversions and `Spark SQL → Source Format` for write conversions
- Implement validation for connector metadata inputs: short name must match the pattern `^[a-z0-9]+(-[a-z0-9]+)*$`, Maven coordinates must follow the `groupId:artifactId:version` format with three non-empty colon-separated segments
- Add fallback/placeholder content for optional sections: a placeholder row for the Configuration Options table when zero options are provided, omission of the write examples and `Spark SQL → Source Format` table when the connector is read-only
- Write unit tests verifying generated README.md structure and content for all section types, placeholder behavior for empty inputs, and validation error messages for malformed inputs

## Edge Cases

- **Empty/Null Input:** Connector short name is null or empty string — the generator must reject the input with a descriptive error message stating `"Connector short name must be a non-empty string containing only lowercase alphanumeric characters and hyphens"` rather than producing a README.md with blank format references in code examples
- **Boundary — Read-Only Connector:** Connector supports only read operations (no write path) — the documentation template must omit the write API examples from the Load and Save Functions section and omit the `Spark SQL → Source Format` type conversion table from the Supported Types section, generating a read-only documentation variant
- **Invalid Maven Coordinates:** Maven coordinates are malformed (missing groupId, artifactId, or version segment, e.g., `"org.apache.spark:spark-myformat_2.13"` with no version) — the generator must validate the `groupId:artifactId:version` format and return an error specifying which segment is missing: `"Maven coordinates must follow the format 'groupId:artifactId:version'; missing segment: version"`
- **Boundary — Zero Configuration Options:** Connector has zero supported configuration options — the Configuration Options section must display a single placeholder row stating `"No custom configuration options. Inherits standard Spark SQL data source options."` rather than rendering an empty table with only header rows
- **Invalid Short Name Format:** Connector short name contains uppercase characters or spaces (e.g., `"MyFormat"` or `"my format"`) — the generator must reject the input with a format validation error stating `"Connector short name must contain only lowercase alphanumeric characters and hyphens; received: <input>"`

## Dependencies

- **STORY-003-01-01** (Generate Connector Project Skeleton) — the documentation template README.md is generated as part of the project skeleton output directory; the project skeleton provides the Maven coordinates and connector name used in documentation examples
  - `Link:` [STORY-003-01-01-generate-connector-project-skeleton.md](./STORY-003-01-01-generate-connector-project-skeleton.md)
- **STORY-003-01-02** (Create Read Path Boilerplate) — read API examples in the documentation reference the generated read path classes and use `spark.read.format("<shortName>")` patterns tied to the DataSourceV2 registration
  - `Link:` [STORY-003-01-02-create-read-path-boilerplate.md](./STORY-003-01-02-create-read-path-boilerplate.md)
- **STORY-003-01-03** (Create Write Path Boilerplate) — write API examples in the documentation reference the generated write path classes and use `df.write.format("<shortName>")` patterns; when write support is disabled, documentation omits write examples
  - `Link:` [STORY-003-01-03-create-write-path-boilerplate.md](./STORY-003-01-03-create-write-path-boilerplate.md)
- **STORY-003-01-04** (Generate DataSource V2 Registration) — the connector's registered short name from the `shortName()` method is used throughout the documentation in `format("<shortName>")` calls and deployment instructions
  - `Link:` [STORY-003-01-04-generate-datasource-v2-registration.md](./STORY-003-01-04-generate-datasource-v2-registration.md)
- **FEATURE-003-01** (Connector Scaffolding Generator) — parent feature defining the overall scaffolding scope including project skeleton, read/write paths, service registration, and this documentation template generation
  - `Link:` [FEATURE-003-01-connector-scaffolding-generator.md](../FEATURE-003-01-connector-scaffolding-generator.md)
- **EPIC-003** (Unified Connector Development SDK) — parent epic defining the SDK scope and the documentation standards that generated connector guides must follow
  - `Link:` [EPIC-003-unified-connector-development-sdk.md](../../EPIC-003-unified-connector-development-sdk.md)
- **`docs/sql-data-sources.md`** — top-level data sources index page providing the navigation structure and section naming conventions for data source documentation
  - `Source: docs/sql-data-sources.md`
- **`docs/sql-data-sources-avro.md`** — Avro data source guide serving as the primary style reference for the generated documentation template, including sections: Deploying (lines 26–38), Load and Save Functions (lines 40–84), Data Source Option (lines 248–363), Configuration (lines 365–457), Supported types for Avro → Spark SQL conversion (lines 479–573), Supported types for Spark SQL → Avro conversion (lines 576–600+)
  - `Source: docs/sql-data-sources-avro.md`

## Story Estimation Guidance

- **Story Points:** 3 (Fibonacci scale)
- **Rationale:** Template-driven documentation generation with a well-defined section structure modeled after the existing Avro data source guide; moderate complexity arising from multi-language code example generation (Scala, Python, SQL), bidirectional type conversion table templating, and conditional section omission for read-only connectors; bounded by well-understood patterns observed in `docs/sql-data-sources-avro.md` and `docs/sql-data-sources.md` which provide explicit structural references for each generated section
- **Sprint Fit:** Completable within a single sprint. This is the final story in FEATURE-003-01 and depends on STORY-003-01-01 (project skeleton), STORY-003-01-02 (read path), STORY-003-01-03 (write path), and STORY-003-01-04 (service registration) for the connector metadata consumed by the documentation template

## Definition of Done

- Documentation template generator produces a README.md containing all required sections: Deploying (with `--packages` command examples), Configuration Options (with Markdown table), Load and Save Functions (with Scala, Python, and SQL code blocks), and Supported Types (with bidirectional conversion tables)
- Generated README.md follows the section structure and style of `docs/sql-data-sources-avro.md`, including the Deploying section pattern (lines 26–38), Load and Save Functions pattern (lines 40–84), Data Source Option table pattern (lines 248–363), and Supported Types table pattern (lines 479–600+)
- Code examples in the Load and Save Functions section use the connector's registered short name in `spark.read.format("<shortName>")` and `df.write.format("<shortName>")` calls across Scala, Python, and SQL languages
- Type conversion tables in the Supported Types section include all Spark SQL primitive types at minimum: BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, BinaryType, DateType, TimestampType, and DecimalType
- Input validation rejects empty or null short names with the error message `"Connector short name must be a non-empty string containing only lowercase alphanumeric characters and hyphens"`
- Input validation rejects malformed Maven coordinates with an error message specifying the missing segment (groupId, artifactId, or version)
- Input validation rejects short names containing uppercase characters or spaces with a format error specifying allowed characters
- Read-only connectors produce a documentation variant that omits write API examples and the `Spark SQL → Source Format` type conversion table
- Zero-configuration connectors produce a Configuration Options section with a placeholder row instead of an empty table
- Unit tests cover all 7 acceptance criteria and all 5 edge cases
- Story is demo-able: running the generator with sample connector metadata (short name `"myformat"`, Maven coordinates `org.apache.spark:spark-myformat_2.13:4.1.0-SNAPSHOT`, sample configuration options, and sample type mappings) produces a complete, readable README.md that a reviewer can inspect and verify against the Avro data source guide structure
