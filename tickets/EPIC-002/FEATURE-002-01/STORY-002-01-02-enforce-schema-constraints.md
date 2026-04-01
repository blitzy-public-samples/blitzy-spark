# Enforce Schema Constraints Against Declared Specifications to Detect Column Type and Structure Violations in DataFrames

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

**As a** data engineer,
**I want to** define a schema constraint specification as a `StructType` and validate a DataFrame against it — checking that all declared columns are present, column data types match the specification, column ordering matches when strict ordering is enabled, and nullable/non-nullable constraints are enforced — producing a detailed validation report listing each constraint violation with the column name, expected value, and actual value,
**So that** schema drift in upstream data sources is detected before it causes downstream pipeline failures or silent data corruption, reducing incident response time for schema-related issues by providing immediate, actionable validation results at the point of data ingestion, and ensuring data contracts are enforced programmatically rather than discovered through production errors.

### Technical Context

- `StructType` (`Source: sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala`) provides the schema definition model with `StructField` entries containing `name`, `DataType`, `nullable` flag, and `Metadata`. The class exposes `fieldNames` (line 111) for column name arrays, `nameToIndex` (line 122) for name-to-position mapping, and `nameToIndexCaseInsensitive` (line 123–124) for case-insensitive lookups via `CaseInsensitiveMap`.
- `StructType` already provides `fieldNames`, `fieldIndex`, `apply(name)`, and `merge` utilities that the schema constraint engine can leverage for column existence checks, field-level access, and schema comparison (`Source: sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala`).
- The Catalyst Analyzer (`Source: sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala`) performs schema resolution including attribute resolution, type coercion, and constraint checking — establishing the pattern that schema constraint validation extends at the user-facing API level. The `SimpleAnalyzer` (line 75) and `caseSensitiveResolution` (line 84) demonstrate the case sensitivity resolution model.
- The `DataType` hierarchy in `sql/api/src/main/scala/org/apache/spark/sql/types/` defines all supported types — `StringType`, `IntegerType`, `LongType`, `DoubleType`, `ArrayType`, `MapType`, `StructType`, and others — enabling recursive type comparison for nested schema validation.
- Column expressions (`Source: sql/api/src/main/scala/org/apache/spark/sql/Column.scala`) provide the predicate foundation for quality rules that schema constraints integrate with.

## Acceptance Criteria

### AC-1: Input Validation — Empty Schema Specification Rejected

- **Given** a schema constraint specification defined as a `StructType` with zero fields
- **When** the constraint is applied to a DataFrame
- **Then** the system raises an `IllegalArgumentException` with a message indicating that the schema specification must contain at least one field

### AC-2: Expected Output — All Schema Constraints Pass

- **Given** a DataFrame with columns (`id`: IntegerType, `name`: StringType, `age`: IntegerType) and a schema specification declaring the same 3 columns with matching types and matching nullability flags
- **When** schema validation is executed
- **Then** the validation result reports a pass status with zero violations and the `SchemaValidationResult.passStatus` field is `true`

### AC-3: Expected Output — Type Mismatch Detected

- **Given** a DataFrame with column "age" of type StringType and a schema specification declaring "age" as IntegerType
- **When** schema validation is executed
- **Then** the validation result reports one `TypeMismatchViolation` with details: column="age", expected=IntegerType, actual=StringType

### AC-4: Error Handling — Missing Columns Detected

- **Given** a DataFrame missing columns "email" and "phone" that are declared in the schema specification
- **When** schema validation is executed
- **Then** the validation result lists two `MissingColumnViolation` entries: one for "email" and one for "phone", each including the expected data type from the specification

### AC-5: Edge Case — Extra Columns Ignored in Non-Strict Mode

- **Given** a DataFrame containing columns "id", "name", and "extra_col" and a schema specification declaring only "id" and "name"
- **When** schema validation is executed with `strictColumns` set to `false`
- **Then** the validation result reports a pass status with zero violations, and the column "extra_col" is not flagged

### AC-6: Edge Case — Strict Column Ordering Enforced

- **Given** a DataFrame with columns in order (`name`, `id`, `age`) and a schema specification declaring order (`id`, `name`, `age`)
- **When** schema validation is executed with `strictOrdering` set to `true`
- **Then** the validation result reports `OrderingViolation` entries listing the expected column position versus actual position for each misplaced column: "id" expected at position 0 but found at position 1, and "name" expected at position 1 but found at position 0

### AC-7: Nullable Constraint Violation Detected

- **Given** a DataFrame with column "email" declared as `nullable=true` and a schema specification declaring "email" as `nullable=false`
- **When** schema validation is executed
- **Then** the validation result reports one `NullabilityViolation` with details: column="email", expected_nullable=false, actual_nullable=true

### AC-8: Nested Struct Type Mismatch Detected Recursively

- **Given** a DataFrame with a nested struct column "address" containing fields (`street`: StringType, `zip`: IntegerType) and a schema specification declaring "address" as StructType with fields (`street`: StringType, `zip`: StringType)
- **When** schema validation is executed
- **Then** the validation result reports a `TypeMismatchViolation` for the nested field "address.zip" with expected=StringType and actual=IntegerType

## Sub-Tasks

- [ ] Define `SchemaConstraint` class accepting a `StructType` specification and two configuration flags: `strictOrdering` (Boolean, default `false`) and `strictColumns` (Boolean, default `false` — when `true`, extra columns in the DataFrame are reported as violations)
- [ ] Add construction-time validation to `SchemaConstraint`: reject specifications with zero fields by raising `IllegalArgumentException`, and reject specifications with duplicate field names by raising `IllegalArgumentException` identifying the duplicate column name
- [ ] Implement column presence validation: iterate specification fields and check each exists in the DataFrame schema using `StructType.fieldNames` and `StructType.fieldIndex` (`Source: sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala` — `fieldNames` at line 111, `nameToIndex` at line 122); produce a `MissingColumnViolation` for each absent column
- [ ] Implement column type validation: compare `DataType` of each specification field against the corresponding DataFrame column `DataType`; for nested `StructType`, `ArrayType` (element type), and `MapType` (key and value types), recurse into child types and report violations using dot-notation paths (e.g., "address.zip")
- [ ] Implement nullable constraint validation: compare the `nullable` flag of each specification field against the DataFrame column's `nullable` flag; produce a `NullabilityViolation` for each mismatch
- [ ] Implement column ordering validation (when `strictOrdering=true`): compare field position indices between specification and DataFrame schema using `StructType.fieldNames` index positions; produce an `OrderingViolation` for each column whose position differs
- [ ] Implement strict column mode (when `strictColumns=true`): detect columns present in the DataFrame but not declared in the specification; produce an `ExtraColumnViolation` for each undeclared column
- [ ] Create `SchemaValidationResult` class with fields: `violations` (`Seq[SchemaViolation]`) and `passStatus` (Boolean, `true` when `violations` is empty)
- [ ] Create `SchemaViolation` sealed trait with five subtypes: `MissingColumnViolation(columnName: String, expectedType: DataType)`, `TypeMismatchViolation(columnPath: String, expectedType: DataType, actualType: DataType)`, `NullabilityViolation(columnName: String, expectedNullable: Boolean, actualNullable: Boolean)`, `OrderingViolation(columnName: String, expectedPosition: Int, actualPosition: Int)`, `ExtraColumnViolation(columnName: String, actualType: DataType)`
- [ ] Integrate case sensitivity handling: use `spark.sql.caseSensitive` configuration to determine whether column name comparisons use case-sensitive or case-insensitive matching, leveraging `StructType.nameToIndexCaseInsensitive` (`Source: sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala` — line 123–124)
- [ ] Add unit tests for each violation type: missing column, type mismatch (flat and nested), nullable mismatch, ordering violation, and extra column violation
- [ ] Add unit tests for configuration flags: `strictOrdering=false` (skip ordering), `strictColumns=false` (ignore extras), and both enabled simultaneously
- [ ] Write API documentation with examples of basic schema constraint definition, strict-mode validation, and nested struct validation in Scala and Python

## Edge Cases

### EC-1: Empty DataFrame Schema — All Specification Columns Reported Missing

The DataFrame being validated has a schema with zero columns (empty DataFrame schema). Validation must report all specification columns as `MissingColumnViolation` entries — one per specified field — each including the expected data type from the specification.

### EC-2: Empty Specification — Rejected at Construction

The schema specification `StructType` contains zero fields. The `SchemaConstraint` constructor must raise an `IllegalArgumentException` with a message indicating that the schema specification must contain at least one field. No validation is executed.

### EC-3: Boundary Value — Wide Schema With 500 Columns

Both the DataFrame and the specification contain 500 columns with matching types and nullability. Validation must complete without stack overflow or excessive memory allocation, comparing all 500 columns and returning a pass status with zero violations.

### EC-4: Invalid Input — Duplicate Column Names in Specification

The schema specification contains two `StructField` entries with the same name (e.g., two fields named "id"). The `SchemaConstraint` constructor must raise an `IllegalArgumentException` identifying the duplicate column name before executing any validation logic.

### EC-5: Case Sensitivity — Column Name Matching Respects Spark Configuration

The DataFrame has column "Name" (uppercase N) and the specification declares "name" (lowercase n). When `spark.sql.caseSensitive` is set to `false` (the default), validation treats these as the same column and reports no missing-column violation. When `spark.sql.caseSensitive` is set to `true`, validation reports a `MissingColumnViolation` for "name" and (if `strictColumns=true`) an `ExtraColumnViolation` for "Name".

## Dependencies

- **STORY-002-01-01** — Define Column Validation Rules: provides the foundational `QualityRule` sealed trait, `RuleValidationResult` model, and `QualityRuleSet` execution infrastructure that `SchemaConstraint` integrates with (`Source: tickets/EPIC-002/FEATURE-002-01/STORY-002-01-01-define-column-validation-rules.md`)
- **STORY-002-01-04** — Configure Rule Severity Levels: provides `QualitySeverity` enum (ERROR, WARNING, INFO) and severity configuration resolution for schema constraint violations, controlling whether violations halt pipeline execution or record metadata (`Source: tickets/EPIC-002/FEATURE-002-01/STORY-002-01-04-configure-rule-severity-levels.md`)
- **EPIC-002** — Declarative Data Quality Validation Framework: parent epic defining the overall scope and objectives for native data quality capabilities (`Source: tickets/EPIC-002-declarative-data-quality-framework.md`)
- **FEATURE-002-01** — DataFrame Quality Rules Engine: parent feature defining the rules engine, including schema constraint enforcement, severity levels, and pipeline integration scope (`Source: tickets/EPIC-002/FEATURE-002-01-dataframe-quality-rules-engine.md`)
- **F-002 — SQL Query Processing Engine**: provides `StructType`, `StructField`, the `DataType` hierarchy, and DataFrame schema access that schema constraint validation operates on
- **StructType**: `sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala` — schema model with `fieldNames`, `fieldIndex`, `nameToIndex`, `nameToIndexCaseInsensitive`, `merge`, and `apply` utilities
- **Catalyst Analyzer**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala` — existing schema resolution and constraint validation patterns that inform the user-facing schema constraint API design
- **DataType Hierarchy**: `sql/api/src/main/scala/org/apache/spark/sql/types/` — all supported data types (`StringType`, `IntegerType`, `LongType`, `DoubleType`, `ArrayType`, `MapType`, `StructType`, etc.) for recursive type comparison

## Story Estimation Guidance

- **Story Points:** 5 (Fibonacci scale)
- **Rationale:** Schema comparison leverages existing `StructType` utilities (`fieldNames`, `fieldIndex`, `nameToIndex`), but nested struct recursion through `StructType`, `ArrayType`, and `MapType` child types, the strict ordering mode with position comparison, case sensitivity integration via `spark.sql.caseSensitive` and `CaseInsensitiveMap`, and the comprehensive violation reporting model (sealed trait with five subtypes) add moderate complexity. The `SchemaViolation` class hierarchy requires careful design to support dot-notation paths for nested fields. The scope is contained and completable within a single sprint.

## Definition of Done

- [ ] `SchemaConstraint` class accepts a `StructType` specification with optional `strictOrdering` (Boolean, default `false`) and `strictColumns` (Boolean, default `false`) configuration flags
- [ ] Construction-time validation rejects specifications with zero fields (`IllegalArgumentException`) and specifications with duplicate field names (`IllegalArgumentException`)
- [ ] Column presence validation detects missing columns and produces `MissingColumnViolation` entries with the expected data type
- [ ] Column type validation compares `DataType` recursively for nested `StructType`, `ArrayType` element types, and `MapType` key/value types, using dot-notation paths for nested field violation reporting
- [ ] Nullable constraint validation detects `nullable` flag mismatches and produces `NullabilityViolation` entries with expected and actual values
- [ ] Column ordering validation (when `strictOrdering=true`) reports `OrderingViolation` entries with expected and actual position indices
- [ ] Strict column mode (when `strictColumns=true`) reports `ExtraColumnViolation` entries for columns in the DataFrame not declared in the specification
- [ ] `SchemaValidationResult` contains typed `SchemaViolation` entries with column names (or dot-notation paths) and expected versus actual values, and `passStatus` is `true` when zero violations exist
- [ ] Case sensitivity handling respects `spark.sql.caseSensitive` configuration for column name matching
- [ ] Unit tests cover all 8 acceptance criteria scenarios including nested struct validation
- [ ] Edge case tests cover empty DataFrame schemas, empty specifications, wide schemas (500 columns), duplicate column names, and case sensitivity behavior
- [ ] API documentation includes examples for basic schema constraint definition, strict-mode validation (ordering and column enforcement), and nested struct validation
- [ ] INVEST compliance verified:
  - **Independent:** Uses the `StructType` API and `DataType` hierarchy directly; depends on STORY-002-01-01 for rule infrastructure but operates on schema-level constraints independently from column-level rules
  - **Negotiable:** Nested struct recursion depth is adjustable; strict ordering and strict column modes are optional flags; `ArrayType` and `MapType` recursion scope is negotiable
  - **Valuable:** Prevents schema drift incidents by detecting column type changes, missing columns, and nullability contract violations before they cause downstream failures or silent data corruption
  - **Estimable:** 5 story points based on the defined scope of `SchemaConstraint` class, `SchemaViolation` hierarchy (5 subtypes), recursive type comparison, and case sensitivity integration
  - **Sized:** Completable within one sprint given the well-bounded implementation scope and clear violation behavior contracts
  - **Testable:** All 8 acceptance criteria have concrete Given/When/Then BDD assertions with specific column names, data types, expected violation types, and expected versus actual values
