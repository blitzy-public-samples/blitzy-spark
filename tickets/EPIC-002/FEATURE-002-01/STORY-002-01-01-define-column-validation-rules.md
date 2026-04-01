# Define Column Validation Rules on DataFrames to Enable Declarative Data Quality Checks for Nullability, Range, Pattern, and Custom Constraints

## User Story

**As a** data engineer,
**I want to** define column-level validation rules on a DataFrame using a declarative API that supports five rule types — nullability checks (non-null enforcement), numeric range validation (minimum and maximum bounds), string pattern matching (regular expression compliance), enum constraints (allowed-value sets), and custom expression-based rules (arbitrary Spark SQL Column expressions) — and execute these rules to produce a validation result containing the rule name, target column, pass/fail status, and count of non-conforming rows for each rule,
**So that** data quality is validated at the point of ingestion or transformation without writing custom validation logic for each pipeline, reducing the code needed for quality checks by providing a standardized, reusable rule API that integrates natively with the DataFrame API, and enabling data engineers to build quality gates that catch null values, out-of-range numbers, malformed strings, and unexpected categorical values before they propagate to downstream consumers.

### Technical Context

- The Column API (`Source: sql/api/src/main/scala/org/apache/spark/sql/Column.scala`) provides the expression-building DSL used for rule predicates: `isNull`, `isNotNull` (nullability), `>`, `<`, `>=`, `<=` (range), `rlike` (regex matching), `isin` (enum), and arbitrary boolean expressions
- `DataFrameNaFunctions` (`Source: sql/api/src/main/scala/org/apache/spark/sql/DataFrameNaFunctions.scala`) provides existing null-handling patterns (`drop`, `fill`, `replace`) that the quality rules extend with declarative validation semantics rather than transformation semantics
- The Catalyst Analyzer (`Source: sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala`) performs schema resolution and attribute validation, providing the foundation for column existence checks at rule creation time
- `StructType.fieldNames` (`Source: sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala`) exposes the array of column names in a DataFrame schema for column existence validation
- The rule execution model follows the pattern: for each rule, apply the rule's predicate expression to the DataFrame using `df.filter(rulePredicate)` to count conforming rows, then compute violations as `totalRows - conformingRows`
- Column expressions from Column.scala are the building blocks: e.g., `col("age").isNotNull` for nullability, `col("age") >= 0 && col("age") <= 150` for range, `col("email").rlike("^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$")` for pattern, `col("status").isin("ACTIVE", "INACTIVE")` for enum

## Acceptance Criteria

### AC-1: Input Validation — Missing Column Raises AnalysisException

- **Given** a column validation rule targeting column "email"
- **When** the rule is applied to a DataFrame that does not contain a column named "email"
- **Then** the system raises an `AnalysisException` with a message identifying "email" as a missing column in the DataFrame schema

### AC-2: Expected Output — Nullability Rule Detects Null Values

- **Given** a DataFrame with 1000 rows where column "name" contains 50 null values
- **When** a nullability rule (non-null check) is defined and executed on column "name"
- **Then** the validation result reports rule_name="name_not_null", column="name", status=FAIL, violating_row_count=50, total_row_count=1000

### AC-3: Expected Output — Range Rule Detects Out-of-Bounds Values

- **Given** a DataFrame with column "age" of type IntegerType containing values ranging from -5 to 200
- **When** a range validation rule with min=0 and max=150 is defined and executed on column "age"
- **Then** the validation result reports the count of rows where the "age" value is less than 0 or greater than 150 as violating rows

### AC-4: Expected Output — Pattern Rule Detects Non-Matching Strings

- **Given** a DataFrame with column "email" containing 100 rows where 10 rows do not match the regex pattern `^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$`
- **When** a pattern validation rule is defined and executed on column "email"
- **Then** the validation result reports exactly 10 violating rows

### AC-5: Expected Output — Enum Rule Detects Disallowed Values

- **Given** a DataFrame with column "status" containing values ["ACTIVE", "INACTIVE", "PENDING", "UNKNOWN"]
- **When** an enum validation rule with allowed values ["ACTIVE", "INACTIVE", "PENDING"] is defined and executed on column "status"
- **Then** the validation result reports all rows containing "UNKNOWN" as violating rows

### AC-6: Error Handling — Invalid Regex Pattern Raises IllegalArgumentException

- **Given** a pattern validation rule with an invalid regular expression string "[unclosed"
- **When** the rule is created
- **Then** the system raises an `IllegalArgumentException` with a message indicating that the provided regex pattern is not a valid regular expression

### AC-7: Expected Output — Custom Expression Rule Validates Cross-Column Logic

- **Given** a DataFrame with columns "start_date" and "end_date"
- **When** a custom expression rule `col("end_date") >= col("start_date")` is defined and executed
- **Then** the validation result reports the count of rows where end_date is before start_date as violating rows

### AC-8: Edge Case — All Rows Pass Validation

- **Given** a DataFrame with 500 rows where all values in column "age" are between 0 and 150
- **When** a range validation rule with min=0 and max=150 is executed
- **Then** the validation result reports status=PASS, violating_row_count=0, total_row_count=500

## Sub-Tasks

- [ ] Define `QualityRule` sealed trait with common fields: `ruleName` (String), `targetColumn` (String), `description` (Option[String])
- [ ] Implement `NullabilityRule` case class for non-null checks using `col(targetColumn).isNotNull` predicate (`Source: sql/api/src/main/scala/org/apache/spark/sql/Column.scala` — `isNotNull` at line 562)
- [ ] Implement `RangeRule` case class with `min`/`max` bounds (Double) using `col(targetColumn) >= min && col(targetColumn) <= max` predicate, with optional inclusivity flags (`Source: sql/api/src/main/scala/org/apache/spark/sql/Column.scala` — `>=` at line 426, `<=` at line 396)
- [ ] Implement `PatternRule` case class with `regex` pattern (String) using `col(targetColumn).rlike(pattern)` predicate, with regex syntax validation at construction time via `java.util.regex.Pattern.compile` (`Source: sql/api/src/main/scala/org/apache/spark/sql/Column.scala` — `rlike` at line 840)
- [ ] Implement `EnumRule` case class with `allowedValues` (Set[Any]) using `col(targetColumn).isin(allowedValues.toSeq: _*)` predicate, with non-empty set validation at construction (`Source: sql/api/src/main/scala/org/apache/spark/sql/Column.scala` — `isin` at line 774)
- [ ] Implement `CustomExpressionRule` case class accepting a `Column` expression (boolean predicate) for arbitrary validation logic
- [ ] Define `RuleValidationResult` case class with fields: `ruleName` (String), `targetColumn` (String), `status` (PASS/FAIL), `violatingRowCount` (Long), `totalRowCount` (Long)
- [ ] Implement `QualityRuleSet` class to hold a collection of rules and execute them against a DataFrame, returning `Seq[RuleValidationResult]`
- [ ] Implement schema validation at rule execution time: verify that the target column exists in the DataFrame schema using `StructType.fieldNames` (`Source: sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala` — `fieldNames` at line 111)
- [ ] Implement the rule execution engine: for each rule, apply the negated predicate via `df.filter(!rulePredicate)` to count violating rows, then compute `totalRowCount` via `df.count()` and set status to PASS when `violatingRowCount == 0`, FAIL otherwise
- [ ] Add fluent DataFrame API extension method: `df.validateQuality(rules: QualityRuleSet)` returning validation results
- [ ] Add unit tests for each rule type (nullability, range, pattern, enum, custom expression)
- [ ] Add unit tests for error handling (missing column raises `AnalysisException`, invalid regex raises `IllegalArgumentException`)
- [ ] Write API documentation with examples for each rule type in Scala and Python

## Edge Cases

### EC-1: Empty DataFrame — Zero Rows Validated

An empty DataFrame (zero rows) is validated against quality rules. All rules must return status=PASS, violating_row_count=0, total_row_count=0 without raising an exception.

### EC-2: Null Values in Range Check — Counted as Violations

A column contains null values and a `RangeRule` is applied. Null values must be counted as violations (since null is not within any numeric range) and reported in the `violatingRowCount`.

### EC-3: Boundary Values — Inclusive Range Bounds by Default

A `RangeRule` with min=0, max=100 is applied to a column containing the exact boundary values 0 and 100. Both boundary values must pass the validation because bounds are inclusive by default.

### EC-4: Invalid Input — Empty Enum Allowed Values Set

An `EnumRule` is created with an empty allowed values set. The system must raise an `IllegalArgumentException` indicating that the allowed values set must contain at least one value.

### EC-5: Type Mismatch — String Enum Applied to Integer Column

An `EnumRule` with String allowed values is applied to an IntegerType column. The system must raise an `AnalysisException` indicating the type mismatch between the rule's allowed values and the column data type.

## Dependencies

- **EPIC-002** — Declarative Data Quality Validation Framework: parent epic providing architectural context and overall scope for native data quality capabilities (`Source: tickets/EPIC-002-declarative-data-quality-framework.md`)
- **FEATURE-002-01** — DataFrame Quality Rules Engine: parent feature defining the rules engine scope, including column validation, schema constraints, referential integrity, severity levels, and pipeline integration (`Source: tickets/EPIC-002/FEATURE-002-01-dataframe-quality-rules-engine.md`)
- **F-002 — SQL Query Processing Engine**: DataFrame API, Column expressions, and StructType schema access required for rule definition and execution
- **Column API**: `sql/api/src/main/scala/org/apache/spark/sql/Column.scala` — column expressions (`isNull`, `isNotNull`, `>=`, `<=`, `rlike`, `isin`, arbitrary boolean expressions) for building rule predicates
- **DataFrameNaFunctions**: `sql/api/src/main/scala/org/apache/spark/sql/DataFrameNaFunctions.scala` — existing null-handling patterns (`drop`, `fill`, `replace`) that inform the quality rule API design by demonstrating DataFrame-level data cleansing operations
- **Catalyst Analyzer**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala` — schema resolution and attribute validation patterns for column existence verification
- **StructType**: `sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala` — schema model providing `fieldNames` for column existence checks and field type access for type-compatibility validation

## Story Estimation Guidance

- **Story Points:** 8 (Fibonacci scale)
- **Rationale:** This is the foundational story for the entire quality rules engine, requiring design of the rule type hierarchy (sealed trait + 5 case classes), the validation result model, the rule execution engine, schema validation at rule execution time, the fluent DataFrame API extension, and comprehensive tests for all 5 rule types plus error handling. The design decisions made in this story — the `QualityRule` trait, `RuleValidationResult` structure, and `QualityRuleSet` execution model — directly affect all subsequent stories in FEATURE-002-01 (schema constraints, referential integrity, severity levels, and pipeline integration).

## Definition of Done

- [ ] `QualityRule` sealed trait and all 5 implementations (`NullabilityRule`, `RangeRule`, `PatternRule`, `EnumRule`, `CustomExpressionRule`) compile and function
- [ ] Each rule type produces a `RuleValidationResult` with accurate `violatingRowCount` and `totalRowCount`
- [ ] Schema validation raises `AnalysisException` for missing columns at rule execution time
- [ ] `PatternRule` validates regex syntax at construction and raises `IllegalArgumentException` for invalid patterns
- [ ] `EnumRule` validates non-empty allowed values set at construction and raises `IllegalArgumentException` for empty sets
- [ ] `QualityRuleSet` executes all contained rules against a DataFrame and returns `Seq[RuleValidationResult]`
- [ ] DataFrame extension method `validateQuality` is available on DataFrame instances and returns validation results
- [ ] Unit tests cover all 8 acceptance criteria scenarios
- [ ] Edge case tests cover empty DataFrames, null values in range checks, boundary values, empty enum sets, and type mismatches
- [ ] API documentation includes examples for all 5 rule types in Scala and Python
- [ ] INVEST compliance verified:
  - **Independent:** Foundational story with no story-level dependencies within FEATURE-002-01; provides the base types consumed by STORY-002-01-02 through STORY-002-01-05
  - **Negotiable:** Custom expression API scope is adjustable (Column-only vs. SQL string expressions); inclusivity flag on `RangeRule` is a negotiable enhancement
  - **Valuable:** Provides the core quality rule engine enabling declarative quality checks that replace per-pipeline custom validation logic
  - **Estimable:** 8 story points based on the defined scope of sealed trait, 5 case classes, validation result model, execution engine, and extension method
  - **Sized:** Completable within one sprint given the well-bounded implementation scope and clear rule behavior contracts
  - **Testable:** All 8 acceptance criteria have concrete Given/When/Then BDD assertions with specific input data, expected violation counts, and exception types
