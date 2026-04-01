# Configure Rule Severity Levels for Quality Validation Rules to Control Pipeline Behavior on Data Quality Violations

## User Story

**As a** data platform administrator,
**I want to** configure severity levels for each quality validation rule, choosing from three levels — ERROR (halts pipeline execution and raises a `QualityValidationException`), WARNING (logs the violation with rule name and violation count to the Spark application log and continues execution), and INFO (records violation metadata in the validation result without logging or halting) — and set severity levels both programmatically via the rule API and through Spark configuration properties (`spark.quality.rule.<ruleName>.severity`),
**So that** the data platform team can control how aggressively quality violations impact pipeline execution, allowing critical rules (e.g., primary key nullability) to halt processing while informational rules (e.g., optional field completeness) record metadata without disruption, reducing false-positive pipeline failures by 40–60% through graduated severity configuration, and enabling environment-specific severity overrides (stricter in production, lenient in development).

### Technical Context

- Spark configuration properties are set via `SparkConf`, `--conf` flags, or `spark-defaults.conf` with a defined precedence order: properties set on `SparkConf` take the highest precedence, then those passed through `--conf` flags or `--properties-file`, then options in `spark-defaults.conf` (`Source: docs/configuration.md`)
- The `spark.quality.rule.<ruleName>.severity` property follows the Spark namespace convention pattern used by `spark.sql.*`, `spark.streaming.*`, and `spark.dynamicAllocation.*` (`Source: docs/configuration.md`)
- Column expressions from the Column API (`Source: sql/api/src/main/scala/org/apache/spark/sql/Column.scala`) provide the predicate foundation for quality rules that severity levels are applied to — including `isNull`, `isNotNull`, comparison operators, `rlike`, and `isin`
- Existing null-handling patterns in `DataFrameNaFunctions` (`Source: sql/api/src/main/scala/org/apache/spark/sql/DataFrameNaFunctions.scala`) inform the quality API design by demonstrating DataFrame-level data cleansing operations (`drop`, `fill`, `replace`)
- No existing severity level mechanism exists in the current Spark quality/validation codebase — this is new functionality
- The ERROR severity must integrate with Spark's exception handling by raising `QualityValidationException` (a new exception class extending `SparkException`) that pipeline callers can catch
- WARNING severity uses Spark's logging infrastructure (Log4j 2.24.3 via the `Logging` trait)

## Acceptance Criteria

### AC-1: Input Validation — Invalid Severity Value

- **Given** a quality rule with severity set to an invalid string value "CRITICAL"
- **When** the rule is configured
- **Then** the system raises an `IllegalArgumentException` with a message listing the valid severity values: ERROR, WARNING, INFO

### AC-2: Expected Output — ERROR Severity Halts Pipeline

- **Given** a quality rule "pk_not_null" with severity set to ERROR
- **When** the rule is executed against a DataFrame containing 5 rows that violate the rule
- **Then** the system raises a `QualityValidationException` with the message containing rule name "pk_not_null", violation count 5, and the target column name, and pipeline execution halts at the point of the violation

### AC-3: Expected Output — WARNING Severity Logs and Continues

- **Given** a quality rule "email_format" with severity set to WARNING
- **When** the rule is executed against a DataFrame containing 20 rows that violate the rule
- **Then** the system logs a warning message containing rule name "email_format", violation count 20, and the target column name to the Spark application log, includes the violation in the validation result, and continues pipeline execution without interruption

### AC-4: Expected Output — INFO Severity Records Metadata Silently

- **Given** a quality rule "optional_field_completeness" with severity set to INFO
- **When** the rule is executed against a DataFrame containing 100 rows that violate the rule
- **Then** the system records the violation metadata (rule name, violation count, target column) in the validation result without logging a warning or raising an exception, and pipeline execution continues

### AC-5: Error Handling — Missing Severity Configuration Applies Default

- **Given** a quality rule "age_range" with no severity explicitly configured and no `spark.quality.rule.age_range.severity` property set
- **When** the rule is executed
- **Then** the system applies the default severity level of ERROR

### AC-6: Configuration Override — SparkConf Takes Precedence

- **Given** a quality rule "name_not_null" with severity set to WARNING programmatically and the Spark configuration property `spark.quality.rule.name_not_null.severity` set to ERROR
- **When** the rule is executed
- **Then** the Spark configuration property value (ERROR) takes precedence over the programmatic setting (WARNING), and the system raises a `QualityValidationException`

### AC-7: Edge Case — Global Default Severity

- **Given** the Spark configuration property `spark.quality.defaultSeverity` set to WARNING and a quality rule with no explicit severity
- **When** the rule is executed and violations are detected
- **Then** the system applies WARNING severity (logs and continues) instead of the built-in default of ERROR

### AC-8: Multiple Rules with Mixed Severity

- **Given** three quality rules — rule_a with ERROR severity, rule_b with WARNING severity, and rule_c with INFO severity — applied to the same DataFrame
- **When** all three rules detect violations
- **Then** rule_a causes a `QualityValidationException` to be raised, while rule_b and rule_c results are included in the validation result metadata (rule_b with WARNING logged, rule_c with INFO recorded), and the exception message from rule_a includes all three rule results for complete diagnostic context

## Sub-Tasks

- [ ] Define `QualitySeverity` enum with three values: ERROR, WARNING, INFO, each with a clear behavioral contract
- [ ] Create `QualityValidationException` class extending `SparkException` with fields: `ruleName`, `targetColumn`, `violationCount`, `totalRowCount`, `allRuleResults` (for multi-rule context)
- [ ] Add `severity` field (default: ERROR) to the `QualityRule` sealed trait and all implementing rule types (`NullabilityRule`, `RangeRule`, `PatternRule`, `EnumRule`, `CustomExpressionRule`, `SchemaConstraint`, `ReferentialIntegrityRule`)
- [ ] Implement Spark configuration property resolution: read `spark.quality.rule.<ruleName>.severity` from `SparkConf` and `spark.quality.defaultSeverity` for global default
- [ ] Implement severity precedence logic: SparkConf property > programmatic setting > global default > built-in default (ERROR)
- [ ] Implement ERROR behavior: collect all rule results first, then raise `QualityValidationException` if any ERROR-severity rule has violations, including all rule results in the exception for diagnostic context
- [ ] Implement WARNING behavior: log violation using Spark's `Logging` trait at WARN level with structured message containing rule name, column, violation count
- [ ] Implement INFO behavior: record violation metadata in `RuleValidationResult` without logging or exception
- [ ] Add severity field to `RuleValidationResult` for downstream processing
- [ ] Add unit tests for each severity level behavior (ERROR throws, WARNING logs, INFO records)
- [ ] Add unit tests for configuration precedence (programmatic < SparkConf property)
- [ ] Add unit tests for default severity (global default and built-in default)
- [ ] Write API documentation covering severity levels, configuration properties, and precedence rules

## Edge Cases

### EC-1: Empty DataFrame — No Violations Detected

A quality rule with severity set to ERROR is executed against an empty DataFrame (zero rows). No violations are detected, no exception is raised, and the validation result reports `status=PASS` with `violating_row_count=0` and `total_row_count=0`.

### EC-2: Null Severity Value — Rejected at Configuration Time

A quality rule is configured with severity set to `null` programmatically. The system raises an `IllegalArgumentException` with a message indicating that severity must be one of ERROR, WARNING, or INFO.

### EC-3: All Rules Pass — No Side Effects

A set of 10 rules with mixed severity levels (ERROR, WARNING, INFO) is executed against a DataFrame where all rows conform to all rules. No exceptions are raised, no warnings are logged, and all validation results report `status=PASS`.

### EC-4: Case-Insensitive Severity Configuration

A quality rule severity is configured as `"error"` (lowercase) via Spark configuration property. The system accepts case-insensitive severity values and interprets `"error"` as ERROR.

### EC-5: All ERROR Rules Fail Simultaneously

Five quality rules all configured with ERROR severity detect violations simultaneously. The `QualityValidationException` contains all five violation results in its message, not just the first one detected, providing complete diagnostic context.

## Dependencies

- **STORY-002-01-01** — Define Column Validation Rules: provides the `QualityRule` sealed trait and implementing rule types (`NullabilityRule`, `RangeRule`, `PatternRule`, `EnumRule`, `CustomExpressionRule`) that severity levels are applied to (`Source: tickets/EPIC-002/FEATURE-002-01/STORY-002-01-01-define-column-validation-rules.md`)
- **EPIC-002** — Declarative Data Quality Validation Framework: parent epic providing architectural context and overall scope for native data quality capabilities (`Source: tickets/EPIC-002-declarative-data-quality-framework.md`)
- **FEATURE-002-01** — DataFrame Quality Rules Engine: parent feature defining the rules engine, severity levels, and pipeline integration scope (`Source: tickets/EPIC-002/FEATURE-002-01-dataframe-quality-rules-engine.md`)
- **Spark Configuration** — Configuration property patterns, precedence rules (SparkConf > `--conf` > `spark-defaults.conf`), and namespace conventions (`spark.sql.*`, `spark.streaming.*`, `spark.dynamicAllocation.*`) (`Source: docs/configuration.md`)
- **Spark Logging** — Log4j 2.24.3 via the `Logging` trait for WARNING-level log output
- **SparkConf** — Configuration property access for severity overrides (`Source: core/src/main/scala/org/apache/spark/SparkConf.scala`)

## Story Estimation Guidance

- **Story Points:** 5 (Fibonacci scale)
- **Rationale:** The `QualitySeverity` enum and behavioral contracts are straightforward, but the configuration property resolution with precedence logic (SparkConf > programmatic > global default > built-in default), the `QualityValidationException` design with multi-rule diagnostic context, and the integration with Spark's `Logging` trait add moderate complexity. Testing the multi-rule mixed-severity scenario and configuration override precedence requires careful test setup. The scope is contained within a single sprint given the well-defined severity behavior contracts.

## Definition of Done

- [ ] `QualitySeverity` enum defines ERROR, WARNING, and INFO with clear behavioral contracts
- [ ] `QualityValidationException` extends `SparkException` with rule name, target column, violation count, total row count, and multi-rule context (`allRuleResults`)
- [ ] All `QualityRule` types accept a configurable `severity` parameter (default: ERROR)
- [ ] Configuration resolution reads `spark.quality.rule.<ruleName>.severity` and `spark.quality.defaultSeverity` from `SparkConf`
- [ ] Precedence enforced: SparkConf property > programmatic setting > global default > built-in ERROR default
- [ ] ERROR severity raises `QualityValidationException` on violation
- [ ] WARNING severity logs to Spark application log at WARN level on violation
- [ ] INFO severity records violation metadata without logging or exception
- [ ] Multi-rule execution collects all results before raising exception (ERROR rules include complete diagnostic context from all evaluated rules)
- [ ] Unit tests cover all 8 acceptance criteria scenarios
- [ ] Edge case tests cover empty DataFrames, null severity, case-insensitive configuration, all-rules-pass scenario, and simultaneous ERROR violations
- [ ] API documentation covers severity levels, configuration properties, and precedence rules with Scala and Python examples
- [ ] INVEST compliance verified:
  - **Independent:** Requires only the basic rule API from STORY-002-01-01; no other story-level dependencies
  - **Negotiable:** Configuration property namespace (`spark.quality.*`) is adjustable; additional severity levels can be added in future iterations
  - **Valuable:** Graduated severity reduces false-positive pipeline failures by 40–60% and enables environment-specific quality enforcement
  - **Estimable:** 5 story points based on defined scope of enum, exception class, configuration resolution, and severity behavior
  - **Sized:** Completable within one sprint given the well-bounded implementation scope
  - **Testable:** All 8 acceptance criteria have concrete Given/When/Then BDD assertions with specific values
