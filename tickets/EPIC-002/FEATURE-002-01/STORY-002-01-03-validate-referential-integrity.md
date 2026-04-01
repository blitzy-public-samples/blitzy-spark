# Validate Referential Integrity Across DataFrames to Detect Orphaned Records and Foreign Key Violations

## User Story

**As a** data engineer,
**I want to** define referential integrity rules that validate foreign key relationships between two DataFrames by specifying a child DataFrame column (or composite key columns), a parent DataFrame, and a parent key column (or composite key columns), then execute validation to identify orphaned records in the child DataFrame — rows whose foreign key values do not match any key in the parent DataFrame — and receive a validation result containing the orphaned record count, the distinct orphaned foreign key values, a separate count of null-key records, a pass/fail status, and the ability to retrieve the full orphaned rows as a DataFrame,
**So that** data pipeline joins do not silently drop or misrepresent records due to undetected referential integrity violations, reducing data loss incidents caused by orphaned records by providing explicit detection before downstream aggregations or writes, and enabling the data engineer to enforce cross-table data contracts within the Spark processing pipeline without relying on external data quality tools.

### Technical Context

- The Spark SQL join infrastructure in `sql/core/` provides the execution mechanism for cross-DataFrame comparisons; a left anti join (`childDF.join(parentDF, childDF("fk_col") === parentDF("pk_col"), "left_anti")`) isolates orphaned rows whose foreign key values have no matching parent key (`Source: sql/core/`)
- The Catalyst Analyzer (`Source: sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala`) resolves relation and attribute references required for cross-DataFrame operations, validating that column names exist in their respective DataFrame schemas before query execution
- Column expressions from the Column API (`Source: sql/api/src/main/scala/org/apache/spark/sql/Column.scala`) — including `===` (equality), `isNull`, `isNotNull`, and `&&` (logical AND) — are used to build join predicates for single-column and composite-key referential integrity checks
- Null foreign key handling requires filtering null values before the left anti join, since null === null evaluates to null (not true) in SQL semantics, which would cause null-key rows to appear as false orphans

## Acceptance Criteria

### AC-1: Input Validation — Missing Column Raises AnalysisException

- **Given** a referential integrity rule specifying a child column "order_customer_id" on an orders DataFrame and a parent DataFrame with key column "customer_id"
- **When** the rule is created and the orders DataFrame schema does not contain a column named "order_customer_id"
- **Then** the system raises an `AnalysisException` with a message identifying "order_customer_id" as a missing column in the child DataFrame schema, and no validation is executed

### AC-2: Expected Output — No Violations Detected

- **Given** an orders DataFrame with a foreign key column "customer_id" referencing a customers DataFrame with primary key "id", where every order customer_id value exists in the customers id column
- **When** the referential integrity rule is executed
- **Then** the validation result reports orphanedCount=0, nullKeyCount=0, and passStatus=true

### AC-3: Expected Output — Orphaned Records Detected

- **Given** an orders DataFrame containing 1000 rows with a foreign key column "customer_id" referencing a customers DataFrame, where 50 order rows contain customer_id values not present in the customers DataFrame
- **When** the referential integrity rule is executed
- **Then** the validation result reports orphanedCount=50, passStatus=false, and includes the distinct orphaned foreign key values in the result metadata

### AC-4: Error Handling — Incompatible Column Types

- **Given** a referential integrity rule where the child column type is StringType and the parent key column type is IntegerType
- **When** the rule is validated at creation time
- **Then** the system raises a `QualityRuleException` with a message stating that the child column type (StringType) and parent key column type (IntegerType) are incompatible for referential integrity comparison

### AC-5: Edge Case — Null Foreign Key Values Reported Separately

- **Given** a child DataFrame containing 200 rows where 30 rows have null values in the foreign key column and 170 rows have non-null foreign key values
- **When** the referential integrity rule is executed
- **Then** rows with null foreign key values are reported separately as null-key records with nullKeyCount=30, and only the 170 non-null rows are evaluated against the parent DataFrame for orphaned record detection

### AC-6: Composite Key — Multi-Column Foreign Key Validation

- **Given** a referential integrity rule defined with a composite foreign key consisting of two columns ("region_id", "product_id") on a sales DataFrame, referencing a parent products DataFrame with composite key ("region_id", "product_id")
- **When** the rule is executed
- **Then** the validation matches on both columns simultaneously using a compound join predicate (`salesDF("region_id") === productsDF("region_id") && salesDF("product_id") === productsDF("product_id")`) and reports orphaned records where the combined key pair is not found in the parent DataFrame

### AC-7: Orphaned Row Retrieval — Full Row Access

- **Given** a referential integrity rule that has been executed and detected 50 orphaned records in the child DataFrame
- **When** the data engineer calls `getOrphanedRows()` on the validation result
- **Then** the method returns a DataFrame containing all 50 orphaned rows from the child DataFrame with all original columns preserved and no additional columns added

## Sub-Tasks

- [ ] Define `ReferentialIntegrityRule` class with fields: `childColumn` (String or Seq[String] for composite keys), `parentDataFrame` (DataFrame), `parentKeyColumn` (String or Seq[String] for composite keys), and optional `severity` level (from STORY-002-01-04)
- [ ] Implement schema validation at rule creation time to verify that each specified child column exists in the child DataFrame schema and each specified parent key column exists in the parent DataFrame schema, raising `AnalysisException` for missing columns
- [ ] Implement type compatibility validation at rule creation time to verify that the data type of each child column matches the data type of the corresponding parent key column, raising `QualityRuleException` for type mismatches
- [ ] Implement the referential integrity check using left anti join semantics (`childDF.join(parentDF, joinCondition, "left_anti")`) to identify orphaned records in the child DataFrame (`Source: sql/core/`)
- [ ] Implement composite key support by constructing compound join predicates using `&&` to chain multiple column equality conditions for multi-column foreign key relationships (`Source: sql/api/src/main/scala/org/apache/spark/sql/Column.scala`)
- [ ] Implement null foreign key handling: before executing the left anti join, filter rows where any foreign key column is null using `isNull` (`Source: sql/api/src/main/scala/org/apache/spark/sql/Column.scala`), count them separately as null-key records, and execute the orphan detection only on non-null rows
- [ ] Create `ReferentialIntegrityResult` class with fields: `orphanedCount` (Long), `orphanedKeys` (DataFrame of distinct orphaned key values), `nullKeyCount` (Long), `passStatus` (Boolean — true when orphanedCount is 0)
- [ ] Implement `getOrphanedRows()` method on `ReferentialIntegrityResult` that returns a DataFrame containing all orphaned rows from the child DataFrame with all original columns preserved
- [ ] Integrate `ReferentialIntegrityRule` with the `QualityRule` trait defined in STORY-002-01-01 and the severity system defined in STORY-002-01-04
- [ ] Add unit tests for single-column referential integrity checks covering pass, fail, and mixed scenarios
- [ ] Add unit tests for composite-key referential integrity checks with two-column and three-column keys
- [ ] Add unit tests for null foreign key handling, type incompatibility detection, missing column detection, and empty DataFrame scenarios
- [ ] Write API documentation with examples for single-key and composite-key referential integrity validation in Scala and Python

## Edge Cases

### EC-1: Empty Child DataFrame — Zero Rows Validated

The child DataFrame is empty (zero rows). Validation must return passStatus=true, orphanedCount=0, and nullKeyCount=0 without raising an exception. The `getOrphanedRows()` method must return an empty DataFrame with the child DataFrame's schema preserved.

### EC-2: Empty Parent DataFrame — All Child Rows Orphaned

The parent DataFrame is empty (zero rows) and the child DataFrame contains 100 rows with non-null foreign key values. All 100 child rows must be reported as orphaned records (orphanedCount=100), and `getOrphanedRows()` must return all 100 rows.

### EC-3: Boundary Value — Large DataFrames with Distributed Execution

Both child and parent DataFrames contain 10 million rows. Referential integrity validation must complete using the distributed join execution engine without collecting data to the driver, relying on Spark's shuffle-based join implementation to handle the cross-DataFrame comparison at scale.

### EC-4: Invalid Input — Duplicate Parent Keys Do Not Inflate Results

The parent DataFrame contains duplicate values in the primary key column (e.g., customer_id 42 appears 3 times). Validation must not produce duplicate orphaned records in the result — each orphaned child row is reported once regardless of parent key duplication, because the left anti join returns child rows with no match rather than multiplying them.

### EC-5: All Null Foreign Keys — Zero Orphaned Records

Every row in the child DataFrame has a null value in the foreign key column. Validation must report all rows as null-key records (nullKeyCount equals the total row count), orphanedCount=0, and passStatus=true (since no non-null foreign key values failed the integrity check).

## Dependencies

- **STORY-002-01-01** — Define Column Validation Rules: provides the foundational `QualityRule` sealed trait, `RuleValidationResult` model, and `QualityRuleSet` execution engine that `ReferentialIntegrityRule` extends and integrates with (`Source: tickets/EPIC-002/FEATURE-002-01/STORY-002-01-01-define-column-validation-rules.md`)
- **STORY-002-01-04** — Configure Rule Severity Levels: provides the `QualitySeverity` enum (ERROR, WARNING, INFO) and severity configuration mechanism that `ReferentialIntegrityRule` uses to control pipeline behavior when violations are detected (`Source: tickets/EPIC-002/FEATURE-002-01/STORY-002-01-04-configure-rule-severity-levels.md`)
- **FEATURE-002-01** — DataFrame Quality Rules Engine: parent feature defining the rules engine scope, including referential integrity validation as a core capability (`Source: tickets/EPIC-002/FEATURE-002-01-dataframe-quality-rules-engine.md`)
- **EPIC-002** — Declarative Data Quality Validation Framework: parent epic providing architectural context and overall scope for native data quality capabilities (`Source: tickets/EPIC-002-declarative-data-quality-framework.md`)
- **F-002 — SQL Query Processing Engine**: DataFrame join operations (left anti join) and Catalyst optimizer for distributed query execution
- **DataFrame Join API**: `sql/core/` — left anti join execution for orphan detection (`Source: sql/core/`)
- **Catalyst Analyzer**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala` — schema resolution and attribute reference validation for cross-DataFrame column references (`Source: sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala`)
- **Column API**: `sql/api/src/main/scala/org/apache/spark/sql/Column.scala` — column expressions for join predicates (`===`, `&&`, `isNull`, `isNotNull`) (`Source: sql/api/src/main/scala/org/apache/spark/sql/Column.scala`)

## Story Estimation Guidance

- **Story Points:** 5 (Fibonacci scale)
- **Rationale:** The core implementation leverages Spark's existing join infrastructure (left anti join), which handles the heavy lifting of distributed orphan detection. Complexity arises from four areas: (1) composite key support requiring compound join predicate construction, (2) null foreign key handling requiring pre-join filtering and separate counting, (3) type compatibility validation between child and parent columns at rule creation time, and (4) the `getOrphanedRows()` result API that must preserve all original child DataFrame columns. Schema validation for cross-DataFrame operations adds moderate complexity. The scope is contained within a single sprint given the well-defined validation behavior and reliance on existing join semantics.

## Definition of Done

- [ ] `ReferentialIntegrityRule` supports single-column and composite-key foreign key definitions with String and Seq[String] column specifications
- [ ] Schema validation checks column existence in both child and parent DataFrames and raises `AnalysisException` for missing columns before rule execution
- [ ] Type compatibility validation checks that child column types match parent key column types and raises `QualityRuleException` for incompatible types
- [ ] Orphaned record detection uses left anti join and returns exact orphanedCount matching the number of child rows with no parent key match
- [ ] Null foreign key values are filtered before orphan detection and reported separately via nullKeyCount, distinct from orphaned non-null records
- [ ] `getOrphanedRows()` returns a DataFrame containing all orphaned rows from the child DataFrame with all original columns preserved
- [ ] `ReferentialIntegrityResult` includes orphanedCount, nullKeyCount, orphanedKeys (distinct values), passStatus, and getOrphanedRows() access
- [ ] Composite key validation constructs compound join predicates and matches on all specified columns simultaneously
- [ ] Unit tests cover all 7 acceptance criteria scenarios with concrete assertions
- [ ] Edge case tests cover empty child DataFrame, empty parent DataFrame, large DataFrames (distributed execution), duplicate parent keys, and all-null foreign keys
- [ ] API documentation includes examples for single-key and composite-key referential integrity checks in Scala and Python
- [ ] INVEST compliance verified:
  - **Independent:** Can be developed after the basic rule API (STORY-002-01-01) is in place; does not block other stories in FEATURE-002-01
  - **Negotiable:** Composite key scope is adjustable (single-column only for MVP, composite as enhancement); the `getOrphanedRows()` lazy evaluation strategy is negotiable
  - **Valuable:** Prevents silent data loss from join mismatches by detecting orphaned records before downstream processing, enabling cross-table data contract enforcement within Spark pipelines
  - **Estimable:** 5 story points based on defined scope of rule class, schema/type validation, left anti join execution, null handling, and result API
  - **Sized:** Completable within one sprint given the well-bounded implementation scope and reliance on existing Spark join infrastructure
  - **Testable:** All 7 acceptance criteria have concrete Given/When/Then BDD assertions with specific input data, expected counts, and exception types
