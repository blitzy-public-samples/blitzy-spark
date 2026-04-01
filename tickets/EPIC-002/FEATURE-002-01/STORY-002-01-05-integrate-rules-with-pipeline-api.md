# Integrate Quality Validation Rules with ML Pipeline API to Enable Inline Data Quality Gates in Training Workflows

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

**As a** data scientist,
**I want to** integrate quality validation rules as a `QualityValidationStage` that implements the `PipelineStage` interface (`Source: mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`), allowing the stage to be inserted at any position in an ML Pipeline and execute configured data quality rules during `Pipeline.fit()` and `PipelineModel.transform()` calls,
**So that** data quality is validated inline during ML pipeline execution without requiring separate pre-processing scripts, reducing pipeline failure debugging time by catching data anomalies before they reach training or inference stages, and ensuring model training data meets declared quality standards before compute-intensive Estimator stages run.

### Technical Context

- `PipelineStage` (line 42 of Pipeline.scala) is an abstract class extending `Params` with `Logging`, requiring `transformSchema(schema: StructType): StructType` and `copy(extra: ParamMap): PipelineStage` (`Source: mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`)
- `Pipeline` (line 94) stores stages as `Param[Array[PipelineStage]]` and iterates them sequentially in `fit()` (line 133), calling `Estimator.fit` or `Transformer.transform` (`Source: mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`)
- `QualityValidationStage` must extend `Transformer` (since it validates data without changing the schema) and implement `MLWritable` for pipeline persistence via `SharedReadWrite.saveImpl` (line 225+) (`Source: mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`)
- The stage must validate schema via `transformSchema` and apply quality rules via `transform(dataset: Dataset[_]): DataFrame`, returning the input DataFrame unmodified when all rules pass
- `Pipeline.copy(extra: ParamMap)` (line 173) copies each stage; the quality stage must implement `copy` to preserve rule configurations (`Source: mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`)
- Column expressions from the Column API (`Source: sql/api/src/main/scala/org/apache/spark/sql/Column.scala`) provide predicates for quality rules: `isNull`, `isNotNull` (nullability), `>=`, `<=` (range), `rlike` (regex matching), `isin` (enum), and arbitrary boolean expressions
- Existing null-handling patterns in `DataFrameNaFunctions` (`Source: sql/api/src/main/scala/org/apache/spark/sql/DataFrameNaFunctions.scala`) demonstrate DataFrame-level data cleansing operations (`drop`, `fill`, `replace`) that inform the quality validation design

## Acceptance Criteria

### AC-1: Input Validation — Schema Column Existence Check

- **Given** a `QualityValidationStage` configured with column validation rules referencing columns "age" and "email"
- **When** the stage's `transformSchema` method is invoked with a `StructType` that contains both "age" and "email" columns
- **Then** the stage returns the input schema unchanged without raising an exception

### AC-2: Expected Output — Quality Rules Execute During Pipeline Fit

- **Given** a `QualityValidationStage` inserted into an ML Pipeline with two quality rules — one nullability check on column "name" and one range check on column "age" with min=0 and max=150
- **When** `Pipeline.fit()` is called on a DataFrame where all rows satisfy both rules
- **Then** the stage executes both quality rules against the DataFrame, reports zero violations for each rule, and passes the unmodified DataFrame to the next stage in the pipeline

### AC-3: Error Handling — ERROR Severity Halts Pipeline Execution

- **Given** a `QualityValidationStage` with an ERROR-severity nullability rule on column "id"
- **When** `Pipeline.fit()` is called and the DataFrame contains 15 rows with null values in column "id"
- **Then** the stage raises a `QualityValidationException` with a message listing the violated rule name, the failing column "id", and the count of 15 non-conforming rows, and pipeline execution halts before subsequent stages run

### AC-4: Edge Case — Empty DataFrame Passes Without Exception

- **Given** a `QualityValidationStage` configured with three quality rules (nullability, range, and pattern)
- **When** `Pipeline.fit()` is called with an empty DataFrame containing zero rows
- **Then** the stage completes without raising an exception and returns the empty DataFrame unchanged to the next pipeline stage

### AC-5: Pipeline Persistence — Save and Load Round-Trip

- **Given** an ML Pipeline containing a `QualityValidationStage` with 3 configured rules (one `NullabilityRule`, one `RangeRule`, and one `PatternRule`)
- **When** `Pipeline.write().save(path)` is called followed by `Pipeline.load(path)`
- **Then** the loaded pipeline contains a `QualityValidationStage` with the identical 3 rule configurations including rule names, target columns, severity levels, and rule-specific parameters (regex pattern, min/max bounds)

### AC-6: WARNING Severity — Log and Continue Execution

- **Given** a `QualityValidationStage` with a WARNING-severity range rule on column "score" with min=0 and max=100
- **When** `Pipeline.fit()` is called and the DataFrame contains 8 rows where "score" exceeds 100
- **Then** the stage logs a warning message to the Spark application log containing the rule name, violation count of 8, and column name "score", records the violation metadata in the validation result, and passes the DataFrame to the next pipeline stage without halting execution

### AC-7: Multiple Stages — Sequential Execution at Different Positions

- **Given** an ML Pipeline containing two `QualityValidationStage` instances — one positioned before a `VectorAssembler` feature transformer and one positioned after it
- **When** `Pipeline.fit()` is called on a valid DataFrame
- **Then** the first `QualityValidationStage` executes its quality rules on the raw DataFrame, the `VectorAssembler` transforms the data, and the second `QualityValidationStage` executes its rules on the transformed DataFrame, each at their configured position in the pipeline sequence

## Sub-Tasks

- [ ] Create `QualityValidationStage` class extending `Transformer` with `MLWritable` in the `org.apache.spark.ml.quality` package
- [ ] Implement `Param` fields for quality rule configurations (rule definitions, severity levels) using `Param[Array[QualityRule]]` following the Param pattern from `mllib/src/main/scala/org/apache/spark/ml/param/Params.scala`
- [ ] Implement `transformSchema(schema: StructType): StructType` to validate that all columns referenced in quality rules exist in the input schema, raising `AnalysisException` for missing columns
- [ ] Implement `transform(dataset: Dataset[_]): DataFrame` to execute quality rules against the dataset and handle violations per configured severity — ERROR: raise `QualityValidationException`, WARNING: log via the `Logging` trait and continue, INFO: record metadata and continue
- [ ] Implement `copy(extra: ParamMap): QualityValidationStage` to duplicate the stage with merged parameters, preserving all rule configurations
- [ ] Implement `MLWriter` and `MLReader` for persistence of quality rule configurations alongside pipeline save/load using `DefaultParamsWriter` and `DefaultParamsReader` patterns from Pipeline.scala
- [ ] Add unit tests for `QualityValidationStage` in isolation covering rule execution, schema validation, severity handling, and empty DataFrame behavior
- [ ] Add integration tests for `QualityValidationStage` within ML Pipeline covering multi-stage pipelines, persistence round-trip via `Pipeline.write().save()` and `Pipeline.load()`, and mixed Estimator/Transformer/QualityValidation stage ordering
- [ ] Add PySpark bindings for `QualityValidationStage` so data scientists can use it from Python with the same API surface
- [ ] Write API documentation for `QualityValidationStage` with usage examples in Scala and Python demonstrating single-stage and multi-stage pipeline configurations

## Edge Cases

### EC-1: Empty/Null Input — Empty DataFrame Passes All Rules

A `QualityValidationStage` is configured with rules but receives an empty DataFrame containing zero rows. The stage must complete without raising an exception and return the empty DataFrame unchanged, with all rule validation results reporting `status=PASS`, `violatingRowCount=0`, and `totalRowCount=0`.

### EC-2: Null Rule Configuration — Rejected at Construction

A `QualityValidationStage` is added to a pipeline without any quality rules configured (null or empty rule array). The stage must raise an `IllegalArgumentException` during `transformSchema` invocation with a message indicating that at least one quality rule must be configured before the stage can execute.

### EC-3: Boundary Value — Maximum Rules Executed Without Failure

A `QualityValidationStage` is configured with 100 quality rules covering nullability, range, pattern, and enum types across 50 columns. All 100 rules must execute within the `transform` call without timeout or stack overflow, and the validation result must contain exactly 100 entries.

### EC-4: Invalid Input — Column Not Found in Schema

A quality rule within a `QualityValidationStage` references a column name "nonexistent_col" that does not exist in the input DataFrame's schema. The `transformSchema` method must raise an `AnalysisException` with a message identifying "nonexistent_col" as the missing column name, preventing pipeline execution before any data processing occurs.

### EC-5: Streaming DataFrame — Per-Micro-Batch Execution

A `QualityValidationStage` is used in a pipeline processing a streaming DataFrame. The stage must execute quality rules on each micro-batch independently without persisting validation state across batches, and each micro-batch receives its own validation result.

## Dependencies

- **STORY-002-01-01** — Define Column Validation Rules: provides the `QualityRule` sealed trait and all implementing rule types (`NullabilityRule`, `RangeRule`, `PatternRule`, `EnumRule`, `CustomExpressionRule`) and the `QualityRuleSet` execution model consumed by `QualityValidationStage` (`Source: tickets/EPIC-002/FEATURE-002-01/STORY-002-01-01-define-column-validation-rules.md`)
- **STORY-002-01-04** — Configure Rule Severity Levels: provides the `QualitySeverity` enum (ERROR, WARNING, INFO), `QualityValidationException`, and severity-based violation handling behavior used by the stage to determine whether to halt, log, or record violations (`Source: tickets/EPIC-002/FEATURE-002-01/STORY-002-01-04-configure-rule-severity-levels.md`)
- **F-004 — Machine Learning Pipelines (MLlib)**: Pipeline API, `PipelineStage`, `Transformer`, `Estimator`, `MLWritable`, `MLReader` interfaces that `QualityValidationStage` implements and integrates with (`Source: mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`)
- **ML Pipeline Persistence**: `SharedReadWrite`, `DefaultParamsWriter`, `DefaultParamsReader` for stage save/load round-trip within pipeline serialization (`Source: mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala` — lines 200–238)
- **ML Params**: Parameter definitions and validation framework for `Param[Array[QualityRule]]` configuration fields (`Source: mllib/src/main/scala/org/apache/spark/ml/param/Params.scala`)
- **DataFrame API**: DataFrame operations (`filter`, `count`, `schema`) for quality rule execution against datasets (`Source: sql/api/src/main/scala/org/apache/spark/sql/Dataset.scala`)
- **FEATURE-002-01** — DataFrame Quality Rules Engine: parent feature defining the rules engine scope including pipeline integration as STORY-002-01-05 (`Source: tickets/EPIC-002/FEATURE-002-01-dataframe-quality-rules-engine.md`)
- **EPIC-002** — Declarative Data Quality Validation Framework: parent epic providing architectural context and overall scope for native data quality capabilities (`Source: tickets/EPIC-002-declarative-data-quality-framework.md`)

## Story Estimation Guidance

- **Story Points:** 8 (Fibonacci scale)
- **Rationale:** Implementing a new `PipelineStage` requires creating a `Transformer` subclass with schema validation (`transformSchema`), rule execution (`transform`), severity-based violation handling (ERROR throws `QualityValidationException`, WARNING logs, INFO records), copy semantics (`copy` with `ParamMap` merging), and persistence via `MLWriter`/`MLReader`. The integration with the existing Pipeline framework's sequential execution model (stages iterated in `Pipeline.fit` at line 133), the persistence round-trip testing through `SharedReadWrite` (line 225+), and the PySpark binding requirement increase complexity beyond a single-component implementation. The story is completable within one sprint given clear interface contracts from `PipelineStage`, `Transformer`, and `MLWritable`.

## Definition of Done

- [ ] `QualityValidationStage` class compiles and extends `Transformer` with `MLWritable` in the `org.apache.spark.ml.quality` package
- [ ] `transformSchema` validates that all columns referenced in configured quality rules exist in the input schema and returns the schema unchanged
- [ ] `transform` executes configured quality rules with severity-based handling: ERROR raises `QualityValidationException`, WARNING logs violation details via the Spark `Logging` trait, INFO records violation metadata in the validation result
- [ ] `copy` returns a new `QualityValidationStage` with merged parameters preserving all rule configurations
- [ ] `Pipeline.write().save(path)` and `Pipeline.load(path)` round-trip preserves the `QualityValidationStage` and its complete rule configurations (rule names, target columns, severity levels, rule-specific parameters)
- [ ] `QualityValidationStage` executes at any position within a multi-stage ML Pipeline alongside Estimators and Transformers
- [ ] Unit tests cover all 7 acceptance criteria scenarios including empty DataFrames, missing columns, ERROR/WARNING severity handling, and persistence round-trip
- [ ] Integration tests validate end-to-end pipeline execution with `QualityValidationStage` in multi-stage pipelines containing Estimators and Transformers
- [ ] PySpark API exposes `QualityValidationStage` with identical configuration and execution functionality
- [ ] API documentation includes Scala and Python usage examples demonstrating single-stage and multi-stage pipeline configurations
- [ ] INVEST compliance verified:
  - **Independent:** Can be implemented after the rule definition API (STORY-002-01-01) and severity configuration (STORY-002-01-04) are complete; does not block other stories in FEATURE-002-01
  - **Negotiable:** PySpark bindings scope is adjustable (Scala-first with Python following); streaming DataFrame support depth is negotiable
  - **Valuable:** Inline quality gates in ML pipelines eliminate separate pre-processing scripts and catch data anomalies before compute-intensive training stages
  - **Estimable:** 8 story points based on `Transformer` subclass implementation, persistence integration, severity handling, and PySpark bindings
  - **Sized:** Completable within one sprint given well-defined interface contracts from `PipelineStage`, `Transformer`, and `MLWritable`
  - **Testable:** All 7 acceptance criteria have concrete Given/When/Then BDD assertions with specific input data, expected behaviors, and verifiable outcomes
