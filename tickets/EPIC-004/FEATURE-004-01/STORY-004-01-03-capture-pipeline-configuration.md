# Capture Complete Pipeline Stage Configuration as Serialized JSON to Enable Experiment Reproducibility

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

**As a** ML engineer, **I want to** automatically snapshot and persist the complete pipeline configuration — including all PipelineStage instances, their class names, UIDs, and full parameter maps — as a serialized JSON document each time `Pipeline.fit()` executes, **so that** I can maintain a precise, machine-readable record of the exact pipeline structure and configuration used for each training run, enabling reproduction of any historical experiment by reconstructing the identical pipeline from the stored JSON without relying on version control archaeology or manual documentation — reducing experiment reproduction setup time from hours to minutes.

## Acceptance Criteria

### AC-1: Full Pipeline Configuration Snapshot (Expected Output)

- **Given** an ML engineer constructs a Pipeline with 3 stages (`StringIndexer` → `VectorAssembler` → `LogisticRegression`) and calls `Pipeline.fit()` on a training DataFrame
- **When** the `fit()` execution begins
- **Then** a JSON document is persisted to the KVStore containing an array of 3 stage objects, each with:
  - The fully qualified class name (e.g., `"org.apache.spark.ml.feature.StringIndexer"`)
  - The stage UID (matching the value from the `Identifiable` trait)
  - A `paramMap` object where each parameter name maps to its JSON-encoded value produced by `Param.jsonEncode()`
  - A `defaultParamMap` object containing the default parameter values

### AC-2: JSON Format Conformance with DefaultParamsWriter (Expected Output)

- **Given** a pipeline configuration snapshot is captured during `Pipeline.fit()`
- **When** the JSON document is inspected
- **Then** each stage entry conforms to the same JSON structure used by `DefaultParamsWriter.getMetadataToSave()` — containing keys `"class"`, `"timestamp"`, `"sparkVersion"`, `"uid"`, `"paramMap"`, and `"defaultParamMap"`
- **And** the `paramMap` values are encoded using `Param.jsonEncode(v)` for each `ParamPair` in the stage's `extractParamMap()` result

### AC-3: Empty Pipeline Handling (Input Validation)

- **Given** an ML engineer constructs a Pipeline with zero stages (empty stages array)
- **When** `Pipeline.fit()` is called on a DataFrame
- **Then** the pipeline configuration snapshot contains an empty stages array in the JSON document
- **And** the `totalStageCount` field is set to `0`
- **And** no error is thrown during snapshot capture

### AC-4: Non-Serializable Parameter Handling (Error Handling)

- **Given** a pipeline stage contains a custom `Param` type that does not override `jsonEncode()`, causing the base `Param.jsonEncode()` to throw `UnsupportedOperationException`
- **When** the configuration snapshot iterates through the stage's `extractParamMap().toSeq` and encounters the failing `ParamPair`
- **Then** the system records the stage with all serializable parameters intact
- **And** the non-serializable parameter value is replaced with a placeholder string `"serialization_failed"` in the JSON document
- **And** the snapshot capture continues without halting for the remaining stages

### AC-5: Nested Pipeline Recursive Capture (Edge Case)

- **Given** an ML engineer constructs a Pipeline containing another Pipeline as one of its stages (nested pipeline with inner stages `[Tokenizer, HashingTF]`)
- **When** `Pipeline.fit()` is called
- **Then** the configuration snapshot recursively captures the inner pipeline's stages and their parameters as a nested JSON structure within the outer pipeline's stage array
- **And** the inner pipeline's stage entries each contain `"class"`, `"timestamp"`, `"sparkVersion"`, `"uid"`, `"paramMap"`, and `"defaultParamMap"` keys

### AC-6: Run ID Association with Hyperparameter Logs

- **Given** an active experiment run was started by STORY-004-01-01's hyperparameter logging mechanism, producing a run ID stored in the `ThreadLocal`-based `ExperimentRunContext`
- **When** `Pipeline.fit()` captures the pipeline configuration
- **Then** the JSON configuration document is stored with the same run ID used for hyperparameter logging
- **And** a single unified record links the hyperparameters and the pipeline configuration for that run

### AC-7: Snapshot Timing Before Stage Execution

- **Given** a `Pipeline.fit()` call is in progress
- **When** the configuration snapshot is captured
- **Then** the snapshot reflects the parameter values at the start of `fit()` — before any stage `Estimator.fit()` or `Transformer.transform()` calls modify the dataset
- **And** the recorded configuration matches the initial training setup as defined by `Pipeline.getStages` at invocation time

## Sub-Tasks

- Define `PipelineConfigSnapshot` data class with fields: `runId` (`String`), `pipelineUid` (`String`), `pipelineClassName` (`String`), `sparkVersion` (`String`), `timestamp` (`Long`), `stages` (`Array[StageConfigSnapshot]`), `totalStageCount` (`Int`)
- Define `StageConfigSnapshot` data class with fields: `stageIndex` (`Int`), `className` (`String`), `uid` (`String`), `paramMap` (`String` — JSON), `defaultParamMap` (`String` — JSON)
- Implement `PipelineConfigCapture` module that hooks into `Pipeline.fit()` before stage execution begins, using the `FitStart` event window from `events.scala`
- Reuse `DefaultParamsWriter.getMetadataToSave()` logic (lines 514–543 of `ReadWrite.scala`) to serialize each `PipelineStage`'s metadata to JSON — specifically: use `Param.jsonEncode(v)` for each `ParamPair` in `extractParamMap()`, construct `JObject` with `class`, `timestamp`, `sparkVersion`, `uid`, `paramMap`, `defaultParamMap`
- Handle `Pipeline.getStages` returning a cloned array — iterate the clone to extract per-stage configuration without mutating the original `Param` value
- Implement nested pipeline detection: check if `stage.isInstanceOf[Pipeline]`, then recursively capture inner stages and their parameters
- Handle non-serializable params: wrap `Param.jsonEncode` invocations in `try/catch` blocks, substitute `"serialization_failed"` placeholder on failure
- Persist `PipelineConfigSnapshot` to KVStore using `write()` with `@KVIndex` annotation on `runId` as the natural key
- Ensure snapshot capture occurs in the `FitStart` event window (using `events.scala` `MLEvents` pattern) before any stage processing begins
- Expose retrieval API: `ExperimentTracker.getPipelineConfig(runId: String): Option[PipelineConfigSnapshot]`
- Implement PySpark API wrapper: `ExperimentTracker.get_pipeline_config(run_id)` returning a Python dictionary representation of the snapshot
- Write unit tests for single-stage pipeline configuration capture
- Write unit tests for multi-stage pipeline configuration capture (3+ stages)
- Write unit tests for nested pipeline recursive capture
- Write unit tests for empty pipeline (zero stages) snapshot
- Write unit test for non-serializable param handling with `"serialization_failed"` placeholder
- Write unit test for JSON format conformance with `DefaultParamsWriter.getMetadataToSave()` output structure
- Write integration test validating end-to-end snapshot capture during an actual `Pipeline.fit()` execution with a real `SparkSession`

## Edge Cases

### 1. Empty/Null Input

- Pipeline with `null` stages array or stages set to empty `Array[PipelineStage]()` — the snapshot captures an empty stages array and stores the configuration with `totalStageCount=0` without error
- `Pipeline.getStages` returns a zero-length cloned array — the JSON document contains `"stages": []` and all other top-level fields (`runId`, `pipelineUid`, `pipelineClassName`, `sparkVersion`, `timestamp`) are populated

### 2. Boundary Values

- Pipeline with 100 stages (maximum practical pipeline length) — the snapshot captures all 100 stage configurations in a single JSON document
- The JSON document size is logged for storage monitoring purposes
- KVStore `write()` succeeds for documents up to 10MB without truncation
- Each of the 100 stages contains its full `paramMap` and `defaultParamMap` encoded via `Param.jsonEncode()`

### 3. Invalid Input

- A `PipelineStage` with a corrupted UID (empty string or containing special characters) — the snapshot captures the UID as-is in the JSON since `uid` is an immutable `String` from `Identifiable.randomUID()` and does not fail validation
- The snapshot marks the stage with a warning annotation if the UID format does not match the expected `"prefix_XXXXXXXXXXXX"` pattern
- A `PipelineStage` whose `extractParamMap()` throws an exception — the snapshot captures the stage class name and UID but records the `paramMap` and `defaultParamMap` as `"extraction_failed"` placeholder strings

### 4. Concurrent Pipelines

- Two `Pipeline.fit()` calls execute simultaneously in the same `SparkSession` — each produces a separate snapshot with its own run ID and pipeline configuration
- Snapshots do not interfere with each other due to `ThreadLocal`-based `ExperimentRunContext` isolation
- Each snapshot's `timestamp` reflects its own `Pipeline.fit()` invocation time, not the other's

## Dependencies

- **STORY-004-01-01** (Log Hyperparameters Per Run) — experiment run context must exist for run ID association; the `ExperimentRunContext` `ThreadLocal` provides the active run ID
- **FEATURE-004-01** (Experiment Metadata Logging) — parent feature defining the overall metadata logging subsystem
- **EPIC-004** (Native ML Experiment Tracking and Model Registry) — parent epic providing the strategic vision for experiment tracking
- **Pipeline class** (`mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`) — `Pipeline`, `PipelineStage`, `PipelineModel`; `stages: Param[Array[PipelineStage]]`; `getStages` (returns cloned array); `fit()` iterating stages sequentially with `instr.withFitEvent()`
- **DefaultParamsWriter** (`mllib/src/main/scala/org/apache/spark/ml/util/ReadWrite.scala`) — `getMetadataToSave()` JSON serialization logic producing keys: `class`, `timestamp`, `sparkVersion`, `uid`, `paramMap`, `defaultParamMap`; uses `Param.jsonEncode(v)` for each `ParamPair`
- **Param and ParamMap** (`mllib/src/main/scala/org/apache/spark/ml/param/params.scala`) — `Param.jsonEncode()` encoding values to JSON strings; `extractParamMap()` returning merged `ParamMap`; `ParamPair(param, value)` as the atomic unit
- **MLEvents** (`mllib/src/main/scala/org/apache/spark/ml/events.scala`) — `FitStart[M]` event with `estimator` and `dataset` fields; posted to `SparkContext.listenerBus` via `withFitEvent()`; used for snapshot timing
- **KVStore** (`common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java`) — `write(Object)` for snapshot persistence; `read(Class<T>, Object)` for retrieval; thread-safe for both reads and writes
- **Identifiable** (`mllib/src/main/scala/org/apache/spark/ml/util/Identifiable.scala`) — `uid` field pattern; `randomUID(prefix: String)` generating UIDs in the format `"prefix_XXXXXXXXXXXX"`

## Story Estimation Guidance

- **Story Points: 5** (Fibonacci scale)
- **Justification:** This story requires reusing the existing `DefaultParamsWriter.getMetadataToSave()` JSON serialization pattern for per-stage metadata capture, handling nested pipelines via recursion, integrating with the experiment run context established by STORY-004-01-01, and implementing fail-safe handling for non-serializable params. The core serialization logic already exists in `ReadWrite.scala` (lines 514–543), reducing implementation risk. However, nested pipeline handling via recursive `isInstanceOf[Pipeline]` detection, comprehensive testing across stage types (Estimator, Transformer, nested Pipeline), and KVStore persistence with `@KVIndex` annotations add complexity beyond a straightforward 3-point story.

## Definition of Done

- `PipelineConfigSnapshot` and `StageConfigSnapshot` data classes are defined with all specified fields and `@KVIndex` annotation on `runId`
- Pipeline configuration is automatically captured at the start of each `Pipeline.fit()` call when an active experiment run exists in the `ExperimentRunContext`
- Each stage's configuration includes: fully qualified class name, UID, `paramMap` (JSON-encoded via `Param.jsonEncode()`), and `defaultParamMap` (JSON-encoded)
- JSON format for each stage matches the structure produced by `DefaultParamsWriter.getMetadataToSave()` — containing keys `"class"`, `"timestamp"`, `"sparkVersion"`, `"uid"`, `"paramMap"`, and `"defaultParamMap"`
- Nested pipelines are recursively captured with inner stages represented as nested JSON structures within the outer pipeline's stage array
- Non-serializable params are handled with `try/catch` around `Param.jsonEncode()`, substituting `"serialization_failed"` placeholder values without halting the snapshot
- Empty pipelines (zero stages) produce a valid snapshot with an empty stages array and `totalStageCount=0`
- Snapshot is associated with the same run ID as hyperparameter logs via the `ThreadLocal`-based `ExperimentRunContext`
- Snapshot capture occurs before any stage `Estimator.fit()` or `Transformer.transform()` calls, reflecting the initial pipeline configuration
- `KVStore.write()` persistence errors are caught and logged without interrupting `Pipeline.fit()` execution
- Unit tests cover: single-stage pipeline, multi-stage pipeline, nested pipeline, empty pipeline, and non-serializable param handling
- Unit tests verify JSON format conformance with `DefaultParamsWriter.getMetadataToSave()` output
- Integration test validates end-to-end snapshot capture during `Pipeline.fit()` with retrieval via `ExperimentTracker.getPipelineConfig(runId)`
- PySpark API wrapping for `get_pipeline_config()` is implemented and tested
- API documentation covers snapshot retrieval and the JSON schema structure
