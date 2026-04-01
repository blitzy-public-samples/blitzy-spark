# Log Estimator Hyperparameters Per Training Run to Enable Automated Experiment Tracking

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

**As a** ML engineer, **I want to** automatically capture and persist all Estimator hyperparameter values from the ParamMap — including both explicitly set parameters and default values — each time `Pipeline.fit()` or `Estimator.fit()` executes, creating a unique experiment run record with a generated run ID and timestamp, **so that** I can maintain a complete, version-controlled record of every hyperparameter configuration used across training runs without manual logging effort, enabling precise comparison of parameter changes between runs and exact reproduction of any historical training configuration — eliminating the need for external experiment tracking tools for hyperparameter management.

## Acceptance Criteria

### AC-1: Hyperparameter Capture on Estimator Fit

- **Given** an ML engineer creates a `LogisticRegression` Estimator with `maxIter=100`, `regParam=0.01`, `elasticNetParam=0.5` and calls `fit()` on a training DataFrame
- **When** `fit()` execution begins
- **Then** a new experiment run record is created in the KVStore containing:
  - A unique run ID generated via a UUID-based scheme
  - The Estimator class name `"org.apache.spark.ml.classification.LogisticRegression"`
  - The Estimator UID (matching `Identifiable.uid`)
  - A millisecond-precision timestamp recorded at run creation time
  - A hyperparameter map with entries for `maxIter=100`, `regParam=0.01`, `elasticNetParam=0.5` encoded via `Param.jsonEncode()`
  - The current `sparkVersion` string

### AC-2: Default Parameters Included and Distinguished

- **Given** an ML engineer creates a `RandomForestClassifier` with only `maxDepth=10` explicitly set while all other parameters retain their defaults
- **When** `fit()` is called on the training DataFrame
- **Then** the stored hyperparameter record includes both the explicitly set `maxDepth=10` and all default parameter values (such as `numTrees`, `featureSubsetStrategy`, `impurity`) retrieved from `extractParamMap()`
- **And** each parameter entry is annotated with a status of `"set"` or `"default"` based on the result of `Params.isSet(param)` for every `Param` in the extracted map

### AC-3: Pipeline Context Capture

- **Given** an ML engineer constructs a `Pipeline` with stages `[Tokenizer, HashingTF, LogisticRegression]` and calls `Pipeline.fit()`
- **When** the pipeline iterates its stages and calls `Estimator.fit()` on the `LogisticRegression` stage
- **Then** an experiment run record is created for the `LogisticRegression` Estimator that includes:
  - The enclosing `Pipeline` UID (from `Pipeline.uid`)
  - The stage index within the pipeline (e.g., `2` for the third stage)
  - All hyperparameters of the `LogisticRegression` Estimator extracted from its `ParamMap`
- **And** Transformer-only stages (`Tokenizer`, `HashingTF`) do not produce experiment run records

### AC-4: Non-Serializable Parameter Handling (Input Validation)

- **Given** an ML engineer calls `fit()` on an Estimator whose `ParamMap` contains a `Param` with a value that causes `Param.jsonEncode()` to throw an `UnsupportedOperationException` (e.g., a non-serializable custom object)
- **When** hyperparameter logging iterates through `extractParamMap().toSeq` and encounters the failing `ParamPair`
- **Then** the system logs a warning message identifying the parameter name and the exception type
- **And** all other serializable parameters from the `ParamMap` are recorded in the run metadata
- **And** the failed parameter is stored with a placeholder value of `"serialization_failed"` and a `paramSetStatus` entry of `"serialization_failed"`

### AC-5: KVStore Write Failure Resilience (Error Handling)

- **Given** the configured `KVStore` backend (`InMemoryStore`, `LevelDB`, or `RocksDB`) is unavailable or encounters a write error when `KVStore.write()` is invoked
- **When** hyperparameter logging attempts to persist the `ExperimentRunMetadata` record after `extractParamMap()` succeeds
- **Then** the system catches the `Exception` thrown by `KVStore.write()`
- **And** the system logs a warning message containing the Estimator class name, the Estimator UID, and the generated run ID
- **And** the `Estimator.fit()` method proceeds without interruption, returning the trained model identically to execution without experiment tracking enabled

### AC-6: ParamMap Override Capture (Edge Case)

- **Given** an ML engineer creates a `LogisticRegression` Estimator with `regParam=0.01` and then calls `fit(dataset, paramMap)` where `paramMap` overrides `regParam` to `0.1`
- **When** `Estimator.fit(dataset, paramMap)` invokes `copy(paramMap).fit(dataset)` and hyperparameter logging captures parameters from the copied instance
- **Then** the stored experiment run record reflects the overridden value `regParam=0.1` (not the original `0.01`)
- **And** the `paramSetStatus` for `regParam` is recorded as `"set"` since the copied instance has the override applied via `copyValues`

### AC-7: Distinct Run Records Per Fit Invocation (Edge Case)

- **Given** an ML engineer calls `fit()` on the same `LogisticRegression` Estimator instance 3 times with 3 different training DataFrames
- **When** each `fit()` invocation triggers hyperparameter logging
- **Then** 3 distinct experiment run records are persisted in the KVStore
- **And** each record has a unique run ID and a distinct timestamp
- **And** all 3 records share the same Estimator UID and identical hyperparameter values (since the Estimator parameters were not changed between calls)

### AC-8: Run ID Uniqueness and Reproducibility

- **Given** an ML engineer calls `fit()` on any Estimator
- **When** the experiment run record is created
- **Then** the run ID is generated using `java.util.UUID.randomUUID().toString`, producing a 36-character hyphenated UUID string that is unique across concurrent `SparkSession` instances
- **And** when a deterministic seed is configured via `spark.ml.experiment.runIdSeed` for testing, the run ID generation uses a seeded `Random` to produce reproducible UUIDs

## Sub-Tasks

- Define `ExperimentRunMetadata` data class with the following fields:
  - `runId` (`String`, annotated with `@KVIndex` as the natural key)
  - `estimatorClassName` (`String`, annotated with `@KVIndex` as a secondary index)
  - `estimatorUid` (`String`)
  - `pipelineUid` (`Option[String]`, annotated with `@KVIndex` as a secondary index)
  - `stageIndex` (`Option[Int]`)
  - `timestamp` (`Long`, annotated with `@KVIndex` as a secondary index)
  - `hyperparameters` (`Map[String, String]` — param name to JSON-encoded value via `Param.jsonEncode()`)
  - `paramSetStatus` (`Map[String, String]` — param name to `"set"` | `"default"` | `"serialization_failed"`)
  - `sparkVersion` (`String`)
- Implement `ExperimentRunContext` as a `ThreadLocal[Option[ExperimentRunContextData]]` holder that stores the active pipeline UID and current stage index, supporting nested `Pipeline.fit()` contexts
- Implement `HyperparameterLogger` that hooks into the `Estimator.fit()` lifecycle using one of two strategies:
  - **Option A (preferred):** Create a `SparkListener` subclass that listens for `FitStart[M]` events (from `events.scala`) and extracts `estimator.extractParamMap()` from the event's `estimator` field
  - **Option B:** Add instrumentation within the `Pipeline.fit()` loop, mirroring the existing `withFitEvent` calls in `Pipeline.scala`
- Implement hyperparameter extraction logic:
  - Call `estimator.extractParamMap().toSeq` to obtain `Seq[ParamPair[Any]]`
  - For each `ParamPair(param, value)`: invoke `param.jsonEncode(value)` wrapped in a `try/catch` block
  - On `UnsupportedOperationException` or any `Exception` from `jsonEncode()`: record the param name with value `"serialization_failed"`
- Implement param set-vs-default status detection:
  - For each `Param` in the extracted map, call `estimator.isSet(param)` — record `"set"` if true, `"default"` if false
  - For params that failed serialization, record `"serialization_failed"`
- Implement run ID generation:
  - Default: `java.util.UUID.randomUUID().toString`
  - Testing mode: use a `scala.util.Random` seeded via `spark.ml.experiment.runIdSeed` configuration parameter to generate reproducible UUIDs
- Implement KVStore persistence:
  - Call `kvStore.write(experimentRunMetadata)` within a `try/catch` block
  - On `Exception`: log a warning containing the Estimator class name, Estimator UID, and run ID; do not re-throw
- Implement pipeline context propagation:
  - Before `Pipeline.fit()` iterates stages, set `ExperimentRunContext` with the `Pipeline.uid`
  - Update the current stage index as each stage is processed
  - After `Pipeline.fit()` completes (or on exception), clear the `ExperimentRunContext`
  - Within `HyperparameterLogger`, read `ExperimentRunContext` to populate `pipelineUid` and `stageIndex` fields
- Implement `ExperimentTracker` singleton/per-session entry point with methods:
  - `startRun()` — initialize a new run context
  - `endRun()` — finalize and persist the current run context
  - `getRunMetadata(runId: String): Option[ExperimentRunMetadata]` — read from KVStore
  - `listRuns(): Seq[ExperimentRunMetadata]` — list all stored runs
- Expose Scala API: integrate `ExperimentTracker` with `SparkSession.experimental` or as an implicit class extension
- Expose Python API: `ExperimentTracker.start_run()`, `.end_run()`, `.get_run_metadata(run_id)`, `.list_runs()` in PySpark
- Expose Java API: `ExperimentTracker.startRun()`, `.endRun()`, `.getRunMetadata(runId)`, `.listRuns()` in Java-compatible wrappers
- Write unit tests covering:
  - Single `Estimator.fit()` with hyperparameter capture and retrieval
  - `Pipeline.fit()` with multiple stages including both Estimators and Transformers
  - `fit(dataset, paramMap)` with parameter overrides captured from the copied instance
  - Non-serializable param handling with `"serialization_failed"` placeholder
  - `KVStore.write()` failure with graceful degradation
  - Multiple `fit()` calls producing distinct run records
  - Concurrent `fit()` calls with `ThreadLocal` isolation
- Write integration test: end-to-end `Pipeline.fit()` with hyperparameter capture, retrieval via `ExperimentTracker.getRunMetadata()`, and verification of all stored fields

## Edge Cases

### 1. Empty/Null Input

- **Estimator with all-default parameters:** An Estimator is created with no explicitly set parameters — `extractParamMap()` returns all default values. The hyperparameter record captures every default parameter with status `"default"` for each entry. No entries have status `"set"`.
- **Estimator with zero `Params` defined:** A custom Estimator subclass declares no `Param` fields — `extractParamMap().toSeq` returns an empty `Seq`. The resulting `ExperimentRunMetadata` stores an empty `hyperparameters` map and an empty `paramSetStatus` map. The run record is still created with a valid `runId`, `estimatorClassName`, `timestamp`, and `sparkVersion`.
- **Null dataset passed to `fit()`:** When `Estimator.fit(null)` is called, the Estimator implementation throws a `NullPointerException` or `IllegalArgumentException` before hyperparameter logging triggers. No partial or orphaned run record is persisted to the KVStore.

### 2. Boundary Values

- **Estimator with 50+ parameters:** A complex Estimator such as `GBTClassifier` or `MultilayerPerceptronClassifier` declares 50 or more parameters — all parameters are extracted via `extractParamMap().toSeq` and individually encoded. The resulting `hyperparameters` map contains all 50+ entries without truncation.
- **Numeric extreme values:** Hyperparameter values at numeric boundaries (`maxIter=Integer.MAX_VALUE`, `regParam=Double.MIN_VALUE`, `threshold=Double.NaN`, `tol=Double.POSITIVE_INFINITY`) are encoded using the specialized `DoubleParam.jsonEncode()` and `IntParam.jsonEncode()` methods, which handle the full `Double` and `Int` ranges including `NaN`, `Infinity`, and `Integer.MAX_VALUE`.
- **Very long string parameter values:** A `StringParam` containing a 10,000-character string is encoded via `Param.jsonEncode()` as a JSON string and stored in full without truncation.

### 3. Invalid Input

- **`Param.jsonEncode()` throws for a custom param type:** A custom `Param[CustomObject]` subclass does not override `jsonEncode()`, causing the base `Param.jsonEncode()` to throw `UnsupportedOperationException`. The failed parameter is recorded with value `"serialization_failed"` and status `"serialization_failed"`. All other parameters in the `ParamMap` are captured.
- **`ParamMap` contains a param from a different parent:** If a `ParamPair` references a `Param` with a mismatched `parent` UID, `isSet()` throws `IllegalArgumentException` via `shouldOwn()`. The logger catches this exception, records the parameter status as `"serialization_failed"`, and continues processing remaining parameters.

### 4. Concurrent Execution

- **Parallel `fit()` calls on different threads:** Two `Estimator.fit()` calls execute simultaneously on separate threads within the same `SparkSession`. Each call creates a separate `ExperimentRunMetadata` record with a unique `runId` generated by `UUID.randomUUID()` (which is thread-safe). The `ThreadLocal`-based `ExperimentRunContext` ensures that pipeline context (pipelineUid, stageIndex) is isolated per thread with no cross-contamination between run records.

### 5. Estimator Copy Semantics

- **`fit(dataset, paramMap)` override capture:** When `Estimator.fit(dataset, paramMap)` is called, it invokes `copy(paramMap).fit(dataset)` as defined in `Estimator.scala:59-61`. The `copy()` method creates a new Estimator instance with the overridden parameters applied via `copyValues(to, extra)`. Hyperparameter logging captures parameters from this copied instance (reflecting the overrides), not from the original instance. This ensures that the recorded hyperparameters match the actual configuration used during training — for example, if `regParam` is overridden from `0.01` to `0.1`, the stored record contains `regParam=0.1`.

## Dependencies

- **FEATURE-004-01** (Experiment Metadata Logging) — parent feature defining the overall metadata logging subsystem scope
- **EPIC-004** (Native ML Experiment Tracking and Model Registry) — parent epic providing the strategic vision for experiment tracking
- **Estimator class** (`mllib/src/main/scala/org/apache/spark/ml/Estimator.scala`) — `abstract class Estimator[M <: Model[M]] extends PipelineStage`; provides `fit(dataset: Dataset[_]): M` (abstract), `fit(dataset, paramMap)` which delegates via `copy(paramMap).fit(dataset)`, `fit(dataset, firstParamPair, otherParamPairs*)` which builds a `ParamMap` then delegates, and `copy(extra: ParamMap): Estimator[M]`
- **Params trait** (`mllib/src/main/scala/org/apache/spark/ml/param/params.scala`) — `extractParamMap(): ParamMap` returning merged `defaultParamMap ++ paramMap`; `isSet(param): Boolean` checking if a param is in the user-supplied `paramMap`; `isDefined(param): Boolean`; `hasDefault(param): Boolean`; `getOrDefault(param): T`; `Param.jsonEncode(value): String` for JSON serialization; `ParamPair(param, value)` as the atomic unit
- **Pipeline class** (`mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`) — `class Pipeline extends Estimator[PipelineModel] with MLWritable`; `stages: Param[Array[PipelineStage]]`; `fit()` iterates stages calling `Estimator.fit()` for Estimators and `Transformer.transform()` for Transformers; uses `instr.withFitEvent(this, dataset)` to bracket execution with ML events
- **MLEvents** (`mllib/src/main/scala/org/apache/spark/ml/events.scala`) — `sealed trait MLEvent extends SparkListenerEvent`; `FitStart[M]` with `@JsonIgnore var estimator: Estimator[M]` and `@JsonIgnore var dataset: Dataset[_]`; `FitEnd[M]` with `estimator` and `model` fields; `MLEvents` trait providing `withFitEvent()` helper that posts events to `SparkContext.listenerBus`
- **DefaultParamsWriter** (`mllib/src/main/scala/org/apache/spark/ml/util/ReadWrite.scala`) — JSON metadata format reference containing keys: `class`, `timestamp`, `sparkVersion`, `uid`, `paramMap`, `defaultParamMap`; provides the serialization pattern for pipeline stage metadata
- **Identifiable** (`mllib/src/main/scala/org/apache/spark/ml/util/Identifiable.scala`) — `uid` field pattern and `randomUID(prefix: String)` method generating UIDs in the format `"prefix_XXXXXXXXXXXX"`
- **KVStore** (`common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java`) — Java interface with `write(Object)` for persistence, `read(Class<T>, Object)` for retrieval, `view(Class<T>)` for listing; thread-safe for both reads and writes
- **KVIndex** (`common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVIndex.java`) — `@KVIndex` annotation for marking indexed fields on entity classes; supports natural keys and secondary indices
- **InMemoryStore** (`common/kvstore/src/main/java/org/apache/spark/util/kvstore/InMemoryStore.java`) — default in-memory `KVStore` backend for development and testing; implements all `KVStore` operations with no external dependencies

## Story Estimation Guidance

- **Story Points: 8** (Fibonacci scale)
- **Justification:** This is the foundational story for the entire experiment tracking subsystem. It requires:
  - Designing the `ExperimentRunMetadata` entity class with `@KVIndex` annotations for `runId` (natural key), `estimatorClassName`, `timestamp`, and `pipelineUid` (secondary indices)
  - Implementing `ThreadLocal`-based `ExperimentRunContext` for nested `Pipeline.fit()` context propagation across stages
  - Integrating with the existing `MLEvents` system (`FitStart`/`FitEnd` events posted via `SparkContext.listenerBus`) or adding instrumentation in the `Pipeline.fit()` loop
  - Implementing fail-safe `ParamMap` extraction with `Param.jsonEncode()` error handling for non-serializable parameter types
  - Distinguishing user-set vs default parameters using `Params.isSet(param)`
  - Supporting three language APIs (Scala, Python via PySpark, Java) for `ExperimentTracker`
  - Writing unit tests covering 7 scenarios (single fit, pipeline fit, override fit, serialization failure, KVStore failure, multiple fits, concurrent execution) plus integration tests
  - The architectural decisions made here — run ID scheme, entity model design, context propagation mechanism, and KVStore integration pattern — constrain all subsequent stories in FEATURE-004-01

## Definition of Done

- `ExperimentRunMetadata` entity is defined with `@KVIndex` annotations on `runId` (natural key), `estimatorClassName` (secondary), `timestamp` (secondary), and `pipelineUid` (secondary)
- Hyperparameters are automatically extracted from `Estimator.extractParamMap()` during each `fit()` call, producing a `Map[String, String]` of parameter names to JSON-encoded values
- Each parameter is JSON-encoded via `Param.jsonEncode()` with fail-safe `try/catch` handling that records `"serialization_failed"` for parameters whose `jsonEncode()` throws
- Parameter set/default status is recorded for each hyperparameter by checking `Params.isSet(param)` and storing `"set"`, `"default"`, or `"serialization_failed"` in the `paramSetStatus` map
- A unique run ID is generated for each `fit()` invocation using `java.util.UUID.randomUUID().toString`, with seeded generation available via `spark.ml.experiment.runIdSeed` for deterministic testing
- Pipeline context (`pipelineUid` from `Pipeline.uid` and `stageIndex` from the iteration position) is captured when `fit()` occurs within `Pipeline.fit()` using the `ThreadLocal`-based `ExperimentRunContext`
- `Estimator.fit(dataset, paramMap)` captures the overridden parameter values from the copied instance (produced by `copy(paramMap)`) rather than the original instance
- `KVStore.write()` failures are caught, logged as warnings (including Estimator class name, UID, and run ID), and do not interrupt `Estimator.fit()` execution or alter the returned model
- Multiple `fit()` calls on the same Estimator instance produce separate run records, each with a unique run ID and distinct timestamp
- Concurrent `fit()` calls on separate threads produce isolated run records with no cross-contamination, enforced by `ThreadLocal` `ExperimentRunContext` isolation
- Scala, Python (PySpark), and Java APIs for `ExperimentTracker` are implemented and tested
- Unit tests cover: single Estimator fit, Pipeline fit with multiple stages, `paramMap` override fit, non-serializable param handling, `KVStore` write failure, multiple fit calls, and concurrent execution
- Integration test validates end-to-end hyperparameter capture during `Pipeline.fit()` and retrieval via `ExperimentTracker.getRunMetadata(runId)`
- API documentation includes usage examples for Scala, Python, and Java demonstrating hyperparameter logging during `Estimator.fit()` and `Pipeline.fit()` workflows
