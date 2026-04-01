# Register Trained Model Artifacts in the Model Registry to Centralize ML Model Management

## User Story

**As a** ML engineer,
**I want to** register a trained Spark ML model (any subclass of `Model[M]`) in the native model registry by providing a unique model name, a human-readable description, the file system path where the model artifacts are persisted via `MLWriter.save()`, and an optional experiment run ID (from FEATURE-004-01) — creating a registry entry in the KVStore backend that stores the model name, description, artifact storage path, registration timestamp, the registering user identity, the model class name (from `Model.getClass.getName`), the model uid (from `Identifiable.uid`), and the parent Estimator class name (from `Model.parent.getClass.getName` if `hasParent` is true),
**so that** I can establish a centralized, searchable catalog of all trained models in the organization, eliminating the need to track model locations in spreadsheets or ad-hoc file naming conventions — reducing model discovery time from hours of searching shared file systems to under 10 seconds via registry lookup, and preventing duplicate training by surfacing existing models for the same task.

## Acceptance Criteria

### AC-1: Input Validation — Register model with all required fields

**Given** a trained `LinearRegressionModel` saved to `/models/linear_regression_v1` via `model.write.save("/models/linear_regression_v1")` using `MLWriter`
**When** the ML engineer calls `registry.registerModel(name="sales_forecaster", description="Predicts quarterly sales revenue", artifactPath="/models/linear_regression_v1", experimentRunId="run-abc-123")`
**Then** a new entry is created in the KVStore with model name `sales_forecaster`, description `Predicts quarterly sales revenue`, artifact path `/models/linear_regression_v1`, experiment run ID `run-abc-123`, a registration timestamp within 1 second of the current time, and the registry returns a unique registration ID

### AC-2: Expected Output — Registry entry contains model metadata

**Given** model `fraud_detector` has been registered with artifact path `/models/fraud_v1` from a `GBTClassificationModel` where `Model.parent` is a `GBTClassifier` with uid `gbt_abc123`
**When** the ML engineer calls `registry.getModel("fraud_detector")`
**Then** the returned registry entry contains: name `fraud_detector`, the provided description, artifactPath `/models/fraud_v1`, modelClass `org.apache.spark.ml.classification.GBTClassificationModel`, modelUid matching the model's `uid` property, parentEstimatorClass `org.apache.spark.ml.classification.GBTClassifier`, parentEstimatorUid `gbt_abc123`, registrationTimestamp, and registeredBy (the authenticated identity)

### AC-3: Expected Output — List all registered models

**Given** 3 models have been registered: `fraud_detector`, `churn_predictor`, and `recommendation_engine`
**When** the ML engineer calls `registry.listModels()`
**Then** the system returns a list of 3 registry entry summaries sorted alphabetically by model name, each containing name, description, latest version (if any), registration timestamp, and model class

### AC-4: Error Handling — Duplicate model name

**Given** a model named `fraud_detector` is already registered in the KVStore
**When** the ML engineer calls `registry.registerModel(name="fraud_detector", description="Another fraud model", artifactPath="/models/fraud_v2")`
**Then** the system raises an `IllegalArgumentException` with message containing `Model 'fraud_detector' is already registered; use createVersion to add a new version`

### AC-5: Error Handling — Missing required fields

**Given** the ML engineer attempts to register a model
**When** `registry.registerModel(name="", description="Some model", artifactPath="/models/something")` is called with an empty name
**Then** the system raises an `IllegalArgumentException` with message containing `Model name must be a non-empty string containing only alphanumeric characters, hyphens, and underscores`

### AC-6: Edge Case — Register model without experiment run linkage

**Given** a trained model saved to `/models/standalone_model` that was not produced during a tracked experiment run
**When** the ML engineer calls `registry.registerModel(name="standalone_model", description="Imported pre-trained model", artifactPath="/models/standalone_model")` without providing an experimentRunId
**Then** the registry entry is created with experimentRunId set to null, and all other fields (name, description, artifactPath, modelClass, timestamp) are populated

### AC-7: Edge Case — Register model with parent Estimator null

**Given** a model loaded from disk via `MLReader.load("/models/loaded_model")` where `Model.hasParent` returns false (the transient `parent` field is null because it is not serialized)
**When** the ML engineer registers this model with `registry.registerModel(name="imported_model", description="Loaded from external source", artifactPath="/models/loaded_model")`
**Then** the registry entry is created with parentEstimatorClass set to `N/A` and parentEstimatorUid set to `N/A`, and no error is raised

## Sub-Tasks

- [ ] Design model registry entry schema for KVStore with `@KVIndex` annotations: modelName (natural key), description, artifactPath, experimentRunId (nullable), registrationTimestamp, registeredBy, modelClass, modelUid, parentEstimatorClass, parentEstimatorUid, status (active/archived)
- [ ] Implement `ModelRegistryEntry` data class annotated with `@KVIndex` for KVStore indexing on modelName (natural index) and registrationTimestamp (secondary index)
- [ ] Implement `ModelRegistry` class that wraps a `KVStore` instance and provides the public API
- [ ] Implement `registerModel(name, description, artifactPath, experimentRunId?)` method that validates inputs, extracts model metadata (class name, uid, parent Estimator info via `hasParent`/`parent`), creates a `ModelRegistryEntry`, and persists it via `KVStore.write(entry)`
- [ ] Implement `getModel(name)` method using `KVStore.read(ModelRegistryEntry.class, name)`
- [ ] Implement `listModels()` method using `KVStore.view(ModelRegistryEntry.class)` with alphabetical ordering
- [ ] Implement `deleteModel(name)` method using `KVStore.delete(ModelRegistryEntry.class, name)` with cascading version cleanup
- [ ] Implement model name validation: non-empty, alphanumeric plus hyphens and underscores, max 256 characters
- [ ] Add PySpark API bindings for `registerModel`, `getModel`, `listModels`, `deleteModel`
- [ ] Add Scala/Java/Python unit tests for: successful registration, duplicate detection, missing fields, null parent, null experimentRunId, list ordering, deletion
- [ ] Add integration test: train `Pipeline` → save model via `MLWriter` → register → query → verify metadata extraction

## Edge Cases

1. **Empty/Null Input**: Calling `registerModel` with null model name, null description, or null artifact path — the system must raise `IllegalArgumentException` with a specific message identifying which field is null or empty: `Model name must be a non-empty string`, `Description must be a non-empty string`, or `Artifact path must be a non-empty string`
2. **Boundary Values — Maximum name length**: A model name at exactly 256 characters must succeed; a model name at 257 characters must be rejected with `IllegalArgumentException` containing `Model name must not exceed 256 characters; received 257`
3. **Invalid Input — Special characters in model name**: Names containing spaces, dots, or special characters (e.g., `my model v1.0!`) must be rejected with `IllegalArgumentException` containing `Model name must contain only alphanumeric characters, hyphens, and underscores`
4. **KVStore Backend Failure**: If the KVStore `write` operation throws an exception (e.g., disk full for LevelDB/RocksDB), the `registerModel` method must propagate the exception wrapped in a `SparkException` with message `Failed to persist model registry entry` and the original cause attached
5. **Concurrent Registration**: Two ML engineers simultaneously registering models with different names — both must succeed independently without interference due to KVStore thread safety; two engineers registering with the same name — the second must receive the duplicate name error

## Dependencies

### Parent Feature

- [FEATURE-004-02 — Model Versioning and Registry](../FEATURE-004-02-model-versioning-and-registry.md) — parent feature defining the overall model lifecycle management scope

### Related Features

- [FEATURE-004-01 — Experiment Metadata Logging](../FEATURE-004-01-experiment-metadata-logging.md) — provides experiment run IDs for optional linkage when registering models produced during tracked experiment runs

### Source Code Dependencies

- **Model abstract class** — `abstract class Model[M <: Model[M]] extends Transformer`; `@transient var parent: Estimator[M]` (may be null for deserialized models), `hasParent: Boolean`, `setParent(parent: Estimator[M]): M`, `estimatedSize: Long` via `SizeEstimator`
  - `Source: mllib/src/main/scala/org/apache/spark/ml/Model.scala` — parent field (line 33), hasParent (line 44), setParent (line 38), estimatedSize (line 60)
- **Estimator abstract class** — `Estimator[M <: Model[M]]` extends `PipelineStage`; provides `fit(dataset: Dataset[_]): M` and `uid: String` for identification
  - `Source: mllib/src/main/scala/org/apache/spark/ml/Estimator.scala` — fit method (line 67), class declaration (line 30)
- **Identifiable trait** — `uid: String` and `randomUID(prefix)` generating `prefix_XXXXXXXXXXXX` for unique identification of pipeline stages and models
  - `Source: mllib/src/main/scala/org/apache/spark/ml/util/Identifiable.scala`
- **MLWriter / MLReader** — `MLWriter.save(path: String)` for model artifact persistence; `DefaultParamsWriter.getMetadataToSave` produces JSON metadata with keys: `class`, `timestamp`, `sparkVersion`, `uid`, `paramMap`, `defaultParamMap`
  - `Source: mllib/src/main/scala/org/apache/spark/ml/util/ReadWrite.scala` — MLWriter.save (line 174), DefaultParamsWriter.getMetadataToSave (lines 514–543)
- **Pipeline and PipelineModel** — `Pipeline` extends `Estimator[PipelineModel]` with `MLWritable`; `PipelineModel` wraps fitted stages and supports `save`/`load` for end-to-end pipeline persistence
  - `Source: mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala` — Pipeline class (line 94), PipelineModel class
- **KVStore interface** — `write(Object)`, `read(Class<T>, Object)`, `view(Class<T>)`, `delete(Class<?>, Object)`, `count(Class<?>)` for registry metadata persistence
  - `Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java` — write (line 103), read (line 90), view (line 118), delete (line 113), count (line 123)
- **KVIndex annotation** — `@KVIndex` with `value` (index name) and `NATURAL_INDEX_NAME` for primary key designation on registry entry fields
  - `Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVIndex.java` — NATURAL_INDEX_NAME (line 56), annotation declaration (line 54)
- **InMemoryStore** — thread-safe in-memory `KVStore` implementation using `ConcurrentHashMap`; used for testing registry operations without persistent storage
  - `Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/InMemoryStore.java` — class declaration (line 44), write method (line 90), read method (line 80)

## Story Estimation Guidance

**Story Points: 5** (Fibonacci scale)

**Rationale:** This is the foundational story establishing the model registry infrastructure. It involves designing the KVStore-backed registry entry schema with `@KVIndex` annotations, implementing the `ModelRegistry` wrapper class with CRUD operations, handling `Model` metadata extraction (including the transient null `parent` field for deserialized models), input validation with specific error messages, PySpark API parity, and comprehensive testing. The KVStore integration pattern is well-established in Spark (used by the History Server and UI status stores), but the schema design and metadata extraction from the ML object hierarchy add moderate complexity.

**Reference:** Similar in scope to implementing the `ApplicationInfo` entity and store in the existing Spark History Server KVStore layer.

**Breakdown:**
- KVStore schema design and `ModelRegistryEntry` implementation: ~1 point
- `ModelRegistry` class with `registerModel`, `getModel`, `listModels`, `deleteModel`: ~1.5 points
- Input validation and error handling (name format, duplicates, null parent): ~0.5 points
- PySpark API bindings: ~1 point
- Unit tests and integration test: ~1 point

## Definition of Done

- [ ] `ModelRegistry` class wraps `KVStore` and provides `registerModel`, `getModel`, `listModels`, and `deleteModel` APIs
- [ ] `registerModel` persists a complete registry entry in KVStore with: model name (unique, validated), description, artifact path, experiment run ID (optional), registration timestamp, registered-by identity, model class name, model uid, parent Estimator class and uid (handling null parent gracefully)
- [ ] `getModel` retrieves a single registry entry by name from KVStore using `KVStore.read(ModelRegistryEntry.class, name)`
- [ ] `listModels` returns all registered models sorted alphabetically by model name with summary metadata
- [ ] Duplicate model names are rejected with clear error messages directing the ML engineer to use `createVersion` for new versions
- [ ] Input validation covers: empty/null fields with field-specific error messages, name format restricted to alphanumeric characters plus hyphens and underscores, maximum name length of 256 characters
- [ ] Models without parent Estimator (loaded from disk via `MLReader.load`) register with `N/A` for parentEstimatorClass and parentEstimatorUid fields
- [ ] Models without experiment run linkage register with null experimentRunId without error
- [ ] KVStore backend failures are wrapped in `SparkException` with the message `Failed to persist model registry entry` and the original cause attached
- [ ] PySpark API parity is maintained for all four registry operations (`registerModel`, `getModel`, `listModels`, `deleteModel`)
- [ ] Unit tests cover: successful registration, duplicate name detection, missing/empty fields, null parent Estimator, null experiment run ID, model name format validation, alphabetical listing, model deletion, concurrent registration
- [ ] Integration test validates the full workflow: train a model via `Pipeline.fit()` → save via `MLWriter.save()` → register in registry → query via `getModel` → verify extracted metadata matches the trained model's class, uid, and parent Estimator
- [ ] Documentation covers the registry API, registration workflow, metadata field descriptions, and PySpark usage examples
