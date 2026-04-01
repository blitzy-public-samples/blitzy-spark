# Version Registered Models with Semantic Versioning and Data Lineage to Enable Reproducible ML Workflows

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

**As an** ML engineer, **I want to** assign semantic version numbers (major.minor.patch) to registered model entries in the model registry, with each version recording complete lineage metadata including: the training dataset identifier, the pipeline configuration hash derived from the PipelineStage array and each stage's ParamMap (serialized via `Param.jsonEncode`), the parent Estimator reference from `Model.parent`, the experiment run ID (from FEATURE-004-01), the Spark version from DefaultParamsWriter metadata, and a creation timestamp — with automatic minor version increment when no explicit version is provided, **so that** I can guarantee full reproducibility of any trained model version by capturing exactly which data, code, parameters, and environment produced it — reducing model debugging and reproduction time from days to minutes by providing complete lineage traceability for every registered model version.

## Acceptance Criteria

### AC 1: Input Validation — Explicit Version Assignment

- **Given** a registered model named "fraud_detector" with existing version "1.0.0" in the KVStore-backed model registry
- **When** the ML engineer calls `registry.createVersion("fraud_detector", version="2.0.0", modelArtifactPath="/models/fraud_detector_v2", experimentRunId="run-abc-123")`
- **Then** a new version entry "2.0.0" is created in the KVStore for model "fraud_detector", the entry contains the storage path "/models/fraud_detector_v2", the linked experiment run ID "run-abc-123", a creation timestamp, and `registry.getVersion("fraud_detector", "2.0.0")` returns the complete version metadata

### AC 2: Expected Output — Automatic Version Increment

- **Given** a registered model "churn_predictor" with latest version "1.2.0" in the registry
- **When** the ML engineer calls `registry.createVersion("churn_predictor", modelArtifactPath="/models/churn_v1.3", experimentRunId="run-xyz-456")` without specifying an explicit version number
- **Then** the system automatically assigns version "1.3.0" (minor version incremented from 1.2.0), persists the version entry in KVStore, and returns the assigned version string "1.3.0"

### AC 3: Expected Output — Lineage Metadata Captured Per Version

- **Given** a registered model "sales_forecaster" with a new version "1.0.0" created from a PipelineModel containing stages [StringIndexer, VectorAssembler, LinearRegression] with the LinearRegression Estimator as `Model.parent`
- **When** the ML engineer queries `registry.getVersionLineage("sales_forecaster", "1.0.0")`
- **Then** the returned lineage metadata contains: (a) trainingDatasetId identifying the training data source, (b) pipelineConfigHash — a deterministic SHA-256 hash of the JSON-serialized PipelineStage array and their ParamMap values (using `Param.jsonEncode`), (c) parentEstimatorClass — the fully qualified class name from `Model.parent.getClass.getName` (e.g., "org.apache.spark.ml.regression.LinearRegression"), (d) parentEstimatorUid — the `Model.parent.uid` string, (e) experimentRunId linking to the FEATURE-004-01 experiment metadata, (f) sparkVersion from the DefaultParamsWriter metadata format, and (g) creationTimestamp

### AC 4: Error Handling — Duplicate Version Number

- **Given** model "text_classifier" already has version "1.0.0" registered in the KVStore
- **When** the ML engineer calls `registry.createVersion("text_classifier", version="1.0.0", modelArtifactPath="/models/text_v1")`
- **Then** the system raises an `IllegalArgumentException` with message containing "Version '1.0.0' already exists for model 'text_classifier'"

### AC 5: Error Handling — Invalid Semantic Version Format

- **Given** a registered model "image_classifier" in the registry
- **When** the ML engineer calls `registry.createVersion("image_classifier", version="v1.0", modelArtifactPath="/models/img_v1")`
- **Then** the system raises an `IllegalArgumentException` with message containing "Version must follow semantic versioning format (major.minor.patch) with non-negative integers; received 'v1.0'"

### AC 6: Edge Case — First Version of a New Model

- **Given** a model "new_model" is registered with no existing versions
- **When** the ML engineer calls `registry.createVersion("new_model", modelArtifactPath="/models/new_v1", experimentRunId="run-first-001")` without specifying a version
- **Then** the system assigns version "1.0.0" as the initial version, persists the entry in KVStore, and returns "1.0.0"

### AC 7: Edge Case — Lineage for Model Without Parent Estimator

- **Given** a model loaded from disk (not produced by a Pipeline fit) where `Model.hasParent` returns false (the `@transient var parent` is null)
- **When** the ML engineer creates a version for this model and queries its lineage
- **Then** the lineage metadata shows parentEstimatorClass as "N/A" and parentEstimatorUid as "N/A" without raising an error, and all other lineage fields (trainingDatasetId, pipelineConfigHash, sparkVersion, timestamp) are still populated

## Sub-Tasks

- [ ] Design version entry schema for KVStore: modelName, version (semantic version string), modelArtifactPath, experimentRunId, creationTimestamp, stage (default "Development"), lineage metadata object
- [ ] Design lineage metadata schema: trainingDatasetId, pipelineConfigHash (SHA-256), parentEstimatorClass, parentEstimatorUid, experimentRunId, sparkVersion, paramMapSnapshot (JSON from `Param.jsonEncode`)
- [ ] Implement semantic version parser and validator (major.minor.patch, all non-negative integers, no prefixes)
- [ ] Implement automatic version increment logic: parse latest version string, increment minor component, reset patch to 0
- [ ] Implement `createVersion(modelName, version?, modelArtifactPath, experimentRunId?)` API with optional explicit version and optional experiment linkage
- [ ] Implement lineage capture logic: extract `Model.parent` class name and uid (handle null parent via `hasParent` check), serialize PipelineStage ParamMaps via `Param.jsonEncode`, compute SHA-256 hash of concatenated JSON
- [ ] Implement `getVersionLineage(modelName, version)` API to retrieve lineage from KVStore
- [ ] Implement `listVersions(modelName)` to return all versions sorted by semantic version descending
- [ ] Implement `getLatestVersion(modelName)` convenience method
- [ ] Add PySpark API bindings for createVersion, getVersionLineage, listVersions, getLatestVersion
- [ ] Write unit tests covering: explicit version, auto-increment, first version, duplicate version, invalid format, lineage capture with parent, lineage without parent, version listing and sorting
- [ ] Write integration test: train Pipeline → register model → create version → verify lineage → create second version → verify auto-increment

## Edge Cases

### 1. Empty/Null Input

Calling `createVersion` with null or empty model name or model artifact path — system must raise `IllegalArgumentException` with message "Model name and artifact path must be non-empty strings".

### 2. Boundary Values — Version Number Limits

Creating a version with very large component numbers (e.g., "999.999.999") must succeed; version "0.0.0" must be rejected with `IllegalArgumentException` "Version '0.0.0' is reserved; minimum version is '0.0.1'".

### 3. Invalid Input — Negative Version Components

Version strings like "1.-1.0" or "-1.0.0" must be rejected with `IllegalArgumentException` describing the invalid component.

### 4. Model Artifact Path Not Accessible

If the specified `modelArtifactPath` does not exist or is not readable at version creation time, the system must log a warning but still create the version entry (artifacts may be written after registration) — the version metadata stores the path as-is without validation.

### 5. Concurrent Version Creation

Two ML engineers simultaneously calling `createVersion` for the same model without explicit versions — KVStore thread safety must ensure each receives a unique auto-incremented version number (e.g., "1.3.0" and "1.4.0") without duplicates.

## Dependencies

- **FEATURE-004-02** (Model Versioning and Registry) — parent feature
- **FEATURE-004-01** (Experiment Metadata Logging) — experiment run IDs used for lineage linkage
- **STORY-004-02-01** (Register Trained Model Artifacts) — models must be registered before versioning
- **Model class** (`Source: mllib/src/main/scala/org/apache/spark/ml/Model.scala`) — `abstract class Model[M <: Model[M]] extends Transformer` with `@transient var parent: Estimator[M]` (may be null for deserialized models), `hasParent: Boolean` (returns `parent != null`), `setParent(parent: Estimator[M]): M`, and `estimatedSize: Long` via `SizeEstimator` for lineage extraction
- **Estimator class** (`Source: mllib/src/main/scala/org/apache/spark/ml/Estimator.scala`) — `abstract class Estimator[M <: Model[M]] extends PipelineStage` providing `uid: String` for lineage tracking and `fit(dataset: Dataset[_]): M` for model production
- **Pipeline class** (`Source: mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`) — `Pipeline` stores stages as `Param[Array[PipelineStage]]` with clone-on-get; `PipelineModel` wraps the fitted stage array; persistence serializes stages under `stages/IDX_UID` directories for pipeline config hash computation
- **ReadWrite utilities** (`Source: mllib/src/main/scala/org/apache/spark/ml/util/ReadWrite.scala`) — `DefaultParamsWriter.getMetadataToSave` produces JSON with keys: `class`, `timestamp`, `sparkVersion`, `uid`, `paramMap`, `defaultParamMap`; `Param.jsonEncode` is used for parameter serialization within `paramMap` construction
- **KVStore interface** (`Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java`) — `write(Object)` for version entry persistence, `read(Class<T>, Object)` for single-version retrieval, `view(Class<T>)` for listing and querying versions with index-based ordering; thread-safe for both reads and writes

## Story Estimation Guidance

- **Story Points: 5** (Fibonacci scale)
- **Rationale:** Moderate-to-high complexity due to semantic version parsing and auto-increment logic, SHA-256 pipeline config hashing from serialized ParamMaps, lineage metadata extraction from the Model/Estimator/Pipeline object graph (handling the transient null parent case), KVStore schema design for version entries, and concurrent version creation safety. The lineage capture integrates with multiple MLlib components (`Model.parent`, Pipeline stages, `Param.jsonEncode`, `DefaultParamsWriter` metadata format).
- **Reference:** Comparable to implementing a versioned metadata layer over the existing MLWriter/MLReader persistence framework.

## Definition of Done

- `createVersion` API creates versioned entries in KVStore with explicit or auto-incremented semantic version numbers
- Semantic version validation rejects malformed version strings and enforces major.minor.patch format with non-negative integers
- Auto-increment assigns the next minor version when no explicit version is provided, starting at "1.0.0" for the first version
- Duplicate version numbers are rejected with clear error messages
- Complete lineage metadata is captured per version: trainingDatasetId, pipelineConfigHash (SHA-256 of serialized stage ParamMaps), parentEstimatorClass, parentEstimatorUid, experimentRunId, sparkVersion, creationTimestamp
- Lineage handles null parent Estimator (Model loaded from disk) without errors
- `getVersionLineage`, `listVersions`, and `getLatestVersion` query APIs return accurate results from KVStore
- Concurrent version creation produces unique version numbers without duplicates
- PySpark API parity for all version creation and query operations
- Unit tests cover explicit version, auto-increment, first version, duplicate, invalid format, lineage with/without parent, version sorting, boundary values, and concurrency
- Integration test validates Pipeline → register → version → lineage → auto-increment workflow
- Documentation covers version schema, lineage fields, auto-increment behavior, and PySpark API
