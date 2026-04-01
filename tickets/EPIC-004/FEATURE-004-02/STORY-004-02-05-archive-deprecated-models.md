# Archive Deprecated Model Versions to Maintain a Clean and Manageable Registry

## User Story

**As a** data platform administrator, **I want to** mark model versions as deprecated and archive them from the active registry, so they no longer appear in active listings or promotion workflows, while the underlying model artifacts remain in persistent storage for audit and rollback purposes, **so that** I can reduce clutter in the active model registry, prevent accidental promotion of outdated models, and maintain compliance with data governance retention policies — reducing active registry size by removing stale entries and cutting registry query response times for active model lookups by up to 40%.

## Acceptance Criteria

### AC-1: Input Validation — Archive request with valid model and version

- **Given** a registered model named "fraud_detector" with version "2.0.0" in "Production" stage in the model registry backed by KVStore
- **When** the data platform administrator calls `registry.archiveModelVersion("fraud_detector", "2.0.0", reason="Superseded by v3.0.0")`
- **Then** the model version status is updated to "Archived" in the KVStore, the model version no longer appears in `registry.listActiveVersions("fraud_detector")` results, and the underlying model artifacts stored via `MLWriter` at the original storage path remain intact and loadable via `MLReader`

### AC-2: Expected Output — Archived model metadata retained

- **Given** model version "1.0.0" of "churn_predictor" has been archived with reason "Replaced by improved pipeline"
- **When** the data platform administrator calls `registry.getArchivedVersion("churn_predictor", "1.0.0")`
- **Then** the returned metadata includes the model name "churn_predictor", version "1.0.0", the original registration timestamp, the archive timestamp, the archive reason "Replaced by improved pipeline", the archiving administrator identity, and the storage path to the persisted model artifacts

### AC-3: Error Handling — Archive non-existent model version

- **Given** no model named "nonexistent_model" exists in the model registry
- **When** the data platform administrator calls `registry.archiveModelVersion("nonexistent_model", "1.0.0")`
- **Then** the system raises a `NoSuchElementException` with message containing "Model 'nonexistent_model' version '1.0.0' not found in registry"

### AC-4: Edge Case — Archive already archived version

- **Given** model version "1.0.0" of "sentiment_analyzer" has already been archived
- **When** the data platform administrator calls `registry.archiveModelVersion("sentiment_analyzer", "1.0.0")`
- **Then** the system raises an `IllegalStateException` with message containing "Model version is already archived"

### AC-5: Restore archived model

- **Given** model version "2.1.0" of "recommendation_engine" is in "Archived" status
- **When** the data platform administrator calls `registry.restoreModelVersion("recommendation_engine", "2.1.0")`
- **Then** the model version status changes to "Development" stage, the model reappears in `registry.listActiveVersions("recommendation_engine")`, and an audit record captures the restore action with timestamp and administrator identity

### AC-6: Batch archive operation

- **Given** model "click_predictor" has 3 versions ("1.0.0", "1.1.0", "1.2.0") all in "Development" stage
- **When** the data platform administrator calls `registry.batchArchive("click_predictor", versions=["1.0.0", "1.1.0"], reason="Consolidating to latest")`
- **Then** both versions "1.0.0" and "1.1.0" are archived, version "1.2.0" remains active, and the operation returns a summary indicating 2 versions archived and 0 failures

## Sub-Tasks

- Design archive metadata schema (archived flag, archive timestamp, archive reason, archiver identity, restore history) for KVStore persistence
- Implement `archiveModelVersion(modelName, version, reason)` API method that updates KVStore entry status to "Archived" and records audit metadata
- Implement `restoreModelVersion(modelName, version)` API method that changes status back to "Development" and logs restore audit trail
- Implement `getArchivedVersion(modelName, version)` to retrieve full metadata for archived versions including archive reason and timestamps
- Implement `listArchivedVersions(modelName)` to query KVStore for all archived versions of a given model, sorted by archive timestamp descending
- Implement `batchArchive(modelName, versions, reason)` for bulk archival with transactional semantics (all-or-nothing)
- Add validation to prevent archiving a model version that is in "Production" stage without an explicit force flag
- Ensure all archive and restore operations write audit trail entries to KVStore with timestamp, actor, action, and reason
- Add PySpark API bindings for archive, restore, and list-archived operations
- Write unit tests for archive, restore, batch archive, error cases, and audit trail
- Write integration test for full lifecycle: register → version → promote → archive → restore → re-promote

## Edge Cases

### 1. Empty/Null Input

Calling `archiveModelVersion` with null or empty string for model name or version — the system must raise an `IllegalArgumentException` with message "Model name and version must be non-empty strings".

### 2. Boundary Values — Sole Production Version

Attempting to archive a model version that is the only active version in "Production" stage for a model — the system must reject with `IllegalStateException` "Cannot archive the sole Production version without force flag; use force=true to override" to prevent accidental production outage.

### 3. Invalid Input — Malformed Version String

Calling `archiveModelVersion` with a version string that does not match semantic versioning format (e.g., "abc", "1.2", negative numbers) — the system must raise an `IllegalArgumentException` with message "Version must follow semantic versioning format (major.minor.patch)".

### 4. Concurrent Archive — Simultaneous Requests

Two administrators simultaneously attempting to archive the same model version — the system must handle this via KVStore thread safety (backed by `ConcurrentHashMap` in `InMemoryStore`), ensuring exactly one request succeeds and the second receives an `IllegalStateException` "Model version is already archived".

### 5. Storage Integrity — Artifact Preservation After Archival

After archiving, the model artifacts stored via `MLWriter` at the original path remain fully intact — calling `MLReader.load(originalPath)` must return the model without corruption or missing data. The archive operation modifies only the registry metadata in KVStore and never touches the underlying model artifact files.

## Dependencies

- **FEATURE-004-02** (Model Versioning and Registry) — parent feature providing the registry infrastructure and versioning capabilities that this story extends with archival functionality
- **FEATURE-004-01** (Experiment Metadata Logging) — experiment run context for linking audit trail entries to originating training runs
- **STORY-004-02-01** (Register Trained Model Artifacts) — models must be registered in the registry before they can be archived
- **STORY-004-02-02** (Version Models with Lineage) — versioned models with semantic versioning are the target of archival operations
- **STORY-004-02-03** (Promote Models Across Stages) — stage information (Development, Staging, Production) determines archive eligibility rules and the production-safety guard
- **KVStore interface** (`Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java`) — persistence backend for archive metadata, providing `read(Class<T>, Object)`, `write(Object)`, `delete(Class<?>, Object)`, `view(Class<T>)`, and `count(Class<?>)` methods for managing archived model version records
- **InMemoryStore** (`Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/InMemoryStore.java`) — thread-safe in-memory KVStore implementation using `ConcurrentHashMap` for development and testing of archive operations; throws `NoSuchElementException` when reading non-existent entries
- **MLWriter/MLReader** (`Source: mllib/src/main/scala/org/apache/spark/ml/util/ReadWrite.scala`) — model artifact persistence APIs; `MLWriter.save(path)` persists model artifacts and `MLReader.load(path)` restores them; `DefaultParamsWriter` serializes JSON metadata with keys `class`, `timestamp`, `sparkVersion`, `uid`, and `paramMap`; archived models must remain loadable via these APIs
- **Model abstract class** (`Source: mllib/src/main/scala/org/apache/spark/ml/Model.scala`) — `Model[M <: Model[M]] extends Transformer` with `parent: Estimator[M]` for lineage tracking via `setParent`/`hasParent`, and `estimatedSize` for storage metrics; serves as the base type for all registry entries subject to archival

## Story Estimation Guidance

- **Story Points: 5** (Fibonacci scale)
- **Rationale:** Moderate complexity involving an archive/restore state machine with audit trail, batch operations, a production-safety guard, and PySpark parity. KVStore integration for the soft-delete pattern is well-understood from existing Spark UI status stores (e.g., History Server). No complex algorithmic work is required, but careful concurrency handling, input validation logic, and comprehensive test coverage are needed.
- **Reference:** Similar in scope to implementing a soft-delete pattern with audit logging in the existing Spark History Server status store, which uses the same KVStore interface for persisting application metadata with lifecycle state transitions.

## Definition of Done

- `archiveModelVersion` API marks a model version as archived in KVStore with full audit metadata (timestamp, reason, administrator identity)
- Archived models no longer appear in active version listings (`listActiveVersions`) or promotion candidate lists
- Archived model metadata remains queryable via `getArchivedVersion` and `listArchivedVersions`
- `restoreModelVersion` API transitions archived versions back to "Development" stage with a complete audit trail
- Batch archive operation processes multiple versions atomically with all-or-nothing transactional semantics
- Production-stage versions require an explicit force flag for archival to prevent accidental production outages
- Model artifacts stored via `MLWriter` remain intact and loadable via `MLReader` after archival — the archive operation modifies only registry metadata
- All archive and restore operations generate audit trail entries in KVStore containing timestamp, actor identity, action type, and reason
- PySpark API parity for all archive and restore operations (archive, restore, batch archive, list archived, get archived)
- Unit tests achieve 100% branch coverage for archive, restore, batch archive, and error paths
- Integration test validates the full model lifecycle: register → version → promote → archive → restore → re-promote
- Documentation covers archive API usage, restore workflow, production safety guards, and batch operations
