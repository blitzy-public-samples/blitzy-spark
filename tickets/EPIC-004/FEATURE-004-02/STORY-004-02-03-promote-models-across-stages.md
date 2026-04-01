# Promote Model Versions Across Lifecycle Stages to Govern the Path from Development to Production

## User Story

**As a** data platform administrator, **I want to** move registered model versions through three defined lifecycle stages — Development, Staging, and Production — with each transition recorded as an immutable audit trail entry in the KVStore-backed registry, including the administrator identity, transition timestamp, source stage, target stage, and optional justification note, **so that** I can enforce a governed model deployment workflow that prevents untested models from reaching production, provide traceability for regulatory compliance, and ensure only one model version occupies each stage per model at a time — eliminating manual deployment tracking spreadsheets and reducing model deployment errors by enforcing stage-gate transitions.

## Acceptance Criteria

### AC 1: Input Validation — Valid Promotion from Development to Staging

- **Given** a registered model "fraud_detector" with version "3.0.0" currently in "Development" stage in the KVStore-backed model registry
- **When** the data platform administrator calls `registry.promoteVersion("fraud_detector", "3.0.0", targetStage="Staging", note="Passed offline evaluation with areaUnderROC=0.95")`
- **Then** the model version stage is updated to "Staging" in the KVStore, an audit trail entry is created containing the administrator identity, timestamp, source stage "Development", target stage "Staging", and the provided note, and `registry.getVersionStage("fraud_detector", "3.0.0")` returns "Staging"

### AC 2: Expected Output — Promotion from Staging to Production with Stage Replacement

- **Given** model "churn_predictor" has version "2.0.0" in "Production" stage and version "3.0.0" in "Staging" stage
- **When** the data platform administrator calls `registry.promoteVersion("churn_predictor", "3.0.0", targetStage="Production", note="Approved by ML review board")`
- **Then** version "3.0.0" stage is updated to "Production", version "2.0.0" is automatically demoted from "Production" to "Staging", both transitions are recorded as separate audit trail entries in KVStore, and `registry.getProductionVersion("churn_predictor")` returns version "3.0.0"

### AC 3: Error Handling — Invalid Stage Transition

- **Given** model "sentiment_analyzer" has version "1.0.0" currently in "Development" stage
- **When** the data platform administrator calls `registry.promoteVersion("sentiment_analyzer", "1.0.0", targetStage="Production")`
- **Then** the system raises an `IllegalStateException` with message containing "Cannot promote from 'Development' to 'Production'; must transition through 'Staging' first"

### AC 4: Error Handling — Non-Existent Version

- **Given** model "text_classifier" exists but has no version "9.9.9" in the registry
- **When** the data platform administrator calls `registry.promoteVersion("text_classifier", "9.9.9", targetStage="Staging")`
- **Then** the system raises a `NoSuchElementException` with message containing "Version '9.9.9' not found for model 'text_classifier'"

### AC 5: Edge Case — Demote from Production to Staging

- **Given** model "recommendation_engine" has version "2.0.0" currently in "Production" stage
- **When** the data platform administrator calls `registry.demoteVersion("recommendation_engine", "2.0.0", targetStage="Staging", note="Performance degradation detected in A/B test")`
- **Then** version "2.0.0" stage is updated to "Staging", an audit trail entry records the demotion with reason, and no other version is automatically promoted to Production

### AC 6: Audit Trail — Full Promotion History Retrieval

- **Given** model "click_predictor" version "1.0.0" has been promoted from Development to Staging and then from Staging to Production over two separate operations
- **When** the data platform administrator calls `registry.getPromotionHistory("click_predictor", "1.0.0")`
- **Then** the system returns a chronologically ordered list of 2 audit entries, each containing: timestamp, administrator identity, source stage, target stage, and note, with the first entry showing Development → Staging and the second showing Staging → Production

## Sub-Tasks

- [ ] Define the stage enumeration: Development, Staging, Production (with ordering constraints: Development → Staging → Production for promotions; reverse for demotions)
- [ ] Design audit trail entry schema for KVStore: modelName, version, timestamp, administratorId, sourceStage, targetStage, note, actionType (promote/demote)
- [ ] Implement `promoteVersion(modelName, version, targetStage, note)` API that validates the transition is allowed (Development→Staging or Staging→Production), updates KVStore via `write()`, and handles automatic demotion of the current occupant of the target stage
- [ ] Implement `demoteVersion(modelName, version, targetStage, note)` API for Production→Staging or Staging→Development transitions with audit trail
- [ ] Implement stage exclusivity logic: only one version per model can occupy "Production" at a time; when a new version is promoted, the previous occupant is automatically demoted one stage
- [ ] Implement `getVersionStage(modelName, version)` to retrieve current stage via KVStore `read()`
- [ ] Implement `getProductionVersion(modelName)` and `getStagingVersion(modelName)` convenience methods using KVStore `view()` with index filtering
- [ ] Implement `getPromotionHistory(modelName, version)` to retrieve chronological audit trail from KVStore using `view()` with index ordering
- [ ] Add PySpark API bindings for promote, demote, get stage, and get history operations
- [ ] Write unit tests for: valid promotion, invalid skip (Dev→Prod), stage replacement, demotion, history retrieval, and error cases
- [ ] Write integration test for full lifecycle: register → promote to Staging → promote to Production → demote back → re-promote

## Edge Cases

### 1. Empty/Null Input

Calling `promoteVersion` with null or empty model name, version, or targetStage — the system must raise `IllegalArgumentException` with message "Model name, version, and target stage must be non-empty strings".

### 2. Boundary Values — Promote to Same Stage

Calling `promoteVersion("model", "1.0.0", targetStage="Development")` when the model is already in "Development" — the system must raise `IllegalStateException` with message "Model version is already in 'Development' stage".

### 3. Invalid Input — Unknown Stage Name

Calling `promoteVersion("model", "1.0.0", targetStage="QA")` with a stage not in the allowed set — the system must raise `IllegalArgumentException` with message "Unknown stage 'QA'; allowed stages are: Development, Staging, Production".

### 4. Concurrent Promotions

Two administrators simultaneously promoting different versions of the same model to "Production" — KVStore thread safety must ensure only one succeeds, the other receives `IllegalStateException` "Another version was promoted to Production during this operation; please retry".

### 5. Promote Archived Version

Attempting to promote a version that has been archived (STORY-004-02-05) — the system must raise `IllegalStateException` with message "Cannot promote archived model version; restore it first using restoreModelVersion".

## Dependencies

- **FEATURE-004-02** (Model Versioning and Registry) — parent feature defining the model registry scope and lifecycle management capability
  - Link: [FEATURE-004-02-model-versioning-and-registry](../FEATURE-004-02-model-versioning-and-registry.md)
- **FEATURE-004-01** (Experiment Metadata Logging) — experiment context for promotion justification and linking training runs to model versions
  - Link: [FEATURE-004-01-experiment-metadata-logging](../FEATURE-004-01-experiment-metadata-logging.md)
- **STORY-004-02-01** (Register Trained Model Artifacts) — models must be registered in the registry before they can be promoted through stages
  - Link: [STORY-004-02-01-register-trained-model-artifacts](./STORY-004-02-01-register-trained-model-artifacts.md)
- **STORY-004-02-02** (Version Models with Lineage) — versioned models with semantic version identifiers are the targets for stage promotion
  - Link: [STORY-004-02-02-version-models-with-lineage](./STORY-004-02-02-version-models-with-lineage.md)
- **KVStore interface** — persistence layer for stage metadata and audit trail entries using `write()`, `read()`, `view()`, and `count()` methods; KVStore instances are thread-safe for both reads and writes
  - `Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java`
- **InMemoryStore** — thread-safe in-memory KVStore backend using `ConcurrentHashMap` for development and concurrent operation testing; throws `NoSuchElementException` on missing keys
  - `Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/InMemoryStore.java`
- **Model abstract class** — `Model[M <: Model[M]] extends Transformer` with `parent: Estimator[M]` reference for lineage tracking via `setParent`/`hasParent`, serving as the base type for all model registry entries
  - `Source: mllib/src/main/scala/org/apache/spark/ml/Model.scala`
- **MLWriter/MLReader persistence APIs** — `MLWriter.save(path)` and `MLReader.load(path)` for model artifact persistence; `DefaultParamsWriter` serializes metadata as JSON with keys: `class`, `timestamp`, `sparkVersion`, `uid`, `paramMap`; model artifact paths are referenced during stage transitions
  - `Source: mllib/src/main/scala/org/apache/spark/ml/util/ReadWrite.scala`

## Story Estimation Guidance

- **Story Points: 5** (Fibonacci scale)
- **Rationale:** Moderate complexity involving a stage-gate state machine with three ordered stages and directed transition constraints, automatic demotion of incumbent versions occupying the target stage, immutable audit trail persistence in KVStore, concurrent access protection leveraging KVStore thread safety, and PySpark API parity. The state transitions are well-defined (3 stages, directed transitions: Development → Staging → Production for promotions, reverse for demotions) but require careful handling of stage exclusivity and audit trail atomicity.
- **Reference:** Similar in complexity to implementing role-based access transitions in the existing Spark History Server configuration.
- **Breakdown:** ~1 point for stage enumeration and validation logic, ~1 point for promote/demote APIs with KVStore integration, ~1 point for stage exclusivity and automatic demotion, ~1 point for audit trail and history query, ~1 point for PySpark bindings and testing.

## Definition of Done

- [ ] `promoteVersion` API transitions model versions through Development → Staging → Production stages with validation that skipping stages is not allowed
- [ ] `demoteVersion` API transitions model versions in the reverse direction (Production → Staging → Development)
- [ ] Stage exclusivity enforced: only one version per model occupies "Production" at a time, with automatic demotion of the prior occupant
- [ ] Every promotion and demotion creates an immutable audit trail entry in KVStore with timestamp, administrator identity, source stage, target stage, and justification note
- [ ] `getVersionStage`, `getProductionVersion`, `getStagingVersion`, and `getPromotionHistory` query APIs are implemented and return accurate results from KVStore
- [ ] Invalid transitions (skip stages, promote to same stage, promote archived version) produce clear error messages with specific exception types (`IllegalStateException`, `IllegalArgumentException`, `NoSuchElementException`)
- [ ] Concurrent promotions to the same stage are handled safely via KVStore thread safety, with conflict detection and retry guidance
- [ ] PySpark API parity for all promotion, demotion, and query operations
- [ ] Unit tests cover all valid transitions, invalid transitions, stage replacement, demotion, history retrieval, concurrency, and error scenarios
- [ ] Integration test validates the full promotion lifecycle from registration through production deployment and back
- [ ] Documentation covers stage model, promotion API, demotion workflow, audit trail queries, and concurrent access behavior
