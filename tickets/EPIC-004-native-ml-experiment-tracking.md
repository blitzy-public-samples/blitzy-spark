# Integrate Native ML Experiment Tracking and Model Registry into MLlib Pipelines

## Epic Summary

Apache Spark's MLlib provides a robust Pipeline API for building machine learning workflows with Estimators, Transformers, and model persistence, yet it lacks built-in experiment tracking capabilities — forcing teams to integrate external tools such as MLflow or Weights & Biases, which adds operational complexity, increases deployment overhead, and fragments the ML lifecycle across disconnected systems. This epic introduces native experiment metadata logging, model versioning, and a model registry that integrate directly into the MLlib Pipeline execution lifecycle, leveraging the existing KVStore backend (`common/kvstore/`) for metadata persistence and the Pipeline save/load APIs (`mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`) for model artifact management. The scope covers automatic capture of hyperparameters, evaluation metrics, and pipeline configurations during training runs; experiment tagging, annotation, and history querying; and a model registry supporting artifact registration, semantic versioning with lineage, stage promotion, cross-version comparison, and archival — while explicitly excluding external tool replacement, model serving infrastructure, and real-time inference endpoints.

## Features Index

| Feature ID | Feature Name | Description | Link |
|---|---|---|---|
| FEATURE-004-01 | Experiment Metadata Logging | Automatic capture and storage of hyperparameters, evaluation metrics, pipeline configurations, and experiment annotations during MLlib Pipeline execution, with a queryable API for browsing experiment history | [FEATURE-004-01-experiment-metadata-logging.md](./EPIC-004/FEATURE-004-01-experiment-metadata-logging.md) |
| FEATURE-004-02 | Model Versioning and Registry | A centralized model registry for registering trained model artifacts, tracking version lineage, promoting models across lifecycle stages, comparing model versions side-by-side, and archiving deprecated models | [FEATURE-004-02-model-versioning-and-registry.md](./EPIC-004/FEATURE-004-02-model-versioning-and-registry.md) |

## Execution Strategy

### Parallel Epic Execution

This epic has **no cross-epic dependencies** and can be executed simultaneously with EPIC-001, EPIC-002, EPIC-003, and EPIC-005. Each epic operates on independent Spark subsystems with distinct module boundaries, enabling all five epics to run in parallel with separate development teams.

### Implementation Phases — Backend Before Frontend

All stories within this epic are organized into two sequential phases. **Phase 1 (Backend)** must be completed and fully tested before **Phase 2 (Frontend)** begins, ensuring that all metadata logging, model registration, versioning, promotion, and archival services are stable before comparison UI and reporting presentation work starts.

#### Phase 1 — Backend (Metadata Logging, Registry, Versioning, and API Layer)

Backend stories establish the experiment metadata capture pipeline, KVStore persistence, model registry CRUD operations, versioning logic, promotion workflow, and query API. These must be implemented and pass all unit and integration tests before Phase 2 begins.

| Story ID | Story Name | Feature | Rationale |
|----------|-----------|---------|-----------|
| STORY-004-01-01 | Log Hyperparameters Per Run | FEATURE-004-01 | Core metadata capture — foundation for experiment tracking |
| STORY-004-01-02 | Record Evaluation Metrics | FEATURE-004-01 | Metrics persistence — backend logging pipeline |
| STORY-004-01-03 | Capture Pipeline Configuration | FEATURE-004-01 | Pipeline serialization — backend reproducibility layer |
| STORY-004-01-04 | Tag and Annotate Experiments | FEATURE-004-01 | Metadata annotation — backend experiment organization |
| STORY-004-01-05 | Query Experiment History | FEATURE-004-01 | Query API — backend data access layer |
| STORY-004-02-01 | Register Trained Model Artifacts | FEATURE-004-02 | Model registration — backend registry core |
| STORY-004-02-02 | Version Models with Lineage | FEATURE-004-02 | Semantic versioning logic — backend version management |
| STORY-004-02-03 | Promote Models Across Stages | FEATURE-004-02 | Lifecycle stage promotion — backend workflow engine |
| STORY-004-02-05 | Archive Deprecated Models | FEATURE-004-02 | Soft-delete archival — backend registry maintenance |

#### Phase 2 — Frontend (Comparison UI and Reporting)

Frontend stories consume data produced by Phase 1 backend services. These must not begin until all Phase 1 stories pass acceptance testing.

| Story ID | Story Name | Feature | Rationale |
|----------|-----------|---------|-----------|
| STORY-004-02-04 | Compare Model Versions | FEATURE-004-02 | Side-by-side version comparison UI — depends on registry data and metrics from Phase 1 |

### Phase Gate Criteria

- **Phase 1 → Phase 2 Gate**: All 9 backend stories must have passing unit tests, passing integration tests, and code review approval before any Phase 2 story begins development
- **Sprint Planning**: Phase 1 stories should be prioritized in Sprints 1–3; Phase 2 story should be planned for Sprint 4 after Phase 1 gate is passed

## Dependencies

- **F-004 — Machine Learning Pipelines (MLlib):** The Pipeline API provides the Estimator, Transformer, and PipelineStage abstractions that experiment tracking hooks into during `fit()` and `transform()` calls. Model persistence via `MLWriter`/`MLReader` underpins artifact registration and versioning.
  - `Source: mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`
  - `Source: mllib/src/main/scala/org/apache/spark/ml/Estimator.scala`
  - `Source: mllib/src/main/scala/org/apache/spark/ml/Transformer.scala`
  - `Source: mllib/src/main/scala/org/apache/spark/ml/events.scala`
- **KVStore Backend:** The embedded key-value store provides indexed, queryable metadata storage for experiment runs, hyperparameter snapshots, metric records, and model registry entries with support for CRUD operations, secondary indices, and ordered iteration.
  - `Source: common/kvstore/`
- **Spark Web UI:** The existing web-based monitoring interface will be extended with experiment browsing views and model comparison displays, enabling interactive exploration of training history and registry contents.
- **ML Pipeline Persistence:** The `MLWriter`/`MLReader` persistence framework and the `DefaultParamsWriter`/`DefaultParamsReader` serialization utilities provide the foundation for storing and loading model artifacts within the registry.
  - `Source: mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`
  - `Source: docs/ml-pipeline.md`
- **MLlib Evaluation Framework:** Evaluator classes (BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator, RankingEvaluator) produce the metrics that experiment tracking captures and stores.
  - `Source: mllib/src/main/scala/org/apache/spark/ml/evaluation/`
- **MLlib Documentation:** Existing MLlib guides provide context for API design consistency and documentation structure.
  - `Source: docs/ml-guide.md`

## Definition of Done

- All child features (FEATURE-004-01: Experiment Metadata Logging and FEATURE-004-02: Model Versioning and Registry) are implemented, tested, and integrated into the MLlib module
- Experiment metadata — including hyperparameters, evaluation metrics, and full pipeline stage configurations — is captured automatically during each `Pipeline.fit()` execution without requiring manual instrumentation by the user
- The experiment history API supports programmatic querying with filtering by experiment name, date range, metric thresholds, and custom tags, returning structured results sorted by any indexed field
- The model registry supports the complete lifecycle: artifact registration with metadata, semantic versioning with data lineage tracking, promotion across defined stages (e.g., Development, Staging, Production), side-by-side comparison of two or more model versions by metrics, and soft-delete archival of deprecated models
- The Spark Web UI displays an experiment tracking tab listing all recorded runs with sortable columns for key metrics, and a model registry view showing registered models with version history and current stage assignments
- Integration tests validate the end-to-end workflow: run a Pipeline with experiment tracking enabled, query the stored experiment, register the resulting model, promote it through stages, compare it against a prior version, and archive the older version
- Unit tests cover all public API methods for both experiment logging and model registry operations, including error paths and boundary conditions
- PySpark bindings expose the experiment tracking and model registry APIs with feature parity to the Scala API
- Documentation covers the experiment tracking API reference, model registry usage guide, configuration options, storage backend selection, and a migration guide for teams transitioning from external tracking tools
- Storage overhead introduced by experiment metadata persistence is benchmarked and documented, confirming that metadata capture adds no more than 5% latency to Pipeline `fit()` execution time
- All acceptance criteria across both features and all child user stories are met and verified through automated test suites
