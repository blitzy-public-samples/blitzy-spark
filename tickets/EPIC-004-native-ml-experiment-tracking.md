# Integrate Native ML Experiment Tracking and Model Registry into MLlib Pipelines

## Epic Summary

Apache Spark's MLlib provides a robust Pipeline API for building machine learning workflows with Estimators, Transformers, and model persistence, yet it lacks built-in experiment tracking capabilities — forcing teams to integrate external tools such as MLflow or Weights & Biases, which adds operational complexity, increases deployment overhead, and fragments the ML lifecycle across disconnected systems. This epic introduces native experiment metadata logging, model versioning, and a model registry that integrate directly into the MLlib Pipeline execution lifecycle, leveraging the existing KVStore backend (`common/kvstore/`) for metadata persistence and the Pipeline save/load APIs (`mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`) for model artifact management. The scope covers automatic capture of hyperparameters, evaluation metrics, and pipeline configurations during training runs; experiment tagging, annotation, and history querying; and a model registry supporting artifact registration, semantic versioning with lineage, stage promotion, cross-version comparison, and archival — while explicitly excluding external tool replacement, model serving infrastructure, and real-time inference endpoints.

## Features Index

| Feature ID | Feature Name | Description | Link |
|---|---|---|---|
| FEATURE-004-01 | Experiment Metadata Logging | Automatic capture and storage of hyperparameters, evaluation metrics, pipeline configurations, and experiment annotations during MLlib Pipeline execution, with a queryable API for browsing experiment history | [FEATURE-004-01-experiment-metadata-logging.md](./EPIC-004/FEATURE-004-01-experiment-metadata-logging.md) |
| FEATURE-004-02 | Model Versioning and Registry | A centralized model registry for registering trained model artifacts, tracking version lineage, promoting models across lifecycle stages, comparing model versions side-by-side, and archiving deprecated models | [FEATURE-004-02-model-versioning-and-registry.md](./EPIC-004/FEATURE-004-02-model-versioning-and-registry.md) |

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
