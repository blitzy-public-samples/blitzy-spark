# Build Model Versioning and Registry to Enable Lifecycle Management of ML Model Artifacts

## Feature Summary

Data scientists and ML engineers currently manage trained model artifacts manually using file system paths or external tools like MLflow, leading to version confusion, lack of reproducibility, and difficulty promoting models to production. This feature provides a native model registry within Apache Spark that supports registration, semantic versioning with data lineage, stage-based promotion, version comparison, and archival — eliminating external tool dependencies. It extends the existing MLlib model persistence APIs (`MLWriter`/`MLReader` in `mllib/src/main/scala/org/apache/spark/ml/util/ReadWrite.scala`), leverages the `Model.parent` Estimator reference for lineage, uses the KVStore backend (`common/kvstore/`) for registry metadata, and integrates with Evaluator classes for model comparison; model serving, deployment, and real-time inference endpoints are excluded from this scope, and this feature depends on FEATURE-004-01 (Experiment Metadata Logging) for experiment context linkage.

## User Stories Index

| Story ID | Story Name | Description | Link |
|----------|-----------|-------------|------|
| STORY-004-02-01 | Register Trained Model Artifacts | Register a trained model with metadata in the registry | [STORY-004-02-01](./FEATURE-004-02/STORY-004-02-01-register-trained-model-artifacts.md) |
| STORY-004-02-02 | Version Models with Lineage | Assign semantic versions and track data/code lineage per version | [STORY-004-02-02](./FEATURE-004-02/STORY-004-02-02-version-models-with-lineage.md) |
| STORY-004-02-03 | Promote Models Across Stages | Move model versions through development, staging, and production stages | [STORY-004-02-03](./FEATURE-004-02/STORY-004-02-03-promote-models-across-stages.md) |
| STORY-004-02-04 | Compare Model Versions | Side-by-side comparison of metrics and parameters across model versions | [STORY-004-02-04](./FEATURE-004-02/STORY-004-02-04-compare-model-versions.md) |
| STORY-004-02-05 | Archive Deprecated Models | Mark and archive deprecated model versions to manage registry size | [STORY-004-02-05](./FEATURE-004-02/STORY-004-02-05-archive-deprecated-models.md) |

## Dependencies

- **EPIC-004** (Native ML Experiment Tracking and Model Registry) — parent epic providing the overarching scope for experiment tracking and model lifecycle management
- **FEATURE-004-01** (Experiment Metadata Logging) — experiment metadata must be available for linking model versions to their originating training runs, including hyperparameters, evaluation metrics, and pipeline configurations
- **F-004** (Machine Learning Pipelines / MLlib) — Pipeline API, Model class, Estimator/Transformer abstractions, and model persistence infrastructure that this feature extends
- **Model abstract class** (`Source: mllib/src/main/scala/org/apache/spark/ml/Model.scala`) — provides `Model[M <: Model[M]] extends Transformer` with `parent: Estimator[M]` for lineage tracking via `setParent`/`hasParent`, and `estimatedSize` via `SizeEstimator` for storage metrics
- **Pipeline and PipelineModel** (`Source: mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`) — `Pipeline` chains `Estimator`s and `Transformer`s via `stages` parameter; `PipelineModel` wraps fitted stages and supports `save`/`load` for end-to-end pipeline persistence
- **MLWriter/MLReader persistence APIs** (`Source: mllib/src/main/scala/org/apache/spark/ml/util/ReadWrite.scala`) — `MLWriter.save(path)`, `MLReader.load(path)`, `DefaultParamsWriter` saves JSON metadata to `path/metadata` with keys: `class`, `timestamp`, `sparkVersion`, `uid`, `paramMap`, `defaultParamMap`; `DefaultParamsReader` loads metadata back; `GeneralMLWriter` supports pluggable export formats via `MLFormatRegister`
- **Evaluator classes** (`Source: mllib/src/main/scala/org/apache/spark/ml/evaluation/Evaluator.scala`) — abstract `Evaluator` with `evaluate(dataset: Dataset[_]): Double` and `isLargerBetter: Boolean`; concrete implementations include `BinaryClassificationEvaluator` (areaUnderROC, areaUnderPR), `RegressionEvaluator` (rmse, mse, r2, mae), `MulticlassClassificationEvaluator`, `ClusteringEvaluator`, `RankingEvaluator`, and `MultilabelClassificationEvaluator` — all used for model version comparison
- **KVStore backend** (`Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java`) — provides the `KVStore` interface with methods `read(Class<T>, Object)`, `write(Object)`, `delete(Class<T>, Object)`, `view(Class<T>)`, `getMetadata(Class<T>)`, `setMetadata(Object)` for registry metadata persistence; supported implementations include `InMemoryStore`, `LevelDB`, and `RocksDB`
- **Spark Web UI** (`Source: core/src/main/scala/org/apache/spark/ui/`) — for model registry browsing and visual inspection of registered models, versions, and stage transitions

### Reference Documentation

- `Source: docs/ml-guide.md` — MLlib documentation landing page covering ML algorithms, featurization, pipelines, persistence, and utilities
- `Source: docs/ml-pipeline.md` — Pipeline API documentation including DataFrame-based ML concepts (Transformer, Estimator, Pipeline, Parameter), pipeline construction, and the persistence section covering `save`/`load` across Scala, Java, and Python

## Definition of Done

- All 5 user stories (STORY-004-02-01 through STORY-004-02-05) are complete and integrated into a cohesive model registry capability
- Trained models can be registered in the registry with a unique name, human-readable description, and linkage to the originating experiment run from FEATURE-004-01
- Each registered model supports semantic versioning using the `major.minor.patch` format with automatic minor version increment on new registrations under the same model name
- Data lineage is recorded per model version including: training dataset identifier, pipeline configuration hash derived from `DefaultParamsWriter` JSON metadata (`class`, `timestamp`, `sparkVersion`, `uid`, `paramMap`), Estimator parameters extracted via `Model.parent`, and parent experiment run ID from FEATURE-004-01
- Models can be promoted through three defined stages — Development, Staging, and Production — with each transition recorded as an audit trail entry containing the promoting user, timestamp, source stage, and target stage
- Side-by-side comparison of up to 5 model versions displays evaluation metrics from all supported `Evaluator` types (`BinaryClassificationEvaluator`, `RegressionEvaluator`, `MulticlassClassificationEvaluator`, `ClusteringEvaluator`, `RankingEvaluator`, `MultilabelClassificationEvaluator`), hyperparameters, and training metadata in a tabular format
- Deprecated models can be archived via soft-deletion, removing them from the active registry listing while retaining artifacts in underlying storage accessible through `MLReader.load(path)`
- Registry metadata is persisted via the `KVStore` interface supporting `InMemoryStore` for testing, `LevelDB` for lightweight deployments, and `RocksDB` for production-scale registries
- Model artifacts remain stored via the existing `MLWriter`/`MLReader` persistence mechanism — the registry manages metadata pointers and lifecycle state only, not the binary artifacts themselves
- Unit tests cover all five operations: registration, versioning, promotion, comparison, and archival with both success and failure paths
- Integration tests validate the full model lifecycle: train a model via `Pipeline.fit` → register with metadata → create new version → promote through Development to Staging to Production → compare two versions → archive a deprecated version
- PySpark API parity is maintained for all registry operations, with Python wrappers mirroring the Scala/Java API surface
- Documentation covers the registry API, version management semantics, promotion workflow with stage definitions, comparison usage with Evaluator integration, and archival behavior
- Registry supports concurrent model registrations from parallel SparkSessions without data corruption or version conflicts, enforced via optimistic locking in the KVStore layer
