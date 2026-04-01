# Implement Experiment Metadata Logging to Capture ML Pipeline Run Details Automatically

## Feature Summary

ML engineers and data scientists currently lack built-in mechanisms to track experiment hyperparameters, evaluation metrics, and pipeline configurations across training runs, forcing reliance on external tools such as MLflow or Weights & Biases that introduce integration overhead and operational complexity. This feature provides native metadata logging that automatically captures and persists experiment details during MLlib pipeline execution — including hyperparameters extracted from Estimator `ParamMap` instances, evaluation metrics from all `Evaluator` subclasses, and full pipeline stage configurations — enabling reproducibility and run-to-run comparison without external dependencies. The scope integrates with the existing MLlib Pipeline API (`Pipeline`, `Estimator`, `Transformer`, `Evaluator`), the ML Events system (`FitStart`, `FitEnd`, `TransformStart`, `TransformEnd`), and the KVStore backend for metadata persistence, while excluding model artifact management which is covered by FEATURE-004-02.

## User Stories Index

| Story ID | Story Name | Description | Link |
|----------|-----------|-------------|------|
| STORY-004-01-01 | Log Hyperparameters Per Run | Capture and persist hyperparameter values from Estimator ParamMap for each training run | [STORY-004-01-01](./FEATURE-004-01/STORY-004-01-01-log-hyperparameters-per-run.md) |
| STORY-004-01-02 | Record Evaluation Metrics | Log evaluation metric results from all Evaluator types with model and run association | [STORY-004-01-02](./FEATURE-004-01/STORY-004-01-02-record-evaluation-metrics.md) |
| STORY-004-01-03 | Capture Pipeline Configuration | Snapshot the complete pipeline stage configuration including all PipelineStage parameters as JSON | [STORY-004-01-03](./FEATURE-004-01/STORY-004-01-03-capture-pipeline-configuration.md) |
| STORY-004-01-04 | Tag and Annotate Experiments | Add custom key-value tags and free-text annotations to experiment runs via programmatic API | [STORY-004-01-04](./FEATURE-004-01/STORY-004-01-04-tag-and-annotate-experiments.md) |
| STORY-004-01-05 | Query Experiment History | Search and filter historical experiment metadata by time range, tag, metric threshold, and pipeline type | [STORY-004-01-05](./FEATURE-004-01/STORY-004-01-05-query-experiment-history.md) |

## Dependencies

- **EPIC-004** (Native ML Experiment Tracking and Model Registry) — parent epic providing the overall vision and coordination for experiment tracking and model registry capabilities
- **F-004** (Machine Learning Pipelines / MLlib) — provides the Pipeline API, Estimator, Transformer, and Evaluator interfaces that this feature hooks into for metadata capture
- **MLlib Pipeline persistence** (`mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`) — `Pipeline` class extends `Estimator[PipelineModel]` with `MLWritable`; the `stages` Param holds an array of `PipelineStage` instances; `Pipeline.fit()` iterates stages calling `Estimator.fit()` and `Transformer.transform()` in sequence to produce a `PipelineModel`
  - `Source: mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala`
- **ML Events system** (`mllib/src/main/scala/org/apache/spark/ml/events.scala`) — the `MLEvent` sealed trait extends `SparkListenerEvent` and provides lifecycle hooks: `FitStart[M]` (fired before `Estimator.fit`), `FitEnd[M]` (fired after `Estimator.fit`), `TransformStart` (fired before `Transformer.transform`), `TransformEnd` (fired after `Transformer.transform`), `SaveInstanceStart`/`SaveInstanceEnd`, and `LoadInstanceStart[T]`/`LoadInstanceEnd[T]`; the `MLEvents` trait provides helper methods `withFitEvent` and `withTransformEvent` that bracket execution with event posting via `SparkContext.listenerBus`
  - `Source: mllib/src/main/scala/org/apache/spark/ml/events.scala`
- **Estimator abstract class** (`mllib/src/main/scala/org/apache/spark/ml/Estimator.scala`) — `Estimator[M <: Model[M]]` extends `PipelineStage`; provides `fit(dataset)` returning a fitted `Model[M]`; supports `fit(dataset, paramMap)` for parameter overrides and `fit(dataset, paramMaps)` for multi-model training
  - `Source: mllib/src/main/scala/org/apache/spark/ml/Estimator.scala`
- **Model abstract class** (`mllib/src/main/scala/org/apache/spark/ml/Model.scala`) — `Model[M <: Model[M]]` extends `Transformer`; holds a `parent: Estimator[M]` reference enabling lineage from fitted model back to the originating Estimator and its parameters
  - `Source: mllib/src/main/scala/org/apache/spark/ml/Model.scala`
- **Estimator/Transformer parameters** (`mllib/src/main/scala/org/apache/spark/ml/param/`) — `Param`, `ParamMap`, and `ParamPair` classes used by all `PipelineStage` subclasses to declare and store hyperparameters; `extractParamMap()` enables retrieval of all set parameters
- **Evaluator classes** (`mllib/src/main/scala/org/apache/spark/ml/evaluation/Evaluator.scala`) — abstract `Evaluator` extends `Params` with `evaluate(dataset): Double` and `isLargerBetter` flag; concrete implementations include `BinaryClassificationEvaluator`, `RegressionEvaluator`, `MulticlassClassificationEvaluator`, `ClusteringEvaluator`, `RankingEvaluator`, and `MultilabelClassificationEvaluator`
  - `Source: mllib/src/main/scala/org/apache/spark/ml/evaluation/Evaluator.scala`
- **KVStore backend** (`common/kvstore/`) — `KVStore` Java interface providing `read(Class<T>, Object)`, `write(Object)`, `delete(Class<T>, Object)`, `view(Class<T>)`, `getMetadata(Class<T>)`, and `setMetadata(Object)` methods; backends include `InMemoryStore`, `LevelDB`, and `RocksDB` for configurable persistence durability
  - `Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java`
  - `Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/InMemoryStore.java`
- **ReadWrite utilities** (`mllib/src/main/scala/org/apache/spark/ml/util/ReadWrite.scala`) — `MLWriter`/`MLReader` base classes; `DefaultParamsWriter` serializes pipeline metadata as JSON including `class`, `timestamp`, `sparkVersion`, `uid`, and `paramMap` keys; used as the persistence format reference for experiment metadata serialization
  - `Source: mllib/src/main/scala/org/apache/spark/ml/util/ReadWrite.scala`
- **Spark Web UI** (`core/src/main/scala/org/apache/spark/ui/`) — extensible tab-based UI framework for adding an experiment browsing interface
- **MLlib documentation** — existing documentation provides context for feature positioning and API reference
  - `Source: docs/ml-guide.md`
  - `Source: docs/ml-pipeline.md`

## Definition of Done

- All 5 user stories (STORY-004-01-01 through STORY-004-01-05) are implemented, tested, and integrated into a cohesive metadata logging subsystem
- Hyperparameters are automatically extracted from each Estimator's `ParamMap` (via `extractParamMap()`) and persisted to the KVStore on every `fit()` call, with each entry keyed by a unique run identifier that includes a timestamp and the Estimator's `uid`
- Evaluation metrics from all six Evaluator types (`BinaryClassificationEvaluator`, `RegressionEvaluator`, `MulticlassClassificationEvaluator`, `ClusteringEvaluator`, `RankingEvaluator`, `MultilabelClassificationEvaluator`) are captured upon `evaluate()` invocation and stored with a reference to the associated model run
- Complete pipeline configuration — all `PipelineStage` instances with their declared parameters, stage ordering, and stage class names — is serialized as a JSON document (following the `DefaultParamsWriter` metadata format with `class`, `timestamp`, `sparkVersion`, `uid`, and `paramMap` keys) and stored per run
- Custom key-value tags (string-to-string pairs) and free-text annotations can be added to any experiment run via a programmatic API available in Scala, Python, and Java
- Experiment history is queryable by time range (start/end timestamps), tag key-value match, metric threshold (greater-than or less-than a specified value), and pipeline type (by stage class name) via both a programmatic API and the Spark Web UI experiments tab
- Metadata storage uses the `KVStore` interface, supporting `InMemoryStore` for development and testing, `LevelDB` for lightweight persistent storage, and `RocksDB` for production-grade persistent storage
- Metadata logging hooks into the existing ML Events system (`FitStart`, `FitEnd`, `TransformStart`, `TransformEnd`) via `SparkListenerBus` without modifying the core event firing mechanism
- Unit tests cover each metadata capture pathway: hyperparameter extraction and persistence, evaluation metric recording, pipeline configuration serialization, tag and annotation CRUD operations, and history query filtering
- Integration tests validate end-to-end metadata logging during `Pipeline.fit()` execution with multi-stage pipelines (at least 3 stages including both Estimators and Transformers) and `Evaluator.evaluate()` calls
- PySpark API parity is achieved: all metadata logging, tagging, annotation, and querying operations are accessible from Python with identical functionality to the Scala API
- Documentation covers API usage examples (Scala, Python, Java), storage backend configuration, query syntax and filtering options, and integration with the Spark Web UI
- Storage overhead per experiment run is measured and documented, with a target of less than 1 MB per run for metadata-only storage (excluding model artifacts)
- No regressions are introduced in existing MLlib Pipeline `fit()`, `transform()`, or `evaluate()` execution paths — metadata logging is additive and does not alter existing return values or side effects
