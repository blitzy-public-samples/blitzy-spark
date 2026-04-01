# Record Evaluation Metrics from Evaluator Results to Track Model Performance Across Training Runs

## User Story

**As a** data scientist, **I want to** automatically capture and persist evaluation metric results — including the metric name, metric value, evaluator class name, evaluator UID, isLargerBetter flag, and evaluator-specific parameters — each time an `Evaluator.evaluate()` call completes, associating each metric record with its corresponding experiment run, **so that** I can maintain a complete, auditable record of model performance across all training runs without manual logging, enabling objective comparison of model quality and identification of performance regressions — reducing metric tracking effort from manual spreadsheet maintenance to zero-touch automated capture.

## Acceptance Criteria

### AC-1: Metric Capture on Evaluator Evaluate (Expected Output)

- **Given** a data scientist has an active experiment run and calls `BinaryClassificationEvaluator.evaluate()` on a predictions DataFrame with the default metricName `"areaUnderROC"`
- **When** the evaluation completes and returns a Double metric value
- **Then** a metric record is persisted to the KVStore associated with the active run ID containing:
  - The evaluator class name `"org.apache.spark.ml.evaluation.BinaryClassificationEvaluator"`
  - The metric name `"areaUnderROC"`
  - The returned Double value as `metricValue`
  - The `isLargerBetter` flag set to `true`
  - The evaluator UID matching the `BinaryClassificationEvaluator` instance UID
  - A millisecond-precision timestamp recorded at metric capture time
  - Evaluator-specific parameters including `numBins=1000`

### AC-2: Multiple Evaluators Within the Same Run (Expected Output)

- **Given** a data scientist evaluates the same model using both `RegressionEvaluator` with metricName `"rmse"` and `RegressionEvaluator` with metricName `"r2"` within the same experiment run
- **When** both evaluations complete
- **Then** two distinct metric records are stored for the run, each with its respective metric name, metric value, and `isLargerBetter` flag (`false` for `"rmse"`, `true` for `"r2"`)
- **And** both records share the same run ID and include the `throughOrigin` parameter value in their evaluator-specific parameters

### AC-3: Non-Finite Metric Value Handling (Input Validation)

- **Given** a data scientist calls `Evaluator.evaluate()` and the evaluation returns a `NaN` or `Infinite` Double value (for example, when `RegressionEvaluator` computes `rmse` on a degenerate dataset)
- **When** the metric recording is triggered
- **Then** the system stores the metric record with the raw `NaN` or `Infinite` value in the `metricValue` field
- **And** the system sets a warning flag `"non_finite_metric_value"` in the `warningFlags` list on the record indicating the metric value is non-finite

### AC-4: KVStore Write Failure Resilience (Error Handling)

- **Given** the KVStore backend (`InMemoryStore`, `LevelDB`, or `RocksDB`) encounters a write failure when `KVStore.write()` is invoked during metric persistence
- **When** the metric recording fails
- **Then** the system catches the `Exception` thrown by `KVStore.write()`
- **And** the system logs a warning message containing the evaluator class name, metric name, and the active run ID
- **And** the `Evaluator.evaluate()` return value is delivered to the caller without interruption — the evaluation pipeline continues identically to execution without metric recording

### AC-5: No Active Experiment Run (Edge Case)

- **Given** no experiment run is currently active (experiment tracking is disabled or not initialized via `ExperimentTracker`)
- **When** an `Evaluator.evaluate()` call completes
- **Then** no metric record is persisted to the KVStore and no error is thrown
- **And** the evaluation operates identically to the baseline behavior without tracking — the returned Double value is unchanged

### AC-6: Evaluator-Specific Parameter Capture (Expected Output)

- **Given** a data scientist uses `RankingEvaluator` with metricName `"ndcgAtK"` and `k=20`
- **When** the evaluation completes
- **Then** the stored metric record includes the evaluator-specific parameter `k=20` in the `evaluatorParams` map in addition to the metric name `"ndcgAtK"` and the computed metric value
- **And** the `isLargerBetter` flag is set to `true` for the `RankingEvaluator`

### AC-7: Metric-to-Run Association (Expected Output)

- **Given** a data scientist calls `evaluate()` on a model produced by a specific `Pipeline.fit()` run that was tracked by STORY-004-01-01 (Log Hyperparameters Per Run)
- **When** the metric is recorded
- **Then** the metric record is linked to the same experiment run ID that captured the hyperparameters during `fit()`
- **And** querying the KVStore by run ID returns both the `ExperimentRunMetadata` (hyperparameters) and all associated `EvaluationMetricRecord` entries for that run

## Sub-Tasks

- Define `EvaluationMetricRecord` data class with the following fields:
  - `runId` (`String`) — the experiment run ID linking to `ExperimentRunMetadata`
  - `evaluatorClassName` (`String`) — fully qualified class name (e.g., `"org.apache.spark.ml.evaluation.BinaryClassificationEvaluator"`)
  - `evaluatorUid` (`String`) — the evaluator instance UID from `Identifiable.uid`
  - `metricName` (`String`) — the value of the evaluator's `metricName` Param (e.g., `"areaUnderROC"`, `"rmse"`, `"f1"`)
  - `metricValue` (`Double`) — the scalar value returned by `Evaluator.evaluate()`
  - `isLargerBetter` (`Boolean`) — from `Evaluator.isLargerBetter`
  - `timestamp` (`Long`) — millisecond-precision epoch time at metric capture
  - `evaluatorParams` (`Map[String, String]`) — JSON-encoded evaluator-specific parameters extracted from `extractParamMap()` (e.g., `numBins`, `k`, `distanceMeasure`, `metricLabel`, `beta`, `eps`, `throughOrigin`)
  - `warningFlags` (`List[String]`) — list of warning indicators (e.g., `"non_finite_metric_value"`)
- Add `@KVIndex` annotations on `EvaluationMetricRecord`:
  - Natural key: composite of `runId` + `evaluatorUid` + `metricName` + `timestamp`
  - Secondary index on `runId` for querying all metrics within a run
  - Secondary index on `timestamp` for chronological ordering
- Implement `MetricRecorder` that intercepts `Evaluator.evaluate()` completion and writes `EvaluationMetricRecord` to the KVStore:
  - Hook into the evaluate lifecycle using a pattern consistent with `MLEvents` (`FitStart`/`FitEnd`) from `events.scala`
  - After `evaluate()` returns, construct the `EvaluationMetricRecord` and call `KVStore.write()`
- Extract metric name from the evaluator's `metricName` Param using `evaluator.getOrDefault(evaluator.getParam("metricName"))` for all 6 concrete evaluator types
- Extract evaluator-specific parameters from each evaluator type's `ParamMap` using `extractParamMap()`:
  - `BinaryClassificationEvaluator`: `numBins`
  - `RegressionEvaluator`: `throughOrigin`
  - `MulticlassClassificationEvaluator`: `metricLabel`, `beta`, `eps`
  - `ClusteringEvaluator`: `distanceMeasure`
  - `RankingEvaluator`: `k`
  - `MultilabelClassificationEvaluator`: `metricLabel`
- Implement run association logic: read active run ID from the `ExperimentTracker` context (established by STORY-004-01-01 via `ThreadLocal` or `SparkSession` configuration)
- Handle `NaN` and `Infinite` metric values: store the raw Double value and append `"non_finite_metric_value"` to the `warningFlags` list; use `Double.isNaN()` and `Double.isInfinite()` for detection
- Implement fail-safe metric recording: wrap `KVStore.write()` in a `try/catch` block catching all `Exception` types, log a warning containing the evaluator class name, metric name, and run ID, and continue execution without re-throwing
- Write unit tests for each concrete Evaluator type:
  - `BinaryClassificationEvaluator` with `"areaUnderROC"` and `"areaUnderPR"`
  - `RegressionEvaluator` with `"rmse"`, `"r2"`, `"mse"`, `"mae"`, `"var"`
  - `MulticlassClassificationEvaluator` with `"f1"`, `"accuracy"`, `"logLoss"`, `"hammingLoss"`
  - `ClusteringEvaluator` with `"silhouette"`
  - `RankingEvaluator` with `"ndcgAtK"`, `"precisionAtK"`, `"meanAveragePrecision"`
  - `MultilabelClassificationEvaluator` with `"subsetAccuracy"`, `"f1Measure"`, `"microF1Measure"`
- Write unit test for `NaN` metric value recording with `"non_finite_metric_value"` warning flag
- Write unit test for `KVStore.write()` failure with graceful degradation (no exception propagated to caller)
- Write unit test for inactive tracking (no active run produces no metric record and no error)
- Write integration test validating metric capture across a full `Pipeline.fit()` → `Evaluator.evaluate()` flow, verifying that the metric record run ID matches the hyperparameter record run ID from STORY-004-01-01

## Edge Cases

### 1. Empty/Null Input

- **Evaluator.evaluate() on an empty DataFrame (zero rows):** The evaluator may return `NaN` (e.g., `RegressionEvaluator` computing `rmse` with no data) or throw an exception (e.g., `BinaryClassificationMetrics` encountering an empty RDD). If the evaluator returns `NaN`, the metric recorder stores the `NaN` value with a `"non_finite_metric_value"` warning flag. If the evaluator throws before returning, the metric recorder does not create a record for the failed evaluation — no partial or orphaned metric record is persisted to the KVStore.

### 2. Boundary Values

- **Metric value is exactly 0.0 or exactly 1.0:** Classification metrics such as `accuracy` or `areaUnderROC` may produce boundary values of `0.0` (all predictions wrong) or `1.0` (all predictions match). The metric record stores the exact Double value without rounding or clamping. A metric value of `Double.MAX_VALUE` or `Double.MIN_VALUE` is stored as-is with no truncation. No warning flag is set for finite boundary values.

### 3. Invalid Input

- **Evaluator metricName param set to an unsupported value:** If a data scientist sets the `metricName` to a string not in the evaluator's allowed list (e.g., setting `"invalid_metric"` on `BinaryClassificationEvaluator`, which allows only `"areaUnderROC"` and `"areaUnderPR"`), the `ParamValidators.inArray()` validation throws an `IllegalArgumentException` before `evaluate()` returns a value. The metric recorder does not create a record for a failed evaluation since no metric value was produced.

### 4. Rapid Successive Evaluations

- **Multiple Evaluator.evaluate() calls in rapid succession within the same run:** When `CrossValidator` evaluates a model across 10 folds, each `evaluate()` call produces a separate `EvaluationMetricRecord` with a distinct millisecond-precision timestamp. All records are associated with the same run ID. If two `evaluate()` calls complete within the same millisecond, the records remain distinct due to the composite natural key (run ID + evaluator UID + metric name + timestamp). No metric records are dropped or merged.

## Dependencies

- **STORY-004-01-01** (Log Hyperparameters Per Run) — experiment run context and `ExperimentRunMetadata` must exist for metric-to-run association; provides the active run ID via `ExperimentTracker` context
- **FEATURE-004-01** (Experiment Metadata Logging) — parent feature defining the overall metadata logging subsystem
- **EPIC-004** (Native ML Experiment Tracking and Model Registry) — parent epic providing strategic direction for experiment tracking
- **Evaluator abstract class** (`mllib/src/main/scala/org/apache/spark/ml/evaluation/Evaluator.scala`) — `evaluate(dataset: Dataset[_]): Double` returning the scalar metric; `evaluate(dataset, paramMap)` delegating via `copy(paramMap).evaluate(dataset)`; `isLargerBetter: Boolean` indicating metric direction
- **BinaryClassificationEvaluator** (`mllib/src/main/scala/org/apache/spark/ml/evaluation/BinaryClassificationEvaluator.scala`) — metricName values: `"areaUnderROC"`, `"areaUnderPR"`; params: `numBins` (IntParam, default 1000); `isLargerBetter` always `true`
- **RegressionEvaluator** (`mllib/src/main/scala/org/apache/spark/ml/evaluation/RegressionEvaluator.scala`) — metricName values: `"mse"`, `"rmse"`, `"r2"`, `"mae"`, `"var"`; params: `throughOrigin` (BooleanParam, default false); `isLargerBetter` `true` for `"r2"` and `"var"`, `false` for `"mse"`, `"rmse"`, `"mae"`
- **MulticlassClassificationEvaluator** (`mllib/src/main/scala/org/apache/spark/ml/evaluation/MulticlassClassificationEvaluator.scala`) — metricName values: `"f1"`, `"accuracy"`, `"weightedPrecision"`, `"weightedRecall"`, `"weightedTruePositiveRate"`, `"weightedFalsePositiveRate"`, `"weightedFMeasure"`, `"truePositiveRateByLabel"`, `"falsePositiveRateByLabel"`, `"precisionByLabel"`, `"recallByLabel"`, `"fMeasureByLabel"`, `"logLoss"`, `"hammingLoss"`; params: `metricLabel` (DoubleParam, default 0.0), `beta` (DoubleParam, default 1.0), `eps` (DoubleParam, default 1e-15); `isLargerBetter` `false` for `"weightedFalsePositiveRate"`, `"falsePositiveRateByLabel"`, `"logLoss"`, `"hammingLoss"`, `true` for all others
- **ClusteringEvaluator** (`mllib/src/main/scala/org/apache/spark/ml/evaluation/ClusteringEvaluator.scala`) — metricName values: `"silhouette"`; params: `distanceMeasure` (Param[String], default `"squaredEuclidean"`, also supports `"cosine"`); `isLargerBetter` always `true`
- **RankingEvaluator** (`mllib/src/main/scala/org/apache/spark/ml/evaluation/RankingEvaluator.scala`) — metricName values: `"meanAveragePrecision"`, `"meanAveragePrecisionAtK"`, `"precisionAtK"`, `"ndcgAtK"`, `"recallAtK"`; params: `k` (IntParam, default 10); `isLargerBetter` always `true`
- **MultilabelClassificationEvaluator** (`mllib/src/main/scala/org/apache/spark/ml/evaluation/MultilabelClassificationEvaluator.scala`) — metricName values: `"subsetAccuracy"`, `"accuracy"`, `"hammingLoss"`, `"precision"`, `"recall"`, `"f1Measure"`, `"precisionByLabel"`, `"recallByLabel"`, `"f1MeasureByLabel"`, `"microPrecision"`, `"microRecall"`, `"microF1Measure"`; params: `metricLabel` (DoubleParam, default 0.0); `isLargerBetter` `false` for `"hammingLoss"`, `true` for all others
- **KVStore interface** (`common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java`) — `write(Object value)` for metric persistence; `read(Class<T>, Object)` for retrieval; `view(Class<T>)` for listing; thread-safe for both reads and writes
- **Param and ParamMap** (`mllib/src/main/scala/org/apache/spark/ml/param/params.scala`) — `extractParamMap(): ParamMap` for obtaining all evaluator parameters; `Param.jsonEncode(value): String` for JSON serialization of parameter values

## Story Estimation Guidance

- **Story Points: 5** (Fibonacci scale)
- **Justification:** This story requires implementing a metric recording mechanism that intercepts `Evaluator.evaluate()` completion across 6 concrete evaluator types (`BinaryClassificationEvaluator`, `RegressionEvaluator`, `MulticlassClassificationEvaluator`, `ClusteringEvaluator`, `RankingEvaluator`, `MultilabelClassificationEvaluator`), extracting evaluator-specific parameters from each type's `ParamMap`, implementing fail-safe `KVStore` persistence with warning-flagged `NaN`/`Infinite` handling, and writing unit tests for all evaluator types plus error and edge case scenarios. The interception pattern leverages the `MLEvents` lifecycle established in STORY-004-01-01, making the integration straightforward. The breadth of 6 evaluator types and their distinct parameter sets accounts for the majority of the effort. This is smaller in scope than STORY-004-01-01 (8 points) because the foundational `ExperimentTracker` context, `KVStore` integration pattern, and run ID scheme are already established by that predecessor story.

## Definition of Done

- `EvaluationMetricRecord` data class is defined with all specified fields: `runId`, `evaluatorClassName`, `evaluatorUid`, `metricName`, `metricValue`, `isLargerBetter`, `timestamp`, `evaluatorParams`, and `warningFlags`
- `@KVIndex` annotations are applied on `EvaluationMetricRecord` with a composite natural key and secondary indices on `runId` and `timestamp`
- Metric recording triggers automatically on each `Evaluator.evaluate()` completion when an active experiment run exists in the `ExperimentTracker` context
- Metric records include the run ID, evaluator class name (fully qualified), evaluator UID, metric name (from the `metricName` Param), metric value (Double), `isLargerBetter` flag, millisecond-precision timestamp, and evaluator-specific parameters
- All 6 concrete Evaluator types are tested and produce metric records:
  - `BinaryClassificationEvaluator` — `"areaUnderROC"`, `"areaUnderPR"` with `numBins` parameter
  - `RegressionEvaluator` — `"mse"`, `"rmse"`, `"r2"`, `"mae"`, `"var"` with `throughOrigin` parameter
  - `MulticlassClassificationEvaluator` — `"f1"`, `"accuracy"`, `"logLoss"`, `"hammingLoss"` (and 10 other metrics) with `metricLabel`, `beta`, `eps` parameters
  - `ClusteringEvaluator` — `"silhouette"` with `distanceMeasure` parameter
  - `RankingEvaluator` — `"meanAveragePrecision"`, `"meanAveragePrecisionAtK"`, `"precisionAtK"`, `"ndcgAtK"`, `"recallAtK"` with `k` parameter
  - `MultilabelClassificationEvaluator` — `"subsetAccuracy"`, `"f1Measure"`, `"microF1Measure"` (and 9 other metrics) with `metricLabel` parameter
- `NaN` and `Infinite` metric values are stored with the raw Double value and a `"non_finite_metric_value"` entry in the `warningFlags` list
- `KVStore.write()` failures are caught, logged as warnings (including evaluator class name, metric name, and run ID), and do not interrupt the evaluation flow — `Evaluator.evaluate()` returns its value to the caller unchanged
- No metric record is created when no active experiment run exists — the evaluation operates identically to baseline behavior
- Multiple metrics within the same run are stored as distinct `EvaluationMetricRecord` entries, each with unique timestamps
- Metric records are linked to the experiment run ID established by STORY-004-01-01, enabling cross-query of hyperparameters and evaluation metrics by run ID
- Unit tests cover all 6 evaluator types, `NaN` metric value handling with warning flag, `KVStore` write failure with graceful degradation, and inactive tracking (no active run)
- Integration test validates metric capture across a full `Pipeline.fit()` → `Evaluator.evaluate()` flow, verifying that the metric record run ID matches the hyperparameter record run ID
- PySpark API wrapping for metric recording and retrieval is implemented and tested
