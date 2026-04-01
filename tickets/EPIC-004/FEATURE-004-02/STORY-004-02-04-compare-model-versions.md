# Compare Model Versions Side-by-Side to Identify the Best-Performing Candidate for Promotion

## User Story

**As a** data scientist, **I want to** compare evaluation metrics, hyperparameters, and training metadata across up to 5 registered model versions of the same model in a side-by-side tabular view — including metrics from BinaryClassificationEvaluator (areaUnderROC, areaUnderPR), RegressionEvaluator (rmse, mse, r2, mae, var), MulticlassClassificationEvaluator (f1, accuracy, weightedPrecision, weightedRecall, logLoss), and ClusteringEvaluator (silhouette), along with all Estimator ParamMap values, **so that** I can make data-driven promotion decisions by objectively identifying which model version delivers the best performance on target evaluation metrics — reducing model selection time from hours of manual comparison across notebook outputs to under 2 minutes per comparison request.

## Acceptance Criteria

### AC-1: Input Validation — Valid Comparison Request

- **Given** a registered model "loan_default_predictor" with 3 versions ("1.0.0", "2.0.0", "3.0.0") each having recorded evaluation metrics from BinaryClassificationEvaluator (areaUnderROC, areaUnderPR) and logged hyperparameters in the KVStore-backed registry
- **When** the data scientist calls `registry.compareVersions("loan_default_predictor", versions=["1.0.0", "2.0.0", "3.0.0"])`
- **Then** the system returns a comparison result containing a metrics table with columns for each version and rows for each metric (areaUnderROC, areaUnderPR), a parameters table with columns for each version and rows for each Estimator Param, and the best-performing version highlighted per metric based on the `isLargerBetter` flag from each Evaluator

### AC-2: Expected Output — Comparison Result Structure

- **Given** model "sales_forecaster" has versions "1.0.0" (rmse=12.5, r2=0.85, mae=9.2) and "2.0.0" (rmse=10.1, r2=0.91, mae=7.8) with metrics recorded from RegressionEvaluator
- **When** the data scientist calls `registry.compareVersions("sales_forecaster", versions=["1.0.0", "2.0.0"])`
- **Then** the comparison result contains: a `metricsTable` DataFrame with schema (metricName: String, version_1_0_0: Double, version_2_0_0: Double, bestVersion: String), where the bestVersion column shows "2.0.0" for rmse (lower is better, `isLargerBetter=false`), "2.0.0" for r2 (higher is better, `isLargerBetter=true`), and "2.0.0" for mae (lower is better, `isLargerBetter=false`)

> `Source: mllib/src/main/scala/org/apache/spark/ml/evaluation/RegressionEvaluator.scala` — `isLargerBetter`: `r2` | `var` → true; `rmse` | `mse` | `mae` → false

### AC-3: Error Handling — Exceed Maximum Version Count

- **Given** model "image_classifier" has 8 registered versions in the registry
- **When** the data scientist calls `registry.compareVersions("image_classifier", versions=["1.0.0", "2.0.0", "3.0.0", "4.0.0", "5.0.0", "6.0.0"])`
- **Then** the system raises an `IllegalArgumentException` with message containing "Cannot compare more than 5 versions at once; requested 6"

### AC-4: Error Handling — Version Not Found

- **Given** model "text_classifier" has versions "1.0.0" and "2.0.0" but no version "3.0.0"
- **When** the data scientist calls `registry.compareVersions("text_classifier", versions=["1.0.0", "3.0.0"])`
- **Then** the system raises a `NoSuchElementException` with message containing "Version '3.0.0' not found for model 'text_classifier'"

> `Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java` — `read(Class<T>, Object)` throws `NoSuchElementException` when the natural key does not exist

### AC-5: Edge Case — Single Version Comparison

- **Given** model "anomaly_detector" has version "1.0.0" with recorded metrics
- **When** the data scientist calls `registry.compareVersions("anomaly_detector", versions=["1.0.0"])`
- **Then** the system returns a comparison result with a single version column, all bestVersion entries show "1.0.0", and the result includes a warning message "Single version provided; comparison is most informative with 2 or more versions"

### AC-6: Comparison with Missing Metrics

- **Given** model "customer_segmenter" has version "1.0.0" with silhouette metric from ClusteringEvaluator and version "2.0.0" with silhouette metric recorded, but version "1.0.0" has no recorded rmse metric while version "2.0.0" does
- **When** the data scientist calls `registry.compareVersions("customer_segmenter", versions=["1.0.0", "2.0.0"])`
- **Then** the comparison result shows "N/A" for version "1.0.0" in the rmse row, shows numeric values for both versions in the silhouette row, and the bestVersion for rmse shows "2.0.0" (the only version with that metric)

## Sub-Tasks

- [ ] **Design comparison result data model**: Define `ModelComparisonResult` containing `metricsTable` (DataFrame), `parametersTable` (DataFrame), `trainingMetadataTable` (DataFrame), `bestVersionPerMetric` (Map[String, String] mapping metric name to best version), and `warnings` (Seq[String])
- [ ] **Implement `compareVersions(modelName, versions)` API**: Validate inputs — model name is non-empty, versions list contains 1–5 entries, all specified versions exist in the KVStore-backed registry for the given model name; retrieve registry entries using `KVStore.read(Class<T>, Object)` keyed by model name and version string
- [ ] **Build metrics aggregation logic**: Collect all evaluation metric values per version using metric names from BinaryClassificationEvaluator (`areaUnderROC`, `areaUnderPR`), RegressionEvaluator (`rmse`, `mse`, `r2`, `mae`, `var`), MulticlassClassificationEvaluator (`f1`, `accuracy`, `weightedPrecision`, `weightedRecall`, `logLoss`), and ClusteringEvaluator (`silhouette`); construct a DataFrame with one row per metric and one column per version
- [ ] **Implement best-version identification per metric**: Use the `isLargerBetter` flag from each Evaluator subclass to determine directionality — `true` for areaUnderROC, areaUnderPR, r2, var, f1, accuracy, weightedPrecision, weightedRecall, silhouette; `false` for rmse, mse, mae, logLoss — and select the version with the optimal value per metric row
- [ ] **Build parameters comparison**: Extract Estimator ParamMap values (via `Param.jsonEncode`) per version using the `Model.parent` Estimator reference and `extractParamMap()`; align parameters by parameter name into a DataFrame with one row per parameter and one column per version
- [ ] **Implement training metadata comparison**: For each version, retrieve and display training dataset identifier, pipeline configuration hash (derived from `DefaultParamsWriter` JSON metadata keys: `class`, `timestamp`, `sparkVersion`, `uid`, `paramMap`), experiment run ID (from FEATURE-004-01), training timestamp, and Spark version
- [ ] **Handle missing metrics gracefully**: Display "N/A" placeholders for versions missing a given metric; determine best version per metric considering only versions with recorded values; if all versions lack a given metric, omit that metric row from the table
- [ ] **Add PySpark API bindings**: Expose `compareVersions` in PySpark returning pandas-compatible DataFrames for `metricsTable` and `parametersTable` via `toPandas()`; include the `warnings` list as a Python list of strings
- [ ] **Write unit tests**: Cover 2-version comparison, 5-version comparison (maximum boundary), single-version comparison with warning, missing metrics with "N/A" display, mixed Evaluator types in one comparison, invalid inputs (empty name, 6 versions, non-existent version, duplicate versions)
- [ ] **Write integration test**: End-to-end workflow — train multiple model versions using `Pipeline.fit()` → register each version → record evaluation metrics via Evaluator → invoke `compareVersions` → assert metrics table, parameters table, and best-version identification match expected values

## Edge Cases

### 1. Empty/Null Input

Calling `compareVersions` with a null or empty model name, or with an empty versions list, must raise an `IllegalArgumentException` with message "Model name must be non-empty and at least one version must be specified". Passing a null value within the versions list must raise an `IllegalArgumentException` with message "Version identifiers must not be null".

### 2. Boundary Values — Maximum 5 Versions

Comparing exactly 5 versions (the documented maximum) must succeed without error and return a comparison result with 5 version columns. Comparing 6 versions must fail with an `IllegalArgumentException` containing "Cannot compare more than 5 versions at once; requested 6".

### 3. Invalid Input — Duplicate Versions in List

Calling `compareVersions("model", versions=["1.0.0", "1.0.0", "2.0.0"])` must deduplicate the input and compare only unique versions ("1.0.0" and "2.0.0"), returning a result with 2 version columns. The deduplication count (from 3 requested to 2 unique) must be logged at `INFO` level but not raise an error.

### 4. No Recorded Metrics for Any Version

If all requested versions have zero recorded evaluation metrics, the comparison result returns an empty `metricsTable` DataFrame (schema preserved but zero rows) with a warning message "No evaluation metrics recorded for the requested versions". The `parametersTable` and `trainingMetadataTable` must still be populated with available data.

### 5. Archived Version in Comparison

If one of the requested versions has been archived (see STORY-004-02-05), the comparison must still include its metrics and parameters in the tabular output. An "Archived" status flag must appear in the `trainingMetadataTable` for that version's status row, and a warning message "Version '&lt;version&gt;' is archived" must be included in the comparison result warnings.

## Dependencies

| Dependency | Type | Description |
|---|---|---|
| FEATURE-004-02 | Parent Feature | Model Versioning and Registry — parent feature providing the registry infrastructure and model lifecycle management |
| FEATURE-004-01 | Prerequisite Feature | Experiment Metadata Logging — provides experiment run metrics, hyperparameters, and pipeline configurations that feed into model version comparison |
| STORY-004-02-01 | Prerequisite Story | Register Trained Model Artifacts — models must be registered in the registry before they can be included in a comparison |
| STORY-004-02-02 | Prerequisite Story | Version Models with Lineage — versioned models with lineage data are the comparison targets; version identifiers and lineage metadata are consumed by the comparison API |
| STORY-004-02-05 | Related Story | Archive Deprecated Models — archived versions must still be accessible for comparison with an "Archived" status flag displayed |

### Source Code Dependencies

| Component | Source Path | Usage in This Story |
|---|---|---|
| Evaluator abstract class | `mllib/src/main/scala/org/apache/spark/ml/evaluation/Evaluator.scala` | `evaluate(dataset: Dataset[_]): Double` for metric computation; `isLargerBetter: Boolean` (default `true`) for metric directionality in best-version identification |
| BinaryClassificationEvaluator | `mllib/src/main/scala/org/apache/spark/ml/evaluation/BinaryClassificationEvaluator.scala` | metricName: `"areaUnderROC"` (default) or `"areaUnderPR"`; `isLargerBetter = true` for both metrics |
| RegressionEvaluator | `mllib/src/main/scala/org/apache/spark/ml/evaluation/RegressionEvaluator.scala` | metricName: `"rmse"` (default), `"mse"`, `"r2"`, `"mae"`, `"var"`; `isLargerBetter`: `true` for `r2` and `var`, `false` for `rmse`, `mse`, `mae` |
| MulticlassClassificationEvaluator | `mllib/src/main/scala/org/apache/spark/ml/evaluation/MulticlassClassificationEvaluator.scala` | metricName: `"f1"` (default), `"accuracy"`, `"weightedPrecision"`, `"weightedRecall"`, `"logLoss"`, and others; `isLargerBetter`: `false` for `logLoss`, `hammingLoss`, `weightedFalsePositiveRate`, `falsePositiveRateByLabel`; `true` for all others |
| ClusteringEvaluator | `mllib/src/main/scala/org/apache/spark/ml/evaluation/ClusteringEvaluator.scala` | metricName: `"silhouette"`; distanceMeasure: `"squaredEuclidean"` or `"cosine"`; `isLargerBetter = true` |
| KVStore interface | `common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java` | `read(Class<T>, Object)` to retrieve version metadata by natural key; `view(Class<T>)` to iterate registered versions; throws `NoSuchElementException` for missing keys |
| Model abstract class | `mllib/src/main/scala/org/apache/spark/ml/Model.scala` | `parent: Estimator[M]` reference for extracting the originating Estimator's parameters via `extractParamMap()` |
| ReadWrite utilities | `mllib/src/main/scala/org/apache/spark/ml/util/ReadWrite.scala` | `DefaultParamsWriter` JSON metadata format with keys: `class`, `timestamp`, `sparkVersion`, `uid`, `paramMap`, `defaultParamMap`; `Param.jsonEncode()` for parameter serialization |

## Story Estimation Guidance

| Attribute | Value |
|---|---|
| **Story Points** | **5** (Fibonacci scale) |
| **Complexity** | Moderate — multi-version data retrieval from KVStore, metrics aggregation across multiple Evaluator types with `isLargerBetter` directionality handling, parameter alignment via ParamMap serialization, missing-data handling, and DataFrame-based result construction |
| **Effort Drivers** | Integration with 4+ Evaluator subclasses, KVStore read operations for each version, `Param.jsonEncode()` alignment across heterogeneous parameter sets, "N/A" placeholder logic, PySpark API parity |
| **Risk Factors** | Metric directionality must match each Evaluator subclass implementation; missing metrics must not cause null pointer failures; version deduplication must preserve ordering |
| **Reference Comparison** | Comparable in scope to the multi-model evaluation summary in existing `CrossValidator` and `TrainValidationSplit` result reporting |
| **Sprint Fit** | Completable within a single sprint by one engineer with familiarity in MLlib Evaluator and KVStore APIs |

## Definition of Done

- [ ] `compareVersions` API accepts a model name (non-empty String) and a list of 1–5 version strings, returning a `ModelComparisonResult` containing `metricsTable`, `parametersTable`, `trainingMetadataTable`, `bestVersionPerMetric`, and `warnings`
- [ ] Metrics table displays all recorded evaluation metrics per version with one row per metric and one column per version, plus a `bestVersion` column identifying the optimal version per metric using `isLargerBetter` directionality from each Evaluator subclass
- [ ] Parameters table displays all Estimator Param values per version aligned by parameter name, with values serialized via `Param.jsonEncode()`; parameters present in one version but absent in another display as "N/A"
- [ ] Training metadata table displays dataset identifier, pipeline configuration hash, experiment run ID (from FEATURE-004-01), training timestamp, and Spark version per version
- [ ] Missing metrics display as "N/A" without causing comparison failure; best version for a metric with partial coverage is determined from versions that have that metric recorded
- [ ] Maximum 5 versions enforced — requests exceeding this limit produce an `IllegalArgumentException` with message "Cannot compare more than 5 versions at once; requested N"
- [ ] Non-existent versions produce a `NoSuchElementException` with message "Version 'X' not found for model 'Y'"
- [ ] Duplicate version entries in the request are deduplicated before comparison, with deduplication logged at `INFO` level
- [ ] Single-version comparison returns a valid result with all bestVersion entries pointing to that version and a warning "Single version provided; comparison is most informative with 2 or more versions"
- [ ] Archived versions are included in comparison results with an "Archived" status flag in the metadata table and a warning message in the result
- [ ] PySpark API parity: `compareVersions` is accessible from Python and returns pandas-compatible DataFrames for `metricsTable` and `parametersTable` via `toPandas()`
- [ ] Unit tests cover: 2-version comparison, 5-version comparison (boundary), single-version comparison, missing metrics with "N/A" handling, mixed Evaluator types, and invalid-input scenarios (empty name, 6 versions, non-existent version, duplicate versions, null input)
- [ ] Integration test validates end-to-end workflow: train multiple versions → register → record metrics → compare → verify metrics table, parameters table, and best-version identification
- [ ] Documentation covers the `compareVersions` API signature, `ModelComparisonResult` structure, metric directionality rules, "N/A" handling semantics, PySpark usage examples, and error conditions
