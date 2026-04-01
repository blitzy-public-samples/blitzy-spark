# Compute Data Completeness Metrics on Validated DataFrames to Quantify Column-Level Data Quality

## User Story

As a **data analyst**, I want to compute completeness metrics — including non-null ratio, distinct value count, and null count — for each column in a validated DataFrame, so that I can quantify data quality at the column level across pipeline outputs and identify columns with missing or sparse data before they impact downstream reporting accuracy, reducing data quality investigation time by up to 40%.

## Acceptance Criteria

- **AC-1 (Input Validation):** Given a DataFrame with 10 columns of mixed types (string, integer, double, boolean, timestamp), When I invoke the completeness metrics computation API, Then the system returns a summary DataFrame with exactly one row per input column containing column_name (string), total_row_count (long), non_null_count (long), null_count (long), non_null_ratio (double between 0.0 and 1.0), and distinct_value_count (long)

- **AC-2 (Expected Output):** Given a DataFrame with 1,000 rows where column "email" contains 150 null values, When completeness metrics are computed for column "email", Then null_count equals 150, non_null_count equals 850, non_null_ratio equals 0.85, and total_row_count equals 1000

- **AC-3 (Error Handling):** Given a DataFrame with zero rows (empty DataFrame), When completeness metrics are computed, Then the system returns a summary DataFrame with one row per column where total_row_count is 0, null_count is 0, non_null_count is 0, non_null_ratio is 0.0, and distinct_value_count is 0

- **AC-4 (Edge Case):** Given a DataFrame where column "status" contains only null values (100% null), When completeness metrics are computed for column "status", Then null_count equals total_row_count, non_null_count equals 0, non_null_ratio equals 0.0, and distinct_value_count equals 0

- **AC-5 (Subset Selection):** Given a DataFrame with 20 columns, When I invoke completeness metrics computation specifying a subset of 5 column names, Then the summary DataFrame contains exactly 5 rows corresponding to the specified columns only

- **AC-6 (Streaming Support):** Given a streaming DataFrame with watermark configured, When completeness metrics are computed on a micro-batch, Then the metrics are calculated for the rows within that micro-batch and the output schema matches the batch completeness metrics schema

## Sub-Tasks

- Define `CompletenessMetrics` case class with fields: columnName, totalRowCount, nonNullCount, nullCount, nonNullRatio, distinctValueCount
- Implement `computeCompleteness(df: DataFrame): DataFrame` method that iterates over all columns and computes aggregation expressions using `count(col)`, `count("*")`, `countDistinct(col)`, and derived null count
- Implement `computeCompleteness(df: DataFrame, columns: Seq[String]): DataFrame` overload for subset column selection with input validation (verify specified columns exist in schema)
- Leverage Spark SQL built-in aggregate functions (`count`, `countDistinct`, `sum` with `when`/`isNull`) to compute metrics in a single DataFrame pass following patterns from `sql/core/src/main/scala/org/apache/spark/sql/execution/stat/StatFunctions.scala`
- Ensure non_null_ratio is computed as `non_null_count / total_row_count` with zero-division guard returning 0.0 for empty DataFrames
- Add support for streaming DataFrames by computing metrics per micro-batch
- Write unit tests covering all acceptance criteria scenarios
- Write integration test validating metrics on a 10,000-row DataFrame with known null distributions

## Edge Cases

- **Edge Case 1 (Empty/Null Input):** DataFrame with zero rows — all metrics return 0 values and non_null_ratio returns 0.0 (not NaN or Infinity)
- **Edge Case 2 (All Nulls):** Column where every value is null — non_null_count is 0, non_null_ratio is 0.0, distinct_value_count is 0
- **Edge Case 3 (Boundary Values):** DataFrame with exactly 1 row — metrics are computed for a single row; non_null_ratio is either 0.0 or 1.0 per column
- **Edge Case 4 (Invalid Input):** Column name specified in subset that does not exist in the DataFrame schema — the system raises an `AnalysisException` with a message identifying the missing column name
- **Edge Case 5 (Complex Types):** Columns with complex types (ArrayType, MapType, StructType) — null_count reflects top-level null status (entire value is null), not nested element nullability

## Dependencies

- **FEATURE-002-01 — DataFrame Quality Rules Engine** (`tickets/EPIC-002/FEATURE-002-01-dataframe-quality-rules-engine.md`): Provides validated DataFrames with quality rule results consumed by this story's metrics computation
- **FEATURE-002-02 — Quality Metrics and Reporting** (`tickets/EPIC-002/FEATURE-002-02-quality-metrics-and-reporting.md`): Parent feature defining the overall metrics and reporting scope within which this story operates
- **EPIC-002 — Declarative Data Quality Validation Framework** (`tickets/EPIC-002-declarative-data-quality-framework.md`): Parent epic governing the full data quality initiative
- **F-002 — SQL Query Processing Engine**: DataFrame API, `count`, `countDistinct`, `sum`, `when`, `isNull` aggregate functions used to compute completeness metrics
- **Source:** `sql/core/src/main/scala/org/apache/spark/sql/execution/stat/StatFunctions.scala` — existing statistical computation patterns using single-pass Column expression composition (approxQuantile, cov, corr, crossTabulate)
- **Source:** `sql/api/src/main/scala/org/apache/spark/sql/DataFrameStatFunctions.scala` — public API surface for DataFrame statistical operations (approxQuantile, cov, corr, crosstab, freqItems, bloomFilter)
- **Source:** `sql/core/src/main/scala/org/apache/spark/sql/classic/DataFrameStatFunctions.scala` — concrete implementation of DataFrame.stat methods using StatFunctions and FrequentItems utilities

## Story Estimation Guidance

- **Story Points:** 5 (Fibonacci)
- **Rationale:** Moderate complexity — requires implementing a new aggregation method on DataFrames using existing Spark SQL aggregate functions, column iteration logic, and zero-division handling. The single-pass aggregation pattern is established in `StatFunctions.scala`. Testing across batch and streaming modes adds integration complexity. Estimated 3-4 days of development effort for one engineer.
- **Risk Factors:** Handling complex types (Array, Map, Struct) at top-level null detection requires careful Column expression composition; streaming micro-batch semantics need validation with watermark configurations

## Definition of Done

- Completeness metrics API is implemented and returns a summary DataFrame with column_name, total_row_count, non_null_count, null_count, non_null_ratio, and distinct_value_count for each column
- Subset column selection is supported with input validation for non-existent column names
- Zero-row DataFrames return all-zero metrics without errors or NaN values
- Streaming DataFrames produce per-micro-batch completeness metrics
- Unit tests cover all 6 acceptance criteria with assertions on exact metric values
- Integration test validates metrics on a DataFrame with at least 10,000 rows and known null distribution
- API documentation with Scaladoc describes parameters, return schema, and usage examples
- Code review completed and merged to feature branch
- No forbidden terms used in any acceptance criteria or documentation
