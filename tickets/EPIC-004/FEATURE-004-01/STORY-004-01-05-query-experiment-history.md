# Search and Filter Historical Experiment Metadata to Enable Data-Driven Model Selection

## Implementation Phase

**Phase:** Backend

This is a **Phase 1 (Backend)** story. It must be implemented and fully tested before any Phase 2 (Frontend) stories in this epic begin development. Backend stories establish data capture, analysis algorithms, persistence, and API layers that frontend stories depend on.

## User Story

**As a** data scientist, **I want to** search and filter historical experiment metadata by time range, tag values, metric thresholds, and pipeline type through a programmatic query API, **so that** I can identify the best-performing model configurations from past training runs and make data-driven decisions about which experiments to reproduce, refine, or promote to production — reducing experiment selection time from hours of manual log review to seconds of structured queries.

## Acceptance Criteria

### AC-1: Invalid Time Range Rejection (Input Validation)

- **Given** a data scientist provides a query with an invalid time range where the start timestamp is after the end timestamp
- **When** the query is submitted via `ExperimentTracker.queryRuns(query)`
- **Then** the system returns a validation error as an `IllegalArgumentException` indicating that the start time must precede the end time
- **And** no query is executed against the KVStore

### AC-2: Time Range Filtering (Expected Output)

- **Given** 10 experiment runs are stored in the KVStore spanning the last 30 days with timestamps recorded at millisecond precision
- **When** the data scientist queries for runs within a 7-day window by calling `ExperimentQueryBuilder.withTimeRange(startTimestamp, endTimestamp)` and submitting the query
- **Then** only the runs with timestamps falling within the inclusive 7-day range are returned
- **And** results are ordered in descending chronological order by default
- **And** each returned record contains the full `ExperimentRunMetadata` including run ID, timestamp, hyperparameters, metrics, tags, and pipeline type

### AC-3: Metric Threshold Filtering (Expected Output)

- **Given** experiment runs with recorded evaluation metrics are stored in the KVStore, including runs with `accuracy` values of 0.72, 0.85, 0.91, and 0.68
- **When** the data scientist filters by a metric threshold using `ExperimentQueryBuilder.withMetricThreshold("accuracy", "greaterThan", 0.85)`
- **Then** only the run where the `accuracy` metric exceeds 0.85 (the run with accuracy 0.91) is returned with its full metadata
- **And** runs with `accuracy` equal to or below 0.85 are excluded from the result set

### AC-4: Tag Key-Value Filtering (Expected Output)

- **Given** experiment runs are stored in the KVStore with custom tags including `{"dataset_version": "v2.3"}`, `{"dataset_version": "v3.0"}`, and `{"environment": "production"}`
- **When** the data scientist filters by tag key-value pair using `ExperimentQueryBuilder.withTag("dataset_version", "v2.3")`
- **Then** only runs containing a tag with key `"dataset_version"` and value `"v2.3"` are returned
- **And** runs with different tag values for the same key or without the specified tag key are excluded

### AC-5: KVStore Backend Unavailable (Error Handling)

- **Given** the KVStore backend (`InMemoryStore`, `LevelDB`, or `RocksDB`) is unavailable or has been closed via `KVStore.close()`
- **When** the data scientist attempts to query experiment history by calling `ExperimentTracker.queryRuns(query)`
- **Then** the system throws an `IllegalStateException` indicating that the experiment store is not accessible
- **And** the error message includes the backend type name (e.g., `"InMemoryStore"`, `"LevelDB"`)

### AC-6: Empty Result Set Handling (Edge Case)

- **Given** no experiment runs match the specified query criteria in the KVStore
- **When** the query executes via `ExperimentTracker.queryRuns(query)`
- **Then** the system returns an empty `Seq[ExperimentRunMetadata]` with a size of zero
- **And** no exception is thrown

### AC-7: Pagination with Skip and Max (Expected Output)

- **Given** 500 experiment runs match a query's filter criteria in the KVStore
- **When** the data scientist specifies pagination using `ExperimentQueryBuilder.skip(100).limit(50)` and submits the query
- **Then** exactly 50 results are returned starting from the 101st matching record
- **And** the results maintain the configured sort order (descending by timestamp by default)
- **And** `KVStoreView.skip(100)` and `KVStoreView.max(50)` are used to implement the pagination

### AC-8: Combined Filter Intersection (Expected Output)

- **Given** experiment runs are stored with mixed pipeline types (`LogisticRegression`, `RandomForestClassifier`), tags (`{"team": "nlp"}`, `{"team": "vision"}`), and metric values (`accuracy` ranging from 0.60 to 0.95)
- **When** the data scientist applies time range AND metric threshold (`accuracy` greaterThan 0.80) AND tag filter (`team=nlp`) simultaneously via `ExperimentQueryBuilder.withTimeRange(start, end).withMetricThreshold("accuracy", "greaterThan", 0.80).withTag("team", "nlp")`
- **Then** only runs satisfying all three filter conditions are returned
- **And** the result set represents the intersection of all applied filters

## Sub-Tasks

- Define `ExperimentQuery` data class with the following fields:
  - `timeRangeStart` (`Option[Long]`) — inclusive start timestamp in milliseconds since epoch
  - `timeRangeEnd` (`Option[Long]`) — inclusive end timestamp in milliseconds since epoch
  - `tagFilters` (`Map[String, String]`) — tag key to exact-match tag value
  - `metricFilters` (`Map[String, (String, Double)]`) — metric name to tuple of comparison operator string and threshold value
  - `pipelineTypeFilter` (`Option[String]`) — fully qualified Pipeline or Estimator class name
  - `skip` (`Long`) — number of matching results to skip, default 0
  - `maxResults` (`Long`) — maximum number of results to return, default `Long.MAX_VALUE`
  - `sortOrder` (`String`) — `"ascending"` or `"descending"`, default `"descending"`
- Implement `ExperimentQueryBuilder` with fluent API methods:
  - `withTimeRange(start: Long, end: Long): ExperimentQueryBuilder` — sets inclusive time bounds
  - `withTag(key: String, value: String): ExperimentQueryBuilder` — adds a tag filter (multiple calls accumulate)
  - `withMetricThreshold(metricName: String, operator: String, value: Double): ExperimentQueryBuilder` — adds a metric filter supporting operators `greaterThan`, `lessThan`, `greaterThanOrEqual`, `lessThanOrEqual`, `equalTo`
  - `withPipelineType(className: String): ExperimentQueryBuilder` — sets the pipeline type filter
  - `skip(n: Long): ExperimentQueryBuilder` — sets the number of results to skip
  - `limit(max: Long): ExperimentQueryBuilder` — sets the maximum results to return
  - `orderByTimestamp(): ExperimentQueryBuilder` — sorts results by timestamp
  - `orderByMetric(metricName: String): ExperimentQueryBuilder` — sorts results by a specified metric value
  - `build(): ExperimentQuery` — validates and returns the immutable query object
- Implement KVStore-backed query executor that translates `ExperimentQuery` to `KVStoreView` configurations:
  - Use `KVStore.view(ExperimentRunMetadata.class)` to obtain the base view
  - Apply `KVStoreView.index("timestamp")` for time-range queries
  - Apply `KVStoreView.first(startTimestamp)` and `KVStoreView.last(endTimestamp)` for inclusive time bounds
  - Apply `KVStoreView.skip(n)` and `KVStoreView.max(max)` for pagination
  - Apply `KVStoreView.reverse()` for descending sort order
- Add `@KVIndex` annotations on `ExperimentRunMetadata` entity for indexed fields: `timestamp` (secondary index), `pipelineType` (secondary index), `tagKeys` (secondary index)
- Implement post-filter logic for metric thresholds since `KVStoreView` supports only single-index traversal — after retrieving results via the primary index (timestamp), apply in-memory filtering for metric conditions
- Implement post-filter logic for tag key-value matching — iterate results and match against `tagFilters` map entries
- Add combined filter evaluation using intersection of index-filtered results and post-filtered results
- Implement result pagination using `KVStoreView.skip()` and `KVStoreView.max()` for index-based pagination, with additional offset tracking for post-filtered results
- Expose Scala API: `ExperimentTracker.queryRuns(query: ExperimentQuery): Seq[ExperimentRunMetadata]`
- Expose Python API: `ExperimentTracker.query_runs()` with keyword arguments `time_range_start`, `time_range_end`, `tag_filters`, `metric_filters`, `pipeline_type`, `skip`, `limit`, `sort_order`
- Expose Java API: `ExperimentTracker.queryRuns(ExperimentQuery query)` returning `java.util.List<ExperimentRunMetadata>`
- Write unit tests for each filter type individually: time range, metric threshold (all 5 operators), tag key-value, pipeline type
- Write unit tests for combined filters: time range + metric, time range + tag, metric + tag, all three combined
- Write unit tests for empty result sets, single result, and boundary pagination cases (skip equal to total count, skip exceeding total count)
- Write integration test validating query across 100+ stored runs with mixed metadata including varied timestamps, tags, metric values, and pipeline types

## Edge Cases

### 1. Empty/Null Input

- Query with all filters set to `null` or empty (no time range, no tag filters, no metric filters, no pipeline type filter) — returns all experiment runs stored in the KVStore without filtering, ordered by timestamp descending, with default pagination (`skip=0`, `max=Long.MAX_VALUE`). The `ExperimentQueryBuilder.build()` produces a valid `ExperimentQuery` with no active filters.

### 2. Boundary Values

- Query with `skip` value equal to the total number of matching runs — returns an empty result set without error since `KVStoreView.skip(n)` skips past all elements. Query with `skip` exceeding the total number of matching runs — also returns an empty result set without error. Query with `max=0` — `ExperimentQueryBuilder.build()` returns a validation error as an `IllegalArgumentException` because `KVStoreView.max(long)` requires `max > 0` (enforced by `JavaUtils.checkArgument(max > 0L, "max must be positive.")`).
  `Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStoreView.java`

### 3. Invalid Input

- Query with a metric name that does not exist in any stored experiment run — returns an empty result set since post-filtering finds no records matching the metric name; no exception is thrown. Query with a negative `skip` value — `ExperimentQueryBuilder.build()` returns a validation error as an `IllegalArgumentException` indicating skip must be non-negative. Query with an unrecognized metric operator (e.g., `"notEqual"`) — `ExperimentQueryBuilder.withMetricThreshold()` throws `IllegalArgumentException` listing the supported operators: `greaterThan`, `lessThan`, `greaterThanOrEqual`, `lessThanOrEqual`, `equalTo`.

### 4. Concurrent Modification

- A new experiment run is written to the KVStore via `KVStore.write()` while a query iteration is in progress using `KVStoreIterator` — the iterator maintains a consistent snapshot view for `LevelDB` and `RocksDB` backends and does not throw `ConcurrentModificationException`. For `InMemoryStore`, the `ConcurrentHashMap`-backed storage provides thread-safe iteration. Newly written data appears in subsequent queries but not in the active iterator's result set.
  `Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java`

### 5. Large Result Set

- Query matching 10,000+ experiment runs with no pagination limit specified — the `KVStoreIterator` streams results lazily without loading all entries into memory at once. Callers using the `closeableIterator()` method must close the iterator explicitly via `KVStoreIterator.close()` to release native resources (LevelDB/RocksDB file handles). Callers using the standard `iterator()` via a for-comprehension benefit from auto-close behavior when all elements are consumed.
  `Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStoreView.java`

## Dependencies

- **STORY-004-01-01** (Log Hyperparameters Per Run) — hyperparameter data must be stored in `ExperimentRunMetadata` records in the KVStore and be queryable; provides the `ExperimentRunMetadata` entity class with `@KVIndex` on `runId`, `estimatorClassName`, `timestamp`, and `pipelineUid`
- **STORY-004-01-02** (Record Evaluation Metrics) — evaluation metric data must be stored in `EvaluationMetricRecord` entries associated with experiment runs and be filterable by metric name and threshold value
- **STORY-004-01-03** (Capture Pipeline Configuration) — pipeline type and configuration must be stored in `PipelineConfigSnapshot` records associated with experiment runs, enabling pipeline-type-based query filtering
- **STORY-004-01-04** (Tag and Annotate Experiments) — tags must be stored in `ExperimentTag` records with `@KVIndex` parent indexing by run ID, enabling tag-based query filtering by key-value pairs via `KVStore.view(ExperimentTag.class).index("runIndex").parent(runId)`
- **FEATURE-004-01** (Experiment Metadata Logging) — parent feature defining the experiment metadata logging subsystem and API surface
- **EPIC-004** (Native ML Experiment Tracking and Model Registry) — parent epic providing the strategic vision for native experiment tracking capabilities
- **KVStore interface** (`common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java`) — `view(Class<T>)` returning `KVStoreView<T>` for configurable iteration, `count(Class<?>, String, Object)` for index-filtered counts, `read(Class<T>, Object)` for single-record retrieval; thread-safe for both reads and writes
  `Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java`
- **KVStoreView** (`common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStoreView.java`) — `index(String)` for selecting a secondary index, `first(Object)` and `last(Object)` for inclusive bound configuration, `skip(long)` for cursor offset, `max(long)` for result limit (requires `max > 0`), `reverse()` for descending iteration order, `parent(Object)` for child index filtering; implements `Iterable<T>` producing `KVStoreIterator<T>`
  `Source: common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStoreView.java`
- **KVStoreIterator** (`common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStoreIterator.java`) — `Closeable` iteration with `next(int)` for batch retrieval and `skip(long)` for cursor advance; auto-closes when exhausted in a for loop, requires explicit `close()` when iteration is terminated early
- **KVIndex annotation** (`common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVIndex.java`) — `@KVIndex` for marking indexed fields on metadata entity classes; supports natural keys via `NATURAL_INDEX_NAME` and named secondary indices for query optimization

## Story Estimation Guidance

- **Story Points: 8** (Fibonacci scale)
- **Justification:** This story requires designing a query DSL with a fluent builder API (`ExperimentQueryBuilder`), implementing multi-criteria filtering across a KVStore-backed datastore where `KVStoreView` supports only single-index traversal — compound queries involving time range, metric thresholds, tag values, and pipeline type demand post-filtering or multiple index scans for criteria beyond the primary index. Pagination logic must account for post-filtered result counts differing from KVStore-level counts. The story spans three language API surfaces (Scala, Python, Java) and requires comprehensive test coverage for individual filters, combined filter intersections, empty results, boundary pagination, and concurrent access patterns. The `KVStoreView` single-index constraint is the primary complexity driver, as metric and tag filtering must be applied as in-memory post-filters after index-based retrieval, requiring careful offset tracking to maintain correct pagination semantics.

## Definition of Done

- `ExperimentQuery` data class is defined with fields for `timeRangeStart`, `timeRangeEnd`, `tagFilters`, `metricFilters`, `pipelineTypeFilter`, `skip`, `maxResults`, and `sortOrder`
- `ExperimentQueryBuilder` fluent API is implemented with methods for `withTimeRange()`, `withTag()`, `withMetricThreshold()`, `withPipelineType()`, `skip()`, `limit()`, `orderByTimestamp()`, `orderByMetric()`, and `build()`
- Query executor translates `ExperimentQuery` to `KVStoreView` operations using `index()`, `first()`, `last()`, `skip()`, `max()`, and `reverse()`, and applies post-filters for compound criteria not supported by single-index traversal
- Time range queries return only runs with timestamps within the specified inclusive bounds using `KVStoreView.first(startTimestamp)` and `KVStoreView.last(endTimestamp)`
- Metric threshold queries support all five comparison operators: `greaterThan`, `lessThan`, `greaterThanOrEqual`, `lessThanOrEqual`, and `equalTo`
- Tag queries match exact key-value pairs by cross-referencing `ExperimentTag` records via the KVStore parent index
- Pipeline type queries match the fully qualified class name of the Pipeline or Estimator stored in the experiment run metadata
- Pagination with `skip` and `max` produces the specified slice of results, accounting for post-filter offset adjustments
- Combined filters return the intersection of all active filter conditions
- Invalid inputs produce clear validation errors: reversed time range throws `IllegalArgumentException`, negative skip throws `IllegalArgumentException`, `max` of zero or less throws `IllegalArgumentException`, unrecognized metric operator throws `IllegalArgumentException`
- Empty result sets return empty collections (`Seq.empty`, `Collections.emptyList()`, `[]`) without throwing exceptions
- Unavailable KVStore backend produces an `IllegalStateException` with the backend type name in the error message
- Scala API `ExperimentTracker.queryRuns(query: ExperimentQuery): Seq[ExperimentRunMetadata]` is implemented and tested
- Python API `ExperimentTracker.query_runs()` with keyword arguments for each filter dimension is implemented and tested
- Java API `ExperimentTracker.queryRuns(ExperimentQuery)` returning `java.util.List<ExperimentRunMetadata>` is implemented and tested
- Unit tests achieve 100% branch coverage for `ExperimentQueryBuilder` validation logic and query executor filter application
- Unit tests cover each filter type individually: time range, metric threshold (all 5 operators), tag key-value, and pipeline type
- Unit tests cover combined filter scenarios: time range + metric, time range + tag, metric + tag, and all three combined
- Unit tests cover pagination edge cases: skip equal to total count, skip exceeding total count, max of 1, max equal to total count
- Integration test validates end-to-end query flow with 100+ diverse experiment runs containing varied timestamps, tags, metric values, and pipeline types
- API documentation with usage examples for Scala, Python, and Java is complete
