# Add Custom Tags and Annotations to Experiment Runs to Organize and Contextualize Training History

## User Story

**As a** data scientist, **I want to** add, update, and remove custom key-value tags and free-text annotations to any experiment run through a programmatic API, both during and after pipeline execution, **so that** I can organize experiment runs by project, dataset version, hypothesis, or team membership using structured tags, and attach human-readable notes explaining experimental rationale or observations — enabling structured categorization of thousands of experiments and reducing time spent searching for specific runs by 80% compared to untagged experiment logs.

## Acceptance Criteria

### AC-1: Add Tag to Experiment Run (Expected Output)

- **Given** a data scientist has a completed experiment run with a known run ID stored in the KVStore
- **When** the data scientist calls `ExperimentTracker.setTag(runId, "dataset_version", "v2.3")`
- **Then** a tag record with key `"dataset_version"` and value `"v2.3"` is persisted to the KVStore via `KVStore.write()` with the run ID as the `@KVIndex` parent index and the current millisecond-precision timestamp
- **And** the tag's composite natural key is set to `runId + ":" + "dataset_version"` annotated with `@KVIndex(NATURAL_INDEX_NAME)`

### AC-2: Add Annotation to Experiment Run (Expected Output)

- **Given** a data scientist has a completed experiment run with a known run ID stored in the KVStore
- **When** the data scientist calls `ExperimentTracker.addAnnotation(runId, "Increased regularization to reduce overfitting on validation set")`
- **Then** an annotation record is persisted to the KVStore containing:
  - The provided text `"Increased regularization to reduce overfitting on validation set"`
  - A system-generated annotation ID produced via `java.util.UUID.randomUUID().toString`
  - The current millisecond-precision timestamp
  - The run ID as the `@KVIndex` parent index
- **And** the method returns the generated annotation ID as a `String`

### AC-3: Tag Key Length Validation (Input Validation)

- **Given** a data scientist attempts to set a tag with an empty string `""` as the key
- **When** the `setTag()` call is made
- **Then** the system throws an `IllegalArgumentException` with a message stating that the tag key must be between 1 and 256 characters inclusive
- **And** given a data scientist attempts to set a tag with a key that is 257 characters long, the same `IllegalArgumentException` is thrown with the same constraint message

### AC-4: Tag Value Null Validation (Input Validation)

- **Given** a data scientist attempts to set a tag with a `null` value
- **When** the `setTag()` call is made
- **Then** the system throws an `IllegalArgumentException` with a message stating that the tag value must not be null

### AC-5: Update Existing Tag Value (Expected Output)

- **Given** a tag with key `"environment"` and value `"development"` exists for a run with a known run ID in the KVStore
- **When** the data scientist calls `ExperimentTracker.setTag(runId, "environment", "production")`
- **Then** the existing tag record's value is overwritten to `"production"` via `KVStore.write()` using the same composite natural key `runId + ":" + "environment"`
- **And** the tag record's timestamp is updated to the current millisecond-precision time

### AC-6: Invalid Run ID Rejection (Error Handling)

- **Given** a data scientist attempts to set a tag on a run ID `"nonexistent-run-abc123"` that does not exist in the KVStore
- **When** `setTag()` is called with that run ID
- **Then** the system throws a `NoSuchElementException` with a message containing the string `"nonexistent-run-abc123"`
- **And** no tag record is written to the KVStore

### AC-7: Delete Existing Tag (Edge Case)

- **Given** a tag with key `"temporary"` exists for a run with a known run ID in the KVStore
- **When** the data scientist calls `ExperimentTracker.deleteTag(runId, "temporary")`
- **Then** the tag record is removed from the KVStore via `KVStore.delete(ExperimentTag.class, compositeKey)` where `compositeKey` equals `runId + ":" + "temporary"`
- **And** a subsequent call to `ExperimentTracker.getTag(runId, "temporary")` returns `None` in Scala or `null` in Java

## Sub-Tasks

- Define `ExperimentTag` data class with fields:
  - `runId` (`String`, annotated with `@KVIndex(value = "runIndex", parent = NATURAL_INDEX_NAME)`)
  - `tagKey` (`String`)
  - `tagValue` (`String`)
  - `timestamp` (`Long` — millisecond-precision epoch time)
  - `compositeKey` (`String` — `runId + ":" + tagKey`, annotated with `@KVIndex(NATURAL_INDEX_NAME)`)
- Define `ExperimentAnnotation` data class with fields:
  - `runId` (`String`, annotated with `@KVIndex(value = "runIndex", parent = NATURAL_INDEX_NAME)`)
  - `annotationId` (`String` — UUID-based via `java.util.UUID.randomUUID().toString`, annotated with `@KVIndex(NATURAL_INDEX_NAME)`)
  - `text` (`String`)
  - `timestamp` (`Long` — millisecond-precision epoch time)
- Implement `ExperimentTracker.setTag(runId: String, key: String, value: String): Unit`:
  - Validate key is non-empty, 1–256 characters, matches pattern `[a-zA-Z0-9_\\-\\.]+`
  - Validate key does not start with `"__"` (reserved for system tags)
  - Validate value is non-null and at most 5000 characters
  - Verify run ID exists in KVStore via `KVStore.read(ExperimentRunMetadata.class, runId)`
  - Write or overwrite tag via `KVStore.write(experimentTag)` using composite natural key
- Implement `ExperimentTracker.getTag(runId: String, key: String): Option[String]`:
  - Read tag by composite key `runId + ":" + key` via `KVStore.read(ExperimentTag.class, compositeKey)`
  - Return `Some(tagValue)` if found, `None` if `NoSuchElementException` is caught
- Implement `ExperimentTracker.getTags(runId: String): Map[String, String]`:
  - Use `KVStore.view(ExperimentTag.class).index("runIndex").parent(runId)` to retrieve all tags for the run
  - Convert to `Map[tagKey -> tagValue]`
- Implement `ExperimentTracker.deleteTag(runId: String, key: String): Unit`:
  - Delete tag by composite key via `KVStore.delete(ExperimentTag.class, compositeKey)`
- Implement `ExperimentTracker.addAnnotation(runId: String, text: String): String`:
  - Generate annotation ID via `java.util.UUID.randomUUID().toString`
  - Validate text is non-empty and at most 10000 characters
  - Verify run ID exists in KVStore
  - Write annotation via `KVStore.write(experimentAnnotation)`
  - Return the generated annotation ID
- Implement `ExperimentTracker.getAnnotations(runId: String): Seq[ExperimentAnnotation]`:
  - Use `KVStore.view(ExperimentAnnotation.class).index("runIndex").parent(runId)` to retrieve annotations
  - Sort results by `timestamp` in ascending order
- Implement `ExperimentTracker.deleteAnnotation(runId: String, annotationId: String): Unit`:
  - Delete annotation via `KVStore.delete(ExperimentAnnotation.class, annotationId)`
- Add tag key character validation: reject keys containing control characters (tab, newline, carriage return) or characters outside the pattern `[a-zA-Z0-9_\\-\\.]+`
- Add tag key reserved prefix validation: reject keys starting with `"__"` (double underscore)
- Add tag value length validation: non-null, maximum 5000 characters
- Add annotation text validation: non-empty, non-null, maximum 10000 characters
- Expose Python API in PySpark: `experiment_tracker.set_tag()`, `get_tag()`, `get_tags()`, `delete_tag()`, `add_annotation()`, `get_annotations()`, `delete_annotation()`
- Expose Java API with equivalent methods: `ExperimentTracker.setTag()`, `getTag()`, `getTags()`, `deleteTag()`, `addAnnotation()`, `getAnnotations()`, `deleteAnnotation()`
- Write unit tests for tag CRUD operations: add tag, get tag, get all tags, update tag, delete tag
- Write unit tests for annotation CRUD operations: add annotation, get annotations, delete annotation
- Write unit tests for all validation constraints: empty key, key exceeding 256 characters, null value, value exceeding 5000 characters, empty annotation text, annotation text exceeding 10000 characters, invalid key characters, reserved key prefix, nonexistent run ID
- Write unit test for concurrent tag modifications on the same run using multiple threads with `InMemoryStore`
- Write integration test validating full tag and annotation CRUD lifecycle: create run → add tags → read tags → update tag → delete tag → add annotations → read annotations → delete annotation

## Edge Cases

### 1. Empty/Null Input

- **Tag key is an empty string `""`**: `setTag()` throws `IllegalArgumentException` with a message stating the tag key must be between 1 and 256 characters inclusive. No tag record is written.
- **Tag value is an empty string `""`**: Permitted — represents a flag-style tag with no value content. The tag is persisted with an empty string as the value.
- **Tag value is `null`**: `setTag()` throws `IllegalArgumentException` with a message stating the tag value must not be null. No tag record is written.
- **Annotation text is `null`**: `addAnnotation()` throws `IllegalArgumentException` with a message stating the annotation text must not be null. No annotation record is written.
- **Annotation text is an empty string `""`**: `addAnnotation()` throws `IllegalArgumentException` with a message stating the annotation text must not be empty. No annotation record is written.

### 2. Boundary Values

- **Tag key is exactly 256 characters**: Accepted — the tag is persisted with the full 256-character key as part of the composite natural key.
- **Tag key is 257 characters**: Rejected — `setTag()` throws `IllegalArgumentException` specifying the 256-character maximum.
- **Tag value is exactly 5000 characters**: Accepted — the tag is persisted with the full 5000-character value.
- **Tag value is 5001 characters**: Rejected — `setTag()` throws `IllegalArgumentException` specifying the 5000-character maximum.
- **Annotation text is exactly 10000 characters**: Accepted — the annotation is persisted with the full text.
- **500 tags added to a single run**: All 500 tags are persisted and retrievable via `getTags()` without data loss. `KVStore.view()` with parent index filtering returns all 500 entries for `InMemoryStore` and LevelDB backends.

### 3. Invalid Input

- **Run ID does not exist in the KVStore**: `setTag()` and `addAnnotation()` both throw `NoSuchElementException` with a message containing the invalid run ID string. No tag or annotation record is written.
- **Tag key contains control characters** (tab `\t`, newline `\n`): `setTag()` throws `IllegalArgumentException` specifying the allowed character pattern `[a-zA-Z0-9_\\-\\.]+`.
- **Tag key starts with reserved prefix `"__"`** (double underscore): `setTag()` throws `IllegalArgumentException` indicating that keys starting with `"__"` are reserved for system use.
- **Tag key contains spaces or special characters** (`@`, `#`, `$`): `setTag()` throws `IllegalArgumentException` specifying the allowed character pattern.

### 4. Duplicate Operations

- **Setting the same tag key twice with different values**: The second `setTag()` call overwrites the first value via `KVStore.write()` using the identical composite natural key. Only the second value and updated timestamp are retained. `getTag()` returns the second value.
- **Deleting a tag that does not exist**: `deleteTag()` throws `NoSuchElementException` when `KVStore.delete()` cannot find the composite key. The caller must handle this exception or verify tag existence before deletion.
- **Adding two annotations with identical text**: Both annotations are persisted as separate records, each with a unique annotation ID generated by `java.util.UUID.randomUUID().toString` and a distinct timestamp. `getAnnotations()` returns both records.

## Dependencies

- **STORY-004-01-01** (Log Hyperparameters Per Run) — the `ExperimentRunMetadata` entity and `ExperimentTracker` entry point defined in this story provide the experiment run context. Run ID validation in `setTag()` and `addAnnotation()` depends on reading `ExperimentRunMetadata` records from the KVStore.
- **FEATURE-004-01** (Experiment Metadata Logging) — parent feature defining the overall metadata logging subsystem scope and API surface
- **EPIC-004** (Native ML Experiment Tracking and Model Registry) — parent epic providing the strategic vision for native experiment tracking
- **KVStore interface** (`common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStore.java`) — `write(Object)` for persisting tag and annotation records, `read(Class<T>, Object)` for retrieving by natural key, `delete(Class<?>, Object)` for removing records, `view(Class<T>)` returning `KVStoreView<T>` for listing records by parent index
- **KVIndex annotation** (`common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVIndex.java`) — `@KVIndex` for marking the natural key (`NATURAL_INDEX_NAME = "__main__"`) and parent-child index relationships on `ExperimentTag` and `ExperimentAnnotation` entity fields
- **KVStoreView** (`common/kvstore/src/main/java/org/apache/spark/util/kvstore/KVStoreView.java`) — `parent(Object)` method for filtering child entities by run ID when iterating via `KVStore.view()`
- **InMemoryStore** (`common/kvstore/src/main/java/org/apache/spark/util/kvstore/InMemoryStore.java`) — `ConcurrentHashMap`-backed thread-safe in-memory `KVStore` implementation for unit testing and lightweight deployments; throws `NoSuchElementException` from `read()` when the natural key is not found

## Story Estimation Guidance

- **Story Points: 3** (Fibonacci scale)
- **Justification:** This story implements straightforward CRUD operations for tags and annotations using the existing KVStore API. The data model consists of two simple entity classes (`ExperimentTag` with a composite key and `ExperimentAnnotation` with a UUID key), both using `@KVIndex` parent indexing for run-scoped queries. The KVStore `write()`, `read()`, `delete()`, and `view().parent()` methods handle all persistence operations directly. Validation rules for tag keys (1–256 characters, alphanumeric pattern, no reserved prefix), tag values (non-null, max 5000 characters), and annotation text (non-empty, max 10000 characters) are well-defined and straightforward to implement. No complex ML framework integration or Catalyst optimizer interaction is needed — this is pure metadata management. The primary effort is in comprehensive input validation, three language API surfaces (Scala, Python, Java), and thorough test coverage across all CRUD operations and error paths.

## Definition of Done

- `ExperimentTag` data class is defined with `@KVIndex(NATURAL_INDEX_NAME)` on the `compositeKey` field (natural key) and `@KVIndex(value = "runIndex", parent = NATURAL_INDEX_NAME)` on the `runId` field (parent index)
- `ExperimentAnnotation` data class is defined with `@KVIndex(NATURAL_INDEX_NAME)` on the `annotationId` field (natural key) and `@KVIndex(value = "runIndex", parent = NATURAL_INDEX_NAME)` on the `runId` field (parent index)
- `setTag()`, `getTag()`, `getTags()`, `deleteTag()` operations are implemented and return expected results for all valid inputs
- `addAnnotation()`, `getAnnotations()`, `deleteAnnotation()` operations are implemented and return expected results for all valid inputs
- Tag key validation enforces: non-empty, 1–256 characters, matches pattern `[a-zA-Z0-9_\-\.]+`, rejects keys starting with reserved `"__"` prefix
- Tag value validation enforces: non-null, maximum 5000 characters
- Annotation text validation enforces: non-null, non-empty, maximum 10000 characters
- Updating an existing tag key via `setTag()` overwrites the value and updates the timestamp using the same composite natural key
- Invalid run ID passed to `setTag()` or `addAnnotation()` produces `NoSuchElementException` with the invalid run ID in the error message
- Tags are retrievable by run ID using `KVStore.view(ExperimentTag.class).index("runIndex").parent(runId)`
- Annotations are retrievable by run ID using `KVStore.view(ExperimentAnnotation.class).index("runIndex").parent(runId)` sorted by timestamp ascending
- Scala, Python (PySpark), and Java APIs are implemented for all tag and annotation operations
- Unit tests cover all CRUD operations: add, get, get-all, update, and delete for both tags and annotations
- Unit tests cover all validation constraints: empty key, key exceeding 256 characters, invalid key characters, reserved key prefix, null value, value exceeding 5000 characters, empty annotation text, annotation text exceeding 10000 characters, nonexistent run ID
- Integration test validates the full tag and annotation lifecycle: create run → add tags → read tags → update tag → delete tag → add annotations → read annotations → delete annotation
- Concurrent tag modification test using multiple threads with `InMemoryStore` passes without data corruption or lost updates
