# Show Checkpoint Status to Enable Proactive Checkpoint Health Monitoring for Streaming Queries

## User Story

**As a** data platform administrator,
**I want to** view checkpoint health information for each active Structured Streaming query — including the last successful checkpoint timestamp, checkpoint duration in milliseconds, cumulative checkpoint failure count, and checkpoint storage size — in the Spark Web UI and via the REST API,
**So that** I can identify checkpoint failures and performance degradation within 5 minutes of occurrence, preventing silent data loss during query restarts and reducing recovery time from checkpoint-related failures by at least 40%.

## Acceptance Criteria

### AC1 — Input Validation (Checkpoint Active)

```
Given a Structured Streaming query is running with checkpointing enabled (a checkpoint location is configured)
When the data platform administrator opens the Structured Streaming tab in the Spark Web UI
Then the UI displays a checkpoint status panel showing the checkpoint location path, the last successfully committed batch ID, and the timestamp of the last successful commit in ISO8601 UTC format
```

- `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala` — resolvedCheckpointRoot field provides the checkpoint location path
- `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/CommitLog.scala` — HDFSMetadataLog[CommitMetadata] provides the last committed batch ID and commit timestamp

### AC2 — Expected Output (Checkpoint Duration)

```
Given a Structured Streaming query has completed at least 5 micro-batches with checkpointing
When the data platform administrator views the checkpoint status panel
Then the checkpoint duration is displayed as the time in milliseconds between the offset log write and the commit log write for the most recent batch
And a trend chart shows checkpoint duration values for the last 100 batches
```

- `Source: sql/api/src/main/scala/org/apache/spark/sql/streaming/progress.scala` — StreamingQueryProgress.durationMs map with "walCommit" key provides commit write duration per batch

### AC3 — Checkpoint Failure Count

```
Given a Structured Streaming query has experienced checkpoint write failures (e.g., HDFS unavailability, permission errors)
When the data platform administrator views the checkpoint status panel
Then the cumulative failure count is displayed as an integer
And each failure is logged with its batch ID, error type, and timestamp in an expandable failure history section (last 20 failures)
```

- `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala` — checkpoint write exceptions intercepted during offset/commit log writes

### AC4 — Checkpoint Storage Size

```
Given a Structured Streaming query has been running with checkpointing for at least 10 batches
When the data platform administrator views the checkpoint status panel
Then the total checkpoint storage size is displayed in human-readable format (KB, MB, GB) calculated from the checkpoint directory size on the configured filesystem
And the size includes offset log files, commit log files, state store checkpoint files, and metadata files
```

- `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/CheckpointFileManager.scala` — filesystem abstraction with list, exists, and mkdirs methods for traversing checkpoint directory tree (offsets/, commits/, state/, metadata, sources/)

### AC5 — Error Handling (No Checkpoint Configured)

```
Given a Structured Streaming query is running without a checkpoint location (memory sink or testing mode)
When the data platform administrator views the Structured Streaming tab
Then the checkpoint status panel displays "Checkpointing Not Configured" as a text label
And no checkpoint metrics, charts, or failure history sections are rendered for that query
```

- `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala` — resolvedCheckpointRoot is checked for null or empty to determine checkpoint availability

### AC6 — REST API Exposure

```
Given a Structured Streaming query is running with checkpointing enabled
When the data platform administrator queries the REST API endpoint /api/v1/applications/[app-id]/streaming/statistics
Then the JSON response includes a "checkpoint" object with fields: lastCommittedBatchId (Long), lastCommitTimestamp (ISO8601 string), checkpointDurationMs (Long), failureCount (Integer), and checkpointSizeBytes (Long)
```

- `Source: docs/monitoring.md` — REST API endpoints (/api/v1/) for application-level streaming statistics
- `Source: docs/web-ui.md` — Structured Streaming tab as the primary UI extension point

### AC7 — Edge Case (Async Progress Tracking)

```
Given a Structured Streaming query is running with async progress tracking enabled (spark.sql.streaming.asyncProgressTracking.enabled=true)
When the data platform administrator views the checkpoint status panel
Then the checkpoint duration reflects the async commit timing (AsyncOffsetSeqLog and AsyncCommitLog write times)
And an "Async Checkpointing" label is displayed next to the checkpoint mode indicator
```

- `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/AsyncOffsetSeqLog.scala` — async variant with ConcurrentHashMap for pending offset writes and AtomicLong for tracking last commit timestamps
- `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/AsyncCommitLog.scala` — async variant with ConcurrentLinkedDeque tracking batches written to durable storage
- `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/AsyncProgressTrackingMicroBatchExecution.scala` — async checkpoint mode execution with configurable checkpointing interval

## Sub-Tasks

- **Sub-task 1:** Extract checkpoint metadata from StreamExecution — resolvedCheckpointRoot, latest committed batchId from CommitLog, and commit timestamp from the commit log entry
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala` — resolvedCheckpointRoot (String), offsetLog (OffsetSeqLog), commitLog (CommitLog)
- **Sub-task 2:** Calculate checkpoint duration by measuring the time between OffsetSeqLog write and CommitLog write for each batch using durationMs["walCommit"] from StreamingQueryProgress
  - `Source: sql/api/src/main/scala/org/apache/spark/sql/streaming/progress.scala` — StreamingQueryProgress.durationMs map contains "walCommit" key with Long millisecond values; batchId identifies the batch
- **Sub-task 3:** Implement checkpoint failure counter by intercepting checkpoint write exceptions in StreamExecution and maintaining a thread-safe AtomicLong counter with failure details (batchId, error class, timestamp)
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala` — checkpoint write operations occur in the streaming execution thread
- **Sub-task 4:** Calculate checkpoint storage size by querying the CheckpointFileManager for the total size of the checkpoint directory tree (offsets/, commits/, state/, metadata, sources/)
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/CheckpointFileManager.scala` — trait provides list(path: Path): Array[FileStatus] for directory traversal and exists(path: Path): Boolean for path validation
- **Sub-task 5:** Build checkpoint status panel in the Structured Streaming tab UI displaying: checkpoint path, last committed batch ID, last commit timestamp, checkpoint duration, failure count, and storage size
  - `Source: docs/web-ui.md` — Structured Streaming tab serves as the primary display surface
- **Sub-task 6:** Implement checkpoint duration trend chart storing the last 100 batch checkpoint durations in a circular buffer
- **Sub-task 7:** Build expandable failure history section showing the last 20 checkpoint failures with batch ID, error type, and timestamp
- **Sub-task 8:** Handle "Checkpointing Not Configured" case by checking if resolvedCheckpointRoot is null or empty
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala` — resolvedCheckpointRoot parameter in the constructor
- **Sub-task 9:** Handle async progress tracking mode by detecting AsyncOffsetSeqLog/AsyncCommitLog usage and adjusting duration calculations
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/AsyncProgressTrackingMicroBatchExecution.scala` — extends MicroBatchExecution with asyncProgressTrackingCheckpointingIntervalMs
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/AsyncOffsetSeqLog.scala` — async non-blocking offset log with pendingOffsetWrites ConcurrentHashMap
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/AsyncCommitLog.scala` — async non-blocking commit log with writtenToDurableStorage ConcurrentLinkedDeque
- **Sub-task 10:** Extend the REST API endpoint (/api/v1/applications/[app-id]/streaming/statistics) with the checkpoint JSON object
  - `Source: docs/monitoring.md` — REST API endpoint patterns and JSON response structure
- **Sub-task 11:** Write unit tests for checkpoint duration calculation, failure counter, size computation, and async mode detection
- **Sub-task 12:** Write integration tests validating checkpoint status display with both synchronous and async progress tracking modes

## Edge Cases

- **Edge Case 1 (Empty/Null Input):** When a streaming query has just started and no batches have been committed yet (CommitLog is empty), display "No Batches Committed" for the last commit fields, show checkpoint duration as "N/A", and display failure count as 0.
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/CommitLog.scala` — HDFSMetadataLog returns None when no entries exist; CommitLog.getLatest() returns None for an empty log

- **Edge Case 2 (Boundary Values):** When checkpoint storage size exceeds 100 GB, display a "Large Checkpoint" warning badge. When checkpoint duration exceeds 10,000 milliseconds (10 seconds), highlight the duration value in red to indicate a performance concern. When failure count reaches 100 or more, display a critical alert recommending checkpoint directory investigation.

- **Edge Case 3 (Invalid Input):** When the checkpoint directory path is inaccessible (permissions error, filesystem unmounted, or network partition), display "Checkpoint Path Inaccessible" with the error message and do not attempt repeated filesystem queries — retry only on the next batch completion event.
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/CheckpointFileManager.scala` — filesystem operations (list, exists) may throw FileNotFoundException or IOException on inaccessible paths

- **Edge Case 4 (Checkpoint Directory Cleanup):** When checkpoint log files have been purged by the automatic log cleanup mechanism (old offset and commit files deleted), the failure history reflects only failures that occurred after the last cleanup, and the checkpoint size reflects the current directory contents — not historical peak size.
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala` — minLogEntriesToMaintain configuration controls how many log entries are retained before purging

- **Edge Case 5 (Multiple Checkpoint Locations):** When a streaming application runs multiple queries each with different checkpoint locations, display each query's checkpoint status independently with its own path, metrics, and failure history without cross-contamination between queries.
  - `Source: sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala` — each StreamExecution instance has its own resolvedCheckpointRoot, offsetLog, and commitLog scoped to a single streaming query

## Dependencies

- **FEATURE-005-01:** Stream Health Monitoring (parent feature) — [FEATURE-005-01-stream-health-monitoring.md](../FEATURE-005-01-stream-health-monitoring.md)
- **EPIC-005:** Enhanced Real-Time Stream Observability (parent epic) — [EPIC-005-enhanced-stream-observability.md](../../EPIC-005-enhanced-stream-observability.md)
- **F-003:** Real-Time Stream Processing (Structured Streaming with checkpointing)
- **OffsetSeqLog:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/OffsetSeqLog.scala` — HDFSMetadataLog-based offset log for batch offset persistence; serializes OffsetSeq objects with version headers and per-source offset JSON
- **CommitLog:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/CommitLog.scala` — HDFSMetadataLog-based commit log for batch completion tracking with version-controlled serialization of CommitMetadata (version string + JSON metadata)
- **AsyncOffsetSeqLog:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/AsyncOffsetSeqLog.scala` — async variant for non-blocking offset writes using ConcurrentHashMap for pending writes and AtomicLong for last commit timestamp tracking
- **AsyncCommitLog:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/AsyncCommitLog.scala` — async variant for non-blocking commit writes using ConcurrentLinkedDeque to track batches written to durable storage
- **CheckpointFileManager:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/checkpointing/CheckpointFileManager.scala` — filesystem abstraction for checkpoint operations providing createAtomic, open, list, mkdirs, exists, delete, isLocal, and createCheckpointDirectory methods
- **StreamExecution:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/StreamExecution.scala` — abstract streaming query execution class providing resolvedCheckpointRoot (String), offsetLog (OffsetSeqLog), commitLog (CommitLog), query lifecycle management, and minLogEntriesToMaintain configuration
- **AsyncProgressTrackingMicroBatchExecution:** `sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/runtime/AsyncProgressTrackingMicroBatchExecution.scala` — async checkpoint mode execution extending MicroBatchExecution with configurable asyncProgressTrackingCheckpointingIntervalMs and single-thread executor for serialized async writes
- **StreamingQueryProgress.durationMs:** `sql/api/src/main/scala/org/apache/spark/sql/streaming/progress.scala` — per-batch progress reporting with durationMs map containing "walCommit" key (Long milliseconds) and batchId (Long) for checkpoint duration tracking
- **Spark Web UI:** `docs/web-ui.md` — Structured Streaming tab as the existing UI extension point for stream health monitoring panels
- **Monitoring Guide:** `docs/monitoring.md` — REST API endpoints (/api/v1/), event logging, History Server, and metric sink configuration (Prometheus, Graphite, StatsD, JMX, CSV, Console)

## Story Estimation Guidance

- **Story Points: 8** (Fibonacci scale)
- **Rationale:** Involves checkpoint metadata extraction from StreamExecution internals (resolvedCheckpointRoot, OffsetSeqLog, CommitLog), checkpoint duration calculation from StreamingQueryProgress.durationMs["walCommit"], failure counter implementation with thread-safe AtomicLong tracking, filesystem size query via CheckpointFileManager.list() traversal, UI panel construction with trend chart (last 100 batches) and expandable failure history (last 20 failures), REST API extension with checkpoint JSON object, and async progress tracking mode handling (AsyncOffsetSeqLog/AsyncCommitLog detection). Higher complexity due to filesystem I/O for size computation across checkpoint subdirectories (offsets/, commits/, state/, metadata, sources/), failure interception in the streaming execution path, and support for both synchronous and async checkpoint modes. Completable within a single sprint with focused effort.

## Definition of Done

- Checkpoint status panel displays: checkpoint path, last committed batch ID, last commit timestamp, checkpoint duration, failure count, and storage size for each active Structured Streaming query
- Checkpoint duration trend chart renders the last 100 batch durations in the checkpoint status panel
- Expandable failure history shows the last 20 checkpoint failures with batch ID, error type, and timestamp
- "Checkpointing Not Configured" is displayed for queries without a checkpoint location (resolvedCheckpointRoot is null or empty)
- REST API endpoint (/api/v1/applications/[app-id]/streaming/statistics) includes a "checkpoint" JSON object with fields: lastCommittedBatchId, lastCommitTimestamp, checkpointDurationMs, failureCount, and checkpointSizeBytes
- Async progress tracking mode is detected (AsyncOffsetSeqLog/AsyncCommitLog usage) and checkpoint duration adjusted to reflect async commit timing with "Async Checkpointing" label displayed
- "No Batches Committed" fallback is shown for newly started queries with an empty CommitLog
- Checkpoint path inaccessibility is handled with "Checkpoint Path Inaccessible" error message display and retry deferred to the next batch completion event
- Unit tests achieve at least 90% line coverage for checkpoint status logic including duration calculation, failure counter, size computation, and async mode detection
- Integration tests validate checkpoint status display with both synchronous and async checkpointing modes
- Code reviewed and merged to the feature branch
