/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.streaming

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.BlockManager

/**
 * The map-side half of streaming shuffle's RUNTIME graceful degradation. `StreamingShuffleManager`
 * wraps every streaming [[StreamingShuffleWriter]] in this writer so that a runtime fallback
 * condition surfaced DURING `write` -- an unsplittable record over the 2MB pipelined cap, a
 * sustained-slow consumer, or an admission deadline under memory pressure, each raised as a
 * [[StreamingShuffleFallbackException]] -- is automatically and transparently re-routed to the
 * composed `SortShuffleManager` instead of failing the task.
 *
 * This is the manager-owned fallback path the design mandates (AAP sections 0.1.3 and 0.4.1:
 * "compose a `SortShuffleManager` ... and delegate to it whenever ... a fallback condition fires").
 * Crucially, the re-route happens BEFORE any map output is advertised: the streaming writer
 * publishes blocks to consumers during `write`, but the map's [[MapStatus]] is only returned from
 * [[stop]] AFTER `write` completes. So when a fallback fires mid-`write`, this writer (1) cleans
 * ALL partial streaming state by stopping the streaming writer unsuccessfully (which frees its
 * buffered/spilled blocks and notifies readers), then (2) replays the COMPLETE record sequence into
 * a real `SortShuffleWriter` and (3) returns the sort writer's `MapStatus` from `stop`. The
 * unmodified `MapOutputTracker`/DAG scheduler therefore see ordinary sort-based output and the
 * scheduler contract is preserved -- no generic task failure, no recompute, zero regression.
 *
 * Replay mechanism: because `StreamingShuffleWriter.write` consumes the input iterator as it
 * streams, the records pulled before the fallback would otherwise be lost. [[write]] tees every
 * record into a bounded, disk-spillable [[SpillableReplayBuffer]] BEFORE the streaming writer sees
 * it; on fallback the sort writer is fed `replayBuffer.iterator ++ remainingSourceRecords`, which
 * is exactly the original sequence (consumed-then-buffered records, followed by the not-yet-pulled
 * tail of the source iterator). The buffer bounds heap use by spilling large maps to a temp file,
 * and is discarded unread on the streaming happy path -- a documented v1 overhead of the fallback
 * guarantee, paid only to make degradation seamless.
 *
 * Coexistence and isolation: this class lives in the streaming package and is constructed ONLY on
 * the streaming dispatch path; sort-based shuffle never sees it. It composes -- never modifies --
 * `SortShuffleManager`, obtaining the sort writer through the public `getWriter` exactly as a plain
 * sort shuffle would (the [[StreamingShuffleHandle]] IS-A `BaseShuffleHandle`, so the sort manager
 * dispatches it to a `SortShuffleWriter` and registers the map id in its own tracking, so the
 * composed manager's `unregisterShuffle` later reclaims the sort output).
 *
 * Threading: a single map task thread drives `write` then `stop`; no shared mutable state is
 * exposed.
 *
 * @param handle the streaming handle for this shuffle, reused verbatim to build the sort writer
 * @param mapId this map task's id, forwarded to the sort writer and its `MapStatus`
 * @param context the active task context (used for a completion listener that reclaims the buffer)
 * @param metrics the write-metrics reporter, shared by whichever underlying writer is active
 * @param streamingWriter the wrapped streaming writer attempted first
 * @param sortShuffleManager the composed sort manager used to build the fallback writer on demand
 * @param blockManager the block manager whose disk store backs the spillable replay buffer
 * @param replayMemoryThresholdBytes in-memory size at which the replay buffer spills to disk
 * @tparam K the type of the keys being written
 * @tparam V the type of the values being written
 */
private[spark] class FallbackShuffleWriter[K, V](
    handle: StreamingShuffleHandle[K, V, _],
    mapId: Long,
    context: TaskContext,
    metrics: ShuffleWriteMetricsReporter,
    streamingWriter: ShuffleWriter[K, V],
    sortShuffleManager: SortShuffleManager,
    blockManager: BlockManager,
    replayMemoryThresholdBytes: Long)
  extends ShuffleWriter[K, V] with Logging {

  // Captures every record the streaming writer pulls so the full sequence can be replayed into sort
  // on a fallback. Bounded in memory (spills to a temp file past the threshold); discarded unread
  // on the streaming happy path.
  private val replayBuffer =
    new SpillableReplayBuffer(
      handle.dependency.serializer, blockManager, replayMemoryThresholdBytes)

  // The sort writer, created lazily ONLY if a fallback fires. While null, the streaming writer is
  // the active writer and owns stop()/getPartitionLengths(); once set, the sort writer takes over.
  private var sortWriter: ShuffleWriter[K, V] = _

  // Backstop cleanup: guarantee the replay buffer's heap bytes / spill file are released even if
  // stop() is never reached (e.g. a failure path that bypasses it). close() is idempotent.
  context.addTaskCompletionListener[Unit](_ => replayBuffer.close())

  /**
   * Attempts the streaming write, teeing each record into the replay buffer first. If the streaming
   * writer signals a runtime fallback, transparently degrades to sort-based shuffle by replaying
   * the complete record sequence into a `SortShuffleWriter`. Any other exception propagates
   * unchanged so the existing failure/cleanup path runs.
   */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // Tee BEFORE the streaming writer consumes each record, so the replay buffer holds everything
    // it pulled even if a fallback fires mid-stream.
    val teed: Iterator[Product2[K, V]] = records.map { rec =>
      replayBuffer.append(rec._1, rec._2)
      rec
    }
    try {
      streamingWriter.write(teed)
      // Streaming succeeded end to end: it owns the output + MapStatus; the replay buffer is unused
      // insurance, reclaimed in stop()/the completion listener.
    } catch {
      case fb: StreamingShuffleFallbackException =>
        // `records` (the RAW source) is positioned at the first record the streaming writer had not
        // yet pulled; pass it as the remaining tail so the replay does not re-tee into the buffer.
        fallBackToSort(fb, records)
    }
  }

  // Re-route this map to sort-based shuffle after a runtime fallback signal. Cleans partial
  // streaming state, builds the sort writer, and replays the full record sequence so the map
  // produces correct sort output. Surfaces NO task failure -- the scheduler sees ordinary sort
  // output.
  private def fallBackToSort(
      signal: StreamingShuffleFallbackException,
      remainingSource: Iterator[Product2[K, V]]): Unit = {
    // Operationally meaningful but bounded (at most one line per fallen-back map task), so logged
    // at INFO without risking the 10MB/hour/executor log-volume budget; fallback is the rare path.
    logInfo(s"Streaming shuffle for shuffle ${handle.shuffleId} map $mapId degrading to " +
      s"sort-based shuffle: ${signal.getMessage}")
    // 1. Clean ALL partial streaming state. stop(false) frees this map's buffered/spilled blocks
    // via MemorySpillManager.unregisterMap and notifies any subscribed readers via producerFailed;
    // it is idempotent. The streaming MapStatus is never returned, so no streamed output is
    // advertised -- there is nothing half-published for a consumer to read.
    streamingWriter.stop(false)
    // 2. Build the sort writer for THIS handle + map. SortShuffleManager.getWriter accepts the
    // StreamingShuffleHandle (it IS-A BaseShuffleHandle) and returns a SortShuffleWriter, and
    // registers the map id in the sort manager's own tracking, so StreamingShuffleManager.
    // unregisterShuffle (which delegates to sort) reclaims this output during cleanup.
    val writer = sortShuffleManager.getWriter[K, V](handle, mapId, context, metrics)
    sortWriter = writer
    // 3. Replay the COMPLETE sequence: every record the streaming writer pulled (now in the replay
    // buffer, in append order) followed by the records it had not yet pulled (the raw source tail,
    // consumed WITHOUT re-teeing now that the buffer is read-only). Each replayed (Any, Any) pair
    // is cast to Product2[K, V] -- a Tuple2 IS a Product2 -- mirroring how the reader casts
    // deserialized pairs.
    val replayed: Iterator[Product2[K, V]] =
      replayBuffer.iterator.map(_.asInstanceOf[Product2[K, V]]) ++ remainingSource
    writer.write(replayed)
  }

  /**
   * Closes whichever writer is active and returns its [[MapStatus]]: the sort writer if a fallback
   * occurred (the streaming writer was already stopped unsuccessfully during fallback), otherwise
   * the streaming writer. Always reclaims the replay buffer. Idempotent through the underlying
   * writers' own stop() idempotency.
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (sortWriter != null) {
        sortWriter.stop(success)
      } else {
        streamingWriter.stop(success)
      }
    } finally {
      // Release the replay buffer's heap bytes and delete any spill file. Idempotent; the
      // completion listener is the backstop if stop() is bypassed.
      replayBuffer.close()
    }
  }

  /** Delegates to the active writer's partition lengths (sort after fallback, else streaming). */
  override def getPartitionLengths(): Array[Long] = {
    if (sortWriter != null) {
      sortWriter.getPartitionLengths()
    } else {
      streamingWriter.getPartitionLengths()
    }
  }
}
