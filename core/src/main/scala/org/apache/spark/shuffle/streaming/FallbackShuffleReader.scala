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

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ShuffleReader

/**
 * The reduce-side half of streaming shuffle's runtime graceful degradation.
 * `StreamingShuffleManager` wraps every streaming [[StreamingShuffleReader]] in this reader so
 * that, when the streaming read cannot proceed and NO record has been yielded yet, the read
 * transparently degrades to the composed `SortShuffleManager`'s reader instead of failing the
 * stage.
 *
 * Safety boundary -- "before any record is yielded": the underlying [[StreamingShuffleReader]] is
 * constructed with `fallbackOnSilence = true`, which makes it raise a
 * [[StreamingReadFallbackException]] (instead of its usual
 * [[org.apache.spark.shuffle.FetchFailedException]]) ONLY when it hits producer silence/failure
 * BEFORE emitting a single record. That precondition is what makes delegating to the sort reader
 * duplication-free: the sort reader re-reads the whole partition from scratch, so it is safe to
 * switch ONLY while the downstream consumer has seen nothing. Once any record has been yielded, the
 * streaming reader keeps its existing behavior and throws `FetchFailedException`, which the
 * unmodified DAG scheduler turns into upstream recomputation -- the AAP-sanctioned fault path.
 *
 * What it covers: the pure-fallback shuffle, where every producer map degraded to sort (so sort
 * output exists for all maps) and the reduce task can read it all through the sort reader. By
 * design in v1, reduce-side fallback is whole-partition (it rebuilds one sort reader for the entire
 * range) rather than per-map. So for a MIXED shuffle -- where some maps streamed successfully and
 * thus have no sort output -- the sort reader cannot locate those maps' blocks and the block fetch
 * raises `FetchFailedException`, which the unmodified DAG scheduler turns into upstream
 * recomputation. That outcome is correct and loses no data; it is the documented v1 behavior for
 * mixed shuffles, consistent with the AAP's fault-recovery-via-FetchFailedException model.
 *
 * Coexistence and isolation: confined to the streaming package and constructed ONLY on the
 * streaming dispatch path. It composes -- never modifies -- `SortShuffleManager`, building the sort
 * reader lazily (only if a fallback actually fires) through the public `getReader`, so the
 * streaming happy path pays nothing for it.
 *
 * @param streamingReader the streaming reader attempted first (built with `fallbackOnSilence=true`)
 * @param buildSortReader a thunk that builds the composed sort manager's reader for the SAME range,
 *                        invoked at most once and only when a pre-yield fallback signal fires
 * @tparam K the type of the keys being read
 * @tparam C the type of the combined values being read
 */
private[spark] class FallbackShuffleReader[K, C](
    streamingReader: ShuffleReader[K, C],
    buildSortReader: () => ShuffleReader[K, C])
  extends ShuffleReader[K, C] with Logging {

  /**
   * Reads records, degrading to the sort reader if the streaming reader signals pre-yield silence.
   * The signal can surface either eagerly inside `streamingReader.read()` (when an aggregator or
   * key ordering consumes the streamed records within `read`) or lazily on the first pull of the
   * returned iterator (the plain pass-through case); both are handled.
   */
  override def read(): Iterator[Product2[K, C]] = {
    try {
      // May consume eagerly (aggregation/ordering); a pre-yield silence signal surfaces here.
      val primary = streamingReader.read()
      // Otherwise the signal surfaces lazily on the first element pull.
      new FallbackOnFirstPullIterator(primary)
    } catch {
      case fb: StreamingReadFallbackException =>
        logInfo(s"Streaming shuffle read degrading to sort-based shuffle before yielding any " +
          s"record: ${fb.getMessage}")
        buildSortReader().read()
    }
  }

  /**
   * Wraps the streaming record iterator and, until the FIRST element is produced, catches a
   * [[StreamingReadFallbackException]] and switches to the sort reader's iterator. After the first
   * element is produced the guard is dropped: streaming is delivering, so any later failure must
   * propagate as `FetchFailedException` (switching then would duplicate already-yielded records).
   */
  private final class FallbackOnFirstPullIterator(primary: Iterator[Product2[K, C]])
    extends Iterator[Product2[K, C]] {

    // The iterator currently being drained: the streaming iterator initially, the sort iterator
    // after a pre-yield fallback.
    private var active: Iterator[Product2[K, C]] = primary
    // True only until the first element has been produced (or we have already switched to sort).
    private var guarding: Boolean = true

    override def hasNext: Boolean = {
      if (!guarding) {
        active.hasNext
      } else {
        try {
          active.hasNext
        } catch {
          case fb: StreamingReadFallbackException => switchToSort(fb); active.hasNext
        }
      }
    }

    override def next(): Product2[K, C] = {
      if (!guarding) {
        active.next()
      } else {
        val record =
          try {
            active.next()
          } catch {
            case fb: StreamingReadFallbackException => switchToSort(fb); active.next()
          }
        // First element produced from the streaming path: streaming is delivering, stop guarding so
        // a later failure surfaces as FetchFailedException (recompute) rather than a duplicating
        // sort re-read.
        guarding = false
        record
      }
    }

    // Replace the streaming iterator with the sort reader's iterator. Called at most once, and only
    // before any record has been yielded, so the sort reader's from-scratch read cannot duplicate.
    private def switchToSort(signal: StreamingReadFallbackException): Unit = {
      logInfo(s"Streaming shuffle read degrading to sort-based shuffle before yielding any " +
        s"record: ${signal.getMessage}")
      active = buildSortReader().read()
      guarding = false
    }
  }
}

/**
 * Internal, package-private signal raised by [[StreamingShuffleReader]] (only when it is built with
 * `fallbackOnSilence = true`) to tell its enclosing [[FallbackShuffleReader]] to degrade this
 * reduce read to sort-based shuffle. It is raised STRICTLY before any record is yielded, so the
 * switch is duplication-free.
 *
 * This is deliberately distinct from [[org.apache.spark.shuffle.FetchFailedException]]: the latter
 * means "streaming output is gone, recompute upstream" and is the reader's behavior once records
 * have already been yielded (or when no fallback wrapper is active); this exception means
 * "streaming never got started, try sort instead" and never escapes the [[FallbackShuffleReader]].
 */
private[spark] class StreamingReadFallbackException(message: String)
  extends SparkException(message)
