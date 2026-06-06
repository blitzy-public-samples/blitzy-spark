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

import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.BaseShuffleHandle

/**
 * Subclass of [[BaseShuffleHandle]] used to mark a shuffle for the OPT-IN streaming path. Its
 * presence makes `StreamingShuffleManager.getWriter`/`getReader` dispatch to the streaming
 * writer/reader; any other handle type is delegated to the composed `SortShuffleManager` (the
 * default + graceful-degradation fallback). Mirrors `SerializedShuffleHandle` /
 * `BypassMergeSortShuffleHandle`.
 *
 * Coexistence strategy: the streaming engine never replaces sort-based shuffle. This marker is the
 * single dispatch signal that lets `StreamingShuffleManager` route a shuffle to the streaming
 * writer/reader while every other handle continues to flow to the unmodified sort engine, so the
 * two implementations run side by side and graceful degradation is automatic.
 *
 * Unlike the sort handles (parameterized `[K, V]`, whose combiner type always equals the value
 * type `V`), this handle carries the full `[K, V, C]` type triple so the manager can recover and
 * dispatch the reader's combiner type `C`.
 *
 * Beyond the dispatch role, the handle carries one piece of pre-computed shuffle metadata that
 * the executor-side [[StreamingShuffleReader]] cannot otherwise obtain: `numMaps`, the total
 * number of producer map tasks for this shuffle. It is captured by
 * `StreamingShuffleManager.registerShuffle` on the DRIVER (where `dependency.rdd` is non-null) as
 * `dependency.rdd.partitions.length` -- the same map count the DAG scheduler registers with the
 * `MapOutputTracker`. It MUST travel inside the handle: `ShuffleDependency.rdd` is `@transient`,
 * so it is null once the handle is deserialized on an executor, leaving the reader no way to
 * recompute the count from the dependency. The reader uses it to resolve the
 * `ShuffleManager.getReader(endMapIndex = Int.MaxValue)` "all maps" sentinel into a concrete
 * completion target (QA finding F-1). As a plain `Int` field of this `Serializable` handle it is
 * carried to executors automatically, so no MapOutputTracker RPC and no scheduler change are
 * needed -- preserving the coexistence/least-modification discipline.
 *
 * The handle remains a thin, immutable data carrier: it stores `numMaps` and adds no behavior
 * beyond the `shuffleId` and the [[ShuffleDependency]] already captured by [[BaseShuffleHandle]].
 *
 * @param shuffleId the unique identifier of the shuffle, forwarded to [[BaseShuffleHandle]]
 * @param dependency the [[ShuffleDependency]] describing this shuffle, forwarded to the superclass
 * @param numMaps the total number of producer map tasks for this shuffle, captured on the driver so
 *                the executor-side reader can resolve the `endMapIndex = Int.MaxValue` sentinel
 * @tparam K the type of the keys being shuffled
 * @tparam V the type of the map-output values
 * @tparam C the type of the combined values produced on the reduce side
 */
private[spark] class StreamingShuffleHandle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C],
    val numMaps: Int)
  extends BaseShuffleHandle[K, V, C](shuffleId, dependency) {
}
