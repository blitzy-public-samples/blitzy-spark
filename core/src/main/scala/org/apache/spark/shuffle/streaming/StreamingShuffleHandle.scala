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
 * This is a pure dispatch marker: it introduces no state or behavior beyond the `shuffleId` and
 * the [[ShuffleDependency]] already captured by [[BaseShuffleHandle]].
 *
 * @param shuffleId the unique identifier of the shuffle, forwarded to [[BaseShuffleHandle]]
 * @param dependency the [[ShuffleDependency]] describing this shuffle, forwarded to the superclass
 * @tparam K the type of the keys being shuffled
 * @tparam V the type of the map-output values
 * @tparam C the type of the combined values produced on the reduce side
 */
private[spark] class StreamingShuffleHandle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C])
  extends BaseShuffleHandle[K, V, C](shuffleId, dependency) {
}
