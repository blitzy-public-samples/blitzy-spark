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
 * A [[org.apache.spark.shuffle.ShuffleHandle ShuffleHandle]] for the streaming shuffle
 * implementation.
 *
 * Extends [[BaseShuffleHandle]] to carry the [[ShuffleDependency]] reference and
 * streaming-specific metadata that the Manager populates at registration time.
 * This metadata is serialized to executors and used by [[StreamingShuffleWriter]]
 * and [[StreamingShuffleReader]] at task runtime to configure buffer allocation
 * and spill behavior.
 *
 * Enables type-based dispatch in [[StreamingShuffleManager]]'s `getWriter()` and
 * `getReader()` methods. Created by `StreamingShuffleManager.registerShuffle()`.
 *
 * This handle type is also checked in `ShuffleWriteProcessor` to skip the push-based
 * shuffle block pushing path, which is only applicable to sort-based shuffle.
 *
 * Coexistence note: This handle coexists with `BaseShuffleHandle`, `SerializedShuffleHandle`,
 * and `BypassMergeSortShuffleHandle`. It is only created when streaming shuffle is enabled
 * via `spark.shuffle.streaming.enabled=true`.
 *
 * @param shuffleId The unique identifier for this shuffle
 * @param dependency The shuffle dependency containing partitioner, serializer, and aggregator
 * @param bufferSizePercent Percentage of executor memory allocated to streaming buffers
 *                          (range: [1, 50], from `spark.shuffle.streaming.bufferSizePercent`)
 * @param spillThreshold Buffer occupancy percentage threshold for triggering automatic disk
 *                       spill (range: [50, 95], from `spark.shuffle.streaming.spillThreshold`)
 * @param expectedPartitionCount Number of partitions in this shuffle, used for per-partition
 *                               buffer size calculation:
 *                               `(executorMemory * bufferSizePercent) / expectedPartitionCount`
 * @tparam K the key type of shuffle records
 * @tparam V the value type of shuffle records
 * @tparam C the combiner type after aggregation
 */
private[spark] class StreamingShuffleHandle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C],
    val bufferSizePercent: Int,
    val spillThreshold: Int,
    val expectedPartitionCount: Int)
  extends BaseShuffleHandle[K, V, C](shuffleId, dependency) {
}
