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
import org.apache.spark.shuffle.ShuffleHandle

/**
 * A shuffle handle implementation for streaming shuffle operations that carries shuffle metadata
 * including buffer allocation information.
 *
 * This handle is used by StreamingShuffleManager to pass streaming-specific configuration between
 * the driver (during shuffle registration) and executor tasks (during shuffle write/read). Unlike
 * BaseShuffleHandle which only carries the shuffle dependency, StreamingShuffleHandle also includes
 * the allocated buffer size that determines memory-mapped I/O buffer capacity for streaming data
 * directly from map tasks to reduce tasks.
 *
 * The buffer size is calculated during shuffle registration as:
 * {{{
 *   bufferSizeBytes = (executorMemory * bufferSizePercent) / numPartitions
 * }}}
 * where bufferSizePercent is configurable via
 * spark.shuffle.streaming.bufferSizePercent (default 20%).
 *
 * This handle enables StreamingShuffleWriter to:
 *  - Allocate per-partition memory buffers of the correct size
 *  - Stream data directly to consumers without full materialization
 *  - Trigger automatic disk spill when buffer utilization exceeds threshold
 *  - Coordinate with MemorySpillManager for graceful degradation
 *
 * And enables StreamingShuffleReader to:
 *  - Request partial blocks before shuffle completion
 *  - Send acknowledgments for buffer reclamation
 *  - Detect producer failures via connection timeout
 *
 * @param shuffleId the unique identifier for this shuffle operation
 * @param bufferSizeBytes the allocated buffer size in bytes per partition for streaming operations,
 *                        calculated based on executor memory and configuration parameters
 * @param dependency the shuffle dependency containing partitioner, serializer, aggregator, and
 *                   other shuffle metadata required for proper data transformation
 *
 * @tparam K the key type for shuffle records
 * @tparam V the value type for shuffle records
 * @tparam C the combiner type for aggregated shuffle records (may be same as V if no aggregation)
 */
private[spark] class StreamingShuffleHandle[K, V, C](
    shuffleId: Int,
    val bufferSizeBytes: Long,
    val dependency: ShuffleDependency[K, V, C])
  extends ShuffleHandle(shuffleId) {

  require(bufferSizeBytes > 0,
    s"Buffer size must be positive, got $bufferSizeBytes bytes")
  require(dependency != null,
    "ShuffleDependency cannot be null")

  /**
   * Returns a human-readable string representation of this handle for debugging and logging.
   * Includes shuffle ID, buffer size, and partition count from the dependency.
   */
  override def toString: String = {
    s"StreamingShuffleHandle(shuffleId=$shuffleId, " +
      s"bufferSizeBytes=$bufferSizeBytes, " +
      s"numPartitions=${dependency.partitioner.numPartitions})"
  }
}
