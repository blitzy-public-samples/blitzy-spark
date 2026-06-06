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

import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper

/**
 * In-memory shuffle-block checksum entry point for the OPT-IN streaming shuffle engine.
 *
 * Coexistence / reuse strategy: this helper introduces NO new checksum implementation. It is a
 * thin, streaming-package-private adapter that delegates to the EXISTING Spark checksum facility,
 * [[ShuffleChecksumHelper.getChecksumByAlgorithm]] -- the very same facility the sort-based shuffle
 * path uses (it backs `org.apache.spark.shuffle.ShuffleChecksumUtils.compareChecksums`,
 * `IndexShuffleBlockResolver`, and `ShuffleBlockFetcherIterator`). Because the algorithm and byte
 * semantics are identical to those paths (CRC32C by default, governed by
 * `spark.shuffle.checksum.algorithm`), a value produced here is directly comparable to one produced
 * anywhere else in Spark's shuffle.
 *
 * Why it lives here rather than on `ShuffleChecksumUtils`: the streaming engine is confined to the
 * `org.apache.spark.shuffle.streaming` package so that no existing shuffle component is modified or
 * forced to depend on streaming-only code (the zero-cross-contamination discipline). The on-disk,
 * file-oriented `ShuffleChecksumUtils` had no in-memory byte-range entry, and adding one to it
 * would extend a reference-only facility outside the streaming boundary; this object provides that
 * single, shared in-memory entry point for streaming callers instead, while still reusing the one
 * underlying checksum facility. It is `private[streaming]` so only the streaming writer/reader (and
 * their tests) can see it, mirroring the package-private `SpillableReplayBuffer` and
 * `StreamingShuffleEndpointCoordinator`.
 */
private[streaming] object StreamingShuffleChecksum {

  /**
   * Compute the checksum of an in-memory byte range using the configured shuffle checksum algorithm
   * (CRC32C by default), delegating to the existing
   * [[ShuffleChecksumHelper.getChecksumByAlgorithm]] facility. No new checksum implementation is
   * introduced; the algorithm and byte semantics are identical to Spark's on-disk shuffle checksum
   * path, so a value produced here is directly comparable to one produced there.
   *
   * @param algorithm the checksum algorithm name (e.g. the value of
   *                   `spark.shuffle.checksum.algorithm`, such as "ADLER32" or "CRC32C")
   * @param data      the buffer to checksum
   * @param offset    the start offset within `data`
   * @param length    the number of bytes to checksum starting at `offset`
   * @return the computed checksum value
   */
  def computeChecksum(algorithm: String, data: Array[Byte], offset: Int, length: Int): Long = {
    val checksum = ShuffleChecksumHelper.getChecksumByAlgorithm(algorithm)
    checksum.update(data, offset, length)
    checksum.getValue
  }

  /**
   * Convenience overload of [[computeChecksum]] that checksums an entire byte array. See the
   * four-argument overload for the algorithm and reuse semantics.
   *
   * @param algorithm the checksum algorithm name
   * @param data      the buffer to checksum in full
   * @return the computed checksum value
   */
  def computeChecksum(algorithm: String, data: Array[Byte]): Long =
    computeChecksum(algorithm, data, 0, data.length)
}
