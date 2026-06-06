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

package org.apache.spark.shuffle

import java.io.{DataInputStream, File, FileInputStream}
import java.util.zip.CheckedInputStream

import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.{BlockId, ShuffleChecksumBlockId, ShuffleDataBlockId}

object ShuffleChecksumUtils {

  /**
   * Return checksumFile for shuffle data block ID. Otherwise, null.
   */
  def getChecksumFileName(blockId: BlockId, algorithm: String): String = blockId match {
    case ShuffleDataBlockId(shuffleId, mapId, _) =>
      ShuffleChecksumHelper.getChecksumFileName(
        ShuffleChecksumBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name, algorithm)
    case _ =>
      null
  }

  /**
   * Ensure that the checksum values are consistent with index file and data file.
   */
  def compareChecksums(
      numPartition: Int,
      algorithm: String,
      checksum: File,
      data: File,
      index: File): Boolean = {
    var checksumIn: DataInputStream = null
    val expectChecksums = Array.ofDim[Long](numPartition)
    try {
      checksumIn = new DataInputStream(new FileInputStream(checksum))
      (0 until numPartition).foreach(i => expectChecksums(i) = checksumIn.readLong())
    } finally {
      if (checksumIn != null) {
        checksumIn.close()
      }
    }

    var dataIn: FileInputStream = null
    var indexIn: DataInputStream = null
    var checkedIn: CheckedInputStream = null
    try {
      dataIn = new FileInputStream(data)
      indexIn = new DataInputStream(new FileInputStream(index))
      var prevOffset = indexIn.readLong
      (0 until numPartition).foreach { i =>
        val curOffset = indexIn.readLong
        val limit = (curOffset - prevOffset).toInt
        val bytes = new Array[Byte](limit)
        val checksumCal = ShuffleChecksumHelper.getChecksumByAlgorithm(algorithm)
        checkedIn = new CheckedInputStream(
          new LimitedInputStream(dataIn, curOffset - prevOffset), checksumCal)
        checkedIn.read(bytes, 0, limit)
        prevOffset = curOffset
        // checksum must be consistent at both write and read sides
        if (checkedIn.getChecksum.getValue != expectChecksums(i)) return false
      }
    } finally {
      if (dataIn != null) {
        dataIn.close()
      }
      if (indexIn != null) {
        indexIn.close()
      }
      if (checkedIn != null) {
        checkedIn.close()
      }
    }
    true
  }

  /**
   * Compute the checksum of an in-memory byte range using the SAME algorithm family the rest of
   * Spark's shuffle uses (CRC32C by default), delegating to
   * [[ShuffleChecksumHelper.getChecksumByAlgorithm]]. This is the single, shared entry point for
   * in-memory shuffle-block integrity so callers reuse ONE checksum facility (this object) rather
   * than each instantiating their own [[java.util.zip.Checksum]]. No new checksum implementation
   * is introduced; the algorithm and byte semantics are identical to the on-disk checksum path
   * above, so a value produced here is directly comparable to one produced there.
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
