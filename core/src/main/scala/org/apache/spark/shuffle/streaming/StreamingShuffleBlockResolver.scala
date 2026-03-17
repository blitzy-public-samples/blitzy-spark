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

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage.{BlockId, ShuffleMergedBlockId}

/**
 * Block resolver for streaming shuffle data.
 *
 * Resolves streaming shuffle blocks from in-memory buffers or spilled disk files.
 * Unlike [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] which uses consolidated
 * data/index files on disk, this resolver primarily serves data from the streaming
 * pipeline's in-memory buffer cache, with disk-spilled data as a fallback for
 * memory-pressure scenarios.
 *
 * Merged block support is intentionally not implemented — streaming shuffle does not
 * participate in push-based shuffle merging. Both [[getMergedBlockData]] and
 * [[getMergedBlockMeta]] return empty/minimal results.
 *
 * Thread Safety: All mutable state is stored in [[ConcurrentHashMap]] instances,
 * ensuring safe concurrent access from multiple executor threads without external
 * synchronization.
 *
 * Coexistence: This resolver is used exclusively by
 * [[org.apache.spark.shuffle.streaming.StreamingShuffleManager]].
 * [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] continues to serve
 * sort-based shuffle independently with zero cross-contamination.
 *
 * @param conf SparkConf for reading streaming shuffle configuration parameters
 */
private[spark] class StreamingShuffleBlockResolver(conf: SparkConf)
  extends ShuffleBlockResolver with Logging {

  /**
   * In-memory block data cache mapping block identifiers to their raw byte arrays.
   * Entries are populated by [[putBlockData]] during streaming writes and removed
   * either explicitly via [[removeBlockData]] after consumer acknowledgment or
   * in bulk via [[removeDataByMap]] during shuffle unregistration.
   */
  private val blockCache = new ConcurrentHashMap[BlockId, Array[Byte]]()

  /**
   * Mapping from (shuffleId, mapId) composite keys to the set of block identifiers
   * produced by that shuffle map task. This enables efficient bulk cleanup when a
   * shuffle map output is invalidated or the shuffle is unregistered. The inner
   * sets are concurrent-safe via [[ConcurrentHashMap.newKeySet]].
   */
  private val shuffleMapBlocks =
    new ConcurrentHashMap[(Int, Long), java.util.Set[BlockId]]()

  // ---------------------------------------------------------------------------
  // ShuffleBlockResolver trait implementation
  // ---------------------------------------------------------------------------

  /**
   * Retrieves block data for a streaming shuffle block.
   *
   * Resolution order:
   * 1. Check the in-memory block cache for a direct hit
   * 2. If not found, the block may have been spilled to disk — callers should
   *    coordinate with [[MemorySpillManager]] for disk-spilled block retrieval
   *    via BlockManager before invoking this method
   *
   * @param blockId The block identifier to resolve
   * @param dirs Optional directory overrides (used by external shuffle service);
   *             currently unused by the streaming resolver since blocks are
   *             served from memory or coordinated through BlockManager
   * @return ManagedBuffer wrapping the block's byte data
   * @throws RuntimeException if the block is not available in the in-memory cache
   */
  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]] = None): ManagedBuffer = {
    val cached = blockCache.get(blockId)
    if (cached != null) {
      // Wrap the cached byte array in a NIO ByteBuffer, then in a NioManagedBuffer
      // for zero-copy delivery to the network transport layer
      new NioManagedBuffer(java.nio.ByteBuffer.wrap(cached))
    } else {
      // Block not in memory — this occurs when the block was already consumed and
      // reclaimed, or was spilled to disk and must be fetched via BlockManager.
      // Throwing here follows the ShuffleBlockResolver contract: "If the data for
      // that block is not available, throws an unspecified exception."
      throw new RuntimeException(
        s"Streaming shuffle block $blockId not found in memory cache. " +
        "The block may have been reclaimed after consumer acknowledgment or " +
        "spilled to disk. Disk-spilled blocks should be retrieved through BlockManager.")
    }
  }

  /**
   * Returns all block identifiers associated with a given shuffle map task.
   *
   * This method is used during shuffle cleanup and by the external shuffle service
   * to enumerate blocks that should be removed when a mapper's output is invalidated.
   *
   * @param shuffleId The shuffle identifier
   * @param mapId The map task identifier
   * @return Sequence of BlockIds for this (shuffleId, mapId) pair, or empty if none exist
   */
  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    val blocks = shuffleMapBlocks.get((shuffleId, mapId))
    if (blocks != null) {
      blocks.asScala.toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * Streaming shuffle does not support merged (push-based) shuffle blocks.
   *
   * Returns an empty sequence since the streaming pipeline streams data directly
   * from producers to consumers without the intermediate merge step used by
   * push-based shuffle. This is intentional per the AAP specification — streaming
   * shuffle operates as a completely separate data path.
   *
   * @param blockId The merged block identifier (unused)
   * @param dirs Optional directory overrides (unused)
   * @return Empty sequence — no merged block data available
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    Seq.empty
  }

  /**
   * Streaming shuffle does not support merged block metadata.
   *
   * Returns a minimal [[MergedBlockMeta]] with zero chunks and an empty bitmap
   * buffer. This satisfies the trait contract while indicating that no merged
   * data exists for streaming shuffle blocks.
   *
   * @param blockId The merged block identifier (unused)
   * @param dirs Optional directory overrides (unused)
   * @return MergedBlockMeta with 0 chunks and an empty NIO buffer
   */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    // Allocate a zero-capacity ByteBuffer for the empty bitmap, wrapped in
    // NioManagedBuffer to satisfy the MergedBlockMeta constructor requirement
    new MergedBlockMeta(0, new NioManagedBuffer(java.nio.ByteBuffer.allocate(0)))
  }

  /**
   * Cleans up all cached block data and tracking state.
   *
   * Called during orderly shutdown of the streaming shuffle subsystem.
   * After this method returns, all in-memory block data is released and
   * the shuffle-to-blocks mapping is cleared. This method is idempotent —
   * calling it multiple times has no adverse effects.
   */
  override def stop(): Unit = {
    val blockCount = blockCache.size()
    val shuffleMapCount = shuffleMapBlocks.size()
    blockCache.clear()
    shuffleMapBlocks.clear()
    logInfo(s"StreamingShuffleBlockResolver stopped — " +
      s"cleared $blockCount cached blocks and $shuffleMapCount shuffle map entries")
  }

  // ---------------------------------------------------------------------------
  // Block management helper methods for streaming pipeline integration
  // ---------------------------------------------------------------------------

  /**
   * Caches block data in memory for the streaming pipeline.
   *
   * Called by [[StreamingShuffleWriter]] when buffered partition data is ready
   * for consumption by downstream reduce tasks. The data is stored as a raw
   * byte array in the block cache and indexed under the (shuffleId, mapId)
   * composite key for efficient bulk cleanup.
   *
   * Thread safety: Both the block cache insertion and the shuffle-map-blocks
   * set update are atomic with respect to concurrent readers and writers.
   *
   * @param blockId Unique identifier for this block
   * @param data Raw byte array containing the serialized block data
   * @param shuffleId The shuffle this block belongs to
   * @param mapId The map task that produced this block
   */
  def putBlockData(
      blockId: BlockId,
      data: Array[Byte],
      shuffleId: Int,
      mapId: Long): Unit = {
    blockCache.put(blockId, data)
    // Atomically obtain or create the block ID set for this (shuffleId, mapId) pair,
    // then add the new blockId to it
    val blocks = shuffleMapBlocks.computeIfAbsent(
      (shuffleId, mapId),
      _ => ConcurrentHashMap.newKeySet[BlockId]())
    blocks.add(blockId)
    logDebug(s"Cached streaming shuffle block $blockId " +
      s"(${data.length} bytes) for shuffle $shuffleId, map $mapId")
  }

  /**
   * Removes a single block's data from the in-memory cache.
   *
   * Called after a consumer sends an acknowledgment that it has successfully
   * received and validated the block, allowing the producer to reclaim the
   * memory. This is the primary mechanism for buffer reclamation in the
   * streaming shuffle pipeline.
   *
   * Note: This method only removes from the block cache. The block ID
   * remains in the shuffleMapBlocks tracking set until [[removeDataByMap]]
   * is called, which is acceptable since the tracking set entries are
   * lightweight references.
   *
   * @param blockId The block identifier to remove
   */
  def removeBlockData(blockId: BlockId): Unit = {
    val removed = blockCache.remove(blockId)
    if (removed != null) {
      logDebug(s"Removed streaming shuffle block $blockId from cache " +
        s"(${removed.length} bytes reclaimed)")
    }
  }

  /**
   * Removes all block data associated with a specific shuffle map task.
   *
   * Called during [[StreamingShuffleManager.unregisterShuffle]] to clean up
   * all blocks produced by a given map task. This performs a two-phase cleanup:
   * 1. Removes the (shuffleId, mapId) entry from the tracking map
   * 2. Removes each associated block from the block cache
   *
   * This method is safe to call even if some blocks have already been
   * individually removed via [[removeBlockData]] — the cache removal
   * operations are idempotent.
   *
   * @param shuffleId The shuffle whose map output should be removed
   * @param mapId The specific map task whose blocks should be removed
   */
  def removeDataByMap(shuffleId: Int, mapId: Long): Unit = {
    val blocks = shuffleMapBlocks.remove((shuffleId, mapId))
    if (blocks != null) {
      var removedCount = 0
      blocks.forEach { blockId =>
        if (blockCache.remove(blockId) != null) {
          removedCount += 1
        }
      }
      logDebug(s"Removed $removedCount blocks for shuffle $shuffleId, " +
        s"map $mapId (${blocks.size()} tracked)")
    }
  }
}
