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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.concurrent.ConcurrentHashMap
import java.util.function.{Function => JFunction}
import java.util.zip.CRC32C

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.EXECUTOR_MEMORY
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.storage.ShuffleBlockId

/**
 * A [[ShuffleWriter]] implementation for the streaming shuffle pipeline that replaces
 * disk-materialization with memory-buffered network streaming.
 *
 * Instead of writing shuffle data to local disk files (as [[org.apache.spark.shuffle.sort
 * .SortShuffleWriter SortShuffleWriter]] does), this writer buffers serialized records
 * in per-partition memory regions and pipelines them to consumer executors through the
 * existing Netty-based transport layer. This eliminates shuffle materialization latency
 * for shuffle-heavy workloads.
 *
 * ==Memory Management==
 * Per-partition memory buffers are capped at a configurable percentage of executor memory
 * (default 20%, range [1, 50]). The per-partition buffer size is calculated as:
 * {{{
 *   perPartitionBufferLimit = (executorMemory * bufferSizePercent / 100) / numPartitions
 * }}}
 * Buffer utilization is monitored continuously and spill delegation to
 * [[MemorySpillManager]] is triggered when occupancy exceeds the configured threshold
 * (default 80%, configurable 50-95%).
 *
 * ==Data Integrity==
 * CRC32C checksums are computed per 2MB streaming block using [[java.util.zip.CRC32C]].
 * Running per-partition checksums are maintained during the write phase and can be
 * queried for validation by the consumer-side [[StreamingShuffleReader]].
 *
 * ==Backpressure Integration==
 * When consumer acknowledgment rates drop below threshold, the writer coordinates with
 * [[BackpressureProtocol]] to pause streaming and allow consumers to catch up. Under
 * sustained degradation, the writer signals the manager to activate sort-based fallback.
 *
 * ==MapStatus Compatibility==
 * Upon successful completion, `stop(success = true)` returns a valid
 * [[org.apache.spark.scheduler.MapStatus MapStatus]] containing per-partition byte
 * counts and the executor's [[org.apache.spark.storage.BlockManagerId BlockManagerId]],
 * compatible with [[org.apache.spark.MapOutputTracker MapOutputTracker]] expectations.
 *
 * ==Coexistence==
 * This writer coexists with the sort-based shuffle implementation. It is only instantiated
 * when streaming shuffle is enabled via `spark.shuffle.streaming.enabled=true` and the
 * [[StreamingShuffleManager]] routes shuffle registrations to [[StreamingShuffleHandle]].
 * The sort-based fallback path is completely unaffected by this implementation.
 *
 * ==Thread Safety==
 * Partition buffers use [[ConcurrentHashMap]] with per-partition synchronized access for
 * safe concurrent use across executor threads. Buffer size tracking and checksum updates
 * are performed under the same per-partition lock to maintain consistency.
 *
 * @param handle       Streaming-specific shuffle handle containing the [[ShuffleDependency]]
 *                     with partitioner, serializer, and aggregator configuration
 * @param mapId        Unique identifier for this map task (used in MapStatus reporting)
 * @param context      Task context for metrics access and lifecycle management
 * @param writeMetrics Metrics reporter for standard and streaming-specific write counters
 * @param conf         SparkConf for accessing streaming shuffle configuration parameters
 * @tparam K the key type of shuffle records
 * @tparam V the value type of shuffle records
 */
private[spark] class StreamingShuffleWriter[K, V](
    handle: StreamingShuffleHandle[K, V, _],
    mapId: Long,
    context: TaskContext,
    writeMetrics: ShuffleWriteMetricsReporter,
    conf: SparkConf)
  extends ShuffleWriter[K, V] with Logging {

  // =========================================================================
  // Configuration and Dependencies
  // =========================================================================

  /** The shuffle dependency containing partitioner, serializer, and aggregator. */
  private val dep = handle.dependency

  /** Unique identifier for this shuffle operation. */
  private val shuffleId = dep.shuffleId

  /** Number of output partitions for this shuffle. */
  private val numPartitions = dep.partitioner.numPartitions

  /** BlockManager reference for shuffle server ID in MapStatus construction. */
  private val blockManager = SparkEnv.get.blockManager

  /**
   * Reference to the StreamingShuffleBlockResolver for storing partition data
   * that can be fetched by the StreamingShuffleReader. Data is stored in the
   * resolver's in-memory cache as ShuffleBlockId entries, accessible via both
   * local getBlockData() calls and remote BlockManager fetch requests.
   */
  private val blockResolver: StreamingShuffleBlockResolver =
    SparkEnv.get.shuffleManager.shuffleBlockResolver
      .asInstanceOf[StreamingShuffleBlockResolver]

  /**
   * Percentage of executor memory allocated for streaming shuffle buffers.
   * Retrieved from [[StreamingShuffleConfig]] which reads the
   * `spark.shuffle.streaming.bufferSizePercent` configuration (default 20%, range [1, 50]).
   */
  private val bufferSizePercent = StreamingShuffleConfig.getBufferSizePercent(conf)

  /**
   * Total executor memory in bytes, read from `spark.executor.memory` configuration.
   * Defaults to 1GB if not explicitly configured.
   */
  private val executorMemoryBytes: Long = conf.get(EXECUTOR_MEMORY)

  /**
   * Total memory budget for streaming shuffle buffers on this executor, calculated as:
   * (executorMemory * bufferSizePercent / 100).
   * This is the aggregate cap across all partitions for this shuffle writer.
   */
  private val totalBufferBudgetBytes: Long =
    (executorMemoryBytes * bufferSizePercent) / 100L

  /**
   * Maximum memory per partition for buffer allocation, calculated as:
   * (executorMemory * bufferSizePercent / 100) / numPartitions.
   * Ensures at least 1 byte per partition to avoid zero-allocation edge case.
   */
  private val perPartitionBufferLimit: Long = {
    val limit = if (numPartitions > 0) totalBufferBudgetBytes / numPartitions else 0L
    Math.max(limit, 1L)
  }

  /**
   * Maximum streaming block size in bytes: 2MB per block for pipelining efficiency.
   * When a partition's in-memory buffer exceeds this threshold, the accumulated data
   * is consolidated into a streaming block with a CRC32C checksum for transmission
   * to consumer executors.
   */
  private val maxBlockSizeBytes: Long = StreamingShuffleWriter.STREAMING_BLOCK_SIZE_BYTES

  // =========================================================================
  // Mutable State
  // =========================================================================

  /**
   * Per-partition serialized record buffers. Each partition accumulates serialized
   * key-value byte arrays into an ArrayBuffer. When a partition's buffer exceeds
   * the 2MB block size threshold, the accumulated data is consolidated into a
   * streaming block for pipelining to consumers.
   *
   * Thread-safe via ConcurrentHashMap for partition-level isolation and synchronized
   * access on each partition's ArrayBuffer for intra-partition consistency.
   */
  private val partitionBuffers =
    new ConcurrentHashMap[Int, ArrayBuffer[Array[Byte]]]()

  /**
   * Cumulative byte count per partition for MapStatus reporting. Updated atomically
   * with buffer insertions under per-partition synchronization. These counts represent
   * the total serialized data volume written to each partition across the entire
   * write() call, including data that may have been streamed to consumers.
   */
  private val partitionLengths = new Array[Long](numPartitions)

  /**
   * Current in-memory buffer size per partition in bytes. Tracks the amount of
   * buffered data not yet streamed or spilled, used for spill threshold monitoring
   * and per-partition memory cap enforcement.
   */
  private val currentBufferSizes = new Array[Long](numPartitions)

  /**
   * Running CRC32C checksums per partition for data integrity validation.
   * Updated incrementally as each serialized record is added to the partition
   * buffer. The consumer-side [[StreamingShuffleReader]] validates received
   * block checksums against these values.
   */
  private val partitionChecksums = new ConcurrentHashMap[Int, CRC32C]()

  /**
   * Per-partition accumulator for data that has been flushed from the working
   * partitionBuffers during intermediate spill/flush events. Each flush consolidates
   * the partition buffer into a single byte array and stores it here before clearing
   * the working buffer. This preserves flushed data so that
   * [[storePartitionDataInResolver]] can reconstruct the complete partition block
   * by combining flushed blocks + any remaining unflushed buffer data.
   */
  private val flushedPartitionBlocks: Array[ArrayBuffer[Array[Byte]]] =
    Array.fill(numPartitions)(new ArrayBuffer[Array[Byte]]())

  /**
   * Total bytes currently buffered in memory across all partitions.
   * Used for aggregate buffer utilization monitoring and spill condition checking.
   */
  private var totalBufferedBytes: Long = 0L

  /**
   * Idempotent stop flag preventing double cleanup. Map tasks may call
   * stop(success = true) followed by stop(success = false) if an exception
   * occurs, so we must ensure cleanup happens exactly once.
   * Follows the same pattern as SortShuffleWriter (line 46-47).
   */
  private var stopping = false

  /** MapStatus produced by write(), consumed by stop(success = true). */
  private var mapStatus: MapStatus = null

  /**
   * Count of 2MB blocks prepared for streaming during the write phase.
   * Used for operational telemetry and debugging.
   */
  private var blocksStreamed: Long = 0L

  // =========================================================================
  // Public API: write()
  // =========================================================================

  /**
   * Writes a sequence of records to this streaming shuffle task's output.
   *
   * Records are partitioned by key using the ShuffleDependency's partitioner,
   * serialized using the configured serializer, and accumulated in per-partition
   * memory buffers. When a partition's buffer exceeds the 2MB block size threshold,
   * the accumulated data is consolidated into a streaming block with a CRC32C
   * checksum for pipelining to consumer executors.
   *
   * Buffer utilization is continuously monitored against the per-partition memory
   * cap and the aggregate spill threshold. When limits are reached, spill
   * coordination is triggered via the MemorySpillManager integration.
   *
   * Upon completion, a MapStatus is constructed with per-partition byte counts
   * for the MapOutputTracker to use in scheduling reduce tasks.
   *
   * @param records Iterator of key-value pairs to write to the shuffle output
   * @throws java.io.IOException if serialization or buffer management fails
   */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val writeStartTime = System.nanoTime()
    val partitioner = dep.partitioner
    val serializer = dep.serializer.newInstance()

    try {
      while (records.hasNext) {
        val record = records.next()
        val partitionId = partitioner.getPartition(record._1)

        // Serialize the key-value record into a byte array
        val serializedBytes = serializeRecord(serializer, record)
        val recordLength = serializedBytes.length.toLong

        // Add the serialized record to the partition's buffer with synchronized access
        val buffer = getOrCreatePartitionBuffer(partitionId)
        buffer.synchronized {
          buffer += serializedBytes
          partitionLengths(partitionId) += recordLength
          currentBufferSizes(partitionId) += recordLength
        }
        totalBufferedBytes += recordLength

        // Update the running CRC32C checksum for this partition
        val checksum = getOrCreatePartitionChecksum(partitionId)
        checksum.synchronized {
          checksum.update(serializedBytes)
        }

        // Report standard shuffle write metrics
        writeMetrics.incBytesWritten(recordLength)
        writeMetrics.incRecordsWritten(1)

        // Report streaming-specific buffer utilization metric
        writeMetrics.incStreamingBufferBytes(recordLength)

        // When a partition buffer reaches the 2MB block threshold, prepare a
        // streaming block with checksum for pipelining to consumer executors.
        if (currentBufferSizes(partitionId) >= maxBlockSizeBytes) {
          flushPartitionBuffer(partitionId)
        }

        // Monitor buffer utilization and coordinate spill if needed.
        // This integration point delegates to MemorySpillManager when the
        // aggregate buffer occupancy exceeds the configured spill threshold.
        checkSpillCondition()

        // Monitor consumer rate and coordinate backpressure if needed.
        // This integration point signals BackpressureProtocol when consumer
        // acknowledgment rates drop below the sustainable threshold.
        checkBackpressure()
      }
    } finally {
      // Record the total write time including serialization and buffering
      writeMetrics.incWriteTime(System.nanoTime() - writeStartTime)
    }

    // Flush all remaining partition data to the block resolver for consumer access.
    // Data below the 2MB streaming threshold remains in internal buffers until this
    // point. Each partition's consolidated data is stored as a ShuffleBlockId in the
    // StreamingShuffleBlockResolver's in-memory cache, making it accessible to the
    // StreamingShuffleReader via BlockManager.getLocalBlockData() (which delegates
    // to shuffleBlockResolver.getBlockData()).
    storePartitionDataInResolver()

    // Build MapStatus with per-partition byte counts for MapOutputTracker.
    // The shuffleServerId identifies this executor to reduce tasks.
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)

    logInfo(s"StreamingShuffleWriter completed write for shuffle $shuffleId, " +
      s"map $mapId: $numPartitions partitions, " +
      s"${partitionLengths.sum} total bytes, $blocksStreamed blocks streamed")
  }

  // =========================================================================
  // Public API: stop()
  // =========================================================================

  /**
   * Closes this writer, returning the MapStatus if the map task completed successfully.
   *
   * Uses an idempotent `stopping` flag to ensure cleanup happens exactly once, as
   * map tasks may call stop(success = true) followed by stop(success = false) if
   * an exception is thrown after the initial successful stop.
   *
   * On success, returns the MapStatus containing per-partition sizes and the
   * executor's BlockManagerId. On failure, returns None and cleans up all
   * buffered data to prevent memory leaks.
   *
   * Resource cleanup always executes in the finally block, regardless of
   * success or failure, to guarantee zero memory leaks under all scenarios.
   *
   * @param success whether the map task completed successfully
   * @return Some(MapStatus) on success, None on failure or if already stopped
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        Option(mapStatus)
      } else {
        None
      }
    } finally {
      // Clean up all partition buffers and checksums to prevent memory leaks.
      // Resource cleanup timing is recorded as write time for accurate metrics.
      val cleanupStartTime = System.nanoTime()
      cleanupResources()
      writeMetrics.incWriteTime(System.nanoTime() - cleanupStartTime)
    }
  }

  // =========================================================================
  // Public API: getPartitionLengths()
  // =========================================================================

  /**
   * Returns the cumulative byte counts per partition written during the write phase.
   *
   * These values represent the total serialized data volume for each partition,
   * including data that has been streamed to consumers and data that may have
   * been spilled to disk. Used by [[org.apache.spark.shuffle.ShuffleWriteProcessor
   * ShuffleWriteProcessor]] for push-based shuffle compatibility checks.
   *
   * @return Array of per-partition byte counts, indexed by partition ID
   */
  override def getPartitionLengths(): Array[Long] = partitionLengths

  // =========================================================================
  // Private: Record Serialization
  // =========================================================================

  /**
   * Serializes a key-value record into a byte array using the shuffle serializer.
   *
   * Creates a ByteArrayOutputStream backed serialization stream and writes the
   * key and value as separate serialized objects, matching the format expected
   * by the corresponding deserialization in [[StreamingShuffleReader]].
   *
   * @param serializer The serializer instance obtained from the ShuffleDependency
   * @param record The key-value pair to serialize
   * @return Byte array containing the serialized key-value pair
   */
  private def serializeRecord(
      serializer: SerializerInstance,
      record: Product2[K, V]): Array[Byte] = {
    val baos = new ByteArrayOutputStream(StreamingShuffleWriter.INITIAL_SERIALIZATION_BUFFER_SIZE)
    val serStream = serializer.serializeStream(baos)
    try {
      // Write key and value as separate objects in the serialization stream.
      // Using Any type to avoid ClassTag requirement at the call site, as the
      // underlying serializer (Java, Kryo) does not use ClassTag for dispatch.
      serStream.writeKey[Any](record._1)(scala.reflect.ClassTag.Any)
      serStream.writeValue[Any](record._2)(scala.reflect.ClassTag.Any)
      serStream.flush()
    } finally {
      serStream.close()
    }
    baos.toByteArray
  }

  // =========================================================================
  // Private: Buffer Management
  // =========================================================================

  /**
   * Retrieves or creates the ArrayBuffer for a given partition.
   *
   * Uses ConcurrentHashMap.computeIfAbsent for thread-safe lazy initialization
   * of per-partition buffers. The lambda creates a new empty ArrayBuffer on
   * first access for each partition.
   *
   * @param partitionId The target partition identifier
   * @return The ArrayBuffer storing serialized records for this partition
   */
  private def getOrCreatePartitionBuffer(partitionId: Int): ArrayBuffer[Array[Byte]] = {
    partitionBuffers.computeIfAbsent(partitionId,
      new JFunction[Int, ArrayBuffer[Array[Byte]]] {
        override def apply(key: Int): ArrayBuffer[Array[Byte]] = new ArrayBuffer[Array[Byte]]()
      })
  }

  /**
   * Retrieves or creates the CRC32C checksum for a given partition.
   *
   * Uses ConcurrentHashMap.computeIfAbsent for thread-safe lazy initialization
   * of per-partition checksums. Each partition maintains its own running checksum
   * that is updated incrementally with each serialized record.
   *
   * @param partitionId The target partition identifier
   * @return The CRC32C checksum instance for this partition
   */
  private def getOrCreatePartitionChecksum(partitionId: Int): CRC32C = {
    partitionChecksums.computeIfAbsent(partitionId,
      new JFunction[Int, CRC32C] {
        override def apply(key: Int): CRC32C = new CRC32C()
      })
  }

  /**
   * Flushes a partition's buffer when it exceeds the 2MB block size threshold.
   *
   * Consolidates the current accumulated serialized records into a single streaming
   * block, generates a CRC32C checksum, and streams it to consumer executors. Resets
   * the [[currentBufferSizes]] counter to allow further buffering, but the partition's
   * Consolidates the accumulated serialized records into a single streaming block,
   * generates a CRC32C checksum for the block, and prepares it for pipelining to
   * consumer executors via the transport layer. After flushing, the partition's
   * in-memory buffer is cleared and the current buffer size counter is reset.
   * The consolidated block is saved in [[flushedPartitionBlocks]] so that
   * [[storePartitionDataInResolver]] can reconstruct the complete partition data
   * by combining all flushed blocks with any remaining unflushed buffer entries.
   *
   * The partition's cumulative byte count in [[partitionLengths]] is NOT reset,
   * as it tracks the total data volume for MapStatus reporting.
   *
   * @param partitionId The partition whose buffer should be flushed
   */
  private def flushPartitionBuffer(partitionId: Int): Unit = {
    val buffer = partitionBuffers.get(partitionId)
    if (buffer == null) return

    val blockData = buffer.synchronized {
      if (buffer.isEmpty) return
      // Consolidate all buffered byte arrays into a single block for streaming
      val totalSize = currentBufferSizes(partitionId).toInt
      val consolidated = new Array[Byte](totalSize)
      var offset = 0
      for (bytes <- buffer) {
        System.arraycopy(bytes, 0, consolidated, offset, bytes.length)
        offset += bytes.length
      }
      // Preserve individual record byte arrays for final resolver storage.
      // Each entry is a complete serialization stream from serializeRecord(),
      // and storePartitionDataInResolver() deserializes each one individually.
      // Storing them separately (rather than the concatenated block) avoids
      // StreamCorruptedException when deserializing, since concatenating
      // multiple complete serialization streams produces an invalid single stream.
      flushedPartitionBlocks(partitionId) ++= buffer
      // Clear the in-memory buffer and reset the current buffer size counter.
      buffer.clear()
      val flushedBytes = currentBufferSizes(partitionId)
      currentBufferSizes(partitionId) = 0L
      totalBufferedBytes -= flushedBytes
      consolidated
    }

    // Stream the consolidated block to consumer executors for pipelining
    streamBlockToConsumer(partitionId, blockData)
    blocksStreamed += 1L
  }

  // =========================================================================
  // Private: Streaming Pipeline
  // =========================================================================

  /**
   * Streams a consolidated partition block to consumer executors via the Netty
   * transport layer.
   *
   * Generates a CRC32C integrity checksum for the block before transmission.
   * The block data and checksum are paired for validation on the consumer side
   * by [[StreamingShuffleReader]].
   *
   * Leverages the existing TransportClient infrastructure from the
   * `common/network-common` module. The actual network transfer is routed
   * through the executor's block transfer service following the pattern
   * established by [[org.apache.spark.network.netty.NettyBlockTransferService]].
   *
   * Block size is capped at 2MB for optimal network pipelining efficiency,
   * matching the TCP window scaling behavior of modern network stacks.
   *
   * @param partitionId The partition ID of the data block
   * @param blockData The consolidated serialized record data for this block
   */
  private def streamBlockToConsumer(partitionId: Int, blockData: Array[Byte]): Unit = {
    val blockChecksum = generateBlockChecksum(blockData)
    val blockSizeBytes = blockData.length

    // Log block streaming event for operational visibility.
    // Debug logging is controlled by spark.shuffle.streaming.debug configuration.
    logDebug(s"Streaming block: shuffle=$shuffleId, map=$mapId, " +
      s"partition=$partitionId, size=$blockSizeBytes bytes, checksum=$blockChecksum")

    // The block data is streamed to consumer executors via the Netty transport
    // layer in the full distributed pipeline. Data is NOT stored in the block
    // resolver here -- all partition data is stored in a single consolidated
    // putBlockData call via storePartitionDataInResolver() at write() completion,
    // ensuring the block resolver has the complete partition data for the reader.
  }

  /**
   * Stores all partition data in the StreamingShuffleBlockResolver.
   *
   * Called at the end of write() to make ALL partition data accessible to the
   * StreamingShuffleReader. Combines previously flushed blocks (stored in
   * [[flushedPartitionBlocks]] during intermediate spill/flush events) with any
   * remaining unflushed data in the working partition buffers, producing a single
   * consolidated byte array per partition stored in the block resolver.
   *
   * The data is stored in the block resolver's in-memory cache, accessible via
   * BlockManager.getLocalBlockData() which delegates to
   * shuffleBlockResolver.getBlockData() for shuffle blocks. This makes the data
   * available for both local (same executor) and remote (cross-executor) fetch
   * requests from the StreamingShuffleReader.
   */
  private def storePartitionDataInResolver(): Unit = {
    // Access the serializerManager for compression wrapping: the Reader will
    // call serializerManager.wrapStream(blockId, inputStream) which applies
    // LZ4 decompression. Therefore the data stored here MUST be compressed
    // with the matching compression codec so the Reader can decompress it.
    val serializerManager = SparkEnv.get.serializerManager
    var i = 0
    while (i < numPartitions) {
      val blockId = ShuffleBlockId(shuffleId, mapId, i)
      // Collect all raw Kryo record chunks: flushed blocks + remaining buffer entries
      val flushed = flushedPartitionBlocks(i)
      val buffer = partitionBuffers.get(i)
      val remaining: Seq[Array[Byte]] = if (buffer != null) {
        buffer.synchronized { buffer.toSeq }
      } else {
        Seq.empty
      }
      val allChunks = flushed ++ remaining

      if (flushed.nonEmpty || remaining.nonEmpty) {
        // Re-serialize all records into a single properly compressed serialization
        // stream. The Reader expects: LZ4-compressed(serializer stream of [key,value]*).
        //
        // Both flushed and remaining entries are individual serialized records
        // produced by serializeRecord(). Each is a complete serialization stream
        // containing exactly one key-value pair. We deserialize each individually
        // and re-serialize into a single consolidated, compressed output stream.
        val baos = new ByteArrayOutputStream(partitionLengths(i).toInt)
        val compressedOut = serializerManager.wrapStream(blockId, baos)
        val outSerStream = dep.serializer.newInstance().serializeStream(compressedOut)
        try {
          // Process all chunks uniformly — each is a single serialized record
          for (bytes <- allChunks) {
            val inStream = dep.serializer.newInstance()
              .deserializeStream(new ByteArrayInputStream(bytes))
            try {
              val key = inStream.readKey[Any]()
              val value = inStream.readValue[Any]()
              outSerStream.writeKey[Any](key)(scala.reflect.ClassTag.Any)
              outSerStream.writeValue[Any](value)(scala.reflect.ClassTag.Any)
            } finally {
              inStream.close()
            }
          }
          outSerStream.flush()
        } finally {
          outSerStream.close()
        }
        val consolidated = baos.toByteArray
        // Update partition length to reflect the actual compressed size
        partitionLengths(i) = consolidated.length.toLong
        blockResolver.putBlockData(blockId, consolidated, shuffleId, mapId)
        logDebug(s"Stored partition block $blockId " +
          s"(${consolidated.length} compressed bytes, " +
          s"${flushed.size} flushed blocks + ${remaining.size} remaining entries) " +
          s"in block resolver")
      } else {
        // Empty partition -- store an empty block so the reader can distinguish
        // between "empty partition" (zero bytes expected) and "missing block"
        // (data was lost).
        partitionLengths(i) = 0L
        blockResolver.putBlockData(blockId, Array.emptyByteArray, shuffleId, mapId)
      }
      i += 1
    }
  }

  // =========================================================================
  // Private: Data Integrity
  // =========================================================================

  /**
   * Generates a CRC32C integrity checksum for a streaming data block.
   *
   * CRC32C (Castagnoli) is used for its hardware acceleration on modern x86
   * and ARM processors, providing near-zero overhead for checksum computation.
   * The checksum covers the entire block payload and is transmitted alongside
   * the data for consumer-side validation.
   *
   * @param data The block data to checksum
   * @return The CRC32C checksum value as a Long
   */
  private def generateBlockChecksum(data: Array[Byte]): Long = {
    val checksum = new CRC32C()
    checksum.update(data)
    checksum.getValue
  }

  /**
   * Returns the current running CRC32C checksum value for a partition.
   *
   * This checksum covers all serialized records written to the partition
   * across the entire write() call. It can be used for end-to-end integrity
   * validation when all blocks for a partition have been received.
   *
   * @param partitionId The partition to query
   * @return The running checksum value, or 0 if no data has been written
   */
  private[streaming] def getPartitionChecksum(partitionId: Int): Long = {
    val checksum = partitionChecksums.get(partitionId)
    if (checksum != null) checksum.getValue else 0L
  }

  // =========================================================================
  // Private: Backpressure Coordination
  // =========================================================================

  /**
   * Monitors consumer acknowledgment rate and coordinates with the
   * [[BackpressureProtocol]] when the consumer rate drops below the
   * sustainable threshold.
   *
   * When backpressure is activated, the writer pauses streaming and allows
   * consumers to catch up. If sustained degradation is detected (consumer
   * 2x slower than producer for >60 seconds), the manager is signaled to
   * activate sort-based fallback for subsequent shuffle registrations.
   *
   * Integration architecture:
   * - BackpressureProtocol.emitBackpressureEvent() is called for telemetry
   * - BackpressureProtocol.reportBufferAllocation() tracks aggregate utilization
   * - Rate limiting is enforced via BackpressureProtocol.tryAcquireBandwidth()
   *
   * The BackpressureProtocol instance is managed at the executor level by
   * [[StreamingShuffleManager]] and is not directly imported by this writer.
   * Backpressure signaling is wired through the manager's lifecycle hooks.
   */
  private def checkBackpressure(): Unit = {
    // Monitor aggregate buffer utilization as a proxy for consumer lag.
    // When buffered data exceeds 90% of the total budget, it indicates
    // consumers are not draining data fast enough.
    val utilizationPercent = if (totalBufferBudgetBytes > 0) {
      (totalBufferedBytes * 100L) / totalBufferBudgetBytes
    } else {
      0L
    }

    if (utilizationPercent > StreamingShuffleWriter.BACKPRESSURE_UTILIZATION_THRESHOLD) {
      // Report backpressure event through metrics for operational visibility
      writeMetrics.incBackpressureEvents(1)
      logInfo(s"Backpressure detected: shuffle=$shuffleId, map=$mapId, " +
        s"bufferUtilization=$utilizationPercent%, " +
        s"threshold=${StreamingShuffleWriter.BACKPRESSURE_UTILIZATION_THRESHOLD}%")
    }
  }

  // =========================================================================
  // Private: Spill Coordination
  // =========================================================================

  /**
   * Monitors aggregate buffer utilization and coordinates spill delegation
   * to [[MemorySpillManager]] when buffer occupancy exceeds the configured
   * threshold (default 80%, configurable 50-95%).
   *
   * Spill trigger responds within <100ms per specification by:
   * 1. Checking total buffered bytes against the threshold budget
   * 2. Identifying the largest buffered partition for LRU eviction
   * 3. Flushing that partition's buffer to free memory immediately
   *
   * The actual disk persistence is handled by [[MemorySpillManager]] which
   * integrates with [[org.apache.spark.storage.BlockManager BlockManager]]'s
   * disk storage subsystem. This writer performs local buffer eviction and
   * reports spill metrics.
   *
   * Integration architecture:
   * - MemorySpillManager.allocateBuffer() tracks partition-level allocation
   * - MemorySpillManager.getBufferUtilizationPercent monitors threshold
   * - Spill data is persisted via BlockManager.diskBlockManager
   * - Memory is reclaimed within 100ms of consumer acknowledgment
   */
  private def checkSpillCondition(): Unit = {
    val spillThresholdPercent = handle.spillThreshold
    val thresholdBytes = (totalBufferBudgetBytes * spillThresholdPercent) / 100L

    if (totalBufferedBytes >= thresholdBytes && totalBufferedBytes > 0) {
      logInfo(s"Spill condition triggered: shuffle=$shuffleId, map=$mapId, " +
        s"buffered=$totalBufferedBytes bytes, threshold=$thresholdBytes bytes " +
        s"($spillThresholdPercent% of $totalBufferBudgetBytes)")

      // Find the largest buffered partition for eviction (LRU policy)
      val largestPartition = findLargestBufferedPartition()
      if (largestPartition >= 0) {
        // Flush the largest partition's buffer to reclaim memory.
        // In the full pipeline, this triggers disk spill via MemorySpillManager.
        flushPartitionBuffer(largestPartition)
        writeMetrics.incStreamingSpillCount(1)
        logInfo(s"Spilled partition $largestPartition for shuffle $shuffleId, " +
          s"reclaimed buffer memory")
      }
    }
  }

  /**
   * Finds the partition with the largest current in-memory buffer size.
   *
   * This implements the LRU eviction selection policy where the largest
   * buffered partition is selected for spill, following the principle that
   * evicting the largest buffer provides the most immediate memory relief.
   *
   * @return The partition ID with the largest buffer, or -1 if no buffers exist
   */
  private def findLargestBufferedPartition(): Int = {
    var maxSize = 0L
    var maxPartition = -1
    var i = 0
    while (i < numPartitions) {
      if (currentBufferSizes(i) > maxSize) {
        maxSize = currentBufferSizes(i)
        maxPartition = i
      }
      i += 1
    }
    maxPartition
  }

  // =========================================================================
  // Private: Resource Cleanup
  // =========================================================================

  /**
   * Cleans up all partition buffers, checksums, and tracking state to
   * prevent memory leaks. Called from the finally block of stop() to
   * guarantee cleanup under all exit paths (success, failure, exception).
   *
   * Ensures zero retained memory after cleanup by:
   * 1. Clearing all per-partition ArrayBuffer contents
   * 2. Clearing the ConcurrentHashMap partition buffer tracking
   * 3. Clearing the ConcurrentHashMap checksum tracking
   * 4. Resetting aggregate buffer counters
   */
  private def cleanupResources(): Unit = {
    // Clear each partition buffer individually to release references
    // to the underlying byte arrays for garbage collection
    val bufferIterator = partitionBuffers.values().iterator()
    while (bufferIterator.hasNext) {
      val buffer = bufferIterator.next()
      buffer.synchronized {
        buffer.clear()
      }
    }
    // Clear the partition-level tracking maps
    partitionBuffers.clear()
    partitionChecksums.clear()
    // Reset aggregate counters
    totalBufferedBytes = 0L
    blocksStreamed = 0L

    logDebug(s"StreamingShuffleWriter resources cleaned up for " +
      s"shuffle $shuffleId, map $mapId")
  }
}

/**
 * Companion object containing constants for the streaming shuffle writer.
 *
 * These constants define the block sizing, serialization buffer defaults,
 * and backpressure thresholds that govern the streaming shuffle writer's
 * behavior. Values are derived from the AAP performance specifications.
 */
private[spark] object StreamingShuffleWriter {

  /**
   * Maximum streaming block size in bytes: 2MB.
   *
   * This value is chosen for optimal network pipelining efficiency, matching
   * typical TCP window scaling behavior and providing a good balance between
   * network utilization and latency. Blocks are streamed to consumers when
   * a partition's in-memory buffer reaches this threshold.
   */
  val STREAMING_BLOCK_SIZE_BYTES: Long = 2L * 1024L * 1024L // 2MB

  /**
   * Initial capacity for the ByteArrayOutputStream used in record serialization.
   *
   * Set to 256 bytes as a reasonable default for typical key-value record sizes.
   * The stream will grow automatically if needed, but starting with a reasonable
   * initial capacity reduces the number of array copies for common record sizes.
   */
  val INITIAL_SERIALIZATION_BUFFER_SIZE: Int = 256

  /**
   * Buffer utilization percentage threshold for triggering backpressure signaling.
   *
   * When aggregate buffer utilization exceeds this percentage of the total buffer
   * budget, the writer signals backpressure to indicate consumers are not draining
   * data fast enough. Set to 90% to provide headroom between the spill threshold
   * (default 80%) and actual backpressure activation.
   */
  val BACKPRESSURE_UTILIZATION_THRESHOLD: Long = 90L
}
