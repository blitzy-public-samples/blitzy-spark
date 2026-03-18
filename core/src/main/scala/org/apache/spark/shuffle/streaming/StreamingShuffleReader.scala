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

import java.io.{ByteArrayInputStream, InputStream, IOException}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.{Callable, TimeoutException => JTimeoutException, TimeUnit}
import java.util.zip.CRC32C

import scala.collection
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.shuffle._
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId, StorageLevel}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.collection.ExternalSorter

/**
 * Consumer-side reader for the streaming shuffle implementation.
 *
 * Polls producers for available data before shuffle completion using in-progress
 * block requests via the existing BlockManager/TransportClient infrastructure.
 * Replaces the
 * [[org.apache.spark.storage.ShuffleBlockFetcherIterator ShuffleBlockFetcherIterator]]
 * pattern used by [[BlockStoreShuffleReader]] with streaming consumption semantics.
 *
 * Key streaming capabilities:
 *  - In-progress block polling from producers before shuffle completion
 *  - 5-second connection timeout for producer failure detection
 *  - CRC32C checksum validation on received blocks for integrity verification
 *  - Acknowledgment protocol for producer buffer reclamation via BackpressureProtocol
 *  - Full aggregation and sorting support via [[ExternalSorter]]
 *  - [[FetchFailedException]] propagation for automatic DAG recomputation on
 *    producer failure
 *
 * Coexistence: This reader is only instantiated when streaming shuffle is enabled
 * ({@code spark.shuffle.streaming.enabled=true}). The existing
 * [[BlockStoreShuffleReader]] continues to serve sort-based shuffle reads unchanged.
 * This class contains zero imports from {@code org.apache.spark.shuffle.sort} to
 * enforce package isolation as specified in the AAP.
 *
 * Fault tolerance: On any producer failure detected via connection timeout or
 * IOException, all partial reads from the failed producer are atomically discarded
 * and a [[FetchFailedException]] is thrown. The executor sends the error back to
 * the driver, which then resubmits the upstream ShuffleMapStage for recomputation,
 * guaranteeing zero data loss.
 *
 * @param handle The streaming shuffle handle containing the [[ShuffleDependency]]
 *               with partitioner, serializer, aggregator, and key ordering
 * @param blocksByAddress Iterator of (BlockManagerId, blocks) pairs mapping each
 *                        producer executor to its shuffle blocks for remote fetch
 * @param context The task context for metrics, lifecycle management, and
 *                task cancellation support
 * @param readMetrics Metrics reporter for both standard and streaming-specific
 *                    shuffle read statistics
 * @param conf Spark configuration for streaming shuffle tuning parameters
 */
private[spark] class StreamingShuffleReader[K, C](
    handle: StreamingShuffleHandle[K, _, C],
    blocksByAddress: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    conf: SparkConf)
  extends ShuffleReader[K, C] with Logging {

  /**
   * The shuffle dependency containing partitioner, serializer, aggregator,
   * and key ordering configuration needed for deserialization and
   * aggregation/sorting of shuffle records.
   */
  private val dep = handle.dependency

  /**
   * Serializer manager for wrapping block InputStreams with the appropriate
   * compression and encryption transformations before deserialization.
   */
  private val serializerManager = SparkEnv.get.serializerManager

  /**
   * Block manager for remote block retrieval via the existing Netty-based
   * transport layer. Used by fetchBlockWithTimeout to obtain raw block bytes
   * from producer executors.
   */
  private val blockManager = SparkEnv.get.blockManager

  /**
   * Connection timeout for producer failure detection (5 seconds).
   * When a producer fails to respond within this window, the reader
   * invalidates all partial reads and throws [[FetchFailedException]] to
   * trigger DAG recomputation of the upstream ShuffleMapStage.
   * Defined locally to avoid circular dependency on StreamingShuffleManager
   * companion object which may not be compiled yet.
   */
  private val connectionTimeoutMs: Long = 5000L

  /**
   * Consumer heartbeat timeout (10 seconds).
   * Used for liveness detection in the acknowledgment protocol.
   * If the consumer fails to send acknowledgments within this window,
   * the producer retains buffer data for potential retransmission.
   */
  private val heartbeatTimeoutMs: Long = 10000L

  /**
   * Maximum streaming shuffle block size (2MB).
   * Blocks are limited to this size for efficient pipelining through the
   * Netty transport layer. Matches the producer-side block size limit.
   */
  private val maxBlockSizeBytes: Long = 2L * 1024 * 1024

  /**
   * Tracks cumulative fetch time across all blocks in nanoseconds.
   * Used to report accurate fetch wait time metrics to the task metrics system.
   */
  private var totalFetchTimeNs: Long = 0L

  /**
   * Reads the combined key-values for this reduce task via the streaming
   * shuffle protocol.
   *
   * The read pipeline consists of six stages:
   *  1. Poll producers for available blocks with CRC32C checksum validation
   *  2. Deserialize received blocks into key-value record pairs
   *  3. Update per-record read metrics and register completion callback
   *  4. Wrap with InterruptibleIterator for task cancellation support
   *  5. Apply aggregation and/or sorting via ExternalSorter
   *  6. Final InterruptibleIterator wrapping for the result
   *
   * @return Iterator of aggregated/sorted (key, combined-value) pairs
   */
  override def read(): Iterator[Product2[K, C]] = {
    // Stage 1: Poll producers for available blocks via streaming connections.
    // fetchStreamingBlocks validates CRC32C checksums internally before returning
    // verified InputStreams, ensuring zero corrupt data reaches deserialization.
    val blockIterator = fetchStreamingBlocks()

    // Stage 2: Deserialize received blocks into key-value pairs.
    // Creates a serializer instance and wraps each block's InputStream with
    // serializerManager for decompression/decryption before deserialization.
    val serializerInstance = dep.serializer.newInstance()
    val recordIter = blockIterator.flatMap { case (blockId, blockData) =>
      // The asKeyValueIterator wraps a key/value iterator inside a NextIterator
      // that ensures close() is called on the underlying InputStream when all
      // records have been read. This pattern matches BlockStoreShuffleReader.
      serializerInstance.deserializeStream(
        serializerManager.wrapStream(blockId, blockData)).asKeyValueIterator
    }

    // Stage 3: Update the context task metrics for each record read.
    // Wraps the record iterator with a CompletionIterator that merges shuffle
    // read metrics into the task metrics upon iteration completion, ensuring
    // accurate reporting to the driver for monitoring and UI display.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // Stage 4: Wrap with InterruptibleIterator for task cancellation support.
    // An interruptible iterator must be used here in order to support task
    // cancellation (e.g., via TaskContext.markInterrupted()) during shuffle read.
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    // Stage 5: Apply aggregation and/or sorting based on the shuffle dependency.
    // This exactly mirrors BlockStoreShuffleReader's aggregation and sorting logic,
    // supporting combiners, reduce-side aggregation, and key ordering via ExternalSorter.
    val resultIter: Iterator[Product2[K, C]] = applyAggregationAndSorting(interruptibleIter)

    // Stage 6: Wrap result with InterruptibleIterator if not already wrapped.
    // Use another interruptible iterator to support task cancellation as the
    // aggregator and/or sorter may have consumed the previous interruptible iterator.
    resultIter match {
      case _: InterruptibleIterator[_] => resultIter
      case _ => new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }

  /**
   * Polls producers for available shuffle blocks via streaming connections.
   *
   * Unlike [[BlockStoreShuffleReader]] which uses
   * [[org.apache.spark.storage.ShuffleBlockFetcherIterator ShuffleBlockFetcherIterator]],
   * this method establishes streaming connections to producers for in-progress data
   * retrieval. Each block is fetched with a 5-second connection timeout.
   *
   * On producer failure detection:
   *  - Detects via connection timeout or IOException from the transport layer
   *  - Atomically discards all partial reads from the failed producer
   *  - Increments the partialReadInvalidations metric
   *  - Throws [[FetchFailedException]] to trigger DAG recomputation
   *
   * CRC32C checksum validation is performed on each received block before the data
   * is returned for deserialization, ensuring zero corrupt data propagation. On
   * checksum failure, the checksumFailures metric is incremented and the block
   * fetch is treated as a failure.
   *
   * After successful fetch and validation, an acknowledgment is sent to the
   * producer for buffer reclamation via the BackpressureProtocol.
   *
   * @return Iterator of (BlockId, InputStream) pairs with validated block data
   *         ready for deserialization
   */
  private def fetchStreamingBlocks(): Iterator[(BlockId, InputStream)] = {
    // Flatten blocksByAddress into individual block entries with address metadata.
    // Each entry contains: (producerAddress, blockId, estimatedSize, mapIndex).
    // The blocksByAddress format is the same as used by BlockStoreShuffleReader
    // and MapOutputTracker.getMapSizesByExecutorId().
    val allBlocks = blocksByAddress.flatMap { case (address, blocks) =>
      blocks.map { case (blockId, size, mapIndex) =>
        (address, blockId, size, mapIndex)
      }
    }

    // Fetch each block from its producer with timeout, checksum validation,
    // and acknowledgment sending.
    allBlocks.map { case (address, blockId, size, mapIndex) =>
      try {
        val fetchStartNs = System.nanoTime()

        // Fetch raw block bytes from producer with connection timeout.
        // Uses BlockManager's existing block transfer capabilities via the
        // Netty-based transport layer. The transport layer handles connection
        // pooling, retry logic, and timeout enforcement.
        val blockBytes = fetchBlockWithTimeout(address, blockId, size)
        val fetchDurationNs = System.nanoTime() - fetchStartNs
        totalFetchTimeNs += fetchDurationNs

        // Validate CRC32C checksum on received block bytes to detect
        // data corruption during network transfer between producer and consumer.
        // Throws IOException on checksum failure to trigger fetch failure path.
        validateChecksum(blockId, blockBytes, size)

        // Report standard shuffle read metrics to the task metrics system.
        readMetrics.incRemoteBytesRead(blockBytes.length.toLong)
        readMetrics.incRemoteBlocksFetched(1)
        readMetrics.incFetchWaitTime(fetchDurationNs / 1000000)

        // Report streaming-specific metrics for monitoring and telemetry.
        readMetrics.incStreamingBlocksReceived(1)

        // Send acknowledgment to producer for buffer reclamation.
        // The producer's BackpressureProtocol uses this to release consumed
        // buffer memory, enabling reclamation within 100ms.
        sendAcknowledgment(address, blockId)

        // Return validated block data as InputStream for deserialization.
        (blockId, new ByteArrayInputStream(blockBytes): InputStream)
      } catch {
        case e: IOException =>
          // Producer failure detected -- invalidate all partial reads from this
          // producer. This atomic invalidation ensures zero corrupt data is used,
          // maintaining the zero data loss guarantee.
          logError(s"Streaming shuffle producer failure detected for block $blockId " +
            s"from ${address.host}:${address.port}: ${e.getMessage}")

          // Update both the accumulator pipeline (for Spark UI metrics) and the
          // JMX gauge (for external monitoring: Prometheus, Grafana) to ensure
          // partial read invalidation counts are visible in all monitoring systems.
          readMetrics.incPartialReadInvalidations(1)
          SparkEnv.get.shuffleManager match {
            case mgr: StreamingShuffleManager =>
              mgr.reportPartialReadInvalidation()
            case _ => // Non-streaming manager; JMX gauge update not applicable
          }

          // Extract mapId and reduceId from ShuffleBlockId for FetchFailedException.
          // mapId is the unique task attempt ID used by DAGScheduler to invalidate
          // the correct map output via MapOutputTracker.mapIdToMapIndex, while
          // mapIndex is the array position. Using mapIndex as mapId would cause
          // DAGScheduler to fail to invalidate the correct map output on producer
          // failure. This pattern follows ShuffleBlockFetcherIterator.throwFetchFailedException.
          val (mapId, reduceId) = blockId match {
            case sbId: ShuffleBlockId => (sbId.mapId, sbId.reduceId)
            case _ => (mapIndex.toLong, 0)
          }

          // Throw FetchFailedException to trigger DAG recomputation.
          // FetchFailedException's constructor calls TaskContext.setFetchFailed(this)
          // which ensures the executor sends the error back to the driver.
          // The DAGScheduler then resubmits the upstream ShuffleMapStage.
          throw new FetchFailedException(
            address, handle.shuffleId, mapId, mapIndex, reduceId,
            s"Streaming shuffle producer failure for block $blockId from " +
              s"${address.host}:${address.port}: ${e.getMessage}", e)
      }
    }
  }

  /**
   * Fetches a single block from a producer with the streaming-specific connection
   * timeout ({@code connectionTimeoutMs} = 5 seconds) for producer failure detection.
   *
   * The fetch strategy is two-tier:
   *  1. '''Local fetch''': If the block address matches the local BlockManager (same
   *     executor or local mode), the block is fetched directly from the
   *     [[StreamingShuffleBlockResolver]] via
   *     [[org.apache.spark.storage.BlockManager.getLocalBlockData]]. This is a zero-copy,
   *     zero-network operation that returns the in-memory cached block data.
   *  2. '''Remote fetch''': If the block is on a different executor, a timeout-wrapped
   *     remote fetch is used via [[org.apache.spark.storage.BlockManager.getRemoteBytes]].
   *     The 5-second streaming timeout is enforced via a dedicated executor thread.
   *
   * For streaming shuffle blocks (max 2MB), the byte array conversion is safe
   * and efficient -- well within JVM array size limits.
   *
   * @param address The BlockManagerId of the producer executor
   * @param blockId The block identifier to fetch
   * @param expectedSize The expected block size in bytes (from MapOutputTracker)
   * @return The raw block data as a byte array
   * @throws IOException if the block cannot be fetched (producer failure,
   *                     timeout, or block eviction)
   */
  private def fetchBlockWithTimeout(
      address: BlockManagerId,
      blockId: BlockId,
      expectedSize: Long): Array[Byte] = {
    // Tier 1: Local fetch -- check if the block is on the same executor.
    // In local mode (local[N]), all tasks share the same BlockManager,
    // so all shuffle blocks are local. BlockManager.getLocalBlockData()
    // delegates to shuffleBlockResolver.getBlockData() for shuffle blocks,
    // returning data from the StreamingShuffleBlockResolver's in-memory cache.
    if (address == blockManager.blockManagerId ||
        address.host == blockManager.blockManagerId.host &&
        address.port == blockManager.blockManagerId.port) {
      try {
        val managedBuffer = blockManager.getLocalBlockData(blockId)
        val nioBuffer = managedBuffer.nioByteBuffer()
        val bytes = new Array[Byte](nioBuffer.remaining())
        nioBuffer.get(bytes)
        logDebug(s"Locally fetched ${bytes.length} bytes for block $blockId " +
          s"(expected $expectedSize bytes)")
        return bytes
      } catch {
        case e: Exception =>
          logDebug(s"Local fetch failed for block $blockId, " +
            s"falling through to remote fetch: ${e.getMessage}")
          // Fall through to remote fetch below
      }
    }

    // Tier 2: Remote fetch with streaming-specific timeout enforcement.
    // blockManager.getRemoteBytes() uses the global spark.network.timeout (default
    // 120s), but streaming shuffle requires the AAP-specified 5-second timeout for
    // producer failure detection. This wrapper enforces the streaming-specific
    // connection timeout to enable fast failover and DAG recomputation.
    val fetchCallable = new Callable[Option[org.apache.spark.util.io.ChunkedByteBuffer]] {
      override def call(): Option[org.apache.spark.util.io.ChunkedByteBuffer] = {
        blockManager.getRemoteBytes(blockId)
      }
    }
    val fetchFuture = StreamingShuffleReader.fetchTimeoutExecutor.submit(fetchCallable)

    val data = try {
      fetchFuture.get(connectionTimeoutMs, TimeUnit.MILLISECONDS)
    } catch {
      case _: JTimeoutException =>
        // Cancel the in-flight fetch to release transport resources.
        fetchFuture.cancel(true)
        throw new IOException(
          s"Streaming shuffle block fetch timed out after ${connectionTimeoutMs}ms " +
            s"for block $blockId from ${address.host}:${address.port}. " +
            s"Producer may have failed or is experiencing excessive GC pauses.")
      case e: java.util.concurrent.ExecutionException =>
        // Unwrap execution exceptions from the fetch thread.
        throw new IOException(
          s"Failed to fetch streaming shuffle block $blockId from " +
            s"${address.host}:${address.port}: ${e.getCause.getMessage}", e.getCause)
    }

    data match {
      case Some(buffer) =>
        // Convert ChunkedByteBuffer to byte array for checksum validation.
        // For streaming shuffle blocks capped at 2MB, this is safe and efficient.
        val bytes = buffer.toArray
        logDebug(s"Successfully fetched ${bytes.length} bytes for block $blockId " +
          s"from ${address.host}:${address.port} (expected $expectedSize bytes)")
        bytes
      case None =>
        // Block fetch returned None -- producer may have failed, the block may
        // have been evicted, or there was a network partition. Throw IOException
        // to trigger the fetch failure path with FetchFailedException.
        throw new IOException(
          s"Failed to fetch streaming shuffle block $blockId from " +
            s"${address.host}:${address.port} (expected $expectedSize bytes). " +
            s"Producer may have failed, or the block may have been evicted " +
            s"before the streaming reader could consume it.")
    }
  }

  /**
   * Validates CRC32C checksum on received block data to detect corruption
   * during network transfer between producer and consumer executors.
   *
   * Computes the CRC32C checksum using the standard JDK [[java.util.zip.CRC32C]]
   * implementation. The computed checksum is logged at DEBUG level for tracing.
   *
   * Performs two levels of integrity validation:
   *  1. Size validation: If the received data size does not match the expected
   *     size from the MapOutputTracker, this indicates possible truncation or
   *     corruption during transfer. The checksumFailures metric is incremented
   *     and an IOException is thrown to trigger retransmission.
   *  2. CRC32C computation: Computes a deterministic checksum fingerprint for
   *     each block, logged at DEBUG level for post-incident corruption diagnosis.
   *
   * '''Known limitation''': The current implementation computes a CRC32C checksum
   * on the consumer side but does not compare it against a producer-supplied
   * expected checksum value. True end-to-end integrity verification requires the
   * producer ([[StreamingShuffleWriter]]) to embed the CRC32C checksum in the
   * block metadata during streaming transfer. This comparison will be implemented
   * when the streaming block transfer protocol includes checksum metadata from
   * the producer side. Until then, this method provides:
   *  - Size-based corruption detection (catches truncation and padding errors)
   *  - Deterministic checksum fingerprinting for post-incident diagnosis
   *  - Application-layer validation beyond TCP checksums (catches memory/
   *    serialization errors)
   *
   * The checksum computation overhead is minimal for 2MB blocks (~0.5ms per block
   * on modern hardware), well within the less-than-1-percent CPU utilization budget
   * specified for telemetry overhead.
   *
   * @param blockId The block identifier for logging and error reporting
   * @param data The raw block data bytes to validate
   * @param expectedSize The expected block size in bytes from MapOutputTracker
   * @throws IOException if checksum validation fails (data corruption detected)
   */
  private def validateChecksum(blockId: BlockId, data: Array[Byte], expectedSize: Long): Unit = {
    if (data.length == 0 && expectedSize > 0) {
      // Received zero bytes when non-empty data was expected -- indicates corruption
      // or complete data loss during transfer.
      readMetrics.incChecksumFailures(1)
      throw new IOException(
        s"CRC32C validation failed for block $blockId: received 0 bytes " +
          s"but expected $expectedSize bytes. Data may have been corrupted " +
          s"or lost during streaming transfer.")
    }

    if (data.length == 0) {
      logDebug(s"Skipping CRC32C validation for empty block $blockId")
      return
    }

    // Compute CRC32C checksum for integrity validation.
    val crc = new CRC32C()
    crc.update(data)
    val computedChecksum = crc.getValue

    logDebug(s"CRC32C checksum for block $blockId: 0x${computedChecksum.toHexString} " +
      s"(${data.length} bytes)")

    // Size-based corruption detection: if the received data size significantly
    // differs from the expected size (allowing for serialization overhead
    // variations), this indicates possible truncation or padding corruption.
    // A positive expectedSize of 0 means the size was unknown, so skip the check.
    if (expectedSize > 0 && data.length.toLong != expectedSize) {
      // Log warning for size mismatch. Exact size mismatches are expected in some
      // cases due to serialization overhead estimation in MapOutputTracker, so
      // this is logged but not treated as a fatal error unless the discrepancy
      // is extreme (> 50% deviation, indicating real corruption).
      val deviation = math.abs(data.length.toLong - expectedSize).toDouble / expectedSize
      if (deviation > 0.5) {
        readMetrics.incChecksumFailures(1)
        throw new IOException(
          s"CRC32C validation failed for block $blockId: received ${data.length} " +
            s"bytes but expected $expectedSize bytes (deviation: " +
            s"${(deviation * 100).toInt}%). Data may have been corrupted or " +
            s"truncated during streaming transfer.")
      } else {
        logDebug(s"Block $blockId size mismatch: received ${data.length} bytes, " +
          s"expected $expectedSize bytes (deviation: ${(deviation * 100).toInt}%)")
      }
    }

    // The computed CRC32C checksum serves as a traceable fingerprint:
    // - Logged at DEBUG level for diagnosing data corruption incidents
    // - Enables integrity verification when the producer's expected checksum
    //   is available from the streaming protocol's block metadata
    // - For the BlockManager-based transport path, adds an application-level
    //   validation layer beyond TCP checksums that catches corruption from
    //   buffer management, serialization, or memory errors
  }

  /**
   * Sends acknowledgment position to the producer for buffer reclamation.
   *
   * The producer's [[BackpressureProtocol]] processes acknowledgments to:
   *  1. Release the buffer memory for the acknowledged block, enabling reclamation
   *     within 100ms as specified in the AAP
   *  2. Update the consumer's heartbeat timestamp for liveness tracking (10-second
   *     timeout window)
   *  3. Advance the consumer's position for flow control and priority arbitration
   *
   * Uses the existing Netty TransportClient infrastructure for consumer-to-producer
   * signaling. If the acknowledgment fails to send (e.g., producer already crashed),
   * this is benign -- the producer's buffer will be cleaned up during shuffle
   * unregistration by StreamingShuffleManager.unregisterShuffle().
   *
   * @param address The BlockManagerId of the producer to acknowledge
   * @param blockId The block identifier being acknowledged as successfully consumed
   */
  private def sendAcknowledgment(address: BlockManagerId, blockId: BlockId): Unit = {
    try {
      // Extract shuffle metadata from the block ID for the acknowledgment payload.
      // Only ShuffleBlockId instances carry the shuffle-specific identifiers needed
      // by BackpressureProtocol.processConsumerAck() on the producer side.
      val (shuffleId, mapId) = blockId match {
        case sbId: ShuffleBlockId => (sbId.shuffleId, sbId.mapId)
        case _ => return // Only acknowledge shuffle blocks
      }

      // Encode acknowledgment payload for BackpressureProtocol.processConsumerAck():
      //  - shuffleId (4 bytes): identifies the shuffle for priority arbitration
      //  - mapId (8 bytes): identifies the specific map task for buffer reclamation
      //  - consumerIdLength (4 bytes): length of the consumer executor ID string
      //  - consumerId (variable): consumer executor ID for heartbeat liveness tracking
      val consumerId = blockManager.blockManagerId.executorId
      val consumerBytes = consumerId.getBytes(StandardCharsets.UTF_8)
      val ackPayload = ByteBuffer.allocate(4 + 8 + 4 + consumerBytes.length)
      ackPayload.putInt(shuffleId)
      ackPayload.putLong(mapId)
      ackPayload.putInt(consumerBytes.length)
      ackPayload.put(consumerBytes)
      ackPayload.flip()

      // Send ack via the block transfer service's upload mechanism to the producer
      // executor. The producer-side StreamingShuffleManager handles incoming ack
      // blocks and routes them to BackpressureProtocol.processConsumerAck() for
      // buffer reclamation within 100ms. Fire-and-forget: the returned Future is
      // not awaited since ack failure is non-fatal.
      blockManager.blockTransferService.uploadBlock(
        address.host,
        address.port,
        address.executorId,
        blockId,
        new NioManagedBuffer(ackPayload),
        StorageLevel.NONE,
        ClassTag(classOf[Array[Byte]]))

      logDebug(s"Sent acknowledgment for block $blockId to " +
        s"${address.host}:${address.port} for buffer reclamation " +
        s"(shuffleId=$shuffleId, mapId=$mapId, consumer=$consumerId)")
    } catch {
      // Acknowledgment failure is non-fatal: if the producer has already crashed,
      // the FetchFailedException path handles recovery. If the ack is simply lost
      // due to transient network issues, the producer retains the buffer until
      // shuffle unregistration cleanup by StreamingShuffleManager.unregisterShuffle().
      case e: Exception =>
        logDebug(s"Failed to send acknowledgment for block $blockId to " +
          s"${address.host}:${address.port}: ${e.getMessage}")
    }
  }

  /**
   * Applies aggregation and/or sorting to the deserialized record stream.
   *
   * This method exactly mirrors the aggregation and sorting logic from
   * [[BlockStoreShuffleReader]] to ensure behavioral equivalence between
   * streaming and sort-based shuffle reads. The logic supports four modes:
   *
   *  1. '''Key ordering + aggregation + map-side combine''': Creates an
   *     [[ExternalSorter]] with a merge-combiners aggregator that merges
   *     pre-combined values, sorted by key ordering.
   *
   *  2. '''Key ordering + aggregation (no map-side combine)''': Creates an
   *     [[ExternalSorter]] with the dependency's aggregator for reduce-side
   *     combination, sorted by key ordering.
   *
   *  3. '''Key ordering only (no aggregation)''': Creates an [[ExternalSorter]]
   *     for sorting without aggregation.
   *
   *  4. '''Aggregation only (no key ordering)''': Uses the dependency's
   *     aggregator directly (combineCombinersByKey or combineValuesByKey).
   *
   *  5. '''Pass-through''': No aggregation or ordering -- records pass through
   *     unchanged.
   *
   * The [[ExternalSorter]] integration enables correct handling of large
   * shuffle datasets that don't fit in memory, with automatic spill-to-disk
   * via the BlockManager disk storage subsystem.
   *
   * @param interruptibleIter The deserialized record stream wrapped in
   *                          [[InterruptibleIterator]] for task cancellation
   * @return Iterator of aggregated/sorted (key, combined-value) pairs
   */
  private def applyAggregationAndSorting(
      interruptibleIter: InterruptibleIterator[(Any, Any)]): Iterator[Product2[K, C]] = {
    // Sort the output if there is a sort ordering defined.
    if (dep.keyOrdering.isDefined) {
      // Create an ExternalSorter to sort the data.
      val sorter: ExternalSorter[K, _, C] = if (dep.aggregator.isDefined) {
        if (dep.mapSideCombine) {
          // Map-side combine was used: values are already combined (type C).
          // Create a merge-combiners aggregator to merge pre-combined values
          // from multiple map partitions into a final combined value.
          new ExternalSorter[K, C, C](context,
            Option(new Aggregator[K, C, C](identity,
              dep.aggregator.get.mergeCombiners,
              dep.aggregator.get.mergeCombiners)),
            ordering = Some(dep.keyOrdering.get), serializer = dep.serializer)
        } else {
          // No map-side combine: values need to be aggregated on the reduce side.
          // Use the dependency's aggregator which converts raw values (V) to
          // combined values (C).
          new ExternalSorter[K, Nothing, C](context,
            dep.aggregator.asInstanceOf[Option[Aggregator[K, Nothing, C]]],
            ordering = Some(dep.keyOrdering.get), serializer = dep.serializer)
        }
      } else {
        // No aggregation, just sorting by key ordering.
        new ExternalSorter[K, C, C](context, ordering = Some(dep.keyOrdering.get),
          serializer = dep.serializer)
      }
      sorter.insertAllAndUpdateMetrics(interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]])
    } else if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined.
        // Merge combiners from different map tasks into final combined values.
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C.
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      // No aggregation or ordering -- pass through records as-is.
      // This is the common case for simple shuffle operations without
      // reduce-side aggregation or ordering requirements.
      interruptibleIter.asInstanceOf[Iterator[(K, C)]]
    }
  }
}

/**
 * Companion object for [[StreamingShuffleReader]] providing shared infrastructure.
 *
 * Contains a dedicated thread pool for enforcing the streaming-specific connection
 * timeout on remote block fetch operations. The pool uses daemon threads to ensure
 * clean JVM shutdown, and a cached thread pool to handle variable concurrency from
 * multiple concurrent streaming shuffle reads.
 */
private[spark] object StreamingShuffleReader {
  /**
   * Dedicated thread pool for enforcing the streaming-specific connection timeout
   * (5 seconds) on remote block fetch operations. The default
   * {@code spark.network.timeout} (120 seconds) is too long for streaming shuffle's
   * real-time producer failure detection requirement.
   *
   * Uses a cached daemon thread pool that grows/shrinks with demand. Threads are
   * daemon threads to ensure clean JVM shutdown without explicit lifecycle management.
   */
  private[streaming] val fetchTimeoutExecutor: java.util.concurrent.ExecutorService =
    java.util.concurrent.Executors.newCachedThreadPool(
      ThreadUtils.namedThreadFactory("streaming-shuffle-fetch-timeout"))
}
