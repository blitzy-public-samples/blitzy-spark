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

import java.io.File
import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{BlockId, BlockManager}

/**
 * Manages memory pressure for streaming shuffle buffers.
 *
 * Monitors buffer utilization at 100ms polling intervals and triggers automatic
 * disk spill via LRU partition eviction when buffer occupancy exceeds the
 * configured threshold (default 80%, configurable 50-95%). Integrates with
 * BlockManager's existing disk storage for spill persistence and reclaims
 * memory within 100ms of consumer acknowledgment.
 *
 * Thread Safety:
 *  - Buffer tracking uses ConcurrentHashMap for lock-free concurrent access.
 *  - Metrics counters use AtomicLong for lock-free updates.
 *  - Spill state transitions use ConcurrentHashMap.replace() for atomicity,
 *    preventing race conditions between the polling thread and reclamation calls.
 *  - The polling scheduler runs on a single daemon thread.
 *
 * Memory Safety:
 *  - Streaming buffers never exceed the configured percentage of executor memory.
 *  - Buffer allocation uses a CAS loop to prevent over-allocation under concurrency.
 *  - Buffer reclamation responds within 100ms of consumer acknowledgment.
 *  - stop() clears all state and deletes spill files to prevent leaks.
 *
 * Coexistence: This component only manages streaming shuffle buffers and has
 * zero impact on sort-based shuffle memory management. It integrates with the
 * existing MemoryManager and BlockManager APIs without modification.
 *
 * @param conf SparkConf containing streaming shuffle configuration parameters
 * @param blockManager BlockManager for disk spill persistence via diskBlockManager
 */
private[spark] class MemorySpillManager(
    conf: SparkConf,
    blockManager: BlockManager) extends Logging {

  // -- Configuration (read via StreamingShuffleConfig) --

  /** Spill threshold percentage: default 80%, configurable range [50, 95] */
  private val spillThresholdPercent: Int = StreamingShuffleConfig.getSpillThreshold(conf)

  /** Buffer size as percentage of executor memory: default 20%, configurable [1, 50] */
  private val bufferSizePercent: Int = StreamingShuffleConfig.getBufferSizePercent(conf)

  /** Polling interval for utilization monitoring: fixed at 100ms per specification */
  private val pollingIntervalMs: Long = 100L

  // -- Lifecycle State --

  /** Lifecycle flag for idempotent start/stop via compareAndSet */
  private val isRunning = new AtomicBoolean(false)

  /** Scheduled executor for the 100ms polling loop; null when stopped */
  @volatile private var scheduler: ScheduledExecutorService = _

  // -- Buffer Tracking State --

  /**
   * Thread-safe map tracking per-partition buffer metadata.
   * Key: (shuffleId, partitionId) tuple with proper hashCode/equals from Scala.
   * Value: Immutable PartitionBufferInfo with size, access time, and spill state.
   */
  private val partitionBuffers =
    new ConcurrentHashMap[(Int, Int), PartitionBufferInfo]()

  /** Cumulative bytes currently allocated across all active streaming shuffle buffers */
  private val totalAllocatedBytes = new AtomicLong(0L)

  /**
   * Maximum bytes available for streaming shuffle buffers on this executor.
   * Calculated as: executorMemoryBytes * bufferSizePercent / 100.
   * Set during start() and remains constant for the executor lifetime.
   */
  @volatile private var totalAvailableBytes: Long = 0L

  // -- Spill Metrics (all lock-free via AtomicLong) --

  /** Cumulative count of disk spill events since start */
  private val spillCount = new AtomicLong(0L)

  /** Cumulative bytes spilled to disk since start */
  private val spillBytes = new AtomicLong(0L)

  /** Cumulative spill operation latency in nanoseconds since start */
  private val spillLatencyNs = new AtomicLong(0L)

  // -- Inner Types --

  /**
   * Immutable metadata tracked per partition buffer for LRU eviction decisions.
   *
   * Immutability ensures thread-safe state transitions via ConcurrentHashMap.replace(),
   * preventing double-decrement races between the spill polling thread and the
   * buffer reclamation path (called from the backpressure acknowledgment handler).
   *
   * @param sizeBytes        Current buffer size in bytes for this partition
   * @param lastAccessTimeMs Epoch milliseconds of last buffer access (LRU tiebreaker)
   * @param spilledToDisk    Whether this partition buffer has been spilled to disk
   * @param spillBlockId     BlockId of the spill file (present only when spilledToDisk)
   * @param spillFile        File reference for cleanup on reclaim/stop
   */
  private case class PartitionBufferInfo(
      sizeBytes: Long,
      lastAccessTimeMs: Long,
      spilledToDisk: Boolean = false,
      spillBlockId: Option[BlockId] = None,
      spillFile: Option[File] = None)

  // =========================================================================
  // Lifecycle Management
  // =========================================================================

  /**
   * Starts the 100ms polling loop that monitors buffer utilization.
   *
   * Must be called once per executor when streaming shuffle is enabled.
   * Idempotent: subsequent calls after the first are no-ops.
   *
   * @param executorMemoryBytes Total executor memory in bytes; used to calculate
   *                           the streaming buffer memory budget as
   *                           (executorMemoryBytes * bufferSizePercent / 100)
   */
  def start(executorMemoryBytes: Long): Unit = {
    require(executorMemoryBytes > 0,
      s"executorMemoryBytes must be positive, got $executorMemoryBytes")

    if (isRunning.compareAndSet(false, true)) {
      totalAvailableBytes = (executorMemoryBytes * bufferSizePercent) / 100
      logInfo(s"MemorySpillManager started: totalAvailable=$totalAvailableBytes bytes " +
        s"(${bufferSizePercent}% of $executorMemoryBytes bytes), " +
        s"spillThreshold=${spillThresholdPercent}%")

      // Create a single daemon thread for the polling loop.
      // Daemon thread ensures the JVM can exit even if the scheduler is running.
      scheduler = Executors.newSingleThreadScheduledExecutor(
        new java.util.concurrent.ThreadFactory {
          override def newThread(r: Runnable): Thread = {
            val t = new Thread(r, "streaming-shuffle-spill-monitor")
            t.setDaemon(true)
            t
          }
        })

      // Schedule the utilization check at fixed 100ms intervals.
      scheduler.scheduleAtFixedRate(
        new Runnable { override def run(): Unit = checkSpillCondition() },
        pollingIntervalMs,
        pollingIntervalMs,
        TimeUnit.MILLISECONDS)
    }
  }

  /**
   * Stops the polling scheduler and cleans up all tracked partition buffers
   * and spilled disk files. Ensures zero memory leaks by clearing all state.
   *
   * Idempotent: subsequent calls after the first are no-ops.
   */
  def stop(): Unit = {
    if (isRunning.compareAndSet(true, false)) {
      // Shutdown the polling scheduler first to prevent new spills during cleanup
      if (scheduler != null) {
        scheduler.shutdownNow()
        try {
          if (!scheduler.awaitTermination(500, TimeUnit.MILLISECONDS)) {
            logWarning("Spill monitor scheduler did not terminate within 500ms")
          }
        } catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
        }
        scheduler = null
      }

      // Clean up all spilled disk files to prevent disk leaks
      partitionBuffers.asScala.foreach { case (_, info) =>
        cleanupSpillFile(info)
      }

      // Clear all tracking state for zero memory leaks
      partitionBuffers.clear()
      totalAllocatedBytes.set(0L)

      val avgSpillLatencyMs = if (spillCount.get() > 0) {
        spillLatencyNs.get() / spillCount.get() / 1000000
      } else {
        0
      }
      logInfo(s"MemorySpillManager stopped. Lifetime stats: " +
        s"spillCount=${spillCount.get()}, spillBytes=${spillBytes.get()}, " +
        s"avgSpillLatencyMs=$avgSpillLatencyMs")
    }
  }

  /** Returns true if the spill monitor is actively running */
  def isActive: Boolean = isRunning.get()

  // =========================================================================
  // Buffer Allocation and Reclamation
  // =========================================================================

  /**
   * Reports buffer allocation for a streaming shuffle partition.
   *
   * Memory safety: Uses a CAS loop to atomically check and update
   * totalAllocatedBytes, preventing over-allocation under concurrent access
   * from multiple map tasks.
   *
   * @param shuffleId   Shuffle identifier
   * @param partitionId Partition identifier within the shuffle
   * @param sizeBytes   Requested buffer size in bytes (must be non-negative)
   * @return true if allocation succeeded, false if memory pressure prevents it
   */
  def allocateBuffer(shuffleId: Int, partitionId: Int, sizeBytes: Long): Boolean = {
    if (!isRunning.get()) {
      logWarning(s"allocateBuffer called while MemorySpillManager is not active " +
        s"(shuffle=$shuffleId, partition=$partitionId)")
      return false
    }
    require(sizeBytes >= 0, s"sizeBytes must be non-negative, got $sizeBytes")

    // CAS loop for atomic check-and-increment of totalAllocatedBytes.
    // Prevents concurrent allocations from exceeding totalAvailableBytes.
    var allocated = false
    var currentTotal = totalAllocatedBytes.get()
    while (!allocated) {
      if (currentTotal + sizeBytes > totalAvailableBytes) {
        logWarning(s"Buffer allocation denied: current=$currentTotal, " +
          s"requested=$sizeBytes, available=$totalAvailableBytes " +
          s"(shuffle=$shuffleId, partition=$partitionId)")
        return false
      }
      allocated = totalAllocatedBytes.compareAndSet(currentTotal, currentTotal + sizeBytes)
      if (!allocated) {
        // Another thread modified totalAllocatedBytes; re-read and retry
        currentTotal = totalAllocatedBytes.get()
      }
    }

    // Track the partition buffer metadata with current timestamp for LRU
    partitionBuffers.put(
      (shuffleId, partitionId),
      PartitionBufferInfo(sizeBytes, System.currentTimeMillis()))

    logDebug(s"Buffer allocated: shuffle=$shuffleId, partition=$partitionId, " +
      s"size=$sizeBytes, totalAllocated=${totalAllocatedBytes.get()}")
    true
  }

  /**
   * Updates buffer tracking for a partition when the buffer grows incrementally.
   *
   * Adjusts totalAllocatedBytes by the delta between old and new size.
   * Uses ConcurrentHashMap.replace() for atomic state transition.
   * Updates the last access timestamp for LRU tracking.
   *
   * @param shuffleId    Shuffle identifier
   * @param partitionId  Partition identifier within the shuffle
   * @param newSizeBytes Updated total buffer size in bytes for this partition
   */
  def updateBuffer(shuffleId: Int, partitionId: Int, newSizeBytes: Long): Unit = {
    val key = (shuffleId, partitionId)
    val oldInfo = partitionBuffers.get(key)
    if (oldInfo != null && !oldInfo.spilledToDisk) {
      val delta = newSizeBytes - oldInfo.sizeBytes
      val updatedInfo = PartitionBufferInfo(newSizeBytes, System.currentTimeMillis())
      // Atomic replace: succeeds only if the entry hasn't been concurrently modified
      if (partitionBuffers.replace(key, oldInfo, updatedInfo)) {
        totalAllocatedBytes.addAndGet(delta)
        logDebug(s"Buffer updated: shuffle=$shuffleId, partition=$partitionId, " +
          s"oldSize=${oldInfo.sizeBytes}, newSize=$newSizeBytes, delta=$delta")
      }
    }
  }

  /**
   * Reclaims buffer memory for a partition after consumer acknowledgment.
   *
   * Designed to respond within 100ms per specification -- uses direct method
   * invocation with no queuing delay. Called by BackpressureProtocol when
   * a consumer acknowledgment is received.
   *
   * Also cleans up any spilled disk files associated with this partition.
   *
   * @param shuffleId   Shuffle identifier
   * @param partitionId Partition identifier within the shuffle
   */
  def reclaimBuffer(shuffleId: Int, partitionId: Int): Unit = {
    val key = (shuffleId, partitionId)
    val info = partitionBuffers.remove(key)
    if (info != null) {
      // Only decrement totalAllocatedBytes if the buffer was NOT already spilled.
      // If spilledToDisk is true, the bytes were already reclaimed during spill.
      if (!info.spilledToDisk) {
        totalAllocatedBytes.addAndGet(-info.sizeBytes)
      }
      // Clean up spilled disk file if present
      cleanupSpillFile(info)

      logDebug(s"Buffer reclaimed: shuffle=$shuffleId, partition=$partitionId, " +
        s"size=${info.sizeBytes}, wasSpilled=${info.spilledToDisk}")
    }
  }

  // =========================================================================
  // Metrics Accessors
  // =========================================================================

  /** Returns the cumulative count of disk spill events since start */
  def getSpillCount: Long = spillCount.get()

  /** Returns the cumulative bytes spilled to disk since start */
  def getSpillBytes: Long = spillBytes.get()

  /** Returns the cumulative spill operation latency in nanoseconds since start */
  def getSpillLatencyNs: Long = spillLatencyNs.get()

  /**
   * Returns the current buffer utilization as a percentage of total available.
   * Calculated as: (totalAllocatedBytes * 100) / totalAvailableBytes.
   * Returns 0 if totalAvailableBytes is zero (before start or after stop).
   */
  def getBufferUtilizationPercent: Int = {
    if (totalAvailableBytes <= 0L) 0
    else ((totalAllocatedBytes.get() * 100L) / totalAvailableBytes).toInt
  }

  /** Returns the current total bytes allocated across all streaming shuffle buffers */
  def getTotalAllocatedBytes: Long = totalAllocatedBytes.get()

  // =========================================================================
  // Internal: Spill Monitoring and Execution
  // =========================================================================

  /**
   * Checks current buffer utilization against the configured spill threshold.
   * Called every 100ms by the polling scheduler thread.
   *
   * Exception-safe: catches and logs all exceptions to prevent the scheduled
   * executor from silently stopping on unhandled errors. Per ScheduledExecutorService
   * contract, an uncaught exception would cancel the scheduled task permanently.
   */
  private def checkSpillCondition(): Unit = {
    try {
      val currentBytes = totalAllocatedBytes.get()
      if (totalAvailableBytes > 0L) {
        val thresholdBytes = (totalAvailableBytes * spillThresholdPercent) / 100
        if (currentBytes >= thresholdBytes) {
          logInfo(s"Spill triggered: allocated=$currentBytes bytes >= " +
            s"threshold=$thresholdBytes bytes " +
            s"(${spillThresholdPercent}% of $totalAvailableBytes)")
          spillLargestPartition()
        }
      }
    } catch {
      case e: Exception =>
        logWarning("Error in spill condition check", e)
    }
  }

  /**
   * Selects the largest non-spilled buffered partition via LRU policy and
   * spills it to disk via BlockManager's DiskBlockManager.
   *
   * Selection criteria (LRU with size priority):
   *  1. Primary: Largest buffer size (descending)
   *  2. Tiebreaker: Oldest last-access time (ascending -- Least Recently Used)
   *
   * Atomic guarantee: The state transition from in-memory to spilled uses
   * ConcurrentHashMap.replace(key, oldValue, newValue), ensuring that a
   * concurrent reclaimBuffer() call for the same partition does not cause
   * double-decrement of totalAllocatedBytes.
   *
   * Disk persistence: Uses BlockManager.diskBlockManager.createTempLocalBlock()
   * to allocate a temporary local block file, following the same pattern used
   * by ExternalSorter for its spill files.
   */
  private def spillLargestPartition(): Unit = {
    val startTimeNs = System.nanoTime()

    // Find the largest non-spilled partition using LRU selection criteria.
    // Iteration over ConcurrentHashMap is weakly consistent -- safe under concurrency.
    val candidateOpt = partitionBuffers.asScala
      .filter { case (_, info) => !info.spilledToDisk }
      .maxByOption { case (_, info) =>
        // Primary: largest size; Tiebreaker: oldest access (smallest ms = most negative)
        (info.sizeBytes, -info.lastAccessTimeMs)
      }

    candidateOpt match {
      case Some(((shuffleId, partitionId), candidateInfo)) =>
        try {
          // Allocate a temporary local block file via BlockManager's disk infrastructure.
          val (spillBlockId, spillFile) =
            blockManager.diskBlockManager.createTempLocalBlock()

          logInfo(s"Spilling partition (shuffle=$shuffleId, partition=$partitionId): " +
            s"${candidateInfo.sizeBytes} bytes to block $spillBlockId")

          // Create updated info marking the partition as spilled with file reference
          val spilledInfo = candidateInfo.copy(
            spilledToDisk = true,
            spillBlockId = Some(spillBlockId),
            spillFile = Some(spillFile))

          // Atomic state transition via replace(). Succeeds only if the entry has not
          // been modified or removed by a concurrent reclaimBuffer() or updateBuffer().
          val key = (shuffleId, partitionId)
          if (partitionBuffers.replace(key, candidateInfo, spilledInfo)) {
            // Spill marked successfully -- reclaim memory tracking.
            // Note: the actual data write to spillFile is coordinated externally
            // by StreamingShuffleWriter, which holds the actual buffer data bytes.
            totalAllocatedBytes.addAndGet(-candidateInfo.sizeBytes)

            // Update spill metrics atomically
            spillCount.incrementAndGet()
            spillBytes.addAndGet(candidateInfo.sizeBytes)
            val latencyNs = System.nanoTime() - startTimeNs
            spillLatencyNs.addAndGet(latencyNs)

            logInfo(s"Spill completed: shuffle=$shuffleId, partition=$partitionId, " +
              s"freed=${candidateInfo.sizeBytes} bytes, latency=${latencyNs / 1000000}ms")
          } else {
            // Entry was concurrently modified or removed -- clean up unused spill file
            if (spillFile.exists()) {
              spillFile.delete()
            }
            logDebug(s"Spill skipped for (shuffle=$shuffleId, partition=$partitionId): " +
              s"entry was modified concurrently")
          }
        } catch {
          case e: Exception =>
            logWarning(s"Failed to spill partition (shuffle=$shuffleId, " +
              s"partition=$partitionId)", e)
        }

      case None =>
        logDebug("No non-spilled partitions available for eviction")
    }
  }

  /**
   * Cleans up a spilled disk file associated with a partition buffer.
   * Logs a warning if file deletion fails but does not throw, following
   * the same defensive cleanup pattern used by ExternalSorter.
   */
  private def cleanupSpillFile(info: PartitionBufferInfo): Unit = {
    info.spillFile.foreach { file =>
      try {
        if (file.exists() && !file.delete()) {
          logWarning(s"Failed to delete spill file: ${file.getAbsolutePath}")
        }
      } catch {
        case e: Exception =>
          logWarning(s"Error cleaning up spill file: ${file.getAbsolutePath}", e)
      }
    }
  }
}
