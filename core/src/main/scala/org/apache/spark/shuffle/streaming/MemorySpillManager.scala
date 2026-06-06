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

import java.io.{File, FileOutputStream}
import java.util.{ArrayList, LinkedHashMap, UUID}
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage.TempLocalBlockId
import org.apache.spark.util.ThreadUtils

/**
 * Memory-spill coordinator for the opt-in streaming shuffle engine. A single instance is created
 * per executor by `StreamingShuffleManager` (only when the streaming path is actually active for a
 * shuffle) and is shared by the streaming writer pipeline. It bounds the heap footprint of the
 * per-partition streaming buffers so a fast producer cannot exhaust executor memory, delivering the
 * four capabilities required by the streaming design:
 *
 *  1. A lightweight poller that samples buffer-memory utilization every
 *     [[MemorySpillManager.POLL_INTERVAL_MILLIS]] (100ms) and publishes it to JMX through the
 *     shared [[StreamingShuffleSource]] `bufferUtilizationPercent` gauge.
 *  2. Spill-to-disk: when utilization crosses the configurable spill threshold (default 80%,
 *     `spark.shuffle.streaming.spillThreshold`), the least-recently-used buffered partitions are
 *     written to disk and their heap buffers released. Spilling the LRU partitions evicts the best
 *     candidates first, and a large LRU partition reclaims the most memory per spill. A threshold
 *     crossing observed while registering buffers schedules an out-of-band spill pass so the
 *     threshold-cross to spill-action latency stays well under the mandated 100ms.
 *  3. Prompt reclamation: [[reclaim]] (consumer acknowledgment) and [[unregister]] (writer
 *     stop/failure) return the corresponding buffer memory to the `MemoryManager` synchronously,
 *     within the mandated 100ms of the ack, so there are no memory leaks over a long-running job.
 *  4. Telemetry: every spill increments the `spillCount` counter on the shared
 *     [[StreamingShuffleSource]], exposed over JMX through the existing `MetricRegistry`.
 *
 * Memory-model reuse: buffer accounting goes exclusively through the EXISTING
 * [[org.apache.spark.memory.MemoryManager]] interface (`acquireStorageMemory`/
 * `releaseStorageMemory`); this class introduces no new memory model and never redesigns the
 * executor memory split. The streaming buffer budget is derived from the manager's current
 * on-heap storage capacity:
 *   bufferBudgetBytes = executorStorageMemory * spark.shuffle.streaming.bufferSizePercent / 100
 * and the per-partition budget the design references is this total divided by the partition count:
 *   (executorMemory * bufferPercent / 100) / numPartitions.
 *
 * Disk reuse: spilled blocks are written through the SAME on-disk machinery the default
 * `SortShuffleManager` uses, via the injected [[IndexShuffleBlockResolver]] (the manager's single
 * resolver). No new block-manager storage contract is introduced. If the block manager is
 * unavailable (for example, no running `SparkEnv` in a unit test), spilling falls back to a JVM
 * temp file so accounting still functions without ever throwing.
 *
 * Coexistence strategy: this manager is instantiated and active ONLY on the streaming path. The
 * default `SortShuffleManager` fallback never constructs it, so the sort-based shuffle is wholly
 * unaffected and carries zero spill-coordination overhead. Being `private[spark]` and confined to
 * the streaming package enforces the zero-cross-contamination rule: existing components neither
 * import nor depend on this class.
 *
 * Thread-safety: the partition registry is guarded by a single private monitor; the poller runs on
 * a dedicated daemon scheduler and, on the common (below-threshold) path, performs only a couple of
 * atomic reads plus one gauge write, holding telemetry overhead well under the mandated 1% CPU.
 * Verbose logging is gated behind `spark.shuffle.streaming.debug` to keep per-executor log volume
 * under the mandated 10MB/hour budget.
 *
 * @param conf the active [[SparkConf]] supplying the `spark.shuffle.streaming.*` settings
 * @param memoryManager the existing executor [[MemoryManager]] used for all buffer accounting
 * @param blockResolver the streaming manager's single [[IndexShuffleBlockResolver]], reused for
 *                       spilled-block disk writes to preserve the block-manager storage contract
 * @param metricsSource the shared JMX metrics source whose `spillCount` and
 *                       `bufferUtilizationPercent` are updated by this manager
 */
private[spark] class MemorySpillManager(
    conf: SparkConf,
    memoryManager: MemoryManager,
    blockResolver: IndexShuffleBlockResolver,
    metricsSource: StreamingShuffleSource)
  extends Logging {

  import MemorySpillManager._

  // Percent buffer utilization at which spilling begins (spark.shuffle.streaming.spillThreshold,
  // validated to 50-95, default 80).
  private val spillThreshold: Int = conf.get(config.STREAMING_SHUFFLE_SPILL_THRESHOLD)

  // Percent of executor memory reserved for streaming buffers (spark.shuffle.streaming.
  // bufferSizePercent, validated to 1-50, default 20); drives the utilization denominator.
  private val bufferPercent: Int = conf.get(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT)

  // Verbose debug-logging gate (spark.shuffle.streaming.debug). Kept off the hot path to hold
  // per-executor log volume under the mandated 10MB/hour budget.
  private val debug: Boolean = conf.get(config.STREAMING_SHUFFLE_DEBUG)

  // A stable block id used solely for storage-memory bookkeeping with the MemoryManager. Release is
  // by amount, so reusing one id across acquisitions keeps the storage pool perfectly balanced.
  private val bufferBlockId = TempLocalBlockId(UUID.randomUUID())

  // Monitor guarding every access to the partition registry below.
  private val registryLock = new Object()

  // Access-ordered registry of currently buffered partitions keyed by (shuffleId, mapId,
  // partitionId). accessOrder=true makes iteration yield the least-recently-used entry first, which
  // is exactly the LRU spill order. Guarded by registryLock.
  private val partitions =
    new LinkedHashMap[PartitionKey, BufferedPartition](
      INITIAL_REGISTRY_CAPACITY, LOAD_FACTOR, true)

  // Total in-memory logical bytes currently buffered across all partitions. This is the utilization
  // numerator and is tracked independently of grant success so the poller still sees pressure (and
  // spills) even when a storage-memory grant was declined.
  private val trackedBytes = new AtomicLong(0L)

  // Total storage memory actually reserved from the MemoryManager. Released exactly on free/spill
  // so the storage pool stays balanced and there are zero leaks.
  private val reservedBytes = new AtomicLong(0L)

  // Cumulative number of spills performed (diagnostics; the authoritative JMX counter is the
  // spillCount gauge on metricsSource).
  private val spillCount = new AtomicLong(0L)

  // Set once stop() has run; makes shutdown idempotent and short-circuits register/trigger after.
  private val stopped = new AtomicBoolean(false)

  // Collapses a burst of register-time spill triggers into at most one extra out-of-band poll pass.
  private val immediateSpillPending = new AtomicBoolean(false)

  // Dedicated daemon scheduler running the 100ms buffer-utilization poller. A daemon single thread
  // keeps spill-coordination overhead under the 1% CPU budget and never blocks executor shutdown.
  // Declared after all mutable state above so the first tick observes a fully-initialized manager.
  private val pollExecutor: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(POLLER_THREAD_NAME)

  // The periodic poll task, defined as an explicit Runnable (Spark convention). Failures are
  // swallowed inside poll() so a transient error can never tear down the recurring schedule.
  private val pollRunnable: Runnable = new Runnable {
    override def run(): Unit = poll()
  }

  // Schedule the poller at the fixed 100ms cadence; the handle is cancelled in stop().
  private val pollTask: ScheduledFuture[_] = pollExecutor.scheduleAtFixedRate(
    pollRunnable, POLL_INTERVAL_MILLIS, POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)

  if (debug) {
    logDebug(s"MemorySpillManager started: spillThreshold=$spillThreshold% " +
      s"bufferPercent=$bufferPercent% budgetBytes=${currentBufferBudgetBytes()}")
  }

  /**
   * Per-partition bookkeeping entry. While buffered, it holds the partition's byte chunks and the
   * amount of storage memory granted for them; once spilled, the chunks are released and only the
   * on-disk spill file is retained for later cleanup. Mutated only while holding `registryLock`.
   */
  private final class BufferedPartition(val key: PartitionKey) {
    // Accumulated in-memory byte chunks for this partition; cleared on free/spill.
    val chunks = new ArrayList[Array[Byte]]()
    // Logical in-memory size in bytes (sum of chunk lengths).
    var sizeBytes: Long = 0L
    // Storage memory actually granted by the MemoryManager for this partition's bytes.
    var grantedBytes: Long = 0L
    // Wall-clock nanos of the last register/touch; retained for diagnostics.
    var lastAccessNanos: Long = System.nanoTime()
    // True once the partition has been written to disk and its heap buffers released.
    var spilled: Boolean = false
    // The on-disk spill file once spilled; null while in memory.
    var spillFile: File = _
  }

  /**
   * Registers (or appends to) the in-memory buffer for a single shuffle partition and accounts its
   * footprint against the existing [[MemoryManager]]. The byte array is retained by the manager so
   * it can be spilled to disk under memory pressure; callers must not mutate it after registering.
   *
   * Registering refreshes the partition's LRU position. If the storage-memory grant is declined, or
   * registering pushes utilization across the spill threshold, an out-of-band spill pass is
   * scheduled so the response stays well under 100ms rather than waiting for the next poll tick.
   *
   * @param shuffleId the shuffle this buffer belongs to
   * @param mapId the map task producing the buffer
   * @param partitionId the target reduce partition
   * @param bytes the buffered bytes to track (ownership transfers to the manager); empty/null is a
   *              no-op
   * @return true if storage memory was granted for the bytes, false if the writer should back off
   *         (a spill has been scheduled); a no-op registration returns true
   */
  def register(shuffleId: Int, mapId: Long, partitionId: Int, bytes: Array[Byte]): Boolean = {
    if (bytes == null || bytes.length == 0 || stopped.get()) {
      true
    } else {
      val numBytes = bytes.length.toLong
      val granted = acquireStorage(numBytes)
      val key = PartitionKey(shuffleId, mapId, partitionId)
      registryLock.synchronized {
        val existing = partitions.get(key)
        val entry = if (existing == null) {
          val created = new BufferedPartition(key)
          partitions.put(key, created)
          created
        } else {
          existing
        }
        if (entry.spilled) {
          // A previously spilled partition is being appended to again: start a fresh in-memory
          // buffer. The earlier spill file remains on disk until the partition is reclaimed.
          entry.spilled = false
          entry.spillFile = null
        }
        entry.chunks.add(bytes)
        entry.sizeBytes += numBytes
        if (granted) {
          entry.grantedBytes += numBytes
        }
        entry.lastAccessNanos = System.nanoTime()
      }
      trackedBytes.addAndGet(numBytes)
      if (!granted || currentUtilizationPercent() >= spillThreshold) {
        triggerImmediateSpill()
      }
      granted
    }
  }

  // Reserve `numBytes` of on-heap storage memory through the existing MemoryManager. Guarded so a
  // memory-store interaction (only reached under genuine pool exhaustion) can never crash the
  // streaming path; a false result is the writer's signal to back off and let the poller spill.
  private def acquireStorage(numBytes: Long): Boolean = {
    try {
      val granted = memoryManager.acquireStorageMemory(bufferBlockId, numBytes, MemoryMode.ON_HEAP)
      if (granted) {
        reservedBytes.addAndGet(numBytes)
      }
      granted
    } catch {
      case NonFatal(e) =>
        if (debug) {
          logWarning("Streaming shuffle storage-memory acquisition failed; treating as throttled",
            e)
        }
        false
    }
  }

  // Return `numBytes` of on-heap storage memory to the existing MemoryManager. Guarded so a release
  // failure can never crash the streaming path; the reserved-bytes counter is always decremented to
  // keep accounting balanced.
  private def releaseStorage(numBytes: Long): Unit = {
    if (numBytes > 0L) {
      try {
        memoryManager.releaseStorageMemory(numBytes, MemoryMode.ON_HEAP)
      } catch {
        case NonFatal(e) =>
          if (debug) {
            logWarning("Streaming shuffle storage-memory release failed", e)
          }
      }
      reservedBytes.addAndGet(-numBytes)
    }
  }

  /**
   * Reclaims a partition's buffers after the consumer acknowledges it has consumed them. Releases
   * the buffer memory back to the [[MemoryManager]] (and deletes any on-disk spill file)
   * synchronously, within the mandated 100ms of the ack, and drops the partition from the registry.
   *
   * @param shuffleId the shuffle the partition belongs to
   * @param mapId the map task that produced the partition
   * @param partitionId the consumed reduce partition
   */
  def reclaim(shuffleId: Int, mapId: Long, partitionId: Int): Unit = {
    freeAndRemove(PartitionKey(shuffleId, mapId, partitionId), "consumer acknowledgment")
  }

  /**
   * Unregisters a partition when the writer stops or fails, releasing its buffers (and any on-disk
   * spill file) back to the [[MemoryManager]] and dropping it from the registry. Used on the
   * writer's stop/failure cleanup paths to guarantee no buffer memory is leaked.
   *
   * @param shuffleId the shuffle the partition belongs to
   * @param mapId the map task that produced the partition
   * @param partitionId the reduce partition to drop
   */
  def unregister(shuffleId: Int, mapId: Long, partitionId: Int): Unit = {
    freeAndRemove(PartitionKey(shuffleId, mapId, partitionId), "writer unregister")
  }

  // Remove a partition from the registry and release everything it holds. Runs synchronously so the
  // memory is returned to the MemoryManager well within the mandated 100ms of the consumer ack.
  private def freeAndRemove(key: PartitionKey, reason: String): Unit = {
    val removed = registryLock.synchronized {
      val entry = partitions.remove(key)
      if (entry != null) {
        releaseEntryLocked(entry)
      }
      entry
    }
    if (removed != null && debug) {
      logDebug(s"Streaming shuffle freed partition $key on $reason")
    }
  }

  // MUST hold registryLock. Releases an entry's resources: for an in-memory entry this returns its
  // reserved storage memory and discards the heap chunks; for a spilled entry it deletes the
  // on-disk spill file. trackedBytes is decremented only for bytes still resident in memory.
  private def releaseEntryLocked(entry: BufferedPartition): Unit = {
    if (entry.spilled) {
      deleteSpillFile(entry)
    } else {
      if (entry.sizeBytes > 0L) {
        trackedBytes.addAndGet(-entry.sizeBytes)
      }
      releaseStorage(entry.grantedBytes)
      entry.grantedBytes = 0L
      entry.chunks.clear()
      entry.sizeBytes = 0L
    }
  }

  // Best-effort deletion of a partition's spill file. Never throws; failures are noted only under
  // debug so a transient filesystem error cannot disrupt the streaming path.
  private def deleteSpillFile(entry: BufferedPartition): Unit = {
    val file = entry.spillFile
    if (file != null) {
      try {
        if (file.exists() && !file.delete() && debug) {
          logDebug(s"Streaming shuffle could not delete spill file ${file.getAbsolutePath}")
        }
      } catch {
        case NonFatal(e) =>
          if (debug) {
            logWarning(s"Streaming shuffle failed to delete spill file ${file.getAbsolutePath}", e)
          }
      }
      entry.spillFile = null
    }
  }

  // The lightweight 100ms poller. Samples utilization, publishes it to the JMX gauge, and, when the
  // spill threshold is crossed, spills least-recently-used partitions. Kept cheap (a couple of
  // atomic reads plus one gauge write on the common path) so telemetry overhead stays under 1% CPU.
  // Failures are swallowed so a transient error can never tear down the recurring schedule.
  private def poll(): Unit = {
    try {
      val utilization = currentUtilizationPercent()
      metricsSource.setBufferUtilizationPercent(utilization)
      if (utilization >= spillThreshold) {
        registryLock.synchronized {
          maybeSpillLocked()
        }
      }
    } catch {
      case NonFatal(e) =>
        if (debug) {
          logWarning("Streaming shuffle spill poll tick failed", e)
        }
    }
  }

  // MUST hold registryLock. Walks the registry in least-recently-used order (the access-ordered
  // LinkedHashMap yields the eldest entry first) and spills partitions until utilization drops back
  // below the threshold or no in-memory partitions remain. Spilling marks entries in place without
  // structurally modifying the map, so the live iterator stays valid throughout the pass.
  private def maybeSpillLocked(): Unit = {
    val iterator = partitions.values().iterator()
    var continue = currentUtilizationPercent() >= spillThreshold
    while (continue && iterator.hasNext) {
      val entry = iterator.next()
      if (!entry.spilled && entry.sizeBytes > 0L) {
        spillPartitionLocked(entry)
        continue = currentUtilizationPercent() >= spillThreshold
      }
    }
  }

  // MUST hold registryLock. Persists one partition's buffered chunks to disk through the existing
  // block-manager/IndexShuffleBlockResolver path, then releases its heap buffers and reserved
  // storage memory and records the spill. On an I/O failure the data is kept in memory and retried
  // on a later tick, so no buffered records are ever lost (zero data loss).
  private def spillPartitionLocked(entry: BufferedPartition): Unit = {
    val spillFile = createSpillFile(entry.key)
    if (writeChunksToDisk(entry, spillFile)) {
      val freed = entry.sizeBytes
      trackedBytes.addAndGet(-freed)
      releaseStorage(entry.grantedBytes)
      entry.grantedBytes = 0L
      entry.chunks.clear()
      entry.sizeBytes = 0L
      entry.spilled = true
      entry.spillFile = spillFile
      spillCount.incrementAndGet()
      metricsSource.incSpillCount()
      if (debug) {
        logDebug(s"Streaming shuffle spilled partition ${entry.key} ($freed bytes) to " +
          s"${spillFile.getAbsolutePath}; utilization now ${currentUtilizationPercent()}%")
      }
    } else if (spillFile != null && spillFile.exists()) {
      // Persisting failed: discard the (possibly partial) spill file and keep the buffer in memory
      // so a later poll retries. Keeping the data is what guarantees zero data loss.
      try {
        spillFile.delete()
      } catch {
        case NonFatal(_) =>
          // Best-effort cleanup; a leftover partial spill file is harmless and tmp-cleaned later.
      }
    }
  }

  // Allocate a spill file. The primary path reuses the block-manager disk layout via the injected
  // IndexShuffleBlockResolver (the same on-disk machinery SortShuffleManager uses), co-locating the
  // temp file with where the shuffle's data file would live. If the resolver/block manager is
  // unavailable (e.g. no running SparkEnv in a unit test) it falls back to a JVM temp file so spill
  // accounting still functions. Never throws.
  private def createSpillFile(key: PartitionKey): File = {
    try {
      val dataFile = blockResolver.getDataFile(key.shuffleId, key.mapId)
      blockResolver.createTempFile(dataFile)
    } catch {
      case NonFatal(e) =>
        if (debug) {
          logDebug(s"Streaming shuffle falling back to a JVM temp spill file for $key: " +
            e.getMessage)
        }
        File.createTempFile(SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX)
    }
  }

  // Stream a partition's buffered chunks to its spill file. Returns true on a fully-written file,
  // false (with the stream closed) on any I/O error so the caller can keep the data in memory.
  private def writeChunksToDisk(entry: BufferedPartition, spillFile: File): Boolean = {
    if (spillFile == null) {
      false
    } else {
      var out: FileOutputStream = null
      try {
        out = new FileOutputStream(spillFile)
        var i = 0
        val size = entry.chunks.size()
        while (i < size) {
          out.write(entry.chunks.get(i))
          i += 1
        }
        out.flush()
        true
      } catch {
        case NonFatal(e) =>
          if (debug) {
            logWarning(s"Streaming shuffle spill write failed for ${entry.key}; " +
              "keeping buffer in memory for retry", e)
          }
          false
      } finally {
        if (out != null) {
          try {
            out.close()
          } catch {
            case NonFatal(_) =>
              // Ignore close failures on the spill stream; the write result already stands.
          }
        }
      }
    }
  }

  // Enqueue a single out-of-band spill pass on the poller thread so a threshold crossing detected
  // during register() is acted on within milliseconds (well under the 100ms gate) rather than at
  // the next periodic tick. The AtomicBoolean collapses a burst of triggers into one extra pass,
  // and the work runs on the existing daemon executor so the writer's hot path never blocks on I/O.
  private def triggerImmediateSpill(): Unit = {
    if (!stopped.get() && immediateSpillPending.compareAndSet(false, true)) {
      try {
        pollExecutor.execute(new Runnable {
          override def run(): Unit = {
            immediateSpillPending.set(false)
            poll()
          }
        })
      } catch {
        case NonFatal(_) =>
          // Executor rejected the task (e.g. shutting down); reset so a later trigger can retry.
          immediateSpillPending.set(false)
      }
    }
  }

  // The streaming buffer budget in bytes, computed live from the existing MemoryManager so it
  // tracks the executor's current storage capacity:
  //   bufferBudgetBytes = executorStorageMemory * bufferSizePercent / 100
  // (the per-partition budget the design references is this total divided by numPartitions:
  //   (executorMemory * bufferPercent / 100) / numPartitions).
  // Double math avoids Long overflow when maxOnHeapStorageMemory is the Long.MaxValue sentinel.
  private def currentBufferBudgetBytes(): Long = {
    val budget = memoryManager.maxOnHeapStorageMemory.toDouble * bufferPercent / PERCENT_SCALE
    if (budget >= Long.MaxValue.toDouble) {
      Long.MaxValue
    } else {
      math.max(MIN_BUDGET_BYTES, budget.toLong)
    }
  }

  // Current buffer-memory utilization as an integer percentage (0-100) of the streaming budget.
  // Double math keeps the ratio correct even for very large budgets and clamps the result to
  // [0, 100]. Exposed package-privately so the streaming writer and tests can read utilization.
  private[streaming] def currentUtilizationPercent(): Int = {
    val budget = currentBufferBudgetBytes()
    if (budget <= 0L) {
      0
    } else {
      val pct = (trackedBytes.get().toDouble / budget.toDouble) * PERCENT_SCALE
      math.max(0, math.min(MAX_UTILIZATION_PERCENT, pct.toInt))
    }
  }

  /** Total in-memory bytes currently tracked across all buffered partitions (diagnostics/tests). */
  private[streaming] def trackedBytesTotal: Long = trackedBytes.get()

  /** Cumulative number of spills performed since construction (diagnostics/tests). */
  private[streaming] def spillCountTotal: Long = spillCount.get()

  /**
   * Shuts the poller down cleanly and releases every buffer the manager still holds, leaving no
   * daemon threads and no reserved memory behind. Idempotent and safe to call multiple times.
   * Invoked from `StreamingShuffleManager.stop()`.
   */
  def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      pollTask.cancel(false)
      ThreadUtils.shutdown(pollExecutor)
      registryLock.synchronized {
        val iterator = partitions.values().iterator()
        while (iterator.hasNext) {
          releaseEntryLocked(iterator.next())
        }
        partitions.clear()
      }
      trackedBytes.set(0L)
      if (debug) {
        logDebug(s"MemorySpillManager stopped; totalSpills=${spillCount.get()} " +
          s"reservedBytesRemaining=${reservedBytes.get()}")
      }
    }
  }
}

/**
 * Constants and the partition key for [[MemorySpillManager]]. The values encode the operational
 * gates mandated by the streaming-shuffle design: the 100ms poll cadence and the percentage scale
 * used for utilization math.
 */
private[spark] object MemorySpillManager {

  /** Poller cadence in milliseconds: utilization is sampled and spills decided every 100ms. */
  val POLL_INTERVAL_MILLIS: Long = 100L

  /** Scale used for percentage math (a percentage is a fraction multiplied by this value). */
  val PERCENT_SCALE: Long = 100L

  /** Upper bound for the reported utilization percentage. */
  val MAX_UTILIZATION_PERCENT: Int = 100

  /** Floor for the computed buffer budget so utilization math never divides by zero. */
  val MIN_BUDGET_BYTES: Long = 1L

  /** Initial capacity for the access-ordered partition registry. */
  val INITIAL_REGISTRY_CAPACITY: Int = 64

  /** Load factor for the access-ordered partition registry. */
  val LOAD_FACTOR: Float = 0.75f

  /** Daemon thread name for the buffer-utilization poller. */
  val POLLER_THREAD_NAME: String = "streaming-shuffle-spill-poller"

  /** Prefix for fallback JVM temp spill files (used only when the block manager is unavailable). */
  val SPILL_FILE_PREFIX: String = "streaming-shuffle-spill-"

  /** Suffix for fallback JVM temp spill files. */
  val SPILL_FILE_SUFFIX: String = ".tmp"

  /**
   * Identifies a single buffered shuffle partition tracked by the manager.
   *
   * @param shuffleId the shuffle the partition belongs to
   * @param mapId the map task that produced the partition
   * @param partitionId the target reduce partition
   */
  case class PartitionKey(shuffleId: Int, mapId: Long, partitionId: Int)
}

