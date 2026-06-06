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
import java.nio.file.Files
import java.util.{LinkedHashMap, UUID}
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage.TempLocalBlockId
import org.apache.spark.util.ThreadUtils

/**
 * Memory-spill coordinator for the opt-in streaming shuffle engine. A single instance is created
 * per executor by `StreamingShuffleManager` (only when the streaming path is active for a shuffle)
 * and is shared by the streaming writer pipeline and the [[StreamingBlockExchange]]. It bounds the
 * heap footprint of the per-partition streaming buffers so a fast producer cannot exhaust executor
 * memory, while keeping every buffered block individually addressable and READABLE so the exchange
 * can serve it (and retransmit it) to consumers. It delivers the four capabilities required by the
 * streaming design:
 *
 *  1. A lightweight poller that samples buffer-memory utilization every
 *     [[MemorySpillManager.POLL_INTERVAL_MILLIS]] (100ms) and publishes it to JMX through the
 *     shared [[StreamingShuffleSource]] `bufferUtilizationPercent` gauge.
 *  2. Spill-to-disk: when utilization crosses the configurable spill threshold (default 80%,
 *     `spark.shuffle.streaming.spillThreshold`), the LARGEST buffered blocks (selected among the
 *     least-recently-used candidates) are written to disk and their heap buffers released. Spilling
 *     the largest blocks reclaims the most memory per spill, and breaking ties by LRU evicts the
 *     best candidates first. A spilled block stays READABLE through [[read]]: its on-disk
 *     file is retained (never orphaned) so the exchange can still serve and retransmit it,
 *     ensuring zero data loss. A threshold crossing observed while registering a block schedules
 *     an out-of-band spill pass so the threshold-cross to spill-action latency stays under 100ms.
 *  3. Prompt reclamation: [[reclaim]] (consumer acknowledgment) and [[unregister]] /
 *     [[unregisterMap]] (writer stop/failure) return the corresponding buffer memory to the
 *     `MemoryManager` and delete any on-disk spill file synchronously, within the mandated 100ms of
 *     the ack, so there are no memory or disk leaks over a long-running job.
 *  4. Telemetry: every spill increments the `spillCount` counter on the shared
 *     [[StreamingShuffleSource]], exposed over JMX through the existing `MetricRegistry`.
 *
 * Per-block addressing: each emitted streaming block is stored as ONE immutable entry keyed by
 * [[MemorySpillManager.BlockKey]] `(shuffleId, mapId, partitionId, seq)`. Because blocks are never
 * appended to in place, a previously spilled block can never be silently overwritten or have its
 * on-disk reference dropped -- the data-loss class of bug that aggregating per-partition buffering
 * is prone to. Each block is independently readable, reclaimable, and retransmittable by key.
 *
 * Memory-model reuse: buffer accounting goes exclusively through the EXISTING
 * [[org.apache.spark.memory.MemoryManager]] interface (`acquireStorageMemory` /
 * `releaseStorageMemory`); this class introduces no new memory model and never redesigns the
 * executor memory split. The streaming buffer budget is derived from the manager's current on-heap
 * storage capacity:
 *   bufferBudgetBytes = executorStorageMemory * spark.shuffle.streaming.bufferSizePercent / 100
 * and the per-partition budget the design references is this total divided by the partition count:
 *   (executorMemory * bufferPercent / 100) / numPartitions.
 *
 * Disk reuse: spilled blocks are written through the SAME on-disk machinery the default
 * `SortShuffleManager` uses, via the injected [[IndexShuffleBlockResolver]] (the manager's single
 * resolver). No new block-manager storage contract is introduced. If the block manager is
 * unavailable (e.g. no running `SparkEnv` in a unit test), spilling falls back to a JVM temp
 * file so accounting and read-back still function without ever throwing.
 *
 * Lock discipline (no disk I/O under the registry lock): spill writes, spill-file reads, and
 * spill-file deletions ALWAYS happen OUTSIDE the `registryLock`. The spill pass snapshots its
 * candidates under the lock, writes them to disk unlocked, then re-acquires the lock to
 * publish the state change atomically; [[reclaim]] / [[unregister]] / [[stop]] likewise collect
 * files under the lock and delete them outside it. This keeps `reclaim` (the consumer-ack hot path)
 * and `register` from ever blocking on a slow disk write, preserving the <100ms reclamation target.
 *
 * Coexistence strategy: this manager is instantiated and active ONLY on the streaming path. The
 * default `SortShuffleManager` fallback never constructs it, so the sort-based shuffle is wholly
 * unaffected and carries zero spill-coordination overhead. Being `private[spark]` and confined to
 * the streaming package enforces the zero-cross-contamination rule: existing components neither
 * import nor depend on this class.
 *
 * Thread-safety: the block registry is guarded by a single private monitor; the poller runs on a
 * dedicated daemon scheduler and, on the common (below-threshold) path, performs only a couple of
 * atomic reads plus one gauge write, holding telemetry overhead well under the mandated 1% CPU.
 * Verbose logging is gated behind `spark.shuffle.streaming.debug` to keep per-executor log volume
 * under the mandated 10MB/hour budget, and never logs absolute spill-file paths.
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

  // Monitor guarding every access to the block registry below. Disk I/O is NEVER performed while
  // holding this monitor (see the class-level lock-discipline note).
  private val registryLock = new Object()

  // Access-ordered registry of currently buffered blocks keyed by (shuffleId, mapId, partitionId,
  // seq). accessOrder=true makes a get/put refresh an entry's recency, so iteration yields the
  // least-recently-used entry first -- the LRU tie-break for spill selection. Guarded by
  // registryLock.
  private val registry =
    new LinkedHashMap[BlockKey, BufferedBlock](INITIAL_REGISTRY_CAPACITY, LOAD_FACTOR, true)

  // Total in-memory logical bytes currently buffered across all blocks. This is the utilization
  // numerator and is tracked independently of grant success so the poller still sees pressure (and
  // spills) even when a storage-memory grant was declined.
  private val trackedBytes = new AtomicLong(0L)

  // Total storage memory actually reserved from the MemoryManager. Released exactly on
  // free/spill/ack so the storage pool stays balanced and there are zero leaks.
  private val reservedBytes = new AtomicLong(0L)

  // Cumulative number of spills performed (diagnostics; the authoritative JMX counter is the
  // spillCount gauge on metricsSource).
  private val spillCount = new AtomicLong(0L)

  // Set once stop() has run; makes shutdown idempotent and short-circuits register/spill after.
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
   * Per-block bookkeeping entry. A block is registered once with its immutable bytes; while it is
   * resident it holds those bytes and the memory granted for them, and once spilled it holds
   * only the on-disk spill file (its bytes are freed but stay READABLE from disk). Mutated only
   * while holding `registryLock`, except the immutable `bytes` snapshot may be read outside the
   * lock by a concurrent spill write or [[read]].
   */
  private final class BufferedBlock(
      val key: BlockKey,
      var bytes: Array[Byte],
      val sizeBytes: Long,
      var grantedBytes: Long) {
    // Wall-clock nanos of the last register/read; the LRU tie-break for spill selection.
    var lastAccessNanos: Long = System.nanoTime()
    // True once the block's bytes have been written to disk and its heap buffer released.
    var spilled: Boolean = false
    // The on-disk spill file once spilled; null while resident in memory.
    var spillFile: File = _
    // Transient marker set while this block is being written to disk OUTSIDE the lock, so a
    // concurrent spill pass does not re-select it. Cleared when the spill is published or aborted.
    var spilling: Boolean = false
  }

  // Snapshot of one block to spill, captured under the lock and written to disk without
  // it. Holds the immutable bytes reference so the disk write needs no further locking.
  private final class SpillCandidate(val key: BlockKey, val bytes: Array[Byte])

  /**
   * Registers the in-memory buffer for a single streaming block and accounts its footprint against
   * the existing [[MemoryManager]]. The byte array is retained (and owned) by the manager so it can
   * be spilled to disk under memory pressure and read back to serve/retransmit it; callers must not
   * mutate it after registering. Each [[BlockKey]] is registered at most once.
   *
   * If the storage-memory grant is declined, or registering pushes utilization across the spill
   * threshold, an out-of-band spill pass is scheduled so the response stays well under 100ms rather
   * than waiting for the next poll tick. Note that the block is tracked and retained even when the
   * grant is declined; a `false` return is purely a backpressure signal to the writer, never a
   * "not stored" signal.
   *
   * @param key the unique block key `(shuffleId, mapId, partitionId, seq)`
   * @param bytes the buffered bytes to track (ownership transfers to the manager); empty/null is a
   *              no-op
   * @return true if storage memory was granted, false if the writer should back off (a
   *         spill has been scheduled), the manager has stopped, or the key was already registered
   */
  def register(key: BlockKey, bytes: Array[Byte]): Boolean = {
    if (bytes == null || bytes.length == 0) {
      true
    } else if (stopped.get()) {
      // Reject new entries once shutdown has begun so no bytes are accounted after cleanup.
      false
    } else {
      val numBytes = bytes.length.toLong
      // Acquire storage memory OUTSIDE the registry lock (no disk I/O, but keep the lock section
      // minimal); the result is reconciled under the lock to close the register/stop race.
      val granted = acquireStorage(numBytes)
      var rejected = false
      registryLock.synchronized {
        if (stopped.get()) {
          // Race fix: stop() ran between the acquire above and here. Do not insert; the acquired
          // memory is released below so nothing leaks after cleanup.
          rejected = true
        } else if (registry.containsKey(key)) {
          // Defensive: block keys are unique (monotonic seq per partition). A duplicate must not
          // overwrite the existing entry (which would orphan its bytes/spill file), so reject it.
          rejected = true
        } else {
          val entry = new BufferedBlock(key, bytes, numBytes, if (granted) numBytes else 0L)
          registry.put(key, entry)
          trackedBytes.addAndGet(numBytes)
        }
      }
      if (rejected) {
        if (granted) {
          releaseStorage(numBytes)
        }
        false
      } else {
        if (!granted || currentUtilizationPercent() >= spillThreshold) {
          triggerImmediateSpill()
        }
        granted
      }
    }
  }

  /**
   * Reads the bytes of a previously registered block, whether it is still resident in memory or has
   * been spilled to disk. This is the read path the [[StreamingBlockExchange]] uses to deliver and
   * retransmit blocks to consumers, so a spilled block is never lost or unreadable.
   *
   * @param key the block key to read
   * @return the block bytes if known (read from memory or its spill file), or None if the key is
   *         unknown or the spill file could not be read
   */
  def read(key: BlockKey): Option[Array[Byte]] = {
    var inMemory: Array[Byte] = null
    var spillFile: File = null
    registryLock.synchronized {
      val entry = registry.get(key)
      if (entry != null) {
        entry.lastAccessNanos = System.nanoTime()
        if (entry.spilled) {
          spillFile = entry.spillFile
        } else {
          inMemory = entry.bytes
        }
      }
    }
    if (inMemory != null) {
      // The bytes array is immutable once registered, so returning the reference is safe.
      Some(inMemory)
    } else if (spillFile != null) {
      // Disk read performed OUTSIDE the registry lock.
      readSpillFile(spillFile)
    } else {
      None
    }
  }

  /**
   * Reclaims a single block after the consumer acknowledges consuming it. Releases the block's
   * buffer memory back to the [[MemoryManager]] (and deletes any on-disk spill file) synchronously,
   * within the mandated 100ms of the ack, and drops it from the registry.
   *
   * @param key the acknowledged block key
   */
  def reclaim(key: BlockKey): Unit = freeAndRemove(key, "consumer acknowledgment")

  /**
   * Unregisters a single block on writer failure, releasing its buffer (and any spill file) back
   * to the [[MemoryManager]] and dropping it from the registry.
   *
   * @param key the block key to drop
   */
  def unregister(key: BlockKey): Unit = freeAndRemove(key, "writer unregister")

  /**
   * Unregisters every block produced by a given map task. Used on the writer's failure-cleanup path
   * to guarantee no buffer memory or spill file is leaked when a producer aborts.
   *
   * @param shuffleId the shuffle the map belongs to
   * @param mapId the map task whose blocks should be dropped
   */
  def unregisterMap(shuffleId: Int, mapId: Long): Unit = {
    val files = new ArrayBuffer[File]()
    registryLock.synchronized {
      val iterator = registry.entrySet().iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
        val k = entry.getKey
        if (k.shuffleId == shuffleId && k.mapId == mapId) {
          val file = releaseEntryLocked(entry.getValue)
          if (file != null) {
            files += file
          }
          iterator.remove()
        }
      }
    }
    deleteFilesQuietly(files)
  }

  /**
   * Unregisters EVERY block belonging to a given shuffle, across all of its map tasks, releasing
   * each block's buffer memory (and any spill file) back to the [[MemoryManager]] and dropping it
   * from the registry. This is the shuffle-level cleanup path invoked from
   * `StreamingShuffleManager.unregisterShuffle`: unlike sort-based shuffle (whose per-map on-disk
   * data is removed by the composed `SortShuffleManager`'s `IndexShuffleBlockResolver`), streaming
   * blocks that a consumer has not yet acknowledged live in this manager's buffers/spill files, so
   * they MUST be reclaimed here or they leak when a shuffle is unregistered before every consumer
   * acknowledges its blocks. Mirrors [[unregisterMap]] but matches on the shuffle id alone.
   *
   * @param shuffleId the shuffle whose buffered/spilled streaming blocks should all be dropped
   */
  def unregisterShuffle(shuffleId: Int): Unit = {
    val files = new ArrayBuffer[File]()
    registryLock.synchronized {
      val iterator = registry.entrySet().iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
        if (entry.getKey.shuffleId == shuffleId) {
          val file = releaseEntryLocked(entry.getValue)
          if (file != null) {
            files += file
          }
          iterator.remove()
        }
      }
    }
    // Disk deletions performed OUTSIDE the lock so a slow filesystem never stalls the registry.
    deleteFilesQuietly(files)
    if (debug) {
      logDebug(s"Streaming shuffle freed all buffered blocks for shuffle $shuffleId on unregister")
    }
  }

  // Remove a block from the registry and release everything it holds. The spill file (if any) is
  // deleted OUTSIDE the lock so a slow filesystem cannot block the consumer-ack reclamation path.
  private def freeAndRemove(key: BlockKey, reason: String): Unit = {
    var file: File = null
    val removed = registryLock.synchronized {
      val entry = registry.remove(key)
      if (entry != null) {
        file = releaseEntryLocked(entry)
      }
      entry
    }
    if (file != null) {
      deleteFileQuietly(file)
    }
    if (removed != null && debug) {
      logDebug(s"Streaming shuffle freed block $key on $reason")
    }
  }

  // MUST hold registryLock. Releases an entry's in-memory resources and returns its on-disk spill
  // file (or null) for the caller to delete OUTSIDE the lock. For a resident entry this returns its
  // reserved storage memory and drops the heap bytes; for a spilled entry it surrenders the spill
  // file. trackedBytes is decremented only for bytes still resident in memory.
  private def releaseEntryLocked(entry: BufferedBlock): File = {
    if (entry.spilled) {
      val file = entry.spillFile
      entry.spillFile = null
      entry.spilling = false
      file
    } else {
      if (entry.sizeBytes > 0L) {
        trackedBytes.addAndGet(-entry.sizeBytes)
      }
      releaseStorage(entry.grantedBytes)
      entry.grantedBytes = 0L
      entry.bytes = null
      entry.spilling = false
      null
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

  // The lightweight 100ms poller. Samples utilization, publishes it to the JMX gauge, and, when the
  // spill threshold is crossed, runs a spill pass. Kept cheap (a couple of atomic reads plus one
  // gauge write on common path) so telemetry overhead stays under 1% CPU. Failures are swallowed
  // so a transient error can never tear down the recurring schedule.
  private def poll(): Unit = {
    try {
      val utilization = currentUtilizationPercent()
      metricsSource.setBufferUtilizationPercent(utilization)
      if (utilization >= spillThreshold) {
        maybeSpill()
      }
    } catch {
      case NonFatal(e) =>
        if (debug) {
          logWarning("Streaming shuffle spill poll tick failed", e)
        }
    }
  }

  /**
   * Runs one spill pass: relieve memory pressure by spilling the LARGEST resident blocks (with LRU
   * as the tie-break) to disk until utilization falls back below the threshold. Disk writes happen
   * OUTSIDE the registry lock: candidates are snapshotted/marked under the lock, written without
   * it, then the resulting state change is published atomically under the lock. Package-private so
   * the writer's immediate-spill trigger and the test suite can drive a deterministic pass.
   */
  private[streaming] def maybeSpill(): Unit = {
    // 1) Select and snapshot candidates under the lock (no disk I/O here).
    val candidates = new ArrayBuffer[SpillCandidate]()
    registryLock.synchronized {
      val budget = currentBufferBudgetBytes()
      val thresholdBytes = (budget.toDouble * spillThreshold / PERCENT_SCALE).toLong
      val overThreshold = currentUtilizationPercent() >= spillThreshold
      // Gather every spillable resident block, noting whether any is holding heap the MemoryManager
      // declined to grant (a memory-pressure signal independent of the streaming-slice percentage).
      val eligible = new ArrayBuffer[BufferedBlock]()
      var hasUngranted = false
      val iterator = registry.values().iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
        if (!entry.spilled && !entry.spilling && entry.sizeBytes > 0L) {
          eligible += entry
          if (entry.grantedBytes == 0L) {
            hasUngranted = true
          }
        }
      }
      if (overThreshold || hasUngranted) {
        // Spill the LARGEST blocks first (most memory reclaimed per spill); break ties by LRU
        // (eldest last-access first). This satisfies the "spill largest buffered partitions, LRU"
        // policy rather than evicting many small old blocks while a large recent block is retained.
        val ordered = eligible.sortWith { (a, b) =>
          if (a.sizeBytes != b.sizeBytes) a.sizeBytes > b.sizeBytes
          else a.lastAccessNanos < b.lastAccessNanos
        }
        // Iterate every candidate (no early exit): spill a block when it is still needed to drop
        // back under the threshold (largest-first, so `projected` falls fastest) OR when it holds
        // ungranted heap, which must be freed regardless of the streaming-slice percentage.
        var projected = trackedBytes.get()
        var i = 0
        while (i < ordered.length) {
          val entry = ordered(i)
          if (projected > thresholdBytes || entry.grantedBytes == 0L) {
            entry.spilling = true
            candidates += new SpillCandidate(entry.key, entry.bytes)
            projected -= entry.sizeBytes
          }
          i += 1
        }
      }
    }
    if (candidates.isEmpty) {
      return
    }
    // 2) Write each candidate to disk OUTSIDE the lock.
    val written = new ArrayBuffer[(SpillCandidate, File, Boolean)]()
    var j = 0
    while (j < candidates.length) {
      val candidate = candidates(j)
      val file = createSpillFile(candidate.key)
      val ok = file != null && writeBytesToDisk(candidate.bytes, file)
      written += ((candidate, file, ok))
      j += 1
    }
    // 3) Publish the results under the lock; delete any orphaned/partial files OUTSIDE the lock.
    val filesToDelete = new ArrayBuffer[File]()
    registryLock.synchronized {
      var k = 0
      while (k < written.length) {
        val (candidate, file, ok) = written(k)
        val entry = registry.get(candidate.key)
        if (entry == null) {
          // The block was reclaimed/unregistered concurrently while we wrote it -- the consumer
          // already has it, so the new spill file is now an orphan and is simply discarded. No data
          // is lost (it was acknowledged) and no memory leaks (reclaim already released it).
          if (file != null) {
            filesToDelete += file
          }
        } else if (ok) {
          // Commit the spill: release the heap bytes and storage memory, RETAIN the readable file.
          if (entry.sizeBytes > 0L) {
            trackedBytes.addAndGet(-entry.sizeBytes)
          }
          releaseStorage(entry.grantedBytes)
          entry.grantedBytes = 0L
          entry.bytes = null
          entry.spilled = true
          entry.spillFile = file
          entry.spilling = false
          spillCount.incrementAndGet()
          metricsSource.incSpillCount()
          if (debug) {
            logDebug(s"Streaming shuffle spilled block ${entry.key} (${entry.sizeBytes} bytes); " +
              s"utilization now ${currentUtilizationPercent()}%")
          }
        } else {
          // The write failed: keep the block resident for a later retry (this is what guarantees
          // zero data loss) and discard the partial file.
          entry.spilling = false
          if (file != null) {
            filesToDelete += file
          }
        }
        k += 1
      }
    }
    deleteFilesQuietly(filesToDelete)
  }

  // Allocate a spill file. The primary path reuses the block-manager disk layout via the injected
  // IndexShuffleBlockResolver (the same on-disk machinery SortShuffleManager uses), co-locating the
  // temp file with where the shuffle's data file would live. If the resolver/block manager is
  // unavailable (e.g. no running SparkEnv in a unit test) it falls back to a JVM temp file so spill
  // accounting and read-back still function. Never throws.
  private def createSpillFile(key: BlockKey): File = {
    try {
      val dataFile = blockResolver.getDataFile(key.shuffleId, key.mapId)
      blockResolver.createTempFile(dataFile)
    } catch {
      case NonFatal(e) =>
        if (debug) {
          logDebug(s"Streaming shuffle falling back to a JVM temp spill file for block $key: " +
            e.getMessage)
        }
        File.createTempFile(SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX)
    }
  }

  // Write one block's bytes to its spill file. Returns true on a full file, false (with the
  // stream closed) on any I/O error so the caller can keep the data in memory. Never throws.
  private def writeBytesToDisk(bytes: Array[Byte], spillFile: File): Boolean = {
    if (spillFile == null || bytes == null) {
      false
    } else {
      var out: FileOutputStream = null
      try {
        out = new FileOutputStream(spillFile)
        out.write(bytes)
        out.flush()
        true
      } catch {
        case NonFatal(e) =>
          if (debug) {
            logWarning(s"Streaming shuffle spill write failed for ${spillFile.getName}; " +
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

  // Read a spilled block's bytes back from disk. Returns None (rather than throwing) on any I/O
  // error so the exchange can treat an unreadable spill as a fetch failure and recompute upstream.
  private def readSpillFile(spillFile: File): Option[Array[Byte]] = {
    try {
      Some(Files.readAllBytes(spillFile.toPath))
    } catch {
      case NonFatal(e) =>
        if (debug) {
          logWarning(s"Streaming shuffle spill read failed for ${spillFile.getName}", e)
        }
        None
    }
  }

  // Best-effort deletion of a single spill file. Never throws; failures are noted only under debug
  // (by file name, never absolute path) so a transient filesystem error cannot disrupt streaming.
  private def deleteFileQuietly(file: File): Unit = {
    try {
      if (file.exists() && !file.delete() && debug) {
        logDebug(s"Streaming shuffle could not delete spill file ${file.getName}")
      }
    } catch {
      case NonFatal(e) =>
        if (debug) {
          logWarning(s"Streaming shuffle failed to delete spill file ${file.getName}", e)
        }
    }
  }

  // Best-effort deletion of a batch of spill files (used off-lock by spill/stop/unregister).
  private def deleteFilesQuietly(files: ArrayBuffer[File]): Unit = {
    var i = 0
    while (i < files.length) {
      deleteFileQuietly(files(i))
      i += 1
    }
  }

  // Enqueue a single out-of-band spill pass on the poller thread so a threshold crossing detected
  // during register() is acted on within milliseconds (under the 100ms gate) rather than at the
  // next periodic tick. The AtomicBoolean collapses many triggers into one extra pass, and the
  // work runs on the existing daemon executor so the writer's hot path never blocks on disk I/O.
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

  // The streaming buffer budget in bytes, computed from the existing MemoryManager so it tracks
  // the executor's current storage capacity:
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

  /** Total in-memory bytes currently tracked across all buffered blocks (diagnostics/tests). */
  private[streaming] def trackedBytesTotal: Long = trackedBytes.get()

  /** Total storage memory still reserved from the MemoryManager (diagnostics/leak tests). */
  private[streaming] def reservedBytesTotal: Long = reservedBytes.get()

  /** Cumulative number of spills performed since construction (diagnostics/tests). */
  private[streaming] def spillCountTotal: Long = spillCount.get()

  /**
   * Shuts the poller down cleanly and releases every block the manager still holds, leaving no
   * daemon threads, no reserved memory, and no spill files behind. Idempotent and safe to call
   * multiple times. Invoked from `StreamingShuffleManager.stop()`.
   */
  def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      pollTask.cancel(false)
      ThreadUtils.shutdown(pollExecutor)
      val files = new ArrayBuffer[File]()
      registryLock.synchronized {
        val iterator = registry.values().iterator()
        while (iterator.hasNext) {
          val file = releaseEntryLocked(iterator.next())
          if (file != null) {
            files += file
          }
        }
        registry.clear()
      }
      // Disk deletions performed OUTSIDE the lock.
      deleteFilesQuietly(files)
      trackedBytes.set(0L)
      if (debug) {
        logDebug(s"MemorySpillManager stopped; totalSpills=${spillCount.get()} " +
          s"reservedBytesRemaining=${reservedBytes.get()}")
      }
    }
  }
}

/**
 * Constants and the per-block key for [[MemorySpillManager]]. The values encode the operational
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

  /** Initial capacity for the access-ordered block registry. */
  val INITIAL_REGISTRY_CAPACITY: Int = 64

  /** Load factor for the access-ordered block registry. */
  val LOAD_FACTOR: Float = 0.75f

  /** Daemon thread name for the buffer-utilization poller. */
  val POLLER_THREAD_NAME: String = "streaming-shuffle-spill-poller"

  /** Prefix for fallback JVM temp spill files (used only when the block manager is unavailable). */
  val SPILL_FILE_PREFIX: String = "streaming-shuffle-spill-"

  /** Suffix for fallback JVM temp spill files. */
  val SPILL_FILE_SUFFIX: String = ".tmp"

  /**
   * Identifies a single streaming block tracked by the manager. Including the per-partition
   * block sequence number makes every emitted block individually addressable, so blocks are stored
   * immutably (never appended to) and can be read back, reclaimed, and retransmitted by key.
   *
   * @param shuffleId the shuffle the block belongs to
   * @param mapId the map task that produced the block
   * @param partitionId the target reduce partition
   * @param seq the monotonic per-(shuffle, map, partition) block sequence number
   */
  case class BlockKey(shuffleId: Int, mapId: Long, partitionId: Int, seq: Long)
}
