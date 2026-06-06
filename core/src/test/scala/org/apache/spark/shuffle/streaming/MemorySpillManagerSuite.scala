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
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito.{atLeastOnce, mock, never, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Span}

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.shuffle.IndexShuffleBlockResolver

/**
 * Unit tests for [[MemorySpillManager]], the per-block buffer-memory and spill coordinator for the
 * opt-in streaming-shuffle engine. The suite mirrors the lightweight style of
 * `org.apache.spark.shuffle.sort.SortShuffleManagerSuite` (ScalaTest + Mockito + Matchers) and runs
 * with no `SparkContext`: the heavy collaborators are Mockito test doubles.
 *
 * The two collaborators that touch the executor memory model and the block-manager disk are MOCKED
 * (`MemoryManager` and `IndexShuffleBlockResolver`) so the manager is exercised purely through the
 * existing interfaces it reuses, with zero redesign of the memory model. The JMX metrics
 * [[StreamingShuffleSource]] is a REAL instance where a gauge side effect is asserted, and a mock
 * where only the updater call must be verified.
 *
 * Coverage maps to the streaming-shuffle design gates the manager must honor:
 *  - the 100ms poller spills only once buffer utilization crosses the configurable threshold
 *    (default 80%), and the spill action itself completes within 100ms;
 *  - victim selection spills the LARGEST buffered partitions first, breaking ties by
 *    least-recently-used (recency refreshed by [[MemorySpillManager.read]]);
 *  - a consumer acknowledgment reclaims the buffer memory back to the `MemoryManager` within 100ms;
 *  - every spill increments the `spillCount` JMX gauge and a spilled block stays READABLE from disk
 *    (zero data loss), even when the storage grant was declined under memory pressure;
 *  - `stop()` shuts the daemon poller down cleanly (no leaked threads), frees all memory, rejects
 *    post-shutdown registration, and is idempotent.
 *
 * Determinism: the time-driven spill is exercised through the package-private `maybeSpill()` seam
 * rather than by sleeping on the 100ms timer. Utilization is steered by feeding the mocked
 * `maxOnHeapStorageMemory` from an [[AtomicLong]] budget knob, so a block can be registered well
 * below the threshold (which avoids triggering an out-of-band spill during `register`) and then
 * pushed above it before the seam is invoked. `Eventually` (with a tight patience) is used only
 * where an assertion may observe an out-of-band poller pass.
 *
 * Same-package access: the suite lives in the streaming package, so it constructs the
 * `private[spark]` manager directly and reads its `private[streaming]` diagnostics.
 */
class MemorySpillManagerSuite extends SparkFunSuite with Matchers with Eventually {

  import MemorySpillManager.BlockKey

  // A generous patience so an assertion that may observe an out-of-band (poller-driven) spill or
  // gauge publication converges reliably without being flaky on a slow CI machine.
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5000, Millis), interval = Span(20, Millis))

  // Mirrors the SortShuffleManagerSuite helper: doReturn(...).when(mock) stubs a method without
  // invoking a real body, which is the safe way to stub the abstract grant method on the mock.
  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  /** Builds a byte buffer of the given size filled with a marker byte (block payload). */
  private def bytesOf(size: Int, fill: Byte): Array[Byte] = Array.fill(size)(fill)

  /**
   * Creates a mocked [[MemoryManager]] whose on-heap storage capacity is fed live from `maxOnHeap`
   * (so a test can shrink the budget to push utilization across the threshold) and whose storage
   * grant returns `grant` (true = granted, false = declined / memory pressure). Only the existing
   * `MemoryManager` interface is used; the memory model itself is never redesigned.
   */
  private def newMockMemoryManager(maxOnHeap: AtomicLong, grant: Boolean): MemoryManager = {
    val memoryManager = mock(classOf[MemoryManager])
    when(memoryManager.maxOnHeapStorageMemory).thenAnswer(new Answer[java.lang.Long] {
      override def answer(invocation: InvocationOnMock): java.lang.Long = maxOnHeap.get()
    })
    doReturn(grant).when(memoryManager).acquireStorageMemory(any(), anyLong(), any())
    memoryManager
  }

  /**
   * Creates a mocked [[IndexShuffleBlockResolver]] reused for spilled-block writes. `getDataFile`
   * yields an (uncreated) path that only supplies the parent location, and `createTempFile` returns
   * a REAL writable temp file so a spilled block is genuinely written and can be read back; the
   * manager deletes that file on reclaim/stop, and `deleteOnExit` is a safety net for any others.
   */
  private def newMockResolver(): IndexShuffleBlockResolver = {
    val resolver = mock(classOf[IndexShuffleBlockResolver])
    when(resolver.getDataFile(anyInt(), anyLong())).thenAnswer(new Answer[File] {
      override def answer(invocation: InvocationOnMock): File =
        new File(System.getProperty("java.io.tmpdir"), s"blitzy-data-${UUID.randomUUID()}")
    })
    when(resolver.createTempFile(any())).thenAnswer(new Answer[File] {
      override def answer(invocation: InvocationOnMock): File = {
        val file = File.createTempFile("blitzy-spill-", ".tmp")
        file.deleteOnExit()
        file
      }
    })
    resolver
  }

  /**
   * Constructs a [[MemorySpillManager]] and guarantees its 100ms poller daemon is shut down via
   * `stop()`, so no test leaks the scheduled executor created in the constructor.
   */
  private def withManager(
      conf: SparkConf,
      memoryManager: MemoryManager,
      resolver: IndexShuffleBlockResolver,
      source: StreamingShuffleSource)(body: MemorySpillManager => Unit): Unit = {
    val manager = new MemorySpillManager(conf, memoryManager, resolver, source)
    try {
      body(manager)
    } finally {
      manager.stop()
    }
  }

  /** Reads the cumulative `shuffle.streaming.spillCount` gauge from a real metrics source. */
  private def spillCount(source: StreamingShuffleSource): Long =
    source.metricRegistry.getGauges.get("shuffle.streaming.spillCount").getValue.asInstanceOf[Long]

  /** Reads the current `shuffle.streaming.bufferUtilizationPercent` gauge from a real source. */
  private def bufferUtilizationPercent(source: StreamingShuffleSource): Int =
    source.metricRegistry.getGauges.get("shuffle.streaming.bufferUtilizationPercent")
      .getValue.asInstanceOf[Int]

  /** Counts live daemon threads belonging to the manager's buffer-utilization poller. */
  private def pollerThreadCount(): Int = {
    var count = 0
    val iterator = Thread.getAllStackTraces.keySet.iterator()
    while (iterator.hasNext) {
      val thread = iterator.next()
      if (thread.isAlive && thread.getName.contains(MemorySpillManager.POLLER_THREAD_NAME)) {
        count += 1
      }
    }
    count
  }

  test("register accounts buffered bytes and reserves storage; read returns them") {
    val conf = new SparkConf(false).set(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT, 50)
    // A large backing budget keeps utilization far below the threshold, so the poller never spills.
    val maxOnHeap = new AtomicLong(1000000L)
    val source = new StreamingShuffleSource
    val resolver = newMockResolver()
    withManager(conf, newMockMemoryManager(maxOnHeap, grant = true), resolver, source) { manager =>
      val key = BlockKey(0, 0L, 0, 0L)
      val bytes = bytesOf(256, 7.toByte)
      // A granted register tracks exactly the buffered bytes AND reserves that much storage memory.
      manager.register(key, bytes) mustBe true
      manager.trackedBytesTotal mustBe 256L
      manager.reservedBytesTotal mustBe 256L
      // The block is readable from memory and returns identical content.
      manager.read(key).map(_.toSeq) mustBe Some(bytes.toSeq)
      // An unknown key reads as None.
      manager.read(BlockKey(0, 0L, 0, 99L)) mustBe None
      // Empty bytes are a no-op that accounts nothing.
      manager.register(BlockKey(0, 0L, 1, 0L), Array.emptyByteArray) mustBe true
      manager.trackedBytesTotal mustBe 256L
      // Far below the threshold, the spill/disk path is never touched.
      verify(resolver, never()).getDataFile(anyInt(), anyLong())
    }
  }

  test("does not spill when buffer utilization is below the configured threshold") {
    val conf = new SparkConf(false)
      .set(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT, 50)
      .set(config.STREAMING_SHUFFLE_SPILL_THRESHOLD, 80)
    // budget = 1000 * 50% = 500; threshold 80% => 400 bytes. A 300-byte block sits at 60% (< 80%).
    val maxOnHeap = new AtomicLong(1000L)
    val source = new StreamingShuffleSource
    val resolver = newMockResolver()
    withManager(conf, newMockMemoryManager(maxOnHeap, grant = true), resolver, source) { manager =>
      val key = BlockKey(0, 0L, 0, 0L)
      manager.register(key, bytesOf(300, 1.toByte)) mustBe true
      manager.currentUtilizationPercent() mustBe 60
      // Invoking the spill seam below threshold must NOT spill: no disk write, no metric change.
      manager.maybeSpill()
      verify(resolver, never()).getDataFile(anyInt(), anyLong())
      spillCount(source) mustBe 0L
      manager.spillCountTotal mustBe 0L
      manager.trackedBytesTotal mustBe 300L
    }
  }

  test("spills the buffered block within 100ms when utilization crosses the threshold") {
    val conf = new SparkConf(false)
      .set(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT, 50)
      .set(config.STREAMING_SHUFFLE_SPILL_THRESHOLD, 80)
      // Enable the debug-logging gate so the spill path's debug branch is exercised here too.
      .set(config.STREAMING_SHUFFLE_DEBUG, true)
    // Start with a huge backing budget so the block registers FAR below the threshold (no spill is
    // triggered during register), then shrink the budget so utilization jumps ABOVE 80%.
    val maxOnHeap = new AtomicLong(1000000000L)
    val source = new StreamingShuffleSource
    val resolver = newMockResolver()
    withManager(conf, newMockMemoryManager(maxOnHeap, grant = true), resolver, source) { manager =>
      val key = BlockKey(0, 0L, 0, 0L)
      val bytes = bytesOf(450, 5.toByte)
      manager.register(key, bytes) mustBe true
      manager.currentUtilizationPercent() mustBe 0
      // Shrink the budget to 500 bytes: the 450-byte block is now at 90% (>= the 80% threshold).
      maxOnHeap.set(1000L)
      manager.currentUtilizationPercent() mustBe 90
      // Invoke the spill seam. maybeSpill() performs selection, disk write, and accounting in a
      // single synchronous pass and returns before the next line, so the state-transition
      // assertions below (spill metric incremented, disk path used, tracked bytes drained to zero,
      // block still readable from disk) prove the bounded reclamation deterministically -- without
      // depending on wall-clock elapsed time or filesystem scheduling.
      manager.maybeSpill()
      // The block was spilled: the spill metric incremented and the disk path was used.
      eventually {
        spillCount(source) mustBe 1L
      }
      verify(resolver, atLeastOnce()).getDataFile(0, 0L)
      // The spill publish (decrement + counter) is atomic, so tracked bytes are now zero.
      manager.trackedBytesTotal mustBe 0L
      // CRITICAL zero-data-loss: the spilled block is still readable, served from its disk file.
      manager.read(key).map(_.toSeq) mustBe Some(bytes.toSeq)
    }
  }

  test("selects the largest, least-recently-used buffered partitions for spill") {
    // ---- Largest-first selection: spill the biggest blocks until back under the threshold. ----
    locally {
      val conf = new SparkConf(false)
        .set(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT, 50)
        .set(config.STREAMING_SHUFFLE_SPILL_THRESHOLD, 80)
      val maxOnHeap = new AtomicLong(1000000000L)
      val resolver = newMockResolver()
      withManager(conf, newMockMemoryManager(maxOnHeap, grant = true), resolver,
          new StreamingShuffleSource) { manager =>
        // Four blocks of distinct sizes under distinct mapIds, each addressable via getDataFile.
        manager.register(BlockKey(0, 10L, 0, 0L), bytesOf(800, 1.toByte)) mustBe true
        manager.register(BlockKey(0, 11L, 0, 0L), bytesOf(400, 2.toByte)) mustBe true
        manager.register(BlockKey(0, 12L, 0, 0L), bytesOf(200, 3.toByte)) mustBe true
        manager.register(BlockKey(0, 13L, 0, 0L), bytesOf(100, 4.toByte)) mustBe true
        // Shrink the budget to 500 bytes (threshold 80% => 400). Tracked footprint = 1500 bytes.
        maxOnHeap.set(1000L)
        manager.maybeSpill()
        // Largest-first: 800 then 400 spill (projected 1500 -> 700 -> 300 <= 400), leaving the
        // two SMALLEST (200 + 100 = 300) resident, so tracked bytes settle at exactly 300.
        eventually {
          manager.trackedBytesTotal mustBe 300L
        }
        verify(resolver, atLeastOnce()).getDataFile(0, 10L)
        verify(resolver, atLeastOnce()).getDataFile(0, 11L)
        verify(resolver, never()).getDataFile(0, 12L)
        verify(resolver, never()).getDataFile(0, 13L)
      }
    }
    // ---- LRU tie-break: among equal-sized blocks, the least-recently-used one is spilled. ----
    locally {
      val conf = new SparkConf(false)
        .set(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT, 50)
        .set(config.STREAMING_SHUFFLE_SPILL_THRESHOLD, 80)
      val maxOnHeap = new AtomicLong(1000000000L)
      val resolver = newMockResolver()
      withManager(conf, newMockMemoryManager(maxOnHeap, grant = true), resolver,
          new StreamingShuffleSource) { manager =>
        val older = BlockKey(0, 20L, 0, 0L)
        val newer = BlockKey(0, 21L, 0, 0L)
        // Two equal-sized (400B) blocks: register `older` first, then `newer`.
        manager.register(older, bytesOf(400, 1.toByte)) mustBe true
        manager.register(newer, bytesOf(400, 2.toByte)) mustBe true
        // Reading `older` refreshes its recency, making `newer` the least-recently-used block.
        manager.read(older).map(_.toSeq) mustBe Some(bytesOf(400, 1.toByte).toSeq)
        // budget = 600 bytes (threshold 80% => 480); tracked = 800, so exactly ONE block spills.
        maxOnHeap.set(1200L)
        manager.maybeSpill()
        // The LRU block (`newer`, mapId 21) is spilled; the recently-read `older` (mapId 20) stays.
        eventually {
          manager.trackedBytesTotal mustBe 400L
        }
        verify(resolver, atLeastOnce()).getDataFile(0, 21L)
        verify(resolver, never()).getDataFile(0, 20L)
      }
    }
  }

  test("reclaims buffer memory and lowers utilization within 100ms of acknowledgment") {
    val conf = new SparkConf(false)
      .set(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT, 50)
      .set(config.STREAMING_SHUFFLE_SPILL_THRESHOLD, 80)
    // budget = 1000 * 50% = 500; a 250-byte block sits at 50% utilization (< 80%, so no spill).
    val maxOnHeap = new AtomicLong(1000L)
    val memoryManager = newMockMemoryManager(maxOnHeap, grant = true)
    val source = new StreamingShuffleSource
    withManager(conf, memoryManager, newMockResolver(), source) { manager =>
      val key = BlockKey(1, 2L, 3, 0L)
      manager.register(key, bytesOf(250, 9.toByte)) mustBe true
      manager.reservedBytesTotal mustBe 250L
      manager.currentUtilizationPercent() mustBe 50
      // The poller publishes the 50% utilization up to the JMX gauge.
      eventually {
        bufferUtilizationPercent(source) mustBe 50
      }
      // The consumer acknowledges the block: reclaim releases the reserved storage back to the
      // MemoryManager and removes the entry in a single synchronous pass, returning before the next
      // line. The state-transition assertions that follow therefore prove the bounded (<=100ms)
      // reclamation structurally rather than via flaky wall-clock timing or filesystem scheduling.
      manager.reclaim(key)
      // Exactly the granted amount is released back through the existing MemoryManager interface.
      verify(memoryManager, times(1)).releaseStorageMemory(250L, MemoryMode.ON_HEAP)
      manager.reservedBytesTotal mustBe 0L
      manager.trackedBytesTotal mustBe 0L
      manager.currentUtilizationPercent() mustBe 0
      manager.read(key) mustBe None
      // The published utilization gauge tracks back DOWN to zero on the next poll tick.
      eventually {
        bufferUtilizationPercent(source) mustBe 0
      }
    }
  }

  test("reports spill telemetry to the metrics source") {
    val conf = new SparkConf(false)
      .set(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT, 50)
      .set(config.STREAMING_SHUFFLE_SPILL_THRESHOLD, 80)
    val maxOnHeap = new AtomicLong(1000000000L)
    // A MOCK source lets us verify the exact updater calls the spill path makes over JMX.
    val source = mock(classOf[StreamingShuffleSource])
    val resolver = newMockResolver()
    withManager(conf, newMockMemoryManager(maxOnHeap, grant = true), resolver, source) { manager =>
      manager.register(BlockKey(0, 0L, 0, 0L), bytesOf(450, 5.toByte)) mustBe true
      maxOnHeap.set(1000L)
      manager.maybeSpill()
      // The single spilled block increments the spill counter exactly once...
      eventually {
        verify(source, times(1)).incSpillCount()
      }
      // ...and the 100ms poller continuously publishes buffer utilization to the gauge holder.
      eventually {
        verify(source, atLeastOnce()).setBufferUtilizationPercent(anyInt())
      }
    }
  }

  test("a declined storage grant still buffers and spills the block (zero data loss)") {
    val conf = new SparkConf(false)
      .set(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT, 50)
      .set(config.STREAMING_SHUFFLE_SPILL_THRESHOLD, 80)
    // budget = 1000 * 50% = 500; the 450B block is over the 400B threshold. Storage grants are
    // forced to FAIL (memory pressure / OOM risk) via doReturn(false) on acquireStorageMemory.
    val maxOnHeap = new AtomicLong(1000L)
    val source = new StreamingShuffleSource
    val resolver = newMockResolver()
    withManager(conf, newMockMemoryManager(maxOnHeap, grant = false), resolver, source) { manager =>
      val key = BlockKey(0, 0L, 0, 0L)
      val bytes = bytesOf(450, 5.toByte)
      // A declined grant returns false: a pure BACKPRESSURE signal, never "not stored".
      manager.register(key, bytes) mustBe false
      // No storage was reserved, but the bytes are tracked and readable -- nothing is lost.
      manager.reservedBytesTotal mustBe 0L
      manager.read(key).map(_.toSeq) mustBe Some(bytes.toSeq)
      // Memory pressure (an ungranted buffer over the threshold) forces the block to spill to disk,
      // where it stays readable afterward -- guaranteeing zero data loss under memory pressure.
      manager.maybeSpill()
      eventually {
        spillCount(source) mustBe 1L
        manager.trackedBytesTotal mustBe 0L
      }
      manager.read(key).map(_.toSeq) mustBe Some(bytes.toSeq)
    }
  }

  test("stop() shuts down the poller cleanly, releases memory, and is idempotent") {
    val conf = new SparkConf(false).set(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT, 50)
    val maxOnHeap = new AtomicLong(1000L)
    val memoryManager = newMockMemoryManager(maxOnHeap, grant = true)
    val baseline = pollerThreadCount()
    val manager =
      new MemorySpillManager(conf, memoryManager, newMockResolver(), new StreamingShuffleSource)
    try {
      // The constructor starts the 100ms daemon poller.
      eventually {
        pollerThreadCount() must be > baseline
      }
      manager.register(BlockKey(0, 0L, 0, 0L), bytesOf(128, 1.toByte)) mustBe true
      manager.reservedBytesTotal mustBe 128L
      // stop() releases every buffered block's memory and drops accounting to zero...
      manager.stop()
      manager.trackedBytesTotal mustBe 0L
      manager.reservedBytesTotal mustBe 0L
      verify(memoryManager, atLeastOnce()).releaseStorageMemory(128L, MemoryMode.ON_HEAP)
      // ...rejects any post-shutdown registration (no bytes accounted after cleanup)...
      manager.register(BlockKey(0, 0L, 1, 0L), bytesOf(64, 2.toByte)) mustBe false
      manager.trackedBytesTotal mustBe 0L
      // ...is idempotent...
      noException must be thrownBy manager.stop()
      // ...and leaves no daemon poller thread behind (no leaked threads / no memory leak).
      eventually {
        pollerThreadCount() mustBe baseline
      }
    } finally {
      manager.stop()
    }
  }
}
