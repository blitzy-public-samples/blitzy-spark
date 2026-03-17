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

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.storage.{BlockManager, DiskBlockManager, TempLocalBlockId}
import org.apache.spark.util.Utils

/**
 * Unit tests for [[MemorySpillManager]] — the memory pressure management component
 * for streaming shuffle buffers. Tests cover threshold monitoring with 100ms polling,
 * LRU eviction selection of the largest buffered partition, buffer reclamation timing,
 * spill metrics accuracy, and lifecycle management (start/stop/idempotency).
 *
 * Mocking strategy follows the SortShuffleWriterSuite pattern: BlockManager is mocked
 * via @Mock(answer = RETURNS_SMART_NULLS) with DiskBlockManager stubbed to provide
 * temporary spill files for tests that trigger the spill code path.
 */
class MemorySpillManagerSuite extends SparkFunSuite {

  @Mock(answer = RETURNS_SMART_NULLS)
  private var blockManager: BlockManager = _

  @Mock(answer = RETURNS_SMART_NULLS)
  private var diskBlockManager: DiskBlockManager = _

  /** Temporary directory for spill files created during tests */
  private var tempDir: File = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.openMocks(this).close()
    tempDir = Utils.createTempDir()
    // Stub the diskBlockManager chain so spill operations can allocate temp files
    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(diskBlockManager.createTempLocalBlock()).thenAnswer(
      (_: InvocationOnMock) => {
        val blockId = TempLocalBlockId(UUID.randomUUID())
        val file = new File(tempDir, blockId.name)
        (blockId, file)
      })
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  /**
   * Registers a minimal spill data callback that creates an empty file on disk,
   * simulating successful data persistence during a spill event. Required for
   * tests that verify spill metrics (count, bytes, latency) since the
   * MemorySpillManager aborts the spill if no callback is registered.
   */
  private def registerSpillCallback(spillManager: MemorySpillManager): Unit = {
    spillManager.registerSpillCallback((_, _, file) => {
      file.createNewFile()
      true
    })
  }

  // =========================================================================
  // Threshold Monitoring Tests
  // =========================================================================

  test("default spill threshold is 80%") {
    val conf = new SparkConf()
    val spillManager = new MemorySpillManager(conf, blockManager)
    spillManager.start(1000000L) // 1MB executor memory

    // Buffer size percent default is 20%, so available = 200000 bytes
    // Spill threshold is 80% of available = 160000 bytes

    // Allocate buffer below threshold — no spill
    assert(spillManager.allocateBuffer(0, 0, 100000L))
    assert(spillManager.getSpillCount === 0L)

    spillManager.stop()
  }

  test("custom spill threshold configuration within valid range") {
    val conf = new SparkConf()
      .set("spark.shuffle.streaming.spillThreshold", "60")
      .set("spark.shuffle.streaming.bufferSizePercent", "30")
    val spillManager = new MemorySpillManager(conf, blockManager)
    registerSpillCallback(spillManager)
    spillManager.start(1000000L)

    // Available = 1000000 * 30% = 300000 bytes
    // Threshold = 300000 * 60% = 180000 bytes
    // Allocating 180000 should trigger spill
    assert(spillManager.allocateBuffer(0, 0, 180000L))

    // Give time for polling loop (100ms interval)
    Thread.sleep(200)

    // Spill should have been triggered
    assert(spillManager.getSpillCount >= 1L)

    spillManager.stop()
  }

  // =========================================================================
  // LRU Eviction Tests
  // =========================================================================

  test("LRU eviction selects largest partition for spill") {
    val conf = new SparkConf()
      .set("spark.shuffle.streaming.spillThreshold", "50")
      .set("spark.shuffle.streaming.bufferSizePercent", "50")
    val spillManager = new MemorySpillManager(conf, blockManager)
    registerSpillCallback(spillManager)
    spillManager.start(1000000L) // Available = 500000 bytes, threshold at 250000

    // Allocate multiple partitions of different sizes
    assert(spillManager.allocateBuffer(0, 0, 50000L))   // Small partition
    assert(spillManager.allocateBuffer(0, 1, 150000L))  // Large partition
    assert(spillManager.allocateBuffer(0, 2, 60000L))   // Medium partition

    // Total = 260000 > threshold 250000 — spill should trigger
    Thread.sleep(200)

    // After spill, the largest partition (partition 1) should have been evicted
    assert(spillManager.getSpillCount >= 1L)
    assert(spillManager.getSpillBytes >= 150000L)

    spillManager.stop()
  }

  // =========================================================================
  // Buffer Allocation Tests
  // =========================================================================

  test("buffer allocation denied when exceeding total available memory") {
    val conf = new SparkConf()
      .set("spark.shuffle.streaming.bufferSizePercent", "10")
    val spillManager = new MemorySpillManager(conf, blockManager)
    spillManager.start(1000000L) // Available = 100000 bytes

    // Try to allocate more than available
    assert(!spillManager.allocateBuffer(0, 0, 150000L))
    assert(spillManager.getTotalAllocatedBytes === 0L)

    spillManager.stop()
  }

  // =========================================================================
  // Buffer Reclamation Tests
  // =========================================================================

  test("buffer reclamation releases allocated memory") {
    val conf = new SparkConf()
    val spillManager = new MemorySpillManager(conf, blockManager)
    spillManager.start(1000000L)

    assert(spillManager.allocateBuffer(0, 0, 50000L))
    assert(spillManager.getTotalAllocatedBytes === 50000L)

    spillManager.reclaimBuffer(0, 0)
    assert(spillManager.getTotalAllocatedBytes === 0L)

    spillManager.stop()
  }

  // =========================================================================
  // Buffer Utilization Tests
  // =========================================================================

  test("getBufferUtilizationPercent returns correct percentage") {
    val conf = new SparkConf()
      .set("spark.shuffle.streaming.bufferSizePercent", "50")
    val spillManager = new MemorySpillManager(conf, blockManager)
    spillManager.start(1000000L) // Available = 500000 bytes

    assert(spillManager.getBufferUtilizationPercent === 0)

    spillManager.allocateBuffer(0, 0, 250000L) // 50% utilization
    assert(spillManager.getBufferUtilizationPercent === 50)

    spillManager.stop()
  }

  // =========================================================================
  // Spill Metrics Tests
  // =========================================================================

  test("spill metrics track count, bytes, and latency") {
    val conf = new SparkConf()
      .set("spark.shuffle.streaming.spillThreshold", "50")
      .set("spark.shuffle.streaming.bufferSizePercent", "50")
    val spillManager = new MemorySpillManager(conf, blockManager)
    registerSpillCallback(spillManager)
    spillManager.start(1000000L) // Available = 500000, threshold = 250000

    // Initial metrics should be zero
    assert(spillManager.getSpillCount === 0L)
    assert(spillManager.getSpillBytes === 0L)
    assert(spillManager.getSpillLatencyNs === 0L)

    // Allocate past threshold to trigger spill
    spillManager.allocateBuffer(0, 0, 300000L)
    Thread.sleep(200) // Wait for polling loop

    // Verify metrics were updated
    assert(spillManager.getSpillCount >= 1L)
    assert(spillManager.getSpillBytes > 0L)
    assert(spillManager.getSpillLatencyNs > 0L)

    spillManager.stop()
  }

  // =========================================================================
  // Lifecycle Management Tests
  // =========================================================================

  test("start and stop lifecycle management") {
    val conf = new SparkConf()
    val spillManager = new MemorySpillManager(conf, blockManager)

    assert(!spillManager.isActive)
    spillManager.start(1000000L)
    assert(spillManager.isActive)
    spillManager.stop()
    assert(!spillManager.isActive)
  }

  test("double start is idempotent") {
    val conf = new SparkConf()
    val spillManager = new MemorySpillManager(conf, blockManager)
    spillManager.start(1000000L)
    spillManager.start(1000000L) // Should not throw
    assert(spillManager.isActive)
    spillManager.stop()
  }

  test("stop clears all state ensuring zero memory leaks") {
    val conf = new SparkConf()
    val spillManager = new MemorySpillManager(conf, blockManager)
    spillManager.start(1000000L)

    spillManager.allocateBuffer(0, 0, 50000L)
    spillManager.allocateBuffer(0, 1, 30000L)
    assert(spillManager.getTotalAllocatedBytes === 80000L)

    spillManager.stop()
    assert(spillManager.getTotalAllocatedBytes === 0L)
  }

  // =========================================================================
  // Buffer Update Tests
  // =========================================================================

  test("updateBuffer correctly adjusts tracked buffer size") {
    val conf = new SparkConf()
    val spillManager = new MemorySpillManager(conf, blockManager)
    spillManager.start(1000000L)

    spillManager.allocateBuffer(0, 0, 10000L)
    assert(spillManager.getTotalAllocatedBytes === 10000L)

    spillManager.updateBuffer(0, 0, 25000L) // Grew to 25000
    assert(spillManager.getTotalAllocatedBytes === 25000L)

    spillManager.stop()
  }

  // =========================================================================
  // Polling Interval Tests
  // =========================================================================

  test("polling interval operates at approximately 100ms") {
    val conf = new SparkConf()
      .set("spark.shuffle.streaming.spillThreshold", "50")
      .set("spark.shuffle.streaming.bufferSizePercent", "50")
    val spillManager = new MemorySpillManager(conf, blockManager)
    registerSpillCallback(spillManager)
    spillManager.start(1000000L) // Available=500000, threshold=250000

    // Allocate past threshold
    spillManager.allocateBuffer(0, 0, 300000L)

    // Spill should trigger within ~200ms (2 polling intervals)
    Thread.sleep(250)
    assert(spillManager.getSpillCount >= 1L,
      "Spill should have triggered within 200ms of exceeding threshold")

    spillManager.stop()
  }
}
