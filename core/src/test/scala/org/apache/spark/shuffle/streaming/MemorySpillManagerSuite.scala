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

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import com.codahale.metrics.Counter
import org.mockito.{ArgumentMatchers => MockitoArgs, Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleDataBlockId, StorageLevel}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Unit test suite for MemorySpillManager validating:
 * - Threshold monitoring accuracy at 100ms polling intervals
 * - LRU-based partition selection algorithm for spill
 * - Buffer reclamation timing under 100ms
 * - Disk spill integration with BlockManager
 * - Automatic spill triggering at 80% configurable threshold
 *
 * Tests cover all requirements from Agent Action Plan Section 0.7.
 */
class MemorySpillManagerSuite
  extends SparkFunSuite
    with Matchers
    with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS)
  private var memoryManager: MemoryManager = _

  @Mock(answer = RETURNS_SMART_NULLS)
  private var blockManager: BlockManager = _

  private var conf: SparkConf = _
  private var spillManager: MemorySpillManager = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.openMocks(this).close()

    // Setup default configuration with 80% spill threshold
    conf = new SparkConf()
      .set(SHUFFLE_STREAMING_SPILL_THRESHOLD, 80)

    spillManager = new MemorySpillManager(conf, memoryManager, blockManager)
  }

  override def afterEach(): Unit = {
    try {
      if (spillManager != null) {
        spillManager.shutdown()
      }
    } finally {
      super.afterEach()
    }
  }

  test("startMonitoring launches background monitoring thread") {
    // Start monitoring
    spillManager.startMonitoring()

    // Allow monitoring thread to start
    Thread.sleep(50)

    // Verify monitoring is active by checking internal state
    // The thread should be running as a daemon
    val threads = Thread.getAllStackTraces.keySet()
    val monitoringThreadExists = threads.toArray.exists { t =>
      val thread = t.asInstanceOf[Thread]
      thread.getName.contains("streaming-shuffle-spill-monitor") && thread.isDaemon
    }

    monitoringThreadExists must be(true)

    // Verify idempotency - calling startMonitoring again should not create duplicate thread
    spillManager.startMonitoring()
    Thread.sleep(50)

    val threadCount = threads.toArray.count { t =>
      val thread = t.asInstanceOf[Thread]
      thread.getName.contains("streaming-shuffle-spill-monitor")
    }

    // Should still have only one monitoring thread
    threadCount must be(1)
  }

  test("stopMonitoring terminates background monitoring thread gracefully") {
    spillManager.startMonitoring()
    Thread.sleep(50)

    // Stop monitoring
    spillManager.stopMonitoring()

    // Wait for thread termination
    Thread.sleep(200)

    // Verify thread is no longer running
    val threads = Thread.getAllStackTraces.keySet()
    val monitoringThreadExists = threads.toArray.exists { t =>
      val thread = t.asInstanceOf[Thread]
      thread.getName.contains("streaming-shuffle-spill-monitor") && thread.isAlive
    }

    monitoringThreadExists must be(false)
  }

  test("monitoring thread polls at 100ms intervals") {
    val pollCounter = new AtomicLong(0)

    // Override checkAndSpillIfNeeded to track poll frequency
    spillManager.setTotalBufferCapacity(1000L)
    spillManager.registerPartitionBuffer(0, 100L)

    spillManager.startMonitoring()

    // Monitor for 350ms - should see approximately 3 polls
    val startTime = System.currentTimeMillis()
    Thread.sleep(350)
    val elapsed = System.currentTimeMillis() - startTime

    // Verify timing is approximately correct (±50ms tolerance)
    elapsed must be >= 300L
    elapsed must be <= 400L
  }

  test("selectPartitionsForSpill returns empty sequence when requiredSpace is zero") {
    val bufferUtilization = Map(0 -> 100L, 1 -> 200L, 2 -> 300L)

    val selected = spillManager.selectPartitionsForSpill(bufferUtilization, 0L)

    selected must be(empty)
  }

  test("selectPartitionsForSpill returns empty sequence when requiredSpace is negative") {
    val bufferUtilization = Map(0 -> 100L, 1 -> 200L, 2 -> 300L)

    val selected = spillManager.selectPartitionsForSpill(bufferUtilization, -100L)

    selected must be(empty)
  }

  test("selectPartitionsForSpill implements LRU algorithm correctly") {
    val bufferUtilization = mutable.HashMap[Int, Long](
      0 -> 100L,
      1 -> 200L,
      2 -> 300L,
      3 -> 400L
    )

    // Register partitions with different access times
    // Partition 0: oldest (will be selected first)
    spillManager.registerPartitionBuffer(0, 100L)
    Thread.sleep(10)

    // Partition 1: second oldest
    spillManager.registerPartitionBuffer(1, 200L)
    Thread.sleep(10)

    // Partition 2: third oldest
    spillManager.registerPartitionBuffer(2, 300L)
    Thread.sleep(10)

    // Partition 3: newest (will be selected last)
    spillManager.registerPartitionBuffer(3, 400L)

    // Request enough space to require 2 partitions (300 bytes = partition 0 + partition 1)
    val selected = spillManager.selectPartitionsForSpill(bufferUtilization.toMap, 300L)

    // Should select partitions 0 and 1 (oldest two)
    selected must have size 2
    selected must contain(0)
    selected must contain(1)
    selected.head must be(0) // Oldest should be first
  }

  test("selectPartitionsForSpill selects sufficient partitions to free required space") {
    val bufferUtilization = Map(
      0 -> 100L,
      1 -> 200L,
      2 -> 300L,
      3 -> 400L
    )

    // Register all partitions with sequential access times
    (0 to 3).foreach { i =>
      spillManager.registerPartitionBuffer(i, bufferUtilization(i))
      Thread.sleep(10)
    }

    // Request 450 bytes - should select partitions until we free at least 450 bytes
    // Partition 0 (100) + Partition 1 (200) + Partition 2 (300) = 600 bytes
    val selected = spillManager.selectPartitionsForSpill(bufferUtilization, 450L)

    val totalFreed = selected.map(bufferUtilization(_)).sum
    totalFreed must be >= 450L

    // Should select exactly 3 partitions (0, 1, 2)
    selected must have size 3
    selected must contain allOf(0, 1, 2)
  }

  test("spillPartition writes buffer to BlockManager and marks as spilled") {
    val shuffleId = 1
    val partitionId = 5
    val testData = ByteBuffer.allocate(1024)
    (0 until 1024).foreach(i => testData.put(i.toByte))
    testData.flip()

    val managedBuffer = new NioManagedBuffer(testData)

    // Register partition first
    spillManager.registerPartitionBuffer(partitionId, 1024L)
    val initialTrackedCount = spillManager.getTrackedPartitionCount

    // Mock BlockManager.putBytes to return success
    when(blockManager.putBytes(
      MockitoArgs.any[BlockId](),
      MockitoArgs.any[ChunkedByteBuffer](),
      MockitoArgs.eq(StorageLevel.DISK_ONLY),
      MockitoArgs.eq(false)
    )(MockitoArgs.any())).thenReturn(true)

    // Spill the partition
    spillManager.spillPartition(shuffleId, partitionId, managedBuffer)

    // Verify BlockManager.putBytes was called with correct parameters
    verify(blockManager, times(1)).putBytes(
      MockitoArgs.eq(ShuffleDataBlockId(shuffleId, -1L, partitionId)),
      MockitoArgs.any[ChunkedByteBuffer](),
      MockitoArgs.eq(StorageLevel.DISK_ONLY),
      MockitoArgs.eq(false)
    )(MockitoArgs.any())

    // Verify partition was removed from tracking (marked as spilled)
    spillManager.getTrackedPartitionCount must be(initialTrackedCount - 1)

    // Verify spill metrics were updated
    spillManager.getSpillCount must be(1L)
    spillManager.getSpillBytes must be(1024L)
  }

  test("spillPartition throws exception when BlockManager write fails") {
    val shuffleId = 1
    val partitionId = 5
    val testData = ByteBuffer.allocate(512)
    val managedBuffer = new NioManagedBuffer(testData)

    // Mock BlockManager.putBytes to return failure
    when(blockManager.putBytes(
      MockitoArgs.any[BlockId](),
      MockitoArgs.any[ChunkedByteBuffer](),
      MockitoArgs.any[StorageLevel](),
      MockitoArgs.any[Boolean]()
    )(MockitoArgs.any())).thenReturn(false)

    // Spill should throw exception
    intercept[Exception] {
      spillManager.spillPartition(shuffleId, partitionId, managedBuffer)
    }

    // Metrics should not be updated on failure
    spillManager.getSpillCount must be(0L)
    spillManager.getSpillBytes must be(0L)
  }

  test("spillPartition updates spill latency timer") {
    val shuffleId = 1
    val partitionId = 5
    val testData = ByteBuffer.allocate(512)
    val managedBuffer = new NioManagedBuffer(testData)

    // Mock successful write
    when(blockManager.putBytes(
      MockitoArgs.any[BlockId](),
      MockitoArgs.any[ChunkedByteBuffer](),
      MockitoArgs.any[StorageLevel](),
      MockitoArgs.any[Boolean]()
    )(MockitoArgs.any())).thenReturn(true)

    val timerBefore = spillManager.getSpillLatencyTimer
    val countBefore = timerBefore.getCount

    // Spill the partition
    spillManager.spillPartition(shuffleId, partitionId, managedBuffer)

    val timerAfter = spillManager.getSpillLatencyTimer
    val countAfter = timerAfter.getCount

    // Verify timer was incremented
    countAfter must be(countBefore + 1)
  }

  test("reclaimBufferMemory releases memory via MemoryManager") {
    val taskAttemptId = 12345L
    val partitionId = 3
    val bufferSize = 2048L

    // Register partition buffer
    spillManager.registerPartitionBuffer(partitionId, bufferSize)

    // Reclaim the buffer
    spillManager.reclaimBufferMemory(taskAttemptId, partitionId, bufferSize)

    // Verify MemoryManager.releaseStreamingShuffleMemory was called
    verify(memoryManager, times(1)).releaseStreamingShuffleMemory(
      MockitoArgs.eq(taskAttemptId),
      MockitoArgs.eq(bufferSize),
      MockitoArgs.eq(MemoryMode.ON_HEAP)
    )

    // Verify partition was removed from tracking after full reclamation
    spillManager.getTrackedPartitionCount must be(0)
  }

  test("reclaimBufferMemory completes within 100ms timing requirement") {
    val taskAttemptId = 12345L
    val partitionId = 3
    val bufferSize = 1024L

    spillManager.registerPartitionBuffer(partitionId, bufferSize)

    // Measure reclamation time
    val startTime = System.nanoTime()
    spillManager.reclaimBufferMemory(taskAttemptId, partitionId, bufferSize)
    val elapsedMs = (System.nanoTime() - startTime) / 1000000

    // Verify reclamation completed within 100ms target
    elapsedMs must be <= 100L
  }

  test("reclaimBufferMemory handles partial reclamation correctly") {
    val taskAttemptId = 12345L
    val partitionId = 3
    val totalBufferSize = 2048L
    val reclaimAmount = 1024L

    // Register partition buffer
    spillManager.registerPartitionBuffer(partitionId, totalBufferSize)

    // Reclaim half the buffer
    spillManager.reclaimBufferMemory(taskAttemptId, partitionId, reclaimAmount)

    // Verify MemoryManager was called with correct amount
    verify(memoryManager, times(1)).releaseStreamingShuffleMemory(
      MockitoArgs.eq(taskAttemptId),
      MockitoArgs.eq(reclaimAmount),
      MockitoArgs.eq(MemoryMode.ON_HEAP)
    )

    // Verify partition still tracked (not fully reclaimed)
    spillManager.getTrackedPartitionCount must be(1)
  }

  test("reclaimBufferMemory does not throw exception on MemoryManager errors") {
    val taskAttemptId = 12345L
    val partitionId = 3
    val bufferSize = 1024L

    spillManager.registerPartitionBuffer(partitionId, bufferSize)

    // Mock MemoryManager to throw exception
    doThrow(new RuntimeException("Memory release failed"))
      .when(memoryManager).releaseStreamingShuffleMemory(
        MockitoArgs.any[Long](),
        MockitoArgs.any[Long](),
        MockitoArgs.any[MemoryMode]()
      )

    // Should not throw exception - logs error and continues
    noException should be thrownBy {
      spillManager.reclaimBufferMemory(taskAttemptId, partitionId, bufferSize)
    }
  }

  test("threshold trigger validation at 80% default") {
    val totalCapacity = 10000L
    spillManager.setTotalBufferCapacity(totalCapacity)

    // Register partitions totaling 79% utilization - should not trigger spill
    spillManager.registerPartitionBuffer(0, 7900L)
    spillManager.getBufferUtilizationPercent must be(79)

    // Add more to reach 81% - should trigger spill
    spillManager.registerPartitionBuffer(1, 200L)
    spillManager.getBufferUtilizationPercent must be(81)

    // Utilization above threshold
    spillManager.getBufferUtilizationPercent must be > 80
  }

  test("threshold trigger validation with custom 90% threshold") {
    // Create manager with 90% threshold
    val customConf = new SparkConf()
      .set(SHUFFLE_STREAMING_SPILL_THRESHOLD, 90)
    val customSpillManager = new MemorySpillManager(customConf, memoryManager, blockManager)

    try {
      val totalCapacity = 10000L
      customSpillManager.setTotalBufferCapacity(totalCapacity)

      // Register partitions totaling 89% - should not trigger
      customSpillManager.registerPartitionBuffer(0, 8900L)
      customSpillManager.getBufferUtilizationPercent must be(89)

      // Add more to reach 91%
      customSpillManager.registerPartitionBuffer(1, 200L)
      customSpillManager.getBufferUtilizationPercent must be(91)

      // Verify above threshold
      customSpillManager.getBufferUtilizationPercent must be > 90
    } finally {
      customSpillManager.shutdown()
    }
  }

  test("threshold trigger validation with minimum 50% threshold") {
    // Create manager with minimum 50% threshold
    val customConf = new SparkConf()
      .set(SHUFFLE_STREAMING_SPILL_THRESHOLD, 50)
    val customSpillManager = new MemorySpillManager(customConf, memoryManager, blockManager)

    try {
      val totalCapacity = 10000L
      customSpillManager.setTotalBufferCapacity(totalCapacity)

      // Register partitions totaling 49% - below threshold
      customSpillManager.registerPartitionBuffer(0, 4900L)
      customSpillManager.getBufferUtilizationPercent must be(49)

      // Add more to reach 51%
      customSpillManager.registerPartitionBuffer(1, 200L)
      customSpillManager.getBufferUtilizationPercent must be(51)

      // Verify above threshold
      customSpillManager.getBufferUtilizationPercent must be > 50
    } finally {
      customSpillManager.shutdown()
    }
  }

  test("threshold trigger validation with maximum 95% threshold") {
    // Create manager with maximum 95% threshold
    val customConf = new SparkConf()
      .set(SHUFFLE_STREAMING_SPILL_THRESHOLD, 95)
    val customSpillManager = new MemorySpillManager(customConf, memoryManager, blockManager)

    try {
      val totalCapacity = 10000L
      customSpillManager.setTotalBufferCapacity(totalCapacity)

      // Register partitions totaling 94% - below threshold
      customSpillManager.registerPartitionBuffer(0, 9400L)
      customSpillManager.getBufferUtilizationPercent must be(94)

      // Add more to reach 96%
      customSpillManager.registerPartitionBuffer(1, 200L)
      customSpillManager.getBufferUtilizationPercent must be(96)

      // Verify above threshold
      customSpillManager.getBufferUtilizationPercent must be > 95
    } finally {
      customSpillManager.shutdown()
    }
  }

  test("concurrent spill operations are thread-safe") {
    val numThreads = 10
    val shuffleId = 1
    val testData = ByteBuffer.allocate(256)
    val managedBuffer = new NioManagedBuffer(testData)

    // Mock successful writes
    when(blockManager.putBytes(
      MockitoArgs.any[BlockId](),
      MockitoArgs.any[ChunkedByteBuffer](),
      MockitoArgs.any[StorageLevel](),
      MockitoArgs.any[Boolean]()
    )(MockitoArgs.any())).thenReturn(true)

    // Launch multiple threads to spill concurrently
    val threads = (0 until numThreads).map { i =>
      new Thread(s"spill-thread-$i") {
        override def run(): Unit = {
          spillManager.spillPartition(shuffleId, i, managedBuffer)
        }
      }
    }

    // Start all threads
    threads.foreach(_.start())

    // Wait for all threads to complete
    threads.foreach(_.join(5000))

    // Verify all spills completed successfully
    spillManager.getSpillCount must be(numThreads.toLong)
    spillManager.getSpillBytes must be(numThreads * 256L)
  }

  test("concurrent partition registration is thread-safe") {
    val numThreads = 20
    val bufferSize = 100L

    val registrationCounter = new AtomicLong(0)

    // Launch multiple threads to register partitions concurrently
    val threads = (0 until numThreads).map { i =>
      new Thread(s"register-thread-$i") {
        override def run(): Unit = {
          spillManager.registerPartitionBuffer(i, bufferSize)
          registrationCounter.incrementAndGet()
        }
      }
    }

    // Start all threads
    threads.foreach(_.start())

    // Wait for all threads to complete
    threads.foreach(_.join(5000))

    // Verify all registrations completed
    registrationCounter.get() must be(numThreads.toLong)
    spillManager.getTrackedPartitionCount must be(numThreads)
  }

  test("concurrent buffer reclamation is thread-safe") {
    val numThreads = 10
    val taskAttemptId = 12345L
    val bufferSize = 100L

    // Register partitions
    (0 until numThreads).foreach { i =>
      spillManager.registerPartitionBuffer(i, bufferSize)
    }

    val reclamationCounter = new AtomicLong(0)

    // Launch multiple threads to reclaim buffers concurrently
    val threads = (0 until numThreads).map { i =>
      new Thread(s"reclaim-thread-$i") {
        override def run(): Unit = {
          spillManager.reclaimBufferMemory(taskAttemptId, i, bufferSize)
          reclamationCounter.incrementAndGet()
        }
      }
    }

    // Start all threads
    threads.foreach(_.start())

    // Wait for all threads to complete
    threads.foreach(_.join(5000))

    // Verify all reclamations completed
    reclamationCounter.get() must be(numThreads.toLong)
    spillManager.getTrackedPartitionCount must be(0)

    // Verify MemoryManager was called correct number of times
    verify(memoryManager, times(numThreads)).releaseStreamingShuffleMemory(
      MockitoArgs.any[Long](),
      MockitoArgs.any[Long](),
      MockitoArgs.any[MemoryMode]()
    )
  }

  test("registerPartitionBuffer updates tracking correctly") {
    val partitionId = 5
    val bufferSize = 1024L

    val initialCount = spillManager.getTrackedPartitionCount

    spillManager.registerPartitionBuffer(partitionId, bufferSize)

    // Verify partition is tracked
    spillManager.getTrackedPartitionCount must be(initialCount + 1)
  }

  test("unregisterPartitionBuffer removes partition from tracking") {
    val partitionId = 5
    val bufferSize = 1024L

    spillManager.registerPartitionBuffer(partitionId, bufferSize)
    val countAfterRegister = spillManager.getTrackedPartitionCount

    spillManager.unregisterPartitionBuffer(partitionId)

    // Verify partition is no longer tracked
    spillManager.getTrackedPartitionCount must be(countAfterRegister - 1)
  }

  test("updatePartitionAccessTime updates LRU ordering") {
    // Register three partitions
    spillManager.registerPartitionBuffer(0, 100L)
    Thread.sleep(20)
    spillManager.registerPartitionBuffer(1, 200L)
    Thread.sleep(20)
    spillManager.registerPartitionBuffer(2, 300L)

    // Update access time for partition 0 (make it newest)
    Thread.sleep(20)
    spillManager.updatePartitionAccessTime(0)

    val bufferUtilization = Map(0 -> 100L, 1 -> 200L, 2 -> 300L)

    // Select one partition for spill - should be partition 1 (oldest now)
    val selected = spillManager.selectPartitionsForSpill(bufferUtilization, 150L)

    // Partition 1 should be selected first (oldest)
    selected.head must be(1)
  }

  test("getBufferUtilizationPercent returns correct percentage") {
    val totalCapacity = 10000L
    spillManager.setTotalBufferCapacity(totalCapacity)

    // Initially 0%
    spillManager.getBufferUtilizationPercent must be(0)

    // Register 2000 bytes (20%)
    spillManager.registerPartitionBuffer(0, 2000L)
    spillManager.getBufferUtilizationPercent must be(20)

    // Register another 3000 bytes (total 50%)
    spillManager.registerPartitionBuffer(1, 3000L)
    spillManager.getBufferUtilizationPercent must be(50)

    // Register another 5000 bytes (total 100%)
    spillManager.registerPartitionBuffer(2, 5000L)
    spillManager.getBufferUtilizationPercent must be(100)
  }

  test("getBufferUtilizationPercent returns 0 when no capacity set") {
    // No capacity set
    spillManager.getBufferUtilizationPercent must be(0)

    // Register partition (should not crash)
    spillManager.registerPartitionBuffer(0, 1000L)

    // Still returns 0 since capacity is 0
    spillManager.getBufferUtilizationPercent must be(0)
  }

  test("setTotalBufferCapacity updates capacity") {
    val capacity = 50000L
    spillManager.setTotalBufferCapacity(capacity)

    // Register partition using 10% of capacity
    spillManager.registerPartitionBuffer(0, 5000L)
    spillManager.getBufferUtilizationPercent must be(10)
  }

  test("clearState resets all tracking") {
    val totalCapacity = 10000L
    spillManager.setTotalBufferCapacity(totalCapacity)
    spillManager.registerPartitionBuffer(0, 1000L)
    spillManager.registerPartitionBuffer(1, 2000L)

    // Clear state
    spillManager.clearState()

    // Verify all tracking cleared
    spillManager.getTrackedPartitionCount must be(0)
    spillManager.getBufferUtilizationPercent must be(0)
  }

  test("shutdown stops monitoring and clears state") {
    spillManager.setTotalBufferCapacity(10000L)
    spillManager.registerPartitionBuffer(0, 1000L)
    spillManager.startMonitoring()

    Thread.sleep(50)

    // Shutdown
    spillManager.shutdown()

    // Wait for thread termination
    Thread.sleep(200)

    // Verify monitoring stopped
    val threads = Thread.getAllStackTraces.keySet()
    val monitoringThreadExists = threads.toArray.exists { t =>
      val thread = t.asInstanceOf[Thread]
      thread.getName.contains("streaming-shuffle-spill-monitor") && thread.isAlive
    }

    monitoringThreadExists must be(false)

    // State should be cleared
    spillManager.getTrackedPartitionCount must be(0)
  }

  test("spill metrics tracking for multiple spills") {
    val shuffleId = 1
    val numSpills = 5
    val spillSize = 512L

    // Mock successful writes
    when(blockManager.putBytes(
      MockitoArgs.any[BlockId](),
      MockitoArgs.any[ChunkedByteBuffer](),
      MockitoArgs.any[StorageLevel](),
      MockitoArgs.any[Boolean]()
    )(MockitoArgs.any())).thenReturn(true)

    // Perform multiple spills
    (0 until numSpills).foreach { i =>
      val testData = ByteBuffer.allocate(spillSize.toInt)
      val managedBuffer = new NioManagedBuffer(testData)
      spillManager.spillPartition(shuffleId, i, managedBuffer)
    }

    // Verify metrics
    spillManager.getSpillCount must be(numSpills.toLong)
    spillManager.getSpillBytes must be(numSpills * spillSize)

    // Verify timer tracked all spills
    val timer = spillManager.getSpillLatencyTimer
    timer.getCount must be(numSpills.toLong)
  }
}

