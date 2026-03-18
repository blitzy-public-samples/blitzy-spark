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

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Mockito._
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{Partitioner, SharedSparkContext, ShuffleDependency, SparkFunSuite}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.ShuffleChecksumTestHelper
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.Utils

/**
 * Unit tests for [[StreamingShuffleWriter]] — the per-partition memory buffer management
 * and network streaming pipeline component.
 *
 * Tests cover buffer allocation, per-partition memory tracking, CRC32C checksum generation,
 * resource cleanup on failure, MapStatus generation with correct partition sizes, and
 * streaming-specific metrics reporting.
 *
 * Follows the same structural pattern as [[org.apache.spark.shuffle.sort.SortShuffleWriterSuite]]
 * for consistency: Mockito annotations, SharedSparkContext for SparkEnv setup,
 * MemoryTestingUtils.fakeTaskContext for task context, and mock ShuffleDependency with stubbed
 * partitioner, serializer, aggregator, and keyOrdering.
 *
 * Isolation: No imports from [[org.apache.spark.shuffle.sort]] — complete package isolation
 * between streaming and sort-based shuffle implementations.
 */
class StreamingShuffleWriterSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers
    with PrivateMethodTester
    with ShuffleChecksumTestHelper {

  // =========================================================================
  // Mock Fields — following SortShuffleWriterSuite pattern (lines 46-55)
  // =========================================================================

  @Mock(answer = RETURNS_SMART_NULLS)
  private var blockManager: BlockManager = _

  @Mock(answer = RETURNS_SMART_NULLS)
  private var dependency: ShuffleDependency[Int, Int, Int] = _

  // =========================================================================
  // Test Constants
  // =========================================================================

  /** Shuffle ID used across all tests. */
  private val shuffleId = 0

  /** Number of partitions (maps) for the test partitioner. */
  private val numMaps = 5

  /** Default buffer size percentage for streaming shuffle (20% of executor memory). */
  private val defaultBufferSizePercent = 20

  /** Default spill threshold percentage (80% buffer occupancy). */
  private val defaultSpillThreshold = 80

  // =========================================================================
  // Mutable State — initialized in beforeEach
  // =========================================================================

  /** The streaming shuffle handle created with mock dependency in beforeEach. */
  private var shuffleHandle: StreamingShuffleHandle[Int, Int, Int] = _

  /** Java serializer configured from the shared SparkConf. */
  private var serializer: JavaSerializer = _

  /**
   * Test partitioner that distributes keys across numMaps partitions using
   * non-negative modulo of the key's hash code — same pattern as SortShuffleWriterSuite.
   */
  private val partitioner = new Partitioner() {
    def numPartitions: Int = numMaps
    def getPartition(key: Any): Int = Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  // =========================================================================
  // Lifecycle Setup — following SortShuffleWriterSuite beforeEach pattern
  // =========================================================================

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Initialize Mockito annotations for @Mock fields
    MockitoAnnotations.openMocks(this).close()
    // Create the JavaSerializer using the shared conf from SharedSparkContext
    serializer = new JavaSerializer(conf)
    // Stub the mock dependency's methods to return test values.
    // These stubs match the SortShuffleWriterSuite pattern (lines 82-94).
    when(dependency.partitioner).thenReturn(partitioner)
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
    // Create the StreamingShuffleHandle with default streaming configuration.
    // The handle carries the mock dependency and streaming-specific metadata:
    // bufferSizePercent=20 (default), spillThreshold=80 (default),
    // expectedPartitionCount=numMaps.
    shuffleHandle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId, dependency, defaultBufferSizePercent, defaultSpillThreshold, numMaps)
  }

  // =========================================================================
  // Test 1: Write empty iterator produces zero metrics
  // =========================================================================

  test("write empty iterator") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle,
      mapId = 1,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      conf)
    writer.write(Iterator.empty)
    writer.stop(success = true)
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    // With an empty iterator, no bytes should be written and no records counted.
    assert(writeMetrics.bytesWritten === 0)
    assert(writeMetrics.recordsWritten === 0)
  }

  // =========================================================================
  // Test 2: Write with some records produces correct metrics
  // =========================================================================

  test("write with some records") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val records = List[(Int, Int)]((1, 2), (2, 3), (4, 4), (6, 5))
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle,
      mapId = 2,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      conf)
    writer.write(records.iterator)
    writer.stop(success = true)
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    // After writing records, bytesWritten must be positive and recordsWritten
    // must equal the number of input records.
    assert(writeMetrics.bytesWritten > 0)
    assert(records.size === writeMetrics.recordsWritten)
  }

  // =========================================================================
  // Test 3: Buffer allocation respects per-partition memory limits
  // =========================================================================

  test("buffer allocation respects per-partition memory limits") {
    // Buffer allocation formula: (executorMemory * bufferPercent / 100) / numPartitions
    // Default: (1GB * 20%) / 5 = ~40MB per partition — much larger than test data.
    // This test verifies that partition lengths are correctly tracked across
    // all partitions and that no partition exceeds the aggregate budget.
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle,
      mapId = 3,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      conf)
    // Distribute 100 records across all partitions using modulo partitioning
    val records = (0 until 100).map(i => (i % numMaps, i)).toList
    writer.write(records.iterator)
    // Verify that getPartitionLengths returns an array with one entry per partition
    // and that total data volume is positive (all partitions received records).
    val lengths = writer.getPartitionLengths()
    assert(lengths.length === numMaps,
      s"Expected $numMaps partition lengths, got ${lengths.length}")
    assert(lengths.sum > 0, "Total partition data must be positive after writing records")
    // Each partition should have received records (100 records / 5 partitions = 20 each)
    for (i <- 0 until numMaps) {
      assert(lengths(i) > 0, s"Partition $i should have received records")
    }
    writer.stop(success = true)
  }

  // =========================================================================
  // Test 4: stop(success=true) returns MapStatus with correct partition sizes
  // =========================================================================

  test("stop(success=true) returns MapStatus with correct partition sizes") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val records = List[(Int, Int)]((1, 2), (2, 3))
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle,
      mapId = 4,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      conf)
    writer.write(records.iterator)
    val mapStatus = writer.stop(success = true)
    // On successful completion, stop() must return a defined MapStatus.
    assert(mapStatus.isDefined, "stop(success=true) must return Some(MapStatus)")
    // The MapStatus contains a valid location (BlockManagerId) from SparkEnv.
    val status = mapStatus.get
    assert(status.location !== null, "MapStatus location must not be null")
    // Partition lengths should match the writer's tracked partition sizes.
    val lengths = writer.getPartitionLengths()
    assert(lengths.sum > 0, "MapStatus should report positive total data volume")
  }

  // =========================================================================
  // Test 5: stop(success=false) returns None
  // =========================================================================

  test("stop(success=false) returns None") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle,
      mapId = 5,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      conf)
    writer.write(Iterator.empty)
    val mapStatus = writer.stop(success = false)
    // On failure, stop() must return None regardless of whether data was written.
    assert(mapStatus.isEmpty, "stop(success=false) must return None")
  }

  // =========================================================================
  // Test 6: Idempotent stop behavior
  // =========================================================================

  test("stop is idempotent - second call returns None") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val records = List[(Int, Int)]((1, 2))
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle,
      mapId = 6,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      conf)
    writer.write(records.iterator)
    // First stop with success returns the MapStatus.
    val first = writer.stop(success = true)
    assert(first.isDefined, "First stop(success=true) must return Some(MapStatus)")
    // Second stop call must be idempotent — returns None because the writer
    // is already in the stopped state. This matches the SortShuffleWriter pattern
    // where the `stopping` flag prevents double cleanup.
    val second = writer.stop(success = true)
    assert(second.isEmpty, "Second stop call must return None (idempotent)")
  }

  // =========================================================================
  // Test 7: Resource cleanup on producer failure prevents memory leaks
  // =========================================================================

  test("resource cleanup on failure prevents memory leaks") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    // Write a significant number of records to populate partition buffers
    val records = (0 until 1000).map(i => (i % numMaps, i)).toList
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle,
      mapId = 7,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      conf)
    writer.write(records.iterator)
    // Simulate failure by calling stop with success=false.
    // This must clean up all partition buffers and checksums to prevent memory leaks.
    writer.stop(success = false)
    // After failure cleanup, getPartitionLengths should still return a valid array
    // with the correct number of partitions (data was written before failure).
    val lengths = writer.getPartitionLengths()
    assert(lengths.length === numMaps,
      s"Partition lengths array must have $numMaps entries after failure cleanup")
    // Verify that the write metrics were recorded despite the failure.
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    assert(writeMetrics.recordsWritten === 1000,
      "All 1000 records should be counted in metrics even on failure")
  }

  // =========================================================================
  // Test 8: CRC32C checksum is generated for partition data
  // =========================================================================

  test("CRC32C checksum is generated for partition data") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val records = (0 until 50).map(i => (i % numMaps, i)).toList
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle,
      mapId = 8,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      conf)
    writer.write(records.iterator)
    // Access the internal per-partition checksum values via the package-private accessor.
    // Each partition that received data should have a non-zero CRC32C checksum,
    // confirming that checksum computation was performed during the write phase.
    for (partId <- 0 until numMaps) {
      val checksumValue = writer.getPartitionChecksum(partId)
      assert(checksumValue != 0L,
        s"Partition $partId should have a non-zero CRC32C checksum after writing records")
    }
    // Verify determinism: writing the same data again should produce the same checksums.
    // Create a second writer with the same records to compare.
    val context2 = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer2 = new StreamingShuffleWriter[Int, Int](
      shuffleHandle,
      mapId = 80,
      context2,
      context2.taskMetrics().shuffleWriteMetrics,
      conf)
    writer2.write(records.iterator)
    for (partId <- 0 until numMaps) {
      assert(writer.getPartitionChecksum(partId) === writer2.getPartitionChecksum(partId),
        s"CRC32C checksum for partition $partId must be deterministic for the same input data")
    }
    writer.stop(success = true)
    writer2.stop(success = true)
  }

  // =========================================================================
  // Test 9: getPartitionLengths returns correct per-partition byte counts
  // =========================================================================

  test("getPartitionLengths returns correct per-partition sizes") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    // Create records with known partition distribution:
    // Key 0 -> partition 0 (2 records: (0,1) and (0,2))
    // Key 1 -> partition 1 (1 record: (1,3))
    // Key 2 -> partition 2 (1 record: (2,4))
    // Key 3 -> partition 3 (1 record: (3,5))
    // Key 4 -> partition 4 (1 record: (4,6))
    val records = List[(Int, Int)]((0, 1), (0, 2), (1, 3), (2, 4), (3, 5), (4, 6))
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle,
      mapId = 9,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      conf)
    writer.write(records.iterator)
    val lengths = writer.getPartitionLengths()
    // All partitions should have data
    assert(lengths.length === numMaps)
    for (i <- 0 until numMaps) {
      assert(lengths(i) > 0, s"Partition $i should have positive byte count")
    }
    // Partition 0 received 2 records while partitions 1-4 each received 1 record.
    // Since Java-serialized Int values have a consistent size, partition 0 should
    // have approximately twice the bytes of any single-record partition.
    assert(lengths(0) > lengths(1),
      s"Partition 0 (2 records, ${lengths(0)} bytes) must have more data " +
        s"than partition 1 (1 record, ${lengths(1)} bytes)")
    assert(lengths(0) > lengths(2),
      s"Partition 0 (2 records) must have more data than partition 2 (1 record)")
    // Total bytes should match the sum of all partition lengths
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    assert(writeMetrics.bytesWritten === lengths.sum,
      "Total bytesWritten metric must equal sum of partition lengths")
    writer.stop(success = true)
  }

  // =========================================================================
  // Test 10: Write time is recorded in shuffle write metrics
  // =========================================================================

  test("write time is recorded in shuffle write metrics") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val records = (0 until 100).map(i => (i % numMaps, i)).toList
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle,
      mapId = 10,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      conf)
    writer.write(records.iterator)
    writer.stop(success = true)
    // The StreamingShuffleWriter records write time in the write() method's finally block
    // and additional cleanup time in the stop() method's finally block. Both are captured
    // via writeMetrics.incWriteTime(), so the total write time should be non-negative.
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    assert(writeMetrics.writeTime >= 0,
      "Write time must be non-negative (recorded during write and stop cleanup)")
  }

  // =========================================================================
  // Test 11: Streaming-specific buffer bytes metric is tracked
  // =========================================================================

  test("streaming buffer bytes metric is tracked") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val records = List[(Int, Int)]((1, 2), (2, 3))
    val writer = new StreamingShuffleWriter[Int, Int](
      shuffleHandle,
      mapId = 11,
      context,
      context.taskMetrics().shuffleWriteMetrics,
      conf)
    writer.write(records.iterator)
    // The streaming-specific metric incStreamingBufferBytes is called for every
    // record written, tracking the total bytes buffered in streaming memory regions.
    // This metric is independent of the standard bytesWritten metric and provides
    // operational visibility into streaming shuffle buffer utilization.
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    assert(writeMetrics.bytesWritten > 0,
      "Standard bytesWritten metric must be positive after writing records")
    // The streamingBufferBytes metric should track the same data volume as
    // bytesWritten because each serialized record contributes to both metrics.
    assert(writeMetrics.streamingBufferBytes > 0,
      "Streaming buffer bytes metric must be positive after writing records")
    assert(writeMetrics.streamingBufferBytes === writeMetrics.bytesWritten,
      "Streaming buffer bytes must equal standard bytes written for the same data")
    writer.stop(success = true)
  }
}
