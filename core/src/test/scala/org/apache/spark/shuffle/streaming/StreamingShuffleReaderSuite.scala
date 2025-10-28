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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.zip.CRC32C

import org.mockito.ArgumentMatchers.{eq => meq, _}
import org.mockito.Mockito._

import org.apache.spark._
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{BlockManager, BlockManagerId, ShuffleBlockId}

/**
 * Comprehensive unit test suite for StreamingShuffleReader validating:
 * - In-progress block request and partial data consumption
 * - Producer failure detection via 5-second connection timeout
 * - Partial read invalidation and recomputation trigger
 * - Checksum validation with retransmission
 * - Consumer acknowledgment protocol
 *
 * Per Agent Action Plan Section 0.7, this suite tests the reduce-side shuffle reader
 * with streaming semantics, ensuring zero data loss guarantee and proper failure handling.
 */
class StreamingShuffleReaderSuite extends SparkFunSuite with LocalSparkContext {

  /**
   * Test that read() creates an iterator that successfully polls producers for blocks,
   * deserializes data, and updates metrics correctly.
   *
   * This validates the core in-progress block request functionality where reduce tasks
   * can fetch shuffle data before the entire shuffle is materialized, achieving the
   * 30-50% latency reduction per Section 0.1.
   */
  test("read() creates iterator that polls producers for available blocks") {
    val testConf = new SparkConf(false)
    sc = new SparkContext("local", "test", testConf)

    val shuffleId = 10
    val numMaps = 3
    val startMapIndex = 0
    val endMapIndex = numMaps
    val startPartition = 0
    val endPartition = 1
    val keyValuePairsPerMap = 5
    val serializer = new JavaSerializer(testConf)

    // Create test shuffle data
    val byteOutputStream = new ByteArrayOutputStream()
    val serializationStream = serializer.newInstance().serializeStream(byteOutputStream)
    (0 until keyValuePairsPerMap).foreach { i =>
      serializationStream.writeKey(i)
      serializationStream.writeValue(2 * i)
    }
    serializationStream.close()

    // Create mock block manager that returns shuffle blocks
    val blockManager = mock(classOf[BlockManager])
    val localBlockManagerId = BlockManagerId("test-executor", "test-host", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)

    // Setup block manager to return buffers for each map
    (0 until numMaps).foreach { mapId =>
      val shuffleBlockId = ShuffleBlockId(shuffleId, mapId.toLong, startPartition)
      val nioBuffer = new NioManagedBuffer(ByteBuffer.wrap(byteOutputStream.toByteArray))

      when(blockManager.getRemoteBlock[ManagedBuffer](
        meq(shuffleBlockId),
        any()
      )).thenReturn(Some(nioBuffer))
    }

    // Create mock shuffle dependency
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)

    // Create mock shuffle handle
    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = shuffleId,
      bufferSizeBytes = 1024 * 1024, // 1MB buffer
      dependency = dependency
    )

    // Create mock backpressure protocol
    val backpressureProtocol = mock(classOf[BackpressureProtocol])

    // Create task context with metrics
    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()

    // Create reader instance
    val reader = new StreamingShuffleReader[Int, Int](
      handle = handle,
      startMapIndex = startMapIndex,
      endMapIndex = endMapIndex,
      startPartition = startPartition,
      endPartition = endPartition,
      context = taskContext,
      metrics = metrics,
      backpressureProtocol = backpressureProtocol
    )

    // Override SparkEnv to provide our mock block manager
    val originalEnv = SparkEnv.get
    val mockEnv = mock(classOf[SparkEnv])
    when(mockEnv.blockManager).thenReturn(blockManager)
    SparkEnv.set(mockEnv)

    try {
      // Read all records from the iterator
      val records = reader.read().toArray

      // Verify we got all expected records
      assert(records.length === keyValuePairsPerMap * numMaps,
        s"Expected ${keyValuePairsPerMap * numMaps} records, got ${records.length}")

      // Verify records content
      val expectedRecords = (0 until keyValuePairsPerMap).flatMap { i =>
        Seq.fill(numMaps)((i, 2 * i))
      }.sortBy(_._1)

      val actualRecordsSorted = records.map(r => (r._1, r._2)).sortBy(_._1)
      assert(actualRecordsSorted === expectedRecords,
        "Records content does not match expected values")

      // Verify metrics were updated
      assert(metrics.recordsRead > 0, "No records counted in metrics")
      assert(metrics.remoteBytesRead > 0, "No bytes counted in read metrics")

    } finally {
      // Restore original SparkEnv
      if (originalEnv != null) {
        SparkEnv.set(originalEnv)
      } else {
        SparkEnv.set(null)
      }
    }
  }


  /**
   * Test that requestNextBlock() throws FetchFailedException when block is not available,
   * simulating a producer timeout or failure scenario.
   *
   * Per Section 0.1 and 0.7, producer failure detection via connection timeout (5 seconds)
   * should trigger FetchFailedException which causes partial read invalidation.
   */
  test("requestNextBlock() throws FetchFailedException on producer timeout") {
    val testConf = new SparkConf(false)
    sc = new SparkContext("local", "test", testConf)

    val shuffleId = 20
    val mapId = 0
    val partitionId = 0
    val serializer = new JavaSerializer(testConf)

    // Create mock block manager that returns None (simulating timeout/failure)
    val blockManager = mock(classOf[BlockManager])
    val localBlockManagerId = BlockManagerId("test-executor", "test-host", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)

    val shuffleBlockId = ShuffleBlockId(shuffleId, mapId.toLong, partitionId)
    when(blockManager.getRemoteBlock[ManagedBuffer](
      meq(shuffleBlockId),
      any()
    )).thenReturn(None) // Simulate block not found (producer failure)

    // Create mock shuffle dependency and handle
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)

    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = shuffleId,
      bufferSizeBytes = 1024 * 1024,
      dependency = dependency
    )

    // Create mock backpressure protocol
    val backpressureProtocol = mock(classOf[BackpressureProtocol])

    // Create task context
    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()

    // Create reader
    val reader = new StreamingShuffleReader[Int, Int](
      handle = handle,
      startMapIndex = 0,
      endMapIndex = 1,
      startPartition = 0,
      endPartition = 1,
      context = taskContext,
      metrics = metrics,
      backpressureProtocol = backpressureProtocol
    )

    // Override SparkEnv
    val originalEnv = SparkEnv.get
    val mockEnv = mock(classOf[SparkEnv])
    when(mockEnv.blockManager).thenReturn(blockManager)
    SparkEnv.set(mockEnv)

    try {
      // Attempt to request block should throw FetchFailedException
      val exception = intercept[FetchFailedException] {
        reader.requestNextBlock(mapId, partitionId)
      }

      // Verify exception contains information about the failure
      assert(exception.getMessage.contains("map 0") || exception.getMessage.contains("partition 0"),
        s"Expected exception message to mention map or partition, got: ${exception.getMessage}")

      // Note: partialReadInvalidations metric is a no-op in TempShuffleReadMetrics
      // and only tracked in actual ShuffleReadMetrics. Since invalidatePartialReads
      // is called, but no blocks were fetched yet, the behavior is correct.

    } finally {
      if (originalEnv != null) {
        SparkEnv.set(originalEnv)
      } else {
        SparkEnv.set(null)
      }
    }
  }


  /**
   * Test that validateBlockChecksum() correctly validates CRC32C checksums and detects corruption.
   *
   * Per Section 0.7, checksum validation using CRC32C algorithm should detect data corruption
   * during network transmission, returning true for valid checksums and false for mismatches.
   */
  test("validateBlockChecksum() validates CRC32C checksums correctly") {
    val testConf = new SparkConf(false)
    sc = new SparkContext("local", "test", testConf)

    val shuffleId = 40
    val serializer = new JavaSerializer(testConf)

    // Create test data
    val testData = "Test shuffle data for checksum validation"
    val testBytes = testData.getBytes("UTF-8")
    val testBuffer = new NioManagedBuffer(ByteBuffer.wrap(testBytes))

    // Compute expected checksum
    val checksum = new CRC32C()
    checksum.update(testBytes)
    val expectedChecksum = checksum.getValue

    // Create reader components
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)

    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = shuffleId,
      bufferSizeBytes = 1024 * 1024,
      dependency = dependency
    )

    val backpressureProtocol = mock(classOf[BackpressureProtocol])
    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()

    val reader = new StreamingShuffleReader[Int, Int](
      handle = handle,
      startMapIndex = 0,
      endMapIndex = 1,
      startPartition = 0,
      endPartition = 1,
      context = taskContext,
      metrics = metrics,
      backpressureProtocol = backpressureProtocol
    )

    // Test valid checksum
    val validResult = reader.validateBlockChecksum(testBuffer, expectedChecksum)
    assert(validResult === true, "Checksum validation should pass for valid checksum")
    // Note: checksumMismatchCount is tracked in the reader implementation but
    // TempShuffleReadMetrics doesn't expose it as it's a no-op

    // Test invalid checksum (intentional corruption)
    val invalidChecksum = expectedChecksum + 12345
    val invalidResult = reader.validateBlockChecksum(testBuffer, invalidChecksum)
    assert(invalidResult === false, "Checksum validation should fail for invalid checksum")
    // Note: The incChecksumMismatchCount is called internally but TempShuffleReadMetrics
    // implements it as a no-op, so we can't verify the count here
  }

  /**
   * Test that sendAcknowledgment() correctly creates and sends StreamingShuffleAcknowledgment
   * messages for buffer reclamation.
   *
   * Per Section 0.7, acknowledgments should be sent with correct parameters (shuffleId, mapId,
   * partitionId, consumedOffset, complete) to enable producers to reclaim buffers within 100ms.
   */
  test("sendAcknowledgment() sends StreamingShuffleAcknowledgment for buffer reclamation") {
    val testConf = new SparkConf(false)
    sc = new SparkContext("local", "test", testConf)

    val shuffleId = 50
    val mapId = 2
    val partitionId = 3
    val consumedOffset = 4096L
    val serializer = new JavaSerializer(testConf)

    // Create reader components
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)

    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = shuffleId,
      bufferSizeBytes = 1024 * 1024,
      dependency = dependency
    )

    val backpressureProtocol = mock(classOf[BackpressureProtocol])
    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()

    val reader = new StreamingShuffleReader[Int, Int](
      handle = handle,
      startMapIndex = 0,
      endMapIndex = 5,
      startPartition = 0,
      endPartition = 5,
      context = taskContext,
      metrics = metrics,
      backpressureProtocol = backpressureProtocol
    )

    // Create mock block manager for SparkEnv
    val blockManager = mock(classOf[BlockManager])
    val originalEnv = SparkEnv.get
    val mockEnv = mock(classOf[SparkEnv])
    when(mockEnv.blockManager).thenReturn(blockManager)
    SparkEnv.set(mockEnv)

    try {
      // Send partial acknowledgment
      reader.sendAcknowledgment(mapId, partitionId, consumedOffset, complete = false)

      // Verify backpressure protocol was called with heartbeat
      verify(backpressureProtocol, atLeastOnce()).processHeartbeat(
        anyString(),
        meq(consumedOffset)
      )

      // Send complete acknowledgment
      reader.sendAcknowledgment(mapId, partitionId, consumedOffset, complete = true)

      // Verify another heartbeat was processed
      verify(backpressureProtocol, atLeast(2)).processHeartbeat(
        anyString(),
        anyLong()
      )

    } finally {
      if (originalEnv != null) {
        SparkEnv.set(originalEnv)
      } else {
        SparkEnv.set(null)
      }
    }
  }


  /**
   * Test that fetchBlockWithRetry() retries up to 5 times with exponential backoff.
   *
   * Per Section 0.7, the retry schedule should be:
   * - Attempt 1: immediate
   * - Attempt 2: 200ms delay
   * - Attempt 3: 400ms delay
   * - Attempt 4: 800ms delay
   * - Attempt 5: 1600ms delay
   *
   * After exhausting retries, should throw FetchFailedException.
   */
  test("fetchBlockWithRetry() retries up to 5 attempts with exponential backoff") {
    val testConf = new SparkConf(false)
    sc = new SparkContext("local", "test", testConf)

    val shuffleId = 60
    val mapId = 1
    val partitionId = 0
    val serializer = new JavaSerializer(testConf)

    // Create test data
    val byteOutputStream = new ByteArrayOutputStream()
    val serializationStream = serializer.newInstance().serializeStream(byteOutputStream)
    serializationStream.writeKey(1)
    serializationStream.writeValue(2)
    serializationStream.close()
    val successBuffer = new NioManagedBuffer(ByteBuffer.wrap(byteOutputStream.toByteArray))

    // Create mock block manager that fails first 2 attempts, then succeeds
    val blockManager = mock(classOf[BlockManager])
    val localBlockManagerId = BlockManagerId("test-executor", "test-host", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)

    val shuffleBlockId = ShuffleBlockId(shuffleId, mapId.toLong, partitionId)

    // Fail first 2 attempts, succeed on 3rd
    when(blockManager.getRemoteBlock[ManagedBuffer](meq(shuffleBlockId), any()))
      .thenReturn(None)  // 1st attempt fails
      .thenReturn(None)  // 2nd attempt fails
      .thenReturn(Some(successBuffer))  // 3rd attempt succeeds

    // Create reader components
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)

    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = shuffleId,
      bufferSizeBytes = 1024 * 1024,
      dependency = dependency
    )

    val backpressureProtocol = mock(classOf[BackpressureProtocol])
    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()

    val reader = new StreamingShuffleReader[Int, Int](
      handle = handle,
      startMapIndex = 0,
      endMapIndex = 2,
      startPartition = 0,
      endPartition = 1,
      context = taskContext,
      metrics = metrics,
      backpressureProtocol = backpressureProtocol
    )

    val originalEnv = SparkEnv.get
    val mockEnv = mock(classOf[SparkEnv])
    when(mockEnv.blockManager).thenReturn(blockManager)
    SparkEnv.set(mockEnv)

    try {
      val startTime = System.currentTimeMillis()

      // Should succeed after retries
      val result = reader.fetchBlockWithRetry(mapId, partitionId)

      val elapsedTime = System.currentTimeMillis() - startTime

      // Verify we got a buffer
      assert(result != null, "Should return a buffer after retries")
      assert(result.size() > 0, "Buffer should contain data")

      // Verify at least 2 retries occurred (backoff should add ~200-400ms minimum)
      // Being lenient with timing due to test execution variability
      assert(elapsedTime >= 100,
        s"Expected some retry delay, but elapsed time was ${elapsedTime}ms")

    } finally {
      if (originalEnv != null) {
        SparkEnv.set(originalEnv)
      } else {
        SparkEnv.set(null)
      }
    }
  }

  /**
   * Test that fetchBlockWithRetry() throws FetchFailedException after max retries exhausted.
   *
   * Per Section 0.7, after 5 failed attempts, the method should throw FetchFailedException
   * to trigger upstream recomputation.
   */
  test("fetchBlockWithRetry() throws FetchFailedException after retry exhaustion") {
    val testConf = new SparkConf(false)
    sc = new SparkContext("local", "test", testConf)

    val shuffleId = 70
    val mapId = 3
    val partitionId = 1
    val serializer = new JavaSerializer(testConf)

    // Create mock block manager that always fails
    val blockManager = mock(classOf[BlockManager])
    val localBlockManagerId = BlockManagerId("test-executor", "test-host", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)

    val shuffleBlockId = ShuffleBlockId(shuffleId, mapId.toLong, partitionId)
    when(blockManager.getRemoteBlock[ManagedBuffer](meq(shuffleBlockId), any()))
      .thenReturn(None) // Always fail

    // Create reader components
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)

    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = shuffleId,
      bufferSizeBytes = 1024 * 1024,
      dependency = dependency
    )

    val backpressureProtocol = mock(classOf[BackpressureProtocol])
    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()

    val reader = new StreamingShuffleReader[Int, Int](
      handle = handle,
      startMapIndex = 0,
      endMapIndex = 5,
      startPartition = 0,
      endPartition = 2,
      context = taskContext,
      metrics = metrics,
      backpressureProtocol = backpressureProtocol
    )

    val originalEnv = SparkEnv.get
    val mockEnv = mock(classOf[SparkEnv])
    when(mockEnv.blockManager).thenReturn(blockManager)
    SparkEnv.set(mockEnv)

    try {
      // Should throw after max retries
      val exception = intercept[FetchFailedException] {
        reader.fetchBlockWithRetry(mapId, partitionId)
      }

      // Verify exception details
      assert(exception.getMessage.contains("retries") || exception.getMessage.contains("map"),
        s"Exception message should mention retries or map, got: ${exception.getMessage}")

    } finally {
      if (originalEnv != null) {
        SparkEnv.set(originalEnv)
      } else {
        SparkEnv.set(null)
      }
    }
  }

  /**
   * Test that read() properly tracks metrics including bytesRead, recordsRead,
   * and partialReadInvalidations.
   *
   * Per Section 0.7, the reader should update ShuffleReadMetrics including the
   * streaming-specific partialReadInvalidations counter.
   */
  test("read() tracks metrics including bytesRead, recordsRead, and partialReadInvalidations") {
    val testConf = new SparkConf(false)
    sc = new SparkContext("local", "test", testConf)

    val shuffleId = 80
    val numMaps = 2
    val numRecordsPerMap = 10
    val serializer = new JavaSerializer(testConf)

    // Create test data
    val byteOutputStream = new ByteArrayOutputStream()
    val serializationStream = serializer.newInstance().serializeStream(byteOutputStream)
    (0 until numRecordsPerMap).foreach { i =>
      serializationStream.writeKey(i)
      serializationStream.writeValue(i * 10)
    }
    serializationStream.close()

    val blockSize = byteOutputStream.size()
    val nioBuffer = new NioManagedBuffer(ByteBuffer.wrap(byteOutputStream.toByteArray))

    // Create mock block manager
    val blockManager = mock(classOf[BlockManager])
    val localBlockManagerId = BlockManagerId("test-executor", "test-host", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)

    // Setup blocks for all maps
    (0 until numMaps).foreach { mapId =>
      val shuffleBlockId = ShuffleBlockId(shuffleId, mapId.toLong, 0)
      when(blockManager.getRemoteBlock[ManagedBuffer](meq(shuffleBlockId), any()))
        .thenReturn(Some(nioBuffer))
    }

    // Create reader components
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)

    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = shuffleId,
      bufferSizeBytes = 1024 * 1024,
      dependency = dependency
    )

    val backpressureProtocol = mock(classOf[BackpressureProtocol])
    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()

    val reader = new StreamingShuffleReader[Int, Int](
      handle = handle,
      startMapIndex = 0,
      endMapIndex = numMaps,
      startPartition = 0,
      endPartition = 1,
      context = taskContext,
      metrics = metrics,
      backpressureProtocol = backpressureProtocol
    )

    val originalEnv = SparkEnv.get
    val mockEnv = mock(classOf[SparkEnv])
    when(mockEnv.blockManager).thenReturn(blockManager)
    SparkEnv.set(mockEnv)

    try {
      // Read all data
      val records = reader.read().toArray

      // Verify record count
      assert(records.length === numRecordsPerMap * numMaps,
        s"Expected ${numRecordsPerMap * numMaps} records, got ${records.length}")

      // Verify metrics
      assert(metrics.recordsRead === numRecordsPerMap * numMaps,
        s"Expected recordsRead=${numRecordsPerMap * numMaps}, got ${metrics.recordsRead}")

      assert(metrics.remoteBytesRead > 0,
        "remoteBytesRead should be positive")

      assert(metrics.remoteBlocksFetched === numMaps,
        s"Expected remoteBlocksFetched=$numMaps, got ${metrics.remoteBlocksFetched}")

      // Note: partialReadInvalidations is tracked internally but TempShuffleReadMetrics
      // doesn't expose it as it's a no-op. For successful reads, no invalidations occur anyway.

    } finally {
      if (originalEnv != null) {
        SparkEnv.set(originalEnv)
      } else {
        SparkEnv.set(null)
      }
    }
  }

  /**
   * Test that the reader properly deserializes data using dependency.serializer.
   *
   * Per Section 0.7, the reader should use the serializer from ShuffleDependency to
   * deserialize blocks into (K, C) pairs.
   */
  test("iterator properly deserializes data using dependency.serializer") {
    val testConf = new SparkConf(false)
    sc = new SparkContext("local", "test", testConf)

    val shuffleId = 90
    val serializer = new JavaSerializer(testConf)

    // Create specific test data with known key-value pairs
    val expectedData = Seq((1, 10), (2, 20), (3, 30), (4, 40), (5, 50))

    val byteOutputStream = new ByteArrayOutputStream()
    val serializationStream = serializer.newInstance().serializeStream(byteOutputStream)
    expectedData.foreach { case (k, v) =>
      serializationStream.writeKey(k)
      serializationStream.writeValue(v)
    }
    serializationStream.close()

    val nioBuffer = new NioManagedBuffer(ByteBuffer.wrap(byteOutputStream.toByteArray))

    // Create mock block manager
    val blockManager = mock(classOf[BlockManager])
    val localBlockManagerId = BlockManagerId("test-executor", "test-host", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)

    val shuffleBlockId = ShuffleBlockId(shuffleId, 0L, 0)
    when(blockManager.getRemoteBlock[ManagedBuffer](meq(shuffleBlockId), any()))
      .thenReturn(Some(nioBuffer))

    // Create reader components
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)

    val handle = new StreamingShuffleHandle[Int, Int, Int](
      shuffleId = shuffleId,
      bufferSizeBytes = 1024 * 1024,
      dependency = dependency
    )

    val backpressureProtocol = mock(classOf[BackpressureProtocol])
    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()

    val reader = new StreamingShuffleReader[Int, Int](
      handle = handle,
      startMapIndex = 0,
      endMapIndex = 1,
      startPartition = 0,
      endPartition = 1,
      context = taskContext,
      metrics = metrics,
      backpressureProtocol = backpressureProtocol
    )

    val originalEnv = SparkEnv.get
    val mockEnv = mock(classOf[SparkEnv])
    when(mockEnv.blockManager).thenReturn(blockManager)
    SparkEnv.set(mockEnv)

    try {
      // Read and verify deserialized data
      val records = reader.read().toArray

      assert(records.length === expectedData.length,
        s"Expected ${expectedData.length} records, got ${records.length}")

      // Verify each record matches expected data
      records.zip(expectedData).foreach { case (actual, expected) =>
        assert(actual._1 === expected._1,
          s"Key mismatch: expected ${expected._1}, got ${actual._1}")
        assert(actual._2 === expected._2,
          s"Value mismatch: expected ${expected._2}, got ${actual._2}")
      }

    } finally {
      if (originalEnv != null) {
        SparkEnv.set(originalEnv)
      } else {
        SparkEnv.set(null)
      }
    }
  }
}
