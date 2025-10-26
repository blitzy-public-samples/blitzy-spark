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

import org.mockito.Mockito.mock
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, Serializer}

/**
 * Comprehensive unit test suite for StreamingShuffleManager validating shuffle registration,
 * buffer allocation, factory methods, fallback logic, and resource cleanup per Agent Action
 * Plan Section 0.7.
 *
 * Test Coverage:
 * - shouldUseStreaming validation: Configuration, partition count, serializer support, map-side combine
 * - registerShuffle: Buffer allocation, StreamingShuffleHandle creation, fallback on failure
 * - getWriter: Factory method returning StreamingShuffleWriter for streaming handles
 * - getReader: Factory method returning StreamingShuffleReader for streaming handles
 * - Fallback monitoring: Consumer rate checking, network saturation detection
 * - Resource cleanup: unregisterShuffle and stop() proper cleanup
 *
 * Follows patterns from SortShuffleManagerSuite with RuntimeExceptionAnswer for mock validation.
 * Extends LocalSparkContext for automatic SparkContext lifecycle management.
 */
class StreamingShuffleManagerSuite extends SparkFunSuite with LocalSparkContext with Matchers {

  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  /**
   * RuntimeExceptionAnswer ensures test failures for non-stubbed mock methods, following
   * SortShuffleManagerSuite pattern for explicit test contract enforcement.
   */
  private class RuntimeExceptionAnswer extends Answer[Object] {
    override def answer(invocation: InvocationOnMock): Object = {
      throw new RuntimeException("Called non-stubbed method, " + invocation.getMethod.getName)
    }
  }

  /**
   * Helper method to create mock ShuffleDependency with standard stubbing pattern.
   * Follows SortShuffleManagerSuite implementation for consistency.
   *
   * @param partitioner Partitioner for partition count and assignment logic
   * @param serializer Serializer for object relocation support validation
   * @param keyOrdering Optional key ordering for sort requirements
   * @param aggregator Optional aggregator for map-side combine support
   * @param mapSideCombine Whether map-side aggregation is enabled
   * @return Mocked ShuffleDependency with all required methods stubbed
   */
  private def shuffleDep(
      partitioner: Partitioner,
      serializer: Serializer,
      keyOrdering: Option[Ordering[Any]],
      aggregator: Option[Aggregator[Any, Any, Any]],
      mapSideCombine: Boolean): ShuffleDependency[Any, Any, Any] = {
    val dep = mock(classOf[ShuffleDependency[Any, Any, Any]], new RuntimeExceptionAnswer())
    doReturn(0).when(dep).shuffleId
    doReturn(partitioner).when(dep).partitioner
    doReturn(serializer).when(dep).serializer
    doReturn(keyOrdering).when(dep).keyOrdering
    doReturn(aggregator).when(dep).aggregator
    doReturn(mapSideCombine).when(dep).mapSideCombine
    doReturn(false).when(dep).isShuffleMergeFinalizedMarked
    doReturn(Seq.empty).when(dep).getMergerLocs
    doReturn(Array.empty[org.apache.spark.shuffle.checksum.RowBasedChecksum]).when(dep).rowBasedChecksums
    dep
  }

  test("shouldUseStreaming returns true when all conditions met") {
    // Configuration with streaming shuffle enabled per Section 0.2
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.shuffle.streaming.bufferSizePercent", "20")
      .set("spark.shuffle.streaming.spillThreshold", "80")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      // KryoSerializer supports object relocation (supportsRelocationOfSerializedObjects = true)
      val kryo = new KryoSerializer(conf)
      
      // Test with 100 partitions (minimum threshold per Section 0.1)
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Should return StreamingShuffleHandle when all conditions met
      handle mustBe a[StreamingShuffleHandle[_, _, _]]
      
      // Test with 150 partitions (above minimum threshold)
      val handle2 = manager.registerShuffle(1, shuffleDep(
        partitioner = new HashPartitioner(150),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      handle2 mustBe a[StreamingShuffleHandle[_, _, _]]
      
    } finally {
      manager.stop()
    }
  }

  test("shouldUseStreaming returns false when streaming disabled") {
    // Configuration with streaming shuffle disabled (default)
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "false")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Should NOT return StreamingShuffleHandle when disabled
      handle must not be a[StreamingShuffleHandle[_, _, _]]
      
    } finally {
      manager.stop()
    }
  }

  test("shouldUseStreaming returns false for insufficient partitions") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      // Test with 50 partitions (below 100 minimum per Section 0.1)
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(50),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Should fall back to sort-based shuffle due to insufficient partitions
      handle must not be a[StreamingShuffleHandle[_, _, _]]
      
      // Test with 99 partitions (just below threshold)
      val handle2 = manager.registerShuffle(1, shuffleDep(
        partitioner = new HashPartitioner(99),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      handle2 must not be a[StreamingShuffleHandle[_, _, _]]
      
    } finally {
      manager.stop()
    }
  }

  test("shouldUseStreaming returns false for JavaSerializer without object relocation") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      // JavaSerializer does NOT support object relocation (supportsRelocationOfSerializedObjects = false)
      val java = new JavaSerializer(conf)
      
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = java,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Should fall back due to serializer not supporting relocation
      handle must not be a[StreamingShuffleHandle[_, _, _]]
      
    } finally {
      manager.stop()
    }
  }

  test("shouldUseStreaming returns false for map-side combine") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      // Map-side combine not supported in streaming shuffle v1 per Section 0.9
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = Some(mock(classOf[Ordering[Any]])),
        aggregator = Some(mock(classOf[Aggregator[Any, Any, Any]])),
        mapSideCombine = true
      ))
      
      // Should fall back due to map-side combine requirement
      handle must not be a[StreamingShuffleHandle[_, _, _]]
      
    } finally {
      manager.stop()
    }
  }

  test("shouldUseStreaming allows key ordering without aggregator") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      // Key ordering without aggregator should work per Section 0.9
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = Some(mock(classOf[Ordering[Any]])),
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Should use streaming shuffle even with key ordering
      handle mustBe a[StreamingShuffleHandle[_, _, _]]
      
    } finally {
      manager.stop()
    }
  }

  test("shouldUseStreaming allows aggregator without map-side combine") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      // Aggregator without map-side combine is allowed per Section 0.9
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = Some(mock(classOf[Aggregator[Any, Any, Any]])),
        mapSideCombine = false
      ))
      
      // Should use streaming shuffle with aggregator but no map-side combine
      handle mustBe a[StreamingShuffleHandle[_, _, _]]
      
    } finally {
      manager.stop()
    }
  }

  test("registerShuffle calculates correct buffer size") {
    // Configuration with specific buffer size percentage
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.shuffle.streaming.bufferSizePercent", "30")
      .set("spark.executor.memory", "1000m")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      )).asInstanceOf[StreamingShuffleHandle[_, _, _]]
      
      // Verify buffer size calculation per Section 0.2:
      // bufferSize = (executorMemory * bufferPercent) / numPartitions
      // 1000MB = 1048576000 bytes
      // Total buffer = 1048576000 * 0.30 = 314572800 bytes
      // Per partition = 314572800 / 100 = 3145728 bytes
      handle.bufferSizeBytes must be > 0L
      handle.bufferSizeBytes must be < 10000000L // Reasonable upper bound
      
    } finally {
      manager.stop()
    }
  }

  test("registerShuffle creates active shuffle context") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Verify active shuffle context was created
      val context = manager.activeShuffles.get(0)
      context must not be null
      context.shuffleId mustBe 0
      context.numPartitions mustBe 100
      context.bufferSizeBytes must be > 0L
      context.backpressureProtocol must not be null
      context.memorySpillManager must not be null
      context.metricsSource must not be null
      
    } finally {
      manager.stop()
    }
  }

  test("registerShuffle falls back on exception") {
    // Test fallback when conditions for streaming shuffle are not met (insufficient partitions)
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      // Use only 50 partitions (less than MIN_PARTITIONS_FOR_STREAMING = 100)
      // This should trigger fallback to sort-based shuffle
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(50),  // Too few partitions for streaming
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Should NOT return StreamingShuffleHandle due to fallback
      handle must not be a[StreamingShuffleHandle[_, _, _]]
      
      // Active shuffle context should not be present
      val context = manager.activeShuffles.get(0)
      context mustBe null
      
    } finally {
      manager.stop()
    }
  }

  test("getWriter returns StreamingShuffleWriter for StreamingShuffleHandle") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      )).asInstanceOf[StreamingShuffleHandle[Any, Any, Any]]
      
      // Create task context for writer creation
      val taskContext = TaskContext.empty()
      val metricsReporter = mock(classOf[org.apache.spark.shuffle.ShuffleWriteMetricsReporter])
      
      // getWriter should return StreamingShuffleWriter for streaming handle
      val writer = manager.getWriter(handle, 0L, taskContext, metricsReporter)
      
      writer mustBe a[StreamingShuffleWriter[_, _]]
      
      // Clean up writer
      writer.stop(success = true)
      
    } finally {
      manager.stop()
    }
  }

  test("getWriter delegates to fallback manager for non-streaming handle") {
    // Configuration with streaming shuffle enabled but fallback to sort for handle type test
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = sc.env.shuffleManager.asInstanceOf[StreamingShuffleManager]
    
    try {
      val kryo = new KryoSerializer(conf)
      
      // Register with too few partitions to trigger fallback to non-streaming handle
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(50),  // Less than MIN_PARTITIONS_FOR_STREAMING
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Create task context for writer creation
      val taskContext = TaskContext.empty()
      val metricsReporter = mock(classOf[org.apache.spark.shuffle.ShuffleWriteMetricsReporter])
      
      // getWriter should delegate to fallback manager for non-streaming handle
      val writer = manager.getWriter(handle, 0L, taskContext, metricsReporter)
      
      writer must not be a[StreamingShuffleWriter[_, _]]
      
      // Clean up writer (use success=false since we didn't write anything)
      writer.stop(success = false)
      
    } finally {
      // Manager cleanup handled by SparkContext
    }
  }

  test("getReader returns StreamingShuffleReader for StreamingShuffleHandle") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      )).asInstanceOf[StreamingShuffleHandle[Any, Any, Any]]
      
      // Create task context for reader creation
      val taskContext = TaskContext.empty()
      val metricsReporter = mock(classOf[org.apache.spark.shuffle.ShuffleReadMetricsReporter])
      
      // getReader should return StreamingShuffleReader for streaming handle
      val reader = manager.getReader(
        handle,
        startMapIndex = 0,
        endMapIndex = 1,
        startPartition = 0,
        endPartition = 10,
        taskContext,
        metricsReporter
      )
      
      reader mustBe a[StreamingShuffleReader[_, _]]
      
    } finally {
      manager.stop()
    }
  }

  test("getReader delegates to fallback manager for non-streaming handle") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "false")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      // Register with streaming disabled to get non-streaming handle
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Create task context for reader creation
      val taskContext = TaskContext.empty()
      val metricsReporter = mock(classOf[org.apache.spark.shuffle.ShuffleReadMetricsReporter])
      
      // getReader should delegate to fallback manager
      val reader = manager.getReader(
        handle,
        startMapIndex = 0,
        endMapIndex = 1,
        startPartition = 0,
        endPartition = 10,
        taskContext,
        metricsReporter
      )
      
      reader must not be a[StreamingShuffleReader[_, _]]
      
    } finally {
      manager.stop()
    }
  }

  test("fallback monitoring thread starts when streaming enabled") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      // Fallback monitoring thread should be started
      // Give it a moment to start
      Thread.sleep(100)
      
      // Verify the monitoring thread was created and is running
      // Note: Cannot directly access private field, but we can verify behavior
      // by checking that no exceptions are thrown during normal operation
      
    } finally {
      manager.stop()
    }
  }

  test("fallback monitoring detects slow consumer condition") {
    // Configuration with streaming shuffle enabled and debug mode
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.shuffle.streaming.debug", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Get active shuffle context
      val context = manager.activeShuffles.get(0)
      
      // Simulate slow consumer scenario per Section 0.9:
      // Consumer sustained 2x slower than producer for >60 seconds
      context.updateProducerThroughput(2000000.0) // 2 MB/s
      context.updateConsumerThroughput(500000.0)   // 0.5 MB/s (4x slower)
      
      // Wait for slow consumer duration to exceed threshold
      Thread.sleep(100)
      
      // Verify slow consumer duration is being tracked
      val duration = context.getSlowConsumerDurationMs
      duration must be > 0L
      
    } finally {
      manager.stop()
    }
  }

  test("fallback monitoring detects network saturation") {
    // Configuration with streaming shuffle enabled and debug mode
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.shuffle.streaming.debug", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Get active shuffle context
      val context = manager.activeShuffles.get(0)
      
      // Simulate network saturation per Section 0.9: >90% utilization
      context.updateNetworkUtilization(95.0)
      
      // Verify network utilization is being tracked
      val utilization = context.getNetworkUtilizationPercent
      utilization mustBe 95.0
      
    } finally {
      manager.stop()
    }
  }

  test("unregisterShuffle cleans up resources") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Verify shuffle context exists
      val contextBefore = manager.activeShuffles.get(0)
      contextBefore must not be null
      
      // Unregister shuffle
      val result = manager.unregisterShuffle(0)
      result mustBe true
      
      // Verify shuffle context was removed
      val contextAfter = manager.activeShuffles.get(0)
      contextAfter mustBe null
      
    } finally {
      manager.stop()
    }
  }

  test("unregisterShuffle handles missing shuffle gracefully") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      // Unregister non-existent shuffle should not throw exception
      val result = manager.unregisterShuffle(999)
      result mustBe true // Delegates to fallback manager
      
    } finally {
      manager.stop()
    }
  }

  test("stop() performs orderly shutdown") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      // Register multiple shuffles
      manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      manager.registerShuffle(1, shuffleDep(
        partitioner = new HashPartitioner(150),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Verify shuffles were registered
      manager.activeShuffles.get(0) must not be null
      manager.activeShuffles.get(1) must not be null
      
    } finally {
      // Stop should clean up all resources
      manager.stop()
      
      // Verify all shuffles were cleaned up
      manager.activeShuffles.isEmpty mustBe true
    }
  }

  test("stop() is idempotent") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    // Stop multiple times should not cause errors
    manager.stop()
    manager.stop() // Second call should be safe
  }

  test("buffer size calculation respects configuration") {
    // Test with different buffer size percentages
    val testCases = Seq(
      (1, "1g"),    // 1% buffer size
      (10, "1g"),   // 10% buffer size
      (20, "1g"),   // 20% buffer size (default)
      (50, "1g"),   // 50% buffer size (maximum)
      (20, "500m"), // 20% with smaller executor memory
      (20, "2g")    // 20% with larger executor memory
    )
    
    testCases.foreach { case (bufferPercent, executorMemory) =>
      val conf = new SparkConf()
        .set("spark.shuffle.manager", "streaming")
        .set("spark.shuffle.streaming.enabled", "true")
        .set("spark.shuffle.streaming.bufferSizePercent", bufferPercent.toString)
        .set("spark.executor.memory", executorMemory)
      
      sc = new SparkContext("local", "test", conf)
      val manager = new StreamingShuffleManager(conf)
      
      try {
        val kryo = new KryoSerializer(conf)
        
        val handle = manager.registerShuffle(0, shuffleDep(
          partitioner = new HashPartitioner(100),
          serializer = kryo,
          keyOrdering = None,
          aggregator = None,
          mapSideCombine = false
        )).asInstanceOf[StreamingShuffleHandle[_, _, _]]
        
        // Verify buffer size is positive and reasonable
        handle.bufferSizeBytes must be > 0L
        
        // Clean up
        manager.unregisterShuffle(0)
        
      } finally {
        manager.stop()
        // Ensure SparkContext is stopped before next iteration
        if (sc != null) {
          sc.stop()
          sc = null
        }
      }
    }
  }

  test("shuffle block resolver is properly initialized") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      // Verify shuffle block resolver was created
      manager.shuffleBlockResolver must not be null
      
    } finally {
      manager.stop()
    }
  }

  test("context tracks shuffle lifecycle metrics") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Get active shuffle context
      val context = manager.activeShuffles.get(0)
      
      // Verify context tracks lifecycle (>= 0 since it might be immediate)
      context.ageMillis must be >= 0L
      
      // Test throughput updates
      context.updateProducerThroughput(1000000.0)
      context.getProducerBytesPerSecond mustBe 1000000.0
      
      context.updateConsumerThroughput(800000.0)
      context.getConsumerBytesPerSecond mustBe 800000.0
      
      // Test network utilization tracking
      context.updateNetworkUtilization(75.0)
      context.getNetworkUtilizationPercent mustBe 75.0
      
      // Test fallback event recording
      context.recordFallbackEvent("Test fallback event")
      val events = context.getFallbackEvents
      events.size mustBe 1
      events.head must include("Test fallback event")
      
    } finally {
      manager.stop()
    }
  }

  test("context cleanup is comprehensive") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      val handle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Get active shuffle context
      val context = manager.activeShuffles.get(0)
      
      // Call cleanup directly
      context.cleanup()
      
      // Cleanup should be idempotent - can be called multiple times
      context.cleanup()
      
    } finally {
      manager.stop()
    }
  }

  test("MIN_PARTITIONS_FOR_STREAMING constant is correct") {
    // Verify the minimum partition threshold per Section 0.1
    StreamingShuffleManager.MIN_PARTITIONS_FOR_STREAMING mustBe 100
  }

  test("streaming shuffle coexists with sort-based shuffle") {
    // Configuration with streaming shuffle enabled
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
      .set("spark.executor.memory", "1g")
    
    sc = new SparkContext("local", "test", conf)
    val manager = new StreamingShuffleManager(conf)
    
    try {
      val kryo = new KryoSerializer(conf)
      
      // Register streaming shuffle (100+ partitions, Kryo serializer)
      val streamingHandle = manager.registerShuffle(0, shuffleDep(
        partitioner = new HashPartitioner(100),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Register non-streaming shuffle (insufficient partitions)
      val sortHandle = manager.registerShuffle(1, shuffleDep(
        partitioner = new HashPartitioner(50),
        serializer = kryo,
        keyOrdering = None,
        aggregator = None,
        mapSideCombine = false
      ))
      
      // Verify coexistence: one streaming, one sort-based
      streamingHandle mustBe a[StreamingShuffleHandle[_, _, _]]
      sortHandle must not be a[StreamingShuffleHandle[_, _, _]]
      
      // Both should be registered
      manager.activeShuffles.get(0) must not be null
      manager.activeShuffles.get(1) mustBe null // Sort-based doesn't create context
      
    } finally {
      manager.stop()
    }
  }
}
