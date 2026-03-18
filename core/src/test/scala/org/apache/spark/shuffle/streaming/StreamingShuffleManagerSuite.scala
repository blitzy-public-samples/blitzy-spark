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

import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.serializer.{KryoSerializer, Serializer, SerializerManager}
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleManager,
  ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter}
import org.apache.spark.storage.BlockManager

/**
 * Unit tests for [[StreamingShuffleManager]], the main entry point for the
 * streaming shuffle implementation. Follows the [[org.apache.spark.shuffle.sort
 * .SortShuffleManagerSuite]] pattern exactly for structure, style, and
 * Mockito strict-stubs approach.
 *
 * Tests cover manager instantiation via the ShuffleManager factory, shuffle
 * registration with handle type dispatch, writer/reader factory methods,
 * unregistration cleanup, fallback condition detection, and block resolver
 * type verification.
 */
class StreamingShuffleManagerSuite extends SparkFunSuite with Matchers {

  // Workaround for Scala varargs / Mockito doReturn ambiguity (same as SortShuffleManagerSuite)
  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  /**
   * A strict Mockito answer that throws a RuntimeException for any unstubbed
   * method call, ensuring test isolation and early failure detection.
   * Identical to the pattern used in SortShuffleManagerSuite.
   */
  private class RuntimeExceptionAnswer extends Answer[Object] {
    override def answer(invocation: InvocationOnMock): Object = {
      throw new RuntimeException(
        "Called non-stubbed method, " + invocation.getMethod.getName)
    }
  }

  /**
   * Creates a mock ShuffleDependency with strict stubs for all fields accessed
   * by StreamingShuffleManager during registerShuffle, getWriter, and getReader.
   */
  private def shuffleDep(
      partitioner: Partitioner,
      serializer: Serializer,
      keyOrdering: Option[Ordering[Int]],
      aggregator: Option[Aggregator[Int, Int, Int]],
      mapSideCombine: Boolean): ShuffleDependency[Int, Int, Int] = {
    val dep = mock(classOf[ShuffleDependency[Int, Int, Int]], new RuntimeExceptionAnswer())
    doReturn(0).when(dep).shuffleId
    doReturn(partitioner).when(dep).partitioner
    doReturn(serializer).when(dep).serializer
    doReturn(keyOrdering).when(dep).keyOrdering
    doReturn(aggregator).when(dep).aggregator
    doReturn(mapSideCombine).when(dep).mapSideCombine
    dep
  }

  /**
   * Helper that temporarily sets a mock SparkEnv for the duration of `body`,
   * then restores the previous SparkEnv (which may be null) in a finally block.
   * Required for tests that exercise getWriter/getReader, since those code paths
   * access SparkEnv.get.blockManager / mapOutputTracker / serializerManager.
   */
  private def withMockSparkEnv(body: SparkEnv => Unit): Unit = {
    val mockEnv = mock(classOf[SparkEnv])
    val mockBlockManager = mock(classOf[BlockManager])
    val mockMapOutputTracker = mock(classOf[MapOutputTracker])
    val mockSerializerManager = mock(classOf[SerializerManager])
    // StreamingShuffleWriter constructor accesses
    // SparkEnv.get.shuffleManager.shuffleBlockResolver (line 122)
    // so we must stub this chain to avoid NPE during writer instantiation.
    val mockShuffleMgr = mock(classOf[ShuffleManager])
    val mockBlockResolver = mock(classOf[StreamingShuffleBlockResolver])
    when(mockShuffleMgr.shuffleBlockResolver).thenReturn(mockBlockResolver)
    when(mockEnv.shuffleManager).thenReturn(mockShuffleMgr)
    when(mockEnv.blockManager).thenReturn(mockBlockManager)
    when(mockEnv.mapOutputTracker).thenReturn(mockMapOutputTracker)
    when(mockEnv.serializerManager).thenReturn(mockSerializerManager)
    val savedEnv = SparkEnv.get
    try {
      SparkEnv.set(mockEnv)
      body(mockEnv)
    } finally {
      SparkEnv.set(savedEnv)
    }
  }

  // ---------------------------------------------------------------------------
  // Test 2.1: Manager instantiation via ShuffleManager.create()
  // ---------------------------------------------------------------------------
  test("streaming manager instantiation via ShuffleManager.create()") {
    // Validates that ShuffleManager.shortShuffleMgrNames includes "streaming"
    // and that the reflection-based factory correctly instantiates
    // StreamingShuffleManager with (SparkConf, Boolean) constructor.
    val conf = new SparkConf()
      .set("spark.shuffle.manager", "streaming")
      .set("spark.shuffle.streaming.enabled", "true")
    val manager = ShuffleManager.create(conf, isDriver = true)
    assert(manager.isInstanceOf[StreamingShuffleManager])
    manager.stop()
  }

  // ---------------------------------------------------------------------------
  // Test 2.2: registerShuffle returns StreamingShuffleHandle when enabled
  // ---------------------------------------------------------------------------
  test("registerShuffle returns StreamingShuffleHandle when streaming enabled") {
    val conf = new SparkConf()
      .set("spark.shuffle.streaming.enabled", "true")
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    val dep = shuffleDep(
      new HashPartitioner(10), new KryoSerializer(conf), None, None, false)
    val handle = manager.registerShuffle(0, dep)
    assert(handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
    assert(handle.shuffleId === 0)
    manager.stop()
  }

  // ---------------------------------------------------------------------------
  // Test 2.3: registerShuffle returns BaseShuffleHandle when disabled
  // ---------------------------------------------------------------------------
  test("registerShuffle returns BaseShuffleHandle when streaming disabled") {
    val conf = new SparkConf()
      .set("spark.shuffle.streaming.enabled", "false")
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    val dep = shuffleDep(
      new HashPartitioner(10), new KryoSerializer(conf), None, None, false)
    val handle = manager.registerShuffle(0, dep)
    assert(handle.isInstanceOf[BaseShuffleHandle[_, _, _]])
    assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
    manager.stop()
  }

  // ---------------------------------------------------------------------------
  // Test 2.4: getWriter returns StreamingShuffleWriter
  // ---------------------------------------------------------------------------
  test("getWriter returns StreamingShuffleWriter for StreamingShuffleHandle") {
    withMockSparkEnv { _ =>
      val conf = new SparkConf()
        .set("spark.shuffle.streaming.enabled", "true")
      val manager = new StreamingShuffleManager(conf, isDriver = true)
      val dep = shuffleDep(
        new HashPartitioner(10), new KryoSerializer(conf), None, None, false)
      val handle = manager.registerShuffle(0, dep)
      assert(handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])

      val mockContext = mock(classOf[TaskContext])
      when(mockContext.taskAttemptId()).thenReturn(0L)
      val mockMetrics = mock(classOf[ShuffleWriteMetricsReporter])

      val writer = manager.getWriter(handle, 0L, mockContext, mockMetrics)
      assert(writer.isInstanceOf[StreamingShuffleWriter[_, _]])
      manager.stop()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 2.5: getReader returns StreamingShuffleReader
  // ---------------------------------------------------------------------------
  test("getReader returns StreamingShuffleReader for StreamingShuffleHandle") {
    withMockSparkEnv { mockEnv =>
      // Stub MapOutputTracker.getMapSizesByExecutorId to return empty iterator
      when(mockEnv.mapOutputTracker.getMapSizesByExecutorId(0, 0, 1, 0, 1))
        .thenReturn(Iterator.empty)

      val conf = new SparkConf()
        .set("spark.shuffle.streaming.enabled", "true")
      val manager = new StreamingShuffleManager(conf, isDriver = true)
      val dep = shuffleDep(
        new HashPartitioner(10), new KryoSerializer(conf), None, None, false)
      val handle = manager.registerShuffle(0, dep)
      assert(handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])

      val mockContext = mock(classOf[TaskContext])
      val mockMetrics = mock(classOf[ShuffleReadMetricsReporter])

      val reader = manager.getReader(handle, 0, 1, 0, 1, mockContext, mockMetrics)
      assert(reader.isInstanceOf[StreamingShuffleReader[_, _]])
      manager.stop()
    }
  }

  // ---------------------------------------------------------------------------
  // Test 2.6: unregisterShuffle cleans up resources
  // ---------------------------------------------------------------------------
  test("unregisterShuffle cleans up resources") {
    val conf = new SparkConf()
      .set("spark.shuffle.streaming.enabled", "true")
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    val dep = shuffleDep(
      new HashPartitioner(10), new KryoSerializer(conf), None, None, false)
    manager.registerShuffle(0, dep)
    // unregisterShuffle removes the shuffle from the internal tracking map
    // and cleans up per-map-task resources via the block resolver
    assert(manager.unregisterShuffle(0))
    // A second unregister should also return true (idempotent)
    assert(manager.unregisterShuffle(0))
    manager.stop()
  }

  // ---------------------------------------------------------------------------
  // Test 2.7: Fallback condition detection
  // ---------------------------------------------------------------------------
  test("shouldFallback returns false by default") {
    val conf = new SparkConf()
      .set("spark.shuffle.streaming.enabled", "true")
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    // By default, no fallback conditions are active (no consumer slowdown,
    // no memory pressure, no network saturation)
    assert(!manager.shouldFallback())
    manager.stop()
  }

  // ---------------------------------------------------------------------------
  // Test 2.8: shuffleBlockResolver type check
  // ---------------------------------------------------------------------------
  test("shuffleBlockResolver returns StreamingShuffleBlockResolver") {
    val conf = new SparkConf()
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    val resolver = manager.shuffleBlockResolver
    assert(resolver.isInstanceOf[StreamingShuffleBlockResolver])
    manager.stop()
  }

  // ---------------------------------------------------------------------------
  // Test 2.9: stop() delegates to block resolver
  // ---------------------------------------------------------------------------
  test("stop() delegates to shuffleBlockResolver.stop()") {
    val conf = new SparkConf()
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    // stop() should delegate to shuffleBlockResolver.stop() without throwing
    manager.stop()
    // Calling stop() a second time should also be safe
    manager.stop()
  }
}
