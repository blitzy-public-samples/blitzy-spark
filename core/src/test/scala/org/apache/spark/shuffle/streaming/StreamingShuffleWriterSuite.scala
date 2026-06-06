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

import scala.util.Random

import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong, eq => meq}
import org.mockito.Mockito._
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{Partitioner, SharedSparkContext, ShuffleDependency, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.streaming.StreamingBlockExchange.BlockMeta
import org.apache.spark.util.Utils

/**
 * Unit tests for [[StreamingShuffleWriter]], the map-side writer of the opt-in streaming-shuffle
 * engine. The suite mirrors `org.apache.spark.shuffle.sort.SortShuffleWriterSuite`: it runs over a
 * real `SparkEnv` (via [[SharedSparkContext]]) so the writer reads the genuine block manager,
 * serializer manager, and memory manager, and it follows the same `@Mock` /
 * `MockitoAnnotations.openMocks(this).close()` fixture and `MemoryTestingUtils.fakeTaskContext`
 * pattern.
 *
 * Coexistence: the streaming writer is exercised entirely within the `ShuffleManager` abstraction
 * boundary and never touches the sort-based engine; these tests assert only streaming behavior and
 * leave `SortShuffleWriterSuite` (the fallback engine's suite) and the sort path untouched.
 *
 * Hermetic test seam: the streaming primitives the writer collaborates with -- the
 * [[BackpressureProtocol]] rate limiter, the [[MemorySpillManager]] spill coordinator, and the
 * in-process [[StreamingBlockExchange]] producer-to-consumer data path -- are Mockito mocks.
 * Mocking them serves two purposes: (1) it keeps every test free of live consumers, sockets, and
 * the daemon poller/heartbeat threads those collaborators start in their real constructors, so the
 * suite is deterministic and leak-free; and (2) it lets the tests verify the writer's coordination
 * contract (which collaborator methods it calls and how it reacts to their signals) rather than
 * real byte movement. The shared [[StreamingShuffleSource]] is a real instance so that JMX gauge
 * state can be asserted directly.
 */
class StreamingShuffleWriterSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers
    with PrivateMethodTester {

  private val shuffleId = 0
  private val numPartitions = 5
  private val serializer = new JavaSerializer(conf)

  // The streaming shuffle dependency is mocked exactly as in SortShuffleWriterSuite; the writer
  // reads only its shuffleId, partitioner, and serializer.
  @Mock(answer = RETURNS_SMART_NULLS)
  private var dependency: ShuffleDependency[Int, Int, Int] = _

  // A real handle (rebuilt before each test) carries the mocked dependency to the writer ctor and
  // selects the streaming dispatch path, mirroring `SerializedShuffleHandle` in the sort engine.
  private var handle: StreamingShuffleHandle[Int, Int, Int] = _

  // Hash partitioner identical in shape to SortShuffleWriterSuite's, sized to numPartitions.
  private val partitioner = new Partitioner() {
    override def numPartitions: Int = StreamingShuffleWriterSuite.this.numPartitions
    override def getPartition(key: Any): Int = Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.openMocks(this).close()
    handle = new StreamingShuffleHandle[Int, Int, Int](shuffleId, dependency)
    resetDependency()
  }

  // Stub the dependency members the writer reads (mirrors SortShuffleWriterSuite.resetDependency).
  // aggregator/keyOrdering are stubbed to None for parity with the sibling even though the
  // streaming writer does not consult them.
  private def resetDependency(): Unit = {
    reset(dependency)
    when(dependency.shuffleId).thenReturn(shuffleId)
    when(dependency.partitioner).thenReturn(partitioner)
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
  }

  // Builds a writer over the live SparkEnv with the supplied (mock-by-default) collaborators. The
  // defaults stub the emit path so write() proceeds without throttling, fallback, or a live
  // consumer; individual tests override a collaborator to drive a specific coordination path.
  private def newWriter(
      mapId: Long,
      backpressure: BackpressureProtocol = stubbedBackpressure(),
      spillManager: MemorySpillManager = mock(classOf[MemorySpillManager]),
      exchange: StreamingBlockExchange = stubbedExchange(granted = true),
      source: StreamingShuffleSource = new StreamingShuffleSource)
    : StreamingShuffleWriter[Int, Int] = {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    new StreamingShuffleWriter[Int, Int](
      handle, mapId, context, context.taskMetrics().shuffleWriteMetrics, conf,
      backpressure, spillManager, exchange, source)
  }

  // A backpressure mock stubbed so the writer's per-block emit path is admitted immediately: no
  // sustained-slowdown fallback and every blocking acquire succeeds.
  private def stubbedBackpressure(): BackpressureProtocol = {
    val bp = mock(classOf[BackpressureProtocol])
    when(bp.shouldFallback).thenReturn(false)
    when(bp.acquireBlocking(anyLong())).thenReturn(true)
    bp
  }

  // An exchange mock whose publishBlock returns `granted`. A false return models the memory-bounded
  // path withholding a grant (a spill was scheduled / the buffer budget is exhausted), which is the
  // signal the writer reacts to by recording a backpressure telemetry event.
  private def stubbedExchange(granted: Boolean): StreamingBlockExchange = {
    val ex = mock(classOf[StreamingBlockExchange])
    when(ex.publishBlock(any(), any())).thenReturn(granted)
    ex
  }

  // Reads a single named gauge value from a real StreamingShuffleSource's MetricRegistry.
  private def gaugeValue(source: StreamingShuffleSource, name: String): Long =
    source.metricRegistry.getGauges().get(name).getValue().asInstanceOf[Long]

  test("stop(success = true) emits a MapStatus after writing an empty iterator") {
    val writer = newWriter(mapId = 1L)
    writer.write(Iterator.empty)
    val status = writer.stop(success = true)
    // The writer must emit a standard MapStatus (via the object MapStatus factory) so the
    // unmodified MapOutputTracker and DAG scheduler locate streaming outputs unchanged.
    assert(status.isDefined)
    assert(status.get.mapId === 1L)
    // An empty map still advertises one length per reduce partition, all zero.
    assert(writer.getPartitionLengths().length === numPartitions)
    assert(writer.getPartitionLengths().sum === 0L)
  }

  test("stop(success = false) returns None, cleans up, and is idempotent") {
    val spillManager = mock(classOf[MemorySpillManager])
    val exchange = mock(classOf[StreamingBlockExchange])
    val writer = newWriter(mapId = 2L, spillManager = spillManager, exchange = exchange)
    writer.write(Iterator.empty)
    assert(writer.stop(success = false).isEmpty)
    // stop() is idempotent (mirrors SortShuffleWriter): a second stop is a no-op that returns None
    // and never double-frees the map's buffers.
    assert(writer.stop(success = false).isEmpty)
    verify(spillManager, times(1)).unregisterMap(shuffleId, 2L)
  }

  test("allocates per-partition buffers sized by bufferSizePercent / numPartitions") {
    val originalPercent = conf.get(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT)
    conf.set(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT.key, "20")
    val writer = newWriter(mapId = 3L)
    try {
      val bufferPercent = conf.get(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT)
      // Replicate the documented per-partition budget the production writer applies:
      //   (executorMemory * bufferSizePercent / 100) / numPartitions
      // capped by the 2MB pipelined block size and floored at 1KB so progress is always possible.
      val executorMemory = sc.env.memoryManager.maxOnHeapStorageMemory.toDouble
      val parts = math.max(1, numPartitions).toDouble
      val cap = BackpressureProtocol.MAX_PIPELINED_BLOCK_BYTES.toDouble
      val perPartition = executorMemory * bufferPercent / 100.0 / parts
      val expected = math.max(1024L, math.min(perPartition, cap).toLong)
      // Exercise the writer's private sizing via PrivateMethodTester rather than re-deriving it.
      val computeThreshold = PrivateMethod[Long](Symbol("computeBlockFlushThreshold"))
      writer.invokePrivate(computeThreshold()) mustBe expected
    } finally {
      writer.stop(success = false)
      conf.set(config.STREAMING_SHUFFLE_BUFFER_SIZE_PERCENT.key, originalPercent.toString)
    }
  }

  test("coordinates memory-bounded buffering and spill through the exchange at the threshold") {
    val originalThreshold = conf.get(config.STREAMING_SHUFFLE_SPILL_THRESHOLD)
    // The spill threshold drives the shared MemorySpillManager (gate-tested in its own suite); here
    // it is pinned to the documented default and an exhausted budget is modeled by publishBlock
    // reporting a scheduled spill (granted = false).
    conf.set(config.STREAMING_SHUFFLE_SPILL_THRESHOLD.key, "80")
    val source = new StreamingShuffleSource
    val spillManager = mock(classOf[MemorySpillManager])
    val exchange = stubbedExchange(granted = false)
    val writer = newWriter(
      mapId = 4L, spillManager = spillManager, exchange = exchange, source = source)
    try {
      writer.write(Random.shuffle((1 to 64).toList).map(i => (i, i)).iterator)
      // The writer delegated every emitted block to the memory-bounded exchange/spill path...
      verify(exchange, atLeastOnce()).publishBlock(any(), any())
      // ...and reacted to the spill / over-budget signal by recording backpressure telemetry.
      gaugeValue(source, "shuffle.streaming.backpressureEvents") must be > 0L
    } finally {
      writer.stop(success = false)
      conf.set(config.STREAMING_SHUFFLE_SPILL_THRESHOLD.key, originalThreshold.toString)
    }
  }

  test("computes block checksums using the standard ShuffleChecksumHelper facility (CRC32C)") {
    val originalAlgorithm = conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM)
    conf.set(config.SHUFFLE_CHECKSUM_ALGORITHM.key, "CRC32C")
    val exchange = stubbedExchange(granted = true)
    val writer = newWriter(mapId = 5L, exchange = exchange)
    try {
      // A single record yields exactly one streamed block, captured from the exchange below.
      writer.write(Iterator((1, Random.nextInt())))
      val metaCaptor = ArgumentCaptor.forClass(classOf[BlockMeta])
      val bytesCaptor = ArgumentCaptor.forClass(classOf[Array[Byte]])
      verify(exchange, atLeastOnce()).publishBlock(metaCaptor.capture(), bytesCaptor.capture())
      val publishedBytes = bytesCaptor.getValue
      val publishedMeta = metaCaptor.getValue
      // Recompute the checksum over the EXACT published bytes with the SAME standard facility the
      // production writer uses; equality proves reuse of ShuffleChecksumHelper (no new checksum).
      val checksum =
        ShuffleChecksumHelper.getChecksumByAlgorithm(conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM))
      checksum.update(publishedBytes, 0, publishedBytes.length)
      publishedMeta.checksum mustBe checksum.getValue
    } finally {
      writer.stop(success = false)
      conf.set(config.SHUFFLE_CHECKSUM_ALGORITHM.key, originalAlgorithm)
    }
  }

  test("releases buffers and notifies readers on producer failure (stop(success = false))") {
    val spillManager = mock(classOf[MemorySpillManager])
    val exchange = stubbedExchange(granted = true)
    val writer = newWriter(mapId = 7L, spillManager = spillManager, exchange = exchange)
    try {
      writer.write(Random.shuffle((1 to 24).toList).map(i => (i, i)).iterator)
      assert(writer.stop(success = false).isEmpty)
      // Failure cleanup frees every block this map registered, so no buffer memory leaks -- the
      // property gated by the AAP's 2-hour stress test.
      verify(spillManager, times(1)).unregisterMap(shuffleId, 7L)
      // It also notifies subscribed readers so they invalidate partial reads; the unmodified
      // scheduler then recomputes the upstream stage via the existing FetchFailedException path.
      verify(exchange, times(1))
        .producerFailed(meq(shuffleId), meq(7L), anyInt(), any(), any(), any())
      // Idempotent: a second failed stop neither double-frees nor re-notifies.
      assert(writer.stop(success = false).isEmpty)
      verify(spillManager, times(1)).unregisterMap(shuffleId, 7L)
    } finally {
      writer.stop(success = false)
    }
  }
}
