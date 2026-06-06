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
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Span}

import org.apache.spark._
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.{FetchFailedException, IndexShuffleBlockResolver}
import org.apache.spark.util.Utils

/**
 * End-to-end integration tests that wire a REAL [[StreamingShuffleWriter]] to a REAL
 * [[StreamingShuffleReader]] through the in-process [[StreamingBlockExchange]] over a live
 * `SparkEnv` (via [[SharedSparkContext]]). Unlike the focused per-class suites, these exercise the
 * actual producer->consumer data path the CP1 review found missing: the writer serializes and emits
 * blocks, the exchange routes them by reduce partition, and the reader validates, acknowledges, and
 * deserializes them back into the exact records the producer wrote.
 *
 * Coverage maps to the cross-file CP1 review findings:
 *  - the producer actually streams blocks that reach the consumer and round-trip to identical
 *    records (the previously-absent end-to-end data path; W1 + reader receive path).
 *  - blocks are routed to the correct reducer by reduce partition (no cross-partition leakage).
 *  - a successful map completion plus full block coverage lets every reducer finish WITHOUT a
 *    truncated read, and consumer acks reclaim every writer-side buffer (ack->reclaim end to end).
 *  - a producer FAILURE (a failed `stop`) drives every affected reducer to an atomic partial-read
 *    invalidation via [[FetchFailedException]], so a crashed producer never yields a silent pass.
 */
class StreamingShuffleIntegrationSuite extends SparkFunSuite with SharedSparkContext
  with Matchers with Eventually {

  private val serializer = new JavaSerializer(conf)

  private def partitionerFor(n: Int): Partitioner = new Partitioner {
    override def numPartitions: Int = n
    override def getPartition(key: Any): Int = Utils.nonNegativeMod(key.hashCode, n)
  }

  private def partitionOf(key: Int, numPartitions: Int): Int =
    Utils.nonNegativeMod(key.hashCode, numPartitions)

  /**
   * Builds the shared streaming collaborators over the live `SparkEnv`, runs the body, and tears
   * everything down so no daemon poller/heartbeat thread leaks.
   */
  private def withStreaming(
      body: (StreamingShuffleSource, MemorySpillManager,
        StreamingBlockExchange, BackpressureProtocol) => Unit): Unit = {
    val source = new StreamingShuffleSource
    val resolver = new IndexShuffleBlockResolver(conf)
    val spill = new MemorySpillManager(conf, sc.env.memoryManager, resolver, source)
    val exchange = new StreamingBlockExchange(conf, spill)
    val backpressure = new BackpressureProtocol(conf, 1, source)
    try {
      body(source, spill, exchange, backpressure)
    } finally {
      backpressure.stop()
      exchange.stop()
      spill.stop()
      resolver.stop()
    }
  }

  /** Build a real streaming writer for one map task over a mocked dependency. */
  private def buildWriter(
      shuffleId: Int,
      mapId: Long,
      numPartitions: Int,
      spill: MemorySpillManager,
      exchange: StreamingBlockExchange,
      backpressure: BackpressureProtocol,
      source: StreamingShuffleSource): StreamingShuffleWriter[Int, Int] = {
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.shuffleId).thenReturn(shuffleId)
    when(dependency.partitioner).thenReturn(partitionerFor(numPartitions))
    when(dependency.serializer).thenReturn(serializer)
    // These integration tests model a single producer map (mapId 0L) and read with a bounded
    // endMapIndex, so numMaps = 1 is the honest, sentinel-irrelevant value.
    val handle = new StreamingShuffleHandle[Int, Int, Int](shuffleId, dependency, numMaps = 1)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    new StreamingShuffleWriter[Int, Int](
      handle, mapId, context, context.taskMetrics().shuffleWriteMetrics, conf,
      backpressure, spill, exchange, source)
  }

  /** Build a real streaming reader for one reduce partition over a mocked dependency. */
  private def buildReader(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      exchange: StreamingBlockExchange,
      backpressure: BackpressureProtocol,
      source: StreamingShuffleSource): StreamingShuffleReader[Int, Int] = {
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
    // Single producer map; the reader is always driven with a bounded endMapIndex here, so the
    // all-maps sentinel branch is not exercised and numMaps = 1 is the honest value.
    val handle = new StreamingShuffleHandle[Int, Int, Int](shuffleId, dependency, numMaps = 1)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    new StreamingShuffleReader[Int, Int](
      handle, startMapIndex, endMapIndex, startPartition, endPartition,
      context, readMetrics, conf, backpressure, source, exchange)
  }

  test("a real writer streams blocks that a real reader round-trips into identical records") {
    val numPartitions = 2
    val shuffleId = 0
    val records = (0 until 12).map(i => (i, i * 10))
    withStreaming { (source, spill, exchange, backpressure) =>
      // The producer runs first; with no reducer subscribed yet, every block and the map-completion
      // are buffered in the exchange (block bytes held by the spill manager) until a reducer joins.
      val writer = buildWriter(shuffleId, 0L, numPartitions, spill, exchange, backpressure, source)
      writer.write(records.iterator)
      val status = writer.stop(success = true)
      status.isDefined mustBe true

      // Each reducer subscribes and drains its partition; the exchange replays the completion and
      // the retained blocks for that partition, and the reader deserializes them back to records.
      val collected = (0 until numPartitions).map { p =>
        val reader = buildReader(shuffleId, 0, 1, p, p + 1, exchange, backpressure, source)
        p -> reader.read().toList.map(r => (r._1, r._2))
      }.toMap

      // Every record reached EXACTLY the reducer that owns its key's partition (no leakage).
      (0 until numPartitions).foreach { p =>
        val expected = records.filter { case (k, _) => partitionOf(k, numPartitions) == p }
        collected(p) must contain theSameElementsAs expected
      }
      // The union across reducers is the complete, unchanged input (zero data loss).
      collected.values.flatten.toList must contain theSameElementsAs records

      // Every consumed block was acknowledged end to end, so writer-side buffers reclaim to zero.
      eventually(timeout(Span(2000, Millis)), interval(Span(20, Millis))) {
        spill.trackedBytesTotal mustBe 0L
      }
    }
  }

  test("a failed producer stop drives the reducer to a FetchFailedException (no silent success)") {
    val numPartitions = 2
    val shuffleId = 0
    val records = (0 until 8).map(i => (i, i * 10))
    withStreaming { (source, spill, exchange, backpressure) =>
      // The producer emits blocks but then FAILS its stop, which frees its buffers and notifies
      // reducers of the failure rather than advertising a completed map.
      val writer = buildWriter(shuffleId, 0L, numPartitions, spill, exchange, backpressure, source)
      writer.write(records.iterator)
      writer.stop(success = false) mustBe None

      // A reducer covering the failed map must invalidate atomically rather than report a truncated
      // success: the failure is replayed on subscription and turned into a FetchFailedException.
      val reader = buildReader(shuffleId, 0, 1, 0, numPartitions, exchange, backpressure, source)
      intercept[FetchFailedException] {
        reader.read().toList
      }
      // The failed map leaked no buffer memory (cleanup freed everything it had registered).
      spill.trackedBytesTotal mustBe 0L
    }
  }
}
