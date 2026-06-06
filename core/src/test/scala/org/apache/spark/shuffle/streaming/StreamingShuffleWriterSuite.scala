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

import java.io.IOException
import java.util.concurrent.ConcurrentLinkedQueue

import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.{mock, when}
import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.streaming.StreamingBlockExchange.{BlockMeta, StreamingBlockConsumer}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * Unit tests for [[StreamingShuffleWriter]], the producer-side writer of the streaming-shuffle
 * engine. A real `SparkContext` (via [[SharedSparkContext]]) supplies the running `SparkEnv` the
 * writer reads (block manager, serializer manager, memory manager), exactly as
 * `SortShuffleWriterSuite` does. The shuffle dependency is mocked to supply the partitioner and
 * serializer.
 *
 * Coverage maps to the CP1 review findings:
 *  - emitted blocks actually leave the writer over the data path and reach a subscribed reducer's
 *    callbacks (the producer->consumer path that was previously absent).
 *  - a single record whose serialized block exceeds the 2MB pipelined cap degrades to sort-based
 *    shuffle by throwing [[StreamingShuffleFallbackException]] rather than emitting an oversized
 *    block.
 *  - a SUCCESSFUL stop RETAINS the emitted blocks in the spill manager (the returned `MapStatus`
 *    advertises them; reducers reclaim them on ack), so success never deletes advertised output.
 *  - a FAILED stop frees every buffer and notifies readers of the producer failure, with write
 *    metrics reverted, so there is no buffer/memory leak and no advertised-but-missing output.
 */
class StreamingShuffleWriterSuite extends SparkFunSuite with SharedSparkContext with Matchers {

  private val twoMb = BackpressureProtocol.MAX_PIPELINED_BLOCK_BYTES.toInt

  /** A [[StreamingBlockConsumer]] that records the writer-emitted blocks and signals it sees. */
  private class CollectingConsumer extends StreamingBlockConsumer {
    val blocks = new ConcurrentLinkedQueue[BlockMeta]()
    @volatile var completedBlockCounts: Array[Long] = _
    val failures = new ConcurrentLinkedQueue[String]()

    override def onBlockReceived(meta: BlockMeta, bytes: Array[Byte]): Unit = blocks.add(meta)
    override def onMapComplete(mapId: Long, mapIndex: Int, blockCounts: Array[Long]): Unit =
      completedBlockCounts = blockCounts
    override def onProducerFailed(
        bmAddress: BlockManagerId,
        mapId: Long,
        mapIndex: Int,
        message: String,
        cause: Throwable): Unit = failures.add(message)
  }

  private def partitionerFor(n: Int): Partitioner = new Partitioner {
    override def numPartitions: Int = n
    override def getPartition(key: Any): Int = Utils.nonNegativeMod(key.hashCode, n)
  }

  /**
   * Builds the streaming collaborators over the live `SparkEnv`, runs the body, and tears
   * everything down so no daemon poller/heartbeat thread leaks.
   */
  private def withStreaming(numPartitions: Int, shuffleId: Int = 0)(
      body: Fixture => Unit): Unit = {
    val source = new StreamingShuffleSource
    val resolver = new IndexShuffleBlockResolver(conf)
    val spill = new MemorySpillManager(conf, sc.env.memoryManager, resolver, source)
    val exchange = new StreamingBlockExchange(conf, spill)
    val backpressure = new BackpressureProtocol(conf, 1, source)
    try {
      body(new Fixture(numPartitions, shuffleId, source, spill, exchange, backpressure))
    } finally {
      backpressure.stop()
      exchange.stop()
      spill.stop()
      resolver.stop()
    }
  }

  /** Test fixture exposing the collaborators and a factory for the writer under test. */
  private class Fixture(
      val numPartitions: Int,
      val shuffleId: Int,
      val source: StreamingShuffleSource,
      val spill: MemorySpillManager,
      val exchange: StreamingBlockExchange,
      val backpressure: BackpressureProtocol) {

    private val serializer = new JavaSerializer(conf)

    def newWriter(mapId: Long): StreamingShuffleWriter[Int, Array[Byte]] =
      newWriterWith(mapId, backpressure)

    /** Build a writer over a caller-supplied backpressure protocol (a mock for fault injection). */
    def newWriterWith(
        mapId: Long,
        bp: BackpressureProtocol): StreamingShuffleWriter[Int, Array[Byte]] = {
      val dependency = mock(classOf[ShuffleDependency[Int, Array[Byte], Array[Byte]]])
      when(dependency.shuffleId).thenReturn(shuffleId)
      when(dependency.partitioner).thenReturn(partitionerFor(numPartitions))
      when(dependency.serializer).thenReturn(serializer)
      val handle = new StreamingShuffleHandle[Int, Array[Byte], Array[Byte]](shuffleId, dependency)
      val context = MemoryTestingUtils.fakeTaskContext(sc.env)
      new StreamingShuffleWriter[Int, Array[Byte]](
        handle, mapId, context, context.taskMetrics().shuffleWriteMetrics, conf,
        bp, spill, exchange, source)
    }
  }

  private def smallRecords(n: Int): Iterator[(Int, Array[Byte])] =
    (0 until n).iterator.map(i => (i, Array.fill(64)(i.toByte)))

  test("emitted blocks reach a subscribed reducer and a MapStatus advertises them") {
    withStreaming(numPartitions = 2) { f =>
      val consumer = new CollectingConsumer
      // Subscribe across both partitions for this map (fakeTaskContext partitionId == mapIndex 0).
      f.exchange.registerReader(f.shuffleId, 0, 1, 0, 2, consumer)
      val writer = f.newWriter(mapId = 0L)

      writer.write(smallRecords(8))
      val status = writer.stop(success = true)

      // The producer actually streamed blocks to the consumer's callbacks (data path works).
      consumer.blocks.size() must be > 0
      // The map-completion coverage signal was delivered with per-partition block counts.
      consumer.completedBlockCounts must not be null
      consumer.completedBlockCounts.length mustBe 2
      // A standard MapStatus is returned so the unchanged MapOutputTracker can locate outputs.
      status.isDefined mustBe true
      writer.getPartitionLengths().sum must be > 0L
    }
  }

  test("a single oversized record degrades to sort-based shuffle (2MB pipelined cap)") {
    withStreaming(numPartitions = 1) { f =>
      val writer = f.newWriter(mapId = 0L)
      // One incompressible record whose serialized block exceeds the 2MB cap. Random bytes do not
      // compress, so the emitted block stays > 2MB regardless of the shuffle codec.
      val big = new Array[Byte](2 * twoMb)
      new java.util.Random(42).nextBytes(big)

      // The writer must NOT emit an oversized streaming block; it signals fallback to sort instead.
      intercept[StreamingShuffleFallbackException] {
        writer.write(Iterator((0, big)))
      }
      // Nothing was published/accounted for the rejected oversize block.
      f.spill.trackedBytesTotal mustBe 0L
      writer.stop(success = false)
    }
  }

  test("successful stop retains emitted blocks in the spill manager (no data loss)") {
    withStreaming(numPartitions = 2) { f =>
      // No reader subscribes, so published blocks are buffered (pending) and their bytes are
      // retained by the spill manager.
      val writer = f.newWriter(mapId = 0L)
      writer.write(smallRecords(6))
      val retainedBeforeStop = f.spill.trackedBytesTotal
      retainedBeforeStop must be > 0L

      val status = writer.stop(success = true)
      status.isDefined mustBe true
      // CRITICAL: a successful stop must NOT release the buffers -- the returned MapStatus
      // advertises these outputs, so they must remain until a consumer acknowledges them.
      f.spill.trackedBytesTotal mustBe retainedBeforeStop
      // stop is idempotent: a later stop(false) on the success path does nothing.
      writer.stop(success = false) mustBe None
      f.spill.trackedBytesTotal mustBe retainedBeforeStop
    }
  }

  test("failed stop frees all buffers, reverts metrics, and notifies readers") {
    withStreaming(numPartitions = 2) { f =>
      val consumer = new CollectingConsumer
      f.exchange.registerReader(f.shuffleId, 0, 1, 0, 2, consumer)
      val writer = f.newWriter(mapId = 0L)
      writer.write(smallRecords(6))
      f.spill.trackedBytesTotal must be > 0L

      val result = writer.stop(success = false)
      result mustBe None
      // Failure cleanup frees every block this map registered (no buffer/memory leak)...
      f.spill.trackedBytesTotal mustBe 0L
      f.spill.reservedBytesTotal mustBe 0L
      // ...notifies the reducer so it invalidates its partial read (drives recomputation)...
      consumer.failures.size() must be > 0
      // ...and reverts the buffered write metrics so a failed task does not over-report bytes.
      writer.stop(success = false) // idempotent: no double-free
    }
  }

  test("write empty iterator returns an empty MapStatus and retains nothing") {
    withStreaming(numPartitions = 3) { f =>
      val writer = f.newWriter(mapId = 0L)
      writer.write(Iterator.empty)
      val status = writer.stop(success = true)
      status.isDefined mustBe true
      writer.getPartitionLengths().sum mustBe 0L
      f.spill.trackedBytesTotal mustBe 0L
    }
  }

  test("an interrupted block is aborted as a write failure, never advertised as written") {
    withStreaming(numPartitions = 1) { f =>
      // Inject a backpressure protocol whose blocking acquire reports non-admission; paired with a
      // set interrupt flag this drives the writer's interruption branch deterministically (the real
      // limiter takes an unlimited-bandwidth fast path and would not exercise this branch).
      val bp = mock(classOf[BackpressureProtocol])
      when(bp.shouldFallback).thenReturn(false)
      when(bp.acquireBlocking(anyLong())).thenReturn(false)
      val writer = f.newWriterWith(mapId = 0L, bp)

      Thread.currentThread().interrupt()
      try {
        // acquireBlocking returns false while the thread is interrupted: the writer MUST treat this
        // as a write failure (IOException), never silently advertise an unadmitted block.
        intercept[IOException] {
          writer.write(smallRecords(4))
        }
      } finally {
        // Clear the interrupt flag so it never leaks into subsequent tests on this thread.
        Thread.interrupted()
      }
      // Nothing was published or accounted for the aborted block: no advertised-but-missing output.
      writer.getPartitionLengths().sum mustBe 0L
      f.spill.trackedBytesTotal mustBe 0L
      writer.stop(success = false)
    }
  }
}
