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
import java.util.concurrent.atomic.AtomicInteger

import org.mockito.Mockito.{mock, when}
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Span}

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.{FetchFailedException, IndexShuffleBlockResolver}
import org.apache.spark.shuffle.streaming.MemorySpillManager.BlockKey
import org.apache.spark.shuffle.streaming.StreamingBlockExchange.BlockMeta
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}

/**
 * Unit tests for [[StreamingShuffleReader]], the reduce-side reader of the streaming-shuffle
 * engine. A real `SparkContext` (via [[SharedSparkContext]]) supplies the running `SparkEnv` the
 * reader uses to unwrap serialized block streams; the shuffle dependency is mocked to supply the
 * serializer with no aggregator/ordering, so the returned iterator yields the streamed key/values
 * directly. Blocks are produced through the in-process [[StreamingBlockExchange]] exactly as the
 * writer would, so these tests exercise the real production data path end to end.
 *
 * Coverage maps to the CP1 review findings for the reader:
 *  - a fully-covered stream returns every record AND acknowledges every block, driving
 *    `MemorySpillManager.reclaim` so writer-side buffers fall back to zero (R2).
 *  - a producer failure is turned into an atomic partial-read invalidation that throws
 *    [[FetchFailedException]] (R1, fast path) and increments the partial-read-invalidation metric.
 *  - a TRUNCATED stream -- a completion signal that claims more blocks than were delivered -- is
 *    NEVER reported as success; after the producer-timeout deadline the reader invalidates (R1).
 *  - the reader inbox is BOUNDED, so a fast producer BLOCKS in the delivery callback until the
 *    consumer drains, propagating backpressure instead of accumulating unbounded byte arrays (R4).
 *  - a corrupt block is re-requested by its addressable key and the retained block is resent and
 *    consumed successfully, with no invalidation (R3 block-specific CRC32C retransmission).
 */
class StreamingShuffleReaderSuite extends SparkFunSuite with SharedSparkContext
  with Matchers with Eventually {

  private val serializer = new JavaSerializer(conf)
  private val bmId = BlockManagerId("exec-reader", "host-reader", 7400)

  // Serialize key/value pairs into one block's bytes through the EXISTING SerializerManager, using
  // the same wrap (compression/encryption) the writer applies, keyed by the shuffle block id.
  private def serialize(blockId: ShuffleBlockId, pairs: Seq[(Int, Int)]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val wrapped = sc.env.serializerManager.wrapStream(blockId, out)
    val ser = serializer.newInstance().serializeStream(wrapped)
    pairs.foreach { case (k, v) => ser.writeKey(k).writeValue(v) }
    ser.close()
    out.toByteArray
  }

  // Compute a block's checksum with the EXISTING ShuffleChecksumHelper (CRC32C by default), exactly
  // as the producer does, so the reader's validation accepts an uncorrupted block.
  private def checksumOf(bytes: Array[Byte]): Long = {
    val algorithm = conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM)
    val checksum = ShuffleChecksumHelper.getChecksumByAlgorithm(algorithm)
    checksum.update(bytes, 0, bytes.length)
    checksum.getValue
  }

  // Build the end-to-end block metadata a producer would attach to a streamed block.
  private def metaFor(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      seq: Long,
      mapIndex: Int,
      bytes: Array[Byte]): BlockMeta = {
    val key = BlockKey(shuffleId, mapId, partitionId, seq)
    BlockMeta(key, bmId, ShuffleBlockId(shuffleId, mapId, partitionId), mapIndex,
      checksumOf(bytes), bytes.length.toLong)
  }

  // Read the cumulative `shuffle.streaming.partialReadInvalidations` gauge from the metrics source.
  private def invalidations(source: StreamingShuffleSource): Long = {
    source.metricRegistry.getGauges.get("shuffle.streaming.partialReadInvalidations")
      .getValue.asInstanceOf[Long]
  }

  /**
   * Builds the streaming collaborators over the live `SparkEnv`, runs the body, and tears
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

  /** Construct the reader under test over a mocked dependency (serializer only, no agg/order). */
  private def newReader(
      exchange: StreamingBlockExchange,
      backpressure: BackpressureProtocol,
      source: StreamingShuffleSource,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      inboxCapacity: Int = 128,
      maxInboxBytes: Int = 64 * 1024 * 1024,
      shuffleId: Int = 0): StreamingShuffleReader[Int, Int] = {
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
    val handle = new StreamingShuffleHandle[Int, Int, Int](shuffleId, dependency)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    new StreamingShuffleReader[Int, Int](
      handle, startMapIndex, endMapIndex, startPartition, endPartition,
      context, readMetrics, conf, backpressure, source, exchange,
      inboxCapacity, maxInboxBytes)
  }

  test("a fully-covered stream returns every record and acks each block (reclaim to zero)") {
    withStreaming { (source, spill, exchange, backpressure) =>
      val reader = newReader(exchange, backpressure, source, 0, 1, 0, 1)
      val block0 = serialize(ShuffleBlockId(0, 0L, 0), Seq((10, 100), (20, 200)))
      val block1 = serialize(ShuffleBlockId(0, 0L, 0), Seq((30, 300)))
      exchange.publishBlock(metaFor(0, 0L, 0, 0L, 0, block0), block0)
      exchange.publishBlock(metaFor(0, 0L, 0, 1L, 0, block1), block1)
      exchange.completeMap(0, 0L, 0, Array(2L))

      val pairs = reader.read().toList.map(r => (r._1, r._2))
      pairs must contain theSameElementsAs Seq((10, 100), (20, 200), (30, 300))
      // Every consumed block was acknowledged, so the writer-side buffers reclaim to zero (R2).
      eventually(timeout(Span(2000, Millis)), interval(Span(20, Millis))) {
        spill.trackedBytesTotal mustBe 0L
      }
    }
  }

  test("a producer failure invalidates the read with FetchFailedException (no truncated success)") {
    withStreaming { (source, _, exchange, backpressure) =>
      val reader = newReader(exchange, backpressure, source, 0, 1, 0, 1)
      exchange.producerFailed(0, 0L, 0, bmId, "simulated producer crash", null)

      intercept[FetchFailedException] {
        reader.read().toList
      }
      invalidations(source) mustBe 1L
    }
  }

  test("a truncated stream (completion claims more blocks than delivered) is never a success") {
    withStreaming { (source, _, exchange, backpressure) =>
      val reader = newReader(exchange, backpressure, source, 0, 1, 0, 1)
      // The map reports it emitted ONE block for partition 0 but no block is ever delivered.
      exchange.completeMap(0, 0L, 0, Array(1L))

      // The reader must NOT accept completion without full block coverage; after the producer
      // timeout it invalidates atomically (~15s; the only intentionally slow reader test).
      intercept[FetchFailedException] {
        reader.read().toList
      }
      invalidations(source) mustBe 1L
    }
  }

  test("the bounded inbox blocks a fast producer until the consumer drains (backpressure)") {
    withStreaming { (source, _, exchange, backpressure) =>
      val block = serialize(ShuffleBlockId(0, 0L, 0), Seq((7, 70)))
      // Bound the inbox to a single block's worth of bytes so the SECOND publish must block.
      val reader = newReader(exchange, backpressure, source, 0, 1, 0, 1,
        inboxCapacity = 16, maxInboxBytes = block.length)
      val delivered = new AtomicInteger(0)

      val producer = new Thread(() => {
        exchange.publishBlock(metaFor(0, 0L, 0, 0L, 0, block), block)
        delivered.incrementAndGet()
        // This second publish blocks in the delivery callback until the consumer drains the first.
        exchange.publishBlock(metaFor(0, 0L, 0, 1L, 0, block), block)
        delivered.incrementAndGet()
        exchange.completeMap(0, 0L, 0, Array(2L))
      }, "streaming-test-producer")
      producer.setDaemon(true)
      producer.start()

      // Exactly one delivery completes; the second stays blocked until the consumer drains.
      eventually(timeout(Span(2000, Millis)), interval(Span(20, Millis))) {
        delivered.get() mustBe 1
      }
      Thread.sleep(300)
      delivered.get() mustBe 1

      // Draining the reader releases the byte budget, unblocking the producer; full coverage (2
      // blocks) then completes the read with both records.
      val records = reader.read().toList
      records.size mustBe 2
      eventually(timeout(Span(2000, Millis)), interval(Span(20, Millis))) {
        delivered.get() mustBe 2
      }
      producer.join(5000)
    }
  }

  test("a corrupt block triggers a block-specific resend of the retained block (CRC retransmit)") {
    withStreaming { (source, spill, exchange, backpressure) =>
      val reader = newReader(exchange, backpressure, source, 0, 1, 0, 1)
      val good = serialize(ShuffleBlockId(0, 0L, 0), Seq((5, 55)))
      val meta = metaFor(0, 0L, 0, 0L, 0, good)
      // The exact good block is RETAINED in the spill manager (as the writer retains it), so a
      // resend can re-read it by its key. The reader first receives a CORRUPTED copy of the block.
      spill.register(meta.key, good)
      val corrupt = good.clone()
      val mid = corrupt.length / 2
      corrupt(mid) = (corrupt(mid) ^ 0xFF).toByte
      reader.onBlockReceived(meta, corrupt)
      exchange.completeMap(0, 0L, 0, Array(1L))

      // The reader validates CRC, NACKs the corrupt block, and the exchange resends the retained
      // good block by its key; the read then succeeds with the correct record (~1s backoff).
      val pairs = reader.read().toList.map(r => (r._1, r._2))
      pairs mustBe Seq((5, 55))
      invalidations(source) mustBe 0L
      // After successful consumption the block is acknowledged and reclaimed.
      eventually(timeout(Span(2000, Millis)), interval(Span(20, Millis))) {
        spill.trackedBytesTotal mustBe 0L
      }
    }
  }

  test("a fully-covered stream across multiple maps returns every record") {
    withStreaming { (source, _, exchange, backpressure) =>
      // Two producing maps both stream one block for partition 0; the reader needs a completion
      // from BOTH maps and every block each reported before it may finish (R1 multi-map coverage).
      val reader = newReader(exchange, backpressure, source, 0, 2, 0, 1)
      val b0 = serialize(ShuffleBlockId(0, 0L, 0), Seq((1, 11)))
      val b1 = serialize(ShuffleBlockId(0, 1L, 0), Seq((2, 22)))
      exchange.publishBlock(metaFor(0, 0L, 0, 0L, 0, b0), b0)
      exchange.publishBlock(metaFor(0, 1L, 0, 0L, 1, b1), b1)
      exchange.completeMap(0, 0L, 0, Array(1L))
      exchange.completeMap(0, 1L, 1, Array(1L))

      val pairs = reader.read().toList.map(r => (r._1, r._2))
      pairs must contain theSameElementsAs Seq((1, 11), (2, 22))
    }
  }
}
