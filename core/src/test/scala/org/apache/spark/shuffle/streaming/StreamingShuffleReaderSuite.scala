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
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.internal.config
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.{FetchFailedException, IndexShuffleBlockResolver}
import org.apache.spark.shuffle.ShuffleChecksumUtils
import org.apache.spark.shuffle.streaming.MemorySpillManager.BlockKey
import org.apache.spark.shuffle.streaming.StreamingBlockExchange.BlockMeta
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}

/**
 * Unit tests for [[StreamingShuffleReader]], the reduce-side reader of the opt-in streaming-shuffle
 * engine. A real `SparkContext` (via [[SharedSparkContext]]) supplies the running `SparkEnv` the
 * reader uses to unwrap serialized block streams; the shuffle dependency is mocked to supply the
 * serializer with no aggregator/ordering, so the returned iterator yields the streamed key/values
 * directly. Blocks are produced through the in-process [[StreamingBlockExchange]] exactly as the
 * writer would, so these tests exercise the real production data path end to end -- no live
 * producer/network is required and the tests stay hermetic and deterministic.
 *
 * Coverage maps to the streaming-reader requirements in the feature plan:
 *  - read() returns a LAZY/blocking iterator: constructing it performs no eager fetching, and the
 *    streamed records (and the read-metrics they report) appear only as the iterator is drained.
 *  - every received block is validated with CRC32C via the EXISTING
 *    [[org.apache.spark.shuffle.ShuffleChecksumUtils]] facility; a corrupt block is re-requested by
 *    its addressable key and the retained block is resent and
 *    consumed successfully, within the bounded retry budget and with no invalidation.
 *  - a producer failure is turned into an atomic partial-read invalidation that throws
 *    [[FetchFailedException]] and increments the `partialReadInvalidations` telemetry.
 *  - a partially-delivered stream (a completion that claims more blocks than were delivered before
 *    the producer failed) is NEVER reported as a truncated success; the read is invalidated
 *    atomically without leaking a half-applied result.
 *  - full coverage across MULTIPLE producing maps returns every record, and the BOUNDED inbox
 *    blocks a fast producer until the consumer drains (backpressure) rather than buffering without
 *    limit.
 */
class StreamingShuffleReaderSuite extends SparkFunSuite with SharedSparkContext
  with Matchers with Eventually {

  // Align block integrity with the feature's mandated CRC32C facility. Both the test-side producer
  // (`checksumOf`) and the reader read this same config, so checksums are computed identically.
  conf.set(config.SHUFFLE_CHECKSUM_ALGORITHM, "CRC32C")
  // Pin streaming debug logging OFF (independent of any inherited system-property default) so the
  // reader's verbose path stays quiet and the test honors the streaming log-volume budget.
  conf.set(config.STREAMING_SHUFFLE_DEBUG, false)

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

  // Compute a block's checksum through the EXISTING ShuffleChecksumUtils facility (CRC32C), exactly
  // as the producer does, so the reader's validation accepts an uncorrupted block.
  private def checksumOf(bytes: Array[Byte]): Long =
    ShuffleChecksumUtils.computeChecksum(conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM), bytes)

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

  /**
   * Construct the reader under test over a mocked dependency (serializer only, no aggregator or key
   * ordering, so the returned iterator yields the streamed pairs directly). Returns both the reader
   * and the [[TempShuffleReadMetrics]] it reports into, so a test can assert the read-metrics it
   * updates (e.g. that NO record is counted until the lazy iterator is drained).
   */
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
      // Test seams (F6/F7): instant retry backoff by default so the bounded-retry loop runs with no
      // real sleeps, and the production producer-timeout deadline unless a test shortens it.
      producerTimeoutMillis: Long = StreamingShuffleReader.DefaultProducerTimeoutMillis,
      retryBackoffStartMillis: Long = 0L,
      shuffleId: Int = 0,
      // Number of producing maps for this shuffle, carried on the StreamingShuffleHandle exactly as
      // StreamingShuffleManager captures it on the driver. The reader resolves the
      // `ShuffleManager.getReader` endMapIndex=Int.MaxValue sentinel to this count, so a test can
      // drive the production full-read path with a reachable completion target.
      numMaps: Int = 1,
      // Cross-executor rendezvous seam (R-A): the one-shot remote-subscribe thunk the reader fires
      // when read() begins. Defaults to a no-op (the local-only path every other test exercises);
      // a test can inject a recording thunk to assert the production wiring `getReader` performs.
      remoteSubscribe: () => Unit = () => ()):
      (StreamingShuffleReader[Int, Int], TempShuffleReadMetrics) = {
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
    val handle = new StreamingShuffleHandle[Int, Int, Int](shuffleId, dependency, numMaps)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    val reader = new StreamingShuffleReader[Int, Int](
      handle, startMapIndex, endMapIndex, startPartition, endPartition,
      context, readMetrics, conf, backpressure, source, exchange,
      inboxCapacity, maxInboxBytes,
      producerTimeoutMillis = producerTimeoutMillis,
      retryBackoffStartMillis = retryBackoffStartMillis,
      remoteSubscribe = remoteSubscribe)
    (reader, readMetrics)
  }

  test("read() returns a lazy iterator over in-progress blocks and reports read metrics") {
    withStreaming { (source, spill, exchange, backpressure) =>
      val (reader, readMetrics) = newReader(exchange, backpressure, source, 0, 1, 0, 1)
      val block0 = serialize(ShuffleBlockId(0, 0L, 0), Seq((10, 100), (20, 200)))
      val block1 = serialize(ShuffleBlockId(0, 0L, 0), Seq((30, 300)))
      exchange.publishBlock(metaFor(0, 0L, 0, 0L, 0, block0), block0)
      exchange.publishBlock(metaFor(0, 0L, 0, 1L, 0, block1), block1)
      exchange.completeMap(0, 0L, 0, Array(2L))

      // CRITICAL lazy boundary: building the iterator performs NO eager fetching, so not a single
      // record has been read or counted yet -- all polling/blocking lives behind hasNext/next.
      val it = reader.read()
      readMetrics.recordsRead mustBe 0L

      // Draining the iterator yields the streamed records and reports exactly one read per record.
      val pairs = it.toList.map(r => (r._1, r._2))
      pairs must contain theSameElementsAs Seq((10, 100), (20, 200), (30, 300))
      readMetrics.recordsRead mustBe 3L
      // Every consumed block was acknowledged, so the writer-side buffers reclaim back to zero.
      eventually(timeout(Span(2000, Millis)), interval(Span(20, Millis))) {
        spill.trackedBytesTotal mustBe 0L
      }
    }
  }

  test("read() fires the remote-subscribe rendezvous thunk exactly once (cross-executor wiring)") {
    withStreaming { (source, spill, exchange, backpressure) =>
      val subscribeCount = new AtomicInteger(0)
      val (reader, _) = newReader(exchange, backpressure, source, 0, 1, 0, 1,
        remoteSubscribe = () => { subscribeCount.incrementAndGet(); () })

      // The thunk must NOT fire at construction: cross-executor discovery is deferred to the lazy
      // read() boundary, so a reader that is built but never consumed pays no rendezvous cost and
      // local-only call sites (and every other test) are unaffected by the default no-op.
      subscribeCount.get() mustBe 0

      // The first read() fires the thunk exactly once, BEFORE returning the lazy iterator. No
      // blocks are published, so the iterator is never drained; rendezvous is independent of them.
      reader.read()
      subscribeCount.get() mustBe 1

      // A second read() must NOT re-subscribe: the one-shot AtomicBoolean guard makes remote
      // producer discovery happen at most once per reader, so it can never double-subscribe.
      reader.read()
      subscribeCount.get() mustBe 1
    }
  }

  test("validates CRC32C and retransmits the retained block on corruption (bounded retries)") {
    withStreaming { (source, spill, exchange, backpressure) =>
      val (reader, _) = newReader(exchange, backpressure, source, 0, 1, 0, 1)
      val good = serialize(ShuffleBlockId(0, 0L, 0), Seq((5, 55)))
      val meta = metaFor(0, 0L, 0, 0L, 0, good)
      // The exact good block is RETAINED in the spill manager (as the writer retains it) so a
      // resend can re-read it by its key. The reader first receives a CORRUPTED copy of the block.
      spill.register(meta.key, good)
      val corrupt = good.clone()
      val mid = corrupt.length / 2
      corrupt(mid) = (corrupt(mid) ^ 0xFF).toByte
      reader.onBlockReceived(meta, corrupt)
      exchange.completeMap(0, 0L, 0, Array(1L))

      // The reader recomputes the CRC32C, detects the mismatch, requests a block-specific resend
      // (instant backoff via the retryBackoffStartMillis test seam -- no real sleep) which the
      // exchange satisfies from the retained block, and the read then succeeds with the correct
      // record. A single resend is well within the <=5-attempt budget, so no invalidation occurs.
      val pairs = reader.read().toList.map(r => (r._1, r._2))
      pairs mustBe Seq((5, 55))
      invalidations(source) mustBe 0L
      // After successful consumption the block is acknowledged and reclaimed.
      eventually(timeout(Span(2000, Millis)), interval(Span(20, Millis))) {
        spill.trackedBytesTotal mustBe 0L
      }
    }
  }

  test("a permanently-corrupt block invalidates after the bounded retry budget is exhausted") {
    withStreaming { (source, spill, exchange, backpressure) =>
      // Instant backoff (newReader passes retryBackoffStartMillis = 0) keeps the bounded-retry loop
      // deterministic with no real sleeps. The block stays corrupt on EVERY resend: the bytes the
      // exchange re-reads from the retained spill entry are themselves corrupt, so each
      // retransmission fails CRC32C again, exercising the full <=5-attempt failure bound.
      val (reader, _) = newReader(exchange, backpressure, source, 0, 1, 0, 1)
      val good = serialize(ShuffleBlockId(0, 0L, 0), Seq((9, 99)))
      val meta = metaFor(0, 0L, 0, 0L, 0, good)
      val corrupt = good.clone()
      val mid = corrupt.length / 2
      corrupt(mid) = (corrupt(mid) ^ 0xFF).toByte
      // Retain the CORRUPT bytes so every key-addressed resend re-delivers corruption.
      spill.register(meta.key, corrupt)
      reader.onBlockReceived(meta, corrupt)
      exchange.completeMap(0, 0L, 0, Array(1L))

      // After MAX_RETRY_ATTEMPTS (5) failed validations the read is invalidated atomically: it
      // throws FetchFailedException (never a truncated success) and bumps partialReadInvalidations
      // exactly once.
      intercept[FetchFailedException] {
        reader.read().toList
      }
      invalidations(source) mustBe 1L
    }
  }

  test("a producer failure invalidates the read with FetchFailedException") {
    withStreaming { (source, _, exchange, backpressure) =>
      val (reader, _) = newReader(exchange, backpressure, source, 0, 1, 0, 1)
      exchange.producerFailed(0, 0L, 0, bmId, "simulated producer crash", null)

      // The producer-failure notification is turned into an atomic partial-read invalidation: the
      // reader throws FetchFailedException (which the unmodified scheduler converts into upstream
      // recomputation) and bumps the partialReadInvalidations telemetry exactly once.
      intercept[FetchFailedException] {
        reader.read().toList
      }
      invalidations(source) mustBe 1L
    }
  }

  test("a partially-delivered stream is atomically invalidated, never a truncated success") {
    withStreaming { (source, _, exchange, backpressure) =>
      val (reader, _) = newReader(exchange, backpressure, source, 0, 1, 0, 1)
      // The map reports it emitted TWO blocks for partition 0, but only ONE is delivered before the
      // producer fails -- a partially-delivered stream. The reader must NEVER treat the single
      // delivered block plus the completion signal as a (truncated) success.
      val block0 = serialize(ShuffleBlockId(0, 0L, 0), Seq((10, 100)))
      exchange.publishBlock(metaFor(0, 0L, 0, 0L, 0, block0), block0)
      exchange.completeMap(0, 0L, 0, Array(2L))
      exchange.producerFailed(0, 0L, 0, bmId, "producer crashed mid-stream", null)

      var consumed = 0
      // The read RAISES rather than silently returning fewer records: it surfaces the invalidation
      // as FetchFailedException after observing at most the single delivered record.
      intercept[FetchFailedException] {
        reader.read().foreach(_ => consumed += 1)
      }
      consumed must be < 2
      invalidations(source) mustBe 1L
    }
  }

  test("a silent producer that never completes times out and is atomically invalidated") {
    withStreaming { (source, _, exchange, backpressure) =>
      // A tiny producer-timeout seam makes the deadline deterministic: the producer sends no block
      // and never signals completion, so the reader's inbox poll times out quickly. The value only
      // bounds how long the poll waits; correctness does not depend on it (nothing is ever sent).
      val (reader, _) = newReader(exchange, backpressure, source, 0, 1, 0, 1,
        producerTimeoutMillis = 100L)

      // With no block and no completion, coverage can never be satisfied, so the timeout is a
      // truncated read -- surfaced as an atomic FetchFailedException (never a truncated success),
      // incrementing partialReadInvalidations exactly once, with no record yielded.
      intercept[FetchFailedException] {
        reader.read().toList
      }
      invalidations(source) mustBe 1L
    }
  }

  test("a fully-covered stream across multiple maps returns every record") {
    withStreaming { (source, _, exchange, backpressure) =>
      // Two producing maps each stream one block for partition 0; the reader needs a completion
      // from BOTH maps and every block each reported before it may finish (multi-map coverage).
      val (reader, _) = newReader(exchange, backpressure, source, 0, 2, 0, 1)
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

  test("read() resolves endMapIndex=Int.MaxValue (the 5-arg getReader sentinel) to the actual " +
      "map count and returns every record") {
    // Regression guard for the production full-read path (QA finding F-1): `ShuffleManager`'s 5-arg
    // getReader delegates with endMapIndex=Int.MaxValue, meaning "all map outputs of the shuffle".
    // The reader MUST resolve that sentinel to the real number of maps (the numMaps the manager
    // captured on the driver and carried in StreamingShuffleHandle);
    // treating it literally makes `expectedMapCount` ~2.1e9 and the completion gate
    // `completedMaps.size >= expectedMapCount` unreachable, so a fully-delivered stream would time
    // out and abort every non-combine streaming job. The other reader tests use only a BOUNDED
    // endMapIndex (1 or 2), so this is the case that exercises the Int.MaxValue contract.
    withStreaming { (source, _, exchange, backpressure) =>
      val numMaps = 4
      // Drive the reader as the 5-arg getReader does: startMapIndex=0, endMapIndex=Int.MaxValue.
      val (reader, _) = newReader(
        exchange, backpressure, source, 0, Int.MaxValue, 0, 1, numMaps = numMaps)
      // Each of the `numMaps` maps streams one block for partition 0 and then completes. Full
      // coverage requires a completion from ALL maps -- only reachable once the sentinel resolves
      // to numMaps (=4) rather than Int.MaxValue.
      val expected = (0 until numMaps).map { m =>
        val pair = (m, m * 10)
        val bytes = serialize(ShuffleBlockId(0, m.toLong, 0), Seq(pair))
        exchange.publishBlock(metaFor(0, m.toLong, 0, 0L, m, bytes), bytes)
        exchange.completeMap(0, m.toLong, m, Array(1L))
        pair
      }

      val pairs = reader.read().toList.map(r => (r._1, r._2))
      pairs must contain theSameElementsAs expected
    }
  }

  test("the bounded inbox blocks a fast producer until the consumer drains (backpressure)") {
    withStreaming { (source, _, exchange, backpressure) =>
      val block = serialize(ShuffleBlockId(0, 0L, 0), Seq((7, 70)))
      // Bound the inbox to a single block's worth of bytes so the SECOND publish must block.
      val (reader, _) = newReader(exchange, backpressure, source, 0, 1, 0, 1,
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

      // Deterministically wait until the producer is provably parked inside the second publish's
      // delivery callback (blocked acquiring the exhausted byte budget) rather than sleeping a
      // fixed wall-clock interval. Once parked, exactly one delivery has completed and the second
      // cannot advance until the consumer drains the first block -- proving backpressure.
      eventually(timeout(Span(2000, Millis)), interval(Span(10, Millis))) {
        val state = producer.getState
        assert(state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING,
          s"expected the producer to be blocked in publishBlock, but it was $state")
      }
      delivered.get() mustBe 1

      // Draining the reader releases the byte budget, unblocking the producer; full coverage (two
      // blocks) then completes the read with both records.
      val records = reader.read().toList
      records.size mustBe 2
      eventually(timeout(Span(2000, Millis)), interval(Span(20, Millis))) {
        delivered.get() mustBe 2
      }
      producer.join(5000)
    }
  }
}
