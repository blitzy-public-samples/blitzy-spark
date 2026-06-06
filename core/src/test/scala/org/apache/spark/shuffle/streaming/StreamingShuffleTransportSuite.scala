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

import java.util.concurrent.ConcurrentLinkedQueue

import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.memory.TestMemoryManager
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.streaming.StreamingBlockExchange.{BlockMeta, StreamingBlockConsumer}
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}

/**
 * Integration tests for the cross-executor data path: [[StreamingShuffleTransport]] (the reuse of
 * Spark's existing Netty `TransportContext` network stack) wired to [[StreamingBlockExchange]].
 *
 * Unlike [[StreamingBlockExchangeSuite]], which drives the exchange purely in-process, this suite
 * stands up TWO independent transport+exchange endpoints on the loopback interface and moves blocks
 * between them over a REAL socket hop. It directly addresses the CP2 finding that the active
 * manager previously used only an in-process exchange and therefore could not satisfy the producer
 * -> consumer streaming-across-executors requirement (AAP row 9 and section 0.4 "Network
 * transport").
 *
 * The two endpoints share no objects: each owns its own [[MemorySpillManager]],
 * [[StreamingBlockExchange]], and [[StreamingShuffleTransport]] bound to its own ephemeral port,
 * exactly as two executors would. Coverage:
 *  - a published block streams from the producer endpoint to the consumer endpoint over the network
 *    and is delivered to the consumer's local reader callback with its bytes and identity intact;
 *  - a consumer ACK travels back over the network and reclaims the PRODUCER's buffer (the bytes are
 *    owned by the producer's spill manager until the remote ack frees them);
 *  - a consumer RESEND request travels back over the network and the producer re-delivers exactly
 *    that block;
 *  - remote map-completion and producer-failure signals route to the consumer's local reader.
 *
 * The network plane is asynchronous, so arrivals are awaited with `eventually` rather than asserted
 * synchronously; there are no fixed `Thread.sleep` waits.
 */
class StreamingShuffleTransportSuite
  extends SparkFunSuite
    with Matchers
    with Eventually {

  import MemorySpillManager.BlockKey

  // Bind to the loopback address explicitly so no DNS lookup is involved and the two in-JVM
  // endpoints address each other deterministically.
  private val LoopbackHost = "127.0.0.1"

  // A producer-side block-manager id stamped into every block's metadata. On a real cluster this is
  // the producing executor's id; here it only needs to round-trip through the wire codec intact.
  private val producerBmId = BlockManagerId("producer-exec", LoopbackHost, 7337)

  // The shared patience for awaiting asynchronous network delivery. Generous timeout, short poll.
  private implicit val patience: PatienceConfig =
    PatienceConfig(timeout = 15.seconds, interval = 25.milliseconds)

  /** A [[StreamingBlockConsumer]] that records every callback for assertions (thread-safe). */
  private class CollectingConsumer extends StreamingBlockConsumer {
    val blocks = new ConcurrentLinkedQueue[(BlockMeta, Array[Byte])]()
    val completions = new ConcurrentLinkedQueue[(Long, Int, Array[Long])]()
    val failures = new ConcurrentLinkedQueue[(Long, Int, String)]()

    override def onBlockReceived(meta: BlockMeta, bytes: Array[Byte]): Unit =
      blocks.add((meta, bytes))
    override def onMapComplete(mapId: Long, mapIndex: Int, blockCounts: Array[Long]): Unit =
      completions.add((mapId, mapIndex, blockCounts))
    override def onProducerFailed(
        bmAddress: BlockManagerId,
        mapId: Long,
        mapIndex: Int,
        message: String,
        cause: Throwable): Unit = failures.add((mapId, mapIndex, message))
  }

  /**
   * One streaming endpoint: a spill manager, an exchange bound to a transport, and the disk
   * resolver backing the spill manager. Models a single executor's streaming plane.
   */
  private class Endpoint(
      val exchange: StreamingBlockExchange,
      val spill: MemorySpillManager,
      val transport: StreamingShuffleTransport,
      val resolver: IndexShuffleBlockResolver) {

    /** This endpoint's streaming-transport host (the loopback address). */
    def host: String = transport.host

    /** This endpoint's streaming-transport ephemeral port. */
    def port: Int = transport.port

    /** Tears the endpoint down in reverse construction order so no Netty/poller threads leak. */
    def stop(): Unit = {
      // Stop the network plane first so no late inbound message reaches a stopped exchange, then
      // the exchange, then the spill manager (its 100ms poller), then the disk resolver.
      try { transport.stop() } catch { case scala.util.control.NonFatal(_) => () }
      try { exchange.stop() } catch { case scala.util.control.NonFatal(_) => () }
      try { spill.stop() } catch { case scala.util.control.NonFatal(_) => () }
      try { resolver.stop() } catch { case scala.util.control.NonFatal(_) => () }
    }
  }

  /** Builds one fully-wired endpoint over a fresh `conf`, binding its transport to loopback. */
  private def newEndpoint(conf: SparkConf): Endpoint = {
    val resolver = new IndexShuffleBlockResolver(conf)
    val spill = new MemorySpillManager(
      conf, new TestMemoryManager(conf), resolver, new StreamingShuffleSource)
    val exchange = new StreamingBlockExchange(conf, spill)
    // Two-step wiring: the transport needs the exchange as its receive listener, and the exchange
    // needs the transport to send outbound messages -- so the transport is constructed first and
    // then bound, exactly as StreamingShuffleManager's engine does in production.
    val transport = new StreamingShuffleTransport(conf, LoopbackHost, exchange)
    exchange.bindTransport(transport)
    new Endpoint(exchange, spill, transport, resolver)
  }

  /**
   * Stands up a producer endpoint and a consumer endpoint (two independent executors on loopback),
   * runs `body`, and always tears both down.
   */
  private def withEndpointPair(body: (Endpoint, Endpoint) => Unit): Unit = {
    val conf = new SparkConf(false)
    val producer = newEndpoint(conf)
    val consumer = newEndpoint(conf)
    try {
      body(producer, consumer)
    } finally {
      consumer.stop()
      producer.stop()
    }
  }

  /** Builds block metadata mirroring [[StreamingBlockExchangeSuite]]'s `metaFor`. */
  private def metaFor(
      shuffleId: Int,
      mapId: Long,
      mapIndex: Int,
      partitionId: Int,
      seq: Long,
      size: Int): BlockMeta = {
    val key = BlockKey(shuffleId, mapId, partitionId, seq)
    BlockMeta(
      key, producerBmId, ShuffleBlockId(shuffleId, mapId, partitionId), mapIndex, 0L, size.toLong)
  }

  /**
   * Subscribes a fresh consumer to the producer for shuffle 0, map index 0, reduce partition 0, and
   * returns the consumer's recording callback. The reader is registered LOCALLY first (so an
   * arriving block has a target) and then a remote SUBSCRIBE is sent to the producer's endpoint.
   */
  private def subscribe(producer: Endpoint, consumer: Endpoint): CollectingConsumer = {
    val reader = new CollectingConsumer
    consumer.exchange.registerReader(
      shuffleId = 0, startMapIndex = 0, endMapIndex = 1,
      startPartition = 0, endPartition = 1, reader)
    consumer.exchange.subscribeRemote(
      producer.host, producer.port,
      shuffleId = 0, startMapIndex = 0, endMapIndex = 1, startPartition = 0, endPartition = 1)
    reader
  }

  test("a published block streams from a remote producer to a remote consumer over a socket") {
    withEndpointPair { (producer, consumer) =>
      val reader = subscribe(producer, consumer)
      val payload = Array.tabulate[Byte](64)(i => (i % 13).toByte)
      val meta = metaFor(
        0, mapId = 0L, mapIndex = 0, partitionId = 0, seq = 0L, size = payload.length)

      // Publish on the producer endpoint. If the SUBSCRIBE has not yet arrived, the producer
      // buffers the block and the exchange replays it the moment the remote reader registers, so
      // delivery is guaranteed regardless of subscribe/publish ordering.
      producer.exchange.publishBlock(meta, payload)

      eventually {
        reader.blocks.size() must be >= 1
      }
      val (receivedMeta, receivedBytes) = reader.blocks.peek()
      // The bytes crossed a real socket intact, and the block's addressable identity is preserved.
      receivedBytes.toSeq mustBe payload.toSeq
      receivedMeta.key mustBe meta.key
      receivedMeta.shuffleId mustBe 0
      receivedMeta.reduceId mustBe 0
    }
  }

  test("a remote consumer ack reclaims the producer's buffer over the network") {
    withEndpointPair { (producer, consumer) =>
      val reader = subscribe(producer, consumer)
      val payload = Array.tabulate[Byte](128)(i => (i % 7).toByte)
      val meta = metaFor(
        0, mapId = 0L, mapIndex = 0, partitionId = 0, seq = 0L, size = payload.length)

      producer.exchange.publishBlock(meta, payload)
      // The producer's spill manager owns the bytes until the consumer acks.
      eventually {
        reader.blocks.size() must be >= 1
        producer.spill.trackedBytesTotal must be > 0L
      }

      // Ack from the consumer: the exchange routes the ACK back to the producer endpoint over the
      // network (the block is remotely owned), and the producer reclaims its buffer.
      consumer.exchange.ack(meta)
      eventually {
        producer.spill.trackedBytesTotal mustBe 0L
        producer.spill.reservedBytesTotal mustBe 0L
      }
    }
  }

  test("a remote consumer resend request re-delivers exactly the requested block") {
    withEndpointPair { (producer, consumer) =>
      val reader = subscribe(producer, consumer)
      val payload = Array.tabulate[Byte](96)(i => (i % 11).toByte)
      val meta = metaFor(
        0, mapId = 0L, mapIndex = 0, partitionId = 0, seq = 0L, size = payload.length)

      producer.exchange.publishBlock(meta, payload)
      eventually {
        reader.blocks.size() must be >= 1
      }

      // Request a resend (as the reader would after a CRC mismatch): the request travels to the
      // producer endpoint, which re-reads the exact block from its spill manager and resends it.
      consumer.exchange.requestResend(meta) mustBe true
      eventually {
        reader.blocks.size() must be >= 2
      }
      // Every delivered copy is byte-identical to the original block.
      reader.blocks.toArray.foreach {
        case (_, bytes: Array[Byte]) => bytes.toSeq mustBe payload.toSeq
        case _ => fail("unexpected entry type in delivered blocks")
      }
    }
  }

  test("remote map-completion and producer-failure signals route to the consumer") {
    withEndpointPair { (producer, consumer) =>
      val reader = subscribe(producer, consumer)

      // A completed map with two blocks for reduce partition 0 routes over the network to the
      // consumer's coverage callback.
      producer.exchange.completeMap(
        shuffleId = 0, mapId = 0L, mapIndex = 0, blockCounts = Array(2L))
      eventually {
        reader.completions.size() must be >= 1
      }
      val (mapId, mapIndex, counts) = reader.completions.peek()
      mapId mustBe 0L
      mapIndex mustBe 0
      counts.toSeq mustBe Seq(2L)

      // A producer failure routes over the network to the consumer's failure callback, which on a
      // real reduce task drives the existing FetchFailedException recomputation path.
      producer.exchange.producerFailed(
        shuffleId = 0, mapId = 0L, mapIndex = 0, bmAddress = producerBmId,
        message = "simulated producer crash", cause = null)
      eventually {
        reader.failures.size() must be >= 1
      }
      val (failedMapId, failedMapIndex, message) = reader.failures.peek()
      failedMapId mustBe 0L
      failedMapIndex mustBe 0
      message mustBe "simulated producer crash"
    }
  }
}
