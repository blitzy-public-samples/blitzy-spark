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

import scala.jdk.CollectionConverters._

import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.streaming.StreamingBlockExchange.{BlockMeta, StreamingBlockConsumer}
import org.apache.spark.memory.TestMemoryManager
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}

/**
 * Unit tests for [[StreamingBlockExchange]], the in-process producer->consumer data path that wires
 * the streaming [[StreamingShuffleWriter]]'s emitted blocks to the [[StreamingShuffleReader]]'s
 * callbacks. The suite needs no running `SparkContext`: it drives the exchange directly with a real
 * [[MemorySpillManager]] (backed by a [[TestMemoryManager]]) and a collecting test consumer, so the
 * end-to-end routing, ack-to-reclamation, block-specific retransmission, and coverage-signal
 * behaviors are verified deterministically.
 *
 * Coverage maps to the CP1 review findings:
 *  - a published block is STORED in the spill manager AND delivered to the owning reducer (the
 *    actual producer->consumer data path that was previously absent).
 *  - blocks published before a reducer subscribes are replayed on registration (no lost blocks).
 *  - ack drives [[MemorySpillManager.reclaim]] so writer-side buffers are freed after consumption.
 *  - requestResend re-delivers EXACTLY the requested block by its addressable key (block-specific
 *    NACK), and fails cleanly once the block is no longer retained.
 *  - map-completion and producer-failure signals are delivered live and replayed to late readers,
 *    so a consumer can verify full coverage and invalidate on failure.
 */
class StreamingBlockExchangeSuite extends SparkFunSuite with Matchers {

  import MemorySpillManager.BlockKey

  private val bmId = BlockManagerId("exec-1", "host-1", 7337)

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
   * Builds an exchange over a real spill manager, runs the body, and tears both down so no daemon
   * poller leaks.
   */
  private def withExchange(conf: SparkConf)(
      body: (StreamingBlockExchange, MemorySpillManager) => Unit): Unit = {
    val resolver = new IndexShuffleBlockResolver(conf)
    val spill = new MemorySpillManager(conf, new TestMemoryManager(conf), resolver,
      new StreamingShuffleSource)
    val exchange = new StreamingBlockExchange(conf, spill)
    try {
      body(exchange, spill)
    } finally {
      exchange.stop()
      spill.stop()
      resolver.stop()
    }
  }

  private def metaFor(
      shuffleId: Int,
      mapId: Long,
      mapIndex: Int,
      partitionId: Int,
      seq: Long,
      size: Int): BlockMeta = {
    val key = BlockKey(shuffleId, mapId, partitionId, seq)
    BlockMeta(key, bmId, ShuffleBlockId(shuffleId, mapId, partitionId), mapIndex, 0L, size.toLong)
  }

  test("publishBlock stores the bytes and delivers them to the subscribed reducer") {
    withExchange(new SparkConf(false)) { (exchange, spill) =>
      val consumer = new CollectingConsumer
      exchange.registerReader(0, startMapIndex = 0, endMapIndex = 1,
        startPartition = 0, endPartition = 1, consumer)
      val meta = metaFor(0, mapId = 0L, mapIndex = 0, partitionId = 0, seq = 0L, size = 64)
      val bytes = Array.fill(64)(3.toByte)

      exchange.publishBlock(meta, bytes) mustBe true

      // The exchange routed the block to the owning reducer...
      consumer.blocks.size() mustBe 1
      val (gotMeta, gotBytes) = consumer.blocks.peek()
      gotMeta.key mustBe meta.key
      gotBytes.toSeq mustBe bytes.toSeq
      // ...and stored the bytes in the shared spill manager so they can be re-read / retransmitted.
      spill.read(meta.key).map(_.toSeq) mustBe Some(bytes.toSeq)
    }
  }

  test("a block published before the reducer subscribes is replayed on registration") {
    withExchange(new SparkConf(false)) { (exchange, _) =>
      val meta = metaFor(0, mapId = 0L, mapIndex = 0, partitionId = 0, seq = 0L, size = 32)
      val bytes = Array.fill(32)(1.toByte)
      // Publish BEFORE any reader exists; the block is buffered as pending (bytes kept in spill).
      exchange.publishBlock(meta, bytes)

      val consumer = new CollectingConsumer
      // Subscribing replays the pending block to the new reducer.
      exchange.registerReader(0, 0, 1, 0, 1, consumer)
      consumer.blocks.size() mustBe 1
      consumer.blocks.peek()._1.key mustBe meta.key
    }
  }

  test("ack reclaims the consumed block from the spill manager") {
    withExchange(new SparkConf(false)) { (exchange, spill) =>
      val consumer = new CollectingConsumer
      exchange.registerReader(0, 0, 1, 0, 1, consumer)
      val meta = metaFor(0, 0L, 0, 0, 0L, size = 100)
      exchange.publishBlock(meta, Array.fill(100)(2.toByte))
      spill.trackedBytesTotal mustBe 100L

      // The reducer acknowledges consumption: the writer-side buffer is reclaimed.
      exchange.ack(meta)
      spill.read(meta.key) mustBe None
      spill.trackedBytesTotal mustBe 0L
      spill.reservedBytesTotal mustBe 0L
    }
  }

  test("requestResend re-delivers exactly the requested block, and fails once reclaimed") {
    withExchange(new SparkConf(false)) { (exchange, _) =>
      val consumer = new CollectingConsumer
      exchange.registerReader(0, 0, 1, 0, 1, consumer)
      val meta = metaFor(0, 0L, 0, 0, 0L, size = 48)
      val bytes = Array.fill(48)(5.toByte)
      exchange.publishBlock(meta, bytes)
      consumer.blocks.size() mustBe 1

      // A block-specific resend (e.g. after a CRC mismatch) re-delivers the SAME block by key.
      exchange.requestResend(meta) mustBe true
      consumer.blocks.size() mustBe 2
      consumer.blocks.asScala.toList.last._1.key mustBe meta.key

      // Once the block is acknowledged and reclaimed, it can no longer be resent.
      exchange.ack(meta)
      exchange.requestResend(meta) mustBe false
    }
  }

  test("map-completion and producer-failure are delivered live and replayed to a late reader") {
    withExchange(new SparkConf(false)) { (exchange, _) =>
      val live = new CollectingConsumer
      exchange.registerReader(0, 0, 2, 0, 1, live)

      val counts = Array(1L)
      exchange.completeMap(0, mapId = 10L, mapIndex = 0, counts)
      exchange.producerFailed(0, mapId = 11L, mapIndex = 1, bmId, "boom", null)

      // The live reader observed both signals.
      live.completions.size() mustBe 1
      live.failures.size() mustBe 1
      live.completions.peek()._1 mustBe 10L
      live.failures.peek()._2 mustBe 1

      // A reader that subscribes AFTER the signals still observes them (persistent replay), so a
      // late-starting reduce task can verify coverage and act on failures.
      val late = new CollectingConsumer
      exchange.registerReader(0, 0, 2, 0, 1, late)
      late.completions.size() mustBe 1
      late.failures.size() mustBe 1
    }
  }

  test("routing delivers each block only to the reducer that owns its partition") {
    withExchange(new SparkConf(false)) { (exchange, _) =>
      val readerP0 = new CollectingConsumer
      val readerP1 = new CollectingConsumer
      exchange.registerReader(0, 0, 1, startPartition = 0, endPartition = 1, readerP0)
      exchange.registerReader(0, 0, 1, startPartition = 1, endPartition = 2, readerP1)

      exchange.publishBlock(metaFor(0, 0L, 0, partitionId = 0, 0L, 16), Array.fill(16)(0.toByte))
      exchange.publishBlock(metaFor(0, 0L, 0, partitionId = 1, 0L, 16), Array.fill(16)(0.toByte))

      // Each reducer received only its own partition's block.
      readerP0.blocks.size() mustBe 1
      readerP1.blocks.size() mustBe 1
      readerP0.blocks.peek()._1.reduceId mustBe 0
      readerP1.blocks.peek()._1.reduceId mustBe 1
    }
  }

  test("stop() short-circuits publish and drops routing state") {
    withExchange(new SparkConf(false)) { (exchange, _) =>
      val consumer = new CollectingConsumer
      exchange.registerReader(0, 0, 1, 0, 1, consumer)
      exchange.stop()
      // After stop, publishing is refused and nothing is delivered.
      exchange.publishBlock(metaFor(0, 0L, 0, 0, 0L, 8), Array.fill(8)(1.toByte)) mustBe false
      consumer.blocks.size() mustBe 0
      exchange.requestResend(metaFor(0, 0L, 0, 0, 0L, 8)) mustBe false
    }
  }
}
