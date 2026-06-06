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
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.streaming.StreamingBlockExchange.{BlockMeta, StreamingBlockConsumer}
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.RpcUtils

/**
 * Tests for the cross-executor rendezvous closure of the streaming-shuffle feature:
 * [[StreamingShuffleEndpointCoordinator]] (the additive driver `RpcEndpoint` directory of
 * `executorId -> (host, port)` streaming transports) and its executor-side client
 * [[StreamingShuffleRendezvous]].
 *
 * The review's CRITICAL finding (group A) was that the cross-executor data path existed but had no
 * PRODUCTION caller: reducers only ever subscribed to co-located producers via the local
 * `exchange.registerReader`, and the remote `subscribeRemote` path was exercised only by a manual
 * transport test. This suite proves the production rendezvous now closes that gap:
 *
 *  1. Directory semantics over a REAL `RpcEnv`: register, last-registration-wins re-advertise,
 *     unregister, full-directory snapshot, self-exclusion, and best-effort behavior when the
 *     coordinator is unreachable (discovery never throws into a reduce task).
 *  2. End-to-end production path: a consumer discovers a remote producer THROUGH the coordinator
 *     and subscribes to it via [[StreamingShuffleRendezvous.subscribeToRemoteProducers]] -- the
 *     exact call `StreamingShuffleManager.getReader` wires into the reader's `remoteSubscribe`
 *     thunk -- after which a block published on the producer streams over a real socket to the
 *     consumer's reader. No scheduler, task, or `MapOutputTracker` code is involved.
 *
 * RPC delivery and network arrival are asynchronous, so effects are awaited with `eventually`; no
 * fixed sleeps are used. The ask timeout is set short so the failed-coordinator path fails fast.
 */
class StreamingShuffleEndpointCoordinatorSuite
  extends SparkFunSuite
    with Matchers
    with Eventually {

  import MemorySpillManager.BlockKey

  // Loopback so the in-JVM RpcEnv(s) and transport endpoints address each other with no DNS lookup.
  private val LoopbackHost = "127.0.0.1"

  // Generous await for asynchronous RPC/network effects; short poll interval.
  private implicit val patience: PatienceConfig =
    PatienceConfig(timeout = 15.seconds, interval = 25.milliseconds)

  // A short ask timeout keeps the "coordinator unreachable" discovery from blocking on the default
  // 120s network timeout; every other ask resolves in well under this bound on loopback.
  private def newConf(): SparkConf = new SparkConf(false).set("spark.rpc.askTimeout", "5s")

  /**
   * Stands up a driver-style `RpcEnv` hosting one coordinator endpoint, runs `body` with the
   * coordinator's [[RpcEndpointRef]] and the owning conf, and always shuts the env down.
   */
  private def withCoordinator(body: (RpcEndpointRef, SparkConf) => Unit): Unit = {
    val conf = newConf()
    val rpcEnv = RpcEnv.create("streaming-rendezvous-test", LoopbackHost, 0, conf,
      new SecurityManager(conf))
    try {
      val ref = rpcEnv.setupEndpoint(
        StreamingShuffleEndpointCoordinator.ENDPOINT_NAME,
        new StreamingShuffleEndpointCoordinator(rpcEnv))
      body(ref, conf)
    } finally {
      rpcEnv.shutdown()
    }
  }

  /** Full directory snapshot (no self-filtering) read straight off the coordinator endpoint. */
  private def directoryOf(ref: RpcEndpointRef, conf: SparkConf): Seq[StreamingEndpointInfo] =
    ref.askSync[StreamingEndpoints](GetStreamingEndpoints, RpcUtils.askRpcTimeout(conf)).endpoints

  test("registers advertised endpoints and returns them in a directory snapshot") {
    withCoordinator { (ref, conf) =>
      new StreamingShuffleRendezvous(ref, conf, "exec-A").register("host-a", 1001)
      new StreamingShuffleRendezvous(ref, conf, "exec-B").register("host-b", 1002)

      // `register` is fire-and-forget over RPC, so await propagation, then assert both rows.
      eventually {
        val byId = directoryOf(ref, conf).map(e => e.executorId -> (e.host, e.port)).toMap
        byId must contain("exec-A" -> (("host-a", 1001)))
        byId must contain("exec-B" -> (("host-b", 1002)))
      }
    }
  }

  test("a re-advertisement replaces the prior endpoint (last registration wins)") {
    withCoordinator { (ref, conf) =>
      val rendezvous = new StreamingShuffleRendezvous(ref, conf, "exec-A")
      rendezvous.register("old-host", 1)
      eventually {
        directoryOf(ref, conf).count(_.executorId == "exec-A") mustBe 1
      }

      // The same executor re-binds its transport (new port) and re-advertises.
      rendezvous.register("new-host", 9999)
      eventually {
        val rows = directoryOf(ref, conf).filter(_.executorId == "exec-A")
        // Still exactly one row for the executor, now carrying the NEW endpoint.
        rows.size mustBe 1
        rows.head.host mustBe "new-host"
        rows.head.port mustBe 9999
      }
    }
  }

  test("unregister withdraws exactly one endpoint and leaves the others") {
    withCoordinator { (ref, conf) =>
      val a = new StreamingShuffleRendezvous(ref, conf, "exec-A")
      new StreamingShuffleRendezvous(ref, conf, "exec-B").register("host-b", 2002)
      a.register("host-a", 2001)
      eventually {
        directoryOf(ref, conf).map(_.executorId).toSet mustBe Set("exec-A", "exec-B")
      }

      a.unregister()
      eventually {
        directoryOf(ref, conf).map(_.executorId).toSet mustBe Set("exec-B")
      }
    }
  }

  test("remoteEndpoints excludes the querying executor itself") {
    withCoordinator { (ref, conf) =>
      new StreamingShuffleRendezvous(ref, conf, "exec-A").register("host-a", 3001)
      new StreamingShuffleRendezvous(ref, conf, "exec-B").register("host-b", 3002)
      eventually {
        directoryOf(ref, conf).map(_.executorId).toSet mustBe Set("exec-A", "exec-B")
      }

      // A self-exclusion guard is essential: a co-located producer is already served in-process by
      // the reader's LOCAL registration, so subscribing to self would duplicate delivery.
      val fromA = new StreamingShuffleRendezvous(ref, conf, "exec-A").remoteEndpoints()
      fromA.map(_.executorId) mustBe Seq("exec-B")

      val fromB = new StreamingShuffleRendezvous(ref, conf, "exec-B").remoteEndpoints()
      fromB.map(_.executorId) mustBe Seq("exec-A")
    }
  }

  test("discovery is best-effort: a failed coordinator ask yields no endpoints, never throws") {
    val conf = newConf()
    val rpcEnv = RpcEnv.create("streaming-rendezvous-besteffort", LoopbackHost, 0, conf,
      new SecurityManager(conf))
    // Obtain a real coordinator ref, then tear the hosting env DOWN so the ref is dangling. A
    // subsequent askSync through it fails (the env is stopped) -- and remoteEndpoints MUST swallow
    // that failure and degrade to local-only, never propagating it into the reduce task. (A live
    // empty directory would also return an empty Seq, so the env is stopped to force the genuine
    // failure path through the NonFatal catch.)
    val ref = rpcEnv.setupEndpoint(
      StreamingShuffleEndpointCoordinator.ENDPOINT_NAME,
      new StreamingShuffleEndpointCoordinator(rpcEnv))
    val rendezvous = new StreamingShuffleRendezvous(ref, conf, "exec-A")
    rpcEnv.shutdown()
    rpcEnv.awaitTermination()

    // The ask now fails internally; remoteEndpoints returns empty without throwing.
    rendezvous.remoteEndpoints() mustBe empty
    // subscribeToRemoteProducers must likewise no-op (zero subscriptions) and not throw.
    val exchange = new StreamingBlockExchange(conf, null)
    try {
      rendezvous.subscribeToRemoteProducers(exchange, 0, 0, 1, 0, 1) mustBe 0
    } finally {
      exchange.stop()
    }
  }

  // ===============================================================================================
  // End-to-end production path: coordinator discovery -> subscribeToRemoteProducers -> transfer
  // ===============================================================================================

  /** A [[StreamingBlockConsumer]] recording delivered blocks for assertions (thread-safe). */
  private class CollectingConsumer extends StreamingBlockConsumer {
    val blocks = new ConcurrentLinkedQueue[(BlockMeta, Array[Byte])]()
    override def onBlockReceived(meta: BlockMeta, bytes: Array[Byte]): Unit =
      blocks.add((meta, bytes))
    override def onMapComplete(mapId: Long, mapIndex: Int, blockCounts: Array[Long]): Unit = ()
    override def onProducerFailed(
        bmAddress: BlockManagerId,
        mapId: Long,
        mapIndex: Int,
        message: String,
        cause: Throwable): Unit = ()
  }

  /** One executor's streaming plane: spill manager + exchange bound to a transport + resolver. */
  private class Endpoint(
      val exchange: StreamingBlockExchange,
      val spill: MemorySpillManager,
      val transport: StreamingShuffleTransport,
      val resolver: IndexShuffleBlockResolver) {
    def host: String = transport.host
    def port: Int = transport.port
    def stop(): Unit = {
      try { transport.stop() } catch { case scala.util.control.NonFatal(_) => () }
      try { exchange.stop() } catch { case scala.util.control.NonFatal(_) => () }
      try { spill.stop() } catch { case scala.util.control.NonFatal(_) => () }
      try { resolver.stop() } catch { case scala.util.control.NonFatal(_) => () }
    }
  }

  /** Builds one fully-wired streaming endpoint over `conf`, binding its transport to loopback. */
  private def newEndpoint(conf: SparkConf): Endpoint = {
    val resolver = new IndexShuffleBlockResolver(conf)
    val spill = new MemorySpillManager(
      conf, new TestMemoryManager(conf), resolver, new StreamingShuffleSource)
    val exchange = new StreamingBlockExchange(conf, spill)
    val transport = new StreamingShuffleTransport(conf, LoopbackHost, exchange)
    exchange.bindTransport(transport)
    new Endpoint(exchange, spill, transport, resolver)
  }

  private def metaFor(size: Int): BlockMeta = {
    val key = BlockKey(shuffleId = 0, mapId = 0L, partitionId = 0, seq = 0L)
    BlockMeta(
      key, BlockManagerId("producer-exec", LoopbackHost, 7337),
      ShuffleBlockId(0, 0L, 0), mapIndex = 0, 0L, size.toLong)
  }

  test("a consumer discovers a remote producer via the coordinator and streams a block from it") {
    val conf = newConf()
    val rpcEnv = RpcEnv.create("streaming-rendezvous-e2e", LoopbackHost, 0, conf,
      new SecurityManager(conf))
    val producer = newEndpoint(conf)
    val consumer = newEndpoint(conf)
    try {
      val coordinatorRef = rpcEnv.setupEndpoint(
        StreamingShuffleEndpointCoordinator.ENDPOINT_NAME,
        new StreamingShuffleEndpointCoordinator(rpcEnv))

      // The PRODUCER executor advertises its streaming-transport endpoint to the driver directory,
      // exactly as StreamingShuffleManager's engine does once its transport binds.
      new StreamingShuffleRendezvous(coordinatorRef, conf, "producer-exec")
        .register(producer.host, producer.port)

      // The CONSUMER reduce task registers its local reader first (so any arriving block has a
      // target), exactly as StreamingShuffleReader does as its last construction step.
      val reader = new CollectingConsumer
      consumer.exchange.registerReader(
        shuffleId = 0, startMapIndex = 0, endMapIndex = 1,
        startPartition = 0, endPartition = 1, reader)

      // Wait until the producer's advertisement is visible in the directory, then run the EXACT
      // production rendezvous call that getReader wires into the reader's remoteSubscribe thunk.
      val consumerRendezvous =
        new StreamingShuffleRendezvous(coordinatorRef, conf, "consumer-exec")
      eventually {
        consumerRendezvous.remoteEndpoints().map(_.executorId) mustBe Seq("producer-exec")
      }
      val subscribed = consumerRendezvous.subscribeToRemoteProducers(
        consumer.exchange, shuffleId = 0,
        startMapIndex = 0, endMapIndex = 1, startPartition = 0, endPartition = 1)
      // Discovery found exactly the one remote producer (self excluded) and subscribed to it.
      subscribed mustBe 1

      // Publishing on the producer now streams the block over a real socket to the consumer's
      // reader -- driven entirely by the coordinator-discovered subscription (no manual wiring).
      val payload = Array.tabulate[Byte](256)(i => (i % 17).toByte)
      producer.exchange.publishBlock(metaFor(payload.length), payload)

      eventually {
        reader.blocks.size() must be >= 1
      }
      val (receivedMeta, receivedBytes) = reader.blocks.peek()
      receivedBytes.toSeq mustBe payload.toSeq
      receivedMeta.shuffleId mustBe 0
      receivedMeta.reduceId mustBe 0
    } finally {
      consumer.stop()
      producer.stop()
      rpcEnv.shutdown()
    }
  }
}
