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

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, RpcTimeout, ThreadSafeRpcEndpoint}
import org.apache.spark.util.RpcUtils

// =================================================================================================
// Wire messages (Java-serialized across the existing RPC transport). Kept package-private so the
// streaming endpoint directory stays fully isolated from the rest of Spark.
// =================================================================================================

/** Marker for the directory request messages the coordinator endpoint understands. */
private[streaming] sealed trait StreamingEndpointMessage extends Serializable

/**
 * An executor advertising the `(host, port)` of its streaming-transport server, tagged with the
 * streaming protocol version it speaks. The version rides along on registration so a discovering
 * peer can detect a producer/consumer protocol-version mismatch (AAP fallback condition (d)) and
 * degrade that shuffle to sort. Defaults to the local [[StreamingShuffleEndpointCoordinator.
 * PROTOCOL_VERSION]] so callers that do not care about the version need not supply it.
 */
private[streaming] case class StreamingEndpointInfo(
    executorId: String,
    host: String,
    port: Int,
    protocolVersion: Int = StreamingShuffleEndpointCoordinator.PROTOCOL_VERSION)

/** Producer/consumer executor advertises (or re-advertises) its streaming-transport endpoint. */
private[streaming] case class RegisterStreamingEndpoint(info: StreamingEndpointInfo)
  extends StreamingEndpointMessage

/** An executor withdraws its endpoint (best-effort, on shuffle-manager shutdown). */
private[streaming] case class UnregisterStreamingEndpoint(executorId: String)
  extends StreamingEndpointMessage

/** A reduce task asks for every currently-advertised endpoint. */
private[streaming] case object GetStreamingEndpoints extends StreamingEndpointMessage

/** Reply to [[GetStreamingEndpoints]]: a by-value snapshot of the directory. */
private[streaming] case class StreamingEndpoints(endpoints: Seq[StreamingEndpointInfo])

/**
 * Driver-side rendezvous directory that lets streaming reduce tasks discover the
 * streaming-transport endpoints of the producer executors they must subscribe to.
 *
 * == Why this exists (coexistence / AAP closure) ==
 * The streaming data plane ([[StreamingBlockExchange]] / [[StreamingShuffleTransport]]) can already
 * carry blocks across executor boundaries, but a consumer must first know the `(host, port)` of
 * each producer executor's streaming-transport server. Spark's
 * [[org.apache.spark.MapOutputTracker]] records only post-completion BLOCK locations and is
 * deliberately NOT modified by this feature, so it cannot answer "which executors host streaming
 * producers". This lightweight directory fills exactly that gap and nothing more: it is an
 * additive NEW driver [[org.apache.spark.rpc.RpcEndpoint]] mapping `executorId -> (host, port)`
 * for streaming transports. It does NOT touch the scheduler, the task lifecycle, the
 * MapOutputTracker, or any user-facing API -- it is set up and queried purely within the
 * `ShuffleManager` abstraction boundary, mirroring `SparkEnv.registerOrLookupEndpoint`.
 *
 * == Lifecycle ==
 *  - The DRIVER instance of [[StreamingShuffleManager]] registers ONE coordinator endpoint
 *    (`rpcEnv.setupEndpoint`) the first time a streaming shuffle is registered. Because
 *    `registerShuffle` runs on the driver BEFORE any map/reduce task is launched, the endpoint is
 *    guaranteed to exist before executors look it up.
 *  - Each EXECUTOR instance advertises its streaming-transport `(host, port)` once its streaming
 *    engine binds a transport, via [[StreamingShuffleRendezvous]].
 *  - Because the unmodified DAG scheduler submits the reduce stage only AFTER the map stage has
 *    fully completed, every producer executor has already advertised its endpoint by the time a
 *    reduce task subscribes -- so a single discovery query at read time is sufficient (no polling).
 *
 * All registry mutations and reads run on the endpoint's single-threaded dispatch
 * ([[ThreadSafeRpcEndpoint]]), so the backing map needs no additional synchronization.
 */
private[streaming] class StreamingShuffleEndpointCoordinator(override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging {

  // executorId -> advertised streaming-transport endpoint. Mutated/read only on the single
  // ThreadSafeRpcEndpoint dispatch thread, so a plain mutable map is safe (no extra locking).
  private val endpoints = new mutable.HashMap[String, StreamingEndpointInfo]()

  override def receive: PartialFunction[Any, Unit] = {
    case RegisterStreamingEndpoint(info) =>
      // Last registration wins (an executor re-advertises after a transport rebind). Idempotent.
      endpoints(info.executorId) = info
      logDebug(s"Registered streaming endpoint ${info.executorId} -> ${info.host}:${info.port} " +
        s"(${endpoints.size} known)")
    case UnregisterStreamingEndpoint(executorId) =>
      endpoints.remove(executorId)
      logDebug(s"Unregistered streaming endpoint $executorId (${endpoints.size} remain)")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetStreamingEndpoints =>
      // Snapshot the directory for a discovering reduce task. Returned by value (an immutable Seq),
      // so the caller never observes later mutations of the live map.
      context.reply(StreamingEndpoints(endpoints.values.toIndexedSeq))
  }

  override def onStop(): Unit = {
    endpoints.clear()
  }
}

private[streaming] object StreamingShuffleEndpointCoordinator {
  // RpcEnv endpoint name; the driver registers under it and executors look it up by it.
  val ENDPOINT_NAME: String = "StreamingShuffleEndpointCoordinator"

  // Streaming wire-protocol version advertised by every executor on registration. A heterogeneous
  // value across the cluster (e.g. a partial/rolling upgrade) is a producer/consumer version
  // mismatch: discovering peers detect it and degrade those shuffles to sort-based shuffle. v1
  // requires an executor restart for config changes, so within a run this is effectively constant.
  val PROTOCOL_VERSION: Int = 1

  // Short, bounded timeout for the periodic version-mismatch probe so a slow/absent coordinator can
  // never stall the backpressure heartbeat thread that issues it (it falls back to "no mismatch").
  val VERSION_PROBE_TIMEOUT_SECONDS: Long = 5L
}

/**
 * Executor-side client for the [[StreamingShuffleEndpointCoordinator]]. Wraps the coordinator
 * [[RpcEndpointRef]] with best-effort, bounded-timeout helpers so a slow or absent coordinator can
 * NEVER stall or fail a reduce task -- it simply degrades to local (in-process) streaming, which in
 * turn degrades to sort-based shuffle if remote blocks never arrive. This preserves the feature's
 * zero-regression guarantee end to end.
 *
 * @param coordinatorRef ref to the driver coordinator endpoint
 * @param conf the SparkConf (supplies the standard RPC ask timeout)
 * @param selfExecutorId this executor's id, excluded from discovery results (co-located producers
 *                       are served in-process, so subscribing to self would duplicate delivery)
 */
private[streaming] class StreamingShuffleRendezvous(
    coordinatorRef: RpcEndpointRef,
    conf: SparkConf,
    selfExecutorId: String) extends Logging {

  // Bounded by the standard spark.rpc.askTimeout / spark.network.timeout, so discovery never blocks
  // a reduce task indefinitely; any timeout is swallowed and treated as "no remote producers yet".
  private val askTimeout = RpcUtils.askRpcTimeout(conf)

  // Short, bounded timeout for the periodic version-mismatch probe (see detectVersionMismatch).
  // Constructed directly -- the prop string is only used in a timeout message and is NEVER read
  // from conf -- so it adds NO new configuration entry while keeping the probe from ever stalling
  // the backpressure heartbeat thread that issues it.
  private val versionProbeTimeout = {
    val secs = StreamingShuffleEndpointCoordinator.VERSION_PROBE_TIMEOUT_SECONDS
    new RpcTimeout(FiniteDuration(secs, TimeUnit.SECONDS),
      "spark.shuffle.streaming.versionProbe.timeout")
  }

  /** Advertise this executor's streaming-transport endpoint (fire-and-forget, best-effort). */
  def register(host: String, port: Int): Unit = bestEffort("register streaming endpoint") {
    coordinatorRef.send(
      RegisterStreamingEndpoint(StreamingEndpointInfo(selfExecutorId, host, port)))
  }

  /** Withdraw this executor's endpoint (fire-and-forget, best-effort). */
  def unregister(): Unit = bestEffort("unregister streaming endpoint") {
    coordinatorRef.send(UnregisterStreamingEndpoint(selfExecutorId))
  }

  /**
   * Discover the currently-advertised REMOTE producer endpoints (this executor excluded). Returns
   * an empty Seq on ANY failure (timeout, coordinator absent, serialization), so discovery is
   * strictly best-effort and never propagates an exception into the reduce task.
   */
  def remoteEndpoints(): Seq[StreamingEndpointInfo] = {
    try {
      coordinatorRef.askSync[StreamingEndpoints](GetStreamingEndpoints, askTimeout)
        .endpoints.filter(_.executorId != selfExecutorId)
    } catch {
      case NonFatal(e) =>
        logDebug("Streaming shuffle endpoint discovery failed; using local-only streaming", e)
        Seq.empty
    }
  }

  /**
   * Best-effort, bounded version handshake: query the directory and report whether ANY advertised
   * endpoint speaks a streaming protocol version different from this executor's
   * [[StreamingShuffleEndpointCoordinator.PROTOCOL_VERSION]]. A mismatch means a producer and a
   * consumer could not interoperate over the streaming transport, so the caller (the backpressure
   * heartbeat) degrades affected shuffles to sort-based shuffle (AAP fallback condition (d)).
   * Returns false on ANY failure (timeout, absent coordinator), so the probe never propagates an
   * exception and a healthy homogeneous cluster simply observes "no mismatch".
   *
   * @return true if a differing protocol version is advertised by any endpoint, false otherwise
   */
  def detectVersionMismatch(): Boolean = {
    try {
      coordinatorRef.askSync[StreamingEndpoints](GetStreamingEndpoints, versionProbeTimeout)
        .endpoints.exists(_.protocolVersion != StreamingShuffleEndpointCoordinator.PROTOCOL_VERSION)
    } catch {
      case NonFatal(e) =>
        logDebug("Streaming shuffle version probe failed (best-effort); assuming no mismatch", e)
        false
    }
  }

  /**
   * PRODUCTION rendezvous entry point: discover every remote producer executor and subscribe THIS
   * consumer's exchange to each for the given map/partition range, so producers begin streaming
   * matching in-progress blocks over the EXISTING transport. This is the production caller the
   * cross-executor data path previously lacked.
   *
   * It is best-effort and self-contained: failures yield zero subscriptions (local-only streaming,
   * which degrades to sort if remote blocks never arrive). Co-located producers are excluded
   * (self) because they are already served in-process via the reader's local registration.
   *
   * @return the count of remote producer endpoints subscribed (0 == local-only for this read)
   */
  def subscribeToRemoteProducers(
      exchange: StreamingBlockExchange,
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Int = {
    val remote = remoteEndpoints()
    remote.foreach { ep =>
      exchange.subscribeRemote(
        ep.host, ep.port, shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
    }
    if (remote.nonEmpty) {
      logDebug(s"Subscribed shuffle $shuffleId reduce [$startPartition, $endPartition) to " +
        s"${remote.size} remote producer endpoint(s)")
    }
    remote.size
  }

  // Run a fire-and-forget control action, swallowing any failure (best-effort advertisement). A
  // failed register/unregister only costs a missed streaming opportunity, never correctness.
  private def bestEffort(action: String)(op: => Unit): Unit = {
    try {
      op
    } catch {
      case NonFatal(e) =>
        logDebug(s"Streaming shuffle failed to $action (best-effort); continuing", e)
    }
  }
}
