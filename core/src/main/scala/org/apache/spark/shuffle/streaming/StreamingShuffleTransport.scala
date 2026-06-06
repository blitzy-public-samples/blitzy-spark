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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.util.Collections

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient, TransportClientFactory}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager, TransportServer}
import org.apache.spark.shuffle.streaming.StreamingBlockExchange.BlockMeta
import org.apache.spark.storage.{BlockId, BlockManagerId}

/**
 * The cross-executor network plane for the opt-in streaming shuffle engine. This is the piece that
 * realizes the AAP's "reuse the EXISTING network transport (`TransportContext`)" mandate: it stands
 * up a real Netty [[TransportServer]] and [[TransportClientFactory]] on each executor that runs
 * streaming shuffle, so producer-emitted blocks and the consumer's control messages travel over the
 * same network stack Spark already uses for block transfer -- NOT an in-process hand-off.
 *
 * Reuse, not modification (least-modification rule): every collaborator here is an EXISTING public
 * transport type used through its public surface. The [[TransportConf]] is built with the existing
 * [[SparkTransportConf.fromSparkConf]] helper (module name `"shuffle-streaming"`), the
 * server/client factory come from [[TransportContext]], and the [[StreamManager]] is the
 * existing [[OneForOneStreamManager]]. No transport internals are touched, so this adds a
 * streaming network plane without changing the shared transport for any other subsystem.
 *
 * How it plugs into the exchange (coexistence strategy): the [[StreamingBlockExchange]] is the only
 * streaming component that knows about routing. This transport is deliberately dumb -- it
 * serializes a message, ships it, and on the receiving side decodes it and hands it to a
 * [[StreamingShuffleTransport.StreamingTransportListener]] (implemented by the exchange). The
 * exchange is bound AFTER construction via [[StreamingBlockExchange.bindTransport]], so an exchange
 * with no transport bound keeps its original in-process behavior verbatim (every existing unit test
 * exercises that path unchanged). When a transport IS bound, the exchange registers a remote
 * subscriber as an ordinary [[StreamingBlockExchange.StreamingBlockConsumer]] proxy and reuses its
 * existing routing -- so the writer/reader contracts are identical whether a consumer is
 * local or on a different executor.
 *
 * Self-addressing wire protocol: messages carry the endpoints needed to reply, so no external
 * registry or scheduler/tracker change is required to route control traffic. A SUBSCRIBE
 * carries the consumer's `(host, port)` so the producer knows where to push blocks; a
 * BLOCK/COMPLETE/FAILED carries the producer's `(host, port)` so the consumer knows where
 * to send ACK/RESEND for exactly that block. This keeps the entire streaming network plane
 * inside the streaming package, honoring the zero-cross-contamination rule.
 *
 * Reliability: block and signal delivery uses one-way [[TransportClient.send]] (fire-and-forget).
 * The streaming protocol itself provides the reliability guarantees: the reader verifies FULL
 * per-map block coverage before completing (no truncated read can look like success), requests a
 * block-specific RESEND on CRC32C mismatch, and atomically invalidates on producer timeout to drive
 * the existing `FetchFailedException` recompute path. So a dropped datagram degrades to a
 * resend or a safe invalidation rather than silent data loss.
 *
 * Threading: the server's Netty event loop invokes [[StreamingShuffleRpcHandler.receive]] on its IO
 * threads; the handler decodes and calls the listener, whose delivery may block on a consumer's
 * bounded inbox (the intended backpressure). Client creation is delegated to the
 * [[TransportClientFactory]], which caches and synchronizes connections per peer. Verbose
 * logging is gated behind `spark.shuffle.streaming.debug` to honor the 10MB/hour/executor
 * log-volume budget.
 *
 * @param conf the active [[SparkConf]]; supplies transport thread/timeout settings and the
 *             debug gate
 * @param bindHost the host/interface the streaming transport server binds to (this executor's host)
 * @param listener the receive-side callback target (the [[StreamingBlockExchange]]) that routes
 *                 decoded messages to local readers / reclamation / resend
 * @param numUsableCores cores hint forwarded to [[SparkTransportConf]] for default thread sizing
 */
private[spark] class StreamingShuffleTransport(
    conf: SparkConf,
    bindHost: String,
    listener: StreamingShuffleTransport.StreamingTransportListener,
    numUsableCores: Int = 0)
  extends Logging {

  // Verbose debug-logging gate; held off the hot path to honor the 10MB/hour log-volume budget.
  private val debug: Boolean = conf.get(config.STREAMING_SHUFFLE_DEBUG)

  // Build the transport configuration through the EXISTING helper, under a dedicated module name so
  // streaming thread/timeout settings (spark.shuffle-streaming.io.*) are independent of other
  // modules while still defaulting from the shared spark.network.* settings.
  private val transportConf = {
    // Enable TCP keepalive on streaming sockets so a half-open connection to a dead peer is
    // detected and reaped rather than lingering. The documented 5s probe cadence
    // ([[BackpressureProtocol.TCP_KEEPALIVE_SECONDS]]) is an OS-level setting -- Spark's transport
    // only toggles the SO_KEEPALIVE socket option (TransportServer sets it on accepted channels and
    // TransportClientFactory always sets it on outbound connections); the kernel governs the actual
    // probe interval. We set the module-scoped key on a CLONED SparkConf so the shared conf is
    // never mutated (avoiding cross-contamination of other transport modules).
    val keepAliveConf = conf.clone
    keepAliveConf.set("spark.shuffle-streaming.io.enableTcpKeepAlive", "true")
    SparkTransportConf.fromSparkConf(keepAliveConf, "shuffle-streaming", numUsableCores)
  }

  // The EXISTING TransportContext, bound to our streaming RpcHandler. This is the single reused
  // entry point to Spark's Netty stack; it manufactures both the server and the client factory.
  private val context: TransportContext =
    new TransportContext(transportConf, new StreamingShuffleRpcHandler(listener, debug))

  // The server other executors connect to in order to deliver streaming messages to THIS executor.
  // Bound to an ephemeral port (0) so multiple executors -- or multiple instances in a single-JVM
  // integration test on loopback -- never collide.
  private val server: TransportServer =
    context.createServer(bindHost, 0, Collections.emptyList())

  // Shared client factory for outbound connections to peer executors' streaming servers. It caches
  // and synchronizes one connection per peer, so repeated sends to the same executor reuse
  // a socket.
  private val clientFactory: TransportClientFactory = context.createClientFactory()

  @volatile private var stopped = false

  if (debug) {
    logDebug(s"StreamingShuffleTransport listening on $bindHost:${server.getPort} " +
      s"(TCP keepalive enabled; target probe cadence " +
      s"${BackpressureProtocol.TCP_KEEPALIVE_SECONDS}s is OS-controlled)")
  }

  /** The host this transport's server is bound to (this executor's streaming endpoint host). */
  def host: String = bindHost

  /**
   * The ephemeral port this transport's server bound to (this executor's streaming endpoint
   * port).
   */
  def port: Int = server.getPort

  /**
   * Test/diagnostic accessor reporting whether TCP keepalive is enabled on this transport's
   * underlying (cloned) [[org.apache.spark.network.util.TransportConf]]. The constructor wires
   * keepalive on by setting `spark.shuffle-streaming.io.enableTcpKeepAlive` on a cloned conf (see
   * `transportConf` above), which makes `TransportServer` set `SO_KEEPALIVE` on accepted channels.
   * Scoped `private[streaming]` so it never widens the public API surface beyond the streaming
   * package, honoring the zero-cross-contamination rule.
   */
  private[streaming] def tcpKeepAliveEnabled: Boolean = transportConf.enableTcpKeepAlive()

  /**
   * Sends one already-encoded streaming message to a peer executor's streaming server, fire and
   * forget. Connection set-up and caching are delegated to the [[TransportClientFactory]]. Delivery
   * failure is logged but never thrown: the streaming protocol's coverage check, RESEND, and
   * producer-timeout invalidation recover from a lost message safely, so a transport hiccup
   * must not fail the producing task here.
   *
   * @param peerHost the destination executor's streaming-transport host
   * @param peerPort the destination executor's streaming-transport port
   * @param message the encoded message body (see the codec in [[StreamingShuffleTransport]])
   */
  def send(peerHost: String, peerPort: Int, message: ByteBuffer): Unit = {
    if (!stopped) {
      try {
        val client: TransportClient = clientFactory.createClient(peerHost, peerPort)
        client.send(message)
      } catch {
        case NonFatal(e) =>
          logWarning(s"Streaming shuffle transport failed to send a message to " +
            s"$peerHost:$peerPort; the streaming protocol will recover via resend/timeout", e)
      }
    }
  }

  /**
   * Tears down the streaming network plane: stops accepting/serving connections, closes all cached
   * outbound connections, and releases the [[TransportContext]] (its Netty event loops).
   * Idempotent. Does NOT stop the [[MemorySpillManager]] or the [[StreamingBlockExchange]] --
   * their lifetimes are owned by the engine that created them.
   */
  def stop(): Unit = {
    if (!stopped) {
      stopped = true
      try {
        clientFactory.close()
      } finally {
        try {
          server.close()
        } finally {
          context.close()
        }
      }
    }
  }
}

/**
 * Wire codec, message types, the receive-side listener contract, and the [[RpcHandler]] for
 * [[StreamingShuffleTransport]]. The codec is a small, self-contained binary format (a leading type
 * byte followed by length-prefixed fields written through [[DataOutputStream]]); it is deliberately
 * independent of any external serializer so the network plane has no cross-package coupling.
 */
private[spark] object StreamingShuffleTransport {

  // Message type tags (first byte of every encoded message).
  private val MSG_SUBSCRIBE: Byte = 1
  private val MSG_BLOCK: Byte = 2
  private val MSG_COMPLETE: Byte = 3
  private val MSG_FAILED: Byte = 4
  private val MSG_ACK: Byte = 5
  private val MSG_RESEND: Byte = 6

  // Defensive upper bounds for network-supplied array lengths (CWE-20 unvalidated input /
  // CWE-400 uncontrolled resource consumption). A malformed or malicious peer could otherwise send
  // a negative or enormous length on a wire field and trigger a NegativeArraySizeException or a
  // huge heap allocation directly on a Netty IO thread (OOM). Both fields below are validated
  // against these caps BEFORE any array is allocated in `decodeAndDispatch`.
  //
  // A legitimate MSG_BLOCK payload is capped at the 2MB pipelined-block size by the producer
  // (see [[BackpressureProtocol.MAX_PIPELINED_BLOCK_BYTES]]).
  private val MAX_BLOCK_PAYLOAD_BYTES: Int = BackpressureProtocol.MAX_PIPELINED_BLOCK_BYTES.toInt
  // A legitimate MSG_COMPLETE carries one Long block-count per reduce partition in the consumer's
  // range; this ceiling sits far above any realistic partition count (Spark shuffles are tuned to
  // thousands of partitions) while bounding the array to at most 8MB.
  private val MAX_PARTITION_COUNT: Int = 1 << 20

  /**
   * Receive-side callback contract the [[StreamingBlockExchange]] implements. The
   * [[StreamingShuffleRpcHandler]] decodes each inbound message and invokes exactly one of these.
   * The split mirrors the producer/consumer roles: an executor acting as a producer receives
   * SUBSCRIBE/ACK/RESEND; one acting as a consumer receives BLOCK/COMPLETE/FAILED. A single
   * executor may play both roles for different shuffles, so one exchange implements them all.
   */
  trait StreamingTransportListener {

    /**
     * A remote consumer wants blocks for the given ranges; reply to `consumerHost:consumerPort`.
     */
    def onRemoteSubscribe(
        shuffleId: Int,
        startMapIndex: Int,
        endMapIndex: Int,
        startPartition: Int,
        endPartition: Int,
        consumerHost: String,
        consumerPort: Int): Unit

    /** A remote producer delivered a block; ACK/RESEND for it go to `producerHost:producerPort`. */
    def onRemoteBlock(meta: BlockMeta, bytes: Array[Byte], producerHost: String,
        producerPort: Int): Unit

    /** A remote producer reported a completed map and its per-reduce block counts. */
    def onRemoteComplete(shuffleId: Int, mapId: Long, mapIndex: Int, blockCounts: Array[Long]): Unit

    /**
     * A remote producer reported a map failure; the consumer invalidates and recomputes upstream.
     */
    def onRemoteFailure(shuffleId: Int, mapId: Long, mapIndex: Int, bmAddress: BlockManagerId,
        message: String): Unit

    /** A remote consumer acknowledged a block; reclaim its buffer locally. */
    def onRemoteAck(meta: BlockMeta): Unit

    /** A remote consumer requested a resend of a specific block; re-deliver it. */
    def onRemoteResend(meta: BlockMeta): Unit
  }

  /** Encodes a SUBSCRIBE (consumer -> producer). */
  def encodeSubscribe(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      consumerHost: String,
      consumerPort: Int): ByteBuffer = {
    withOutput { out =>
      out.writeByte(MSG_SUBSCRIBE)
      out.writeInt(shuffleId)
      out.writeInt(startMapIndex)
      out.writeInt(endMapIndex)
      out.writeInt(startPartition)
      out.writeInt(endPartition)
      out.writeUTF(consumerHost)
      out.writeInt(consumerPort)
    }
  }

  /**
   * Encodes a BLOCK (producer -> consumer), carrying the producer endpoint for ACK/RESEND replies.
   */
  def encodeBlock(
      producerHost: String,
      producerPort: Int,
      meta: BlockMeta,
      bytes: Array[Byte]): ByteBuffer = {
    withOutput { out =>
      out.writeByte(MSG_BLOCK)
      out.writeUTF(producerHost)
      out.writeInt(producerPort)
      writeMeta(out, meta)
      out.writeInt(bytes.length)
      out.write(bytes)
    }
  }

  /** Encodes a COMPLETE (producer -> consumer). */
  def encodeComplete(
      shuffleId: Int,
      mapId: Long,
      mapIndex: Int,
      blockCounts: Array[Long]): ByteBuffer = {
    withOutput { out =>
      out.writeByte(MSG_COMPLETE)
      out.writeInt(shuffleId)
      out.writeLong(mapId)
      out.writeInt(mapIndex)
      out.writeInt(blockCounts.length)
      var i = 0
      while (i < blockCounts.length) {
        out.writeLong(blockCounts(i))
        i += 1
      }
    }
  }

  /** Encodes a FAILED (producer -> consumer). */
  def encodeFailure(
      shuffleId: Int,
      mapId: Long,
      mapIndex: Int,
      bmAddress: BlockManagerId,
      message: String): ByteBuffer = {
    withOutput { out =>
      out.writeByte(MSG_FAILED)
      out.writeInt(shuffleId)
      out.writeLong(mapId)
      out.writeInt(mapIndex)
      writeBlockManagerId(out, bmAddress)
      out.writeUTF(if (message == null) "" else message)
    }
  }

  /**
   * Encodes an ACK (consumer -> producer). The full meta is sent so the producer reclaims by key.
   */
  def encodeAck(meta: BlockMeta): ByteBuffer = withOutput { out =>
    out.writeByte(MSG_ACK)
    writeMeta(out, meta)
  }

  /**
   * Encodes a RESEND request (consumer -> producer). The full meta lets the producer re-deliver.
   */
  def encodeResend(meta: BlockMeta): ByteBuffer = withOutput { out =>
    out.writeByte(MSG_RESEND)
    writeMeta(out, meta)
  }

  /**
   * Decodes one inbound message and dispatches it to the listener. Called on a transport IO thread.
   * Unknown type tags are ignored defensively (a forward/backward-compatible producer could one day
   * add message types); a decode failure is surfaced to the caller, which logs and drops it.
   */
  def decodeAndDispatch(buffer: ByteBuffer, listener: StreamingTransportListener): Unit = {
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    val msgType = in.readByte()
    msgType match {
      case MSG_SUBSCRIBE =>
        val shuffleId = in.readInt()
        val startMapIndex = in.readInt()
        val endMapIndex = in.readInt()
        val startPartition = in.readInt()
        val endPartition = in.readInt()
        val consumerHost = in.readUTF()
        val consumerPort = in.readInt()
        listener.onRemoteSubscribe(shuffleId, startMapIndex, endMapIndex, startPartition,
          endPartition, consumerHost, consumerPort)
      case MSG_BLOCK =>
        val producerHost = in.readUTF()
        val producerPort = in.readInt()
        val meta = readMeta(in)
        val len = in.readInt()
        // Security: validate the network-supplied payload length BEFORE allocating so a malformed
        // peer cannot drive a NegativeArraySizeException or an OOM allocation on this IO thread.
        // The throw is caught by StreamingShuffleRpcHandler.dispatch (NonFatal -> log + drop the
        // single message), leaving the channel intact.
        require(len >= 0 && len <= MAX_BLOCK_PAYLOAD_BYTES,
          s"Streaming shuffle MSG_BLOCK payload length $len is out of bounds " +
            s"[0, $MAX_BLOCK_PAYLOAD_BYTES]")
        val payload = new Array[Byte](len)
        in.readFully(payload)
        listener.onRemoteBlock(meta, payload, producerHost, producerPort)
      case MSG_COMPLETE =>
        val shuffleId = in.readInt()
        val mapId = in.readLong()
        val mapIndex = in.readInt()
        val n = in.readInt()
        // Security: bound the network-supplied partition-count array length before allocating, for
        // the same CWE-20/CWE-400 reasons as MSG_BLOCK above.
        require(n >= 0 && n <= MAX_PARTITION_COUNT,
          s"Streaming shuffle MSG_COMPLETE partition count $n is out of bounds " +
            s"[0, $MAX_PARTITION_COUNT]")
        val counts = new Array[Long](n)
        var i = 0
        while (i < n) {
          counts(i) = in.readLong()
          i += 1
        }
        listener.onRemoteComplete(shuffleId, mapId, mapIndex, counts)
      case MSG_FAILED =>
        val shuffleId = in.readInt()
        val mapId = in.readLong()
        val mapIndex = in.readInt()
        val bmAddress = readBlockManagerId(in)
        val message = in.readUTF()
        listener.onRemoteFailure(shuffleId, mapId, mapIndex, bmAddress, message)
      case MSG_ACK =>
        listener.onRemoteAck(readMeta(in))
      case MSG_RESEND =>
        listener.onRemoteResend(readMeta(in))
      case other =>
        // Unknown message type: ignore defensively rather than failing the channel.
    }
  }

  // Runs `f` against a fresh DataOutputStream and returns the written bytes wrapped as a
  // ByteBuffer.
  private def withOutput(f: DataOutputStream => Unit): ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)
    f(out)
    out.flush()
    ByteBuffer.wrap(bos.toByteArray)
  }

  // Writes a BlockMeta: its addressable key, producer BlockManagerId, block id name, and integrity
  // fields. The fields are exactly those needed to reconstruct the meta on the far side and to
  // reclaim/resend the precise block.
  private def writeMeta(out: DataOutputStream, meta: BlockMeta): Unit = {
    out.writeInt(meta.key.shuffleId)
    out.writeLong(meta.key.mapId)
    out.writeInt(meta.key.partitionId)
    out.writeLong(meta.key.seq)
    writeBlockManagerId(out, meta.bmAddress)
    out.writeUTF(meta.blockId.name)
    out.writeInt(meta.mapIndex)
    out.writeLong(meta.checksum)
    out.writeLong(meta.sizeBytes)
  }

  // Reconstructs a BlockMeta written by writeMeta.
  private def readMeta(in: DataInputStream): BlockMeta = {
    val shuffleId = in.readInt()
    val mapId = in.readLong()
    val partitionId = in.readInt()
    val seq = in.readLong()
    val bmAddress = readBlockManagerId(in)
    val blockId = BlockId(in.readUTF())
    val mapIndex = in.readInt()
    val checksum = in.readLong()
    val sizeBytes = in.readLong()
    BlockMeta(
      MemorySpillManager.BlockKey(shuffleId, mapId, partitionId, seq),
      bmAddress, blockId, mapIndex, checksum, sizeBytes)
  }

  // Writes a BlockManagerId as a presence flag followed by (execId, host, port). The topology info
  // is intentionally omitted (re-read as None): it is advisory and never used by the
  // streaming path.
  private def writeBlockManagerId(out: DataOutputStream, bmId: BlockManagerId): Unit = {
    if (bmId == null) {
      out.writeBoolean(false)
    } else {
      out.writeBoolean(true)
      out.writeUTF(bmId.executorId)
      out.writeUTF(bmId.host)
      out.writeInt(bmId.port)
    }
  }

  // Reconstructs a BlockManagerId written by writeBlockManagerId, or null if it was absent.
  private def readBlockManagerId(in: DataInputStream): BlockManagerId = {
    if (!in.readBoolean()) {
      null
    } else {
      val execId = in.readUTF()
      val host = in.readUTF()
      val port = in.readInt()
      BlockManagerId(execId, host, port)
    }
  }
}

/**
 * The streaming shuffle [[RpcHandler]] bound to the reused [[TransportContext]]. It decodes each
 * inbound message with the [[StreamingShuffleTransport]] codec and dispatches it to the
 * [[StreamingShuffleTransport.StreamingTransportListener]] (the [[StreamingBlockExchange]]).
 *
 * Streaming messages are sent one-way via [[TransportClient.send]], so they arrive through the
 * no-reply [[receive]] overload; the abstract reply-carrying overload is also implemented (and
 * simply acknowledges) so a defensive `sendRpc` would not break. The required [[StreamManager]] is
 * the existing [[OneForOneStreamManager]]; the streaming plane does not use chunk streams, so it is
 * never asked for one. This handler is confined to the streaming package, honoring the
 * zero-cross-contamination rule.
 */
private[spark] class StreamingShuffleRpcHandler(
    listener: StreamingShuffleTransport.StreamingTransportListener,
    debug: Boolean)
  extends RpcHandler with Logging {

  private val streamManager = new OneForOneStreamManager()

  // One-way path: this is what TransportClient.send delivers to. Decode and dispatch; never reply.
  override def receive(client: TransportClient, message: ByteBuffer): Unit = {
    dispatch(message)
  }

  // Reply-carrying path: not used by the one-way streaming sends, but implemented for safety so a
  // defensive sendRpc still decodes and is acknowledged exactly once.
  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    dispatch(message)
    callback.onSuccess(ByteBuffer.allocate(0))
  }

  override def getStreamManager: StreamManager = streamManager

  private def dispatch(message: ByteBuffer): Unit = {
    try {
      StreamingShuffleTransport.decodeAndDispatch(message, listener)
    } catch {
      case NonFatal(e) =>
        // Drop a single undecodable/un-routable message rather than failing the whole channel; the
        // streaming protocol recovers missing data via coverage check, resend, and timeout.
        logWarning("Streaming shuffle transport dropped an undecodable inbound message", e)
    }
  }
}
