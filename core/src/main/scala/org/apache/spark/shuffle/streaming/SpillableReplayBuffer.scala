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

import java.io.{BufferedInputStream, BufferedOutputStream, ByteArrayInputStream,
  ByteArrayOutputStream, File, FileInputStream, FileOutputStream, OutputStream}

import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializationStream, Serializer}
import org.apache.spark.storage.{BlockId, BlockManager}

/**
 * A bounded, spillable, write-once-then-read-once log of `(key, value)` records used SOLELY to make
 * the streaming shuffle's runtime fallback to sort-based shuffle seamless.
 *
 * Why this exists: [[StreamingShuffleWriter.write]] consumes the map task's input iterator as it
 * pipelines blocks to consumers. If a runtime fallback condition fires partway through the write
 * (an unsplittable >2MB record, a sustained-slow consumer, or an admission deadline under memory
 * pressure), the records already pulled from the source iterator are gone -- yet the
 * [[FallbackShuffleWriter]] must replay the COMPLETE record sequence into a `SortShuffleWriter` so
 * the map produces correct sort-based output instead of failing the task. This buffer captures
 * every record the streaming writer pulls (tee'd in [[FallbackShuffleWriter.write]] BEFORE the
 * streaming writer sees it), so on fallback the writer replays `buffer.iterator ++
 * remainingSource`.
 *
 * Memory safety: records accumulate in an in-memory [[ByteArrayOutputStream]] until the serialized
 * size crosses `memoryThresholdBytes`, after which the buffer transparently spills to a single
 * block-manager temp file and continues appending there. Because a single, continuous
 * [[SerializationStream]] writes through one mem-then-file [[OutputStream]] (the switch happens
 * UNDER the serialization framing), the spilled file holds one continuous serialized stream that
 * [[iterator]] reads back with a single [[org.apache.spark.serializer.DeserializationStream]]. This
 * bounds heap use for large maps (no unbounded buffering, honoring the memory-safety gate) while
 * keeping small/CPU-bound maps entirely in memory (no disk, no regression for those workloads).
 *
 * Lifecycle: append records, then call [[iterator]] exactly once to replay them; [[close]] releases
 * the in-memory bytes and deletes any spill file and is idempotent. On the streaming happy path the
 * buffer is written and then discarded unread, so its records are pure fallback insurance -- a
 * documented v1 overhead of the zero-regression fallback guarantee. The buffer is uncompressed: it
 * is transient and self-consistent (written and read by the same serializer instance), so it does
 * not reuse the [[org.apache.spark.serializer.SerializerManager]] compression wrap that the
 * streaming writer/reader use for their on-wire blocks.
 *
 * Threading: driven entirely by the single map task thread; no synchronization is required or
 * provided. Confined to the streaming package (`private[streaming]`) to honor the
 * zero-cross-contamination rule.
 *
 * @param serializer the shuffle dependency's serializer; the SAME instance type serializes on
 *                   append and deserializes on replay, so records round-trip exactly
 * @param blockManager supplies `diskBlockManager.createTempLocalBlock()` for the spill file, the
 *                     same temp-block facility `ExternalSorter` uses; reuse only -- no new storage
 * @param memoryThresholdBytes serialized in-memory size at which the buffer spills to disk
 */
private[streaming] class SpillableReplayBuffer(
    serializer: Serializer,
    blockManager: BlockManager,
    memoryThresholdBytes: Long)
  extends Logging {

  // One serializer instance for the whole buffer; its stream framing must be continuous across the
  // mem->file switch, so a SINGLE serializeStream wraps the mem-then-file output below.
  private val serInstance = serializer.newInstance()

  // The mem-then-file sink. The serialization stream writes here; it switches the backing store
  // from memory to a temp file once the threshold is crossed, transparently to the serializer.
  private val sink = new SpillableSink()

  // The single, continuous serialization stream all records are written through.
  private val serStream: SerializationStream = serInstance.serializeStream(sink)

  // Number of records appended; surfaced for debug logging and tests.
  private var records: Long = 0L

  // Lifecycle guards so iterator()/close() are safe to call once / repeatedly respectively.
  private var writingFinished = false
  private var closed = false

  /** Number of records appended so far. */
  def recordCount: Long = records

  /** True once the buffer has spilled its in-memory bytes to a temp file. */
  def hasSpilled: Boolean = sink.spilled

  /**
   * Appends one record. The key and value are taken as `Any` so the ClassTag-bounded
   * `writeKey`/`writeValue` resolve `ClassTag[Any]` exactly as `DiskBlockObjectWriter.write` does
   * (the map task's K/V carry no ClassTag here), mirroring [[StreamingShuffleWriter]].
   */
  def append(key: Any, value: Any): Unit = {
    require(!writingFinished, "SpillableReplayBuffer.append called after iterator()")
    require(!closed, "SpillableReplayBuffer.append called after close()")
    serStream.writeKey(key)(ClassTag.Any.asInstanceOf[ClassTag[Any]])
    serStream.writeValue(value)(ClassTag.Any.asInstanceOf[ClassTag[Any]])
    records += 1L
  }

  /**
   * Finalizes writing and returns a one-shot iterator that replays every appended record in append
   * order. Must be called at most once. The returned iterator closes its underlying stream on
   * exhaustion (via the serializer's `asKeyValueIterator`); [[close]] still deletes the spill file.
   */
  def iterator: Iterator[(Any, Any)] = {
    require(!writingFinished, "SpillableReplayBuffer.iterator called more than once")
    require(!closed, "SpillableReplayBuffer.iterator called after close()")
    writingFinished = true
    // Close the write stream so all buffered/framed bytes are flushed to the active sink.
    serStream.close()
    val in = sink.openForRead()
    serInstance.deserializeStream(in).asKeyValueIterator
  }

  /**
   * Releases the in-memory bytes and deletes any spill file. Idempotent: safe to call from both
   * `FallbackShuffleWriter.stop` and a task-completion listener, and after a partial/failed write.
   */
  def close(): Unit = {
    if (!closed) {
      closed = true
      if (!writingFinished) {
        // Writing never finalized (e.g. the happy path discards the buffer, or a failure aborted
        // mid-append): close the write stream to release any underlying file handle. Swallow errors
        // so cleanup always proceeds to the file deletion below.
        try {
          serStream.close()
        } catch {
          case scala.util.control.NonFatal(_) => // best-effort; deletion below reclaims the file
        }
      }
      sink.cleanup()
    }
  }

  /**
   * A [[OutputStream]] that buffers writes in memory and, once the in-memory size would exceed
   * `memoryThresholdBytes`, spills the accumulated bytes to a block-manager temp file and continues
   * writing there. The mem->file switch is invisible to the serialization stream layered on top, so
   * the produced byte sequence is one continuous serialized stream regardless of where it lives.
   */
  private final class SpillableSink extends OutputStream {
    private var mem: ByteArrayOutputStream = new ByteArrayOutputStream()
    private var fileOut: OutputStream = _
    private[this] var file: File = _
    private[this] var blockId: BlockId = _
    private var spilledBytes: Long = 0L
    private var readOpened = false

    /** True once this sink has switched from the in-memory buffer to a spill file. */
    def spilled: Boolean = file != null

    override def write(b: Int): Unit = {
      maybeSpill(1)
      if (spilled) fileOut.write(b) else mem.write(b)
    }

    override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
      maybeSpill(len)
      if (spilled) fileOut.write(bytes, off, len) else mem.write(bytes, off, len)
    }

    override def flush(): Unit = {
      if (spilled) fileOut.flush()
    }

    override def close(): Unit = {
      // Close only the file sink; a ByteArrayOutputStream.close() is a no-op and we must keep its
      // bytes readable for the in-memory (unspilled) replay path.
      if (fileOut != null) {
        fileOut.close()
      }
    }

    // Switch to the spill file if appending `incoming` bytes would push the in-memory buffer past
    // the threshold. The already-accumulated in-memory bytes are written to the file first so the
    // file holds the COMPLETE prefix, preserving continuous serialization framing.
    private def maybeSpill(incoming: Int): Unit = {
      if (!spilled && mem.size().toLong + incoming > memoryThresholdBytes) {
        val (id, f) = blockManager.diskBlockManager.createTempLocalBlock()
        blockId = id
        file = f
        fileOut = new BufferedOutputStream(new FileOutputStream(f))
        val prefix = mem.toByteArray
        fileOut.write(prefix)
        spilledBytes = prefix.length.toLong
        mem = null // release the heap buffer; subsequent writes go to the file
        logDebug(s"SpillableReplayBuffer spilled to $blockId after $spilledBytes in-memory bytes")
      }
    }

    /** Finalizes writing and returns an input stream over the complete byte sequence. */
    def openForRead(): java.io.InputStream = {
      readOpened = true
      if (spilled) {
        new BufferedInputStream(new FileInputStream(file))
      } else {
        new ByteArrayInputStream(mem.toByteArray)
      }
    }

    /** Releases the in-memory buffer and deletes the spill file (if any). Idempotent. */
    def cleanup(): Unit = {
      mem = null
      if (fileOut != null) {
        try {
          fileOut.close()
        } catch {
          case scala.util.control.NonFatal(_) => // already closed or never opened for writing
        }
        fileOut = null
      }
      if (file != null) {
        if (file.exists() && !file.delete()) {
          logWarning(s"Failed to delete streaming-shuffle replay spill file " +
            s"${file.getAbsolutePath}")
        }
        file = null
      }
    }
  }
}
