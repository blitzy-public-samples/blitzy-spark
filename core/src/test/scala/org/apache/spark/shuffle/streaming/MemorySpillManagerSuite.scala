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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Span}

import org.apache.spark._
import org.apache.spark.memory.TestMemoryManager
import org.apache.spark.shuffle.IndexShuffleBlockResolver

/**
 * Unit tests for [[MemorySpillManager]], the per-block memory-buffering and spill coordinator for
 * the streaming-shuffle engine. The suite needs no running `SparkContext`: the spill manager
 * acquires storage through an injected [[TestMemoryManager]] and, when no `SparkEnv` is available,
 * spills to JVM temp files, so the full register/read/spill/reclaim lifecycle is exercised
 * deterministically in isolation.
 *
 * Coverage maps to the CP1 review findings:
 *  - register accounts bytes and reserves storage memory; reclaim releases BOTH to zero within the
 *    100ms target (the ack-to-reclamation path that bounds writer-side buffer lifetime).
 *  - a spilled block stays READABLE from disk and the LARGEST resident block is spilled first (the
 *    "spill largest buffered partitions, LRU tie-break" policy), so spilled data is never lost.
 *  - a declined storage grant still stores and retains the block (a false return is pure
 *    backpressure, never "not stored"), guaranteeing zero data loss under memory pressure.
 *  - register racing with stop never leaks reserved storage memory, and stop is idempotent.
 *
 * Same-package access: the suite lives in the streaming package, so it constructs the
 * `private[spark]` manager directly and reads its `private[streaming]` diagnostics.
 */
class MemorySpillManagerSuite extends SparkFunSuite with Matchers with Eventually {

  import MemorySpillManager.BlockKey

  // A generous patience so assertions that may observe an out-of-band (poller / immediate) spill
  // pass converge reliably without being flaky on a slow CI machine.
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5000, Millis), interval = Span(20, Millis))

  /**
   * A [[TestMemoryManager]] with a small on-heap storage budget so the streaming buffer slice
   * (budget * bufferSizePercent / 100) is small enough to cross the spill threshold with tiny
   * blocks deterministically, and whose storage grants can optionally be forced to fail to
   * exercise the memory-pressure (declined grant) path.
   */
  private class BoundedMemoryManager(conf: SparkConf, budget: Long, grant: Boolean)
    extends TestMemoryManager(conf) {
    override def maxOnHeapStorageMemory: Long = budget
    override def acquireStorageMemory(
        blockId: org.apache.spark.storage.BlockId,
        numBytes: Long,
        memoryMode: org.apache.spark.memory.MemoryMode): Boolean = grant
  }

  /**
   * Constructs a [[MemorySpillManager]] and guarantees its 100ms poller daemon is shut down via
   * stop(), so no test leaks the scheduled executor created in the constructor.
   */
  private def withManager(
      conf: SparkConf,
      memoryManager: org.apache.spark.memory.MemoryManager)(
      body: MemorySpillManager => Unit): Unit = {
    val resolver = new IndexShuffleBlockResolver(conf)
    val source = new StreamingShuffleSource
    val manager = new MemorySpillManager(conf, memoryManager, resolver, source)
    try {
      body(manager)
    } finally {
      manager.stop()
      resolver.stop()
    }
  }

  private def bytesOf(size: Int, fill: Byte): Array[Byte] = Array.fill(size)(fill)

  test("register accounts bytes and reserves storage memory; read returns them") {
    val conf = new SparkConf(false)
    withManager(conf, new TestMemoryManager(conf)) { manager =>
      val key = BlockKey(0, 0L, 0, 0L)
      val bytes = bytesOf(256, 7.toByte)
      // A granted register tracks the bytes and reserves exactly that many storage bytes.
      manager.register(key, bytes) mustBe true
      manager.trackedBytesTotal mustBe 256L
      manager.reservedBytesTotal mustBe 256L
      // The block is readable from memory, returning the same content.
      manager.read(key).map(_.toSeq) mustBe Some(bytes.toSeq)
      // An unknown key reads as None.
      manager.read(BlockKey(0, 0L, 0, 99L)) mustBe None
      // Empty/null bytes are a no-op that accounts nothing.
      manager.register(BlockKey(0, 0L, 1, 0L), Array.emptyByteArray) mustBe true
      manager.trackedBytesTotal mustBe 256L
    }
  }

  test("reclaim releases buffer memory and reserved storage back to zero (ack-to-reclamation)") {
    val conf = new SparkConf(false)
    withManager(conf, new TestMemoryManager(conf)) { manager =>
      val key = BlockKey(1, 2L, 3, 0L)
      val bytes = bytesOf(1024, 9.toByte)
      manager.register(key, bytes) mustBe true
      manager.trackedBytesTotal mustBe 1024L
      manager.reservedBytesTotal mustBe 1024L

      // The consumer acknowledges the block: reclaim must drop tracked AND reserved bytes to zero
      // (synchronously, well within the mandated 100ms) and forget the key.
      val startNanos = System.nanoTime()
      manager.reclaim(key)
      val elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)
      manager.trackedBytesTotal mustBe 0L
      manager.reservedBytesTotal mustBe 0L
      manager.read(key) mustBe None
      elapsedMillis must be < 100L
    }
  }

  test("spills the LARGEST resident block first and serves it from disk (no data loss)") {
    // Budget = 1000 * 20% = 200 bytes; spill threshold 80% => 160 bytes.
    val conf = new SparkConf(false)
    withManager(conf, new BoundedMemoryManager(conf, budget = 1000L, grant = true)) { manager =>
      // Register a SMALL, OLDER block first, then a LARGE, NEWER block. Pure access-order LRU would
      // evict the small old one; the required policy spills the LARGEST first.
      val small = BlockKey(0, 0L, 0, 0L)
      val large = BlockKey(0, 0L, 1, 0L)
      val smallBytes = bytesOf(50, 1.toByte)
      val largeBytes = bytesOf(300, 2.toByte)
      manager.register(small, smallBytes)
      // Registering the large block crosses the 80% threshold and schedules an out-of-band spill
      // pass on the manager's own (single-thread, serialized) poller. The largest block (300B) is
      // spilled until the tracked footprint falls back under the threshold, leaving the small
      // block (50B) resident -- proving the "spill largest first" policy.
      manager.register(large, largeBytes)

      eventually {
        // Only the small block remains in memory; the large one was spilled (its heap freed).
        manager.trackedBytesTotal mustBe 50L
        manager.spillCountTotal must be >= 1L
      }
      // CRITICAL: the spilled (large) block is still READABLE -- served back from its disk file --
      // and the resident (small) block reads from memory. Spilled data is never lost.
      manager.read(large).map(_.toSeq) mustBe Some(largeBytes.toSeq)
      manager.read(small).map(_.toSeq) mustBe Some(smallBytes.toSeq)
      // Reclaiming both releases everything back to zero, deleting the spill file.
      manager.reclaim(small)
      manager.reclaim(large)
      manager.trackedBytesTotal mustBe 0L
      manager.reservedBytesTotal mustBe 0L
    }
  }

  test("a declined storage grant still stores and spills the block (zero data loss)") {
    val conf = new SparkConf(false)
    // Storage grants always fail (OOM-risk): register returns a backpressure false, but the block
    // is still tracked and retained so nothing is lost.
    withManager(conf, new BoundedMemoryManager(conf, budget = 1000L, grant = false)) { manager =>
      val key = BlockKey(0, 0L, 0, 0L)
      // Size it past the 80% threshold (budget slice is 200B) so memory pressure forces a spill.
      val bytes = bytesOf(256, 5.toByte)
      // false is a pure backpressure signal, NOT "not stored".
      manager.register(key, bytes) mustBe false
      // No storage was reserved (the grant was declined), but the bytes are tracked and readable
      // -- the block is never lost just because the grant failed.
      manager.reservedBytesTotal mustBe 0L
      manager.read(key).map(_.toSeq) mustBe Some(bytes.toSeq)

      // The declined grant + over-threshold footprint schedules an out-of-band spill pass; the
      // ungranted block is spilled to relieve pressure and stays readable from its disk file
      // afterward (zero data loss under memory pressure).
      eventually {
        manager.spillCountTotal must be >= 1L
        manager.trackedBytesTotal mustBe 0L
      }
      manager.read(key).map(_.toSeq) mustBe Some(bytes.toSeq)
    }
  }

  test("register after stop is rejected and reserves nothing") {
    val conf = new SparkConf(false)
    val resolver = new IndexShuffleBlockResolver(conf)
    val manager = new MemorySpillManager(
      conf, new TestMemoryManager(conf), resolver, new StreamingShuffleSource)
    try {
      manager.stop()
      // After shutdown no new bytes may be accounted (prevents the register/stop leak).
      manager.register(BlockKey(0, 0L, 0, 0L), bytesOf(64, 3.toByte)) mustBe false
      manager.trackedBytesTotal mustBe 0L
      manager.reservedBytesTotal mustBe 0L
      // stop() is idempotent.
      noException must be thrownBy manager.stop()
    } finally {
      resolver.stop()
    }
  }

  test("concurrent register during stop never leaks reserved storage memory") {
    val conf = new SparkConf(false)
    val resolver = new IndexShuffleBlockResolver(conf)
    val manager = new MemorySpillManager(
      conf, new TestMemoryManager(conf), resolver, new StreamingShuffleSource)
    try {
      val threads = 6
      val perThread = 200
      val startLatch = new CountDownLatch(1)
      val failures = new AtomicInteger(0)
      val workers = (0 until threads).map { t =>
        val thread = new Thread(() => {
          try {
            startLatch.await()
            var i = 0
            while (i < perThread) {
              // Each (map, seq) key is unique, so registers never collide; some land before stop
              // and some race with / land after it.
              manager.register(BlockKey(0, t.toLong, 0, i.toLong), bytesOf(32, 1.toByte))
              i += 1
            }
          } catch {
            case _: Throwable => failures.incrementAndGet()
          }
        })
        thread.start()
        thread
      }
      // Release the workers and tear down concurrently to maximize the race window.
      startLatch.countDown()
      manager.stop()
      workers.foreach(_.join(TimeUnit.SECONDS.toMillis(30)))

      // Whatever the interleaving, shutdown must have released every byte any worker reserved: a
      // register that acquired storage as stop ran re-checks `stopped` under the lock and releases.
      failures.get() mustBe 0
      manager.reservedBytesTotal mustBe 0L
      manager.trackedBytesTotal mustBe 0L
    } finally {
      resolver.stop()
    }
  }

  test("stop releases all buffered blocks and reserved memory") {
    val conf = new SparkConf(false)
    val resolver = new IndexShuffleBlockResolver(conf)
    val manager = new MemorySpillManager(
      conf, new TestMemoryManager(conf), resolver, new StreamingShuffleSource)
    try {
      manager.register(BlockKey(0, 0L, 0, 0L), bytesOf(128, 1.toByte))
      manager.register(BlockKey(0, 0L, 1, 0L), bytesOf(256, 2.toByte))
      manager.trackedBytesTotal mustBe 384L
      manager.reservedBytesTotal mustBe 384L
      manager.stop()
      manager.trackedBytesTotal mustBe 0L
      manager.reservedBytesTotal mustBe 0L
    } finally {
      resolver.stop()
    }
  }
}
