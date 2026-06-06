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

import scala.util.Random

import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, when}
import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.{ShuffleWriter, ShuffleWriteMetricsReporter}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.util.Utils

/**
 * Tests for [[StreamingShuffleManager]], the control-plane entry point of the opt-in streaming
 * shuffle engine. The suite verifies the three responsibilities the manager owns:
 *
 *   1. Selection -- `registerShuffle` returns a [[StreamingShuffleHandle]] only on the two-fold
 *      opt-in (manager selected AND `spark.shuffle.streaming.enabled=true`) for a supported
 *      dependency, and otherwise delegates to the composed [[SortShuffleManager]].
 *   2. Dispatch -- `getWriter`/`getReader` route a [[StreamingShuffleHandle]] to the
 *      fallback-wrapped streaming writer/reader and every other handle to the sort engine.
 *   3. Runtime graceful degradation -- a fallback condition raised mid-write (oversize unsplittable
 *      block, sustained-slow consumer, exhausted admission deadline / memory pressure) is caught by
 *      the manager-owned [[FallbackShuffleWriter]] BEFORE any output is advertised, transparently
 *      re-routed to sort, and still produces a standard [[MapStatus]] -- never a task failure.
 *
 * Plus the two resource-safety properties: `unregisterShuffle` reclaims streaming buffers for a
 * shuffle torn down before consumers acknowledge, and `stop` is idempotent and race-safe against a
 * concurrent first streaming use.
 *
 * The suite runs over a real `SparkEnv` (via [[SharedSparkContext]]) so the composed sort engine,
 * block manager, serializer, and memory manager are genuine. Coexistence is preserved: every test
 * exercises only the streaming dispatch path or the documented fallback to sort, and the sort
 * engine's own behavior (covered by `SortShuffleManagerSuite`) is never modified.
 */
class StreamingShuffleManagerSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers {

  // SharedSparkContext starts the SparkContext from `conf`, but Spark only stamps `spark.app.id`
  // onto the context's internal clone (`sc.conf`), never back onto this `conf`. The composed
  // SortShuffleManager reads the app id when it lazily loads its executor components on the first
  // sort `getWriter` (the fallback path), so we copy the running application's id back onto `conf`
  // here. This mirrors what `SparkEnv` does in production and keeps the fallback writer real.
  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.set("spark.app.id", sc.applicationId)
  }

  private val serializer = new JavaSerializer(conf)

  // A hash partitioner shaped like the other streaming suites', sized to `n`.
  private def partitionerFor(n: Int): Partitioner = new Partitioner {
    override def numPartitions: Int = n
    override def getPartition(key: Any): Int = Utils.nonNegativeMod(key.hashCode, n)
  }

  // A live task context over the shared SparkEnv, used to build writers/readers.
  private def fakeContext(): TaskContext = MemoryTestingUtils.fakeTaskContext(sc.env)

  // Builds a mocked Int->Int shuffle dependency stubbing every member the manager, the streaming
  // writer, and the composed sort writer read (partitioner, serializer, aggregator/keyOrdering are
  // None, and the map-side-combine gate). `mapSideCombine=true` drives the registration-time
  // fallback to sort.
  private def intDependency(
      shuffleId: Int,
      numPartitions: Int,
      mapSideCombine: Boolean = false): ShuffleDependency[Int, Int, Int] = {
    val dep = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dep.shuffleId).thenReturn(shuffleId)
    when(dep.partitioner).thenReturn(partitionerFor(numPartitions))
    when(dep.serializer).thenReturn(serializer)
    when(dep.aggregator).thenReturn(None)
    when(dep.keyOrdering).thenReturn(None)
    when(dep.mapSideCombine).thenReturn(mapSideCombine)
    // registerShuffle (the streaming path) reads dependency.rdd.partitions.length on the driver to
    // capture numMaps for the handle (QA finding F-1). The production dependency's rdd is non-null
    // there; stub a real (tiny) parent RDD here so the manager can compute the map count. Using a
    // real RDD (not a mock) sidesteps RDD's `final` partitions method.
    when(dep.rdd).thenReturn(
      sc.parallelize(0 until numPartitions, numPartitions).map(i => (i, i))
        .asInstanceOf[RDD[Product2[Int, Int]]])
    // The composed sort writer's ExternalSorter reads the dependency's row-based checksums even
    // when none are configured; the production default is an empty array, so mirror that here.
    // Without this stub the mock returns null and the real sort fallback writer NPEs on insertAll.
    when(dep.rowBasedChecksums).thenReturn(ShuffleDependency.EMPTY_ROW_BASED_CHECKSUMS)
    dep
  }

  private def intHandle(
      shuffleId: Int,
      numPartitions: Int,
      numMaps: Int = 1): StreamingShuffleHandle[Int, Int, Int] =
    new StreamingShuffleHandle[Int, Int, Int](
      shuffleId, intDependency(shuffleId, numPartitions), numMaps)

  // Constructs a manager with the streaming feature flag set as requested, runs `body`, and ALWAYS
  // stops the manager (tearing down any lazily-built streaming engine: heartbeat/poller daemon
  // threads, the in-process exchange, and the best-effort network transport) plus the composed sort
  // engine. The flag is read in the manager constructor, so it is applied to `conf` before
  // construction and restored afterwards.
  private def withManager(enabled: Boolean)(body: StreamingShuffleManager => Unit): Unit = {
    val original = conf.get(config.STREAMING_SHUFFLE_ENABLED)
    conf.set(config.STREAMING_SHUFFLE_ENABLED, enabled)
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    try {
      body(manager)
    } finally {
      manager.stop()
      conf.set(config.STREAMING_SHUFFLE_ENABLED, original)
    }
  }

  // Reads the manager's lazily-built streaming engine spill manager via reflection so a test can
  // assert real byte accounting after unregisterShuffle. White-box access mirrors the
  // PrivateMethodTester usage already established in StreamingShuffleWriterSuite; the
  // field/accessor names are verified against the compiled class. Returns None if never built.
  private def engineSpillManager(manager: StreamingShuffleManager): Option[MemorySpillManager] = {
    val field = classOf[StreamingShuffleManager].getDeclaredField("engine")
    field.setAccessible(true)
    Option(field.get(manager)).map { engine =>
      val accessor = engine.getClass.getDeclaredMethod("spillManager")
      accessor.setAccessible(true)
      accessor.invoke(engine).asInstanceOf[MemorySpillManager]
    }
  }

  // ===========================================================================================
  // Group A -- Selection (registerShuffle): the two-fold opt-in and registration-time fallback.
  // ===========================================================================================

  test("registerShuffle returns a streaming handle when enabled and the dependency is supported") {
    withManager(enabled = true) { manager =>
      val handle = manager.registerShuffle(0, intDependency(0, numPartitions = 4))
      handle mustBe a[StreamingShuffleHandle[_, _, _]]
    }
  }

  test("registerShuffle delegates to the sort engine when streaming is disabled") {
    withManager(enabled = false) { manager =>
      val handle = manager.registerShuffle(0, intDependency(0, numPartitions = 4))
      // Disabled streaming means every shuffle is handled end-to-end by the composed sort engine,
      // which returns one of ITS handle types -- never a StreamingShuffleHandle.
      handle must not be a[StreamingShuffleHandle[_, _, _]]
    }
  }

  test("registerShuffle delegates to the sort engine when the dependency needs map-side combine") {
    withManager(enabled = true) { manager =>
      // Streaming is enabled, but the streaming writer performs no map-side aggregation, so a
      // combine-requesting dependency must fall back to sort at registration time.
      val handle =
        manager.registerShuffle(0, intDependency(0, numPartitions = 4, mapSideCombine = true))
      handle must not be a[StreamingShuffleHandle[_, _, _]]
    }
  }

  // ===========================================================================================
  // Group B -- Dispatch (getWriter/getReader): handle-type based routing.
  // ===========================================================================================

  test("getWriter dispatches a streaming handle to the fallback-wrapped streaming writer") {
    withManager(enabled = true) { manager =>
      val handle = intHandle(shuffleId = 0, numPartitions = 2)
      val context = fakeContext()
      val writer =
        manager.getWriter[Int, Int](handle, 0L, context, context.taskMetrics().shuffleWriteMetrics)
      try {
        writer mustBe a[FallbackShuffleWriter[_, _]]
      } finally {
        writer.stop(success = false)
      }
    }
  }

  test("getReader dispatches a streaming handle to the fallback-wrapped streaming reader") {
    withManager(enabled = true) { manager =>
      val handle = intHandle(shuffleId = 0, numPartitions = 2)
      val context = fakeContext()
      val reader = manager.getReader[Int, Int](
        handle, 0, 1, 0, 1, context, context.taskMetrics().createTempShuffleReadMetrics())
      // Only assert the dispatch type; read() blocks on a live producer and is covered elsewhere.
      reader mustBe a[FallbackShuffleReader[_, _]]
    }
  }

  test("getWriter dispatches a non-streaming handle to the composed sort writer") {
    withManager(enabled = true) { manager =>
      // Obtain a genuine sort handle from a standalone sort manager and feed it to the streaming
      // manager: a non-streaming handle must flow to the composed sort engine, never the wrapper.
      val sortManager = new SortShuffleManager(conf)
      try {
        val sortHandle = sortManager.registerShuffle(1, intDependency(1, numPartitions = 2))
        val context = fakeContext()
        val writer = manager.getWriter[Int, Int](
          sortHandle, 0L, context, context.taskMetrics().shuffleWriteMetrics)
        try {
          writer must not be a[FallbackShuffleWriter[_, _]]
        } finally {
          writer.stop(success = false)
        }
      } finally {
        sortManager.stop()
      }
    }
  }

  // ===========================================================================================
  // Group C -- Runtime graceful degradation: every fallback condition produces a MapStatus and
  // surfaces NO StreamingShuffleFallbackException to the task.
  // ===========================================================================================

  test("an oversize unsplittable record degrades to sort end-to-end and still emits a MapStatus") {
    withManager(enabled = true) { manager =>
      // A single record whose serialized block exceeds the 2MB pipelined cap cannot be streamed;
      // the real streaming writer raises a fallback signal that the manager's wrapper catches and
      // re-routes to sort. Random bytes are incompressible so the block stays over the cap.
      val big = new Array[Byte](3 * 1024 * 1024)
      new Random(42).nextBytes(big)
      val dependency = mock(classOf[ShuffleDependency[Int, Array[Byte], Array[Byte]]])
      when(dependency.shuffleId).thenReturn(0)
      when(dependency.partitioner).thenReturn(partitionerFor(2))
      when(dependency.serializer).thenReturn(serializer)
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      when(dependency.mapSideCombine).thenReturn(false)
      // The sort fallback writer's ExternalSorter reads the (empty) row-based checksums; stub them
      // so the real sort write path does not NPE once the oversize record degrades to sort.
      when(dependency.rowBasedChecksums).thenReturn(ShuffleDependency.EMPTY_ROW_BASED_CHECKSUMS)
      val handle =
        new StreamingShuffleHandle[Int, Array[Byte], Array[Byte]](0, dependency, numMaps = 1)
      val context = fakeContext()
      val writer = manager.getWriter[Int, Array[Byte]](
        handle, 0L, context, context.taskMetrics().shuffleWriteMetrics)
      // write() must NOT throw: the fallback is handled internally, not surfaced as a task failure.
      writer.write(Iterator((0, big)))
      val status = writer.stop(success = true)
      status.isDefined mustBe true
      status.get.mapId mustBe 0L
      // The record really landed in sort output: at least one reduce partition has bytes.
      writer.getPartitionLengths().sum must be > 0L
    }
  }

  test("a sustained-slow consumer degrades the writer to sort without failing the task") {
    // A REAL streaming writer with a backpressure mock whose shouldFallback signal is asserted:
    // emitBlock raises the fallback, and the manager-owned wrapper re-routes to a REAL sort writer.
    val backpressure = mock(classOf[BackpressureProtocol])
    when(backpressure.shouldFallback).thenReturn(true)
    assertWriterFallsBackToSort(backpressure, mapId = 2L)
  }

  test("an exhausted admission deadline degrades the writer to sort without failing the task") {
    // A REAL streaming writer whose token-bucket admission is denied (not interrupted) raises the
    // fallback at emitBlock; the wrapper degrades to a REAL sort writer.
    val backpressure = mock(classOf[BackpressureProtocol])
    when(backpressure.shouldFallback).thenReturn(false)
    when(backpressure.acquireBlocking(anyLong())).thenReturn(false)
    assertWriterFallsBackToSort(backpressure, mapId = 3L)
  }

  // Drives a real StreamingShuffleWriter (whose emit path is forced to raise a fallback by the
  // supplied backpressure mock) through a manager-owned FallbackShuffleWriter backed by a real sort
  // manager, and asserts the task does not fail and a sort MapStatus with all records is produced.
  private def assertWriterFallsBackToSort(
      backpressure: BackpressureProtocol,
      mapId: Long): Unit = {
    val sortManager = new SortShuffleManager(conf)
    val source = new StreamingShuffleSource
    try {
      val numPartitions = 3
      val handle = intHandle(shuffleId = 5, numPartitions)
      val context = fakeContext()
      val metrics = context.taskMetrics().shuffleWriteMetrics
      // Mock the spill/exchange collaborators: with a fallback forced before any block is admitted,
      // neither participates in real byte movement; the writer's failure cleanup just calls them.
      val streamingWriter = new StreamingShuffleWriter[Int, Int](
        handle, mapId, context, metrics, conf, backpressure,
        mock(classOf[MemorySpillManager]), mock(classOf[StreamingBlockExchange]), source)
      val fallbackWriter = new FallbackShuffleWriter[Int, Int](
        handle, mapId, context, metrics, streamingWriter, sortManager,
        sc.env.blockManager, 4L * 1024L * 1024L)
      val records = (0 until 24).map(i => (i, i * 7))
      // The fallback is caught inside write(); no exception escapes to the task.
      fallbackWriter.write(records.iterator)
      val status = fallbackWriter.stop(success = true)
      status.isDefined mustBe true
      status.get.mapId mustBe mapId
      fallbackWriter.getPartitionLengths().sum must be > 0L
    } finally {
      sortManager.stop()
    }
  }

  test("a memory-pressure fallback signal replays every record into sort with no loss") {
    // Precise replay-correctness check for the manager's fallback mechanism: a streaming writer
    // that consumes part of the input (tee'd into the replay buffer) then raises the fallback must
    // result in the COMPLETE record sequence reaching the sort writer, in order, with none lost or
    // duplicated. A recording sort manager captures exactly what sort received.
    val records = (0 until 20).map(i => (i, i * 3))
    val recorded = scala.collection.mutable.ArrayBuffer[(Int, Int)]()
    val recordingSortWriter = new ShuffleWriter[Int, Int] {
      override def write(in: Iterator[Product2[Int, Int]]): Unit =
        in.foreach(r => recorded += ((r._1, r._2)))
      override def stop(success: Boolean): Option[MapStatus] = Some(mock(classOf[MapStatus]))
      override def getPartitionLengths(): Array[Long] = Array.emptyLongArray
    }
    val sortManager = mock(classOf[SortShuffleManager])
    org.mockito.Mockito
      .doReturn(recordingSortWriter)
      .when(sortManager).getWriter(any(), anyLong(), any(), any[ShuffleWriteMetricsReporter]())

    // A streaming writer that consumes 5 records (tee'd into the replay buffer) then signals a
    // memory-pressure fallback -- the OOM-risk-during-buffer-allocation condition from the AAP.
    val streamingWriter = new ShuffleWriter[Int, Int] {
      override def write(in: Iterator[Product2[Int, Int]]): Unit = {
        var consumed = 0
        while (consumed < 5 && in.hasNext) {
          in.next()
          consumed += 1
        }
        throw new StreamingShuffleFallbackException(
          "simulated memory pressure prevented buffer allocation; falling back to sort")
      }
      override def stop(success: Boolean): Option[MapStatus] = None
      override def getPartitionLengths(): Array[Long] = Array.emptyLongArray
    }

    val handle = intHandle(shuffleId = 6, numPartitions = 2)
    val context = fakeContext()
    val fallbackWriter = new FallbackShuffleWriter[Int, Int](
      handle, 4L, context, context.taskMetrics().shuffleWriteMetrics, streamingWriter,
      sortManager, sc.env.blockManager, 4L * 1024L * 1024L)
    fallbackWriter.write(records.iterator)
    val status = fallbackWriter.stop(success = true)
    status.isDefined mustBe true
    // The exact original sequence reached sort: the 5 consumed-then-buffered records followed by
    // the 15 not-yet-pulled source records, in order, with nothing lost or duplicated.
    recorded.toSeq mustBe records
  }

  // ===========================================================================================
  // Group D -- Resource lifecycle and safety: unregister cleanup and stop race/idempotency.
  // ===========================================================================================

  test("unregisterShuffle reclaims streaming buffers when torn down before consumers acknowledge") {
    withManager(enabled = true) { manager =>
      val shuffleId = 7
      val handle = intHandle(shuffleId, numPartitions = 4)
      val context = fakeContext()
      val writer = manager.getWriter[Int, Int](
        handle, 0L, context, context.taskMetrics().shuffleWriteMetrics)
      // Stream blocks but do NOT stop the writer successfully and do NOT subscribe a consumer, so
      // the blocks remain buffered (unacknowledged) in the engine's spill manager.
      writer.write((0 until 64).map(i => (i, i)).iterator)
      val spill = engineSpillManager(manager).getOrElse(
        fail("the streaming engine was never constructed"))
      spill.trackedBytesTotal must be > 0L
      // Unregistering the shuffle before any acknowledgment must reclaim those streaming buffers
      // (the F3 cleanup) -- delegating to sort alone could never free them.
      manager.unregisterShuffle(shuffleId) mustBe true
      spill.trackedBytesTotal mustBe 0L
      spill.reservedBytesTotal mustBe 0L
    }
  }

  test("stop is idempotent and rejects new streaming use afterwards") {
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    val handle = intHandle(shuffleId = 8, numPartitions = 2)
    // Activate the streaming engine once so stop() has real lifecycles to tear down.
    val context = fakeContext()
    manager.getWriter[Int, Int](handle, 0L, context, context.taskMetrics().shuffleWriteMetrics)
      .stop(success = false)
    manager.stop()
    // Idempotent: a second stop is a harmless no-op.
    noException should be thrownBy manager.stop()
    // First use after stop fails fast (F4) instead of silently spawning daemon threads post-stop.
    val ctx = fakeContext()
    intercept[IllegalStateException] {
      manager.getWriter[Int, Int](handle, 1L, ctx, ctx.taskMetrics().shuffleWriteMetrics)
    }
  }

  test("stop is race-safe against a concurrent first streaming use") {
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    val handle = intHandle(shuffleId = 9, numPartitions = 2)
    @volatile var built: ShuffleWriter[Int, Int] = null
    // One thread attempts the first streaming use while the main thread stops the manager. F4
    // guarantees exactly one consistent outcome: either the engine is constructed and then stopped
    // by stop(), or construction is refused with IllegalStateException -- never a leaked engine.
    val user = new Thread(() => {
      try {
        val ctx = fakeContext()
        built = manager.getWriter[Int, Int](
          handle, 0L, ctx, ctx.taskMetrics().shuffleWriteMetrics)
      } catch {
        case _: IllegalStateException => // stop() won the race; acceptable and expected
        case _: Throwable => // ignore any other transient error in the narrow race window
      }
    }, "streaming-stop-race-user")
    user.start()
    manager.stop()
    user.join(10000)
    assert(!user.isAlive, "the concurrent user thread did not terminate")
    if (built != null) {
      try built.stop(success = false) catch { case _: Throwable => () }
    }
    // Invariant after stop: any further streaming use is consistently rejected.
    val ctx = fakeContext()
    intercept[IllegalStateException] {
      manager.getWriter[Int, Int](handle, 1L, ctx, ctx.taskMetrics().shuffleWriteMetrics)
    }
  }
}
