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

import org.mockito.Mockito.{mock, when}
import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleManager}
import org.apache.spark.shuffle.sort.SortShuffleManager

/**
 * Tests for [[StreamingShuffleManager]], the control-plane entry point of the opt-in streaming
 * shuffle engine. The suite verifies the three responsibilities the manager owns at the
 * `ShuffleManager` abstraction boundary -- everything else (the streaming write/read data path,
 * spill, backpressure) is exercised by the dedicated `StreamingShuffle*Suite` siblings:
 *
 *   1. Selection -- `spark.shuffle.manager=streaming` resolves to this class through
 *      `object ShuffleManager.getShuffleManagerClassName`, while `"sort"`/`"tungsten-sort"` stay
 *      mapped to [[SortShuffleManager]] (the default AND fallback engine is left unchanged).
 *   2. Handle dispatch -- `registerShuffle` returns a [[StreamingShuffleHandle]] only on the
 *      two-fold opt-in (manager selected AND `spark.shuffle.streaming.enabled=true`) for a
 *      supported dependency, and `getWriter`/`getReader` route a [[StreamingShuffleHandle]] to the
 *      fallback-wrapped streaming writer/reader and every other handle to the composed sort engine.
 *   3. Fallback delegation + lifecycle -- a disabled flag or an unsupported dependency (map-side
 *      combine) delegates registration to the composed [[SortShuffleManager]];
 *      `shuffleBlockResolver` returns the composed sort resolver; `unregisterShuffle`/`stop`
 *      delegate to it.
 *
 * ==Design notes for this suite==
 *  - Base trait: [[SharedSparkContext]] (not `LocalSparkContext`). The streaming-path
 *    `registerShuffle` reads `dependency.rdd.partitions.length` on the driver, and the composed
 *    sort fallback writer lazily loads executor components keyed by `spark.app.id`; both require a
 *    live `SparkContext`, so a single shared context (with the running app id copied onto `conf`)
 *    keeps every test real and hermetic. The pure factory-lookup selection tests simply ignore it.
 *  - Mocking: the [[org.apache.spark.ShuffleDependency]] is a LENIENT Mockito mock with only the
 *    members the manager / streaming writer / composed sort writer actually dereference stubbed.
 *    A strict (throw-on-unstubbed) mock would be brittle here because dispatch crosses several
 *    collaborators authored in parallel; a lenient mock tests the documented behavior without
 *    coupling to private read order.
 *  - Coexistence is preserved end to end: every test exercises only the streaming dispatch path or
 *    the documented delegation to sort, and `SortShuffleManager`'s own behavior (covered by
 *    `SortShuffleManagerSuite`) is never modified.
 */
class StreamingShuffleManagerSuite extends SparkFunSuite with Matchers with SharedSparkContext {

  // SharedSparkContext starts the SparkContext from `conf`, but Spark only stamps `spark.app.id`
  // onto the context's internal clone, never back onto this `conf`. The composed SortShuffleManager
  // reads the app id when it lazily loads its executor components on the first sort `getWriter`
  // (the fallback path), so copy the running application's id back onto `conf` here. This mirrors
  // what production `SparkEnv` does and keeps the fallback writer real.
  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.set("spark.app.id", sc.applicationId)
  }

  private val serializer = new JavaSerializer(conf)

  // A live task context over the shared SparkEnv, used to build streaming/sort writers and readers.
  private def fakeContext(): TaskContext = MemoryTestingUtils.fakeTaskContext(sc.env)

  // Builds a lenient Int->Int shuffle dependency stubbing exactly the members the manager and the
  // writers read: the partitioner (for numPartitions), the serializer, the empty aggregator/key
  // ordering, and the map-side-combine gate (`mapSideCombine=true` drives the registration-time
  // fallback to sort). `rdd` is stubbed with a REAL tiny parent RDD because the streaming
  // `registerShuffle` reads `dependency.rdd.partitions.length` on the driver to capture `numMaps`,
  // and `RDD.partitions` is `final` (so a real RDD is cleaner than mocking a final method). The
  // empty row-based checksums mirror the production default the composed sort writer reads.
  private def streamingDep(
      shuffleId: Int,
      numPartitions: Int,
      mapSideCombine: Boolean = false): ShuffleDependency[Int, Int, Int] = {
    val dep = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dep.shuffleId).thenReturn(shuffleId)
    when(dep.partitioner).thenReturn(new HashPartitioner(numPartitions))
    when(dep.serializer).thenReturn(serializer)
    when(dep.aggregator).thenReturn(None)
    when(dep.keyOrdering).thenReturn(None)
    when(dep.mapSideCombine).thenReturn(mapSideCombine)
    when(dep.rdd).thenReturn(
      sc.parallelize(0 until numPartitions, numPartitions).map(i => (i, i))
        .asInstanceOf[RDD[Product2[Int, Int]]])
    when(dep.rowBasedChecksums).thenReturn(ShuffleDependency.EMPTY_ROW_BASED_CHECKSUMS)
    dep
  }

  // A streaming dispatch handle over a supported dependency, used to drive getWriter/getReader
  // type-dispatch directly (bypassing registerShuffle). The 3-arg constructor carries `numMaps`,
  // which the executor-side reader uses to resolve the `endMapIndex = Int.MaxValue` sentinel.
  private def streamingHandle(
      shuffleId: Int,
      numPartitions: Int,
      numMaps: Int = 1): StreamingShuffleHandle[Int, Int, Int] =
    new StreamingShuffleHandle[Int, Int, Int](
      shuffleId, streamingDep(shuffleId, numPartitions), numMaps)

  // Constructs a manager with the streaming feature flag set as requested, runs `body`, and ALWAYS
  // stops the manager afterwards (tearing down any lazily-built streaming engine: the backpressure
  // heartbeat + spill poller daemon threads, the in-process exchange, and the best-effort network
  // transport) plus the composed sort engine. The flag is read in the manager constructor, so it is
  // applied to `conf` before construction and restored afterwards.
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

  // =============================================================================================
  // Group A -- Selection: `spark.shuffle.manager` name resolution through object ShuffleManager.
  // These are pure factory lookups and need no live context or manager instance.
  // =============================================================================================

  test("spark.shuffle.manager=streaming resolves to the StreamingShuffleManager class name") {
    // Coexistence: registering "streaming" in object ShuffleManager.shortShuffleMgrNames is the
    // ONLY wiring required for SparkEnv to instantiate the streaming engine dynamically; the
    // SparkEnv instantiation path itself is left untouched.
    val streamingConf = new SparkConf(false).set(config.SHUFFLE_MANAGER, "streaming")
    assert(ShuffleManager.getShuffleManagerClassName(streamingConf) ===
      classOf[StreamingShuffleManager].getName)
  }

  test("sort and tungsten-sort selection remain mapped to SortShuffleManager") {
    // Non-regression: the streaming entry must coexist with -- never replace -- the existing names,
    // so the default and the fallback engine resolve to SortShuffleManager exactly as before.
    val sortConf = new SparkConf(false).set(config.SHUFFLE_MANAGER, "sort")
    assert(ShuffleManager.getShuffleManagerClassName(sortConf) ===
      classOf[SortShuffleManager].getName)
    val tungstenConf = new SparkConf(false).set(config.SHUFFLE_MANAGER, "tungsten-sort")
    assert(ShuffleManager.getShuffleManagerClassName(tungstenConf) ===
      classOf[SortShuffleManager].getName)
  }

  // =============================================================================================
  // Group B -- registerShuffle dispatch: the two-fold opt-in and the registration-time fallback.
  // =============================================================================================

  test("registerShuffle returns a StreamingShuffleHandle when enabled and the dependency is " +
    "supported") {
    withManager(enabled = true) { manager =>
      val handle = manager.registerShuffle(0, streamingDep(0, numPartitions = 4))
      assert(handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
    }
  }

  test("registerShuffle delegates to the composed SortShuffleManager when streaming is disabled") {
    withManager(enabled = false) { manager =>
      // Disabled streaming means every shuffle is handled end-to-end by the composed sort engine,
      // which returns one of ITS handle types -- a BaseShuffleHandle subtype, not a streaming one.
      val handle = manager.registerShuffle(0, streamingDep(0, numPartitions = 4))
      assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
      assert(handle.isInstanceOf[BaseShuffleHandle[_, _, _]])
    }
  }

  test("registerShuffle delegates to the sort engine when the dependency needs map-side combine") {
    withManager(enabled = true) { manager =>
      // Streaming is enabled, but the streaming writer performs no map-side aggregation, so a
      // combine-requesting dependency must fall back to sort at registration time (graceful
      // degradation's registration-time half).
      val handle =
        manager.registerShuffle(0, streamingDep(0, numPartitions = 4, mapSideCombine = true))
      assert(!handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
    }
  }

  // =============================================================================================
  // Group C -- Resolver and lifecycle delegation: shuffleBlockResolver, unregisterShuffle, stop.
  // =============================================================================================

  test("shuffleBlockResolver returns the composed sort IndexShuffleBlockResolver") {
    withManager(enabled = true) { manager =>
      // Reusing the composed sort manager's single resolver (rather than creating a second one)
      // preserves the block-manager storage contract: spilled streaming blocks and sort blocks
      // share one on-disk path.
      assert(manager.shuffleBlockResolver.isInstanceOf[IndexShuffleBlockResolver])
    }
  }

  test("unregisterShuffle delegates to the composed SortShuffleManager and returns true") {
    withManager(enabled = true) { manager =>
      // No streaming engine has been activated (no getWriter/getReader yet), so unregisterShuffle
      // delegates straight to the composed sort manager, which returns true for any shuffle id --
      // matching the default manager's semantics so callers observe no behavioral difference.
      assert(manager.unregisterShuffle(0))
    }
  }

  test("stop is idempotent and rejects new streaming use afterwards") {
    val original = conf.get(config.STREAMING_SHUFFLE_ENABLED)
    conf.set(config.STREAMING_SHUFFLE_ENABLED, true)
    val manager = new StreamingShuffleManager(conf, isDriver = true)
    try {
      // Activate the streaming engine once (first streaming getWriter) so stop() has real
      // lifecycles -- the backpressure heartbeat + spill poller daemon threads, the in-process
      // exchange, the best-effort transport -- to tear down.
      val handle = streamingHandle(shuffleId = 1, numPartitions = 2)
      val ctx = fakeContext()
      manager.getWriter[Int, Int](handle, 0L, ctx, ctx.taskMetrics().shuffleWriteMetrics)
        .stop(success = false)
      manager.stop()
      // Idempotent: a second stop is a harmless no-op (composed sort stop() is also idempotent).
      noException should be thrownBy manager.stop()
      // First use after stop fails fast instead of silently spawning post-stop daemon threads.
      val ctx2 = fakeContext()
      intercept[IllegalStateException] {
        manager.getWriter[Int, Int](handle, 1L, ctx2, ctx2.taskMetrics().shuffleWriteMetrics)
      }
    } finally {
      manager.stop()
      conf.set(config.STREAMING_SHUFFLE_ENABLED, original)
    }
  }

  // =============================================================================================
  // Group D -- getWriter/getReader handle-type dispatch (needs a live SparkEnv).
  // =============================================================================================

  test("getWriter dispatches a StreamingShuffleHandle to the fallback-wrapped streaming writer") {
    withManager(enabled = true) { manager =>
      val handle = streamingHandle(shuffleId = 0, numPartitions = 2)
      val ctx = fakeContext()
      val writer =
        manager.getWriter[Int, Int](handle, 0L, ctx, ctx.taskMetrics().shuffleWriteMetrics)
      try {
        // The streaming writer is wrapped in a FallbackShuffleWriter so a runtime fallback raised
        // mid-write degrades to sort transparently. The wrapper is built ONLY on the streaming
        // path, so its presence proves streaming dispatch (the sort path never produces it).
        assert(writer.isInstanceOf[FallbackShuffleWriter[_, _]])
      } finally {
        writer.stop(success = false)
      }
    }
  }

  test("getWriter delegates a non-streaming handle to the composed sort writer") {
    withManager(enabled = true) { manager =>
      // A genuine sort handle (registered through a standalone SortShuffleManager) fed to the
      // streaming manager must flow to the composed sort engine, never the streaming wrapper --
      // the coexistence contract for every non-streaming handle.
      val sortManager = new SortShuffleManager(conf)
      try {
        val sortHandle = sortManager.registerShuffle(1, streamingDep(1, numPartitions = 2))
        val ctx = fakeContext()
        val writer =
          manager.getWriter[Int, Int](sortHandle, 0L, ctx, ctx.taskMetrics().shuffleWriteMetrics)
        try {
          assert(!writer.isInstanceOf[FallbackShuffleWriter[_, _]])
        } finally {
          writer.stop(success = false)
        }
      } finally {
        sortManager.stop()
      }
    }
  }

  test("getReader dispatches a StreamingShuffleHandle to the fallback-wrapped streaming reader") {
    withManager(enabled = true) { manager =>
      val handle = streamingHandle(shuffleId = 0, numPartitions = 2)
      val ctx = fakeContext()
      // Call the 7-arg getReader override directly (the 5-arg overload is final in the trait and
      // delegates here). read() is intentionally NOT invoked: it blocks on a live producer stream
      // and is covered by StreamingShuffleReaderSuite -- here we assert only the dispatch type, and
      // the streaming reader defers all fetching (and any MapOutputTracker contact) until read().
      val reader = manager.getReader[Int, Int](
        handle, 0, 1, 0, 1, ctx, ctx.taskMetrics().createTempShuffleReadMetrics())
      assert(reader.isInstanceOf[FallbackShuffleReader[_, _]])
    }
  }
}
