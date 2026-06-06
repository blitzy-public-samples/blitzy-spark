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

import java.util.concurrent.{Executors, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Random

import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.util.ThreadUtils

/**
 * End-to-end integration tests for the OPT-IN streaming shuffle engine. Unlike the focused
 * per-class unit suites (`StreamingShuffleManagerSuite`, `StreamingShuffleWriterSuite`,
 * `StreamingShuffleReaderSuite`, `BackpressureProtocolSuite`, `MemorySpillManagerSuite`), this
 * suite drives a REAL [[SparkContext]] selected purely via configuration
 * (`spark.shuffle.manager=streaming` + `spark.shuffle.streaming.enabled=true`) and exercises actual
 * shuffle jobs through the PUBLIC RDD API (`parallelize`/`map`/`reduceByKey`/`collect`). It
 * therefore validates the streaming engine end to end -- factory selection, the producer->consumer
 * data path, scheduler-level fault recovery, and concurrent-shuffle arbitration -- without
 * referencing any internal streaming method names, so it remains stable as the engine internals
 * evolve. Selection happens entirely through [[SparkConf]]; the streaming engine plugs in behind
 * the unchanged `ShuffleManager` abstraction boundary, with sort-based shuffle as the
 * default/fallback.
 *
 * Mirrors the local-cluster harness pattern of
 * `org.apache.spark.shuffle.HostLocalShuffleReadingSuite`
 * (`SparkFunSuite with Matchers with LocalSparkContext`, a local-cluster master string,
 * `TestUtils.waitUntilExecutorsUp`, `reduceByKey` jobs, and `shuffleReadMetrics` assertions).
 *
 * CI / robustness note: the AAP's "10GB / 100-partition latency check" is a PERFORMANCE design
 * target (documented in `docs/streaming-shuffle.md` and measured by
 * `StreamingShufflePerformanceBenchmark`), NOT a literal unit-test dataset. These tests scale the
 * data down to a CI-friendly size and assert FUNCTIONAL CORRECTNESS plus soft sanity bounds only;
 * no test asserts a hard wall-clock SLA, which would flake on shared CI hosts. Each test assigns
 * the context to the `sc` var from [[LocalSparkContext]] so its `afterEach` stops the context and
 * no test leaks a [[SparkContext]].
 */
class StreamingShuffleIntegrationSuite extends SparkFunSuite with Matchers with LocalSparkContext {

  /**
   * Builds a [[SparkConf]] that opts into the streaming shuffle engine and starts the context. The
   * manager name (`spark.shuffle.manager=streaming`) and the feature flag
   * (`spark.shuffle.streaming.enabled=true`) MUST be set BEFORE `SparkContext` initialization,
   * because `SparkEnv.initializeShuffleManager` resolves and constructs the `ShuffleManager` from
   * these values during context bring-up. The result is returned for assignment to the `sc` var so
   * [[LocalSparkContext]] tears it down automatically after each test.
   *
   * @param master the master URL (defaults to `local[4]`; tests that need cross-JVM executors or
   *               task retries pass a local-cluster or `local[N, maxFailures]` master)
   * @param extra  optional extra `SparkConf` entries (e.g. scheduler knobs) merged in last
   */
  private def newStreamingContext(
      master: String = "local[4]",
      extra: Map[String, String] = Map.empty): SparkContext = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("StreamingShuffleIntegrationSuite")
      .set(config.SHUFFLE_MANAGER, "streaming")
      .set(config.STREAMING_SHUFFLE_ENABLED, true)
    extra.foreach { case (k, v) => conf.set(k, v) }
    new SparkContext(conf)
  }

  test("SparkEnv uses StreamingShuffleManager when spark.shuffle.manager=streaming") {
    // End-to-end proof of the factory-map registration (the "streaming" entry in
    // ShuffleManager.shortShuffleMgrNames) plus the config wiring: selection is purely
    // config-driven, so SparkEnv must construct a StreamingShuffleManager once both knobs are set.
    // This is the cheapest, most direct assertion that the streaming engine is reachable through
    // the public configuration surface.
    sc = newStreamingContext()
    assert(sc.env.shuffleManager.isInstanceOf[StreamingShuffleManager])
  }

  test("reduceByKey shuffle produces correct results through the streaming manager") {
    // Scaled-down stand-in for the AAP's "10GB / 100-partition" target. 10GB is a PERFORMANCE
    // design target (documented in docs/streaming-shuffle.md and exercised by
    // StreamingShufflePerformanceBenchmark), NOT a literal CI dataset -- allocating 10GB in a unit
    // test would be hostile to CI. We instead drive a few hundred-thousand synthetic records across
    // ~100 partitions and assert FUNCTIONAL CORRECTNESS of the shuffle through the streaming
    // engine.
    sc = newStreamingContext("local[4]")
    val n = 200000
    val numParts = 100
    val rdd = sc.parallelize(0 until n, numParts).map(i => (i % 1000, 1))
    val startNanos = System.nanoTime()
    val result = rdd.reduceByKey(_ + _, numParts).collect().toMap
    val elapsedMs = (System.nanoTime() - startNanos) / 1000000L
    // Timing is LOGGED for visibility, never asserted as a hard SLA (CI safety). The 30-50% latency
    // reduction target is validated by StreamingShufflePerformanceBenchmark, not here.
    logInfo(s"reduceByKey over $n records / $numParts partitions completed in $elapsedMs ms")
    // Each key 0..999 appears exactly n/1000 times with value 1, so every reduced sum is n/1000.
    assert(result.size === 1000)
    assert(result.values.forall(_ == n / 1000))
    // Very loose smoke guard only -- generous enough to never flake on a busy CI host.
    assert(elapsedMs < 120000L)
  }

  test("multi-partition shuffle over a local cluster yields correct aggregates") {
    // Multi-executor (real cross-JVM) variant: exercises the streaming engine across executor
    // boundaries on a local cluster, mirroring HostLocalShuffleReadingSuite's harness. Memory is
    // kept modest (1024 MiB) for CI. We wait for both executors to register so the work is not
    // skewed onto a single executor (same rationale as the mirror).
    sc = newStreamingContext("local-cluster[2, 1, 1024]")
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)

    val n = 100000
    val numParts = 100
    val numKeys = 500
    // scala.util.Random with a FIXED seed makes the synthetic key distribution deterministic
    // (reproducible across runs) while still being non-trivial. The reduced TOTAL is invariant to
    // the distribution, which keeps the correctness assertion exact and non-flaky.
    val rng = new Random(2025)
    // Build an IndexedSeq directly (not an Array) so sc.parallelize takes a Seq without the
    // deprecated Array -> immutable.IndexedSeq implicit copy (deprecation is fatal in the test
    // compile via -Wconf). IndexedSeq.fill yields a Vector, which parallelize slices efficiently.
    val keys = IndexedSeq.fill(n)(rng.nextInt(numKeys))
    val reduced = sc.parallelize(keys, numParts).map(k => (k, 1L)).reduceByKey(_ + _, numParts)
    val result = reduced.collect().toMap

    // Every emitted "1" is accounted for exactly once across all reducers (zero data loss), every
    // observed key falls within the synthetic key space, and no spurious keys appear.
    assert(result.values.sum === n.toLong)
    assert(result.keys.forall(k => k >= 0 && k < numKeys))
    assert(result.size <= numKeys)

    // Soft telemetry check (presence/positivity, NOT exact byte/record counts): the reduce side
    // actually read shuffle records through the streaming engine. `iter.size` forces the shuffle
    // read before the per-task metric is sampled. Exact spill/byte counts are unit-tested elsewhere
    // (MemorySpillManagerSuite); here we only assert the read path produced records and that the
    // consumed record count matches the distinct-key total.
    val perTask = reduced.mapPartitions { iter =>
      val consumed = iter.size.toLong
      Iterator.single((consumed, TaskContext.get().taskMetrics().shuffleReadMetrics.recordsRead))
    }.collect()
    assert(perTask.map(_._1).sum === result.size.toLong)
    perTask.map(_._2).sum must be > 0L
  }

  test("streaming shuffle survives a task failure and recomputes via lineage") {
    // Fault-tolerance is delegated, UNCHANGED, to the Spark scheduler: the streaming reader throws
    // the existing FetchFailedException on partial-read invalidation and the DAG scheduler converts
    // it into upstream stage recomputation (the throw itself is unit-tested in
    // StreamingShuffleReaderSuite). Here we validate the SCHEDULER-LEVEL recovery path by injecting
    // a transient task failure and asserting the job still produces correct results.
    //
    // We must use the "local[N, maxFailures]" master form: a plain "local[N]" master pins
    // maxFailures to 1 and IGNORES spark.task.maxFailures, so the first failure aborts the job.
    // "local[4, 4]" enables the scheduler's retry-and-recompute path in local mode (this is exactly
    // the documented "used in tests with failing tasks" master format).
    sc = newStreamingContext("local[4, 4]")
    val n = 50000
    val numParts = 20
    val numKeys = 100
    val rdd = sc.parallelize(0 until n, numParts).map { i =>
      // Fail exactly once, on the FIRST attempt of partition 0. The scheduler retries the task
      // (attemptNumber becomes 1) and it then succeeds, so the producer's map output is recomputed.
      if (TaskContext.get().attemptNumber() == 0 && TaskContext.get().partitionId() == 0) {
        throw new RuntimeException("injected transient producer failure")
      }
      (i % numKeys, 1)
    }.reduceByKey(_ + _, numParts)
    val result = rdd.collect().toMap
    // Despite the injected failure and recompute, the aggregate is exactly correct (no data loss).
    assert(result.size === numKeys)
    assert(result.values.forall(_ == n / numKeys))
  }

  test("an unconditional task failure surfaces as a SparkException (no silent success)") {
    // Companion to the recompute test: when failures are NOT transient, the job must FAIL loudly
    // rather than silently produce a truncated result. We pin maxFailures to 1 (a plain local[N]
    // master disables retries regardless, so an unconditional failure aborts on the first attempt)
    // and assert the SparkException propagates. This guards against a streaming engine that might
    // swallow a producer error. Kept clearly separate so the recompute test above stays green.
    sc = newStreamingContext("local[4]", Map("spark.task.maxFailures" -> "1"))
    val rdd = sc.parallelize(0 until 1000, 8).map { i =>
      // The `else` branch fixes the closure's element type to (Int, Int) so reduceByKey compiles;
      // `i >= 0` always holds for this input, so every record fails (no dead code after `throw`).
      if (i >= 0) {
        throw new RuntimeException("unconditional producer failure")
      } else {
        (i % 10, 1)
      }
    }.reduceByKey(_ + _)
    intercept[SparkException] {
      rdd.count()
    }
  }

  test("consumer slowdown does not corrupt results (graceful handling)") {
    // Simulate a SLOW CONSUMER by sleeping briefly on a subset of reduce partitions and assert the
    // results are still exactly correct. The sleep is intentionally tiny (a few ms on a few
    // partitions) so CI stays fast: the goal is correctness under consumer slowdown, NOT measuring
    // the "2x slower for 60s" fallback timer (that threshold is unit-tested in
    // BackpressureProtocolSuite). Full-fidelity network-partition injection is not feasible in a
    // unit test; partition/timeout -> FetchFailedException -> recompute is covered by
    // StreamingShuffleReaderSuite, and the scheduler-level recovery path by the task-failure test
    // above.
    sc = newStreamingContext("local[4]")
    val n = 40000
    val numParts = 16
    val numKeys = 200
    val reduced = sc.parallelize(0 until n, numParts).map(i => (i % numKeys, 1))
      .reduceByKey(_ + _, numParts)
    val result = reduced.mapPartitionsWithIndex { (idx, iter) =>
      // Throttle the first few consumer partitions to model an uneven / slow consumer.
      if (idx < 3) {
        Thread.sleep(5)
      }
      iter
    }.collect().toMap
    assert(result.size === numKeys)
    assert(result.values.forall(_ == n / numKeys))
  }

  test("concurrent shuffles complete correctly under arbitration") {
    // Two independent shuffle jobs run CONCURRENTLY on the same context from separate threads. This
    // validates that the BackpressureProtocol's token-bucket split
    // (maxBandwidthMBps / numConcurrentShuffles) and the QoS arbitration neither corrupt nor
    // deadlock concurrent shuffles -- each job must produce its own correct, independent result.
    sc = newStreamingContext("local[4]")
    val pool = Executors.newFixedThreadPool(2)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
    try {
      // Each job aggregates the same number of records but over a different key space and partition
      // count, so a cross-job leak or arbitration bug would corrupt one of the two known answers.
      def shuffleJob(numKeys: Int, numParts: Int): Future[Map[Int, Int]] = Future {
        val recordsPerJob = 60000
        sc.parallelize(0 until recordsPerJob, numParts).map(i => (i % numKeys, 1))
          .reduceByKey(_ + _, numParts).collect().toMap
      }
      val firstJob = shuffleJob(numKeys = 100, numParts = 50)
      val secondJob = shuffleJob(numKeys = 250, numParts = 80)
      // Generous await -- the bound only guards against a true deadlock, never a performance SLA.
      // ThreadUtils.awaitResult is the mandated Spark idiom (it handles fork-join pool blocking and
      // exception unwrapping); a bare blocking await on the future is banned by scalastyle.
      val firstResult = ThreadUtils.awaitResult(firstJob, 120.seconds)
      val secondResult = ThreadUtils.awaitResult(secondJob, 120.seconds)
      assert(firstResult.size === 100)
      assert(firstResult.values.forall(_ == 60000 / 100))
      assert(secondResult.size === 250)
      assert(secondResult.values.forall(_ == 60000 / 250))
    } finally {
      pool.shutdown()
      pool.awaitTermination(10, TimeUnit.SECONDS)
    }
  }
}
