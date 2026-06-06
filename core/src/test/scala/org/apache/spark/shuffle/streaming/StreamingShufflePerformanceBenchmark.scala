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

import scala.util.Try

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config

/**
 * Benchmark for the streaming shuffle engine vs. the sort-based baseline.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/StreamingShufflePerformanceBenchmark-results.txt".
 * }}}
 */
object StreamingShufflePerformanceBenchmark extends BenchmarkBase {

  // This benchmark is read-only with respect to production code: it picks the shuffle engine
  // purely through configuration and reuses existing Spark APIs (SparkContext, RDD operations,
  // Benchmark/BenchmarkBase). It adds no new external dependencies and never mutates a shipped
  // class. The "streaming" case routes through the StreamingShuffleManager registered in the
  // shortShuffleMgrNames factory map, which composes a SortShuffleManager and gracefully falls
  // back to it when streaming is disabled or a fallback condition fires -- so the benchmark
  // exercises the real sort/streaming coexistence path rather than a forked code path.

  // CI-friendly defaults. The AAP target workload for full/manual runs is 10GB+ data over 100+
  // partitions; these defaults are scaled down so automated execution stays bounded. Override
  // them via mainArgs: mainArgs(0) = numRecords, mainArgs(1) = numPartitions, each parsed with a
  // safe fallback to the corresponding default below.
  val defaultNumRecords = 1 * 1000 * 1000
  val defaultNumPartitions = 16

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // Optional positional overrides; fall back to defaults when absent or non-numeric.
    val numRecords = Try(mainArgs(0).toInt).getOrElse(defaultNumRecords)
    val numPartitions = Try(mainArgs(1).toInt).getOrElse(defaultNumPartitions)

    runBenchmark("Streaming Shuffle vs Sort Shuffle") {
      val benchmark = new Benchmark("shuffle latency", numRecords, 3, output = output)
      benchmark.addCase("sort-baseline") { _ =>
        runShuffle(shuffleManager = "sort", streamingEnabled = false, numRecords, numPartitions)
      }
      benchmark.addCase("streaming") { _ =>
        runShuffle(
          shuffleManager = "streaming", streamingEnabled = true, numRecords, numPartitions)
      }
      benchmark.run()
    }
  }

  // Drives one real shuffle through the configured engine and materializes it, so the Benchmark
  // harness times an end-to-end producer->consumer exchange. The engine is chosen purely via
  // configuration: spark.shuffle.manager selects "sort" (baseline) or "streaming" (opt-in) via
  // the shortShuffleMgrNames factory map, and spark.shuffle.streaming.enabled is the extra opt-in
  // flag. When that flag is false -- or a fallback condition fires -- the streaming manager
  // delegates to its composed SortShuffleManager, so this helper always completes via that path.
  private def runShuffle(
      shuffleManager: String,
      streamingEnabled: Boolean,
      numRecords: Int,
      numPartitions: Int): Unit = {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("StreamingShufflePerformanceBenchmark")
      // Picks sort vs streaming through the shortShuffleMgrNames factory map.
      .set("spark.shuffle.manager", shuffleManager)
      // Extra opt-in flag; false for the sort baseline, true for the streaming case.
      .set(config.STREAMING_SHUFFLE_ENABLED.key, streamingEnabled.toString)
    val sc = new SparkContext(conf)
    try {
      // reduceByKey forces a shuffle boundary; count() materializes it so the configured engine
      // performs a complete map->reduce exchange that the benchmark closure measures.
      sc.parallelize(0 until numRecords, numPartitions)
        .map(i => (i % numPartitions, i))
        .reduceByKey(_ + _, numPartitions)
        .count()
    } finally {
      // Always stop the context so a leaked SparkContext cannot break later benchmark cases.
      sc.stop()
    }
  }
}
