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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Performance benchmark comparing sort-based shuffle vs streaming shuffle.
 *
 * Measures end-to-end latency for groupByKey and reduceByKey operations using both the
 * default sort-based shuffle manager and the streaming shuffle manager. Also benchmarks
 * the impact of different buffer size configurations on streaming shuffle performance.
 *
 * AAP Performance Targets:
 * - 30-50% end-to-end latency reduction for shuffle-heavy workloads (10GB+ data, 100+ partitions)
 * - 5-10% improvement for CPU-bound workloads through reduced scheduler overhead
 * - Zero performance regression for memory-bound workloads via automatic fallback
 *
 * The streaming shuffle manager (StreamingShuffleManager) is loaded at runtime via reflection
 * when benchmark cases set `spark.shuffle.manager=streaming`. It is not directly imported
 * but must be on the classpath for the streaming shuffle benchmark cases to function.
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to
 *      "benchmarks/StreamingShufflePerformanceBenchmark-results.txt".
 * }}}
 */
object StreamingShufflePerformanceBenchmark extends BenchmarkBase {

  // Number of elements to process in each benchmark iteration.
  // For CI environments use 1M elements; for full production benchmarks increase to
  // match the AAP target of 10GB+ datasets by adjusting this constant.
  private val NUM_ELEMENTS = 1000000L

  // Number of shuffle partitions — matches AAP target of 100+ partitions.
  private val NUM_PARTITIONS = 100

  // Minimum number of timed iterations per benchmark case (excludes warmup).
  private val NUM_ITERATIONS = 3

  /**
   * Creates a base SparkConf suitable for benchmark execution.
   * Disables the Spark UI to eliminate overhead and sets local mode with 4 threads
   * to simulate a realistic multi-core executor environment.
   *
   * @param appName descriptive application name for this benchmark case
   * @param shuffleManager the shuffle manager short name ("sort" or "streaming")
   * @return configured SparkConf ready for SparkContext construction
   */
  private def createBaseConf(appName: String, shuffleManager: String): SparkConf = {
    new SparkConf(false)
      .setMaster("local[4]")
      .setAppName(appName)
      .set("spark.shuffle.manager", shuffleManager)
      .set("spark.ui.enabled", "false")
  }

  /**
   * Runs all benchmark suites comparing sort-based and streaming shuffle performance.
   *
   * Three benchmark suites are executed:
   *  1. groupByKey comparison (sort vs streaming)
   *  2. reduceByKey comparison (sort vs streaming)
   *  3. Buffer size impact analysis for streaming shuffle (10%, 20%, 30%, 40%)
   *
   * Each benchmark case creates an isolated SparkContext that is properly shut down
   * after execution to prevent resource leaks between cases.
   *
   * @param mainArgs command-line arguments (currently unused but required by BenchmarkBase)
   */
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmark("Sort-Based vs Streaming Shuffle: groupByKey") {
      val benchmark = new Benchmark(
        "Shuffle Performance (groupByKey)",
        NUM_ELEMENTS,
        NUM_ITERATIONS,
        output = output)

      // Baseline: sort-based shuffle with groupByKey.
      // This case establishes the performance baseline using the default SortShuffleManager.
      benchmark.addCase("Sort-based shuffle (groupByKey)") { _ =>
        val conf = createBaseConf("sort-shuffle-groupByKey-benchmark", "sort")
        val sc = new SparkContext(conf)
        try {
          val data = sc.parallelize(1L to NUM_ELEMENTS, NUM_PARTITIONS)
          data.map(x => (x % NUM_PARTITIONS, x)).groupByKey(NUM_PARTITIONS).count()
        } finally {
          sc.stop()
        }
      }

      // Streaming shuffle with groupByKey.
      // This case uses the StreamingShuffleManager (loaded via reflection) to measure
      // the streaming data pipeline's latency improvement over the sort-based baseline.
      benchmark.addCase("Streaming shuffle (groupByKey)") { _ =>
        val conf = createBaseConf("streaming-shuffle-groupByKey-benchmark", "streaming")
          .set("spark.shuffle.streaming.enabled", "true")
        val sc = new SparkContext(conf)
        try {
          val data = sc.parallelize(1L to NUM_ELEMENTS, NUM_PARTITIONS)
          data.map(x => (x % NUM_PARTITIONS, x)).groupByKey(NUM_PARTITIONS).count()
        } finally {
          sc.stop()
        }
      }

      benchmark.run()
    }

    runBenchmark("Sort-Based vs Streaming Shuffle: reduceByKey") {
      val benchmark = new Benchmark(
        "Shuffle Performance (reduceByKey)",
        NUM_ELEMENTS,
        NUM_ITERATIONS,
        output = output)

      // Baseline: sort-based shuffle with reduceByKey.
      // reduceByKey performs map-side combine before shuffle, which exercises a different
      // code path than groupByKey and may show different streaming shuffle benefits.
      benchmark.addCase("Sort-based shuffle (reduceByKey)") { _ =>
        val conf = createBaseConf("sort-shuffle-reduceByKey-benchmark", "sort")
        val sc = new SparkContext(conf)
        try {
          val data = sc.parallelize(1L to NUM_ELEMENTS, NUM_PARTITIONS)
          data.map(x => (x % NUM_PARTITIONS, x)).reduceByKey(_ + _, NUM_PARTITIONS).count()
        } finally {
          sc.stop()
        }
      }

      // Streaming shuffle with reduceByKey.
      // Measures streaming pipeline benefit when map-side aggregation reduces shuffle volume.
      benchmark.addCase("Streaming shuffle (reduceByKey)") { _ =>
        val conf = createBaseConf("streaming-shuffle-reduceByKey-benchmark", "streaming")
          .set("spark.shuffle.streaming.enabled", "true")
        val sc = new SparkContext(conf)
        try {
          val data = sc.parallelize(1L to NUM_ELEMENTS, NUM_PARTITIONS)
          data.map(x => (x % NUM_PARTITIONS, x)).reduceByKey(_ + _, NUM_PARTITIONS).count()
        } finally {
          sc.stop()
        }
      }

      benchmark.run()
    }

    runBenchmark("Streaming Shuffle: Memory Pressure Configuration") {
      val benchmark = new Benchmark(
        "Buffer Size Impact",
        NUM_ELEMENTS,
        NUM_ITERATIONS,
        output = output)

      // Test the impact of different buffer size percentages on streaming shuffle performance.
      // The buffer size (spark.shuffle.streaming.bufferSizePercent) controls what fraction
      // of executor memory is allocated for streaming buffers. Higher values reduce spill
      // frequency but increase memory pressure; lower values trigger more spills but leave
      // more memory for computation.
      //
      // AAP specifies: configurable 1-50%, default 20%.
      Seq(10, 20, 30, 40).foreach { bufferPct =>
        benchmark.addCase(s"Streaming shuffle (bufferSizePercent=$bufferPct%)") { _ =>
          val conf = createBaseConf(s"streaming-buffer-$bufferPct-benchmark", "streaming")
            .set("spark.shuffle.streaming.enabled", "true")
            .set("spark.shuffle.streaming.bufferSizePercent", bufferPct.toString)
          val sc = new SparkContext(conf)
          try {
            val data = sc.parallelize(1L to NUM_ELEMENTS, NUM_PARTITIONS)
            data.map(x => (x % NUM_PARTITIONS, x)).groupByKey(NUM_PARTITIONS).count()
          } finally {
            sc.stop()
          }
        }
      }

      benchmark.run()
    }
  }
}
