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

import java.io.ByteArrayOutputStream

import org.mockito.Mockito.{mock, when}

import org.apache.spark.{Partitioner, ShuffleDependency, SparkConf, SparkContext}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils

/**
 * Benchmark for the streaming-shuffle in-process data plane implemented at this checkpoint. It runs
 * a complete producer->consumer round-trip -- a real [[StreamingShuffleWriter]] serializes and
 * emits blocks through the in-process [[StreamingBlockExchange]], and a real
 * [[StreamingShuffleReader]] per reduce partition validates (CRC32C), acknowledges, and
 * deserializes them -- and compares it against the irreducible serialize+partition cost any shuffle
 * engine must pay. The Relative column therefore isolates the streaming data-path overhead.
 *
 * Scope note (honest to the checkpoint): the `spark.shuffle.manager=streaming` selector and the
 * distributed `TransportContext`-backed path are NOT part of this checkpoint, so this is NOT a
 * sort-vs-streaming end-to-end comparison; it measures the implemented in-process engine. The
 * serialize+partition case is the baseline every shuffle (sort or streaming) shares.
 *
 * To run this benchmark (optionally set SPARK_GENERATE_BENCHMARK_FILES=1 to write a result file):
 * {{{
 *   build/sbt "core/Test/runMain \
 *     org.apache.spark.shuffle.streaming.StreamingShufflePerformanceBenchmark"
 * }}}
 */
object StreamingShufflePerformanceBenchmark extends BenchmarkBase {

  private val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("StreamingShufflePerformanceBenchmark")
    // Opt in so buffer accounting (and any spill) is exercised on the streaming data path.
    .set("spark.shuffle.streaming.enabled", "true")

  private val serializer = new JavaSerializer(conf)

  private def partitionerFor(n: Int): Partitioner = new Partitioner {
    override def numPartitions: Int = n
    override def getPartition(key: Any): Int = Utils.nonNegativeMod(key.hashCode, n)
  }

  // The irreducible cost shared by every shuffle: route each record to its reduce partition and
  // serialize it into that partition's block stream through the EXISTING SerializerManager.
  private def serializeAndPartition(
      sc: SparkContext,
      records: Array[(Int, Int)],
      numPartitions: Int): Long = {
    val partitioner = partitionerFor(numPartitions)
    val streams = Array.fill(numPartitions)(new ByteArrayOutputStream())
    val serInstance = serializer.newInstance()
    val wrapped = (0 until numPartitions).map { p =>
      val blockId = ShuffleBlockId(0, 0L, p)
      serInstance.serializeStream(sc.env.serializerManager.wrapStream(blockId, streams(p)))
    }.toArray
    var i = 0
    while (i < records.length) {
      val record = records(i)
      wrapped(partitioner.getPartition(record._1)).writeKey(record._1).writeValue(record._2)
      i += 1
    }
    var bytes = 0L
    var p = 0
    while (p < numPartitions) {
      wrapped(p).close()
      bytes += streams(p).size()
      p += 1
    }
    bytes
  }

  // A full streaming round-trip: a real writer emits blocks through the exchange and a real reader
  // per reduce partition consumes them back into records. Fresh collaborators per invocation keep
  // each round-trip independent; everything is torn down so no poller/heartbeat thread leaks.
  private def streamingRoundTrip(
      sc: SparkContext,
      records: Array[(Int, Int)],
      numPartitions: Int,
      writerDep: ShuffleDependency[Int, Int, Int],
      readerDep: ShuffleDependency[Int, Int, Int]): Long = {
    val source = new StreamingShuffleSource
    val resolver = new IndexShuffleBlockResolver(conf)
    val spill = new MemorySpillManager(conf, sc.env.memoryManager, resolver, source)
    val exchange = new StreamingBlockExchange(conf, spill)
    val backpressure = new BackpressureProtocol(conf, 1, source)
    try {
      val handle = new StreamingShuffleHandle[Int, Int, Int](0, writerDep)
      val wctx = MemoryTestingUtils.fakeTaskContext(sc.env)
      val writer = new StreamingShuffleWriter[Int, Int](
        handle, 0L, wctx, wctx.taskMetrics().shuffleWriteMetrics, conf,
        backpressure, spill, exchange, source)
      writer.write(records.iterator)
      writer.stop(success = true)

      var consumed = 0L
      var p = 0
      while (p < numPartitions) {
        val rHandle = new StreamingShuffleHandle[Int, Int, Int](0, readerDep)
        val rctx = MemoryTestingUtils.fakeTaskContext(sc.env)
        val reader = new StreamingShuffleReader[Int, Int](
          rHandle, 0, 1, p, p + 1, rctx, rctx.taskMetrics().createTempShuffleReadMetrics(),
          conf, backpressure, source, exchange)
        val it = reader.read()
        while (it.hasNext) {
          it.next()
          consumed += 1L
        }
        p += 1
      }
      consumed
    } finally {
      backpressure.stop()
      exchange.stop()
      spill.stop()
      resolver.stop()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val sc = new SparkContext(conf)
    try {
      runBenchmark("Streaming shuffle in-process data path round-trip") {
        val numRecords = 100000
        val records = (0 until numRecords).map(i => (i, i)).toArray
        Seq(8, 64, 200).foreach { numPartitions =>
          // Stable mocked dependencies reused across iterations so Mockito setup is not timed.
          val writerDep = mock(classOf[ShuffleDependency[Int, Int, Int]])
          when(writerDep.shuffleId).thenReturn(0)
          when(writerDep.partitioner).thenReturn(partitionerFor(numPartitions))
          when(writerDep.serializer).thenReturn(serializer)
          val readerDep = mock(classOf[ShuffleDependency[Int, Int, Int]])
          when(readerDep.serializer).thenReturn(serializer)
          when(readerDep.aggregator).thenReturn(None)
          when(readerDep.keyOrdering).thenReturn(None)

          val benchmark = new Benchmark(
            s"$numRecords records over $numPartitions partitions", numRecords, output = output)
          benchmark.addCase("baseline: serialize + partition") { _ =>
            val bytes = serializeAndPartition(sc, records, numPartitions)
            if (bytes < 0L) throw new IllegalStateException("unreachable")
          }
          benchmark.addCase("streaming: write -> exchange -> read") { _ =>
            val consumed = streamingRoundTrip(sc, records, numPartitions, writerDep, readerDep)
            if (consumed != numRecords.toLong) {
              throw new IllegalStateException(s"round-trip lost records: $consumed != $numRecords")
            }
          }
          benchmark.run()
        }
      }
    } finally {
      sc.stop()
    }
  }
}
