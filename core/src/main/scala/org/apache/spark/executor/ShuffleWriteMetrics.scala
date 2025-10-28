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

package org.apache.spark.executor

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.util.LongAccumulator


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about writing shuffle data.
 * Operations are not thread-safe.
 */
@DeveloperApi
class ShuffleWriteMetrics private[spark] () extends ShuffleWriteMetricsReporter with Serializable {
  private[executor] val _bytesWritten = new LongAccumulator
  private[executor] val _recordsWritten = new LongAccumulator
  private[executor] val _writeTime = new LongAccumulator
  private[executor] val _bufferUtilizationPercent = new LongAccumulator
  private[executor] val _spillCount = new LongAccumulator
  private[executor] val _backpressureEventCount = new LongAccumulator

  /**
   * Number of bytes written for the shuffle by this task.
   */
  def bytesWritten: Long = _bytesWritten.sum

  /**
   * Total number of records written to the shuffle by this task.
   */
  def recordsWritten: Long = _recordsWritten.sum

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds.
   */
  def writeTime: Long = _writeTime.sum

  /**
   * Current buffer utilization percentage for streaming shuffle.
   * Represents the percentage of allocated executor memory buffers currently in use.
   * This is a gauge metric that is updated in real-time during streaming shuffle operations.
   */
  def bufferUtilizationPercent: Long = _bufferUtilizationPercent.sum

  /**
   * Number of times streaming shuffle buffers were spilled to disk.
   * Incremented each time the buffer utilization exceeds the configured spill threshold
   * (default 80%) and data is written to disk to free memory.
   */
  def spillCount: Long = _spillCount.sum

  /**
   * Number of backpressure events during streaming shuffle.
   * Tracks how many times the consumer could not keep up with producer rate,
   * triggering flow control mechanisms like rate limiting or additional spilling.
   */
  def backpressureEventCount: Long = _backpressureEventCount.sum

  private[spark] override def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
  private[spark] override def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
  private[spark] override def incWriteTime(v: Long): Unit = _writeTime.add(v)
  private[spark] def incBufferUtilization(v: Long): Unit = _bufferUtilizationPercent.setValue(v)
  private[spark] def incSpillCount(v: Long): Unit = _spillCount.add(v)
  private[spark] def incBackpressureEvents(v: Long): Unit = _backpressureEventCount.add(v)
  private[spark] override def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }
  private[spark] override def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.setValue(recordsWritten - v)
  }
}
