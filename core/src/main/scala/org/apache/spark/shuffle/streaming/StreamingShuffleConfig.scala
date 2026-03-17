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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * Centralized configuration constants and validation for streaming shuffle parameters.
 *
 * All configuration keys follow the spark.shuffle.streaming.* naming convention.
 * The streaming shuffle feature is opt-in only (disabled by default) and requires
 * executor restart for configuration changes (no dynamic reconfiguration in v1).
 *
 * Coexistence: These configuration keys are independent of sort-based shuffle
 * configuration. When streaming is disabled (the default), these parameters
 * have no effect on the sort-based shuffle path.
 */
private[spark] object StreamingShuffleConfig extends Logging {

  // ========== Configuration Key Constants ==========

  /** Whether streaming shuffle is enabled. Opt-in only, default false. */
  val STREAMING_ENABLED_KEY: String = "spark.shuffle.streaming.enabled"
  val STREAMING_ENABLED_DEFAULT: Boolean = false

  /**
   * Percentage of executor memory to allocate for streaming shuffle buffers.
   * Valid range: [1, 50], default: 20.
   * Per-partition buffer size formula: (executorMemory * bufferPercent) / numPartitions
   */
  val BUFFER_SIZE_PERCENT_KEY: String = "spark.shuffle.streaming.bufferSizePercent"
  val BUFFER_SIZE_PERCENT_DEFAULT: Int = 20
  val BUFFER_SIZE_PERCENT_MIN: Int = 1
  val BUFFER_SIZE_PERCENT_MAX: Int = 50

  /**
   * Buffer occupancy percentage threshold for triggering automatic disk spill.
   * Valid range: [50, 95], default: 80.
   * When buffer occupancy exceeds this threshold, MemorySpillManager triggers
   * automatic disk spill via LRU partition eviction.
   */
  val SPILL_THRESHOLD_KEY: String = "spark.shuffle.streaming.spillThreshold"
  val SPILL_THRESHOLD_DEFAULT: Int = 80
  val SPILL_THRESHOLD_MIN: Int = 50
  val SPILL_THRESHOLD_MAX: Int = 95

  /**
   * Maximum bandwidth in MB/s for per-executor rate limiting. 0 means unlimited.
   * Per-executor token bucket refill rate: maxBandwidthMBps / numConcurrentShuffles
   */
  val MAX_BANDWIDTH_MBPS_KEY: String = "spark.shuffle.streaming.maxBandwidthMBps"
  val MAX_BANDWIDTH_MBPS_DEFAULT: Int = 0

  /**
   * Whether to enable debug logging for streaming shuffle.
   * Default false. When enabled, detailed operational logs are emitted.
   * Log volume is capped at less than 10MB/hour per executor.
   */
  val DEBUG_ENABLED_KEY: String = "spark.shuffle.streaming.debug"
  val DEBUG_ENABLED_DEFAULT: Boolean = false

  // ========== Helper Methods ==========

  /** Check if streaming shuffle is enabled in the given configuration. */
  def isStreamingEnabled(conf: SparkConf): Boolean = {
    conf.getBoolean(STREAMING_ENABLED_KEY, STREAMING_ENABLED_DEFAULT)
  }

  /**
   * Get buffer size percentage with range validation [1, 50].
   * Returns the configured value if within valid range, otherwise logs a warning
   * and returns the default value.
   */
  def getBufferSizePercent(conf: SparkConf): Int = {
    val value = conf.getInt(BUFFER_SIZE_PERCENT_KEY, BUFFER_SIZE_PERCENT_DEFAULT)
    if (value < BUFFER_SIZE_PERCENT_MIN || value > BUFFER_SIZE_PERCENT_MAX) {
      logWarning(s"$BUFFER_SIZE_PERCENT_KEY=$value is outside valid range " +
        s"[$BUFFER_SIZE_PERCENT_MIN, $BUFFER_SIZE_PERCENT_MAX], " +
        s"using default $BUFFER_SIZE_PERCENT_DEFAULT")
      BUFFER_SIZE_PERCENT_DEFAULT
    } else {
      value
    }
  }

  /**
   * Get spill threshold percentage with range validation [50, 95].
   * Returns the configured value if within valid range, otherwise logs a warning
   * and returns the default value.
   */
  def getSpillThreshold(conf: SparkConf): Int = {
    val value = conf.getInt(SPILL_THRESHOLD_KEY, SPILL_THRESHOLD_DEFAULT)
    if (value < SPILL_THRESHOLD_MIN || value > SPILL_THRESHOLD_MAX) {
      logWarning(s"$SPILL_THRESHOLD_KEY=$value is outside valid range " +
        s"[$SPILL_THRESHOLD_MIN, $SPILL_THRESHOLD_MAX], " +
        s"using default $SPILL_THRESHOLD_DEFAULT")
      SPILL_THRESHOLD_DEFAULT
    } else {
      value
    }
  }

  /** Get maximum bandwidth in MB/s. 0 means unlimited. */
  def getMaxBandwidthMBps(conf: SparkConf): Int = {
    conf.getInt(MAX_BANDWIDTH_MBPS_KEY, MAX_BANDWIDTH_MBPS_DEFAULT)
  }

  /** Check if debug logging is enabled for streaming shuffle. */
  def isDebugEnabled(conf: SparkConf): Boolean = {
    conf.getBoolean(DEBUG_ENABLED_KEY, DEBUG_ENABLED_DEFAULT)
  }
}
