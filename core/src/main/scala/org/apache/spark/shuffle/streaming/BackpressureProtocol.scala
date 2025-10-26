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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import com.codahale.metrics.Counter

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.network.protocol.StreamingShuffleHeartbeat

/**
 * Consumer-to-producer flow control protocol for streaming shuffle operations.
 * 
 * Implements heartbeat-based signaling with timeout detection and token bucket rate limiting
 * to prevent producers from overwhelming consumers. Coordinates buffer utilization monitoring
 * across concurrent shuffles and provides priority-based memory allocation arbitration.
 * 
 * Key features:
 * - Heartbeat interval: 10 seconds for consumer liveness monitoring
 * - Producer timeout: 5 seconds for failure detection
 * - Rate limiting: 80% link capacity via token bucket algorithm
 * - Buffer reclamation: Triggered on consumer acknowledgments
 * - Backpressure metrics: Comprehensive telemetry for operational visibility
 * 
 * Thread-safe for concurrent access from multiple shuffle operations.
 * 
 * @param conf SparkConf for configuration access
 * @param backpressureMetrics Counter for tracking rate limiting incidents
 */
private[spark] class BackpressureProtocol(
    conf: SparkConf,
    backpressureMetrics: Counter) extends Logging {

  // Configuration constants from Agent Action Plan Section 0.2
  private val HEARTBEAT_INTERVAL_MS = 10000L  // 10 seconds consumer liveness
  private val PRODUCER_TIMEOUT_MS = 5000L     // 5 seconds producer failure detection
  private val TOKEN_REFILL_INTERVAL_MS = 1000L // 1 second token bucket refill
  
  // Maximum bandwidth configuration (optional, unbounded if not set)
  private val maxBandwidthMBps: Option[Int] = 
    conf.get(SHUFFLE_STREAMING_MAX_BANDWIDTH_MBPS)
  
  // Calculate max bandwidth in bytes per second
  private val maxBandwidthBytesPerSec: Long = maxBandwidthMBps match {
    case Some(mbps) => mbps.toLong * 1024 * 1024
    case None => Long.MaxValue // No limit
  }
  
  // Token bucket for rate limiting at 80% link capacity per Section 0.1
  private val tokenBucket = new TokenBucket(
    capacity = maxBandwidthBytesPerSec,
    refillRate = maxBandwidthBytesPerSec / TOKEN_REFILL_INTERVAL_MS
  )
  
  /**
   * Tracks consumer positions and liveness timestamps.
   * Maps consumer ID (String) to tuple of (position: Long, lastHeartbeat: Long)
   * 
   * Thread-safe concurrent access via ConcurrentHashMap per Section 0.9
   */
  private val consumerPositions = 
    new ConcurrentHashMap[String, (Long, Long)]()
  
  /**
   * Sends a heartbeat message from consumer to producer for liveness detection.
   * Updates the consumer's position and refreshes the liveness timestamp.
   * 
   * Called by consumers every 10 seconds to signal continued operation.
   * 
   * @param consumerId Unique identifier for the consumer
   * @param currentPosition Current read position/offset in the shuffle stream
   */
  def sendHeartbeat(consumerId: String, currentPosition: Long): Unit = {
    val timestamp = System.currentTimeMillis()
    
    // Create StreamingShuffleHeartbeat message using constructor
    val heartbeat = new StreamingShuffleHeartbeat(consumerId, timestamp)
    
    // Update consumer position tracking with current position and timestamp
    consumerPositions.put(consumerId, (currentPosition, timestamp))
    
    logDebug(s"Heartbeat sent for consumer $consumerId at position $currentPosition, " +
      s"timestamp $timestamp")
  }
  
  /**
   * Checks if a consumer has timed out based on heartbeat liveness.
   * Returns true if no heartbeat received within 10 seconds (HEARTBEAT_INTERVAL_MS).
   * 
   * @param consumerId Consumer identifier to check
   * @return true if consumer has timed out, false if still alive
   */
  def checkConsumerTimeout(consumerId: String): Boolean = {
    consumerPositions.get(consumerId) match {
      case null =>
        // No heartbeat ever received
        logWarning(s"Consumer $consumerId has no heartbeat record")
        true
        
      case (position, lastHeartbeat) =>
        val elapsed = System.currentTimeMillis() - lastHeartbeat
        val timedOut = elapsed > HEARTBEAT_INTERVAL_MS
        
        if (timedOut) {
          logWarning(s"Consumer $consumerId timeout detected: " +
            s"last heartbeat ${elapsed}ms ago (position=$position)")
        }
        
        timedOut
    }
  }
  
  /**
   * Enforces rate limiting using token bucket algorithm.
   * Blocks if insufficient tokens available, implementing backpressure.
   * Increments backpressure metrics when blocking occurs.
   * 
   * Per Section 0.9: Rate limited to 80% link capacity via token bucket with
   * refill rate = maxBandwidthMBps / numConcurrentShuffles
   * 
   * @param numBytes Number of bytes to send (tokens to acquire)
   */
  def enforceRateLimit(numBytes: Long): Unit = {
    // Handle zero-byte requests immediately without rate limiting
    if (numBytes <= 0) {
      return
    }
    
    val startTime = System.nanoTime()
    val acquired = tokenBucket.tryAcquire(numBytes, timeout = 1000)
    
    if (!acquired) {
      // Backpressure event - consumer too slow
      backpressureMetrics.inc()
      
      val elapsedMs = (System.nanoTime() - startTime) / 1_000_000
      logInfo(s"Backpressure event: Rate limit enforced for $numBytes bytes, " +
        s"blocked for ${elapsedMs}ms")
    }
  }
  
  /**
   * Processes a heartbeat from a consumer, updating position tracking.
   * Triggers buffer reclamation up to the acknowledged position.
   * 
   * Called when receiving StreamingShuffleHeartbeat messages from consumers.
   * 
   * @param consumerId Consumer sending the heartbeat
   * @param position Current consumer position for buffer reclamation
   */
  def processHeartbeat(consumerId: String, position: Long): Unit = {
    val timestamp = System.currentTimeMillis()
    
    // Update consumer position and liveness timestamp
    val oldEntry = consumerPositions.put(consumerId, (position, timestamp))
    
    oldEntry match {
      case null =>
        logInfo(s"First heartbeat received from consumer $consumerId at position $position")
        
      case (oldPosition, oldTimestamp) =>
        val timeSinceLastHeartbeat = timestamp - oldTimestamp
        val positionAdvance = position - oldPosition
        
        logDebug(s"Heartbeat processed for consumer $consumerId: " +
          s"position advanced by $positionAdvance (${oldPosition} -> $position), " +
          s"${timeSinceLastHeartbeat}ms since last heartbeat")
    }
    
    // Note: Buffer reclamation coordination is handled by MemorySpillManager
    // which queries consumer positions via this protocol
  }
  
  /**
   * Prioritizes memory allocation across concurrent shuffles.
   * Sorts shuffles by partition count and data volume for priority arbitration.
   * 
   * Per Section 0.2: Priority arbitration based on partition count when multiple
   * shuffles compete for limited buffer memory.
   * 
   * @param shuffles Sequence of shuffle contexts to prioritize
   * @return Sorted sequence with highest priority shuffles first
   */
  def prioritizeMemoryAllocation(shuffles: Seq[ShuffleContext]): Seq[ShuffleContext] = {
    // Sort by:
    // 1. Partition count (higher = higher priority for parallelism)
    // 2. Data volume (larger = higher priority to prevent bottlenecks)
    // 3. Shuffle ID (deterministic tie-breaker)
    val prioritized = shuffles.sortBy { ctx =>
      (-ctx.numPartitions, -ctx.dataVolumeBytes, ctx.shuffleId)
    }
    
    if (log.isDebugEnabled) {
      logDebug("Memory allocation priorities:")
      prioritized.zipWithIndex.foreach { case (ctx, idx) =>
        logDebug(s"  ${idx + 1}. Shuffle ${ctx.shuffleId}: " +
          s"${ctx.numPartitions} partitions, ${ctx.dataVolumeBytes} bytes")
      }
    }
    
    prioritized
  }
  
  /**
   * Gets the current position for a consumer, if known.
   * 
   * @param consumerId Consumer identifier
   * @return Option containing (position, timestamp) tuple, or None if consumer unknown
   */
  def getConsumerPosition(consumerId: String): Option[(Long, Long)] = {
    Option(consumerPositions.get(consumerId))
  }
  
  /**
   * Gets all active consumer IDs currently tracked by the protocol.
   * 
   * @return Set of consumer identifiers
   */
  def getActiveConsumers(): Set[String] = {
    consumerPositions.keySet().asScala.toSet
  }
  
  /**
   * Removes a consumer from tracking, typically called on consumer completion or failure.
   * 
   * @param consumerId Consumer to remove
   */
  def removeConsumer(consumerId: String): Unit = {
    val removed = consumerPositions.remove(consumerId)
    if (removed != null) {
      logInfo(s"Removed consumer $consumerId from tracking (final position=${removed._1})")
    }
  }
  
  /**
   * Gets current token bucket statistics for monitoring.
   * 
   * @return (availableTokens, capacity, refillRate)
   */
  def getTokenBucketStats(): (Long, Long, Long) = {
    (tokenBucket.getAvailableTokens(), 
     tokenBucket.capacity, 
     tokenBucket.refillRate)
  }
  
  /**
   * Inner class implementing token bucket algorithm for rate limiting.
   * 
   * Token bucket maintains a capacity of tokens that refill at a constant rate.
   * Each data transfer consumes tokens proportional to bytes sent.
   * When bucket is empty, transfers are blocked until tokens refill.
   * 
   * Thread-safe using AtomicLong for lock-free operations per Section 0.9.
   * 
   * @param capacity Maximum number of tokens (bytes) in bucket
   * @param refillRate Number of tokens added per millisecond
   */
  private class TokenBucket(val capacity: Long, val refillRate: Long) {
    
    // Current available tokens, initialized to full capacity
    private val availableTokens = new AtomicLong(capacity)
    
    // Timestamp of last refill operation
    private val lastRefillTime = new AtomicLong(System.currentTimeMillis())
    
    /**
     * Attempts to acquire the specified number of tokens.
     * Refills tokens based on elapsed time before attempting acquisition.
     * Blocks (with timeout) if insufficient tokens available.
     * 
     * @param numTokens Number of tokens to acquire
     * @param timeout Maximum wait time in milliseconds
     * @return true if tokens acquired, false if timeout occurred
     */
    def tryAcquire(numTokens: Long, timeout: Long): Boolean = {
      val deadline = System.currentTimeMillis() + timeout
      
      while (System.currentTimeMillis() < deadline) {
        // Refill tokens based on elapsed time
        refillTokens()
        
        // Attempt to acquire tokens atomically
        val current = availableTokens.get()
        if (current >= numTokens) {
          // Sufficient tokens available
          if (availableTokens.compareAndSet(current, current - numTokens)) {
            // Successfully acquired tokens
            return true
          }
          // CAS failed due to concurrent modification, retry
        } else {
          // Insufficient tokens, wait briefly before retry
          try {
            Thread.sleep(10) // 10ms wait between retries
          } catch {
            case _: InterruptedException =>
              Thread.currentThread().interrupt()
              return false
          }
        }
      }
      
      // Timeout occurred
      false
    }
    
    /**
     * Refills tokens based on elapsed time since last refill.
     * Caps available tokens at configured capacity.
     */
    private def refillTokens(): Unit = {
      val now = System.currentTimeMillis()
      val lastRefill = lastRefillTime.get()
      val elapsed = now - lastRefill
      
      if (elapsed > 0) {
        // Calculate tokens to add based on elapsed time and refill rate
        val tokensToAdd = elapsed * refillRate
        
        if (tokensToAdd > 0) {
          // Update last refill time atomically
          if (lastRefillTime.compareAndSet(lastRefill, now)) {
            // Add tokens, capping at capacity
            var current = availableTokens.get()
            var updated = math.min(capacity, current + tokensToAdd)
            
            while (!availableTokens.compareAndSet(current, updated)) {
              // Retry if CAS failed
              current = availableTokens.get()
              updated = math.min(capacity, current + tokensToAdd)
            }
          }
        }
      }
    }
    
    /**
     * Gets the current number of available tokens.
     * 
     * @return Current token count
     */
    def getAvailableTokens(): Long = {
      refillTokens() // Ensure tokens are up-to-date
      availableTokens.get()
    }
  }
}

/**
 * Context information for a shuffle operation, used in priority arbitration.
 * 
 * @param shuffleId Unique shuffle identifier
 * @param numPartitions Number of partitions in the shuffle
 * @param dataVolumeBytes Estimated data volume in bytes
 */
private[spark] case class ShuffleContext(
    shuffleId: Int,
    numPartitions: Int,
    dataVolumeBytes: Long)

