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

package org.apache.spark.network.protocol;

import java.util.Objects;

import io.netty.buffer.ByteBuf;

/**
 * Liveness detection message for streaming shuffle producer-consumer health monitoring.
 * Sent every 10 seconds with 5-second timeout.
 * 
 * This control message enables the streaming shuffle backpressure protocol to detect
 * producer and consumer failures, allowing for timely partial read invalidation and
 * upstream task recomputation when needed.
 */
public final class StreamingShuffleHeartbeat extends AbstractMessage {
  /** Unique identifier for the consumer sending the heartbeat */
  public final String consumerId;
  
  /** Timestamp (milliseconds since epoch) when heartbeat was generated */
  public final long timestamp;

  /**
   * Constructs a new StreamingShuffleHeartbeat message.
   * 
   * @param consumerId Unique identifier for the consumer
   * @param timestamp Timestamp in milliseconds since epoch
   */
  public StreamingShuffleHeartbeat(String consumerId, long timestamp) {
    super();  // Control message with no body
    this.consumerId = consumerId;
    this.timestamp = timestamp;
  }

  @Override
  public Message.Type type() {
    return Type.StreamingShuffleHeartbeat;
  }

  @Override
  public int encodedLength() {
    // String encoding: 4 bytes for length + UTF-8 bytes
    // Long encoding: 8 bytes
    return Encoders.Strings.encodedLength(consumerId) + 8;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, consumerId);
    buf.writeLong(timestamp);
  }

  /**
   * Decodes a StreamingShuffleHeartbeat from the given ByteBuf.
   * 
   * @param buf ByteBuf containing encoded message data
   * @return Decoded StreamingShuffleHeartbeat instance
   */
  public static StreamingShuffleHeartbeat decode(ByteBuf buf) {
    String consumerId = Encoders.Strings.decode(buf);
    long timestamp = buf.readLong();
    return new StreamingShuffleHeartbeat(consumerId, timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consumerId, timestamp);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StreamingShuffleHeartbeat o) {
      return consumerId.equals(o.consumerId) && timestamp == o.timestamp;
    }
    return false;
  }

  @Override
  public String toString() {
    return "StreamingShuffleHeartbeat[consumerId=" + consumerId + 
           ",timestamp=" + timestamp + "]";
  }
}
