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
 * Consumer acknowledgment message for streaming shuffle buffer reclamation.
 * Supports partial and complete acknowledgments.
 *
 * This message is sent by shuffle consumers to producers to indicate how much
 * data has been successfully consumed and processed. Producers use this information
 * to reclaim buffer memory and manage backpressure in the streaming shuffle protocol.
 */
public final class StreamingShuffleAcknowledgment extends AbstractMessage {
  /** The shuffle ID this acknowledgment is for. */
  public final long shuffleId;

  /** The map task ID (producer) this acknowledgment is for. */
  public final int mapId;

  /** The partition ID this acknowledgment is for. */
  public final int partitionId;

  /** The offset up to which data has been consumed (in bytes). */
  public final long consumedOffset;

  /** Whether this is a complete acknowledgment (true) or partial (false). */
  public final boolean complete;

  /**
   * Constructs a new StreamingShuffleAcknowledgment.
   *
   * @param shuffleId The shuffle ID
   * @param mapId The map task ID (producer)
   * @param partitionId The partition ID
   * @param consumedOffset The offset up to which data has been consumed (in bytes)
   * @param complete Whether this acknowledgment indicates complete consumption
   */
  public StreamingShuffleAcknowledgment(
      long shuffleId,
      int mapId,
      int partitionId,
      long consumedOffset,
      boolean complete) {
    super(); // Control message with no body
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.partitionId = partitionId;
    this.consumedOffset = consumedOffset;
    this.complete = complete;
  }

  @Override
  public Message.Type type() {
    return Type.StreamingShuffleAck;
  }

  @Override
  public int encodedLength() {
    // 8 bytes (long) + 4 bytes (int) + 4 bytes (int) + 8 bytes (long) + 1 byte (boolean) = 25 bytes
    return 8 + 4 + 4 + 8 + 1;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(shuffleId);
    buf.writeInt(mapId);
    buf.writeInt(partitionId);
    buf.writeLong(consumedOffset);
    buf.writeBoolean(complete);
  }

  /**
   * Decodes a StreamingShuffleAcknowledgment from the given ByteBuf.
   *
   * @param buf The buffer to decode from
   * @return The decoded StreamingShuffleAcknowledgment
   */
  public static StreamingShuffleAcknowledgment decode(ByteBuf buf) {
    long shuffleId = buf.readLong();
    int mapId = buf.readInt();
    int partitionId = buf.readInt();
    long consumedOffset = buf.readLong();
    boolean complete = buf.readBoolean();
    return new StreamingShuffleAcknowledgment(
        shuffleId, mapId, partitionId, consumedOffset, complete);
  }

  @Override
  public int hashCode() {
    return Objects.hash(shuffleId, mapId, partitionId, consumedOffset, complete);
  }

  /**
   * Override the AbstractMessage equals method to delegate to the proper equals(Object) method.
   * This prevents method resolution ambiguity when calling equals on typed references.
   */
  protected boolean equals(AbstractMessage other) {
    return equals((Object) other);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof StreamingShuffleAcknowledgment)) {
      return false;
    }
    StreamingShuffleAcknowledgment o = (StreamingShuffleAcknowledgment) other;
    return shuffleId == o.shuffleId &&
           mapId == o.mapId &&
           partitionId == o.partitionId &&
           consumedOffset == o.consumedOffset &&
           complete == o.complete;
  }

  @Override
  public String toString() {
    return "StreamingShuffleAcknowledgment[shuffleId=" + shuffleId +
           ",mapId=" + mapId +
           ",partitionId=" + partitionId +
           ",consumedOffset=" + consumedOffset +
           ",complete=" + complete + "]";
  }
}
