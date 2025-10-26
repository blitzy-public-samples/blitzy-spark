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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link StreamingShuffleAcknowledgment} and {@link StreamingShuffleHeartbeat}
 * protocol messages used in streaming shuffle backpressure and health monitoring.
 */
public class StreamingShuffleProtocolSuite {

  /**
   * Tests round-trip encoding/decoding for StreamingShuffleAcknowledgment with all fields.
   */
  @Test
  public void testStreamingShuffleAcknowledgmentEncodeDecode() {
    long shuffleId = 123L;
    int mapId = 456;
    int partitionId = 789;
    long consumedOffset = 1024L;
    boolean complete = true;

    StreamingShuffleAcknowledgment original = new StreamingShuffleAcknowledgment(
        shuffleId, mapId, partitionId, consumedOffset, complete);

    // Allocate buffer with exact encoded length
    ByteBuf buf = Unpooled.buffer(25);
    original.encode(buf);

    // Decode and verify equality
    StreamingShuffleAcknowledgment decoded = StreamingShuffleAcknowledgment.decode(buf);
    assertEquals(original, decoded);
    assertEquals(shuffleId, decoded.shuffleId);
    assertEquals(mapId, decoded.mapId);
    assertEquals(partitionId, decoded.partitionId);
    assertEquals(consumedOffset, decoded.consumedOffset);
    assertEquals(complete, decoded.complete);

    buf.release();
  }

  /**
   * Tests round-trip encoding/decoding for StreamingShuffleHeartbeat.
   */
  @Test
  public void testStreamingShuffleHeartbeatEncodeDecode() {
    String consumerId = "executor-1";
    long timestamp = System.currentTimeMillis();

    StreamingShuffleHeartbeat original = new StreamingShuffleHeartbeat(consumerId, timestamp);

    // Allocate buffer with calculated encoded length
    ByteBuf buf = Unpooled.buffer(original.encodedLength());
    original.encode(buf);

    // Decode and verify equality
    StreamingShuffleHeartbeat decoded = StreamingShuffleHeartbeat.decode(buf);
    assertEquals(original, decoded);
    assertEquals(consumerId, decoded.consumerId);
    assertEquals(timestamp, decoded.timestamp);

    buf.release();
  }

  /**
   * Tests StreamingShuffleAcknowledgment with complete flag variations.
   */
  @Test
  public void testStreamingShuffleAcknowledgmentCompleteFlag() {
    // Test with complete = true
    StreamingShuffleAcknowledgment completeAck = new StreamingShuffleAcknowledgment(
        100L, 10, 5, 2048L, true);
    ByteBuf buf1 = Unpooled.buffer(25);
    completeAck.encode(buf1);
    StreamingShuffleAcknowledgment decoded1 = StreamingShuffleAcknowledgment.decode(buf1);
    assertTrue(decoded1.complete);
    buf1.release();

    // Test with complete = false
    StreamingShuffleAcknowledgment partialAck = new StreamingShuffleAcknowledgment(
        100L, 10, 5, 1024L, false);
    ByteBuf buf2 = Unpooled.buffer(25);
    partialAck.encode(buf2);
    StreamingShuffleAcknowledgment decoded2 = StreamingShuffleAcknowledgment.decode(buf2);
    assertFalse(decoded2.complete);
    buf2.release();
  }

  /**
   * Tests StreamingShuffleHeartbeat with various timestamp values.
   */
  @Test
  public void testStreamingShuffleHeartbeatTimestamp() {
    String consumerId = "executor-test";

    // Test with timestamp = 0L
    StreamingShuffleHeartbeat heartbeat1 = new StreamingShuffleHeartbeat(consumerId, 0L);
    ByteBuf buf1 = Unpooled.buffer(heartbeat1.encodedLength());
    heartbeat1.encode(buf1);
    StreamingShuffleHeartbeat decoded1 = StreamingShuffleHeartbeat.decode(buf1);
    assertEquals(0L, decoded1.timestamp);
    buf1.release();

    // Test with timestamp = Long.MAX_VALUE
    StreamingShuffleHeartbeat heartbeat2 = new StreamingShuffleHeartbeat(consumerId, Long.MAX_VALUE);
    ByteBuf buf2 = Unpooled.buffer(heartbeat2.encodedLength());
    heartbeat2.encode(buf2);
    StreamingShuffleHeartbeat decoded2 = StreamingShuffleHeartbeat.decode(buf2);
    assertEquals(Long.MAX_VALUE, decoded2.timestamp);
    buf2.release();

    // Test with current timestamp
    long currentTime = System.currentTimeMillis();
    StreamingShuffleHeartbeat heartbeat3 = new StreamingShuffleHeartbeat(consumerId, currentTime);
    ByteBuf buf3 = Unpooled.buffer(heartbeat3.encodedLength());
    heartbeat3.encode(buf3);
    StreamingShuffleHeartbeat decoded3 = StreamingShuffleHeartbeat.decode(buf3);
    assertEquals(currentTime, decoded3.timestamp);
    buf3.release();
  }

  /**
   * Tests that StreamingShuffleAcknowledgment.encodedLength() returns exactly 25 bytes.
   */
  @Test
  public void testStreamingShuffleAcknowledgmentEncodedLength() {
    StreamingShuffleAcknowledgment ack = new StreamingShuffleAcknowledgment(
        1L, 2, 3, 4L, true);
    
    // Verify encoded length is exactly 25 bytes (8+4+4+8+1)
    assertEquals(25, ack.encodedLength());

    // Verify ByteBuf writerIndex after encode equals encodedLength
    ByteBuf buf = Unpooled.buffer(25);
    int writerIndexBefore = buf.writerIndex();
    ack.encode(buf);
    int writerIndexAfter = buf.writerIndex();
    assertEquals(25, writerIndexAfter - writerIndexBefore);
    buf.release();
  }

  /**
   * Tests that StreamingShuffleHeartbeat.encodedLength() matches expected calculation.
   */
  @Test
  public void testStreamingShuffleHeartbeatEncodedLength() {
    String consumerId = "executor-1";
    StreamingShuffleHeartbeat heartbeat = new StreamingShuffleHeartbeat(consumerId, 12345L);

    // Verify encoded length matches Encoders.Strings.encodedLength(consumerId) + 8
    int expectedLength = Encoders.Strings.encodedLength(consumerId) + 8;
    assertEquals(expectedLength, heartbeat.encodedLength());

    // Verify ByteBuf writerIndex after encode equals encodedLength
    ByteBuf buf = Unpooled.buffer(heartbeat.encodedLength());
    int writerIndexBefore = buf.writerIndex();
    heartbeat.encode(buf);
    int writerIndexAfter = buf.writerIndex();
    assertEquals(expectedLength, writerIndexAfter - writerIndexBefore);
    buf.release();
  }

  /**
   * Tests StreamingShuffleAcknowledgment with zero values for all fields.
   */
  @Test
  public void testStreamingShuffleAcknowledgmentZeroValues() {
    StreamingShuffleAcknowledgment ack = new StreamingShuffleAcknowledgment(
        0L, 0, 0, 0L, false);

    ByteBuf buf = Unpooled.buffer(25);
    ack.encode(buf);
    StreamingShuffleAcknowledgment decoded = StreamingShuffleAcknowledgment.decode(buf);

    assertEquals(0L, decoded.shuffleId);
    assertEquals(0, decoded.mapId);
    assertEquals(0, decoded.partitionId);
    assertEquals(0L, decoded.consumedOffset);
    assertFalse(decoded.complete);
    buf.release();
  }

  /**
   * Tests StreamingShuffleAcknowledgment with maximum values for all fields.
   */
  @Test
  public void testStreamingShuffleAcknowledgmentMaxValues() {
    StreamingShuffleAcknowledgment ack = new StreamingShuffleAcknowledgment(
        Long.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, true);

    ByteBuf buf = Unpooled.buffer(25);
    ack.encode(buf);
    StreamingShuffleAcknowledgment decoded = StreamingShuffleAcknowledgment.decode(buf);

    assertEquals(Long.MAX_VALUE, decoded.shuffleId);
    assertEquals(Integer.MAX_VALUE, decoded.mapId);
    assertEquals(Integer.MAX_VALUE, decoded.partitionId);
    assertEquals(Long.MAX_VALUE, decoded.consumedOffset);
    assertTrue(decoded.complete);
    buf.release();
  }

  /**
   * Tests StreamingShuffleHeartbeat with empty consumerId string.
   */
  @Test
  public void testStreamingShuffleHeartbeatEmptyConsumerId() {
    String emptyConsumerId = "";
    long timestamp = 99999L;

    StreamingShuffleHeartbeat heartbeat = new StreamingShuffleHeartbeat(emptyConsumerId, timestamp);

    ByteBuf buf = Unpooled.buffer(heartbeat.encodedLength());
    heartbeat.encode(buf);
    StreamingShuffleHeartbeat decoded = StreamingShuffleHeartbeat.decode(buf);

    assertEquals(emptyConsumerId, decoded.consumerId);
    assertEquals(timestamp, decoded.timestamp);
    buf.release();
  }

  /**
   * Tests StreamingShuffleHeartbeat with long consumerId string (1000+ characters).
   */
  @Test
  public void testStreamingShuffleHeartbeatLongConsumerId() {
    // Create a 1000+ character string
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      sb.append("x");
    }
    String longConsumerId = sb.toString();
    long timestamp = 55555L;

    StreamingShuffleHeartbeat heartbeat = new StreamingShuffleHeartbeat(longConsumerId, timestamp);

    ByteBuf buf = Unpooled.buffer(heartbeat.encodedLength());
    heartbeat.encode(buf);
    StreamingShuffleHeartbeat decoded = StreamingShuffleHeartbeat.decode(buf);

    assertEquals(longConsumerId, decoded.consumerId);
    assertEquals(timestamp, decoded.timestamp);
    assertEquals(1000, decoded.consumerId.length());
    buf.release();
  }

  /**
   * Tests that message type identifiers match the protocol specification.
   * StreamingShuffleAck should have id 13, StreamingShuffleHeartbeat should have id 14.
   */
  @Test
  public void testMessageTypeIdentifiers() {
    // Verify StreamingShuffleAck type id is 13
    assertEquals(13, Message.Type.StreamingShuffleAck.id());

    // Verify StreamingShuffleHeartbeat type id is 14
    assertEquals(14, Message.Type.StreamingShuffleHeartbeat.id());

    // Verify type() methods return correct types
    StreamingShuffleAcknowledgment ack = new StreamingShuffleAcknowledgment(
        1L, 2, 3, 4L, true);
    assertEquals(Message.Type.StreamingShuffleAck, ack.type());

    StreamingShuffleHeartbeat heartbeat = new StreamingShuffleHeartbeat("consumer", 12345L);
    assertEquals(Message.Type.StreamingShuffleHeartbeat, heartbeat.type());
  }

  /**
   * Tests equals() and hashCode() contracts for StreamingShuffleAcknowledgment.
   */
  @Test
  public void testStreamingShuffleAcknowledgmentEqualsAndHashCode() {
    StreamingShuffleAcknowledgment ack1 = new StreamingShuffleAcknowledgment(
        100L, 20, 30, 4096L, true);
    StreamingShuffleAcknowledgment ack2 = new StreamingShuffleAcknowledgment(
        100L, 20, 30, 4096L, true);
    StreamingShuffleAcknowledgment ack3 = new StreamingShuffleAcknowledgment(
        100L, 20, 30, 4096L, false);

    // Test equals() - identical objects should be equal
    assertEquals(ack1, ack2);
    assertTrue(ack1.equals(ack2));

    // Test hashCode() - equal objects must have equal hash codes
    assertEquals(ack1.hashCode(), ack2.hashCode());

    // Test equals() - different objects should not be equal
    assertNotEquals(ack1, ack3);
    assertFalse(ack1.equals(ack3));

    // Test equals() with null
    assertFalse(ack1.equals(null));

    // Test equals() with different type
    assertFalse(ack1.equals("not an acknowledgment"));
  }

  /**
   * Tests equals() and hashCode() contracts for StreamingShuffleHeartbeat.
   */
  @Test
  public void testStreamingShuffleHeartbeatEqualsAndHashCode() {
    StreamingShuffleHeartbeat heartbeat1 = new StreamingShuffleHeartbeat("executor-1", 10000L);
    StreamingShuffleHeartbeat heartbeat2 = new StreamingShuffleHeartbeat("executor-1", 10000L);
    StreamingShuffleHeartbeat heartbeat3 = new StreamingShuffleHeartbeat("executor-2", 10000L);

    // Test equals() - identical objects should be equal
    assertEquals(heartbeat1, heartbeat2);
    assertTrue(heartbeat1.equals(heartbeat2));

    // Test hashCode() - equal objects must have equal hash codes
    assertEquals(heartbeat1.hashCode(), heartbeat2.hashCode());

    // Test equals() - different objects should not be equal
    assertNotEquals(heartbeat1, heartbeat3);
    assertFalse(heartbeat1.equals(heartbeat3));

    // Test equals() with null
    assertFalse(heartbeat1.equals(null));

    // Test equals() with different type
    assertFalse(heartbeat1.equals("not a heartbeat"));
  }

  /**
   * Tests toString() method for StreamingShuffleAcknowledgment contains all field values.
   */
  @Test
  public void testStreamingShuffleAcknowledgmentToString() {
    long shuffleId = 987L;
    int mapId = 654;
    int partitionId = 321;
    long consumedOffset = 8192L;
    boolean complete = true;

    StreamingShuffleAcknowledgment ack = new StreamingShuffleAcknowledgment(
        shuffleId, mapId, partitionId, consumedOffset, complete);

    String str = ack.toString();

    // Verify toString() contains class name
    assertTrue(str.contains("StreamingShuffleAcknowledgment"));

    // Verify toString() contains all field values
    assertTrue(str.contains("shuffleId=" + shuffleId));
    assertTrue(str.contains("mapId=" + mapId));
    assertTrue(str.contains("partitionId=" + partitionId));
    assertTrue(str.contains("consumedOffset=" + consumedOffset));
    assertTrue(str.contains("complete=" + complete));
  }

  /**
   * Tests toString() method for StreamingShuffleHeartbeat contains all field values.
   */
  @Test
  public void testStreamingShuffleHeartbeatToString() {
    String consumerId = "executor-test-123";
    long timestamp = 123456789L;

    StreamingShuffleHeartbeat heartbeat = new StreamingShuffleHeartbeat(consumerId, timestamp);

    String str = heartbeat.toString();

    // Verify toString() contains class name
    assertTrue(str.contains("StreamingShuffleHeartbeat"));

    // Verify toString() contains all field values
    assertTrue(str.contains("consumerId=" + consumerId));
    assertTrue(str.contains("timestamp=" + timestamp));
  }

  /**
   * Tests StreamingShuffleAcknowledgment with negative values to ensure proper encoding.
   */
  @Test
  public void testStreamingShuffleAcknowledgmentNegativeValues() {
    // Note: In practice, negative values may not be semantically valid,
    // but the encoding should handle them correctly
    StreamingShuffleAcknowledgment ack = new StreamingShuffleAcknowledgment(
        -1L, -1, -1, -1L, false);

    ByteBuf buf = Unpooled.buffer(25);
    ack.encode(buf);
    StreamingShuffleAcknowledgment decoded = StreamingShuffleAcknowledgment.decode(buf);

    assertEquals(-1L, decoded.shuffleId);
    assertEquals(-1, decoded.mapId);
    assertEquals(-1, decoded.partitionId);
    assertEquals(-1L, decoded.consumedOffset);
    assertFalse(decoded.complete);
    buf.release();
  }

  /**
   * Tests multiple round-trips to verify encoding stability.
   */
  @Test
  public void testStreamingShuffleAcknowledgmentMultipleRoundTrips() {
    StreamingShuffleAcknowledgment original = new StreamingShuffleAcknowledgment(
        555L, 666, 777, 8888L, true);

    // First round trip
    ByteBuf buf1 = Unpooled.buffer(25);
    original.encode(buf1);
    StreamingShuffleAcknowledgment decoded1 = StreamingShuffleAcknowledgment.decode(buf1);
    assertEquals(original, decoded1);
    buf1.release();

    // Second round trip with decoded message
    ByteBuf buf2 = Unpooled.buffer(25);
    decoded1.encode(buf2);
    StreamingShuffleAcknowledgment decoded2 = StreamingShuffleAcknowledgment.decode(buf2);
    assertEquals(original, decoded2);
    assertEquals(decoded1, decoded2);
    buf2.release();
  }

  /**
   * Tests multiple round-trips for StreamingShuffleHeartbeat to verify encoding stability.
   */
  @Test
  public void testStreamingShuffleHeartbeatMultipleRoundTrips() {
    StreamingShuffleHeartbeat original = new StreamingShuffleHeartbeat(
        "executor-stable-test", 987654321L);

    // First round trip
    ByteBuf buf1 = Unpooled.buffer(original.encodedLength());
    original.encode(buf1);
    StreamingShuffleHeartbeat decoded1 = StreamingShuffleHeartbeat.decode(buf1);
    assertEquals(original, decoded1);
    buf1.release();

    // Second round trip with decoded message
    ByteBuf buf2 = Unpooled.buffer(decoded1.encodedLength());
    decoded1.encode(buf2);
    StreamingShuffleHeartbeat decoded2 = StreamingShuffleHeartbeat.decode(buf2);
    assertEquals(original, decoded2);
    assertEquals(decoded1, decoded2);
    buf2.release();
  }

  /**
   * Tests StreamingShuffleHeartbeat with special characters in consumerId.
   */
  @Test
  public void testStreamingShuffleHeartbeatSpecialCharacters() {
    // Test with various special characters and UTF-8 characters
    String consumerId = "executor-@#$%^&*()-_=+[]{}|;:',.<>?/~`";
    long timestamp = 11111L;

    StreamingShuffleHeartbeat heartbeat = new StreamingShuffleHeartbeat(consumerId, timestamp);

    ByteBuf buf = Unpooled.buffer(heartbeat.encodedLength());
    heartbeat.encode(buf);
    StreamingShuffleHeartbeat decoded = StreamingShuffleHeartbeat.decode(buf);

    assertEquals(consumerId, decoded.consumerId);
    assertEquals(timestamp, decoded.timestamp);
    buf.release();
  }
}

