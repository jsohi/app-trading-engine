package com.trading.engine.testsupport.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TestBuffers}.
 *
 * <p>Verifies buffer sizes, concrete type, and instance isolation.
 */
class TestBuffersTest {

  @Test
  void command_returns256ByteExpandableArrayBuffer() {
    final MutableDirectBuffer buf = TestBuffers.command();
    assertInstanceOf(ExpandableArrayBuffer.class, buf);
    assertEquals(256, ((ExpandableArrayBuffer) buf).capacity());
  }

  @Test
  void event_returns512ByteExpandableArrayBuffer() {
    final MutableDirectBuffer buf = TestBuffers.event();
    assertInstanceOf(ExpandableArrayBuffer.class, buf);
    assertEquals(512, ((ExpandableArrayBuffer) buf).capacity());
  }

  @Test
  void batch_returns4096ByteExpandableArrayBuffer() {
    final MutableDirectBuffer buf = TestBuffers.batch();
    assertInstanceOf(ExpandableArrayBuffer.class, buf);
    assertEquals(4096, ((ExpandableArrayBuffer) buf).capacity());
  }

  @Test
  void snapshot_returns65536ByteExpandableArrayBuffer() {
    final MutableDirectBuffer buf = TestBuffers.snapshot();
    assertInstanceOf(ExpandableArrayBuffer.class, buf);
    assertEquals(65_536, ((ExpandableArrayBuffer) buf).capacity());
  }

  @Test
  void of_returnsBufferWithRequestedSize() {
    final MutableDirectBuffer buf = TestBuffers.of(1024);
    assertInstanceOf(ExpandableArrayBuffer.class, buf);
    assertEquals(1024, ((ExpandableArrayBuffer) buf).capacity());
  }

  @Test
  void eachCall_returnsNewInstance() {
    final MutableDirectBuffer a = TestBuffers.command();
    final MutableDirectBuffer b = TestBuffers.command();
    assertNotSame(a, b);
  }
}
