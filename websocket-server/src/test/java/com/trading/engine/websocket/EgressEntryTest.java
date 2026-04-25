package com.trading.engine.websocket;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link EgressEntry} — verifies construction, metadata setting, byte filling, templateId
 * extraction, and reliability classification.
 *
 * <p>No Netty ByteBuf involved; pure byte-array tests.
 */
final class EgressEntryTest {

  @Test
  void constructor_validSize_createsByteArray() {
    final int maxSize = 1024;
    final var entry = new EgressEntry(maxSize);

    assertEquals(maxSize, entry.bytes().length);
  }

  @Test
  void setMetadata_validArgs_setsLengthAndTemplateId() {
    final var entry = new EgressEntry(256);

    entry.setMetadata(100, 70);

    assertEquals(100, entry.length());
    assertEquals(70, entry.templateId());
  }

  @Test
  void fill_fromExternalSource_copiesBytes() {
    final var entry = new EgressEntry(64);
    final var source = new byte[] {0x0A, 0x0B, 0x0C, 0x0D, 0x0E};
    final int templateId = 51;

    entry.fill(source, 0, source.length, templateId);

    assertEquals(source.length, entry.length());
    assertEquals(templateId, entry.templateId());

    // Verify that the first N bytes of the backing array match the source
    final var actual = new byte[source.length];
    System.arraycopy(entry.bytes(), 0, actual, 0, source.length);
    assertArrayEquals(source, actual);
  }

  @Test
  void extractTemplateId_littleEndianHeader_returnsCorrectId() {
    // SBE header: [blockLength:uint16][templateId:uint16][schemaId:uint16][version:uint16]
    // templateId at offset 2, little-endian
    final var buffer = new byte[8];
    final int expectedTemplateId = 300; // 0x012C in LE: buffer[2]=0x2C, buffer[3]=0x01

    // Write blockLength placeholder
    buffer[0] = 0x00;
    buffer[1] = 0x00;
    // Write templateId in little-endian
    buffer[2] = (byte) (expectedTemplateId & 0xFF);
    buffer[3] = (byte) ((expectedTemplateId >> 8) & 0xFF);

    final int result = EgressEntry.extractTemplateId(buffer, 0);
    assertEquals(expectedTemplateId, result);
  }

  @Test
  void isReliable_domainEvent_returnsTrue() {
    final var entry = new EgressEntry(64);
    // templateId 100 = first domain event (events 100+)
    entry.setMetadata(32, 100);
    assertTrue(entry.isReliable());

    // templateId 116 = another domain event
    entry.setMetadata(32, 116);
    assertTrue(entry.isReliable());
  }

  @Test
  void isReliable_priceResponse_returnsFalse() {
    final var entry = new EgressEntry(64);
    // templateId 51 = PriceResponse (best-effort)
    entry.setMetadata(32, 51);
    assertFalse(entry.isReliable());
  }

  @Test
  void isReliable_commandAck_returnsTrue() {
    final var entry = new EgressEntry(64);
    // templateId 70 = CommandAck (reliable)
    entry.setMetadata(32, 70);
    assertTrue(entry.isReliable());
  }
}
