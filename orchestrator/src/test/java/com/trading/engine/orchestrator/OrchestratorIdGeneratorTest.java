package com.trading.engine.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OrchestratorIdGenerator}. Covers sequential generation, format rendering,
 * constructor validation, counter exhaustion, and accessor consistency.
 */
class OrchestratorIdGeneratorTest {

  // ===========================================================================
  // Sequential generation
  // ===========================================================================

  @Test
  void nextInto_firstCall_generatesIdStartingAt1() {
    final var gen = new OrchestratorIdGenerator("QTE");
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(32);

    final int len = gen.nextInto(buf, 0);

    final String id = buf.getStringWithoutLengthAscii(0, len);
    assertEquals("QTE-00000000001", id);
  }

  @Test
  void nextInto_sequential_incrementsCounter() {
    final var gen = new OrchestratorIdGenerator("QTE");
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(32);

    gen.nextInto(buf, 0);
    gen.nextInto(buf, 0);
    final int len = gen.nextInto(buf, 0);

    final String id = buf.getStringWithoutLengthAscii(0, len);
    assertEquals("QTE-00000000003", id);
    assertEquals(3, gen.currentCounter());
  }

  @Test
  void nextInto_rendersCorrectFormat() {
    final var gen = new OrchestratorIdGenerator("AB");
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(32);

    final int len = gen.nextInto(buf, 0);

    final String id = buf.getStringWithoutLengthAscii(0, len);
    // Format: PREFIX-NNNNNNNNNNN (prefix + dash + 11 digits)
    assertEquals("AB-00000000001", id);
    assertEquals(14, len); // 2 + 1 + 11
  }

  @Test
  void nextInto_writesToBufferAtOffset() {
    final var gen = new OrchestratorIdGenerator("QTE");
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(64);

    final int offset = 10;
    final int len = gen.nextInto(buf, offset);

    final String id = buf.getStringWithoutLengthAscii(offset, len);
    assertEquals("QTE-00000000001", id);
    // Bytes before offset should be untouched (default zero)
    assertEquals(0, buf.getByte(0));
  }

  @Test
  void next_returnsStringRepresentation() {
    final var gen = new OrchestratorIdGenerator("QTE");

    assertEquals("QTE-00000000001", gen.next());
    assertEquals("QTE-00000000002", gen.next());
  }

  // ===========================================================================
  // Constructor validation
  // ===========================================================================

  @Test
  void constructor_nullPrefix_throwsNpe() {
    assertThrows(NullPointerException.class, () -> new OrchestratorIdGenerator(null));
  }

  @Test
  void constructor_blankPrefix_throwsIae() {
    assertThrows(IllegalArgumentException.class, () -> new OrchestratorIdGenerator(""));
  }

  @Test
  void constructor_prefixTooLong_throwsIae() {
    // MAX_PREFIX_LENGTH = 8; 9 chars should fail
    assertThrows(IllegalArgumentException.class, () -> new OrchestratorIdGenerator("ABCDEFGHI"));
  }

  @Test
  void constructor_nonAsciiPrefix_throwsIae() {
    assertThrows(IllegalArgumentException.class, () -> new OrchestratorIdGenerator("QT\u00E9"));
  }

  @Test
  void constructor_maxLengthPrefix_succeeds() {
    // MAX_PREFIX_LENGTH = 8; exactly 8 chars should succeed (8 + 1 + 11 = 20 = SBE QuoteID length)
    final var gen = new OrchestratorIdGenerator("ABCDEFGH");
    assertEquals(20, gen.idByteLength());
  }

  // ===========================================================================
  // Counter exhaustion
  // ===========================================================================

  @Test
  void nextInto_counterAtMaxMinusOne_lastIdSucceeds() throws Exception {
    final var gen = new OrchestratorIdGenerator("QTE");
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(32);

    // Use reflection to set counter to MAX_COUNTER - 1 (avoids 100B iterations)
    final var field = OrchestratorIdGenerator.class.getDeclaredField("counter");
    field.setAccessible(true);
    field.setLong(gen, OrchestratorIdGenerator.MAX_COUNTER - 1);

    // Last valid ID succeeds (counter becomes MAX_COUNTER = 99_999_999_999)
    final int len = gen.nextInto(buf, 0);
    assertEquals(OrchestratorIdGenerator.MAX_COUNTER, gen.currentCounter());

    final String lastId = buf.getStringWithoutLengthAscii(0, len);
    assertEquals("QTE-99999999999", lastId);
  }

  @Test
  void nextInto_counterExhaustion_throwsIse() throws Exception {
    final var gen = new OrchestratorIdGenerator("QTE");
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(32);

    // Set counter to MAX_COUNTER so the next call triggers the exhaustion guard
    final var field = OrchestratorIdGenerator.class.getDeclaredField("counter");
    field.setAccessible(true);
    field.setLong(gen, OrchestratorIdGenerator.MAX_COUNTER);

    final var ex = assertThrows(IllegalStateException.class, () -> gen.nextInto(buf, 0));
    assertTrue(ex.getMessage().contains("counter exhausted"));
    assertTrue(ex.getMessage().contains("QTE"));
  }

  // ===========================================================================
  // Accessors
  // ===========================================================================

  @Test
  void prefix_returnsConstructionPrefix() {
    final var gen = new OrchestratorIdGenerator("QTE");
    assertEquals("QTE", gen.prefix());
  }

  @Test
  void idByteLength_matchesPrefixPlusDashPlus11() {
    assertEquals(15, new OrchestratorIdGenerator("QTE").idByteLength()); // 3+1+11
    assertEquals(14, new OrchestratorIdGenerator("AB").idByteLength()); // 2+1+11
    assertEquals(20, new OrchestratorIdGenerator("ABCDEFGH").idByteLength()); // 8+1+11
  }

  @Test
  void idByteLength_stableAcrossCalls() {
    final var gen = new OrchestratorIdGenerator("QTE");
    final int len1 = gen.idByteLength();
    gen.next();
    gen.next();
    assertEquals(len1, gen.idByteLength());
  }

  @Test
  void currentCounter_beforeAnyCall_returnsZero() {
    final var gen = new OrchestratorIdGenerator("QTE");
    assertEquals(0, gen.currentCounter());
  }
}
