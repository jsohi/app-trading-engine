package com.trading.engine.pricing.market;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MarketDataTickSlot}.
 *
 * <p><b>Purpose.</b> Verifies the in-place mutation contract of {@link MarketDataTickSlot#set}: all
 * five wire fields ({@code bidPrice}, {@code askPrice}, {@code bidSize}, {@code askSize}, {@code
 * ingressNanos}) must be overwritten on every call; {@code symbolSeq} must remain independent of
 * {@code set} (the publisher owns the sequence counter). Boundary values ({@code Long.MIN_VALUE},
 * {@code Long.MAX_VALUE}, {@code 0}) are exercised to confirm no silent truncation or
 * sign-extension occurs in the field storage.
 *
 * <p><b>Threading model.</b> Single-threaded — {@link MarketDataTickSlot} is not thread-safe and
 * all calls here run on the JUnit test thread.
 *
 * <p><b>Allocation.</b> Not asserting zero-alloc — {@link MarketDataTickSlot} is a plain
 * package-private POJO; the allocation contract is only relevant on the publisher's hot path, not
 * for the slot itself in isolation.
 *
 * <p><b>Design rationale.</b> Testing the slot in isolation from the publisher simplifies failure
 * attribution: a regression in {@code set} semantics is immediately locatable without needing a
 * publisher fixture.
 *
 * <p><b>Dependencies.</b> {@link MarketDataTickSlot} only; no Aeron or SBE dependencies.
 */
final class MarketDataTickSlotTest {

  // =========================================================================
  // §1 — set() overwrites all five fields in place
  // =========================================================================

  /**
   * A single {@link MarketDataTickSlot#set} call must overwrite all five fields ({@code bidPrice},
   * {@code askPrice}, {@code bidSize}, {@code askSize}, {@code ingressNanos}) and must leave {@code
   * symbolSeq} untouched.
   */
  @Test
  void set_overwrites_allFiveFields() {
    final var slot = new MarketDataTickSlot();
    slot.symbolSeq = 42L;

    slot.set(100L, 200L, 300L, 400L, 500L);

    assertEquals(100L, slot.bidPrice, "bidPrice must be overwritten");
    assertEquals(200L, slot.askPrice, "askPrice must be overwritten");
    assertEquals(300L, slot.bidSize, "bidSize must be overwritten");
    assertEquals(400L, slot.askSize, "askSize must be overwritten");
    assertEquals(500L, slot.ingressNanos, "ingressNanos must be overwritten");
    assertEquals(42L, slot.symbolSeq, "symbolSeq must NOT be touched by set()");
  }

  // =========================================================================
  // §2 — symbolSeq is independent of set()
  // =========================================================================

  /**
   * {@code symbolSeq} is managed exclusively by the publisher. It must survive repeated {@link
   * MarketDataTickSlot#set} calls with arbitrary values unchanged.
   */
  @Test
  void set_doesNotModify_symbolSeq() {
    final var slot = new MarketDataTickSlot();

    for (long seq = 0L; seq < 10L; seq++) {
      slot.symbolSeq = seq;
      slot.set(1L, 2L, 3L, 4L, 5L);
      assertEquals(seq, slot.symbolSeq, "symbolSeq must equal " + seq + " after set()");
    }
  }

  // =========================================================================
  // §3 — multiple set() calls mutate in place (same object reference)
  // =========================================================================

  /**
   * Successive {@link MarketDataTickSlot#set} calls must mutate the same object in place (not
   * produce a new instance) and the final values must reflect the last call.
   */
  @Test
  void set_multipleCalls_mutatesInPlace_sameObjectReference() {
    final var slot = new MarketDataTickSlot();

    slot.set(10L, 20L, 30L, 40L, 50L);
    final var ref1 = slot;

    slot.set(11L, 21L, 31L, 41L, 51L);
    assertSame(ref1, slot, "set() must not replace the slot object");

    assertEquals(11L, slot.bidPrice, "bidPrice must reflect second set()");
    assertEquals(21L, slot.askPrice, "askPrice must reflect second set()");
    assertEquals(31L, slot.bidSize, "bidSize must reflect second set()");
    assertEquals(41L, slot.askSize, "askSize must reflect second set()");
    assertEquals(51L, slot.ingressNanos, "ingressNanos must reflect second set()");
  }

  // =========================================================================
  // §4 — boundary values: Long.MIN_VALUE, Long.MAX_VALUE, 0
  // =========================================================================

  /** {@code Long.MIN_VALUE} across all five fields must survive a round-trip without corruption. */
  @Test
  void set_boundaryValue_longMinValue() {
    final var slot = new MarketDataTickSlot();
    final long v = Long.MIN_VALUE;

    slot.set(v, v, v, v, v);

    assertEquals(v, slot.bidPrice, "bidPrice must hold Long.MIN_VALUE");
    assertEquals(v, slot.askPrice, "askPrice must hold Long.MIN_VALUE");
    assertEquals(v, slot.bidSize, "bidSize must hold Long.MIN_VALUE");
    assertEquals(v, slot.askSize, "askSize must hold Long.MIN_VALUE");
    assertEquals(v, slot.ingressNanos, "ingressNanos must hold Long.MIN_VALUE");
  }

  /** {@code Long.MAX_VALUE} across all five fields must survive a round-trip without corruption. */
  @Test
  void set_boundaryValue_longMaxValue() {
    final var slot = new MarketDataTickSlot();
    final long v = Long.MAX_VALUE;

    slot.set(v, v, v, v, v);

    assertEquals(v, slot.bidPrice, "bidPrice must hold Long.MAX_VALUE");
    assertEquals(v, slot.askPrice, "askPrice must hold Long.MAX_VALUE");
    assertEquals(v, slot.bidSize, "bidSize must hold Long.MAX_VALUE");
    assertEquals(v, slot.askSize, "askSize must hold Long.MAX_VALUE");
    assertEquals(v, slot.ingressNanos, "ingressNanos must hold Long.MAX_VALUE");
  }

  /** {@code 0} across all five fields must survive a round-trip (zero is a valid sentinel). */
  @Test
  void set_boundaryValue_zero() {
    final var slot = new MarketDataTickSlot();
    // Pre-set non-zero values to confirm zero overwrite.
    slot.set(99L, 99L, 99L, 99L, 99L);

    slot.set(0L, 0L, 0L, 0L, 0L);

    assertEquals(0L, slot.bidPrice, "bidPrice must hold 0");
    assertEquals(0L, slot.askPrice, "askPrice must hold 0");
    assertEquals(0L, slot.bidSize, "bidSize must hold 0");
    assertEquals(0L, slot.askSize, "askSize must hold 0");
    assertEquals(0L, slot.ingressNanos, "ingressNanos must hold 0");
  }

  // =========================================================================
  // §5 — default-constructed slot has zero-valued fields
  // =========================================================================

  /**
   * A freshly default-constructed {@link MarketDataTickSlot} must have all fields initialised to
   * zero (Java primitive default). The publisher relies on this — it calls {@code set()} before the
   * first drain, but the slot object itself is created by the factory at first sight.
   */
  @Test
  void defaultConstruct_allFieldsAreZero() {
    final var slot = new MarketDataTickSlot();

    assertEquals(0L, slot.bidPrice, "default bidPrice must be 0");
    assertEquals(0L, slot.askPrice, "default askPrice must be 0");
    assertEquals(0L, slot.bidSize, "default bidSize must be 0");
    assertEquals(0L, slot.askSize, "default askSize must be 0");
    assertEquals(0L, slot.ingressNanos, "default ingressNanos must be 0");
    assertEquals(0L, slot.symbolSeq, "default symbolSeq must be 0");
  }
}
