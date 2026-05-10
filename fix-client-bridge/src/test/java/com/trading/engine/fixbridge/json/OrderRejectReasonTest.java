package com.trading.engine.fixbridge.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OrderRejectReason}. Covers uniqueness of wire values, classification
 * assignments (locking the FAILURE and TRANSIENT taxonomies per §B-r2-14), and {@link
 * OrderRejectReason#lookup(String)} round-trip and null-safety contracts.
 */
class OrderRejectReasonTest {

  // ===========================================================================
  // Wire-value uniqueness
  // ===========================================================================

  @Test
  void wireValues_allUnique_setSizeMatchesValuesLength() {
    final var values = OrderRejectReason.values();
    final var wireValues = new HashSet<String>();
    for (final var r : values) {
      wireValues.add(r.wireValue());
    }
    assertEquals(values.length, wireValues.size(),
        "every OrderRejectReason must have a unique wireValue()");
  }

  // ===========================================================================
  // Classification non-null
  // ===========================================================================

  @Test
  void classification_allConstants_nonNull() {
    for (final var r : OrderRejectReason.values()) {
      assertNotNull(r.classification(),
          "classification() must not be null for constant " + r.name());
    }
  }

  // ===========================================================================
  // Locked FAILURE taxonomy (§B-r2-14)
  // ===========================================================================

  @Test
  void classification_failureSet_exactlyLockedTaxonomy() {
    final var expected = EnumSet.of(
        OrderRejectReason.MALFORMED,
        OrderRejectReason.INTERNAL,
        OrderRejectReason.BRIDGE_DOWN,
        OrderRejectReason.PRE_TRADE_LIMIT_EXCEEDED,
        OrderRejectReason.QUOTE_NOT_OWNED,
        OrderRejectReason.DUPLICATE_CLORD,
        OrderRejectReason.DUPLICATE_REQID);

    final var actual = EnumSet.noneOf(OrderRejectReason.class);
    for (final var r : OrderRejectReason.values()) {
      if (r.classification() == OrderRejectReason.Classification.FAILURE) {
        actual.add(r);
      }
    }

    assertEquals(expected, actual,
        "FAILURE set must exactly match the locked §B-r2-14 taxonomy");
  }

  // ===========================================================================
  // Locked TRANSIENT taxonomy (§B-r2-14)
  // ===========================================================================

  @Test
  void classification_transientSet_exactlyLockedTaxonomy() {
    final var expected = EnumSet.of(
        OrderRejectReason.QUOTE_EXPIRED,
        OrderRejectReason.QUOTE_NOT_FOUND,
        OrderRejectReason.QUOTE_ALREADY_ACCEPTED,
        OrderRejectReason.QUOTE_ALREADY_REJECTED,
        OrderRejectReason.BACKPRESSURE,
        OrderRejectReason.RATE_LIMIT_EXCEEDED,
        OrderRejectReason.RATE_LIMIT_INITIAL_WINDOW,
        OrderRejectReason.STALE_DPOP);

    final var actual = EnumSet.noneOf(OrderRejectReason.class);
    for (final var r : OrderRejectReason.values()) {
      if (r.classification() == OrderRejectReason.Classification.TRANSIENT) {
        actual.add(r);
      }
    }

    assertEquals(expected, actual,
        "TRANSIENT set must exactly match the locked §B-r2-14 taxonomy");
  }

  // ===========================================================================
  // lookup() round-trip
  // ===========================================================================

  @Test
  void lookup_everyWireValue_roundTripsToSameConstant() {
    for (final var r : OrderRejectReason.values()) {
      final var found = OrderRejectReason.lookup(r.wireValue());
      assertEquals(r, found,
          "lookup(wireValue()) must round-trip to the same constant for " + r.name());
    }
  }

  // ===========================================================================
  // lookup() null-safety
  // ===========================================================================

  @Test
  void lookup_null_returnsNull() {
    assertNull(OrderRejectReason.lookup(null),
        "lookup(null) must return null");
  }

  @Test
  void lookup_unknownString_returnsNull() {
    assertNull(OrderRejectReason.lookup("unknown-reason"),
        "lookup(unknown-reason) must return null");
  }

  @Test
  void lookup_emptyString_returnsNull() {
    assertNull(OrderRejectReason.lookup(""),
        "lookup(\"\") must return null — no constant has an empty wire value");
  }
}
