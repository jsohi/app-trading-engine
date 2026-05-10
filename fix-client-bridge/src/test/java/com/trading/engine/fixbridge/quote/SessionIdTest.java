package com.trading.engine.fixbridge.quote;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SessionId} — validates the value-type invariants: constructor guards,
 * equals/hashCode contract, and toString readability.
 *
 * <p><b>Threading.</b> Single-threaded test execution; no concurrency concerns.
 */
final class SessionIdTest {

  // ---------------------------------------------------------------------------
  // Constructor — acceptance
  // ---------------------------------------------------------------------------

  @Test
  void ctor_nonEmptyString_wrapsValue() {
    final var id = new SessionId("S-001");
    assertEquals("S-001", id.value());
  }

  @Test
  void ctor_singleCharString_accepted() {
    final var id = new SessionId("X");
    assertEquals("X", id.value());
  }

  // ---------------------------------------------------------------------------
  // Constructor — guard clauses
  // ---------------------------------------------------------------------------

  @Test
  void ctor_nullValue_throwsNPE() {
    assertThrows(NullPointerException.class, () -> new SessionId(null));
  }

  @Test
  void ctor_emptyString_throwsIAE() {
    assertThrows(IllegalArgumentException.class, () -> new SessionId(""));
  }

  // ---------------------------------------------------------------------------
  // equals / hashCode contract
  // ---------------------------------------------------------------------------

  @Test
  void equals_sameValue_returnsTrue() {
    final var a = new SessionId("SESS-1");
    final var b = new SessionId("SESS-1");
    assertEquals(a, b);
  }

  @Test
  void equals_differentValues_returnsFalse() {
    final var a = new SessionId("SESS-1");
    final var b = new SessionId("SESS-2");
    assertNotEquals(a, b);
  }

  @Test
  void equals_sameReference_returnsTrue() {
    final var a = new SessionId("SESS-REF");
    assertEquals(a, a);
  }

  @Test
  void equals_null_returnsFalse() {
    final var a = new SessionId("SESS-1");
    assertNotEquals(null, a);
  }

  @Test
  void equals_nonSessionIdType_returnsFalse() {
    final var a = new SessionId("SESS-1");
    assertNotEquals("SESS-1", a);
  }

  @Test
  void hashCode_sameValue_sameHash() {
    final var a = new SessionId("SESS-1");
    final var b = new SessionId("SESS-1");
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void hashCode_differentValues_typicallyDifferentHash() {
    // Not a strict contract (collisions are possible), but with these two distinct strings the
    // JDK's String.hashCode is guaranteed to differ.
    final var a = new SessionId("SESS-1");
    final var b = new SessionId("SESS-2");
    assertNotEquals(a.hashCode(), b.hashCode());
  }

  // ---------------------------------------------------------------------------
  // toString
  // ---------------------------------------------------------------------------

  @Test
  void toString_includesValue() {
    final var id = new SessionId("MY-SESSION-ID");
    final var str = id.toString();
    assertTrue(
        str.contains("MY-SESSION-ID"), "toString() should contain the wrapped value; got: " + str);
  }
}
