package com.trading.engine.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.AccountTypeEnum;
import com.trading.engine.messages.sbe.AcctIDSourceEnum;
import com.trading.engine.messages.sbe.ComplianceStatusEnum;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link WebSocketLauncher#parseEnumOrNull(String, Class, Enum)} — the generic
 * helper used by the four typed wrappers in {@link WebSocketLauncher}.
 *
 * <p><b>Why this test exists:</b> the four parsers are private statics only exercised transitively
 * via the full launcher boot. A locale or trim-handling regression (e.g. someone removing {@code
 * Locale.ROOT}) would not be caught by any other test in {@code launcher/src/test} until the
 * full-stack-e2e harness flagged it — by which point the test signal is much noisier.
 *
 * <p>Covers the boundary cases the iter-2 Gemini review called out: null, empty, whitespace,
 * mixed-case, all-uppercase, all-lowercase, unknown value, and the Turkish dotted-i hazard for the
 * case-folding step.
 */
final class WebSocketLauncherEnumParseTest {

  @Test
  void parseEnumOrNull_null_returnsNullValue() {
    assertEquals(
        AccountStatusEnum.NULL_VAL,
        WebSocketLauncher.parseEnumOrNull(
            null, AccountStatusEnum.class, AccountStatusEnum.NULL_VAL));
  }

  @Test
  void parseEnumOrNull_empty_returnsNullValue() {
    assertEquals(
        AccountStatusEnum.NULL_VAL,
        WebSocketLauncher.parseEnumOrNull("", AccountStatusEnum.class, AccountStatusEnum.NULL_VAL));
  }

  @Test
  void parseEnumOrNull_whitespace_returnsNullValue() {
    assertEquals(
        AccountStatusEnum.NULL_VAL,
        WebSocketLauncher.parseEnumOrNull(
            "   ", AccountStatusEnum.class, AccountStatusEnum.NULL_VAL));
  }

  @Test
  void parseEnumOrNull_unknown_returnsNullValue() {
    assertEquals(
        AccountStatusEnum.NULL_VAL,
        WebSocketLauncher.parseEnumOrNull(
            "BOGUS", AccountStatusEnum.class, AccountStatusEnum.NULL_VAL));
  }

  @Test
  void parseEnumOrNull_exactMatch_returnsValue() {
    // Canonical SBE-generated PascalCase form.
    assertEquals(
        AccountStatusEnum.Active,
        WebSocketLauncher.parseEnumOrNull(
            "Active", AccountStatusEnum.class, AccountStatusEnum.NULL_VAL));
  }

  @Test
  void parseEnumOrNull_uppercaseInput_returnsValue() {
    // YAML written as ACTIVE — case-insensitive match.
    assertEquals(
        AccountStatusEnum.Active,
        WebSocketLauncher.parseEnumOrNull(
            "ACTIVE", AccountStatusEnum.class, AccountStatusEnum.NULL_VAL));
  }

  @Test
  void parseEnumOrNull_mixedCase_returnsValue() {
    assertEquals(
        AccountStatusEnum.Active,
        WebSocketLauncher.parseEnumOrNull(
            "Active", AccountStatusEnum.class, AccountStatusEnum.NULL_VAL));
  }

  @Test
  void parseEnumOrNull_lowerCase_returnsValue() {
    assertEquals(
        AccountStatusEnum.Active,
        WebSocketLauncher.parseEnumOrNull(
            "active", AccountStatusEnum.class, AccountStatusEnum.NULL_VAL));
  }

  @Test
  void parseEnumOrNull_paddedWhitespace_isTrimmed() {
    assertEquals(
        AccountStatusEnum.Active,
        WebSocketLauncher.parseEnumOrNull(
            "  Active  ", AccountStatusEnum.class, AccountStatusEnum.NULL_VAL));
  }

  @Test
  void parseEnumOrNull_turkishDottedI_doesNotCorruptCaseFolding() {
    // The implementation lowercases BOTH the input and each enum constant name via
    // Locale.ROOT, then compares for equality. On a Turkish-locale host the default
    // toLowerCase() folds ASCII "I" → "ı" (U+0131), which would NOT match the lowercased
    // enum constant name "active" (whose default-folded form is the same "ı...ctive"
    // because the enum constant "Active" → default-locale toLowerCase on tr_TR also
    // produces "ı..."). Locale.ROOT keeps the folding deterministic on both sides so
    // ASCII "i" stays "i" and matching succeeds. Regression guard: replacing either
    // toLowerCase(Locale.ROOT) call with default-locale toLowerCase() will fail this
    // test on a tr_TR JVM.
    assertEquals(
        AccountStatusEnum.Active,
        WebSocketLauncher.parseEnumOrNull(
            "active", AccountStatusEnum.class, AccountStatusEnum.NULL_VAL));
  }

  @Test
  void parseEnumOrNull_eachEnumType_unknownMapsToNullValue() {
    // Smoke-test all four enum families used by the YAML loader so the type plumbing on the
    // production wrappers is exercised.
    assertEquals(
        AccountTypeEnum.NULL_VAL,
        WebSocketLauncher.parseEnumOrNull(
            "BOGUS", AccountTypeEnum.class, AccountTypeEnum.NULL_VAL));
    assertEquals(
        AcctIDSourceEnum.NULL_VAL,
        WebSocketLauncher.parseEnumOrNull(
            "BOGUS", AcctIDSourceEnum.class, AcctIDSourceEnum.NULL_VAL));
    assertEquals(
        ComplianceStatusEnum.NULL_VAL,
        WebSocketLauncher.parseEnumOrNull(
            "BOGUS", ComplianceStatusEnum.class, ComplianceStatusEnum.NULL_VAL));
  }

  @Test
  void parseEnumOrNull_nullType_throws() {
    assertThrows(
        NullPointerException.class,
        () -> WebSocketLauncher.parseEnumOrNull("ACTIVE", null, AccountStatusEnum.NULL_VAL));
  }

  @Test
  void parseEnumOrNull_nullNullValue_throws() {
    assertThrows(
        NullPointerException.class,
        () -> WebSocketLauncher.parseEnumOrNull("ACTIVE", AccountStatusEnum.class, null));
  }
}
