package com.trading.engine.testsupport.sbe;

/**
 * Typed record for batch account encoding via {@link SbeTestEncoder#encodeLoadAccountBatch}.
 *
 * <p>Carries all fields that vary per entry in a {@code LoadAccountBatch} repeating group. The
 * {@code capabilities} field avoids a subtle default mismatch between single ({@code
 * CAN_TRADE|CAN_RFQ}) and batch ({@code CAN_TRADE} only) encoders found in the original test code.
 *
 * <p>Thread-safe — immutable value type.
 *
 * @param id unique account identifier
 * @param code account code string; max 16 ASCII characters
 * @param name human-readable account name; max 64 ASCII characters
 * @param baseCcy base currency ISO code; 3 ASCII characters
 * @param capabilities bitfield of account capabilities
 */
public record AccountRecord(long id, String code, String name, String baseCcy, long capabilities) {

  /** CAN_TRADE capability flag (mirrors {@code AccountState.Capabilities.CAN_TRADE} in cluster). */
  public static final long CAN_TRADE = 1L;

  /** CAN_RFQ capability flag (mirrors {@code AccountState.Capabilities.CAN_RFQ} in cluster). */
  public static final long CAN_RFQ = 2L;

  /**
   * Convenience constructor — defaults name to {@code "Account " + code} and capabilities to {@link
   * #CAN_TRADE} only.
   *
   * @param id unique account identifier
   * @param code account code string
   * @param baseCcy base currency ISO code
   */
  public AccountRecord(final long id, final String code, final String baseCcy) {
    this(id, code, "Account " + code, baseCcy, CAN_TRADE);
  }
}
