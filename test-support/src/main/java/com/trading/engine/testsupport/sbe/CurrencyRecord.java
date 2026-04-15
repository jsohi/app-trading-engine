package com.trading.engine.testsupport.sbe;

import com.trading.engine.messages.sbe.AccountStatusEnum;
import com.trading.engine.messages.sbe.CurrencyClassEnum;

/**
 * Typed record for batch currency encoding via {@link SbeTestEncoder#encodeLoadCurrencyBatch}.
 *
 * <p>Thread-safe — immutable value type.
 *
 * @param code currency ISO code; 3 ASCII characters (e.g., "USD")
 * @param isoNumeric ISO 4217 numeric code (e.g., 840 for USD)
 * @param name currency display name (e.g., "US Dollar")
 * @param decimals decimal precision (e.g., 2 for USD)
 * @param cls currency classification (Fiat, Crypto, etc.)
 * @param status currency status (Active, Suspended)
 */
public record CurrencyRecord(
    String code,
    int isoNumeric,
    String name,
    int decimals,
    CurrencyClassEnum cls,
    AccountStatusEnum status) {}
