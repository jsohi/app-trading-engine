package com.trading.refdata.account;

/** Immutable account record deserialized from YAML / CSV / RDBMS. */
public record AccountRecord(
    long accountId,
    long parentAccountId,
    String accountCode,
    String acctIdSource,
    String accountName,
    String accountType,
    String baseCurrency,
    String status,
    String complianceStatus,
    long capabilities) {}
