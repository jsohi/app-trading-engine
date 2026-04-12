# Reference Data — Accounts

## Data Flow

```text
STARTUP SEQUENCE:
  accounts.yaml --> YamlAccountLoader --> AccountRecord[]
    --> AccountCommandEncoder --> SBE LoadAccount (templateId=11)
    --> ReferenceDataOrchestrator --> Aeron ingress --> Cluster
    --> LoadAccountHandler validates & upserts into AccountStore
    --> emits AccountLoaded (110) or AccountLoadRejected (111)
    --> ReferenceDataOrchestrator awaits ALL acks (fail-fast on any rejection)
    --> THEN Gateway opens FIX acceptor (no orders can arrive before accounts loaded)

RUNTIME (PlaceOrder):
  FIX NOS (account="ACME-001") -> Gateway -> SBE NOS -> Cluster
    -> NewOrderSingleHandler (via CommandHandler dispatch):
        1. symbol must not be empty -> reject UNKNOWN_SYMBOL
        2. orderQty must be positive -> reject INVALID_QUANTITY
        3. limit orders must have positive price -> reject INVALID_PRICE
        4. accountCode must not be empty -> reject ACCOUNT_NOT_FOUND
        5. accountStore.getByCode(buffer, offset, length) -> AccountState
        6. null -> reject ACCOUNT_NOT_FOUND
        7. status != ACTIVE -> reject ACCOUNT_SUSPENDED
        8. !canTrade -> reject ACCOUNT_NO_TRADE_PERMISSION
        9. currency must be 3 uppercase ASCII letters -> reject INVALID_CURRENCY_CODE
       10. currency must exist in CurrencyStore -> reject UNKNOWN_CURRENCY
       11. orderQty > maxOrderSize risk limit -> reject ORDER_EXCEEDS_MAX_SIZE
       12. OrderBook must not be full -> reject BOOK_FULL
       13. proceed with order creation

RUNTIME (QuoteRequest):
  FIX QuoteRequest (account="ACME-001") -> Gateway -> Cluster
    -> QuoteRequestHandler (planned — APP-30):
        1. accountStore.getByCode(accountBuffer, offset, length) -> AccountState
        2. null -> reject UNKNOWN_ACCOUNT
        3. status != ACTIVE -> reject ACCOUNT_SUSPENDED
        4. !canRequestQuotes -> reject ACCOUNT_NO_QUOTE_PERMISSION
        5. proceed -> emits QuoteRequested -> Orchestrator -> Pricing Service

RUNTIME (CancelOrder):
  -> NO account validation (by design - see "CancelOrder Bypass" below)
```

## AccountStore Dual Index

The NOS SBE message carries `account` as `char[16]` (the string code, e.g., "ACME-001"). AccountStore needs both a numeric primary key (for snapshots, admin operations) and a string secondary key (for order/quote validation at runtime).

```text
AccountStore:
  primary:   Long2ObjectHashMap<AccountState>                   (keyed by accountId — for snapshots, idempotent upsert)
  secondary: Object2ObjectHashMap<ByteArrayKey, AccountState>   (keyed by accountCode — for runtime validation)
  sidecar:   Long2ObjectHashMap<ByteArrayKey>                   (accountId → ByteArrayKey — for safe mutation tracking)
```

Both indexes are populated atomically during `LoadAccountHandler.onCommand()` (deterministic, replicated). Lookup by account code uses a reusable scratch `ByteArrayKey` wrapping the SBE decoder's account field — zero allocation on the hot lookup path.

**ByteArrayKey safety:** The secondary index uses a custom `ByteArrayKey` wrapper (defensive-copy approach) rather than raw `DirectBuffer` keys. `DirectBuffer` is a mutable wrapper whose underlying buffer is reused by the SBE decoder, making it unsafe as a hash key. On insertion, `ByteArrayKey` copies the `accountCode` bytes into an owned `byte[]`. This copy happens once per `LoadAccountHandler.onCommand()` (not on the hot lookup path). For lookups, a reusable scratch `ByteArrayKey` is safe because lookups are read-only and single-threaded.

### API

```java
AccountState getByCode(DirectBuffer buffer, int offset, int length);  // runtime hot path
AccountState get(long accountId);                                      // snapshots, admin
```

Handlers call `accountStore.getByCode(buffer, offset, length)` — NOT `accountStore.get(accountId)`.

## AccountCode Uniqueness Enforcement

Two accounts with the same accountCode but different accountId would cause the secondary index to silently overwrite. This is prevented at two levels:

1. **LoadAccountHandler** (cluster-side, authoritative): On upsert, if accountCode already exists under a different accountId, emit AccountLoadRejected (111) with reason `DUPLICATE_ACCOUNT_CODE`.
2. **YamlAccountLoader** (client-side, defensive): Validate uniqueness at file-load time before sending commands. This catches errors early but is not the source of truth.

## Startup Failure Mode

The ReferenceDataOrchestrator implements fail-fast semantics:

1. Send all LoadAccount commands
2. Await AccountLoaded (110) for each, with **10-second timeout**
3. **ANY** AccountLoadRejected (111) -> log error, throw `ReferenceDataLoadException`, prevent startup
4. Timeout (no ack within 10s) -> throw `ReferenceDataLoadException`, prevent startup
5. Only after ALL accounts confirmed loaded -> proceed to bind Gateway

The invariant is: **the FIX acceptor MUST NOT bind until all reference data is loaded and confirmed**.

## Startup Ordering

```text
MediaDriver -> Cluster -> ReferenceDataOrchestrator (await ALL acks) -> Gateway -> RFQ Orchestrator
```

The ReferenceDataOrchestrator (APP-59) is a **separate component** from the RFQ Orchestrator (APP-30). It only runs during the startup sequence to load reference data. The RFQ Orchestrator handles runtime quote workflows and does NOT need an AccountStore replica — account validation happens in the cluster before events reach the RFQ Orchestrator.

## CancelOrder Account Bypass

CancelOrder bypasses account validation by design. Cancellation must always be possible regardless of account status. If an account is suspended after an order is placed, the trader must still be able to cancel the outstanding order. The CancelOrder SBE message carries the `account` field for audit purposes only — it is not validated.

## Restart & Recovery Scenarios

### A. RFQ Orchestrator bounces alone
No impact on accounts. AccountStore lives in the cluster, which continues running. Orchestrator reconnects and resumes RFQ processing.

### B. Entire system bounces (cluster + orchestrator + gateway)
Cluster restores AccountStore from **Aeron snapshot** (AccountSnapshot, templateId 201). Accounts are available immediately after cluster leader election — no YAML reload needed. ReferenceDataOrchestrator runs again anyway (startup sequence), re-sends LoadAccount commands. Idempotent upsert means re-loading is safe (overwrites with same data). Gateway opens only after all acks received.

### C. Fresh start / snapshot lost
AccountStore is empty after cluster starts. ReferenceDataOrchestrator MUST run and load from YAML before Gateway opens. Covered by startup ordering.

### D. New account added to YAML while system is running
There is **no hot-reload mechanism** for the initial delivery. The ReferenceDataOrchestrator only runs at startup. Adding accounts requires restarting the orchestrator process (which will re-send all LoadAccount commands, including new ones).

This is acceptable for most trading firms where account onboarding is a controlled, infrequent process. Hot-reload via a `ReloadReferenceData` admin command is deferred as a future enhancement.

## maxDailyVolume

The `maxDailyVolume` field exists in the SBE schema and AccountState but is **not enforced** in the initial delivery. It requires timer infrastructure (daily reset) and volume tracking (cumulative fill aggregation) that does not yet exist. Reserved for future use.
