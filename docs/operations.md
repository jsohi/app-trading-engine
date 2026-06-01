# Operations Runbook

This document captures live-operations procedures specific to APP-62 and later risk-engine features. The broader process / cluster / artio runbook lives in `docs/ops-guide.md`; this file is the focused trading-risk-engine reference operators consult during an incident.

---

## APP-62 Pre-Trade Risk Engine Operations

The APP-62 risk engine adds five new reject reasons, eight new in-memory metrics (4 reject counters, 3 silent-skip counters, 3 gauges), one live-tune cluster command (`LoadRiskLimit`), and one start-of-day data dependency (`restricted-symbols.yaml`). This section is the operator's reference for triage, alerting, and intervention.

### 3.1 Reject-reason triage table

Each new reject reason emitted by `NewOrderSingleHandler.validateNewOrder` (`cluster/src/main/java/com/trading/engine/cluster/handler/NewOrderSingleHandler.java`). The Tag 58 (Text) prefix is stamped by `SbeToFixTranslator` so operators grep on a stable token.

| Reason                  | Tag 103 | Tag 58 prefix              | RiskMetrics counter                                                  | GFLog WARN token                                         | Resolution                                                                                                                                                                                                                                                                                                                                                                         |
| ----------------------- | ------- | -------------------------- | -------------------------------------------------------------------- | -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PositionLimitExceeded` | 3       | `[POSITION_LIMIT]`         | `rejectPositionLimit`                                                | `NOS rejected — reason=PositionLimitExceeded`            | Inspect per-(account, symbol) `workingLong` / `workingShort` vs `maxLongPosition` / `maxShortPosition`. If legitimate, raise the cap via `LoadRiskLimit` (4-eyes required — see 3.3). If a runaway working position is the cause, force-cancel the open child orders via the operator CLI to drain the working counter.                                                            |
| `PriceTooFarFromMarket` | 99      | `[FAT_FINGER]`             | `rejectFatFinger`                                                    | `NOS rejected — reason=PriceTooFarFromMarket`            | First check `pricing-service` health and `lastQuotedMidAsOfNanos` staleness (see 3.4). Do NOT loosen `LAST_PRICE_STALENESS_NANOS` to silence a true stale-quote signal. If a per-symbol tighter band is correct (illiquid tenor) confirm `priceDeviationBpsOverride` on the eligibility record (§I).                                                                               |
| `RiskLimitsNotLoaded`   | 3       | `[RISK_LIMITS_NOT_LOADED]` | `rejectRiskLimitsNotLoaded`                                          | `NOS rejected — reason=RiskLimitsNotLoaded`              | The account is missing a `RiskLimitRecord` — the YAML loader at start-of-day did not provision it. Inspect `--risk-limits.file=` argument on the cluster launcher; restart the cluster with the corrected YAML, or publish a `LoadRiskLimit` for the affected account (preferred — no cluster restart). Cross-check `risk.gauge.positionMapSize` to confirm the new record landed. |
| `RegulatoryRestriction` | 99      | `[SYMBOL_ELIGIBILITY]`     | `rejectSymbolEligibility`                                            | `NOS rejected — reason=RegulatoryRestriction`            | Symbol is in `restricted-symbols.yaml` with `tradingAllowed=0` (full halt for the symbol) or `shortSaleAllowed=0` and the order was a Sell (Reg SHO restricted-symbol). Confirm operator intent with the regulatory desk before mutating — restricted-symbol relaxation is an audit event. Use `LoadSymbolEligibility` to update (§G).                                             |
| `FourEyesViolation`     | 99      | `[FOUR_EYES]`              | (4-eyes ingress; not in `RiskMetrics` — counted in `RefDataMetrics`) | `LoadRiskLimit rejected proposerId==approverId or empty` | Resubmit `LoadRiskLimit` with distinct non-empty `proposerId` AND `approverId` strings per MiFID II RTS 6 §1(2) dual-control (APP-62 §H).                                                                                                                                                                                                                                          |

### 3.2 Counter dashboards — alert thresholds

`RiskMetrics` (`cluster/src/main/java/com/trading/engine/cluster/metrics/RiskMetrics.java`) currently exposes per-process in-memory counters. **Aeron CnC export is deferred to APP-137** — until then, operators read counters by attaching to the cluster process via JMX / heap dump / the diagnostics REPL, NOT via the standard `aeron-stat` CnC reader.

#### Reject counters (delta per minute)

| Counter                     | INFO      | WARN               | PAGE                                                                          |
| --------------------------- | --------- | ------------------ | ----------------------------------------------------------------------------- |
| `rejectPositionLimit`       | > 0       | > 5/min            | > 50/min sustained 5min                                                       |
| `rejectFatFinger`           | > 0       | > 10/min           | > 100/min sustained 5min OR co-occurs with pricing-service health degradation |
| `rejectRiskLimitsNotLoaded` | > 0 (any) | > 0 sustained 1min | > 0 sustained 5min — risk-limits feed broken                                  |
| `rejectSymbolEligibility`   | > 0       | > 5/min            | > 50/min sustained 5min OR `restricted-symbols.yaml` recently mutated         |

#### Silent-skip counters

`updateLastQuotedMid` skips writing the cache when input is crossed/locked/non-monotonic. These never reject an order; they're the "did the cache update?" signal.

| Counter                         | WARN               | PAGE                                                   |
| ------------------------------- | ------------------ | ------------------------------------------------------ |
| `skipUpdateCrossedOrLocked`     | > 10/min           | > 100/min — pricing-service producing bad books        |
| `skipUpdateTimestampRegression` | > 0 sustained 1min | > 0 sustained 5min — Raft replay anomaly or clock skew |
| `skipUpdateInvalidInput`        | > 0                | > 10/min — schema-violating PriceResponse              |

#### Gauges (current value, sampled at 1 Hz)

| Gauge                | WARN                           | PAGE  | Note                                                                                                          |
| -------------------- | ------------------------------ | ----- | ------------------------------------------------------------------------------------------------------------- |
| `positionMapSize`    | > 80% of expected universe     | > 95% | If gauge approaches `Long2ObjectHashMap` resize threshold, latency tail will spike. Pre-size at start-of-day. |
| `lastPriceCacheSize` | > 1.5x symbol universe         | > 2x  | Indicates stale entries — cache is currently never evicted (APP-180).                                         |
| `dailyVolumeMapSize` | > 1.1x active-account universe | > 2x  | Should reset at SOD; non-zero pre-SOD == reset job failed.                                                    |

All thresholds are starting points — tune per desk volume profile after one week of baseline data.

### 3.3 Live-tune procedure (LoadRiskLimit)

Publishing a new `LoadRiskLimit` cluster command rewrites the account's `RiskLimitRecord` in-place. `RiskLimitProjection` (`projections/.../risklimits/RiskLimitProjection.java`) consumes the resulting `RiskLimitChangedEvent` (template 119) and updates the read-side store. No cluster restart required.

Command shape (`trading-schema.xml` template 16 / `LoadRiskLimit`):

```
accountCode        — 8-byte ASCII
maxOrderSize       — int64, units (0 = unlimited)
maxOrderNotional   — int64, fixed-point notional (0 = unlimited)
maxDailyVolume     — int64, units (0 = unlimited)
ratePerSecond      — int32 (0 = unlimited)
burstCapacity      — int32 (0 = unlimited)
maxLongPosition    — int64, units (effective only when positionLimitEnabled=1)
maxShortPosition   — int64, units (effective only when positionLimitEnabled=1)
positionLimitEnabled — uint8 (0 / 1)
priceDeviationBps  — int32, bps (effective only when fatFingerEnabled=1)
fatFingerEnabled   — uint8 (0 / 1)
fatFingerFailClosed — uint8 (0 = skip on missing/stale ref, 1 = reject)
idleSessionTimeoutNanos — int64
proposerId         — 16-byte ASCII (MUST be non-empty)
approverId         — 16-byte ASCII (MUST be non-empty AND != proposerId)
```

**4-eyes**: the cluster ingress validates `proposerId` and `approverId` are both non-empty and distinct (`NewOrderSingleHandler`-adjacent admin handler — APP-62 §H). Failure emits `RejectReasonEnum.FourEyesViolation` on the admin response stream and the projection is NOT mutated.

**Verification**: after publishing, observe `risk.gauge.positionMapSize` for an upward delta if a new account was provisioned; for an in-place update, query the projection's read API (`QueryService.getAccountLimits(accountCode)`) and confirm the post-image. The bridge consumer caches lookups (`ClusterAccountLimitsProvider`) — observed update may be delayed up to the cache TTL.

### 3.4 Fat-finger diagnostic decision tree

Symptom: legitimate market-making flow rejected with `[FAT_FINGER] price deviates from last mid by more than priceDeviationBps`.

```
1. Is `lastQuotedMidAsOfNanos` for the symbol older than LAST_PRICE_STALENESS_NANOS
   (default = 5 minutes) relative to clusterTimestamp?
   ├── YES → STALE REFERENCE
   │   ├── Is pricing-service healthy? (process up, publishing PriceResponse?)
   │   │   ├── NO → recover pricing-service. DO NOT loosen LAST_PRICE_STALENESS_NANOS;
   │   │   │       that would defeat the §5 staleness invariant and let stale quotes
   │   │   │       pass the band test.
   │   │   └── YES → check pricing-service input feed for the symbol. Likely upstream
   │   │            quote feed silence; this is exactly the case fail-closed protects.
   │   └── If fatFingerFailClosed=1 → reject is correct; restore reference, retry.
   └── NO → REFERENCE FRESH; deviation exceeded the band.
       ├── Is `effectivePriceDeviationBps = priceDeviationBpsOverride > 0
       │     ? priceDeviationBpsOverride
       │     : account.priceDeviationBps` tight enough that legitimate flow trips it?
       │   ├── If a per-symbol override is set (§I) — review the override value;
       │   │   tightening is common in illiquid tenors but should be desk-approved.
       │   └── Otherwise — adjust the account-wide priceDeviationBps via LoadRiskLimit
       │       (4-eyes required).
       └── Audit the rejected price vs lastMid: a true mis-priced order (digit transpose,
           wrong side) is the system working as designed.
```

`LAST_PRICE_STALENESS_NANOS` lives in `NewOrderSingleHandler` as a class constant; runtime tuning is not exposed — it is a fail-closed safety constant.

### 3.5 Position-limit two-sided market-maker example

Working LONG and working SHORT are tracked as independent monotonic counters per (account, symbol) — they are NOT netted before the limit check. This matches the CME PTRM (Pre-Trade Risk Management) Long-Qty / Short-Qty convention and ICE Trade Risk's two-sided exposure model.

**Why independent**: a two-sided market-maker simultaneously sitting on a 1M Buy and a 1M Sell of EUR/USD is exposed to 1M units on EACH side at the worst-case fill. If we netted the working notional to zero, we would understate the maximum loss the desk could suffer if both sides filled adversely in rapid succession (e.g., a one-sided market move that lifts the Sell and ignores the Buy, or a venue outage that fills the Buy at the same moment as a price gap).

**Worked example**: account `MM-01`, symbol `EUR/USD`, `maxLongPosition = 5_000_000`, `maxShortPosition = 5_000_000`.

```
T0:  Working long = 0,         working short = 0
T1:  NOS Buy  3_000_000        ── projected long = 3_000_000  ≤ 5M → admit
                                  projected short = 0         ≤ 5M → admit
T2:  NOS Sell 4_000_000        ── projected long = 3_000_000  ≤ 5M → admit
                                  projected short = 4_000_000 ≤ 5M → admit
T3:  NOS Buy  3_000_000        ── projected long = 6_000_000  > 5M → REJECT
                                  (working short untouched at 4M)
T4:  NOS Sell 2_000_000        ── projected long = 3_000_000  ≤ 5M
                                  projected short = 6_000_000 > 5M → REJECT
```

If netted, T3's projected net would be `3M long - 4M short = 1M net short`, well inside any net cap — but the worst-case adversarial fill at T3 leaves the desk with `6M long` of risk before the `4M short` can off-set. The unnetted model rejects T3 as intended.

### 3.6 Snapshot disk/memory budget

The cluster takes a single coordinated snapshot via the Aeron Cluster `onTakeSnapshot` callback. APP-62 raised the `SNAPSHOT_STORE_COUNT` count from 8 to 11 (added `RiskLimit` extension, `LastQuotedPrice`, `AccountPosition`, `SymbolEligibility`; pre-APP-62 baseline was 8).

**Encode order** (must match restore order on the cold path):

```
200 SnapshotTaken
206 EventSequencer
205 IdGenerator
201 Account
208 Currency
209 RiskLimit             ← extended in APP-62 §4/§5/§B/§I (positionLimitEnabled,
                              maxLong/Short, priceDeviationBps, fatFingerEnabled,
                              fatFingerFailClosed, idleSessionTimeoutNanos)
213 SymbolEligibility     ← NEW APP-62 §G/§I
202 OrderBook
203 RfqState
210 ClOrdIdDedup
211 LastQuotedPrice       ← NEW APP-62 §5
212 AccountPosition       ← NEW APP-62 §4 (working long + working short)
```

**Size budget (plan §3.5a)**: at current scale (50k accounts × 100 symbols universe, 10k working positions, 100k dedup entries) the total snapshot payload is ~3.6 MB, comfortably under Aeron's default `MAX_MESSAGE_LENGTH = 8 MB`. Scale-out path: bump `aeron.term.length` (cluster launcher) and `MAX_MESSAGE_LENGTH` together — they have a 1:8 ratio constraint (Aeron requires `maxMessageLength <= termLength / 8`).

**Schema-changing PRs**: after merging a PR that touches `trading-schema.xml`, run `./gradlew e2eClean` before restart. Stale snapshot bytes from a previous schema can deserialize incorrectly under the new codec layout.

### 3.7 Gauge-vs-counter operator note

`RiskMetrics` gauges (`positionMapSize`, `lastPriceCacheSize`, `dailyVolumeMapSize`) and reject counters (`rejectPositionLimit`, etc.) currently live in-memory in the cluster process; they are **not** exported to Aeron CnC counters today. Operators must read them via:

- JMX (the cluster JVM has the standard `com.sun.management.jmxremote` flags wired in `launcher/.../TradingEngineLauncher.java`)
- Diagnostics REPL: `bin/cluster-diag --metric risk.reject.positionLimit`
- Heap-dump field probe (last-resort post-mortem)

Counter values are exact (single-writer in the cluster duty cycle). Gauges read map size atomically each sample but are not snapshotted into a time series — operators see only the current value. APP-137 will wrap each `public long` field in an `AtomicCounter` backed by the Aeron CnC file so `aeron-stat` displays them alongside Raft / archive counters.

### 3.8 §G symbol-eligibility YAML deployment

`restricted-symbols.yaml` is consumed by `TradingEngineLauncher` Step 8 (after currencies/accounts/risk-limits — see `launcher/src/main/java/com/trading/engine/launcher/TradingEngineLauncher.java`). Schema:

```yaml
symbols:
    - symbol: AAPL
      tradingAllowed: true
      shortSaleAllowed: false # SEC Reg SHO restricted list, e.g., circuit-breaker triggered
      priceDeviationBpsOverride: 0 # 0 = use account default (§I)
    - symbol: TSLA
      tradingAllowed: false # full halt — even Buy orders reject with RegulatoryRestriction
      shortSaleAllowed: false
      priceDeviationBpsOverride: 25 # tighter band than account default (illiquid tenor example)
```

**Fail-closed contract**: every tradable symbol MUST have an eligibility record at start-of-day. The §G check 11g rejects with `RegulatoryRestriction` when `symbolEligibilityStore.get(packedSymbolKey) == null`. There is no fail-open mode — operators cannot bypass §G by omitting a symbol from the YAML.

**Adding a symbol**: append an entry, redeploy via `LoadSymbolEligibility` (single) or `LoadSymbolEligibilityBatch` (bulk — preferred for SOD). Both are cluster commands and journal as `SymbolEligibilityLoadedEvent` (template 120).

**Removing a symbol**: this is a regulated audit event. Recommended workflow is to set `tradingAllowed=false` rather than deleting the entry — keep the audit trail of the symbol's prior eligibility. Outright deletion will cause every subsequent NOS for that symbol to reject with `RegulatoryRestriction` (same as a never-loaded symbol).

The YAML is replayed deterministically into the cluster via `ReferenceDataOrchestrator`; under cluster snapshot+restore the eligibility records survive via snapshot template 213.
