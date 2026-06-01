# Clock Synchronisation

All trading engine processes use nanosecond epoch timestamps. The cluster provides
deterministic timestamps via `NanosecondClusterClock`; non-cluster processes use
`OffsetEpochNanoClock` (zero-allocation, anchored to `System.nanoTime()`).

Both delegate to the operating system clock. Cross-box timestamp consistency is an
infrastructure concern — the application cannot solve it.

## PTP (IEEE 1588v2) — Preferred

Precision Time Protocol provides sub-microsecond synchronisation with PTP-capable
NICs and a grandmaster clock on the network.

**Linux setup (linuxptp):**

```bash
# Start PTP daemon on the PTP-capable NIC
ptp4l -i eth0 -m -f /etc/ptp4l.conf

# Synchronise the system clock from the PTP hardware clock
phc2sys -a -r -m
```

Expected offset: < 1 microsecond with hardware timestamping.

## chrony with Hardware Timestamping — Fallback

If PTP hardware is unavailable, chrony with hardware timestamping achieves
low-microsecond accuracy.

```bash
# /etc/chrony.conf
server ntp1.example.com iburst
hwtimestamp eth0
```

Expected offset: 1-10 microseconds.

## Verification

```bash
# Check current clock offset and drift
chronyc tracking

# Key fields:
#   System time     : offset from NTP (should be < 1us with PTP)
#   RMS offset      : root-mean-square of recent offsets
#   Frequency       : clock drift rate (ppm)
```

## Alert Thresholds

| Metric              | Warning  | Critical |
| ------------------- | -------- | -------- |
| System clock offset | > 100 us | > 1 ms   |
| RMS offset          | > 50 us  | > 500 us |

## Why Application Code Cannot Solve This

`OffsetEpochNanoClock` anchors `System.nanoTime()` to `System.currentTimeMillis()` at
startup and re-samples hourly. If two boxes have OS clocks that diverge by 50 ms, their
epoch-nano timestamps diverge by 50 ms — no amount of application logic can fix this
without a synchronised time source at the OS/network level.

## JWT expiry comparison

`JwtExpirySweeper` (`websocket-server/.../JwtExpirySweeper.java`) compares the
RFC 7519 `exp` claim against `EpochNanoClock`, **not** the monotonic
`NanoClock`. This is deliberate: RFC 7519 §4.1.4 defines `exp` as a
wall-clock NumericDate ("seconds since 1970-01-01T00:00:00Z UTC"), so the
only correct comparator is a wall-clock source. A monotonic clock would
drift relative to wall time across the lifetime of a session — most
importantly across a JVM pause or a leap-second smear — and would either
close sessions early (false-positive) or honour stale tokens past their
real expiry (security regression). The hard-close at `exp` and the
soft-warn at `exp − 60s` therefore both read from `EpochNanoClock` and
inherit the PTP/chrony accuracy budget documented above. Monotonic
`NanoClock` remains correct for elapsed-time concerns (rate limiters,
heartbeat tracking, drain-cycle latency) — not for absolute wall-clock
deadlines.

## Cross-clock latency in browser

Browser code measuring "publisher-to-render" latency must be careful: the
DOM `performance.now()` API returns milliseconds since
`performance.timeOrigin` (page load), **not** since the Unix epoch. Mixing
a server-side epoch-nanosecond `serverNanos` with raw `performance.now()`
yields a meaningless delta. The cross-clock fix lives in
`web-ui/src/workers/marketDataConflation.ts` — `nowEpochMillis` defaults to
`performance.timeOrigin + performance.now()`, which IS both monotonic
**and** epoch-based (`performance.timeOrigin` is a constant epoch-ms value
established at page load). See the field-level Javadoc on
`nowEpochMillis` for the full rationale (added in Gemini review iter-2 of
APP-244 Phase 3 Commit B). The same cross-clock concern applies to any
browser-resident latency probe added later — always anchor wall-time
comparisons to `performance.timeOrigin + performance.now()`, never to bare
`performance.now()` or `Date.now()` (the latter is subject to system clock
jumps).

## APP-62 §5 fat-finger staleness vs PTP drift

`NewOrderSingleHandler.LAST_PRICE_STALENESS_NANOS` (default 5 minutes) is the
window during which a cached `lastQuotedMidPrice` is considered usable as a
fat-finger reference. The check is

```
haveReference = lastTs <= clusterTimestamp
             && (clusterTimestamp - lastTs) <= LAST_PRICE_STALENESS_NANOS
```

where `lastTs` is the cluster timestamp at which the `PriceResponse` event was
applied. The price flows pricing-service → cluster, both running on their own
boxes — so `lastTs` and `clusterTimestamp` are produced by different OS
clocks. The 5-minute knob MUST exceed the worst-case PTP cross-box clock skew
between the pricing-service host and the cluster leader, otherwise legitimate
fresh quotes appear stale relative to the receiving cluster's clock and trip
the fail-closed `RejectReasonEnum.PriceTooFarFromMarket` path.

Typical PTP skew budgets for this comparison:

| Topology                                      | Expected skew       | Headroom vs 5 min      |
| --------------------------------------------- | ------------------- | ---------------------- |
| Single rack, PTP hardware timestamping        | < 1 microsecond     | 8 orders of magnitude  |
| Same data centre, PTP via switch              | 1-10 microseconds   | 7+ orders of magnitude |
| Cross-AZ within a region                      | 1-10 milliseconds   | 5+ orders of magnitude |
| Cross-region (chrony fallback, internet path) | 10-100 milliseconds | 3+ orders of magnitude |

The 5-minute default is therefore safe for every supported deployment
topology by a wide margin. **Do not tune `LAST_PRICE_STALENESS_NANOS` down
below the worst expected cross-box skew** — that would convert clock drift
into fat-finger rejects of legitimate flow. If a desk genuinely needs sub-
minute staleness (HFT cash-equity context, for example), the operational
prerequisite is PTP hardware timestamping (sub-microsecond skew) — not a
software knob change.

The reverse case — `LAST_PRICE_STALENESS_NANOS` set too high — silently
weakens §5: a stale reference that drifts arbitrarily far from the true
market still passes the band check. The default of 5 minutes balances PTP
headroom against the operational expectation that pricing-service publishes
at least one quote per symbol per minute under normal conditions.
