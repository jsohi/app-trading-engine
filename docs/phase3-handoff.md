# Phase 3 Handoff — Full-Stack E2E

**Branch:** `feat/app-237-phase3-egress-gap` (from `main` @ `70d4def`).
**Linear parent:** APP-237 (per-account entitlement / AccountProjection / panel layouts). Adjust if a better-fitting ticket exists in `.linear-allowlist` (allowed: APP-31, APP-160, APP-237, APP-245).
**Source plan:** `/Users/jasandeepsingh/.claude/plans/plan-e2e-test-with-enchanted-truffle.md` — sections "Phase 3 — Closing the Egress Gap", "Phase 3 — Compliance + Completeness Addendum", and "Foolproof Full-Stack E2E Contract" are the authoritative spec.

## What landed in APP-37 PR (now on main)

- 3-node Aeron Cluster + Artio FIX gateway + pricing-service + websocket-server + Vite + Playwright Chromium with mkcert SPKI-pinning + RS256 JWT (multi-issuer + OIDC discovery).
- `EventSink.emit` broadcasts to every `cluster.clientSessions()` via the synchronous-iteration `forEachClientSession` contract; `FakeCluster` mirrors it; tests register sessions via `addClientSession`.
- `OidcDiscoveryClient` with RFC 8414 §3 host-match + HTTPS-only.
- `commandClient` + `useOrderSubmission` + `OrderEntryForm` (browser→cluster command path skeleton; APP-160).
- E2E scripts (`scripts/full-stack-e2e.sh`), Playwright `playwright.full-stack.config.ts`, 8 numbered spec files (most still `.fixme`'d).
- JCStress source set + `ReliableStreamTrackerReconnectTest`.
- Gradle `fullStackE2e` task wiring + CI workflow.

## Carry-over from APP-37 (Gemini-found bugs that DID NOT land on main — fix these first)

These were in branch commits `d7c3b71` → `7ab8be5` (rebased away or never merged). They are **real bugs** that the reviewer flagged; the production code on main still has them. Re-apply them as the first commits on the new branch:

1. **`commandClient.ts` — hardcoded `settlDate: "20260518"`** → replace with `spotSettlementDate(now)` helper computing T+2 with Sat/Sun skip; weekend-only (US bank holidays = APP-237 production-tier follow-up).
2. **`commandClient.ts` — hardcoded `accountCode: "ACME-001"`** → make `accountCode` a required field on `NewOrderSinglePayload`; thread through from `OrderEntryForm` prop. Use a thin `OrderEntryFormPanel` wrapper to bind `DEFAULT_DEV_ACCOUNT_CODE` for the panel-registry mount-site (single named substitution point so audit-grep finds every non-authenticated account).
3. **`commandClient.ts` — hardcoded `currency: "USD"`** → derive from quote-currency slot (`canonicalSymbol.slice(3, 6)`); use `replaceAll("/", "")` defensively (not `replace`).
4. **`commandClient.ts` — two independent wall-clock samples** (`Date.now()` for `transactTime` + `new Date()` for `settlDate`) → sample once: `const nowMs = Date.now(); const nowDate = new Date(nowMs);` so a midnight-UTC tick can't produce a 1-business-day skew.
5. **`commandClient.ts` — also reject empty + whitespace-only `accountCode`** with a typed `InvalidAccountCodeError` synchronously before any worker traffic.
6. **`commandClient.ts` — outboundPool sized to `MAX_IN_FLIGHT` (256) but indexed by `seq & SLOT_MASK` (0..1023)** → resize pool to `SLOT_COUNT (1024)`; remove the `?? new Uint8Array(...)` silent-allocation fallback. Throw on missing slot.
7. **`workerClient.ts` — `submitCommand` uses Transferable** → switch to structured-clone so the pooled buffer stays attached (matches the documented zero-alloc-after-warmup contract; verified by the existing alloc tripwire).
8. **`worker.ts` `COMMAND_FRAME` length validator** → also reject envelopes whose underlying ArrayBuffer has been detached.
9. **`OidcDiscoveryClient.java` — `BodyHandlers.ofByteArray()` buffers entire response before checking `MAX_RESPONSE_BYTES` cap** → switch to `BodyHandlers.ofInputStream()` + bounded `readBoundedFully` that aborts at 64 KiB. Throw `OidcDiscoveryResponseTooLarge` on overflow. Redact query/fragment from URIs in exception messages.
10. **`WebSocketLauncher.java` `parse*` enum helpers** → add null guards (`Enum.valueOf(null)` throws NPE not IAE), and case-insensitive scan via `parseEnumOrNull<E>` generic helper backed by `ClassValue<Enum<?>[]>` (SBE codec emits PascalCase like `Active`; uppercase normalization breaks the canonical YAML form). `Locale.ROOT` on both sides for Turkish-i hazard.
11. **`E2EFixTestClient.java` `messageQueue.offer()`** → check return value, set static `AtomicBoolean CAPTURE_QUEUE_OVERFLOWED`, and surface as `EXIT_EXCEPTION` with a clear log line instead of an opaque `RESPONSE_TIMEOUT 30s later`.
12. **`scripts/dev-jwks-server.mjs`** — default `host` = `127.0.0.1` (not `0.0.0.0`); add `--public-host` CLI flag.
13. **`ReliableStreamTrackerCaptureReplayJCStress.java`** — Javadoc enumeration says `(1, 64)` / `(-1, 0)` but `@Outcome` ids are `(0, 0)` / `(64, 192)`. Reconcile.
14. **`.github/workflows/full-stack-e2e.yml`** — read Playwright version from `package-lock.json` (node_modules doesn't exist at that step).

Each comes with corresponding test coverage that should also be re-applied (e.g. `spotSettlementDate.test.ts`, `WebSocketLauncherEnumParseTest.java`, currency-derivation SBE-byte regression cases in `commandClient.test.ts`). Re-derive from the prior branch's diff: `git diff 70d4def..7ab8be5` (was `feat/app-37-full-stack-e2e`'s post-merge HEAD).

## Phase 3 — Gap 1: worker `onEvent` decoders (blocks specs 03, 04, 05, 06.1/.2/.3, 07)

**Problem:** `web-ui/src/workers/worker.ts` `onEvent` is a no-op for every domain event template. Server-side delivery works end-to-end; the drop is `void templateId;` in the worker dispatcher.

**Fix:**

- Implement decoders for templates **100** (OrderCreatedEvent), **101** (OrderRejectedEvent), **102** (OrderFilledEvent), **103** (OrderCanceledEvent), **112** (lifecycle), **204** (Position).
- Each decoder constructs a typed `WorkerMessage` matching `web-ui/src/shared/transport/MessageShape.ts` and appends to the existing `outboundBatch`.
- Validate `schemaId`/`version == 1` before constructing.
- **Template 51** (`PriceResponse`) is orchestrator-bound, NEVER browser-bound — route to `onUnexpectedServerTemplate` + `marketDataMisroutedRfq` metric (regression guard for the §Gap 2 deliberate separation).
- New unit test: `web-ui/test/unit/workers/worker-onEvent.test.ts` round-trips each template.

## Phase 3 — Gap 2: browser-facing market-data feed (blocks spec 02)

**Problem:** Pricing service today only handles RFQ request/response (template 51, orchestrator-bound). No broadcast feed for browser PriceBlotter.

**Schema (NEW templates in pricing-broadcast subrange 54-59):**

- `MarketDataTick` (id=54) — symbol/bidPx/askPx/bidSize/askSize/symbolSeq/serverNanos.
- `MarketDataHeartbeat` (id=55) — serverNanos only; emitted every 1s when feed is quiet.
- `MarketDataSnapshotRequest` (id=56) — issued by ws-server on first symbol subscribe.
- `MarketDataFeedStateChange` (id=57) — synthetic `LIVE/QUIET/STALE` transitions.

**CLAUDE.md amendment** — extend the FIX-tag carve-out from `60-72` to also cover `54-59` (in same docs PR). Field IDs reuse FIX where possible: `symbol=55`, `bidPrice=132`, `askPrice=133`, `bidSize=134`, `askSize=135`, `symbolSeq=83`, `serverNanos=52`.

**Pricing-service:**

- New `MarketDataPublisher` + `MarketDataTickSlot` (composed-adapter pattern, single agent thread, zero alloc after construction).
- `Long2ObjectHashMap` conflation; `forEachLong` drain via final-field `LongObjConsumer` (NOT per-call lambda — would allocate SAM).
- `MARKET_DATA_PUBLISH_CADENCE_MICROS = 5_000` (5ms; tunable). Conflation gives ~5× wire compression on a 1000Hz symbol while always representing the latest top-of-book.
- Sanity rejects: crossed (`bid >= ask`), non-positive, unconfigured-spread.
- `snapshotForSymbol(packed)` re-uses the cached slot with `symbolSeq = 0` so client can distinguish snapshot from live.
- GFLog hot-path; rate-limited 1/sec/reason via primitive `long[]` reason-ordinal index.

**Websocket-server:**

- New `Subscription` on `aeron:ipc` stream **204** (separate from cluster egress).
- **One `FragmentAssembler` per `Subscription`** (sharing corrupts state).
- `AeronEgressThread` polls cluster-first (correctness-critical) then market-data (best-effort) via DWRR (cluster:market-data = 2:1 weight) — final design, NO TODO for "revisit fairness".
- `EGRESS_FRAGMENT_LIMIT = 10`, `MARKET_DATA_FRAGMENT_LIMIT = 32`.
- New `MarketDataSubscriptionLivenessTracker` — `LIVE / QUIET / STALE` state machine, 500 ms periodic check, emits `MarketDataFeedStateChange` (template 57).
- `SubscriptionFilter` — map id 54/55/57 → `BIT_PRICES`; **REMOVE** template 51 from `BIT_PRICES`; add `entitledSymbolsByAccount` (`org.agrona.collections.LongHashSet`) per-`matches()` final guard. Set-before-publication invariant; volatile field.
- New `SymbolEntitlementMap` loaded at startup from `integration-tests/e2e/data/symbols.yaml`.
- `WebSocketSession.initSubscriptionFilter` resets bitmap on resume; `EgressEntry` carries `sessionEpoch`; drain skips stale-epoch entries.
- `WebSocketFrameDispatcher.handleSubscribe` issues `MarketDataSnapshotRequest` (template 56 / stream 205) for newly-added subscription bits.

**Browser:**

- `worker.ts` decodes 54/55/57; per-symbol conflation via `Map<bigint, MarketDataTickFrame>`; 30 Hz drain via `setInterval(MARKET_DATA_RENDER_MS = 33)`. `performance.now() * 1e6` for `lastTickNs` (NOT `Date.now()`).
- New **`feedState$`** subject (separate from `connectionStream$`) — STALE feed must NOT trip the WS reconnect breaker.
- AG Grid `asyncTransactionWaitMillis = 16` pinned in `useGridStreamSink`.

**Per-account preferences (data layer; UI is APP-237 future):**

- Extend `WebSocketAuthAck` (template 61) with repeating groups `symbolPreferences` + `panelLayout`.
- `AccountRecord` + `AccountReadModel` carry the new fields; `YamlAccountLoader` parses; `WebSocketLauncher.toReadModel` propagates.
- `JwtAuthHandler.sendAuthAck` populates from resolved account.
- Worker subscribes to union of (`symbolPreferences` ∪ `DEFAULT_SUBSCRIBE_SYMBOLS`).
- `App.tsx` mounts `OrderEntryForm` into the slot named by `panelLayout`.

## Phase 3 — Compliance + Completeness Addendum (§A-EE)

These are required per the plan; do NOT defer:

- **§A** Always start with `/effort high`.
- **§B** CLAUDE.md FIX-tag carve-out extended to 54-59 (docs change in same PR as schema).
- **§C** Ticket hygiene — NO new Linear issues. Only APP-31, APP-160, APP-237, APP-245 in `.linear-allowlist`.
- **§D** Class-level Javadoc on every NEW class (purpose / threading / allocation / design rationale / dependencies).
- **§E** New `cluster/.../EventSinkAllocTest` — `*AllocTest` pattern, `ThreadMXBean.getThreadAllocatedBytes`, asserts byte-delta == 0 on the broadcast loop.
- **§G** `MarketDataSnapshotBurstTest` (1000 simultaneous snapshot requests; zero-alloc); snapshot timeout (`MARKET_DATA_SNAPSHOT_TIMEOUT_MS = 2000`); unconfigured-symbol sentinel; race-with-disconnect handling.
- **§H** JWT expiry mid-session — `WebSocketSession` records `exp`, periodic check; `4401 AuthExpiringSoon` warn at `exp - 60s`, `4401 AuthExpired` close at `exp`. Browser worker triggers in-session reauth. New `JwtSessionExpiryTest`.
- **§I** New `JwksRotationTest` — pre-rotation session stays valid until exp; new auth with old kid fails after rotation; new auth with new kid succeeds; exactly one refresh fetch per failure (no hammering).
- **§J** Egress channel back-pressure — `channel.isWritable()` check + drop counter + GFLog rate-limited + new `WebSocketDrainHandlerBackpressureTest`.
- **§K** Auth-complete vs first-subscribe race — `JwtAuthHandlerPipelineRaceTest` (Netty `EmbeddedChannel`) + `SubscriptionFilterAuthSubscribeRaceJCStress`.
- **§L** Session-resume vs prior-session cleanup race — `sessionEpoch` field, drain skip, counter, `WebSocketSessionResumeRaceTest`.
- **§M** Per-spec metrics assertion — every PASS verdict must verify a server- or client-side metric increment. Spec table in plan §M.
- **§N** Spotless / lint / typecheck for web-ui in CI.
- **§O** `ReliableStreamTrackerSeqNoWrapTest` exercises `Long.MAX_VALUE - 5` boundary.
- **§P** `commandClient` pool exhaustion — `inFlight >= POOL_SIZE` rejects with `BackpressureError("pool exhausted")`; `commandClient-pool-exhaustion.test.ts`.
- **§Q** Bundle-guard string list — add `feedState$`, `__connStates`, `__connStatesUnsub`, `MarketDataFeedStateChange`.
- **§R** JFR custom events — `trading.MarketDataTickPublished`, `trading.MarketDataTickRejected`, `trading.MarketDataFeedStateTransition`, `trading.CommandSubmitted`.
- **§S** OTel spans — server-side per-drain-cycle (out-of-hot-path); browser-side via `performance.mark`/`measure`.
- **§T** SBE codec checksum regen procedure documented.
- **§U** Boundary tests — empty subscribe, max symbols, max ClOrdID.
- **§V** Log4j2 disk-full handler — `Log4j2DiskFullErrorHandler` + `log.appender.failure` counter + test.
- **§W** Backwards-compat statement (pre-prod, additive only — no version bump).
- **§X** `MultiIssuerLauncherRebootArtioTest` — FIX session survives launcher reboot.
- **§Y** Spec 8 extended — `feedState$ === LIVE` + new tick within 5s post-reboot.
- **§Z** Pin Jetty `11.0.24` in `gradle/libs.versions.toml` (for `WebSocketServerConfigOidcDiscoveryTest`).
- **§AA** Architecture diagram update for Phase 3 (market-data IPC streams 204/205).
- **§BB** `TokenIsolationContexts.browser.test.ts` — two `BrowserContext`s, disjoint `__E2E_JWT_OVERRIDE__`, no shared module-state bleed.
- **§CC** Ship as ONE PR with the internal commit ordering at plan §CC commits 3.0–3.9.
- **§DD** `messageSource.ts` ↔ `e2eHooks.ts` coupling docs + `messageSource-eHooksContract.test.ts`.
- **§EE** Spotless `EnforceLinearTicketTodos` lint reading from `.linear-allowlist`; pre-commit hook `enforce-precommit-gate.sh` greps for placeholder strings.

## Outstanding small carry-over tasks (from the prior task list)

- **B3** `OrderBlotter` — register AG Grid `api` on `window.__ordersGridApi` (env-gated by `VITE_E2E_REAL_BACKEND === "true"`; bundle-guarded). Required by spec 05.
- **B2-a** `__E2E_JWT_OVERRIDE__` consumer in `devTokenProvider.ts` (production guard: ignore in PROD bundle; exists ONLY in dev mode).
- **B2-b** Un-fixme spec 8 (multi-issuer); per-context token via `context.addInitScript`.
- **B2-c** Un-fixme spec 6; add Throttle/Backpressure/Duplicate/Entitlement/Validation subtests.
- **NEW spec 09 — `feed-stale`** (Phase 3 §M) — kill pricing-service; assert `feedState$ === STALE` within 5s; restart; assert `LIVE` resumes within 1s.

## Verification gates (mandatory per orchestrate)

```bash
./gradlew spotlessApply                           # format
./gradlew spotlessCheck                           # CI gate
./gradlew test                                    # all unit tests
./gradlew :integration-tests:test                 # full RFQ + cluster lifecycle
./gradlew e2eClean e2e                            # 3-node cluster + FIX validation
./gradlew :websocket-server:jcstress              # concurrency stress
./gradlew :web-ui:bundleGuard                     # bundle leak grep + size budget
./gradlew fullStackE2e                            # the foolproof one-command gate
( cd web-ui && npm run lint && npx tsc --noEmit && npm test )
```

`./gradlew fullStackE2e` is the single authoritative end-of-Phase-3 gate. Per the plan's "Foolproof Full-Stack E2E Contract": real cluster + real gateway + real pricing-service + real ws-server + real Vite + real Chromium with mkcert SPKI-pinning + real RS256 JWT against local JWKS — zero stubs / fakes / mocks on the data path.

## Conventions to remember (CLAUDE.md highlights)

- `/effort high` always.
- `final var` for refs, `final <type>` for primitives — carve-out for buffer-scan pointers + classic for-loop counters.
- Cluster service is deterministic — no `java.util.*`, no wall-clock, no randomness, no heap alloc in hot path.
- GFLog for hot-path modules (cluster/gateway/pricing-service/orchestrator/projections); Log4j2 for infra (launcher/media-driver/websocket-server/reference-data/fix-client-bridge); no SLF4J.
- Pre-prod schema pin: `version="1"`, `semanticVersion="0.1.0"`. Add fields freely; no `sinceVersion=`.
- Fixed-point only: `long` × `100_000_000L` for prices/qty/amounts.
- Branch naming: `feat/app-{N}-short-description`. Commit prefix: `APP-{N}: ...`.
- Push: `LOCALLOOM_REVIEW_VERIFIED=1 LOCALLOOM_E2E_VERIFIED=1 LOCALLOOM_ORCHESTRATE_VERIFIED=1 make push` — bare `git push` is blocked.

## Suggested order of attack

1. **Cherry-pick the 14 carry-over fixes from APP-37** (Gemini + post-Gemini) — small commits, pure regressions, no design change.
2. **Schema delta first** (templates 54-57; CLAUDE.md FIX-tag carve-out amendment). Codec regen + `messages/build/generated-codec.sha256` regen + commit.
3. **Cluster `EventSink` alloc test** (just adds the regression guard; no functional change).
4. **Worker `onEvent` decoders (Gap 1)** — unblocks specs 03/04/05/06.1-3/07.
5. **Pricing-service `MarketDataPublisher` + tests + alloc test** (Gap 2 producer side).
6. **WS-server market-data subscription + livenessTracker + entitlement + session-epoch (Gap 2 consumer side).**
7. **Browser worker market-data decoders + `feedState$` + 30Hz drain (Gap 2 client side).**
8. **Snapshot-on-subscribe wiring (template 56 / stream 205) + burst test.**
9. **JWT expiry pre-emptive reauth + JWKS rotation test.**
10. **Spec 09 (feed-stale) + boundary tests + JFR custom events + OTel + Log4j2 disk-full + multi-issuer Artio reboot test + spec 8 post-reboot assertions.**
11. **Bundle-guard grep extension + docs/architecture diagram update.**
12. **Final `/orchestrate` to convergence + `make push`.**

## Pointers

- Plan: `/Users/jasandeepsingh/.claude/plans/plan-e2e-test-with-enchanted-truffle.md`
- Memory index: `/Users/jasandeepsingh/.claude/projects/-Users-jasandeepsingh-dev-git-ai-plan-app-trading-engine/memory/MEMORY.md`
- Diff to mine for carry-over fixes: `git diff 70d4def..origin/feat/app-37-full-stack-e2e -- '*'` (the original branch lives on origin if not pruned; otherwise extract from this session's commit history `cdc05a7 .. 7ab8be5`).
