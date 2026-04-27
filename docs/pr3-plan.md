# PR 3: Authentication + Entitlements + Filtering (APP-35)

## Context

PR 1 (schema+config) and PR 2 (core pipeline+TLS) are merged. The WebSocket server has 16 production classes with a working Netty pipeline (SSL, HTTP, WS upgrade, rate limiting, origin validation) and a drain handler that fans out all egress messages to all connected channels unfiltered with `seqNo=0`. PR 3 adds:
- JWT RS256 authentication gating WebSocket access
- Re-authentication (token refresh) before expiry
- Per-session subscription filtering (symbol + event type + account) to reduce O(M*S) drain fan-out
- Per-session reliable sequence numbers
- Account entitlement validation from JWT `accounts` claim
- Account-level event filtering on the drain path (architecture doc: "restrict events to entitled accounts")
- Symbol-level and no-symbol event routing with globalEventBitMask

Pipeline insertion point: `WebSocketServerMain.java:142` -- after `origin-validator`.

---

## New Production Classes (10)

### 1. `JwtValidator` -- RS256 JWT validation (thread-safe singleton)
**File:** `websocket-server/src/main/java/.../websocket/JwtValidator.java`

**Javadoc:** Purpose, threading (thread-safe via nimbus internal locks), allocation (JWKSource caches internally; ValidatedClaims record allocated per validation -- acceptable, not hot path).

- nimbus-jose-jwt `DefaultJWTProcessor` with `JWSAlgorithm.RS256` only
- Per-issuer `JWKSource` built via `JWKSourceBuilder.create(url).cache(3600s).rateLimited(true)` from config `issuerRegistry` map
- **JWKS forced-refresh on SignatureVerificationException**: on `BadJWSException`, force-refresh JWK set, retry ONCE, reject on second failure (architecture doc Section 4, go-live gate #11)
- **kid header validation**: reject tokens where `header.getKeyID() == null` to prevent key confusion during rotation
- `DefaultJWTClaimsVerifier` with 5s clock skew for exp/nbf, **required** audience match (no empty audience)
- Manual `iat` check: reject tokens issued >15 minutes ago
- **Token size limit**: reject raw JWT byte length > `config.maxTokenSizeBytes()` (default 8192) before any parsing
- Returns `ValidatedClaims` record: `(String sub, String jti, List<String> accounts, long expiryEpochSec)`
- Rejects: alg:none, HS256, ES256, unknown issuer, missing sub/jti/accounts claims, null kid, empty accounts list
- **Preflight JWKS fetch at construction**: for each issuer, attempt JWKS fetch; log ERROR on failure (fail-fast at startup)
- **JWKS endpoint hardening**: 5s connect timeout, 5s read timeout, require HTTPS scheme (reject `http://` URLs in config validation), disable HTTP redirects
- **Shutdown**: implement `AutoCloseable`; close all `JWKSource` instances on server shutdown

**Logging:** INFO: auth success with userId (never log token); WARN: auth failure with reason code + remoteIp (never log token); ERROR: JWKS fetch failure with issuer + endpoint

### 2. `JtiRevocationCache` -- Revoked JWT ID tracking
**File:** `websocket-server/src/main/java/.../websocket/JtiRevocationCache.java`

**Javadoc:** Purpose, threading (thread-safe via ConcurrentHashMap), allocation (String key per revocation -- acceptable, auth-time only).

- `ConcurrentHashMap<String, Long>` -- key=**full JTI string** (NOT hashCode -- 32-bit hashCode has ~1.2% collision probability at 10K entries; widening to long adds zero entropy), value=insertion nanoTime
- **TTL: `revocationTtlMinutes + 2` (17 minutes)** -- extends 2 minutes beyond 15-min token lifetime to cover clock skew gap
- `NanoClock` injected for testability
- `boolean isRevoked(String jti)` -- O(1) lookup + TTL check
- `void revoke(String jti)` -- add entry + lazy eviction of expired entries
- Fail-safe: if at capacity after eviction, `isRevoked()` returns true for ALL (rejects new tokens), log WARN
- Config: `maxRevokedJtis` (10K), `revocationTtlMinutes` (15min) -- TTL internally extended by 2 min
- `int size()` -- for monitoring/metrics

**Logging:** WARN on fail-safe activation (capacity full), WARN on eviction batch > 1000

### 3. `SubscriptionFilter` -- Per-session zero-alloc event filter
**File:** `websocket-server/src/main/java/.../websocket/SubscriptionFilter.java`

**Javadoc:** Purpose, threading (write from channel event loop, read from drain event loop via volatile publish), allocation (copy-on-write snapshot allocated on subscribe/unsubscribe -- not hot path; matches() is zero-alloc).

**Thread-safety -- copy-on-write with volatile publish:**
- **Mutable state** (channel event loop only): `Long2LongHashMap` (Agrona) -- key=packed symbol, value=eventTypes bitmask
- **Published snapshot** (drain thread reads): `volatile SubscriptionSnapshot snapshot` -- immutable record containing:
  - `long[] packedSymbols` -- sorted array of packed symbols
  - `long[] eventBitmasks` -- parallel array of event type bitmasks (same index)
  - `int globalEventBitMask` -- OR of all subscription bitmasks (for no-symbol template matching)
  - `int count` -- number of subscriptions
- On `addSubscription()` / `removeSubscription()`: mutate `Long2LongHashMap`, then rebuild and volatile-publish a new `SubscriptionSnapshot`. **The published snapshot is never mutated after publish -- this invariant is critical.**
- `matches(int templateId, byte[] sbePayload, int payloadOffset, int payloadLength)`:
  1. Read `volatile SubscriptionSnapshot snap` (single volatile read)
  2. If `snap.count == 0` -> return false (empty filter, nothing passes)
  3. Map templateId -> event bit via static lookup
  4. If templateId has no mapping -> return false (unknown template, drop)
  5. If `SymbolExtractor.extractPackedSymbol()` returns `UNKNOWN_SYMBOL` (templates 110, 111, 112, 204):
     -> return `(snap.globalEventBitMask & eventBit) != 0` (pass if ANY subscription includes this event type)
  6. Extract packed symbol via `SymbolExtractor.extractPackedSymbol(templateId, sbePayload, payloadOffset, payloadLength)` -- **with bounds check**
  7. Binary search `snap.packedSymbols[]` for the symbol -> if found, check `(snap.eventBitmasks[idx] & eventBit) != 0`
- `addSubscription(long packedSymbol, int eventTypes)` -- mask eventTypes with `VALID_EVENT_TYPES_MASK = 0x1F` to strip undefined bits (5-31); reject if undefined bits set
- `removeSubscription(long packedSymbol)`
- `clear()` -- remove all subscriptions, publish empty snapshot. Called on disconnect.
- `int subscriptionCount()` -- from snapshot
- `boolean isEmpty()` -- from snapshot count == 0

**Static mapping `templateIdToEventBit(int templateId)`:**
  - bit 0 (0x01): Orders -- 100, 101, 102, 103, **112** (OrderCancelRejected is order lifecycle)
  - bit 1 (0x02): Positions -- 204
  - bit 2 (0x04): Prices -- 51
  - bit 3 (0x08): Quotes -- 104, 105, 106, 107
  - bit 4 (0x10): Account events -- 110, 111
  - **-1: Internal events (108, 109, 113-116) -- never delivered to WebSocket clients**

### 4. `SymbolExtractor` -- Static symbol field extraction from egress SBE payloads
**File:** `websocket-server/src/main/java/.../websocket/SymbolExtractor.java`

**Javadoc:** Purpose, threading (stateless, thread-safe), allocation (zero -- uses `SymbolPacker.pack(byte[], offset)`).

Verified absolute offsets (header 8B + field encoding offset):
| TemplateId | Event | Symbol Offset |
|---|---|---|
| 100 | OrderCreated | 8 + 76 = 84 |
| 101 | OrderRejected | 8 + 36 = 44 |
| 102 | OrderFilled | 8 + 76 = 84 |
| 103 | OrderCanceled | 8 + 76 = 84 |
| 104 | QuoteRequested | 8 + 36 = 44 |
| 105 | QuoteCreated | 8 + 56 = 64 |
| 106 | QuoteRejected | 8 + 36 = 44 |
| 107 | QuoteExpired | 8 + 56 = 64 |
| 51 | PriceResponse | 8 + 20 = 28 |

No symbol field (return `UNKNOWN_SYMBOL = 0L`): 110, 111, 112, 204

- `static long extractPackedSymbol(int templateId, byte[] sbePayload, int offset, int length)`
- **Bounds check**: if `offset + symbolAbsoluteOffset + 8 > length`, return `UNKNOWN_SYMBOL` (prevents ArrayIndexOutOfBoundsException on truncated payloads -- failure of one message must not crash the drain loop for all sessions)
- Reuses: `SymbolPacker.pack(byte[], offset)` from projections module
- Switch statement on templateId (JIT compiles to lookupswitch -- sparse IDs 51, 100-107)

### 5. `AccountExtractor` -- Static account field extraction from egress SBE payloads
**File:** `websocket-server/src/main/java/.../websocket/AccountExtractor.java`

**Javadoc:** Purpose (extracts account code from SBE events for account-level entitlement filtering on drain path), threading (stateless, thread-safe), allocation (allocates String per extraction -- acceptable, happens only when SubscriptionFilter matches and account check is needed).

Architecture doc requires: "SubscriptionFilter + UserEntitlementService restrict events to entitled accounts." This class extracts the account field from events that have one, so the drain handler can check `session.entitledAccounts().contains(account)`.

Account field offsets (SBE header 8B + accountCode encoding offset, 16-byte fixed char[]):
| TemplateId | Event | Account Offset | Account Length |
|---|---|---|---|
| 100 | OrderCreated | 8 + accountEncodingOffset | 16 |
| 101 | OrderRejected | 8 + accountEncodingOffset | 16 |
| 102 | OrderFilled | 8 + accountEncodingOffset | 16 |
| 103 | OrderCanceled | 8 + accountEncodingOffset | 16 |
| 104 | QuoteRequested | 8 + accountEncodingOffset | 16 |
| 105 | QuoteCreated | 8 + accountEncodingOffset | 16 |
| 106 | QuoteRejected | 8 + accountEncodingOffset | 16 |
| 107 | QuoteExpired | 8 + accountEncodingOffset | 16 |

No account field: 51 (PriceResponse -- prices are not account-specific), 110/111 (account IS the event), 112 (no account field), 204 (account in repeating group).

- `static String extractAccountCode(int templateId, byte[] sbePayload, int offset, int length)` -- returns trimmed account code string or `null` if template has no account field or payload is truncated
- **Bounds check**: same pattern as SymbolExtractor
- **Note:** Account field offsets must be verified from generated SBE decoders (like SymbolExtractor offsets were verified). Implementation step: grep `accountCodeEncodingOffset()` from each generated decoder.

### 6. `UserEntitlementService` -- Account entitlement validation
**File:** `websocket-server/src/main/java/.../websocket/UserEntitlementService.java`

**Javadoc:** Purpose, threading (thread-safe via QueryService's StampedLock reads), allocation (Set allocation per auth -- acceptable).

- Wraps `QueryService.getAccountByCode(String)` -> `AccountReadModel`
- `Set<String> validateAccounts(List<String> accountCodes)` -> set of active account codes
- Validation: account exists AND `status == Active`
- **Reject if validated set is empty**: auth must fail with `AuthorizationFailed` if all accounts are inactive/unknown or if the accounts claim is empty
- No caching in PR 3 (auth-time only, not hot path)
- **Symbol-level entitlement**: `// TODO(APP-236): add symbol-level subscription entitlement check. Currently any authenticated user can subscribe to any symbol. Architecture doc's entitlement model is account-based (JWT accounts claim), not symbol-based. Symbol ACLs require a separate symbol-entitlement mapping not yet in the data model.`

**Logging:** INFO: validated N/M accounts for userId; WARN: account rejected (code + reason)

### 7. `JwtAuthHandler` -- One-shot Netty auth handler (per-channel)
**File:** `websocket-server/src/main/java/.../websocket/JwtAuthHandler.java`

**Javadoc:** Purpose, threading (per-channel, Netty event loop only), allocation (ExpandableArrayBuffer pre-sized to 128 bytes; SBE encoders per-channel), lifecycle (one-shot -- removes self after successful auth, adds WebSocketFrameDispatcher).

- `extends ChannelInboundHandlerAdapter`, NOT `@Sharable`
- **Pre-auth connection tracking**: shared `AtomicInteger pendingAuthCount` (across all channels), incremented on `channelActive`, decremented on auth success/failure/close. Reject new connections when `pendingAuthCount >= config.maxPendingAuth()` (default 64). Prevents FD exhaustion from unauthenticated connection floods.
- **Double-decrement guard**: `volatile boolean authResolved` flag per handler instance. Set to true on first resolution (success or failure). `channelInactive` only decrements if `!authResolved`. CAS not needed since per-channel handler is single-threaded.
- **Auth timeout**: schedule **5s** timeout (matches architecture doc Section 3/4) via `ctx.executor().schedule()`. Store `ScheduledFuture<?>` as field. Cancel on: auth success, auth failure, `channelInactive()`. On timeout: send `WebSocketError(AuthenticationFailed)`, close, decrement pendingAuth.
- `channelRead(BinaryWebSocketFrame)`:
  1. Token size check: if frame content length > `config.maxTokenSizeBytes()`, reject immediately (no parsing)
  2. Decode SBE header -> reject if templateId != 60
  3. Decode `WebSocketAuth`: extract protocolVersion, token bytes
  4. **Protocol version validation**: if `protocolVersion != EXPECTED_PROTOCOL_VERSION`, send `WebSocketError(VersionMismatch)`, close
  5. **Per-IP lockout check**: `authFailureTracker.isBlocked(remoteIp)` -> reject without JWT validation
  6. Convert token bytes to String (UTF-8)
  7. **Auth latency timer start**: `metrics.authLatency().record(() -> { ... })`
  8. Validate via `jwtValidator.validate(tokenString)`
  9. **JTI revocation check**: `jtiCache.isRevoked(claims.jti())` -- reject if revoked
  10. **Empty/all-inactive accounts check**: `entitlementService.validateAccounts(claims.accounts())` -> reject with `AuthorizationFailed` if empty
  11. Session registration: `sessionManager.tryRegister(channel)` -> reject if null (global capacity)
  12. Set userId: `sessionManager.setUserId(session, claims.sub())` -> reject if false (per-user limit)
  13. Store JTI: `session.jti(claims.jti())`; **revoke** old JTI in cache (prevents replay)
  14. Store entitlements: `session.entitledAccounts(validatedAccounts)`
  15. Init subscription filter: `session.initSubscriptionFilter(config.maxSubscriptionsPerClient())`
  16. Cancel auth timeout
  17. Send `WebSocketAuthAck(session.sessionId(), EXPECTED_PROTOCOL_VERSION, config.maxSubscriptionsPerClient())`
  18. Add `WebSocketFrameDispatcher` to pipeline after this handler
  19. Remove self from pipeline
  20. Set `authResolved = true`, decrement `pendingAuthCount`
  21. `metrics.authSucceeded()`
- On ANY failure: single error response `WebSocketError(AuthenticationFailed)` -- **no distinguishing error codes to client** (prevents auth oracle). Different reason codes in server logs only. Set `authResolved = true`, decrement `pendingAuthCount`, `metrics.authFailed()`, `authFailureTracker.recordFailure(remoteIp)`.
- `channelInactive(ctx)`: cancel auth timeout if pending; if `!authResolved`, decrement `pendingAuthCount`

**Logging:** INFO: auth success (userId, sessionId, accountCount); WARN: auth failure (reason, remoteIp); ERROR: unexpected exceptions. **Never log JWT token, claims content, or account codes.**

### 8. `WebSocketFrameDispatcher` -- Post-auth frame router (per-channel)
**File:** `websocket-server/src/main/java/.../websocket/WebSocketFrameDispatcher.java`

**Javadoc:** Purpose, threading (per-channel, Netty event loop only), allocation (SBE decoders reused per-channel; UnsafeBuffer wraps ByteBuf.nioBuffer() -- valid only during channelRead).

- `extends ChannelInboundHandlerAdapter`, NOT `@Sharable`
- Decodes SBE `MessageHeader` -> routes by templateId:
  - **60 -> `handleReAuth`**: re-authentication before token expiry. Validate new JWT, verify `sub` matches existing session, **revoke superseded JTI** in cache, refresh entitlements via `entitlementService.validateAccounts()`, update session `jti` and `entitledAccounts`. Reject if sub mismatch.
  - 62 -> `handleSubscribe`: decode `WebSocketSubscribe`, **mask eventTypes with 0x1F** (reject if undefined bits set), enforce `maxSubscriptionsPerClient`, add to `SubscriptionFilter`
  - 63 -> `handleUnsubscribe`: decode `WebSocketUnsubscribe`, remove from filter. **Empty symbols group = unsubscribe all** (`filter.clear()`)
  - 65 -> `handleClientHeartbeat`: update `session.updateHeartbeat(nanoClock.nanoTime())`
  - 68 -> `handleGapRequest`: log + stub `// TODO(APP-35): implement with ReliableStreamTracker in PR 4`
  - 69 -> `handleSessionResume`: log + stub `// TODO(APP-35): implement session resume with ReliableStreamTracker in PR 4`
  - 71 -> `handleClientAck`: update `session.lastClientCmdSeqNo()`
  - default -> **log WARN + ignore** (not close -- forward compatibility with future protocol versions). Increment `unknownTemplateId` counter. Close only after 3 consecutive unknowns from same session.
- Reusable SBE decoders as fields -- `wrap()` on `UnsafeBuffer` backed by `ByteBuf.nioBuffer()`, valid only within `channelRead()` scope
- **SBE decoder lifecycle**: decoders are re-wrapped per message via `decoder.wrapAndApplyHeader(buffer, offset, headerDecoder)`. No state leaks between messages.

**Logging:** DEBUG: subscribe/unsubscribe (symbol count); WARN: unknown templateId; INFO: re-auth success

### 9. `AuthFailureTracker` -- Per-IP auth failure rate limiting
**File:** `websocket-server/src/main/java/.../websocket/AuthFailureTracker.java`

**Javadoc:** Purpose (DDoS mitigation for brute-force JWT attacks), threading (thread-safe via ConcurrentHashMap + AtomicInteger), allocation (one entry per unique IP with failures).

- `ConcurrentHashMap<String, FailureRecord>` where `FailureRecord` contains `AtomicInteger failureCount` + `long lastFailureNs` + `long lockoutUntilNs`
- **Max capacity: 16384 entries** with LRU eviction on insert when full (prevents unbounded growth from botnet with millions of unique IPs). Eviction: remove the entry with oldest `lastFailureNs`.
- `boolean isBlocked(String ip)` -- check if IP is in lockout period
- `void recordFailure(String ip)` -- increment failure count, apply lockout if threshold exceeded
- Lockout policy: 5 failures within 60s -> 60s lockout; resets after lockout expires
- Lazy eviction of stale entries (>5 min since last failure)
- Config: `authFailureLockoutThreshold` (default 5), `authFailureLockoutSeconds` (default 60)

### 10. `AccountExtractorTest` -- (test class, but AccountExtractor is the 10th production class)

See `AccountExtractor` in class #5 above.

---

## Modifications to Existing Classes (7)

### 11. `WebSocketSession` -- Add subscription + entitlement fields
- New field: `SubscriptionFilter subscriptionFilter`
- New field: `Set<String> entitledAccounts` (immutable, set at auth time)
- **Change field**: `jti` from `long` to `String` (full JTI for collision resistance)
- New method: `initSubscriptionFilter(int maxSubscriptions)` (called by JwtAuthHandler)
- Accessors: `subscriptionFilter()`, `entitledAccounts()`, `entitledAccounts(Set<String>)`, `jti()` returns `String`
- **On disconnect**: `subscriptionFilter.clear()` (architecture doc: grace period does NOT hold subscriptions)

### 12. `WebSocketServerConfig` -- Add new config fields
- `String jwtAudience` -- **required, no default** (fail startup if unconfigured). Log WARN if empty in dev profile. YAML key: `jwtAudience`
- `int maxTokenSizeBytes` -- default 8192, range [256, 65536]. YAML key: `maxTokenSizeBytes`
- `int maxPendingAuth` -- default 64, >= 1. YAML key: `maxPendingAuth`
- `int authFailureLockoutThreshold` -- default 5. YAML key: `authFailureLockoutThreshold`
- `int authFailureLockoutSeconds` -- default 60. YAML key: `authFailureLockoutSeconds`
- **Validation**: `jwtAudience` must be non-empty; `maxTokenSizeBytes` in [256, 65536]; `maxPendingAuth >= 1`
- **issuerRegistry validation**: all JWKS URLs must use `https://` scheme; reject `http://`. Must have at least 1 entry (empty registry = server can never authenticate anyone).

### 13. `WebSocketServerMain` -- Wire auth handler into pipeline
- New constructor params: `JwtValidator`, `JtiRevocationCache`, `UserEntitlementService`, `AuthFailureTracker`
- In `initChannel()` at line 142: add `JwtAuthHandler` after `origin-validator`
- `WebSocketFrameDispatcher` is NOT added here -- `JwtAuthHandler` dynamically adds it on auth success
- **Shutdown**: close `JwtValidator` (releases JWKS HTTP clients)

### 14. `WebSocketSessionManager` -- Add direct iteration method
- **New method**: `Iterable<WebSocketSession> sessions()` -- returns `sessions.values()` directly for for-loop iteration in drain handler. Avoids `Consumer<>` lambda allocation per drain cycle.
- Existing `forEachSession(Consumer)` retained for backward compatibility.

### 15. `WebSocketDrainHandler` -- SubscriptionFilter + per-session seqNo + account filtering

Key refactor of `writeToAllChannels()` into two methods. **Use `for` loop over `sessionManager.sessions()` (NOT lambda)** to avoid per-message Runnable/lambda GC pressure.

**`writeReliableToAllChannels(EgressEntry entry)`:**
- For each session via `for (final var session : sessionManager.sessions())`:
  - Guard: `session.subscriptionFilter() == null` -> skip (pre-auth)
  - Guard: `!session.subscriptionFilter().matches(templateId, bytes, 0, length)` -> skip; `metrics.filterFiltered()`
  - **Account entitlement check**: `AccountExtractor.extractAccountCode(templateId, bytes, 0, length)` -> if non-null, check `session.entitledAccounts().contains(accountCode)` -> skip if not entitled
  - Allocate per-session ByteBuf (17B header + payload) via `PooledByteBufAllocator`
  - `FrameParser.encodeReliable(buf, session.nextReliableSeqNo(), entry.bytes(), 0, entry.length())`
  - **try/finally**: on success, write `BinaryWebSocketFrame(buf)`; on exception, `buf.release()` to prevent leak
  - `metrics.filterMatched()`
- **Design note on CompositeByteBuf**: Considered separating header from payload via CompositeByteBuf to avoid N payload copies. Rejected because CRC32C must be recomputed per session (different seqNo -> different header -> different CRC), so payload bytes are hashed N times regardless. CompositeByteBuf overhead (object creation, component management, 2x ref counting) exceeds memcpy savings for typical SBE payloads (~100-200 bytes). Plain per-session ByteBuf with full `encodeReliable()` is simpler and correct. At 256 sessions * 200B avg payload = ~50KB per reliable message -- acceptable.
- Flush at end of drain cycle via `for` loop (not lambda)

**`writeBestEffortToAllChannels(EgressEntry entry)`:**
- Shared ByteBuf with `retainedDuplicate()` (seqNo=0 for all) -- unchanged pattern
- Add SubscriptionFilter + account entitlement checks before writing
- Guard: skip sessions without SubscriptionFilter
- Guard: skip unwritable channels (existing behavior)
- `metrics.filterMatched()` / `metrics.filterFiltered()` as appropriate

### 16. `WebSocketLauncher` -- Wire new dependencies
- Add `QueryService` parameter to `launch()`
- Create: `JwtValidator`, `JtiRevocationCache`, `UserEntitlementService`, `AuthFailureTracker`
- Pass to `WebSocketServerMain` constructor
- Update `TradingEngineLauncher` call site to pass `QueryService`
- Add JwtValidator to `WebSocketComponents` for shutdown

### 17. `WebSocketMetrics` -- Add new metrics
- `websocket.subscriptions.active` Gauge (AtomicInteger) -- total active subscriptions across all sessions
- `websocket.auth.latency` Timer -- JWT validation latency (includes JWKS fetch on cache miss)
- `websocket.filter.matched` Counter -- messages that passed SubscriptionFilter
- `websocket.filter.filtered` Counter -- messages that were filtered out by SubscriptionFilter
- `websocket.auth.lockout` Counter -- connections rejected due to per-IP lockout

---

## Dependencies to Add

- `implementation(project(":projections"))` in `websocket-server/build.gradle.kts` -- required for `SymbolPacker` (Gradle `implementation` scope in `query-service` does NOT expose transitively)

---

## Test Infrastructure

### `SbeTestEncoder` additions (test-support module)
Add 7 static encode methods following existing pattern `(MutableDirectBuffer dst, int offset, ...)`:
- `encodeWebSocketAuth(dst, offset, protocolVersion, tokenBytes)` -> int encodedLength
- `encodeWebSocketSubscribe(dst, offset, String[] symbols, int[] eventTypes)` -> int (repeating group: encode group header with count, then per entry: 8-byte symbol + 4-byte eventTypes)
- `encodeWebSocketUnsubscribe(dst, offset, String[] symbols)` -> int (repeating group: encode group header with count, then per entry: 8-byte symbol)
- `encodeClientHeartbeat(dst, offset, clientNanos)` -> int
- `encodeClientAck(dst, offset, lastReceivedSeqNo)` -> int
- `encodeSessionResume(dst, offset, long msBits, long lsBits, lastSeqNo)` -> int
- `encodeWebSocketGapRequest(dst, offset, fromSeqNo, toSeqNo)` -> int

---

## New Test Classes (9)

| # | Test Class | Key Test Cases |
|---|---|---|
| 1 | `JwtValidatorTest` | RS256 valid, alg rejection (none/HS256/ES256), expired, nbf future, iat >15min, unknown issuer, missing claims (sub/jti/accounts), 5s clock skew accepted, null kid rejected, empty accounts rejected, token size limit, JWKS forced-refresh on bad signature, **audience validation required** |
| 2 | `JtiRevocationCacheTest` | Revoke+check, TTL expiry (17-min extended), capacity overflow, fail-safe reject-all, size tracking, full JTI string key (no collision) |
| 3 | `SubscriptionFilterTest` | Symbol+eventType match, wrong event type, unsubscribed symbol, add/remove, subscription count, empty filter returns false, **0xFFFFFFFF bitmask masked to 0x1F**, undefined bits rejected, clear(), globalEventBitMask for no-symbol templates (110/111/112/204), **template 112 in order lifecycle bit**, binary search correctness, volatile snapshot publish verified |
| 4 | `SymbolExtractorTest` | Extract from each templateId (100-107, 51), no-symbol templates (110, 111, 112, 204), **truncated payload returns UNKNOWN_SYMBOL**, zero-length payload, exact boundary |
| 5 | `AccountExtractorTest` | Extract from each templateId with account (100-107), no-account templates (51, 110, 111, 112, 204), truncated payload returns null |
| 6 | `UserEntitlementServiceTest` | Active account validated, suspended excluded, closed excluded, unknown excluded, **empty accounts list rejected**, **all inactive rejected** |
| 7 | `JwtAuthHandlerTest` | EmbeddedChannel: valid auth sends AuthAck + adds dispatcher, invalid token sends error + closes, **5s auth timeout** fires + closes, per-user limit exceeded, revoked JTI rejected, non-auth templateId rejected, **protocol version mismatch**, **token size exceeded**, **pendingAuth limit**, **per-IP lockout**, channelInactive cancels timeout, **authResolved double-decrement guard**, **metrics.authFailed() called on rejection** |
| 8 | `WebSocketFrameDispatcherTest` | EmbeddedChannel: subscribe adds to filter, unsubscribe removes, **re-auth (templateId=60) refreshes entitlements**, clientHeartbeat updates session, clientAck updates seqNo, unknown templateId warns (not closes), **subscription limit enforced**, **undefined eventTypes bits rejected**, **empty unsubscribe clears all** |
| 9 | `AuthFailureTrackerTest` | Record failures, lockout after threshold, lockout expiry, stale entry eviction, concurrent access, **max capacity with LRU eviction** |

Updated existing tests:
- `WebSocketDrainHandlerTest` -- filter matching, filter miss, per-session seqNo, **account entitlement check**, best-effort shared ByteBuf, no-filter skip, **ByteBuf leak detection (PARANOID)**, **metrics.filterMatched/filterFiltered incremented**, **for-loop iteration (no lambda)**
- `WebSocketSessionTest` -- new fields (subscriptionFilter, entitledAccounts, jti as String), clear on disconnect
- `WebSocketServerMainTest` -- expanded constructor, pipeline wiring verification
- `WebSocketServerConfigTest` -- new fields validation (jwtAudience required, maxTokenSizeBytes range, HTTPS-only issuer URIs, non-empty issuerRegistry)
- `WebSocketSessionManagerTest` -- new `sessions()` iterable method

---

## Implementation Order

0. `websocket-server/build.gradle.kts` -- add `:projections` dependency (required by steps 1-2)
1. `SymbolExtractor` + `SymbolExtractorTest` (no deps beyond SymbolPacker)
2. `AccountExtractor` + `AccountExtractorTest` (same pattern, needs SBE decoder offset verification)
3. `SubscriptionFilter` + `SubscriptionFilterTest` (depends on SymbolExtractor + SymbolPacker)
4. `JtiRevocationCache` + `JtiRevocationCacheTest` (no deps)
5. `AuthFailureTracker` + `AuthFailureTrackerTest` (no deps)
6. `JwtValidator` + `JwtValidatorTest` (nimbus-jose-jwt only)
7. `UserEntitlementService` + `UserEntitlementServiceTest` (QueryService)
8. `WebSocketSession` modifications + updated tests
9. `WebSocketServerConfig` modifications + updated tests
10. `WebSocketSessionManager` modifications (add `sessions()`) + updated tests
11. `WebSocketMetrics` modifications
12. `SbeTestEncoder` additions (test infrastructure)
13. `JwtAuthHandler` + `JwtAuthHandlerTest`
14. `WebSocketFrameDispatcher` + `WebSocketFrameDispatcherTest`
15. `WebSocketDrainHandler` modifications + updated tests
16. `WebSocketServerMain` modifications + updated tests
17. `WebSocketLauncher` + `TradingEngineLauncher` modifications
18. Update `docs/websocket-architecture.md` -- mark PR 3 components as implemented, document 5s auth timeout, AuthFailureTracker, copy-on-write SubscriptionFilter, account-level drain filtering

---

## Key Design Decisions

1. **SubscriptionFilter thread safety**: Copy-on-write with volatile publish. Channel event loop mutates `Long2LongHashMap`, rebuilds immutable `SubscriptionSnapshot` (sorted long[] arrays + globalEventBitMask), publishes via volatile write. Drain thread reads snapshot via single volatile read -- zero synchronization, zero allocation on read path. **Critical invariant: published snapshots are never mutated after volatile-publish.**

2. **Per-session reliable encoding -- plain ByteBuf (CompositeByteBuf rejected)**: CompositeByteBuf was considered to share payload across sessions but rejected because CRC32C must be recomputed per session (different seqNo -> different header -> different CRC -> payload hashed N times regardless). Plain per-session ByteBuf with full `FrameParser.encodeReliable()` is simpler, correct, and the memcpy overhead for ~200B SBE payloads is negligible. At 256 sessions this is ~50KB per reliable message.

3. **Account-level event filtering on drain path**: `AccountExtractor` extracts the account code from events that have one (100-107). Drain handler checks `session.entitledAccounts().contains(accountCode)`. Events without account fields (prices 51, account events 110/111, position snapshot 204) pass through based on symbol+eventType match only. This prevents User A seeing User B's orders on the same symbol.

4. **No-symbol template routing via globalEventBitMask**: Templates without extractable symbol fields (110, 111, 112, 204) are matched against the OR of all subscription bitmasks. If ANY subscription includes the event type, the message passes through.

5. **JTI cache uses full String key**: ConcurrentHashMap<String, Long> with full JTI string. At 10K entries of ~36-char UUIDs = ~360KB -- negligible memory cost, zero collision risk.

6. **Single error code to client for all auth failures**: `WebSocketError(AuthenticationFailed)` for all rejection reasons. Detailed reason codes in server logs only. Prevents auth oracle attacks.

7. **Re-authentication via templateId=60 in WebSocketFrameDispatcher**: After initial auth, WebSocketAuth frames are routed to `handleReAuth()`. Validates new JWT, verifies sub matches, revokes superseded JTI, refreshes entitlements. No session interruption.

8. **Stubs with ticket references**: `// TODO(APP-35)` GapRequest, `// TODO(APP-35)` SessionResume, `// TODO(APP-236)` symbol-level entitlements. No TODO without ticket (CLAUDE.md).

9. **Per-IP auth failure rate limiting**: `AuthFailureTracker` with configurable lockout + max 16384 entries with LRU eviction (prevents botnet unbounded memory growth).

10. **jwtAudience required**: No default value. Server fails to start if unconfigured. Prevents confused deputy attacks.

11. **Auth timeout 5 seconds**: Matches architecture doc Section 3/4. Not 3s (was incorrectly changed in R1).

12. **forEachSession replaced with direct iteration**: New `sessionManager.sessions()` returns `Iterable<WebSocketSession>` for drain handler for-loop. Eliminates per-message lambda/Runnable allocation and GC pressure.

13. **authResolved double-decrement guard**: `volatile boolean authResolved` per JwtAuthHandler instance prevents double-decrement of `pendingAuthCount` if `channelInactive` races with auth failure handling (both on same event loop -- volatile is sufficient, no CAS needed).

---

## Logging Standards

| Class | Level | What | Redaction |
|---|---|---|---|
| JwtValidator | INFO | Auth success: userId, sessionId | Never log JWT token or claims |
| JwtValidator | WARN | Auth failure: reason, remoteIp | Never log token |
| JwtValidator | ERROR | JWKS fetch failure: issuer, endpoint | - |
| JtiRevocationCache | WARN | Fail-safe activation, large eviction batch | - |
| JwtAuthHandler | INFO | Auth success: userId, sessionId, accountCount | Never log token |
| JwtAuthHandler | WARN | Auth failure: reason, remoteIp | Never log token, claims, accounts |
| WebSocketFrameDispatcher | DEBUG | Subscribe/unsubscribe: symbol count | - |
| WebSocketFrameDispatcher | WARN | Unknown templateId | - |
| WebSocketFrameDispatcher | INFO | Re-auth success: userId | Never log token |
| AuthFailureTracker | WARN | IP lockout activated | - |
| UserEntitlementService | INFO | Account validation: N/M active | Never log account codes in prod |

---

## Verification

1. `./gradlew :websocket-server:test` -- all new + updated unit tests pass
2. `./gradlew :test-support:test` -- SbeTestEncoder additions compile
3. `./gradlew :launcher:compileJava` -- WebSocketLauncher + TradingEngineLauncher compile
4. `./gradlew build` -- full build passes
5. `./gradlew spotlessCheck` -- formatting OK
6. `./gradlew e2e` -- 3-node cluster integration tests still pass
7. **ByteBuf leak detection**: all tests run with `-Dio.netty.leakDetection.level=PARANOID`
8. **No token logging**: grep test output for JWT patterns to verify no accidental logging
9. **Architecture doc updated**: `docs/websocket-architecture.md` reflects PR 3 implementation
