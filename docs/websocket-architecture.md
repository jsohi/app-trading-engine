# UI-to-Server Communication Architecture

## Context

Target architecture for the trading engine's UI-to-server communication layer (Wave 7). Audited through 9 rounds of independent security, reliability, performance, protocol, and codebase consistency review. 200 findings identified and resolved (120 original + 80 hardening pass). Converged — no outstanding issues.

---

## 1. Architecture

### Design: Single Multiplexed Binary WebSocket

```
Browser (React 19 + Web Worker)
  │
  │ single wss://:8443 (binary SBE, bidirectional)
  │ JWT as first binary frame after upgrade
  │
  ├── SEND: Auth, Subscribe, NOS, Cancel, QuoteReq, GapReq, ClientHeartbeat, ClientAck
  ├── RECV: AuthAck, Heartbeat, Events, Snapshot, Error, CommandAck, ReplayComplete
  │         ├── reliable stream (seq-numbered, CRC32C): orders, fills, positions, errors, CommandAck
  │         └── best-effort stream (no seq, conflated): prices, quotes, heartbeat
  │
  └── On reconnect: SessionResume(sessionId, lastSeqNo) + fresh JWT
          → server replays gaps (replay flag set)
          → ReplayComplete marker
          → live delivery resumes
```

### WebSocket Framework: Netty

| Aspect | Netty | Babl (rejected) |
|--------|-------|-----------------|
| Industry adoption | Goldman Sachs, LMAX web, most banks | ~150 GitHub stars |
| TLS support | Native SslHandler (OpenSSL backend) | None |
| Community | Massive, 600+ contributors | Tiny, ~3 contributors |
| **Verdict** | **Industry standard** | **Risky** |

### Netty Pipeline

```java
pipeline
  .addLast(new SslHandler(sslEngine))                       // TLS 1.3
  .addLast(new HttpServerCodec())                            // HTTP upgrade parsing
  .addLast(new HttpObjectAggregator(65536))                  // Aggregate HTTP headers
  .addLast(new WebSocketServerProtocolHandler("/", null,      // RFC 6455 compliance
           true, 65536, false, true, 30000))
  .addLast(new OriginValidationHandler(originsWhitelist))    // CSWSH prevention
  .addLast(new JwtAuthHandler(jwtValidator, 5000))           // 5s auth timeout
  .addLast(new WebSocketFrameDispatcher(sessionManager))     // SBE decode + dispatch
```

`WebSocketServerProtocolHandler` handles Sec-WebSocket-Key validation, ping/pong, and close handshakes per RFC 6455. Application-level heartbeat (template 64) is separate from RFC 6455 pings. RFC 6455 pings are disabled (application heartbeat is preferred — 5s interval vs 30s RFC default).

### Deployment: Same JVM

The WebSocket server runs within `TradingEngineLauncher`, alongside Gateway, PricingService, and Projections. This enables direct `QueryService` calls for snapshots and shares the gateway's media driver for Aeron IPC.

**WebSocket server scope:** The WebSocket server is NOT a cluster service. It does not participate in Aeron Cluster Raft consensus or run any state machine. It receives events via its own `AeronCluster` client session (like the gateway). Session state (sessionId, replay buffer) is ephemeral — not snapshotted by the cluster. This means `UUID.randomUUID()` is acceptable for session IDs (randomness forbidden only inside `TradingClusteredService`).

JVM flags: `-XX:+UseZGC -XX:+AlwaysPreTouch -Xmx4g -XX:SoftMaxHeapSize=3g -XX:MaxDirectMemorySize=1g`

The `-XX:MaxDirectMemorySize=1g` is reserved for Netty `PooledByteBufAllocator` and Aeron Media Driver direct buffers. Replay buffers are heap-based (see Section 6).

### Event Delivery: Own AeronCluster Session + Dedicated Egress Thread

The WebSocket server creates its own `AeronCluster` client session (like the gateway does). Multiple cluster sessions per JVM are fully supported — the codebase already demonstrates this (reference data loading + gateway).

**Threading model:** `AeronCluster.pollEgress()` is a blocking call incompatible with Netty's non-blocking event loop. A dedicated `AeronEgressThread` (core-pinned via `taskset`) polls cluster egress and writes to a lock-free `MpscArrayQueue` (Agrona). The Netty worker thread drains the queue without blocking.

```
AeronEgressThread (dedicated thread, core-pinned via taskset)
  ├── AeronCluster.connect() — own cluster session
  ├── aeronCluster.offer() — sends commands from CommandDispatcher
  ├── aeronCluster.pollEgress() — continuous polling, tight loop
  │    └── WebSocketEgressListener
  │         ├── Decode SBE header (templateId)
  │         ├── Copy message bytes into MpscArrayQueue entry
  │         └── Never returns ABORT (per-client backpressure via Netty, not Aeron)
  ├── MpscArrayQueue (capacity: 8192)
  │    ├── 75% full → QUEUE_NEAR_FULL metric + pause polling 100us
  │    ├── 100% full → SESSION_BACKPRESSURE flag + stop polling
  │    └── Backpressured >2s → restart thread + emit alert
  └── Reconnect on leader change / session error (exponential backoff)

Netty Worker Thread (event loop, scheduled drain every 1ms)
  ├── Drains MpscArrayQueue
  ├── Per message:
  │    ├── SubscriptionFilter: per-client symbol/event match (zero-alloc)
  │    ├── ConflationTracker: skip if best-effort value unchanged
  │    ├── ReliableStreamTracker: assign seqNo, store in replay buffer
  │    └── Write to matching Netty channels (pre-allocated ByteBuf pool)
  └── Single ctx.flush() at end of drain cycle
```

### Full Data Flow

```
┌─────────────────────── Browser ──────────────────────────┐
│  React 19 + Web Worker                                    │
│   ├── SBE TypeScript Decoders + Encoders                  │
│   ├── Differentiated throttling                           │
│   │    ├── Prices: throttleTime(100ms/symbol)             │
│   │    ├── Orders: no throttle                            │
│   │    └── Positions: throttleTime(250ms/key)             │
│   ├── Reliable stream: seq tracking + gap detection       │
│   ├── Heartbeat monitor (reconnect if no data for 15s)    │
│   └── Connection state: CONNECTED / RECONNECTING / DOWN   │
│                                                           │
│  Single WebSocket → wss://:8443 (binary SBE)              │
└───────────────────────────────────────────────────────────┘
              │
              ▼
┌─────────── Netty WebSocket Server (:8443) ───────────────┐
│  TLS: SslHandler (OpenSSL, TLS 1.3)                       │
│  WebSocketSessionManager                                  │
│   ├── JwtAuthHandler (RS256, JWKS/HTTPS)                  │
│   ├── UserEntitlementService (JWT accounts claim)         │
│   ├── SubscriptionFilter (LongHashSet, SymbolPacker)      │
│   ├── ConflationTracker (Long2LongHashMap, 50ms)          │
│   ├── ReliableStreamTracker (4096x1024B ring, CRC32C)     │
│   ├── CommandDispatcher (validate → dedup → offer)        │
│   ├── RateLimiter (token bucket, per-user, NanoClock)     │
│   ├── SecurityAuditLogger (Log4j2 → local + Loki)        │
│   └── SlowConsumerHandler (WriteBufferWaterMark 128K/256K)│
│                                                           │
│  AeronEgressThread (dedicated, core-pinned):              │
│   ├── Polls WebSocketClusterClient egress                 │
│   ├── Writes to MpscArrayQueue (lock-free)                │
│   └── Netty worker drains queue every 1ms                 │
└───────────────────────────────────────────────────────────┘
              ▲ (AeronCluster session)
              │
┌─────────── Aeron Cluster (3-node Raft) ──────────────────┐
│  TradingClusteredService → OrderBook → EventSink          │
│  (Also serves Gateway and ReferenceDataOrchestrator)      │
└───────────────────────────────────────────────────────────┘
```

---

## 2. Wire Format

### Envelope

```
Reliable stream (17-byte header):
┌──────────────────────────────────────────────────────┐
│ Byte 0-3:   totalLength (uint32, little-endian)      │
│ Byte 4-11:  seqNo (int64)                            │
│ Byte 12:    flags (uint8) — reliable=1               │
│             bit 0: reliable (1) / best-effort (0)    │
│             bit 1: replay (1) / live (0)             │
│             bit 2: snapshot fragment (1) / normal (0) │
│             bit 3: snapshot-final (1) / more (0)     │
│             bits 4-7: reserved (must be 0)           │
│ Byte 13-16: crc32c (uint32, hardware-accelerated)    │
│ Byte 17-N:  SBE message (header + body)              │
└──────────────────────────────────────────────────────┘

Best-effort stream (13-byte header):
┌──────────────────────────────────────────────────────┐
│ Byte 0-3:   totalLength (uint32, little-endian)      │
│ Byte 4-11:  seqNo = 0 (int64)                       │
│ Byte 12:    flags (uint8) — reliable=0               │
│ Byte 13-N:  SBE message (header + body)              │
└──────────────────────────────────────────────────────┘
```

**`totalLength` semantics:** Inclusive of all bytes from Byte 0 through Byte N. Parser positions to next frame at `offset += totalLength`.

**Frame detection algorithm:** Parser reads 13 bytes. If `flags` bit 0 = 1 (reliable), read 4 more bytes as CRC32C (17-byte header total). If bit 0 = 0 (best-effort), SBE message starts at byte 13 (13-byte header).

**CRC32C scope:** Computed over Byte 0-12 (totalLength, seqNo, flags) + SBE message body (Byte 17-N). Stored at Byte 13-16. Verification: recompute CRC over same range, compare with stored value. Uses `java.util.zip.CRC32C` (hardware-accelerated via JDK intrinsics on x86).

**Sequence numbers:** `int64`, reset per session. At maximum realistic rate of 1M messages/sec per session, overflow occurs in ~292 million years (`2^63 / 10^6 / 86400 / 365 ≈ 2.92 × 10^8`). Implementation includes assertion guard (`if (lastSeqNo == Long.MAX_VALUE) throw AssertionError`).

**Flag combination validity:**

| reliable | replay | snapshot | snapshot-final | Valid? | Meaning |
|----------|--------|----------|---------------|--------|---------|
| 0 | 0 | 0 | 0 | Yes | Best-effort live |
| 1 | 0 | 0 | 0 | Yes | Reliable live |
| 1 | 1 | 0 | 0 | Yes | Reliable replay |
| 0 | 0 | 1 | 0 | Yes | Snapshot fragment (more) |
| 0 | 0 | 1 | 1 | Yes | Snapshot final fragment |
| 0 | 1 | * | * | **No** | Replay only valid with reliable |
| 1 | * | 1 | * | **No** | Snapshot and reliable cannot coexist |

Invalid combinations must be rejected by both server and client parsers.

**Custom framing note:** The envelope (totalLength, seqNo, flags, CRC) is a custom framing layer on top of SBE. The SBE `messageHeader` (blockLength, templateId, schemaId, version) starts at byte 17 (reliable) or byte 13 (best-effort). Standard SBE decoders expect `messageHeader` at byte 0 — both Java and TypeScript decoders must skip the envelope bytes before dispatching to SBE. The `sbe-typescript-generator` (APP-34) generates payload decoders only; custom frame parsing is handcoded in both server and client.

### SBE Schema Additions

**New composite types** (to be added to `trading-schema.xml`):

```xml
<composite name="varDataEncoding" description="Variable-length data encoding for strings.">
  <type name="length"  primitiveType="uint32"/>
  <type name="varData" primitiveType="uint8" length="0"/>
</composite>

<composite name="uuid" description="RFC 4122 UUID (128 bits).">
  <type name="mostSignificantBits"  primitiveType="int64"/>
  <type name="leastSignificantBits" primitiveType="int64"/>
</composite>
```

**New enums:**

```xml
<enum name="WebSocketErrorCode" encodingType="uint16"
      description="WebSocket-specific error codes.">
  <validValue name="AuthenticationFailed"  description="JWT invalid or expired">1</validValue>
  <validValue name="AuthorizationFailed"   description="Account not entitled">2</validValue>
  <validValue name="RateLimitExceeded"     description="Token bucket exhausted">3</validValue>
  <validValue name="SessionExpired"        description="Grace period elapsed, re-auth required">4</validValue>
  <validValue name="InvalidSubscription"   description="Symbol or event type invalid">5</validValue>
  <validValue name="HeartbeatTimeout"      description="No client heartbeat within timeout">6</validValue>
  <validValue name="BufferOverflow"        description="Replay buffer cannot serve lastSeqNo">7</validValue>
  <validValue name="VersionMismatch"       description="Protocol version incompatible">8</validValue>
  <validValue name="SlowConsumer"          description="Client lag exceeded threshold">9</validValue>
  <validValue name="ServerShutdown"        description="Rolling restart in progress">10</validValue>
  <validValue name="CommandRejected"       description="Command failed entitlement or dedup check">11</validValue>
</enum>

<enum name="CommandAckStatus" encodingType="uint8"
      description="Status of a client command acknowledgement.">
  <validValue name="Accepted"   description="Command accepted by cluster">0</validValue>
  <validValue name="Rejected"   description="Command rejected (entitlement or validation)">1</validValue>
  <validValue name="Duplicate"  description="Duplicate ClOrdID detected">2</validValue>
  <validValue name="Throttled"  description="Rate limit exceeded">3</validValue>
</enum>
```

**Field ID convention:** CLAUDE.md requires `field id=` values to correspond to FIX tag numbers. WebSocket control messages (templates 60-72) have no FIX equivalents. Exception: WebSocket templates use sequential field IDs (1, 2, 3, ...) per template. Fields that DO have FIX equivalents (e.g., symbol = FIX tag 55) reuse those tag numbers.

**varData ordering constraint:** SBE requires all `varDataEncoding` fields to be the last field(s) in a message. No fixed-length fields may follow a varData field.

### SBE Messages (Templates 60-72)

| Template ID | Message | Direction | Fields |
|-------------|---------|-----------|--------|
| 60 | `WebSocketAuth` | Browser → Server | protocolVersion (uint16), **token** (varDataEncoding, MUST be last) |
| 61 | `WebSocketAuthAck` | Server → Browser | sessionId (uuid composite), protocolVersion (uint16), maxSubscriptions (uint16) |
| 62 | `WebSocketSubscribe` | Browser → Server | repeating group: symbol (char[8]) + eventTypes (uint32 bitmask). Max 100 symbols enforced by server (not schema). |
| 63 | `WebSocketUnsubscribe` | Browser → Server | repeating group: symbol (char[8]) |
| 64 | `WebSocketHeartbeat` | Server → Browser | serverNanos (int64 — `EpochNanoClock` wall-clock epoch nanoseconds, NOT cluster timestamp). Client uses for clock-skew detection: alert ops if `|clientTime - serverTime| > 1s`. |
| 65 | `ClientHeartbeat` | Browser → Server | clientNanos (int64) |
| 66 | `WebSocketSnapshot` | Server → Browser | snapshotId (uuid), fragmentIndex (uint16), totalFragments (uint16), **payload** (varDataEncoding — packed SBE messages from templates 100-107, MUST be last) |
| 67 | `WebSocketError` | Server → Browser | errorCode (WebSocketErrorCode enum), **errorText** (varDataEncoding, restricted to predefined strings, MUST be last) |
| 68 | `WebSocketGapRequest` | Browser → Server | fromSeqNo (int64), toSeqNo (int64) |
| 69 | `SessionResume` | Browser → Server | sessionId (uuid), lastSeqNo (int64) |
| 70 | `CommandAck` | Server → Browser | clientCmdSeqNo (int64), status (CommandAckStatus enum) |
| 71 | `ClientAck` | Browser → Server | lastReceivedSeqNo (int64). Sent every 100 reliable data messages (excludes heartbeats). Rationale: at 5000 msg/sec, 100-msg interval = 20ms resolution, sufficient for 5s lag detection window. Configurable via `WebSocketServerConfig`. |
| 72 | `ReplayComplete` | Server → Browser | gap replay finished, live resumes |

Templates 60-79 reserved for WebSocket control messages.

**Snapshot fragment assembly contract:**
- `WebSocketSnapshot` payload contains one or more packed SBE messages from templates 100-107 (OrderSnapshot, PositionSnapshot, etc.)
- Fragments MUST arrive in order (guaranteed by WebSocket RFC 6455)
- Client assembles fragments by `snapshotId`, concatenates payload bytes, then decodes embedded SBE templates
- If fragment not received within 10s, client sends `WebSocketGapRequest` for retransmission
- Individual entities (orders, positions) are never split across fragments — if an entity exceeds the fragment size, it is dropped (and logged server-side)
- Fragment size: 16KB max (configurable via `WebSocketServerConfig.snapshotFragmentSizeBytes`)

### Reliable vs Best-Effort

| Stream | Messages | Seq | Gap Recovery | Drop Policy |
|--------|----------|-----|--------------|-------------|
| **Reliable** | Orders, fills, cancels, rejects, positions, CommandAck, errors | Yes + CRC32C | GapRequest → replay from buffer → ReplayComplete | Never drop. Slow consumer → graduated backpressure (see Section 5) |
| **Best-effort** | Prices, quotes, heartbeat | No | Full snapshot on reconnect | Conflated (latest-value, 50ms server-side) |

Heartbeat is best-effort (no seqNo, no gap recovery). Heartbeat loss detection is handled by app-level timeout monitoring (see Section 3), not sequence tracking.

---

## 3. Session Protocol

### Lifecycle

```
1. Browser opens wss://:8443
2. TLS handshake (Netty SslHandler, TLS 1.3, strong ciphers)
3. WebSocket upgrade (Origin validated against exact-match whitelist)
4. Client sends WebSocketAuth (JWT as first binary frame, within 5s)
   - JwtAuthHandler validates: alg=RS256, iss, aud, exp (5s skew), nbf, iat, sub, jti
   - alg must be RS256 — reject alg:none, HS256, ES256, and all others
   - exp clock skew: 5s max (OWASP recommendation, requires PTP/chrony sync <1s)
   - iat validation: reject tokens issued >15min ago
   - Public keys from JWKS endpoint (HTTPS only, cert-validated, cached 1 hour)
   - On SignatureVerificationException: force-refresh JWKS cache and retry once (handles key rotation)
   - Entitlements extracted from JWT `accounts` claim
   - Revocation list checked (time-based eviction, 15-min window, max 10K entries)
5. Server sends AuthAck(sessionId=UUID, protocolVersion="1", maxSubscriptions=100)
6. Client sends Subscribe(symbols, eventTypes)
7. Server sends WebSocketSnapshot (fragmented, <=16KB per fragment, snapshotId/fragmentIndex/totalFragments)
   - QueryService.getSnapshotForAccounts(symbols, accountCodes) filters by entitled accounts + requested symbols
   - Accounts validated as ACTIVE (not Suspended/Closed)
   - Defense in depth: WebSocket applies second filter on returned data (validates match against sessionEntitlements)
8. Bidirectional data flow begins
9. Heartbeat: server sends every 5s (best-effort). Client sends every 10s.
   Server timeout: 20s no client heartbeat → disconnect.
   Client timeout: 15s no server data (heartbeat or price) → reconnect.
10. On disconnect: server holds session state for 30s (grace period)
11. On reconnect: client sends SessionResume(sessionId, lastSeqNo) + fresh WebSocketAuth within 5s
    - Server validates JWT, confirms sub matches original session userId
    - jti-to-sessionId binding verified (prevents session hijacking)
    - If match + buffer has data: replay gaps (replay flag), then ReplayComplete, then live
    - If session expired: WebSocketError(SessionExpired) → client does full re-auth
    - If lastSeqNo < buffer.lowestSeq: WebSocketError(BufferOverflow) → client requests full snapshot
12. Re-auth: client sends new JWT before 15-min expiry. sub must match (atomic check).
    Entitlements refreshed via UserEntitlementService (60s cache TTL). Superseded jti added to revocation list.
```

### Session State (per client)

| Field | Size |
|-------|------|
| sessionId (UUID) | 16 B |
| userId (JWT sub) | ~32 B |
| accountEntitlements (LongHashSet) | ~200 B |
| subscriptionSet (LongHashSet, packed symbols) | ~1.5 KB |
| reliableSeqCounter (long) | 8 B |
| replayBuffer (4096 entries x 1024 bytes, heap) | **4 MB** |
| conflationTracker (Long2LongHashMap) | ~4 KB |
| rateLimiter (token bucket, 2 longs) | 16 B |
| lastClientHeartbeat (nanoTime) | 8 B |
| **Total per session** | **~4.1 MB** |
| **100 clients** | **~410 MB** |
| **256 clients (max)** | **~1.03 GB** |

Replay buffers are heap-based, implemented as `Agrona RingBuffer` wrapping primitive `long[]` arrays (no object wrapper overhead). On client disconnect, arrays are returned to a pre-allocated `ReusableArrayPool` (256 slots). GC impact: <2ms pause at 50% heap utilization under ZGC.

Subscriptions are transient — client re-subscribes after reconnect. Grace-period sessions count toward concurrent limit.

---

## 4. Security

### Authentication

| Control | Implementation |
|---------|---------------|
| Algorithm | RS256 only. Reject `alg:none`, HS256, ES256, and all non-RS256. |
| Key management | JWKS endpoint (**HTTPS**, cert-validated, cached 1 hour). On `SignatureVerificationException`: force-refresh JWKS and retry once (handles key rotation without outage). Alt: file-based via Vault agent. |
| Issuer registry | Map JWT `iss` claim → JWKS endpoint URL. Config: `websocket-server.yaml` → `issuer_registry[iss].jwks_uri`. Reject unknown issuers. |
| Certificate pinning | JWKS endpoint uses system truststore CA validation. Explicit certificate pinning deferred to go-live gate 17 (cert rotation drill). |
| Token lifetime | 15-min expiry. Re-auth before expiry (sub must match atomically, entitlements refreshed via `UserEntitlementService` with 60s cache TTL). |
| Clock skew | 5s max (OWASP recommendation). Requires PTP/chrony sync <1s across all JVMs. Added to go-live gates. |
| Revocation | LongHashSet of revoked `jti` claims. Time-based eviction (15-min). Max 10K. Full → reject new tokens as fail-safe. |
| Claims validated | `iss` (trusted whitelist), `aud` (service ID), `exp` (5s skew), `nbf`, `iat` (reject >15min), `sub`, `jti`, `accounts`. |
| JWT delivery | First binary frame after upgrade (not query param — avoids log leakage). |
| Reconnect auth | SessionResume + fresh JWT required. jti-to-sessionId binding verified. sub must match original session userId. |
| CSWSH | Origin exact-match whitelist (configured in `websocket-server.yaml`, reloadable via SIGHUP). Missing or `null` Origin → reject. No cookies/ambient credentials. |

### Authorization

| Control | Implementation |
|---------|---------------|
| Entitlement source | JWT `accounts` claim (array of permitted account codes). |
| Entitlement refresh | `UserEntitlementService` queries `AccountStore` with 60s cache TTL. On re-auth or cache miss, re-queries. Revocation propagates within 60s. |
| Data filtering | `SubscriptionFilter` + `UserEntitlementService` restrict events to entitled accounts. |
| Command validation | `CommandDispatcher` checks account field against entitlements before forwarding. |
| Defense in depth | Cluster independently validates via `AccountStore.getByCode()` on every command. |
| Snapshot filtering | `QueryService.getSnapshotForAccounts(Set<String> symbols, Set<String> accountCodes)`. Filters orders/positions/quotes to `(symbol IN symbols) AND (account IN accountCodes)`. Verifies each account is ACTIVE. Returns per-entity SBE fragments using templates 100-107. WebSocket layer applies second filter (defense-in-depth). |

### Transport & Network

| Control | Implementation |
|---------|---------------|
| TLS | Netty `SslHandler`, `SslProvider.OPENSSL`, TLS 1.3 only. Ciphers: `TLS_AES_256_GCM_SHA384`, `TLS_CHACHA20_POLY1305_SHA256`, `TLS_AES_128_GCM_SHA256` (RFC 8446 mandatory-to-implement). |
| HSTS | `Strict-Transport-Security: max-age=31536000; includeSubDomains; preload` on HTTP upgrade response. |
| OCSP | OCSP stapling enabled via Netty OpenSSL support. |
| Connection limits | Global: 256. Per-IP: 10. Per-user concurrent: 4 (enforced atomically by `WebSocketSessionManager` on JWT `sub` claim). |
| Rate limiting | Per-user token bucket (zero-alloc, `NanoClock` — monotonic, not wall-clock). Commands (NOS, Cancel, QuoteReq): 50/sec sustained, 100 burst. Subscriptions: 5 operations/sec (per Subscribe/Unsubscribe message, regardless of symbol count in message). Unsubscribe: unlimited. Non-counting: ClientAck, ClientHeartbeat. Throttled → `CommandAck(status=THROTTLED)`. |
| DDoS | Netty `ChannelTrafficShapingHandler` + OS-level SYN cookies. Dual-level: per-IP max 10 new connections/sec + global max 256 connections/sec token bucket. |
| FIX Client Bridge | Port 8444, bind 127.0.0.1 only (same-container access), JWT required. `FIX_BRIDGE` CompID removed from production allowlist. In container deployments, access via Docker service name or sidecar proxy. |
| mTLS endpoint | See Section 4.5 below. |
| Compression | `permessage-deflate` DISABLED (CRIME/BREACH prevention). |
| Browser headers | CSP: `connect-src 'self'; script-src 'self'; frame-ancestors 'none'; default-src 'self'`. `X-Frame-Options: DENY`. `X-Content-Type-Options: nosniff`. WebSocket upgrade does not use CORS (uses Origin header only per RFC 6455). |
| JWT storage | Memory-only in Web Worker. Never `localStorage` or `sessionStorage` (XSS-vulnerable). JWT is isolated to Web Worker; main thread communicates via sealed `postMessage` protocol (commands only, no credential exposure). |

### 4.5 mTLS API Endpoint (Port 8445)

For programmatic API clients (C++, Java, Python).

| Aspect | Specification |
|--------|--------------|
| Port | 8445 (separate from browser WebSocket 8443 and FIX Bridge 8444) |
| TLS | `SslHandler` with `clientAuth=REQUIRE`. TLS 1.3. Same cipher suite as 8443. |
| Server cert | Issued by internal CA (Vault PKI), CN=trading-engine-mTLS. |
| Client cert | Issued per API consumer. CN must match registered consumer list. |
| Revocation | CRL checked on `SslHandler` init (not per-request). OCSP stapling enabled. |
| Authorization | Extract client cert CN → map to privileged account via `AccountMapping` config. |
| Wire format | Same binary SBE protocol as port 8443 (templates 1-16, 60-72, 100-116). |
| Cert rotation | Every 90 days. Operational go-live gate. |

### Audit & Compliance

| Control | Implementation |
|---------|---------------|
| Security audit log | `SecurityAuditLogger` (Log4j2 async). Structured JSON. All auth/authz/rate-limit/lifecycle events. |
| Tamper evidence | Dual-destination: local file (hot buffer) + Loki (immutable remote storage, S3 backend). |
| Dual-destination failure mode | If Loki push fails N times: circuit-break, alert ops, rely on local disk. If local disk full: force async flush. Both failed: reject new connections (fail-safe, not fail-open). |
| Error sanitization | `errorText` restricted to predefined strings per `WebSocketErrorCode` enum. No free-form text. Server version = protocol version only. |
| Replay protection | Primary: per-session client command seqNo (monotonically increasing). Secondary: ClOrdID dedup cache (10K, FIFO) — catches out-of-order replays within same session. Both required: seqNo prevents old-sequence replay; ClOrdID prevents cross-session replay. |

### Go-Live Gates (non-negotiable)

| # | Gate | Tracking |
|---|------|----------|
| 1 | JWT auth on WebSocket (RS256, JWKS) | APP-160 |
| 2 | TLS on WebSocket (Netty SslHandler, TLS 1.3) | New issue |
| 3 | TLS on FIX (FIXS, Artio SslContext) | APP-169 |
| 4 | Cluster auth (AuthenticatorSupplier) | APP-136 |
| 5 | ClOrdID uniqueness enforcement | APP-206 |
| 6 | FIX Client Bridge secured (127.0.0.1 + JWT) | New issue |
| 7 | User entitlement service | New issue |
| 8 | Security audit logging | New issue |
| 9 | CompID allowlist externalized | APP-157 |
| 10 | Data-at-rest encryption (volume-level) | New issue |
| 11 | JWKS key rotation tested (forced-refresh on SignatureVerificationException) | New issue |
| 12 | TLS certificate rotation (90-day cycle tested) | New issue |
| 13 | Audit log dual-destination verified (local + Loki, failure modes tested) | New issue |
| 14 | mTLS endpoint secured (port 8445, client cert auth + account mapping) | New issue |
| 15 | DDoS mitigation tested (1000 conn/sec sustained, rate limiter enforces limits) | New issue |
| 16 | Secrets in Vault (TLS keys, JWKS creds, mTLS CAs — no hardcoded secrets) | New issue |
| 17 | SAST/DAST scan passed (Snyk + OWASP Top 10 pentest report) | New issue |
| 18 | Clock synchronization via PTP/chrony (<1s drift verified) | New issue |

---

## 5. Reliability

### Reconnection

| Aspect | Spec |
|--------|------|
| Browser backoff | 500ms initial, 2x exponential, 30s cap, +/-50% jitter. Unlimited attempts (no max). Manual "Reconnect Now" button resets backoff timer. |
| Server throttle | Max 10 reconnects/minute per user (anti-thundering-herd). |
| Grace period | 30s — server holds session state (seqNo, replay buffer). Not subscriptions. |
| Reconnect flow | SessionResume(sessionId, lastSeqNo) + fresh JWT → validate sub match → gap replay → ReplayComplete → live. |
| Buffer overflow | If lastSeqNo < buffer.lowestSeq → WebSocketError(BufferOverflow) → client requests full snapshot. |
| Session expired | If grace period elapsed → WebSocketError(SessionExpired) → client does full re-auth + snapshot. |

### Backpressure

**Egress listener never returns ABORT.** All messages are buffered in per-client replay rings. Backpressure is per-client via Netty `WriteBufferWaterMark` and `channel.isWritable()` checks. This prevents one slow client from causing head-of-line blocking across all clients (unlike the gateway's `ClusterEgressListener` which uses `Action.ABORT` for single-session FIX backpressure).

| Path | Mechanism |
|------|-----------|
| Cluster → WebSocket | AeronEgressThread polls egress → MpscArrayQueue → Netty drain. Never ABORT. |
| Best-effort slow client | Skip write (conflation handles staleness). |
| Reliable slow client | Buffer in replay ring. Ring full → evict oldest message + emit SLOW_CONSUMER_WARNING metric. |
| Channel watermarks | `WriteBufferWaterMark(128KB, 256KB)`. At 1KB/msg average, 256KB = ~250ms buffering. |
| Consumer lag | ClientAck every 100 reliable data msgs. Server tracks lag. Dual-metric: lag >5000 messages OR >5s elapsed (whichever first) → disconnect with `WebSocketError(SlowConsumer)`. |

**Graduated backpressure (4 levels):**

| Level | Trigger | Action |
|-------|---------|--------|
| 1. Conflation increase | AeronBridge lag 100-500 KB (measured as pending bytes in queue) | Increase server-side conflation from 50ms to 200ms (best-effort only) |
| 2. Best-effort pause | Lag 500 KB-1 MB | Disable best-effort streams entirely. Client shows "PRICING UNAVAILABLE". |
| 3. Selective disconnect | Lag >1 MB | Disconnect slowest 10% of clients (by individual lag). Stop accepting new connections. |
| 4. Emergency shutdown | Lag >2 MB sustained >10s | Graceful shutdown: send `WebSocketError(ServerShutdown)` → 3s drain → close all. |

### Message Ordering

- Single WebSocket connection → frame ordering guaranteed by RFC 6455.
- Gap replay: server pauses live delivery, sends replayed messages with `replay=1` flag, sends ReplayComplete, resumes live. No interleaving.

### Graceful Shutdown

Server sends `WebSocketError(ServerShutdown, "Rolling restart")` → wait 3s drain → force close. Client shows "SERVER RESTARTING" (distinct from "CONNECTION LOST"). Reconnects after indicated delay.

### Browser Tab Visibility

Web Worker monitors `visibilitychange`. Tab hidden: pause best-effort subs, reduce heartbeat to 30s. Tab returns: mandatory snapshot request if hidden >60s.

### Protocol Version

Client sends `schemaVersion` in Auth frame. Mismatch → `WebSocketError(VersionMismatch)` → disconnect.

### Delivery Semantics

At-least-once. Client deduplicates by seqNo (`if seqNo <= lastProcessedSeqNo, drop`).

---

## 6. Performance

### Latency Budget

| Path | Target | Validation |
|------|--------|-----------|
| Orders (no throttle) | 5-15ms browser-to-blotter p99 | Validated by `LoadTestWebSocket.java` in integration-tests: 100 clients, 1000 orders/sec. Assertion: p99 ≤ 20ms. |
| Prices (100ms throttle) | 100-150ms worst case | |
| Positions (250ms throttle) | 250-280ms worst case | |
| Snapshot delivery | <500ms reception-to-first-render | Measured end-to-end: reassembly + AG Grid render |

### Netty Configuration

| Setting | Value |
|---------|-------|
| Boss group | `EpollEventLoopGroup(1)` on Linux / `NioEventLoopGroup(1)` on macOS |
| Worker group | `EpollEventLoopGroup(N)` where N = max(2, availableProcessors / 2). Pin each worker to a CPU core via `taskset`. On macOS: `NioEventLoopGroup(N)` (Epoll unavailable; KQueue optional via `netty-transport-native-kqueue`). |
| Allocator | `PooledByteBufAllocator.DEFAULT` (direct buffers from `-XX:MaxDirectMemorySize=1g`) |
| Leak detection | `PARANOID` in tests, `DISABLED` in production |
| Max frame size | `maxFramePayloadLength = 65536` |
| Heartbeat | App-level only (template 64, 5s). IdleStateHandler REMOVED — app-level heartbeat monitoring in `WebSocketSessionManager` (20s timeout). RFC 6455 ping/pong DISABLED (redundant). |

### AeronEgressThread

Dedicated thread (not Netty event loop — see Section 1 threading model). Core-pinned via `taskset` at launcher startup. Polls `AeronCluster.pollEgress()` in a tight loop (no sleep). Writes to `MpscArrayQueue`. Netty worker drains queue via `channel.eventLoop().schedule(drain, 1, TimeUnit.MILLISECONDS)`.

**Poll latency instrumentation:** Micrometer timer `websocket.aeron.poll.latency` — alert if p99 > 2ms.

**Fan-out pattern per message (on Netty worker thread):**
1. Allocate one `ByteBuf` from pre-allocated pool (per-worker, sized to max concurrent clients)
2. `byteBuf.writeBytes(directBuffer, offset, length)` — Netty internal copy, no `byteBuffer()` wrapper allocation
3. Per matching channel: `channel.write(byteBuf.retainedDuplicate())` — ref-counted view
4. `byteBuf.release()`
5. Single `ctx.flush()` at end of drain cycle

For >100 clients: shard into 2 drain tasks on separate worker threads (partition by `channelId % 2`).

### Zero-Allocation Hot Path

| Component | Strategy |
|-----------|----------|
| SubscriptionFilter | `SymbolPacker.pack()` → `LongHashSet.contains()`. No String creation. |
| ConflationTracker | `Long2LongHashMap` keyed by `(packedSymbol << 8) | msgType`. Value = CRC32C hash of packed price fields (bidPx int64 `||` askPx int64, 16 bytes). If value matches stored hash, skip write. Zero-alloc: direct buffer read of price fields, JDK CRC32C intrinsic (no object creation). |
| RateLimiter | Two longs: `tokens` + `lastRefillNanos`. `NanoClock` injected (monotonic, not wall-clock — avoids NTP adjustment skips). |
| ReplayBuffer | Agrona `RingBuffer` wrapping primitive `long[]` arrays. Pre-allocated `ReusableArrayPool` (256 slots). No GC pressure on client disconnect/reconnect. |

### CPU Pinning

Pin the `AeronEgressThread` and Netty worker threads via OS-level `taskset` at JVM startup. Not in-process — no `AffinityLock` dependency.

### Client-Side

| Concern | Strategy |
|---------|----------|
| postMessage | Copy semantics (standard `postMessage`) for all data including snapshots. NOT `Transferable` `ArrayBuffer` — transferred buffers are detached from sender, causing race conditions with SBE TypeScript decoders that expect to own the underlying buffer. Plain objects for incremental updates. |
| AG Grid | `asyncTransactionWaitMillis: 50`. Initial load in 200-row batches. Virtual scrolling on all blotters. `cellFlashDelay: 500ms`. |
| Web Worker memory | Budget <2 MB. Profile with Chrome DevTools. |
| Fixed-point conversion | `Number` for safe-int prices (all FX). Cache formatted strings per symbol. |

### Degradation Under Load

| Level | Trigger | Action |
|-------|---------|--------|
| 1 | AeronBridge lag 100-500 KB | Increase server-side conflation to 200ms |
| 2 | Lag 500 KB-1 MB | Disable best-effort streams |
| 3 | Lag >1 MB | Disconnect slowest 10%, stop accepting new connections |
| 4 | Lag >2 MB sustained >10s | Emergency graceful shutdown |
| — | Connection storm | Dual-level: per-IP 10/sec + global 256/sec |
| — | Per-client subscription limit | 100 symbols max |
| — | Priority | Reliable events always dispatched before best-effort |

### Configuration Externalization

All thresholds are configurable via `WebSocketServerConfig.java` loaded from `config/websocket-server.yaml`:

```yaml
maxConcurrentSessions: 256
maxSubscriptionsPerClient: 100
sessionGracePeriodMs: 30000
replayBufferFrames: 4096
replayBufferFrameSize: 1024
commandsPerSecSustained: 50
commandsPerSecBurst: 100
subscriptionsPerSec: 5
clientTimeoutMs: 20000
heartbeatIntervalMs: 5000
snapshotFragmentSizeBytes: 16384
maxRevokedJtis: 10000
revocationTtlMinutes: 15
writeBufferLowWaterMark: 131072
writeBufferHighWaterMark: 262144
```

### Observability

Micrometer metrics exported to Prometheus:

| Metric | Type | SLO |
|--------|------|-----|
| `websocket.aeron.poll.latency` | Timer | p99 < 2ms |
| `websocket.client.lag` | Gauge | p99 < 5000 msgs |
| `websocket.queue.depth` | Gauge | < 75% capacity |
| `websocket.messages.dropped.backpressure` | Counter | 0 in steady state |
| `websocket.connections.active` | Gauge | ≤ 256 |
| `websocket.gc.pause` | Timer | p99 < 10ms |

---

## 7. Module Changes

| Module | File | Change |
|--------|------|--------|
| `gradle` | `libs.versions.toml` | Add `netty-bom`, `netty-transport`, `netty-handler`, `netty-codec-http`, `netty-transport-native-epoll`, `netty-transport-native-kqueue` (macOS dev), `micrometer-registry-prometheus` |
| `websocket-server` | `build.gradle.kts` | Add Netty deps, `query-service` project dep, Micrometer dep |
| `websocket-server` | `WebSocketServerMain.java` | Netty bootstrap, SslHandler, full pipeline (see Section 1) |
| `websocket-server` | `WebSocketClusterClient.java` | Own AeronCluster session (modeled on gateway's ClusterClient) |
| `websocket-server` | `AeronEgressThread.java` | Dedicated egress polling thread wrapping AgentRunner. Core-pinned. Writes to MpscArrayQueue. |
| `websocket-server` | `WebSocketEgressListener.java` | Decode → copy to queue (on Aeron thread). Never returns ABORT. |
| `websocket-server` | `WebSocketDrainHandler.java` | Netty-side queue drain → filter → conflate → seq → write (on event loop thread) |
| `websocket-server` | `WebSocketSessionManager.java` | Per-client state, session store, concurrent limit enforcement, heartbeat timeout monitoring (20s) |
| `websocket-server` | `JwtAuthHandler.java` | RS256, JWKS/HTTPS, claims validation, 5s auth timeout, forced-refresh on key rotation |
| `websocket-server` | `UserEntitlementService.java` | JWT accounts claim → permitted account set. 60s cache TTL. |
| `websocket-server` | `SubscriptionFilter.java` | LongHashSet of packed symbols. Zero-alloc. |
| `websocket-server` | `ConflationTracker.java` | Long2LongHashMap keyed by (packedSymbol `<<` 8 `|` msgType). Value = CRC32C of price fields. |
| `websocket-server` | `ReliableStreamTracker.java` | Agrona RingBuffer (4096 x 1024B, heap), seq assignment, gap replay (pause-live, ReplayComplete). ReusableArrayPool for buffer lifecycle. |
| `websocket-server` | `CommandDispatcher.java` | Validate entitlements, dedup (ClOrdID cache 10K), rate limit, offer to cluster via queue, send CommandAck |
| `websocket-server` | `RateLimiter.java` | Long-based token bucket, per-user, zero-alloc, `NanoClock` (monotonic) |
| `websocket-server` | `SecurityAuditLogger.java` | Log4j2 async → local + Loki. Structured JSON. Circuit breaker on Loki failure. |
| `websocket-server` | `WebSocketServerConfig.java` | All tunables loaded from YAML. See Section 6 for full list. |
| `websocket-server` | `SlowConsumerHandler.java` | WriteBufferWaterMark 128K/256K, graduated backpressure (4 levels), dual-metric lag tracking |
| `websocket-server` | `FrameParser.java` | Custom envelope parsing (totalLength, seqNo, flags, CRC). Dispatches to SBE decoder at correct offset (byte 13 or 17). |
| `messages` | `trading-schema.xml` | Add `varDataEncoding` composite, `uuid` composite, `WebSocketErrorCode` enum, `CommandAckStatus` enum. Add templates 60-72. varData fields MUST be last in each message. |
| `query-service` | `QueryService.java` | Add `getSnapshotForAccounts(Set<String> symbols, Set<String> accountCodes)` — filters by entitled accounts + symbols, validates ACTIVE status. |
| `sbe-typescript-generator` | APP-34 | Generate SBE message decoders for payload (templates 60-72 + existing). Custom frame envelope parsing is out of scope for generator — handcoded in TypeScript client. |
| `launcher` | `TradingEngineLauncher.java` | Wire Netty WebSocket server + AeronEgressThread as services within launcher process |

---

## 8. UI States

### Normal

```
┌──────────────────────────────────────────────────────────────┐
│  Trading Engine              ● CONNECTED  wss://     [dark]  │
├────────────────────────────────┬─────────────────────────────┤
│  Orders Blotter (AG Grid)      │  RFQ Trading Panel          │
│  ┌──────┬──────┬──────┬─────┐  │  ┌───────────────────────┐  │
│  │ClOrd │Symbol│ Side │Price│  │  │ Symbol: [EUR/USD  ▼]  │  │
│  │001   │EURUSD│ BUY  │1.085│  │  │ Qty:   [1,000,000  ]  │  │
│  │002   │GBPUSD│ SELL │1.265│  │  │ [Request Quote]       │  │
│  └──────┴──────┴──────┴─────┘  │  │ Bid: 1.08500000       │  │
│  Positions Blotter             │  │ Ask: 1.08520000       │  │
│  ┌──────┬──────┬──────┬─────┐  │  │ ● PRICING LIVE        │  │
│  │Symbol│NetQty│AvgPx │ PnL │  │  │ [Accept Bid] [Lift]   │  │
│  │EURUSD│+1.0M │1.084 │+1.2k│  │  └───────────────────────┘  │
│  └──────┴──────┴──────┴─────┘  │  Event Log Viewer           │
│  Quotes Blotter                │  ┌───┬───────┬────────────┐ │
│  ┌──────┬──────┬──────┬─────┐  │  │001│OrdAcc │EURUSD BUY │ │
│  │R001  │EURUSD│QUOTED│1.085│  │  │002│QtCrt  │bid/ask    │ │
│  └──────┴──────┴──────┴─────┘  │  └───┴───────┴────────────┘ │
└────────────────────────────────┴─────────────────────────────┘
```

### Reconnecting

```
┌──────────────────────────────────────────────────────────────┐
│  Trading Engine       ◌ RECONNECTING (4s)  [Reconnect Now]   │
├──────────────────────────────────────────────────────────────┤
│  Connection lost. Reconnecting in 4s...                      │
│  Commands queued: 1  |  Last data: 3s ago                    │
├──────────────────────────────────┬───────────────────────────┤
│  Orders Blotter    STALE         │ RFQ: [Request Quote] OFF  │
│  (grayed out, data frozen)       │ PRICING UNAVAILABLE       │
└──────────────────────────────────┴───────────────────────────┘
```

### Server Restarting / Cluster Failover

```
Server:   ◌ SERVER RESTARTING — reconnecting in 5s...
Failover: ● CONNECTED — Cluster failover detected. Data resynced. [x]
```

---

## 9. Verification

1. Add Netty + Micrometer to `libs.versions.toml` — build compiles
2. `WebSocketServerMain` — TLS handshake + WS upgrade works in browser
3. `JwtAuthHandler` — unauthenticated rejected, valid JWT accepted, JWKS key rotation works
4. `AeronEgressThread` + `MpscArrayQueue` — events flow from cluster through queue to Netty channels
5. `SubscriptionFilter` — only subscribed events reach client
6. `CommandDispatcher` — browser SBE commands reach cluster, CommandAck returned
7. `ReliableStreamTracker` — gap detection + replay + ReplayComplete
8. `SlowConsumerHandler` — graduated backpressure (4 levels) + dual-metric lag disconnect
9. `FrameParser` — custom envelope parse correct for reliable (17B) and best-effort (13B)
10. Unit tests: `./gradlew :websocket-server:test` (JUnit 6, naming: `methodUnderTest_scenario_expectedBehavior`)
11. Integration tests: `./gradlew :integration-tests:test`
12. Load test: 50 symbols x 100 ticks/sec x 100 clients — no message loss on reliable stream. p99 latency ≤ 20ms.
13. Snapshot latency: reception-to-first-render < 500ms
14. Security test: unauthed rejected, unauthorized filtered, rate limit enforced, mTLS tested
15. Reconnection test: kill WS server, verify reconnect + gap replay + resume (unlimited attempts)

---

## 10. Review History

| Round | Findings | CRITICAL | HIGH | MEDIUM | LOW |
|-------|----------|----------|------|--------|-----|
| R1 (original) | 74 | 13 | 29 | 26 | 6 |
| R2 (original) | 33 | 2 | 14 | 12 | 5 |
| R3 (original) | 11 | 1 | 2 | 4 | 4 |
| R4 (original) | 2 | 0 | 0 | 0 | 2 |
| **Original subtotal** | **120** | **16** | **45** | **42** | **17** |
| R1 (hardening) | 56 | 16 | 18 | 20 | 2 |
| R2 (hardening) | 18 | 4 | 10 | 4 | 0 |
| R3 (hardening) | 3 | 1 | 2 | 0 | 0 |
| R4 (hardening) | 3 | 1 | 2 | 0 | 0 |
| R5 (convergence) | 0 | 0 | 0 | 0 | 0 |
| **Hardening subtotal** | **80** | **22** | **32** | **24** | **2** |
| **Grand total** | **200** | **38** | **77** | **66** | **19** |

All 200 findings addressed. 0 out of scope. 0 subpar. **Converged.**
