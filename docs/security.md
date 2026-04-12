# Security Model

This document describes the authentication, authorization, network exposure, and data
protection posture of the trading engine. It is explicit about what IS and what IS NOT
secured, so operators can make informed deployment decisions.

**Status:** Pre-production. Several security controls are planned but not yet implemented.
Gaps are called out in each section and consolidated in
[Planned Security Enhancements](#5-planned-security-enhancements).

---

## 1. Authentication Model

### 1.1 FIX Sessions (Port 9880)

FIX session authentication uses Artio's `MessageValidationStrategy` to enforce a CompID
allowlist. The engine acts as a FIX 4.4 acceptor (TargetCompID = `TRADING`).

**How it works.** On every inbound Logon (MsgType=A), Artio evaluates two chained
validators before the session is established:

```java
// FixGateway.java, lines 178-181
MessageValidationStrategy.targetCompId(targetCompId)
    .and(MessageValidationStrategy.senderCompId(allowedSenderCompIds));
```

1. **TargetCompID validation** -- the client's TargetCompID (tag 56) must match the
   gateway's configured identity (`TRADING`).
2. **SenderCompID allowlist** -- the client's SenderCompID (tag 49) must appear in the
   configured set of allowed identifiers.

If either check fails, Artio rejects the Logon and the TCP connection is closed. No
`FixSessionHandler` is created and no cluster resources are allocated.

**Per-CompID session limits.** After CompID authentication succeeds, `SessionRegistry`
enforces two capacity gates before the session is admitted:

| Limit | Default | Purpose |
|-------|---------|---------|
| Global max sessions | 64 | Prevents resource exhaustion from connection storms |
| Per-CompID max sessions | 4 | Prevents a single counterparty from monopolizing capacity |

If either limit is exceeded, the session is immediately logged out and disconnected. A
stateless `NoOpSessionHandler` is returned to avoid leaking resources.

```
GatewayLauncher.java:
  MAX_SESSIONS = 64
  MAX_SESSIONS_PER_COMP_ID = 4
```

**Current configuration.** The allowed SenderCompID set is hardcoded in
`GatewayLauncher.java` (line 102):

```java
Set.of("CLIENT1", "CLIENT2", "FIX_BRIDGE")
```

This is a pre-production convenience. `TODO(APP-157)` tracks externalizing the allowlist
to a configuration file or environment variable so it can be managed without recompilation.

**What is NOT authenticated:**

- There is no password, certificate, or token exchange during FIX Logon. Authentication
  is identity-based (CompID match) only. A client that knows a valid SenderCompID can
  connect.
- There is no IP-based access control at the application layer. Network-level firewalling
  is the expected control.
- `TODO(APP-166)` plans async FIX session authentication via an `AccountStore` lookup,
  which would allow richer credential validation (e.g., password in tag 554, or
  out-of-band key verification).

### 1.2 WebSocket Connections (Ports 8443, 8444)

**Currently UNAUTHENTICATED.**

Both WebSocket endpoints accept connections without any credential exchange:

- **Babl WebSocket Server (port 8443)** -- binary SBE frame streaming for the browser UI.
  Any client that connects receives real-time order, position, and quote updates.
- **FIX Client Bridge (port 8444)** -- JSON API for browser-based order entry and RFQ
  submission. Any connected client can submit orders.

**Deployment assumption:** These ports are intended for internal network access only. They
must not be exposed to untrusted networks without authentication. In the Docker Compose
topology, the `web-ui` nginx container proxies `/ws` and `/fix` to these ports; the raw
ports should not be published to the host network.

**Planned fix:** `TODO(APP-160)` will add JWT/token-based authentication to WebSocket
connections, validating tokens on the HTTP upgrade request before establishing the
WebSocket session.

### 1.3 Cluster Inter-Node Communication (Ports 9000-9002)

**Currently UNAUTHENTICATED.**

Aeron Cluster uses UDP for consensus (Raft log replication, leader election, heartbeats).
The Aeron Cluster library does not natively support mutual TLS or any transport-layer
authentication on inter-node communication.

A rogue process that can reach ports 9000-9002 could theoretically inject consensus
messages or disrupt leader election. This risk is mitigated by:

- Deploying all cluster nodes on a private network segment
- Using OS-level firewall rules to restrict access to cluster ports

**Planned fix:** `TODO(APP-136)` will implement an `AuthenticatorSupplier` SPI seam in
Aeron Cluster to add a shared-secret or certificate-based challenge on node join. This is
the idiomatic extension point provided by Aeron Cluster for authentication.

---

## 2. Network Exposure

### 2.1 Port Security Classification

| Port | Service | Protocol | Exposure | Auth | Rationale |
|------|---------|----------|----------|------|-----------|
| 9880 | FIX Gateway (Artio) | FIX 4.4 / TCP | **External** | CompID allowlist | Counterparty FIX connections |
| 8443 | Babl WebSocket | WS (binary) | Internal only | **None** | Unauthenticated SBE streaming |
| 8444 | FIX Client Bridge | WS (JSON) | Internal only | **None** | Unauthenticated order/RFQ API |
| 9000-9002 | Cluster nodes | Aeron / UDP | Internal only | **None** | Consensus replication |
| 5173 | Vite dev server | HTTP | Internal only | **None** | Development mode only; not deployed |
| 9090 | Prometheus | HTTP | Internal only | **None** | Metrics scraping |
| 9464 | EventLogger metrics | HTTP | Internal only | **None** | Prometheus scrape target |
| 3000 | Grafana | HTTP | Internal only | Anonymous | Dashboards (anonymous auth enabled) |
| 3100 | Loki | HTTP | Internal only | **None** | Log ingestion (push API) |

### 2.2 Firewall Recommendations

For any environment beyond localhost development:

```
ALLOW  tcp/9880  FROM counterparty-CIDRs   # FIX clients
ALLOW  tcp/8443  FROM web-ui-container      # Babl WebSocket (internal proxy only)
ALLOW  tcp/8444  FROM web-ui-container      # FIX Client Bridge (internal proxy only)
ALLOW  udp/9000-9002  FROM cluster-nodes    # Inter-node consensus
DENY   ALL  FROM 0.0.0.0/0                 # Default deny
```

The observability ports (9090, 3000, 3100, 9464) should be restricted to the monitoring
network segment.

---

## 3. Data Protection

### 3.1 Audit Trail

Artio logs all inbound and outbound FIX messages to disk. This is configured in
`FixGateway.onStart()`:

```java
engineConfig
    .logInboundMessages(true)
    .logOutboundMessages(true)
    .logFileDir(logFileDir)
```

These logs provide a complete, timestamped audit trail of every FIX message processed by
the gateway, including Logon/Logout, orders, executions, and rejects.

### 3.2 Encryption at Rest

**Not implemented.** The trading engine processes data in-memory:

- **Aeron shared memory** (`/dev/shm/aeron-trading/`) -- IPC publications and log buffers
  reside in shared memory. Access is controlled by OS-level file permissions on the
  `/dev/shm` directory.
- **Aeron Archive recordings** (`/tmp/cluster-*/recording-log/`) -- Raft log segments and
  snapshots are stored as raw binary files. They contain the full event history (orders,
  fills, positions) in SBE-encoded form.
- **Artio FIX logs** (`fix-logs/`) -- plaintext FIX messages on disk.

All three stores contain sensitive trading data and must be protected with appropriate
filesystem permissions. Encryption at rest, if required, should be provided at the volume
level (e.g., LUKS, dm-crypt, AWS EBS encryption).

### 3.3 Encryption in Transit

**Not implemented.**

- **FIX sessions (port 9880):** Plaintext TCP. No TLS/FIXS. `TODO(APP-169)` plans
  mutual TLS (FIXS -- FIX over TLS) for counterparty connections.
- **Cluster communication (ports 9000-9002):** Plaintext UDP. Aeron does not natively
  support TLS on its transport. `TODO(APP-136)` is the planned mitigation.
- **WebSocket (ports 8443, 8444):** Plaintext `ws://`. No `wss://` (TLS). For production,
  TLS termination should be handled by a reverse proxy (nginx, HAProxy) in front of the
  WebSocket ports.
- **Observability (Prometheus, Loki, Grafana):** Plaintext HTTP.

### 3.4 Shared Memory Security

Aeron IPC uses shared memory segments under `/dev/shm/`. Any process running as the same
OS user (or root) can read and write to these segments. This means:

- A compromised process on the same host could inject messages into the Aeron IPC channel
- A compromised process could read all cluster traffic, including order flow

**Mitigation:** Run the trading engine under a dedicated OS user with restrictive
`/dev/shm` permissions. In containerized deployments, avoid sharing the `/dev/shm` mount
across containers unless necessary.

---

## 4. What Is NOT Secured (and Why)

This section consolidates all known security gaps for pre-production awareness. Each gap
is an intentional trade-off for development velocity, not an oversight.

| Gap | Current State | Risk | Mitigation | Tracking |
|-----|---------------|------|------------|----------|
| WebSocket authentication | None | Any internal client can submit orders | Internal network assumption; firewall | APP-160 |
| FIX session credentials | CompID match only (no password/cert) | Known CompID = access | Network perimeter; APP-166 for richer auth | APP-166 |
| FIX transport encryption | Plaintext TCP | Message interception on wire | Private network segment; planned FIXS | APP-169 |
| Cluster authentication | None | Rogue node injection | Private network segment; planned AuthenticatorSupplier | APP-136 |
| Cluster transport encryption | Plaintext UDP | Consensus traffic interception | Private network segment | APP-136 |
| CompID allowlist management | Hardcoded in Java source | Requires recompilation to change | Planned externalization | APP-157 |
| Shared memory access | OS user permissions only | Co-located process can read/write | Dedicated OS user; container isolation | -- |
| Admin API | Does not exist | No operational controls | -- | -- |
| Artio FIX logs | Plaintext on disk | Contain full message history | Filesystem permissions; volume encryption | -- |
| Grafana | Anonymous auth enabled | Anyone on network can view dashboards | Internal network segment | -- |

---

## 5. Planned Security Enhancements

### APP-136: Cluster AuthenticatorSupplier + TLS

Implement mutual authentication for Aeron Cluster inter-node communication using the
`AuthenticatorSupplier` SPI. Nodes will exchange a shared secret or X.509 certificate
during the cluster join handshake. This prevents rogue nodes from participating in
consensus.

### APP-157: Externalize CompID Allowlist

Move the hardcoded `Set.of("CLIENT1", "CLIENT2", "FIX_BRIDGE")` from
`GatewayLauncher.java` (line 102) to an external configuration source (properties file,
environment variable, or configuration service). This enables operational management of
FIX client access without recompilation.

### APP-160: WebSocket JWT Authentication

Add token-based authentication to the Babl WebSocket server (port 8443) and FIX Client
Bridge (port 8444). Tokens will be validated during the HTTP upgrade handshake. Rejected
connections will receive a 401 response before the WebSocket session is established.

### APP-166: Async FIX Session Authentication via AccountStore

Extend FIX Logon authentication beyond CompID matching. The gateway will perform an
asynchronous lookup against an `AccountStore` to validate credentials (e.g., password in
FIX tag 554, or an out-of-band key). This enables per-account access control and
credential rotation without gateway restarts.

### APP-169: FIX Session TLS/FIXS (Mutual TLS)

Enable TLS on the FIX acceptor port (9880) using the FIXS (FIX Secure) standard. Both
server and client certificates will be validated (mutual TLS). This encrypts all FIX
traffic in transit and provides cryptographic identity verification of counterparties.

---

## 6. Operational Security Checklist

Pre-deployment checklist for any environment beyond localhost:

- [ ] Restrict ports 8443, 8444, 9000-9002 to internal network only
- [ ] Configure OS firewall rules per Section 2.2
- [ ] Run trading engine processes under a dedicated, unprivileged OS user
- [ ] Set `/dev/shm/aeron-trading/` permissions to `0700` for the engine user
- [ ] Restrict filesystem permissions on Artio FIX log directory
- [ ] Restrict filesystem permissions on Aeron Archive recording directories
- [ ] Enable volume-level encryption for disks containing FIX logs and Archive recordings
- [ ] Place a TLS-terminating reverse proxy in front of WebSocket ports if exposed
- [ ] Disable Grafana anonymous auth or restrict to monitoring network
- [ ] Review and update the CompID allowlist before onboarding counterparties
