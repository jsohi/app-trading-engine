# Deployment Topology

## Development Mode (`./gradlew devAll`)

All processes run on localhost, single JVM per service, shared filesystem for Aeron directories.

```
localhost
├── Media Driver 0        (shared memory: /tmp/aeron-node-0)
├── Media Driver 1        (shared memory: /tmp/aeron-node-1)
├── Media Driver 2        (shared memory: /tmp/aeron-node-2)
├── Media Driver GW       (shared memory: /tmp/aeron-gateway)
├── Cluster Node 0        (leader,   port 9000, aeron-dir: /tmp/aeron-node-0)
├── Cluster Node 1        (follower, port 9001, aeron-dir: /tmp/aeron-node-1)
├── Cluster Node 2        (follower, port 9002, aeron-dir: /tmp/aeron-node-2)
├── Gateway               (FIX acceptor, port 9880)
├── Pricing Service       (Aeron IPC)
├── Babl WebSocket Server (port 8443)
├── FIX Client Bridge     (WS port 8444)
└── Web UI (Vite)         (port 5173 → proxies WS to 8443/8444)
```

## Docker Compose (`docker compose up`)

```
┌──────────────────────── docker-compose.yml ─────────────────────────┐
│                                                                      │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │  trading-engine (single container)                      │        │
│  │                                                          │        │
│  │  Media Driver ─── shared memory (/dev/shm/aeron)        │        │
│  │       │                                                  │        │
│  │  ┌────┴────┐  ┌────────┐  ┌────────┐                   │        │
│  │  │Cluster-0│  │Cluster-1│  │Cluster-2│  (3 threads)     │        │
│  │  └────┬────┘  └────────┘  └────────┘                   │        │
│  │       │                                                  │        │
│  │  ┌────┴─────┐ ┌──────────┐ ┌────────────────┐          │        │
│  │  │ Gateway  │ │ Pricing  │ │ Babl WebSocket │          │        │
│  │  │ :9880    │ │ Service  │ │ :8443          │          │        │
│  │  └──────────┘ └──────────┘ └────────────────┘          │        │
│  │                                                          │        │
│  │  ┌──────────────┐ ┌─────────────┐                       │        │
│  │  │FIX Bridge    │ │ EventLogger │                       │        │
│  │  │:8444         │ │             │                       │        │
│  │  └──────────────┘ └─────────────┘                       │        │
│  │                                                          │        │
│  │  Exposed: 9880 (FIX), 8443 (WS binary), 8444 (WS JSON) │        │
│  └─────────────────────────────────────────────────────────┘        │
│                                                                      │
│  ┌────────────────────────┐                                         │
│  │  web-ui (nginx)        │                                         │
│  │  :80 → static React    │                                         │
│  │  /ws → proxy :8443     │                                         │
│  │  /fix → proxy :8444    │                                         │
│  └────────────────────────┘                                         │
│                                                                      │
│  ── profile: observability ──────────────────────────────────       │
│                                                                      │
│  ┌────────────┐  ┌────────────┐  ┌────────────────────┐            │
│  │ Prometheus │  │    Loki    │  │     Grafana        │            │
│  │ :9090      │  │   :3100   │  │     :3000          │            │
│  │            │  │            │  │                    │            │
│  │ scrapes:   │  │ receives:  │  │ datasources:      │            │
│  │  /metrics  │  │  push API  │  │  - Prometheus     │            │
│  │  from      │  │  from      │  │  - Loki           │            │
│  │  EventLog  │  │  EventLog  │  │                    │            │
│  └────────────┘  └────────────┘  │ dashboards:       │            │
│                                   │  - Trading        │            │
│                                   │  - Aeron Metrics  │            │
│                                   │  - Event Log      │            │
│                                   │                    │            │
│                                   │ auth: anonymous    │            │
│                                   │ (no login needed)  │            │
│                                   └────────────────────┘            │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

## Port Map

| Port | Service | Protocol | Purpose | Network |
|------|---------|----------|---------|---------|
| 9880 | Gateway (Artio) | FIX 4.4 TCP | Counterparty FIX sessions | **External** |
| 5173 | Vite dev server | HTTP | Dev mode only, hot reload | Internal only |
| 8443 | Babl WebSocket | WS (binary) | Browser streaming (SBE frames) | Internal only |
| 8444 | FIX Client Bridge | WS (JSON) | Browser RFQ/order entry | Internal only |
| 9000-9002 | Cluster nodes | Aeron UDP | Inter-node consensus | Internal only |
| 9090 | Prometheus | HTTP | Metrics scraping | Internal only |
| 3000 | Grafana | HTTP | Dashboards (anonymous auth) | Internal only |
| 3100 | Loki | HTTP | Log ingestion (push API) | Internal only |
| 9464 | EventLogger | HTTP | Prometheus metrics endpoint | Internal only |

## Aeron Directory Layout

```
/tmp/aeron-node-0/               # Node 0 Aeron media driver directory
├── cnc.dat                      # Command-and-control file
├── images/                      # Log buffers
└── publications/                # IPC publications

/tmp/aeron-node-1/               # Node 1 Aeron media driver directory
/tmp/aeron-node-2/               # Node 2 Aeron media driver directory
/tmp/aeron-gateway/              # Gateway Aeron media driver directory

cluster-data/                    # Configurable via LauncherConfig (default: cluster-data)
├── cluster-0/                   # Node 0 cluster state
│   ├── recording-log/           # Aeron Archive recordings (NEVER truncated)
│   ├── consensus-module/        # Raft state
│   └── snapshots/               # Periodic write-model snapshots (last 3 retained)
├── cluster-1/                   # Node 1 cluster state
├── cluster-2/                   # Node 2 cluster state
├── archive-0/                   # Node 0 archive directory
├── archive-1/                   # Node 1 archive directory
└── archive-2/                   # Node 2 archive directory
```

## Archive Log Retention Policy

The Aeron Archive log must **never be truncated**. Projections depend on replaying all events from position 0 on recovery (projections have no snapshots).

**Disk planning:**
- Estimate ~1 KB per event, 100K events/day = ~100 MB/day
- For production, event archival (APP-68) copies events to an external durable store for EOD reconciliation, but the Archive itself remains intact
- Snapshot retention is limited to the last 3 snapshots per node to control disk growth (configured via `ConsensusModule.Configuration`)

## Resource Requirements

| Component | CPU | Memory | Disk |
|-----------|-----|--------|------|
| Media Driver | 1 core (dedicated) | 256 MB (shared memory) | minimal |
| Cluster (3 nodes) | 1 core each | 512 MB each | 1 GB (log) |
| Gateway | 0.5 core | 256 MB | minimal |
| Pricing Service | 0.5 core | 128 MB | none |
| Babl WebSocket | 0.5 core | 128 MB | none |
| Observability stack | 1 core | 1 GB | 5 GB |
| **Total (dev)** | **~6 cores** | **~3 GB** | **~7 GB** |
