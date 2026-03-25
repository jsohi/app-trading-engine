# Deployment Topology

## Development Mode (`./gradlew devAll`)

All processes run on localhost, single JVM per service, shared filesystem for Aeron directories.

```
localhost
├── Media Driver          (shared memory: /dev/shm/aeron-trading)
├── Cluster Node 0        (leader,   port 9000, aeron-dir: /tmp/cluster-0)
├── Cluster Node 1        (follower, port 9001, aeron-dir: /tmp/cluster-1)
├── Cluster Node 2        (follower, port 9002, aeron-dir: /tmp/cluster-2)
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

| Port | Service | Protocol | Purpose |
|------|---------|----------|---------|
| 5173 | Vite dev server | HTTP | Dev mode only, hot reload |
| 9880 | Gateway (Artio) | FIX 4.4 TCP | Counterparty FIX sessions |
| 8443 | Babl WebSocket | WS (binary) | Browser streaming (SBE frames) |
| 8444 | FIX Client Bridge | WS (JSON) | Browser RFQ/order entry |
| 9000-9002 | Cluster nodes | Aeron UDP | Inter-node consensus |
| 9090 | Prometheus | HTTP | Metrics scraping |
| 3000 | Grafana | HTTP | Dashboards (anonymous auth) |
| 3100 | Loki | HTTP | Log ingestion (push API) |
| 9464 | EventLogger | HTTP | Prometheus metrics endpoint |

## Aeron Directory Layout

```
/dev/shm/aeron-trading/          # Media Driver shared memory
├── cnc.dat                      # Command-and-control file
├── images/                      # Log buffers
│   ├── cluster-0.log
│   ├── cluster-1.log
│   └── cluster-2.log
└── publications/                # IPC publications

/tmp/cluster-0/                  # Node 0 cluster directory
├── recording-log/               # Aeron Archive recordings
├── consensus-module/            # Raft state
└── snapshots/                   # Periodic state snapshots
```

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
