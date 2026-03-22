# Architecture

## System Context

Who talks to what, and over which transport.

```mermaid
graph TB
    Trader["Trader (Browser)"]
    FIXClient["FIX Counterparty"]

    subgraph Web["Web Tier"]
        WebUI["Web UI<br/>React + AG Grid"]
        Babl["Babl WebSocket<br/>:8443"]
        FIXBridge["FIX Client Bridge<br/>:8444"]
    end

    subgraph Server["Server Tier"]
        Gateway["Gateway<br/>Artio FIX Acceptor :9880"]
        Cluster["Aeron Cluster (3-node)<br/>OrderBook · RFQ · Matching"]
        MediaDriver["Aeron Media Driver"]
        Pricing["Pricing Service"]
    end

    subgraph ReadSide["Read Side"]
        Projections["Projections<br/>Order · Position · Quote"]
        QueryService["QueryService"]
        EventLogger["EventLogger"]
    end

    subgraph Observability["Observability (optional profile)"]
        Prometheus["Prometheus :9090"]
        Loki["Loki :3100"]
        Grafana["Grafana :3000"]
    end

    Trader -- "WebSocket (binary SBE)" --> Babl
    Trader -- "WebSocket (JSON)" --> FIXBridge
    FIXClient -- "FIX 4.4 TCP" --> Gateway

    Babl -- "Aeron IPC" --> MediaDriver
    FIXBridge -- "Aeron IPC" --> Gateway
    Gateway -- "Aeron IPC" --> MediaDriver
    MediaDriver -- "Aeron UDP" --> Cluster
    Pricing -- "Aeron IPC" --> MediaDriver

    Cluster -- "Events (Aeron)" --> Projections
    Cluster -- "Events (Aeron)" --> EventLogger
    Projections --> QueryService
    QueryService -- "Aeron IPC" --> Babl

    EventLogger -- "Micrometer" --> Prometheus
    EventLogger -- "Structured logs" --> Loki
    Prometheus --> Grafana
    Loki --> Grafana
```

## Module Dependencies

Build order flows top-to-bottom. No circular dependencies.

```mermaid
graph TB
    messages["messages<br/>(SBE codecs)"]

    messages --> cluster
    messages --> gateway
    messages --> projections
    messages --> pricing-service["pricing-service"]
    messages --> reference-data["reference-data"]
    messages --> fix-client-bridge["fix-client-bridge"]
    messages --> websocket-server["websocket-server"]
    messages --> event-logger["event-logger"]
    messages --> sbe-ts-generator["sbe-typescript-generator"]

    cluster --> launcher
    gateway --> launcher
    media-driver["media-driver"] --> launcher
    pricing-service --> launcher
    reference-data --> launcher
    websocket-server --> launcher

    projections --> query-service["query-service"]
    cluster --> query-service

    gateway --> fix-client-bridge

    launcher --> integration-tests["integration-tests"]
    query-service --> integration-tests
```

## Transport Map

| From | To | Transport | Latency |
|------|----|-----------|---------|
| FIX Client | Gateway | TCP (FIX 4.4) | ~0.5ms |
| Gateway | Cluster | Aeron IPC (shared memory) | ~1-5us |
| Cluster | Cluster (inter-node) | Aeron UDP multicast | ~5-50us |
| Cluster | Projections | Aeron IPC | ~1-5us |
| QueryService | Babl | Aeron IPC | ~1-5us |
| Babl | Browser | WebSocket TCP | ~0.1-1ms |
| FIX Bridge | Browser | WebSocket TCP (JSON) | ~0.1-1ms |
