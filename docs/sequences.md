# Sequence Diagrams

## 1. New Order Single (NOS) — Happy Path

The core trading flow: FIX client sends order, cluster validates, executes, and returns execution report.

```mermaid
sequenceDiagram
    participant C as FIX Client
    participant G as Gateway (Artio)
    participant M as Media Driver
    participant K as Cluster (Leader)
    participant P as Projections
    participant Q as QueryService
    participant B as Netty WebSocket
    participant W as Browser

    C->>G: FIX 4.4 NewOrderSingle (35=D)
    activate G
    G->>G: FixToSbeTranslator.encode()
    G->>M: PlaceOrder (SBE, Aeron IPC)
    deactivate G

    M->>K: PlaceOrder (Aeron UDP, replicated)
    activate K
    K->>K: NewOrderSingleHandler.validateNewOrder()
    K->>K: OrderBook.add()
    K->>K: EventSink.emit(OrderCreated)
    K-->>M: OrderCreated (egress)
    deactivate K

    par FIX response
        M-->>G: OrderCreated (Aeron IPC)
        activate G
        G->>G: SbeToFixTranslator.decode()
        G-->>C: FIX 4.4 ExecutionReport (35=8, OrdStatus=0)
        deactivate G
    and Projection update
        M-->>P: OrderCreated (Aeron IPC)
        activate P
        P->>P: OrderProjection.onEvent(OrderCreated)
        P->>Q: updated OrderView
        deactivate P
    and Browser streaming
        Q->>B: OrderView (SBE, Aeron IPC)
        B->>W: SBE binary frame (WebSocket)
        W->>W: Web Worker decodes SBE
        W->>W: React re-renders blotter row
    end
```

### Latency Budget

```text
FIX parse + SBE encode:     ~5 us
Aeron IPC to cluster:       ~1-5 us
Cluster validate + apply:  ~10 us
Aeron egress:               ~1-5 us
SBE decode + FIX encode:    ~5 us
TCP to FIX client:          ~0.1 ms
─────────────────────────────────────
Total (FIX-to-FIX):        ~0.15 ms
```

---

## 2. RFQ Full Flow

Request-for-quote: client asks for a price, Pricing Service responds, client accepts, order fills. **(Planned — APP-29, APP-30. QuoteRequestHandler, RfqStateMachine, RFQ Orchestrator, and Pricing Service are not yet implemented.)**

```mermaid
sequenceDiagram
    participant C as FIX Client
    participant G as Gateway
    participant K as Cluster
    participant O as Orchestrator
    participant PS as Pricing Service
    participant P as Projections
    participant B as Browser (via Netty)

    C->>G: FIX QuoteRequest (35=R)
    G->>K: QuoteRequest (SBE)

    activate K
    K->>K: QuoteRequestHandler.validate() (planned)
    K->>K: RfqStateMachine → REQUESTED
    K-->>O: QuoteRequested (event)
    deactivate K

    activate O
    O->>PS: RequestPrice (symbol, qty, side)
    PS->>PS: Calculate bid/ask spread
    PS-->>O: PriceResponse (bid, ask)
    O->>K: SubmitQuote (SBE)
    deactivate O

    activate K
    K->>K: RfqStateMachine → QUOTED
    K-->>G: QuoteCreated (event)
    K-->>P: QuoteCreated (event)
    deactivate K

    G-->>C: FIX Quote (35=S, bid + ask)
    P-->>B: QuoteCreated (streaming)

    Note over C: Trader reviews price...

    C->>G: FIX AcceptQuote
    G->>K: AcceptQuote (SBE)

    activate K
    K->>K: RfqStateMachine → ACCEPTED
    K->>K: OrderBook.fill() (atomic)
    K->>K: RfqStateMachine → FILLED
    K-->>G: OrderFilled (event)
    K-->>P: OrderFilled (event)
    deactivate K

    G-->>C: FIX ExecutionReport (35=8, OrdStatus=2)
    P-->>B: OrderFilled (streaming)
```

---

## 3. Leader Failover

Node 0 (leader) dies. Cluster elects new leader. No messages lost.

```mermaid
sequenceDiagram
    participant C as FIX Client
    participant G as Gateway
    participant N0 as Node 0 (Leader)
    participant N1 as Node 1 (Follower)
    participant N2 as Node 2 (Follower)

    Note over N0,N2: Normal operation — Node 0 is leader

    C->>G: PlaceOrder
    G->>N0: PlaceOrder (via Media Driver)
    N0->>N1: Replicate log entry
    N0->>N2: Replicate log entry
    N0-->>G: OrderCreated
    G-->>C: ExecutionReport

    Note over N0: Node 0 crashes!

    N1->>N1: Election timeout
    N1->>N2: RequestVote
    N2-->>N1: VoteGranted

    Note over N1: Node 1 becomes leader

    N1->>G: New leader notification
    G->>G: Reconnect ClusterClient to Node 1

    C->>G: PlaceOrder (next order)
    G->>N1: PlaceOrder (via Media Driver)
    N1->>N2: Replicate log entry
    N1-->>G: OrderCreated
    G-->>C: ExecutionReport

    Note over N1,N2: Service continues with 2/3 nodes
    Note over N0: Node 0 restarts, replays log, rejoins as follower
```

### Failover Guarantees

| Guarantee | Mechanism |
|-----------|-----------|
| No message loss | Aeron log replication (majority ack before commit) |
| No duplicate processing | Cluster sequence numbers (idempotent replay) |
| Automatic reconnect | ClusterClient detects leader change, reconnects |
| State recovery | New leader has full replicated log |
| Rejoining node | Replays log from snapshot + remaining entries |

---

## 4. Startup Sequence — Reference Data Loading

The system enforces a strict startup ordering: the FIX acceptor MUST NOT bind until all reference data is loaded and confirmed by the cluster.

```mermaid
sequenceDiagram
    participant MD as Media Driver
    participant K as Cluster (3-node)
    participant RDO as ReferenceDataOrchestrator
    participant G as Gateway (Artio)
    participant O as RFQ Orchestrator

    Note over MD: Phase 1: Infrastructure
    MD->>MD: Start 4 Media Drivers (3 cluster + 1 gateway)

    Note over K: Phase 2: Cluster
    K->>K: Start 3 cluster nodes
    K->>K: Leader election
    K->>K: Restore AccountStore from snapshot (if exists)

    Note over RDO: Phase 3: Reference Data (fail-fast)
    RDO->>RDO: YamlAccountLoader reads accounts.yaml
    loop For each account
        RDO->>MD: LoadAccount (SBE, templateId=11)
        MD->>K: LoadAccount (Aeron UDP, replicated)
        K->>K: LoadAccountHandler validates + upserts
        alt Valid
            K-->>MD: AccountLoaded (110)
            MD-->>RDO: AccountLoaded (Aeron IPC)
        else Invalid or duplicate code
            K-->>MD: AccountLoadRejected (111)
            MD-->>RDO: AccountLoadRejected (Aeron IPC)
            Note over RDO: ABORT STARTUP
        end
    end
    Note over RDO: All accounts confirmed (or 10s timeout -> abort)

    Note over G: Phase 4: Gateway
    G->>G: Start Artio FIX acceptor
    G->>G: Bind to configured port (default 9880, see gateway.properties)
    Note over G: FIX clients can now connect

    Note over O: Phase 5: RFQ Orchestrator
    O->>O: Start, connect to cluster egress
    Note over O: Ready to process QuoteRequested events
```

### Startup Invariants

| Invariant | Enforcement |
|-----------|-------------|
| No orders before accounts loaded | Gateway starts AFTER ReferenceDataOrchestrator completes |
| Partial load = no startup | Any AccountLoadRejected or 10s timeout aborts the process |
| Idempotent on bounce | Upsert semantics — re-loading same accounts is safe |
| Snapshot recovery is transparent | Cluster restores from snapshot; orchestrator re-sends (idempotent) |
| Account changes require restart | No hot-reload; restart ReferenceDataOrchestrator to pick up new accounts |
| Projections replay from position 0 | No projection snapshots; Archive log never truncated |
| Stale RFQs expired on recovery | RfqStateMachine checks TTL post-snapshot restore; emits QuoteExpired for elapsed RFQs |
