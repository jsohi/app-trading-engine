# State Machines

## Order Lifecycle

```mermaid
stateDiagram-v2
    [*] --> PendingNew: PlaceOrder received

    PendingNew --> New: OrderAccepted
    PendingNew --> Rejected: OrderRejected

    New --> PartiallyFilled: OrderPartiallyFilled
    New --> Filled: OrderFilled
    New --> Cancelled: OrderCancelled

    PartiallyFilled --> PartiallyFilled: OrderPartiallyFilled
    PartiallyFilled --> Filled: OrderFilled
    PartiallyFilled --> Cancelled: OrderCancelled

    Filled --> [*]
    Cancelled --> [*]
    Rejected --> [*]
```

### Rejection Reasons

| Reason | Trigger |
|--------|---------|
| `UNKNOWN_SYMBOL` | Symbol not in OrderBook |
| `INVALID_QUANTITY` | qty <= 0 or not int64 |
| `INVALID_PRICE` | price <= 0 (fixed-point) |
| `DUPLICATE_CLORDID` | clOrdId already exists |
| `BOOK_FULL` | OrderBook capacity exceeded |

---

## RFQ Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Idle

    Idle --> Requested: QuoteRequest
    Requested --> Quoted: PriceResponse (from Pricing Service)
    Requested --> Rejected: validation failure
    Requested --> Expired: timeout (no price within TTL)

    Quoted --> Accepted: AcceptQuote
    Quoted --> Rejected: RejectQuote
    Quoted --> Expired: quote TTL elapsed

    Accepted --> Filled: OrderFilled (cluster confirms)
    Accepted --> Rejected: fill failed

    Filled --> [*]
    Rejected --> [*]
    Expired --> [*]
```

### RFQ Timeouts

| State | Timeout | Action |
|-------|---------|--------|
| Requested | 5s (configurable) | Transition to Expired, notify client |
| Quoted | 30s (configurable) | Transition to Expired, notify client |

### Multi-Leg RFQ (Swap)

Same state machine, but the `QuoteRequest` contains a `NoLegs` repeating group (near leg + far leg). The Pricing Service prices both legs and the cluster fills both atomically.

```
QuoteRequest (Swap)
├── Leg 1: EUR/USD Spot  T+2    1,000,000
└── Leg 2: EUR/USD Fwd   1M     1,000,000 (opposite side)

QuoteCreated (Swap)
├── Leg 1: bid=1.08500000  ask=1.08520000
└── Leg 2: bid=1.08650000  ask=1.08670000
     swap points = fwd - spot = 15.0 / 15.0 pips

AcceptQuote → fills BOTH legs atomically
```

---

## Cluster Consensus

```mermaid
stateDiagram-v2
    [*] --> Follower

    Follower --> Candidate: election timeout
    Follower --> Follower: AppendEntries from leader

    Candidate --> Leader: majority votes received
    Candidate --> Follower: higher term discovered
    Candidate --> Candidate: election timeout (split vote)

    Leader --> Follower: higher term discovered
    Leader --> Leader: heartbeat / log replication

    note right of Leader
        Only the leader processes
        commands and emits events.
        Followers replay the log.
    end note
```

This is handled by Aeron Cluster (Raft-based) — we don't implement consensus, but we must understand it for failover testing (APP-18).
