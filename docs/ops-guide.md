# Trading Engine -- Operational Runbook

Production operations guide for the Aeron Cluster-based trading engine.
Covers health checks, failover, emergency procedures, audit trails, archive management,
log retention, and common troubleshooting scenarios.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Health Checks](#2-health-checks)
3. [Failover Procedures](#3-failover-procedures)
4. [Emergency Trading Halt](#4-emergency-trading-halt)
5. [FIX Message Audit Trail](#5-fix-message-audit-trail)
6. [Archive Management](#6-archive-management)
7. [Log Retention Policy](#7-log-retention-policy)
8. [Common Troubleshooting](#8-common-troubleshooting)

---

## 1. System Overview

### Architecture

```text
FIX Clients ──TCP:9880──► Artio FIX Gateway ──IPC──► Aeron Cluster (3-node Raft)
                              │                            │
                              ▼                            ▼
                        ClusterClient              TradingClusteredService
                        (egress poll)              (order matching, state machine)
                                                         │
                                                         ▼
                                                   Aeron Archive
                                                   (event log)
                                                         │
                                                         ▼
                                                   Projections
                                                   (replay from pos 0)
```

### Process Layout

| Process            | Count | Aeron Directory          | Description                          |
|--------------------|-------|--------------------------|--------------------------------------|
| Media Driver 0     | 1     | `/tmp/aeron-node-0`      | Shared memory transport for node 0   |
| Media Driver 1     | 1     | `/tmp/aeron-node-1`      | Shared memory transport for node 1   |
| Media Driver 2     | 1     | `/tmp/aeron-node-2`      | Shared memory transport for node 2   |
| Media Driver (GW)  | 1     | `/tmp/aeron-gateway`     | Shared memory transport for gateway  |
| Cluster Node 0     | 1     | Uses driver 0            | Raft consensus + order matching      |
| Cluster Node 1     | 1     | Uses driver 1            | Raft consensus + order matching      |
| Cluster Node 2     | 1     | Uses driver 2            | Raft consensus + order matching      |
| FIX Gateway        | 1     | Uses gateway driver      | Artio FIX 4.4 acceptor               |

### Port Scheme

| Node | Ingress (UDP) | Consensus (UDP) | Log (UDP) | Catchup (UDP) | Archive (UDP) |
|------|---------------|-----------------|-----------|---------------|---------------|
| 0    | 20110         | 20220           | 20330     | 20440         | 8010          |
| 1    | 21110         | 21220           | 21330     | 21440         | 8011          |
| 2    | 22110         | 22220           | 22330     | 22440         | 8012          |

| Service     | Port  | Protocol |
|-------------|-------|----------|
| FIX Gateway | 9880  | TCP      |

### Directory Structure

```text
<baseDir>/                        # Default: cluster-data/
  archive-0/                      # Aeron Archive data for node 0
  archive-1/                      # Aeron Archive data for node 1
  archive-2/                      # Aeron Archive data for node 2
  cluster-0/                      # Cluster state (Raft log, snapshots) for node 0
  cluster-1/                      # Cluster state for node 1
  cluster-2/                      # Cluster state for node 2

/tmp/aeron-node-0/                # Media Driver CnC + shared memory for node 0
/tmp/aeron-node-1/                # Media Driver CnC + shared memory for node 1
/tmp/aeron-node-2/                # Media Driver CnC + shared memory for node 2
/tmp/aeron-gateway/               # Media Driver CnC + shared memory for gateway

logs/                             # Default log directory
  media-driver-0.stdout.log       # Media driver 0 stdout/stderr
  media-driver-1.stdout.log
  media-driver-2.stdout.log
  media-driver-3.stdout.log       # Gateway media driver
  pids/
    media-driver-0.pid            # PID files for orphan detection
    media-driver-1.pid
    media-driver-2.pid
    media-driver-3.pid

fix-logs/                         # Artio FIX message logs
```

### Startup Sequence

The `TradingEngineLauncher` enforces a strict 12-step startup invariant:

1. Parse + validate config from system properties
2. Create log directories
3. Register shutdown hook (before any resource creation)
4. Spawn media driver processes (one per cluster node + one for gateway)
5. Validate driver liveness via `Aeron.connect()` with 15s timeout
6. Build cluster member + ingress endpoint strings
7. Launch cluster nodes (Archive + ConsensusModule + ClusteredServiceContainer per node)
8. Load reference data (waits for leader election via `AeronCluster.connect()`)
9. Launch FIX gateway (Artio engine + library + ClusterClient)
10. Wait for gateway cluster client CONNECTED state (30s timeout)
11. Log `SYSTEM_READY` with total startup time
12. Block on `ShutdownSignalBarrier.await()`

The system logs `SYSTEM_READY` only after all components are verified healthy.

---

## 2. Health Checks

### 2.1 Media Driver Health

The Media Driver's CnC (Command and Control) file is the primary liveness indicator.

**Check CnC file exists and is readable:**

```bash
# For each node driver (0, 1, 2) and the gateway driver
ls -la /tmp/aeron-node-0/cnc.dat
ls -la /tmp/aeron-node-1/cnc.dat
ls -la /tmp/aeron-node-2/cnc.dat
ls -la /tmp/aeron-gateway/cnc.dat
```

If `cnc.dat` is missing, the media driver is not running or has crashed. The
`ClusterNodeLauncher` checks for this file and fails fast with:
`"media driver not running at <dir>: cnc.dat missing"`.

**Check driver PID is alive:**

```bash
cat logs/pids/media-driver-0.pid
# Use the PID to verify the process is running
ps -p $(cat logs/pids/media-driver-0.pid)
```

**Check driver error counters with AeronStat:**

```bash
# Inspect all counters for a specific media driver
java -cp <classpath> io.aeron.samples.AeronStat \
  aeron.dir=/tmp/aeron-node-0

# Filter for error counters only
java -cp <classpath> io.aeron.samples.AeronStat \
  aeron.dir=/tmp/aeron-node-0 \
  | grep -i error
```

Key counters to watch:

| Counter                          | Healthy Value | Action if Abnormal                    |
|----------------------------------|---------------|---------------------------------------|
| `Errors`                         | 0             | Check error log, may need restart     |
| `Short sends`                    | 0             | Network/driver saturation             |
| `Back pressure`                  | Low/stable    | Consumers too slow                    |
| `Unblocked publications`         | 0             | Slow consumer detected                |
| `Conductor max cycle time (ns)`  | < 1ms         | Driver thread is stalling             |

**Check driver heartbeat (CnC timestamp):**

```bash
java -cp <classpath> io.aeron.samples.CncFileReader \
  aeron.dir=/tmp/aeron-node-0
```

The CnC file contains a heartbeat timestamp updated by the driver conductor. A stale
timestamp (not updating for > 10s) indicates the driver thread is hung.

### 2.2 Cluster Leader Status

**Inspect cluster state via AeronStat counters:**

```bash
# Check all three nodes
for i in 0 1 2; do
  echo "=== Node $i ==="
  java -cp <classpath> io.aeron.samples.AeronStat \
    aeron.dir=/tmp/aeron-node-$i \
    | grep -E "(Cluster|Consensus|Election|Role)"
done
```

Key cluster counters:

| Counter                          | Description                              |
|----------------------------------|------------------------------------------|
| `Cluster node role`              | 0=Follower, 1=Candidate, 2=Leader        |
| `Cluster election state`         | Current election phase                   |
| `Cluster commit position`        | Highest committed log position           |
| `Cluster append position`        | Leader's append position                 |
| `Cluster snapshot count`         | Number of snapshots taken                |
| `Cluster errors`                 | Cluster-level error count                |

**Identify the current leader:**

```bash
for i in 0 1 2; do
  role=$(java -cp <classpath> io.aeron.samples.AeronStat \
    aeron.dir=/tmp/aeron-node-$i \
    | grep "Cluster node role" | awk '{print $NF}')
  if [ "$role" = "2" ]; then
    echo "Leader: node $i"
  fi
done
```

**Using ClusterTool (Aeron's built-in cluster management tool):**

```bash
# Query cluster state from any node's cluster directory
java -cp <classpath> io.aeron.cluster.ClusterTool \
  cluster-data/cluster-0 describe

# Check recording log
java -cp <classpath> io.aeron.cluster.ClusterTool \
  cluster-data/cluster-0 recording-log
```

### 2.3 Projection Lag Monitoring

Projections replay all events from Aeron Archive position 0 on recovery and track their
progress via `ProjectionRegistry`.

**Health check API:**

```java
// Returns true iff every registered projection's lag <= lagThreshold
boolean healthy = projectionRegistry.isHealthy();

// Per-projection lag snapshot (name -> message count behind head)
Map<String, Long> lag = projectionRegistry.getLag();
```

**Monitoring checklist:**

- `ProjectionRegistry.isHealthy()` should return `true` under normal operations
- `ProjectionRegistry.getLag()` values should be near 0 (< lagThreshold)
- On startup/recovery, lag will be temporarily elevated as projections replay from position 0
- Sustained lag indicates a slow projection or a projection processing error

**Alerting thresholds:**

| Metric                    | Warning     | Critical      |
|---------------------------|-------------|---------------|
| Projection lag (messages) | > 1,000     | > 10,000      |
| isHealthy()               | --          | false         |
| Recovery replay duration  | > 5 min     | > 30 min      |

### 2.4 FIX Session Status

**Check Artio FIX engine health:**

The `FixGateway` logs session events via GFLog:
- `"Session acquired: sessionId=<id>"` -- new session connected
- `"Session capacity exceeded, disconnecting: sessionId=<id>"` -- capacity limit hit
- `"Swept <N> stale correlations"` -- periodic cleanup (every 60s)

**Session capacity:**

| Parameter                | Default | Description                       |
|--------------------------|---------|-----------------------------------|
| MAX_SESSIONS             | 64      | Global max concurrent sessions    |
| MAX_SESSIONS_PER_COMP_ID | 4       | Per-CompID session limit          |
| CORRELATION_CAPACITY     | 1024    | Max in-flight request correlations|
| IN_FLIGHT_CAPACITY       | 4096    | In-flight command tracker size    |
| IN_FLIGHT_TIMEOUT_NS     | 30s     | In-flight request timeout         |

**Check FIX session connectivity via AeronStat:**

```bash
# Artio library counters visible in the gateway media driver
java -cp <classpath> io.aeron.samples.AeronStat \
  aeron.dir=/tmp/aeron-gateway \
  | grep -i artio
```

### 2.5 Cluster Client Health (Gateway -> Cluster)

The `ClusterClient` maintains a state machine: `DISCONNECTED -> CONNECTED <-> RECONNECTING -> CLOSED`.

**Health indicators:**

- `ClusterClient.isConnected()` -- `true` when the gateway has an active cluster session
- On connection loss, the client enters `RECONNECTING` with exponential backoff
- After `maxReconnectAttempts`, the client transitions to `CLOSED` and reports a fatal error

**Check gateway cluster connection via AeronStat:**

```bash
java -cp <classpath> io.aeron.samples.AeronStat \
  aeron.dir=/tmp/aeron-gateway \
  | grep -E "(Cluster|session)"
```

---

## 3. Failover Procedures

### 3.1 Single-Node Failure (Automatic)

**Behavior:** Aeron Cluster uses Raft consensus with 3 nodes. A single-node failure triggers
automatic leader re-election. With the `NanosecondClusterClock`, re-election typically
completes in < 100ms.

**Symptoms:**

- `Cluster node role` counter on surviving nodes shows election activity
- A new leader is elected from the remaining 2 nodes
- In-flight messages may see brief backpressure during election
- The `ClusterClient` in the gateway may briefly enter `RECONNECTING` state

**Detection:**

```bash
# Check which nodes are still responding
for i in 0 1 2; do
  if [ -f /tmp/aeron-node-$i/cnc.dat ]; then
    echo "Node $i: driver alive"
  else
    echo "Node $i: driver MISSING"
  fi
done
```

**Recovery:**

1. Diagnose the root cause on the failed node (OOM, disk full, hardware failure)
2. Clear stale data if necessary:
   ```bash
   # Only if the node's state is corrupted
   rm -rf cluster-data/archive-<N>
   rm -rf cluster-data/cluster-<N>
   ```
3. Restart the failed node -- it will catch up from the leader's archive via Aeron's
   built-in catchup replay
4. Verify the node has rejoined:
   ```bash
   java -cp <classpath> io.aeron.samples.AeronStat \
     aeron.dir=/tmp/aeron-node-<N> \
     | grep "Cluster node role"
   # Should show 0 (Follower) after catching up
   ```

### 3.2 Manual Failover Trigger

To force a specific node to step down as leader (e.g., for planned maintenance):

```bash
# Use ClusterTool to initiate orderly shutdown of the leader node.
# This triggers a clean snapshot, drains in-flight messages, and steps down.
# Remaining nodes will elect a new leader.
java -cp <classpath> io.aeron.cluster.ClusterTool \
  cluster-data/cluster-<LEADER_NODE_ID> shutdown

# Note: `invalidate-latest-snapshot` is for CORRUPTION RECOVERY only,
# not for planned failover. Using it for maintenance risks unnecessary
# full replay from archive on restart.
```

**Planned maintenance procedure:**

1. Identify current leader (see section 2.2)
2. Verify the other two nodes are healthy and caught up (commit positions match)
3. Gracefully stop the leader node
4. Wait for re-election (< 100ms)
5. Verify new leader is elected and processing requests
6. Perform maintenance on the stopped node
7. Restart the maintained node; it rejoins as a follower

### 3.3 Quorum Loss (2 of 3 Nodes Down)

**This is a critical failure.** Raft requires a majority quorum (2 of 3) for consensus.
With only 1 node remaining, the cluster cannot:
- Accept new commands
- Commit new log entries
- Take snapshots
- Elect a leader

**Symptoms:**

- Gateway `ClusterClient` enters `RECONNECTING` and eventually `CLOSED` after
  `maxReconnectAttempts`
- FIX clients receive no execution reports
- The surviving node's `Cluster node role` counter shows `1` (Candidate), endlessly
  attempting elections it cannot win
- No new entries appear in the cluster log

**Detection:**

```bash
# Count alive nodes
alive=0
for i in 0 1 2; do
  if [ -f /tmp/aeron-node-$i/cnc.dat ] && \
     ps -p $(cat logs/pids/media-driver-$i.pid 2>/dev/null) > /dev/null 2>&1; then
    alive=$((alive + 1))
  fi
done
echo "Alive nodes: $alive"
if [ $alive -lt 2 ]; then
  echo "CRITICAL: Quorum lost"
fi
```

**Recovery -- restore at least 2 nodes:**

1. **Preferred:** Bring a second node back online. It will catch up from the surviving
   node's archive and re-establish quorum.
2. **If node state is corrupted:** Clear the corrupted node's data directories and let it
   do a full catch-up:
   ```bash
   rm -rf cluster-data/archive-<N>
   rm -rf cluster-data/cluster-<N>
   ```

**Recovery -- forced unsafe bootstrap (last resort):**

Use this ONLY when the surviving node's peers cannot be recovered and you accept the risk
of data loss for any uncommitted entries.

```bash
# WARNING: This is a destructive operation. It rewrites the cluster's membership
# to a single node, discarding any uncommitted log entries.

# 1. Stop ALL remaining cluster processes

# 2. Use ClusterTool to force a single-node bootstrap from the surviving node
java -cp <classpath> io.aeron.cluster.ClusterTool \
  cluster-data/cluster-<SURVIVING_NODE_ID> \
  describe

# 3. Verify the recording log is intact
java -cp <classpath> io.aeron.cluster.ClusterTool \
  cluster-data/cluster-<SURVIVING_NODE_ID> \
  recording-log

# 4. Create a single-node cluster configuration and restart
#    (requires updating clusterMembers to contain only the surviving node)

# 5. Once stable, add replacement nodes one at a time using dynamic membership
#    changes (Aeron Cluster supports adding members at runtime)
```

**Post-quorum-loss checklist:**

- [ ] Verify EventSequencer gaplessness (see section 8.1)
- [ ] Verify projection replay completes without gaps
- [ ] Reconcile FIX message audit trail against cluster events
- [ ] Notify counterparties of any potential message loss
- [ ] File incident report

---

## 4. Emergency Trading Halt

### 4.1 Halt Order Acceptance Without Shutting Down

The `FixGateway` supports a draining mode that rejects new orders while keeping the
system running for monitoring and position queries.

**Gateway-level halt (reject new FIX orders):**

The gateway's `draining` flag stops new order acceptance. Currently this is triggered
only during shutdown. For a live trading halt without shutdown, the recommended approach
is:

1. **Disable FIX connectivity** at the network level:
   ```bash
   # Block new TCP connections to FIX port 9880
   iptables -A INPUT -p tcp --dport 9880 -j REJECT
   # Existing sessions remain but new orders will not reach the cluster
   ```

2. **Disconnect all FIX sessions** by sending Logout to connected clients:
   ```bash
   # The FixGateway.onClose() drain sequence handles this automatically.
   # For a controlled halt without full shutdown, trigger via JMX or a
   # dedicated admin command (requires implementation per APP-153).
   ```

### 4.2 Account Suspension (Per-Account Trading Halt)

Account-level suspension is managed through the cluster's reference data system:

1. Submit an account status change command through the cluster client
2. The `AccountStore` within `TradingClusteredService` will reject commands for
   suspended accounts
3. This is deterministic and replicated across all nodes

### 4.3 Full System Halt

**Graceful shutdown procedure:**

```bash
# 1. Send SIGTERM to the TradingEngineLauncher process
kill <launcher_pid>

# The shutdown hook fires and executes the following in order:
#   a. Close gateway (drain in-flight, send FIX Logouts, close Artio)
#   b. Close cluster nodes in reverse order (container -> consensus -> archive)
#   c. SIGTERM all media driver processes (10s timeout, then SIGKILL)
#   d. Clean up PID files
```

**Emergency shutdown (immediate):**

```bash
# Only if graceful shutdown is unresponsive
kill -9 <launcher_pid>

# Then clean up media driver processes manually
for pid_file in logs/pids/media-driver-*.pid; do
  kill -9 $(cat $pid_file) 2>/dev/null
done

# Clean up shared memory (only if driver processes are confirmed dead)
rm -rf /tmp/aeron-node-0
rm -rf /tmp/aeron-node-1
rm -rf /tmp/aeron-node-2
rm -rf /tmp/aeron-gateway
```

**Post-halt verification:**

```bash
# Verify no orphaned processes
ps aux | grep -E "(MediaDriverLauncher|TradingEngineLauncher)" | grep -v grep

# Verify no stale shared memory
ls -la /tmp/aeron-node-* 2>/dev/null
ls -la /tmp/aeron-gateway 2>/dev/null
```

---

## 5. FIX Message Audit Trail

### 5.1 Artio Message Logging

Artio is configured to log all inbound and outbound FIX messages:

```java
// From FixGateway.onStart()
engineConfig
    .logInboundMessages(true)
    .logOutboundMessages(true)
    .logFileDir("fix-logs");
```

### 5.2 Log Location

FIX message logs are written to the directory specified by `logFileDir` (currently
`fix-logs/` relative to the working directory).

```bash
ls -la fix-logs/
```

Artio writes binary log files containing the raw FIX message bytes with metadata
(timestamps, session info, direction).

### 5.3 Log Format

Artio uses a native binary format for FIX message logging:

- Each log entry contains the raw FIX message bytes
- Metadata includes: timestamp, session ID, direction (inbound/outbound), sequence number
- The binary format is space-efficient and fast to write (zero-copy where possible)

### 5.4 Querying FIX Message History

**Using Artio's built-in log reader:**

```bash
# Read FIX message logs using Artio's built-in log reader.
# NOTE: The exact class name may vary by Artio version. Verify against
# Artio 0.175 Javadoc before use. If FixMessageLogger is not available,
# check for FixArchiveScanner or ReplayQuery in the Artio API.
java -cp <classpath> uk.co.real_logic.artio.engine.logger.FixMessageLogger \
  fix-logs/
```

**Filtering by session or time range:**

```bash
# Custom reader for filtered FIX message queries.
# This tool does not exist yet -- implement using Artio's ReplayQuery API.
# Filter by SenderCompID, time range, message type (MsgType tag 35).
# java -cp <classpath> com.trading.engine.tools.FixLogReader \
#   --log-dir=fix-logs \
#   --sender-comp-id=CLIENT1 \
#   --start-time="2026-04-12T00:00:00Z" \
#   --end-time="2026-04-12T23:59:59Z" \
#   --msg-type=D
```

**Key FIX message types for audit:**

| MsgType (tag 35) | Name              | Direction | Description                     |
|-------------------|-------------------|-----------|---------------------------------|
| A                 | Logon             | Both      | Session authentication          |
| 5                 | Logout            | Both      | Session termination             |
| D                 | NewOrderSingle    | Inbound   | Order submission                |
| F                 | OrderCancelRequest| Inbound   | Order cancellation              |
| 8                 | ExecutionReport   | Outbound  | Order fill/ack/reject           |
| 9                 | OrderCancelReject | Outbound  | Cancel rejection                |
| S                 | Quote             | Outbound  | RFQ quote response              |

### 5.5 Regulatory Compliance

- FIX message logs are the primary regulatory audit trail for all client-facing messages
- Retain for the regulatory period (see section 7)
- Logs must be immutable after write -- do not modify or delete during retention period
- Cross-reference FIX logs with cluster event sequence numbers for full traceability

---

## 6. Archive Management

### 6.1 Critical Rule: Never Truncate the Archive Log

**The Aeron Archive log MUST NEVER be truncated.** Projections replay all events from
position 0 on recovery. Truncating the archive will cause projections to produce
incomplete or inconsistent read models.

```text
WARNING: Truncating the Aeron Archive log is a data-loss event.
Projections do NOT snapshot -- they replay the ENTIRE event stream on startup.
```

### 6.2 Disk Growth Estimation

| Metric                    | Estimate                              |
|---------------------------|---------------------------------------|
| Average event size        | ~1 KB (SBE-encoded, including header) |
| Events per day (typical)  | ~100,000                              |
| Daily disk growth         | ~100 MB/day                           |
| Monthly disk growth       | ~3 GB/month                           |
| Annual disk growth        | ~36 GB/year                           |

These estimates assume typical FX trading volumes. Scale linearly for higher throughput.

**Monitor archive disk usage:**

```bash
# Check archive directory sizes per node
for i in 0 1 2; do
  echo "=== Archive Node $i ==="
  du -sh cluster-data/archive-$i/
done
```

**Set up disk space alerts:**

| Threshold       | Action                                      |
|-----------------|---------------------------------------------|
| 80% disk usage  | Warning alert -- plan capacity expansion     |
| 90% disk usage  | Critical alert -- expand storage immediately |
| 95% disk usage  | Emergency -- risk of archive write failure   |

### 6.3 Snapshot Retention

Snapshots are taken periodically by `TradingClusteredService.onTakeSnapshot()`. Each
snapshot is a 7-fragment envelope:

1. `SnapshotTaken` header (template 200) with CRC32C checksum
2. `EventSequencerSnapshot` (template 206)
3. `IdGeneratorSnapshot` (template 205)
4. `AccountSnapshot` (template 201)
5. `CurrencySnapshot` (template 208)
6. `RiskLimitSnapshot` (template 209)
7. `OrderBookSnapshot` (template 202)

**Note:** Templates 203 (`RfqStateSnapshot`) and 204 (`PositionSnapshot`) are defined in the SBE schema but are **not included** in the current snapshot envelope. `RfqStateMachine` (APP-30) and `PositionTracker` are not yet implemented. When added, the fragment count will increase accordingly.

**Retention policy:**

- Retain the last 3 snapshots per node
- Aeron Cluster manages snapshot lifecycle automatically via the recording log
- Older snapshots can be invalidated using ClusterTool:
  ```bash
  java -cp <classpath> io.aeron.cluster.ClusterTool \
    cluster-data/cluster-<N> snapshot-count
  ```

**Verify snapshot integrity:**

```bash
# List all recordings (snapshots + log segments) for a node
java -cp <classpath> io.aeron.archive.samples.ListRecordings \
  aeron.dir=/tmp/aeron-node-0 \
  aeron.archive.dir=cluster-data/archive-0
```

The snapshot envelope includes a CRC32C checksum. On restore, `TradingClusteredService.onStart()`
verifies the checksum and rejects corrupted snapshots with a clear error message.

### 6.4 Future: External Event Archival (APP-68)

APP-68 is planned to implement event archival to an external store (e.g., S3, GCS) for:
- Long-term retention beyond local disk capacity
- Cross-region disaster recovery
- Regulatory compliance archival

Until APP-68 is implemented, all event data lives exclusively in the Aeron Archive.
Plan disk capacity accordingly.

---

## 7. Log Retention Policy

### 7.1 Retention Matrix

| Log Type                          | Location                   | Rotation   | Retention      | Rationale                          |
|-----------------------------------|----------------------------|------------|----------------|------------------------------------|
| Aeron Archive (event log)         | `cluster-data/archive-N/`  | None       | **Never delete** | Projections replay from position 0 |
| Artio FIX message logs            | `fix-logs/`                | None       | 7 years        | Regulatory (FX: MiFID II/Dodd-Frank)|
| GFLog (cluster, gateway hot path) | Per `gflog.xml` config     | Daily      | 30 days        | Operational diagnostics            |
| Log4j2 (launcher, media driver)   | Per `log4j2.xml` config    | Daily      | 14 days        | Infrastructure diagnostics         |
| Media driver stdout               | `logs/media-driver-N.stdout.log` | Per restart | 14 days  | Driver crash diagnostics           |
| Prometheus metrics                | Prometheus TSDB            | Continuous | 15 days (default) | Performance trending            |
| Loki structured logs              | Loki storage               | Continuous | Configurable   | Centralized log search             |

### 7.2 GFLog Configuration (Hot Path)

GFLog is used for zero-allocation logging in the cluster service and FIX gateway.
Configuration is in `src/main/resources/gflog.xml`.

```bash
# Typical GFLog rotation setup
# Configured via gflog.xml -- daily rotation, 30-day retention
```

### 7.3 Log4j2 Configuration (Infrastructure)

Log4j2 with LMAX Disruptor async logging is used for infrastructure modules (launcher,
media driver, websocket-server). Configuration is in `src/main/resources/log4j2.xml`.

```bash
# Log4j2 uses AsyncLoggerContextSelector for garbage-free mode
# Configured via log4j2.xml -- daily rotation, 14-day retention
```

### 7.4 Automated Cleanup Script

```bash
#!/usr/bin/env bash
# cleanup-logs.sh -- Run daily via cron
# Cleans up rotated logs past their retention period.
# Does NOT touch Aeron Archive or Artio FIX logs.

LOG_DIR="${1:-logs}"
GFLOG_RETENTION_DAYS=30
LOG4J2_RETENTION_DAYS=14

echo "$(date): Starting log cleanup in $LOG_DIR"

# Clean GFLog rotated files (30 days)
find "$LOG_DIR" -name "*.gflog.*" -mtime +$GFLOG_RETENTION_DAYS -delete -print

# Clean Log4j2 rotated files (14 days)
find "$LOG_DIR" -name "*.log.*" -mtime +$LOG4J2_RETENTION_DAYS -delete -print

# Clean media driver stdout logs (14 days, but keep the current one)
find "$LOG_DIR" -name "media-driver-*.stdout.log.*" -mtime +$LOG4J2_RETENTION_DAYS -delete -print

echo "$(date): Log cleanup complete"
```

---

## 8. Common Troubleshooting

### 8.1 EventSequencer Gap Debugging

The `EventSequencer` assigns monotonically increasing, gapless sequence numbers (1-based)
to all domain events. Projections rely on gaplessness to detect event loss.

**Symptoms of a gap:**

- Projection reports events arriving out of order
- `ProjectionRegistry.isHealthy()` returns `false` with one projection lagging
- Inconsistencies between cluster state and projection read models

**Debugging steps:**

1. **Identify the gap range:**
   ```java
   // Check the EventSequencer's current counter on each node
   // The leader's counter should be the authoritative value
   long currentSeq = eventSequencer.currentSequence();
   ```

2. **Check snapshot consistency:**
   The `EventSequencer` serializes `nextSequence` (= `counter + 1`) in little-endian
   format during snapshot. After snapshot restore, the next call to `nextSequence()`
   returns the restored value. A corrupted snapshot could cause a sequence jump.

   ```bash
   # Verify snapshot integrity via CRC32C
   java -cp <classpath> io.aeron.cluster.ClusterTool \
     cluster-data/cluster-<N> describe
   ```

3. **Replay from archive to identify the gap:**
   Use Aeron Archive replay to read the event stream and find the missing sequence:
   ```bash
   # List recordings to find the log recording ID
   java -cp <classpath> io.aeron.archive.samples.ListRecordings \
     aeron.dir=/tmp/aeron-node-0 \
     aeron.archive.dir=cluster-data/archive-0
   ```

4. **Check for node divergence:**
   If nodes have diverged (different commit positions), the gap may be caused by a
   split-brain scenario (should be impossible with Raft, but check):
   ```bash
   for i in 0 1 2; do
     echo "=== Node $i ==="
     java -cp <classpath> io.aeron.samples.AeronStat \
       aeron.dir=/tmp/aeron-node-$i \
       | grep "commit position"
   done
   ```

### 8.2 Snapshot Failure Recovery

**Symptoms:**

- `onTakeSnapshot()` throws or logs an error
- Snapshot recording appears incomplete in the archive
- Node restart takes longer than expected (replaying from an older snapshot)

**Common causes and fixes:**

1. **Backpressure during snapshot publication:**
   The service retries up to `MAX_BACKPRESSURE_RETRY` (128) times. If all retries fail:
   - Check archive disk space
   - Check archive thread health
   - The snapshot channel uses a 128 MB term buffer (`SNAPSHOT_CHANNEL`), yielding a
     16 MB max message length. If state exceeds this:
     ```
     Double the term-length in SNAPSHOT_CHANNEL to 256 MB (32 MB max message)
     and restart all nodes.
     ```

2. **CRC32C checksum mismatch on restore:**
   - The snapshot envelope is corrupt
   - The service will refuse to restore and throw an error
   - Recovery: the node will fall back to the previous valid snapshot, or replay from
     the beginning of the log if no valid snapshot exists

3. **Missing snapshot fragments:**
   The service expects exactly 6 body fragments (+ 1 header = 7 total). If any are
   missing:
   ```
   EventSequencerSnapshot, IdGeneratorSnapshot, AccountSnapshot,
   CurrencySnapshot, RiskLimitSnapshot, OrderBookSnapshot
   ```
   Check the archive recording for the snapshot and verify fragment count.

**Force a new snapshot:**

```bash
# Trigger snapshot via ClusterTool
java -cp <classpath> io.aeron.cluster.ClusterTool \
  cluster-data/cluster-<LEADER_NODE_ID> snapshot
```

### 8.3 Media Driver Stuck/Unresponsive

**Symptoms:**

- `CncFileReader` shows a stale heartbeat timestamp
- `AeronStat` hangs or times out
- Cluster nodes or gateway report `NOT_CONNECTED` on publications
- `Aeron.connect()` times out (configured at 15s in the launcher)

**Detection:**

```bash
# Check if the driver process is alive
ps -p $(cat logs/pids/media-driver-0.pid)

# Check CPU usage -- a stuck driver may be spinning or completely idle
top -p $(cat logs/pids/media-driver-0.pid)

# Check the driver's stdout log for errors
tail -100 logs/media-driver-0.stdout.log
```

**Recovery:**

1. **If the driver process is alive but unresponsive:**
   ```bash
   # Send SIGTERM first (graceful)
   kill $(cat logs/pids/media-driver-0.pid)

   # Wait 10s, then SIGKILL if still alive
   sleep 10
   kill -9 $(cat logs/pids/media-driver-0.pid) 2>/dev/null

   # Clean up the Aeron directory
   rm -rf /tmp/aeron-node-0
   ```

2. **If shared memory is corrupted:**
   ```bash
   # Remove the Aeron media driver directories
   rm -rf /tmp/aeron-node-0 /tmp/aeron-node-1 /tmp/aeron-node-2 /tmp/aeron-gateway
   ```

3. **Restart the media driver:**
   ```bash
   java --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
        --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
        -cp <classpath> \
        com.trading.engine.media.driver.MediaDriverLauncher \
        --aeron-dir=/tmp/aeron-node-0 \
        --dir-delete-on-start=true
   ```

4. **The cluster node using this driver must also be restarted** -- it will catch up
   from the surviving nodes.

**Prevention:**

- Use `DEDICATED` threading mode for production (configured in `ClusterNodeLauncher`
  for the Archive; media driver defaults can be set via `media-driver.properties`)
- Monitor `Conductor max cycle time` counter -- sustained values > 1ms indicate thread
  starvation
- Ensure no other processes are competing for the CPU cores assigned to the driver

### 8.4 Aeron Counter Inspection

AeronStat is the primary tool for inspecting Aeron's internal counters.

**Full counter dump:**

```bash
java -cp <classpath> io.aeron.samples.AeronStat \
  aeron.dir=/tmp/aeron-node-0
```

**Filtered counter inspection:**

```bash
# Errors only
java -cp <classpath> io.aeron.samples.AeronStat \
  aeron.dir=/tmp/aeron-node-0 \
  | grep -i error

# Publication/subscription state
java -cp <classpath> io.aeron.samples.AeronStat \
  aeron.dir=/tmp/aeron-node-0 \
  | grep -E "(pub|sub)" -i

# Archive counters
java -cp <classpath> io.aeron.samples.AeronStat \
  aeron.dir=/tmp/aeron-node-0 \
  | grep -i archive

# Cluster consensus counters
java -cp <classpath> io.aeron.samples.AeronStat \
  aeron.dir=/tmp/aeron-node-0 \
  | grep -i cluster
```

**Key counter reference:**

| Counter Name                         | What It Tells You                                     |
|--------------------------------------|-------------------------------------------------------|
| `Errors`                             | Total error count since driver start                  |
| `Client heartbeat timestamps`        | Last heartbeat from each connected client             |
| `Conductor max cycle time (ns)`      | Worst-case conductor loop latency                     |
| `Sender/Receiver flow control`       | Network-level flow control events                     |
| `Cluster node role`                  | 0=Follower, 1=Candidate, 2=Leader                    |
| `Cluster commit position`            | Highest committed log position (should match leader)  |
| `Cluster append position`            | Leader's latest append position                       |
| `Cluster election state`             | Current election phase (0=none)                       |
| `Cluster snapshot count`             | Number of snapshots taken since start                 |
| `Archive recording position`         | Current recording position in the archive             |
| `Bytes sent/received`                | Network throughput                                    |
| `NAKs sent/received`                 | Retransmission requests (high = network issues)       |

### 8.5 Gateway Cluster Client Reconnection Failures

**Symptoms:**

- FIX clients connected but receiving no execution reports
- Gateway log shows repeated `RECONNECTING` state transitions
- `ClusterClient` eventually transitions to `CLOSED`

**Debugging:**

1. Check if any cluster node is leader (section 2.2)
2. Check gateway media driver health (section 2.1)
3. Verify ingress endpoints are reachable:
   ```bash
   # Default ingress endpoints (Aeron UDP — use -u flag for UDP scan)
   # 0=localhost:20110,1=localhost:21110,2=localhost:22110
   for port in 20110 21110 22110; do
     nc -zuv localhost $port 2>&1
   done
   ```
4. Check the gateway's AeronStat for connection errors:
   ```bash
   java -cp <classpath> io.aeron.samples.AeronStat \
     aeron.dir=/tmp/aeron-gateway \
     | grep -i error
   ```

### 8.6 Startup Failure Diagnosis

The `TradingEngineLauncher` logs each startup step with timing. On failure, the
exception propagates out of `main()`, the JVM exits, and the shutdown hook fires for
cleanup.

**Common startup failures:**

| Step | Failure                          | Cause                                    | Fix                            |
|------|----------------------------------|------------------------------------------|--------------------------------|
| 4    | Media driver spawn failed        | Port conflict, permissions, JDK missing  | Check logs, fix env            |
| 5    | Driver connect timeout (15s)     | Driver crashed immediately after spawn   | Check driver stdout log        |
| 7    | Cluster node launch failed       | `cnc.dat` missing, port conflict         | Verify driver, check ports     |
| 8    | Reference data load failed       | No leader elected, cluster unhealthy     | Check cluster health           |
| 10   | Gateway connect timeout (30s)    | Cluster unreachable from gateway driver  | Check ingress endpoints        |

**Collecting diagnostic info on startup failure:**

```bash
# Check all media driver logs
for i in 0 1 2 3; do
  echo "=== Media Driver $i ==="
  tail -50 logs/media-driver-$i.stdout.log
done

# Check launcher log (Log4j2)
tail -100 logs/trading-engine.log

# Check for port conflicts
for port in 20110 20220 20330 20440 21110 21220 21330 21440 22110 22220 22330 22440 8010 8011 8012 9880 8443 8444 9090 3000 3100 9464; do
  if lsof -i :$port > /dev/null 2>&1; then
    echo "Port $port is in use: $(lsof -i :$port | tail -1)"
  fi
done
```

---

## Appendix A: System Properties Reference

| Property                           | Default          | Description                              |
|------------------------------------|------------------|------------------------------------------|
| `fix.host`                         | `localhost`      | TCP bind address for FIX connections      |
| `fix.port`                         | `9880`           | TCP port for FIX connections              |
| `cluster.nodeCount`                | `3`              | Number of cluster nodes (1-3)            |
| `cluster.baseDir`                  | `cluster-data`   | Base directory for cluster data files     |
| `log.dir`                          | `logs`           | Directory for media driver log files      |
| `driver.shutdown.timeout.seconds`  | `10`             | Seconds to wait for driver SIGTERM        |
| `accounts.file`                    | `accounts.yaml`  | Path to accounts YAML for reference data  |

## Appendix B: Quick Reference Commands

```bash
# === Health Check ===
# Verify all media drivers are alive
for d in /tmp/aeron-node-0 /tmp/aeron-node-1 /tmp/aeron-node-2 /tmp/aeron-gateway; do
  [ -f "$d/cnc.dat" ] && echo "$d: OK" || echo "$d: MISSING"
done

# Find the cluster leader
for i in 0 1 2; do
  java -cp <classpath> io.aeron.samples.AeronStat aeron.dir=/tmp/aeron-node-$i 2>/dev/null \
    | grep "Cluster node role" | sed "s/^/Node $i: /"
done

# Check cluster commit positions match
for i in 0 1 2; do
  java -cp <classpath> io.aeron.samples.AeronStat aeron.dir=/tmp/aeron-node-$i 2>/dev/null \
    | grep "commit position" | sed "s/^/Node $i: /"
done

# Check archive disk usage
du -sh cluster-data/archive-*/

# === Emergency ===
# Graceful shutdown
kill <launcher_pid>

# Force shutdown (last resort)
kill -9 <launcher_pid>
for f in logs/pids/media-driver-*.pid; do [ -s "$f" ] && kill -9 "$(cat "$f")" 2>/dev/null; done
