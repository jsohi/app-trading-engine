# APP-16: E2E Integration Test — Gradle-Driven, Real Processes, Zero Mocks

## Context

First real e2e test for the trading engine. All processes start independently like production — no embedded drivers, no in-process cluster, no JUnit lifecycle hacks. A Gradle `e2e` task boots the full stack via `TradingEngineLauncher`, loads reference data from files, then a standalone FIX test client connects and validates the order flow.

## How It Works

```
./gradlew e2e
  │
  ├─ 1. Kill stale e2e processes (pkill by aeron.dir.prefix=e2e marker)
  ├─ 2. Clean previous run artifacts (logs, cluster-data, aeron dirs)
  ├─ 2b. Check port 19880 is free (fail fast)
  ├─ 3. Build project (depends on `build` task)
  ├─ 4. Start TradingEngineLauncher (background process)
  │      └─ Spawns 4 media drivers (external processes)
  │      └─ Boots 3-node Aeron cluster
  │      └─ Loads ref-data from YAML files (currencies → accounts → risk limits)
  │      └─ Starts pricing + orchestrator + gateway
  │      └─ Logs SYSTEM_READY
  ├─ 5. Wait for SYSTEM_READY in logs (poll with timeout)
  ├─ 6. Run E2EFixTestClient (connects to gateway:19880, sends NOS, validates ExecutionReport)
  ├─ 7. Verify launcher still alive (detect mid-test crash)
  └─ 8. Kill launcher (SIGTERM → graceful shutdown) + report result
```

## Where Logs Go

| Component | Log File | Mechanism |
|-----------|----------|-----------|
| Launcher (main) | `e2e/logs/launcher.log` | stdout/stderr redirect |
| Media driver 0-2 | `e2e/logs/media-driver-{0,1,2}.stdout.log` | TradingEngineLauncher redirects |
| Media driver gw | `e2e/logs/media-driver-3.stdout.log` | TradingEngineLauncher redirects |
| Cluster nodes | `e2e/logs/launcher.log` (Log4j2) | Same JVM as launcher |
| Gateway (Artio) | `e2e/logs/fix-logs/` | Artio FIX message logs |
| E2E test client | `e2e/logs/e2e-client.log` | stdout/stderr redirect |
| Aeron CnC files | `/tmp/aeron-e2e-node-{0,1,2}/`, `/tmp/aeron-e2e-gateway/` | Memory-mapped; `-Daeron.dir.prefix=e2e` isolates from dev cluster |

On failure: script tails the last 50 lines of each log to stderr for immediate diagnosis.

## Test Data — Loaded From Files, Not Hardcoded

All reference data lives in `integration-tests/e2e/data/` and is loaded by the real `ReferenceDataOrchestrator` — same path as production.

**`currencies.yaml`:**
```yaml
currencies:
  - ccyCode: "USD"
    isoNumeric: 840
    name: "US Dollar"
    decimals: 2
    currencyClass: "Fiat"
    status: "Active"
  - ccyCode: "EUR"
    isoNumeric: 978
    name: "Euro"
    decimals: 2
    currencyClass: "Fiat"
    status: "Active"
```

**`accounts.yaml`:**
```yaml
accounts:
  - accountId: 1
    parentAccountId: 0
    accountCode: "ACME"
    acctIdSource: "Internal"
    accountName: "Acme Trading"
    accountType: "Client"
    baseCurrency: "USD"
    status: "Active"
    complianceStatus: "OK"
    capabilities: 3
  - accountId: 2
    parentAccountId: 0
    accountCode: "LOCKED"
    acctIdSource: "Internal"
    accountName: "Locked Account"
    accountType: "Client"
    baseCurrency: "USD"
    status: "Suspended"
    complianceStatus: "OK"
    capabilities: 1
```

**`risk-limits.yaml`:**
```yaml
riskLimits:
  - accountId: 1
    maxOrderSize: 1000000000     # 10.0 in fixed-point (10 * 10^8)
    maxOrderNotional: 0          # unlimited
    maxDailyVolume: 0            # unlimited
    maxDailyLossBps: 50
    status: "Active"
```

Loading order enforced by `TradingEngineLauncher.loadReferenceData()`: **currencies → accounts → risk limits** (FK chain).

## Files to Create

```
# Reference data loaders (production code — resolves TODO APP-204)
reference-data/src/main/java/com/trading/refdata/currency/
  CurrencyRecord.java                    — immutable DTO
  YamlCurrencyLoader.java                — ReferenceDataLoader<CurrencyRecord>
  CurrencyCommandEncoder.java            — ReferenceDataEncoder<CurrencyRecord>

reference-data/src/main/java/com/trading/refdata/risklimit/
  RiskLimitRecord.java                   — immutable DTO
  YamlRiskLimitLoader.java               — ReferenceDataLoader<RiskLimitRecord>
  RiskLimitCommandEncoder.java           — ReferenceDataEncoder<RiskLimitRecord>

# E2E test client (standalone main class — integration-tests/src/main/java/ must be created)
integration-tests/src/main/java/com/trading/engine/e2e/
  E2EFixTestClient.java                  — Artio initiator, sends NOS, validates response

# E2E test data (loaded like production)
integration-tests/e2e/data/
  currencies.yaml
  accounts.yaml
  risk-limits.yaml

# E2E orchestration script (called by Gradle e2e task)
scripts/e2e.sh                           — start, wait, test, teardown
```

## Files to Modify

- `launcher/.../TradingEngineLauncher.java` — wire currency + risk-limit loading (resolve TODO APP-204) + parameterize aeron dir prefix + add `LogManager.shutdown()` to shutdown hook
- `launcher/.../LauncherConfig.java` — add `currencies.file`, `risk-limits.file`, `aeron.dir.prefix` system properties (3 new record fields + validation)
- `launcher/.../RefDataEgressBridge.java` — make `public` (already handles all 6 response types)
- `integration-tests/build.gradle.kts` — add `application` plugin + dependencies for E2EFixTestClient
- `build.gradle.kts` (root) — add `e2e` and `e2eClean` Gradle tasks
- `.gitignore` — add `e2e/` and `.gradle-e2e-client/` to prevent test artifacts from being committed

## Implementation

### Step 1: Currency Loader (reference-data module)

Follow the exact pattern of `YamlAccountLoader` + `AccountCommandEncoder`. Uses SnakeYAML (already a dependency in reference-data module).

**CurrencyRecord.java** — immutable record:
```java
public record CurrencyRecord(
    String ccyCode, int isoNumeric, String name,
    int decimals, String currencyClass, String status) {}
```

**YamlCurrencyLoader.java** — implements `ReferenceDataLoader<CurrencyRecord>`:
- Parse YAML with SnakeYAML (`currencies:` array)
- Validate: ccyCode is 3 uppercase ASCII, isoNumeric in [1,999], decimals in [0,18]
- Enforce unique ccyCodes
- `sourceName()` returns the file path

**CurrencyCommandEncoder.java** — implements `ReferenceDataEncoder<CurrencyRecord>`:
- Encodes into `LoadCurrencyBatchEncoder` SBE message (templateId=14)
- Maps string enums → `CurrencyClassEnum`, `AccountStatusEnum`
- `maxBatchSize()` = 200 (same as accounts)
- `entityType()` = "Currency"

### Step 2: Risk Limit Loader (reference-data module)

Same pattern:

**RiskLimitRecord.java:**
```java
public record RiskLimitRecord(
    long accountId, long maxOrderSize, long maxOrderNotional,
    long maxDailyVolume, long maxDailyLossBps, String status) {}
```

**YamlRiskLimitLoader.java** — `ReferenceDataLoader<RiskLimitRecord>`:
- Parse YAML (`riskLimits:` array)
- Validate: accountId > 0, all limits >= 0, maxDailyLossBps in uint32 range

**RiskLimitCommandEncoder.java** — `ReferenceDataEncoder<RiskLimitRecord>`:
- Encodes into `LoadRiskLimitBatchEncoder` SBE message (templateId=16)
- `maxBatchSize()` = 200

### Step 3: Wire loaders into TradingEngineLauncher

**LauncherConfig.java** — add three new record fields (expand from 7 to 10):
```java
// New fields in the record:
String currenciesFile              // default: "currencies.yaml"
String riskLimitsFile              // default: "risk-limits.yaml"
String aeronDirPrefix              // default: "" (empty — uses /tmp/aeron-node-{i}, /tmp/aeron-gateway)

// fromSystemProperties() additions:
System.getProperty("currencies.file", "currencies.yaml"),
System.getProperty("risk-limits.file", "risk-limits.yaml"),
System.getProperty("aeron.dir.prefix", "")

// Compact constructor: validate non-null/non-blank for file paths (same as accountsFile)
// aeronDirPrefix allowed to be empty (means no prefix = production default)
```

**TradingEngineLauncher aeron dir construction** — use prefix when set:
```java
// If prefix is "e2e": /tmp/aeron-e2e-node-0, /tmp/aeron-e2e-gateway
// If prefix is "": /tmp/aeron-node-0, /tmp/aeron-gateway (production default)
String dirInfix = config.aeronDirPrefix().isEmpty() ? "" : config.aeronDirPrefix() + "-";
aeronDirs[i] = "/tmp/aeron-" + dirInfix + "node-" + i;
aeronDirs[gwIndex] = "/tmp/aeron-" + dirInfix + "gateway";
```

**TradingEngineLauncher.loadReferenceData()** — replace the TODO with real loading:
```java
// Load currencies FIRST (no FK dependencies)
orchestrator.load(
    new YamlCurrencyLoader(Path.of(currenciesFile)),
    new CurrencyCommandEncoder(),
    sender, pollEgress, collector);

// Load accounts SECOND (FK: baseCurrency must exist)
orchestrator.load(
    new YamlAccountLoader(Path.of(accountsFile)),
    new AccountCommandEncoder(),
    sender, pollEgress, collector);

// Load risk limits THIRD (FK: accountId must exist)
orchestrator.load(
    new YamlRiskLimitLoader(Path.of(riskLimitsFile)),
    new RiskLimitCommandEncoder(),
    sender, pollEgress, collector);
```

**TradingEngineLauncher shutdown hook** — add Log4j2 flush:
```java
// In the shutdown hook, BEFORE closing resources:
LogManager.shutdown();  // flush async logger ring buffer so last error lines aren't lost
```

### Step 4: Make `RefDataEgressBridge` public

One-word change: `final class` → `public final class`. It already handles all 6 response types (currencies, accounts, risk limits — loaded/rejected).

### Step 5: Create test data files

Create `integration-tests/e2e/data/` directory with the 3 YAML files shown in the "Test Data" section above:
- `currencies.yaml` — USD + EUR
- `accounts.yaml` — ACME (active, canTrade) + LOCKED (suspended)
- `risk-limits.yaml` — permissive limit for account 1

These are checked into git — they're the e2e test's "environment."

### Step 6: Create E2EFixTestClient (standalone main class)

**Module:** `integration-tests` (add `application` plugin)

**What it does:**
1. Parse CLI args via manual loop (no external library — keeps dependencies minimal):
   ```java
   // Simple --key value parsing, no library needed
   String host = "localhost", senderCompId = "CLIENT1", targetCompId = "TRADING";
   int port = 19880;
   for (int i = 0; i < args.length - 1; i += 2) {
       switch (args[i]) {
           case "--host" -> host = args[i + 1];
           case "--port" -> port = Integer.parseInt(args[i + 1]);
           case "--sender-comp-id" -> senderCompId = args[i + 1];
           case "--target-comp-id" -> targetCompId = args[i + 1];
       }
   }
   ```
2. Launch embedded MediaDriver (SHARED, `Files.createTempDirectory("e2e-fix-client")` — auto-cleaned on JVM exit via `dirDeleteOnShutdown(true)`)
3. Create Artio FixEngine (initiator mode — no `bindTo()`, `logFileDir` set to `e2e/logs/fix-client-logs/`)
4. Connect FixLibrary + initiate session to gateway
5. Poll until session reaches ACTIVE (check `Reply.hasErrored()`)
6. Send FIX NewOrderSingle:
   - ClOrdID = "E2E-" + timestamp
   - Symbol = "EURUSD"
   - Side = Buy ('1')
   - OrdType = Limit ('2')
   - Price = 1.05 (DecimalFloat(105, 2))
   - OrderQty = 1.0 (DecimalFloat(1, 0))
   - Account = "ACME"
   - Currency = "USD"
   - TimeInForce = Day ('0')
7. Poll for ExecutionReport response (30s timeout)
8. Validate: ExecType='0', OrdStatus='0', ClOrdID echoed, OrderID non-empty
9. Print PASS/FAIL to stdout
10. Exit 0 on success, 1 on failure
11. Clean shutdown: library → engine → driver

**Key Artio details (verified against codebase):**
- EngineConfiguration: no `.bindTo()`, `.libraryAeronChannel("aeron:ipc")`, `.logFileDir(tempLogDir)`
- SessionConfiguration: `.address(host)`, `.port(port)`, `.senderCompId("CLIENT1")`, `.targetCompId("TRADING")`
- NOS encoding: `encoder.instrument().symbol(sym)`, `encoder.orderQtyData().orderQty(qty)`
- Response decoding: `MutableAsciiBuffer` wrapper, `decoder.execType()` returns char directly
- CompIDs: CLIENT1 is in gateway's allowlist `Set.of("CLIENT1", "CLIENT2", "FIX_BRIDGE")`

### Step 7: Create `scripts/e2e.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

E2E_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$E2E_DIR/e2e/logs"
DATA_DIR="$E2E_DIR/integration-tests/e2e/data"
LAUNCHER_PID=""

cleanup() {
    if [[ -n "$LAUNCHER_PID" ]] && kill -0 "$LAUNCHER_PID" 2>/dev/null; then
        echo "Stopping trading engine (PID $LAUNCHER_PID)..."
        kill -TERM "$LAUNCHER_PID"
        wait "$LAUNCHER_PID" 2>/dev/null || true
    fi
    # Collect logs on failure
    if [[ "${E2E_RESULT:-1}" -ne 0 ]]; then
        echo "=== LAST 50 LINES OF LAUNCHER LOG ==="
        tail -50 "$LOG_DIR/launcher.log" 2>/dev/null || true
        echo "=== LAST 50 LINES OF E2E CLIENT LOG ==="
        tail -50 "$LOG_DIR/e2e-client.log" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# 1. Kill stale processes from a previous crashed/interrupted run
# Match by the unique aeron.dir.prefix=e2e system property — only kills e2e processes,
# never a dev cluster (which uses default prefix).
if pgrep -f "aeron.dir.prefix=e2e" > /dev/null 2>&1; then
    echo "Killing stale e2e processes..."
    pkill -TERM -f "aeron.dir.prefix=e2e" 2>/dev/null || true
    sleep 2
    pkill -9 -f "aeron.dir.prefix=e2e" 2>/dev/null || true  # force-kill stragglers
fi
# Also kill stale media drivers by aeron dir path
if pgrep -f "aeron-e2e-" > /dev/null 2>&1; then
    pkill -TERM -f "aeron-e2e-" 2>/dev/null || true
    sleep 1
    pkill -9 -f "aeron-e2e-" 2>/dev/null || true
fi

# 2. Clean previous run artifacts
rm -rf "$LOG_DIR" /tmp/aeron-e2e-* e2e/cluster-data "$E2E_DIR/.gradle-e2e-client"
mkdir -p "$LOG_DIR"

# 2b. Check port 19880 is free — fail fast instead of waiting 60s for SYSTEM_READY timeout
if lsof -i :19880 -sTCP:LISTEN > /dev/null 2>&1; then
    echo "FAIL: Port 19880 already in use. Kill the process or use a different port."
    lsof -i :19880 -sTCP:LISTEN
    E2E_RESULT=1
    exit 1
fi

# 3. Start trading engine
echo "Starting trading engine..."
# --no-daemon ensures Gradle runs the JVM directly (no daemon fork).
# This way SIGTERM to $LAUNCHER_PID reaches the actual launcher JVM
# and triggers the ShutdownSignalBarrier → orderly cleanup.
./gradlew :launcher:run --no-daemon \
    -Dfix.host=localhost \
    -Dfix.port=19880 \
    -Dcluster.nodeCount=3 \
    -Dcluster.baseDir=e2e/cluster-data \
    -Dlog.dir=e2e/logs \
    -Daeron.dir.prefix=e2e \
    -Daccounts.file="$DATA_DIR/accounts.yaml" \
    -Dcurrencies.file="$DATA_DIR/currencies.yaml" \
    -Drisk-limits.file="$DATA_DIR/risk-limits.yaml" \
    > "$LOG_DIR/launcher.log" 2>&1 &
LAUNCHER_PID=$!

# 4. Wait for SYSTEM_READY (max 60s)
echo "Waiting for SYSTEM_READY..."
DEADLINE=$((SECONDS + 60))
while ! grep -q "SYSTEM_READY" "$LOG_DIR/launcher.log" 2>/dev/null; do
    if [[ $SECONDS -ge $DEADLINE ]]; then
        echo "FAIL: Trading engine did not reach SYSTEM_READY within 60s"
        E2E_RESULT=1
        exit 1
    fi
    if ! kill -0 "$LAUNCHER_PID" 2>/dev/null; then
        echo "FAIL: Trading engine process died during startup"
        E2E_RESULT=1
        exit 1
    fi
    sleep 1
done
echo "Trading engine ready."

# 5. Run E2E test client (--no-daemon for clean classpath)
# Use separate GRADLE_USER_HOME to avoid .gradle lock contention with the
# background launcher:run process. Both are --no-daemon but share the same
# project directory — Gradle's file hash cache and configuration cache use
# file locking that causes "Timeout waiting to lock" failures in CI.
echo "Running E2E FIX test client..."
GRADLE_USER_HOME="$E2E_DIR/.gradle-e2e-client" \
    ./gradlew :integration-tests:run --no-daemon \
    --args="--host localhost --port 19880" \
    > "$LOG_DIR/e2e-client.log" 2>&1
E2E_RESULT=$?

# 6. Verify launcher didn't crash during the test
if ! kill -0 "$LAUNCHER_PID" 2>/dev/null; then
    echo "FAIL: Trading engine crashed during test run"
    E2E_RESULT=1
fi

# 7. Report
if [[ $E2E_RESULT -eq 0 ]]; then
    echo "E2E PASSED"
else
    echo "E2E FAILED (exit code $E2E_RESULT)"
fi
exit $E2E_RESULT
```

### Step 8: Add Gradle `e2e` task (root build.gradle.kts)

```kotlin
// root build.gradle.kts — add at the end

tasks.register<Exec>("e2e") {
    group = "verification"
    description = "Run full e2e integration test — boots real 3-node cluster, sends FIX NOS, validates ExecutionReport"
    dependsOn("build")

    commandLine("bash", "scripts/e2e.sh")

    // 3-minute hard timeout prevents hanging on deadlocked cluster
    timeout.set(Duration.ofMinutes(3))
}

tasks.register<Delete>("e2eClean") {
    group = "verification"
    description = "Remove e2e test artifacts (logs, cluster data, aeron dirs)"
    delete("e2e/logs", "e2e/cluster-data", ".gradle-e2e-client")
    doLast {
        // Use providers.exec {} (Gradle 9.4+ non-deprecated API) for process cleanup
        providers.exec { commandLine("bash", "-c", "rm -rf /tmp/aeron-e2e-*") }
        providers.exec { commandLine("bash", "-c", "pkill -9 -f 'aeron.dir.prefix=e2e' 2>/dev/null || true") }
        providers.exec { commandLine("bash", "-c", "pkill -9 -f 'aeron-e2e-' 2>/dev/null || true") }
    }
}
```

Usage:
```bash
./gradlew e2e           # run full e2e test (builds first via dependsOn)
./gradlew e2eClean      # clean e2e artifacts + kill stale processes
```

### Step 9: Update `integration-tests/build.gradle.kts`

```kotlin
plugins {
    application   // for E2EFixTestClient main class
}

application {
    mainClass.set("com.trading.engine.e2e.E2EFixTestClient")
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    )
}

dependencies {
    // existing
    testImplementation(project(":launcher"))
    testImplementation(project(":test-support"))
    testImplementation(libs.artio.core)
    testImplementation(libs.aeron.test.support)

    // new — for E2EFixTestClient (main, not test)
    implementation(project(":fix-codecs"))
    implementation(libs.artio.core)
    implementation(libs.aeron.driver)
    implementation(libs.agrona)
}
```

## Message Flow (fully real, production path)

```
./gradlew e2e
  └─ scripts/e2e.sh
      └─ ./gradlew :launcher:run                ← real TradingEngineLauncher.main()
          ├─ Spawns 4 media driver PROCESSES     ← real MediaDriverLauncher.main()
          ├─ Boots 3-node Aeron cluster          ← real ClusterNodeLauncher.launch()
          ├─ Loads currencies.yaml → cluster     ← real YamlCurrencyLoader + ReferenceDataOrchestrator
          ├─ Loads accounts.yaml → cluster       ← real YamlAccountLoader + ReferenceDataOrchestrator
          ├─ Loads risk-limits.yaml → cluster    ← real YamlRiskLimitLoader + ReferenceDataOrchestrator
          ├─ Starts pricing service              ← real PricingServiceLauncher
          ├─ Starts orchestrator                 ← real OrchestratorLauncher
          └─ Starts FIX gateway on :19880        ← real GatewayLauncher (Artio acceptor)

      └─ ./gradlew :integration-tests:run        ← E2EFixTestClient.main()
          ├─ Launches own MediaDriver (embedded)
          ├─ Creates Artio FixEngine (initiator)
          ├─ Connects to gateway:19880 (TCP)
          ├─ FIX Logon (CLIENT1 → TRADING)
          ├─ Sends NewOrderSingle (35=D)
          │    → gateway translates FIX→SBE
          │    → cluster validates (11 checks) → OrderCreatedEvent
          │    → gateway translates SBE→FIX ExecutionReport (35=8)
          ├─ Receives ExecutionReport
          ├─ Validates: ExecType=New, OrdStatus=New, ClOrdID echoed, OrderID present
          └─ Exit 0 (PASS) or Exit 1 (FAIL)
```

## Key Design Decisions

1. **Real processes** — TradingEngineLauncher spawns external media driver processes, exactly like production
2. **Real data files** — YAML loaded by real `ReferenceDataOrchestrator` with real loaders, FK validation
3. **Standalone test client** — separate JVM with own MediaDriver, connects over TCP like a real counterparty
4. **Gradle task orchestration** — `./gradlew e2e` is one command; CI-friendly; `dependsOn("build")` ensures fresh build; 3-minute `timeout` prevents hangs
5. **FIX port 19880** — distinct from production 9880, avoids conflicts
6. **Graceful shutdown** — SIGTERM triggers `ShutdownSignalBarrier` in launcher → orderly close
7. **Log collection on failure** — script tails launcher + client logs for diagnosis
8. **No JUnit in the hot path** — test client is a standalone main(), not JUnit-managed

## Pitfalls & Mitigations

| Pitfall | Mitigation |
|---------|-----------|
| Stale processes from crashed previous run | pkill by `aeron.dir.prefix=e2e` marker + `aeron-e2e-` dir pattern. SIGTERM → 2s wait → SIGKILL stragglers. |
| Port 19880 still bound from previous crash | Script checks `lsof -i :19880` before starting. Fails fast with clear message. |
| Leader election latency (5-15s) | `AeronCluster.connect()` during ref-data loading inherently waits. 60s SYSTEM_READY timeout. |
| Launcher crashes mid-test, test still passes | Script checks `kill -0 $LAUNCHER_PID` after test client exits. If launcher died, force FAIL. |
| FIX session not reaching ACTIVE | Test client polls `session.state()` + `Reply.hasErrored()`. 30s timeout. |
| Log4j2 async logger not flushing on SIGTERM | Add `LogManager.shutdown()` in TradingEngineLauncher shutdown hook BEFORE closing resources. |
| Aeron dir collision with dev cluster | `-Daeron.dir.prefix=e2e` → `/tmp/aeron-e2e-node-{i}` isolated from `/tmp/aeron-node-{i}`. |
| Artio fix-logs hardcoded to CWD | Known limitation (TODO APP-205). Acceptable for e2e. |
| Transient Artio response buffer | `onMessage()` defensively copies buffer to `byte[]` before queuing. |
| Script hangs forever | Gradle `timeout.set(Duration.ofMinutes(3))` hard ceiling on the `e2e` task. |

## Verification

1. `./gradlew e2e` — passes end-to-end
2. All processes start independently (media drivers as separate PIDs)
3. Reference data loaded from YAML files, not hardcoded
4. FIX NOS traverses full pipeline: TCP → Artio → SBE → Raft → validation → ExecutionReport
5. Logs collected in `e2e/logs/` — inspectable post-run
6. Clean shutdown — no orphaned processes, no stale Aeron directories
7. Repeatable — run `./gradlew e2e` 3 times consecutively, all pass (idempotent)
8. Completes within 3 minutes (Gradle task timeout)
