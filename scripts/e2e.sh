#!/usr/bin/env bash
# =============================================================================
# E2E Integration Test — boots real 3-node cluster, sends FIX NOS, validates ER
#
# Usage: ./scripts/e2e.sh (or via ./gradlew e2e)
#
# Known limitation: cluster UDP ports (20110-22440, 8010-8012) are hardcoded in
# ClusterConfig. Cannot run e2e while a dev cluster is running on the same host.
# =============================================================================
set -euo pipefail

E2E_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$E2E_DIR/e2e/logs"
DATA_DIR="$E2E_DIR/integration-tests/e2e/data"
LAUNCHER_PID=""
E2E_RESULT=1
E2E_STARTUP_TIMEOUT="${E2E_STARTUP_TIMEOUT:-90}"

# --- Pre-flight checks ---
for cmd in lsof pkill pgrep; do
    command -v "$cmd" >/dev/null 2>&1 || { echo "FAIL: $cmd not found"; exit 1; }
done

# --- Cleanup trap ---
cleanup() {
    if [[ -n "$LAUNCHER_PID" ]] && kill -0 "$LAUNCHER_PID" 2>/dev/null; then
        echo "Stopping trading engine (PID $LAUNCHER_PID)..."
        kill -TERM "$LAUNCHER_PID" 2>/dev/null || true
        # Wait up to 10s for graceful shutdown, then force-kill
        for _ in $(seq 1 10); do
            kill -0 "$LAUNCHER_PID" 2>/dev/null || break
            sleep 1
        done
        kill -0 "$LAUNCHER_PID" 2>/dev/null && kill -9 "$LAUNCHER_PID" 2>/dev/null || true
    fi
    # Belt-and-suspenders: kill any orphaned e2e processes (media drivers, etc.)
    pkill -f -- "-Daeron.dir.prefix=e2e" 2>/dev/null || true
    sleep 2
    pkill -9 -f -- "-Daeron.dir.prefix=e2e" 2>/dev/null || true

    # Collect logs on failure
    if [[ "${E2E_RESULT}" -ne 0 ]]; then
        echo ""
        echo "=== ERRORS IN LAUNCHER LOG ==="
        grep -i "error\|exception\|fatal" "$LOG_DIR/launcher.log" 2>/dev/null | tail -30 || true
        echo ""
        echo "=== LAST 100 LINES OF LAUNCHER LOG ==="
        tail -100 "$LOG_DIR/launcher.log" 2>/dev/null || true
        echo ""
        echo "=== LAST 100 LINES OF E2E CLIENT LOG ==="
        tail -100 "$LOG_DIR/e2e-client.log" 2>/dev/null || true
        echo ""
        echo "=== MEDIA DRIVER ERRORS ==="
        for f in "$LOG_DIR"/media-driver-*.stdout.log; do
            [ -f "$f" ] && grep -il "error\|exception" "$f" 2>/dev/null && tail -20 "$f" || true
        done
    fi
}
trap cleanup EXIT

# --- 1. Kill stale e2e processes from a previous crashed/interrupted run ---
if pgrep -f -- "-Daeron.dir.prefix=e2e" > /dev/null 2>&1; then
    echo "Killing stale e2e processes..."
    pkill -TERM -f -- "-Daeron.dir.prefix=e2e" 2>/dev/null || true
    # Wait for processes to die before removing dirs
    for _ in $(seq 1 5); do
        pgrep -f -- "-Daeron.dir.prefix=e2e" > /dev/null 2>&1 || break
        sleep 1
    done
    pkill -9 -f -- "-Daeron.dir.prefix=e2e" 2>/dev/null || true
    sleep 1
fi
# Also kill stale media drivers by aeron dir path
if pgrep -f "aeron-e2e-" > /dev/null 2>&1; then
    pkill -TERM -f "aeron-e2e-" 2>/dev/null || true
    sleep 1
    pkill -9 -f "aeron-e2e-" 2>/dev/null || true
fi

# --- 2. Clean previous run artifacts (AFTER processes are dead) ---
rm -rf "$LOG_DIR" /tmp/aeron-e2e-* "$E2E_DIR/e2e/cluster-data"
mkdir -p "$LOG_DIR"

# --- 2b. Check port 19880 is free ---
if lsof -i :19880 -sTCP:LISTEN > /dev/null 2>&1; then
    echo "FAIL: Port 19880 already in use. Kill the process or use a different port."
    lsof -i :19880 -sTCP:LISTEN
    exit 1
fi

# --- 3. Start trading engine ---
echo "Starting trading engine..."
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

# --- 4. Wait for SYSTEM_READY (configurable timeout, default 90s) ---
echo "Waiting for SYSTEM_READY (timeout: ${E2E_STARTUP_TIMEOUT}s)..."
DEADLINE=$((SECONDS + E2E_STARTUP_TIMEOUT))
while ! grep -q "SYSTEM_READY" "$LOG_DIR/launcher.log" 2>/dev/null; do
    if [[ $SECONDS -ge $DEADLINE ]]; then
        echo "FAIL: Trading engine did not reach SYSTEM_READY within ${E2E_STARTUP_TIMEOUT}s"
        exit 1
    fi
    if ! kill -0 "$LAUNCHER_PID" 2>/dev/null; then
        echo "FAIL: Trading engine process died during startup"
        exit 1
    fi
    # Fail fast on startup errors — match "Startup failed" (Log4j2 ERROR level) specifically,
    # not "fatal" generically (which false-matches GFLog config warnings containing the word
    # FATAL in enumeration lists like '[TRACE, DEBUG, INFO, WARN, ERROR, FATAL]').
    if grep -q "Startup failed" "$LOG_DIR/launcher.log" 2>/dev/null; then
        echo "FAIL: Startup error detected"
        exit 1
    fi
    sleep 1
done
echo "Trading engine ready."

# --- 5. Run E2E test client via installDist (no second Gradle process) ---
echo "Running E2E FIX test client..."
# Temporarily disable set -e so a non-zero client exit does not skip the
# launcher liveness check and final report below.
set +e
"$E2E_DIR/integration-tests/build/install/integration-tests/bin/integration-tests" \
    --host localhost --port 19880 \
    --data-dir "$DATA_DIR" \
    > "$LOG_DIR/e2e-client.log" 2>&1
E2E_RESULT=$?
set -e

# --- 6. Verify launcher didn't crash during the test ---
if ! kill -0 "$LAUNCHER_PID" 2>/dev/null; then
    echo "FAIL: Trading engine crashed during test run"
    E2E_RESULT=1
fi

# --- 7. Report ---
if [[ $E2E_RESULT -eq 0 ]]; then
    echo "E2E PASSED"
else
    echo "E2E FAILED (exit code $E2E_RESULT)"
fi
exit $E2E_RESULT
