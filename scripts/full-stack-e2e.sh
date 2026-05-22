#!/usr/bin/env bash
# =============================================================================
# Full-Stack E2E — release-rehearsal harness (APP-225) and broader regression.
#
# DEFAULT invocation:
#   bash scripts/full-stack-e2e.sh
#     boots cluster + websocket-server (TLS+JWT) + dev JWKS, runs the curated
#     release-rehearsal narrative (one ordered trader-day Playwright spec), and
#     on failure assembles a triage bundle (logs + Playwright trace + Aeron
#     archive + env fingerprint + rendered YAMLs) at
#     e2e/logs/release-rehearsal-failure-<UTC-ts>.tar.gz. Single PASS/FAIL exit
#     code. This is the path CI runs as a release gate.
#
# --keep-running:
#   bash scripts/full-stack-e2e.sh --keep-running
#     boots the cluster AND Vite dev server, then waits. Open
#     https://localhost:5173 to play. VITE_DEV_JWT is bound to the freshly-
#     minted issuer-A token, so the UI authenticates without manual paste.
#
# --full-sweep:
#   TRADING_E2E_FULL_SWEEP=1 bash scripts/full-stack-e2e.sh
#     in addition to the release-rehearsal narrative, runs the broader spec
#     01-07 regression, JCStress, the @Tag("stress") JUnit phase, the multi-
#     issuer launcher reboot for spec 08, and aggregates all exit codes.
#
# Plan §5 + §10 + §15. See docs/full-stack-e2e.md for the operator runbook.
# =============================================================================
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$REPO_ROOT/e2e/logs"
CONFIG_DIR="$REPO_ROOT/e2e/config"
KEEP_RUNNING=0

# ----- CLI -----
while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-running) KEEP_RUNNING=1; shift ;;
    --help|-h) sed -n '1,15p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 1 ;;
  esac
done

# ----- Shared helpers (PR1 — see scripts/lib/*.sh) -----
# shellcheck source=lib/wait-system-ready.sh
source "$REPO_ROOT/scripts/lib/wait-system-ready.sh"
# shellcheck source=lib/log-capture.sh
source "$REPO_ROOT/scripts/lib/log-capture.sh"
# shellcheck source=lib/mkcert-spki.sh
source "$REPO_ROOT/scripts/lib/mkcert-spki.sh"
# shellcheck source=lib/collect-failure-bundle.sh
source "$REPO_ROOT/scripts/lib/collect-failure-bundle.sh"

# ----- PIDs (populated as we go) -----
LAUNCHER_PID=""
JWKS_PID_A=""
JWKS_PID_B=""
JCSTRESS_PID=""
STRESS_TEST_PID=""
VITE_PID=""
EXIT_CODE=1
CLEANED=0

cleanup() {
  if [[ "$CLEANED" -ne 0 ]]; then return; fi
  CLEANED=1
  echo "" >&2
  echo "[full-stack-e2e] cleanup: tearing down..." >&2

  for pid in "$STRESS_TEST_PID" "$JCSTRESS_PID" "$VITE_PID" "$LAUNCHER_PID" "$JWKS_PID_A" "$JWKS_PID_B"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done
  # Bounded grace for graceful Aeron archive flush, then SIGKILL stragglers.
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    local any=0
    for pid in "$STRESS_TEST_PID" "$JCSTRESS_PID" "$VITE_PID" "$LAUNCHER_PID" "$JWKS_PID_A" "$JWKS_PID_B"; do
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then any=1; fi
    done
    [[ "$any" -eq 0 ]] && break
    sleep 1
  done
  for pid in "$STRESS_TEST_PID" "$JCSTRESS_PID" "$VITE_PID" "$LAUNCHER_PID" "$JWKS_PID_A" "$JWKS_PID_B"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done

  # Belt-and-suspenders: kill any orphaned aeron media drivers from this run.
  pkill -KILL -f -- "-Daeron.dir.prefix=e2e" 2>/dev/null || true

  if [[ "$EXIT_CODE" -ne 0 ]]; then
    dump_logs_on_failure "$LOG_DIR"
    # APP-225: triage-friendly bundle on red — single tar.gz with everything
    # an engineer needs to reproduce / debug offline.
    # Stdout silenced (the bundle path also goes to stderr inside the helper, so the operator
    # still sees the "[bundle] failure bundle: ..." line). Stderr deliberately UNREDIRECTED so
    # `tar` errors like "No space left on device" reach the operator — silencing them would
    # defeat the explicit error-surface in collect-failure-bundle.sh.
    collect_failure_bundle "$REPO_ROOT" >/dev/null || true
  fi

  # Best-effort scrub of on-disk JWTs. Files are gitignored under e2e/logs/
  # but no token must outlive the suite.
  rm -f "$LOG_DIR/jwt-A.txt" "$LOG_DIR/jwt-B.txt" 2>/dev/null || true
}
trap cleanup EXIT INT TERM HUP

# ----- 1. Cleanup previous run -----
#
# The launcher's `-Dcluster.baseDir=...` is consumed verbatim by `./gradlew
# :launcher:run`, whose CWD is the `:launcher` subproject. A relative path
# therefore resolves under `launcher/`, NOT under `$REPO_ROOT`. We pin both
# launcher CWD-relative and REPO_ROOT-relative locations so cleanup catches
# whatever the launcher actually wrote, regardless of how the working-directory
# semantics evolve.
rm -rf "$LOG_DIR" \
       "$REPO_ROOT/e2e/cluster-data" "$REPO_ROOT/e2e/cluster-data-mi" \
       "$REPO_ROOT/launcher/e2e/cluster-data" "$REPO_ROOT/launcher/e2e/cluster-data-mi"
mkdir -p "$LOG_DIR" "$CONFIG_DIR"

# ----- 2. Pre-flight tooling -----
command -v lsof >/dev/null 2>&1 || { echo "FATAL: install lsof (apt install lsof / brew install lsof)" >&2; exit 2; }
command -v mkcert >/dev/null 2>&1 || { echo "FATAL: install mkcert (brew install mkcert nss / apt install mkcert libnss3-tools)" >&2; exit 2; }
command -v node >/dev/null 2>&1 || { echo "FATAL: install node 22+ (matches engines.node in package.json)" >&2; exit 2; }

# ----- 2b. E2E management endpoint port (Option A — spec 09 feed-stale) -----
# The launcher exposes a JDK HttpServer on 127.0.0.1:$TRADING_E2E_MGMT_PORT when
# TRADING_E2E_MGMT_ENABLED=1, used by Playwright spec 09 to pause/resume the
# pricing-service AgentRunner without killing the launcher JVM. Production
# deployments never set these env vars — see launcher/E2eManagementServer.java.
export TRADING_E2E_MGMT_ENABLED=1
export TRADING_E2E_MGMT_PORT="${TRADING_E2E_MGMT_PORT:-9876}"

# ----- 3. Pre-flight ports -----
PORTS_TO_CHECK=(5173 7100 7101 8443 19880 20110 21110 22110 20220 21220 22220 8010 8011 8012 "$TRADING_E2E_MGMT_PORT")
PORT_FAIL=0
for p in "${PORTS_TO_CHECK[@]}"; do
  if lsof -i ":$p" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "FATAL: port $p is already bound — kill the process or pick a different port" >&2
    lsof -i ":$p" -sTCP:LISTEN >&2 || true
    PORT_FAIL=1
  fi
done
[[ "$PORT_FAIL" -eq 0 ]] || exit 2

# ----- 4. mkcert TLS PEMs -----
bash "$REPO_ROOT/scripts/dev-cert.sh"

# ----- 5. Force CA materialization without trust-store install (atomic, concurrent-CI safe) -----
TMP_CERT=$(mktemp -t mkcert-discard-cert.XXXXXX)
TMP_KEY=$(mktemp -t mkcert-discard-key.XXXXXX)
mkcert -cert-file "$TMP_CERT" -key-file "$TMP_KEY" localhost >/dev/null 2>&1 || true
rm -f "$TMP_CERT" "$TMP_KEY"

# ----- 6. Generate two RS256 keypairs + JWKS docs (A and B) -----
bash "$REPO_ROOT/scripts/dev-key-gen.sh" --no-yaml --prefix A
bash "$REPO_ROOT/scripts/dev-key-gen.sh" --no-yaml --prefix B

# ----- 7. Start two dev JWKS servers in background -----
node "$REPO_ROOT/scripts/dev-jwks-server.mjs" --port 7100 --keyset A \
  >"$LOG_DIR/jwks-A.log" 2>&1 &
JWKS_PID_A=$!
node "$REPO_ROOT/scripts/dev-jwks-server.mjs" --port 7101 --keyset B \
  --with-oidc-discovery --issuer https://dev-issuer-b.local \
  >"$LOG_DIR/jwks-B.log" 2>&1 &
JWKS_PID_B=$!

# Poll readiness (curl -k because the cert is locally self-signed for dev).
for jwks_url in https://localhost:7100/jwks.json https://localhost:7101/jwks.json; do
  for _ in $(seq 1 30); do
    curl -k -fsS "$jwks_url" >/dev/null 2>&1 && break
    sleep 1
  done
  curl -k -fsS "$jwks_url" >/dev/null 2>&1 || { echo "FATAL: JWKS not ready at $jwks_url"; exit 3; }
done

# ----- 8. Mint two JWTs (1800s TTL) -----
# Capture stderr so 'Missing keypair' surfaces. The grep '^eyJ' filter strips any
# stderr noise that survives the redirect — defence-in-depth against a future
# regression where dev-token.mjs prints the token on stderr.
# NOTE: do NOT pass --kid here. dev-key-gen.sh derives the JWK kid as the
# RFC 7638 thumbprint of the public key (kept in sync with dev-jwks-build.mjs);
# overriding `--kid A-1` produces a JWT header kid that the JWKS does not contain
# and the server rejects with "no matching key(s) found". Per-issuer separation
# is enforced by the `iss` claim → keyset mapping in the issuerRegistry, not by
# disjoint kid prefixes.
# Write tokens to dedicated files (chmod 600). Capturing via stdout + stderr
# interleave is fragile — a stray "eyJ"-prefixed log line on stderr could win
# the grep race. Files are deleted in the EXIT trap so they never persist.
JWT_FILE_A="$LOG_DIR/jwt-A.txt"
JWT_FILE_B="$LOG_DIR/jwt-B.txt"
node "$REPO_ROOT/scripts/dev-token.mjs" --keyset A \
  --iss https://dev-issuer.local --ttl 3600 \
  >"$JWT_FILE_A" 2>>"$LOG_DIR/dev-token.log"
node "$REPO_ROOT/scripts/dev-token.mjs" --keyset B \
  --iss https://dev-issuer-b.local --ttl 3600 \
  >"$JWT_FILE_B" 2>>"$LOG_DIR/dev-token.log"
chmod 600 "$JWT_FILE_A" "$JWT_FILE_B" 2>/dev/null || true
E2E_JWT_A=$(head -n 1 "$JWT_FILE_A" 2>/dev/null || true)
E2E_JWT_B=$(head -n 1 "$JWT_FILE_B" 2>/dev/null || true)
[[ "$E2E_JWT_A" =~ ^eyJ ]] && [[ "$E2E_JWT_B" =~ ^eyJ ]] || {
  echo "FATAL: JWT mint failed — see $LOG_DIR/dev-token.log" >&2
  exit 4
}

# ----- 9. Render YAML overlays (Node one-liner — avoids envsubst on macOS) -----
node -e '
  const fs = require("fs");
  for (const name of ["websocket-server-e2e.yaml", "websocket-server-multi-issuer.yaml"]) {
    const t = fs.readFileSync("e2e/config/" + name + ".tmpl", "utf8");
    const out = t.replace(/\$REPO_ROOT/g, process.cwd());
    if (/\$[A-Z_]+/.test(out)) {
      console.error("Unsubstituted token in", name, ":", out.match(/\$[A-Z_]+/)[0]);
      process.exit(1);
    }
    fs.writeFileSync("e2e/config/" + name, out);
  }
'
# Validate YAML structure before the launcher tries to consume it.
for f in "$CONFIG_DIR/websocket-server-e2e.yaml" "$CONFIG_DIR/websocket-server-multi-issuer.yaml"; do
  node -e 'require("js-yaml").load(require("fs").readFileSync(process.argv[1], "utf8"))' "$f" \
    || { echo "FATAL: invalid YAML at $f" >&2; exit 5; }
done

# ----- 10. Compute mkcert SPKI for Chromium pinning (real TLS chain validation) -----
MKCERT_SPKI=$(mkcert_spki) || { echo "FATAL: mkcert SPKI extraction failed" >&2; exit 6; }
export MKCERT_SPKI

# ----- 11. Boot launcher (single-issuer overlay first; tests 1-7 use it) -----
echo "[full-stack-e2e] booting launcher with single-issuer overlay..."
REFDATA_DIR="$REPO_ROOT/integration-tests/e2e/data"
./gradlew :launcher:run --no-daemon \
  -Dfix.host=localhost -Dfix.port=19880 \
  -Dcluster.nodeCount=3 \
  -Dcluster.baseDir="$REPO_ROOT/e2e/cluster-data" \
  -Dlog.dir=e2e/logs \
  -Daeron.dir.prefix=e2e \
  -Daccounts.file="$REFDATA_DIR/accounts.yaml" \
  -Dcurrencies.file="$REFDATA_DIR/currencies.yaml" \
  -Drisk-limits.file="$REFDATA_DIR/risk-limits.yaml" \
  -Dsymbols.file="$REFDATA_DIR/symbols.yaml" \
  -Dwebsocket.config.file="$CONFIG_DIR/websocket-server-e2e.yaml" \
  >"$LOG_DIR/launcher.log" 2>&1 &
LAUNCHER_PID=$!
wait_for_system_ready "$LOG_DIR/launcher.log" 90 "$LAUNCHER_PID"
echo "[full-stack-e2e] launcher ready."

if [[ "$KEEP_RUNNING" -eq 1 ]]; then
  # ----- 11b. --keep-running: also start Vite so the browser can play -----
  #
  # The default rehearsal path delegates Vite to Playwright's `webServer:` block
  # in playwright.release-rehearsal.config.ts; that path tears Vite down when
  # the suite ends. `--keep-running` is the "let a human click around" path —
  # the browser needs Vite alive, JWT-bound, until the operator Ctrl+C's.
  if [[ ! -x "$REPO_ROOT/node_modules/.bin/vite" && ! -x "$REPO_ROOT/web-ui/node_modules/.bin/vite" ]]; then
    echo "[full-stack-e2e] installing web-ui npm deps via hermetic Gradle webUiInstall..."
    # Gemini R8 fix: go through the Gradle :web-ui:webUiInstall task instead of bare `npm install`
    # so the pinned Node + npm versions (com.github.node-gradle.node plugin) are used, not whatever
    # the host happens to have on PATH.
    ( cd "$REPO_ROOT" && ./gradlew :web-ui:webUiInstall ) >"$LOG_DIR/npm-install.log" 2>&1 \
      || { echo "FATAL: webUiInstall failed — see $LOG_DIR/npm-install.log" >&2; exit 7; }
  fi
  export VITE_E2E_REAL_BACKEND=true
  export VITE_DEV_JWT="$E2E_JWT_A"
  export VITE_DEV_JWT_A="$E2E_JWT_A"
  export VITE_DEV_JWT_B="$E2E_JWT_B"
  echo "[full-stack-e2e] starting Vite dev server (JWT-bound to issuer A)..."
  ( cd "$REPO_ROOT/web-ui" && npm run dev ) >"$LOG_DIR/vite.log" 2>&1 &
  VITE_PID=$!
  # Poll Vite readiness; tolerate self-signed cert (mkcert chain is trusted via
  # `mkcert -install` but CI runners often skip that, hence -k).
  for _ in $(seq 1 60); do
    if curl -k -fsS "https://localhost:5173" -o /dev/null 2>/dev/null; then break; fi
    sleep 1
  done
  if ! curl -k -fsS "https://localhost:5173" -o /dev/null 2>/dev/null; then
    echo "FATAL: Vite did not become ready on https://localhost:5173 — see $LOG_DIR/vite.log" >&2
    exit 8
  fi
  echo "[full-stack-e2e] --keep-running: stack is up."
  echo "[full-stack-e2e]   browser:   https://localhost:5173 (JWT-bound; no paste needed)"
  echo "[full-stack-e2e]   fix gw:    localhost:19880"
  echo "[full-stack-e2e]   ws server: wss://localhost:8443"
  echo "[full-stack-e2e] press Ctrl+C to tear down."
  EXIT_CODE=0
  # Portable wait-for-either: poll both PIDs every second until one exits, then
  # let the cleanup trap tear the other down. `wait -n` is bash 4.3+; the
  # `kill -0` polling pattern works on any POSIX shell and avoids version drift
  # on CI runners. Sleep cadence chosen to bound teardown latency at 1 s.
  #
  # Safety upper bound — KEEP_RUNNING_MAX_SECONDS (default 12h, overridable via env).
  # Two zombie children that survive their parent could keep `kill -0` returning
  # truthy indefinitely; this cap ensures an unattended session eventually winds
  # down with a clear log line instead of spinning forever. Production / human-
  # interactive sessions are bounded by terminal idle anyway; the cap is the
  # safety net for unattended CI / nohup invocations.
  # LC_ALL=C pins `date +%s` to POSIX behaviour (epoch seconds, locale-independent
  # numeric output) so the arithmetic guard below cannot be perturbed by an
  # unusual LC_TIME / TZ env on a CI runner.
  KEEP_RUNNING_START_S=$(LC_ALL=C date +%s) || { echo "FATAL: date +%s failed" >&2; exit 9; }
  KEEP_RUNNING_MAX_SECONDS="${KEEP_RUNNING_MAX_SECONDS:-43200}"  # 12h default
  while kill -0 "$LAUNCHER_PID" 2>/dev/null && kill -0 "$VITE_PID" 2>/dev/null; do
    if (( $(LC_ALL=C date +%s) - KEEP_RUNNING_START_S > KEEP_RUNNING_MAX_SECONDS )); then
      echo "[full-stack-e2e] --keep-running: ${KEEP_RUNNING_MAX_SECONDS}s safety cap reached — initiating teardown" >&2
      break
    fi
    sleep 1
  done
  exit 0
fi

# ----- 12. Common Playwright env (release-rehearsal narrative + optional sweep) -----
export VITE_E2E_REAL_BACKEND=true
export VITE_DEV_JWT="$E2E_JWT_A"
export VITE_DEV_JWT_A="$E2E_JWT_A"
export VITE_DEV_JWT_B="$E2E_JWT_B"
export E2E_REPO_ROOT="$REPO_ROOT"

if [[ "${TRADING_E2E_FULL_SWEEP:-0}" != "1" ]]; then
  # ----- 13. DEFAULT: release-rehearsal narrative only (APP-225) -----
  #
  # One curated trader-day Playwright spec, single PASS/FAIL, diagnostic
  # bundle on failure via the cleanup trap. JCStress, the stress-test JUnit
  # phase, the broader specs 01-07 regression, and the multi-issuer spec 08
  # are gated behind TRADING_E2E_FULL_SWEEP=1.
  echo "[full-stack-e2e] running release-rehearsal narrative (trader-day.spec.ts)..."
  ./gradlew :web-ui:releaseRehearsal --no-daemon
  EXIT_CODE=$?
  echo "[full-stack-e2e] release-rehearsal exit=$EXIT_CODE"
  exit $EXIT_CODE
fi

# ----- TRADING_E2E_FULL_SWEEP=1: broader regression (legacy default behavior) -----

# Fire JCStress + stress JUnit phase IN PARALLEL with Playwright.
echo "[full-stack-e2e] starting :websocket-server:jcstress in parallel..."
./gradlew :websocket-server:jcstress --no-daemon \
  >"$LOG_DIR/jcstress.log" 2>&1 &
JCSTRESS_PID=$!

echo "[full-stack-e2e] starting :websocket-server:test -Pstress=true in parallel..."
./gradlew :websocket-server:test --no-daemon -Pstress=true \
  --tests "*ReliableStreamTrackerReconnectTest*" \
  >"$LOG_DIR/stress-test.log" 2>&1 &
STRESS_TEST_PID=$!

# Release-rehearsal narrative first (single PASS/FAIL) — keeps the rehearsal
# semantic stable even under the full-sweep path, so a regression sweep that
# would otherwise mask a rehearsal failure still reports it cleanly.
echo "[full-stack-e2e] full-sweep: running release-rehearsal narrative first..."
./gradlew :web-ui:releaseRehearsal --no-daemon
REHEARSAL_RESULT=$?

# Broader specs 01-07 regression.
echo "[full-stack-e2e] full-sweep: running Playwright specs 01-07..."
./gradlew :web-ui:fullStackE2eRun --no-daemon
PLAYWRIGHT_RESULT=$?

# Spec 08 multi-issuer — relaunch the launcher with the multi-issuer overlay.
# The launcher PID dying is necessary but not sufficient — its child processes
# (3 cluster-node JVMs, 3 archive JVMs, the media-driver process) may still hold
# file locks on the Aeron archive segments and the cluster-data dir. Always
# rotate the cluster-data dir for the relaunch.
echo "[full-stack-e2e] restarting launcher with multi-issuer overlay for spec 08..."
kill -TERM "$LAUNCHER_PID" 2>/dev/null || true
for _ in $(seq 1 15); do
  if ! kill -0 "$LAUNCHER_PID" 2>/dev/null; then break; fi
  sleep 1
done
if kill -0 "$LAUNCHER_PID" 2>/dev/null; then
  pkill -KILL -P "$LAUNCHER_PID" 2>/dev/null || true
  kill -KILL "$LAUNCHER_PID" 2>/dev/null || true
fi
pkill -KILL -f -- "-Daeron.dir.prefix=e2e" 2>/dev/null || true
sleep 1
ROTATE_ARGS=(-Daeron.dir.prefix=e2e-mi -Dcluster.baseDir="$REPO_ROOT/e2e/cluster-data-mi")
LAUNCHER_PID=""
./gradlew :launcher:run --no-daemon \
  -Dfix.host=localhost -Dfix.port=19880 \
  -Dcluster.nodeCount=3 \
  "${ROTATE_ARGS[@]}" \
  -Dlog.dir=e2e/logs \
  -Daccounts.file="$REFDATA_DIR/accounts.yaml" \
  -Dcurrencies.file="$REFDATA_DIR/currencies.yaml" \
  -Drisk-limits.file="$REFDATA_DIR/risk-limits.yaml" \
  -Dsymbols.file="$REFDATA_DIR/symbols.yaml" \
  -Dwebsocket.config.file="$CONFIG_DIR/websocket-server-multi-issuer.yaml" \
  >>"$LOG_DIR/launcher.log" 2>&1 &
LAUNCHER_PID=$!
wait_for_system_ready "$LOG_DIR/launcher.log" 90 "$LAUNCHER_PID"

echo "[full-stack-e2e] running Playwright spec 08 (multi-issuer)..."
./gradlew :web-ui:fullStackE2eRunMultiIssuer --no-daemon
MULTI_ISSUER_RESULT=$?

echo "[full-stack-e2e] waiting for JCStress + stress phases to finish..."
wait "$JCSTRESS_PID"; JCSTRESS_RESULT=$?
wait "$STRESS_TEST_PID"; STRESS_TEST_RESULT=$?

# Aggregate — rehearsal failure dominates (the release gate), then the rest.
EXIT_CODE=$(( REHEARSAL_RESULT > 0 ? REHEARSAL_RESULT : 0 ))
[[ "$PLAYWRIGHT_RESULT" -gt 0 && "$EXIT_CODE" -eq 0 ]] && EXIT_CODE=$PLAYWRIGHT_RESULT
[[ "$MULTI_ISSUER_RESULT" -gt 0 && "$EXIT_CODE" -eq 0 ]] && EXIT_CODE=$MULTI_ISSUER_RESULT
[[ "$JCSTRESS_RESULT" -gt 0 && "$EXIT_CODE" -eq 0 ]] && EXIT_CODE=$JCSTRESS_RESULT
[[ "$STRESS_TEST_RESULT" -gt 0 && "$EXIT_CODE" -eq 0 ]] && EXIT_CODE=$STRESS_TEST_RESULT

echo "[full-stack-e2e] full-sweep result: rehearsal=$REHEARSAL_RESULT specs01-07=$PLAYWRIGHT_RESULT multi-issuer=$MULTI_ISSUER_RESULT jcstress=$JCSTRESS_RESULT stress=$STRESS_TEST_RESULT — exit $EXIT_CODE"
exit $EXIT_CODE
