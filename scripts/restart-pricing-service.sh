#!/usr/bin/env bash
# =============================================================================
# restart-pricing-service.sh — pause-then-resume the embedded pricing-service
# AgentRunner via the launcher's E2E management HTTP endpoint.
#
# Used by the Playwright full-stack spec 09 (feed-stale lifecycle) to drive the
# STALE → LIVE transition without killing the launcher JVM (which would also
# stop the websocket-server egress thread that emits the LIVE/STALE state
# notifications to the browser).
#
# Requirements:
#   - TRADING_E2E_MGMT_PORT env var set to the launcher's management endpoint
#     port (configured in scripts/full-stack-e2e.sh).
#   - The launcher must have been started with TRADING_E2E_MGMT_ENABLED=1.
#
# Behaviour:
#   1. POST /e2e/pricing/resume (idempotent — no-op if pricing is already up)
#   2. Health-poll until the endpoint reports ok within 5 s.
#   3. Exit 0 when complete.
# =============================================================================
set -Eeuo pipefail

PORT="${TRADING_E2E_MGMT_PORT:-}"
if [[ -z "$PORT" ]]; then
  echo "FATAL: TRADING_E2E_MGMT_PORT not set — full-stack-e2e.sh must export it" >&2
  exit 2
fi

BASE_URL="http://127.0.0.1:${PORT}"

# 1. Health-check the endpoint up front so we surface a clear error if the
#    launcher's management server is not listening.
if ! curl -fsS --max-time 2 "${BASE_URL}/e2e/health" >/dev/null; then
  echo "FATAL: ${BASE_URL}/e2e/health unreachable — is launcher running with TRADING_E2E_MGMT_ENABLED=1?" >&2
  exit 3
fi

# 2. POST resume. Server is idempotent; safe to call when pricing is already up.
RESP=$(curl -fsS --max-time 5 -X POST "${BASE_URL}/e2e/pricing/resume" || true)
if [[ "$RESP" != "resumed" ]]; then
  echo "FATAL: resume call did not return 'resumed' (got: '${RESP}')" >&2
  exit 4
fi

# 3. Re-verify health after resume so the script only exits 0 when the endpoint
#    is still responsive (i.e. the launcher did not crash mid-resume).
if ! curl -fsS --max-time 2 "${BASE_URL}/e2e/health" >/dev/null; then
  echo "FATAL: ${BASE_URL}/e2e/health unreachable after resume" >&2
  exit 5
fi

echo "[restart-pricing-service] pricing-service resumed via ${BASE_URL}"
