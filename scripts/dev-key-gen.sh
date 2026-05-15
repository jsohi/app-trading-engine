#!/usr/bin/env bash
#
# First-time-contributor script that generates an RS256 keypair for
# locally signed dev JWTs, writes the corresponding JWKS document,
# and creates a websocket-server dev override that points the
# issuerRegistry at the dev JWKS endpoint.
#
# Output:
#   web-ui/.dev-certs/jwt-private.pem      (RSA 2048 private key — gitignored)
#   web-ui/.dev-certs/jwt-public.pem       (RSA 2048 public key)
#   web-ui/.dev-certs/jwks.json            (served by scripts/dev-jwks-server.mjs)
#   websocket-server/src/main/resources/websocket-server-dev.local.yaml
#
# Idempotent: re-running rotates the keypair (writes fresh PEMs) and
# regenerates the JWKS + override. If you only want to refresh the
# override, regenerate after rotating keys.
#
# Pre-flight:
#   - openssl on PATH (mac/linux: usually present; windows: ships with Git Bash).
#   - node on PATH (used to compute the public-key JWK via the bundled mjs helper).

set -euo pipefail

# CLI flags (added for the full-stack-e2e flow):
#   --no-yaml          Skip writing the websocket-server-dev.local.yaml
#                      override. Used by scripts/full-stack-e2e.sh which passes
#                      its own -Dwebsocket.config.file overlay and must not be
#                      shadowed by a sibling YAML on the classpath.
#   --prefix <NAME>    Suffix the keypair + JWKS output paths with -<NAME>
#                      (e.g. --prefix A → jwt-private-A.pem, jwks-A.json).
#                      Used by the multi-issuer E2E to mint two disjoint
#                      keysets (A and B) without collision.
WRITE_YAML=1
PREFIX=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-yaml) WRITE_YAML=0; shift ;;
    --prefix)
      [[ $# -ge 2 ]] || { echo "--prefix requires a value" >&2; exit 1; }
      [[ "$2" =~ ^[A-Za-z0-9_-]+$ ]] || { echo "--prefix '$2' must match [A-Za-z0-9_-]+" >&2; exit 1; }
      PREFIX="$2"; shift 2 ;;
    --help|-h)
      sed -n '1,30p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 1 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERT_DIR="${REPO_ROOT}/web-ui/.dev-certs"
SUFFIX=""
[[ -n "${PREFIX}" ]] && SUFFIX="-${PREFIX}"
PRIVATE_PEM="${CERT_DIR}/jwt-private${SUFFIX}.pem"
PUBLIC_PEM="${CERT_DIR}/jwt-public${SUFFIX}.pem"
JWKS_JSON="${CERT_DIR}/jwks${SUFFIX}.json"
WS_DEV_OVERRIDE="${REPO_ROOT}/websocket-server/src/main/resources/websocket-server-dev.local.yaml"

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required. macOS: built in. Linux: install via apt/yum." >&2
  exit 1
fi
if ! command -v node >/dev/null 2>&1; then
  echo "node 22+ is required (matches engines.node in root package.json)." >&2
  exit 1
fi

mkdir -p "${CERT_DIR}"

echo "Generating RSA 2048 dev signing keypair..."
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "${PRIVATE_PEM}"
openssl rsa -in "${PRIVATE_PEM}" -pubout -out "${PUBLIC_PEM}"
chmod 600 "${PRIVATE_PEM}"

echo "Building JWKS document..."
node "${REPO_ROOT}/scripts/dev-jwks-build.mjs" \
  --public "${PUBLIC_PEM}" \
  --output "${JWKS_JSON}"

if [[ "${WRITE_YAML}" -eq 1 ]]; then
  echo "Writing websocket-server dev override..."
  mkdir -p "$(dirname "${WS_DEV_OVERRIDE}")"
  cat > "${WS_DEV_OVERRIDE}" <<'YAML'
# Local-only dev override. Gitignored — see root .gitignore.
# Loaded by websocket-server when present; falls back to
# checked-in websocket-server.yaml otherwise.
jwtAudience: "trading-ui"
issuerRegistry:
  "https://dev-issuer.local":
    jwksUri: "https://localhost:7000/jwks.json"
YAML
else
  echo "--no-yaml passed: skipping websocket-server dev override write."
fi

cat <<EOF
Done.
  Private key: ${PRIVATE_PEM} (gitignored)
  Public key:  ${PUBLIC_PEM}
  JWKS doc:    ${JWKS_JSON}
$( [[ "${WRITE_YAML}" -eq 1 ]] && echo "  Server cfg:  ${WS_DEV_OVERRIDE} (gitignored)" )

Next steps:
  ./scripts/dev-cert.sh                   # if you have not generated the TLS cert
  ./scripts/dev-jwks-server.sh &          # serve JWKS at https://localhost:7000
  TOKEN=\$(./scripts/dev-token.sh)
EOF
