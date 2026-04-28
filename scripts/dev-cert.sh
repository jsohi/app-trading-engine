#!/usr/bin/env bash
#
# Generate a locally trusted TLS certificate for the Vite dev server
# (https://localhost:5173) AND for the dev JWKS server
# (https://localhost:7000). Reuses the same PEMs across both endpoints
# so a single mkcert chain trusts everything in dev.
#
# Output:
#   web-ui/.dev-certs/cert.pem
#   web-ui/.dev-certs/key.pem
#
# Pre-flight: requires `mkcert` on PATH. Emits a clear install hint if
# missing. Runs `mkcert -install` once to add the local CA to the
# system trust store.
#
# Usage:
#   ./scripts/dev-cert.sh
#
# This is a developer-only convenience — never invoked by CI.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERT_DIR="${REPO_ROOT}/web-ui/.dev-certs"

if ! command -v mkcert >/dev/null 2>&1; then
  cat >&2 <<'EOF'
mkcert is not installed. Install it first:
  macOS:    brew install mkcert nss
  Linux:    See https://github.com/FiloSottile/mkcert#linux
  Windows:  choco install mkcert
After installation, rerun this script.
EOF
  exit 1
fi

mkdir -p "${CERT_DIR}"
cd "${CERT_DIR}"

# `mkcert -install` is idempotent — re-running is a no-op once the CA
# is registered. Run it unconditionally so first-time contributors get
# the system trust install without a separate step.
mkcert -install

mkcert \
  -cert-file cert.pem \
  -key-file key.pem \
  localhost 127.0.0.1 ::1

echo "Wrote ${CERT_DIR}/cert.pem and ${CERT_DIR}/key.pem"
echo "Vite dev server will pick these up automatically (precedence over basic-ssl fallback)."
