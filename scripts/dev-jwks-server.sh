#!/usr/bin/env bash
# POSIX wrapper around scripts/dev-jwks-server.mjs.
# Forwards all args; respects PORT / HOST env overrides.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec node "${REPO_ROOT}/scripts/dev-jwks-server.mjs" "$@"
