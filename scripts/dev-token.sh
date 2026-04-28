#!/usr/bin/env bash
# POSIX wrapper around scripts/dev-token.mjs.
# Forwards all args verbatim.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec node "${REPO_ROOT}/scripts/dev-token.mjs" "$@"
