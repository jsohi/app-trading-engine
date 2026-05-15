#!/usr/bin/env bash
# =============================================================================
# mkcert_spki — print the base64 SHA-256 SPKI hash of the local mkcert root CA.
#
# Used by scripts/full-stack-e2e.sh to feed Chromium's
# --ignore-certificate-errors-spki-list flag, so Playwright performs REAL TLS
# chain validation against the mkcert-issued cert (no `ignoreHTTPSErrors`
# shortcut).
#
# Usage (sourced):
#   source scripts/lib/mkcert-spki.sh
#   MKCERT_SPKI=$(mkcert_spki) || exit $?
#
# Failure modes (each emits a distinct error to stderr and returns 1):
#   - mkcert binary missing
#   - $(mkcert -CAROOT)/rootCA.pem missing or empty (CA never materialized;
#     callers should run a one-shot `mkcert -cert-file ... -key-file ...` first
#     to force generation, without touching the system trust store)
#   - openssl pipeline produces a value that does not match the expected
#     base64 SPKI hash shape (^[A-Za-z0-9+/]{43}=$). A malformed value silently
#     disables Chromium pinning, so we fail hard rather than ship a no-op flag.
# =============================================================================

mkcert_spki() {
    if ! command -v mkcert >/dev/null 2>&1; then
        echo "mkcert_spki: mkcert binary not found in PATH" >&2
        echo "  install hint (macOS): brew install mkcert nss" >&2
        echo "  install hint (Linux): apt install mkcert libnss3-tools" >&2
        return 1
    fi
    if ! command -v openssl >/dev/null 2>&1; then
        echo "mkcert_spki: openssl not found in PATH" >&2
        return 1
    fi

    local caroot
    caroot="$(mkcert -CAROOT 2>/dev/null)"
    if [[ -z "$caroot" || ! -s "$caroot/rootCA.pem" ]]; then
        echo "mkcert_spki: rootCA.pem not found at '$caroot/rootCA.pem'" >&2
        echo "  the CA file has not been generated yet. The full-stack-e2e.sh" >&2
        echo "  script forces generation via a discard-cert step BEFORE calling" >&2
        echo "  this helper; if you are calling mkcert_spki standalone, run:" >&2
        echo "    mkcert -cert-file /tmp/_discard.pem -key-file /tmp/_discard-key.pem localhost" >&2
        return 1
    fi

    # Local pipefail so any pipeline-stage failure (corrupt rootCA.pem, etc.)
    # surfaces as a non-empty stderr from the offending stage, not as silent
    # garbage that the regex check below catches without naming the culprit.
    # Snapshot+restore so we don't mutate caller pipefail state. The combined
    # `local var="..."` form masks the pipeline exit code (would otherwise
    # abort under set -e if grep found no match — though `set +o` always
    # emits 'set +o pipefail' or 'set -o pipefail').
    local _prior_pipefail="$(set +o | grep pipefail)"
    set -o pipefail
    local spki
    spki="$(openssl x509 -in "$caroot/rootCA.pem" -noout -pubkey \
        | openssl pkey -pubin -outform DER 2>/dev/null \
        | openssl dgst -sha256 -binary \
        | openssl enc -base64)" || {
            echo "mkcert_spki: openssl pipeline failed (see stderr above for offending stage)" >&2
            eval "$_prior_pipefail"
            return 1
        }
    eval "$_prior_pipefail"

    # Strict shape check — base64 of a 32-byte SHA-256 is exactly 44 chars
    # (43 base64 chars + one '=' pad). Anything else means a pipeline failure
    # silently produced garbage; Chromium would accept the flag and then ignore
    # every cert (silent no-op pinning).
    if [[ ! "$spki" =~ ^[A-Za-z0-9+/]{43}=$ ]]; then
        echo "mkcert_spki: openssl pipeline produced an unexpected value: '$spki'" >&2
        echo "  expected base64(SHA-256(SPKI)) — 43 base64 chars + '=' pad" >&2
        return 1
    fi

    echo "$spki"
}
