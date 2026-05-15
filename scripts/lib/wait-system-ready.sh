#!/usr/bin/env bash
# =============================================================================
# wait_for_system_ready — block until the launcher logs SYSTEM_READY, with
# fail-fast on startup errors, launcher death, and media-driver FATALs.
#
# Usage (sourced):
#   source scripts/lib/wait-system-ready.sh
#   wait_for_system_ready <log-file> <timeout-seconds> [launcher-pid]
#
# Behavior:
#   - Polls <log-file> every 1s for the literal string "SYSTEM_READY".
#   - Fails fast (returns 1) if:
#       * "Startup failed" appears in <log-file>
#       * <launcher-pid> (if provided) is no longer alive
#       * any "<log-dir>/media-driver-*.stdout.log" file contains "[FATAL]"
#       * <timeout-seconds> elapses
#   - Returns 0 on success.
#
# Why "Startup failed" specifically (not generic "fatal"): GFLog config emits
# enumeration lists like '[TRACE, DEBUG, INFO, WARN, ERROR, FATAL]' that
# false-match a generic 'fatal' grep. Match the Log4j2 ERROR phrase instead.
# =============================================================================

# Shared by both scripts/e2e.sh and scripts/full-stack-e2e.sh; do not change
# semantics without verifying both consumers.
wait_for_system_ready() {
    local log_file="$1"
    local timeout_seconds="${2:-90}"
    local launcher_pid="${3:-}"
    local log_dir
    log_dir="$(dirname "$log_file")"

    local deadline=$((SECONDS + timeout_seconds))
    while ! grep -q "SYSTEM_READY" "$log_file" 2>/dev/null; do
        if [[ $SECONDS -ge $deadline ]]; then
            echo "FAIL: did not reach SYSTEM_READY within ${timeout_seconds}s (log: $log_file)" >&2
            return 1
        fi
        if [[ -n "$launcher_pid" ]] && ! kill -0 "$launcher_pid" 2>/dev/null; then
            echo "FAIL: launcher process (PID $launcher_pid) died during startup" >&2
            return 1
        fi
        if grep -q "Startup failed" "$log_file" 2>/dev/null; then
            echo "FAIL: 'Startup failed' detected in $log_file" >&2
            return 1
        fi
        # Watch for fatal media-driver errors. Use a glob; if no files match,
        # the loop body is skipped. Snapshot+restore nullglob (NEVER blindly
        # `shopt -u nullglob`) so we don't mutate caller shell state if the
        # caller (or an outer script) had nullglob enabled.
        #
        # NOTE (added by helper-extraction PR): this `[FATAL]` fast-fail is a
        # NEW behavior vs the pre-PR scripts/e2e.sh which only checked the
        # launcher.log "Startup failed" string. Aeron media driver does not
        # emit `[FATAL]` during normal port-bind retries — only on hard,
        # non-recoverable failures (cnc-file write failure, address-in-use
        # after retries exhausted). Documented as deliberate tightening.
        # `shopt -p nullglob` returns exit code 1 when the option is OFF
        # (default state) — under `set -e` that would abort the function.
        # Combining into the `local` declaration masks the exit code (well-known
        # bash idiom). The captured string is still a valid `shopt -u nullglob`
        # restore command.
        local md_file
        local _prior_nullglob="$(shopt -p nullglob)"
        shopt -s nullglob
        for md_file in "$log_dir"/media-driver-*.stdout.log; do
            if grep -q "\[FATAL\]" "$md_file" 2>/dev/null; then
                echo "FAIL: [FATAL] detected in media-driver log: $md_file" >&2
                eval "$_prior_nullglob"
                return 1
            fi
        done
        eval "$_prior_nullglob"
        sleep 1
    done
    return 0
}
