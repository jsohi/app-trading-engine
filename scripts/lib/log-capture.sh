#!/usr/bin/env bash
# =============================================================================
# dump_logs_on_failure — tail the last 200 lines of every *.log under <log-dir>
# and grep each for ERROR / EXCEPTION / FATAL fingerprints. Idempotent.
#
# Usage (sourced):
#   source scripts/lib/log-capture.sh
#   dump_logs_on_failure <log-dir>
#
# Output is unconditional (we are dumping precisely BECAUSE something failed).
# Caller controls when to call this — typically from the EXIT trap on a non-zero
# exit code path.
# =============================================================================

dump_logs_on_failure() {
    local log_dir="$1"
    if [[ ! -d "$log_dir" ]]; then
        echo "dump_logs_on_failure: log dir not found: $log_dir" >&2
        return 0
    fi

    echo ""
    echo "=== ERRORS / EXCEPTIONS / FATALS (across *.log under $log_dir) ==="
    # Case-insensitive so we match Aeron stack-trace lowercase variants
    # (`error`, `Exception`, `fatal`) — preserves the original scripts/e2e.sh
    # diagnostic surface. -I skips binary files (e.g. *.jfr, *.hprof) and
    # --include limits to *.log so a stray YAML/JSON snapshot under
    # $log_dir does not pollute the failure dump.
    grep -RIniE --include='*.log' "ERROR|Exception|FATAL" "$log_dir" 2>/dev/null | tail -40 || true

    echo ""
    echo "=== LAST 200 LINES OF EACH *.log UNDER $log_dir ==="
    # Snapshot+restore nullglob so we don't mutate caller shell state.
    # Explicit `*.log */*.log` (top-level + one level deep) instead of
    # `**/*.log` — without `globstar` enabled, `**` collapses to `*` anyway,
    # so the explicit form is more honest and resists future maintainer
    # surprise if someone later enables globstar elsewhere.
    # `local var="$(...)"` masks the exit code of `shopt -p nullglob`, which
    # returns 1 when nullglob is OFF (default). Without masking, `set -e`
    # callers would abort on the snapshot.
    local f
    local _prior_nullglob="$(shopt -p nullglob)"
    shopt -s nullglob
    for f in "$log_dir"/*.log "$log_dir"/*/*.log; do
        [[ -f "$f" ]] || continue
        echo ""
        echo "--- $f ---"
        tail -200 "$f" 2>/dev/null || true
    done
    eval "$_prior_nullglob"
}
