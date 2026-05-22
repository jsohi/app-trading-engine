#!/usr/bin/env bash
# =============================================================================
# Failure-bundle helper (APP-225 release-rehearsal).
#
# Assembles a triage-friendly tar.gz of everything an engineer needs to debug a
# red rehearsal run offline:
#
#   logs/         — every e2e/logs/*.log produced by the run.
#   playwright/   — web-ui/playwright-report-release-rehearsal/ + test-results/
#                   (trace.zip + screenshots + videos when retain-on-failure).
#   cluster-data/ — Aeron archive segments (*.rec) + cluster snapshots (*.snp).
#                   Capped at the most-recent N segments when over 1 GB; a
#                   `cluster-data.truncated` marker records what was dropped.
#   config/       — rendered YAML overlays + ref-data YAMLs the launcher loaded.
#   env.txt       — `java -version`, `node -v`, `git rev-parse HEAD`, `uname`,
#                   `mkcert -version`, port state at failure (`lsof`), and on
#                   Linux the first 30 lines of /proc/{mem,cpu}info.
#   summary.txt   — single-shot human-readable summary: timestamp, exit code,
#                   which Playwright spec/step failed (parsed from results JSON),
#                   bundle size, environment overlay names.
#
# Output: e2e/logs/release-rehearsal-failure-<UTC-timestamp>.tar.gz.
#
# Threading / safety: pure-bash; runs from the cleanup trap so it must never
# `exit` itself — any failure logs a warning and continues so the trap can
# finish other teardown.
# =============================================================================
set -uo pipefail

# shellcheck disable=SC2034  # consumers source-and-call.
COLLECT_BUNDLE_HELPER_LOADED=1

# Usage: collect_failure_bundle <repo-root>
#
# Side effect: writes the bundle to <repo-root>/e2e/logs/release-rehearsal-failure-<ts>.tar.gz
# and emits the path on stderr (so it survives even when stdout is silenced).
collect_failure_bundle() {
  local repo_root="$1"
  local log_dir="$repo_root/e2e/logs"
  # Belt-and-suspenders: sweep stale staging dirs from previous invocations that
  # may have leaked if a prior call was interrupted by a signal before the
  # function-local RETURN trap could fire (RETURN traps run on function exit,
  # not on parent-shell signal-kill). 1-hour ceiling is conservative —
  # collect_failure_bundle finishes in seconds, so anything older is an orphan.
  find "${TMPDIR:-/tmp}" -maxdepth 1 -type d -name 'release-rehearsal-bundle.*' \
    -mmin +60 -exec rm -rf {} + 2>/dev/null || true
  # Defense-in-depth: the launcher's -Dcluster.baseDir is now an absolute path
  # ($REPO_ROOT/e2e/cluster-data) so the canonical location is the first entry,
  # but older script generations resolved that flag relative to the launcher's
  # CWD (the :launcher subproject), so we still scan the subproject location
  # if the canonical path is empty.
  local cluster_data="$repo_root/e2e/cluster-data"
  local cluster_data_fallback="$repo_root/launcher/e2e/cluster-data"
  if [[ ! -d "$cluster_data" || -z "$(ls -A "$cluster_data" 2>/dev/null)" ]] \
     && [[ -d "$cluster_data_fallback" ]]; then
    cluster_data="$cluster_data_fallback"
  fi
  local config_dir="$repo_root/e2e/config"
  local refdata_dir="$repo_root/integration-tests/e2e/data"
  local web_ui_dir="$repo_root/web-ui"
  local ts
  ts=$(date -u +'%Y%m%dT%H%M%SZ')
  local stage
  # Gemini R8 fix: use full path template instead of `mktemp -t` (the `-t` flag has divergent
  # semantics between GNU and BSD mktemp; the full-path form is portable and explicit).
  stage=$(mktemp -d "${TMPDIR:-/tmp}/release-rehearsal-bundle.XXXXXX") || {
    echo "[bundle] FATAL: mktemp failed" >&2
    return 1
  }
  # Ensure the staging directory is cleaned up on any return path from here
  # forward, including early errors. The trap is function-local — `RETURN`
  # fires when the function returns by any path (success, return 1, or set -e
  # propagation). The outer ${stage}/ var is captured by closure.
  # shellcheck disable=SC2317  # invoked by RETURN trap
  _cfb_cleanup_stage() { rm -rf "$stage" 2>/dev/null || true; }
  trap _cfb_cleanup_stage RETURN
  local bundle_dir="$stage/release-rehearsal-failure-$ts"
  mkdir -p "$bundle_dir"/{logs,playwright,cluster-data,config} || {
    echo "[bundle] FATAL: mkdir failed under $stage" >&2
    return 1
  }

  # --- logs/ ---
  if [[ -d "$log_dir" ]]; then
    # Copy *.log only — exclude the JWT plaintext files which the trap deletes
    # anyway. Best-effort; cp -p preserves timestamps for forensic ordering.
    find "$log_dir" -maxdepth 1 -type f -name '*.log' -exec cp -p {} "$bundle_dir/logs/" \; 2>/dev/null || true
  fi

  # --- playwright/ ---
  if [[ -d "$web_ui_dir/playwright-report-release-rehearsal" ]]; then
    cp -R "$web_ui_dir/playwright-report-release-rehearsal" "$bundle_dir/playwright/report" 2>/dev/null || true
  fi
  if [[ -d "$web_ui_dir/test-results" ]]; then
    cp -R "$web_ui_dir/test-results" "$bundle_dir/playwright/test-results" 2>/dev/null || true
  fi

  # --- cluster-data/ (capped) ---
  if [[ -d "$cluster_data" ]]; then
    local cluster_size_bytes
    cluster_size_bytes=$(du -sk "$cluster_data" 2>/dev/null | awk '{print $1 * 1024}')
    cluster_size_bytes=${cluster_size_bytes:-0}
    if (( cluster_size_bytes > 1073741824 )); then # 1 GB
      # Take only the 20 most-recent archive segments + all *.snp snapshots.
      find "$cluster_data" -type f -name '*.snp' -exec cp -p {} "$bundle_dir/cluster-data/" \; 2>/dev/null || true
      # Gemini PR #81 R2 fix: even `find ... -exec ls -t {} +` can split into multiple `ls`
      # invocations if the path-list exceeds ARG_MAX, and each batch sorts only WITHIN itself
      # — the final `head -20` then picks the 20-most-recent-per-batch, not globally. Use a
      # Node one-liner instead: read all paths from stdin, stat each for mtime, sort globally,
      # take the top 20. Single process, no batch splitting, portable across GNU + BSD. Node
      # is already a hard prereq of the harness (Vite, dev-jwks, etc.) so no new dependency.
      find "$cluster_data" -type f -name '*.rec' 2>/dev/null \
        | node -e '
            const fs = require("fs");
            const paths = fs.readFileSync(0, "utf8").trim().split("\n").filter(Boolean);
            const sorted = paths
              .map((p) => { try { return { p, m: fs.statSync(p).mtimeMs }; } catch { return null; } })
              .filter(Boolean)
              .sort((a, b) => b.m - a.m)
              .slice(0, 20)
              .map((x) => x.p);
            process.stdout.write(sorted.join("\n") + (sorted.length ? "\n" : ""));
          ' 2>/dev/null \
        | while read -r f; do cp -p "$f" "$bundle_dir/cluster-data/" 2>/dev/null || true; done
      echo "cluster-data truncated to most-recent 20 *.rec segments + all *.snp; original size=${cluster_size_bytes} bytes" \
        > "$bundle_dir/cluster-data.truncated"
    else
      # Whole thing fits — copy the entire cluster-data tree.
      # The Aeron Cluster layout uses per-node subdirs (archive-0/, archive-1/,
      # archive-2/, archive-gateway/, cluster-0/, …) rather than a single
      # archive/ directory, so we copy everything rather than listing specific
      # subdirs. The 1 GB cap above keeps the bundle manageable.
      cp -R "$cluster_data/." "$bundle_dir/cluster-data/" 2>/dev/null || true
    fi
  fi

  # --- config/ (rendered overlays + ref-data) ---
  for f in "$config_dir/websocket-server-e2e.yaml" "$config_dir/websocket-server-multi-issuer.yaml"; do
    [[ -f "$f" ]] && cp -p "$f" "$bundle_dir/config/" 2>/dev/null || true
  done
  for f in accounts.yaml currencies.yaml risk-limits.yaml symbols.yaml; do
    [[ -f "$refdata_dir/$f" ]] && cp -p "$refdata_dir/$f" "$bundle_dir/config/" 2>/dev/null || true
  done

  # --- env.txt (fingerprint) ---
  {
    echo "=== timestamp ==="; date -u +'%Y-%m-%d %H:%M:%S UTC'
    echo ""; echo "=== java ==="; java -version 2>&1 | head -5 || true
    echo ""; echo "=== node ==="; node -v 2>&1 || true
    echo ""; echo "=== npm ==="; npm -v 2>&1 || true
    echo ""; echo "=== mkcert ==="; mkcert -version 2>&1 || true
    echo ""; echo "=== uname ==="; uname -a 2>&1 || true
    if [[ "$(uname -s)" == "Darwin" ]]; then
      echo ""; echo "=== sw_vers ==="; sw_vers 2>&1 || true
      echo ""; echo "=== sysctl hw.memsize / ncpu ==="; sysctl -n hw.memsize hw.ncpu 2>&1 || true
    else
      echo ""; echo "=== /proc/meminfo (first 10) ==="; head -10 /proc/meminfo 2>/dev/null || true
      echo ""; echo "=== /proc/cpuinfo (first 30) ==="; head -30 /proc/cpuinfo 2>/dev/null || true
    fi
    echo ""; echo "=== git ==="
    ( cd "$repo_root" && git rev-parse HEAD 2>/dev/null; git status --short 2>/dev/null; git log -1 --oneline 2>/dev/null ) || true
    echo ""; echo "=== ports at failure ==="
    for p in 5173 7100 7101 8443 19880 20220 21220 22220 8010 8011 8012; do
      lsof -i ":$p" -sTCP:LISTEN 2>/dev/null || true
    done
  } > "$bundle_dir/env.txt" 2>&1

  # --- summary.txt ---
  local results_json="$web_ui_dir/test-results/release-rehearsal-results.json"
  local failing_step="unknown"
  if [[ -f "$results_json" ]] && command -v node >/dev/null 2>&1; then
    # Walk the Playwright JSON reporter shape: root.suites[].specs[].tests[].results[].
    # For each failing result, descend into result.steps[] (which carries the
    # `test.step(...)` titles) and pick the first non-passing step. That gives
    # us `<spec title> :: <step title>` instead of the test title alone.
    failing_step=$(node -e '
      try {
        const r = JSON.parse(require("fs").readFileSync(process.argv[1], "utf8"));
        const failing = [];
        const findFailingStep = (steps) => {
          if (!Array.isArray(steps)) return null;
          for (const s of steps) {
            if (s.error || (s.duration && s.skipped === false && s.error)) return s.title;
            const child = findFailingStep(s.steps);
            if (child) return child;
          }
          // Fallback: last step (often the one that timed out without raising)
          return steps.length > 0 ? steps[steps.length - 1].title : null;
        };
        const walk = (n) => {
          if (!n) return;
          if (Array.isArray(n.suites)) n.suites.forEach(walk);
          if (Array.isArray(n.specs)) n.specs.forEach(walk);
          if (Array.isArray(n.tests)) {
            n.tests.forEach((t) => {
              const res = (t.results || []).find((r) => r.status !== "passed" && r.status !== "skipped");
              if (res) {
                const stepTitle = findFailingStep(res.steps) || "<no-step-info>";
                failing.push((n.title || "") + " :: " + (t.title || "") + " :: " + stepTitle);
              }
            });
          }
        };
        (r.suites || []).forEach(walk);
        process.stdout.write(failing.join(" | ") || "no-failing-spec-found");
      } catch (e) { process.stdout.write("results-parse-error: " + e.message); }
    ' "$results_json" 2>/dev/null) || failing_step="results-parse-error"
  fi
  {
    echo "Release-rehearsal failure bundle"
    echo "================================"
    echo "timestamp:        $ts"
    echo "host:             $(uname -n)"
    echo "repo:             $repo_root"
    echo "failing spec/step: $failing_step"
    echo ""
    echo "Contents:"
    ( cd "$bundle_dir" && find . -type f -maxdepth 3 | sort )
  } > "$bundle_dir/summary.txt" 2>&1

  # --- tar.gz ---
  local out="$log_dir/release-rehearsal-failure-$ts.tar.gz"
  mkdir -p "$log_dir" || {
    echo "[bundle] WARN: cannot create $log_dir" >&2
    return 1
  }
  # Surface tar errors to stderr (the RETURN trap will still clean the staging
  # dir). Previously the 2>/dev/null hid e.g. "Cannot mkdir: No space left on
  # device" — that's a real operator-actionable failure, not noise.
  local tar_err
  tar_err=$( ( cd "$stage" && tar -czf "$out" "release-rehearsal-failure-$ts" ) 2>&1 ) || {
    echo "[bundle] WARN: tar failed: $tar_err" >&2
    return 1
  }
  echo "[bundle] failure bundle: $out" >&2
  echo "$out"
}
