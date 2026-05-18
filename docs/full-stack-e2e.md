# Full-Stack UI E2E Plan — Real Backend, Real WebSocket, Real Browser

## Context

The APP-37 UI (Orders / Positions / Quotes blotters + ConnectionIndicator) currently has:

- Playwright smoke tests (`web-ui/e2e/smoke.spec.ts`, `blotters.spec.ts`) running only against the Vite dev server with `fakeStream` (synthetic in-process Observable).
- A backend E2E (`scripts/e2e.sh` → `:e2e` Gradle task) booting a 3-node cluster + Artio FIX acceptor and asserting via a Java FIX initiator. **No WebSocket, no UI.**

Goal: a real end-to-end test where **nothing is mocked** — cluster, gateway, pricing service, websocket-server, and browser UI are all live, connected over real sockets, and the test asserts that data injected at the FIX edge AND submitted from the real OrderEntryForm flows through the cluster, out the WebSocket, and into the rendered AG Grid blotters.

Branch: `feat/app-{N}-full-stack-e2e` — resolve `{N}` to the parent Linear issue number **before** branch creation; do not push a literal `{N}`. Commit prefix: `APP-{N}: ...` per CLAUDE.md.

---

## Verified facts (no deferrals — every previously "out of scope" item is absorbed below)

1. **`WebSocketLauncher` is already wired** into `TradingEngineLauncher` (Step 10b, gated on `-Dwebsocket.config.file=…`, see `TradingEngineLauncher.java:259-267`). No launcher code change is required — only runtime configuration plus the `-D` flag.
2. **`WorkerClient` already exists** (`web-ui/src/main-thread/workerClient.ts`) and owns worker lifecycle, INIT posting, token MessagePort transfer, watchdog channel, and respawn/breaker logic. Both the test-mode switch (Step 4) and the new browser-→-cluster command path (Step 12) reuse it.
3. **Playwright already auto-starts Vite** via `playwright.config.ts` `webServer`. Per-project `webServer` is not supported. We isolate the full-stack flow via a separate `web-ui/playwright.full-stack.config.ts`.
4. **The browser-→-cluster command path is 85% wired already**: `WebSocketFrameDispatcher` routes inbound template IDs (NewOrderSingle = template 4, CancelOrderRequest = template 6) into `CommandDispatcher.dispatch()` (`websocket-server/.../CommandDispatcher.java:182-270`), which performs entitlement + rate-limit + dedup, then enqueues onto the `AeronEgressThread` for `aeronCluster.offer(bytes)`. `CommandAck` is a single SBE template (id `70` per `messages/src/main/resources/trading-schema.xml:882`) carrying a `CommandAckStatus` discriminator enum (`Accepted` / `Rejected` / `Duplicate` / `Throttled` — verify exact enum name + ordinals against the schema). Step 12 lands the missing browser-side encode + ack-correlation.
5. **Chromium WILL validate the mkcert chain**: `--ignore-certificate-errors-spki-list=<sha256-of-mkcert-rootCA-spki>` is passed via Playwright `launchOptions.args`; `ignoreHTTPSErrors` is `false` in the full-stack config. Step 13 covers SPKI extraction with cross-OS `mkcert -CAROOT` resolution.
6. **AG Grid Community is industry-standard** for the APP-37 blotter feature set (verified in `web-ui/src/main.tsx:14-16` and `docs/web-ui.md`). Enterprise features (row grouping, pivoting, master-detail, Excel export) are not used by any panel.

Pinned constants (verified against current code):

- FIX port (override): `19880` (default in `LauncherConfig`: `9880`).
- Cluster ports (per `ClusterConfig`): ingress `20110/21110/22110`; consensus `20220/21220/22220`; archive `8010/8011/8012`.
- WebSocket TLS: `8443`.
- Vite dev: `5173` (HTTPS, `--strictPort`).
- JWKS primary: `7000` (HTTPS); JWKS secondary (multi-issuer test): `7001` (HTTPS).
- JWT issuers pinned: `iss-A=https://dev-issuer.local`, `iss-B=https://dev-issuer-b.local`. `aud=trading-ui`. `dev-token.mjs` must accept `--iss` and `--kid` (Step 15 adds these flags if absent).

---

## Architecture

```
                                                        Playwright (Chromium, real TLS chain validation)
                                                            │ navigate, assert
                                                            ▼
[E2EFixTestClient] --FIX/19880--> [Gateway/Artio] --IPC--> [Cluster] --IPC--> [WebSocketServer:8443]
                                                                                    ▲    │ wss (mkcert SPKI-pinned, JWT RS256)
                                                            ┌───────────────────────┘    │
                                                            │   submit(NOS, template 4)  ▼
                                                                              [Vite dev server :5173]
                                                                                 (worker → stores → grids)
[PricingService] --IPC--> [Cluster] --IPC--> [WebSocketServer:8443] (ticks auto-emit)
[JWKS-A:7000] [JWKS-B:7001] (multi-issuer)
```

---

## Files to add / modify

### 1. Cluster launcher — verify only

`launcher/src/main/java/com/trading/engine/launcher/TradingEngineLauncher.java` — Step 10b already invokes `WebSocketLauncher.launch()` when `-Dwebsocket.config.file` is set, with the correct shutdown order. **No code change.** Step 10b's JVM args additionally set:

- `-Xlog:gc*:file=e2e/logs/gc-launcher.log:time,uptime,level,tags` (GC visibility for CI duration regressions).
- `-XX:StartFlightRecording=duration=30m,filename=e2e/logs/launcher.jfr,settings=default,dumponexit=true` — **`default` not `profile`**: `profile` enables method sampling + allocation profiling that adds ~5–10% overhead and can mask the 1500ms post-reconnect replay budget (test 7). Pass `-PenableProfileJfr` to opt into `profile` for diagnostic runs only.
- `-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=e2e/logs/launcher.hprof`.
- `launcher.log` itself uses Log4j2 RollingFile (size-based, 50 MB × 5 files) so `--keep-running` does not grow unbounded.

### 2. FIX injector — extract args + add CLI flags

`integration-tests/src/main/java/com/trading/engine/e2e/E2EFixTestClient.java`:

- New: `E2EFixTestClientArgs.java` — pure POJO + parser, no Artio dep.
- Add CLI flags: `--clord-id <string>`, `--symbol <string>`, `--side <buy|sell>`, `--qty <long>`, `--price <long fixed-point>`, `--scenario <single|match>`. `match` injects two opposing orders to produce a fill.
- New JUnit 6 test `E2EFixTestClientArgsTest`: hermetic, with `@Timeout(value=10, unit=SECONDS)`.

### 3. WebSocket / JWKS configuration — generated overlays (gitignored)

The default `websocket-server/src/main/resources/websocket-server.yaml` has empty `tlsCertPath`/`tlsKeyPath` and a commented-out `issuerRegistry`. The script generates two overlays per run:

- `e2e/config/websocket-server-e2e.yaml` (single-issuer; primary path for tests 1–7).
- `e2e/config/websocket-server-multi-issuer.yaml` (two issuers; used only by test 8 — script reboots launcher with this overlay just before test 8 runs).

Templates `*.tmpl` are committed under `e2e/config/`; rendered files are gitignored. Each overlay contains:

- `tlsCertPath: <ABS>/web-ui/.dev-certs/cert.pem`, `tlsKeyPath: <ABS>/web-ui/.dev-certs/key.pem` (chmod 600 enforced on key in `dev-cert.sh`).
- `issuerRegistry`: one or two entries — `iss-A` → `https://localhost:7000/jwks.json`; `iss-B` → `https://localhost:7001/jwks.json`. **Issuers use disjoint `kid` namespaces** (issuer A uses `kid` prefix `A-`, issuer B uses `B-`) so a confused-deputy attack cannot succeed even if `kid`s otherwise collided.
- `jwtAudience: trading-ui`.
- Reference Log4j2 config: `websocket-server/src/main/resources/log4j2.xml`. Add a RollingFile appender for `com.trading.engine.websocket.*` writing to `e2e/logs/ws-server.log`. Node-side `dev-jwks-server.mjs` uses console logging redirected to file (not subject to the JVM GFLog/Log4j2 split — explicitly noted).

### 4. UI — switch `messageSource.ts` to a real `WorkerClient` in test mode

Edit `web-ui/src/main-thread/messageSource.ts`:

- Add imports:
    ```ts
    import { WorkerClient } from "@/main-thread/workerClient";
    import { devTokenProvider } from "@/main-thread/devTokenProvider";
    ```
    `pushConnectionState` is already imported.
- Augment `web-ui/src/vite-env.d.ts` `ImportMetaEnv` with `VITE_E2E_REAL_BACKEND?: "true" | undefined;` and `VITE_WS_URL?: string | undefined;`.
- Inside `startMessageSource()`, **after** the `started` guard, **before** dev/prod fakeStream paths:
    ```ts
    if (import.meta.env.VITE_E2E_REAL_BACKEND === "true") {
        if (import.meta.env.DEV !== true) {
            throw new Error(
                "VITE_E2E_REAL_BACKEND must only be set in dev mode (defence-in-depth)",
            );
        }
        const wsUrl = import.meta.env.VITE_WS_URL ?? "wss://localhost:5173/ws";
        const client = new WorkerClient({
            tokenProvider: devTokenProvider,
            wsUrl,
        });
        const sub = client.messages$.subscribe({
            next: (m) => _messages.next(m),
            error: (e) => {
                console.error("WorkerClient stream error", e);
                pushConnectionState("DOWN");
            },
        });
        client.start().catch((e) => {
            console.error("WorkerClient start failed", e);
            pushConnectionState("DOWN");
        });
        if (import.meta.hot) {
            import.meta.hot.dispose(() => {
                sub.unsubscribe();
                client.dispose();
                _messages.complete();
                started = false;
            });
        }
        return;
    }
    ```
- New vitest browser-project test `web-ui/test/browser/messageSource-realBackend.browser.test.ts` (canonical filename, used everywhere in this plan): asserts (a) emissions reach `messages$`; (b) `start()` rejection drives connection state to `DOWN`; (c) HMR `dispose` calls `client.dispose()` and resets `started`; (d) the `import.meta.env.DEV` defence-in-depth assertion throws when the flag is set in a prod build.
- Existing `web-ui/test/browser/security/TokenIsolation.browser.test.ts` must continue to pass.

**Production-bundle guard test** at `web-ui/test/integration/build-bundle.test.ts`. Runs against the artifact of `npm run build` (Vite outputs to `web-ui/dist/`). The test invokes `vite build` in a `beforeAll` (or the surrounding Gradle task does). Asserts every `dist/**/*.js` is free of:

1. `VITE_E2E_REAL_BACKEND`
2. `VITE_DEV_JWT`
3. `eyJ[A-Za-z0-9_-]{20,}` (any inlined JWT)
4. `__ordersGridApi`
5. `__forceWsClose`
6. `__cellFlashes`
7. `__e2eHooks`

Plus a **bundle size budget**: both gzipped AND brotli-compressed sums of `dist/assets/*.js` ≤ baselines recorded in checked-in `web-ui/bundle-budget.json` + 10 % headroom each. Brotli is what the production CDN serves; gzip-only would let brotli regress unnoticed. Fails the build if the OrderEntryForm + commandClient + SBE encoder push the bundle past budget.

This test runs as Gradle task `:web-ui:bundleGuard` wired into `check`, **not** into `:web-ui:test` — `vite build` cold-cost is too high to pay on every unit test invocation.

### 5. New script: `scripts/full-stack-e2e.sh`

`set -Eeuo pipefail`. Sequence:

1. **Cleanup**: `rm -rf e2e/logs/* && mkdir -p e2e/logs e2e/config` (bounded log dir per run; `e2e/cluster-data` rotated by `e2eClean` only).
2. **Trap** `EXIT INT TERM HUP` (NOT `ERR` — avoids double-cleanup; covers terminal disconnect/SIGHUP). Guard with `CLEANED=0` flag. On exit: `dump_logs_on_failure`, then SIGTERM all tracked PIDs (`LAUNCHER_PID`, `JWKS_PID_A`, `JWKS_PID_B`), wait up to 10s for graceful Aeron archive flush, then SIGKILL stragglers.
3. **Pre-flight tooling**: `command -v lsof >/dev/null || { echo 'install lsof (apt install lsof / brew install lsof)'; exit 2; }`. Then `lsof -i :5173,:7000,:7001,:8443,:19880,:20110,:21110,:22110,:20220,:21220,:22220,:8010,:8011,:8012` — fail fast if any are bound. (Fallback: if `lsof` unavailable on a stripped CI image, use `ss -ltn 'sport = ...'` per port.)
4. `bash scripts/dev-cert.sh` — generates `web-ui/.dev-certs/{cert,key}.pem`. **Skip `mkcert -install`** in all environments. `dev-cert.sh` `chmod 600` the key. On CI: fatal if `mkcert` binary is missing, with platform install hints (`brew install mkcert nss` on macOS; `apt install mkcert libnss3-tools` on Linux).
5. **Force CA materialization without trust-store install** (atomic, concurrent-CI-job safe):
    ```
    TMP_CERT=$(mktemp -t mkcert-discard-cert.XXXXXX)
    TMP_KEY=$(mktemp -t mkcert-discard-key.XXXXXX)
    trap 'rm -f "$TMP_CERT" "$TMP_KEY"' RETURN  # local sub-trap
    mkcert -cert-file "$TMP_CERT" -key-file "$TMP_KEY" localhost
    ```
    Ensures `$(mkcert -CAROOT)/rootCA.pem` exists. Uses `mktemp` so two concurrent jobs cannot collide; the broad `rm /tmp/discard-*` form is rejected. Resolves §13's SPKI extraction across mac/Linux/CI.
6. **`bash scripts/dev-key-gen.sh --no-yaml`** — adds the `--no-yaml` flag so the script does NOT write the sibling `websocket-server-dev.local.yaml` (the explicit `-D` overlay we pass on command line is the sole source of truth). Generates `web-ui/.dev-certs/jwks-A.json` and `web-ui/.dev-certs/jwks-B.json` with disjoint `kid` namespaces (script extension `--prefix A` / `--prefix B`).
7. Start `bash scripts/dev-jwks-server.sh --port 7000 --keyset jwks-A.json` → `e2e/logs/jwks-A.log` (JWKS_PID_A). Start `bash scripts/dev-jwks-server.sh --port 7001 --keyset jwks-B.json` → `e2e/logs/jwks-B.log` (JWKS_PID_B). Poll both with `curl -k` (30s ceiling each).
8. Mint primary JWT: `E2E_JWT_A=$(node scripts/dev-token.mjs --ttl 1800 --iss https://dev-issuer.local --kid A-1 2>>e2e/logs/dev-token.log | grep -E '^eyJ' )`. The `grep -E '^eyJ'` strips any non-token stderr noise (defence-in-depth against stderr→token leak per security review). Mint secondary: `E2E_JWT_B=$(... --iss https://dev-issuer-b.local --kid B-1 ...)`. Validate: `[[ "$E2E_JWT_A" =~ ^eyJ ]] && [[ "$E2E_JWT_B" =~ ^eyJ ]] || { echo "JWT mint failed"; exit 1; }`. Tokens TTL 1800s (10× margin over worst-case suite duration; tighter blast-radius). Tokens **never** persisted to disk. iat/nbf/exp fixed in `dev-token.mjs` as `iat=floor(Date.now()/1000); nbf=iat-5; exp=iat+ttl` for clock-skew determinism.
9. **Render YAML overlays** (mkdir -p e2e/config first):
    ```
    node -e '
      const fs=require("fs");
      for (const name of ["websocket-server-e2e.yaml","websocket-server-multi-issuer.yaml"]) {
        const t=fs.readFileSync("e2e/config/"+name+".tmpl","utf8");
        const out=t.replace(/\$REPO_ROOT/g, process.cwd());
        if (/\$[A-Z_]+/.test(out)) { console.error("Unsubstituted token in",name,":",out.match(/\$[A-Z_]+/)[0]); process.exit(1); }
        fs.writeFileSync("e2e/config/"+name, out);
      }'
    for f in e2e/config/websocket-server-e2e.yaml e2e/config/websocket-server-multi-issuer.yaml; do
      node -e 'require("js-yaml").load(require("fs").readFileSync(process.argv[1],"utf8"))' "$f" || { echo "Invalid YAML: $f"; exit 1; }
    done
    ```
10. **Boot launcher** with primary overlay:
    ```
    ./gradlew :launcher:run \
      -Dfix.host=localhost -Dfix.port=19880 \
      -Dcluster.nodeCount=3 -Daeron.dir.prefix=e2e \
      -Dwebsocket.config.file="$PWD/e2e/config/websocket-server-e2e.yaml" \
      >e2e/logs/launcher.log 2>&1 &
    LAUNCHER_PID=$!
    ```
    `wait_for_system_ready e2e/logs/launcher.log 90` (extracted helper — see Step 6). Inside the wait, periodically grep `media-driver-*.log` for `[FATAL]` and abort with a clean message if found.
11. **Run Playwright** via Gradle:
    ```
    ./gradlew :web-ui:fullStackE2eRun \
      -PviteDevJwtA="$E2E_JWT_A" -PviteDevJwtB="$E2E_JWT_B" \
      -PmkcertSpki="$MKCERT_SPKI" -PrepoRoot="$PWD"
    ```
    The Gradle task `environment(...)`-passes `VITE_DEV_JWT_A`, `VITE_DEV_JWT_B`, `MKCERT_SPKI`, `E2E_REPO_ROOT`, `VITE_E2E_REAL_BACKEND=true`.
12. **Test 8 multi-issuer** runs LAST (filename ordering `08-multi-issuer.spec.ts`). Before it runs, the script restarts the launcher with the multi-issuer overlay: (a) SIGTERM `LAUNCHER_PID` and wait up to 30s for the Aeron archive to flush its position file (poll `e2e/cluster-data/*/archive/*.position`); if flush incomplete, **rotate the cluster-data directory** by setting `-Daeron.dir.prefix=e2e-mi -Dcluster.archive.dir=e2e/cluster-data-mi` for the second launch — avoids state collision between runs without waiting indefinitely. (b) Re-launch with `-Dwebsocket.config.file=...multi-issuer.yaml`. (c) Wait for SYSTEM_READY again before resuming Playwright.
13. `--keep-running` flag skips Step 11/12 and `wait`s on the launcher PID, leaving the stack up for browser exploration.

### 6. Refactor — shared shell helpers (separate PR landed first)

To avoid `:e2e` regression risk:

- `scripts/lib/wait-system-ready.sh` exposing `wait_for_system_ready <log-file> <timeout-seconds>` (default 90s) plus `[FATAL]` watcher on media-driver logs.
- `scripts/lib/log-capture.sh` exposing `dump_logs_on_failure <dir>` that tails the last 200 lines of every `*.log` under `e2e/logs/`.
- `scripts/lib/mkcert-spki.sh` exposing `mkcert_spki()` — runs `mkcert -CAROOT`, asserts `[[ -s "$CAROOT/rootCA.pem" ]]` (otherwise emits "run mkcert -install once" fatal — distinct from the no-install policy because we're not installing into trust store, only requiring the CA file exists), pipes through `openssl x509 -pubkey | openssl pkey -pubin -outform DER | openssl dgst -sha256 -binary | openssl enc -base64`, then asserts the result matches `^[A-Za-z0-9+/]{43}=$` (catches malformed base64 that would silently disable Chromium pinning).
- Update `scripts/e2e.sh` to source these helpers (no behavior change). Refactor PR must land with green `./gradlew e2e` AND `bash -n scripts/full-stack-e2e.sh` parse check.

### 7. Playwright — separate config for full-stack

`web-ui/playwright.full-stack.config.ts`:

- `testDir: "e2e/full-stack"`, `fullyParallel: false`, `workers: 1`, `retries: 1` (cut from 2 to fit time budget — flake debugging via JSON reporter timing).
- `webServer`: launches Vite with `env: { VITE_E2E_REAL_BACKEND: "true", VITE_DEV_JWT_A: process.env.VITE_DEV_JWT_A, VITE_DEV_JWT_B: process.env.VITE_DEV_JWT_B, ...}`, `reuseExistingServer: false`, `timeout: 120_000` (cold Vite + cluster + worker bootstrap on CI).
- `use.ignoreHTTPSErrors: false` (real chain validation), `use.launchOptions.args: ['--ignore-certificate-errors-spki-list=' + process.env.MKCERT_SPKI]`.
- `use.actionTimeout: 30_000`.
- **Per-context token injection**: tests use `browser.newContext()` and `context.addInitScript(token => { window.__E2E_JWT_OVERRIDE__ = token; }, jwt)`. `devTokenProvider` checks `window.__E2E_JWT_OVERRIDE__` first, falling back to `VITE_DEV_JWT_A`. This enables test 8 to authenticate two contexts with two different tokens. The override hook is added to the production-bundle guard grep list (`__E2E_JWT_OVERRIDE__`).
- **Port collision warning**: both Playwright configs bind 5173. Gradle build-service `usesService` serializes `:web-ui:webUiE2e` and `:web-ui:fullStackE2eRun`.

`package.json` script: `"e2e:full-stack": "playwright test --config=playwright.full-stack.config.ts"`.

### 8. Playwright specs — `web-ui/e2e/full-stack/`

Filename ordering enforces serial execution (workers: 1):

- `01-connection.spec.ts` — test 1
- `02-prices.spec.ts` — test 2
- `03-orders-fix.spec.ts` — test 3
- `04-positions.spec.ts` — test 4
- `05-flash.spec.ts` — test 5
- `06-orders-ui-submit.spec.ts` — test 6
- `07-replay-reconnect.spec.ts` — test 7
- `08-multi-issuer.spec.ts` — test 8 (last; launcher reboots before this spec)

Common imports (`crypto.randomUUID` from `node:crypto` — Playwright runs specs in Node):

```ts
import { randomUUID } from "node:crypto";
import path from "node:path";
const fixCli = path.resolve(
    process.env.E2E_REPO_ROOT!,
    "integration-tests/build/install/integration-tests/bin/integration-tests",
);
```

Common `beforeEach`:

- `await page.goto("/")` (forces fresh worker bootstrap).
- **Subscription readiness gate** (selector verified: wrapper carries `data-state`, dot carries `conn-{green|amber|red}` — `ConnectionIndicator.tsx`):
    ```ts
    await expect(
        page.locator('.conn-indicator[data-state="CONNECTED"]'),
    ).toBeVisible({ timeout: 45_000 });
    ```
    45s ceiling: cold WS connect + AuthAck + JWKS first-hit + worker spawn + cluster ingress session.
- **Cluster-state baseline + drain quiescence**: (a) wait for "drain quiescence" — assert `await page.locator('.ag-row').count()` returns the same value across two reads 500ms apart (re-poll up to 5s if not stable); ensures no in-flight async row from a prior spec is still landing. (b) Snapshot the stable count per blotter into a per-spec `baseline` record. All assertions are deltas vs baseline (spec 7 asserts "10 NEW rows added," not "10 total rows"). ClOrdIDs prefixed with the spec number (`E2E-S03-`, `E2E-S07-`) so a stray row from a prior spec cannot satisfy a later assertion.

`afterEach`: `page.screenshot({ path: "test-results/<test>.png" })` and dump page console messages on failure.

**Test cases:**

1. **ConnectionIndicator turns green on real connect.** After the readiness gate, also assert `.conn-dot.conn-green`. Validates real WS upgrade + RS256 JWT acceptance + `connectionStream$`.
2. **PriceBlotter populates from live PricingService.** ≥1 cell matches `/EUR\/USD|GBP\/USD|USD\/JPY/` within 15s.
3. **OrderBlotter receives a FIX-injected order.** `clordId = "E2E-S03-" + randomUUID()`. `spawnSync(fixCli, ["--scenario","single","--clord-id",clordId,"--host","localhost","--port","19880"])`. Assert one row whose ClOrdID cell equals `clordId` appears within 15s with status `NEW` or `FILLED`.
4. **PositionsBlotter aggregates a fill.** `--scenario match` with two opposing orders, namespaced ClOrdIDs `E2E-S04-buy-...`/`E2E-S04-sell-...`. Assert Positions delta-row with non-zero `qty`.
5. **OrderBlotter status flash on transition.** Pre-FIX-inject, attach a recorder via `page.evaluate(() => { (window as any).__cellFlashes = []; (window as any).__ordersGridApi.addEventListener("cellFlash", e => (window as any).__cellFlashes.push({field: e.colDef.field, rowId: e.node.id})); })`. Assert recorder picks up `field === "status"` within 10s. Both `__ordersGridApi` and `__cellFlashes` are env-gated and bundle-guarded (Step 4).
6. **OrderBlotter receives a UI-submitted order (real APP-160 path).** Test types `clordId = "E2E-S06-" + randomUUID()` into the `OrderEntryForm` (uses `data-testid="order-entry-clord-id"`), fills symbol/qty/side/price, clicks Submit. Asserts: (a) form's submit button reaches loading state; (b) `useOrderSubmission` resolves with `Accepted` status (form clears); (c) row with that exact ClOrdID appears in OrderBlotter within 15s with status `NEW` or `FILLED`. **Negative subtests** in same file (each pinned to one expected outcome — no "either is acceptable"):
    - **Throttle (server-side)**: submit ≤256 orders rapidly with **a unique ClOrdID per submit** (`"E2E-S06-throttle-" + randomUUID()` per call) so the server-side dedup path cannot trigger first; the client-side buffer cap (256) does not engage; the server-side rate-limit does. Assert exactly the `Throttled` status. (A `BackpressureError` here is a regression — the client buffer would silently absorb instead of round-tripping; a `Duplicate` here means the per-submit unique-ID discipline broke.)
    - **Backpressure (client-side)**: submit 257 orders in <50ms with **a unique ClOrdID per submit** (one over the cap, before any can complete); assert the 257th submit rejects with `BackpressureError` synchronously, no worker traffic generated.
    - **Duplicate**: submit the **same** ClOrdID twice (the only subtest that intentionally repeats the ClOrdID); assert second resolves with `Duplicate` status.
    - **Entitlement**: submit a symbol the test JWT's `accounts` claim does not cover; assert `Rejected` with reason `ENTITLEMENT`.
    - **Validation**: type a malformed symbol (`"abc"` not matching `^[A-Z]{3}/[A-Z]{3}$`); assert client-side validator blocks the submit before any worker traffic.
7. **Replay reconnect — no event loss within window.** Inject seqNo 1–5 via FIX (`E2E-S07-` prefix). Wait until OrderBlotter shows the 5 new rows. **Before** `__forceWsClose()`, install an in-page transition recorder via `page.evaluate(() => { (window as any).__connStates = []; const sub = window.__e2eHooks.connectionState$.subscribe(s => (window as any).__connStates.push({s, t: performance.now()})); (window as any).__connStatesUnsub = () => sub.unsubscribe(); })` (the `connectionState$` Observable is exposed by `e2eHooks.ts` only when `import.meta.env.VITE_E2E_REAL_BACKEND === "true"`; `__connStates` and `__connStatesUnsub` are added to the bundle-guard grep list). The recorder captures EVERY state transition in real time — Playwright's poll cadence (~100 ms) cannot miss a sub-poll-interval amber flash. Call `await page.evaluate(() => (window as any).__forceWsClose())`. Inject seqNo 6–10 via FIX during the disconnect window. Assert reconnect with the timeout **derived from `WorkerClient` backoff constants** (do not hardcode 30_000 — import the formula). The spec uses `RECONNECT_GATE_MS = workerClient.maxBackoffMs * workerClient.maxAttempts + WS_HANDSHAKE_BUDGET_MS` constants exported from `workerClient.ts`; tuning the backoff stays in sync with the gate. After `CONNECTED`, assert `await page.evaluate(() => (window as any).__connStates.map(x=>x.s))` includes the ordered subsequence `[..., "RECONNECTING", ..., "CONNECTED"]` (proves the breaker observed the disconnect; a missing `RECONNECTING` state means the worker masked the close and the test is invalid). Tear down the recorder via `__connStatesUnsub()` in the spec's `afterEach`. **Warmup**: before the destructive `__forceWsClose()`, perform one full inject+reconnect cycle (3 orders, force-close, wait for replay) and **measure** the post-reconnect duration. The actual assertion budget is `replay_budget_ms = max(1500, p99_warmup_ms * 1.5)`, with the warmup p99 also written to a checked-in `web-ui/perf-baselines.json` for trend tracking. Then run the real subtest: capture `t0 = await page.evaluate(() => performance.now())` at reconnect; assert all 10 new rows arrive within `replay_budget_ms` of `t0`. Assert no duplicate ClOrdIDs and ascending seqNo order across the 10 rows.
8. **Multi-issuer** (file `08-...`, runs after launcher reboot with multi-issuer overlay).
    - `const ctxA = await browser.newContext(); await ctxA.addInitScript(t => (window as any).__E2E_JWT_OVERRIDE__ = t, process.env.VITE_DEV_JWT_A);`
    - `const ctxB = await browser.newContext(); await ctxB.addInitScript(t => (window as any).__E2E_JWT_OVERRIDE__ = t, process.env.VITE_DEV_JWT_B);`
    - `const pageA = await ctxA.newPage(); const pageB = await ctxB.newPage();` Both `goto("/")`.
    - Assert both reach `.conn-indicator[data-state="CONNECTED"]` within 45s and both show ≥1 price tick. Confirms multi-issuer JWKS fetch + per-issuer `kid` lookup work end-to-end. Test 8 runs last; documents `serial+workers:1` invariant relaxation explicitly.

### 9. Gradle wiring

Root `build.gradle.kts`:

- `fullStackE2e`: depends on `build`, `:integration-tests:installDist`, `:web-ui:webUiE2eDeps`, `:web-ui:bundleGuard`. Shells out to `bash scripts/full-stack-e2e.sh`. The shell script kicks off `:websocket-server:jcstress` IN PARALLEL with the Playwright phase (separate forked JVM, no shared ports). **Timeout: 40 minutes.** Worst-case math: `build` 5–8 min + `npm ci` + Playwright Chromium download 2–3 min + cluster boot 90s + 8 specs × 30s × `retries: 1` = 4 min + multi-issuer relaunch 90s + JCStress 3–5 min (parallel — only adds wall-clock if it exceeds the Playwright phase) + safety = 28–35 min.
- `fullStackE2eClean`: delegates to `e2eClean`, removes `web-ui/playwright-report/`, `web-ui/test-results/`, `web-ui/dist/`, `e2e/config/*.yaml`. `--clean-certs` flag also removes `web-ui/.dev-certs/`.
- **Mutual exclusion**: Gradle build-service `PortLockService` (`BuildService<NoParams>`) shared by `:fullStackE2e`, `:e2e`, `:web-ui:webUiE2e`, `:web-ui:fullStackE2eRun`. All four `usesService` it with `maxParallelUsages = 1`. Documents that local devs must not run them concurrently across shells either (port pre-flight catches that).

`web-ui/build.gradle.kts`:

- `webUiE2eDeps`: runs `npm ci` + `npx playwright install --with-deps chromium`. Inputs: `package-lock.json`. Outputs: `node_modules/.package-lock.json`, `~/.cache/ms-playwright`. Up-to-date checks let Gradle cache it.
- `bundleGuard`: runs `npx vitest run --project=node web-ui/test/integration/build-bundle.test.ts` (pinned to the `node` vitest project — must NOT run under the browser project, where `vite build` cannot execute). The test owns its own `vite build` in `beforeAll`. Wired into `check`, NOT into `:web-ui:test`.
- `fullStackE2eRun`: invokes `npx playwright test --config=playwright.full-stack.config.ts`. Sets env via `environment(...)` for `VITE_DEV_JWT_A/B`, `MKCERT_SPKI`, `E2E_REPO_ROOT`, `VITE_E2E_REAL_BACKEND=true`.

### 10. Logging conventions

All new log files under `e2e/logs/`:

- `e2e/logs/launcher.log`, `e2e/logs/media-driver-*.log` (existing).
- `e2e/logs/gc-launcher.log`, `e2e/logs/launcher.jfr` (new — JVM observability).
- `e2e/logs/jwks-A.log`, `e2e/logs/jwks-B.log`.
- `e2e/logs/ws-server.log` (Log4j2 RollingFile appender added in §3).
- `e2e/logs/dev-token.log` (stderr capture, never contains the token itself thanks to `grep -E '^eyJ'` filter in §5 step 8).
- Vite stdout: Playwright HTML report.

Hot-path module logging (cluster, gateway, pricing, orchestrator) remains GFLog. Infra modules (websocket-server, launcher) Log4j2 Async. Node JWKS server uses console redirected to file (outside the JVM logging matrix). No SLF4J introduced.

### 11. CI

- Job `full-stack-e2e` runs on PR (`if: github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository`) — skips fork PRs by default.
- **Maintainer opt-in for fork PRs** via `workflow_dispatch` with required input `head_sha` (must be a commit SHA, not a branch ref). The `actions/checkout` step pins `ref: ${{ inputs.head_sha }}`. **No `secrets.*` are exposed to the job env beyond `GITHUB_TOKEN`** with `permissions: contents: read` — mitigates the pwn-request class. Document that the maintainer must inspect the SHA before triggering.
- Job timeout: **45 min** (5-min margin over Gradle's 40-min ceiling).
- Cache `~/.cache/ms-playwright`. Shell substitution does NOT expand inside `${{ }}` GitHub Actions expressions, so the cache key is built in two steps:
    ```yaml
    - name: Resolve Playwright version
      run: echo "PLAYWRIGHT_VERSION=$(node -p "require('@playwright/test/package.json').version")" >> "$GITHUB_ENV"
    - uses: actions/cache@v4
      with:
          path: ~/.cache/ms-playwright
          key: playwright-chromium-${{ hashFiles('web-ui/package-lock.json') }}-${{ env.PLAYWRIGHT_VERSION }}
    ```
    Stale Chromium cannot survive a Playwright bump that doesn't change the lockfile range. Gradle cache key includes Gradle version + `*.gradle.kts` content hash.
- `npm audit --audit-level=high` runs as a separate step (fails the job on HIGH+ findings). SHA-256 lockfile-integrity check on the generated SBE codec output (compares against a checked-in `messages/build/generated-codec.sha256`).
- Reporters: `--reporter=list,html,json` — archive `playwright-report/`, `test-results/results.json`, `e2e/logs/` (incl. JFR), `web-ui/dist/`, AND `websocket-server/build/reports/jcstress/` on every run (success and failure). The JCStress HTML report is critical for post-mortem of any concurrency outcome failure.

### 12. Browser-→-cluster command path (APP-160)

The websocket-server and cluster sides are fully implemented. Land the browser side in this plan.

- New: `web-ui/src/main-thread/commandClient.ts`
    - Wraps `WorkerClient` with a typed `submitOrder(payload: NewOrderSinglePayload): Promise<CommandAckResult>` API.
    - Encodes via the **generated SBE codec** (templateId=4, header constants from `MessageHeaderEncoder` — never hardcode `0.1.0`/`schemaId`). Uses `WorkerClient`'s outbound port (extend the worker channel set with a dedicated `commandPort: MessagePort`).
    - The CommandAck wire contract: pin both fields by reading `messages/src/main/resources/trading-schema.xml`'s `CommandAck` template before implementation — confirm (a) `correlationId` field id (the dedup key matched against `seq`), (b) `status` field id (= 2 per the schema reading), (c) `CommandAckStatus` enum ordinals (Accepted=0/Rejected=1/Duplicate=2/Throttled=3 per the schema reading). Hardcode neither — derive from generated decoders.
    - **Request-id table**: pre-allocated `Array<Slot>(1024)` of `{ seq: number, resolve, reject, deadlineMs: number }`. Slot index = `seq & 1023`. **Counter type**: `seq` is `number` masked with `& 0xFFFFFFFF` to match the wire u32 width (BigInt avoided on the hot path because BigInt arithmetic allocates per-op in V8). **Slot-wrap policy** (the wrap that actually happens): at issue time, assert `slots[seq & 1023].resolve === undefined` — if not, reject the new submit immediately with `RequestIdCollisionError` (means an unacked request older than the cap × ack-RTT remains pending). **u32-wire-wrap test**: a separate unit test seeds `seq` near `0xFFFFFFFF`, asserts the masked counter wraps cleanly to 0 without sign-bit corruption. **Timeout**: each slot has `deadlineMs = Date.now() + 5_000` (5 s, `number` not `bigint` — sufficient resolution, zero allocation in V8 small-int path); a single pre-allocated `setInterval(scanForExpiredSlots, 250)` walks slots with a fixed cursor and early-exits when the next slot's `deadlineMs === 0` (free), rejecting expired promises with `CommandTimeoutError` and zeroing the slot.
    - **Bounded outbound buffer cap = 256**, derived from: assumed peak submit rate 100/s × p99 ack RTT 50ms × safety factor 50 = 250, rounded to power-of-2 (256). Documented in `commandClient.ts` Javadoc per CLAUDE.md "design rationale" rule.
    - On the single ack template (id 70), demux on `correlationId` field (matches `seq`); switch on `CommandAckStatus` enum: `Accepted` → resolve; `Rejected/Throttled` → reject with typed error carrying `reasonCode`; `Duplicate` → reject with `DuplicateError`. On `connectionStream$ === DOWN` between submit and ack, reject all in-flight slots with `ConnectionLostError`.
- Worker side: `web-ui/src/workers/worker.ts` accepts the `commandPort`, posts each frame onto the existing wss send queue. Outbound buffers are pooled (free-list of 256 `Uint8Array`s sized to **`NewOrderSingleEncoder.BLOCK_LENGTH + MAX_VAR_DATA_BYTES`** — the exact byte count derived from the generated codec; `MAX_VAR_DATA_BYTES` pinned to a constant matching the longest possible symbol/text fields per the schema). The outbound `submit` path asserts `encodedLength <= POOL_BUFFER_SIZE` and throws `EncoderOverflowError` rather than allocating a larger buffer, so a future schema growth fails loud instead of leaking allocations. `postMessage(buf, [buf.buffer])` transferable transfer for zero-copy; structured-clone envelope cost is unavoidable but constant per call.
- **UI surface**: `web-ui/src/panels/order-entry/OrderEntryForm.tsx` (single-symbol, qty, side, price). Joins panel registry. **Slot to be confirmed by reading `web-ui/src/app/panelRegistry.ts` before implementation** (current slots: `top-bar`, `left-top`, `left-bottom`, `right-top`, `right-middle`, `right-bottom` — pick the first vacant one; if `right-top` is taken, document the picked slot in the implementation PR).
    - Form state is local React `useState` only (no worker traffic per keystroke).
    - **Input validation**: symbol matches `^[A-Z]{3}/[A-Z]{3}$`; qty parsed as `bigint`, bounded `1 ≤ qty ≤ 10_000_000`; price parsed as fixed-point `bigint` with scale `100_000_000n` (`PRICE_SCALE` per CLAUDE.md), bounded by reference-data symbol limits (read from existing reference-data store). Rejected client-side before submit.
    - **Render hygiene**: every dynamic field renders via React text nodes (never `dangerouslySetInnerHTML`). No XSS surface.
    - **Submit rate-limit**: max 10 submits/s client-side (debounce + `setLastSubmit`). Submit button disabled while previous `useOrderSubmission` promise is pending.
    - The `clOrdId` is a **required** TypeScript prop on the typed component contract; the prod call site (currently the panel registry mount) computes `"UI-" + crypto.randomUUID()` at submit time and passes it explicitly. No "default-when-undefined" magic — keeps test injection (`data-testid="order-entry-clord-id"`) and prod codegen on the same path. Test 6 overrides via the `data-testid` input.
- New `web-ui/src/main-thread/useOrderSubmission.ts` hook + co-located vitest unit tests:
    - Resolves on `Accepted`; rejects on `Rejected`/`Throttled`/`Duplicate`/`BackpressureError`/`CommandTimeoutError`/`RequestIdCollisionError`/`ConnectionLostError`/`EncoderOverflowError`.
    - Bounded buffer overflow → `BackpressureError`.
    - **Slot wrap (1024 cycle)**: submit 1024 in-flight without acks (mock the worker to swallow); assert the 1025th rejects with `RequestIdCollisionError`. This is the actual wrap the table experiences.
    - **u32-wire wrap**: separately, seed the `seq` counter near `0xFFFFFFFF`, submit 5 in flight; assert the masked counter wraps to 0 cleanly without sign-bit corruption.
    - Timeout: simulate no ack → reject within 5500ms with `CommandTimeoutError`, slot freed.
    - **Prod-build leakage of `__E2E_JWT_OVERRIDE__`**: simulate `import.meta.env.PROD === true`; assert `devTokenProvider` ignores `window.__E2E_JWT_OVERRIDE__` (the production path must not honour the override hook even if it somehow appears).
- **Allocation tripwire test** at `web-ui/test/browser/perf/commandClient-alloc.browser.test.ts` (uses the vitest browser project with Chromium launched via `--js-flags=--expose-gc` and COOP/COEP headers set in vite for `crossOriginIsolated`):
    - Warmup 100 calls.
    - `globalThis.gc()` then `t0 = performance.measureUserAgentSpecificMemory()` (preferred — accurate cross-origin-isolated reading) or fallback to median of 5 `performance.memory.usedJSHeapSize` reads after a forced GC.
    - 10_000 `submitOrder` calls (with mocked instant-ack worker so slots free immediately).
    - `globalThis.gc()` then `t1 = performance.measureUserAgentSpecificMemory()`.
    - Assert `(t1 - t0) / 10_000 ≤ 16` bytes/call (room for the masked-int counter increment + Promise creation; SBE encode + buffer + send envelope all pooled).
    - Idle-ticks subassertion: 1_000 idle `setInterval(scanForExpiredSlots, 250)` ticks produce zero heap delta (proves the scanner is zero-alloc).
    - Mirrors the `*AllocTest` pattern referenced in CLAUDE.md.

### 13. Real TLS chain validation in Playwright

- `scripts/lib/mkcert-spki.sh` (Step 6 — moved here for clarity): cross-OS resolution via `mkcert -CAROOT`; force CA materialization (Step 5.5); SPKI extraction; base64 sanity assertion.
- `scripts/full-stack-e2e.sh` exports `MKCERT_SPKI`. Playwright config (`playwright.full-stack.config.ts`):
    ```ts
    use: {
      ignoreHTTPSErrors: false,
      launchOptions: { args: [`--ignore-certificate-errors-spki-list=${process.env.MKCERT_SPKI!}`] },
    },
    ```
- Test 1 (ConnectionIndicator green) implicitly validates: a misconfigured server cert (wrong CN/SAN/issuer) fails the chain, which fails the connect, which fails the readiness gate — no silent pass.
- CI: `mkcert` pre-installed in the runner image (document in `.github/workflows/full-stack-e2e.yml` setup step).

### 14. WebSocket replay buffer correctness

- **Server-side** `websocket-server/src/test/java/.../ReliableStreamTrackerReconnectTest.java` (JUnit 6, **`@Timeout(value = 180, unit = SECONDS)` at class level** — covers the 50-iteration smoke at worst-case 2 s/iter = 100 s plus warm-up + non-repeated cases with margin; `@Timeout(value = 2, unit = SECONDS)` per `@RepeatedTest` iteration so a single slow iteration is reported as an iteration timeout, not a misleading class-level timeout):
    - Capacity 8. Capture seqNo 1–10. Assert `oldestSeqNo() == 3`.
    - Request gap [5..10]: assert `copyPayload` returns 6 frames in order, byte-identical to capture inputs, no duplicates.
    - Request gap [1..4]: assert returns the documented "below window" sentinel (verify exact contract by reading `ReliableStreamTracker` API — likely `-1` or `EMPTY` constant; pin in test).
    - **Smoke**: `@RepeatedTest(50)` with `Thread.yield()` injection — per-iteration `@Timeout(2s)` ensures one slow iteration cannot starve the rest. Per-frame CRC validation (CRC32c already applied in framing) so torn frames fail loud.
    - **Stress**: `@RepeatedTest(1000)` with the same body, tagged `@Tag("stress")`. Runs in **every PR's `fullStackE2e` job** as part of an explicit `:websocket-server:test -Pstress=true --tests *ReliableStreamTrackerReconnectTest*` Gradle invocation. **Wiring**: `scripts/full-stack-e2e.sh` Step 11 executes this Gradle task as a separate child process **in parallel with the Playwright phase** (its own forked test JVM, no shared ports — same parallelism rationale as JCStress); the standalone `./gradlew :websocket-server:test` invocation does NOT include the `-Pstress=true` flag, keeping the inner dev loop fast. Class-level `@Timeout(300s)` bounds the stress phase: at the per-iter `@Timeout(2s)` worst case, 1000 × 2s = 2000s (way over) — the 300s class budget assumes a typical p99 iteration of ≤300ms (so 1000 × 0.3s = 300s); if any individual iteration hits its 2s cap, the per-iter timeout fires first and is reported as the iteration index, not a misleading class timeout. Failures report the iteration index for reproducibility; a stable seed is set via `System.setProperty("jqwik.seed", ...)` if the test uses property-based interleavings.
    - **JCStress harness**. New Gradle source set `websocket-server/src/jcstress/java/` using the official `org.openjdk.jcstress:jcstress-core` (pinned in `gradle/libs.versions.toml`) with the JCStress Gradle plugin. Two test classes:
        - `ReliableStreamTrackerCaptureReplayJCStress` — `@JCStressTest @State` two-thread interleaving of one capture thread (writes seqNo N..N+9) and one replay thread (reads gap [N-3..N]); `@Outcome` matrix asserts (a) replay either returns the full ordered range or the documented below-window sentinel, never a partial/torn frame; (b) captured payload bytes match the expected pattern for every accepted outcome.
        - `ReliableStreamTrackerEvictReplayJCStress` — interleaves explicit `evict(seqNo)` with a concurrent gap request that includes that seqNo; `@Outcome` asserts no torn frames and no phantom-gap sentinel for fully-evicted slots.
        - **JCStress run budget pinned** in the Gradle task to bound wall-clock: `mode=quick`, `timeMillis=20_000` per test (20 s), `forks=1`, `iterations=5`. Default `normal` mode is multi-minute per test and would blow the budget. With two test classes × 20 s × (warmup + measurement) + JVM cold start, the realistic ceiling is ~3–5 min.
        - Wired into `:websocket-server:jcstress` Gradle task; runs **in parallel with the Playwright phase** in `scripts/full-stack-e2e.sh` (they share no JVM and contend on no ports — JCStress runs in its own forked JVM under `~/.gradle`, Playwright drives Chromium against the long-lived launcher). Gradle timeout bumped to **40 min** to absorb worst-case + safety margin (see §9). Documented in the runbook with expected duration and `:websocket-server:jcstress` failure triage steps.
- **Browser-side** test 7 covered in §8.

### 15. Production JWT issuer integration

`WebSocketServerConfig` already supports a multi-issuer registry; `JwtValidator` does RS256-only verification with `kid`-required headers, claims validation, JWKS caching via Nimbus `RemoteJWKSet`, refresh-on-failure, and startup preflight. `JwtValidator`'s exp/nbf check **uses the injected `EpochNanoClock` (per CLAUDE.md "Outside cluster" — not `Instant.now()`).** Verify in this plan's first PR commit; if a regression has crept in (`Instant.now()`), fix it as part of this PR.

- `e2e/config/websocket-server-multi-issuer.yaml.tmpl` lists two issuers: A → `https://localhost:7000/jwks.json`, B → `https://localhost:7001/jwks.json`. **kid namespaces are deliberately disjoint** (issuer A uses `A-` prefix, B uses `B-`). Avoids confused-deputy ambiguity.
- New JUnit 6 test `WebSocketServerConfigTest.rejectsHttpJwksUri`: asserts an `http://` jwksUri throws at config load.
- New JUnit 6 test `JwtValidatorMultiIssuerTest.confusedDeputy`: registers two issuers with deliberately-colliding `kid`s; asserts a token signed by issuer A's key cannot be verified against issuer B's key for the same `kid` (must select issuer-then-kid, not kid-alone).
- Production posture: `issuerRegistry` populated via env-var injection at deployment; `kid` rotation handled by Nimbus on-failure refresh; **cache TTL is Nimbus' `RemoteJWKSet` default of 5 minutes (300 s)**, tunable via `DefaultResourceRetriever` constructor — corrected from the earlier "1 h" claim, which was wrong.
- **OIDC `/.well-known/openid-configuration` auto-bootstrap.** Extend `WebSocketServerConfig`'s `IssuerRegistryEntry` with an `oidcDiscoveryUri: String` field (config-optional in the sense that an entry sets _either_ `jwksUri` _or_ `oidcDiscoveryUri` — the Java field is `Optional<String>` because exactly one of the two must be present per entry; this is enforced by the validator in §15 below). At config load (startup, single-threaded, allocation OK):
    - If `jwksUri` is set, use it directly (deterministic; no DNS at startup beyond the JWKS host).
    - If `oidcDiscoveryUri` is set instead, perform a one-time HTTPS GET against it, parse the JSON via Jackson (already on classpath via Artio), extract `jwks_uri` from the discovery document, and substitute. Fail-fast at startup if the doc is unreachable, malformed, or returns a non-`https://` `jwks_uri` — startup must NOT silently fall back. Cache the resolved `jwks_uri` for the process lifetime (no runtime re-discovery — Nimbus' `RemoteJWKSet` already handles JWKS refresh).
    - Both fields set simultaneously is a config error (mutually exclusive; throws at validation).
    - **Trust boundary**: `oidcDiscoveryUri` is operator-controlled config (YAML/env at startup), NOT tenant-supplied. SSRF surface is therefore the operator's responsibility; document this constraint in the YAML schema doc-comment so the field never silently becomes tenant-driven later.
    - **HTTPS-only enforcement** applies to `oidcDiscoveryUri` AND to the `jwks_uri` returned in the discovery doc — same `WebSocketServerConfigTest` extends to assert an `http://` discovery URI throws.
    - **Discovery → JWKS host check (RFC 8414 §3 best practice)**: the resolved `jwks_uri` must share the same host as the `oidcDiscoveryUri` (case-insensitive host comparison; ports may differ). A compromised discovery doc cannot redirect JWKS to an attacker-controlled host.
    - **HTTPS client posture (defence-in-depth, no overrides)**: use the JDK `HttpClient` with default `SSLContext` (system trust store) and default hostname verification. **No** custom `SSLContext`, **no** `HostnameVerifier` override, **no** `trustAll`. Document this in a `// SECURITY:` comment block in the implementation.
    - **Bounded transport**: `connectTimeout=5s`, `requestTimeout=10s`, `responseSizeCap=64 KiB` (read into a bounded buffer; throw `OidcDiscoveryResponseTooLarge` if the body exceeds the cap mid-read). Prevents slowloris stall and OOM from a hostile/slow IdP.
    - **Jackson posture (defence-in-depth)**: dedicated `ObjectMapper` instance for OIDC parsing with `DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES=false` (forward-compat with IdP doc growth), `enableDefaultTyping` explicitly NOT enabled (no polymorphic deserialization gadget chain), no `JavaTimeModule` (the doc has no temporal fields). Deserialize into a small POJO with `jwks_uri` only.
    - **Determinism**: the discovery fetch uses the injected `EpochNanoClock` (no wall clock); failure errors are logged via Log4j2 and the launcher exits with a clean message — no partial-startup state.
    - New JUnit 6 test `WebSocketServerConfigOidcDiscoveryTest`: spins up a local Jetty stub on port `0` (read the bound port back via `ServerConnector.getLocalPort()` so two CI jobs cannot collide); class annotated with single-threaded execution; uses the same mkcert-issued cert as the WS server. Asserts:
        - (a) Successful resolution populates the in-memory registry.
        - (b) Malformed JSON throws at startup.
        - (c) `http://` `jwks_uri` in the discovery doc throws.
        - (d) Unreachable discovery URI throws within the 5 s connect timeout — uses **`https://127.0.0.1:1`** (a deterministic blackhole port; faster and more reliable than relying on OS-level connect-refused timing on restricted CI containers).
        - (e) Cross-host `jwks_uri` throws (RFC 8414 §3 host-mismatch check).
        - (f) Response body > 64 KiB throws `OidcDiscoveryResponseTooLarge`.
    - The dev JWKS server (`scripts/dev-jwks-server.mjs`) is extended with a `--with-oidc-discovery` flag that also serves `/.well-known/openid-configuration`. The `08-multi-issuer.spec.ts` test gets a sub-test that boots the launcher with one issuer using `oidcDiscoveryUri` and the other using direct `jwksUri`, asserting both auth paths work end-to-end.

---

## Critical files

- `launcher/src/main/java/.../TradingEngineLauncher.java` (verify Step 10b at lines 259-267; add JVM observability flags)
- `launcher/src/main/java/.../WebSocketLauncher.java`
- `launcher/src/main/java/.../LauncherConfig.java`
- `cluster/.../ClusterConfig.java` (verified port constants)
- `messages/src/main/resources/trading-schema.xml` (verify CommandAck templateId=70 + `CommandAckStatus` enum ordinals)
- `websocket-server/src/main/resources/websocket-server.yaml` (template for overlays)
- `websocket-server/src/main/java/.../WebSocketServerConfig.java` — extended with optional `oidcDiscoveryUri` per `IssuerRegistryEntry` (mutually exclusive with `jwksUri`); HTTPS-only enforced on both URIs; RFC 8414 §3 host-match; bounded transport (5s connect / 10s request / 64 KiB body cap); Jackson defence-in-depth posture
- `websocket-server/src/main/java/.../JwtValidator.java` (audit `EpochNanoClock` usage on exp/nbf; no other changes)
- `websocket-server/src/main/java/.../CommandDispatcher.java`
- `websocket-server/src/main/java/.../ReliableStreamTracker.java`
- `websocket-server/src/test/java/.../ReliableStreamTrackerReconnectTest.java` (NEW)
- `websocket-server/src/test/java/.../JwtValidatorMultiIssuerTest.java` (NEW)
- `websocket-server/src/test/java/.../WebSocketServerConfigTest.java` (extended with `rejectsHttpJwksUri`)
- `websocket-server/src/test/java/.../WebSocketServerConfigOidcDiscoveryTest.java` (NEW; Jetty stub on port 0; six asserts; uses `https://127.0.0.1:1` blackhole for the unreachable case)
- `websocket-server/src/jcstress/java/.../ReliableStreamTrackerCaptureReplayJCStress.java` and `.../ReliableStreamTrackerEvictReplayJCStress.java` (NEW JCStress source set)
- `websocket-server/build.gradle.kts` (apply JCStress Gradle plugin; new `:websocket-server:jcstress` task with pinned `mode=quick, timeMillis=20_000, forks=1, iterations=5`)
- `gradle/libs.versions.toml` (pin `org.openjdk.jcstress:jcstress-core` version)
- `websocket-server/src/main/resources/log4j2.xml` (`ws-server.log` appender)
- `web-ui/src/main-thread/messageSource.ts`, `workerClient.ts`, `devTokenProvider.ts`, `tokenProvider.ts`
- `web-ui/src/main-thread/commandClient.ts` (NEW — §12)
- `web-ui/src/main-thread/useOrderSubmission.ts` (NEW — §12)
- `web-ui/src/workers/worker.ts` (extend with `commandPort` + outbound buffer pool)
- `web-ui/src/panels/order-entry/OrderEntryForm.tsx` + `register.ts` (NEW — §12; slot pinned in implementation PR)
- `web-ui/src/app/panelRegistry.ts` (read to pick the vacant slot)
- `web-ui/src/shared/layout/connection-indicator/ConnectionIndicator.tsx` (selectors verified)
- `web-ui/src/panels/orders/OrderBlotter.tsx` (verified `enableCellChangeFlash: true`; expose `apiRef.current` on `window.__ordersGridApi` only when `import.meta.env.VITE_E2E_REAL_BACKEND === "true"`)
- `web-ui/src/main-thread/e2eHooks.ts` (NEW — single home for `__forceWsClose`, `__cellFlashes`, `__E2E_JWT_OVERRIDE__`, `__ordersGridApi` references; entire module top-level guarded `if (import.meta.env.VITE_E2E_REAL_BACKEND === "true")`)
- `web-ui/test/browser/security/TokenIsolation.browser.test.ts` (must keep passing)
- `web-ui/test/browser/messageSource-realBackend.browser.test.ts` (NEW)
- `web-ui/test/browser/perf/commandClient-alloc.browser.test.ts` (NEW — §12 alloc tripwire)
- `web-ui/test/integration/build-bundle.test.ts` (NEW — §4 bundle guard, ≥7-string grep + size budget)
- `web-ui/bundle-budget.json` (NEW — checked-in baseline)
- `web-ui/vite.config.ts` (`/ws` proxy)
- `web-ui/playwright.config.ts` (untouched) and new `web-ui/playwright.full-stack.config.ts`
- `web-ui/e2e/full-stack/01-..-08-..spec.ts` (NEW — eight numbered specs)
- `scripts/e2e.sh`, `scripts/dev-cert.sh`, `scripts/dev-key-gen.sh` (extend with `--no-yaml` and `--prefix` flags), `scripts/dev-jwks-server.sh` (extend with `--port`, `--keyset`, and `--with-oidc-discovery` flags), `scripts/dev-token.mjs` (extend with `--iss`, `--kid` flags + pin iat/nbf/exp)
- `scripts/full-stack-e2e.sh` (NEW)
- `scripts/lib/wait-system-ready.sh`, `scripts/lib/log-capture.sh`, `scripts/lib/mkcert-spki.sh` (NEW — separate refactor PR)
- `integration-tests/src/main/java/.../E2EFixTestClient.java` + `E2EFixTestClientArgs.java` + JUnit 6 test
- `e2e/config/websocket-server-e2e.yaml.tmpl`, `e2e/config/websocket-server-multi-issuer.yaml.tmpl` (NEW templates, committed; rendered files gitignored)
- Root `build.gradle.kts` and `web-ui/build.gradle.kts`
- `docs/full-stack-e2e.md` (NEW runbook — **authored in the SAME PR as the implementation, not after**; covers prerequisites, one-command run, `--keep-running` workflow, troubleshooting, log locations, cleanup, multi-issuer launcher reboot procedure, perf-baseline regeneration)
- `web-ui/perf-baselines.json` (NEW — checked-in baselines for the test 7 reconnect replay budget; regenerated via `npm run e2e:full-stack -- --update-baselines`)

---

## Verification

1. Static: `./gradlew spotlessCheck build` passes.
2. Unit/integration: `./gradlew test` covers `E2EFixTestClientArgsTest`, `messageSource-realBackend.browser.test.ts`, `useOrderSubmission` unit tests, `commandClient-alloc.browser.test.ts`, `:websocket-server:test` (incl. new `ReliableStreamTrackerReconnectTest`, `JwtValidatorMultiIssuerTest`, `WebSocketServerConfigTest.rejectsHttpJwksUri`, `WebSocketServerConfigOidcDiscoveryTest`).
   2a. Concurrency: `./gradlew :websocket-server:jcstress` runs the JCStress harness (`ReliableStreamTrackerCaptureReplayJCStress` + `ReliableStreamTrackerEvictReplayJCStress`); HTML report at `websocket-server/build/reports/jcstress/`.
3. Bundle guard: `./gradlew :web-ui:bundleGuard` (≥7-string grep + size budget).
4. Backwards compat: `./gradlew e2e` still green after the helper-extraction PR.
5. Dry-run FIX: `./gradlew :integration-tests:installDist && integration-tests/build/install/integration-tests/bin/integration-tests --scenario single --clord-id MANUAL-1 --host localhost --port 19880`.
6. Manual UI: `bash scripts/full-stack-e2e.sh --keep-running` — leaves cluster + WS + Vite up; open `https://localhost:5173`, watch the blotters fill from real PricingService, then submit via OrderEntryForm.
7. Full automated: `./gradlew fullStackE2e`. Expected green within the 40-minute Gradle ceiling (typical wall-clock 28–35 min including parallel JCStress).
8. Negative paths: kill JWKS-A mid-run → fail-fast at readiness probe; remove mkcert root CA → SPKI sanity assertion fires; corrupt one byte in a replayed frame → CRC mismatch fails test 7.
9. Determinism: `npm audit --audit-level=high` and `messages/build/generated-codec.sha256` integrity check both green.

---

## JFR custom events

Three JDK Flight Recorder custom event classes ship in `messages/src/main/java/com/trading/engine/messages/telemetry/`. They are emitted on the pricing-service agent thread and the websocket-server egress thread respectively; the launcher's existing JFR recording (Phase 1 §1, `settings=default`) captures them automatically — no new launcher wiring is required.

| Event name                              | Class                           | Emitter                                                | Sampling                                                    |
| --------------------------------------- | ------------------------------- | ------------------------------------------------------ | ----------------------------------------------------------- |
| `trading.MarketDataTickPublished`       | `MarketDataTickPublished`       | `MarketDataPublisher.publishOneSlot()`                 | `@Period("100 ms")` — 10 Hz sampled; see rationale below    |
| `trading.MarketDataTickRejected`        | `MarketDataTickRejected`        | `MarketDataPublisher.dropWithSymbol()` / `drop()`      | `@Threshold("0 ms")` — every reject emits unconditionally   |
| `trading.MarketDataFeedStateTransition` | `MarketDataFeedStateTransition` | `MarketDataSubscriptionLivenessTracker.transitionTo()` | No threshold/period — transitions are rare lifecycle events |

### Fields

**`trading.MarketDataTickPublished`**

| Field                 | Type     | Description                                                                                               |
| --------------------- | -------- | --------------------------------------------------------------------------------------------------------- |
| `symbol`              | `String` | ASCII symbol name (e.g. `EURUSD`), stripped of padding                                                    |
| `symbolSeq`           | `long`   | Per-symbol monotonic sequence number at drain time; `0` is the snapshot sentinel                          |
| `publishLatencyNanos` | `long`   | `serverNanos − ingressNanos` in nanoseconds; measures conflation delay from adapter-sample to Aeron-offer |

**`trading.MarketDataTickRejected`**

| Field           | Type     | Description                                                                                                                                     |
| --------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `reasonOrdinal` | `int`    | `RejectReason.ordinal()`: 0=CROSSED, 1=NON_POSITIVE, 2=UNCONFIGURED, 3=BACK_PRESSURED, 4=NOT_CONNECTED, 5=ADMIN_ACTION, 6=MAX_POSITION_EXCEEDED |
| `symbol`        | `String` | Symbol at rejection site; empty string when not determinable (heartbeat path)                                                                   |

**`trading.MarketDataFeedStateTransition`**

| Field            | Type     | Description                                                                                                                                               |
| ---------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `from`           | `String` | Prior state name: `"Live"`, `"Quiet"`, or `"Stale"`                                                                                                       |
| `to`             | `String` | New state name: `"Live"`, `"Quiet"`, or `"Stale"`                                                                                                         |
| `lastFragmentNs` | `long`   | Monotonic ns of the last inbound Aeron fragment (tick or heartbeat) at transition time; `transitionNs − lastFragmentNs` reconstructs the silence duration |

### Threshold / Period rationale

- **`@Period("100 ms")` on `MarketDataTickPublished`**: at 5 ms drain cadence × 4 symbols, the publisher offers up to ~800 ticks/s. Recording every publish at that rate would saturate the JFR chunk buffer within seconds. `@Threshold(value = "0 ms")` would be incorrect here — `@Threshold` gates on event _duration_, which is undefined for a point-in-time publish call (~0 µs); every event would fall below any non-zero threshold and emit zero records. `@Period` is the standard JDK pattern for periodic sampling of high-frequency point events (mirrors CME MDP 3.0 instrumentation).

- **`@Threshold("0 ms")` on `MarketDataTickRejected`**: rejects are pathological. Missing one reject event under a sampling window defeats the diagnostic purpose. Volume is self-limiting: a healthy publisher produces zero rejects/s; an unhealthy publisher whose rejects are detectable via JFR is exactly the scenario this event was designed for.

- **No annotation on `MarketDataFeedStateTransition`**: state transitions occur at most once per ~1–3 s in failure conditions and once per session in normal operation. Volume is negligible; unconditional emit guarantees complete post-incident audit trails per the EBS Direct / ICE Impact pattern.

### Zero-allocation guarantee

All three event commits are wrapped in the `shouldCommit()` guard:

```java
final var e = new MarketDataTickPublished();
if (e.shouldCommit()) {
    e.symbol = unpackSymbol(packedSymbol); // only allocates when JFR is recording
    e.symbolSeq = slot.symbolSeq;
    e.publishLatencyNanos = serverNanos - ingressNanos;
    e.commit();
}
```

When JFR is not recording (the default in production without an explicit `jcmd` or `-XX:StartFlightRecording`), `shouldCommit()` returns `false` in nanoseconds and the field writes — including the `unpackSymbol` String allocation — are skipped. The `MarketDataPublisherAllocTest` zero-alloc regression test continues to pass with this wiring in place.

### Querying

```bash
# List all trading events in a JFR dump
jfr print --events trading.MarketDataTickPublished,trading.MarketDataTickRejected,trading.MarketDataFeedStateTransition launcher.jfr

# Count rejects by reason (requires jfr + jq)
jfr print --json --events trading.MarketDataTickRejected launcher.jfr | jq '[.recording.events[].values.reasonOrdinal] | group_by(.) | map({reason: .[0], count: length})'
```
