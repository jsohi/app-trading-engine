#!/usr/bin/env node
/*
 * Local HTTPS JWKS server for dev. Serves
 *   https://localhost:7000/jwks.json
 * (or whatever --port / PORT env var overrides) using the mkcert PEMs created
 * by scripts/dev-cert.sh.
 *
 * The endpoint MUST be HTTPS — websocket-server's
 * WebSocketServerConfig validates that every `jwksUri` in
 * `issuerRegistry` starts with `https://` and throws
 * IllegalArgumentException otherwise.
 *
 * CLI flags (added for the full-stack-e2e flow):
 *   --port <N>                 HTTPS port to bind (default: PORT env or 7000).
 *   --keyset <NAME>            Serve web-ui/.dev-certs/jwks-<NAME>.json instead of
 *                              the default jwks.json. Used by the multi-issuer
 *                              E2E (plan §15) to run two parallel JWKS servers
 *                              on 7000 and 7001 with disjoint keysets (A / B).
 *   --with-oidc-discovery      Also serve /.well-known/openid-configuration as
 *                              an RFC 8414-style discovery doc pointing at this
 *                              server's own /jwks.json. The `issuer` claim is
 *                              derived from --issuer (default https://dev-issuer.local).
 *   --issuer <URL>             OIDC `issuer` claim used in the discovery doc.
 *                              Required when --with-oidc-discovery is set.
 *
 * Pre-flight:
 *   - web-ui/.dev-certs/cert.pem and key.pem exist (run dev-cert.sh).
 *   - web-ui/.dev-certs/jwks(-<keyset>).json exists (run dev-key-gen.sh [--prefix <NAME>]).
 *
 * Threading model: single-threaded HTTPS server. No concurrency.
 */
import { readFileSync, existsSync } from "node:fs";
import { createServer } from "node:https";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import process from "node:process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, "..");

const { values } = parseArgs({
  options: {
    port: { type: "string" },
    keyset: { type: "string" },
    "with-oidc-discovery": { type: "boolean", default: false },
    issuer: { type: "string", default: "https://dev-issuer.local" },
    // Public host used in URL-construction (issuer, jwks_uri). Distinct from
    // the bind host (`HOST` env, default 0.0.0.0). RFC 8414 §3 requires the
    // discovery URI's host to equal the jwks_uri's host (case-insensitive). Our
    // YAML-configured discovery URI uses `localhost`, so this default must
    // match. Override via --public-host for non-localhost deployments.
    "public-host": { type: "string", default: "localhost" },
  },
  allowPositionals: false,
});

const certDir = resolve(repoRoot, "web-ui", ".dev-certs");
const certPath = resolve(certDir, "cert.pem");
const keyPath = resolve(certDir, "key.pem");
const jwksFile = values.keyset ? `jwks-${values.keyset}.json` : "jwks.json";
const jwksPath = resolve(certDir, jwksFile);

const port = Number(values.port ?? process.env.PORT ?? 7000);
if (!Number.isFinite(port) || port < 0 || port > 65535) {
  process.stderr.write(`--port must be 0..65535, got '${values.port ?? process.env.PORT}'\n`);
  process.exit(1);
}
// Bind to IPv4 wildcard by default. Node's `server.listen(port, "localhost")`
// would resolve to a single AF (IPv6 ::1 first on modern macOS), and the JVM's
// `RemoteJWKSet` resolves `localhost` to IPv4 (127.0.0.1) → "Connection refused".
// `0.0.0.0` is dev-only; the JWT/JWKS contents are non-sensitive ephemeral
// keys minted by `dev-key-gen.sh` and the listener exits with the e2e script.
const host = process.env.HOST ?? "0.0.0.0";

for (const [label, p] of [
  ["TLS cert", certPath],
  ["TLS key", keyPath],
  ["JWKS doc", jwksPath],
]) {
  if (!existsSync(p)) {
    const hint = label === "JWKS doc" && values.keyset
      ? ` Run scripts/dev-key-gen.sh --prefix ${values.keyset} first.`
      : " Run scripts/dev-cert.sh and scripts/dev-key-gen.sh first.";
    process.stderr.write(`Missing ${label} at ${p}.${hint}\n`);
    process.exit(1);
  }
}

const cert = readFileSync(certPath);
const key = readFileSync(keyPath);
const jwks = readFileSync(jwksPath);

// OIDC discovery payload assembled lazily — only constructed if the flag is on,
// so the default JWKS-only mode has zero extra allocation. The discovery URL is
// host-relative; the WebSocketServerConfig (RFC 8414 §3) requires the resolved
// `jwks_uri` to share host with the discovery URI, so we always emit the same
// host:port for both — never an external redirect.
function buildDiscoveryDoc(actualPort) {
  // Use the public-host (default `localhost`) — NOT the bind host (default
  // `0.0.0.0`). RFC 8414 §3: discovery URI host MUST equal jwks_uri host. Our
  // YAML overlay points the discovery URI at `localhost:<port>`; emitting
  // `0.0.0.0:<port>` here would make OidcDiscoveryClient reject the doc with
  // a host-mismatch exception at launcher startup → multi-issuer launch fails.
  const base = `https://${values["public-host"]}:${actualPort}`;
  return JSON.stringify(
    {
      issuer: values.issuer,
      jwks_uri: `${base}/jwks.json`,
      // Subset of RFC 8414 §2 — we only fill the fields a JWT verifier might
      // consult. Adding more (token_endpoint, etc.) would imply server
      // capabilities we don't have.
      id_token_signing_alg_values_supported: ["RS256"],
    },
  );
}

let discoveryDocCached = null;

const server = createServer({ cert, key }, (req, res) => {
  // Parse the pathname so cache-busting query strings (`/jwks.json?v=1`)
  // and fragments don't 404. The base host is irrelevant — `req.url`
  // is server-relative — we just need a valid base for the URL ctor.
  const pathname = new URL(req.url ?? "/", `https://${host}`).pathname;
  if (pathname === "/jwks.json" && req.method === "GET") {
    res.writeHead(200, {
      "content-type": "application/json",
      "cache-control": "no-store",
    });
    res.end(jwks);
    return;
  }
  if (
    values["with-oidc-discovery"] &&
    pathname === "/.well-known/openid-configuration" &&
    req.method === "GET"
  ) {
    if (discoveryDocCached === null) {
      discoveryDocCached = buildDiscoveryDoc(server.address().port);
    }
    res.writeHead(200, {
      "content-type": "application/json",
      "cache-control": "no-store",
    });
    res.end(discoveryDocCached);
    return;
  }
  res.writeHead(404, { "content-type": "text/plain" });
  res.end("not found\n");
});

server.on("error", (err) => {
  if (err && err.code === "EADDRINUSE") {
    process.stderr.write(
      `Port ${port} is in use. On macOS, AirPlay Receiver claims port 7000 — disable it ` +
        `(System Settings → General → AirDrop & Handoff) or override with --port 7100.\n`,
    );
    process.exit(1);
  }
  // Other errors: emit a clean diagnostic and exit non-zero rather
  // than rethrow (rethrowing inside an event-emitter callback yields
  // an uncaught-exception with a noisier stack and the same exit code).
  process.stderr.write(`dev JWKS server error: ${err?.message ?? String(err)}\n`);
  process.exit(1);
});

server.listen(port, host, () => {
  // Read the actual bound port from the server (not the requested `port`):
  // when --port=0 the OS auto-picks an ephemeral port, and the IT harness needs
  // the real number to construct the issuerRegistry JWKS URL. Logging the
  // requested port (e.g. "0") would leak straight through to a malformed URL.
  const actual = server.address().port;
  const extras = values["with-oidc-discovery"]
    ? ` (+ /.well-known/openid-configuration; issuer=${values.issuer})`
    : "";
  const keysetLabel = values.keyset ? ` keyset=${values.keyset}` : "";
  process.stdout.write(
    `dev JWKS server listening on https://${host}:${actual}/jwks.json${extras}${keysetLabel}\n`,
  );
});
