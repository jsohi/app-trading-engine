#!/usr/bin/env node
/*
 * Local HTTPS JWKS server for dev. Serves
 *   https://localhost:7000/jwks.json
 * (or whatever PORT env var overrides) using the mkcert PEMs created
 * by scripts/dev-cert.sh.
 *
 * The endpoint MUST be HTTPS — websocket-server's
 * WebSocketServerConfig validates that every `jwksUri` in
 * `issuerRegistry` starts with `https://` and throws
 * IllegalArgumentException otherwise.
 *
 * Pre-flight:
 *   - web-ui/.dev-certs/cert.pem and key.pem exist (run dev-cert.sh).
 *   - web-ui/.dev-certs/jwks.json exists (run dev-key-gen.sh).
 *
 * Threading model: single-threaded HTTPS server. No concurrency.
 */
import { readFileSync, existsSync } from "node:fs";
import { createServer } from "node:https";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import process from "node:process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, "..");

const certDir = resolve(repoRoot, "web-ui", ".dev-certs");
const certPath = resolve(certDir, "cert.pem");
const keyPath = resolve(certDir, "key.pem");
const jwksPath = resolve(certDir, "jwks.json");

const port = Number(process.env.PORT ?? 7000);
const host = process.env.HOST ?? "localhost";

for (const [label, p] of [
  ["TLS cert", certPath],
  ["TLS key", keyPath],
  ["JWKS doc", jwksPath],
]) {
  if (!existsSync(p)) {
    process.stderr.write(
      `Missing ${label} at ${p}. Run scripts/dev-cert.sh and scripts/dev-key-gen.sh first.\n`,
    );
    process.exit(1);
  }
}

const cert = readFileSync(certPath);
const key = readFileSync(keyPath);
const jwks = readFileSync(jwksPath);

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
  res.writeHead(404, { "content-type": "text/plain" });
  res.end("not found\n");
});

server.on("error", (err) => {
  if (err && err.code === "EADDRINUSE") {
    process.stderr.write(
      `Port ${port} is in use. On macOS, AirPlay Receiver claims port 7000 — disable it ` +
        `(System Settings → General → AirDrop & Handoff) or override with PORT=7100.\n`,
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
  process.stdout.write(
    `dev JWKS server listening on https://${host}:${port}/jwks.json\n`,
  );
});
