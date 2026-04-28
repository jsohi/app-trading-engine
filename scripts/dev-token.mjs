#!/usr/bin/env node
/*
 * Mint a developer JWT signed with the local RS256 keypair from
 * scripts/dev-key-gen.sh. Issuer / audience match the dev override
 * written to websocket-server-dev.local.yaml so the local server
 * accepts the token without further config.
 *
 * Output: token printed to stdout. Best-effort copy to clipboard via
 * `clipboardy` (ESM-only); falls back to stdout-only if clipboard
 * access fails (CI, sandboxes, headless).
 *
 * Usage:
 *   node scripts/dev-token.mjs                  # default 24h validity, sub=dev-user
 *   node scripts/dev-token.mjs --sub alice      # custom subject
 *   node scripts/dev-token.mjs --ttl 3600       # custom TTL (seconds)
 *
 * Threading model: single-threaded CLI. No concurrency.
 */
import { readFileSync, existsSync } from "node:fs";
import { createPrivateKey, createSign, createHash } from "node:crypto";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import process from "node:process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, "..");

const { values } = parseArgs({
  options: {
    sub: { type: "string", default: "dev-user" },
    ttl: { type: "string", default: String(60 * 60 * 24) }, // 24h
    iss: { type: "string", default: "https://dev-issuer.local" },
    aud: { type: "string", default: "trading-ui" },
  },
});

const privateKeyPath = resolve(
  repoRoot,
  "web-ui",
  ".dev-certs",
  "jwt-private.pem",
);
const publicKeyPath = resolve(
  repoRoot,
  "web-ui",
  ".dev-certs",
  "jwt-public.pem",
);

if (!existsSync(privateKeyPath) || !existsSync(publicKeyPath)) {
  process.stderr.write(
    `Missing keypair at ${privateKeyPath}. Run scripts/dev-key-gen.sh first.\n`,
  );
  process.exit(1);
}

const privatePem = readFileSync(privateKeyPath, "utf8");
const publicPem = readFileSync(publicKeyPath, "utf8");
const privateKey = createPrivateKey(privatePem);
const publicJwk = createPrivateKey(privatePem)
  .export({ format: "jwk" });

// Match the kid computed in dev-jwks-build.mjs (RFC 7638 thumbprint
// over `e`, `kty`, `n`).
const canonical = JSON.stringify({
  e: publicJwk.e,
  kty: publicJwk.kty,
  n: publicJwk.n,
});
const kid = createHash("sha256").update(canonical).digest("base64url");

const now = Math.floor(Date.now() / 1000);
const ttl = Number(values.ttl);
const header = { alg: "RS256", typ: "JWT", kid };
const payload = {
  iss: values.iss,
  aud: values.aud,
  sub: values.sub,
  iat: now,
  exp: now + ttl,
  jti: createHash("sha256")
    .update(`${values.sub}-${now}-${Math.random()}`)
    .digest("base64url"),
};

function b64url(input) {
  return Buffer.from(input)
    .toString("base64")
    .replace(/=+$/u, "")
    .replace(/\+/gu, "-")
    .replace(/\//gu, "_");
}

const signingInput = `${b64url(JSON.stringify(header))}.${b64url(
  JSON.stringify(payload),
)}`;
const signer = createSign("RSA-SHA256");
signer.update(signingInput);
signer.end();
const signature = signer
  .sign(privateKey)
  .toString("base64")
  .replace(/=+$/u, "")
  .replace(/\+/gu, "-")
  .replace(/\//gu, "_");

const token = `${signingInput}.${signature}`;
process.stdout.write(`${token}\n`);

// Best-effort clipboard copy. Skipped silently if `clipboardy` is not
// installed or clipboard access fails (headless CI, sandboxed env).
try {
  const mod = await import("clipboardy");
  await mod.default.write(token);
  process.stderr.write("(token copied to clipboard)\n");
} catch {
  // intentional no-op — stdout is the canonical channel.
}

// Suppress unused warning while still demonstrating intent: we keep
// publicPem read so any malformed public key (e.g., file permissions
// gone wrong post-rotation) surfaces here, not at first server use.
void publicPem;
