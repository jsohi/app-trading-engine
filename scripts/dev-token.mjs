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
import {
  createPrivateKey,
  createPublicKey,
  createSign,
  createHash,
  randomUUID,
} from "node:crypto";
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
    // Custom `accounts` claim (List<String>) — JwtValidator requires at least
    // one non-empty entry. Default "ACME" matches integration-tests/e2e/data/accounts.yaml.
    // Pass --accounts ACME,LOCKED for multi-account dev tokens.
    accounts: { type: "string", default: "ACME" },
    // Override the auto-derived RFC 7638 thumbprint kid. Used by the
    // full-stack-e2e multi-issuer test (plan §15) which mints two tokens
    // against two issuers with deliberately disjoint kid namespaces
    // (`A-1` vs `B-1`) to exercise the JwtValidator's issuer-then-kid
    // selection (confused-deputy guard).
    kid: { type: "string" },
    // Load a non-default keypair: jwt-private-<keyset>.pem instead of
    // jwt-private.pem. Mirrors scripts/dev-key-gen.sh --prefix and
    // scripts/dev-jwks-server.mjs --keyset so that minting → serving →
    // verifying all stay aligned for the A/B multi-issuer flow.
    keyset: { type: "string" },
  },
});

const ttlSeconds = Number(values.ttl);
if (!Number.isFinite(ttlSeconds) || ttlSeconds <= 0) {
  process.stderr.write(`--ttl must be a positive integer (seconds); got '${values.ttl}'\n`);
  process.exit(1);
}

const keysetSuffix = values.keyset ? `-${values.keyset}` : "";
if (values.keyset && !/^[A-Za-z0-9_-]+$/u.test(values.keyset)) {
  process.stderr.write(`--keyset must match [A-Za-z0-9_-]+, got '${values.keyset}'\n`);
  process.exit(1);
}
const privateKeyPath = resolve(repoRoot, "web-ui", ".dev-certs", `jwt-private${keysetSuffix}.pem`);
const publicKeyPath = resolve(repoRoot, "web-ui", ".dev-certs", `jwt-public${keysetSuffix}.pem`);

if (!existsSync(privateKeyPath) || !existsSync(publicKeyPath)) {
  process.stderr.write(`Missing keypair at ${privateKeyPath}. Run scripts/dev-key-gen.sh first.\n`);
  process.exit(1);
}

const privatePem = readFileSync(privateKeyPath, "utf8");
// Read the public PEM purely to validate it parses; throws on malformed
// keys (e.g., file-permissions-rotated state) so we surface the error at
// token mint time rather than at first server use.
const publicPem = readFileSync(publicKeyPath, "utf8");
createPublicKey(publicPem); // throws on malformed PEM
const privateKey = createPrivateKey(privatePem);
// `KeyObject.export({ format: 'jwk' })` on a private key emits the JWK
// containing the public modulus + exponent — that's what we need for the
// `kid` thumbprint. Avoids re-parsing the PEM a second time.
const publicJwk = privateKey.export({ format: "jwk" });

// Match the kid computed in dev-jwks-build.mjs (RFC 7638 §3.1
// thumbprint over `e`, `kty`, `n`). Both files MUST use the same
// canonical form — keys sorted lexicographically, no whitespace,
// UTF-8 encoded — or `kid` won't match across mint and verify and
// JWKS lookup will fail. Explicit sort prevents silent kid drift if
// a future maintainer reorders the input object literal.
const canonical = canonicaliseJwk({
  e: publicJwk.e,
  kty: publicJwk.kty,
  n: publicJwk.n,
});
const derivedKid = createHash("sha256").update(canonical, "utf8").digest("base64url");
const kid = values.kid ?? derivedKid;

/**
 * RFC 7638 §3.1 canonical JSON: keys sorted lex, no whitespace.
 *
 * @param {Record<string, string | undefined>} obj
 * @returns {string}
 */
function canonicaliseJwk(obj) {
  const sorted = Object.keys(obj).sort();
  const parts = sorted.map((k) => `${JSON.stringify(k)}:${JSON.stringify(obj[k])}`);
  return `{${parts.join(",")}}`;
}

// Pin iat/nbf/exp deterministically (plan §5 step 8). nbf=iat-5 absorbs
// CI clock skew; iat is wall-second of mint; exp is iat+ttl. The 5-second
// nbf back-window matches the JwtValidator's documented skew tolerance.
const now = Math.floor(Date.now() / 1000);
const header = { alg: "RS256", typ: "JWT", kid };
const accountsList = values.accounts
  .split(",")
  .map((s) => s.trim())
  .filter((s) => s.length > 0);
if (accountsList.length === 0) {
  process.stderr.write("--accounts must contain at least one non-empty entry\n");
  process.exit(1);
}

const payload = {
  iss: values.iss,
  aud: values.aud,
  sub: values.sub,
  iat: now,
  nbf: now - 5,
  exp: now + ttlSeconds,
  // RFC 7519 `jti` (JWT ID): unique-per-mint identifier. `randomUUID()`
  // is the standard Node idiom — backed by `crypto.randomBytes(16)`,
  // RFC 4122 v4 format, no per-character entropy concerns.
  jti: randomUUID(),
  // Custom claim consumed by JwtValidator.extractAccountsClaim — must be a
  // non-empty List<String>; each entry is matched against AccountStore by code.
  accounts: accountsList,
};

function b64url(input) {
  return Buffer.from(input)
    .toString("base64")
    .replace(/=+$/u, "")
    .replace(/\+/gu, "-")
    .replace(/\//gu, "_");
}

const signingInput = `${b64url(JSON.stringify(header))}.${b64url(JSON.stringify(payload))}`;
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
