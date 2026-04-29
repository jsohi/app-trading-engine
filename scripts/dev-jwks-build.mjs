#!/usr/bin/env node
/*
 * Build a JWKS document from an RSA public-key PEM.
 *
 * Used by scripts/dev-key-gen.sh as a portable replacement for
 * `openssl rsa -pubin -modulus`-style hand-rolled extraction.
 * Computes RFC 7517 JWK fields directly from the parsed key via
 * Node's `KeyObject.export({ format: 'jwk' })`.
 *
 * Usage:
 *   node scripts/dev-jwks-build.mjs --public <path-to-public.pem> --output <jwks.json>
 *
 * Threading model: single-threaded CLI. No concurrency.
 */
import { readFileSync, writeFileSync } from "node:fs";
import { createPublicKey, createHash } from "node:crypto";
import { parseArgs } from "node:util";
import process from "node:process";

const { values } = parseArgs({
  options: {
    public: { type: "string" },
    output: { type: "string" },
  },
});

if (!values.public || !values.output) {
  process.stderr.write(
    "Usage: dev-jwks-build.mjs --public <pubkey.pem> --output <jwks.json>\n",
  );
  process.exit(2);
}

const pem = readFileSync(values.public, "utf8");
const key = createPublicKey(pem);
const jwk = key.export({ format: "jwk" });

// Compute a stable JWK thumbprint (RFC 7638 §3.1) for the `kid` so
// downstream verifiers can match by `kid` deterministically. The spec
// requires a JSON object containing the JWK's required members in
// LEXICOGRAPHIC order with no whitespace, encoded UTF-8. For RSA keys
// the required members are e, kty, n. We sort the keys explicitly
// (rather than relying on JS object insertion order) so a future
// maintainer reordering the literal can't silently change every kid.
const canonical = canonicaliseJwk({ e: jwk.e, kty: jwk.kty, n: jwk.n });
const kid = createHash("sha256").update(canonical, "utf8").digest("base64url");

/**
 * Build the RFC 7638 canonical JSON form: keys sorted lexicographically,
 * no whitespace, no insignificant chars. Sort guarantees byte-stable
 * output regardless of input object literal key order.
 *
 * @param {Record<string, string | undefined>} obj
 * @returns {string}
 */
function canonicaliseJwk(obj) {
  const sorted = Object.keys(obj).sort();
  const parts = sorted.map((k) => `${JSON.stringify(k)}:${JSON.stringify(obj[k])}`);
  return `{${parts.join(",")}}`;
}

const augmented = {
  ...jwk,
  alg: "RS256",
  use: "sig",
  kid,
};

const jwks = { keys: [augmented] };
writeFileSync(values.output, JSON.stringify(jwks, null, 2) + "\n", "utf8");
process.stdout.write(`Wrote ${values.output} (kid=${kid})\n`);
