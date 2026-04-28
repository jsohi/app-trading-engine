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

// Compute a stable JWK thumbprint (RFC 7638) for the `kid` so
// downstream verifiers can match by `kid` deterministically.
const canonical = JSON.stringify({ e: jwk.e, kty: jwk.kty, n: jwk.n });
const kid = createHash("sha256").update(canonical).digest("base64url");

const augmented = {
  ...jwk,
  alg: "RS256",
  use: "sig",
  kid,
};

const jwks = { keys: [augmented] };
writeFileSync(values.output, JSON.stringify(jwks, null, 2) + "\n", "utf8");
process.stdout.write(`Wrote ${values.output} (kid=${kid})\n`);
