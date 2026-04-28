/*
 * Vite 8 configuration for the Trading Engine web-ui.
 *
 * Threading model:
 *   - Main thread: React 19 app + AG Grid Enterprise.
 *   - Web Worker: APP-36 owns src/workers/worker.ts — wired here via
 *     the canonical `new Worker(new URL(..., import.meta.url),
 *     { type: 'module' })` idiom (no plugin needed in Vite 6+).
 *
 * HTTPS dev cert resolution (precedence):
 *   1. web-ui/.dev-certs/{cert,key}.pem (mkcert; browser-trusted) →
 *      use directly.
 *   2. Otherwise → @vitejs/plugin-basic-ssl auto self-signed fallback.
 *      Browser shows a one-time warning; accept and proceed.
 *
 * Mock-only path requires neither — `npm run dev` boots against
 * fakeStream.ts / fakeFixBridge.ts with zero infrastructure.
 *
 * Proxy table (matches docs/web-ui.md):
 *   /ws  → wss://localhost:8443  (Netty WebSocket, binary SBE — APP-35)
 *   /fix → ws://localhost:8444   (FIX client bridge JSON — Wave 7 RFQ)
 */
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import basicSsl from "@vitejs/plugin-basic-ssl";
import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv, type Plugin, type ServerOptions } from "vite";

/**
 * Strip the dev-only CSP block from index.html in production builds.
 *
 * The dev CSP meta is permissive (uses 'unsafe-inline') so HMR + React
 * Refresh + AG Grid theme injection work without nonce wiring. We must
 * NOT ship that policy to production. Strict prod CSP is APP-246's job
 * and is delivered as an HTTP response header from the static-asset
 * host (CDN config) — emitting nothing here is the right answer.
 *
 * Dev (mode === 'development'): leave markers + meta as-is.
 * Build (mode === 'production'): strip the wrapped meta + its markers.
 *
 * Markers use the `@csp-dev-only-start` / `@csp-dev-only-end` tokens
 * (in HTML comments) so the literal marker text never appears inside
 * the wrapped block — eliminates the regex-eats-its-own-tail risk if
 * a future maintainer puts a code-reference comment near the markers.
 */
function cspDevOnlyPlugin(): Plugin {
  return {
    name: "trading-engine:csp-dev-only",
    apply: "build",
    transformIndexHtml(html) {
      return html.replace(
        /<!--\s*@csp-dev-only-start\s*-->[\s\S]*?<!--\s*@csp-dev-only-end\s*-->/g,
        "",
      );
    },
  };
}

const __dirname = fileURLToPath(new URL(".", import.meta.url));

const certPath = resolve(__dirname, ".dev-certs", "cert.pem");
const keyPath = resolve(__dirname, ".dev-certs", "key.pem");

function resolveHttps(forceBasic: boolean): ServerOptions["https"] | undefined {
  if (forceBasic) {
    return undefined;
  }
  if (existsSync(certPath) && existsSync(keyPath)) {
    return {
      cert: readFileSync(certPath),
      key: readFileSync(keyPath),
    };
  }
  // Fall through to @vitejs/plugin-basic-ssl auto self-signed.
  return undefined;
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, __dirname, "VITE_");
  const forceBasic = env.VITE_FORCE_BASIC_SSL === "1";
  const httpsConfig = resolveHttps(forceBasic);
  const useMkcertHttps = httpsConfig !== undefined;

  return {
    root: __dirname,
    plugins: [
      react(),
      // Auto self-signed cert fallback when mkcert PEMs are absent OR
      // when VITE_FORCE_BASIC_SSL=1 is set. The plugin is a no-op when
      // server.https is already a concrete cert object.
      ...(useMkcertHttps ? [] : [basicSsl()]),
      // Strip the dev CSP meta from production HTML (APP-246 owns the
      // strict prod policy via response header — see plugin docstring).
      cspDevOnlyPlugin(),
    ],
    resolve: {
      alias: {
        "@": resolve(__dirname, "src"),
      },
    },
    define: {
      // Vite replaces these at build time; expose Node `process.env` shape
      // expected by some Node-targeted libs (e.g., debug). Do NOT leak
      // arbitrary process.env keys into the client bundle.
      "process.env.NODE_ENV": JSON.stringify(mode),
    },
    server: {
      host: "localhost",
      port: 5173,
      strictPort: true,
      ...(useMkcertHttps ? { https: httpsConfig } : {}),
      proxy: {
        "/ws": {
          target: "wss://localhost:8443",
          secure: false,
          ws: true,
          changeOrigin: true,
        },
        "/fix": {
          target: "ws://localhost:8444",
          ws: true,
          changeOrigin: true,
        },
      },
    },
    preview: {
      port: 5174,
      strictPort: true,
    },
    build: {
      target: "es2022",
      sourcemap: true,
      // AG Grid Enterprise + RxJS + OTel are large; let Vite split
      // chunks naturally. Manual chunking can wait until APP-245+.
      chunkSizeWarningLimit: 1024,
    },
    worker: {
      format: "es",
    },
    test: {
      // Vitest config lives in vitest.config.ts to keep the Vite
      // config focused on dev/build/preview. See vitest.config.ts.
    },
  };
});
