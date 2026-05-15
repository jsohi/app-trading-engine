/// <reference types="vite/client" />

/**
 * Augments the default Vite ImportMetaEnv with the project-specific build-time
 * env vars consumed by the web-ui. Adding fields here keeps `import.meta.env.X`
 * strictly typed across the codebase and prevents the strict-TS `=== "true"`
 * narrowing from rejecting `unknown` values.
 *
 * Vite contract: every `VITE_*` var passed via the build process or `.env*`
 * file is inlined as a static string at build time. Whether or not a var is
 * defined is therefore a build-time concern — runtime branches that read these
 * vars get dead-code-eliminated by esbuild when the var is "false"/undefined.
 *
 * Bundle-guard cross-reference: `web-ui/test/integration/build-bundle.test.ts`
 * greps every dist/*.js for `VITE_E2E_REAL_BACKEND` and `VITE_DEV_JWT` to
 * prove the test-mode escape hatch never ships to prod even by accident.
 */
interface ImportMetaEnv {
  /** Dev-mode JWT injected by `scripts/full-stack-e2e.sh` for the primary issuer. */
  readonly VITE_DEV_JWT?: string | undefined;
  /** Multi-issuer A JWT (plan §15). Used by the multi-issuer Playwright spec. */
  readonly VITE_DEV_JWT_A?: string | undefined;
  /** Multi-issuer B JWT (plan §15). */
  readonly VITE_DEV_JWT_B?: string | undefined;
  /** Test-mode escape hatch — when `"true"`, messageSource wires the real WorkerClient. */
  readonly VITE_E2E_REAL_BACKEND?: "true" | undefined;
  /** Override for the WebSocket URL the WorkerClient opens (defaults to `wss://localhost:5173/ws` via Vite proxy). */
  readonly VITE_WS_URL?: string | undefined;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
