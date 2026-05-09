/**
 * devTokenProvider — DEV-ONLY token issuer.
 *
 * Reads `VITE_DEV_JWT` from the build environment, packages it onto a
 * `MessagePort`, and returns the port for the worker to consume.
 * Throws in production builds so a missed import surfaces fast.
 *
 * Multiple defense layers ensure this file does not ship to prod:
 *   1. Throws at runtime if `import.meta.env.PROD === true`.
 *   2. ESLint `local/no-dev-token-provider-outside-dev` (registered
 *      in C1; activated against this filename pattern in C7) bans
 *      imports outside `*.dev.ts` and `test/**`.
 *   3. `vite.config.ts` `define` strips `VITE_DEV_JWT` from prod env
 *      to `undefined` at build time.
 *   4. CI `size-limit` regex check fails the prod worker bundle if
 *      this symbol or `VITE_DEV_JWT` appears in emitted artefacts
 *      (wired in C9).
 *
 * Threading: main thread (dev only).
 *
 * Allocation: one `MessageChannel` per call.
 *
 * Plan reference: §4.2 / §5.4 / §6 row 3.
 */

import { type TokenProvider } from "@/main-thread/tokenProvider";

/**
 * Dev-only token provider. Reads `import.meta.env.VITE_DEV_JWT` (defined
 * in `.env.local` or via shell). Throws in PROD; the four-layer guard
 * above means a misimport never lands in a prod bundle.
 */
export const devTokenProvider: TokenProvider = () => {
  if (import.meta.env.PROD) {
    throw new Error(
      "devTokenProvider invoked in production build — APP-160 must own the prod token-issuer iframe path",
    );
  }
  const token: unknown = import.meta.env.VITE_DEV_JWT;
  if (typeof token !== "string" || token === "") {
    return Promise.reject(
      new Error(
        "devTokenProvider: VITE_DEV_JWT not set; configure web-ui/.env.local for local dev",
      ),
    );
  }
  const channel = new MessageChannel();
  channel.port1.postMessage({ type: "TOKEN", value: token });
  channel.port1.close();
  return Promise.resolve(channel.port2);
};
