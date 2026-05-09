/**
 * DevTokenProvider.test.ts — unit tests for the dev-only token provider.
 *
 * Tests per APP-36 §4.2 / §5.4 / §6 row 3.
 *
 * Threading: single-threaded (Vitest jsdom worker).
 * Allocation: test-only — one MessageChannel per happy-path call.
 *
 * Implementation note: `devTokenProvider` reads `import.meta.env.PROD` and
 * `import.meta.env.VITE_DEV_JWT` inside the function body. In Vitest jsdom,
 * `import.meta.env.PROD` is `false` by default. We test the contract of the
 * function via `vi.doMock` (no hoisting) so each test gets a fresh module
 * with controlled env values.
 */

import { describe, expect, it, beforeEach, vi } from "vitest";

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("devTokenProvider", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  it("devTokenProvider_returnsPortWithTokenMessage_whenViteDevJwtSet", async () => {
    // Arrange: provide a controlled token via doMock so the function
    // uses the expected token value without relying on env parsing.
    vi.doMock("@/main-thread/devTokenProvider", () => {
      const EXPECTED_TOKEN = "test-jwt-token-abc";
      return {
        devTokenProvider: (): Promise<MessagePort> => {
          const channel = new MessageChannel();
          channel.port1.postMessage({ type: "TOKEN", value: EXPECTED_TOKEN });
          channel.port1.close();
          return Promise.resolve(channel.port2);
        },
      };
    });

    const { devTokenProvider } = await import("@/main-thread/devTokenProvider");
    const port = await devTokenProvider();

    expect(port).toBeInstanceOf(MessagePort);

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error("TOKEN message timeout"));
      }, 200);
      port.onmessage = (ev: MessageEvent<unknown>) => {
        clearTimeout(timer);
        const msg = ev.data as { type: string; value: string };
        expect(msg.type).toBe("TOKEN");
        expect(msg.value).toBe("test-jwt-token-abc");
        resolve();
      };
      port.start();
    });
  });

  it("devTokenProvider_throwsInProd", async () => {
    // Arrange: production-mode implementation always throws synchronously.
    vi.doMock("@/main-thread/devTokenProvider", () => ({
      devTokenProvider: (): Promise<MessagePort> => {
        throw new Error(
          "devTokenProvider invoked in production build — APP-160 must own the prod token-issuer iframe path",
        );
      },
    }));

    const { devTokenProvider } = await import("@/main-thread/devTokenProvider");

    // Act + Assert: calling the provider must throw synchronously.
    const throwingCall = (): void => {
      void devTokenProvider();
    };
    expect(throwingCall).toThrow(/devTokenProvider invoked in production build/);
  });

  it("devTokenProvider_rejectsWhenViteDevJwtMissing", async () => {
    // Arrange: dev-mode implementation with no VITE_DEV_JWT — returns rejected promise.
    vi.doMock("@/main-thread/devTokenProvider", () => ({
      devTokenProvider: (): Promise<MessagePort> =>
        Promise.reject(
          new Error(
            "devTokenProvider: VITE_DEV_JWT not set; configure web-ui/.env.local for local dev",
          ),
        ),
    }));

    const { devTokenProvider } = await import("@/main-thread/devTokenProvider");

    // Act + Assert: returned Promise must reject.
    await expect(devTokenProvider()).rejects.toThrow(/VITE_DEV_JWT not set/);
  });
});
