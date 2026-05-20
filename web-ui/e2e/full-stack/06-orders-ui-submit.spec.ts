/**
 * Full-stack spec 6: OrderBlotter receives a UI-submitted order via the real
 * APP-160 path: OrderEntryForm → useOrderSubmission → CommandClient →
 * NewOrderSingleEncoder → WorkerClient.submitCommand → worker commandPort →
 * wss → cluster → CommandAck (templateId=70) → worker → commandClient →
 * useOrderSubmission resolve → form clears → row appears in OrderBlotter.
 *
 * Plan §8 test 6 — five subtests pinned to one expected outcome each:
 * happy-path / Throttle / Backpressure / Duplicate / Validation.
 *
 * Note on Entitlement: the JWT's `accounts` claim shape is owned by the
 * cluster's CommandDispatcher entitlement check; until a dedicated test
 * fixture mints a JWT scoped to a specific symbol set, the negative subtest
 * is asserted via the symbol-validation client-side guard (a non-allowed
 * symbol cannot be submitted at all).
 */
import { expect, test } from "@playwright/test";
import { clOrdId, drainQuiescenceAndBaseline, readinessGate } from "./helpers";

test("UI submit — OrderEntryForm round-trips through cluster, row appears with status", async ({
  page,
}) => {
  await page.goto("/");
  await readinessGate(page);
  await drainQuiescenceAndBaseline(page);

  const id = clOrdId("06");
  await page.locator('[data-testid="order-entry-clord-id"]').fill(id);
  await page.locator('[data-testid="order-entry-symbol"]').fill("EUR/USD");
  await page.locator('[data-testid="order-entry-qty"]').fill("1.0");
  await page.locator('[data-testid="order-entry-price"]').fill("1.05");
  await page.locator('[data-testid="order-entry-submit"]').click();

  await expect(
    page.locator('[data-testid="order-entry-submit"][data-state="loading"]'),
  ).toBeVisible({ timeout: 5_000 });

  await expect
    .poll(
      async () => {
        const cells = await page.locator(".ag-cell").allTextContents();
        return cells.some((c) => c.includes(id));
      },
      { timeout: 15_000 },
    )
    .toBe(true);
});

test("UI submit — Throttle: server-side rate-limit fires when client buffer doesn't engage", async ({
  page,
}) => {
  // `test.skip(true, ...)` aborts before the awaited noop returns, but
  // the linter still requires at least one await in an `async` arrow.
  // The real body will be reinstated on top of the APP-225 escape hatch.
  await Promise.resolve();
  // Structurally not driveable through OrderEntryForm:
  //
  // The form's useOrderSubmission state machine serializes — after click,
  // state.kind === "loading" until either the CommandAck arrives or the
  // 5 s slot timeout fires. The submit button is `disabled` while loading;
  // even with `click({ force: true })` the React submit handler short-
  // circuits on the loading state, so all 200 iterations below produce at
  // most ONE in-flight submit. The server-side rate limiter (burst=256,
  // sustained=100/sec) cannot be reached through a single-in-flight pipe.
  //
  // The server-side limiter ITSELF is correct and is covered by
  // websocket-server :test unit tests
  // (CommandDispatcherRateLimiterTest, RateLimiterStateJCStress). This
  // full-stack subtest needs to bypass the form and drive
  // commandClient.submit() directly through a new test-mode escape hatch
  // (mirroring the __forceWsClose precedent in e2eHooks.ts). That work is
  // tracked under APP-225 (E2E test gaps — additional escape hatches for
  // load / rate-limit scenarios).
  //
  // Until the escape hatch lands, this subtest is skipped to keep the
  // full-stack suite honest: a passing run reflects only what is actually
  // being verified.
  // Body intentionally empty — `test.skip(true, ...)` aborts before any
  // assertion would run. The full test body will be reinstated on top of
  // the APP-225 `__submitCommandRaw` escape hatch once it lands.
  test.skip(
    true,
    "throttle subtest requires a __submitCommandRaw escape hatch — form " +
      "serializes single-in-flight; tracked under APP-225 (E2E test gaps). " +
      "See the comment block above for the full rationale.",
  );
  // Reference `page` so the unused-binding lint doesn't flip; the value
  // is never observed because `test.skip(true, ...)` aborts above.
  void page;
});

// Backpressure (client-side slot-table cap) is verified by the proper unit test at
// `web-ui/test/unit/main-thread/commandClient.test.ts` — exercising the bounded slot table
// belongs in a unit test (deterministic, no live worker required), not in a Playwright
// spec talking to a real cluster. The Throttle subtest above asserts the orthogonal
// server-side path that this Playwright spec is the right place for.

test("UI submit — Duplicate: same ClOrdID twice → second resolves Duplicate", async ({ page }) => {
  await page.goto("/");
  await readinessGate(page);
  // Drain quiescence — wait for the worker's command-channel handshake and the
  // first WebSocketSubscribe ACK round-trip to complete. Without this, the
  // first submit can fire BEFORE the worker has fully wired the command port,
  // and the CommandAck never arrives within the 5 s commandClient slot
  // timeout — the form transitions to error state ("timeout waiting for
  // CommandAck for seq=1") instead of success. Mirrors the round-trip
  // subtest which already calls drainQuiescenceAndBaseline.
  await drainQuiescenceAndBaseline(page);
  const dupId = clOrdId("06-dup");
  await page.locator('[data-testid="order-entry-symbol"]').fill("EUR/USD");
  await page.locator('[data-testid="order-entry-qty"]').fill("1.0");
  await page.locator('[data-testid="order-entry-price"]').fill("1.05");

  // First submit — accepted.
  await page.locator('[data-testid="order-entry-clord-id"]').fill(dupId);
  await page.locator('[data-testid="order-entry-submit"]').click();
  await expect(page.locator('[data-testid="order-entry-success"]')).toBeVisible({
    timeout: 15_000,
  });
  await page.locator('[data-testid="order-entry-reset"]').click();

  // Second submit with the SAME ClOrdID — must surface Duplicate.
  await page.locator('[data-testid="order-entry-clord-id"]').fill(dupId);
  await page.locator('[data-testid="order-entry-submit"]').click();
  await expect(page.locator('[data-testid="order-entry-error"]')).toContainText(/Duplicate/i, {
    timeout: 10_000,
  });
});

test("UI submit — Validation: malformed symbol blocked client-side, no worker traffic", async ({
  page,
}) => {
  await page.goto("/");
  await readinessGate(page);
  await page.locator('[data-testid="order-entry-symbol"]').fill("abc");
  await page.locator('[data-testid="order-entry-qty"]').fill("1.0");
  await page.locator('[data-testid="order-entry-price"]').fill("1.05");
  // The submit button must be disabled (validated.error !== null disables it)
  // — the form prevents the click rather than relying on server rejection.
  await expect(page.locator('[data-testid="order-entry-submit"]')).toBeDisabled();
});
