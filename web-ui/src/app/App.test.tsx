/**
 * App-mount smoke test: panel registry resolves the blotter panels and
 * ConnectionIndicator, they mount without console errors, and the layout
 * shell renders with correct aria-labels for all three blotter slots.
 */
import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { EMPTY } from "rxjs";

// ── Hoisted mocks (vi.mock is hoisted before imports) ─────────────────────

// Stub connectionStore so ConnectionIndicator doesn't subscribe to the real
// BehaviorSubject — which would cause teardown issues in the error-spy check.
vi.mock("@/stores/connection-store", () => ({
  connectionStore: {
    subscribe: () => (): void => undefined,
    getSnapshot: () => "CONNECTED" as const,
  },
  __resetConnectionStoreForTests: (): void => undefined,
}));

// Stub messageSource so blotters don't subscribe to fakeStream during the
// smoke test. messages$ is an Observable that immediately completes; panels
// subscribe and get no emissions (which is fine — no rows, no error).
vi.mock("@/main-thread/messageSource", () => ({
  messages$: EMPTY,
  startMessageSource: (): void => undefined,
  __resetMessageSourceForTests: (): void => undefined,
}));

// Stub ag-grid-react so the blotters mount without ResizeObserver / real DOM.
vi.mock("ag-grid-react", () => ({
  AgGridReact: (): null => null,
}));

import { App } from "./App";

describe("App", () => {
  it("mount_smokeNoConsoleErrors_layoutShellRenders", () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation((): void => undefined);
    render(<App />);

    // Layout shell title.
    expect(screen.getByText("Trading Engine")).toBeDefined();

    // Blotter panels registered into left-top, left-bottom, right-middle.
    expect(screen.getByLabelText("Orders")).toBeDefined();
    expect(screen.getByLabelText("Positions")).toBeDefined();
    expect(screen.getByLabelText("Quotes")).toBeDefined();

    // ConnectionIndicator in top-bar slot: .conn-dot is rendered with role="status".
    expect(screen.getByRole("status")).toBeDefined();

    expect(errorSpy).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});
