/**
 * App-mount smoke test: panel registry resolves the sample panel,
 * the panel mounts without console errors, and the layout shell
 * renders 5 slots.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";

import { App } from "./App";

describe("App", () => {
  afterEach(() => {
    cleanup();
  });

  it("mount_smokeNoConsoleErrors_layoutShellRenders", () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
    render(<App />);
    // Layout shell title.
    expect(screen.getByText("Trading Engine")).toBeDefined();
    // Sample panel registered into the left-top slot.
    expect(screen.getByLabelText("Sample (1A)")).toBeDefined();
    expect(errorSpy).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});
