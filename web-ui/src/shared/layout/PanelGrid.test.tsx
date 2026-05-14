/**
 * Purpose: PanelGrid layout snapshot test — verifies the 5 grid slots render
 *   their `unregistered` chrome when empty, registered grid panels render
 *   their component body in the correct slot, and the new `top-bar` slot
 *   (APP-37) renders inside the existing `<header className="app-header">`.
 *
 * Rationale: APP-37 extended `PanelSlot` with `'top-bar'`. The new slot
 *   renders nothing when empty (NOT an "unregistered" placeholder) so the
 *   5-grid-slot count is unchanged for the empty-grid case.
 *
 * @see ./PanelGrid.tsx — system under test.
 * @see ../../app/panelRegistry.ts — peer (PanelSlot union, slot ordering).
 */
import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";

import { PanelGrid } from "./PanelGrid";
import type { PanelRegistration } from "@/app/panelRegistry";

// Stub connectionStore in case any registered panel uses it.
vi.mock("@/stores/connection-store", () => ({
  connectionStore: {
    subscribe: () => (): void => undefined,
    getSnapshot: () => "CONNECTED" as const,
  },
  __resetConnectionStoreForTests: (): void => undefined,
}));

describe("PanelGrid", () => {
  it("render_emptyPanels_showsAllFiveSlotsUnregistered", () => {
    render(<PanelGrid panels={[]} />);
    // Top-bar slot renders nothing when empty (TopBarSlot returns <></>).
    // The 5 grid slots (left-top, left-bottom, right-top, right-middle,
    // right-bottom) each render "unregistered" when no panel is registered.
    expect(screen.getAllByText("unregistered")).toHaveLength(5);
  });

  it("render_registeredPanel_invokesComponentInCorrectSlot", () => {
    const registration: PanelRegistration = {
      id: "test",
      title: "Test Panel",
      slot: "right-top",
      component: () => <div data-testid="test-body">hello</div>,
    };
    render(<PanelGrid panels={[registration]} />);
    expect(screen.getByTestId("test-body")).toBeDefined();
    expect(screen.getByLabelText("Test Panel")).toBeDefined();
  });

  it("render_topBarPanel_rendersInHeader", () => {
    // A tiny inline component stands in for ConnectionIndicator.
    const TopBarPanel = () => <span data-testid="top-bar-content">live</span>;

    const registration: PanelRegistration = {
      id: "connection",
      title: "Status",
      slot: "top-bar",
      component: TopBarPanel,
    };

    const { container } = render(<PanelGrid panels={[registration]} />);

    // The content must appear inside <header className="app-header">.
    const header = container.querySelector("header.app-header");
    expect(header).not.toBeNull();
    expect(header!.querySelector("[data-testid='top-bar-content']")).not.toBeNull();

    // aria-label comes from the TopBarSlot wrapper div.
    expect(screen.getByLabelText("Status")).toBeDefined();
  });
});
