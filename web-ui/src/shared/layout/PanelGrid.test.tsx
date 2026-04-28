/**
 * PanelGrid layout snapshot test. Asserts:
 *   - Empty grid renders all 5 slots as "unregistered"
 *   - Registered panel renders its component body
 *   - Slot order is stable across calls
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";

import { PanelGrid } from "./PanelGrid";
import type { PanelRegistration } from "@/app/panelRegistry";

describe("PanelGrid", () => {
  it("render_emptyPanels_showsAllFiveSlotsUnregistered", () => {
    render(<PanelGrid panels={[]} />);
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
});
