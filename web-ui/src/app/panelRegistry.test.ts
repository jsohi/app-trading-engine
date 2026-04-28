import { describe, expect, it, beforeEach } from "vitest";
import {
  __clearPanelRegistryForTests,
  collectPanels,
  registerPanel,
  type PanelRegistration,
} from "@/app/panelRegistry";

const stub = (id: string, slot: PanelRegistration["slot"]): PanelRegistration => ({
  id,
  title: id,
  slot,
  component: () => null,
});

describe("panelRegistry", () => {
  beforeEach(() => {
    __clearPanelRegistryForTests();
  });

  it("registers a single panel", () => {
    registerPanel(stub("orders", "left-top"));
    expect(collectPanels().map((p) => p.id)).toEqual(["orders"]);
  });

  it("replaces an existing registration with the same id (HMR-friendly)", () => {
    registerPanel(stub("orders", "left-top"));
    // Simulate Vite HMR: same id, fresh object literal — must not throw.
    const reloaded = stub("orders", "left-top");
    registerPanel(reloaded);
    const all = collectPanels();
    expect(all).toHaveLength(1);
    expect(all[0]).toBe(reloaded);
  });

  it("throws when two different panels target the same slot", () => {
    registerPanel(stub("orders", "left-top"));
    expect(() => {
      registerPanel(stub("positions", "left-top"));
    }).toThrow(/Panel slot collision/);
  });

  it("allows panels in different slots", () => {
    registerPanel(stub("orders", "left-top"));
    registerPanel(stub("positions", "left-bottom"));
    registerPanel(stub("rfq", "right-top"));
    expect(
      collectPanels()
        .map((p) => p.id)
        .sort(),
    ).toEqual(["orders", "positions", "rfq"]);
  });

  it("sorts by slot order then id", () => {
    registerPanel(stub("z-events", "right-bottom"));
    registerPanel(stub("a-rfq", "right-top"));
    registerPanel(stub("orders", "left-top"));
    expect(collectPanels().map((p) => p.id)).toEqual(["orders", "a-rfq", "z-events"]);
  });
});
