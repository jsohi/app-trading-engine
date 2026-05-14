/**
 * Purpose: Storybook story for PriceBlotter — verifies the component mounts
 * and receives live price data from fakeStream within 5 seconds.
 *
 * Rationale: preserves the SamplePanel template contract — every registered
 * panel has a play function asserting .ag-root-wrapper mounts and ≥1 row
 * appears.
 *
 * @see PriceBlotter — component under story.
 */
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect } from "storybook/test";

import { PriceBlotter } from "./PriceBlotter";

const meta: Meta<typeof PriceBlotter> = {
  title: "Panels/Quotes/PriceBlotter",
  component: PriceBlotter,
  parameters: {
    layout: "fullscreen",
  },
};

export default meta;
type Story = StoryObj<typeof PriceBlotter>;

export const Default: Story = {
  play: async ({ canvasElement }) => {
    // AG Grid root wrapper mounts synchronously after grid init.
    const wrapper = canvasElement.querySelector(".ag-root-wrapper");
    await expect(wrapper).not.toBeNull();

    // fakeStream emits prices at 250 ms intervals; PriceBlotter projects
    // them via delta-diff. Wait up to 5s for ≥1 row to appear.
    const sleep = (ms: number): Promise<void> =>
      new Promise<void>((r) => {
        setTimeout(r, ms);
      });
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      if (canvasElement.querySelectorAll(".ag-row").length >= 1) break;
      await sleep(200);
    }
    await expect(canvasElement.querySelectorAll(".ag-row").length).toBeGreaterThanOrEqual(1);
  },
};
