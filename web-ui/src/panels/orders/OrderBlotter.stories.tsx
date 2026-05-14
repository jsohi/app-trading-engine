/**
 * Purpose: Storybook story for OrderBlotter — verifies the component mounts
 * successfully and receives live data from fakeStream within 5 seconds.
 *
 * Rationale: preserves the SamplePanel template contract (every registered
 * panel must have a story with a play function asserting ≥1 row visible).
 *
 * @see OrderBlotter — component under story.
 */
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect } from "storybook/test";

import { OrderBlotter } from "./OrderBlotter";

const meta: Meta<typeof OrderBlotter> = {
  title: "Panels/Orders/OrderBlotter",
  component: OrderBlotter,
  parameters: {
    layout: "fullscreen",
  },
};

export default meta;
type Story = StoryObj<typeof OrderBlotter>;

export const Default: Story = {
  play: async ({ canvasElement }) => {
    // AG Grid renders the root wrapper even without rows.
    const wrapper = canvasElement.querySelector(".ag-root-wrapper");
    await expect(wrapper).not.toBeNull();

    // fakeStream emits at 250 ms intervals; within 5s we should have ≥1 row.
    const deadline = Date.now() + 5_000;
    const sleep = (ms: number): Promise<void> =>
      new Promise<void>((r) => {
        setTimeout(r, ms);
      });
    while (Date.now() < deadline) {
      const rows = canvasElement.querySelectorAll(".ag-row");
      if (rows.length >= 1) break;
      await sleep(200);
    }
    const rowCount = canvasElement.querySelectorAll(".ag-row").length;
    await expect(rowCount).toBeGreaterThanOrEqual(1);
  },
};
