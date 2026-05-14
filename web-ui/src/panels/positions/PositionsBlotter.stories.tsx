/**
 * Purpose: Storybook story for PositionsBlotter — verifies the component
 * mounts and receives fill-derived position data within 5 seconds.
 *
 * Rationale: preserves the SamplePanel template contract — every registered
 * panel has a play function asserting .ag-root-wrapper mounts and ≥1 row
 * appears.
 *
 * @see PositionsBlotter — component under story.
 */
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect } from "storybook/test";

import { PositionsBlotter } from "./PositionsBlotter";

const meta: Meta<typeof PositionsBlotter> = {
  title: "Panels/Positions/PositionsBlotter",
  component: PositionsBlotter,
  parameters: {
    layout: "fullscreen",
  },
};

export default meta;
type Story = StoryObj<typeof PositionsBlotter>;

export const Default: Story = {
  play: async ({ canvasElement }) => {
    // AG Grid root wrapper mounts synchronously after grid init.
    const wrapper = canvasElement.querySelector(".ag-root-wrapper");
    await expect(wrapper).not.toBeNull();

    // fakeStream emits fills at 250 ms intervals; positionStream aggregates
    // them into rows. Wait up to 5s for ≥1 row to appear.
    const sleep = (ms: number): Promise<void> =>
      new Promise<void>((r) => {
        setTimeout(r, ms);
      });
    const deadline = performance.now() + 5_000;
    while (performance.now() < deadline) {
      if (canvasElement.querySelectorAll(".ag-row").length >= 1) break;
      await sleep(200);
    }
    await expect(canvasElement.querySelectorAll(".ag-row").length).toBeGreaterThanOrEqual(1);
  },
};
