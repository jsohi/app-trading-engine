/**
 * Setup for the `storybook` Vitest project (per @storybook/addon-vitest@10).
 *
 * Calls `setProjectAnnotations` with the preview module's annotations so
 * decorators registered in `.storybook/preview.ts` (theme CSS imports,
 * AG Grid `ModuleRegistry` registration) propagate to story-as-test runs.
 * Without this, every blotter story's `play` function would mount AG Grid
 * with no modules registered and fail.
 *
 * @see vitest.config.ts — wires this file into the `storybook` project.
 * @see preview.ts — source of project annotations.
 */
import { setProjectAnnotations } from "@storybook/react-vite";

import * as previewAnnotations from "./preview";

setProjectAnnotations(previewAnnotations);
