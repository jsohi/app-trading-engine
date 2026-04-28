/**
 * Aggregator for the local ESLint rules registered as a flat-config
 * plugin. Add rules here when new ones are created.
 */
import noSpanInHotPath from "./no-span-in-hot-path.js";

export default {
  rules: {
    "no-span-in-hot-path": noSpanInHotPath,
  },
};
