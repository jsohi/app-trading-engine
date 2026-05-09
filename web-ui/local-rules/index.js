/**
 * Aggregator for the local ESLint rules registered as a flat-config
 * plugin. Add rules here when new ones are created.
 *
 * APP-36 added 7 new rules in C1; some are stubs at registration time
 * and gain logic in later commits as the relevant source lands. The
 * registration is stable so eslint.config.js can reference them
 * uniformly throughout the PR sequence.
 */
import noSpanInHotPath from "./no-span-in-hot-path.js";
import noBigintToNumberCoerce from "./no-bigint-to-number-coerce.js";
import noBannedGlobalsInWorker from "./no-banned-globals-in-worker.js";
import noOtelAttributeOutsideAllowlist from "./no-otel-attribute-outside-allowlist.js";
import noPrototypePollutionFromDecoder from "./no-prototype-pollution-from-decoder.js";
import noDevTokenProviderOutsideDev from "./no-dev-token-provider-outside-dev.js";
import noCryptoWithStorageOrExfil from "./no-crypto-with-storage-or-exfil.js";
import requireThreadingAllocationTags from "./require-threading-allocation-tags.js";

export default {
  rules: {
    "no-span-in-hot-path": noSpanInHotPath,
    "no-bigint-to-number-coerce": noBigintToNumberCoerce,
    "no-banned-globals-in-worker": noBannedGlobalsInWorker,
    "no-otel-attribute-outside-allowlist": noOtelAttributeOutsideAllowlist,
    "no-prototype-pollution-from-decoder": noPrototypePollutionFromDecoder,
    "no-dev-token-provider-outside-dev": noDevTokenProviderOutsideDev,
    "no-crypto-with-storage-or-exfil": noCryptoWithStorageOrExfil,
    "require-threading-allocation-tags": requireThreadingAllocationTags,
  },
};
