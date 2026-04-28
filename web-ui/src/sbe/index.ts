/**
 * Path-alias re-export of the generated SBE TypeScript codecs.
 *
 * Phase 1A ships the workspace seam; APP-34 (1B) replaces the stub
 * `@trading/sbe-codecs/build/generated-ts/index.ts` with the real
 * generated decoders. Phase 2 panels import via `@/sbe/...` instead
 * of the workspace name so the `tsconfig.json` `paths` alias stays
 * the single source of truth for codec resolution — feature tickets
 * don't need to know whether codecs come from the workspace or some
 * future location.
 *
 * Until APP-34 lands, this barrel re-exports an empty namespace
 * (the stub generator emits `export {};`). Once APP-34 ships, every
 * generated decoder + helper + constant becomes available here.
 */
export * from "@trading/sbe-codecs";
