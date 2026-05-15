/**
 * Path-alias re-export of the generated SBE TypeScript codecs.
 *
 * <p>Project-wide convention: panels and main-thread modules import via
 * {@code @/sbe/...} instead of the workspace package name, so the
 * {@code tsconfig.json} {@code paths} alias is the single source of truth
 * for codec resolution. Feature tickets do not need to know whether the
 * generated codecs come from the workspace or any future location.
 *
 * <p>The {@code :sbe-typescript-generator} module emits one decoder per SBE
 * message into {@code sbe-typescript-generator/build/generated-ts/} (88 files
 * at last count) — this barrel re-exports every public symbol.
 *
 * <p>Hand-written encoders live under {@code @/sbe/encoders/} (currently
 * {@link ../encoders/NewOrderSingleEncoder}) — they share the SBE wire layout
 * with the generated decoders and are validated by round-trip tests.
 */
export * from "@trading/sbe-codecs";
