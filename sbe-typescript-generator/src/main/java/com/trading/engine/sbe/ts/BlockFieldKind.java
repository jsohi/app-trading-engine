/*
 * Copyright 2026 Jasandeep Singh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.trading.engine.sbe.ts;

/**
 * Dispatch tag for {@link BlockField} getter emission. Replaces the chunk-5 private nested {@code
 * MessageGenerator.FieldKind}; lifted to package scope so {@link GroupGenerator} can pattern-match
 * on the same value space without duplicating the enum.
 */
enum BlockFieldKind {
  /**
   * Bare primitive (int8/16/32/64, uint8/16/32/64, float, double). Returns {@code number} or {@code
   * bigint}.
   */
  PRIMITIVE,
  /** Schema-declared {@code <enum>} reference. Returns the imported enum union type. */
  ENUM,
  /**
   * Fixed-length char array (e.g. {@code <type primitiveType="char" length="20"/>}). Returns {@code
   * string}.
   */
  CHAR_ARRAY,
  /**
   * SBE {@code uuid} composite — two {@code int64} halves ({@code mostSignificantBits} + {@code
   * leastSignificantBits}). Returns the {@code UuidValue} interface ({@code { msb: bigint; lsb:
   * bigint }}) exported from {@code _codecRuntime.ts}. Stringification is intentionally deferred to
   * the consumer's render edge — see {@link UuidCompositeGenerator}.
   */
  UUID_COMPOSITE
}
