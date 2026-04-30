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
 * Shared constants used by every emitter class to keep the emitted TypeScript consistent across
 * outputs. Avoids the prior chunks-1-through-11 pattern where each emitter privately declared its
 * own {@code private static final String NL = "\n";} — eight identical declarations are a
 * maintenance hazard if the line-ending convention ever needs to change.
 *
 * <h2>Threading model</h2>
 *
 * Constants only — immutable. Safe to share across all emitters in a single build invocation; safe
 * across parallel builds (no mutable state).
 */
final class EmitterConstants {

  /**
   * Line separator used in every emitted TypeScript file. Hardcoded as LF (the only newline
   * convention TypeScript tooling treats as canonical); explicitly NOT {@link
   * System#lineSeparator()} because the generated tree must be byte-identical across Linux / macOS
   * / Windows builds.
   */
  static final String NL = "\n";

  private EmitterConstants() {
    // utility class — not instantiable
  }
}
