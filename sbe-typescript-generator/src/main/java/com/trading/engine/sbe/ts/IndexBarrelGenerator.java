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

import static com.trading.engine.sbe.ts.EmitterConstants.NL;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.Signal;
import uk.co.real_logic.sbe.ir.Token;

/**
 * Emits the workspace barrel ({@code build/generated-ts/index.ts}) — the single re-export entry
 * point consumed by APP-40 via the {@code @trading/sbe-codecs} workspace.
 *
 * <h2>Output shape</h2>
 *
 * Re-exports, alphabetised by symbol name within each section for stable cross-build output:
 *
 * <ul>
 *   <li>Every per-message {@code <Name>Decoder}.
 *   <li>Every per-enum {@code <Name>Enum} value (which TypeScript co-exports as a type alias).
 *   <li>Per-enum {@code <Name>Enum_NULL_VAL} sentinel — emitted ONLY when at least one optional
 *       field across the entire schema references that enum.
 *   <li>{@code MessageHeaderDecoder} from the header module.
 *   <li>{@code toFixed8 / parseFixed8 / nanosToDate} from {@code helpers.js}.
 *   <li>{@code PRICE_SCALE / SCHEMA_ID / SCHEMA_VERSION} from {@code constants.js}.
 *   <li>{@code route} + {@code type DecodedFrame} + {@code type Decoder} from {@code
 *       MessageRouter.js}.
 *   <li>{@code type UuidValue} from {@code _codecRuntime.js} — emitted ONLY when at least one
 *       schema field has the uuid composite. Skipped otherwise (forward-proof for a future schema
 *       with no uuid fields).
 * </ul>
 *
 * <h2>Whole-schema scan</h2>
 *
 * The conditional NULL_VAL and {@code UuidValue} re-exports require knowing which optional-enum
 * names appear anywhere in the schema and whether any uuid composite is used. This emitter walks
 * {@link Ir#messages()} once, recursing through groups via the same {@link
 * BlockField#parseBlockField} machinery the message emitter uses. The scan is intentionally local
 * to this class — it does NOT call back into {@link MessageGenerator}'s per-message helpers, so a
 * future emitter refactor that removes those private helpers cannot silently break the barrel.
 *
 * <h2>Internal-helper hiding</h2>
 *
 * {@code _codecRuntime.ts} exports two symbols: {@code readFixedString} (internal — never
 * re-exported) and {@code UuidValue} (consumer-visible type, conditionally re-exported here). The
 * leading underscore on {@code _codecRuntime.ts} signals internal status; the barrel codifies it. A
 * future ESLint {@code no-restricted-imports} rule banning consumer-side {@code _codecRuntime.js}
 * imports is the next layer of enforcement, deferred to a follow-up ticket.
 *
 * <h2>JSDoc forwarding</h2>
 *
 * TypeScript's {@code export { X } from "./y.js"} forwards JSDoc to IDE tooling and the generated
 * {@code .d.ts} declarations by default. Chunk-13 {@code
 * RoundTripIT.tsdocPropagation_routeJsDocVisibleInBarrel} locks this against silent regression by
 * greppping the emitted {@code index.d.ts} for the locked "MUST consume" phrase from {@code
 * RouterGenerator}'s {@code route()} JSDoc.
 *
 * <h2>Sort discipline</h2>
 *
 * All decoder + enum lists are sorted via {@link Comparator#naturalOrder()} regardless of the input
 * ordering. {@link MessageGenerator#generate} returns names in schema-declaration order; {@link
 * EnumGenerator#generate} likewise. Sorting here removes any dependency on caller order.
 *
 * <h2>Threading model</h2>
 *
 * Not thread-safe. Build-time only.
 *
 * <h2>Allocation behavior</h2>
 *
 * Build-time only — allocates freely while constructing emitted source.
 *
 * @see MessageGenerator
 * @see EnumGenerator
 * @see RouterGenerator
 * @see HelpersGenerator
 * @see ConstantsGenerator
 * @see TypeScriptCodeGenerator
 */
final class IndexBarrelGenerator {

  /** Filename for the workspace barrel. */
  static final String INDEX_FILENAME = "index.ts";

  /** Constructor — no state. */
  IndexBarrelGenerator() {
    // intentionally empty
  }

  /**
   * Emit {@code index.ts} with re-exports for every workspace symbol.
   *
   * @param ir parsed SBE intermediate representation; used for the whole-schema scan and the
   *     schema-identity banner
   * @param messageDecoderNames list of generated decoder class names (typically schema-declaration
   *     order — sorted alphabetically here)
   * @param enumNames list of generated enum names (typically schema-declaration order — sorted
   *     alphabetically here)
   * @param outputDir absolute output directory, already created by the orchestrator
   * @throws IOException if the file cannot be written
   * @throws NullPointerException if any argument is null
   */
  void generate(
      final Ir ir,
      final List<String> messageDecoderNames,
      final List<String> enumNames,
      final Path outputDir)
      throws IOException {
    Objects.requireNonNull(ir, "ir");
    Objects.requireNonNull(messageDecoderNames, "messageDecoderNames");
    Objects.requireNonNull(enumNames, "enumNames");
    Objects.requireNonNull(outputDir, "outputDir");

    final var sortedDecoders = sorted(messageDecoderNames);
    final var sortedEnums = sorted(enumNames);
    final var optionalEnums = scanOptionalEnumUsage(ir);
    final boolean usesUuidComposite = scanUsesUuidComposite(ir);

    final var sb = new StringBuilder(16_384);
    sb.append("// AUTO-GENERATED by :sbe-typescript-generator:generateTsCodecs (APP-34).")
        .append(NL)
        .append("// Workspace barrel — single import surface for @trading/sbe-codecs. Do not edit.")
        .append(NL)
        .append("//")
        .append(NL);
    // Skip the namespace line entirely when the schema declares no package=, rather than
    // emitting "Schema package: " with a dangling empty value. Today's schema declares the
    // attribute, so the line is always present; the conditional is forward-proofing.
    final var namespace = ir.applicableNamespace();
    if (namespace != null && !namespace.isBlank()) {
      sb.append("// Schema package: ").append(namespace).append(NL);
    }
    sb.append("// Schema id     : ")
        .append(ir.id())
        .append(NL)
        .append("// Schema version: ")
        .append(ir.version())
        .append(NL)
        .append(NL);

    // Decoders (alphabetised) — concrete classes; single value re-export covers ctor + type usage.
    for (final var name : sortedDecoders) {
      sb.append("export { ")
          .append(name)
          .append(" } from \"./")
          .append(name)
          .append(".js\";")
          .append(NL);
    }
    if (!sortedDecoders.isEmpty()) {
      sb.append(NL);
    }

    // Enums (alphabetised). Single value re-export covers BOTH the as-const object and the
    // identically-named type alias from the source module — TypeScript treats values and types as
    // separate namespaces under one name. The _NULL_VAL sentinel re-exports only when at least one
    // optional field references the enum (avoids unused-import warnings under
    // verbatimModuleSyntax).
    for (final var name : sortedEnums) {
      if (optionalEnums.contains(name)) {
        sb.append("export { ")
            .append(name)
            .append(", ")
            .append(name)
            .append("_NULL_VAL } from \"./")
            .append(name)
            .append(".js\";")
            .append(NL);
      } else {
        sb.append("export { ")
            .append(name)
            .append(" } from \"./")
            .append(name)
            .append(".js\";")
            .append(NL);
      }
    }
    if (!sortedEnums.isEmpty()) {
      sb.append(NL);
    }

    // Header + helpers + constants + router — fixed order; pinned for ergonomic IDE listing.
    sb.append("export { MessageHeaderDecoder } from \"./messageHeader.js\";").append(NL);
    sb.append("export { toFixed8, parseFixed8, nanosToDate } from \"./helpers.js\";").append(NL);
    sb.append("export { PRICE_SCALE, SCHEMA_ID, SCHEMA_VERSION } from \"./constants.js\";")
        .append(NL);
    sb.append("export { route, type DecodedFrame, type Decoder } from \"./MessageRouter.js\";")
        .append(NL);

    // UuidValue re-export only when at least one schema field uses the uuid composite. The
    // interface is emitted into _codecRuntime.ts unconditionally today (it is a small static
    // shape) but the barrel-level re-export gates on usage so the consumer surface stays clean
    // when a future schema has zero uuid fields. Blank-line separator visually segments this
    // conditional block from the fixed-format re-exports above.
    if (usesUuidComposite) {
      sb.append(NL).append("export type { UuidValue } from \"./_codecRuntime.js\";").append(NL);
    }

    Files.writeString(outputDir.resolve(INDEX_FILENAME), sb, StandardCharsets.UTF_8);
  }

  // -----------------------------------------------------------------------------------------
  // Whole-schema scans — performed once per build; do NOT call MessageGenerator helpers
  // -----------------------------------------------------------------------------------------

  /**
   * Build the set of enum names that appear at least once with {@code presence="optional"} across
   * the whole schema (root blocks + group record blocks, recursive). Reuses {@link
   * BlockField#parseBlockField} so the scan stays in lock-step with the per-message emitter's
   * detection logic.
   */
  private static Set<String> scanOptionalEnumUsage(final Ir ir) {
    final var optionalEnums = new TreeSet<String>();
    for (final var messageTokens : ir.messages()) {
      if (messageTokens.isEmpty()) {
        continue;
      }
      collectOptionalEnumsFromBlock(messageTokens, optionalEnums);
    }
    return optionalEnums;
  }

  /**
   * Walk message tokens — root block fields plus a recursion into every nested group — and add any
   * optional-enum field's enum name to {@code sink}.
   */
  private static void collectOptionalEnumsFromBlock(
      final List<Token> tokens, final Set<String> sink) {
    int i = 1; // skip BEGIN_MESSAGE
    while (i < tokens.size()) {
      final var token = tokens.get(i);
      switch (token.signal()) {
        case BEGIN_FIELD -> {
          final var inner = tokens.get(i + 1);
          if (inner.signal() != Signal.BEGIN_COMPOSITE) {
            final var field = BlockField.parseBlockField(token, inner);
            if (field.kind() == BlockFieldKind.ENUM && field.optional()) {
              sink.add(field.enumName());
            }
          }
          i += token.componentTokenCount();
        }
        case BEGIN_GROUP -> {
          collectOptionalEnumsFromGroup(tokens, i, sink);
          i += token.componentTokenCount();
        }
        case BEGIN_VAR_DATA -> i += token.componentTokenCount();
        case END_MESSAGE -> {
          return;
        }
        default -> i++;
      }
    }
  }

  /**
   * Walk a single group's record-block fields plus any nested groups, recursively. {@code
   * groupBeginIndex} points at the {@code BEGIN_GROUP} token; the walk runs until the matching
   * {@code END_GROUP}.
   *
   * <p><b>Termination strategy (locked):</b> outer walks (block-level) skip whole groups via {@link
   * Token#componentTokenCount()}; inner walks (this method + {@link #groupHasUuidComposite})
   * descend into the group body and terminate on END_GROUP because {@code componentTokenCount} on
   * BEGIN_GROUP would skip past the body entirely. Both strategies coexist by design.
   */
  private static void collectOptionalEnumsFromGroup(
      final List<Token> tokens, final int groupBeginIndex, final Set<String> sink) {
    // Skip BEGIN_GROUP and the dimension composite (groupSizeEncoding); the dimension token's
    // componentTokenCount tells us how many tokens to skip.
    int i = groupBeginIndex + 1;
    final var dimension = tokens.get(i);
    i += dimension.componentTokenCount();
    while (i < tokens.size()) {
      final var token = tokens.get(i);
      switch (token.signal()) {
        case END_GROUP -> {
          return;
        }
        case BEGIN_FIELD -> {
          final var inner = tokens.get(i + 1);
          if (inner.signal() != Signal.BEGIN_COMPOSITE) {
            final var field = BlockField.parseBlockField(token, inner);
            if (field.kind() == BlockFieldKind.ENUM && field.optional()) {
              sink.add(field.enumName());
            }
          }
          i += token.componentTokenCount();
        }
        case BEGIN_GROUP -> {
          collectOptionalEnumsFromGroup(tokens, i, sink);
          i += token.componentTokenCount();
        }
        case BEGIN_VAR_DATA -> i += token.componentTokenCount();
        default -> i++;
      }
    }
  }

  /** True if any field anywhere in the schema is a {@code uuid} composite. */
  private static boolean scanUsesUuidComposite(final Ir ir) {
    for (final var messageTokens : ir.messages()) {
      if (blockHasUuidComposite(messageTokens)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Walk root + nested-group fields looking for any {@code BEGIN_COMPOSITE} with name {@code
   * "uuid"}.
   */
  private static boolean blockHasUuidComposite(final List<Token> tokens) {
    int i = 1;
    while (i < tokens.size()) {
      final var token = tokens.get(i);
      switch (token.signal()) {
        case BEGIN_FIELD -> {
          final var inner = tokens.get(i + 1);
          if (inner.signal() == Signal.BEGIN_COMPOSITE
              && UuidCompositeGenerator.UUID_TYPE_NAME.equals(inner.applicableTypeName())) {
            return true;
          }
          i += token.componentTokenCount();
        }
        case BEGIN_GROUP -> {
          if (groupHasUuidComposite(tokens, i)) {
            return true;
          }
          i += token.componentTokenCount();
        }
        case BEGIN_VAR_DATA -> i += token.componentTokenCount();
        case END_MESSAGE -> {
          return false;
        }
        default -> i++;
      }
    }
    return false;
  }

  private static boolean groupHasUuidComposite(
      final List<Token> tokens, final int groupBeginIndex) {
    int i = groupBeginIndex + 1;
    final var dimension = tokens.get(i);
    i += dimension.componentTokenCount();
    while (i < tokens.size()) {
      final var token = tokens.get(i);
      switch (token.signal()) {
        case END_GROUP -> {
          return false;
        }
        case BEGIN_FIELD -> {
          final var inner = tokens.get(i + 1);
          if (inner.signal() == Signal.BEGIN_COMPOSITE
              && UuidCompositeGenerator.UUID_TYPE_NAME.equals(inner.applicableTypeName())) {
            return true;
          }
          i += token.componentTokenCount();
        }
        case BEGIN_GROUP -> {
          if (groupHasUuidComposite(tokens, i)) {
            return true;
          }
          i += token.componentTokenCount();
        }
        case BEGIN_VAR_DATA -> i += token.componentTokenCount();
        default -> i++;
      }
    }
    return false;
  }

  /**
   * De-dupe and sort with {@link Comparator#naturalOrder()} (case-sensitive — all current schema
   * names are PascalCase, so ASCII order matches semantic order). {@link TreeSet} dedupes and sorts
   * in one pass.
   */
  private static List<String> sorted(final List<String> input) {
    return List.copyOf(new TreeSet<>(input));
  }
}
