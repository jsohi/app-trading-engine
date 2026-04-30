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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import uk.co.real_logic.sbe.PrimitiveType;
import uk.co.real_logic.sbe.PrimitiveValue;
import uk.co.real_logic.sbe.ir.Encoding;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.Signal;
import uk.co.real_logic.sbe.ir.Token;

/**
 * Emits one TypeScript decoder per SBE message: {@code <messageName>Decoder.ts}.
 *
 * <h2>Output shape</h2>
 *
 * For each message in {@link Ir#messages()} this emitter writes a {@code DataView}-flyweight
 * decoder class with:
 *
 * <ul>
 *   <li>{@code TEMPLATE_ID} / {@code BLOCK_LENGTH} / {@code SCHEMA_ID} / {@code SCHEMA_VERSION}
 *       static readonly constants — used by the future {@code MessageRouter} (chunk 9) for
 *       templateId dispatch and to size the root-block read.
 *   <li>{@code wrap(buffer, offset)} — binds the flyweight to a byte slice; returns {@code this} so
 *       callers can chain a getter call on the construction expression.
 *   <li>{@code encodedLength()} — returns the root-block length; repeating groups and var-data
 *       (when present) extend the message past this point and are exposed by accessors landing in
 *       chunks 6/7.
 *   <li>One getter per root-block field, dispatching on the SBE token signal:
 *       <ul>
 *         <li>{@code Signal.ENCODING} primitive → {@code DataView.get<Width>} call. {@code int64} /
 *             {@code uint64} return {@code bigint} per CLAUDE.md "int64/uint64 → bigint (avoids
 *             2^53 precision loss)"; narrower integers fit safely in {@code number}.
 *         <li>{@code Signal.ENCODING} char-array (length &gt; 1) → null-padded fixed-length string
 *             via {@code readFixedString} from {@code _codecRuntime.ts}.
 *         <li>{@code Signal.BEGIN_ENUM} → {@code DataView.get<Width>} cast to the imported enum
 *             union type. Optional-presence enums return {@code <Enum> | null} with the schema's
 *             {@code nullValue=...} sentinel honoured (or the SBE-default sentinel otherwise).
 *         <li>{@code Signal.BEGIN_COMPOSITE} (e.g. the {@code uuid} two-int64 halves) → deferred to
 *             chunk 7's {@code UuidCompositeGenerator}; this chunk emits a stub-comment placeholder
 *             so the field's offset stays documented.
 *       </ul>
 *   <li>For optional primitive fields, the getter wraps the raw read with a sentinel comparison:
 *       {@code int64} → {@code Long.MIN_VALUE} ({@code -9223372036854775808n}); {@code uint64} →
 *       {@code 0xFFFFFFFFFFFFFFFFn}; narrower widths use the SBE-default sentinel for that
 *       primitive (e.g. {@code 255} for {@code uint8}, {@code Integer.MIN_VALUE} for {@code
 *       int32}). Source of the sentinel is {@link Encoding#applicableNullValue()} so an explicit
 *       schema {@code nullValue=...} attribute (e.g. on {@code
 *       QuoteRejectReasonEnum.nullValue="255"}) overrides the primitive default.
 * </ul>
 *
 * <h2>Repeating groups</h2>
 *
 * Repeating groups are handled by {@link GroupGenerator} per chunk 6. Each {@code <group>} declared
 * inside a message becomes one or more iterator classes appended to the same emitted {@code .ts}
 * file (one class per group, plus one per nested group recursively for cases like {@code
 * RfqStateSnapshot.noRfqs.noLegs}). The cursor model uses a shared {@code _limit} field on the
 * message decoder; group iterators read/advance it via the package-internal {@code _getBuffer()} /
 * {@code _getLimit()} / {@code _setLimit(...)} accessors emitted on the message class. Iteration
 * uses {@code count()} / {@code hasNext()} / {@code next()} returning {@code this}, mirroring
 * Aeron's Java SBE codec idiom; no per-iteration allocation occurs.
 *
 * <h2>Var-data fields</h2>
 *
 * {@code <data type="varDataEncoding">} fields are handled by {@link VarDataGenerator} per chunk 6.
 * Each var-data field becomes a {@code Uint8Array}-returning getter that consumes its uint32 length
 * prefix from the wire on first call, advances {@code _limit} past the data bytes, and caches the
 * resulting view so subsequent calls within the same {@code wrap()} are idempotent. Multi-var-data
 * messages require getters to be called in declaration order; out-of-order calls are undefined
 * behavior. The returned view is invalidated by the next {@code wrap()} of the producing decoder.
 *
 * <h2>Composite (uuid) fields — deferred to chunk 7</h2>
 *
 * Composite-typed root fields (only the {@code uuid} composite today; used by {@code
 * WebSocketAuthAck.sessionId}, {@code SessionResume.sessionId}, {@code
 * WebSocketSnapshot.snapshotId}) are still skipped at chunk 6 and surfaced as a trailing TODO
 * comment on each affected decoder so the offset stays documented for chunk 7's {@code
 * UuidCompositeGenerator}.
 *
 * <h2>Shared runtime</h2>
 *
 * One additional file is written per build: {@code _codecRuntime.ts}, exporting {@code
 * readFixedString(buffer, offset, length)} for null-padded char-array decoding. The leading
 * underscore marks it as an internal helper module — consumers should import message decoders by
 * name via the index barrel, not pull from {@code _codecRuntime.ts} directly.
 *
 * <h2>Threading model</h2>
 *
 * Not thread-safe. Build-time only. Each {@link #generate(Ir, Path)} invocation runs single-
 * threaded inside the SBE-tool JVM.
 *
 * <h2>Allocation behavior</h2>
 *
 * Build-time only — allocates freely while constructing emitted source.
 *
 * <h2>Design rationale</h2>
 *
 * The flyweight pattern (a single decoder instance bound to a buffer slice via {@code wrap})
 * matches Aeron/SBE's Java decoder idiom and keeps per-message allocation at zero on the consumer
 * side once the decoder is constructed. Returning {@code this} from {@code wrap} permits the {@code
 * new <Name>Decoder().wrap(buf, off).fieldName()} chained-call idiom that the SBE Java codec also
 * exposes.
 *
 * @see TypeScriptTargetCodeGenerator
 * @see HeaderGenerator
 * @see EnumGenerator
 */
final class MessageGenerator {

  /** Filename for the shared decoder runtime emitted alongside the per-message files. */
  static final String CODEC_RUNTIME_FILENAME = "_codecRuntime.ts";

  /** Newline used in emitted TypeScript. */
  private static final String NL = "\n";

  /**
   * Walk all messages in {@code ir} and emit one decoder per message plus the shared {@code
   * _codecRuntime.ts} helper.
   *
   * @param ir parsed SBE intermediate representation; must not be {@code null}
   * @param outputDir absolute output directory, already created by the caller; must not be {@code
   *     null}
   * @return ordered list of decoder class names that were emitted (used by the index barrel
   *     re-exports). Element order matches the schema's message-declaration order so the barrel
   *     listing is deterministic across builds.
   * @throws IOException if any output file cannot be written
   * @throws IllegalStateException if a field uses a schema construct this emitter does not yet
   *     support (e.g. a {@code BEGIN_SET} bitset, which is not declared in {@code
   *     trading-schema.xml})
   */
  List<String> generate(final Ir ir, final Path outputDir) throws IOException {
    Objects.requireNonNull(ir, "ir");
    Objects.requireNonNull(outputDir, "outputDir");

    writeCodecRuntime(outputDir);

    final var emitted = new ArrayList<String>();
    for (final var messageTokens : ir.messages()) {
      if (messageTokens.isEmpty()) {
        continue;
      }
      emitted.add(emitMessage(messageTokens, ir, outputDir));
    }
    return List.copyOf(emitted);
  }

  /** Write the shared {@code _codecRuntime.ts} once per build. */
  private static void writeCodecRuntime(final Path outputDir) throws IOException {
    final var sb = new StringBuilder(1_024);
    sb.append("// AUTO-GENERATED by :sbe-typescript-generator:generateTsCodecs (APP-34).")
        .append(NL)
        .append("// Internal runtime helpers shared across generated decoders. Do not edit.")
        .append(NL)
        .append("//")
        .append(NL)
        .append("// Not part of the public surface — consumers should import named decoders")
        .append(NL)
        .append("// from the package barrel rather than reaching into this module.")
        .append(NL)
        .append(NL)
        // Single shared TextDecoder — TextDecoder construction is non-trivial and the
        // generated decoders are called many times per second by the websocket
        // ingest path; one shared instance avoids per-call allocation.
        //
        // fatal: true so malformed UTF-8 throws at decode time instead of silently
        // substituting U+FFFD. SBE char fields in trading-schema.xml carry ASCII
        // identifiers (FX symbols, account codes, FIX strings); a non-UTF-8 byte means
        // wire corruption or a producer bug, so surface it loudly rather than swallow it.
        .append(
            "const TEXT_DECODER = new TextDecoder(\"utf-8\", { fatal: true, ignoreBOM: false });")
        .append(NL)
        .append(NL)
        .append("/**")
        .append(NL)
        .append(" * Read a fixed-length null-padded char array as a string.")
        .append(NL)
        .append(" *")
        .append(NL)
        .append(" * SBE pads unused trailing bytes with NUL (0x00); the decoder stops at the first")
        .append(NL)
        .append(" * NUL and decodes only the prefix. Empty (all-NUL) buffers decode to the empty")
        .append(NL)
        .append(" * string. The returned slice references the underlying ArrayBuffer; callers must")
        .append(NL)
        .append(" * not retain it past the next wrap of the decoder that produced it.")
        .append(NL)
        .append(" */")
        .append(NL)
        .append(
            "export function readFixedString(buffer: DataView, offset: number, length: number): string {")
        .append(NL)
        .append("  let actualLength = length;")
        .append(NL)
        .append("  for (let i = 0; i < length; i++) {")
        .append(NL)
        .append("    if (buffer.getUint8(offset + i) === 0) {")
        .append(NL)
        .append("      actualLength = i;")
        .append(NL)
        .append("      break;")
        .append(NL)
        .append("    }")
        .append(NL)
        .append("  }")
        .append(NL)
        .append("  if (actualLength === 0) {")
        .append(NL)
        .append("    return \"\";")
        .append(NL)
        .append("  }")
        .append(NL)
        .append("  return TEXT_DECODER.decode(")
        .append(NL)
        .append("    new Uint8Array(buffer.buffer, buffer.byteOffset + offset, actualLength),")
        .append(NL)
        .append("  );")
        .append(NL)
        .append("}")
        .append(NL);
    Files.writeString(outputDir.resolve(CODEC_RUNTIME_FILENAME), sb, StandardCharsets.UTF_8);
  }

  /** Emit a single message's decoder file; returns the decoder class name. */
  private static String emitMessage(final List<Token> tokens, final Ir ir, final Path outputDir)
      throws IOException {
    final var begin = tokens.get(0);
    if (begin.signal() != Signal.BEGIN_MESSAGE) {
      throw new IllegalStateException(
          "First token of message list must be BEGIN_MESSAGE, got " + begin.signal());
    }

    final var messageName = begin.name();
    final var className = messageName + "Decoder";
    final int templateId = begin.id();
    final int blockLength = begin.encodedLength();

    final var fields = collectRootFields(tokens);
    final var groups = GroupGenerator.parseGroups(tokens, messageName);
    final var varDataFields = VarDataGenerator.parseVarData(tokens);
    final var deferredComposites = collectDeferredComposites(tokens);
    final boolean hasTrailingVarData = GroupGenerator.messageHasVarData(tokens);

    // Imports cover ALL features (root fields + group fields recursively). Var-data needs no
    // string/enum dependency — it returns Uint8Array. Char-array detection scans both the root
    // block and the recursive group tree.
    final boolean usesFixedString =
        fields.stream().anyMatch(f -> f.kind() == BlockFieldKind.CHAR_ARRAY)
            || GroupGenerator.anyUsesFixedString(groups);
    final var enumImports = mergedEnumImports(fields, groups);

    final var sb = new StringBuilder(16_384);
    final var groupClassBodies = new StringBuilder(8_192);
    final var groupGenerator = new GroupGenerator();
    final var varDataGenerator = new VarDataGenerator();

    // ---- File banner ----------------------------------------------------------------
    sb.append("// AUTO-GENERATED by :sbe-typescript-generator:generateTsCodecs (APP-34).")
        .append(NL)
        .append("// Source: SBE message ")
        .append(messageName)
        .append(" (templateId=")
        .append(templateId)
        .append(", blockLength=")
        .append(blockLength)
        .append(", schemaId=")
        .append(ir.id())
        .append(", schemaVersion=")
        .append(ir.version())
        .append("). Do not edit.")
        .append(NL)
        .append("//")
        .append(NL)
        .append("// DataView flyweight decoder — little-endian, zero-allocation reads.")
        .append(NL)
        .append("// int64/uint64 → bigint (avoids 2^53 precision loss).")
        .append(NL)
        .append(NL);

    // ---- Imports ---------------------------------------------------------------------
    // Only import the `<EnumName>_NULL_VAL` sentinel when at least one field of that enum is
    // optional anywhere in this message (root or any nested group). Importing it
    // unconditionally would produce unused-import warnings on the consumer side under strict
    // tsconfig (`noUnusedLocals` / `noUnusedImports`), since the sentinel is referenced only
    // in the optional getter's `v === <Enum>_NULL_VAL ? null : ...` check.
    if (usesFixedString) {
      sb.append("import { readFixedString } from \"./_codecRuntime.js\";").append(NL);
    }
    for (final var enumName : enumImports) {
      final boolean needsNullSentinel = anyOptionalEnumUsage(enumName, fields, groups);
      sb.append("import { ").append(enumName);
      if (needsNullSentinel) {
        sb.append(", ").append(enumName).append("_NULL_VAL");
      }
      sb.append(" } from \"./").append(enumName).append(".js\";").append(NL);
    }
    if (usesFixedString || !enumImports.isEmpty()) {
      sb.append(NL);
    }

    // ---- Class header + static constants + state fields + wrap() ----------------------
    sb.append("export class ").append(className).append(" {").append(NL);
    sb.append("  static readonly TEMPLATE_ID = ").append(templateId).append(";").append(NL);
    sb.append("  static readonly BLOCK_LENGTH = ").append(blockLength).append(";").append(NL);
    sb.append("  static readonly SCHEMA_ID = ").append(ir.id()).append(";").append(NL);
    sb.append("  static readonly SCHEMA_VERSION = ").append(ir.version()).append(";").append(NL);
    sb.append(NL);
    sb.append("  private buffer!: DataView;").append(NL);
    sb.append("  private bufferOffset = 0;").append(NL);
    // _limit is the cursor; advances past the root block at wrap() and is mutated by group
    // iterators and var-data getters as they consume bytes from the wire. Var-data getters
    // following groups land at the right start because of this shared cursor.
    sb.append("  private _limit = 0;").append(NL);

    // Cached-iterator and var-data cache fields go right after _limit so the
    // field-declaration block is contiguous in the emitted file (chunk-6 canonical-call-order):
    // group cache fields first (so they read above the accessor methods that reference them),
    // then var-data cache fields.
    groupGenerator.emitCachedFields(groups, sb);
    varDataGenerator.emitFields(varDataFields, sb);

    sb.append(NL);
    // wrap(buffer, offset) — initialise state, reset all caches, return this for chaining.
    sb.append("  wrap(buffer: DataView, offset: number): this {").append(NL);
    sb.append("    this.buffer = buffer;").append(NL);
    sb.append("    this.bufferOffset = offset;").append(NL);
    sb.append("    this._limit = offset + ").append(className).append(".BLOCK_LENGTH;").append(NL);
    varDataGenerator.emitWrapResets(varDataFields, sb);
    sb.append("    return this;").append(NL);
    sb.append("  }").append(NL);

    sb.append(NL);
    // encodedLength() returns _limit - bufferOffset, which equals BLOCK_LENGTH for messages
    // with no groups/varData (preserving chunk-5 semantics) and the true end-of-message
    // position after groups/varData have been consumed.
    sb.append("  /**")
        .append(NL)
        .append("   * Returns the byte length of the decoded message relative to the wrap offset.")
        .append(NL)
        .append("   *")
        .append(NL)
        .append("   * For messages with only root-block fields this equals `BLOCK_LENGTH`. For")
        .append(NL)
        .append("   * messages with groups and/or var-data, the true end-of-message position is")
        .append(NL)
        .append(
            "   * known only AFTER all groups have been iterated to completion AND all var-data")
        .append(NL)
        .append("   * getters have fired — the cursor advances on `next()` and on each var-data")
        .append(NL)
        .append("   * getter call. Calling this before draining returns the partial position.")
        .append(NL)
        .append("   */")
        .append(NL);
    sb.append("  encodedLength(): number {").append(NL);
    sb.append("    return this._limit - this.bufferOffset;").append(NL);
    sb.append("  }").append(NL);

    sb.append(NL);
    // Internal accessors used by group iterators in this file. Underscore-prefixed and tagged
    // @internal to signal "not part of the public API contract" — TypeScript has no friend-class
    // mechanism, so these are public methods, but consumers reaching in get undefined behavior.
    sb.append("  /**")
        .append(NL)
        .append("   * @internal Used by group iterators in this file. NOT part of the public API")
        .append(NL)
        .append("   * contract — TypeScript has no friend-class mechanism, so these accessors are")
        .append(NL)
        .append("   * public. Consumer code MUST NOT call them.")
        .append(NL)
        .append("   */")
        .append(NL);
    sb.append("  _getBuffer(): DataView {").append(NL);
    sb.append("    return this.buffer;").append(NL);
    sb.append("  }").append(NL);
    sb.append(NL);
    sb.append("  /** @internal */").append(NL);
    sb.append("  _getLimit(): number {").append(NL);
    sb.append("    return this._limit;").append(NL);
    sb.append("  }").append(NL);
    sb.append(NL);
    sb.append("  /** @internal */").append(NL);
    sb.append("  _setLimit(limit: number): void {").append(NL);
    sb.append("    this._limit = limit;").append(NL);
    sb.append("  }").append(NL);

    // ---- Root-block field getters ---------------------------------------------------
    for (final var field : fields) {
      sb.append(NL).append(emitFieldGetter(field));
    }

    // ---- Group accessors + iterator classes -----------------------------------------
    final var groupCtx =
        new GroupGenerator.GroupEmitContext(messageName, className, sb, groupClassBodies);
    groupGenerator.emit(groups, groupCtx, hasTrailingVarData);

    // ---- Var-data getters (positioned after groups per wire layout) -----------------
    varDataGenerator.emitGetters(varDataFields, sb);

    // ---- Trailing TODO block for deferred features (chunk 7+ only) ------------------
    if (!deferredComposites.isEmpty()) {
      sb.append(NL).append("  // Deferred to subsequent APP-34 chunks:").append(NL);
      for (final var compositeField : deferredComposites) {
        sb.append("  //   - composite field `")
            .append(compositeField.fieldName())
            .append("` of type `")
            .append(compositeField.compositeName())
            .append("` (chunk 7 — UuidCompositeGenerator)")
            .append(NL);
      }
    }

    sb.append("}").append(NL);

    // Iterator class declarations land AFTER the message decoder's closing brace.
    sb.append(groupClassBodies);

    Files.writeString(outputDir.resolve(className + ".ts"), sb, StandardCharsets.UTF_8);
    return className;
  }

  /** Merge enum-name imports across root fields and (recursively) all groups, in stable order. */
  private static List<String> mergedEnumImports(
      final List<BlockField> rootFields, final List<GroupGenerator.GroupSpec> groups) {
    final var enums = new LinkedHashSet<String>();
    for (final var field : rootFields) {
      if (field.kind() == BlockFieldKind.ENUM) {
        enums.add(field.enumName());
      }
    }
    enums.addAll(GroupGenerator.collectEnumImports(groups));
    return List.copyOf(enums);
  }

  /**
   * True if any root or nested-group field of this enum is declared {@code presence="optional"}.
   */
  private static boolean anyOptionalEnumUsage(
      final String enumName,
      final List<BlockField> rootFields,
      final List<GroupGenerator.GroupSpec> groups) {
    for (final var f : rootFields) {
      if (f.kind() == BlockFieldKind.ENUM && f.optional() && enumName.equals(f.enumName())) {
        return true;
      }
    }
    return GroupGenerator.anyEnumOptional(groups, enumName);
  }

  /**
   * Walk message tokens and collect root-block field metadata. Group/var-data tokens are skipped
   * via {@link Token#componentTokenCount()}, which spans the entire group or var-data sub-tree.
   */
  private static List<BlockField> collectRootFields(final List<Token> tokens) {
    final var fields = new ArrayList<BlockField>();
    // Skip BEGIN_MESSAGE (i=0); END_MESSAGE is the last token. Everything between is
    // either a root field, a repeating group, or a var-data declaration.
    int i = 1;
    while (i < tokens.size()) {
      final var token = tokens.get(i);
      switch (token.signal()) {
        case BEGIN_FIELD -> {
          final var inner = tokens.get(i + 1);
          final var field = BlockField.parseBlockField(token, inner);
          if (field != null) {
            fields.add(field);
          }
          i += token.componentTokenCount();
        }
        case BEGIN_GROUP, BEGIN_VAR_DATA -> i += token.componentTokenCount();
        case END_MESSAGE -> {
          return List.copyOf(fields);
        }
        default -> i++;
      }
    }
    return List.copyOf(fields);
  }

  /**
   * Collect composite-typed fields whose emitter lives in chunk 7 ({@code UuidCompositeGenerator}).
   * After chunk 6 lands, groups and var-data are no longer "deferred" — only composite fields
   * remain. Each entry surfaces as a trailing TODO comment on the emitted decoder so the field's
   * offset stays documented for chunk 7.
   */
  private static List<DeferredCompositeField> collectDeferredComposites(final List<Token> tokens) {
    final var composites = new ArrayList<DeferredCompositeField>();
    int i = 1;
    while (i < tokens.size()) {
      final var token = tokens.get(i);
      switch (token.signal()) {
        case BEGIN_FIELD -> {
          final var inner = tokens.get(i + 1);
          if (inner.signal() == Signal.BEGIN_COMPOSITE) {
            composites.add(new DeferredCompositeField(token.name(), inner.applicableTypeName()));
          }
          i += token.componentTokenCount();
        }
        case BEGIN_GROUP, BEGIN_VAR_DATA -> i += token.componentTokenCount();
        case END_MESSAGE -> {
          return List.copyOf(composites);
        }
        default -> i++;
      }
    }
    return List.copyOf(composites);
  }

  private static String emitFieldGetter(final BlockField field) {
    return switch (field.kind()) {
      case PRIMITIVE -> emitPrimitiveGetter(field);
      case ENUM -> emitEnumGetter(field);
      case CHAR_ARRAY -> emitCharArrayGetter(field);
    };
  }

  private static String emitPrimitiveGetter(final BlockField field) {
    final var dataViewMethod = dataViewMethodFor(field.primitive());
    final var returnType = primitiveReturnType(field.primitive());
    final boolean optional = field.optional();
    final boolean needsEndianArg = field.primitive().size() > 1;

    final var sb = new StringBuilder(256);
    sb.append("  ").append(field.fieldName()).append("(): ");
    sb.append(optional ? returnType + " | null" : returnType);
    sb.append(" {").append(NL);
    if (optional) {
      sb.append("    const v = this.buffer.")
          .append(dataViewMethod)
          .append("(this.bufferOffset + ")
          .append(field.offset())
          .append(needsEndianArg ? ", true);" : ");")
          .append(NL);
      sb.append("    return v === ")
          .append(numericLiteral(field.primitive(), field.encoding().applicableNullValue()))
          .append(" ? null : v;")
          .append(NL);
    } else {
      sb.append("    return this.buffer.")
          .append(dataViewMethod)
          .append("(this.bufferOffset + ")
          .append(field.offset())
          .append(needsEndianArg ? ", true);" : ");")
          .append(NL);
    }
    sb.append("  }").append(NL);
    return sb.toString();
  }

  private static String emitEnumGetter(final BlockField field) {
    final var dataViewMethod = dataViewMethodFor(field.primitive());
    final var enumName = field.enumName();
    final boolean optional = field.optional();
    final boolean needsEndianArg = field.primitive().size() > 1;

    final var sb = new StringBuilder(256);
    sb.append("  ").append(field.fieldName()).append("(): ");
    sb.append(optional ? enumName + " | null" : enumName);
    sb.append(" {").append(NL);
    sb.append("    const v = this.buffer.")
        .append(dataViewMethod)
        .append("(this.bufferOffset + ")
        .append(field.offset())
        .append(needsEndianArg ? ", true);" : ");")
        .append(NL);
    if (optional) {
      sb.append("    return v === ")
          .append(enumName)
          .append("_NULL_VAL ? null : (v as ")
          .append(enumName)
          .append(");")
          .append(NL);
    } else {
      sb.append("    return v as ").append(enumName).append(";").append(NL);
    }
    sb.append("  }").append(NL);
    return sb.toString();
  }

  private static String emitCharArrayGetter(final BlockField field) {
    final var sb = new StringBuilder(192);
    sb.append("  ").append(field.fieldName()).append("(): string {").append(NL);
    sb.append("    return readFixedString(this.buffer, this.bufferOffset + ")
        .append(field.offset())
        .append(", ")
        .append(field.arrayLength())
        .append(");")
        .append(NL);
    sb.append("  }").append(NL);
    return sb.toString();
  }

  /** Package-private — also used by {@link GroupGenerator} for record-block field emission. */
  static String dataViewMethodFor(final PrimitiveType primitive) {
    return switch (primitive) {
      case INT8 -> "getInt8";
      case UINT8 -> "getUint8";
      case INT16 -> "getInt16";
      case UINT16 -> "getUint16";
      case INT32 -> "getInt32";
      case UINT32 -> "getUint32";
      case INT64 -> "getBigInt64";
      case UINT64 -> "getBigUint64";
      case FLOAT -> "getFloat32";
      case DOUBLE -> "getFloat64";
      default -> throw new IllegalStateException("No DataView accessor for primitive " + primitive);
    };
  }

  /** Package-private — also used by {@link GroupGenerator} for record-block field emission. */
  static String primitiveReturnType(final PrimitiveType primitive) {
    return switch (primitive) {
      case INT8, UINT8, INT16, UINT16, INT32, UINT32, FLOAT, DOUBLE -> "number";
      case INT64, UINT64 -> "bigint";
      default -> throw new IllegalStateException("No TS return type for primitive " + primitive);
    };
  }

  /**
   * Render a {@link PrimitiveValue} sentinel as the matching TypeScript numeric literal. Widths up
   * to 32 bits emit a JS {@code number}; 64-bit widths emit a {@code bigint} literal (suffix {@code
   * n}). For {@code uint64} the SBE constant is stored as a signed Java {@code long} ({@code -1L}
   * for the default null bit-pattern); {@link Long#toUnsignedString(long)} renders it as the
   * canonical positive form ({@code 18446744073709551615n}).
   */
  /** Package-private — also used by {@link GroupGenerator} for record-block field emission. */
  static String numericLiteral(final PrimitiveType primitive, final PrimitiveValue value) {
    return switch (primitive) {
      case INT64 -> Long.toString(value.longValue()) + "n";
      case UINT64 -> Long.toUnsignedString(value.longValue()) + "n";
      case INT8, UINT8, INT16, UINT16, INT32, UINT32 -> Long.toString(value.longValue());
      case FLOAT, DOUBLE -> Double.toString(value.doubleValue());
      default -> throw new IllegalStateException("No literal form for primitive " + primitive);
    };
  }

  /** A composite-typed root field deferred to chunk 7 (e.g. the {@code uuid} composite). */
  private record DeferredCompositeField(String fieldName, String compositeName) {}
}
