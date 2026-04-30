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
 * <h2>Repeating groups, var-data, composite (uuid) fields — deferred</h2>
 *
 * Per the APP-34 chunk plan, this emitter handles only root-block fields. Token sequences for
 * repeating groups ({@code Signal.BEGIN_GROUP} … {@code END_GROUP}) and var-data fields ({@code
 * BEGIN_VAR_DATA} … {@code END_VAR_DATA}) are skipped via the parent token's {@link
 * Token#componentTokenCount()}. The names of any deferred features are recorded in a trailing
 * comment block on each emitted file so the future {@code VarDataGenerator} (chunk 6) and
 * group/composite emitters know what to fill in. This keeps chunk 5's diff focused while preserving
 * the per-message decoder shape.
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
    final var deferred = collectDeferred(tokens);
    final var enumImports = collectEnumImports(fields);
    final boolean usesFixedString = fields.stream().anyMatch(f -> f.kind() == FieldKind.CHAR_ARRAY);

    final var sb = new StringBuilder(8_192);

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
    // optional in this message. Importing it unconditionally would produce unused-import
    // warnings on the consumer side under strict tsconfig (`noUnusedLocals` /
    // `noUnusedImports`), since the sentinel is referenced only in the optional getter's
    // `v === <Enum>_NULL_VAL ? null : ...` check.
    if (usesFixedString) {
      sb.append("import { readFixedString } from \"./_codecRuntime.js\";").append(NL);
    }
    for (final var enumName : enumImports) {
      final boolean needsNullSentinel =
          fields.stream()
              .anyMatch(
                  f -> f.kind() == FieldKind.ENUM && f.optional() && enumName.equals(f.enumName()));
      sb.append("import { ").append(enumName);
      if (needsNullSentinel) {
        sb.append(", ").append(enumName).append("_NULL_VAL");
      }
      sb.append(" } from \"./").append(enumName).append(".js\";").append(NL);
    }
    if (usesFixedString || !enumImports.isEmpty()) {
      sb.append(NL);
    }

    // ---- Class header + static constants + wrap() -------------------------------------
    sb.append("export class ").append(className).append(" {").append(NL);
    sb.append("  static readonly TEMPLATE_ID = ").append(templateId).append(";").append(NL);
    sb.append("  static readonly BLOCK_LENGTH = ").append(blockLength).append(";").append(NL);
    sb.append("  static readonly SCHEMA_ID = ").append(ir.id()).append(";").append(NL);
    sb.append("  static readonly SCHEMA_VERSION = ").append(ir.version()).append(";").append(NL);
    sb.append(NL);
    sb.append("  private buffer!: DataView;").append(NL);
    sb.append("  private bufferOffset = 0;").append(NL);
    sb.append(NL);
    sb.append("  wrap(buffer: DataView, offset: number): this {").append(NL);
    sb.append("    this.buffer = buffer;").append(NL);
    sb.append("    this.bufferOffset = offset;").append(NL);
    sb.append("    return this;").append(NL);
    sb.append("  }").append(NL);
    sb.append(NL);
    // Root-block length only. Repeating groups and var-data (if present) extend the
    // message past this point and will be exposed by accessors landing in chunks 6/7.
    sb.append("  encodedLength(): number {").append(NL);
    sb.append("    return ").append(className).append(".BLOCK_LENGTH;").append(NL);
    sb.append("  }").append(NL);

    // ---- Field getters ---------------------------------------------------------------
    for (final var field : fields) {
      sb.append(NL).append(emitFieldGetter(field));
    }

    // ---- Trailing TODO block for deferred features ----------------------------------
    if (!deferred.groups().isEmpty()
        || !deferred.varData().isEmpty()
        || !deferred.composites().isEmpty()) {
      sb.append(NL).append("  // Deferred to subsequent APP-34 chunks:").append(NL);
      for (final var groupName : deferred.groups()) {
        sb.append("  //   - repeating group `")
            .append(groupName)
            .append("` (chunk 6 — group emitter)")
            .append(NL);
      }
      for (final var varDataName : deferred.varData()) {
        sb.append("  //   - var-data field `")
            .append(varDataName)
            .append("` (chunk 6 — VarDataGenerator)")
            .append(NL);
      }
      for (final var compositeField : deferred.composites()) {
        sb.append("  //   - composite field `")
            .append(compositeField.fieldName())
            .append("` of type `")
            .append(compositeField.compositeName())
            .append("` (chunk 7 — UuidCompositeGenerator)")
            .append(NL);
      }
    }

    sb.append("}").append(NL);

    Files.writeString(outputDir.resolve(className + ".ts"), sb, StandardCharsets.UTF_8);
    return className;
  }

  /**
   * Walk message tokens and collect root-block field metadata. Group/var-data tokens are skipped
   * via {@link Token#componentTokenCount()}, which spans the entire group or var-data sub-tree.
   */
  private static List<RootField> collectRootFields(final List<Token> tokens) {
    final var fields = new ArrayList<RootField>();
    // Skip BEGIN_MESSAGE (i=0); END_MESSAGE is the last token. Everything between is
    // either a root field, a repeating group, or a var-data declaration.
    int i = 1;
    while (i < tokens.size()) {
      final var token = tokens.get(i);
      switch (token.signal()) {
        case BEGIN_FIELD -> {
          final var inner = tokens.get(i + 1);
          final var field = parseRootField(token, inner);
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
   * Translate a (BEGIN_FIELD, inner-type) token pair into a {@link RootField}. Returns {@code null}
   * for composite fields (e.g. {@code uuid}) — these are tracked separately by {@link
   * #collectDeferred(List)} and surfaced as a trailing TODO comment so the offset stays documented
   * for the chunk-7 emitter.
   */
  private static RootField parseRootField(final Token fieldToken, final Token inner) {
    final var name = fieldToken.name();
    final int offset = fieldToken.offset();

    // SBE attaches the field-level `presence="optional"` attribute to different tokens
    // depending on field kind:
    //
    //  - Primitive fields (e.g. `<field price type="int64" presence="optional"/>`): the
    //    inner ENCODING token carries the optional presence on its encoding.
    //  - Enum fields (e.g. `<field settlType type="SettlTypeEnum" presence="optional"/>`):
    //    the BEGIN_ENUM token's encoding stays REQUIRED (since the underlying enum type
    //    declaration is required); the optional flag is on the OUTER BEGIN_FIELD token.
    //
    // Honour either source so both field kinds detect optionality correctly.
    final boolean fieldLevelOptional =
        fieldToken.isOptionalEncoding()
            || inner.encoding().presence() == Encoding.Presence.OPTIONAL;

    // `presence="constant"` fields embed the value directly in the schema with no wire
    // bytes; SBE marks the offset as -1 and consumers are expected to read the constant
    // from the Encoding metadata. The chunk-5 emitter does not yet support this — emitting
    // a regular DataView read at offset -1 would produce broken codecs. Schema declares no
    // constant fields today; reject loudly if a future change introduces one so the gap is
    // surfaced rather than silently miscompiled.
    if (inner.signal() == Signal.ENCODING
        && inner.encoding().presence() == Encoding.Presence.CONSTANT) {
      throw new IllegalStateException(
          "Constant-presence field not supported by chunk 5: "
              + name
              + " (schema declares none today; add explicit emitter support before introducing one)");
    }

    return switch (inner.signal()) {
      case ENCODING -> {
        final var primitive = inner.encoding().primitiveType();
        final int arrayLength = inner.arrayLength();
        if (primitive == PrimitiveType.CHAR && arrayLength > 1) {
          yield new RootField(
              name,
              offset,
              FieldKind.CHAR_ARRAY,
              primitive,
              arrayLength,
              null,
              inner.encoding(),
              fieldLevelOptional);
        }
        if (primitive == PrimitiveType.CHAR) {
          // Single-byte char (length=1) is not exercised by trading-schema.xml today and
          // has no clean JS string mapping — a one-character `String.fromCharCode` is
          // wasteful and a `number` getter would silently break consumers expecting text.
          // Reject so a future schema change forces a deliberate decision.
          throw new IllegalStateException(
              "Single-byte char field not supported: "
                  + name
                  + " (use char[N>=2] for fixed-length strings, or uint8 for a single byte)");
        }
        yield new RootField(
            name,
            offset,
            FieldKind.PRIMITIVE,
            primitive,
            1,
            null,
            inner.encoding(),
            fieldLevelOptional);
      }
      case BEGIN_ENUM -> {
        final var primitive = inner.encoding().primitiveType();
        requireEnumPrimitive(name, primitive);
        yield new RootField(
            name,
            offset,
            FieldKind.ENUM,
            primitive,
            1,
            inner.applicableTypeName(),
            inner.encoding(),
            fieldLevelOptional);
      }
      // BEGIN_COMPOSITE is observed only as the inner type of a BEGIN_FIELD wrapper
      // (e.g. the `uuid` composite under `<field type="uuid"/>`); it is never a top-level
      // signal. The deferred-features collector picks it up via the same field walk.
      case BEGIN_COMPOSITE -> null;
      case BEGIN_SET ->
          throw new IllegalStateException(
              "BEGIN_SET (bitset) field not supported by chunk 5: "
                  + name
                  + " (schema does not declare any today)");
      default ->
          throw new IllegalStateException(
              "Unexpected inner token signal " + inner.signal() + " for field " + name);
    };
  }

  /** Collect names of deferred features so the emitted file's trailing TODO block lists them. */
  private static DeferredFeatures collectDeferred(final List<Token> tokens) {
    final var groups = new ArrayList<String>();
    final var varData = new ArrayList<String>();
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
        case BEGIN_GROUP -> {
          groups.add(token.name());
          i += token.componentTokenCount();
        }
        case BEGIN_VAR_DATA -> {
          varData.add(token.name());
          i += token.componentTokenCount();
        }
        case END_MESSAGE -> {
          return new DeferredFeatures(
              List.copyOf(groups), List.copyOf(varData), List.copyOf(composites));
        }
        default -> i++;
      }
    }
    return new DeferredFeatures(List.copyOf(groups), List.copyOf(varData), List.copyOf(composites));
  }

  /** Distinct enum names referenced by the message's root fields, in stable insertion order. */
  private static List<String> collectEnumImports(final List<RootField> fields) {
    final var enums = new LinkedHashSet<String>();
    for (final var field : fields) {
      if (field.kind() == FieldKind.ENUM) {
        enums.add(field.enumName());
      }
    }
    return List.copyOf(enums);
  }

  private static void requireEnumPrimitive(final String fieldName, final PrimitiveType primitive) {
    switch (primitive) {
      case UINT8, UINT16, UINT32 -> {
        // supported — match the EnumGenerator's accepted set
      }
      default ->
          throw new IllegalStateException(
              "Enum field '"
                  + fieldName
                  + "' has unsupported encoding type "
                  + primitive
                  + "; EnumGenerator accepts only uint8/uint16/uint32");
    }
  }

  private static String emitFieldGetter(final RootField field) {
    return switch (field.kind()) {
      case PRIMITIVE -> emitPrimitiveGetter(field);
      case ENUM -> emitEnumGetter(field);
      case CHAR_ARRAY -> emitCharArrayGetter(field);
    };
  }

  private static String emitPrimitiveGetter(final RootField field) {
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

  private static String emitEnumGetter(final RootField field) {
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

  private static String emitCharArrayGetter(final RootField field) {
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

  private static String dataViewMethodFor(final PrimitiveType primitive) {
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

  private static String primitiveReturnType(final PrimitiveType primitive) {
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
  private static String numericLiteral(final PrimitiveType primitive, final PrimitiveValue value) {
    return switch (primitive) {
      case INT64 -> Long.toString(value.longValue()) + "n";
      case UINT64 -> Long.toUnsignedString(value.longValue()) + "n";
      case INT8, UINT8, INT16, UINT16, INT32, UINT32 -> Long.toString(value.longValue());
      case FLOAT, DOUBLE -> Double.toString(value.doubleValue());
      default -> throw new IllegalStateException("No literal form for primitive " + primitive);
    };
  }

  /** Field-kind tag used to dispatch getter emission. */
  private enum FieldKind {
    PRIMITIVE,
    ENUM,
    CHAR_ARRAY
  }

  /**
   * Parsed root field metadata, sufficient to emit the TS getter without re-walking tokens.
   *
   * @param fieldName schema field name (also the emitted TS getter name)
   * @param offset byte offset within the message body, taken from the IR token's {@link
   *     Token#offset()}
   * @param kind dispatch tag — {@link FieldKind#PRIMITIVE}, {@link FieldKind#ENUM}, or {@link
   *     FieldKind#CHAR_ARRAY}
   * @param primitive underlying primitive (the enum's {@code encodingType} for enum kinds; {@code
   *     CHAR} for char arrays)
   * @param arrayLength array length for {@code CHAR_ARRAY}; always 1 for the other kinds
   * @param enumName imported enum name (e.g. {@code SideEnum}) for {@link FieldKind#ENUM}; {@code
   *     null} otherwise
   * @param encoding raw IR encoding, kept for {@code applicableNullValue} queries during getter
   *     emission (the optional sentinel)
   * @param optional whether the field is declared {@code presence="optional"}. Computed at parse
   *     time because SBE attaches the field-level optional flag to the {@link
   *     Token#isOptionalEncoding() outer field token} for enum-typed fields and to the inner
   *     ENCODING token for primitive-typed fields; deriving this once at parse time keeps the
   *     getter emitters simple
   */
  private record RootField(
      String fieldName,
      int offset,
      FieldKind kind,
      PrimitiveType primitive,
      int arrayLength,
      String enumName,
      Encoding encoding,
      boolean optional) {}

  /** Names of group/var-data/composite features deferred to later APP-34 chunks. */
  private record DeferredFeatures(
      List<String> groups, List<String> varData, List<DeferredCompositeField> composites) {}

  /** A composite-typed root field deferred to chunk 7 (e.g. the {@code uuid} composite). */
  private record DeferredCompositeField(String fieldName, String compositeName) {}
}
