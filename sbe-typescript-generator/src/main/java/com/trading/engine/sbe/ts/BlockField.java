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

import uk.co.real_logic.sbe.PrimitiveType;
import uk.co.real_logic.sbe.ir.Encoding;
import uk.co.real_logic.sbe.ir.Signal;
import uk.co.real_logic.sbe.ir.Token;

/**
 * Parsed-field metadata shared by the root-block emitter ({@link MessageGenerator}) and the group
 * record-block emitter ({@link GroupGenerator}). A {@link BlockField} describes one field inside a
 * block — either the message root block or a repeating-group record block — at the level of detail
 * needed to emit its TypeScript getter.
 *
 * <h2>Why this lives in its own file</h2>
 *
 * The chunk-5 emitter kept this record private inside {@code MessageGenerator}. Chunk 6 introduces
 * {@code GroupGenerator} which parses the same {@code (BEGIN_FIELD, inner)} token pair shape for
 * record-block fields. A package-private record nested inside a {@code final} class is not legally
 * accessible from sibling classes in the same package, so the chunk-6 extraction promotes both the
 * record and the parser to top-level package-private types in this file. Both emitters then call
 * {@link #parseBlockField(Token, Token)} and read {@link BlockField} fields directly without
 * duplicating the parse logic.
 *
 * <h2>Optional-flag dual-source detection (locked invariant)</h2>
 *
 * {@link #parseBlockField(Token, Token)} preserves the chunk-5 invariant — established in commit
 * {@code d8bc245} and hardened in {@code 09e5128} — that SBE attaches the field-level {@code
 * presence="optional"} attribute to <em>different</em> tokens depending on field kind:
 *
 * <ul>
 *   <li><b>Primitive fields</b> (e.g. {@code <field price type="int64" presence="optional"/>}): the
 *       inner ENCODING token carries the optional presence on its encoding.
 *   <li><b>Enum fields</b> (e.g. {@code <field settlType type="SettlTypeEnum"
 *       presence="optional"/>}): the BEGIN_ENUM token's encoding stays REQUIRED (since the
 *       underlying enum-type declaration is required); the optional flag is on the OUTER
 *       BEGIN_FIELD token.
 * </ul>
 *
 * The parser ORs both sources so optional detection works for both kinds. The {@code
 * MessageGeneratorChunk6Test#parseBlockField_optionalFlagDualSource} test enforces this rule with
 * one fixture per kind so a future "tidy" refactor cannot regress it silently.
 *
 * <h2>Threading model</h2>
 *
 * Not thread-safe. Build-time only.
 *
 * <h2>Allocation behavior</h2>
 *
 * Build-time only — allocates freely while constructing emitter state.
 *
 * @param fieldName schema field name (also the emitted TS getter name)
 * @param offset byte offset within the parent block (message root or group record), taken from the
 *     IR token's {@link Token#offset()}
 * @param kind dispatch tag — {@link BlockFieldKind#PRIMITIVE}, {@link BlockFieldKind#ENUM}, or
 *     {@link BlockFieldKind#CHAR_ARRAY}
 * @param primitive underlying primitive (the enum's {@code encodingType} for enum kinds; {@code
 *     CHAR} for char arrays; the bare primitive type otherwise)
 * @param arrayLength array length for {@code CHAR_ARRAY}; always 1 for the other kinds
 * @param enumName imported enum name (e.g. {@code SideEnum}) for {@link BlockFieldKind#ENUM};
 *     {@code null} otherwise
 * @param encoding raw IR encoding, kept for {@code applicableNullValue} queries during getter
 *     emission (the optional sentinel)
 * @param optional whether the field is declared {@code presence="optional"} — computed at parse
 *     time from the dual-source rule documented above
 */
record BlockField(
    String fieldName,
    int offset,
    BlockFieldKind kind,
    PrimitiveType primitive,
    int arrayLength,
    String enumName,
    Encoding encoding,
    boolean optional) {

  /**
   * Translate a {@code (BEGIN_FIELD, inner-type)} token pair into a {@link BlockField}.
   *
   * <p>Recognises the {@code uuid} composite (two {@code int64} halves) as {@link
   * BlockFieldKind#UUID_COMPOSITE}; chunk 7's {@link UuidCompositeGenerator} handles getter
   * emission. Any other composite type triggers {@link IllegalStateException} — the schema declares
   * only the {@code uuid} composite today, and adding a new one (e.g. a future {@code decimal64})
   * needs a deliberate emitter extension rather than a silent skip.
   *
   * <p>Throws {@link IllegalStateException} for schema constructs the emitter does not support:
   * {@code BEGIN_SET} (bitset), constant-presence ENCODING fields (offset = -1, value embedded in
   * metadata), and single-byte {@code char} fields (length = 1, no clean JS string mapping). The
   * schema declares none of these today; the guards force a deliberate decision if a future schema
   * change introduces one.
   *
   * @param fieldToken the outer {@code BEGIN_FIELD} token
   * @param inner the immediately-following inner-type token (ENCODING, BEGIN_ENUM, or
   *     BEGIN_COMPOSITE)
   * @return the parsed {@link BlockField} (never {@code null} — every supported field kind yields a
   *     record)
   * @throws IllegalStateException if the field uses an unsupported schema construct
   */
  static BlockField parseBlockField(final Token fieldToken, final Token inner) {
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

    // `presence="constant"` fields embed the value directly in the schema with no wire bytes;
    // SBE marks the offset as -1 and consumers are expected to read the constant from the
    // Encoding metadata. The chunk-6 emitter does not yet support this — emitting a regular
    // DataView read at offset -1 would produce broken codecs. Schema declares no constant
    // fields today; reject loudly if a future change introduces one so the gap is surfaced
    // rather than silently miscompiled.
    if (inner.signal() == Signal.ENCODING
        && inner.encoding().presence() == Encoding.Presence.CONSTANT) {
      throw new IllegalStateException(
          "Constant-presence field not supported by chunk 6: "
              + name
              + " (schema declares none today; add explicit emitter support before introducing one)");
    }

    // Optional FLOAT/DOUBLE need NaN-aware sentinel comparison (`Number.isNaN(v)` since
    // `v === NaN` is always false in JS). The chunk-6 emitter does not yet implement that
    // branch — emitting a regular `v === <NaN-literal>` comparison would silently produce
    // a getter that never returns null. Schema declares no float/double fields today;
    // reject loudly if a future change introduces an optional one so the gap is surfaced
    // rather than silently miscompiled.
    if (inner.signal() == Signal.ENCODING && fieldLevelOptional) {
      final var primitive = inner.encoding().primitiveType();
      if (primitive == PrimitiveType.FLOAT || primitive == PrimitiveType.DOUBLE) {
        throw new IllegalStateException(
            "Optional FLOAT/DOUBLE field not supported by chunk 6: "
                + name
                + " (NaN sentinel requires Number.isNaN check; schema declares none today)");
      }
    }

    return switch (inner.signal()) {
      case ENCODING -> {
        final var primitive = inner.encoding().primitiveType();
        final int arrayLength = inner.arrayLength();
        if (primitive == PrimitiveType.CHAR && arrayLength > 1) {
          yield new BlockField(
              name,
              offset,
              BlockFieldKind.CHAR_ARRAY,
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
        yield new BlockField(
            name,
            offset,
            BlockFieldKind.PRIMITIVE,
            primitive,
            1,
            null,
            inner.encoding(),
            fieldLevelOptional);
      }
      case BEGIN_ENUM -> {
        final var primitive = inner.encoding().primitiveType();
        requireEnumPrimitive(name, primitive);
        yield new BlockField(
            name,
            offset,
            BlockFieldKind.ENUM,
            primitive,
            1,
            inner.applicableTypeName(),
            inner.encoding(),
            fieldLevelOptional);
      }
      // BEGIN_COMPOSITE is observed only as the inner type of a BEGIN_FIELD wrapper (e.g. the
      // `uuid` composite under `<field type="uuid"/>`). Chunk 7 lights up the `uuid` composite
      // via UuidCompositeGenerator; any other composite type would silently miscompile, so it
      // throws to surface the gap.
      case BEGIN_COMPOSITE -> {
        final var typeName = inner.applicableTypeName();
        if (!UuidCompositeGenerator.UUID_TYPE_NAME.equals(typeName)) {
          throw new IllegalStateException(
              "Composite-typed field '"
                  + name
                  + "' uses composite '"
                  + typeName
                  + "'; only the `uuid` composite is supported (extend the emitter before"
                  + " introducing a new one)");
        }
        // `presence="constant"` on a composite-typed field embeds the value directly in schema
        // metadata (offset = -1, no wire bytes). The chunk-7 emitter does not support this for
        // composites — emitting a uuid getter that reads from offset -1 would produce broken
        // codecs. Schema declares no constant composite fields today; reject loudly if a future
        // change introduces one so the gap is surfaced rather than silently miscompiled.
        if (fieldToken.encoding().presence() == Encoding.Presence.CONSTANT) {
          throw new IllegalStateException(
              "Constant-presence composite field not supported: "
                  + name
                  + " (uuid composite at offset -1 has no wire layout; schema declares none today)");
        }
        // The uuid composite has no nullable encoding — decoders return a UuidValue object
        // directly. `presence="optional"` on a uuid field is not declared in the schema today;
        // reject loudly if a future schema change adds one so the gap is surfaced.
        if (fieldLevelOptional) {
          throw new IllegalStateException(
              "Optional uuid field not supported: "
                  + name
                  + " (no UuidValue null sentinel today; schema declares none)");
        }
        yield new BlockField(
            name,
            offset,
            BlockFieldKind.UUID_COMPOSITE,
            // The `primitive`/`encoding` slots are unused for uuid getter emission — the
            // emitter reads directly from the field offset using two getBigInt64 calls.
            // Keep them populated with the inner composite token's encoding so callers that
            // generically inspect `.encoding()` do not NPE.
            PrimitiveType.INT64,
            1,
            null,
            inner.encoding(),
            false);
      }
      case BEGIN_SET ->
          throw new IllegalStateException(
              "BEGIN_SET (bitset) field not supported by chunk 6: "
                  + name
                  + " (schema does not declare any today)");
      default ->
          throw new IllegalStateException(
              "Unexpected inner token signal " + inner.signal() + " for field " + name);
    };
  }

  /**
   * Reject enum encoding types not covered by {@link EnumGenerator}'s emit path.
   *
   * <p>The project schema declares only {@code uint8} for enums; {@code uint16}/{@code uint32}
   * would also be safe to support but are accepted preemptively so a future schema change does not
   * need a compiler edit. Signed and {@code uint64} encodings need additional emit logic (e.g.
   * char-typed enums use single-character literals; uint64 enums would force {@code bigint} on the
   * consumer side, breaking the {@code as const} number-literal idiom).
   *
   * @param fieldName schema field name (for the diagnostic)
   * @param primitive the enum's underlying encoding type
   * @throws IllegalStateException if {@code primitive} is anything other than UINT8 / UINT16 /
   *     UINT32
   */
  static void requireEnumPrimitive(final String fieldName, final PrimitiveType primitive) {
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
}
