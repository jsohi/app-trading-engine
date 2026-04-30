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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import uk.co.real_logic.sbe.PrimitiveType;
import uk.co.real_logic.sbe.ir.Signal;
import uk.co.real_logic.sbe.ir.Token;

/**
 * Emits SBE var-data field accessors into the per-message TypeScript decoder file.
 *
 * <h2>Output shape</h2>
 *
 * For every {@code <data type="varDataEncoding">} declared inside a message, this emitter writes:
 *
 * <ul>
 *   <li>One private cache field declared on the message decoder class: {@code private
 *       _<fieldName>Cache: Uint8Array | null = null;}
 *   <li>One {@code wrap()}-body reset line: {@code this._<fieldName>Cache = null;}
 *   <li>One getter method: {@code <fieldName>(): Uint8Array {...}} that reads the uint32 length
 *       prefix at {@code _limit}, constructs a zero-copy {@code Uint8Array} view over the data
 *       bytes, advances {@code _limit} by {@code 4 + length}, caches the view, and returns it.
 *       Subsequent calls within the same {@code wrap()} return the cached view without advancing
 *       {@code _limit}.
 * </ul>
 *
 * <h2>Cursor semantics — read-once-per-wrap, declaration-order required</h2>
 *
 * Calling a var-data getter mutates the parent's {@code _limit} cursor on first call. The cache
 * lets the consumer call the getter idempotently within a single {@code wrap()}; calling
 * <em>different</em> var-data getters out of declaration order is undefined behavior because each
 * subsequent getter reads its length from {@code _limit} on the assumption that all earlier
 * var-data fields have advanced past their data. The chunk-6 plan documents this explicitly on
 * every emitted getter's JSDoc.
 *
 * <h2>Encapsulation contract</h2>
 *
 * This emitter <strong>owns the {@code _<x>Cache} field-name format</strong>. {@code
 * MessageGenerator} MUST NOT format cache field names directly anywhere — every textual reference
 * to a var-data cache field name routes through one of the four public methods exposed here. The
 * chunk-6 test {@code
 * MessageGeneratorChunk6Test#messageGeneratorSource_doesNotFormatVarDataCacheNamesDirectly}
 * enforces this convention by asserting the literal substring {@code _Cache} does not appear in
 * non-comment lines of {@code MessageGenerator.java}.
 *
 * <h2>Wire layout</h2>
 *
 * SBE's {@code varDataEncoding} composite is a {@code uint32} length prefix followed by {@code
 * length} bytes of raw {@code uint8} data. The schema declares {@code maxValue="1048576"} (1 MiB)
 * on the length field; chunk 6 does not enforce the cap at decode time (encoder-side enforcement;
 * decoder zero-trusts the wire — matches Aeron Java decoder idiom and chunk-5's {@code
 * readFixedString}).
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
 * @see GroupGenerator
 */
final class VarDataGenerator {

  /**
   * Length-prefix size for the {@code varDataEncoding} composite ({@code uint32}, 4 bytes).
   * Hardcoded because the schema declares only one var-data encoding and chunk-6's emitter supports
   * only that shape. If a future schema introduces a different length-prefix width, this constant
   * becomes a {@link VarDataSpec}-level field instead of a static.
   */
  private static final int VAR_DATA_LENGTH_PREFIX_SIZE = 4;

  /** Constructor — no state. */
  VarDataGenerator() {
    // intentionally empty
  }

  /**
   * Emit the per-var-data private cache field declarations into the parent class body.
   *
   * @param varDataFields parsed var-data specs in declaration order
   * @param messageBody StringBuilder for the parent class body
   */
  void emitFields(final List<VarDataSpec> varDataFields, final StringBuilder messageBody) {
    Objects.requireNonNull(varDataFields, "varDataFields");
    Objects.requireNonNull(messageBody, "messageBody");
    if (varDataFields.isEmpty()) {
      return;
    }
    for (final var spec : varDataFields) {
      messageBody
          .append(NL)
          .append("  private ")
          .append(cacheFieldName(spec))
          .append(": Uint8Array | null = null;")
          .append(NL);
    }
  }

  /**
   * Emit the wrap-body reset lines for every var-data cache. Called from inside {@code
   * MessageGenerator}'s emitted {@code wrap(...)} body, after {@code this._limit} is initialised.
   * The {@code _limit} reset itself stays in {@code MessageGenerator}; this method handles only
   * var-data cache fields.
   *
   * @param varDataFields parsed var-data specs in declaration order
   * @param wrapBody StringBuilder for the wrap method body
   */
  void emitWrapResets(final List<VarDataSpec> varDataFields, final StringBuilder wrapBody) {
    Objects.requireNonNull(varDataFields, "varDataFields");
    Objects.requireNonNull(wrapBody, "wrapBody");
    for (final var spec : varDataFields) {
      wrapBody.append("    this.").append(cacheFieldName(spec)).append(" = null;").append(NL);
    }
  }

  /**
   * Emit the getter methods for every var-data field — one per field, in declaration order. Each
   * getter reads its length from {@code this._limit}, constructs a zero-copy view over the data
   * bytes, advances {@code _limit} by {@code 4 + length}, caches the view, and returns it.
   *
   * @param varDataFields parsed var-data specs in declaration order
   * @param messageBody StringBuilder for the parent class body
   */
  void emitGetters(final List<VarDataSpec> varDataFields, final StringBuilder messageBody) {
    Objects.requireNonNull(varDataFields, "varDataFields");
    Objects.requireNonNull(messageBody, "messageBody");
    for (final var spec : varDataFields) {
      messageBody.append(NL).append(emitOneGetter(spec));
    }
  }

  private static String emitOneGetter(final VarDataSpec spec) {
    final var cacheField = cacheFieldName(spec);
    final var sb = new StringBuilder(640);
    sb.append("  /**").append(NL);
    // description() is coalesced to "" in parseOneVarData when the schema declares none, so
    // a null check here would be dead.
    if (!spec.description().isBlank()) {
      sb.append("   * ").append(escapeJsDoc(spec.description())).append(NL);
      sb.append("   *").append(NL);
    }
    sb.append("   * Returns a zero-copy `Uint8Array` view over the underlying buffer; callers")
        .append(NL);
    sb.append("   * must not retain the view past the next `wrap()` of this decoder. The result")
        .append(NL);
    sb.append("   * is cached so repeat calls within the same `wrap()` return the same view")
        .append(NL);
    sb.append("   * without advancing the cursor.").append(NL);
    sb.append("   *").append(NL);
    sb.append("   * Cursor invariants:").append(NL);
    sb.append("   *   - All preceding repeating groups (if any) MUST be drained exhaustively")
        .append(NL);
    sb.append("   *     before this getter fires. Calling it pre-drain reads the next group's")
        .append(NL);
    sb.append("   *     dimension bytes as the var-data length, producing garbage or RangeError.")
        .append(NL);
    sb.append("   *   - In messages with multiple var-data fields, getters MUST be called in")
        .append(NL);
    sb.append("   *     declaration order. `_limit` advances on first call; out-of-order reads")
        .append(NL);
    sb.append("   *     silently corrupt subsequent reads.").append(NL);
    sb.append("   */").append(NL);
    sb.append("  ").append(spec.fieldName()).append("(): Uint8Array {").append(NL);
    sb.append("    if (this.")
        .append(cacheField)
        .append(" !== null) return this.")
        .append(cacheField)
        .append(";")
        .append(NL);
    sb.append("    const length = this.buffer.getUint32(this._limit, true);").append(NL);
    sb.append(
            "    const view = new Uint8Array(this.buffer.buffer, this.buffer.byteOffset + this._limit + ")
        .append(VAR_DATA_LENGTH_PREFIX_SIZE)
        .append(", length);")
        .append(NL);
    sb.append("    this._limit += ")
        .append(VAR_DATA_LENGTH_PREFIX_SIZE)
        .append(" + length;")
        .append(NL);
    sb.append("    this.").append(cacheField).append(" = view;").append(NL);
    sb.append("    return view;").append(NL);
    sb.append("  }").append(NL);
    return sb.toString();
  }

  // ---------------------------------------------------------------------------------------
  // Token → VarDataSpec parsing
  // ---------------------------------------------------------------------------------------

  /**
   * Walk the message's IR token list and return one {@link VarDataSpec} per {@code <data
   * type="varDataEncoding">} declaration, in source order.
   */
  static List<VarDataSpec> parseVarData(final List<Token> tokens) {
    Objects.requireNonNull(tokens, "tokens");
    final var specs = new ArrayList<VarDataSpec>();
    int i = 1; // skip BEGIN_MESSAGE
    while (i < tokens.size()) {
      final var token = tokens.get(i);
      switch (token.signal()) {
        case BEGIN_VAR_DATA -> {
          specs.add(parseOneVarData(token));
          i += token.componentTokenCount();
        }
        case BEGIN_FIELD, BEGIN_GROUP -> i += token.componentTokenCount();
        case END_MESSAGE -> {
          return List.copyOf(specs);
        }
        default -> i++;
      }
    }
    return List.copyOf(specs);
  }

  private static VarDataSpec parseOneVarData(final Token beginVarData) {
    if (beginVarData.signal() != Signal.BEGIN_VAR_DATA) {
      throw new IllegalStateException(
          "Expected BEGIN_VAR_DATA, got " + beginVarData.signal() + " for " + beginVarData.name());
    }
    return new VarDataSpec(
        beginVarData.name(),
        beginVarData.id(),
        PrimitiveType.UINT32,
        beginVarData.description() == null ? "" : beginVarData.description());
  }

  // ---------------------------------------------------------------------------------------
  // Naming helpers
  // ---------------------------------------------------------------------------------------

  /** Per-field private cache field name, e.g. {@code _tokenCache}. */
  private static String cacheFieldName(final VarDataSpec spec) {
    return "_" + spec.fieldName() + "Cache";
  }

  /**
   * Escape a schema-side description for safe inclusion in TS JSDoc. The close-comment delimiter
   * {@code asterisk-slash} would terminate JSDoc prematurely; replace it with an escaped variant.
   * Mirrors {@link EnumGenerator}'s identical helper to keep JSDoc emission consistent across
   * emitters.
   */
  private static String escapeJsDoc(final String description) {
    if (description.indexOf("*/") < 0) {
      return description;
    }
    return description.replace("*/", "*\\/");
  }

  // ---------------------------------------------------------------------------------------
  // Records
  // ---------------------------------------------------------------------------------------

  /**
   * Parsed var-data field metadata.
   *
   * @param fieldName schema field name (also the emitted TS getter name and the basis of the {@code
   *     _<fieldName>Cache} private field)
   * @param id SBE schema field id
   * @param lengthType width of the length prefix; always {@link PrimitiveType#UINT32} for the
   *     project's {@code varDataEncoding} composite, but kept on the spec so a future
   *     varDataEncoding variant could be supported without code surgery
   * @param description schema {@code description} attribute, used as the leading line of the
   *     emitted JSDoc; empty string if the schema declares none
   */
  record VarDataSpec(String fieldName, int id, PrimitiveType lengthType, String description) {}
}
