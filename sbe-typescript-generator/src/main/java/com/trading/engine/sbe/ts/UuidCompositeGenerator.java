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
 * Emits TypeScript getters for the SBE {@code uuid} composite (two-int64 halves) used by {@code
 * WebSocketAuthAck.sessionId}, {@code SessionResume.sessionId}, and {@code
 * WebSocketSnapshot.snapshotId} in the project's schema.
 *
 * <h2>Output shape</h2>
 *
 * Each {@code <field type="uuid"/>} produces one getter on its parent decoder class:
 *
 * <pre>{@code
 * sessionId(): UuidValue {
 *   return {
 *     msb: this.buffer.getBigInt64(this.bufferOffset + 0, true),
 *     lsb: this.buffer.getBigInt64(this.bufferOffset + 8, true),
 *   };
 * }
 * }</pre>
 *
 * The {@code UuidValue} interface ({@code { msb: bigint; lsb: bigint }}) is exported from {@code
 * _codecRuntime.ts} alongside {@link MessageGenerator}'s {@code readFixedString} helper — see
 * {@link #emitUuidValueTypeAlias(StringBuilder)}. A new {@code UuidValue} object is allocated per
 * call (no cache); consumers who need caching should hold the result themselves. One allocation per
 * call is acceptable here — uuid fields appear in a small handful of websocket control templates,
 * not on the high-throughput trade ingest path.
 *
 * <h2>Stringify semantics — render-edge only</h2>
 *
 * Per the chunk plan ({@code ~/.claude/plans/3-1b-sprightly-falcon.md} §"Schema features"), uuid is
 * decoded to {@code { msb: bigint; lsb: bigint }} and stringified ONLY at the rendering edge.
 * Decoders MUST NOT format the value — bigint preserves the full 128-bit identity round-trip
 * without precision loss; the consumer's UI layer is responsible for the RFC 4122 hex stringify if
 * displayed.
 *
 * <h2>Wire layout</h2>
 *
 * SBE's {@code uuid} composite is two {@code int64} fields: {@code mostSignificantBits} at offset 0
 * + {@code leastSignificantBits} at offset 8 = 16 bytes total. Both little-endian per the schema's
 * global {@code byteOrder="littleEndian"}. The composite's BEGIN_FIELD offset is the byte position
 * within the parent block — root block for top-level uuid fields, record block for nested-in-group
 * (none today, but emit logic is symmetric).
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
 * @see BlockField
 */
final class UuidCompositeGenerator {

  /** Newline used in emitted TypeScript. */
  private static final String NL = "\n";

  /** Wire size of the {@code uuid} composite: 2 × int64 = 16 bytes. */
  static final int UUID_BYTES = 16;

  /** Schema-side composite type name we recognise. Anything else is an error today. */
  static final String UUID_TYPE_NAME = "uuid";

  /**
   * Byte offset of {@code leastSignificantBits} within the uuid composite (after the 8-byte msb).
   */
  private static final int LSB_OFFSET = 8;

  /** Constructor — no state. */
  UuidCompositeGenerator() {
    // intentionally empty
  }

  /**
   * Emit the {@code UuidValue} TypeScript interface into the shared {@code _codecRuntime.ts}.
   * Called once per build by {@link MessageGenerator#writeCodecRuntime} alongside the {@code
   * readFixedString} runtime helper.
   *
   * @param sb the {@code _codecRuntime.ts} StringBuilder
   */
  static void emitUuidValueTypeAlias(final StringBuilder sb) {
    sb.append(NL)
        .append("/**")
        .append(NL)
        .append(" * RFC 4122 UUID as two int64 halves. Used by SBE composite-typed fields")
        .append(NL)
        .append(" * (e.g. `WebSocketAuthAck.sessionId`). Stringify via your render edge — do")
        .append(NL)
        .append(" * NOT format inside the decoder; bigint preserves the full 128-bit identity")
        .append(NL)
        .append(" * round-trip without precision loss.")
        .append(NL)
        .append(" */")
        .append(NL)
        .append("export interface UuidValue {")
        .append(NL)
        .append("  readonly msb: bigint;")
        .append(NL)
        .append("  readonly lsb: bigint;")
        .append(NL)
        .append("}")
        .append(NL);
  }

  /**
   * Emit one uuid getter for a {@link BlockField} of {@link BlockFieldKind#UUID_COMPOSITE}.
   *
   * <p>The two access expressions are parameters so this helper works in both contexts:
   *
   * <ul>
   *   <li>Message root block: {@code bufferRef = "this.buffer"}, {@code offsetExpr =
   *       "this.bufferOffset"}.
   *   <li>Group record block: {@code bufferRef = "this.parent._getBuffer()"}, {@code offsetExpr =
   *       "this.recordOffset"}.
   * </ul>
   *
   * @param field the parsed uuid field
   * @param bufferRef TypeScript expression yielding the {@code DataView} to read from
   * @param offsetExpr TypeScript expression yielding the base byte offset of the field's parent
   *     block
   * @return the emitted getter (multi-line)
   */
  static String emitGetter(
      final BlockField field, final String bufferRef, final String offsetExpr) {
    final var sb = new StringBuilder(384);
    sb.append("  /**")
        .append(NL)
        .append("   * Decode the SBE `uuid` composite at byte offset ")
        .append(field.offset())
        .append(" — two `int64` halves (mostSignificantBits + leastSignificantBits,")
        .append(NL)
        .append("   * little-endian). Returns a fresh {@link UuidValue} object per call;")
        .append(NL)
        .append("   * consumers who need caching should hold the result themselves. Do not")
        .append(NL)
        .append("   * stringify inside the decoder — defer RFC 4122 formatting to the render edge.")
        .append(NL)
        .append("   */")
        .append(NL);
    sb.append("  ").append(field.fieldName()).append("(): UuidValue {").append(NL);
    sb.append("    return {").append(NL);
    sb.append("      msb: ")
        .append(bufferRef)
        .append(".getBigInt64(")
        .append(offsetExpr)
        .append(" + ")
        .append(field.offset())
        .append(", true),")
        .append(NL);
    sb.append("      lsb: ")
        .append(bufferRef)
        .append(".getBigInt64(")
        .append(offsetExpr)
        .append(" + ")
        .append(field.offset() + LSB_OFFSET)
        .append(", true),")
        .append(NL);
    sb.append("    };").append(NL);
    sb.append("  }").append(NL);
    return sb.toString();
  }
}
