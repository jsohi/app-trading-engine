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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import uk.co.real_logic.sbe.ir.Signal;
import uk.co.real_logic.sbe.ir.Token;

/**
 * Emits SBE repeating-group iterator classes into the per-message TypeScript decoder file.
 *
 * <h2>Output shape</h2>
 *
 * For every {@code <group>} declared inside a message (and recursively for nested groups), this
 * emitter writes one class: {@code <Path>Decoder}, where {@code Path} concatenates the message name
 * with the capitalize-first-applied group-name chain (e.g. {@code QuoteRequestNoLegsDecoder},
 * {@code RfqStateSnapshotNoRfqsDecoder}, {@code RfqStateSnapshotNoRfqsNoLegsDecoder}). Each emitted
 * class exposes:
 *
 * <ul>
 *   <li>{@code wrap(parent)} — reads the {@code groupSizeEncoding} dimensions ({@code blockLength}
 *       + {@code numInGroup}, both {@code uint16}) at the parent's current limit, captures them,
 *       resets the iteration index, and advances the parent's limit past the dimension header.
 *   <li>{@code count()} — total records (cached at {@code wrap} time; does NOT advance the cursor).
 *   <li>{@code hasNext()} — true while there are more records to iterate.
 *   <li>{@code next()} — advance to the next record. Captures {@code recordOffset} from the
 *       parent's current limit and bumps the parent's limit by the per-record block length.
 *   <li>One getter per record-block field, dispatching on the SBE token signal as in {@link
 *       MessageGenerator}: primitive, enum (with optional-presence sentinel handling), fixed-length
 *       char-array via {@code readFixedString}.
 *   <li>Nested-group accessor methods — return a pre-allocated nested group iterator already
 *       wrapped to the same parent message decoder. Nested groups consume the same {@code _limit}
 *       cursor on the root message decoder so var-data positioned after the outer group still finds
 *       the correct start.
 * </ul>
 *
 * <h2>Cursor model — shared parent {@code _limit}</h2>
 *
 * Every group iterator (top-level or nested) holds a typed reference to the <em>root</em> {@code
 * <MessageName>Decoder} and reads/writes the cursor via the parent's package-private {@code
 * _getBuffer()} / {@code _getLimit()} / {@code _setLimit(...)} accessors — the same idiom Aeron's
 * Java SBE codec uses. Field getters reference the current record start as {@code this.recordOffset
 * + fieldOffset}, where {@code recordOffset} is captured at {@code next()} time and stays stable
 * until the next {@code next()} call (so subsequent nested-group reads or var-data reads can
 * advance {@code _limit} without invalidating field accesses on the current record).
 *
 * <h2>Cursor-ordering invariants enforced by emit-time JSDoc</h2>
 *
 * Per the chunk-6 plan, every emitted iterator class carries a JSDoc warning that the iterator MUST
 * be drained exhaustively when the parent message has var-data after the group (otherwise the
 * trailing var-data getter reads from the wrong wire offset). The drain warning is conditionally
 * emitted via {@link #messageHasVarDataAfterGroups(List)} so iterator classes for messages without
 * trailing var-data (the common case) keep the JSDoc terse.
 *
 * <h2>Threading model</h2>
 *
 * Not thread-safe. Build-time only.
 *
 * <h2>Allocation behavior</h2>
 *
 * Build-time only — allocates freely while constructing emitted source.
 *
 * <h2>Forward-reference soundness</h2>
 *
 * The parent decoder's cached iterator field initializer (e.g. {@code private _noRfqsGroup = new
 * RfqStateSnapshotNoRfqsDecoder();}) references a class declared later in the same module. Sound
 * under ES2022 module semantics: top-level class declarations create hoisted module-scope bindings,
 * and field initializers evaluate at instance construction (after the entire module has been
 * evaluated). Verified against the TC39 spec; chunk-13 {@code RoundTripIT} exercises it at runtime.
 *
 * @see MessageGenerator
 * @see VarDataGenerator
 * @see BlockField
 */
final class GroupGenerator {

  /** Newline used in emitted TypeScript. */
  private static final String NL = "\n";

  /** SBE group dimension header: 2-byte uint16 blockLength + 2-byte uint16 numInGroup. */
  private static final int GROUP_HEADER_SIZE = 4;

  /** Constructor — no state. */
  GroupGenerator() {
    // intentionally empty
  }

  /**
   * Emit both (a) the in-class accessor methods + cached-iterator-instance fields into the parent
   * message decoder's class body, and (b) the iterator class declarations themselves (one per
   * group, depth-first for nested groups) appended after the parent class's closing brace.
   *
   * @param groups parsed top-level group specs for this message; each may carry nested children
   * @param ctx emit context (root message name, parent class name, two destination string builders)
   * @param hasTrailingVarData true if the parent message declares any var-data field — controls
   *     conditional emission of the drain-on-trailing-varData JSDoc line on every iterator class in
   *     the tree
   */
  void emit(
      final List<GroupSpec> groups, final GroupEmitContext ctx, final boolean hasTrailingVarData) {
    Objects.requireNonNull(groups, "groups");
    Objects.requireNonNull(ctx, "ctx");
    if (groups.isEmpty()) {
      return;
    }

    // Cached-iterator-instance fields go before the wrap() method's body in the parent class so
    // they're declared above the methods that reference them — matches chunk-5 idiom for
    // private fields. Emit one `private _<group>Group = new <Class>();` per top-level group.
    for (final var group : groups) {
      ctx.messageBody()
          .append(NL)
          .append("  private ")
          .append(privateFieldName(group))
          .append(" = new ")
          .append(group.qualifiedClassName())
          .append("();")
          .append(NL);
    }

    // Group accessor methods follow the field getters in the parent class body.
    for (final var group : groups) {
      ctx.messageBody().append(NL);
      ctx.messageBody()
          .append("  /**")
          .append(NL)
          .append("   * Iterator accessor for the SBE repeating group `")
          .append(group.name())
          .append("` (id=")
          .append(group.id())
          .append(", blockLength=")
          .append(group.blockLength())
          .append(").")
          .append(NL)
          .append("   *")
          .append(NL)
          .append("   * Call at most once per `wrap()` of this decoder. Each call re-reads the")
          .append(NL)
          .append("   * group's dimension header from the current `_limit`; calling twice after")
          .append(NL)
          .append("   * iteration advances the cursor would interpret subsequent bytes (next")
          .append(NL)
          .append("   * group's dimensions or var-data length) as group dimensions, silently")
          .append(NL)
          .append("   * corrupting iteration.")
          .append(NL)
          .append("   */")
          .append(NL)
          .append("  ")
          .append(group.name())
          .append("(): ")
          .append(group.qualifiedClassName())
          .append(" {")
          .append(NL)
          .append("    return this.")
          .append(privateFieldName(group))
          .append(".wrap(this);")
          .append(NL)
          .append("  }")
          .append(NL);
    }

    // Iterator class declarations — depth-first so nested classes appear in declaration order.
    for (final var group : groups) {
      emitOneGroupClass(group, ctx.parentClassName(), hasTrailingVarData, ctx.fileBody());
    }
  }

  /** Emit one group-iterator class plus, recursively, any nested group classes. */
  private static void emitOneGroupClass(
      final GroupSpec group,
      final String parentClassName,
      final boolean hasTrailingVarData,
      final StringBuilder sb) {
    final var className = group.qualifiedClassName();

    sb.append(NL).append(NL);
    sb.append("/**").append(NL);
    sb.append(" * Iterator for the SBE repeating group `")
        .append(group.name())
        .append("` (id=")
        .append(group.id())
        .append(", blockLength=")
        .append(group.blockLength())
        .append(") inside `")
        .append(parentClassName)
        .append("`.")
        .append(NL);
    sb.append(" *").append(NL);
    sb.append(" * Iterate via `while (group.hasNext()) { group.next(); ... }`.").append(NL);
    sb.append(" * Field getters reference the current record (advanced by `next()`); do not")
        .append(NL);
    sb.append(" * retain return values past the next `next()` call.").append(NL);
    if (hasTrailingVarData) {
      sb.append(" *").append(NL);
      sb.append(" * If the parent message has var-data fields after this group, the iterator")
          .append(NL);
      sb.append(" * MUST be drained exhaustively before var-data getters fire; partial").append(NL);
      sb.append(" * iteration corrupts the cursor.").append(NL);
    }
    sb.append(" */").append(NL);
    sb.append("export class ").append(className).append(" {").append(NL);
    sb.append("  static readonly HEADER_SIZE = ").append(GROUP_HEADER_SIZE).append(";").append(NL);
    sb.append("  static readonly BLOCK_LENGTH = ")
        .append(group.blockLength())
        .append(";")
        .append(NL);
    sb.append(NL);
    sb.append("  private parent!: ").append(parentClassName).append(";").append(NL);
    sb.append("  private numInGroup = 0;").append(NL);
    sb.append("  private blockLengthRuntime = 0;").append(NL);
    sb.append("  private index = -1;").append(NL);
    // recordOffset is the absolute byte offset of the current record's start within the
    // buffer. Captured at next() time so field getters do not depend on parent._limit being
    // unchanged between next() and field reads (nested groups + var-data on the current record
    // would advance parent._limit further, but recordOffset stays stable).
    sb.append("  private recordOffset = 0;").append(NL);

    // Cached nested-group iterator instances (one per nested child group).
    for (final var nested : group.nestedGroups()) {
      sb.append("  private ")
          .append(privateFieldName(nested))
          .append(" = new ")
          .append(nested.qualifiedClassName())
          .append("();")
          .append(NL);
    }

    sb.append(NL);
    // wrap(parent) — read dimensions at parent._getLimit() and advance the limit past header.
    sb.append("  wrap(parent: ").append(parentClassName).append("): this {").append(NL);
    sb.append("    const buf = parent._getBuffer();").append(NL);
    sb.append("    const limit = parent._getLimit();").append(NL);
    sb.append("    this.parent = parent;").append(NL);
    sb.append("    this.blockLengthRuntime = buf.getUint16(limit, true);").append(NL);
    sb.append("    this.numInGroup = buf.getUint16(limit + 2, true);").append(NL);
    sb.append("    this.index = -1;").append(NL);
    sb.append("    this.recordOffset = 0;").append(NL);
    sb.append("    parent._setLimit(limit + ")
        .append(className)
        .append(".HEADER_SIZE);")
        .append(NL);
    sb.append("    return this;").append(NL);
    sb.append("  }").append(NL);

    sb.append(NL);
    sb.append("  count(): number {").append(NL);
    sb.append("    return this.numInGroup;").append(NL);
    sb.append("  }").append(NL);

    sb.append(NL);
    sb.append("  hasNext(): boolean {").append(NL);
    sb.append("    return this.index + 1 < this.numInGroup;").append(NL);
    sb.append("  }").append(NL);

    sb.append(NL);
    // next() — advance index, capture recordOffset = parent._limit, advance parent._limit past
    // this record's primitive block. Nested groups + var-data inside the record (none today,
    // but the schema could add them) would mutate parent._limit further before the next next()
    // call; recordOffset is therefore recomputed each call.
    sb.append("  next(): this {").append(NL);
    sb.append("    if (!this.hasNext()) {").append(NL);
    sb.append("      throw new Error(\"No more elements in group `")
        .append(group.name())
        .append("`\");")
        .append(NL);
    sb.append("    }").append(NL);
    sb.append("    this.index++;").append(NL);
    sb.append("    this.recordOffset = this.parent._getLimit();").append(NL);
    sb.append("    this.parent._setLimit(this.recordOffset + this.blockLengthRuntime);").append(NL);
    sb.append("    return this;").append(NL);
    sb.append("  }").append(NL);

    // Field getters (record-relative offsets).
    for (final var field : group.fields()) {
      sb.append(NL).append(emitGroupFieldGetter(field));
    }

    // Nested group accessor methods — return the cached nested iterator wrapped on the SAME
    // root parent (the _limit cursor lives on the root message decoder, regardless of nesting
    // depth).
    for (final var nested : group.nestedGroups()) {
      sb.append(NL);
      sb.append("  /**")
          .append(NL)
          .append("   * Iterator accessor for the nested SBE group `")
          .append(nested.name())
          .append("` (id=")
          .append(nested.id())
          .append(", blockLength=")
          .append(nested.blockLength())
          .append(") inside this record.")
          .append(NL)
          .append("   *")
          .append(NL)
          .append("   * Call at most once per `next()` of the outer group; calling twice after")
          .append(NL)
          .append("   * iteration advances the cursor would silently corrupt subsequent reads.")
          .append(NL)
          .append("   */")
          .append(NL);
      sb.append("  ")
          .append(nested.name())
          .append("(): ")
          .append(nested.qualifiedClassName())
          .append(" {")
          .append(NL);
      sb.append("    return this.")
          .append(privateFieldName(nested))
          .append(".wrap(this.parent);")
          .append(NL);
      sb.append("  }").append(NL);
    }

    sb.append("}").append(NL);

    // Recurse depth-first so nested classes appear after their enclosing parent.
    for (final var nested : group.nestedGroups()) {
      emitOneGroupClass(nested, parentClassName, hasTrailingVarData, sb);
    }
  }

  // ---------------------------------------------------------------------------------------
  // Field getter emission inside a group iterator class
  //
  // Differs from MessageGenerator's getters because group fields read from
  // `this.parent._getBuffer().get<X>(this.recordOffset + N)` rather than
  // `this.buffer.get<X>(this.bufferOffset + N)`.
  // ---------------------------------------------------------------------------------------

  private static String emitGroupFieldGetter(final BlockField field) {
    return switch (field.kind()) {
      case PRIMITIVE -> emitGroupPrimitiveGetter(field);
      case ENUM -> emitGroupEnumGetter(field);
      case CHAR_ARRAY -> emitGroupCharArrayGetter(field);
    };
  }

  private static String emitGroupPrimitiveGetter(final BlockField field) {
    final var dataViewMethod = MessageGenerator.dataViewMethodFor(field.primitive());
    final var returnType = MessageGenerator.primitiveReturnType(field.primitive());
    final boolean optional = field.optional();
    final boolean needsEndianArg = field.primitive().size() > 1;

    final var sb = new StringBuilder(256);
    sb.append("  ").append(field.fieldName()).append("(): ");
    sb.append(optional ? returnType + " | null" : returnType);
    sb.append(" {").append(NL);
    sb.append("    const v = this.parent._getBuffer().")
        .append(dataViewMethod)
        .append("(this.recordOffset + ")
        .append(field.offset())
        .append(needsEndianArg ? ", true);" : ");")
        .append(NL);
    if (optional) {
      sb.append("    return v === ")
          .append(
              MessageGenerator.numericLiteral(
                  field.primitive(), field.encoding().applicableNullValue()))
          .append(" ? null : v;")
          .append(NL);
    } else {
      sb.append("    return v;").append(NL);
    }
    sb.append("  }").append(NL);
    return sb.toString();
  }

  private static String emitGroupEnumGetter(final BlockField field) {
    final var dataViewMethod = MessageGenerator.dataViewMethodFor(field.primitive());
    final var enumName = field.enumName();
    final boolean optional = field.optional();
    final boolean needsEndianArg = field.primitive().size() > 1;

    final var sb = new StringBuilder(256);
    sb.append("  ").append(field.fieldName()).append("(): ");
    sb.append(optional ? enumName + " | null" : enumName);
    sb.append(" {").append(NL);
    sb.append("    const v = this.parent._getBuffer().")
        .append(dataViewMethod)
        .append("(this.recordOffset + ")
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

  private static String emitGroupCharArrayGetter(final BlockField field) {
    final var sb = new StringBuilder(192);
    sb.append("  ").append(field.fieldName()).append("(): string {").append(NL);
    sb.append("    return readFixedString(this.parent._getBuffer(), this.recordOffset + ")
        .append(field.offset())
        .append(", ")
        .append(field.arrayLength())
        .append(");")
        .append(NL);
    sb.append("  }").append(NL);
    return sb.toString();
  }

  // ---------------------------------------------------------------------------------------
  // Token → GroupSpec parsing (depth-first)
  // ---------------------------------------------------------------------------------------

  /**
   * Parse all top-level groups declared inside a message.
   *
   * @param tokens the message's IR token list (BEGIN_MESSAGE … END_MESSAGE)
   * @param messageName the message name; used to qualify nested class names via the
   *     capitalize-first rule
   * @return ordered list of top-level groups (each carries any nested children)
   */
  static List<GroupSpec> parseGroups(final List<Token> tokens, final String messageName) {
    Objects.requireNonNull(tokens, "tokens");
    Objects.requireNonNull(messageName, "messageName");
    final var groups = new ArrayList<GroupSpec>();
    int i = 1; // skip BEGIN_MESSAGE
    while (i < tokens.size()) {
      final var token = tokens.get(i);
      switch (token.signal()) {
        case BEGIN_GROUP -> {
          groups.add(parseGroup(tokens, i, messageName, List.of()));
          i += token.componentTokenCount();
        }
        case BEGIN_FIELD, BEGIN_VAR_DATA -> i += token.componentTokenCount();
        case END_MESSAGE -> {
          return List.copyOf(groups);
        }
        default -> i++;
      }
    }
    return List.copyOf(groups);
  }

  /**
   * Recursively parse one group and any of its nested children.
   *
   * @param tokens the enclosing message's full token list
   * @param beginIndex index of the {@code BEGIN_GROUP} token starting this group
   * @param messageName root message name (for class-name qualification)
   * @param ancestorPath capitalize-first names of enclosing groups (e.g. {@code ["NoRfqs"]} when
   *     parsing {@code noRfqs.noLegs}); empty for top-level groups
   * @return parsed {@link GroupSpec}
   */
  private static GroupSpec parseGroup(
      final List<Token> tokens,
      final int beginIndex,
      final String messageName,
      final List<String> ancestorPath) {
    final var begin = tokens.get(beginIndex);
    if (begin.signal() != Signal.BEGIN_GROUP) {
      throw new IllegalStateException(
          "Expected BEGIN_GROUP at token index " + beginIndex + ", got " + begin.signal());
    }
    final var name = begin.name();
    final int id = begin.id();
    final int blockLength = begin.encodedLength();
    final int endExclusive = beginIndex + begin.componentTokenCount();

    final var fields = new ArrayList<BlockField>();
    final var nestedGroups = new ArrayList<GroupSpec>();

    final var nestedAncestorPath = new ArrayList<>(ancestorPath);
    nestedAncestorPath.add(capitalize(name));

    int i = beginIndex + 1;
    // First inner block is the dimension type (BEGIN_COMPOSITE for groupSizeEncoding). Skip it
    // via componentTokenCount — SBE 1.37.x always emits this block first inside a group.
    if (i < endExclusive && tokens.get(i).signal() == Signal.BEGIN_COMPOSITE) {
      i += tokens.get(i).componentTokenCount();
    }
    while (i < endExclusive) {
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
        case BEGIN_GROUP -> {
          nestedGroups.add(parseGroup(tokens, i, messageName, nestedAncestorPath));
          i += token.componentTokenCount();
        }
        case END_GROUP -> i++;
        default -> i++;
      }
    }

    final var qualifiedClassName = qualifiedClassNameFor(messageName, ancestorPath, name);
    return new GroupSpec(
        name, id, blockLength, qualifiedClassName, List.copyOf(fields), List.copyOf(nestedGroups));
  }

  // ---------------------------------------------------------------------------------------
  // Aggregations used by MessageGenerator to compute message-level imports
  // ---------------------------------------------------------------------------------------

  /** Distinct enum names referenced by all groups (recursively), in stable insertion order. */
  static List<String> collectEnumImports(final List<GroupSpec> groups) {
    final var enums = new LinkedHashSet<String>();
    for (final var group : groups) {
      collectEnumsRecursive(group, enums);
    }
    return List.copyOf(enums);
  }

  private static void collectEnumsRecursive(
      final GroupSpec group, final LinkedHashSet<String> sink) {
    for (final var field : group.fields()) {
      if (field.kind() == BlockFieldKind.ENUM) {
        sink.add(field.enumName());
      }
    }
    for (final var nested : group.nestedGroups()) {
      collectEnumsRecursive(nested, sink);
    }
  }

  /**
   * Whether any group in the tree declares a {@code char[N]} field needing {@code readFixedString}.
   */
  static boolean anyUsesFixedString(final List<GroupSpec> groups) {
    for (final var group : groups) {
      if (anyUsesFixedStringRecursive(group)) {
        return true;
      }
    }
    return false;
  }

  private static boolean anyUsesFixedStringRecursive(final GroupSpec group) {
    for (final var field : group.fields()) {
      if (field.kind() == BlockFieldKind.CHAR_ARRAY) {
        return true;
      }
    }
    for (final var nested : group.nestedGroups()) {
      if (anyUsesFixedStringRecursive(nested)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether any field across all groups (recursively) declares {@code presence="optional"} on the
   * named enum.
   */
  static boolean anyEnumOptional(final List<GroupSpec> groups, final String enumName) {
    for (final var group : groups) {
      if (anyEnumOptionalRecursive(group, enumName)) {
        return true;
      }
    }
    return false;
  }

  private static boolean anyEnumOptionalRecursive(final GroupSpec group, final String enumName) {
    for (final var field : group.fields()) {
      if (field.kind() == BlockFieldKind.ENUM
          && field.optional()
          && enumName.equals(field.enumName())) {
        return true;
      }
    }
    for (final var nested : group.nestedGroups()) {
      if (anyEnumOptionalRecursive(nested, enumName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * True when the message's IR token list contains any {@code BEGIN_VAR_DATA} after the last {@code
   * END_GROUP} (i.e. at least one var-data field follows all groups in declaration order). Used by
   * the iterator JSDoc to conditionally emit the drain-on-trailing-varData warning line. Today no
   * real schema message has both groups AND var-data, but the chunk-6 synthetic test fixture
   * exercises this case.
   */
  static boolean messageHasVarDataAfterGroups(final List<Token> messageTokens) {
    Objects.requireNonNull(messageTokens, "messageTokens");
    for (final var token : messageTokens) {
      if (token.signal() == Signal.BEGIN_VAR_DATA) {
        return true;
      }
    }
    return false;
  }

  // ---------------------------------------------------------------------------------------
  // Naming helpers
  // ---------------------------------------------------------------------------------------

  /**
   * Capitalize the first letter of an SBE name and preserve the rest verbatim. Schema names are
   * camelCase ({@code noLegs}, {@code noRfqs}) so first-letter-only is sufficient; ASCII-only names
   * mean {@link Character#toUpperCase(char)} is locale-independent for this purpose.
   */
  private static String capitalize(final String s) {
    if (s.isEmpty()) {
      return s;
    }
    return Character.toUpperCase(s.charAt(0)) + s.substring(1);
  }

  /**
   * Compose the qualified iterator class name as {@code
   * <messageName><Ancestor1><Ancestor2>...<GroupName>Decoder}, applying capitalize-first to each
   * ancestor segment and to the group name itself. Examples:
   *
   * <ul>
   *   <li>top-level {@code noLegs} in {@code QuoteRequest} → {@code QuoteRequestNoLegsDecoder}
   *   <li>{@code noRfqs} in {@code RfqStateSnapshot} → {@code RfqStateSnapshotNoRfqsDecoder}
   *   <li>nested {@code noLegs} inside {@code noRfqs} in {@code RfqStateSnapshot} → {@code
   *       RfqStateSnapshotNoRfqsNoLegsDecoder}
   * </ul>
   */
  private static String qualifiedClassNameFor(
      final String messageName, final List<String> ancestorPath, final String groupName) {
    final var sb = new StringBuilder(messageName.length() + 32);
    sb.append(messageName);
    for (final var ancestor : ancestorPath) {
      sb.append(ancestor);
    }
    sb.append(capitalize(groupName));
    sb.append("Decoder");
    return sb.toString();
  }

  /** Per-instance private field name used to cache the iterator (e.g. {@code _noLegsGroup}). */
  private static String privateFieldName(final GroupSpec group) {
    return "_" + group.name() + "Group";
  }

  // ---------------------------------------------------------------------------------------
  // Records
  // ---------------------------------------------------------------------------------------

  /**
   * Emit context — the four state-and-config items {@link GroupGenerator#emit} needs. Single record
   * collapses the prior 5-positional argument list. {@code rootMessageName} and {@code
   * parentClassName} hold the same value today (chunk 6); they may diverge in chunk 7 if {@code
   * UuidCompositeGenerator} introduces composite types that own nested fields, since the parent
   * type for an inner iterator could then differ from the root message name.
   *
   * @param rootMessageName the message name (e.g. {@code RfqStateSnapshot}); used to compose
   *     qualified iterator class names
   * @param parentClassName the type referenced from emitted code as the iterator's {@code parent:
   *     …} field (the root message decoder type today)
   * @param messageBody StringBuilder for the parent class body (cached-instance fields and accessor
   *     methods get appended here)
   * @param fileBody StringBuilder for the post-class file body (iterator class declarations get
   *     appended here)
   */
  record GroupEmitContext(
      String rootMessageName,
      String parentClassName,
      StringBuilder messageBody,
      StringBuilder fileBody) {}

  /**
   * Parsed group metadata — name, schema id, fixed block length, fully-qualified emitted class
   * name, record-block fields, and any nested groups (in declaration order).
   *
   * @param name SBE group name (e.g. {@code noLegs}, {@code noRfqs})
   * @param id SBE schema field id (e.g. NoLegs's FIX tag = 555)
   * @param blockLength per-record primitive block length in bytes (sum of record-block field
   *     sizes); does NOT include any nested-group dimension header or records
   * @param qualifiedClassName emitted TS iterator class name (e.g. {@code
   *     RfqStateSnapshotNoRfqsNoLegsDecoder})
   * @param fields the group's record-block fields
   * @param nestedGroups any nested {@code <group>}s declared inside this group's record block
   */
  record GroupSpec(
      String name,
      int id,
      int blockLength,
      String qualifiedClassName,
      List<BlockField> fields,
      List<GroupSpec> nestedGroups) {}
}
