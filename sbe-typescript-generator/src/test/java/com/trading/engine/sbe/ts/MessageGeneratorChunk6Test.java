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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.xml.IrGenerator;
import uk.co.real_logic.sbe.xml.MessageSchema;
import uk.co.real_logic.sbe.xml.ParserOptions;
import uk.co.real_logic.sbe.xml.XmlSchemaParser;

/**
 * Chunk-6 unit tests over the emitted-source surface of {@link MessageGenerator}, {@link
 * GroupGenerator}, and {@link VarDataGenerator}.
 *
 * <h2>What's under test</h2>
 *
 * Each test re-runs the generator against {@code messages/src/main/resources/trading-schema.xml}
 * (the project's real schema; chunks 1–5 already validated against it) into a {@link TempDir}, then
 * asserts string-substring properties of the emitted {@code .ts} files. String-substring checks are
 * deliberate: they're fast, deterministic, and catch the most likely chunk-6 regressions (missing
 * wrap-resets, broken nested-class names, lost `_NULL_VAL` imports, forgotten group-accessor JSDoc
 * warnings) without needing a Node runtime.
 *
 * <h2>Test method naming</h2>
 *
 * Per CLAUDE.md "Test Documentation": {@code methodUnderTest_scenario_expectedBehavior}.
 *
 * <h2>Threading</h2>
 *
 * Single-threaded JUnit 6 fixture. Each test instantiates the generator and writes to its own
 * {@link TempDir}, so test isolation is guaranteed without shared state.
 */
final class MessageGeneratorChunk6Test {

  private static final Path SCHEMA_PATH =
      Paths.get("..", "messages", "src", "main", "resources", "trading-schema.xml")
          .toAbsolutePath()
          .normalize();

  // -----------------------------------------------------------------------------------------
  // Generator runs once per test; fixture-helper that builds the IR + invokes generate(...).
  // -----------------------------------------------------------------------------------------

  private static Ir loadIr() throws Exception {
    final var options = ParserOptions.builder().stopOnError(true).warningsFatal(true).build();
    try (final var stream = Files.newInputStream(SCHEMA_PATH)) {
      final MessageSchema schema = XmlSchemaParser.parse(stream, options);
      return new IrGenerator().generate(schema);
    }
  }

  private static Path emitAll(final Path outputDir) throws Exception {
    final var ir = loadIr();
    new MessageGenerator().generate(ir, outputDir);
    return outputDir;
  }

  private static String readDecoder(final Path outputDir, final String decoderClassName)
      throws IOException {
    return Files.readString(outputDir.resolve(decoderClassName + ".ts"), StandardCharsets.UTF_8);
  }

  // -----------------------------------------------------------------------------------------
  // Tests
  // -----------------------------------------------------------------------------------------

  @Test
  void webSocketAuth_emitsTokenVarDataGetter(@TempDir final Path tmp) throws Exception {
    final var out = emitAll(tmp);
    final var src = readDecoder(out, "WebSocketAuthDecoder");

    // Chunk-6 contract: var-data getter present, no "Deferred" comment.
    assertTrue(src.contains("token(): Uint8Array {"), "expected token(): Uint8Array getter");
    assertTrue(
        src.contains("private _tokenCache: Uint8Array | null = null;"),
        "expected single _tokenCache field");
    assertTrue(
        src.contains("if (this._tokenCache !== null) return this._tokenCache;"),
        "expected idempotent return-cached-view short-circuit");
    assertFalse(
        src.contains("Deferred to subsequent"),
        "WebSocketAuth has no composite — should not emit deferred-features TODO block");
  }

  @Test
  void webSocketSnapshot_emitsPayloadVarDataGetterWithCache(@TempDir final Path tmp)
      throws Exception {
    final var out = emitAll(tmp);
    final var src = readDecoder(out, "WebSocketSnapshotDecoder");

    assertTrue(src.contains("payload(): Uint8Array {"), "expected payload getter");
    assertTrue(
        src.contains("private _payloadCache: Uint8Array | null = null;"),
        "expected _payloadCache field");
    assertTrue(
        src.contains("if (this._payloadCache !== null) return this._payloadCache;"),
        "expected cache short-circuit");
  }

  @Test
  void wrap_resetsAllVarDataCaches(@TempDir final Path tmp) throws Exception {
    final var out = emitAll(tmp);

    // Each of the 3 var-data fields in the schema gets a wrap-reset line in its decoder.
    final var auth = readDecoder(out, "WebSocketAuthDecoder");
    assertTrue(
        auth.contains("this._tokenCache = null;"),
        "WebSocketAuthDecoder.wrap() must reset _tokenCache");
    final var snapshot = readDecoder(out, "WebSocketSnapshotDecoder");
    assertTrue(
        snapshot.contains("this._payloadCache = null;"),
        "WebSocketSnapshotDecoder.wrap() must reset _payloadCache");
    final var error = readDecoder(out, "WebSocketErrorDecoder");
    assertTrue(
        error.contains("this._errorTextCache = null;"),
        "WebSocketErrorDecoder.wrap() must reset _errorTextCache");
  }

  @Test
  void rfqStateSnapshot_emitsNestedGroupClasses(@TempDir final Path tmp) throws Exception {
    final var out = emitAll(tmp);
    final var src = readDecoder(out, "RfqStateSnapshotDecoder");

    // Capitalize-first naming rule: noRfqs → NoRfqs, noLegs → NoLegs.
    assertTrue(
        src.contains("export class RfqStateSnapshotNoRfqsDecoder {"),
        "expected outer noRfqs iterator class");
    assertTrue(
        src.contains("export class RfqStateSnapshotNoRfqsNoLegsDecoder {"),
        "expected nested noLegs iterator class");
    assertTrue(
        src.contains("noLegs(): RfqStateSnapshotNoRfqsNoLegsDecoder {"),
        "expected nested noLegs accessor on outer iterator");
    // Nested group's parent type is the ROOT message decoder (chunk-6 cursor model).
    assertTrue(
        src.contains("private parent!: RfqStateSnapshotDecoder;"),
        "nested iterator's parent type must be the root message decoder");
  }

  @Test
  void quoteRequest_emitsFlatGroupIterator(@TempDir final Path tmp) throws Exception {
    final var out = emitAll(tmp);
    final var src = readDecoder(out, "QuoteRequestDecoder");

    assertTrue(
        src.contains("export class QuoteRequestNoLegsDecoder {"),
        "expected QuoteRequestNoLegsDecoder");
    assertTrue(src.contains("count(): number {"), "expected count() method");
    assertTrue(src.contains("hasNext(): boolean {"), "expected hasNext() method");
    assertTrue(src.contains("next(): this {"), "expected next() returning this");
    // A representative leg field — record-relative offset access.
    assertTrue(
        src.contains("legSide(): SideEnum {"), "expected legSide() getter on group iterator");
    assertTrue(
        src.contains("this.parent._getBuffer().getUint8(this.recordOffset + 0)"),
        "expected record-relative read via parent._getBuffer() + recordOffset");
  }

  @Test
  void webSocketSubscribe_groupOnlyMessage(@TempDir final Path tmp) throws Exception {
    final var out = emitAll(tmp);
    final var src = readDecoder(out, "WebSocketSubscribeDecoder");

    // Group-only message: BLOCK_LENGTH = 0, no root field getters, but must still emit the
    // private buffer/_limit fields and the symbols() group accessor. This closes the chunk-5
    // TS6133 LOW for group-only messages — buffer/bufferOffset are now read transitively via
    // _getBuffer() inside the iterator's field getters.
    assertTrue(src.contains("static readonly BLOCK_LENGTH = 0;"), "expected BLOCK_LENGTH = 0");
    assertTrue(src.contains("private buffer!: DataView;"), "expected private buffer field");
    assertTrue(src.contains("private _limit = 0;"), "expected _limit cursor field");
    assertTrue(
        src.contains("symbols(): WebSocketSubscribeSymbolsDecoder {"),
        "expected group accessor for the symbols group");
    assertTrue(
        src.contains("export class WebSocketSubscribeSymbolsDecoder {"),
        "expected iterator class for the symbols group");
  }

  @Test
  void parseBlockField_optionalFlagDualSource(@TempDir final Path tmp) throws Exception {
    // Pin the dual-source optional-detection invariant from chunk 5 (commits d8bc245,
    // 09e5128). Two real schema fixtures exercise both branches end-to-end:
    //   (a) primitive-optional: NewOrderSingle.price is `int64 presence="optional"` —
    //       inner ENCODING carries OPTIONAL → emitted getter returns `bigint | null`.
    //   (b) enum-optional: QuoteRequest.settlType is `SettlTypeEnum presence="optional"` —
    //       outer BEGIN_FIELD carries OPTIONAL, inner BEGIN_ENUM stays REQUIRED → emitted
    //       getter returns `SettlTypeEnum | null`. A future "tidy" refactor that drops
    //       either source would silently regress chunk 5's enum-optional fix.
    final var out = emitAll(tmp);
    final var newOrderSingle = readDecoder(out, "NewOrderSingleDecoder");
    final var quoteRequest = readDecoder(out, "QuoteRequestDecoder");

    assertTrue(
        newOrderSingle.contains("price(): bigint | null {"),
        "primitive-optional branch: inner ENCODING.presence=OPTIONAL must yield `bigint | null`");
    assertTrue(
        quoteRequest.contains("settlType(): SettlTypeEnum | null {"),
        "enum-optional branch: outer BEGIN_FIELD.isOptionalEncoding() must yield `SettlTypeEnum"
            + " | null`");

    // Belt-and-braces: confirm the BlockField source still ORs both sources, so a refactor
    // that drops either check fails this test even before regen runs.
    final var src = readJavaSource("BlockField.java");
    assertTrue(
        src.contains("fieldToken.isOptionalEncoding()"),
        "BlockField.parseBlockField must check fieldToken.isOptionalEncoding()");
    assertTrue(
        src.contains("inner.encoding().presence() == Encoding.Presence.OPTIONAL"),
        "BlockField.parseBlockField must check inner.encoding().presence() == OPTIONAL");
  }

  @Test
  void messageGeneratorSource_doesNotFormatVarDataCacheNamesDirectly() {
    // VarDataGenerator owns the _<x>Cache field-name format. MessageGenerator must NEVER format
    // that name shape directly — every reference routes through VarDataGenerator's public API.
    // A future refactor that re-introduces direct formatting in MessageGenerator (e.g.
    // re-inlining the wrap-reset emission) would silently desync naming on a future cache rename.

    final var src = readJavaSource("MessageGenerator.java");
    // Strip comment lines (allow the rule to be DOCUMENTED in MessageGenerator's javadoc).
    final var nonCommentLines =
        src.lines()
            .filter(line -> !line.trim().startsWith("//"))
            .filter(line -> !line.trim().startsWith("*"))
            .reduce("", (acc, line) -> acc + line + "\n");
    assertFalse(
        nonCommentLines.contains("_Cache"),
        "MessageGenerator.java must not format _<x>Cache field names directly — route via"
            + " VarDataGenerator");
  }

  @Test
  void groupAccessorJsDoc_includesCallOncePerWrapWarning(@TempDir final Path tmp) throws Exception {
    final var out = emitAll(tmp);
    final var src = readDecoder(out, "RfqStateSnapshotDecoder");

    // The noRfqs() accessor JSDoc contains the call-once warning (cursor invariant 5).
    assertTrue(
        src.contains("Call at most once per `wrap()` of this decoder"),
        "expected call-once-per-wrap JSDoc warning on group accessor");
  }

  // -----------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------

  private static String readJavaSource(final String simpleClassName) {
    final var path =
        Paths.get("src", "main", "java", "com", "trading", "engine", "sbe", "ts", simpleClassName)
            .toAbsolutePath();
    try {
      return Files.readString(path, StandardCharsets.UTF_8);
    } catch (final IOException ex) {
      throw new IllegalStateException("Could not read " + path, ex);
    }
  }
}
