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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.QuoteEncoder;
import com.trading.engine.messages.sbe.QuoteRequestEncoder;
import com.trading.engine.messages.sbe.QuoteStatusEnum;
import com.trading.engine.messages.sbe.RfqStateEnum;
import com.trading.engine.messages.sbe.RfqStateSnapshotEncoder;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import com.trading.engine.messages.sbe.WebSocketAuthAckEncoder;
import com.trading.engine.messages.sbe.WebSocketAuthEncoder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/**
 * Java↔TS byte-for-byte round-trip integration tests for APP-34's generated TypeScript decoders.
 * Exercises every shape of field the generator emits (root-block primitives, optional int64 with
 * null-sentinel, optional enum with null-sentinel, char-array with null-padding, var-data, uuid
 * composite, nested repeating groups) by encoding via {@code :messages}'s Java SBE encoder, hex-
 * spawning a {@code tsx} driver process that decodes via {@link
 * com.trading.engine.sbe.ts.RouterGenerator}-emitted {@code MessageRouter.route()}, and asserting
 * the JSON the driver emits matches the source-side values.
 *
 * <h2>What's under test</h2>
 *
 * <p>Three groups of tests:
 *
 * <ul>
 *   <li><b>Outer Java-only tests</b>: regression locks on the generated source surface (router
 *       throw format, driver discipline, barrel re-exports, encode-helper ordering). No tsx
 *       required — runs in any environment.
 *   <li><b>{@link TsxRoundTripTests} nested class</b>: spawns the {@code roundtrip-driver.ts}
 *       process per test, encodes a real SBE buffer, asserts the driver's JSON output matches.
 *       Skips if {@code tsx} or {@code tsc} is not present at {@code <rootProject>/node_modules/
 *       .bin/}.
 *   <li><b>{@code tsdocPropagation_routeJsDocVisibleInBarrel}</b>: spawns {@code tsc
 *       --emitDeclarationOnly} against the regenerated tree under a {@code @TempDir} and asserts
 *       the locked {@code MUST consume} JSDoc phrase forwards through the barrel re-export to the
 *       emitted {@code index.d.ts}.
 * </ul>
 *
 * <h2>Wire format (locked)</h2>
 *
 * <ul>
 *   <li>Hex: lowercase {@code [0-9a-f]} only, even length. {@link HexFormat#of()}.
 *   <li>JSON: bigints emitted as decimal strings via the driver's {@code bigintReplacer}; Java
 *       parses via {@code new BigInteger(node.asText())}. Never {@code JsonNode.asLong()} —
 *       silently truncates above 2^53.
 *   <li>Null sentinels: optional int64 / enum at SBE null sentinel decode to TS {@code null} → JSON
 *       {@code null} (NOT the string {@code "null"}). Java side branches on {@link
 *       JsonNode#isNull()}.
 *   <li>Stdout: driver writes exactly one {@code process.stdout.write(JSON.stringify(...))} call;
 *       no trailing newline. Java reads via the drained byte buffer in {@link
 *       #runProcessWithTimeout}.
 *   <li>Stdout + stderr: drained concurrently via virtual-thread pumps + joined before the buffers
 *       are read. Both the classic ProcessBuilder deadlock (parent waits on stdout EOF while child
 *       blocks on a full stderr pipe) and the timeout-bypass case (a hung child holding stdout open
 *       indefinitely) are prevented — see {@link #runProcessWithTimeout}.
 * </ul>
 *
 * <h2>Threading</h2>
 *
 * <p>Single-threaded JUnit 6 Jupiter. Each test allocates its own buffer + spawns its own driver
 * process; no shared state.
 *
 * <h2>Allocation</h2>
 *
 * <p>Build/test-time only — allocates freely while encoding buffers, hex-rendering, parsing JSON.
 * Not part of any production hot path.
 *
 * <h2>Cross-references</h2>
 *
 * @see com.trading.engine.sbe.ts.RouterGenerator
 * @see com.trading.engine.sbe.ts.HelpersGenerator
 * @see com.trading.engine.sbe.ts.UuidCompositeGenerator
 * @see com.trading.engine.sbe.ts.IndexBarrelGenerator
 */
final class RoundTripTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HexFormat HEX = HexFormat.of();

  /**
   * SBE optional-int64 null sentinel — applies uniformly to every {@code int64} field declared with
   * {@code presence="optional"} in {@code trading-schema.xml}. Locked at {@link Long#MIN_VALUE} by
   * the SBE specification. Using one named constant per call site makes the intent legible (vs.
   * reusing an unrelated field's {@code *NullValue()} accessor, which would silently encode the
   * wrong sentinel if that field's type ever changed to {@code uint64}).
   */
  private static final long INT64_OPTIONAL_NULL = Long.MIN_VALUE;

  /**
   * Resolves a project-directory system property set by {@code tasks.test}, asserting the property
   * was injected (i.e. the test was launched via Gradle, not a direct IDE runner). Caller-supplied
   * property name keeps the diagnostic specific while sharing the resolution / skip-on-missing
   * logic between {@link #moduleDir()} and {@link #rootDir()}.
   */
  private static Path requireSystemPropPath(final String propName) {
    final var prop = System.getProperty(propName);
    assumeTrue(
        prop != null,
        propName
            + " system property unset — run via `./gradlew :sbe-typescript-generator:test`, "
            + "not the IDE direct test runner. Configure your IDE to delegate test execution to Gradle.");
    return Path.of(prop);
  }

  /**
   * Resolves the module's project directory via the system property set by {@code tasks.test}. Used
   * by the outer Java-only tests to read the real {@code build.gradle.kts}, the driver source, and
   * the generated {@code MessageRouter.ts} / {@code index.ts}.
   */
  private static Path moduleDir() {
    return requireSystemPropPath("moduleProjectDir");
  }

  // ===========================================================================================
  // Outer Java-only tests (no tsx required)
  // ===========================================================================================

  /**
   * Regression lock for parent plan §4 MEDIUM #12: {@code MessageRouter.route()}'s unknown-
   * templateId throw expression must include enough header context for triage. String-substring on
   * the generated source — sufficient because the throw text is static.
   */
  @Test
  void route_unknownTemplateIdThrowsWithRichDiagnostics() throws IOException {
    final var router = Files.readString(moduleDir().resolve("build/generated-ts/MessageRouter.ts"));
    assertTrue(
        router.contains("Unknown SBE templateId"),
        "throw must include the locked phrase 'Unknown SBE templateId' for triage logs to grep");
    assertTrue(router.contains("${templateId}"), "throw must interpolate templateId");
    assertTrue(router.contains("at offset ${offset}"), "throw must interpolate decode offset");
    assertTrue(
        router.contains("schemaId=${headerDecoder.schemaId()}"),
        "throw must include schemaId for cross-version diagnosis");
    assertTrue(
        router.contains("schemaVersion=${headerDecoder.version()}"),
        "throw must include schemaVersion");
    assertTrue(
        router.contains("blockLength=${headerDecoder.blockLength()}"),
        "throw must include blockLength to distinguish layout-mismatch from id-mismatch");
  }

  /**
   * Driver MUST emit JSON exclusively via {@code process.stdout.write}; {@code console.log} is
   * async-buffered and injects a trailing newline that corrupts the Java-side {@code
   * readAllBytes()} parse. Locked here so a future driver edit cannot silently regress.
   */
  @Test
  void driverSource_usesStdoutWriteNotConsoleLog() throws IOException {
    final var driver = readDriverSource();
    assertTrue(
        driver.contains("process.stdout.write("),
        "driver must emit JSON via process.stdout.write(...) — not console.log (newline + buffering)");
    assertFalse(
        driver.contains("console.log"),
        "driver MUST NOT use console.log anywhere — corrupts the JSON-only-on-stdout contract");
  }

  /**
   * BigInt round-trip discipline: driver's replacer emits bigints as decimal strings via {@code
   * value.toString()}; {@code Number(value)} would silently truncate above 2^53. Locked here so a
   * future "simplification" cannot silently drop precision for int64 boundary values.
   */
  @Test
  void driverSource_bigintReplacerEmitsStringNotNumber() throws IOException {
    final var driver = readDriverSource();
    assertTrue(
        driver.contains("typeof value === \"bigint\""), "replacer must guard on bigint type");
    assertTrue(driver.contains("value.toString()"), "replacer must emit bigint via toString()");
    assertFalse(
        driver.contains("Number(value)"),
        "replacer MUST NOT use Number(value) — silent precision loss above 2^53");
  }

  /**
   * Hex input validation: {@code Buffer.from('hex')} silently truncates on invalid characters (e.g.
   * uppercase, non-hex). Driver must regex-validate the charset, not just length, to fail loudly on
   * encoder mistakes rather than corrupt the decode silently.
   */
  @Test
  void driverSource_validatesHexCharsetNotJustLength() throws IOException {
    final var driver = readDriverSource();
    assertTrue(
        driver.contains("/^[0-9a-f]+$/.test(hex)"),
        "driver must regex-validate hex charset (one-or-more, not zero-or-more — empty hex would otherwise produce a zero-byte DataView and a confusing RangeError downstream); Buffer.from('hex') silently truncates on invalid chars");
  }

  /**
   * Null-sentinel JSON contract: optional int64 / enum at SBE null sentinel must be emitted as JSON
   * {@code null}, NOT a translated string like {@code "null"}, {@code "—"}, {@code "N/A"}. Java
   * side branches on {@link JsonNode#isNull()} so any string substitute would silently mis-route
   * the assertion.
   */
  @Test
  void driverSource_emitsNullSentinelAsJsonNullNotString() throws IOException {
    final var driver = readDriverSource();
    assertFalse(
        driver.contains("\"null\""),
        "driver MUST NOT emit the literal string \"null\" — emit JSON null (TS null) so Java can branch on isNull()");
    assertFalse(
        driver.contains("|| 'null'"), "driver MUST NOT translate sentinels to a string fallback");
    assertFalse(
        driver.contains("?? 'null'"), "driver MUST NOT translate sentinels to a string fallback");
  }

  /**
   * The {@code __generation} dev-mode rebind counter is the locked aliasing-detection mechanism
   * (parent plan §4 lines 248–256). Must stay emitted in {@code MessageRouter.ts} so {@code
   * route_returnsSharedFlyweightThatRebindsOnNextCall} can observe a delta after each call when
   * {@code NODE_ENV=development}.
   */
  @Test
  void routerSource_emitsGenerationCounterIncrement() throws IOException {
    final var router = Files.readString(moduleDir().resolve("build/generated-ts/MessageRouter.ts"));
    assertTrue(
        router.contains("__generation"),
        "MessageRouter must emit the __generation rebind counter for aliasing-detection tests");
    assertTrue(
        router.contains("frame.__generation = (frame.__generation ?? 0) + 1"),
        "MessageRouter must increment __generation under the dev-mode guard");
  }

  /**
   * The {@code @trading/sbe-codecs} barrel must re-export the helpers ({@code toFixed8 /
   * parseFixed8 / nanosToDate}) and the router ({@code route / DecodedFrame / Decoder}). A future
   * regression dropping a re-export would surface as an opaque tsx ESM import error nested inside
   * one of the {@code roundTrip_*} tests; this string-assertion fails fast and loud instead.
   */
  @Test
  void barrelExportsHelpersAndRouter() throws IOException {
    final var barrel = Files.readString(moduleDir().resolve("build/generated-ts/index.ts"));
    assertTrue(
        barrel.contains("export { toFixed8, parseFixed8, nanosToDate } from \"./helpers.js\""),
        "barrel must re-export helpers — driver imports them for the toFixed8/parseFixed8 round-trip");
    assertTrue(
        barrel.contains(
            "export { route, type DecodedFrame, type Decoder } from \"./MessageRouter.js\""),
        "barrel must re-export route/DecodedFrame/Decoder — driver narrows decoder via the union");
  }

  /**
   * Static-text lock for parent plan §8 HIGH #6: {@link #encodeRfqStateSnapshot} MUST call {@code
   * noRfqsCount(...)} before {@code .next()} on the iterator. Off-order calls produce
   * malformed-but-decodable buffers that the decoder reads at wrong offsets — silently passing
   * tests. Locking the order via static-text assertion on this file keeps the encode discipline
   * traceable as the test class evolves.
   */
  @Test
  void encodeRfqStateSnapshot_callsCountBeforeNextInLockedOrder() throws IOException {
    final var src =
        Files.readString(
            moduleDir().resolve("src/test/java/com/trading/engine/sbe/ts/RoundTripTest.java"));
    // Build the search needles at runtime via concatenation so this method's own string literals
    // don't match — `src.indexOf("...")` would otherwise anchor to the literal inside this test
    // body before reaching the actual method declaration, leaving the substring empty of the
    // patterns we want to validate.
    final var openBrace = "{";
    final int rfqBlockStart =
        src.indexOf("private static byte[] encodeRfqStateSnapshot() " + openBrace);
    assertTrue(
        rfqBlockStart > 0, "encodeRfqStateSnapshot helper missing — required by chunk-13 plan");
    // Upper bound at the next method's signature; without this bound the substring runs to EOF and
    // a future unrelated helper that invokes the iterator before declaring the count would falsely
    // fail this assertion.
    final int rfqBlockEnd =
        src.indexOf("private static byte[] sliceToByteArray(" + "final", rfqBlockStart);
    assertTrue(
        rfqBlockEnd > rfqBlockStart,
        "sliceToByteArray helper missing — required as the upper bound of encodeRfqStateSnapshot's source");
    final var rfqBlock = src.substring(rfqBlockStart, rfqBlockEnd);
    final int countIdx = rfqBlock.indexOf(".noRfqsCount(");
    final int nextIdx = rfqBlock.indexOf(".next()");
    assertTrue(countIdx > 0, "encodeRfqStateSnapshot MUST call noRfqsCount(...)");
    assertTrue(nextIdx > 0, "encodeRfqStateSnapshot MUST call .next() on the iterator");
    assertTrue(
        countIdx < nextIdx,
        "encodeRfqStateSnapshot MUST call noRfqsCount(...) BEFORE .next() — off-order calls "
            + "produce malformed-but-decodable buffers that silently pass tests at wrong offsets");
  }

  // ===========================================================================================
  // @Nested TsxRoundTripTests — tsx-driven byte-for-byte parity tests
  // ===========================================================================================

  /**
   * Holds every test that requires {@code tsx} (or {@code tsc} for {@code tsdocPropagation_*}). The
   * {@code @BeforeEach} guard skips the entire group if the binaries are missing, with a clear
   * diagnostic pointing at {@code :web-ui:webUiInstall}. Default-on; opt-out via {@code
   * SKIP_TSX_TESTS=1}.
   */
  @Nested
  @DisabledIfEnvironmentVariable(
      named = "SKIP_TSX_TESTS",
      matches = "1",
      disabledReason =
          "Opt-out for environments without tsx; default-on so regressions surface locally.")
  class TsxRoundTripTests {

    @BeforeEach
    void requireTsxAndTsc() {
      assumeTrue(
          Files.exists(rootDir().resolve("node_modules/.bin/tsx")),
          "tsx not installed at <rootProject>/node_modules/.bin/tsx — run `:web-ui:webUiInstall`");
      assumeTrue(
          Files.exists(rootDir().resolve("node_modules/.bin/tsc")),
          "tsc not installed at <rootProject>/node_modules/.bin/tsc — run `:web-ui:webUiInstall`");
    }

    // ---------------------------------------------------------------------------------------
    // Round-trip tests — encode in Java, decode in tsx, assert JSON match
    // ---------------------------------------------------------------------------------------

    @Test
    void roundTrip_rfqStateSnapshot_nestedGroupsDecodeIdentically() throws Exception {
      final var bytes = encodeRfqStateSnapshot();
      final var json = runDriverSingle(bytes, RfqStateSnapshotEncoder.TEMPLATE_ID);
      assertEquals(RfqStateSnapshotEncoder.TEMPLATE_ID, json.get("templateId").asInt());

      final var rfqs = json.get("fields").get("noRfqs");
      assertEquals(1, rfqs.size(), "exactly one outer RFQ encoded");
      final var rfq = rfqs.get(0);
      assertEquals("RFQ-1", rfq.get("quoteReqId").asText());
      assertEquals("EUR/USD", rfq.get("symbol").asText());
      assertEquals(BigInteger.valueOf(42L), new BigInteger(rfq.get("accountId").asText()));

      final var legs = rfq.get("noLegs");
      assertEquals(2, legs.size(), "exactly two nested legs encoded");
      assertEquals(
          BigInteger.valueOf(100_000_000L),
          new BigInteger(legs.get(0).get("legOrderQty").asText()));
      assertEquals(
          BigInteger.valueOf(200_000_000L),
          new BigInteger(legs.get(1).get("legOrderQty").asText()));
    }

    @Test
    void roundTrip_webSocketAuth_varDataTokenDecodesIdentically() throws Exception {
      final var bytes = encodeWebSocketAuth(1, "hello");
      final var json = runDriverSingle(bytes, WebSocketAuthEncoder.TEMPLATE_ID);
      assertEquals(WebSocketAuthEncoder.TEMPLATE_ID, json.get("templateId").asInt());
      assertEquals(1, json.get("fields").get("protocolVersion").asInt());
      assertEquals("hello", json.get("fields").get("token").asText());
    }

    /**
     * UUID composite — encodes msb=0x123, lsb=0x456 and asserts the TS decoder produces the same
     * bigint pair through {@link com.trading.engine.sbe.ts.UuidCompositeGenerator}-emitted {@code
     * sessionId(): UuidValue} accessor.
     */
    @Test
    void roundTrip_webSocketAuthAck_uuidCompositeDecodesIdentically() throws Exception {
      final var bytes = encodeWebSocketAuthAck(0x123L, 0x456L, 1, 100);
      final var json = runDriverSingle(bytes, WebSocketAuthAckEncoder.TEMPLATE_ID);
      assertEquals(WebSocketAuthAckEncoder.TEMPLATE_ID, json.get("templateId").asInt());
      assertEquals(
          BigInteger.valueOf(0x123L),
          new BigInteger(json.get("fields").get("sessionIdMsb").asText()));
      assertEquals(
          BigInteger.valueOf(0x456L),
          new BigInteger(json.get("fields").get("sessionIdLsb").asText()));
      assertEquals(1, json.get("fields").get("protocolVersion").asInt());
      assertEquals(100, json.get("fields").get("maxSubscriptions").asInt());
    }

    /** Optional int64 at the SBE null sentinel decodes to JSON null. */
    @Test
    void roundTrip_quote_optionalInt64NullSentinelDecodesToNull() throws Exception {
      final var bytes = encodeQuote(108_520_000L, QuoteEncoder.swapPointsNullValue(), null);
      final var json = runDriverSingle(bytes, QuoteEncoder.TEMPLATE_ID);
      final var swapPoints = json.get("fields").get("swapPoints");
      assertNotNull(swapPoints, "swapPoints field must be present in JSON envelope");
      assertTrue(
          swapPoints.isNull(),
          "swapPoints at SBE null sentinel must decode to JSON null (not the string \"null\")");
    }

    /**
     * Optional int64 with explicit value of {@code 250_000n} — typical FX swap-points magnitude at
     * fixed-point 10^8 ({@code 0.0025}). Documents the schema-level expectation for downstream
     * consumers via the test's chosen value.
     */
    @Test
    void roundTrip_quote_optionalInt64ExplicitValueRoundTrips() throws Exception {
      final long swapPoints = 250_000L;
      final var bytes = encodeQuote(108_520_000L, swapPoints, null);
      final var json = runDriverSingle(bytes, QuoteEncoder.TEMPLATE_ID);
      assertEquals(
          BigInteger.valueOf(swapPoints),
          new BigInteger(json.get("fields").get("swapPoints").asText()));
    }

    /**
     * End-to-end exercise of the {@code helpers.ts} surface: driver decodes {@code bidPx}, calls
     * {@code toFixed8(bidPx) → "1.08520000"}, then {@code parseFixed8("1.08520000") → bigint}, and
     * emits both. Java asserts the round-trip equals the source — catches helper regressions via
     * the same gate that catches generator regressions.
     */
    @Test
    void roundTrip_quote_bidPxThroughHelpersToFixed8RoundTrips() throws Exception {
      // 1.08520000 — a typical FX rate; chosen to exercise the helpers' fractional handling.
      final long bidPx = 108_520_000L;
      final var bytes = encodeQuote(bidPx, QuoteEncoder.swapPointsNullValue(), null);
      final var json = runDriverSingle(bytes, QuoteEncoder.TEMPLATE_ID);
      final var fields = json.get("fields");
      assertEquals(BigInteger.valueOf(bidPx), new BigInteger(fields.get("bidPx").asText()));
      assertEquals(
          "1.08520000",
          fields.get("bidPxFixed8Display").asText(),
          "toFixed8 must render 1.08520000 in 8-fractional-digit canonical form");
      assertEquals(
          BigInteger.valueOf(bidPx),
          new BigInteger(fields.get("bidPxRoundTripped").asText()),
          "parseFixed8(toFixed8(b)) must equal b — locks helpers' round-trip identity");
    }

    /**
     * Boundary exercise: {@code bidPx = Long.MAX_VALUE} (signed int64 max). Driver emits via
     * bigintReplacer; Java parses via {@code new BigInteger(...)}. Catches a regression where
     * {@code bigintReplacer} is silently bypassed and {@code Number(value)} truncates above 2^53 —
     * the static driverSource lock guards the source, this guards the parsing path with a value
     * that would actually expose truncation.
     */
    @Test
    void roundTrip_quote_bidPxAtInt64MaxBoundaryRoundTripsWithoutTruncation() throws Exception {
      final long bidPx = Long.MAX_VALUE;
      final var bytes = encodeQuote(bidPx, QuoteEncoder.swapPointsNullValue(), null);
      final var json = runDriverSingle(bytes, QuoteEncoder.TEMPLATE_ID);
      final var decoded = new BigInteger(json.get("fields").get("bidPx").asText());
      assertEquals(
          BigInteger.valueOf(Long.MAX_VALUE),
          decoded,
          "int64 max must round-trip without truncation via the bigint-string JSON path");
    }

    @Test
    void roundTrip_quoteRequest_optionalEnumNullSentinelDecodesToNull() throws Exception {
      final var bytes = encodeQuoteRequest("EUR/USD", SettlTypeEnum.NULL_VAL);
      final var json = runDriverSingle(bytes, QuoteRequestEncoder.TEMPLATE_ID);
      final var settlType = json.get("fields").get("settlType");
      assertNotNull(settlType, "settlType field must be present in JSON envelope");
      assertTrue(
          settlType.isNull(),
          "settlType at SBE null sentinel must decode to JSON null (not the string \"null\")");
    }

    /**
     * Char-array null-padding: schema-fixed-width char arrays right-pad with NUL bytes; the TS
     * decoder's {@code readFixedString} helper trims trailing NULs. Encode {@code "EUR/USD"} into a
     * 16-byte field and assert the decoded string strips the NUL padding.
     */
    @Test
    void roundTrip_quoteRequest_charArraySymbolNullPaddingTrimmedCorrectly() throws Exception {
      final var bytes = encodeQuoteRequest("EUR/USD", SettlTypeEnum.Regular);
      final var json = runDriverSingle(bytes, QuoteRequestEncoder.TEMPLATE_ID);
      assertEquals(
          "EUR/USD",
          json.get("fields").get("symbol").asText(),
          "char-array decoder must strip trailing NUL padding");
    }

    // ---------------------------------------------------------------------------------------
    // route() flyweight + dispatch contract tests (tsx-driven)
    // ---------------------------------------------------------------------------------------

    /**
     * Aliasing-rebind contract: {@code route()} returns a SHARED flyweight; the SAME held
     * reference's fields rebind on the next call. Driver decodes A → captures, decodes B → captures
     * the SAME reference's state. Java asserts captureBefore reflects A and captureAfter reflects B
     * — proves rebind. Cannot be string-asserted: the contract is runtime aliasing.
     */
    @Test
    void route_returnsSharedFlyweightThatRebindsOnNextCall() throws Exception {
      final var bytesA = encodeWebSocketAuth(1, "hello");
      final var bytesB = encodeWebSocketAuth(2, "world");

      final var json = runDriverAlias(bytesA, bytesB);
      assertTrue(
          json.get("sameInstance").asBoolean(),
          "route() must return the same flyweight instance across calls");
      assertEquals(1, json.get("captureBefore").get("fields").get("protocolVersion").asInt());
      assertEquals(
          "hello",
          json.get("captureBefore").get("fields").get("token").asText(),
          "captureBefore must reflect buffer A");
      assertEquals(2, json.get("captureAfter").get("fields").get("protocolVersion").asInt());
      assertEquals(
          "world",
          json.get("captureAfter").get("fields").get("token").asText(),
          "captureAfter (read off the SAME held reference) must reflect buffer B — proves rebind");

      // Dev-mode counter delta — driver runs under NODE_ENV=development per the process contract.
      final var before = json.get("generationBefore");
      final var after = json.get("generationAfter");
      assertFalse(before.isNull(), "generationBefore must be populated under NODE_ENV=development");
      assertFalse(after.isNull(), "generationAfter must be populated under NODE_ENV=development");
      assertTrue(
          after.asLong() > before.asLong(),
          "__generation must increment between calls under NODE_ENV=development");
    }

    /**
     * Multi-message dispatch: one buffer holds two encoded messages at known offsets; {@code
     * route()} must dispatch correctly to two different decoders depending on templateId in the
     * header at each offset.
     */
    @Test
    void route_dispatchesAcrossMultipleTemplateIdsInSameBuffer() throws Exception {
      // Encode Quote at offset 0, WebSocketAuth at offset = quoteEnd.
      final var quoteBytes = encodeQuote(108_520_000L, QuoteEncoder.swapPointsNullValue(), null);
      final var authBytes = encodeWebSocketAuth(1, "hello");
      final var combined = new byte[quoteBytes.length + authBytes.length];
      System.arraycopy(quoteBytes, 0, combined, 0, quoteBytes.length);
      System.arraycopy(authBytes, 0, combined, quoteBytes.length, authBytes.length);

      final var json = runDriverMulti(combined, 0, quoteBytes.length);
      final var first = json.get("first");
      final var second = json.get("second");
      assertEquals(QuoteEncoder.TEMPLATE_ID, first.get("templateId").asInt());
      assertEquals(WebSocketAuthEncoder.TEMPLATE_ID, second.get("templateId").asInt());
      assertEquals(
          BigInteger.valueOf(108_520_000L),
          new BigInteger(first.get("fields").get("bidPx").asText()));
      assertEquals("hello", second.get("fields").get("token").asText());
    }

    // ---------------------------------------------------------------------------------------
    // tsdocPropagation — tsc declaration-emit asserts JSDoc forwards through the barrel
    // ---------------------------------------------------------------------------------------

    /**
     * Asserts the {@code "MUST consume"} JSDoc phrase on {@code route()} forwards through the
     * barrel re-export to the emitted {@code index.d.ts}. Spawns {@code tsc --emitDeclarationOnly}
     * against a copy of the regenerated tree to verify TypeScript's default JSDoc-forwarding still
     * reaches the consumer.
     */
    @Test
    void tsdocPropagation_routeJsDocVisibleInBarrel(@TempDir final Path tmp) throws Exception {
      // Copy regenerated tree to @TempDir (do NOT regen — :web-ui:webUiInstall already triggered
      // :sbe-typescript-generator:generateTsCodecs as a transitive dependency of tasks.test).
      final var generatedSrc = moduleDir().resolve("build/generated-ts");
      try (final var stream = Files.walk(generatedSrc)) {
        stream.forEach(
            src -> {
              // relativize/resolve don't throw; keep dst out of the try so the catch can cite it.
              final var rel = generatedSrc.relativize(src);
              final var dst = tmp.resolve(rel);
              try {
                if (Files.isDirectory(src)) {
                  Files.createDirectories(dst);
                } else {
                  Files.copy(src, dst);
                }
              } catch (final IOException ex) {
                throw new UncheckedIOException("copying " + src + " -> " + dst, ex);
              }
            });
      }

      // Minimal tsconfig — declaration-only, strict, ESNext modules with bundler resolution.
      // typeRoots resolves @types/node from the npm workspace because MessageRouter.ts
      // references process.env.NODE_ENV (the dev-mode __generation guard); without node
      // types tsc fails with TS2591 on `process`.
      final var typeRoots =
          rootDir().resolve("node_modules/@types").toAbsolutePath().toString().replace("\\", "/");
      Files.writeString(
          tmp.resolve("tsconfig.json"),
          "{\"compilerOptions\":{\"declaration\":true,\"emitDeclarationOnly\":true,"
              + "\"strict\":true,\"target\":\"ES2022\",\"module\":\"ESNext\","
              + "\"moduleResolution\":\"bundler\",\"outDir\":\"./types\","
              + "\"typeRoots\":[\""
              + typeRoots
              + "\"],\"types\":[\"node\"]}}");

      final var tscPath = rootDir().resolve("node_modules/.bin/tsc");
      final var pb =
          new ProcessBuilder(tscPath.toString(), "--project", "tsconfig.json")
              .directory(tmp.toFile())
              .redirectErrorStream(false);
      pb.redirectError(ProcessBuilder.Redirect.PIPE);
      pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
      pb.environment().keySet().removeIf(k -> k.startsWith("NODE_") || k.startsWith("TS_"));

      final var outputs = runProcessWithTimeout(pb, 60, TimeUnit.SECONDS);
      if (outputs.exitCode() != 0) {
        fail(
            "tsc exited "
                + outputs.exitCode()
                + "; stdout:\n"
                + outputs.stdoutAsString()
                + "\nstderr:\n"
                + outputs.stderrAsString());
      }

      final var indexDts = tmp.resolve("types/index.d.ts");
      assertTrue(Files.exists(indexDts), "tsc must emit index.d.ts under outDir");

      // TypeScript preserves JSDoc on the originating .d.ts (here MessageRouter.d.ts), and
      // consumer IDEs follow the re-export chain back to that file via type-symbol resolution.
      // The phrase is REQUIRED to remain in the emitted .d.ts surface so consumer tooling
      // (LSP/IDE hover, generated docs) still surfaces the flyweight-aliasing warning. Walk
      // every .d.ts under outDir and assert the phrase is reachable from the barrel-emit path.
      assertTrue(
          dtsTreeContainsPhrase(tmp.resolve("types"), "MUST consume"),
          "JSDoc 'MUST consume' phrase must survive in the emitted .d.ts surface (most likely "
              + "MessageRouter.d.ts, reached from index.d.ts via the barrel's type re-export). "
              + "Consumer IDEs follow the re-export chain and surface the flyweight-aliasing warning "
              + "on hover; if no .d.ts contains the phrase, that warning has been silently lost.");
    }
  }

  /**
   * Walk every {@code .d.ts} file under {@code typesDir} and return whether any contains {@code
   * phrase}. Extracted from {@link
   * TsxRoundTripTests#tsdocPropagation_routeJsDocVisibleInBarrel(Path)} so the lambda's caught
   * {@link IOException} can be wrapped without forcing the caller's local to be non-{@code final}.
   */
  private static boolean dtsTreeContainsPhrase(final Path typesDir, final String phrase)
      throws IOException {
    try (final var dtsStream = Files.walk(typesDir)) {
      return dtsStream
          .filter(p -> p.toString().endsWith(".d.ts"))
          .anyMatch(
              p -> {
                try {
                  return Files.readString(p).contains(phrase);
                } catch (final IOException ex) {
                  throw new RuntimeException(ex);
                }
              });
    }
  }

  // ===========================================================================================
  // Encode helpers — Java-side SBE encoders
  // ===========================================================================================

  /**
   * Fills the required-but-uninteresting fields of a Quote with sane defaults so tests can focus on
   * the field they actually assert against. Returns the encoded byte slice (header + message).
   *
   * @param bidPx fixed-point price; the test's main assertion target.
   * @param swapPoints raw int64 to write; pass {@link QuoteEncoder#swapPointsNullValue()} to test
   *     the null-sentinel path or an explicit value for the round-trip path.
   * @param settlType optional enum to write; pass {@code null} to default to {@link
   *     SettlTypeEnum#Regular} (sane non-sentinel value for the Quote tests, which assert other
   *     fields and only require the buffer to be well-formed). settlType-as-null-sentinel
   *     assertions live in the QuoteRequest test path where the test passes {@link
   *     SettlTypeEnum#NULL_VAL} explicitly.
   */
  private static byte[] encodeQuote(
      final long bidPx, final long swapPoints, final SettlTypeEnum settlType) {
    final var buffer = new ExpandableArrayBuffer(2048);
    final var header = new MessageHeaderEncoder();
    final var encoder = new QuoteEncoder();
    encoder
        .wrapAndApplyHeader(buffer, 0, header)
        .quoteReqId("RFQ-1")
        .quoteId("Q-1")
        .symbol("EUR/USD")
        .side(SideEnum.Buy)
        .bidPx(bidPx)
        .offerPx(bidPx + 100L)
        .bidSize(100_000_000L)
        .offerSize(100_000_000L)
        .transactTime(1_700_000_000_000_000_000L)
        .quoteStatus(QuoteStatusEnum.Accepted)
        .text("ok")
        .productType(ProductTypeEnum.Spot)
        .settlDate("20260101")
        .settlType(settlType == null ? SettlTypeEnum.Regular : settlType)
        .currency("USD")
        .settlCurrency("USD")
        .tenor(TenorEnum.ON)
        .validUntil(1_700_000_000_000_000_000L + 30_000_000_000L)
        .swapPoints(swapPoints)
        .noLegsCount(0);
    final int totalLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    return sliceToByteArray(buffer, totalLength);
  }

  /**
   * Encodes a QuoteRequest with no legs. Pass an explicit {@code settlType} (use {@link
   * SettlTypeEnum#NULL_VAL} for the null-sentinel test). The {@code symbol} char-array test relies
   * on the input being shorter than 16 bytes so trailing NUL padding is exercised.
   */
  private static byte[] encodeQuoteRequest(final String symbol, final SettlTypeEnum settlType) {
    final var buffer = new ExpandableArrayBuffer(1024);
    final var header = new MessageHeaderEncoder();
    final var encoder = new QuoteRequestEncoder();
    encoder
        .wrapAndApplyHeader(buffer, 0, header)
        .quoteReqId("RFQ-1")
        .symbol(symbol)
        .side(SideEnum.Buy)
        .orderQty(100_000_000L)
        .accountCode("ACC-1")
        .transactTime(1_700_000_000_000_000_000L)
        .productType(ProductTypeEnum.Spot)
        .settlDate("20260101")
        .settlType(settlType)
        .currency("USD")
        .settlCurrency("USD")
        .tenor(TenorEnum.ON)
        .noLegsCount(0);
    final int totalLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    return sliceToByteArray(buffer, totalLength);
  }

  private static byte[] encodeWebSocketAuth(final int protocolVersion, final String token) {
    final var buffer = new ExpandableArrayBuffer(1024);
    final var header = new MessageHeaderEncoder();
    final var encoder = new WebSocketAuthEncoder();
    final var tokenBytes = token.getBytes(StandardCharsets.UTF_8);
    encoder
        .wrapAndApplyHeader(buffer, 0, header)
        .protocolVersion(protocolVersion)
        .putToken(tokenBytes, 0, tokenBytes.length);
    final int totalLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    return sliceToByteArray(buffer, totalLength);
  }

  private static byte[] encodeWebSocketAuthAck(
      final long sessionMsb,
      final long sessionLsb,
      final int protocolVersion,
      final int maxSubscriptions) {
    final var buffer = new ExpandableArrayBuffer(1024);
    final var header = new MessageHeaderEncoder();
    final var encoder = new WebSocketAuthAckEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, header);
    encoder.sessionId().mostSignificantBits(sessionMsb).leastSignificantBits(sessionLsb);
    encoder.protocolVersion(protocolVersion).maxSubscriptions(maxSubscriptions);
    final int totalLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    return sliceToByteArray(buffer, totalLength);
  }

  /**
   * Encodes a single-RFQ snapshot with two nested legs. Encode order is locked by parent plan §8
   * HIGH #6: outer {@code noRfqsCount(N)} MUST precede outer {@code .next()}; inner {@code
   * noLegsCount(M)} MUST precede inner {@code .next()}. Off-order calls produce malformed-but-
   * decodable buffers — the test {@link #encodeRfqStateSnapshot_callsCountBeforeNextInLockedOrder}
   * statically asserts this method's source matches the locked order.
   */
  private static byte[] encodeRfqStateSnapshot() {
    final var buffer = new ExpandableArrayBuffer(2048);
    final var header = new MessageHeaderEncoder();
    final var encoder = new RfqStateSnapshotEncoder();
    encoder.wrapAndApplyHeader(buffer, 0, header);

    final var noRfqs = encoder.noRfqsCount(1);
    noRfqs.next();
    noRfqs
        .quoteReqId("RFQ-1")
        .accountId(42L)
        .state(RfqStateEnum.Quoted)
        .quoteId("Q-1")
        .symbol("EUR/USD")
        .side(SideEnum.Buy)
        .orderQty(100_000_000L)
        .bidPx(108_520_000L)
        .offerPx(108_530_000L)
        .bidSize(100_000_000L)
        .offerSize(100_000_000L)
        .lastPx(INT64_OPTIONAL_NULL)
        .swapPoints(INT64_OPTIONAL_NULL)
        .validUntil(1_700_000_000_000_000_000L + 30_000_000_000L)
        .transactTime(1_700_000_000_000_000_000L)
        .productType(ProductTypeEnum.Spot)
        .settlDate("20260101")
        .settlType(SettlTypeEnum.Regular)
        .currency("USD")
        .settlCurrency("USD")
        .tenor(TenorEnum.ON);

    final var noLegs = noRfqs.noLegsCount(2);
    noLegs.next();
    noLegs
        .legSide(SideEnum.Buy)
        .legSettlDate("20260101")
        .legSettlType(SettlTypeEnum.Regular)
        .legCurrency("USD")
        .legTenor(TenorEnum.ON)
        .legOrderQty(100_000_000L)
        .legPrice(INT64_OPTIONAL_NULL)
        .legBidPx(INT64_OPTIONAL_NULL)
        .legOfferPx(INT64_OPTIONAL_NULL)
        .legBidSize(INT64_OPTIONAL_NULL)
        .legOfferSize(INT64_OPTIONAL_NULL);
    noLegs.next();
    noLegs
        .legSide(SideEnum.Sell)
        .legSettlDate("20260201")
        .legSettlType(SettlTypeEnum.Future)
        .legCurrency("EUR")
        .legTenor(TenorEnum.ON)
        .legOrderQty(200_000_000L)
        .legPrice(INT64_OPTIONAL_NULL)
        .legBidPx(INT64_OPTIONAL_NULL)
        .legOfferPx(INT64_OPTIONAL_NULL)
        .legBidSize(INT64_OPTIONAL_NULL)
        .legOfferSize(INT64_OPTIONAL_NULL);

    final int totalLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    return sliceToByteArray(buffer, totalLength);
  }

  private static byte[] sliceToByteArray(final ExpandableArrayBuffer buffer, final int length) {
    final var out = new byte[length];
    buffer.getBytes(0, out, 0, length);
    return out;
  }

  // ===========================================================================================
  // tsx driver invocation helpers
  // ===========================================================================================

  /**
   * Runs the driver in {@code single} mode and returns the parsed JSON result. See {@link
   * #spawnDriver} for the locked process model (timeout, stderr-drain, env-strip).
   */
  private static JsonNode runDriverSingle(final byte[] bytes, final int templateId)
      throws IOException, InterruptedException {
    return spawnDriver("single", HEX.formatHex(bytes), String.valueOf(templateId));
  }

  /**
   * Runs the driver in {@code alias} mode (decode A → hold reference → decode B → emit both
   * captures of the held reference). See {@link #spawnDriver} for the locked process model.
   */
  private static JsonNode runDriverAlias(final byte[] bytesA, final byte[] bytesB)
      throws IOException, InterruptedException {
    return spawnDriver("alias", HEX.formatHex(bytesA), HEX.formatHex(bytesB));
  }

  /**
   * Runs the driver in {@code multi} mode (decode same buffer at two offsets, emit both captures).
   * See {@link #spawnDriver} for the locked process model.
   */
  private static JsonNode runDriverMulti(
      final byte[] combined, final int offsetA, final int offsetB)
      throws IOException, InterruptedException {
    return spawnDriver(
        "multi", HEX.formatHex(combined), String.valueOf(offsetA), String.valueOf(offsetB));
  }

  /**
   * Spawns {@code tsx} with the {@code roundtrip-driver.ts} script and returns the parsed JSON
   * stdout. Locked process model: stderr drained concurrently via virtual thread (prevents classic
   * ProcessBuilder deadlock when the child fills the stderr pipe ~64 KB while we wait for stdout
   * EOF), 60 s timeout (tsx cold-start under CI disk-cache pressure measures 5–10 s), NODE_* / TS_*
   * env stripped (so developer-local NODE_OPTIONS=--inspect doesn't break the test),
   * NODE_ENV=development (LOAD-BEARING — MessageRouter's __generation counter only increments under
   * the dev-mode guard).
   */
  private static JsonNode spawnDriver(final String... args)
      throws IOException, InterruptedException {
    final var rootDir = rootDir();
    final var tsxPath = rootDir.resolve("node_modules/.bin/tsx");
    final var driverPath = "sbe-typescript-generator/src/test/resources/roundtrip-driver.ts";

    final var cmd = new String[args.length + 2];
    cmd[0] = tsxPath.toString();
    cmd[1] = driverPath;
    System.arraycopy(args, 0, cmd, 2, args.length);

    final var pb = new ProcessBuilder(cmd).directory(rootDir.toFile()).redirectErrorStream(false);
    pb.redirectError(ProcessBuilder.Redirect.PIPE);
    pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
    pb.environment().keySet().removeIf(k -> k.startsWith("NODE_") || k.startsWith("TS_"));
    // LOAD-BEARING: MessageRouter's __generation counter only increments under
    // process.env.NODE_ENV === "development". Earlier "test" value would silently break
    // route_returnsSharedFlyweightThatRebindsOnNextCall's generation-delta assertion.
    pb.environment().put("NODE_ENV", "development");

    final var outputs = runProcessWithTimeout(pb, 60, TimeUnit.SECONDS);
    if (outputs.exitCode() != 0) {
      fail("tsx driver exited " + outputs.exitCode() + "; stderr:\n" + outputs.stderrAsString());
    }
    return MAPPER.readTree(outputs.stdout());
  }

  /**
   * Concurrent stdout + stderr drain via virtual threads, with a real timeout that fires even when
   * the child holds the stdout pipe open. Prevents two failure modes:
   *
   * <ol>
   *   <li>The classic ProcessBuilder deadlock — reading stdout via {@link
   *       java.io.InputStream#readAllBytes()} BEFORE {@link Process#waitFor(long, TimeUnit)} would
   *       deadlock if the child fills the stderr pipe (~64 KB) and blocks on its own write while
   *       the parent waits for stdout EOF.
   *   <li>Timeout-bypass — {@code readAllBytes()} blocks until the stdout write end closes (process
   *       exit or fork that keeps the pipe open). If a misbehaving tsx loader hangs with stdout
   *       still open, the {@code waitFor} timeout never fires because we'd never reach it. Draining
   *       stdout on its own pump and using {@code waitFor} as the gate means a hung child is killed
   *       at exactly the configured timeout.
   * </ol>
   *
   * Both pumps are joined (with a generous deadline) before the buffers are read so the diagnostic
   * byte-arrays are complete and not racy with respect to the failing-assertion snapshot.
   */
  private static ProcessOutputs runProcessWithTimeout(
      final ProcessBuilder pb, final long timeout, final TimeUnit unit)
      throws IOException, InterruptedException {
    final var proc = pb.start();
    // try-finally guarantees the child process is destroyed on any escape path —
    // InterruptedException, OutOfMemoryError, JUnit assertion failure inside fail(), etc. Without
    // this, a CI runner could accumulate orphaned tsx/tsc processes that hold node_modules locks
    // and memory across test reruns (Gemini medium-priority feedback).
    try {
      final var stdoutBuf = new ByteArrayOutputStream();
      final var stderrBuf = new ByteArrayOutputStream();
      final var stdoutPump = startDrainPump(proc.getInputStream(), stdoutBuf);
      final var stderrPump = startDrainPump(proc.getErrorStream(), stderrBuf);

      if (!proc.waitFor(timeout, unit)) {
        proc.destroyForcibly();
        stdoutPump.join(1_000);
        stderrPump.join(1_000);
        fail(
            "process timed out after "
                + timeout
                + " "
                + unit
                + "; partial stdout:\n"
                + stdoutBuf.toString(StandardCharsets.UTF_8)
                + "\npartial stderr:\n"
                + stderrBuf.toString(StandardCharsets.UTF_8));
      }
      // Process exited; pumps will see EOF imminently. Join with a small deadline so the
      // diagnostic byte-arrays are fully populated before any caller reads them — without this, a
      // failing assertion's stderr message can be truncated mid-write.
      stdoutPump.join(5_000);
      stderrPump.join(5_000);
      return new ProcessOutputs(proc.exitValue(), stdoutBuf.toByteArray(), stderrBuf.toByteArray());
    } finally {
      // No-op if the process already exited normally above; idempotent destroy on any abnormal
      // escape. Process.destroyForcibly() returns the same Process instance and is safe to call on
      // an already-terminated process.
      if (proc.isAlive()) {
        proc.destroyForcibly();
      }
    }
  }

  private static Thread startDrainPump(final InputStream src, final ByteArrayOutputStream dst) {
    return Thread.ofVirtual()
        .start(
            () -> {
              try {
                src.transferTo(dst);
              } catch (final IOException ignored) {
                // process exit closes the stream — ignore
              }
            });
  }

  /** Outputs of a finished child process. Stdout/stderr are fully drained before construction. */
  private record ProcessOutputs(int exitCode, byte[] stdout, byte[] stderr) {
    String stdoutAsString() {
      return new String(stdout, StandardCharsets.UTF_8);
    }

    String stderrAsString() {
      return new String(stderr, StandardCharsets.UTF_8);
    }
  }

  // ===========================================================================================
  // Path resolution helpers
  // ===========================================================================================

  private static Path rootDir() {
    return requireSystemPropPath("rootProjectDir");
  }

  private static String readDriverSource() throws IOException {
    return Files.readString(moduleDir().resolve("src/test/resources/roundtrip-driver.ts"));
  }
}
