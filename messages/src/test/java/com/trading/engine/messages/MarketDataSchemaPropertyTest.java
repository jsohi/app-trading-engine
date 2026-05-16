package com.trading.engine.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.engine.messages.sbe.FeedStateEnum;
import com.trading.engine.messages.sbe.MarketDataFeedStateChangeDecoder;
import com.trading.engine.messages.sbe.MarketDataFeedStateChangeEncoder;
import com.trading.engine.messages.sbe.MarketDataHeartbeatDecoder;
import com.trading.engine.messages.sbe.MarketDataHeartbeatEncoder;
import com.trading.engine.messages.sbe.MarketDataSnapshotRequestDecoder;
import com.trading.engine.messages.sbe.MarketDataSnapshotRequestEncoder;
import com.trading.engine.messages.sbe.MarketDataTickDecoder;
import com.trading.engine.messages.sbe.MarketDataTickEncoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import java.nio.charset.StandardCharsets;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Property-based round-trip tests for the four Phase 3 market-data SBE templates (54–57).
 *
 * <p>Uses <a href="https://jqwik.net">jqwik 1.9.2</a> to generate 1 000 random inputs per template
 * and verify the codec encode→decode identity. Each {@link Property} uses a separate {@link
 * UnsafeBuffer} allocation per trial so buffer state never bleeds between trials.
 *
 * <p>Coverage goals:
 *
 * <ul>
 *   <li>{@code MarketDataTick} (54): all eight fields randomised; {@code bidPrice} / {@code
 *       askPrice} span the full signed 64-bit range; random 6–8-char ASCII symbols.
 *   <li>{@code MarketDataHeartbeat} (55): {@code lastPublishedSeq} group size 0–16 with random
 *       per-entry symbol and seq values.
 *   <li>{@code MarketDataSnapshotRequest} (56): single random symbol field.
 *   <li>{@code MarketDataFeedStateChange} (57): random state from {@code FeedStateEnum} ordinal;
 *       random {@code serverNanos} over the full long range.
 * </ul>
 *
 * <p>Threading model: Not thread-safe — jqwik executes properties in a single thread per property.
 * Allocation: Allocates one {@code UnsafeBuffer} per trial (inside each property body), reused
 * across the encode and decode phases of that trial.
 */
final class MarketDataSchemaPropertyTest {

  /** 8 KiB per trial — sufficient for any generated market-data frame including heartbeat. */
  private static final int BUF_SIZE = 8_192;

  /**
   * Fixed-point scale factor: 1 whole unit = {@code 100_000_000L} (10^-8). Matches the {@code
   * PRICE_SCALE} constant defined in {@code MarketDataConstants}.
   */
  private static final long PRICE_SCALE = 100_000_000L;

  // =========================================================================
  // Template 54 — MarketDataTick
  // =========================================================================

  /**
   * Property: for any combination of random field values, a {@link MarketDataTickEncoder} followed
   * by a {@link MarketDataTickDecoder} produces byte-identical field values. Exercises the full
   * signed 64-bit range on {@code bidPrice} and {@code askPrice} (both directions, including {@code
   * Long.MIN_VALUE} which is the SBE null sentinel for this type). Random 6–8-char ASCII symbols
   * ensure the NUL-pad path is exercised at varying lengths.
   *
   * @param symbol random ASCII symbol string (6–8 chars from provider {@code "asciiSymbol"})
   * @param bidPrice random long in [{@link Long#MIN_VALUE}, {@link Long#MAX_VALUE}]
   * @param askPrice random long in [{@link Long#MIN_VALUE}, {@link Long#MAX_VALUE}]
   * @param bidSize random long in [{@link Long#MIN_VALUE}, {@link Long#MAX_VALUE}]
   * @param askSize random long in [{@link Long#MIN_VALUE}, {@link Long#MAX_VALUE}]
   * @param symbolSeq random long in [{@link Long#MIN_VALUE}, {@link Long#MAX_VALUE}]
   * @param ingressNanos random long in [{@link Long#MIN_VALUE}, {@link Long#MAX_VALUE}]
   * @param serverNanos random long in [{@link Long#MIN_VALUE}, {@link Long#MAX_VALUE}]
   */
  @Property(tries = 1000)
  void marketDataTick_property_roundTrip(
      @ForAll("asciiSymbol") final String symbol,
      @ForAll final long bidPrice,
      @ForAll final long askPrice,
      @ForAll final long bidSize,
      @ForAll final long askSize,
      @ForAll final long symbolSeq,
      @ForAll final long ingressNanos,
      @ForAll final long serverNanos) {

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataTickEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder
        .symbol(symbol)
        .bidPrice(bidPrice)
        .askPrice(askPrice)
        .bidSize(bidSize)
        .askSize(askSize)
        .symbolSeq(symbolSeq)
        .ingressNanos(ingressNanos)
        .serverNanos(serverNanos);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataTickDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(MarketDataTickDecoder.TEMPLATE_ID, headerDecoder.templateId(), "templateId");

    final var symDst = new byte[MarketDataTickDecoder.symbolLength()];
    decoder.getSymbol(symDst, 0);
    assertArrayEquals(padRight(symbol, MarketDataTickDecoder.symbolLength()), symDst, "symbol");

    assertEquals(bidPrice, decoder.bidPrice(), "bidPrice");
    assertEquals(askPrice, decoder.askPrice(), "askPrice");
    assertEquals(bidSize, decoder.bidSize(), "bidSize");
    assertEquals(askSize, decoder.askSize(), "askSize");
    assertEquals(symbolSeq, decoder.symbolSeq(), "symbolSeq");
    assertEquals(ingressNanos, decoder.ingressNanos(), "ingressNanos");
    assertEquals(serverNanos, decoder.serverNanos(), "serverNanos");
  }

  // =========================================================================
  // Template 55 — MarketDataHeartbeat
  // =========================================================================

  /**
   * Property: for any {@code serverNanos} and any group of 0–16 (symbol, seq) pairs, a {@link
   * MarketDataHeartbeatEncoder} followed by a {@link MarketDataHeartbeatDecoder} produces
   * byte-identical field values and preserves group iteration order. The 0-entry case (empty group
   * header written, no entries follow) is included in the generated space.
   *
   * @param serverNanos random long over the full signed 64-bit range
   * @param groupSize random group count in [0, 16]
   */
  @Property(tries = 1000)
  void marketDataHeartbeat_property_roundTrip(
      @ForAll final long serverNanos,
      // Upper bound = symbolForIndex pool size (16 entries). Raising the bound without
      // expanding the pool would push past the 16 distinct ASCII symbols available and
      // wrap modulo 16, weakening the per-entry position-bug coverage.
      @ForAll @IntRange(min = 0, max = 16) final int groupSize) {

    // Build parallel arrays of distinct per-entry symbol and seq values.
    final String[] symbols = new String[groupSize];
    final long[] seqs = new long[groupSize];
    for (int i = 0; i < groupSize; i++) {
      // Use a deterministic but distinct value per entry to expose position bugs.
      symbols[i] = symbolForIndex(i);
      seqs[i] = (long) i * 1_000_001L + serverNanos;
    }

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataHeartbeatEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.serverNanos(serverNanos);

    final var groupEnc = encoder.lastPublishedSeqCount(groupSize);
    for (int i = 0; i < groupSize; i++) {
      groupEnc.next().symbol(symbols[i]).seq(seqs[i]);
    }

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataHeartbeatDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(MarketDataHeartbeatDecoder.TEMPLATE_ID, headerDecoder.templateId(), "templateId");
    assertEquals(serverNanos, decoder.serverNanos(), "serverNanos");

    final var group = decoder.lastPublishedSeq();
    assertEquals(groupSize, group.count(), "group count");

    for (int i = 0; i < groupSize; i++) {
      group.next();
      final var symDst =
          new byte[MarketDataHeartbeatDecoder.LastPublishedSeqDecoder.symbolLength()];
      group.getSymbol(symDst, 0);
      assertArrayEquals(
          padRight(symbols[i], MarketDataHeartbeatDecoder.LastPublishedSeqDecoder.symbolLength()),
          symDst,
          "symbol at index " + i);
      assertEquals(seqs[i], group.seq(), "seq at index " + i);
    }
  }

  // =========================================================================
  // Template 56 — MarketDataSnapshotRequest
  // =========================================================================

  /**
   * Property: for any random ASCII symbol string of 6–8 characters, a {@link
   * MarketDataSnapshotRequestEncoder} followed by a {@link MarketDataSnapshotRequestDecoder}
   * produces a byte-identical 8-byte symbol field (left-justified, NUL-padded to 8).
   *
   * @param symbol random ASCII symbol string (6–8 chars from provider {@code "asciiSymbol"})
   */
  @Property(tries = 1000)
  void marketDataSnapshotRequest_property_roundTrip(@ForAll("asciiSymbol") final String symbol) {

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataSnapshotRequestEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.symbol(symbol);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataSnapshotRequestDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(
        MarketDataSnapshotRequestDecoder.TEMPLATE_ID, headerDecoder.templateId(), "templateId");

    final var symDst = new byte[MarketDataSnapshotRequestDecoder.symbolLength()];
    decoder.getSymbol(symDst, 0);
    assertArrayEquals(
        padRight(symbol, MarketDataSnapshotRequestDecoder.symbolLength()), symDst, "symbol");
  }

  // =========================================================================
  // Template 57 — MarketDataFeedStateChange
  // =========================================================================

  /**
   * Property: for any {@link FeedStateEnum} state and any {@code serverNanos}, a {@link
   * MarketDataFeedStateChangeEncoder} followed by a {@link MarketDataFeedStateChangeDecoder}
   * produces byte-identical field values. The enum ordinal (0=Live, 1=Quiet, 2=Stale) is verified
   * via the decoded enum constant. The raw wire byte is verified to equal the ordinal to confirm
   * the single-byte state field does not alias the adjacent 8-byte {@code serverNanos}.
   *
   * @param stateOrdinal random int in [0, 2] mapping to Live/Quiet/Stale (from provider {@code
   *     "feedStateOrdinal"})
   * @param serverNanos random long over the full signed 64-bit range
   */
  @Property(tries = 1000)
  void marketDataFeedStateChange_property_roundTrip(
      @ForAll("feedStateOrdinal") final int stateOrdinal, @ForAll final long serverNanos) {

    final var state = FeedStateEnum.get((short) stateOrdinal);

    final var buf = new UnsafeBuffer(new byte[BUF_SIZE]);
    final var headerEncoder = new MessageHeaderEncoder();
    final var encoder = new MarketDataFeedStateChangeEncoder();
    encoder.wrapAndApplyHeader(buf, 0, headerEncoder);
    encoder.state(state).serverNanos(serverNanos);

    final var headerDecoder = new MessageHeaderDecoder();
    final var decoder = new MarketDataFeedStateChangeDecoder();
    decoder.wrapAndApplyHeader(buf, 0, headerDecoder);

    assertEquals(
        MarketDataFeedStateChangeDecoder.TEMPLATE_ID, headerDecoder.templateId(), "templateId");
    assertEquals(state, decoder.state(), "state enum");
    assertEquals((short) stateOrdinal, decoder.stateRaw(), "state wire byte");
    assertEquals(serverNanos, decoder.serverNanos(), "serverNanos");
  }

  // =========================================================================
  // Arbitrary providers
  // =========================================================================

  /**
   * Provides random ASCII symbol strings of 6–8 characters drawn from the uppercase Latin alphabet
   * (A–Z) — the character set used by FX symbol codes in the trading engine. Lengths 6, 7, and 8
   * are equally likely so the NUL-pad path is exercised at all three widths.
   *
   * @return arbitrary producing 6–8-char uppercase ASCII strings
   */
  @Provide
  Arbitrary<String> asciiSymbol() {
    return Arbitraries.integers()
        .between(6, 8)
        .flatMap(
            len ->
                Arbitraries.chars()
                    .range('A', 'Z')
                    .list()
                    .ofSize(len)
                    .map(
                        chars -> {
                          final var sb = new StringBuilder(len);
                          for (final var c : chars) {
                            sb.append(c);
                          }
                          return sb.toString();
                        }));
  }

  /**
   * Provides a random {@link FeedStateEnum} ordinal in [0, 2] (Live=0, Quiet=1, Stale=2). NULL_VAL
   * (255) is excluded because it represents an absent/unset field — not a valid production state to
   * encode.
   *
   * @return arbitrary producing integers 0, 1, or 2
   */
  @Provide
  Arbitrary<Integer> feedStateOrdinal() {
    return Arbitraries.of(0, 1, 2);
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  /**
   * Returns a short, distinct 6-char ASCII symbol for the given entry index, cycling through a
   * fixed pool. Used by the heartbeat group property to give each entry a predictable but distinct
   * symbol without introducing external randomness into the group-ordering verification.
   *
   * @param index the zero-based entry index
   * @return a 6-char uppercase ASCII symbol string
   */
  private static String symbolForIndex(final int index) {
    final String[] pool = {
      "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF", "NZDUSD",
      "USDCAD", "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "CHFJPY",
      "EURCHF", "EURAUD", "GBPAUD", "GBPCHF"
    };
    return pool[index % pool.length];
  }

  /**
   * Returns a NUL-padded byte array of exactly {@code width} bytes containing the ASCII encoding of
   * {@code value}. Mirrors the SBE encoder behaviour: characters are written left-justified and the
   * tail is zero-filled.
   *
   * @param value the string to encode; must be &lt;= {@code width} characters long
   * @param width the fixed field width in bytes
   * @return byte array of length {@code width}
   */
  private static byte[] padRight(final String value, final int width) {
    final var dst = new byte[width];
    if (value != null && !value.isEmpty()) {
      final byte[] src = value.getBytes(StandardCharsets.US_ASCII);
      System.arraycopy(src, 0, dst, 0, Math.min(src.length, width));
    }
    return dst;
  }
}
