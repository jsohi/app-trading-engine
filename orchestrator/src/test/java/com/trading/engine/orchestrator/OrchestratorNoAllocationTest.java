package com.trading.engine.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.PriceResponseDecoder;
import com.trading.engine.messages.sbe.QuoteRequestDecoder;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.orchestrator.codec.OrchestratorMessageEncoder;
import com.trading.engine.testsupport.buffer.SbeFieldUtil;
import com.trading.engine.testsupport.sbe.SbeTestEncoder;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Allocation regression tripwire for the orchestrator hot path. Each test pre-builds inputs outside
 * the timed section, then runs the target method {@code N} times in a tight loop and asserts {@link
 * GarbageCollectorMXBean#getCollectionCount()} did not advance — confirming the JIT-warmed path is
 * allocation-free under steady-state load. Mirrors the gateway's {@code NoAllocationTest} (file
 * {@code gateway/src/test/.../NoAllocationTest.java}).
 *
 * <p>GC counts are flaky on shared CI (a stop-the-world from another process advances the count),
 * so these tests are gated behind {@code -DrunAllocTests=true} and do NOT run by default. Local
 * invocation:
 *
 * <pre>./gradlew :orchestrator:test --tests OrchestratorNoAllocationTest -DrunAllocTests=true</pre>
 *
 * <p>{@code WARMUP_ITERATIONS} drives JIT compilation into the steady-state path before the GC
 * delta is sampled.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
class OrchestratorNoAllocationTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 10_000;

  // Fixed identifiers reused across warmup AND measured loop so Agrona Object2ObjectHashMap.put
  // hits the SAME hash bucket every iteration — the internal table is sized once during warmup,
  // and subsequent puts are zero-alloc.
  private static final String FIXED_QUOTE_REQ_ID = "QR-WARMUP-0000000001";
  private static final String FIXED_QUOTE_ID = "QTE-WARMUP-000000001";
  private static final String SYMBOL = "EURUSD";
  private static final long ORDER_QTY = 100_000_000L;
  private static final long BID_PX = 110_000_000L;
  private static final long OFFER_PX = 111_000_000L;
  private static final long NOW = 1_000_000_000L;

  @Test
  void orchestratorIdGenerator_nextInto_zeroAllocation() {
    final var idGen = new OrchestratorIdGenerator("QTE");
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(32);

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      idGen.nextInto(buf, 0);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      final int len = idGen.nextInto(buf, 0);
      assertTrue(len > 0); // sanity
    }
    final long afterGc = totalGcCount();
    assertEquals(
        beforeGc,
        afterGc,
        "idGenerator.nextInto allocated enough to trigger GC. before="
            + beforeGc
            + " after="
            + afterGc);
  }

  @Test
  void rfqStateMachine_acquireReleaseLifecycle_zeroAllocation() {
    // Same fixed quoteReqId/quoteId across warmup + measured loop ensures the byQuoteReqId and
    // byQuoteId hash tables are sized once during warmup; subsequent acquire→quote→validate
    // cycles reuse the same bucket → Agrona Object2ObjectHashMap.put is zero-alloc.
    final var sm = new RfqStateMachine(4, 5_000_000_000L, 30_000_000_000L, 5_000_000_000L);
    final var qrid = SbeFieldUtil.zeroPad(FIXED_QUOTE_REQ_ID, RfqState.QUOTE_REQ_ID_LENGTH);
    final var qid = SbeFieldUtil.zeroPad(FIXED_QUOTE_ID, RfqState.QUOTE_ID_LENGTH);
    final var qrDecoder = preBuildQuoteRequestDecoder();
    final var prDecoder = preBuildPriceResponseDecoder();

    // Warmup: full lifecycle per iteration so the slot is released back to the pool each time.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      runOneLifecycle(sm, qrDecoder, prDecoder, qrid, qid);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      runOneLifecycle(sm, qrDecoder, prDecoder, qrid, qid);
    }
    final long afterGc = totalGcCount();
    assertEquals(
        beforeGc,
        afterGc,
        "RfqStateMachine acquire→quote→validate lifecycle allocated enough to trigger GC. before="
            + beforeGc
            + " after="
            + afterGc);
  }

  @Test
  void rfqStateMachine_acquireReleaseLifecycle_rfqOverloads_zeroAllocation() {
    // Same as rfqStateMachine_acquireReleaseLifecycle_zeroAllocation but exercises the
    // rfq-accepting zero-probe overloads of onPriceResponseAccepted / onValidationValid that
    // OrchestratorService actually calls on the hot path after PR #45. Adds regression coverage
    // for the new transition overloads.
    final var sm = new RfqStateMachine(4, 5_000_000_000L, 30_000_000_000L, 5_000_000_000L);
    final var qid = SbeFieldUtil.zeroPad(FIXED_QUOTE_ID, RfqState.QUOTE_ID_LENGTH);
    final var qrDecoder = preBuildQuoteRequestDecoder();
    final var prDecoder = preBuildPriceResponseDecoder();

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      runOneLifecycleRfqOverloads(sm, qrDecoder, prDecoder, qid);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      runOneLifecycleRfqOverloads(sm, qrDecoder, prDecoder, qid);
    }
    final long afterGc = totalGcCount();
    assertEquals(
        beforeGc,
        afterGc,
        "RfqStateMachine rfq-overload lifecycle allocated enough to trigger GC. before="
            + beforeGc
            + " after="
            + afterGc);
  }

  @Test
  void orchestratorMessageEncoder_encodePriceRequest_zeroAllocation() {
    final var sm = new RfqStateMachine(4, 5_000_000_000L, 30_000_000_000L, 5_000_000_000L);
    final var qrDecoder = preBuildQuoteRequestDecoder();
    final var rfq = sm.onQuoteRequest(qrDecoder, NOW);
    assertNotNull(rfq);

    final var encoder = new OrchestratorMessageEncoder();
    final MutableDirectBuffer dst = new ExpandableArrayBuffer(512);

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      encoder.encodePriceRequest(dst, 0, rfq);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      final int len = encoder.encodePriceRequest(dst, 0, rfq);
      assertTrue(len > 0);
    }
    final long afterGc = totalGcCount();
    assertEquals(
        beforeGc,
        afterGc,
        "encoder.encodePriceRequest allocated enough to trigger GC. before="
            + beforeGc
            + " after="
            + afterGc);
  }

  @Test
  void rfqStateMachine_reapCallback_zeroAllocation() {
    // Validates the duty-cycle reap loop hot path. The callback is captured ONCE outside the
    // measured loop (final local) so passing it to reapExpired is not a per-iteration lambda
    // allocation. nowNanos stays at NOW (less than the timeout) so no RFQs are actually expired
    // — this measures the empty-sweep path which runs every duty cycle in production.
    final var sm = new RfqStateMachine(4, 5_000_000_000L, 30_000_000_000L, 5_000_000_000L);
    final RfqStateMachine.ReapCallback noOpCallback = state -> {};

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      sm.reapExpired(NOW, noOpCallback);
    }

    final long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      sm.reapExpired(NOW, noOpCallback);
    }
    final long afterGc = totalGcCount();
    assertEquals(
        beforeGc,
        afterGc,
        "RfqStateMachine.reapExpired allocated enough to trigger GC. before="
            + beforeGc
            + " after="
            + afterGc);
  }

  // ---------------------------------------------------------------------------
  // Helpers (pre-built outside any timed section)
  // ---------------------------------------------------------------------------

  /** Runs one acquire → onPriceResponseAccepted → onValidationValid lifecycle (releases slot). */
  private static void runOneLifecycle(
      final RfqStateMachine sm,
      final QuoteRequestDecoder qrDecoder,
      final PriceResponseDecoder prDecoder,
      final byte[] qrid,
      final byte[] qid) {
    final var rfq = sm.onQuoteRequest(qrDecoder, NOW);
    assertNotNull(rfq);
    sm.onPriceResponseAccepted(qrid, 0, qrid.length, prDecoder, qid, 0, qid.length, NOW);
    sm.onValidationValid(qid, 0, qid.length);
  }

  /**
   * Same lifecycle as {@link #runOneLifecycle} but uses the rfq-accepting zero-probe overloads —
   * the paths that OrchestratorService actually calls on the hot path after PR #45. Validates that
   * the new overloads are also allocation-free.
   */
  private static void runOneLifecycleRfqOverloads(
      final RfqStateMachine sm,
      final QuoteRequestDecoder qrDecoder,
      final PriceResponseDecoder prDecoder,
      final byte[] qid) {
    final var rfq = sm.onQuoteRequest(qrDecoder, NOW);
    assertNotNull(rfq);
    sm.onPriceResponseAccepted(rfq, prDecoder, qid, 0, qid.length, NOW);
    sm.onValidationValid(rfq);
  }

  /** Builds a wrapped QuoteRequestDecoder for the fixed quoteReqId. Non-allocating after init. */
  private static QuoteRequestDecoder preBuildQuoteRequestDecoder() {
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
    SbeTestEncoder.encodeQuoteRequest(
        buf, 0, FIXED_QUOTE_REQ_ID, SYMBOL, SideEnum.Buy, ORDER_QTY, "ACCT001");
    final var hdr = new MessageHeaderDecoder();
    hdr.wrap(buf, 0);
    final var dec = new QuoteRequestDecoder();
    dec.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hdr.blockLength(), hdr.version());
    return dec;
  }

  /** Builds a wrapped accepted PriceResponseDecoder for the fixed quoteReqId. */
  private static PriceResponseDecoder preBuildPriceResponseDecoder() {
    final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
    SbeTestEncoder.encodePriceResponse(
        buf, 0, FIXED_QUOTE_REQ_ID, SYMBOL, true, BID_PX, OFFER_PX, NOW);
    final var hdr = new MessageHeaderDecoder();
    hdr.wrap(buf, 0);
    final var dec = new PriceResponseDecoder();
    dec.wrap(buf, MessageHeaderDecoder.ENCODED_LENGTH, hdr.blockLength(), hdr.version());
    return dec;
  }

  private static long totalGcCount() {
    long total = 0;
    final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (final GarbageCollectorMXBean bean : gcBeans) {
      final long count = bean.getCollectionCount();
      if (count >= 0) {
        total += count;
      }
    }
    return total;
  }
}
