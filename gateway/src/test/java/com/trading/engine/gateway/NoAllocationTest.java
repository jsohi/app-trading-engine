package com.trading.engine.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.engine.fix.builder.ExecutionReportEncoder;
import com.trading.engine.fix.builder.HeaderEncoder;
import com.trading.engine.fix.decoder_flyweight.NewOrderSingleDecoder;
import com.trading.engine.messages.sbe.ExecTypeEnum;
import com.trading.engine.messages.sbe.MessageHeaderEncoder;
import com.trading.engine.messages.sbe.OrdStatusEnum;
import com.trading.engine.messages.sbe.ProductTypeEnum;
import com.trading.engine.messages.sbe.SettlTypeEnum;
import com.trading.engine.messages.sbe.SideEnum;
import com.trading.engine.messages.sbe.TenorEnum;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

/**
 * Allocation regression tripwire for the FIX↔SBE translators. Each test pre-builds the input
 * messages outside the timed section, then runs the translator {@code N} times in a tight loop and
 * asserts that {@link GarbageCollectorMXBean#getCollectionCount()} did not advance — confirming the
 * JIT-warmed translator path is allocation-free under steady-state load.
 *
 * <p>GC counts are inherently flaky on shared CI (a stop-the-world from another process can advance
 * the count), so these tests are gated behind the system property {@code -DrunAllocTests=true} and
 * not run by default. Local invocation:
 *
 * <pre>./gradlew :gateway:test --tests NoAllocationTest -DrunAllocTests=true</pre>
 *
 * <p>The test exercises the two paths most likely to regress: a complete FIX→SBE NewOrderSingle
 * translation (covers char-array padding, fixed-point conversion, enum mapping, optional fields,
 * UTC timestamp parsing) and a complete SBE→FIX ExecutionReport translation (covers byte-buffer
 * scratch, DecimalFloat reuse, UTC timestamp encoding, leg group iteration).
 *
 * <p>{@code WARMUP_ITERATIONS} runs first to drive JIT compilation into the steady-state path
 * before the GC delta is sampled.
 */
@EnabledIfSystemProperty(named = "runAllocTests", matches = "true")
class NoAllocationTest {

  private static final int WARMUP_ITERATIONS = 5_000;
  private static final int MEASURED_ITERATIONS = 10_000;

  @Test
  void fixToSbeNewOrderSingleAllocatesNothingInSteadyState() {
    // Pre-build a FIX NewOrderSingle decoder pointing at a static wire. Done outside the timed
    // section so any allocations here don't pollute the GC count.
    com.trading.engine.fix.builder.NewOrderSingleEncoder fixEnc =
        new com.trading.engine.fix.builder.NewOrderSingleEncoder();
    HeaderEncoder hdr = fixEnc.header();
    hdr.senderCompID("CLIENT").targetCompID("EXCH").msgSeqNum(1);
    hdr.sendingTime("20260407-12:00:00".getBytes());
    fixEnc.clOrdID("ORD-1");
    fixEnc.instrument().symbol("EURUSD");
    fixEnc.side('1');
    fixEnc.transactTime("20260407-12:00:00".getBytes());
    fixEnc.ordType('2');
    fixEnc.price(new DecimalFloat(11_000L, 4));
    fixEnc.orderQtyData().orderQty(new DecimalFloat(1_000_000L, 0));
    fixEnc.timeInForce('0');
    fixEnc.account("ACCT-1");
    fixEnc.currency("EUR");
    fixEnc.settlCurrency("USD");
    fixEnc.settlType('3');
    fixEnc.settlDate("20260409".getBytes());

    MutableAsciiBuffer wire = new MutableAsciiBuffer(new byte[2048]);
    long encoded = fixEnc.encode(wire, 0);
    int wireOffset = (int) (encoded >>> 32);
    int wireLen = (int) encoded;

    NewOrderSingleDecoder fixDec = new NewOrderSingleDecoder();
    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    FixToSbeTranslator translator = new FixToSbeTranslator();

    // Warm up the JIT to drive the translator path into steady-state compilation.
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      fixDec.decode(wire, wireOffset, wireLen);
      translator.translateNewOrderSingle(fixDec, sbeBuf, 0);
    }

    // Sample the GC count, run the measured loop, sample again, assert delta == 0.
    long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      fixDec.decode(wire, wireOffset, wireLen);
      int len = translator.translateNewOrderSingle(fixDec, sbeBuf, 0);
      assertTrue(len > 0); // sanity
    }
    long afterGc = totalGcCount();
    assertEquals(
        beforeGc,
        afterGc,
        "Translator allocated enough to trigger GC. before=" + beforeGc + " after=" + afterGc);
  }

  @Test
  void sbeToFixExecutionReportAllocatesNothingInSteadyState() {
    // Pre-build an SBE ExecutionReport in a buffer.
    MutableDirectBuffer sbeBuf = new ExpandableArrayBuffer(512);
    com.trading.engine.messages.sbe.ExecutionReportEncoder enc =
        new com.trading.engine.messages.sbe.ExecutionReportEncoder();
    enc.wrapAndApplyHeader(sbeBuf, 0, new MessageHeaderEncoder());
    enc.clOrdId("ORD-1");
    enc.orderId("ORDER-1");
    enc.execId("EXEC-1");
    enc.quoteId("");
    enc.execType(ExecTypeEnum.Fill);
    enc.ordStatus(OrdStatusEnum.Filled);
    enc.symbol("EURUSD");
    enc.side(SideEnum.Buy);
    enc.leavesQty(0L);
    enc.cumQty(100_000_000_000L);
    enc.avgPx(110_000_000L);
    enc.transactTime(1_712_491_200_000_000_000L);
    enc.text("FILLED");
    enc.productType(ProductTypeEnum.NULL_VAL);
    enc.settlDate("20260409");
    enc.settlType(SettlTypeEnum.TPlus2);
    enc.currency("EUR");
    enc.settlCurrency("USD");
    enc.tenor(TenorEnum.NULL_VAL);
    enc.noLegsCount(0);

    com.trading.engine.messages.sbe.MessageHeaderDecoder hdrDec =
        new com.trading.engine.messages.sbe.MessageHeaderDecoder();
    hdrDec.wrap(sbeBuf, 0);
    com.trading.engine.messages.sbe.ExecutionReportDecoder sbeDec =
        new com.trading.engine.messages.sbe.ExecutionReportDecoder();
    SbeToFixTranslator translator = new SbeToFixTranslator();
    ExecutionReportEncoder fixEnc = new ExecutionReportEncoder();
    HeaderEncoder fixHdr = fixEnc.header();
    fixHdr.senderCompID("EXCH").targetCompID("CLIENT");
    byte[] sendingTimeBytes = "20260407-12:00:00".getBytes();
    fixHdr.sendingTime(sendingTimeBytes);

    // Warmup
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      sbeDec.wrap(
          sbeBuf,
          com.trading.engine.messages.sbe.MessageHeaderDecoder.ENCODED_LENGTH,
          hdrDec.blockLength(),
          hdrDec.version());
      fixHdr.msgSeqNum(i);
      translator.translateExecutionReport(sbeDec, fixEnc);
    }

    // Measured
    long beforeGc = totalGcCount();
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      sbeDec.wrap(
          sbeBuf,
          com.trading.engine.messages.sbe.MessageHeaderDecoder.ENCODED_LENGTH,
          hdrDec.blockLength(),
          hdrDec.version());
      fixHdr.msgSeqNum(i);
      translator.translateExecutionReport(sbeDec, fixEnc);
    }
    long afterGc = totalGcCount();
    assertEquals(
        beforeGc,
        afterGc,
        "Translator allocated enough to trigger GC. before=" + beforeGc + " after=" + afterGc);
  }

  private static long totalGcCount() {
    long total = 0;
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean bean : gcBeans) {
      long count = bean.getCollectionCount();
      if (count >= 0) {
        total += count;
      }
    }
    return total;
  }
}
