package com.trading.engine.launcher;

import com.trading.engine.messages.sbe.AccountLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.AccountLoadedEventDecoder;
import com.trading.engine.messages.sbe.CurrencyLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.CurrencyLoadedEventDecoder;
import com.trading.engine.messages.sbe.MessageHeaderDecoder;
import com.trading.engine.messages.sbe.RiskLimitLoadRejectedEventDecoder;
import com.trading.engine.messages.sbe.RiskLimitLoadedEventDecoder;
import com.trading.refdata.ResponseCollector;
import io.aeron.cluster.client.ControlledEgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Lightweight {@link ControlledEgressListener} for reference data loading at startup. Decodes
 * AccountLoaded (110), AccountLoadRejected (111), CurrencyLoaded (113), CurrencyLoadRejected (114),
 * RiskLimitLoaded (115), RiskLimitLoadRejected (116) from cluster egress and routes to a {@link
 * ResponseCollector}.
 *
 * <p><b>Batch response handling.</b> A single egress fragment may contain multiple concatenated SBE
 * messages (one per record in a batch command). This bridge iterates through the buffer using
 * {@code MessageHeaderDecoder.ENCODED_LENGTH + blockLength} to find message boundaries.
 *
 * <p><b>Flat-only invariant.</b> All ref-data event types (110-116) are flat SBE messages with no
 * groups or vardata. The boundary calculation relies on this. An explicit {@code if/throw} check in
 * the static initializer verifies BLOCK_LENGTH matches expectations; if a future schema change adds
 * vardata, the check will fail at class load time with an {@link IllegalStateException}.
 *
 * <p><b>Forward compatibility.</b> Template IDs 113-116 (currency, risk-limit) are handled even
 * though client-side loaders do not yet exist (APP-196). This avoids a code change when those
 * loaders are implemented.
 *
 * <p><b>Threading.</b> Not thread-safe — called from the single-threaded orchestrator poll loop.
 *
 * <p><b>Allocation.</b> Zero allocation per {@link #onMessage} (decoders pre-allocated). The {@code
 * text()} call on rejected events allocates a String, acceptable at startup.
 */
final class RefDataEgressBridge implements ControlledEgressListener {

  private static final Logger LOG = LogManager.getLogger(RefDataEgressBridge.class);

  // Template IDs for ref-data events (must match trading-schema.xml)
  private static final int ACCOUNT_LOADED = AccountLoadedEventDecoder.TEMPLATE_ID;
  private static final int ACCOUNT_LOAD_REJECTED = AccountLoadRejectedEventDecoder.TEMPLATE_ID;
  private static final int CURRENCY_LOADED = CurrencyLoadedEventDecoder.TEMPLATE_ID;
  private static final int CURRENCY_LOAD_REJECTED = CurrencyLoadRejectedEventDecoder.TEMPLATE_ID;
  private static final int RISK_LIMIT_LOADED = RiskLimitLoadedEventDecoder.TEMPLATE_ID;
  private static final int RISK_LIMIT_LOAD_REJECTED = RiskLimitLoadRejectedEventDecoder.TEMPLATE_ID;

  // Verify ALL 6 ref-data event types are flat (no groups/vardata) at class load time.
  // Java assert is disabled by default in production (-ea not set). These explicit if/throw
  // checks fire unconditionally — a BLOCK_LENGTH mismatch means silent data corruption when
  // iterating concatenated SBE messages. Pattern: LMAX exchange-core / Aeron codec validation.
  static {
    verifyBlockLength("AccountLoadedEvent", AccountLoadedEventDecoder.BLOCK_LENGTH, 135);
    verifyBlockLength("AccountLoadRejectedEvent", AccountLoadRejectedEventDecoder.BLOCK_LENGTH, 97);
    verifyBlockLength("CurrencyLoadedEvent", CurrencyLoadedEventDecoder.BLOCK_LENGTH, 96);
    verifyBlockLength(
        "CurrencyLoadRejectedEvent", CurrencyLoadRejectedEventDecoder.BLOCK_LENGTH, 84);
    verifyBlockLength("RiskLimitLoadedEvent", RiskLimitLoadedEventDecoder.BLOCK_LENGTH, 61);
    verifyBlockLength(
        "RiskLimitLoadRejectedEvent", RiskLimitLoadRejectedEventDecoder.BLOCK_LENGTH, 89);
  }

  // Pre-allocated SBE decoders — one per reject event type because the text field
  // is at DIFFERENT offsets in each (Account=33, Currency=20, RiskLimit=25).
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final AccountLoadRejectedEventDecoder acctRejDecoder =
      new AccountLoadRejectedEventDecoder();
  private final CurrencyLoadRejectedEventDecoder ccyRejDecoder =
      new CurrencyLoadRejectedEventDecoder();
  private final RiskLimitLoadRejectedEventDecoder riskRejDecoder =
      new RiskLimitLoadRejectedEventDecoder();
  private final ResponseCollector collector;

  /**
   * @param collector receives {@link ResponseCollector#onLoaded()} and {@link
   *     ResponseCollector#onRejected(String)} callbacks for each decoded event
   */
  RefDataEgressBridge(final ResponseCollector collector) {
    if (collector == null) {
      throw new NullPointerException("collector must not be null");
    }
    this.collector = collector;
  }

  /**
   * Decodes concatenated SBE ref-data events from a single egress fragment and routes each to the
   * {@link ResponseCollector}.
   *
   * @param clusterSessionId the cluster session that produced the response
   * @param timestamp cluster timestamp (epoch nanoseconds)
   * @param buffer contains one or more concatenated SBE messages
   * @param offset start of the first SBE header
   * @param length total bytes in this fragment
   * @param header Aeron fragment header
   * @return {@link Action#CONTINUE} always — startup loading does not need backpressure
   */
  @Override
  public Action onMessage(
      final long clusterSessionId,
      final long timestamp,
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final Header header) {
    int pos = offset;
    final int end = offset + length;

    while (pos + MessageHeaderDecoder.ENCODED_LENGTH <= end) {
      headerDecoder.wrap(buffer, pos);
      final int templateId = headerDecoder.templateId();
      final int blockLen = headerDecoder.blockLength();
      final int msgLen = MessageHeaderDecoder.ENCODED_LENGTH + blockLen;

      if (pos + msgLen > end) {
        break; // guard against truncated fragment
      }

      final int bodyOffset = pos + MessageHeaderDecoder.ENCODED_LENGTH;
      switch (templateId) {
        case ACCOUNT_LOADED, CURRENCY_LOADED, RISK_LIMIT_LOADED -> collector.onLoaded();
        case ACCOUNT_LOAD_REJECTED -> {
          acctRejDecoder.wrap(buffer, bodyOffset, blockLen, headerDecoder.version());
          collector.onRejected(acctRejDecoder.text().trim());
        }
        case CURRENCY_LOAD_REJECTED -> {
          ccyRejDecoder.wrap(buffer, bodyOffset, blockLen, headerDecoder.version());
          collector.onRejected(ccyRejDecoder.text().trim());
        }
        case RISK_LIMIT_LOAD_REJECTED -> {
          riskRejDecoder.wrap(buffer, bodyOffset, blockLen, headerDecoder.version());
          collector.onRejected(riskRejDecoder.text().trim());
        }
        default -> {
          // Ignore unknown template IDs — future ref-data types will be added here.
        }
      }
      pos += msgLen;
    }
    return Action.CONTINUE;
  }

  @Override
  public void onSessionEvent(
      final long correlationId,
      final long clusterSessionId,
      final long leadershipTermId,
      final int leaderMemberId,
      final EventCode code,
      final String detail) {
    LOG.debug(
        "Session event: correlationId={} sessionId={} code={} detail={}",
        correlationId,
        clusterSessionId,
        code,
        detail);
  }

  @Override
  public void onNewLeader(
      final long clusterSessionId,
      final long leadershipTermId,
      final int leaderMemberId,
      final String ingressEndpoints) {
    LOG.debug(
        "New leader: sessionId={} termId={} memberId={} endpoints={}",
        clusterSessionId,
        leadershipTermId,
        leaderMemberId,
        ingressEndpoints);
  }

  /**
   * Validates that an SBE decoder's BLOCK_LENGTH matches the expected value. Throws at class load
   * time if mismatched — prevents silent data corruption when iterating concatenated messages.
   */
  private static void verifyBlockLength(final String name, final int actual, final int expected) {
    if (actual != expected) {
      throw new IllegalStateException(
          name
              + " layout changed: expected BLOCK_LENGTH="
              + expected
              + ", got "
              + actual
              + ". If only flat fields were added/removed, update the expected constant in"
              + " RefDataEgressBridge. If groups or vardata were added, the iteration logic"
              + " must also be updated.");
    }
  }
}
