package com.trading.engine.pricing.market;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import org.agrona.DirectBuffer;

/**
 * Narrow seam over the Aeron {@link ExclusivePublication} surface that {@link MarketDataPublisher}
 * consumes. The rationale follows the established {@code com.trading.engine.orchestrator.Publisher}
 * pattern in this codebase: Aeron's {@code ExclusivePublication} is {@code final} and cannot be
 * subclassed, so isolating the methods the publisher actually uses behind an interface lets unit
 * tests inject a fake (or a mock) without spinning up a real Aeron Media Driver.
 *
 * <p><b>Three-method surface — deliberate deviation from the canonical SAM publisher pattern.</b>
 * CLAUDE.md documents the "single-method functional interface" idiom (e.g. {@code
 * com.trading.engine.orchestrator.Publisher} which is a single {@code publish(...)} method bound by
 * a method reference). {@code BroadcastPublisher} carries THREE methods because the publisher's
 * five-case Aeron return-code handling needs {@code position()} and {@code termBufferLength()} for
 * the forensic-rich {@link io.aeron.Publication#MAX_POSITION_EXCEEDED} log line (per CLAUDE.md
 * "design rationale" rule — non-obvious decisions document their trade-off). Consequently the
 * launcher binds this interface via an anonymous inner class rather than a method reference; the
 * anonymous class is allocated once at startup (cold path) and lives for the agent's lifetime, so
 * the allocation-profile is equivalent to a SAM. See {@code docs/publishers.md} for the canonical
 * SAM pattern; {@code BroadcastPublisher} is the documented exception where
 * forensic-context-carrying motivated the three-method design.
 *
 * <p><b>Threading model.</b> Implementations are invoked on the pricing-service agent thread only.
 * Concurrent invocation is undefined behaviour; the {@link MarketDataPublisher}'s single-writer
 * runtime guard catches the violation if a future refactor introduces cross-thread invocation.
 *
 * <p><b>Allocation.</b> Zero allocation per call after JIT warmup, conditional on the
 * implementation being bound at construction time. Because this is a 3-method interface (not a
 * SAM), the production binding in {@code PricingServiceLauncher} uses an anonymous inner class
 * created ONCE at launcher startup; subsequent calls invoke INVOKEVIRTUAL on the single
 * pre-constructed instance — identical allocation profile to a method-reference SAM. Inline lambda
 * / anonymous-class expressions inside hot loops are NOT zero-allocation.
 *
 * <p><b>Design rationale.</b> The plan's Commit-4 spec asserts five Aeron offer return codes
 * (BACK_PRESSURED / NOT_CONNECTED / ADMIN_ACTION / MAX_POSITION_EXCEEDED / CLOSED) plus
 * forensic-rich logging on MAX_POSITION_EXCEEDED. {@link #position()} and {@link
 * #termBufferLength()} are needed for that log line; including them in the seam keeps production
 * parity with the real {@code ExclusivePublication} while leaving the test surface free to return
 * zero for those values (the test asserts on behaviour, not log strings).
 *
 * <p><b>Dependencies.</b> None beyond Agrona's {@link DirectBuffer}.
 *
 * @see MarketDataPublisher
 * @see com.trading.engine.orchestrator.Publisher
 */
public interface BroadcastPublisher {

  /**
   * Publish a buffer slice. Return value mirrors {@link Publication#offer(DirectBuffer, int, int)}
   * exactly: positive on success (new stream position), negative on failure ({@link
   * Publication#NOT_CONNECTED} -1, {@link Publication#BACK_PRESSURED} -2, {@link
   * Publication#ADMIN_ACTION} -3, {@link Publication#CLOSED} -4, {@link
   * Publication#MAX_POSITION_EXCEEDED} -5).
   *
   * @param buffer the message buffer.
   * @param offset start offset within the buffer.
   * @param length message length in bytes.
   * @return the Aeron publication-result code.
   */
  long offer(DirectBuffer buffer, int offset, int length);

  /**
   * Current publication position. Used by the MAX_POSITION_EXCEEDED forensic log only.
   * Implementations may return {@code 0L} from tests.
   *
   * @return the publication position in bytes.
   */
  long position();

  /**
   * Term buffer length. Used by the MAX_POSITION_EXCEEDED forensic log only. Implementations may
   * return {@code 0} from tests.
   *
   * @return the configured term buffer length in bytes.
   */
  int termBufferLength();
}
