package com.trading.engine.pricing.market;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import org.agrona.DirectBuffer;

/**
 * Narrow seam over the Aeron {@link ExclusivePublication} surface that {@link MarketDataPublisher}
 * consumes. Mirrors the established {@code com.trading.engine.orchestrator.Publisher} pattern in
 * this codebase: Aeron's {@code ExclusivePublication} is {@code final} and cannot be subclassed, so
 * isolating the trio of methods the publisher actually uses behind an interface lets unit tests
 * inject a fake (or a mock) without spinning up a real Aeron Media Driver. Three-method surface —
 * intentionally small.
 *
 * <p><b>Threading model.</b> Implementations are invoked on the pricing-service agent thread only.
 * Concurrent invocation is undefined behaviour; the {@link MarketDataPublisher}'s single-writer
 * runtime guard catches the violation if a future refactor introduces cross-thread invocation.
 *
 * <p><b>Allocation.</b> Zero allocation per call after JIT warmup, conditional on the
 * implementation being bound at construction time. The recommended idiom is to bind the production
 * implementation as method references against a {@code final} field ({@code publication::offer},
 * {@code publication::position}, {@code publication::termBufferLength}) captured once at
 * construction; this lets the JVM inline through the SAM and never re-allocate.
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
