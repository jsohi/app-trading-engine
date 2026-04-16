package com.trading.engine.orchestrator;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import org.agrona.DirectBuffer;

/**
 * SAM abstraction over an Aeron outbound publication. Decouples {@link OrchestratorService} from
 * the concrete {@link io.aeron.ExclusivePublication} class so that tests can inject a fake (Aeron's
 * {@code ExclusivePublication} is {@code final} and cannot be subclassed). Mirrors the established
 * codebase pattern in {@code com.trading.refdata.ClusterCommandSender}.
 *
 * <h3>Contract</h3>
 *
 * <p>Return value mirrors {@link Publication#offer(DirectBuffer, int, int)} exactly. Positive value
 * indicates the new stream position; negative values indicate error per Aeron constants: {@link
 * Publication#NOT_CONNECTED} (-1), {@link Publication#BACK_PRESSURED} (-2), {@link
 * Publication#ADMIN_ACTION} (-3), {@link Publication#CLOSED} (-4), {@link
 * Publication#MAX_POSITION_EXCEEDED} (-5). Callers must handle these per Aeron semantics; see
 * {@link OrchestratorService#offerWithRetry} for the retry/terminal-failure mapping used in
 * production.
 *
 * <h3>Threading</h3>
 *
 * <p><b>Not thread-safe.</b> Implementations are invoked from the single duty-cycle thread of the
 * owning {@code Agent}; concurrent invocation is undefined behaviour.
 *
 * <h3>Allocation</h3>
 *
 * <p><b>Zero allocation per call after JIT warmup,</b> conditional on the implementation being
 * bound at construction time. Method-reference bindings such as {@code ExclusivePublication::offer}
 * are evaluated to a SAM instance at lambda-creation time (JLS §15.27.4 guarantees the instance is
 * created at expression evaluation; the JVM is permitted but not required to cache identical
 * lambdas). The recommended idiom is to assign the binding to a {@code final} field at construction
 * so the SAM instance lives for the lifetime of the holder and the JIT can inline through it
 * (mirrors the {@code reapCallback} pattern in {@link OrchestratorService} — captured once, never
 * reassigned). Inline lambda expressions inside hot loops are NOT zero-allocation. See {@link
 * OrchestratorLauncher} for the canonical binding example: {@code gatewayPublication::offer}.
 *
 * @see ExclusivePublication#offer(DirectBuffer, int, int)
 */
@FunctionalInterface
public interface Publisher {

  /**
   * Publish a buffer slice. Return value mirrors {@link Publication#offer(DirectBuffer, int, int)}.
   *
   * @param buffer the message buffer to publish
   * @param offset offset within the buffer where the message starts
   * @param length length of the message in bytes
   * @return Aeron publication result: positive on success (new stream position), negative on
   *     failure (see {@link Publication#NOT_CONNECTED}, {@link Publication#BACK_PRESSURED}, {@link
   *     Publication#ADMIN_ACTION}, {@link Publication#CLOSED}, {@link
   *     Publication#MAX_POSITION_EXCEEDED}).
   */
  long publish(DirectBuffer buffer, int offset, int length);
}
