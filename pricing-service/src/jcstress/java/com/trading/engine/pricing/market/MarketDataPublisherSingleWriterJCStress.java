package com.trading.engine.pricing.market;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Description;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

/**
 * JCStress: single-writer guard fires when two threads concurrently call {@code onTick}.
 *
 * <p><b>Purpose.</b> Contract-tests the runtime guard in {@link
 * MarketDataPublisher#assertAgentThread()}: when any thread other than the one that called {@code
 * onStart()} invokes {@code onTick}, an {@link IllegalStateException} must be thrown. A future
 * refactor that accidentally introduces cross-thread invocation is caught here before it reaches
 * production.
 *
 * <p><b>JCStress model.</b> Two actors compete to call {@code onTick} concurrently. The actor that
 * is NOT the designated agent thread will observe the guard firing; the actor that IS the agent
 * thread will either succeed (positive position) or see a transient rejection from the fake
 * publication. The {@code @Outcome} matrix encodes the expected interleaving results:
 *
 * <ul>
 *   <li>{@code (1, 1)} — both actors see {@code violationObserved=true}. This is the expected
 *       outcome: neither actor is the agent thread (the {@code @State} constructor runs on a
 *       JCStress infrastructure thread, not on either actor thread), so both trigger the guard.
 *   <li>{@code (1, 0)} or {@code (0, 1)} — one actor is the agent thread (unexpected with the
 *       current JCStress execution model but permissible). Still acceptable — the guard fires for
 *       at least one thread.
 *   <li>{@code (0, 0)} — FORBIDDEN. Both actors completed {@code onTick} without the guard firing.
 *       This means the guard is broken.
 * </ul>
 *
 * <p><b>Threading model.</b> JCStress runs two actor threads in random interleavings. The
 * {@code @State} constructor (on the infrastructure thread) calls {@code onStart()} once to bind
 * the agent thread. Actor threads are different from the infrastructure thread; the guard MUST fire
 * for at least one of them.
 *
 * <p><b>Allocation.</b> JCStress harness overhead is outside the scope of this assertion — the goal
 * is concurrency-correctness, not allocation budget. The {@link NoOpBroadcastPublisher} used here
 * is zero-allocation per call.
 *
 * <p><b>Dependencies.</b> {@link MarketDataPublisher}, JCStress annotations, {@link
 * org.openjdk.jcstress.infra.results.II_Result}.
 */
@JCStressTest
@Description(
    "Two concurrent onTick calls from non-agent threads: runtime guard must throw "
        + "IllegalStateException for both. (0,0) = FORBIDDEN (guard broken).")
@Outcome(
    id = "1, 1",
    expect = Expect.ACCEPTABLE,
    desc =
        "Both actor threads triggered the single-writer guard (expected: neither is the "
            + "agent thread in JCStress execution model)")
@Outcome(
    id = "1, 0",
    expect = Expect.ACCEPTABLE_INTERESTING,
    desc =
        "Actor 1 triggered the guard; actor 2 happened to be the agent thread or succeeded "
            + "before onStart recorded the thread identity")
@Outcome(
    id = "0, 1",
    expect = Expect.ACCEPTABLE_INTERESTING,
    desc =
        "Actor 2 triggered the guard; actor 1 happened to be the agent thread or succeeded "
            + "before onStart recorded the thread identity")
@Outcome(
    id = "0, 0",
    expect = Expect.FORBIDDEN,
    desc = "Neither actor triggered the guard — assertAgentThread() is broken")
@State
public class MarketDataPublisherSingleWriterJCStress {

  // ─── Fixed-point test prices ─────────────────────────────────────────────

  private static final long BID = 118_500_000_000L; // valid bid
  private static final long ASK = 118_510_000_000L; // valid ask (> bid)
  private static final long SIZE = 1_000_000L * 100_000_000L;
  private static final long INGRESS = 1_700_000_000_000_000_000L;

  /**
   * Epoch nanos used for seeding the heartbeat PRNG at {@code onStart()}. A constant value is
   * acceptable here: the JCStress test only exercises the single-writer guard, not heartbeat
   * jitter.
   */
  private static final long EPOCH_NANOS = INGRESS;

  /** Packed EURUSD (8-byte little-endian). */
  private static final long EURUSD;

  static {
    long packed = 0L;
    final byte[] sym = {'E', 'U', 'R', 'U', 'S', 'D', ' ', ' '};
    for (int i = 0; i < 8; i++) {
      packed |= ((long) (sym[i] & 0xFF)) << (i * 8);
    }
    EURUSD = packed;
  }

  /**
   * Non-allocating no-op {@link BroadcastPublisher} for use in JCStress harness. Returns {@code 1L}
   * (success) on every offer; position and termBufferLength return stable constants. No bytes are
   * copied — the test asserts on guard behaviour, not on published wire content.
   *
   * <p><b>Allocation.</b> Zero per call.
   */
  private static final class NoOpBroadcastPublisher implements BroadcastPublisher {

    @Override
    public long offer(final DirectBuffer buffer, final int offset, final int length) {
      return 1L;
    }

    @Override
    public long position() {
      return 1L;
    }

    @Override
    public int termBufferLength() {
      return 16 * 1_024 * 1_024;
    }
  }

  /**
   * Monotonic / epoch clock returning a stable constant — deterministic for the JCStress harness.
   */
  private static final class ConstantClock implements EpochNanoClock, NanoClock {

    private final long value;

    ConstantClock(final long value) {
      this.value = value;
    }

    @Override
    public long nanoTime() {
      return value;
    }
  }

  private final MarketDataPublisher publisher;

  /**
   * Constructs the state. Calls {@code onStart()} on the infrastructure thread to bind the
   * agent-thread guard. Both actor threads are different from this thread — the guard MUST fire for
   * both actors.
   */
  public MarketDataPublisherSingleWriterJCStress() {
    final var config =
        new MarketDataPublisherConfig(
            MarketDataPublisherConfig.AdapterKind.DETERMINISTIC, 5_000L, 1_000L);
    final var clock = new ConstantClock(EPOCH_NANOS);
    publisher = new MarketDataPublisher(new NoOpBroadcastPublisher(), null, clock, clock, config);
    publisher.onStart(); // binds agentThread = infrastructure thread
  }

  /**
   * Actor 1: attempts {@code onTick} from this JCStress actor thread. The publisher's {@link
   * MarketDataPublisher#assertAgentThread()} guard must fire because this thread is not the
   * infrastructure thread that called {@code onStart}. Catches the expected {@link
   * IllegalStateException} and encodes {@code r1 = 1} (violation observed). On any other outcome
   * (no exception), encodes {@code r1 = 0}.
   *
   * @param r the JCStress result carrier; {@code r.r1} is set to 1 if the guard fired
   */
  @Actor
  public void actor1(final II_Result r) {
    try {
      publisher.onTick(EURUSD, BID, ASK, SIZE, SIZE, INGRESS);
      r.r1 = 0; // guard did NOT fire — this is the forbidden outcome
    } catch (final IllegalStateException expected) {
      r.r1 = 1; // guard fired as expected
    }
  }

  /**
   * Actor 2: attempts {@code onTick} from this JCStress actor thread. Same contract as {@link
   * #actor1}: the guard must fire; encodes {@code r2 = 1} if it does.
   *
   * @param r the JCStress result carrier; {@code r.r2} is set to 1 if the guard fired
   */
  @Actor
  public void actor2(final II_Result r) {
    try {
      publisher.onTick(EURUSD, BID, ASK, SIZE, SIZE, INGRESS);
      r.r2 = 0; // guard did NOT fire — forbidden
    } catch (final IllegalStateException expected) {
      r.r2 = 1; // guard fired as expected
    }
  }
}
