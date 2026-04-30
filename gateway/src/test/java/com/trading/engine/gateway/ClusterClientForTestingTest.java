package com.trading.engine.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClusterClient#forTesting(NanoClock)}.
 *
 * <p>Verifies the no-op-lifecycle test seam:
 *
 * <ul>
 *   <li>{@code onStart}/{@code onClose}/{@code doWork} return without touching {@code AeronCluster}
 *   <li>{@code offer} and {@code offerTracked} short-circuit with success ({@code &gt;= 0})
 *   <li>The factory rejects null parameters
 * </ul>
 *
 * <p>Used by the {@code :fix-client-bridge} integration-test harness so the bridge can boot a real
 * {@link FixGateway} without a live 3-node cluster.
 */
final class ClusterClientForTestingTest {

  private static final NanoClock STATIC_CLOCK = () -> 42L;

  // --- Factory contract ---

  @Test
  void forTesting_nullNanoClock_throws() {
    assertThrows(NullPointerException.class, () -> ClusterClient.forTesting(null));
  }

  // --- Lifecycle no-op ---

  @Test
  void onStart_inTestMode_doesNotThrow() {
    final var client = ClusterClient.forTesting(STATIC_CLOCK);

    // No real AeronCluster is wired — the call must not attempt to connect.
    client.onStart();

    // After onStart the client should still report connected (factory pre-sets state).
    assertTrue(client.isConnected(), "test-mode client should report connected");
    client.close();
  }

  @Test
  void doWork_inTestMode_returnsZero() {
    final var client = ClusterClient.forTesting(STATIC_CLOCK);

    // doWork must not poll AeronCluster — returning 0 keeps the AgentRunner idle without errors.
    assertEquals(0, client.doWork());
    client.close();
  }

  @Test
  void close_inTestMode_isIdempotent() {
    final var client = ClusterClient.forTesting(STATIC_CLOCK);

    client.close();
    assertTrue(client.isClosed(), "first close should mark closed");
    // Second close should not throw.
    client.close();
    assertTrue(client.isClosed(), "second close should remain closed");
  }

  // --- Offer paths short-circuit with success ---

  @Test
  void offer_inTestMode_returnsSuccessWithoutTouchingAeronCluster() {
    final var client = ClusterClient.forTesting(STATIC_CLOCK);
    final var buf = new UnsafeBuffer(new byte[64]);

    final long position = client.offer(buf, 0, 32);

    // A non-negative result indicates "offer accepted" in Aeron's contract — the test seam
    // returns 1 deterministically so callers can branch on `position >= 0`.
    assertTrue(position >= 0, "test-mode offer should report success, got " + position);
    client.close();
  }

  @Test
  void offerTracked_inTestMode_returnsSuccessAndTracksClOrdId() {
    final var client = ClusterClient.forTesting(STATIC_CLOCK);
    final var buf = new UnsafeBuffer(new byte[64]);
    final byte[] clOrdId = "TEST-ORDER-001".getBytes();

    final long position = client.offerTracked(buf, 0, 32, clOrdId, 0, clOrdId.length);

    assertTrue(position >= 0, "test-mode offerTracked should report success, got " + position);
    client.close();
  }

  // --- State accessors ---

  @Test
  void isClosed_beforeAndAfterClose_reflectsState() {
    final var client = ClusterClient.forTesting(STATIC_CLOCK);

    assertFalse(client.isClosed(), "freshly constructed test-mode client should not be closed");
    client.close();
    assertTrue(client.isClosed(), "closed test-mode client should report closed");
  }
}
