package com.trading.engine.fixbridge.quote;

import java.util.Objects;

/**
 * Bridge-issued opaque session identifier. One {@code SessionId} is minted at WebSocket
 * handshake (post-Auth, when the {@code sub} claim is known) and dies when the WebSocket closes —
 * never reused.
 *
 * <p><b>Purpose.</b> Stable, hashable key for the per-session quote-correlation indexes in
 * {@link SessionQuoteIndex} and for the audit-log {@code session_terminated} fan-out path. Wraps
 * a {@code String} so map lookups go through the JDK's {@code String} hash + equals (mature, well-
 * tested) without leaking the implementation choice to callers.
 *
 * <p><b>Threading.</b> Immutable; safe to share across threads. The Netty event-loop ownership
 * invariant of {@link SessionQuoteIndex} (single-thread mutation) is enforced on the index, not
 * on this value type.
 *
 * <p><b>Allocation.</b> Two allocations at session start ({@code SessionId} wrapper + the
 * underlying {@code String}). Zero on the hot path — once the {@code SessionId} reference is
 * captured by the per-session handler, every map lookup re-uses it.
 *
 * <p><b>Lifecycle.</b> Per-session.
 *
 * <p><b>Dependencies.</b> JDK only.
 */
public final class SessionId {

  private final String value;

  /**
   * @param value the opaque session id (e.g. a 26-char ULID); must be non-null and non-empty
   * @throws NullPointerException if {@code value} is null
   * @throws IllegalArgumentException if {@code value} is empty
   */
  public SessionId(final String value) {
    if (value == null) {
      throw new NullPointerException("value must not be null");
    }
    if (value.isEmpty()) {
      throw new IllegalArgumentException("value must not be empty");
    }
    this.value = value;
  }

  /**
   * Underlying string form. Intended for audit-log emission and {@code toString} debugging only —
   * not for downstream comparison; use {@link #equals(Object)} for that.
   *
   * @return the wrapped string
   */
  public String value() {
    return value;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof final SessionId other)) {
      return false;
    }
    return value.equals(other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  @Override
  public String toString() {
    return "SessionId[" + value + "]";
  }
}
