/**
 * feed-state-stream — BehaviorSubject of market-data feed-liveness state.
 *
 * Mirrors the {@code connection-stream} pattern: a module-private {@code let _subject}
 * BehaviorSubject seeded {@code "LIVE"}, exposed publicly as a {@code defer(...)} wrapper so
 * test-isolation resets can swap the inner subject without invalidating the exported binding.
 *
 * <p><b>Why separate from {@code connectionStream$}?</b> A STALE market-data feed MUST NOT
 * trip the WebSocket reconnect breaker. The WS transport is healthy (the worker is still
 * receiving heartbeats); only the price-feed thread on the publisher side is dead. Keeping the
 * two streams independent lets the UI surface a "feed stale" banner without simultaneously
 * showing a "disconnecting" indicator (which would falsely imply the user is being logged out).
 * EBS Direct / ICE Impact pattern: feed-health and transport-health are orthogonal.
 *
 * <p><b>Reset-on-reconnect contract.</b> The worker {@code WorkerClient.onConnect} handler
 * MUST push {@code "LIVE"} on every {@code CONNECTED} transition — the prior state was
 * captured pre-disconnect and is stale; the post-reconnect server emits its own
 * {@code MarketDataFeedStateChange} (template 57) which the worker then forwards. Without this
 * reset, a reconnect after a STALE state would carry the stale flag forward and falsely flag a
 * healthy post-reconnect publisher.
 *
 * <p><b>Threading:</b> main thread.
 *
 * <p><b>Allocation:</b> per state-change emission only (RxJS Subject internal).
 *
 * <p><b>Dependencies:</b> {@code @/shared/transport/MessageShape} — peer: {@code FeedState}
 * type.
 *
 * @see connection-stream peer with identical defer-and-swap pattern.
 * @see messageSource — where {@code FeedStateMsg} is dispatched to {@link pushFeedState}.
 */

import { BehaviorSubject, type Observable, defer } from "rxjs";

import { type FeedState } from "@/shared/transport/MessageShape";

// Module-private; `let` so `__resetFeedStateStreamForTests()` can swap.
let _subject: BehaviorSubject<FeedState> = new BehaviorSubject<FeedState>("LIVE");

/**
 * Public feed-state stream. Late subscribers receive the most recent state immediately
 * (BehaviorSubject seed-replay). The {@code defer(...)} wrapper reads the CURRENT
 * {@code _subject} at subscribe time so test resets that swap the inner subject are visible
 * to fresh subscribers.
 */
export const feedState$: Observable<FeedState> = defer(() => _subject.asObservable());

/**
 * Internal pusher — {@code messageSource} calls this on every {@code FeedStateMsg}
 * dispatched from the worker (template 57 frame OR the reconnect-reset path).
 */
export function pushFeedState(next: FeedState): void {
  _subject.next(next);
}

/**
 * @internal — test-isolation reset. Called from the global {@code afterEach} so React
 * subscribers are unmounted first. Completes the old subject then swaps in a fresh
 * BehaviorSubject seeded {@code "LIVE"}.
 *
 * Production code MUST NOT call this — it would terminate every active subscription.
 */
export function __resetFeedStateStreamForTests(): void {
  _subject.complete();
  _subject = new BehaviorSubject<FeedState>("LIVE");
}

/**
 * HMR-safe dispose. Mirrors {@code connection-stream}'s pattern: completes the OLD subject so
 * consumers retaining stale references to the OLD {@code defer} closure see a {@code
 * complete} notification. On module re-evaluation Vite re-runs the top-level initialiser, so
 * the NEW subject is supplied via the {@code defer(...)} wrapper.
 */
if (import.meta.hot) {
  import.meta.hot.dispose(() => {
    _subject.complete();
  });
}
