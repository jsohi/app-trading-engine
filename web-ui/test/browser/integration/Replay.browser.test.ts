/**
 * Replay.browser.test.ts — drop mid-stream → reconnect → SessionResume →
 * ReplayComplete integration test per APP-36 §2.6.
 *
 * Tier: @vitest/browser (Chromium). Skipped until the Chromium runner is wired
 * in CI per the C9 follow-up.
 *
 * C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
 *   Requires a mock WebSocket server that can inject a mid-stream disconnect,
 *   then observe that the worker sends SessionResume (template 69) after the
 *   new AuthAck, replays from lastSeqNo, and emits ReplayComplete without
 *   duplicates or gaps.
 *
 * Threading: browser main thread + worker thread.
 * Allocation: test-only.
 *
 * Plan reference: APP-36 §2.6 / §5.8.
 */

import { describe, it } from "vitest";

describe("Replay (browser)", () => {
  it.skip("replay_dropMidStream_reconnect_sessionResume_replayComplete_noDuplicates", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
    // Steps when enabled:
    //   1. Establish a worker session; receive N reliable frames.
    //   2. Simulate network drop (server closes with 1006).
    //   3. On reconnect, worker sends Auth, receives AuthAck.
    //   4. Worker sends SessionResume{priorSessionId, lastSeqNo=N}.
    //   5. Server replays frames N+1..M with FLAG_RELIABLE | FLAG_REPLAY.
    //   6. Server sends ReplayComplete.
    //   7. Assert messages$.values are monotonically ordered, no duplicates,
    //      no gaps between seqNo N and M.
  });

  it.skip("replay_bareReplayCompleteBeforeAuthAck_protocolViolation", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
    // Scenario: server sends ReplayComplete before AuthAck on a new connection.
    // Worker must close with PROTOCOL_VIOLATION per §2.6.
  });

  it.skip("replay_bufferOverflowAfterSessionResume_coldStart", () => {
    // C9 follow-up: needs @vitest/browser harness — Chromium runner not yet wired.
    // Scenario: server sends WebSocketError{code=7, BufferOverflow} after
    // SessionResume — worker cold-starts (both session IDs cleared).
  });
});
