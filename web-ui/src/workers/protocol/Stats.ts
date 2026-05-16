/**
 * Stats — in-worker integer counters posted to main via STATS message
 * every 1 s.
 *
 * APP-245 (RUM / OTel-JS metrics SDK) bridges these to the OTel meter
 * when vendor decision lands; until then the counters are surfaced for
 * UI status display.
 *
 * Threading: worker scope only.
 *
 * Allocation: zero per increment (bigint mutation in place). One JS
 * object allocated per 1 s flush for the postMessage payload; cold path.
 *
 * Plan reference: §3 / §5.3 / §6 row 18 / §6 row 42.
 */

export interface StatsSnapshot {
  framesDecoded: bigint;
  bytesDecoded: bigint;
  crcMismatches: bigint;
  gaps: bigint;
  reconnects: bigint;
  replayFrames: bigint;
  snapshotBytes: bigint;
  bufferedAmountPeak: bigint;
  /**
   * Cumulative count of inbound PriceResponse (template 51) frames reaching the browser worker.
   *
   * <p>Template 51 is orchestrator-bound: the cluster routes PriceResponse to the orchestrator's
   * own session, never to the browser worker's session. Arrival here means a broadcast routing
   * regression — see Phase 3 plan §Gap 2 semantic separation between orchestrator-bound RFQ
   * pricing and browser-bound market-data ticks. A non-zero value should fail the per-spec metric
   * assertion in spec 07 (replay/reconnect) and trip an alert in production. Surfaced through the
   * STATS pipeline (APP-245 OTel bridge).
   */
  marketdataMisroutedRfq: bigint;
  degradedTimingMode: boolean;
}

export class Stats {
  private framesDecoded = 0n;
  private bytesDecoded = 0n;
  private crcMismatches = 0n;
  private gaps = 0n;
  private reconnects = 0n;
  private replayFrames = 0n;
  private snapshotBytes = 0n;
  private bufferedAmountPeak = 0n;
  private marketdataMisroutedRfq = 0n;
  private degradedTimingMode = false;

  incFramesDecoded(): void {
    this.framesDecoded += 1n;
  }
  addBytes(n: number): void {
    this.bytesDecoded += BigInt(n);
  }
  incCrcMismatch(): void {
    this.crcMismatches += 1n;
  }
  incGap(): void {
    this.gaps += 1n;
  }
  incReconnect(): void {
    this.reconnects += 1n;
  }
  addReplayFrames(n: number): void {
    this.replayFrames += BigInt(n);
  }
  addSnapshotBytes(n: number): void {
    this.snapshotBytes += BigInt(n);
  }
  observeBufferedAmount(n: number): void {
    const big = BigInt(n);
    if (big > this.bufferedAmountPeak) this.bufferedAmountPeak = big;
  }
  /**
   * Increments the misrouted-RFQ counter. Called by the worker's `onEvent` dispatch when a
   * PriceResponse (template 51) is observed — that template is orchestrator-bound and must never
   * reach the browser; arrival here is a routing regression. Phase 3 Commit 3 wires this; Phase 3
   * spec 07 asserts the counter remains zero across replay/reconnect.
   */
  incMarketdataMisroutedRfq(): void {
    this.marketdataMisroutedRfq += 1n;
  }
  setDegradedTimingMode(b: boolean): void {
    this.degradedTimingMode = b;
  }

  snapshot(): StatsSnapshot {
    return {
      framesDecoded: this.framesDecoded,
      bytesDecoded: this.bytesDecoded,
      crcMismatches: this.crcMismatches,
      gaps: this.gaps,
      reconnects: this.reconnects,
      replayFrames: this.replayFrames,
      snapshotBytes: this.snapshotBytes,
      bufferedAmountPeak: this.bufferedAmountPeak,
      marketdataMisroutedRfq: this.marketdataMisroutedRfq,
      degradedTimingMode: this.degradedTimingMode,
    };
  }
}
