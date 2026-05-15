/**
 * OrderEntryForm — minimal production form for browser-→-cluster order submission
 * (plan §12, APP-160). Renders a NewOrderSingle composer (symbol/side/qty/price)
 * and submits via {@link useOrderSubmission}.
 *
 * Validation is client-side, fixed-point, and rendered via React text nodes only
 * (no `dangerouslySetInnerHTML`):
 *   - symbol matches `^[A-Z]{3}/[A-Z]{3}$`
 *   - qty is a positive decimal in `[0.0000_0001, 10_000_000]` (× 10^8 fixed-point)
 *   - price is a positive decimal (same fixed-point scale)
 *
 * Client-side rate-limit: max 10 submits/s; the submit button is disabled
 * while the previous submit is pending.
 */
import { useCallback, useMemo, useRef, useState, type ReactElement } from "react";
import { PRICE_SCALE, type NewOrderSinglePayload } from "@/main-thread/commandClient";
import { useOrderSubmission } from "@/main-thread/useOrderSubmission";

// Accepts FIX-style "EUR/USD" (with slash, common UI rendering) OR the
// no-slash 6-char form "EURUSD" the cluster + pricing-service use as the
// canonical wire symbol. The encoder strips the slash before SBE-encoding so
// both render to the same on-wire bytes.
const SYMBOL_REGEX = /^[A-Z]{3}\/?[A-Z]{3}$/;
const MIN_QTY_FP = 1n;
const MAX_QTY_FP = 10_000_000n * PRICE_SCALE;
const MIN_SUBMIT_INTERVAL_MS = 100;

function decimalToFixedPoint(s: string): bigint | null {
  if (!/^[0-9]+(\.[0-9]+)?$/.test(s)) return null;
  const dot = s.indexOf(".");
  if (dot < 0) return BigInt(s) * PRICE_SCALE;
  const whole = s.slice(0, dot);
  const frac = s.slice(dot + 1);
  if (frac.length > 8) return null;
  const padded = (frac + "00000000").slice(0, 8);
  return BigInt(whole) * PRICE_SCALE + BigInt(padded);
}

export function OrderEntryForm(): ReactElement {
  const { state, submit, reset } = useOrderSubmission();
  const [clOrdId, setClOrdId] = useState<string>("");
  const [symbol, setSymbol] = useState<string>("EUR/USD");
  const [side, setSide] = useState<"buy" | "sell">("buy");
  const [qty, setQty] = useState<string>("1.0");
  const [price, setPrice] = useState<string>("1.05");
  const [error, setError] = useState<string | null>(null);
  const lastSubmitRef = useRef<number>(0);

  const validated = useMemo<{ payload: NewOrderSinglePayload | null; error: string | null }>(() => {
    if (!SYMBOL_REGEX.test(symbol)) {
      return { payload: null, error: "symbol must match XXX/YYY" };
    }
    const qtyFp = decimalToFixedPoint(qty);
    if (qtyFp === null || qtyFp < MIN_QTY_FP || qtyFp > MAX_QTY_FP) {
      return { payload: null, error: "qty out of range" };
    }
    const priceFp = decimalToFixedPoint(price);
    if (priceFp === null || priceFp <= 0n) {
      return { payload: null, error: "price must be positive decimal" };
    }
    const id = clOrdId || `UI-${crypto.randomUUID()}`;
    // Strip slash to canonical 6-char form — cluster expects "EURUSD".
    const canonicalSymbol = symbol.replace("/", "");
    return {
      payload: { clOrdId: id, symbol: canonicalSymbol, side, qty: qtyFp, price: priceFp },
      error: null,
    };
  }, [clOrdId, symbol, side, qty, price]);

  const onSubmit = useCallback(
    (e: { preventDefault(): void }): void => {
      e.preventDefault();
      if (validated.error) {
        setError(validated.error);
        return;
      }
      // Use performance.now() (monotonic) so an NTP step / VM snapshot restore
      // can't let bursts slip through the rate-limit window. Reviewer A LOW.
      const now = performance.now();
      if (now - lastSubmitRef.current < MIN_SUBMIT_INTERVAL_MS) {
        setError("rate-limit: max 10 submits/s");
        return;
      }
      lastSubmitRef.current = now;
      setError(null);
      const payload = validated.payload;
      if (payload === null) return;
      // Submit returns a Promise the form does not await; the hook publishes
      // resolution + rejection state via `state` for the JSX to render.
      void submit(payload);
    },
    [submit, validated],
  );

  const isLoading = state.kind === "loading";
  const submitState = isLoading ? "loading" : "idle";

  return (
    <form onSubmit={onSubmit} aria-label="Order Entry">
      <label>
        ClOrdID
        <input
          data-testid="order-entry-clord-id"
          value={clOrdId}
          onChange={(e) => {
            setClOrdId(e.target.value);
          }}
          placeholder="(auto)"
        />
      </label>
      <label>
        Symbol
        <input
          data-testid="order-entry-symbol"
          value={symbol}
          onChange={(e) => {
            setSymbol(e.target.value);
          }}
        />
      </label>
      <label>
        Side
        <select
          data-testid="order-entry-side"
          value={side}
          onChange={(e) => {
            setSide(e.target.value === "sell" ? "sell" : "buy");
          }}
        >
          <option value="buy">buy</option>
          <option value="sell">sell</option>
        </select>
      </label>
      <label>
        Qty
        <input
          data-testid="order-entry-qty"
          value={qty}
          onChange={(e) => {
            setQty(e.target.value);
          }}
        />
      </label>
      <label>
        Price
        <input
          data-testid="order-entry-price"
          value={price}
          onChange={(e) => {
            setPrice(e.target.value);
          }}
        />
      </label>
      <button
        type="submit"
        data-testid="order-entry-submit"
        data-state={submitState}
        disabled={isLoading || validated.error !== null}
      >
        Submit
      </button>
      {error !== null && (
        <div data-testid="order-entry-validation-error" role="alert">
          {error}
        </div>
      )}
      {state.kind === "error" && (
        <div data-testid="order-entry-error" role="alert">
          {state.message}
        </div>
      )}
      {state.kind === "success" && (
        <div data-testid="order-entry-success" role="status">
          Accepted (correlationId={state.ack.correlationId})
          <button type="button" onClick={reset}>
            Reset
          </button>
        </div>
      )}
    </form>
  );
}
