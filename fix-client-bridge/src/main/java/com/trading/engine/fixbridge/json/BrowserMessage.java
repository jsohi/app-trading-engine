package com.trading.engine.fixbridge.json;

/**
 * Sealed model of the six inbound browser-to-bridge JSON message kinds.
 *
 * <p><b>Purpose.</b> Test-facing immutable record types that mirror the wire protocol described in
 * {@code docs/fix-client-bridge.md}. The production hot path does NOT decode JSON into these
 * records — it uses the {@link MutableParsedMessage} flyweight to avoid per-frame allocation. These
 * records exist so that unit tests (and in particular {@code JsonToFixTranslatorTest}) can build
 * input fixtures using a strongly-typed, easy-to-read API and assert byte-exact translation output.
 * They also serve as the canonical specification of which fields each message carries.
 *
 * <p><b>Threading.</b> All permitted records are immutable and safe to share across threads.
 *
 * <p><b>Allocation.</b> Records allocate on construction. They are intentionally NOT used on the
 * hot dispatch path — see {@link MutableParsedMessage} for the zero-alloc flyweight.
 *
 * <p><b>Lifecycle.</b> Per-instance; throw-away test fixtures.
 *
 * <p><b>Dependencies.</b> JDK only.
 *
 * <p><b>Wire protocol.</b>
 *
 * <pre>
 * Auth                : {"type":"Auth","token":"&lt;jwt&gt;"}
 * QuoteRequest        : {"type":"QuoteRequest","reqId":"&lt;id&gt;","symbol":"EURUSD",
 *                        "side":"Buy|Sell","qty":"&lt;decimal&gt;"}
 * AcceptQuote         : {"type":"AcceptQuote","quoteId":"&lt;qid&gt;","clOrdId":"&lt;cid&gt;"}
 * RejectQuote         : {"type":"RejectQuote","quoteId":"&lt;qid&gt;"}
 * NewOrderSingle      : {"type":"NewOrderSingle","clOrdId":"&lt;cid&gt;","symbol":"EURUSD",
 *                        "side":"Buy|Sell","qty":"&lt;dec&gt;","price":"&lt;dec&gt;",
 *                        "ordType":"Limit|Market","timeInForce":"GTC|IOC|FOK|DAY|GTD",
 *                        "account":"&lt;acct&gt;"}
 * CancelOrder         : {"type":"CancelOrder","clOrdId":"&lt;new&gt;","origClOrdId":"&lt;old&gt;",
 *                        "symbol":"EURUSD","side":"Buy|Sell"}
 * OrderStatusRequest  : {"type":"OrderStatusRequest","clOrdId":"&lt;cid&gt;"}
 * </pre>
 *
 * <p>Every message MAY additionally carry an optional {@code "_meta"} envelope (§3.6) for trace
 * propagation:
 *
 * <pre>
 * "_meta": {"traceparent":"&lt;w3c-traceparent&gt;"}
 * </pre>
 *
 * <p>The parser ignores other {@code _meta} fields via skip-balanced; only {@code traceparent} is
 * extracted. {@code idempotencyKey} was removed in v4 (§B-r2-16) — {@code clOrdId} is the only
 * idempotency key for order-issuing commands.
 *
 * <p>Decimal numerics ({@code qty}, {@code price}) are JSON strings — never JSON numbers — to avoid
 * IEEE-754 round-tripping. The parser rejects fractional precision finer than {@code 10^-8} (locked
 * §3).
 *
 * <p>String-valued enums ({@code side}, {@code ordType}, {@code timeInForce}) are reproduced in
 * their canonical FIX 4.4 spelling. The parser maps them to the byte codes encoded in {@link
 * MutableParsedMessage} ({@code '1'}/{@code '2'} for Buy/Sell, {@code '1'}/{@code '2'} for
 * Market/Limit, etc.).
 */
public sealed interface BrowserMessage
    permits BrowserMessage.Auth,
        BrowserMessage.QuoteRequest,
        BrowserMessage.AcceptQuote,
        BrowserMessage.RejectQuote,
        BrowserMessage.NewOrderSingleCmd,
        BrowserMessage.CancelOrder,
        BrowserMessage.OrderStatusRequest {

  /**
   * First-frame authentication request. The bridge validates {@code token} via {@code
   * JwtValidator.validate} on a worker-pool thread (see Locked §12); the channel is closed if the
   * token is missing, malformed, expired, or fails JWKS verification.
   *
   * @param token JWT compact-serialised by the auth provider
   */
  record Auth(String token) implements BrowserMessage {}

  /**
   * RFQ initiation. Translated to FIX {@code QuoteRequest (35=R)} via {@code JsonToFixTranslator
   * .handleQuoteRequest}.
   *
   * @param reqId client-chosen correlation id; round-tripped on the resulting {@code Quote}
   * @param symbol FIX {@code Symbol (55)} — e.g. {@code EURUSD}
   * @param side FIX {@code Side (54)} as the canonical FIX spelling: {@code "Buy"} or {@code
   *     "Sell"}
   * @param qty FIX {@code OrderQty (38)} as a decimal string with up to 8 fractional digits
   */
  record QuoteRequest(String reqId, String symbol, String side, String qty)
      implements BrowserMessage {}

  /**
   * Accept a previously-received {@code Quote}. The translator pulls {@code symbol/side/qty/price}
   * from the per-session quote cache (locked §2) and emits FIX {@code NewOrderSingle (35=D)} with
   * {@code OrdType=D (Previously Quoted)} and {@code QuoteID (117) = quoteId}.
   *
   * @param quoteId quote identifier echoed back from the inbound {@code Quote}
   * @param clOrdId client order id chosen by the browser (must match the bridge's restart-stable
   *     20-byte format if the browser uses it; otherwise the bridge accepts any non-empty string of
   *     length ≤20)
   */
  record AcceptQuote(String quoteId, String clOrdId) implements BrowserMessage {}

  /**
   * Reject a previously-received {@code Quote}. Emits no FIX (locked §11); the translator simply
   * evicts the cache entry.
   *
   * @param quoteId quote identifier echoed back from the inbound {@code Quote}
   */
  record RejectQuote(String quoteId) implements BrowserMessage {}

  /**
   * Direct order entry (no preceding quote). Translated to FIX {@code NewOrderSingle (35=D)}.
   *
   * @param clOrdId client order id (≤20 bytes)
   * @param symbol FIX {@code Symbol (55)}
   * @param side FIX {@code Side (54)} — {@code "Buy"} or {@code "Sell"}
   * @param qty FIX {@code OrderQty (38)} as a decimal string
   * @param price FIX {@code Price (44)} as a decimal string (ignored when {@code ordType} is {@code
   *     "Market"})
   * @param ordType FIX {@code OrdType (40)} — {@code "Market"} or {@code "Limit"}
   * @param timeInForce FIX {@code TimeInForce (59)} — {@code "GTC"}, {@code "IOC"}, {@code "FOK"},
   *     {@code "DAY"}, or {@code "GTD"}
   * @param account FIX {@code Account (1)}
   */
  record NewOrderSingleCmd(
      String clOrdId,
      String symbol,
      String side,
      String qty,
      String price,
      String ordType,
      String timeInForce,
      String account)
      implements BrowserMessage {}

  /**
   * Cancel a working order. Translated to FIX {@code OrderCancelRequest (35=F)}.
   *
   * @param clOrdId NEW client order id for the cancel request itself
   * @param origClOrdId the {@code ClOrdID (11)} of the order being cancelled
   * @param symbol FIX {@code Symbol (55)} — Artio requires it on cancel
   * @param side FIX {@code Side (54)} — Artio requires it on cancel
   */
  record CancelOrder(String clOrdId, String origClOrdId, String symbol, String side)
      implements BrowserMessage {}

  /**
   * Status query for an order whose UI state is STUCK or STUCK_LONG (§3.15 / §4.5). Recovery path
   * — excluded from per-type rate limiter. Bridge forwards to the cluster's {@code
   * OrderQueryByClOrdId} projection; the reply is emitted as {@link
   * BrowserEvent.OrderStatusReply}. 5s server-side timeout produces a reply with {@code
   * status="Unknown"}.
   *
   * @param clOrdId originating client order id (≤20 bytes, RFC4648 base32 per §4.14)
   */
  record OrderStatusRequest(String clOrdId) implements BrowserMessage {}
}
