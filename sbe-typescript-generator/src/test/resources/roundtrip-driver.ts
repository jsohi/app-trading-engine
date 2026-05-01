// Java↔TS round-trip driver for APP-34 chunk-13 RoundTripTest.
// Spawned by RoundTripTest.java's @Nested TsxRoundTripTests via ProcessBuilder.
//
// Stdout discipline (LOAD-BEARING): emit JSON exclusively via process.stdout.write.
// Other logging mechanisms are forbidden because they are async-buffered and inject
// a trailing newline that would corrupt the Java-side readAllBytes() parse.
//
// bigint discipline: bigints serialise as decimal strings via bigintReplacer below.
// Java parses via new BigInteger(node.asText()) so int64 boundary values (incl.
// Long.MAX_VALUE) round-trip without precision loss. Casting through the JS numeric
// primitive would silently truncate above 2^53.
//
// Null-sentinel: optional fields at SBE null sentinel decode to TS null → emit as
// JSON null. Java side branches on node.isNull(). Translating the sentinel to a
// stand-in string would silently break that branch.
//
// Hex format: lowercase [0-9a-f], even-length. Buffer.from('hex') silently truncates
// on invalid characters; the regex guard fails loudly instead.
//
// Buffer-pool note: Node's Buffer.from('hex') may return a slice of a shared internal
// pool; the offset+length args to `new DataView(...)` are LOAD-BEARING — DO NOT
// simplify to `new DataView(buffer.buffer)`, that would expose the entire pool.

import {
    route,
    type DecodedFrame,
    WebSocketAuthDecoder,
    WebSocketAuthAckDecoder,
    QuoteDecoder,
    QuoteRequestDecoder,
    RfqStateSnapshotDecoder,
    toFixed8,
    parseFixed8,
} from "@trading/sbe-codecs";

function bigintReplacer(_key: string, value: unknown): unknown {
    return typeof value === "bigint" ? value.toString() : value;
}

function hexToView(hex: string): DataView {
    if (hex.length === 0) {
        throw new Error("hex must not be empty");
    }
    if (hex.length % 2 !== 0) {
        throw new Error(`hex must be even length, got ${hex.length}`);
    }
    if (!/^[0-9a-f]+$/.test(hex)) {
        throw new Error(
            `hex must be lowercase [0-9a-f] only, got: ${hex.slice(0, 32)}…`,
        );
    }
    const buffer = Buffer.from(hex, "hex");
    // offset+length args LOAD-BEARING — see file header.
    return new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
}

/**
 * Extract decoded message fields from a DecodedFrame's currently-bound decoder. Emits a
 * stable JSON shape per templateId. bigint fields are returned as bigints (the outer
 * JSON.stringify with bigintReplacer handles serialisation). null-sentinel fields are
 * returned as TS null → JSON null.
 */
function extractFields(frame: DecodedFrame): Record<string, unknown> {
    switch (frame.templateId) {
        case WebSocketAuthDecoder.TEMPLATE_ID: {
            const d = frame.decoder as WebSocketAuthDecoder;
            const tokenBytes = d.token();
            return {
                protocolVersion: d.protocolVersion(),
                token: Buffer.from(tokenBytes).toString("utf-8"),
            };
        }
        case WebSocketAuthAckDecoder.TEMPLATE_ID: {
            const d = frame.decoder as WebSocketAuthAckDecoder;
            const sessionId = d.sessionId();
            return {
                sessionIdMsb: sessionId.msb,
                sessionIdLsb: sessionId.lsb,
                protocolVersion: d.protocolVersion(),
                maxSubscriptions: d.maxSubscriptions(),
            };
        }
        case QuoteDecoder.TEMPLATE_ID: {
            const d = frame.decoder as QuoteDecoder;
            // Read every field the chunk-13 Quote tests inspect. Optional fields surface as
            // bigint | null / EnumName | null and pass through to JSON as bigint-string / null.
            const bidPx = d.bidPx();
            return {
                quoteReqId: d.quoteReqId(),
                quoteId: d.quoteId(),
                symbol: d.symbol(),
                side: d.side(),
                bidPx,
                offerPx: d.offerPx(),
                bidSize: d.bidSize(),
                offerSize: d.offerSize(),
                transactTime: d.transactTime(),
                settlType: d.settlType(),
                swapPoints: d.swapPoints(),
                // Helpers exposure for roundTrip_quote_bidPxThroughHelpersToFixed8RoundTrips —
                // emit both the textual fixed-8 rendering and the parseFixed8 round-trip so the
                // Java side can assert helpers correctness end-to-end via the same JSON envelope.
                bidPxFixed8Display: toFixed8(bidPx),
                bidPxRoundTripped: parseFixed8(toFixed8(bidPx)),
            };
        }
        case QuoteRequestDecoder.TEMPLATE_ID: {
            const d = frame.decoder as QuoteRequestDecoder;
            return {
                quoteReqId: d.quoteReqId(),
                symbol: d.symbol(),
                side: d.side(),
                orderQty: d.orderQty(),
                settlType: d.settlType(),
                currency: d.currency(),
                tenor: d.tenor(),
            };
        }
        case RfqStateSnapshotDecoder.TEMPLATE_ID: {
            const d = frame.decoder as RfqStateSnapshotDecoder;
            const noRfqs = d.noRfqs();
            const rfqs: Array<Record<string, unknown>> = [];
            while (noRfqs.hasNext()) {
                noRfqs.next();
                const quoteReqId = noRfqs.quoteReqId();
                const accountId = noRfqs.accountId();
                const state = noRfqs.state();
                const symbol = noRfqs.symbol();
                const side = noRfqs.side();
                const orderQty = noRfqs.orderQty();
                // Inner group MUST be drained before the outer iterator advances — encoder
                // ordering and decoder reading discipline are mirror images.
                const noLegs = noRfqs.noLegs();
                const legs: Array<Record<string, unknown>> = [];
                while (noLegs.hasNext()) {
                    noLegs.next();
                    legs.push({
                        legSide: noLegs.legSide(),
                        legSettlDate: noLegs.legSettlDate(),
                        legCurrency: noLegs.legCurrency(),
                        legOrderQty: noLegs.legOrderQty(),
                    });
                }
                rfqs.push({
                    quoteReqId,
                    accountId,
                    state,
                    symbol,
                    side,
                    orderQty,
                    noLegs: legs,
                });
            }
            return { noRfqs: rfqs };
        }
        default:
            throw new Error(`Unhandled templateId ${frame.templateId}`);
    }
}

const mode = process.argv[2];

if (mode === "single") {
    const hex = process.argv[3];
    const view = hexToView(hex);
    const frame = route(view, 0);
    const decoded = {
        templateId: frame.templateId,
        schemaId: frame.schemaId,
        schemaVersion: frame.schemaVersion,
        blockLength: frame.blockLength,
        fields: extractFields(frame),
    };
    process.stdout.write(JSON.stringify(decoded, bigintReplacer));
} else if (mode === "alias") {
    // Aliasing-rebind contract: hold a reference to `frame` across two route() calls
    // and verify the second call rebinds the SAME instance to the new buffer. Captures
    // the held-reference's fields BEFORE and AFTER the second route().
    const hexA = process.argv[3];
    const hexB = process.argv[4];
    const viewA = hexToView(hexA);
    const viewB = hexToView(hexB);

    const heldFrame = route(viewA, 0);
    const generationBefore = heldFrame.__generation ?? null;
    const captureBefore = {
        templateId: heldFrame.templateId,
        fields: extractFields(heldFrame),
    };

    const secondFrame = route(viewB, 0);
    // Same flyweight: heldFrame === secondFrame. Java side asserts captureAfter
    // (read off heldFrame) reflects buffer B's contents, proving rebind.
    const sameInstance = heldFrame === secondFrame;
    const generationAfter = heldFrame.__generation ?? null;
    const captureAfter = {
        templateId: heldFrame.templateId,
        fields: extractFields(heldFrame),
    };

    process.stdout.write(
        JSON.stringify(
            {
                sameInstance,
                generationBefore,
                generationAfter,
                captureBefore,
                captureAfter,
            },
            bigintReplacer,
        ),
    );
} else if (mode === "multi") {
    // Multi-message stream contract: one buffer holds two encoded messages at known
    // offsets; route(view, offsetA) then route(view, offsetB) must both succeed.
    // Captures decoded fields BEFORE the second route() so the first capture's data
    // is not corrupted by the shared-flyweight rebind.
    const hex = process.argv[3];
    const offsetA = Number(process.argv[4]);
    const offsetB = Number(process.argv[5]);
    const view = hexToView(hex);

    const frameA = route(view, offsetA);
    const captureA = {
        templateId: frameA.templateId,
        fields: extractFields(frameA),
    };

    const frameB = route(view, offsetB);
    const captureB = {
        templateId: frameB.templateId,
        fields: extractFields(frameB),
    };

    process.stdout.write(
        JSON.stringify({ first: captureA, second: captureB }, bigintReplacer),
    );
} else {
    throw new Error(`Unknown mode: ${mode} (expected single | alias | multi)`);
}
