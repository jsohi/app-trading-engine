/**
 * Round-trip test for the hand-written NewOrderSingleEncoder.
 *
 * Encodes via the encoder, decodes via the AUTO-GENERATED decoder, asserts every field round-trips
 * byte-identically. This is the only thing protecting the hand-written wire layout from drifting
 * out of sync with the schema — the test fails loud on any offset mismatch.
 *
 * Plan reference: APP-160-W (real Web Worker SBE wiring) gating test.
 */
import { describe, expect, it } from "vitest";
import { NewOrderSingleEncoder } from "@/sbe/encoders/NewOrderSingleEncoder";
import {
  NewOrderSingleDecoder,
  OrdTypeEnum,
  ProductTypeEnum,
  SettlTypeEnum,
  SideEnum,
  TenorEnum,
  TimeInForceEnum,
} from "@trading/sbe-codecs";

describe("NewOrderSingleEncoder", () => {
  it("encodes a Limit Buy with all required fields and round-trips through the generated decoder", () => {
    const buf = new ArrayBuffer(NewOrderSingleEncoder.ENCODED_FRAME_LENGTH);
    const view = new DataView(buf);
    const enc = new NewOrderSingleEncoder();
    enc.wrapAndApplyHeader(view, 0).setFields({
      clOrdId: "E2E-12345",
      quoteId: "",
      symbol: "EURUSD",
      side: SideEnum.Buy,
      ordType: OrdTypeEnum.Limit,
      price: 105_000_000n,
      orderQty: 100_000_000n,
      timeInForce: TimeInForceEnum.Day,
      transactTime: 1_700_000_000_000_000_000n,
      accountCode: "ACME-001",
      productType: ProductTypeEnum.Spot,
      settlDate: "20260518",
      settlType: SettlTypeEnum.Regular,
      currency: "USD",
      settlCurrency: "",
      tenor: TenorEnum.SN,
    });

    // Header decode — confirm template/schema/version match.
    expect(view.getUint16(0, true)).toBe(NewOrderSingleEncoder.BLOCK_LENGTH);
    expect(view.getUint16(2, true)).toBe(NewOrderSingleEncoder.TEMPLATE_ID);
    expect(view.getUint16(4, true)).toBe(NewOrderSingleEncoder.SCHEMA_ID);
    expect(view.getUint16(6, true)).toBe(NewOrderSingleEncoder.SCHEMA_VERSION);

    // Block decode via the generated decoder — every field must round-trip.
    const dec = new NewOrderSingleDecoder().wrap(view, 8);
    expect(dec.clOrdId()).toBe("E2E-12345");
    expect(dec.quoteId()).toBe("");
    expect(dec.symbol()).toBe("EURUSD");
    expect(dec.side()).toBe(SideEnum.Buy);
    expect(dec.ordType()).toBe(OrdTypeEnum.Limit);
    expect(dec.price()).toBe(105_000_000n);
    expect(dec.orderQty()).toBe(100_000_000n);
    expect(dec.timeInForce()).toBe(TimeInForceEnum.Day);
    expect(dec.transactTime()).toBe(1_700_000_000_000_000_000n);
    expect(dec.accountCode()).toBe("ACME-001");
    expect(dec.productType()).toBe(ProductTypeEnum.Spot);
    expect(dec.settlDate()).toBe("20260518");
    expect(dec.settlType()).toBe(SettlTypeEnum.Regular);
    expect(dec.currency()).toBe("USD");
    expect(dec.settlCurrency()).toBe("");
    expect(dec.tenor()).toBe(TenorEnum.SN);
  });

  it("encodes a Market Sell with price=null (sentinel) and settlType=null", () => {
    const buf = new ArrayBuffer(NewOrderSingleEncoder.ENCODED_FRAME_LENGTH);
    const view = new DataView(buf);
    new NewOrderSingleEncoder().wrapAndApplyHeader(view, 0).setFields({
      clOrdId: "E2E-MKT",
      quoteId: "",
      symbol: "GBPUSD",
      side: SideEnum.Sell,
      ordType: OrdTypeEnum.Market,
      price: null,
      orderQty: 50_000_000n,
      timeInForce: TimeInForceEnum.IOC,
      transactTime: 1_700_000_000_000_000_001n,
      accountCode: "HEDGE",
      productType: ProductTypeEnum.Spot,
      settlDate: "20260518",
      settlType: null,
      currency: "GBP",
      settlCurrency: "USD",
      tenor: TenorEnum.SN,
    });
    const dec = new NewOrderSingleDecoder().wrap(view, 8);
    expect(dec.side()).toBe(SideEnum.Sell);
    expect(dec.ordType()).toBe(OrdTypeEnum.Market);
    expect(dec.price()).toBeNull();
    expect(dec.settlType()).toBeNull();
    expect(dec.currency()).toBe("GBP");
    expect(dec.settlCurrency()).toBe("USD");
  });

  it("zero-pads fixed-length char arrays so a reused pool buffer cannot leak prior bytes", () => {
    const buf = new ArrayBuffer(NewOrderSingleEncoder.ENCODED_FRAME_LENGTH);
    const view = new DataView(buf);
    // Pollute the buffer with non-zero bytes BEFORE encoding.
    new Uint8Array(buf).fill(0xff);
    new NewOrderSingleEncoder().wrapAndApplyHeader(view, 0).setFields({
      clOrdId: "X",
      quoteId: "",
      symbol: "EURUSD",
      side: SideEnum.Buy,
      ordType: OrdTypeEnum.Limit,
      price: 1n,
      orderQty: 1n,
      timeInForce: TimeInForceEnum.Day,
      transactTime: 0n,
      accountCode: "A",
      productType: ProductTypeEnum.Spot,
      settlDate: "20260518",
      settlType: SettlTypeEnum.Regular,
      currency: "USD",
      settlCurrency: "",
      tenor: TenorEnum.SN,
    });
    const dec = new NewOrderSingleDecoder().wrap(view, 8);
    // String fields must NOT carry the 0xff prior bytes.
    expect(dec.clOrdId()).toBe("X");
    expect(dec.quoteId()).toBe("");
    expect(dec.symbol()).toBe("EURUSD");
    expect(dec.accountCode()).toBe("A");
    expect(dec.settlCurrency()).toBe("");
  });

  it("throws RangeError on insufficient buffer", () => {
    const view = new DataView(new ArrayBuffer(50));
    expect(() => new NewOrderSingleEncoder().wrapAndApplyHeader(view, 0)).toThrow(RangeError);
  });
});
