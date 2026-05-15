/**
 * Hand-written SBE NewOrderSingle encoder. Wire layout is byte-identical to the
 * generated {@code NewOrderSingleDecoder} (templateId=4, blockLength=108, schemaId=1,
 * schemaVersion=1) — see {@code sbe-typescript-generator/build/generated-ts/NewOrderSingleDecoder.ts}.
 *
 * <p><b>Block layout (108 bytes, little-endian):</b>
 * <pre>
 *  offset  field            type           bytes
 *  ------  ---------------  -------------  -----
 *    +0    clOrdId          char[20]         20
 *   +20    quoteId          char[20]         20
 *   +40    symbol           char[8]           8
 *   +48    side             uint8 (enum)      1
 *   +49    ordType          uint8 (enum)      1
 *   +50    price            int64 (opt)       8   nullValue = -2^63
 *   +58    orderQty         int64             8
 *   +66    timeInForce      uint8 (enum)      1
 *   +67    transactTime     uint64            8
 *   +75    accountCode      char[16]         16
 *   +91    productType      uint8 (enum)      1
 *   +92    settlDate        char[8]           8
 *  +100    settlType        uint8 (opt)       1   nullValue = 255
 *  +101    currency         char[3]           3
 *  +104    settlCurrency    char[3]           3
 *  +107    tenor            uint8 (enum)      1
 *  ===================================================
 *  total                                    108
 * </pre>
 *
 * <p>Frame on the wire is {@code messageHeader (8) + block (108) = 116 bytes} for the
 * NewOrderSingle case (no var-data, no groups). The encoder's {@link #encodedFrameLength}
 * accounts for both.
 *
 * <p><b>Allocation:</b> caller-provided {@link DataView} backed by a pooled buffer. The encoder
 * is a flyweight — wrap, set fields, call {@link #encodedFrameLength}, post.
 */
import {
  MESSAGE_HEADER_LENGTH,
  writeFixedString,
  writeInt64LE,
  writeMessageHeader,
  writeUint64LE,
  writeUint8,
} from "@/sbe/encoders/_codecRuntimeWriter";
import { type OrdTypeEnum } from "@trading/sbe-codecs";
import { type ProductTypeEnum } from "@trading/sbe-codecs";
import { type SettlTypeEnum, SettlTypeEnum_NULL_VAL } from "@trading/sbe-codecs";
import { type SideEnum } from "@trading/sbe-codecs";
import { type TenorEnum } from "@trading/sbe-codecs";
import { type TimeInForceEnum } from "@trading/sbe-codecs";

/** SBE NewOrderSingle template constants — matches the generated decoder. */
export const NEW_ORDER_SINGLE = {
  TEMPLATE_ID: 4,
  BLOCK_LENGTH: 108,
  SCHEMA_ID: 1,
  SCHEMA_VERSION: 1,
} as const;

/** Sentinel value for the optional {@code price} field (SBE int64 nullValue). */
const PRICE_NULL = -9_223_372_036_854_775_808n;

export interface NewOrderSingleFields {
  readonly clOrdId: string;
  /** Empty string for non-RFQ orders. */
  readonly quoteId: string;
  readonly symbol: string;
  readonly side: SideEnum;
  readonly ordType: OrdTypeEnum;
  /** {@code null} for Market orders; fixed-point 10^-8 otherwise. */
  readonly price: bigint | null;
  /** Fixed-point 10^-8. */
  readonly orderQty: bigint;
  readonly timeInForce: TimeInForceEnum;
  /** Epoch nanoseconds. */
  readonly transactTime: bigint;
  readonly accountCode: string;
  readonly productType: ProductTypeEnum;
  /** YYYYMMDD. */
  readonly settlDate: string;
  /** {@code null} encodes as the SBE nullValue sentinel (255). */
  readonly settlType: SettlTypeEnum | null;
  /** ISO 4217 — exactly 3 chars. */
  readonly currency: string;
  /** ISO 4217 — empty string = same as currency. */
  readonly settlCurrency: string;
  readonly tenor: TenorEnum;
}

export class NewOrderSingleEncoder {
  static readonly TEMPLATE_ID = NEW_ORDER_SINGLE.TEMPLATE_ID;
  static readonly BLOCK_LENGTH = NEW_ORDER_SINGLE.BLOCK_LENGTH;
  static readonly SCHEMA_ID = NEW_ORDER_SINGLE.SCHEMA_ID;
  static readonly SCHEMA_VERSION = NEW_ORDER_SINGLE.SCHEMA_VERSION;
  static readonly ENCODED_FRAME_LENGTH = MESSAGE_HEADER_LENGTH + NEW_ORDER_SINGLE.BLOCK_LENGTH;

  private buffer!: DataView;
  private bufferOffset = 0;

  /**
   * Wrap a buffer, write the SBE message header, position the cursor at the block start.
   * Returns this for chaining.
   */
  wrapAndApplyHeader(buffer: DataView, offset: number): this {
    if (offset + NewOrderSingleEncoder.ENCODED_FRAME_LENGTH > buffer.byteLength) {
      throw new RangeError(
        `NewOrderSingleEncoder.wrapAndApplyHeader: insufficient buffer (${String(buffer.byteLength - offset)} < ${String(NewOrderSingleEncoder.ENCODED_FRAME_LENGTH)})`,
      );
    }
    this.buffer = buffer;
    this.bufferOffset = offset + MESSAGE_HEADER_LENGTH;
    writeMessageHeader(
      buffer,
      offset,
      NewOrderSingleEncoder.BLOCK_LENGTH,
      NewOrderSingleEncoder.TEMPLATE_ID,
      NewOrderSingleEncoder.SCHEMA_ID,
      NewOrderSingleEncoder.SCHEMA_VERSION,
    );
    return this;
  }

  /** Total bytes written by this encoder, including the SBE message header. */
  encodedFrameLength(): number {
    return NewOrderSingleEncoder.ENCODED_FRAME_LENGTH;
  }

  /** Write every field at its block offset in one call. */
  setFields(f: NewOrderSingleFields): this {
    writeFixedString(this.buffer, this.bufferOffset + 0, 20, f.clOrdId);
    writeFixedString(this.buffer, this.bufferOffset + 20, 20, f.quoteId);
    writeFixedString(this.buffer, this.bufferOffset + 40, 8, f.symbol);
    writeUint8(this.buffer, this.bufferOffset + 48, f.side);
    writeUint8(this.buffer, this.bufferOffset + 49, f.ordType);
    writeInt64LE(this.buffer, this.bufferOffset + 50, f.price ?? PRICE_NULL);
    writeInt64LE(this.buffer, this.bufferOffset + 58, f.orderQty);
    writeUint8(this.buffer, this.bufferOffset + 66, f.timeInForce);
    writeUint64LE(this.buffer, this.bufferOffset + 67, f.transactTime);
    writeFixedString(this.buffer, this.bufferOffset + 75, 16, f.accountCode);
    writeUint8(this.buffer, this.bufferOffset + 91, f.productType);
    writeFixedString(this.buffer, this.bufferOffset + 92, 8, f.settlDate);
    writeUint8(this.buffer, this.bufferOffset + 100, f.settlType ?? SettlTypeEnum_NULL_VAL);
    writeFixedString(this.buffer, this.bufferOffset + 101, 3, f.currency);
    writeFixedString(this.buffer, this.bufferOffset + 104, 3, f.settlCurrency);
    writeUint8(this.buffer, this.bufferOffset + 107, f.tenor);
    return this;
  }
}
