/**
 * MessageRouter — dispatch table for inbound SBE templates.
 *
 * Reads `MessageHeaderDecoder.templateId()` from the frame's payload
 * and dispatches to the correct handler. The ws-control templates
 * (60–72) are handled inline; event templates (100–116) and snapshot
 * entity templates (200–209) are dispatched to caller-supplied
 * handlers.
 *
 * Templates 62 (Subscribe), 63 (Unsubscribe), 70 (CommandAck) are
 * server-bug guards: typed unimplemented handlers that surface a
 * warning if the server emits one.
 *
 * Threading: worker scope only.
 *
 * Allocation: per frame, one handler invocation + one DTO emit. The
 * SBE decoder instances are reused (one per templateId).
 *
 * Plan reference: §2.4 / §5.3 / §6 row 40.
 */

import {
  ClientHeartbeatDecoder,
  MessageHeaderDecoder,
  ReplayCompleteDecoder,
  WebSocketAuthAckDecoder,
  WebSocketErrorDecoder,
  WebSocketHeartbeatDecoder,
  WebSocketSnapshotDecoder,
} from "@/sbe";

import { type AuthAck } from "@/workers/session/AuthClient";
import { type UuidComposite } from "@/workers/session/SessionState";

/**
 * Caller-supplied handlers. Each is invoked synchronously on the
 * matching template; payload views are zero-copy and invalid after
 * the handler returns.
 */
export interface RouterHandlers {
  onAuthAck: (ack: AuthAck) => void;
  /** Activity-refresh: any inbound frame should refresh `lastServerActivityNs`. */
  onAnyInbound: () => void;
  onServerHeartbeat: (serverNanos: bigint) => void;
  onSnapshotFragment: (
    snapshotId: UuidComposite,
    fragmentIndex: number,
    totalFragments: number,
    payload: Uint8Array,
    isFinal: boolean,
  ) => void;
  /** errorCode is the WebSocketErrorCode SBE enum int value (1..12). */
  onWebSocketError: (errorCode: number) => void;
  onReplayComplete: () => void;
  /** templateIds 100..116 — event templates; payload is decoded by C8. */
  onEvent: (templateId: number, payload: Uint8Array) => void;
  /** Server-bug guards. */
  onUnexpectedServerTemplate: (templateId: number) => void;
}

/**
 * Schema-version assertion result. Returned by the FIRST `route()`
 * invocation per session — caller closes with SCHEMA_MISMATCH on
 * `false` per §2.11.
 */
export interface SchemaCheckResult {
  readonly schemaIdMatch: boolean;
  readonly versionMatch: boolean;
  readonly templateId: number;
}

/** SBE message header is 8 bytes; bodies start at offset 8. */
const SBE_HEADER_BYTES = 8;

const TEMPLATE_AUTH_ACK = WebSocketAuthAckDecoder.TEMPLATE_ID;
const TEMPLATE_SERVER_HEARTBEAT = WebSocketHeartbeatDecoder.TEMPLATE_ID;
const TEMPLATE_CLIENT_HEARTBEAT = ClientHeartbeatDecoder.TEMPLATE_ID;
const TEMPLATE_SNAPSHOT = WebSocketSnapshotDecoder.TEMPLATE_ID;
const TEMPLATE_ERROR = WebSocketErrorDecoder.TEMPLATE_ID;
const TEMPLATE_REPLAY_COMPLETE = ReplayCompleteDecoder.TEMPLATE_ID;
/** Hard-coded per APP-36 §2.4; server-bug-guard surface. */
const TEMPLATE_COMMAND_ACK = 70;
const TEMPLATE_SUBSCRIBE = 62;
const TEMPLATE_UNSUBSCRIBE = 63;

const EVENT_TEMPLATE_LO = 100;
const EVENT_TEMPLATE_HI = 116;

export class MessageRouter {
  /** Reused decoders — zero-alloc steady state. */
  private readonly headerDec = new MessageHeaderDecoder();
  private readonly authAckDec = new WebSocketAuthAckDecoder();
  private readonly serverHbDec = new WebSocketHeartbeatDecoder();
  private readonly snapshotDec = new WebSocketSnapshotDecoder();
  private readonly errorDec = new WebSocketErrorDecoder();
  // ReplayCompleteDecoder is empty-bodied; allocation-free use is fine.

  private readonly handlers: RouterHandlers;
  private readonly expectedSchemaId: number;
  private readonly expectedVersion: number;

  constructor(handlers: RouterHandlers, expectedSchemaId: number, expectedVersion: number) {
    this.handlers = handlers;
    this.expectedSchemaId = expectedSchemaId;
    this.expectedVersion = expectedVersion;
  }

  /**
   * Dispatch an inbound payload (the SBE bytes, not the frame envelope).
   * Returns the schema-check result so the caller can close on mismatch
   * the first time it's invoked per session.
   *
   * @param payload SBE-encoded message: 8-byte header + body
   * @param frameFlags raw envelope flags byte; used to derive isFinal for snapshots
   */
  route(payload: Uint8Array, frameFlags: number): SchemaCheckResult {
    const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
    // SBE message header: blockLength u16 (offset 0), templateId u16 (2),
    // schemaId u16 (4), version u16 (6).
    const templateId = view.getUint16(2, true);
    const schemaId = view.getUint16(4, true);
    const version = view.getUint16(6, true);

    const result: SchemaCheckResult = {
      schemaIdMatch: schemaId === this.expectedSchemaId,
      versionMatch: version === this.expectedVersion,
      templateId,
    };

    // Activity refresh — any inbound frame counts.
    this.handlers.onAnyInbound();

    switch (templateId) {
      case TEMPLATE_AUTH_ACK:
        this.dispatchAuthAck(view);
        break;
      case TEMPLATE_SERVER_HEARTBEAT:
        this.dispatchServerHeartbeat(view);
        break;
      case TEMPLATE_CLIENT_HEARTBEAT:
        // ClientHeartbeat is C→S only; receiving from server is a server bug.
        this.handlers.onUnexpectedServerTemplate(templateId);
        break;
      case TEMPLATE_SNAPSHOT:
        this.dispatchSnapshot(view, payload, frameFlags);
        break;
      case TEMPLATE_ERROR:
        this.dispatchError(view);
        break;
      case TEMPLATE_REPLAY_COMPLETE:
        this.handlers.onReplayComplete();
        break;
      case TEMPLATE_COMMAND_ACK:
      case TEMPLATE_SUBSCRIBE:
      case TEMPLATE_UNSUBSCRIBE:
        this.handlers.onUnexpectedServerTemplate(templateId);
        break;
      default:
        if (templateId >= EVENT_TEMPLATE_LO && templateId <= EVENT_TEMPLATE_HI) {
          this.handlers.onEvent(templateId, payload);
        } else {
          // Snapshot-entity templates (200–209) and any unknown id.
          this.handlers.onEvent(templateId, payload);
        }
        break;
    }

    return result;
  }

  private dispatchAuthAck(view: DataView): void {
    this.authAckDec.wrap(view, SBE_HEADER_BYTES);
    const session = this.authAckDec.sessionId();
    const ack: AuthAck = {
      sessionId: {
        mostSignificantBits: session.msb,
        leastSignificantBits: session.lsb,
      },
      protocolVersion: this.authAckDec.protocolVersion(),
      maxSubscriptions: this.authAckDec.maxSubscriptions(),
      serverHeartbeatIntervalMs: this.authAckDec.serverHeartbeatIntervalMs(),
      clientHeartbeatIntervalMs: this.authAckDec.clientHeartbeatIntervalMs(),
    };
    this.handlers.onAuthAck(ack);
  }

  private dispatchServerHeartbeat(view: DataView): void {
    this.serverHbDec.wrap(view, SBE_HEADER_BYTES);
    this.handlers.onServerHeartbeat(this.serverHbDec.serverNanos());
  }

  private dispatchSnapshot(view: DataView, _payload: Uint8Array, frameFlags: number): void {
    this.snapshotDec.wrap(view, SBE_HEADER_BYTES);
    const sid = this.snapshotDec.snapshotId();
    const snapshotId: UuidComposite = {
      mostSignificantBits: sid.msb,
      leastSignificantBits: sid.lsb,
    };
    const fragmentIndex = this.snapshotDec.fragmentIndex();
    const totalFragments = this.snapshotDec.totalFragments();
    // FLAG_SNAPSHOT_FINAL = 0x0C; we check bit 3 specifically.
    const isFinal = (frameFlags & 0x08) !== 0;

    // Per Gemini review (HIGH): the SnapshotAssembler concatenates
    // fragment payloads verbatim into the assembled entity. The fixed
    // SBE fields (snapshotId, fragmentIndex, totalFragments) MUST NOT
    // be included in the concatenated bytes — only the `varData`
    // section. The generated decoder's `payload()` accessor returns a
    // zero-copy view of just the varData bytes, which is what the
    // assembler should accumulate.
    const entityBytes = this.snapshotDec.payload();
    this.handlers.onSnapshotFragment(
      snapshotId,
      fragmentIndex,
      totalFragments,
      entityBytes,
      isFinal,
    );
  }

  private dispatchError(view: DataView): void {
    this.errorDec.wrap(view, SBE_HEADER_BYTES);
    // WebSocketErrorCode is a tree-shakeable as-const object whose
    // values are direct numeric ids (1..12). The decoder's accessor
    // returns the numeric value typed as the union — coerce to number
    // for the public callback.
    const code = this.errorDec.errorCode() as unknown as number;
    this.handlers.onWebSocketError(code);
  }
}
