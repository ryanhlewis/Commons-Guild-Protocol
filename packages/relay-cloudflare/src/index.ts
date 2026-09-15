import {
  applyEvent,
  canReadGuild,
  canViewChannel,
  computeEventId,
  createInitialState,
  encodeCgpFrame,
  generatePrivateKey,
  getPublicKey,
  hashObject,
  parseCgpWireData,
  rebuildStateFromEvents,
  relayHeadId,
  serializeState,
  sign,
  validateEvent,
  verify,
  verifyObject,
  type ChannelId,
  type CgpWireFormat,
  type EventBody,
  type GuildEvent,
  type GuildId,
  type GuildState,
  type RelayHead,
  type RelayHeadUnsigned,
  type UserId,
} from "@cgp/core";
import {
  D1RelayStore,
  DurableObjectSqlRelayStore,
  type HistoryQuery,
  type WorkerRelayStore,
} from "./store";

export interface Env {
  RELAY: DurableObjectNamespace;
  DB?: D1Database;
  CGP_RELAY_ID?: string;
  CGP_RELAY_NAME?: string;
  CGP_RELAY_PRIVATE_KEY_HEX?: string;
  CGP_RELAY_REQUIRE_SIGNED_READS?: string;
  CGP_RELAY_STORAGE?: "durable-object-sql" | "d1";
  CGP_RELAY_MAX_SNAPSHOT_EVENTS?: string;
  CGP_RELAY_MAX_HISTORY_EVENTS?: string;
  CGP_RELAY_MAX_LOG_RANGE_EVENTS?: string;
  CGP_RELAY_MAX_PUBLISH_BATCH_SIZE?: string;
}

interface SocketSubscription {
  subId: string;
  guildId: GuildId;
  channels?: ChannelId[];
  author?: UserId;
}

interface WebSocketAttachment {
  wireFormat?: CgpWireFormat;
  subscriptions: SocketSubscription[];
}

interface RebuiltGuild {
  state: GuildState;
  endEvent: GuildEvent;
  checkpointEvent?: GuildEvent;
}

interface PublishPayload {
  body: EventBody;
  author: string;
  signature: string;
  createdAt: number;
  clientEventId?: string;
}

const SUPPORTED_WIRE_FORMATS: CgpWireFormat[] = ["json", "binary-json", "binary-v1", "binary-v2"];

function json(data: unknown, init?: ResponseInit) {
  return new Response(JSON.stringify(data, null, 2), {
    ...init,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...init?.headers,
    },
  });
}

function positiveInteger(value: unknown, fallback: number, max: number) {
  const parsed = Math.floor(Number(value));
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.min(max, parsed);
}

function optionalSeq(value: unknown) {
  const parsed = Math.floor(Number(value));
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

function objectPayload(payload: unknown): Record<string, unknown> {
  return payload && typeof payload === "object" && !Array.isArray(payload)
    ? payload as Record<string, unknown>
    : {};
}

function wireFormatFromValue(value: unknown): CgpWireFormat | undefined {
  return SUPPORTED_WIRE_FORMATS.includes(value as CgpWireFormat)
    ? value as CgpWireFormat
    : undefined;
}

function privateKeyFromHex(value?: string) {
  const trimmed = value?.trim() ?? "";
  if (/^[a-fA-F0-9]{64}$/.test(trimmed)) {
    const bytes = new Uint8Array(32);
    for (let index = 0; index < 32; index += 1) {
      bytes[index] = Number.parseInt(trimmed.slice(index * 2, index * 2 + 2), 16);
    }
    return bytes;
  }
  return generatePrivateKey();
}

function eventChannelId(event: GuildEvent) {
  const body = event.body as unknown as Record<string, unknown>;
  return typeof body.channelId === "string" ? body.channelId : "";
}

function eventTargetChannelId(event: GuildEvent) {
  const body = event.body as unknown as Record<string, unknown>;
  const target = body.target && typeof body.target === "object"
    ? body.target as unknown as Record<string, unknown>
    : undefined;
  return typeof target?.channelId === "string" ? target.channelId : "";
}

function eventChannelIds(event: GuildEvent) {
  return [eventChannelId(event), eventTargetChannelId(event)].filter(Boolean);
}

function visibleToSubscription(
  event: GuildEvent,
  rebuilt: RebuiltGuild | undefined,
  subscription: SocketSubscription,
) {
  if (subscription.guildId !== event.body.guildId) {
    return false;
  }
  if (!rebuilt || !subscription.author) {
    return true;
  }
  if (!canReadGuild(rebuilt.state, subscription.author)) {
    return false;
  }
  const requestedChannels = subscription.channels;
  const channels = eventChannelIds(event);
  if (channels.length === 0) {
    return true;
  }
  if (requestedChannels && channels.every((channelId) => !requestedChannels.includes(channelId))) {
    return false;
  }
  return channels.some((channelId) => canViewChannel(rebuilt.state, subscription.author, channelId));
}

function filterVisibleEvents(
  events: GuildEvent[],
  rebuilt: RebuiltGuild | undefined,
  author: string | undefined,
  channels?: ChannelId[],
) {
  if (!rebuilt || !author) {
    return events;
  }
  if (!canReadGuild(rebuilt.state, author)) {
    return [];
  }
  return events.filter((event) => visibleToSubscription(event, rebuilt, {
    subId: "",
    guildId: event.body.guildId,
    channels,
    author,
  }));
}

function responseError(code: string, message: string, extra: Record<string, unknown> = {}) {
  return { code, message, ...extra };
}

function toSocketMessage(frame: string | Uint8Array): string | ArrayBuffer {
  if (typeof frame === "string") {
    return frame;
  }
  return frame.buffer.slice(frame.byteOffset, frame.byteOffset + frame.byteLength) as ArrayBuffer;
}

function safeObjectNamePart(value: string | null | undefined) {
  const text = value?.trim();
  if (!text) {
    return undefined;
  }
  return text.slice(0, 256).replace(/[^a-zA-Z0-9:._~-]/g, "_");
}

function decodePathPart(value: string | undefined) {
  if (!value) {
    return undefined;
  }
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function relayObjectName(url: URL) {
  const pathParts = url.pathname.split("/").filter(Boolean);
  const pathScope = pathParts[0] === "relay" ? pathParts[1] : undefined;
  const pathValue = pathParts[0] === "relay" ? decodePathPart(pathParts[2]) : undefined;
  const queryGuild = url.searchParams.get("guildId");
  const queryBucket = url.searchParams.get("bucket");
  const queryScope = url.searchParams.get("scope");

  if (pathScope === "guild") {
    const guildId = safeObjectNamePart(pathValue);
    if (guildId) {
      return `guild:${guildId}`;
    }
  }
  if (pathScope === "bucket") {
    const bucket = safeObjectNamePart(pathValue);
    if (bucket) {
      return `bucket:${bucket}`;
    }
  }

  const guildId = safeObjectNamePart(queryGuild);
  if (guildId) {
    return `guild:${guildId}`;
  }
  const bucket = safeObjectNamePart(queryBucket || queryScope);
  if (bucket) {
    return `bucket:${bucket}`;
  }
  return "global-relay";
}

export class RelayDO {
  private readonly store: WorkerRelayStore;
  private readonly relayName: string;
  private readonly relayId: string;
  private readonly relayPrivateKey: Uint8Array;
  private readonly relayPublicKey: string;
  private readonly requireSignedReads: boolean;
  private readonly maxSnapshotEvents: number;
  private readonly maxHistoryEvents: number;
  private readonly maxLogRangeEvents: number;
  private readonly maxPublishBatchSize: number;
  private readonly stateCache = new Map<GuildId, RebuiltGuild>();
  private readonly mutexes = new Map<GuildId, Promise<unknown>>();

  constructor(private readonly state: DurableObjectState, private readonly env: Env) {
    const storage = env.CGP_RELAY_STORAGE || "durable-object-sql";
    this.store = storage === "d1" && env.DB
      ? new D1RelayStore(env.DB)
      : new DurableObjectSqlRelayStore((state.storage as DurableObjectStorage & { sql: SqlStorage }).sql);
    this.relayPrivateKey = privateKeyFromHex(env.CGP_RELAY_PRIVATE_KEY_HEX);
    this.relayPublicKey = getPublicKey(this.relayPrivateKey);
    this.relayId = env.CGP_RELAY_ID || `cf-${this.relayPublicKey.slice(0, 16)}`;
    this.relayName = env.CGP_RELAY_NAME || "Cloudflare CGP Relay";
    this.requireSignedReads = env.CGP_RELAY_REQUIRE_SIGNED_READS !== "0";
    this.maxSnapshotEvents = positiveInteger(env.CGP_RELAY_MAX_SNAPSHOT_EVENTS, 5000, 100000);
    this.maxHistoryEvents = positiveInteger(env.CGP_RELAY_MAX_HISTORY_EVENTS, 500, 5000);
    this.maxLogRangeEvents = positiveInteger(env.CGP_RELAY_MAX_LOG_RANGE_EVENTS, 5000, 10000);
    this.maxPublishBatchSize = positiveInteger(env.CGP_RELAY_MAX_PUBLISH_BATCH_SIZE, 512, 2048);
  }

  async fetch(request: Request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.state.acceptWebSocket(server);
    server.serializeAttachment({
      subscriptions: [],
      wireFormat: "json",
    } satisfies WebSocketAttachment);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(socket: WebSocket, message: string | ArrayBuffer) {
    try {
      const { kind, payload } = parseCgpWireData(message, { includeRawFrame: true });
      await this.handleFrame(socket, kind, payload);
    } catch (error: any) {
      this.sendFrame(socket, "ERROR", responseError("INVALID_FRAME", error?.message || "Invalid frame"));
    }
  }

  async webSocketClose() {
    // WebSocket hibernation storage owns lifecycle cleanup.
  }

  async webSocketError(socket: WebSocket, error: unknown) {
    this.sendFrame(socket, "ERROR", responseError("SOCKET_ERROR", error instanceof Error ? error.message : "Socket error"));
  }

  private async handleFrame(socket: WebSocket, kind: string, payload: unknown) {
    switch (kind) {
      case "HELLO":
        this.handleHello(socket, payload);
        return;
      case "SUB":
        await this.handleSubscribe(socket, payload);
        return;
      case "GET_HISTORY":
        await this.handleHistory(socket, payload);
        return;
      case "GET_LOG_RANGE":
        await this.handleLogRange(socket, payload);
        return;
      case "GET_STATE":
        await this.handleState(socket, payload);
        return;
      case "GET_HEAD":
        await this.handleRelayHead(socket, payload, false);
        return;
      case "GET_HEADS":
        await this.handleRelayHead(socket, payload, true);
        return;
      case "GET_MEMBERS":
        await this.handleMembers(socket, payload);
        return;
      case "SEARCH":
        await this.handleSearch(socket, payload);
        return;
      case "PUBLISH":
        await this.handlePublish(socket, payload);
        return;
      case "PUBLISH_TRANSIENT":
        await this.handleTransientPublish(socket, payload);
        return;
      case "PUBLISH_BATCH":
        await this.handlePublishBatch(socket, payload);
        return;
      default:
        this.sendFrame(socket, "ERROR", responseError("UNKNOWN_FRAME", `Unsupported frame ${kind}`));
    }
  }

  private handleHello(socket: WebSocket, payload: unknown) {
    const p = objectPayload(payload);
    const requestedWireFormat = wireFormatFromValue(p.wireFormat);
    const attachment = this.attachment(socket);
    attachment.wireFormat = requestedWireFormat || attachment.wireFormat || "json";
    socket.serializeAttachment(attachment);
    this.sendFrame(socket, "HELLO_OK", {
      protocol: "cgp/0.1",
      relayName: this.relayName,
      relayId: this.relayId,
      relayPublicKey: this.relayPublicKey,
      wireFormat: attachment.wireFormat,
      supportedWireFormats: SUPPORTED_WIRE_FORMATS,
      deployment: "cloudflare-workers",
      storage: this.env.CGP_RELAY_STORAGE || "durable-object-sql",
      plugins: [],
    });
  }

  private async handleSubscribe(socket: WebSocket, payload: unknown) {
    const p = objectPayload(payload);
    const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `sub-${Date.now()}`;
    const guildId = typeof p.guildId === "string" ? p.guildId : "";
    const channels = Array.isArray(p.channels)
      ? p.channels.filter((channelId): channelId is string => typeof channelId === "string" && channelId.trim().length > 0)
      : undefined;
    if (!guildId) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "SUB requires a guildId", { subId }));
      return;
    }

    let author: string | undefined;
    try {
      author = this.verifyReadRequest("SUB", payload);
    } catch {
      this.sendFrame(socket, "ERROR", responseError("AUTH_FAILED", "Signed read request failed", { subId, guildId }));
      return;
    }

    const rebuilt = await this.rebuildGuild(guildId);
    if (rebuilt && author && !canReadGuild(rebuilt.state, author)) {
      this.sendFrame(socket, "ERROR", responseError("FORBIDDEN", "You do not have permission to subscribe to this guild", { subId, guildId }));
      return;
    }

    const attachment = this.attachment(socket);
    const withoutExisting = attachment.subscriptions.filter((sub) => sub.subId !== subId && sub.guildId !== guildId);
    withoutExisting.push({ subId, guildId, channels, author });
    attachment.subscriptions = withoutExisting;
    socket.serializeAttachment(attachment);

    const rawEvents = await this.store.getHistory({ guildId, limit: this.maxSnapshotEvents });
    const events = filterVisibleEvents(rawEvents, rebuilt, author, channels);
    const tailEvent = rebuilt?.endEvent ?? events[events.length - 1];
    const checkpointEvent = rebuilt?.checkpointEvent ?? events.find((event) => event.body.type === "CHECKPOINT");
    this.sendFrame(socket, "SNAPSHOT", {
      subId,
      guildId,
      events,
      endSeq: tailEvent?.seq ?? -1,
      endHash: tailEvent?.id ?? null,
      oldestSeq: events.length > 0 ? events[0].seq : null,
      newestSeq: events.length > 0 ? events[events.length - 1].seq : null,
      hasMore: events.length >= this.maxSnapshotEvents,
      checkpointSeq: checkpointEvent?.seq ?? null,
      checkpointHash: checkpointEvent?.id ?? null,
    });
  }

  private async handleHistory(socket: WebSocket, payload: unknown) {
    const p = objectPayload(payload);
    const guildId = typeof p.guildId === "string" ? p.guildId : "";
    const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `history-${Date.now()}`;
    if (!guildId) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "GET_HISTORY requires a guildId", { subId }));
      return;
    }

    let author: string | undefined;
    try {
      author = this.verifyReadRequest("GET_HISTORY", payload);
    } catch {
      this.sendFrame(socket, "ERROR", responseError("AUTH_FAILED", "Signed read request failed", { subId, guildId }));
      return;
    }

    const query: HistoryQuery = {
      guildId,
      channelId: typeof p.channelId === "string" && p.channelId.trim() ? p.channelId : undefined,
      beforeSeq: optionalSeq(p.beforeSeq),
      afterSeq: optionalSeq(p.afterSeq),
      limit: Math.min(this.maxHistoryEvents + 1, positiveInteger(p.limit, 100, this.maxHistoryEvents) + 1),
      includeStructural: p.includeStructural === true,
    };
    const rebuilt = await this.rebuildGuild(guildId);
    if (rebuilt && author && !canReadGuild(rebuilt.state, author)) {
      this.sendFrame(socket, "ERROR", responseError("FORBIDDEN", "You do not have permission to read this guild history", { subId, guildId }));
      return;
    }
    if (rebuilt && author && query.channelId && !canViewChannel(rebuilt.state, author, query.channelId)) {
      this.sendFrame(socket, "ERROR", responseError("FORBIDDEN", "You do not have permission to read this channel history", { subId, guildId, channelId: query.channelId }));
      return;
    }

    const requestedLimit = Math.max(1, (query.limit ?? 101) - 1);
    const rawEvents = await this.store.getHistory(query);
    const visibleEvents = filterVisibleEvents(rawEvents, rebuilt, author, query.channelId ? [query.channelId] : undefined);
    const hasMore = visibleEvents.length > requestedLimit;
    const events = query.afterSeq !== undefined
      ? visibleEvents.slice(0, requestedLimit)
      : visibleEvents.slice(Math.max(0, visibleEvents.length - requestedLimit));
    const tailEvent = events[events.length - 1] ?? rebuilt?.endEvent;
    this.sendFrame(socket, "SNAPSHOT", {
      subId,
      guildId,
      channelId: query.channelId,
      events,
      endSeq: tailEvent?.seq ?? -1,
      endHash: tailEvent?.id ?? null,
      oldestSeq: events.length > 0 ? events[0].seq : null,
      newestSeq: events.length > 0 ? events[events.length - 1].seq : null,
      hasMore,
      checkpointSeq: rebuilt?.checkpointEvent?.seq ?? null,
      checkpointHash: rebuilt?.checkpointEvent?.id ?? null,
    });
  }

  private async handleLogRange(socket: WebSocket, payload: unknown) {
    const p = objectPayload(payload);
    const guildId = typeof p.guildId === "string" ? p.guildId : "";
    const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `range-${Date.now()}`;
    if (!guildId) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "GET_LOG_RANGE requires a guildId", { subId }));
      return;
    }
    let author: string | undefined;
    try {
      author = this.verifyReadRequest("GET_LOG_RANGE", payload);
    } catch {
      this.sendFrame(socket, "ERROR", responseError("AUTH_FAILED", "Signed read request failed", { subId, guildId }));
      return;
    }
    const rebuilt = await this.rebuildGuild(guildId);
    if (rebuilt && author && !canReadGuild(rebuilt.state, author)) {
      this.sendFrame(socket, "ERROR", responseError("FORBIDDEN", "You do not have permission to read this guild log", { subId, guildId }));
      return;
    }
    const limit = positiveInteger(p.limit, this.maxLogRangeEvents, this.maxLogRangeEvents);
    const rawEvents = await this.store.getLogRange({ guildId, afterSeq: optionalSeq(p.afterSeq), limit });
    const events = filterVisibleEvents(rawEvents, rebuilt, author);
    this.sendFrame(socket, "LOG_RANGE", {
      subId,
      guildId,
      events,
      afterSeq: optionalSeq(p.afterSeq) ?? null,
      endSeq: rebuilt?.endEvent.seq ?? (events[events.length - 1]?.seq ?? -1),
      endHash: rebuilt?.endEvent.id ?? (events[events.length - 1]?.id ?? null),
      hasMore: rawEvents.length >= limit,
      checkpointSeq: rebuilt?.checkpointEvent?.seq ?? null,
      checkpointHash: rebuilt?.checkpointEvent?.id ?? null,
    });
  }

  private async handleState(socket: WebSocket, payload: unknown) {
    const p = objectPayload(payload);
    const guildId = typeof p.guildId === "string" ? p.guildId : "";
    const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `state-${Date.now()}`;
    if (!guildId) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "GET_STATE requires a guildId", { subId }));
      return;
    }
    let author: string | undefined;
    try {
      author = this.verifyReadRequest("GET_STATE", payload);
    } catch {
      this.sendFrame(socket, "ERROR", responseError("AUTH_FAILED", "Signed read request failed", { subId, guildId }));
      return;
    }
    const rebuilt = await this.rebuildGuild(guildId);
    if (!rebuilt) {
      this.sendFrame(socket, "ERROR", responseError("NOT_FOUND", "Guild state not found", { subId, guildId }));
      return;
    }
    if (author && !canReadGuild(rebuilt.state, author)) {
      this.sendFrame(socket, "ERROR", responseError("FORBIDDEN", "You do not have permission to read this guild state", { subId, guildId }));
      return;
    }
    const serialized = serializeState(rebuilt.state);
    const head = await this.signRelayHead(guildId, rebuilt);
    this.sendFrame(socket, "STATE", {
      subId,
      guildId,
      state: serialized,
      rootHash: hashObject(serialized),
      endSeq: rebuilt.endEvent.seq,
      endHash: rebuilt.endEvent.id,
      checkpointSeq: rebuilt.checkpointEvent?.seq ?? null,
      checkpointHash: rebuilt.checkpointEvent?.id ?? null,
      head,
      stateIncludes: {
        members: "full",
        messages: "full",
        appObjects: "full",
      },
    });
  }

  private async handleRelayHead(socket: WebSocket, payload: unknown, includeObserved: boolean) {
    const p = objectPayload(payload);
    const guildId = typeof p.guildId === "string" ? p.guildId : "";
    const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `head-${Date.now()}`;
    if (!guildId) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "GET_HEAD requires a guildId", { subId }));
      return;
    }
    try {
      this.verifyReadRequest(includeObserved ? "GET_HEADS" : "GET_HEAD", payload);
    } catch {
      this.sendFrame(socket, "ERROR", responseError("AUTH_FAILED", "Signed read request failed", { subId, guildId }));
      return;
    }
    const rebuilt = await this.rebuildGuild(guildId);
    const head = await this.signRelayHead(guildId, rebuilt);
    if (!head) {
      this.sendFrame(socket, "ERROR", responseError("NOT_FOUND", "Guild head not found", { subId, guildId }));
      return;
    }
    if (includeObserved) {
      this.sendFrame(socket, "RELAY_HEADS", {
        subId,
        guildId,
        heads: [head],
        quorum: {
          guildId,
          validCount: 1,
          invalidCount: 0,
          conflictCount: 0,
          canonical: { seq: head.headSeq, hash: head.headHash, count: 1 },
          conflicts: [],
        },
      });
    } else {
      this.sendFrame(socket, "RELAY_HEAD", { subId, guildId, head });
    }
  }

  private async handleMembers(socket: WebSocket, payload: unknown) {
    const p = objectPayload(payload);
    const guildId = typeof p.guildId === "string" ? p.guildId : "";
    const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `members-${Date.now()}`;
    if (!guildId) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "GET_MEMBERS requires a guildId", { subId }));
      return;
    }
    let author: string | undefined;
    try {
      author = this.verifyReadRequest("GET_MEMBERS", payload);
    } catch {
      this.sendFrame(socket, "ERROR", responseError("AUTH_FAILED", "Signed read request failed", { subId, guildId }));
      return;
    }
    const rebuilt = await this.rebuildGuild(guildId);
    if (!rebuilt || (author && !canReadGuild(rebuilt.state, author))) {
      this.sendFrame(socket, "MEMBERS", { subId, guildId, members: [], nextCursor: null, hasMore: false, totalApprox: 0 });
      return;
    }
    const limit = positiveInteger(p.limit, 100, 500);
    const afterUserId = typeof p.afterUserId === "string" ? p.afterUserId : undefined;
    const members = [...rebuilt.state.members.values()].sort((left, right) => left.userId.localeCompare(right.userId));
    const start = afterUserId ? members.findIndex((member) => member.userId > afterUserId) : 0;
    const pageStart = start >= 0 ? start : members.length;
    const page = members.slice(pageStart, pageStart + limit + 1);
    const visible = page.slice(0, limit);
    const hasMore = page.length > limit;
    this.sendFrame(socket, "MEMBERS", {
      subId,
      guildId,
      members: visible,
      nextCursor: hasMore && visible.length > 0 ? visible[visible.length - 1].userId : null,
      hasMore,
      totalApprox: members.length,
    });
  }

  private async handleSearch(socket: WebSocket, payload: unknown) {
    const p = objectPayload(payload);
    const guildId = typeof p.guildId === "string" ? p.guildId : "";
    const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `search-${Date.now()}`;
    const query = typeof p.query === "string" ? p.query.trim() : "";
    if (!guildId || !query) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "SEARCH requires a guildId and query", { subId, guildId }));
      return;
    }
    let author: string | undefined;
    try {
      author = this.verifyReadRequest("SEARCH", payload);
    } catch {
      this.sendFrame(socket, "ERROR", responseError("AUTH_FAILED", "Signed read request failed", { subId, guildId }));
      return;
    }
    const rebuilt = await this.rebuildGuild(guildId);
    if (!rebuilt || (author && !canReadGuild(rebuilt.state, author))) {
      this.sendFrame(socket, "SEARCH_RESULTS", { subId, guildId, query, results: [], hasMore: false, oldestSeq: null, newestSeq: null });
      return;
    }
    const channelId = typeof p.channelId === "string" && p.channelId.trim() ? p.channelId : undefined;
    if (author && channelId && !canViewChannel(rebuilt.state, author, channelId)) {
      this.sendFrame(socket, "ERROR", responseError("FORBIDDEN", "You do not have permission to search this channel", { subId, guildId, channelId }));
      return;
    }
    const limit = positiveInteger(p.limit, 50, 100);
    const needle = query.toLowerCase();
    const candidates = await this.store.getHistory({
      guildId,
      channelId,
      limit: Math.min(5000, Math.max(limit * 20, 500)),
    });
    const visible = filterVisibleEvents(candidates, rebuilt, author, channelId ? [channelId] : undefined);
    const results = visible
      .filter((event) => event.body.type === "MESSAGE" && typeof (event.body as any).content === "string")
      .filter((event) => String((event.body as any).content).toLowerCase().includes(needle))
      .slice(-limit)
      .map((event) => ({
        type: "message",
        guildId,
        channelId: eventChannelId(event),
        messageId: (event.body as any).messageId,
        seq: event.seq,
        event,
      }));
    const seqs = results.map((result) => result.seq);
    this.sendFrame(socket, "SEARCH_RESULTS", {
      subId,
      guildId,
      channelId,
      query,
      scopes: ["messages"],
      results,
      hasMore: visible.length > results.length,
      oldestSeq: seqs.length > 0 ? Math.min(...seqs) : null,
      newestSeq: seqs.length > 0 ? Math.max(...seqs) : null,
      checkpointSeq: rebuilt.checkpointEvent?.seq ?? null,
      checkpointHash: rebuilt.checkpointEvent?.id ?? null,
    });
  }

  private async handlePublish(socket: WebSocket, payload: unknown) {
    const ack = await this.appendPublishPayload(payload as PublishPayload);
    if (ack.ok) {
      this.sendFrame(socket, "PUB_ACK", ack.ack);
      await this.broadcastEvent(ack.event);
    } else {
      this.sendFrame(socket, "ERROR", responseError(ack.code, ack.message, { clientEventId: ack.clientEventId }));
    }
  }

  private async handlePublishBatch(socket: WebSocket, payload: unknown) {
    const p = objectPayload(payload);
    const batchId = typeof p.batchId === "string" ? p.batchId : undefined;
    const events = Array.isArray(p.events) ? p.events.slice(0, this.maxPublishBatchSize) : [];
    if (events.length === 0) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "PUBLISH_BATCH requires events", { batchId }));
      return;
    }
    const results = [];
    const appended: GuildEvent[] = [];
    for (let index = 0; index < events.length; index += 1) {
      const result = await this.appendPublishPayload(events[index] as PublishPayload);
      if (result.ok) {
        results.push({ ok: true, ...result.ack });
        appended.push(result.event);
      } else {
        results.push({
          ok: false,
          code: result.code,
          message: result.message,
          clientEventId: result.clientEventId,
        });
      }
    }
    this.sendFrame(socket, "PUB_BATCH_ACK", {
      batchId,
      results,
      truncated: Array.isArray(p.events) && p.events.length > events.length,
    });
    for (const event of appended) {
      await this.broadcastEvent(event);
    }
  }

  private async handleTransientPublish(socket: WebSocket, payload: unknown) {
    const p = payload as PublishPayload;
    const validation = await this.validatePublishPayload(p);
    if (!validation.ok) {
      this.sendFrame(socket, "ERROR", responseError(validation.code, validation.message, { clientEventId: validation.clientEventId }));
      return;
    }
    const event = {
      id: hashObject({
        transient: true,
        body: p.body,
        author: p.author,
        signature: p.signature,
        createdAt: p.createdAt,
        clientEventId: p.clientEventId,
      }),
      seq: Number.NaN,
      prevHash: null,
      createdAt: p.createdAt,
      author: p.author,
      body: p.body,
      signature: p.signature,
      transient: true,
    } as GuildEvent & { transient: true };
    this.sendFrame(socket, "PUB_TRANSIENT_ACK", {
      clientEventId: p.clientEventId,
      guildId: p.body.guildId,
      eventId: event.id,
      seq: Number.NaN,
    });
    await this.broadcastEvent(event, true);
  }

  private async appendPublishPayload(payload: PublishPayload): Promise<
    | { ok: true; event: GuildEvent; ack: { clientEventId?: string; guildId: GuildId; eventId: string; seq: number; prevHash: string | null } }
    | { ok: false; code: string; message: string; clientEventId?: string }
  > {
    const validation = await this.validatePublishPayload(payload);
    if (!validation.ok) {
      return validation;
    }
    const guildId = payload.body.guildId;
    return this.withGuildMutex(guildId, async () => {
      const lastEvent = await this.store.getLastEvent(guildId);
      const seq = lastEvent ? lastEvent.seq + 1 : 0;
      const prevHash = lastEvent ? lastEvent.id : null;
      const event: GuildEvent = {
        id: "",
        seq,
        prevHash,
        createdAt: payload.createdAt,
        author: payload.author,
        body: payload.body,
        signature: payload.signature,
      };
      event.id = computeEventId(event);

      let rebuilt = this.stateCache.get(guildId);
      if (seq === 0) {
        if (payload.body.type !== "GUILD_CREATE") {
          return { ok: false as const, code: "VALIDATION_FAILED", message: "First event must be GUILD_CREATE", clientEventId: payload.clientEventId };
        }
        rebuilt = {
          state: createInitialState(event),
          endEvent: event,
          checkpointEvent: event.body.type === "CHECKPOINT" ? event : undefined,
        };
      } else {
        if (!rebuilt || rebuilt.endEvent.seq !== seq - 1 || rebuilt.endEvent.id !== prevHash) {
          rebuilt = await this.rebuildGuild(guildId);
        }
        if (!rebuilt) {
          return { ok: false as const, code: "VALIDATION_FAILED", message: "Guild state could not be rebuilt", clientEventId: payload.clientEventId };
        }
        try {
          validateEvent(rebuilt.state, event);
        } catch (error: any) {
          return { ok: false as const, code: "VALIDATION_FAILED", message: error?.message || "Event validation failed", clientEventId: payload.clientEventId };
        }
        rebuilt = {
          state: applyEvent(rebuilt.state, event),
          endEvent: event,
          checkpointEvent: event.body.type === "CHECKPOINT" ? event : rebuilt.checkpointEvent,
        };
      }

      await this.store.append(guildId, event);
      this.cacheGuild(guildId, rebuilt);
      return {
        ok: true as const,
        event,
        ack: {
          clientEventId: payload.clientEventId,
          guildId,
          eventId: event.id,
          seq: event.seq,
          prevHash: event.prevHash,
        },
      };
    });
  }

  private async validatePublishPayload(payload: PublishPayload): Promise<
    | { ok: true }
    | { ok: false; code: string; message: string; clientEventId?: string }
  > {
    const body = payload?.body;
    const guildId = body && typeof body === "object" ? (body as unknown as Record<string, unknown>).guildId : undefined;
    if (typeof guildId !== "string" || !guildId.trim()) {
      return { ok: false, code: "VALIDATION_FAILED", message: "Publish body requires a guildId", clientEventId: payload?.clientEventId };
    }
    if (body.type === "CHECKPOINT") {
      return { ok: false, code: "VALIDATION_FAILED", message: "CHECKPOINT events are relay-maintained", clientEventId: payload.clientEventId };
    }
    if (typeof payload.author !== "string" || typeof payload.signature !== "string" || typeof payload.createdAt !== "number") {
      return { ok: false, code: "VALIDATION_FAILED", message: "Publish requires author, signature, and createdAt", clientEventId: payload?.clientEventId };
    }
    if (!verifyObject(payload.author, { body, author: payload.author, createdAt: payload.createdAt }, payload.signature)) {
      return { ok: false, code: "INVALID_SIGNATURE", message: "Signature verification failed", clientEventId: payload.clientEventId };
    }
    const rebuilt = await this.rebuildGuild(guildId);
    if (rebuilt && !canReadGuild(rebuilt.state, payload.author)) {
      return { ok: false, code: "FORBIDDEN", message: "You do not have permission to publish to this guild", clientEventId: payload.clientEventId };
    }
    return { ok: true };
  }

  private verifyReadRequest(kind: string, payload: unknown) {
    const p = objectPayload(payload);
    const { signature, ...unsignedPayload } = p;
    if (typeof p.author !== "string" || typeof p.createdAt !== "number" || typeof signature !== "string") {
      if (this.requireSignedReads) {
        throw new Error("Missing signed read fields");
      }
      return undefined;
    }
    const ok = verify(p.author, hashObject({ kind, payload: unsignedPayload }), signature);
    if (!ok) {
      throw new Error("Invalid read signature");
    }
    return p.author;
  }

  private async signRelayHead(guildId: GuildId, rebuilt?: RebuiltGuild): Promise<RelayHead | null> {
    const endEvent = rebuilt?.endEvent ?? await this.store.getLastEvent(guildId);
    if (!endEvent) {
      return null;
    }
    const unsigned: RelayHeadUnsigned = {
      protocol: "cgp/0.1",
      relayId: this.relayId,
      relayPublicKey: this.relayPublicKey,
      guildId,
      headSeq: endEvent.seq,
      headHash: endEvent.id,
      prevHash: endEvent.prevHash,
      checkpointSeq: rebuilt?.checkpointEvent?.seq ?? null,
      checkpointHash: rebuilt?.checkpointEvent?.id ?? null,
      observedAt: Date.now(),
    };
    return {
      ...unsigned,
      signature: await sign(this.relayPrivateKey, relayHeadId(unsigned)),
    };
  }

  private async rebuildGuild(guildId: GuildId): Promise<RebuiltGuild | undefined> {
    const cached = this.stateCache.get(guildId);
    const lastEvent = await this.store.getLastEvent(guildId);
    if (!lastEvent) {
      this.stateCache.delete(guildId);
      return undefined;
    }
    if (cached?.endEvent.id === lastEvent.id && cached.endEvent.seq === lastEvent.seq) {
      return cached;
    }
    const log = await this.store.getLog(guildId);
    if (log.length === 0) {
      return undefined;
    }
    const rebuilt = rebuildStateFromEvents(log);
    const checkpointEvent = [...log].reverse().find((event) => event.body.type === "CHECKPOINT");
    const result = {
      state: rebuilt.state,
      endEvent: log[log.length - 1],
      checkpointEvent,
    };
    this.cacheGuild(guildId, result);
    return result;
  }

  private cacheGuild(guildId: GuildId, rebuilt: RebuiltGuild) {
    this.stateCache.set(guildId, rebuilt);
    if (this.stateCache.size > 512) {
      const oldest = this.stateCache.keys().next().value;
      if (oldest) {
        this.stateCache.delete(oldest);
      }
    }
  }

  private async broadcastEvent(event: GuildEvent, transient = false) {
    const guildId = event.body.guildId;
    const rebuilt = transient ? await this.rebuildGuild(guildId) : this.stateCache.get(guildId);
    const frameByWireFormat = new Map<CgpWireFormat, string | Uint8Array>();
    for (const socket of this.state.getWebSockets()) {
      if (socket.readyState !== WebSocket.OPEN) {
        continue;
      }
      const attachment = this.attachment(socket);
      if (!attachment.subscriptions.some((subscription) => visibleToSubscription(event, rebuilt, subscription))) {
        continue;
      }
      const wireFormat = attachment.wireFormat || "json";
      let frame = frameByWireFormat.get(wireFormat);
      if (!frame) {
        frame = encodeCgpFrame("EVENT", event, wireFormat);
        frameByWireFormat.set(wireFormat, frame);
      }
      socket.send(toSocketMessage(frame));
    }
  }

  private async withGuildMutex<T>(guildId: GuildId, task: () => Promise<T>): Promise<T> {
    const previous = this.mutexes.get(guildId) ?? Promise.resolve();
    const current = previous.catch(() => undefined).then(task);
    this.mutexes.set(guildId, current);
    try {
      return await current;
    } finally {
      if (this.mutexes.get(guildId) === current) {
        this.mutexes.delete(guildId);
      }
    }
  }

  private attachment(socket: WebSocket): WebSocketAttachment {
    try {
      const attachment = socket.deserializeAttachment() as WebSocketAttachment | undefined;
      if (attachment && Array.isArray(attachment.subscriptions)) {
        return attachment;
      }
    } catch {
      // Hibernated sockets without an attachment are treated as fresh.
    }
    return { subscriptions: [], wireFormat: "json" };
  }

  private sendFrame(socket: WebSocket, kind: string, payload: unknown) {
    if (socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    const wireFormat = this.attachment(socket).wireFormat || "json";
    const frame = encodeCgpFrame(kind, payload, wireFormat);
    socket.send(toSocketMessage(frame));
    return true;
  }
}

export default {
  async fetch(request: Request, env: Env) {
    const url = new URL(request.url);
    if (url.pathname === "/" || url.pathname === "/healthz") {
      return json({
        ok: true,
        protocol: "cgp/0.1",
        relay: "cloudflare",
        websocket: "/relay",
        scopedWebSockets: {
          guild: "/relay/guild/{guildId}",
          bucket: "/relay/bucket/{bucketId}",
        },
      });
    }

    if (url.pathname === "/relay" || url.pathname.startsWith("/relay/")) {
      if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 });
      }
      const stub = env.RELAY.getByName(relayObjectName(url));
      return stub.fetch(request);
    }

    return new Response("Not found", { status: 404 });
  },
} satisfies ExportedHandler<Env>;
