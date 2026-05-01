import { WebSocketServer, WebSocket, RawData } from "ws";
import { createServer, IncomingMessage, ServerResponse } from "http";
import { createReadStream } from "fs";
import { stat } from "fs/promises";
import path from "path";
import { monitorEventLoopDelay } from "perf_hooks";
import {
  GuildEvent,
  computeEventId,
  GuildId,
  ChannelId,
  createInitialState,
  applyEvent,
  GuildState,
  isValidCheckpointEvent,
  rebuildStateFromEvents,
  verify,
  verifyObject,
  hashObject,
  generatePrivateKey,
  getPublicKey,
  serializeState,
  sign,
  EventBody,
  Message,
  Checkpoint,
  EphemeralPolicyUpdate,
  Member,
  SerializableMember,
  SerializableMessageRef,
  RelayHead,
  RelayHeadUnsigned,
  RelayHeadConflict,
  RelayHeadQuorum,
  validateEvent,
  canReadGuild,
  canViewChannel,
  findRelayHeadConflicts,
  relayHeadId,
  assertRelayHeadQuorum,
  summarizeRelayHeadQuorum,
  verifyRelayHead,
  CgpWireFormat,
  parseCgpWireData,
  InvalidCgpFrameError,
  encodeCgpFrame,
  encodeCgpWireFrame,
  stringifyCgpFrame,
} from "@cgp/core";
import { LevelStore } from "./store_level";
import {
  HistoryQuery,
  MemberPageQuery,
  normalizeMemberPageLimit,
  Store,
  StoreStorageEstimate,
} from "./store";
import {
  RelayPlugin,
  RelayPluginContext,
  RateLimitPolicy,
  createAbuseControlPolicyPlugin,
  createAppSurfacePolicyPlugin,
  createAppObjectPermissionPlugin,
  createMediaStoragePolicyPlugin,
  createRateLimitPolicyPlugin,
  createRelayPushPlugin,
  createSafetyReportPlugin,
  createWebhookIngressPlugin,
} from "./plugins";

interface Subscription {
  guildId: GuildId;
  channels?: ChannelId[];
  channelSet?: Set<ChannelId>;
  author?: string;
  fingerprint?: string;
}

interface SignedReadRequest {
  author?: string;
  createdAt?: number;
  signature?: string;
}

interface HistoryRequest extends SignedReadRequest {
  subId?: string;
  guildId?: GuildId;
  channelId?: ChannelId;
  beforeSeq?: number;
  afterSeq?: number;
  limit?: number;
  includeStructural?: boolean;
}

interface LogRangeRequest extends SignedReadRequest {
  subId?: string;
  guildId?: GuildId;
  afterSeq?: number;
  limit?: number;
}

type SearchScope = "messages" | "channels" | "members" | "appObjects";
type SearchResultKind = "message" | "channel" | "member" | "appObject";

interface SearchRequest extends SignedReadRequest {
  subId?: string;
  guildId?: GuildId;
  channelId?: ChannelId;
  query?: string;
  scopes?: SearchScope[];
  beforeSeq?: number;
  afterSeq?: number;
  limit?: number;
  includeDeleted?: boolean;
  includeEncrypted?: boolean;
  includeEvent?: boolean;
}

interface SearchResult {
  kind: SearchResultKind;
  guildId: GuildId;
  channelId?: ChannelId;
  messageId?: string;
  userId?: string;
  objectId?: string;
  namespace?: string;
  objectType?: string;
  eventId?: string;
  seq?: number;
  createdAt?: number;
  author?: string;
  encrypted?: boolean;
  preview?: string;
  content?: string;
  event?: GuildEvent;
}

type OutboundFrame = string | Uint8Array;
export type RelayWireFormat = CgpWireFormat;
type FanoutFrameCache = Partial<Record<RelayWireFormat, OutboundFrame>>;

interface NodeTransportSocket {
  setNoDelay?(noDelay?: boolean): void;
  cork?(): void;
  uncork?(): void;
}

interface FanoutQueue {
  frames: OutboundFrame[];
  head: number;
  pending: boolean;
}

export interface RelayPubSubEnvelope {
  originId: string;
  guildId: GuildId;
  event?: GuildEvent;
  events?: GuildEvent[];
  head?: RelayHead;
  frame?: string;
  liveTopics?: boolean;
}

export interface RelayPubSubSubscribeOptions {
  /**
   * Ask replay-capable pubsub transports to replay retained log envelopes whose
   * maximum event sequence is greater than this local sequence.
   */
  afterSeq?: number;
  /**
   * Transport-local cap for replayed envelopes. This bounds catch-up work when a
   * relay restarts after a long outage.
   */
  replayLimit?: number;
}

export interface RelayPubSubAdapter {
  publish(topic: string, envelope: RelayPubSubEnvelope): Promise<void> | void;
  subscribe(
    topic: string,
    handler: (envelope: RelayPubSubEnvelope) => void,
    options?: RelayPubSubSubscribeOptions,
  ): Promise<() => Promise<void> | void> | (() => Promise<void> | void);
  close?(): Promise<void> | void;
}

interface RetainedPubSubEntry {
  envelope: RelayPubSubEnvelope;
  minSeq: number;
  maxSeq: number;
}

interface RetainedPubSubTopic {
  entries: RetainedPubSubEntry[];
  head: number;
}

export class LocalRelayPubSubAdapter implements RelayPubSubAdapter {
  private handlers = new Map<
    string,
    Set<(envelope: RelayPubSubEnvelope) => void>
  >();
  private retained = new Map<string, RetainedPubSubTopic>();

  constructor(private retainEnvelopesPerTopic = 50000) {}

  private appendRetained(topic: string, entry: RetainedPubSubEntry) {
    if (this.retainEnvelopesPerTopic <= 0) {
      return;
    }
    const retained = this.retained.get(topic) ?? { entries: [], head: 0 };
    retained.entries.push(entry);
    while (
      retained.entries.length - retained.head >
      this.retainEnvelopesPerTopic
    ) {
      retained.head += 1;
    }
    if (retained.head > 1024 && retained.head * 2 > retained.entries.length) {
      retained.entries = retained.entries.slice(retained.head);
      retained.head = 0;
    }
    this.retained.set(topic, retained);
  }

  publish(topic: string, envelope: RelayPubSubEnvelope) {
    const events =
      Array.isArray(envelope.events) && envelope.events.length > 0
        ? envelope.events
        : envelope.event
          ? [envelope.event]
          : [];
    const headSeq = Number(envelope.head?.headSeq);
    if (events.length > 0) {
      let minSeq = Number.POSITIVE_INFINITY;
      let maxSeq = Number.NEGATIVE_INFINITY;
      for (const event of events) {
        if (!Number.isFinite(event.seq)) continue;
        minSeq = Math.min(minSeq, event.seq);
        maxSeq = Math.max(maxSeq, event.seq);
      }
      if (Number.isFinite(minSeq) && Number.isFinite(maxSeq)) {
        this.appendRetained(topic, { envelope, minSeq, maxSeq });
      }
    } else if (Number.isFinite(headSeq)) {
      this.appendRetained(topic, {
        envelope,
        minSeq: headSeq,
        maxSeq: headSeq,
      });
    }
    const handlers = this.handlers.get(topic);
    if (handlers?.size === 1) {
      const [handler] = handlers;
      queueMicrotask(() => handler(envelope));
    } else if (handlers && handlers.size > 1) {
      const snapshot = Array.from(handlers);
      queueMicrotask(() => {
        for (const handler of snapshot) {
          handler(envelope);
        }
      });
    }
  }

  subscribe(
    topic: string,
    handler: (envelope: RelayPubSubEnvelope) => void,
    options: RelayPubSubSubscribeOptions = {},
  ) {
    const handlers =
      this.handlers.get(topic) ??
      new Set<(envelope: RelayPubSubEnvelope) => void>();
    handlers.add(handler);
    this.handlers.set(topic, handlers);
    if (options.afterSeq !== undefined) {
      const retained = this.retained.get(topic);
      let replayed = 0;
      const replayLimit =
        options.replayLimit ??
        (retained ? retained.entries.length - retained.head : 0);
      if (retained) {
        for (
          let index = retained.head;
          index < retained.entries.length && replayed < replayLimit;
          index += 1
        ) {
          const item = retained.entries[index];
          if (!item || item.maxSeq <= options.afterSeq) continue;
          queueMicrotask(() => handler(item.envelope));
          replayed += 1;
        }
      }
    }
    return () => {
      handlers.delete(handler);
      if (handlers.size === 0) {
        this.handlers.delete(topic);
      }
    };
  }

  close() {
    this.handlers.clear();
    this.retained.clear();
  }
}

interface StateRequest extends SignedReadRequest {
  subId?: string;
  guildId?: GuildId;
  includeMembers?: boolean;
  includeMessages?: boolean;
  includeAppObjects?: boolean;
  memberAfter?: string;
  memberLimit?: number;
}

interface PublishPayload {
  body: EventBody;
  author: string;
  signature: string;
  createdAt: number;
  clientEventId?: string;
}

interface PublishAck {
  clientEventId?: string;
  guildId: GuildId;
  eventId: string;
  seq: number;
}

interface IndexedPublishPayload extends PublishPayload {
  index: number;
  signatureVerified?: boolean;
}

interface PublishBatchResult {
  index: number;
  ack: PublishAck;
  event?: GuildEvent;
}

interface PublishReplayEntry {
  ack: PublishAck;
  payloadHash: string;
  observedAt: number;
}

interface MembersRequest extends SignedReadRequest {
  subId?: string;
  guildId?: GuildId;
  afterUserId?: string;
  limit?: number;
}

interface RelayHeadRequest extends SignedReadRequest {
  subId?: string;
  guildId?: GuildId;
}

interface RelayHeadsRequest extends SignedReadRequest {
  subId?: string;
  guildId?: GuildId;
}

export interface RelayServerOptions {
  /**
   * Default plugins are recommended reference relay policy, not mandatory CGP protocol state.
   * Set false when embedding a relay with a custom plugin stack.
   */
  enableDefaultPlugins?: boolean;
  rateLimitPolicy?: Partial<RateLimitPolicy>;
  maxFrameBytes?: number;
  searchScanLimit?: number;
  snapshotEventLimit?: number;
  checkpointMessageRefLimit?: number;
  verboseLogging?: boolean;
  maxSocketBufferedBytes?: number;
  maxQueuedFramesPerSocket?: number;
  maxStateCacheGuilds?: number;
  maxCachedMessageRefs?: number;
  maxPublishBatchSize?: number;
  maxPubSubBatchEvents?: number;
  maxFanoutDrainSocketsPerTick?: number;
  fanoutDrainDelayMs?: number;
  instanceId?: string;
  relayPrivateKeyHex?: string;
  pubSubAdapter?: RelayPubSubAdapter;
  peerUrls?: string[];
  peerCatchupIntervalMs?: number;
  peerCatchupMinCanonicalHeads?: number;
  peerCatchupNoSourceCooldownMs?: number;
  trustedPeerReadPublicKeys?: string[];
  wireFormat?: RelayWireFormat;
  /**
   * Optional live pubsub partition topics. Enabled by default for lower live
   * fanout latency; set false or CGP_RELAY_PUBSUB_LIVE_TOPICS=0 when an
   * operator prefers a single durable log topic over partitioned live delivery.
   */
  pubSubLiveTopics?: boolean;
  storagePolicy?: Partial<RelayStoragePolicy>;
}

export interface RelayStoragePolicy {
  /**
   * Readiness flips to not_ready once the local store reaches this byte count.
   * The relay can still serve reads and may still accept writes until hardLimitBytes.
   */
  softLimitBytes: number;
  /**
   * Publish requests are rejected once current usage plus the estimated incoming
   * payload bytes reaches this limit. Set 0 to disable write rejection.
   */
  hardLimitBytes: number;
  /**
   * Minimum interval between recursive store-size scans. Publish paths use the
   * cached estimate unless it is stale.
   */
  estimateIntervalMs: number;
}

interface RelayStorageStatus extends StoreStorageEstimate {
  softLimitBytes: number;
  hardLimitBytes: number;
  pressure: number;
  overSoftLimit: boolean;
  overHardLimit: boolean;
  unavailable: boolean;
}

function positiveIntegerFromEnv(name: string, fallback: number) {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function nonNegativeIntegerFromEnv(name: string, fallback: number) {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed >= 0 ? Math.floor(parsed) : fallback;
}

function byteLimitFromEnv(name: string, fallback: number) {
  const raw = process.env[name];
  if (!raw) return fallback;
  const trimmed = raw.trim().toLowerCase();
  const match = /^(\d+(?:\.\d+)?)\s*([kmgt]?i?b?)?$/.exec(trimmed);
  if (!match) return fallback;
  const value = Number(match[1]);
  if (!Number.isFinite(value) || value < 0) return fallback;
  const unit = match[2] || "";
  const multiplier =
    unit === "k" || unit === "kb" || unit === "kib"
      ? 1024
      : unit === "m" || unit === "mb" || unit === "mib"
        ? 1024 ** 2
        : unit === "g" || unit === "gb" || unit === "gib"
          ? 1024 ** 3
          : unit === "t" || unit === "tb" || unit === "tib"
            ? 1024 ** 4
            : 1;
  return Math.floor(value * multiplier);
}

const RECENT_EVENT_PRUNE_INTERVAL_MS = 1000;

function parseUniqueCsv(value: string | undefined) {
  if (!value) return [];
  const seen = new Set<string>();
  const result: string[] = [];
  for (const raw of value.split(",")) {
    const entry = raw.trim();
    if (!entry || seen.has(entry)) {
      continue;
    }
    seen.add(entry);
    result.push(entry);
  }
  return result;
}

function parsePeerUrls(value: string | undefined) {
  return parseUniqueCsv(value);
}

function parsePublicKeyList(value: string | undefined) {
  return parseUniqueCsv(value);
}

function rawDataByteLength(data: RawData) {
  if (typeof data === "string") return Buffer.byteLength(data);
  if (Array.isArray(data))
    return data.reduce((total, chunk) => total + chunk.byteLength, 0);
  return data.byteLength;
}

function objectPayload(payload: unknown): Record<string, any> {
  return payload && typeof payload === "object" && !Array.isArray(payload)
    ? (payload as Record<string, any>)
    : {};
}

function isStoreClosedError(error: unknown) {
  const e = error as { code?: string; message?: string } | undefined;
  return (
    e?.code === "LEVEL_DATABASE_NOT_OPEN" ||
    e?.code === "LEVEL_DATABASE_NOT_CLOSED" ||
    (typeof e?.message === "string" &&
      /database is not open|database is closed/i.test(e.message))
  );
}

function wireFormatFromValue(value: unknown): RelayWireFormat | undefined {
  return value === "json" ||
    value === "binary-json" ||
    value === "binary-v1" ||
    value === "binary-v2"
    ? value
    : undefined;
}

function relayPrivateKeyFromHex(raw?: string) {
  if (!raw) {
    return undefined;
  }
  const normalized = raw.trim().replace(/^0x/i, "");
  if (!/^[0-9a-fA-F]{64}$/.test(normalized)) {
    throw new Error("Relay private key must be a 32-byte hex string");
  }
  return new Uint8Array(Buffer.from(normalized, "hex"));
}

function transportSocket(ws: WebSocket): NodeTransportSocket | undefined {
  return (ws as unknown as { _socket?: NodeTransportSocket })._socket;
}

const DIRECT_MESSAGE_SNAPSHOT_MESSAGE_LIMIT = Math.max(
  1,
  Number(process.env.CGP_RELAY_DM_SNAPSHOT_MESSAGE_LIMIT ?? "96") || 96,
);

const DIRECT_MESSAGE_SNAPSHOT_TRANSIENT_TYPES = new Set([
  "CALL_EVENT",
  "AGENT_INTENT",
  "CHECKPOINT",
]);
const DIRECT_MESSAGE_SNAPSHOT_DEPENDENT_TYPES = new Set([
  "EDIT_MESSAGE",
  "DELETE_MESSAGE",
  "REACTION_ADD",
  "REACTION_REMOVE",
  "APP_OBJECT_UPSERT",
  "APP_OBJECT_DELETE",
]);

function stringValue(...values: unknown[]) {
  for (const value of values) {
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed) return trimmed;
    }
  }
  return "";
}

function eventType(event: GuildEvent) {
  return stringValue((event.body as any)?.type).toUpperCase();
}

function targetMessageId(event: GuildEvent) {
  const body = event.body as unknown as Record<string, unknown>;
  const target =
    body.target && typeof body.target === "object"
      ? (body.target as Record<string, unknown>)
      : {};
  return stringValue(body.messageId, target.messageId);
}

function compareEventsByHistoryOrder(left: GuildEvent, right: GuildEvent) {
  if (left.seq !== right.seq) {
    return left.seq - right.seq;
  }
  if (left.createdAt !== right.createdAt) {
    return left.createdAt - right.createdAt;
  }
  return stringValue(left.id).localeCompare(stringValue(right.id));
}

function isDirectMessageSnapshotGuild(guildId: GuildId, events: GuildEvent[]) {
  for (const event of events) {
    const body = event.body as unknown as Record<string, unknown>;
    const type = eventType(event);
    const channelId = stringValue(body.channelId);
    const name = stringValue(body.name);
    if (
      type === "CHANNEL_CREATE" &&
      (channelId === guildId || channelId.startsWith("dm:"))
    ) {
      return true;
    }
    if (type === "GUILD_CREATE" && /^(DM|GROUP DM):/i.test(name)) {
      return true;
    }
    if (
      channelId &&
      channelId === stringValue(body.guildId) &&
      channelId === guildId
    ) {
      return true;
    }
  }
  return false;
}

function compactDirectMessageSnapshot(events: GuildEvent[]) {
  const latestGuildCreateByGuild = new Map<string, GuildEvent>();
  const latestChannelCreateByChannel = new Map<string, GuildEvent>();
  const latestPresenceByUser = new Map<string, GuildEvent>();
  const passthroughFrames: GuildEvent[] = [];
  const messageFrames: GuildEvent[] = [];
  const dependentFrames: GuildEvent[] = [];

  for (const event of events) {
    const body = event.body as unknown as Record<string, unknown>;
    const type = eventType(event);
    if (DIRECT_MESSAGE_SNAPSHOT_TRANSIENT_TYPES.has(type)) {
      continue;
    }

    switch (type) {
      case "GUILD_CREATE": {
        const guildId =
          stringValue(body.guildId) || `guild:${latestGuildCreateByGuild.size}`;
        latestGuildCreateByGuild.set(guildId, event);
        break;
      }
      case "CHANNEL_CREATE": {
        const channelId =
          stringValue(body.channelId, body.guildId) ||
          `channel:${latestChannelCreateByChannel.size}`;
        latestChannelCreateByChannel.set(channelId, event);
        break;
      }
      case "MEMBER_UPDATE":
      case "GUILD_MEMBER_ADD": {
        const userId = stringValue(body.userId, body.authorId, event.author);
        if (userId) {
          latestPresenceByUser.set(userId.toLowerCase(), event);
        }
        break;
      }
      case "MESSAGE":
        messageFrames.push(event);
        break;
      case "EDIT_MESSAGE":
      case "DELETE_MESSAGE":
      case "REACTION_ADD":
      case "REACTION_REMOVE":
      case "APP_OBJECT_UPSERT":
      case "APP_OBJECT_DELETE":
        dependentFrames.push(event);
        break;
      default:
        passthroughFrames.push(event);
        break;
    }
  }

  const tailMessages = messageFrames.slice(
    -DIRECT_MESSAGE_SNAPSHOT_MESSAGE_LIMIT,
  );
  const keptMessageIds = new Set(
    tailMessages
      .map((event) => stringValue((event.body as any)?.messageId, event.id))
      .filter(Boolean),
  );
  const keptDependentFrames = dependentFrames.filter((event) =>
    keptMessageIds.has(targetMessageId(event)),
  );

  const structuralFrames = [
    ...latestGuildCreateByGuild.values(),
    ...latestChannelCreateByChannel.values(),
    ...latestPresenceByUser.values(),
  ].sort(compareEventsByHistoryOrder);
  const timelineFrames = [
    ...passthroughFrames,
    ...tailMessages,
    ...keptDependentFrames,
  ].sort(compareEventsByHistoryOrder);

  return [...structuralFrames, ...timelineFrames];
}

function buildSnapshotEvents(guildId: GuildId, events: GuildEvent[]) {
  if (!isDirectMessageSnapshotGuild(guildId, events)) {
    return events;
  }
  return compactDirectMessageSnapshot(events);
}

function buildReplaySnapshotEvents(guildId: GuildId, events: GuildEvent[]) {
  const compacted = buildSnapshotEvents(guildId, events);
  if (compacted.length !== events.length) {
    return compacted;
  }

  for (let index = events.length - 1; index >= 0; index--) {
    const event = events[index];
    if (event.body.type === "CHECKPOINT" && isValidCheckpointEvent(event)) {
      return events.slice(index);
    }
  }

  return events;
}

function normalizeHistoryLimit(limit: unknown) {
  const parsed = Number(limit);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 100;
  }

  return Math.min(500, Math.floor(parsed));
}

function normalizeLogRangeLimit(limit: unknown) {
  const parsed = Number(limit);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 5000;
  }

  return Math.min(10000, Math.floor(parsed));
}

function normalizeSearchLimit(limit: unknown) {
  const parsed = Number(limit);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 25;
  }

  return Math.min(100, Math.floor(parsed));
}

function normalizeOptionalSeq(value: unknown) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0
    ? Math.floor(parsed)
    : undefined;
}

function normalizeSearchQuery(query: unknown) {
  if (typeof query !== "string") return "";
  return query.trim().replace(/\s+/g, " ").slice(0, 256);
}

const SEARCH_SCOPES: SearchScope[] = [
  "messages",
  "channels",
  "members",
  "appObjects",
];

function normalizeSearchScopes(scopes: unknown) {
  if (!Array.isArray(scopes)) {
    return new Set<SearchScope>(SEARCH_SCOPES);
  }

  const selected = scopes.filter((scope): scope is SearchScope =>
    SEARCH_SCOPES.includes(scope as SearchScope),
  );
  return new Set<SearchScope>(selected.length > 0 ? selected : SEARCH_SCOPES);
}

function searchableText(value: unknown): string {
  if (value === undefined || value === null) return "";
  if (typeof value === "string") return value.toLowerCase();
  if (typeof value === "number" || typeof value === "boolean")
    return String(value).toLowerCase();
  try {
    return JSON.stringify(value).toLowerCase();
  } catch {
    return "";
  }
}

function matchesSearchQuery(query: string, ...values: unknown[]) {
  const needle = query.toLowerCase();
  return values.some((value) => searchableText(value).includes(needle));
}

function previewText(value: unknown) {
  const text = typeof value === "string" ? value : "";
  const collapsed = text.replace(/\s+/g, " ").trim();
  if (!collapsed) return undefined;
  return collapsed.length > 180 ? `${collapsed.slice(0, 177)}...` : collapsed;
}

function attachmentSearchValues(
  attachments: unknown,
  includeEncrypted: boolean,
) {
  if (!Array.isArray(attachments)) return [];
  const values: unknown[] = [];
  for (const attachment of attachments) {
    if (!attachment || typeof attachment !== "object") continue;
    const ref = attachment as Record<string, unknown>;
    if (ref.encrypted === true && !includeEncrypted) continue;
    values.push(
      ref.id,
      ref.name,
      ref.mimeType,
      ref.type,
      ref.hash,
      ref.url,
      ref.scheme,
      ref.external,
    );
  }
  return values;
}

function passesSeqWindow(
  seq: number | undefined,
  beforeSeq?: number,
  afterSeq?: number,
) {
  if (seq === undefined) {
    return beforeSeq === undefined && afterSeq === undefined;
  }
  if (beforeSeq !== undefined && seq >= beforeSeq) return false;
  if (afterSeq !== undefined && seq <= afterSeq) return false;
  return true;
}

function stripReadSignature(payload: unknown) {
  const source =
    payload && typeof payload === "object"
      ? (payload as Record<string, unknown>)
      : {};
  const { signature: _signature, ...unsigned } = source;
  return unsigned;
}

function eventChannelId(state: GuildState, event: GuildEvent) {
  const body = event.body as unknown as Record<string, any>;
  const directChannelId = stringValue(body.channelId, body.target?.channelId);
  if (directChannelId) {
    return directChannelId;
  }

  const messageId = stringValue(body.messageId, body.target?.messageId);
  if (messageId) {
    return state.messages.get(messageId)?.channelId;
  }

  return undefined;
}

function eventVisibleToReader(
  state: GuildState,
  event: GuildEvent,
  author?: string,
) {
  const body = event.body as unknown as Record<string, unknown>;
  const type = eventType(event);
  if (
    author &&
    stringValue(body.userId) === author &&
    (type === "BAN_USER" ||
      type === "BAN_ADD" ||
      type === "MEMBER_KICK" ||
      type === "ROLE_REVOKE")
  ) {
    return true;
  }

  if (!canReadGuild(state, author)) {
    return false;
  }

  const channelId = eventChannelId(state, event);
  if (!channelId) {
    return true;
  }

  return canViewChannel(state, author, channelId);
}

function filterEventsForReader(
  state: GuildState,
  events: GuildEvent[],
  author?: string,
) {
  return events.filter((event) => eventVisibleToReader(state, event, author));
}

function eventMatchesRequestedChannels(
  state: GuildState,
  event: GuildEvent,
  channels?: ChannelId[],
  channelSet?: Set<ChannelId>,
) {
  const hasChannelSet = channelSet && channelSet.size > 0;
  if (!hasChannelSet && (!Array.isArray(channels) || channels.length === 0)) {
    return true;
  }

  const channelId = eventChannelId(state, event);
  return (
    !channelId ||
    (hasChannelSet ? channelSet.has(channelId) : channels!.includes(channelId))
  );
}

function filterSerializedStateForReader(
  state: GuildState,
  author?: string,
  options: {
    includeMembers?: boolean;
    includeMessages?: boolean;
    includeAppObjects?: boolean;
  } = {},
) {
  const channels: ReturnType<typeof serializeState>["channels"] = [];
  for (const entry of state.channels) {
    if (canViewChannel(state, author, entry[0])) {
      channels.push(entry);
    }
  }

  const needsChannelVisibility =
    options.includeMessages !== false || options.includeAppObjects !== false;
  const visibleChannelIds = needsChannelVisibility
    ? new Set(channels.map(([channelId]) => channelId))
    : null;

  const members: ReturnType<typeof serializeState>["members"] = [];
  if (options.includeMembers !== false) {
    for (const [id, member] of state.members) {
      members.push([
        id,
        {
          ...member,
          roles: Array.from(member.roles),
        },
      ]);
    }
  }

  const messages: ReturnType<typeof serializeState>["messages"] = [];
  if (options.includeMessages !== false && visibleChannelIds) {
    for (const entry of state.messages) {
      if (visibleChannelIds.has(entry[1].channelId)) {
        messages.push(entry);
      }
    }
  }

  const appObjects: ReturnType<typeof serializeState>["appObjects"] = [];
  if (options.includeAppObjects !== false && visibleChannelIds) {
    for (const entry of state.appObjects) {
      const object = entry[1];
      const channelId = object.channelId || object.target?.channelId;
      if (!channelId || visibleChannelIds.has(channelId)) {
        appObjects.push(entry);
      }
    }
  }

  return {
    guildId: state.guildId,
    name: state.name,
    description: state.description || "",
    ownerId: state.ownerId,
    channels,
    members,
    roles: Array.from(state.roles.entries()),
    bans: Array.from(state.bans.entries()),
    messages,
    appObjects,
    access: state.access,
    policies: state.policies,
  };
}

function serializeBoundedCheckpointState(
  state: GuildState,
  messageRefLimit: number,
) {
  const serialized = serializeState(state);
  if (
    Number.isFinite(messageRefLimit) &&
    messageRefLimit >= 0 &&
    serialized.messages &&
    serialized.messages.length > messageRefLimit
  ) {
    serialized.messages = serialized.messages.slice(
      serialized.messages.length - messageRefLimit,
    );
  }
  return serialized;
}

interface SearchMessageRecord {
  guildId: GuildId;
  channelId: ChannelId;
  messageId: string;
  author: string;
  event: GuildEvent;
  latestSeq: number;
  latestCreatedAt: number;
  latestContent: string;
  attachments: unknown;
  encrypted: boolean;
  deleted: boolean;
}

function collectSearchMessageRecords(events: GuildEvent[]) {
  const records = new Map<string, SearchMessageRecord>();

  for (const event of events) {
    const body = event.body as unknown as Record<string, unknown>;
    const type = eventType(event);
    const messageId = stringValue(
      body.messageId,
      (body.target as any)?.messageId,
    );
    if (!messageId) continue;

    if (type === "MESSAGE") {
      const channelId = stringValue(body.channelId);
      if (!channelId) continue;
      records.set(messageId, {
        guildId: stringValue(body.guildId),
        channelId,
        messageId,
        author: event.author,
        event,
        latestSeq: event.seq,
        latestCreatedAt: event.createdAt,
        latestContent: typeof body.content === "string" ? body.content : "",
        attachments: body.attachments,
        encrypted: body.encrypted === true,
        deleted: false,
      });
      continue;
    }

    const record = records.get(messageId);
    if (!record) continue;

    if (type === "EDIT_MESSAGE") {
      record.latestSeq = event.seq;
      record.latestCreatedAt = event.createdAt;
      record.latestContent =
        typeof body.newContent === "string"
          ? body.newContent
          : record.latestContent;
    } else if (type === "DELETE_MESSAGE") {
      record.latestSeq = event.seq;
      record.latestCreatedAt = event.createdAt;
      record.deleted = true;
    }
  }

  return Array.from(records.values());
}

function finalizeSearchResults(
  results: SearchResult[],
  request: {
    limit: number;
  },
) {
  results.sort((a, b) => {
    const seqDelta = (b.seq ?? -1) - (a.seq ?? -1);
    if (seqDelta !== 0) return seqDelta;
    return (b.createdAt ?? 0) - (a.createdAt ?? 0);
  });

  const limited = results.slice(0, request.limit + 1);
  const hasMore = limited.length > request.limit;
  return {
    results: limited.slice(0, request.limit),
    hasMore,
  };
}

function buildSearchResults(
  state: GuildState,
  events: GuildEvent[],
  author: string | undefined,
  request: Required<
    Pick<SearchRequest, "includeDeleted" | "includeEncrypted" | "includeEvent">
  > & {
    guildId: GuildId;
    channelId?: ChannelId;
    query: string;
    scopes: Set<SearchScope>;
    beforeSeq?: number;
    afterSeq?: number;
    limit: number;
  },
  seedResults: SearchResult[] = [],
) {
  const results: SearchResult[] = [...seedResults];
  const hasEnoughResults = () => results.length > request.limit;

  if (request.scopes.has("messages")) {
    const visibleEvents = filterEventsForReader(state, events, author);
    for (const record of collectSearchMessageRecords(visibleEvents)) {
      if (request.channelId && record.channelId !== request.channelId) continue;
      if (
        !passesSeqWindow(record.latestSeq, request.beforeSeq, request.afterSeq)
      )
        continue;
      if (record.deleted && !request.includeDeleted) continue;

      const attachmentValues = attachmentSearchValues(
        record.attachments,
        request.includeEncrypted,
      );
      const contentValues = record.encrypted ? [] : [record.latestContent];
      if (
        !matchesSearchQuery(
          request.query,
          record.messageId,
          record.channelId,
          record.author,
          ...contentValues,
          ...attachmentValues,
        )
      ) {
        continue;
      }

      const result: SearchResult = {
        kind: "message",
        guildId: request.guildId,
        channelId: record.channelId,
        messageId: record.messageId,
        eventId: record.event.id,
        seq: record.latestSeq,
        createdAt: record.latestCreatedAt,
        author: record.author,
        encrypted: record.encrypted,
        preview: record.encrypted
          ? "Encrypted message"
          : previewText(record.latestContent),
        content: record.encrypted ? undefined : record.latestContent,
      };
      if (request.includeEvent) {
        result.event = record.event;
      }
      results.push(result);
    }
  }

  if (!hasEnoughResults() && request.scopes.has("channels")) {
    for (const [channelId, channel] of state.channels) {
      if (request.channelId && channelId !== request.channelId) continue;
      if (!canViewChannel(state, author, channelId)) continue;
      if (!passesSeqWindow(undefined, request.beforeSeq, request.afterSeq))
        continue;
      if (
        !matchesSearchQuery(
          request.query,
          channelId,
          channel.name,
          channel.kind,
          channel.topic,
          channel.description,
          channel.categoryId,
        )
      ) {
        continue;
      }
      results.push({
        kind: "channel",
        guildId: request.guildId,
        channelId,
        preview: previewText(
          [channel.name, channel.topic || channel.description]
            .filter(Boolean)
            .join(" - "),
        ),
      });
      if (hasEnoughResults()) break;
    }
  }

  if (!hasEnoughResults() && request.scopes.has("members")) {
    for (const [userId, member] of state.members) {
      if (!passesSeqWindow(undefined, request.beforeSeq, request.afterSeq))
        continue;
      if (
        !matchesSearchQuery(
          request.query,
          userId,
          member.nickname,
          member.bio,
          Array.from(member.roles),
        )
      ) {
        continue;
      }
      results.push({
        kind: "member",
        guildId: request.guildId,
        userId,
        preview: previewText(member.nickname || member.bio || userId),
      });
      if (hasEnoughResults()) break;
    }
  }

  if (!hasEnoughResults() && request.scopes.has("appObjects")) {
    for (const [, object] of state.appObjects) {
      if (!passesSeqWindow(undefined, request.beforeSeq, request.afterSeq))
        continue;
      const channelId = object.channelId || object.target?.channelId;
      if (request.channelId && channelId !== request.channelId) continue;
      if (channelId && !canViewChannel(state, author, channelId)) continue;
      if (
        !matchesSearchQuery(
          request.query,
          object.namespace,
          object.objectType,
          object.objectId,
          object.target,
          object.value,
        )
      ) {
        continue;
      }
      results.push({
        kind: "appObject",
        guildId: request.guildId,
        channelId,
        objectId: object.objectId,
        namespace: object.namespace,
        objectType: object.objectType,
        author: object.authorId,
        createdAt: object.updatedAt,
        preview: previewText(
          `${object.namespace}/${object.objectType}/${object.objectId}`,
        ),
      });
      if (hasEnoughResults()) break;
    }
  }

  return finalizeSearchResults(results, request);
}

export class RelayServer {
  private wss: WebSocketServer;
  private httpServer: ReturnType<typeof createServer>;
  private store: Store;
  private plugins: RelayPlugin[];
  private pluginCtx: RelayPluginContext;
  private pluginsReady: Promise<void>;
  private subscriptions = new Map<WebSocket, Map<string, Subscription>>();
  private guildSubscriptions = new Map<GuildId, Map<WebSocket, Set<string>>>();
  private guildAllChannelSubscriptions = new Map<
    GuildId,
    Map<WebSocket, Set<string>>
  >();
  private guildChannelSubscriptions = new Map<
    GuildId,
    Map<ChannelId, Map<WebSocket, Set<string>>>
  >();
  private socketWireFormats = new WeakMap<WebSocket, RelayWireFormat>();
  private messageQueues = new Map<WebSocket, Promise<void>>();
  private fanoutQueues = new Map<WebSocket, FanoutQueue>();
  private fanoutDrainPending: WebSocket[] = [];
  private fanoutDrainPendingHead = 0;
  private fanoutDrainTimer?: NodeJS.Timeout;
  private fanoutDrainImmediate?: NodeJS.Immediate;
  private fanoutDrainScheduled = false;
  private fanoutDrainActive = false;
  private scheduledBroadcasts = new Set<NodeJS.Immediate>();
  private pubSubUnsubscribers = new Map<string, () => Promise<void> | void>();
  private pubSubPendingGuilds = new Set<string>();
  private hostedGuilds = new Set<GuildId>();
  private stateCache = new Map<GuildId, GuildState>();
  private checkpointCache = new Map<GuildId, GuildEvent | undefined>();
  private observedRelayHeads = new Map<GuildId, Map<string, RelayHead>>();
  private relayHeadConflicts = new Map<GuildId, RelayHeadConflict[]>();
  private pubSubCatchupKeys = new Set<string>();
  private peerCatchupKeys = new Set<string>();
  private mutexes = new Map<GuildId, Promise<void>>();
  private maxFrameBytes: number;
  private searchScanLimit: number;
  private snapshotEventLimit: number;
  private checkpointMessageRefLimit: number;
  private verboseLogging: boolean;
  private maxSocketBufferedBytes: number;
  private maxQueuedFramesPerSocket: number;
  private maxStateCacheGuilds: number;
  private maxCachedMessageRefs: number;
  private maxPublishBatchSize: number;
  private maxRecentPublishAcks: number;
  private recentPublishAckTtlMs: number;
  private maxPubSubBatchEvents: number;
  private maxFanoutDrainSocketsPerTick: number;
  private fanoutDrainDelayMs: number;
  private pubSubReplayLimit: number;
  private relayInstanceId: string;
  private pubSubAdapter?: RelayPubSubAdapter;
  private peerUrls: string[];
  private peerCatchupIntervalMs: number;
  private peerCatchupMinCanonicalHeads: number;
  private peerCatchupNoSourceCooldownMs: number;
  private trustedPeerReadPublicKeys = new Set<string>();
  private peerCatchupTimer?: NodeJS.Timeout;
  private peerCatchupNoSourceUntil = new Map<GuildId, number>();
  private wireFormat: RelayWireFormat;
  private pubSubLiveTopics: boolean;
  private closing = false;
  private closePromise?: Promise<void>;
  private startedAt = Date.now();
  private eventLoopDelay = monitorEventLoopDelay({ resolution: 20 });
  private storagePolicy: RelayStoragePolicy;
  private storageStatus: RelayStorageStatus = {
    bytes: 0,
    files: 0,
    checkedAt: 0,
    softLimitBytes: 0,
    hardLimitBytes: 0,
    pressure: 0,
    overSoftLimit: false,
    overHardLimit: false,
    unavailable: true,
  };
  private storageEstimatePromise?: Promise<RelayStorageStatus>;
  private storageUnestimatedWrittenBytes = 0;

  private pruneTimer: NodeJS.Timeout;
  private checkpointTimer?: NodeJS.Timeout;
  private keyPair: { publicKey: string; privateKey: Uint8Array };
  private recentPublishAcks = new Map<string, PublishReplayEntry>();
  private recentKnownEventIds = new Map<string, number>();
  private recentDeliveredPubSubEventIds = new Map<string, number>();
  private pendingPubSubDeliveryEvents = new Map<
    string,
    { event: GuildEvent; observedAt: number }
  >();
  private recentEventPruneAt = new WeakMap<Map<string, number>, number>();
  private pendingPubSubPruneAt = 0;

  constructor(
    port: number,
    dbPathOrStore: string | Store = "./relay-db",
    plugins: RelayPlugin[] = [],
    options: RelayServerOptions = {},
  ) {
    if (typeof dbPathOrStore === "string") {
      this.store = new LevelStore(dbPathOrStore);
    } else {
      this.store = dbPathOrStore;
    }
    this.maxFrameBytes =
      options.maxFrameBytes ??
      positiveIntegerFromEnv("CGP_RELAY_MAX_FRAME_BYTES", 1024 * 1024);
    this.searchScanLimit =
      options.searchScanLimit ??
      positiveIntegerFromEnv("CGP_RELAY_SEARCH_SCAN_EVENTS", 25000);
    this.snapshotEventLimit =
      options.snapshotEventLimit ??
      positiveIntegerFromEnv("CGP_RELAY_SNAPSHOT_EVENTS", 5000);
    this.checkpointMessageRefLimit =
      options.checkpointMessageRefLimit ??
      positiveIntegerFromEnv("CGP_RELAY_CHECKPOINT_MESSAGE_REFS", 50000);
    this.verboseLogging =
      options.verboseLogging ?? process.env.CGP_RELAY_VERBOSE_LOGS === "1";
    this.maxSocketBufferedBytes =
      options.maxSocketBufferedBytes ??
      positiveIntegerFromEnv(
        "CGP_RELAY_MAX_SOCKET_BUFFERED_BYTES",
        4 * 1024 * 1024,
      );
    this.maxQueuedFramesPerSocket =
      options.maxQueuedFramesPerSocket ??
      positiveIntegerFromEnv("CGP_RELAY_MAX_QUEUED_FRAMES_PER_SOCKET", 256);
    this.maxStateCacheGuilds =
      options.maxStateCacheGuilds ??
      positiveIntegerFromEnv("CGP_RELAY_MAX_STATE_CACHE_GUILDS", 1000);
    this.maxCachedMessageRefs =
      options.maxCachedMessageRefs ??
      positiveIntegerFromEnv(
        "CGP_RELAY_MAX_CACHED_MESSAGE_REFS",
        this.checkpointMessageRefLimit,
      );
    this.maxPublishBatchSize =
      options.maxPublishBatchSize ??
      positiveIntegerFromEnv("CGP_RELAY_MAX_PUBLISH_BATCH_SIZE", 1024);
    this.maxRecentPublishAcks = positiveIntegerFromEnv(
      "CGP_RELAY_RECENT_PUBLISH_ACKS",
      250_000,
    );
    this.recentPublishAckTtlMs = positiveIntegerFromEnv(
      "CGP_RELAY_RECENT_PUBLISH_ACK_TTL_MS",
      60 * 60 * 1000,
    );
    this.maxPubSubBatchEvents =
      options.maxPubSubBatchEvents ??
      positiveIntegerFromEnv("CGP_RELAY_PUBSUB_BATCH_EVENTS", 512);
    this.maxFanoutDrainSocketsPerTick =
      options.maxFanoutDrainSocketsPerTick ??
      positiveIntegerFromEnv("CGP_RELAY_FANOUT_DRAIN_SOCKETS_PER_TICK", 2048);
    this.fanoutDrainDelayMs =
      options.fanoutDrainDelayMs ??
      nonNegativeIntegerFromEnv("CGP_RELAY_FANOUT_DRAIN_DELAY_MS", 0);
    this.pubSubReplayLimit = positiveIntegerFromEnv(
      "CGP_RELAY_PUBSUB_REPLAY_LIMIT",
      100000,
    );
    this.relayInstanceId =
      options.instanceId ||
      `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    this.pubSubAdapter = options.pubSubAdapter;
    this.peerUrls =
      options.peerUrls ?? parsePeerUrls(process.env.CGP_RELAY_PEERS);
    this.peerCatchupIntervalMs =
      options.peerCatchupIntervalMs ??
      positiveIntegerFromEnv("CGP_RELAY_PEER_CATCHUP_INTERVAL_MS", 0);
    const defaultPeerCatchupQuorum = this.peerUrls.length <= 1 ? 1 : 2;
    this.peerCatchupMinCanonicalHeads =
      options.peerCatchupMinCanonicalHeads ??
      positiveIntegerFromEnv(
        "CGP_RELAY_PEER_CATCHUP_MIN_CANONICAL_HEADS",
        defaultPeerCatchupQuorum,
      );
    this.peerCatchupNoSourceCooldownMs =
      options.peerCatchupNoSourceCooldownMs ??
      positiveIntegerFromEnv(
        "CGP_RELAY_PEER_CATCHUP_NO_SOURCE_COOLDOWN_MS",
        1000,
      );
    this.trustedPeerReadPublicKeys = new Set(
      options.trustedPeerReadPublicKeys ??
        parsePublicKeyList(process.env.CGP_RELAY_TRUSTED_PEER_READ_PUBLIC_KEYS),
    );
    this.wireFormat =
      options.wireFormat ??
      wireFormatFromValue(process.env.CGP_RELAY_WIRE_FORMAT) ??
      "json";
    this.pubSubLiveTopics =
      options.pubSubLiveTopics ??
      process.env.CGP_RELAY_PUBSUB_LIVE_TOPICS !== "0";
    this.storagePolicy = {
      softLimitBytes: Math.max(
        0,
        Math.floor(
          options.storagePolicy?.softLimitBytes ??
            byteLimitFromEnv("CGP_RELAY_STORAGE_SOFT_LIMIT_BYTES", 0),
        ),
      ),
      hardLimitBytes: Math.max(
        0,
        Math.floor(
          options.storagePolicy?.hardLimitBytes ??
            byteLimitFromEnv("CGP_RELAY_STORAGE_HARD_LIMIT_BYTES", 0),
        ),
      ),
      estimateIntervalMs: Math.max(
        1000,
        Math.floor(
          options.storagePolicy?.estimateIntervalMs ??
            positiveIntegerFromEnv(
              "CGP_RELAY_STORAGE_ESTIMATE_INTERVAL_MS",
              15000,
            ),
        ),
      ),
    };
    this.storageStatus = this.emptyStorageStatus();
    this.eventLoopDelay.enable();
    const defaultPluginsEnabled =
      options.enableDefaultPlugins ??
      process.env.CGP_RELAY_DEFAULT_PLUGINS !== "0";
    this.plugins = defaultPluginsEnabled
      ? [
          createRateLimitPolicyPlugin(options.rateLimitPolicy),
          createAbuseControlPolicyPlugin(),
          createSafetyReportPlugin(),
          createAppSurfacePolicyPlugin(),
          createMediaStoragePolicyPlugin(),
          createWebhookIngressPlugin(),
          ...(process.env.CGP_RELAY_PUSH_ENABLED === "0"
            ? []
            : [createRelayPushPlugin()]),
          createAppObjectPermissionPlugin({
            rules: [
              {
                namespace: "org.cgp.chat",
                objectType: "message-pin",
                permissionScope: "messages",
              },
            ],
          }),
          ...plugins,
        ]
      : [...plugins];
    this.httpServer = createServer((req, res) => {
      void this.handleHttp(req, res).catch((err) => {
        console.error("Relay HTTP handler failed:", err);
        if (!res.headersSent) {
          res.statusCode = 500;
          res.end("Internal error");
        } else {
          res.end();
        }
      });
    });
    this.wss = new WebSocketServer({
      server: this.httpServer,
      perMessageDeflate: false,
    });

    const configuredPrivateKey = relayPrivateKeyFromHex(
      options.relayPrivateKeyHex ?? process.env.CGP_RELAY_PRIVATE_KEY_HEX,
    );
    const privateKey = configuredPrivateKey ?? generatePrivateKey();
    this.keyPair = {
      privateKey,
      publicKey: getPublicKey(privateKey),
    };
    console.log(`Relay started with public key: ${this.keyPair.publicKey}`);

    this.pluginCtx = {
      relayPublicKey: this.keyPair.publicKey,
      store: this.store,
      publishAsRelay: async (body: EventBody, createdAt?: number) => {
        return this.publishAsRelay(body, createdAt);
      },
      broadcast: (guildId: string, event: GuildEvent) => {
        this.broadcast(guildId, event);
      },
      getLog: async (guildId: GuildId) => {
        return await this.store.getLog(guildId);
      },
      getState: async (guildId: GuildId) => {
        return (await this.rebuildGuildState(guildId))?.state ?? null;
      },
    };
    this.pluginsReady = this.initPlugins();

    this.wss.on("connection", (socket) => {
      if (this.closing) {
        socket.close(1001, "Relay shutting down");
        return;
      }
      transportSocket(socket)?.setNoDelay?.(true);
      this.subscriptions.set(socket, new Map());
      this.socketWireFormats.set(socket, this.wireFormat);
      this.messageQueues.set(socket, Promise.resolve());

      socket.on("message", (data) => {
        if (this.closing) {
          return;
        }
        const currentQueue =
          this.messageQueues.get(socket) || Promise.resolve();
        const nextTask = currentQueue.then(async () => {
          if (this.closing) {
            return;
          }
          try {
            if (rawDataByteLength(data) > this.maxFrameBytes) {
              this.sendFrame(socket, "ERROR", {
                code: "PAYLOAD_TOO_LARGE",
                message: `Frame exceeds ${this.maxFrameBytes} byte relay limit`,
              });
              return;
            }

            const { kind, payload } = parseCgpWireData(data, {
              includeRawFrame: false,
            });
            await this.handleMessage(socket, kind, payload);
          } catch (e: any) {
            if (this.closing) {
              return;
            }
            if (e instanceof InvalidCgpFrameError) {
              this.sendFrame(socket, "ERROR", {
                code: "INVALID_FRAME",
                message: "Parse error",
              });
            } else {
              console.error("Error handling message", e);
              this.sendFrame(socket, "ERROR", {
                code: "INTERNAL_ERROR",
                message: "Internal relay error",
              });
            }
          }
        });
        this.messageQueues.set(socket, nextTask);
      });

      socket.on("close", () => {
        this.removeSocketSubscriptions(socket);
        this.messageQueues.delete(socket);
        this.clearFanoutQueue(socket);
      });
    });

    // Run prune every 60 seconds
    this.pruneTimer = setInterval(() => {
      this.prune().catch((err) => console.error("Prune failed:", err));
    }, 60000);

    const rawCheckpointInterval = process.env.CGP_RELAY_CHECKPOINT_INTERVAL_MS;
    const parsedCheckpointInterval =
      rawCheckpointInterval === undefined ? NaN : Number(rawCheckpointInterval);
    const checkpointIntervalMs =
      rawCheckpointInterval === undefined
        ? 60000
        : Number.isFinite(parsedCheckpointInterval) &&
            parsedCheckpointInterval >= 0
          ? Math.floor(parsedCheckpointInterval)
          : 60000;
    if (checkpointIntervalMs > 0) {
      this.checkpointTimer = setInterval(() => {
        this.createCheckpoints().catch((err) =>
          console.error("Checkpoint failed:", err),
        );
      }, checkpointIntervalMs);
    }

    this.httpServer.listen(port);
    console.log(`Relay listening on port ${port}`);
    void this.bootstrapPubSubHostedGuilds().catch((err) => {
      console.error("Relay pubsub hosted guild bootstrap failed:", err);
    });
    if (this.peerUrls.length > 0 && this.peerCatchupIntervalMs > 0) {
      this.peerCatchupTimer = setInterval(() => {
        void this.runPeriodicPeerCatchup().catch((err) => {
          console.error("Relay peer catch-up sweep failed:", err);
        });
      }, this.peerCatchupIntervalMs);
      this.peerCatchupTimer.unref?.();
    }
  }

  public getPort(): number {
    const addr = this.wss.address();
    if (!addr || typeof addr === "string") return NaN;
    return addr.port;
  }

  private setExtensionHeaders(
    res: ServerResponse,
    contentType?: string,
    length?: number,
  ) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");
    res.setHeader("Cross-Origin-Resource-Policy", "cross-origin");
    res.setHeader("Cache-Control", "no-store");
    if (contentType) res.setHeader("Content-Type", contentType);
    if (typeof length === "number") res.setHeader("Content-Length", length);
  }

  private getContentType(filePath: string) {
    const ext = path.extname(filePath).toLowerCase();
    switch (ext) {
      case ".js":
      case ".mjs":
        return "application/javascript; charset=utf-8";
      case ".json":
      case ".map":
        return "application/json; charset=utf-8";
      case ".css":
        return "text/css; charset=utf-8";
      case ".svg":
        return "image/svg+xml";
      case ".png":
        return "image/png";
      case ".jpg":
      case ".jpeg":
        return "image/jpeg";
      case ".gif":
        return "image/gif";
      default:
        return "application/octet-stream";
    }
  }

  private findExtensionPlugin(extensionName: string): RelayPlugin | undefined {
    if (!extensionName) return undefined;
    return (
      this.plugins.find(
        (plugin) => plugin.metadata?.clientExtension === extensionName,
      ) || this.plugins.find((plugin) => plugin.name === extensionName)
    );
  }

  private async handleHttp(req: IncomingMessage, res: ServerResponse) {
    const rawUrl = req.url || "/";
    const pathname = rawUrl.split("?")[0] || "/";
    await this.pluginsReady;

    if (await this.handleOpsHttp(req, res, pathname)) {
      return;
    }

    if (await this.handlePluginHttp(req, res, rawUrl, pathname)) {
      return;
    }

    if (await this.handleExtensionHttp(req, res, pathname)) {
      return;
    }

    res.statusCode = 404;
    res.end("Not found");
  }

  private async handleOpsHttp(
    req: IncomingMessage,
    res: ServerResponse,
    pathname: string,
  ) {
    if (
      pathname !== "/healthz" &&
      pathname !== "/readyz" &&
      pathname !== "/metrics"
    ) {
      return false;
    }

    if (req.method !== "GET" && req.method !== "HEAD") {
      res.statusCode = 405;
      res.end("Method not allowed");
      return true;
    }

    if (pathname === "/metrics") {
      await this.refreshStorageStatus(true);
      const body = this.renderPrometheusMetrics();
      res.setHeader("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
      res.statusCode = 200;
      if (req.method === "HEAD") {
        res.end();
      } else {
        res.end(body);
      }
      return true;
    }

    const storage = await this.refreshStorageStatus(true);
    const ready =
      !this.closing && this.httpServer.listening && !storage.overSoftLimit;
    const payload = {
      ok: pathname === "/healthz" ? true : ready,
      status: pathname === "/healthz" ? "ok" : ready ? "ready" : "not_ready",
      relayId: this.relayInstanceId,
      relayPublicKey: this.keyPair.publicKey,
      uptimeMs: Date.now() - this.startedAt,
      websocketClients: this.wss.clients.size,
      subscriptions: this.countSubscriptions(),
      hostedGuilds: this.hostedGuilds.size,
      pubSubTopics: this.pubSubUnsubscribers.size,
      storage,
      closing: this.closing,
    };
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.statusCode = payload.ok ? 200 : 503;
    if (req.method === "HEAD") {
      res.end();
    } else {
      res.end(JSON.stringify(payload));
    }
    return true;
  }

  private countSubscriptions() {
    let count = 0;
    for (const subscriptions of this.subscriptions.values()) {
      count += subscriptions.size;
    }
    return count;
  }

  private countQueuedFanoutFrames() {
    let count = 0;
    for (const queue of this.fanoutQueues.values()) {
      count += Math.max(0, queue.frames.length - queue.head);
    }
    return count;
  }

  private emptyStorageStatus(): RelayStorageStatus {
    return {
      bytes: 0,
      files: 0,
      checkedAt: Date.now(),
      softLimitBytes: this.storagePolicy?.softLimitBytes ?? 0,
      hardLimitBytes: this.storagePolicy?.hardLimitBytes ?? 0,
      pressure: 0,
      overSoftLimit: false,
      overHardLimit: false,
      unavailable: !this.store?.estimateStorage,
    };
  }

  private storageStatusFromEstimate(
    estimate: StoreStorageEstimate,
  ): RelayStorageStatus {
    const softLimitBytes = this.storagePolicy.softLimitBytes;
    const hardLimitBytes = this.storagePolicy.hardLimitBytes;
    const activeLimit = hardLimitBytes > 0 ? hardLimitBytes : softLimitBytes;
    return {
      ...estimate,
      softLimitBytes,
      hardLimitBytes,
      pressure: activeLimit > 0 ? estimate.bytes / activeLimit : 0,
      overSoftLimit: softLimitBytes > 0 && estimate.bytes >= softLimitBytes,
      overHardLimit: hardLimitBytes > 0 && estimate.bytes >= hardLimitBytes,
      unavailable: Boolean(estimate.error),
    };
  }

  private async refreshStorageStatus(
    force: boolean,
  ): Promise<RelayStorageStatus> {
    if (!this.store.estimateStorage) {
      this.storageStatus = this.emptyStorageStatus();
      return this.storageStatus;
    }
    const now = Date.now();
    const stale =
      now - (this.storageStatus.checkedAt || 0) >=
      this.storagePolicy.estimateIntervalMs;
    if (!force && !stale) {
      return this.storageStatus;
    }
    if (this.storageEstimatePromise) {
      return this.storageEstimatePromise;
    }

    this.storageEstimatePromise = Promise.resolve(this.store.estimateStorage())
      .then((estimate) => {
        this.storageStatus = this.storageStatusFromEstimate(estimate);
        this.storageUnestimatedWrittenBytes = 0;
        return this.storageStatus;
      })
      .catch((error: any) => {
        this.storageStatus = this.storageStatusFromEstimate({
          bytes: this.storageStatus.bytes,
          files: this.storageStatus.files,
          path: this.storageStatus.path,
          checkedAt: Date.now(),
          error: error?.message || String(error),
        });
        return this.storageStatus;
      })
      .finally(() => {
        this.storageEstimatePromise = undefined;
      });
    return this.storageEstimatePromise;
  }

  private estimatedPublishBytes(payload: unknown) {
    try {
      return Buffer.byteLength(JSON.stringify(payload), "utf8");
    } catch {
      return this.maxFrameBytes;
    }
  }

  private async checkStorageWriteGate(projectedBytes: number) {
    const hardLimitBytes = this.storagePolicy.hardLimitBytes;
    if (hardLimitBytes <= 0) {
      return { ok: true };
    }
    const storage = await this.refreshStorageStatus(false);
    const projectedTotal =
      storage.bytes +
      this.storageUnestimatedWrittenBytes +
      Math.max(0, projectedBytes);
    if (projectedTotal >= hardLimitBytes) {
      return {
        ok: false,
        storage,
        projectedTotal,
        message: `Relay storage limit reached (${storage.bytes}/${hardLimitBytes} bytes used; projected ${projectedTotal}).`,
      };
    }
    return { ok: true, storage, projectedTotal };
  }

  private renderPrometheusMetrics() {
    const lines: string[] = [];
    const metric = (
      name: string,
      value: number,
      help: string,
      type = "gauge",
    ) => {
      lines.push(`# HELP ${name} ${help}`);
      lines.push(`# TYPE ${name} ${type}`);
      lines.push(`${name} ${Number.isFinite(value) ? value : 0}`);
    };
    const currentStorageBytes =
      this.storageStatus.bytes + this.storageUnestimatedWrittenBytes;
    const currentActiveStorageLimit =
      this.storageStatus.hardLimitBytes > 0
        ? this.storageStatus.hardLimitBytes
        : this.storageStatus.softLimitBytes;
    metric(
      "cgp_relay_uptime_seconds",
      (Date.now() - this.startedAt) / 1000,
      "Relay process uptime in seconds.",
    );
    metric(
      "cgp_relay_websocket_clients",
      this.wss.clients.size,
      "Currently connected websocket clients.",
    );
    metric(
      "cgp_relay_subscriptions",
      this.countSubscriptions(),
      "Active client subscriptions.",
    );
    metric(
      "cgp_relay_fanout_queued_frames",
      this.countQueuedFanoutFrames(),
      "Queued fanout frames waiting for socket delivery.",
    );
    metric(
      "cgp_relay_pubsub_topics",
      this.pubSubUnsubscribers.size,
      "Active relay pubsub topic subscriptions.",
    );
    metric(
      "cgp_relay_pubsub_pending_guilds",
      this.pubSubPendingGuilds.size,
      "Guilds waiting for pubsub subscription setup.",
    );
    metric(
      "cgp_relay_hosted_guilds",
      this.hostedGuilds.size,
      "Guilds this relay has hosted or replicated.",
    );
    metric(
      "cgp_relay_state_cache_guilds",
      this.stateCache.size,
      "Guild states in the relay state cache.",
    );
    metric(
      "cgp_relay_observed_head_guilds",
      this.observedRelayHeads.size,
      "Guilds with observed relay-head gossip.",
    );
    metric(
      "cgp_relay_head_conflict_guilds",
      this.relayHeadConflicts.size,
      "Guilds with relay-head conflict evidence.",
    );
    metric(
      "cgp_relay_storage_used_bytes",
      currentStorageBytes,
      "Approximate bytes used by the relay store.",
    );
    metric(
      "cgp_relay_storage_soft_limit_bytes",
      this.storageStatus.softLimitBytes,
      "Relay store readiness soft limit in bytes.",
    );
    metric(
      "cgp_relay_storage_hard_limit_bytes",
      this.storageStatus.hardLimitBytes,
      "Relay store write hard limit in bytes.",
    );
    metric(
      "cgp_relay_storage_pressure_ratio",
      currentActiveStorageLimit > 0
        ? currentStorageBytes / currentActiveStorageLimit
        : 0,
      "Relay store pressure ratio against the active configured limit.",
    );
    metric(
      "cgp_relay_storage_over_soft_limit",
      this.storageStatus.softLimitBytes > 0 &&
        currentStorageBytes >= this.storageStatus.softLimitBytes
        ? 1
        : 0,
      "Whether relay store is past its readiness soft limit.",
    );
    metric(
      "cgp_relay_storage_over_hard_limit",
      this.storageStatus.hardLimitBytes > 0 &&
        currentStorageBytes >= this.storageStatus.hardLimitBytes
        ? 1
        : 0,
      "Whether relay store is past its write hard limit.",
    );
    metric(
      "cgp_relay_event_loop_delay_mean_seconds",
      this.eventLoopDelay.mean / 1e9,
      "Mean event loop delay in seconds.",
    );
    metric(
      "cgp_relay_event_loop_delay_p99_seconds",
      this.eventLoopDelay.percentile(99) / 1e9,
      "P99 event loop delay in seconds.",
    );
    return `${lines.join("\n")}\n`;
  }

  private async handlePluginHttp(
    req: IncomingMessage,
    res: ServerResponse,
    rawUrl: string,
    pathname: string,
  ) {
    if (!pathname.startsWith("/plugins/")) {
      return false;
    }

    let relPath = pathname.slice("/plugins/".length);
    try {
      relPath = decodeURIComponent(relPath);
    } catch {
      res.statusCode = 400;
      res.end("Bad request");
      return true;
    }

    relPath = relPath.replace(/^\/+/, "");
    const pathSegments = relPath.split("/").filter(Boolean);
    const pluginName = pathSegments[0] || "";
    const plugin = this.plugins.find(
      (candidate) => candidate.name === pluginName,
    );
    if (!plugin || !plugin.onHttp) {
      res.statusCode = 404;
      res.end("Plugin route not found");
      return true;
    }

    try {
      const handled = await plugin.onHttp(
        {
          req,
          res,
          rawUrl,
          pathname,
          pathSegments,
        },
        this.pluginCtx,
      );
      if (!handled && !res.writableEnded) {
        res.statusCode = 404;
        res.end("Plugin route not found");
      }
    } catch (e: any) {
      console.error(
        `Relay plugin ${plugin.name} failed handling HTTP route ${pathname}:`,
        e,
      );
      if (!res.writableEnded) {
        res.statusCode = 500;
        res.end("Internal plugin error");
      }
    }

    return true;
  }

  private async handleExtensionHttp(
    req: IncomingMessage,
    res: ServerResponse,
    pathname: string,
  ) {
    if (!pathname.startsWith("/extensions/")) {
      return false;
    }

    if (req.method === "OPTIONS") {
      this.setExtensionHeaders(res);
      res.statusCode = 204;
      res.end();
      return true;
    }

    if (req.method !== "GET" && req.method !== "HEAD") {
      this.setExtensionHeaders(res);
      res.statusCode = 405;
      res.end("Method not allowed");
      return true;
    }

    let relPath = pathname.slice("/extensions/".length);
    try {
      relPath = decodeURIComponent(relPath);
    } catch {
      this.setExtensionHeaders(res);
      res.statusCode = 400;
      res.end("Bad request");
      return true;
    }

    relPath = relPath.replace(/^\/+/, "");
    const segments = relPath.split("/").filter(Boolean);
    const extensionName = segments.shift() || "";
    const plugin = this.findExtensionPlugin(extensionName);
    if (!plugin || !plugin.staticDir) {
      this.setExtensionHeaders(res);
      res.statusCode = 404;
      res.end("Extension not available");
      return true;
    }

    const filePath = segments.length > 0 ? segments.join("/") : "index.js";
    const baseDir = path.resolve(plugin.staticDir);
    const resolved = path.resolve(baseDir, filePath);
    if (!resolved.startsWith(baseDir)) {
      this.setExtensionHeaders(res);
      res.statusCode = 403;
      res.end("Forbidden");
      return true;
    }

    let fileStat;
    try {
      fileStat = await stat(resolved);
    } catch {
      this.setExtensionHeaders(res);
      res.statusCode = 404;
      res.end("Not found");
      return true;
    }

    if (!fileStat.isFile()) {
      this.setExtensionHeaders(res);
      res.statusCode = 404;
      res.end("Not found");
      return true;
    }

    const contentType = this.getContentType(resolved);
    this.setExtensionHeaders(res, contentType, fileStat.size);

    if (req.method === "HEAD") {
      res.statusCode = 200;
      res.end();
      return true;
    }

    res.statusCode = 200;
    createReadStream(resolved).pipe(res);
    return true;
  }

  private async initPlugins() {
    for (const plugin of this.plugins) {
      try {
        await plugin.onInit?.(this.pluginCtx);
      } catch (e: any) {
        console.error(`Relay plugin ${plugin.name} failed to init:`, e);
      }
    }
  }

  private async publishAsRelay(
    body: EventBody,
    createdAt = Date.now(),
  ): Promise<GuildEvent | undefined> {
    const author = this.keyPair.publicKey;
    const unsignedForSig = { body, author, createdAt };
    const signature = await sign(
      this.keyPair.privateKey,
      hashObject(unsignedForSig),
    );
    const fullEvent = await this.appendSequencedEvent({
      body,
      author,
      signature,
      createdAt,
    });
    if (fullEvent) {
      await this.runOnEventAppended(fullEvent);
    }
    return fullEvent;
  }

  public async createCheckpoints() {
    const guilds = await this.store.getGuildIds();

    for (const guildId of guilds) {
      let rebuilt: {
        state: GuildState;
        endEvent: GuildEvent;
        checkpointEvent?: GuildEvent;
      } | null;
      try {
        rebuilt = await this.rebuildGuildState(guildId);
      } catch (e) {
        console.error(
          `Failed to rebuild state for guild ${guildId} during checkpoint:`,
          e,
        );
        continue;
      }
      if (!rebuilt) continue;

      // Create checkpoint event
      const { state, endEvent: lastEvent } = rebuilt;
      if (lastEvent.body.type === "CHECKPOINT") {
        continue;
      }

      const serializedState = serializeBoundedCheckpointState(
        state,
        this.checkpointMessageRefLimit,
      );
      const stateHash = hashObject(serializedState);

      const body: Checkpoint = {
        type: "CHECKPOINT",
        guildId,
        rootHash: stateHash,
        seq: lastEvent.seq + 1,
        state: serializedState,
      };

      const fullEvent: GuildEvent = {
        id: "",
        seq: lastEvent.seq + 1,
        prevHash: lastEvent.id,
        createdAt: Date.now(),
        author: this.keyPair.publicKey,
        body,
        signature: "",
      };

      fullEvent.id = computeEventId(fullEvent);
      fullEvent.signature = await sign(
        this.keyPair.privateKey,
        hashObject({
          body,
          author: fullEvent.author,
          createdAt: fullEvent.createdAt,
        }),
      );

      await this.store.append(guildId, fullEvent);
      this.storageUnestimatedWrittenBytes +=
        this.estimatedPublishBytes(fullEvent);
      this.ensurePubSubHostedGuildReplication(guildId);
      this.cacheGuildState(guildId, applyEvent(state, fullEvent), fullEvent);
      if (this.verboseLogging) {
        console.log(
          `Relay created checkpoint for guild ${guildId} at seq ${fullEvent.seq}`,
        );
      }
      this.broadcast(guildId, fullEvent);
    }
  }

  public async prune() {
    const guilds = await this.store.getGuildIds();
    const now = Date.now();

    for (const guildId of guilds) {
      let rebuilt: {
        state: GuildState;
        endEvent: GuildEvent;
        checkpointEvent?: GuildEvent;
      } | null;
      try {
        rebuilt = await this.rebuildGuildState(guildId);
      } catch (e) {
        console.error(
          `Failed to rebuild state for guild ${guildId} during prune:`,
          e,
        );
        continue;
      }
      if (!rebuilt) continue;

      const ttlChannels = new Map<ChannelId, number>();
      for (const [channelId, channel] of rebuilt.state.channels) {
        if (channel.retention?.mode === "ttl" && channel.retention.seconds) {
          ttlChannels.set(channelId, channel.retention.seconds);
        }
      }
      if (ttlChannels.size === 0) {
        continue;
      }

      const events = await this.store.getLog(guildId);
      if (events.length === 0) continue;

      const seqsToDelete: number[] = [];
      for (const event of events) {
        if (event.body.type === "MESSAGE") {
          const body = event.body as Message;
          const ttlSeconds = ttlChannels.get(body.channelId);
          if (ttlSeconds) {
            const ageSeconds = (now - event.createdAt) / 1000;
            if (ageSeconds > ttlSeconds) {
              seqsToDelete.push(event.seq);
            }
          }
        }
      }

      if (seqsToDelete.length > 0) {
        if (this.store.deleteEvents) {
          await this.store.deleteEvents(guildId, seqsToDelete);
        } else {
          for (const seq of seqsToDelete) {
            await this.store.deleteEvent(guildId, seq);
          }
        }
        this.stateCache.delete(guildId);
        this.checkpointCache.delete(guildId);
        await this.refreshStorageStatus(true);
      }
    }
  }

  private async handleMessage(
    socket: WebSocket,
    kind: string,
    payload: unknown,
  ) {
    if (this.closing) {
      return;
    }
    if (this.verboseLogging) {
      console.log(`[RelayServer] handleMessage: ${kind}`);
    }
    await this.pluginsReady;
    if (this.closing) {
      return;
    }

    for (const plugin of this.plugins) {
      try {
        const handled = await plugin.onFrame?.(
          { socket, kind, payload },
          this.pluginCtx,
        );
        if (handled) return;
      } catch (e: any) {
        console.error(
          `Relay plugin ${plugin.name} failed handling frame ${kind}:`,
          e,
        );
        this.sendFrame(socket, "ERROR", {
          code: "PLUGIN_ERROR",
          message: "Plugin frame handler failed",
        });
        return;
      }
    }

    if (kind === "HELLO") {
      const helloPayload = objectPayload(payload);
      const negotiatedWireFormat = wireFormatFromValue(helloPayload.wireFormat);
      if (negotiatedWireFormat) {
        this.socketWireFormats.set(socket, negotiatedWireFormat);
      }
      const pluginList = this.plugins.map((p) => ({
        name: p.name,
        metadata: p.metadata,
        inputs: p.inputs,
      }));
      this.sendFrame(socket, "HELLO_OK", {
        protocol: "cgp/0.1",
        relayName: "Reference Relay",
        relayId: this.relayInstanceId,
        relayPublicKey: this.keyPair.publicKey,
        plugins: pluginList,
        wireFormat: this.socketWireFormats.get(socket) ?? this.wireFormat,
        supportedWireFormats: ["json", "binary-json", "binary-v1", "binary-v2"],
      });
    } else if (kind === "PLUGIN_CONFIG") {
      const p = objectPayload(payload) as { pluginName: string; config: any };
      const plugin = this.plugins.find((pl) => pl.name === p.pluginName);
      if (plugin) {
        try {
          await plugin.onConfig?.({ socket, config: p.config }, this.pluginCtx);
          this.sendFrame(socket, "PLUGIN_CONFIG_OK", {
            pluginName: p.pluginName,
          });
        } catch (e: any) {
          console.error(
            `Relay plugin ${p.pluginName} configuration failed:`,
            e,
          );
          this.sendFrame(socket, "ERROR", {
            code: "PLUGIN_CONFIG_ERROR",
            pluginName: p.pluginName,
            message: e?.message || "Plugin configuration failed",
          });
        }
      } else {
        this.sendFrame(socket, "ERROR", {
          code: "PLUGIN_NOT_FOUND",
          pluginName: p.pluginName,
          message: `Plugin ${p.pluginName} not found`,
        });
      }
    } else if (kind === "SUB") {
      const p = objectPayload(payload) as {
        subId?: string;
        guildId?: GuildId;
        channels?: ChannelId[];
      } & SignedReadRequest;
      const subId =
        typeof p.subId === "string" && p.subId.trim()
          ? p.subId
          : `sub-${Date.now()}`;
      const guildId = typeof p.guildId === "string" ? p.guildId : "";
      const channels = Array.isArray(p.channels)
        ? p.channels.filter(
            (channelId): channelId is ChannelId =>
              typeof channelId === "string" && channelId.trim().length > 0,
          )
        : undefined;
      if (!guildId.trim()) {
        this.sendRequestError(
          socket,
          "VALIDATION_FAILED",
          "SUB requires a guildId",
          { subId },
        );
        return;
      }

      await this.withGuildMutex(guildId, async () => {
        const author = await this.readAuthorOrError(
          socket,
          "SUB",
          payload,
          subId,
          guildId,
        );
        if (author === null) return;

        const rebuilt = await this.rebuildGuildState(guildId);
        if (rebuilt && !canReadGuild(rebuilt.state, author)) {
          this.sendRequestError(
            socket,
            "FORBIDDEN",
            "You do not have permission to subscribe to this guild",
            { subId, guildId },
          );
          return;
        }

        const rawSnapshotEvents = await this.readReplaySnapshotEvents(guildId);
        const snapshotEvents = rebuilt
          ? filterEventsForReader(
              rebuilt.state,
              rawSnapshotEvents,
              author,
            ).filter((event) =>
              eventMatchesRequestedChannels(rebuilt.state, event, channels),
            )
          : rawSnapshotEvents;
        this.addSubscription(socket, subId, { guildId, channels, author });
        const tailEvent =
          rebuilt?.endEvent ??
          (snapshotEvents.length > 0
            ? snapshotEvents[snapshotEvents.length - 1]
            : null);
        const oldestSeq =
          snapshotEvents.length > 0 ? snapshotEvents[0].seq : null;
        const newestSeq =
          snapshotEvents.length > 0
            ? snapshotEvents[snapshotEvents.length - 1].seq
            : null;
        const checkpointEvent = snapshotEvents.find(
          (event) => event.body.type === "CHECKPOINT",
        );
        this.sendFrame(socket, "SNAPSHOT", {
          subId,
          guildId,
          events: snapshotEvents,
          endSeq: tailEvent?.seq ?? -1,
          endHash: tailEvent?.id ?? null,
          oldestSeq,
          newestSeq,
          hasMore: oldestSeq !== null && oldestSeq > 0,
          checkpointSeq: checkpointEvent?.seq ?? null,
          checkpointHash: checkpointEvent?.id ?? null,
        });
      });
    } else if (kind === "GET_HISTORY") {
      await this.handleHistoryRequest(socket, objectPayload(payload));
    } else if (kind === "GET_LOG_RANGE") {
      await this.handleLogRangeRequest(socket, objectPayload(payload));
    } else if (kind === "GET_STATE") {
      await this.handleStateRequest(socket, objectPayload(payload));
    } else if (kind === "GET_HEAD") {
      await this.handleRelayHeadRequest(socket, objectPayload(payload));
    } else if (kind === "GET_HEADS") {
      await this.handleRelayHeadsRequest(socket, objectPayload(payload));
    } else if (kind === "SEARCH") {
      await this.handleSearchRequest(socket, objectPayload(payload));
    } else if (kind === "PUBLISH") {
      const p = objectPayload(payload) as PublishPayload;
      const { body, author, signature, createdAt, clientEventId } = p;

      const fullEvent = await this.appendSequencedEvent(
        { body, author, signature, createdAt, clientEventId },
        socket,
      );
      if (fullEvent) {
        await this.runOnEventAppended(fullEvent, socket);
      }
    } else if (kind === "PUBLISH_BATCH") {
      const p = objectPayload(payload) as {
        batchId?: string;
        events?: PublishPayload[];
      };
      const batchId = typeof p.batchId === "string" ? p.batchId : undefined;
      const inputEvents = Array.isArray(p.events) ? p.events : [];
      const eventCount = Math.min(inputEvents.length, this.maxPublishBatchSize);
      if (eventCount === 0) {
        this.sendRequestError(
          socket,
          "VALIDATION_FAILED",
          "PUBLISH_BATCH requires events",
          { batchId },
        );
        return;
      }
      const indexedEvents: IndexedPublishPayload[] = new Array(eventCount);
      for (let index = 0; index < eventCount; index += 1) {
        indexedEvents[index] = { ...inputEvents[index], index };
      }

      const batchResults = await this.appendSequencedEventsBatch(
        indexedEvents,
        socket,
        batchId,
      );
      const appendedEvents = batchResults
        .map((result) => result.event)
        .filter((event): event is GuildEvent => Boolean(event));
      const results = batchResults
        .sort((left, right) => left.index - right.index)
        .map((result) => result.ack);
      this.sendFrame(socket, "PUB_BATCH_ACK", {
        batchId,
        results,
        truncated: inputEvents.length > eventCount,
      });
      void this.runOnEventsAppended(appendedEvents, socket).catch((e) => {
        console.error("Relay append hooks failed:", e);
      });
    } else if (kind === "GET_MEMBERS") {
      const p = objectPayload(payload) as MembersRequest;
      const subId =
        typeof p.subId === "string" && p.subId.trim()
          ? p.subId
          : `members-${Date.now()}`;
      const guildId = typeof p.guildId === "string" ? p.guildId : "";
      if (!guildId.trim()) {
        this.sendRequestError(
          socket,
          "VALIDATION_FAILED",
          "GET_MEMBERS requires a guildId",
          { subId },
        );
        return;
      }

      await this.withGuildMutex(guildId, async () => {
        const author = await this.readAuthorOrError(
          socket,
          "GET_MEMBERS",
          payload,
          subId,
          guildId,
        );
        if (author === null) return;

        const rebuilt = await this.rebuildGuildState(guildId);
        if (!rebuilt) {
          this.sendRequestError(
            socket,
            "NOT_FOUND",
            "Guild members not found",
            { subId, guildId },
          );
          return;
        }

        if (!canReadGuild(rebuilt.state, author)) {
          this.sendRequestError(
            socket,
            "FORBIDDEN",
            "You do not have permission to read this guild's members",
            { subId, guildId },
          );
          return;
        }

        for (const plugin of this.plugins) {
          try {
            const members = await plugin.onGetMembers?.(
              { guildId, author, socket },
              this.pluginCtx,
            );
            if (members) {
              this.sendFrame(socket, "MEMBERS", {
                subId,
                guildId,
                members,
                nextCursor: null,
                hasMore: false,
              });
              return;
            }
          } catch (e: any) {
            console.error(
              `Relay plugin ${plugin.name} failed onGetMembers:`,
              e,
            );
          }
        }

        const limit = normalizeMemberPageLimit(p.limit);
        const afterUserId =
          typeof p.afterUserId === "string" && p.afterUserId.trim()
            ? p.afterUserId
            : undefined;
        if (this.store.getMembersPage) {
          const page = await this.store.getMembersPage({
            guildId,
            afterUserId,
            limit,
          });
          this.sendFrame(socket, "MEMBERS", { subId, guildId, ...page });
          return;
        }

        const members = serializeState(rebuilt.state)
          .members.map(([, member]) => member)
          .sort((left, right) => left.userId.localeCompare(right.userId));
        const startIndex = afterUserId
          ? members.findIndex((member) => member.userId > afterUserId)
          : 0;
        const pageStart = startIndex >= 0 ? startIndex : members.length;
        const page = members.slice(pageStart, pageStart + limit + 1);
        const hasMore = page.length > limit;
        const visible = hasMore ? page.slice(0, limit) : page;
        this.sendFrame(socket, "MEMBERS", {
          subId,
          guildId,
          members: visible,
          nextCursor:
            hasMore && visible.length > 0
              ? visible[visible.length - 1].userId
              : null,
          hasMore,
          totalApprox: members.length,
        });
      });
    } else {
      this.sendFrame(socket, "ERROR", {
        code: "UNKNOWN_FRAME",
        message: `Unsupported frame kind: ${kind}`,
      });
    }
  }

  private async rebuildGuildState(
    guildId: GuildId,
  ): Promise<{
    state: GuildState;
    endEvent: GuildEvent;
    checkpointEvent?: GuildEvent;
  } | null> {
    if (this.closing) {
      return null;
    }
    const endEvent = await this.store.getLastEvent(guildId);
    if (!endEvent) {
      this.stateCache.delete(guildId);
      this.checkpointCache.delete(guildId);
      return null;
    }

    const cachedState = this.stateCache.get(guildId);
    if (cachedState?.headSeq === endEvent.seq) {
      if (
        endEvent.body.type === "CHECKPOINT" &&
        isValidCheckpointEvent(endEvent)
      ) {
        this.checkpointCache.set(guildId, endEvent);
      }
      this.cacheGuildState(
        guildId,
        cachedState,
        this.checkpointCache.get(guildId),
      );
      return {
        state: cachedState,
        endEvent,
        checkpointEvent: this.checkpointCache.get(guildId),
      };
    }

    let events = this.store.getReplaySnapshotEvents
      ? buildReplaySnapshotEvents(
          guildId,
          await this.store.getReplaySnapshotEvents({
            guildId,
            limit: this.snapshotEventLimit,
          }),
        )
      : await this.store.getLog(guildId);
    const canReplayFromFirstEvent =
      events[0]?.seq === 0 ||
      (events[0]?.body.type === "CHECKPOINT" &&
        isValidCheckpointEvent(events[0]));
    if (!canReplayFromFirstEvent) {
      events = await this.store.getLog(guildId);
    }
    if (events.length === 0) {
      return null;
    }

    const { state, checkpointEvent } = rebuildStateFromEvents(events);
    this.cacheGuildState(guildId, state, checkpointEvent);

    return {
      state,
      endEvent: events[events.length - 1],
      checkpointEvent,
    };
  }

  private verifyReadRequest(kind: string, payload: unknown) {
    const p = payload as SignedReadRequest;
    const author =
      typeof p?.author === "string" && p.author.trim() ? p.author : undefined;
    const signature =
      typeof p?.signature === "string" && p.signature.trim()
        ? p.signature
        : undefined;
    const createdAt = Number(p?.createdAt);

    if (!author && !signature && !Number.isFinite(createdAt)) {
      return undefined;
    }

    if (!author || !signature || !Number.isFinite(createdAt)) {
      throw new Error(
        "Signed read request requires author, createdAt, and signature",
      );
    }

    const maxSkewMs = 5 * 60 * 1000;
    if (Math.abs(Date.now() - createdAt) > maxSkewMs) {
      throw new Error("Signed read request is outside the allowed clock skew");
    }

    const unsignedPayload = stripReadSignature(payload);
    const ok = verify(
      author,
      hashObject({ kind, payload: unsignedPayload }),
      signature,
    );
    if (!ok) {
      throw new Error("Invalid signed read request");
    }

    return author;
  }

  private async signRelayHead(
    guildId: GuildId,
    rebuilt?: { endEvent: GuildEvent; checkpointEvent?: GuildEvent },
  ): Promise<RelayHead | null> {
    const endEvent =
      rebuilt?.endEvent ?? (await this.store.getLastEvent(guildId));
    if (!endEvent) {
      return null;
    }
    const checkpointEvent =
      rebuilt?.checkpointEvent ?? this.checkpointCache.get(guildId);
    const unsigned: RelayHeadUnsigned = {
      protocol: "cgp/0.1",
      relayId: this.relayInstanceId,
      relayPublicKey: this.keyPair.publicKey,
      guildId,
      headSeq: endEvent.seq,
      headHash: endEvent.id,
      prevHash: endEvent.prevHash,
      checkpointSeq: checkpointEvent?.seq ?? null,
      checkpointHash: checkpointEvent?.id ?? null,
      observedAt: Date.now(),
    };
    return {
      ...unsigned,
      signature: await sign(this.keyPair.privateKey, relayHeadId(unsigned)),
    };
  }

  private relayHeadObservationKey(head: RelayHead) {
    return `${head.relayPublicKey}:${head.relayId}`;
  }

  private recordRelayHeadObservation(
    head: RelayHead,
  ): RelayHeadQuorum | undefined {
    if (!verifyRelayHead(head)) {
      return undefined;
    }
    const guildId = head.guildId;
    const observations =
      this.observedRelayHeads.get(guildId) ?? new Map<string, RelayHead>();
    const key = this.relayHeadObservationKey(head);
    const previous = observations.get(key);
    observations.set(key, head);
    this.observedRelayHeads.set(guildId, observations);

    const heads = [...observations.values()];
    if (
      previous &&
      previous.headSeq === head.headSeq &&
      previous.headHash !== head.headHash
    ) {
      heads.push(previous);
    }
    const conflicts = findRelayHeadConflicts(heads);
    this.relayHeadConflicts.set(guildId, conflicts);
    return summarizeRelayHeadQuorum(guildId, heads);
  }

  private async observedHeadsForGuild(
    guildId: GuildId,
    ownHead?: RelayHead | null,
  ) {
    const heads = [...(this.observedRelayHeads.get(guildId)?.values() ?? [])];
    const selfHead = ownHead ?? (await this.signRelayHead(guildId));
    if (selfHead) {
      const key = this.relayHeadObservationKey(selfHead);
      const existingIndex = heads.findIndex(
        (head) => this.relayHeadObservationKey(head) === key,
      );
      if (existingIndex >= 0) {
        heads[existingIndex] = selfHead;
      } else {
        heads.push(selfHead);
      }
    }
    return heads;
  }

  private async publishRelayHeadGossip(guildId: GuildId) {
    if (!this.pubSubAdapter) {
      return;
    }
    const head = await this.signRelayHead(guildId);
    if (!head) {
      return;
    }
    this.recordRelayHeadObservation(head);
    this.ensurePubSubHeadSubscription(guildId);
    const frame = JSON.stringify(["RELAY_HEAD_GOSSIP", { guildId, head }]);
    void Promise.resolve(
      this.pubSubAdapter.publish(this.pubSubHeadTopic(guildId), {
        originId: this.relayInstanceId,
        guildId,
        head,
        frame,
      }),
    ).catch((e) => {
      console.error(
        `Relay head gossip publish failed for guild ${guildId}:`,
        e,
      );
    });
  }

  private sendRequestError(
    socket: WebSocket,
    code: string,
    message: string,
    extra?: Record<string, unknown>,
  ) {
    this.sendFrame(socket, "ERROR", { code, message, ...(extra ?? {}) });
  }

  private clientSafeError(code: string) {
    if (code === "AUTH_FAILED") return "Authentication failed";
    if (code === "VALIDATION_FAILED") return "Event validation failed";
    return "Request failed";
  }

  private clientSafeValidationError(message: unknown) {
    const text = typeof message === "string" ? message : "";
    if (/sendMessages/i.test(text)) return "permission denied: sendMessages";
    if (/permission/i.test(text)) return "permission denied";
    if (text) return text;
    return this.clientSafeError("VALIDATION_FAILED");
  }

  private pubSubTopic(guildId: GuildId) {
    return `guild:${guildId}`;
  }

  private pubSubLogTopic(guildId: GuildId) {
    return `guild:${guildId}:log`;
  }

  private pubSubHeadTopic(guildId: GuildId) {
    return `guild:${guildId}:heads`;
  }

  private pubSubAllChannelsTopic(guildId: GuildId) {
    return `guild:${guildId}:channels`;
  }

  private pubSubChannelTopic(guildId: GuildId, channelId: ChannelId) {
    return `guild:${guildId}:channel:${encodeURIComponent(channelId)}`;
  }

  private async pubSubReplayOptions(
    guildId: GuildId,
  ): Promise<RelayPubSubSubscribeOptions> {
    const lastEvent = await this.store.getLastEvent(guildId);
    return {
      afterSeq: lastEvent?.seq ?? -1,
      replayLimit: this.pubSubReplayLimit,
    };
  }

  private async bootstrapPubSubHostedGuilds() {
    if (!this.pubSubAdapter || this.closing) {
      return;
    }

    const guildIds = await this.store.getGuildIds();
    for (const guildId of guildIds) {
      if (this.closing) {
        return;
      }
      const lastEvent = await this.store.getLastEvent(guildId);
      if (!lastEvent) {
        continue;
      }
      this.ensurePubSubHostedGuildReplication(guildId);
      await this.publishRelayHeadGossip(guildId);
    }
  }

  private async requestPubSubLogReplay(guildId: GuildId, targetSeq?: number) {
    if (!this.pubSubAdapter || this.closing) {
      return;
    }
    const lastEvent = await this.store.getLastEvent(guildId);
    const afterSeq = lastEvent?.seq ?? -1;
    if (typeof targetSeq === "number" && afterSeq >= targetSeq) {
      return;
    }

    const catchupKey = guildId;
    if (this.pubSubCatchupKeys.has(catchupKey)) {
      return;
    }

    this.pubSubCatchupKeys.add(catchupKey);
    const topic = this.pubSubLogTopic(guildId);
    const handler = (envelope: RelayPubSubEnvelope) =>
      this.handlePubSubReplicationEnvelope(guildId, envelope);
    let unsubscribe: (() => Promise<void> | void) | undefined;
    let lastObservedSeq = afterSeq;
    let idleSince = Date.now();
    const startedAt = idleSince;
    let finished = false;

    try {
      unsubscribe = await Promise.resolve(
        this.pubSubAdapter.subscribe(topic, handler, {
          afterSeq,
          replayLimit: this.pubSubReplayLimit,
        }),
      );
    } catch (e) {
      this.pubSubCatchupKeys.delete(catchupKey);
      throw e;
    }

    const finish = () => {
      if (finished) {
        return;
      }
      finished = true;
      this.pubSubCatchupKeys.delete(catchupKey);
      if (unsubscribe) {
        void Promise.resolve(unsubscribe()).catch((e) => {
          console.error(
            `Relay pubsub replay unsubscribe failed for guild ${guildId}:`,
            e,
          );
        });
      }
    };
    let checking = false;
    const timer = setInterval(() => {
      if (checking || finished) {
        return;
      }
      if (this.closing) {
        clearInterval(timer);
        finish();
        return;
      }
      checking = true;
      void (async () => {
        try {
          if (this.closing) {
            clearInterval(timer);
            finish();
            return;
          }
          const currentSeq =
            (await this.store.getLastEvent(guildId))?.seq ?? -1;
          if (currentSeq > lastObservedSeq) {
            lastObservedSeq = currentSeq;
            idleSince = Date.now();
          }
          const reachedTarget =
            typeof targetSeq === "number" && currentSeq >= targetSeq;
          const idleTimedOut = Date.now() - idleSince >= 5000;
          const maxTimedOut = Date.now() - startedAt >= 30000;
          if (reachedTarget || idleTimedOut || maxTimedOut || this.closing) {
            clearInterval(timer);
            finish();
          }
        } catch (error) {
          if (this.closing || isStoreClosedError(error)) {
            clearInterval(timer);
            finish();
            return;
          }
          console.error(
            `Relay pubsub catch-up monitor failed for guild ${guildId}:`,
            error,
          );
          clearInterval(timer);
          finish();
        } finally {
          checking = false;
        }
      })();
    }, 250);
    timer.unref?.();
  }

  private async requestPeerLogRange(
    peerUrl: string,
    guildId: GuildId,
    afterSeq: number,
    limit: number,
  ): Promise<{
    events: GuildEvent[];
    hasMore: boolean;
    nextAfterSeq: number;
    endSeq?: number;
    endHash?: string | null;
  }> {
    return await new Promise((resolve, reject) => {
      const socket = new WebSocket(peerUrl, { perMessageDeflate: false });
      const subId = `peer-catchup-${this.relayInstanceId}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
      const timeout = setTimeout(() => {
        socket.close();
        reject(new Error(`Timed out fetching peer log range from ${peerUrl}`));
      }, 15000);

      socket.once("open", async () => {
        try {
          const payload = await this.signedRelayReadPayload("GET_LOG_RANGE", {
            subId,
            guildId,
            afterSeq,
            limit,
          });
          socket.send(JSON.stringify(["GET_LOG_RANGE", payload]));
        } catch (error) {
          clearTimeout(timeout);
          socket.close();
          reject(error);
        }
      });
      socket.once("error", (error) => {
        clearTimeout(timeout);
        socket.close();
        reject(error);
      });
      socket.on("message", (raw) => {
        try {
          const { kind, payload } = parseCgpWireData(raw, {
            includeRawFrame: false,
          }) as { kind: string; payload: any };
          if (payload?.subId !== subId) {
            return;
          }
          clearTimeout(timeout);
          socket.close();
          if (kind === "ERROR") {
            reject(
              new Error(
                payload?.code ||
                  payload?.message ||
                  "Peer log range request failed",
              ),
            );
            return;
          }
          if (kind !== "LOG_RANGE") {
            reject(new Error(`Unexpected peer catch-up frame ${kind}`));
            return;
          }
          const events = Array.isArray(payload.events)
            ? (payload.events as GuildEvent[])
            : [];
          const nextAfterSeq = Number.isFinite(payload.nextAfterSeq)
            ? Number(payload.nextAfterSeq)
            : afterSeq;
          const endSeq = Number.isFinite(payload.endSeq)
            ? Number(payload.endSeq)
            : undefined;
          const endHash =
            typeof payload.endHash === "string"
              ? payload.endHash
              : payload.endHash === null
                ? null
                : undefined;
          resolve({
            events,
            hasMore: payload.hasMore === true,
            nextAfterSeq,
            endSeq,
            endHash,
          });
        } catch (error) {
          clearTimeout(timeout);
          socket.close();
          reject(error);
        }
      });
    });
  }

  private async requestPeerRelayHead(
    peerUrl: string,
    guildId: GuildId,
  ): Promise<{ peerUrl: string; head?: RelayHead; error?: string }> {
    return await new Promise((resolve) => {
      const socket = new WebSocket(peerUrl, { perMessageDeflate: false });
      const subId = `peer-head-${this.relayInstanceId}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
      const timeout = setTimeout(() => {
        socket.close();
        resolve({ peerUrl, error: "timeout" });
      }, 10000);

      socket.once("open", async () => {
        try {
          const payload = await this.signedRelayReadPayload("GET_HEAD", {
            subId,
            guildId,
          });
          socket.send(JSON.stringify(["GET_HEAD", payload]));
        } catch (error: any) {
          clearTimeout(timeout);
          socket.close();
          resolve({ peerUrl, error: error?.message || String(error) });
        }
      });
      socket.once("error", (error) => {
        clearTimeout(timeout);
        socket.close();
        resolve({
          peerUrl,
          error: error instanceof Error ? error.message : String(error),
        });
      });
      socket.on("message", (raw) => {
        try {
          const { kind, payload } = parseCgpWireData(raw, {
            includeRawFrame: false,
          }) as { kind: string; payload: any };
          if (payload?.subId !== subId) return;
          clearTimeout(timeout);
          socket.close();
          if (kind === "RELAY_HEAD" && payload.head) {
            resolve({ peerUrl, head: payload.head });
          } else if (kind === "ERROR") {
            resolve({
              peerUrl,
              error:
                payload?.code || payload?.message || "peer head request failed",
            });
          } else {
            resolve({ peerUrl, error: `unexpected peer head frame ${kind}` });
          }
        } catch (error: any) {
          clearTimeout(timeout);
          socket.close();
          resolve({ peerUrl, error: error?.message || String(error) });
        }
      });
    });
  }

  private async peerCanonicalCatchupSources(guildId: GuildId) {
    const ownHead = await this.signRelayHead(guildId);
    if (ownHead) {
      this.recordRelayHeadObservation(ownHead);
    }

    const peerHeadResults = await Promise.all(
      this.peerUrls.map((peerUrl) =>
        this.requestPeerRelayHead(peerUrl, guildId),
      ),
    );
    for (const result of peerHeadResults) {
      if (result.head) {
        this.recordRelayHeadObservation(result.head);
      }
    }

    const heads = [
      ...(ownHead ? [ownHead] : []),
      ...peerHeadResults.flatMap((result) =>
        result.head ? [result.head] : [],
      ),
    ];
    if (heads.length === 0) {
      return [];
    }

    const minCanonicalCount = Math.min(
      this.peerCatchupMinCanonicalHeads,
      heads.length,
    );
    const summarized = summarizeRelayHeadQuorum(guildId, heads);
    if (summarized.conflicts.length > 0) {
      if (this.verboseLogging) {
        console.warn(
          `Relay peer catch-up refused conflicting heads for guild ${guildId}`,
        );
      }
      return [];
    }

    let quorum: RelayHeadQuorum | undefined;
    try {
      quorum = assertRelayHeadQuorum(guildId, heads, {
        minValidHeads: minCanonicalCount,
        minCanonicalCount,
        requireNoConflicts: true,
      });
    } catch (error: any) {
      if (this.verboseLogging) {
        console.warn(
          `Relay peer catch-up quorum failed for guild ${guildId}: ${error?.message || error}`,
        );
      }
    }

    const aheadSources = peerHeadResults
      .filter(
        (result) =>
          result.head && (!ownHead || result.head.headSeq > ownHead.headSeq),
      )
      .sort(
        (left, right) =>
          (right.head?.headSeq ?? -1) - (left.head?.headSeq ?? -1),
      )
      .map((result) => ({ peerUrl: result.peerUrl, head: result.head! }));
    const canonical = quorum?.canonical;
    if (canonical && (!ownHead || canonical.seq > ownHead.headSeq)) {
      return peerHeadResults
        .filter(
          (result) =>
            result.head &&
            result.head.headSeq === canonical.seq &&
            result.head.headHash === canonical.hash,
        )
        .map((result) => ({ peerUrl: result.peerUrl, head: result.head! }));
    }

    return aheadSources;
  }

  private async requestPeerLogCatchup(guildId: GuildId, targetSeq?: number) {
    if (this.closing || this.peerUrls.length === 0) {
      return;
    }

    let initialHead: GuildEvent | undefined;
    try {
      initialHead = await this.store.getLastEvent(guildId);
    } catch (error) {
      if (this.closing || isStoreClosedError(error)) {
        return;
      }
      throw error;
    }
    const initialAfterSeq = initialHead?.seq ?? -1;
    if (typeof targetSeq === "number" && initialAfterSeq >= targetSeq) {
      return;
    }

    const noSourceUntil = this.peerCatchupNoSourceUntil.get(guildId) ?? 0;
    if (Date.now() < noSourceUntil) {
      return;
    }

    const catchupKey = guildId;
    if (this.peerCatchupKeys.has(catchupKey)) {
      return;
    }
    this.peerCatchupKeys.add(catchupKey);

    let madeProgress = false;
    try {
      const pageLimit = Math.min(
        Math.max(this.maxPublishBatchSize * 4, 1000),
        5000,
      );
      let progressed = true;
      let rounds = 0;
      while (!this.closing && progressed && rounds < 64) {
        rounds += 1;
        progressed = false;
        const sources = await this.peerCanonicalCatchupSources(guildId);
        if (sources.length === 0) {
          if (!madeProgress) {
            this.peerCatchupNoSourceUntil.set(
              guildId,
              Date.now() + this.peerCatchupNoSourceCooldownMs,
            );
          }
          return;
        }
        for (const source of sources) {
          if (this.closing) {
            return;
          }
          const currentHead = await Promise.resolve(
            this.store.getLastEvent(guildId),
          ).catch((error: unknown) => {
            if (this.closing || isStoreClosedError(error)) {
              return undefined;
            }
            throw error;
          });
          if (this.closing) {
            return;
          }
          const afterSeq = currentHead?.seq ?? -1;
          if (typeof targetSeq === "number" && afterSeq >= targetSeq) {
            return;
          }
          if (source.head.headSeq <= afterSeq) {
            continue;
          }

          let page: {
            events: GuildEvent[];
            hasMore: boolean;
            nextAfterSeq: number;
            endSeq?: number;
            endHash?: string | null;
          };
          try {
            page = await this.requestPeerLogRange(
              source.peerUrl,
              guildId,
              afterSeq,
              pageLimit,
            );
          } catch {
            continue;
          }
          if (
            page.endSeq !== undefined &&
            source.head.headSeq === page.endSeq &&
            page.endHash !== undefined &&
            source.head.headHash !== page.endHash
          ) {
            if (this.verboseLogging) {
              console.warn(
                `Ignoring peer catch-up range whose end hash does not match signed head for guild ${guildId}`,
              );
            }
            continue;
          }
          if (page.events.length === 0) {
            continue;
          }

          const beforeSeq =
            (
              await Promise.resolve(this.store.getLastEvent(guildId)).catch(
                (error: unknown) => {
                  if (this.closing || isStoreClosedError(error)) {
                    return undefined;
                  }
                  throw error;
                },
              )
            )?.seq ?? -1;
          if (this.closing) {
            return;
          }
          const accepted = await this.replicatePubSubEvents(
            guildId,
            page.events,
          );
          const afterAcceptedSeq =
            (
              await Promise.resolve(this.store.getLastEvent(guildId)).catch(
                (error: unknown) => {
                  if (this.closing || isStoreClosedError(error)) {
                    return undefined;
                  }
                  throw error;
                },
              )
            )?.seq ?? beforeSeq;
          if (this.closing) {
            return;
          }
          progressed =
            progressed || accepted.length > 0 || afterAcceptedSeq > beforeSeq;
          madeProgress =
            madeProgress || accepted.length > 0 || afterAcceptedSeq > beforeSeq;
          if (madeProgress) {
            this.peerCatchupNoSourceUntil.delete(guildId);
          }

          if (typeof targetSeq === "number" && afterAcceptedSeq >= targetSeq) {
            return;
          }
          if (page.endSeq !== undefined && afterAcceptedSeq >= page.endSeq) {
            continue;
          }
        }
      }
    } finally {
      if (!madeProgress) {
        this.peerCatchupNoSourceUntil.set(
          guildId,
          Date.now() + this.peerCatchupNoSourceCooldownMs,
        );
      }
      this.peerCatchupKeys.delete(catchupKey);
    }
  }

  private async runPeriodicPeerCatchup() {
    if (
      this.closing ||
      this.peerUrls.length === 0 ||
      this.hostedGuilds.size === 0
    ) {
      return;
    }
    for (const guildId of this.hostedGuilds) {
      await this.requestPeerLogCatchup(guildId).catch(() => undefined);
    }
  }

  private deliverPubSubEvents(
    guildId: GuildId,
    deliverable: GuildEvent[],
    sourceLength: number,
    frame?: string,
  ) {
    if (deliverable.length === 0) {
      return;
    }
    this.rememberRecentDeliveredPubSubEvents(guildId, deliverable);
    if (deliverable.length === 1) {
      this.deliverBroadcastFrame(
        guildId,
        deliverable[0],
        deliverable.length === sourceLength ? frame : undefined,
      );
    } else {
      this.deliverBroadcastBatchFrame(
        guildId,
        deliverable,
        deliverable.length === sourceLength ? frame : undefined,
      );
    }
  }

  private async handlePubSubEnvelope(
    guildId: GuildId,
    envelope: RelayPubSubEnvelope,
  ) {
    if (
      envelope.originId === this.relayInstanceId ||
      envelope.guildId !== guildId
    ) {
      return;
    }
    const events =
      Array.isArray(envelope.events) && envelope.events.length > 0
        ? envelope.events
        : envelope.event
          ? [envelope.event]
          : [];
    if (events.length === 0) {
      return;
    }

    if (this.pubSubLiveTopics) {
      const deliverable = this.filterRecentKnownUndeliveredPubSubEvents(
        envelope.guildId,
        events,
      );
      const pending =
        deliverable.length === events.length
          ? []
          : deliverable.length === 0
            ? events
            : (() => {
                const deliverableIds = new Set<string>();
                for (const event of deliverable) {
                  deliverableIds.add(event.id);
                }
                return events.filter((event) => !deliverableIds.has(event.id));
              })();
      if (pending.length > 0) {
        this.rememberPendingPubSubDeliveries(envelope.guildId, pending);
      }
      this.deliverPubSubEvents(
        envelope.guildId,
        deliverable,
        events.length,
        envelope.frame,
      );
    } else if (envelope.event) {
      await this.replicatePubSubEvents(envelope.guildId, [envelope.event]);
      const deliverable = this.filterRecentKnownUndeliveredPubSubEvents(
        envelope.guildId,
        [envelope.event],
      );
      this.deliverPubSubEvents(
        envelope.guildId,
        deliverable,
        1,
        envelope.frame,
      );
    } else {
      await this.replicatePubSubEvents(envelope.guildId, events);
      const deliverable = this.filterRecentKnownUndeliveredPubSubEvents(
        envelope.guildId,
        events,
      );
      this.deliverPubSubEvents(
        envelope.guildId,
        deliverable,
        events.length,
        envelope.frame,
      );
    }
  }

  private async replicatePubSubEvents(guildId: GuildId, events: GuildEvent[]) {
    if (events.length === 0) {
      return [];
    }

    const ordered = [...events].sort((left, right) => left.seq - right.seq);
    return await this.withGuildMutex(guildId, async () => {
      let lastEvent = await this.store.getLastEvent(guildId);
      let state = this.stateCache.get(guildId);
      let checkpointEvent = this.checkpointCache.get(guildId);
      const accepted: GuildEvent[] = [];

      for (const event of ordered) {
        if (lastEvent && event.seq <= lastEvent.seq) {
          continue;
        }

        const expectedSeq = lastEvent ? lastEvent.seq + 1 : 0;
        const expectedPrevHash = lastEvent ? lastEvent.id : null;
        if (this.verboseLogging) {
          console.log(
            `Relay pubsub replicate guild ${guildId}: got seq ${event.seq}, expected ${expectedSeq}`,
          );
        }
        if (event.seq !== expectedSeq || event.prevHash !== expectedPrevHash) {
          if (this.verboseLogging) {
            console.warn(
              `Skipping pubsub replication gap for guild ${guildId}: got seq ${event.seq}, expected ${expectedSeq}`,
            );
          }
          void this.requestPubSubLogReplay(guildId, event.seq).catch((e) => {
            console.error(
              `Relay pubsub gap catch-up failed for guild ${guildId}:`,
              e,
            );
          });
          void this.requestPeerLogCatchup(guildId, event.seq).catch((e) => {
            console.error(
              `Relay peer gap catch-up failed for guild ${guildId}:`,
              e,
            );
          });
          break;
        }

        const expectedId = computeEventId({
          seq: event.seq,
          prevHash: event.prevHash,
          createdAt: event.createdAt,
          author: event.author,
          body: event.body,
        });
        if (expectedId !== event.id) {
          console.warn(
            `Skipping pubsub replication with invalid event id for guild ${guildId} seq ${event.seq}`,
          );
          continue;
        }

        if (
          !verifyObject(
            event.author,
            {
              body: event.body,
              author: event.author,
              createdAt: event.createdAt,
            },
            event.signature,
          )
        ) {
          console.warn(
            `Skipping pubsub replication with invalid signature for guild ${guildId} seq ${event.seq}`,
          );
          continue;
        }

        if (event.seq > 0) {
          if (!state || state.headSeq !== event.seq - 1) {
            let history = this.store.getReplaySnapshotEvents
              ? buildReplaySnapshotEvents(
                  guildId,
                  await this.store.getReplaySnapshotEvents({
                    guildId,
                    limit: this.snapshotEventLimit,
                  }),
                )
              : await this.store.getLog(guildId);
            const canReplayFromFirstEvent =
              history[0]?.seq === 0 ||
              (history[0]?.body.type === "CHECKPOINT" &&
                isValidCheckpointEvent(history[0]));
            if (!canReplayFromFirstEvent) {
              history = await this.store.getLog(guildId);
            }
            if (history.length === 0) {
              break;
            }
            const rebuilt = rebuildStateFromEvents(history);
            state = rebuilt.state;
            checkpointEvent = rebuilt.checkpointEvent;
          }

          try {
            if (this.canFastValidateOpenMessage(state, event)) {
              state = this.applyFastOpenMessage(state, event);
            } else {
              const validationState = await this.hydrateValidationIndexes(
                state,
                event,
              );
              validateEvent(validationState, event);
              state = applyEvent(validationState, event, { mutable: true });
            }
            if (
              event.body.type === "CHECKPOINT" &&
              isValidCheckpointEvent(event)
            ) {
              checkpointEvent = event;
            }
          } catch (e: any) {
            console.warn(
              `Skipping pubsub replication validation failure for guild ${guildId} seq ${event.seq}: ${e?.message || e}`,
            );
            continue;
          }
        } else {
          if (event.body.type !== "GUILD_CREATE") {
            continue;
          }
          state = createInitialState(event);
          checkpointEvent = undefined;
        }

        accepted.push(event);
        lastEvent = event;
      }

      if (accepted.length > 0) {
        if (this.verboseLogging) {
          console.log(
            `Relay accepted pubsub replication for guild ${guildId}: ${accepted[0]?.seq}-${accepted[accepted.length - 1]?.seq}`,
          );
        }
        if (this.store.appendEvents) {
          await this.store.appendEvents(guildId, accepted);
        } else {
          for (const event of accepted) {
            await this.store.append(guildId, event);
          }
        }
        if (state) {
          this.cacheGuildState(guildId, state, checkpointEvent);
        }
        this.rememberRecentKnownEvents(guildId, accepted);
        this.ensurePubSubHostedGuildReplication(guildId);
        await this.runOnEventsAppended(accepted);
      }

      return accepted;
    });
  }

  private handlePubSubReplicationEnvelope(
    guildId: GuildId,
    envelope: RelayPubSubEnvelope,
  ) {
    if (
      envelope.originId === this.relayInstanceId ||
      envelope.guildId !== guildId
    ) {
      return;
    }
    const events =
      Array.isArray(envelope.events) && envelope.events.length > 0
        ? envelope.events
        : envelope.event
          ? [envelope.event]
          : [];
    if (this.verboseLogging && events.length > 0) {
      console.log(
        `Relay received pubsub log replay for guild ${guildId}: ${events[0]?.seq}-${events[events.length - 1]?.seq}`,
      );
    }
    void this.replicatePubSubEvents(envelope.guildId, events)
      .then((accepted) => {
        if (accepted.length === 0) {
          return;
        }
        const hasPartitionedLiveDelivery =
          this.pubSubLiveTopics && envelope.liveTopics !== false;
        const deliverable = hasPartitionedLiveDelivery
          ? this.flushPendingPubSubDeliveries(envelope.guildId, accepted)
          : accepted;
        this.deliverPubSubEvents(
          envelope.guildId,
          deliverable,
          events.length,
          envelope.frame,
        );
      })
      .catch((e) => {
        console.error(
          `Relay pubsub log replication failed for guild ${guildId}:`,
          e,
        );
      });
  }

  private handlePubSubHeadEnvelope(
    guildId: GuildId,
    envelope: RelayPubSubEnvelope,
  ) {
    if (
      envelope.originId === this.relayInstanceId ||
      envelope.guildId !== guildId ||
      !envelope.head
    ) {
      return;
    }
    const quorum = this.recordRelayHeadObservation(envelope.head);
    if (!quorum) {
      if (this.verboseLogging) {
        console.warn(
          `Ignoring invalid relay head gossip for guild ${guildId} from ${envelope.originId}`,
        );
      }
      return;
    }
    this.deliverRelayHeadGossip(guildId, envelope.head, quorum);
    void this.requestPubSubLogReplay(guildId, envelope.head.headSeq).catch(
      (e) => {
        console.error(
          `Relay pubsub head catch-up failed for guild ${guildId}:`,
          e,
        );
      },
    );
    void this.requestPeerLogCatchup(guildId, envelope.head.headSeq).catch(
      (e) => {
        console.error(
          `Relay peer head catch-up failed for guild ${guildId}:`,
          e,
        );
      },
    );
  }

  private ensurePubSubTopicSubscription(
    topic: string,
    guildId: GuildId,
    hasInterest: () => boolean,
    options: { replicationOnly?: boolean; headOnly?: boolean } = {},
  ) {
    if (
      !this.pubSubAdapter ||
      this.pubSubUnsubscribers.has(topic) ||
      this.pubSubPendingGuilds.has(topic)
    ) {
      return;
    }

    this.pubSubPendingGuilds.add(topic);
    void (async () => {
      const subscribeOptions = options.replicationOnly
        ? await this.pubSubReplayOptions(guildId)
        : options.headOnly
          ? { afterSeq: -1, replayLimit: 128 }
          : undefined;
      return await Promise.resolve(
        this.pubSubAdapter!.subscribe(
          topic,
          (envelope) => {
            if (options.replicationOnly) {
              this.handlePubSubReplicationEnvelope(guildId, envelope);
            } else if (options.headOnly) {
              this.handlePubSubHeadEnvelope(guildId, envelope);
            } else {
              void this.handlePubSubEnvelope(guildId, envelope).catch((e) => {
                console.error(
                  `Relay pubsub envelope handling failed for topic ${topic}:`,
                  e,
                );
              });
            }
          },
          subscribeOptions,
        ),
      );
    })()
      .then((unsubscribe) => {
        this.pubSubPendingGuilds.delete(topic);
        if (this.pubSubUnsubscribers.has(topic) || !hasInterest()) {
          void Promise.resolve(unsubscribe()).catch((e) => {
            console.error(
              `Relay pubsub unsubscribe failed for topic ${topic}:`,
              e,
            );
          });
          return;
        }
        this.pubSubUnsubscribers.set(topic, unsubscribe);
      })
      .catch((e) => {
        this.pubSubPendingGuilds.delete(topic);
        console.error(`Relay pubsub subscribe failed for topic ${topic}:`, e);
      });
  }

  private maybeReleasePubSubTopicSubscription(
    topic: string,
    hasInterest: () => boolean,
  ) {
    if (hasInterest()) {
      return;
    }
    const unsubscribe = this.pubSubUnsubscribers.get(topic);
    if (!unsubscribe) {
      return;
    }
    this.pubSubUnsubscribers.delete(topic);
    void Promise.resolve(unsubscribe()).catch((e) => {
      console.error(`Relay pubsub unsubscribe failed for topic ${topic}:`, e);
    });
  }

  private ensurePubSubGuildSubscription(guildId: GuildId) {
    if (this.pubSubLiveTopics) {
      this.ensurePubSubTopicSubscription(
        this.pubSubTopic(guildId),
        guildId,
        () => this.guildSubscriptions.has(guildId),
      );
    } else {
      this.maybeReleasePubSubTopicSubscription(
        this.pubSubTopic(guildId),
        () => false,
      );
    }
    this.ensurePubSubTopicSubscription(
      this.pubSubLogTopic(guildId),
      guildId,
      () =>
        this.hostedGuilds.has(guildId) || this.guildSubscriptions.has(guildId),
      { replicationOnly: true },
    );
    this.ensurePubSubHeadSubscription(guildId);
  }

  private maybeReleasePubSubGuildSubscription(guildId: GuildId) {
    this.maybeReleasePubSubTopicSubscription(this.pubSubTopic(guildId), () =>
      this.guildSubscriptions.has(guildId),
    );
    this.maybeReleasePubSubTopicSubscription(
      this.pubSubLogTopic(guildId),
      () =>
        this.hostedGuilds.has(guildId) || this.guildSubscriptions.has(guildId),
    );
    this.maybeReleasePubSubHeadSubscription(guildId);
  }

  private ensurePubSubHostedGuildReplication(guildId: GuildId) {
    this.hostedGuilds.add(guildId);
    this.ensurePubSubTopicSubscription(
      this.pubSubLogTopic(guildId),
      guildId,
      () => this.hostedGuilds.has(guildId),
      { replicationOnly: true },
    );
    this.ensurePubSubHeadSubscription(guildId);
  }

  private hasRelayHeadInterest(guildId: GuildId) {
    return (
      this.hostedGuilds.has(guildId) || this.guildSubscriptions.has(guildId)
    );
  }

  private ensurePubSubHeadSubscription(guildId: GuildId) {
    this.ensurePubSubTopicSubscription(
      this.pubSubHeadTopic(guildId),
      guildId,
      () => this.hasRelayHeadInterest(guildId),
      { headOnly: true },
    );
  }

  private maybeReleasePubSubHeadSubscription(guildId: GuildId) {
    this.maybeReleasePubSubTopicSubscription(
      this.pubSubHeadTopic(guildId),
      () => this.hasRelayHeadInterest(guildId),
    );
  }

  private hasAllChannelPubSubInterest(guildId: GuildId) {
    return Boolean(this.guildAllChannelSubscriptions.get(guildId)?.size);
  }

  private hasSpecificChannelPubSubInterest(
    guildId: GuildId,
    channelId: ChannelId,
  ) {
    if (this.hasAllChannelPubSubInterest(guildId)) {
      return false;
    }
    return Boolean(
      this.guildChannelSubscriptions.get(guildId)?.get(channelId)?.size,
    );
  }

  private ensurePubSubAllChannelsSubscription(guildId: GuildId) {
    this.ensurePubSubTopicSubscription(
      this.pubSubAllChannelsTopic(guildId),
      guildId,
      () => this.hasAllChannelPubSubInterest(guildId),
    );
  }

  private maybeReleasePubSubAllChannelsSubscription(guildId: GuildId) {
    this.maybeReleasePubSubTopicSubscription(
      this.pubSubAllChannelsTopic(guildId),
      () => this.hasAllChannelPubSubInterest(guildId),
    );
  }

  private ensurePubSubChannelSubscription(
    guildId: GuildId,
    channelId: ChannelId,
  ) {
    this.ensurePubSubTopicSubscription(
      this.pubSubChannelTopic(guildId, channelId),
      guildId,
      () => this.hasSpecificChannelPubSubInterest(guildId, channelId),
    );
  }

  private maybeReleasePubSubChannelSubscription(
    guildId: GuildId,
    channelId: ChannelId,
  ) {
    this.maybeReleasePubSubTopicSubscription(
      this.pubSubChannelTopic(guildId, channelId),
      () => this.hasSpecificChannelPubSubInterest(guildId, channelId),
    );
  }

  private refreshPubSubChannelSubscriptionsForGuild(guildId: GuildId) {
    if (!this.pubSubLiveTopics) {
      this.maybeReleasePubSubAllChannelsSubscription(guildId);
      for (const channelId of this.guildChannelSubscriptions
        .get(guildId)
        ?.keys() ?? []) {
        this.maybeReleasePubSubChannelSubscription(guildId, channelId);
      }
      return;
    }

    if (this.hasAllChannelPubSubInterest(guildId)) {
      this.ensurePubSubAllChannelsSubscription(guildId);
      for (const channelId of this.guildChannelSubscriptions
        .get(guildId)
        ?.keys() ?? []) {
        this.maybeReleasePubSubChannelSubscription(guildId, channelId);
      }
      return;
    }

    this.maybeReleasePubSubAllChannelsSubscription(guildId);
    for (const channelId of this.guildChannelSubscriptions
      .get(guildId)
      ?.keys() ?? []) {
      this.ensurePubSubChannelSubscription(guildId, channelId);
    }
  }

  private addSubscription(
    socket: WebSocket,
    subId: string,
    subscription: Subscription,
  ) {
    const subs = this.subscriptions.get(socket);
    if (!subs) {
      return;
    }

    const existing = subs.get(subId);
    if (existing) {
      this.removeSubscriptionIndex(socket, subId, existing);
    }

    if (
      Array.isArray(subscription.channels) &&
      subscription.channels.length > 0
    ) {
      subscription.channelSet = new Set(subscription.channels);
      subscription.channels = Array.from(subscription.channelSet);
    }
    subscription.fingerprint = this.subscriptionFingerprint(subscription);
    subs.set(subId, subscription);
    let guildSockets = this.guildSubscriptions.get(subscription.guildId);
    if (!guildSockets) {
      guildSockets = new Map<WebSocket, Set<string>>();
      this.guildSubscriptions.set(subscription.guildId, guildSockets);
    }
    const socketSubs = guildSockets.get(socket) ?? new Set<string>();
    socketSubs.add(subId);
    guildSockets.set(socket, socketSubs);

    if (
      Array.isArray(subscription.channels) &&
      subscription.channels.length > 0
    ) {
      let channelSocketsByGuild = this.guildChannelSubscriptions.get(
        subscription.guildId,
      );
      if (!channelSocketsByGuild) {
        channelSocketsByGuild = new Map<
          ChannelId,
          Map<WebSocket, Set<string>>
        >();
        this.guildChannelSubscriptions.set(
          subscription.guildId,
          channelSocketsByGuild,
        );
      }
      for (const channelId of subscription.channels) {
        let channelSockets = channelSocketsByGuild.get(channelId);
        if (!channelSockets) {
          channelSockets = new Map<WebSocket, Set<string>>();
          channelSocketsByGuild.set(channelId, channelSockets);
        }
        const channelSocketSubs =
          channelSockets.get(socket) ?? new Set<string>();
        channelSocketSubs.add(subId);
        channelSockets.set(socket, channelSocketSubs);
      }
    } else {
      let allChannelSockets = this.guildAllChannelSubscriptions.get(
        subscription.guildId,
      );
      if (!allChannelSockets) {
        allChannelSockets = new Map<WebSocket, Set<string>>();
        this.guildAllChannelSubscriptions.set(
          subscription.guildId,
          allChannelSockets,
        );
      }
      const allChannelSocketSubs =
        allChannelSockets.get(socket) ?? new Set<string>();
      allChannelSocketSubs.add(subId);
      allChannelSockets.set(socket, allChannelSocketSubs);
    }

    this.ensurePubSubGuildSubscription(subscription.guildId);
    this.refreshPubSubChannelSubscriptionsForGuild(subscription.guildId);
  }

  private subscriptionFingerprint(subscription: Subscription) {
    const channels =
      Array.isArray(subscription.channels) && subscription.channels.length > 0
        ? [...subscription.channels].sort().join(",")
        : "*";
    return `${subscription.author ?? ""}|${channels}`;
  }

  private removeSubscriptionIndex(
    socket: WebSocket,
    subId: string,
    subscription: Subscription,
  ) {
    const guildId = subscription.guildId;
    const guildSockets = this.guildSubscriptions.get(guildId);
    const socketSubs = guildSockets?.get(socket);
    if (guildSockets && socketSubs) {
      socketSubs.delete(subId);
      if (socketSubs.size === 0) {
        guildSockets.delete(socket);
      }
      if (guildSockets.size === 0) {
        this.guildSubscriptions.delete(guildId);
      }
    }

    if (
      Array.isArray(subscription.channels) &&
      subscription.channels.length > 0
    ) {
      const channelSocketsByGuild = this.guildChannelSubscriptions.get(guildId);
      if (channelSocketsByGuild) {
        for (const channelId of subscription.channels) {
          const channelSockets = channelSocketsByGuild.get(channelId);
          const channelSocketSubs = channelSockets?.get(socket);
          if (!channelSockets || !channelSocketSubs) continue;
          channelSocketSubs.delete(subId);
          if (channelSocketSubs.size === 0) {
            channelSockets.delete(socket);
          }
          if (channelSockets.size === 0) {
            channelSocketsByGuild.delete(channelId);
          }
        }
        if (channelSocketsByGuild.size === 0) {
          this.guildChannelSubscriptions.delete(guildId);
        }
      }
    } else {
      const allChannelSockets = this.guildAllChannelSubscriptions.get(guildId);
      const allChannelSocketSubs = allChannelSockets?.get(socket);
      if (allChannelSockets && allChannelSocketSubs) {
        allChannelSocketSubs.delete(subId);
        if (allChannelSocketSubs.size === 0) {
          allChannelSockets.delete(socket);
        }
        if (allChannelSockets.size === 0) {
          this.guildAllChannelSubscriptions.delete(guildId);
        }
      }
    }
    this.refreshPubSubChannelSubscriptionsForGuild(guildId);
    this.maybeReleasePubSubGuildSubscription(guildId);
  }

  private removeSocketSubscriptions(socket: WebSocket) {
    const subs = this.subscriptions.get(socket);
    if (subs) {
      for (const [subId, sub] of subs) {
        this.removeSubscriptionIndex(socket, subId, sub);
      }
    }
    this.subscriptions.delete(socket);
  }

  private async readAuthorOrError(
    socket: WebSocket,
    kind: string,
    payload: unknown,
    subId?: string,
    guildId?: string,
  ) {
    try {
      return this.verifyReadRequest(kind, payload);
    } catch (e: any) {
      this.sendRequestError(
        socket,
        "AUTH_FAILED",
        this.clientSafeError("AUTH_FAILED"),
        { subId, guildId },
      );
      return null;
    }
  }

  private canReadAsPeerRelay(author?: string) {
    return !!author && this.trustedPeerReadPublicKeys.has(author);
  }

  private canReadGuildForRequest(state: GuildState, author?: string) {
    return canReadGuild(state, author) || this.canReadAsPeerRelay(author);
  }

  private async signedRelayReadPayload(
    kind: string,
    payload: Record<string, unknown>,
  ) {
    const unsignedPayload = {
      ...payload,
      author: this.keyPair.publicKey,
      createdAt: Date.now(),
    };
    return {
      ...unsignedPayload,
      signature: await sign(
        this.keyPair.privateKey,
        hashObject({ kind, payload: unsignedPayload }),
      ),
    };
  }

  private async handleRelayHeadRequest(socket: WebSocket, payload: unknown) {
    const p = payload as RelayHeadRequest;
    const guildId = typeof p?.guildId === "string" ? p.guildId : "";
    const subId =
      typeof p?.subId === "string" && p.subId.trim()
        ? p.subId
        : `head-${Date.now()}`;

    if (!guildId.trim()) {
      this.sendRequestError(
        socket,
        "VALIDATION_FAILED",
        "GET_HEAD requires a guildId",
        { subId },
      );
      return;
    }

    let verifiedAuthor: string | undefined;
    try {
      verifiedAuthor = this.verifyReadRequest("GET_HEAD", payload);
    } catch (e: any) {
      this.sendRequestError(
        socket,
        "AUTH_FAILED",
        this.clientSafeError("AUTH_FAILED"),
        { subId, guildId },
      );
      return;
    }

    if (this.canReadAsPeerRelay(verifiedAuthor)) {
      const head = await this.signRelayHead(guildId);
      if (!head) {
        this.sendRequestError(socket, "NOT_FOUND", "Guild head not found", {
          subId,
          guildId,
        });
        return;
      }
      this.sendFrame(socket, "RELAY_HEAD", { subId, guildId, head });
      return;
    }

    await this.withGuildMutex(guildId, async () => {
      const rebuilt = await this.rebuildGuildState(guildId);
      if (!rebuilt) {
        this.sendRequestError(socket, "NOT_FOUND", "Guild head not found", {
          subId,
          guildId,
        });
        return;
      }

      if (!this.canReadGuildForRequest(rebuilt.state, verifiedAuthor)) {
        this.sendRequestError(
          socket,
          "FORBIDDEN",
          "You do not have permission to read this guild head",
          { subId, guildId },
        );
        return;
      }

      const head = await this.signRelayHead(guildId, rebuilt);
      if (!head) {
        this.sendRequestError(socket, "NOT_FOUND", "Guild head not found", {
          subId,
          guildId,
        });
        return;
      }
      this.sendFrame(socket, "RELAY_HEAD", { subId, guildId, head });
    });
  }

  private async handleRelayHeadsRequest(socket: WebSocket, payload: unknown) {
    const p = payload as RelayHeadsRequest;
    const guildId = typeof p?.guildId === "string" ? p.guildId : "";
    const subId =
      typeof p?.subId === "string" && p.subId.trim()
        ? p.subId
        : `heads-${Date.now()}`;

    if (!guildId.trim()) {
      this.sendRequestError(
        socket,
        "VALIDATION_FAILED",
        "GET_HEADS requires a guildId",
        { subId },
      );
      return;
    }

    let verifiedAuthor: string | undefined;
    try {
      verifiedAuthor = this.verifyReadRequest("GET_HEADS", payload);
    } catch (e: any) {
      this.sendRequestError(
        socket,
        "AUTH_FAILED",
        this.clientSafeError("AUTH_FAILED"),
        { subId, guildId },
      );
      return;
    }

    if (this.canReadAsPeerRelay(verifiedAuthor)) {
      const ownHead = await this.signRelayHead(guildId);
      if (!ownHead) {
        this.sendRequestError(socket, "NOT_FOUND", "Guild heads not found", {
          subId,
          guildId,
        });
        return;
      }
      this.recordRelayHeadObservation(ownHead);
      const heads = await this.observedHeadsForGuild(guildId, ownHead);
      const quorum = summarizeRelayHeadQuorum(guildId, heads);
      this.sendFrame(socket, "RELAY_HEADS", {
        subId,
        guildId,
        heads,
        quorum: {
          guildId,
          validCount: quorum.validHeads.length,
          invalidCount: quorum.invalidHeads.length,
          conflictCount: quorum.conflicts.length,
          canonical: quorum.canonical,
          conflicts: quorum.conflicts.map((conflict) => ({
            guildId: conflict.guildId,
            seq: conflict.seq,
            reason: conflict.reason,
            leftRelayId: conflict.left.relayId,
            leftHash: conflict.left.headHash,
            rightRelayId: conflict.right.relayId,
            rightHash: conflict.right.headHash,
          })),
        },
      });
      return;
    }

    await this.withGuildMutex(guildId, async () => {
      const rebuilt = await this.rebuildGuildState(guildId);
      if (!rebuilt) {
        this.sendRequestError(socket, "NOT_FOUND", "Guild heads not found", {
          subId,
          guildId,
        });
        return;
      }

      if (!this.canReadGuildForRequest(rebuilt.state, verifiedAuthor)) {
        this.sendRequestError(
          socket,
          "FORBIDDEN",
          "You do not have permission to read this guild's relay heads",
          { subId, guildId },
        );
        return;
      }

      const ownHead = await this.signRelayHead(guildId, rebuilt);
      if (ownHead) {
        this.recordRelayHeadObservation(ownHead);
      }
      const heads = await this.observedHeadsForGuild(guildId, ownHead);
      const quorum = summarizeRelayHeadQuorum(guildId, heads);
      this.sendFrame(socket, "RELAY_HEADS", {
        subId,
        guildId,
        heads,
        quorum: {
          guildId,
          validCount: quorum.validHeads.length,
          invalidCount: quorum.invalidHeads.length,
          conflictCount: quorum.conflicts.length,
          canonical: quorum.canonical,
          conflicts: quorum.conflicts.map((conflict) => ({
            guildId: conflict.guildId,
            seq: conflict.seq,
            reason: conflict.reason,
            leftRelayId: conflict.left.relayId,
            leftHash: conflict.left.headHash,
            rightRelayId: conflict.right.relayId,
            rightHash: conflict.right.headHash,
          })),
        },
      });
    });
  }

  private async handleStateRequest(socket: WebSocket, payload: unknown) {
    const p = payload as StateRequest;
    const guildId = typeof p?.guildId === "string" ? p.guildId : "";
    const subId =
      typeof p?.subId === "string" && p.subId.trim()
        ? p.subId
        : `state-${Date.now()}`;

    if (!guildId.trim()) {
      this.sendFrame(socket, "ERROR", {
        code: "VALIDATION_FAILED",
        message: "GET_STATE requires a guildId",
      });
      return;
    }

    await this.withGuildMutex(guildId, async () => {
      const author = await this.readAuthorOrError(
        socket,
        "GET_STATE",
        payload,
        subId,
        guildId,
      );
      if (author === null) return;

      const rebuilt = await this.rebuildGuildState(guildId);
      if (!rebuilt) {
        this.sendFrame(socket, "ERROR", {
          code: "NOT_FOUND",
          message: "Guild state not found",
          subId,
          guildId,
        });
        return;
      }

      if (!canReadGuild(rebuilt.state, author)) {
        this.sendRequestError(
          socket,
          "FORBIDDEN",
          "You do not have permission to read this guild state",
          { subId, guildId },
        );
        return;
      }

      const wantsPagedMembers =
        p.memberLimit !== undefined ||
        (typeof p.memberAfter === "string" && p.memberAfter.trim());
      const memberLimit = normalizeMemberPageLimit(p.memberLimit);
      const memberAfter =
        typeof p.memberAfter === "string" && p.memberAfter.trim()
          ? p.memberAfter
          : undefined;
      const includeMembers = wantsPagedMembers
        ? false
        : p.includeMembers !== false;
      const includeMessages = p.includeMessages !== false;
      const includeAppObjects = p.includeAppObjects !== false;
      const serializedState = filterSerializedStateForReader(
        rebuilt.state,
        author,
        {
          includeMembers,
          includeMessages,
          includeAppObjects,
        },
      );
      const membersPage = wantsPagedMembers
        ? await this.readMembersPage(rebuilt.state, {
            guildId,
            afterUserId: memberAfter,
            limit: memberLimit,
          })
        : null;
      if (membersPage) {
        serializedState.members = membersPage.members.map((member) => [
          member.userId,
          member,
        ]);
      }
      this.sendFrame(socket, "STATE", {
        subId,
        guildId,
        state: serializedState,
        rootHash: hashObject(serializedState),
        endSeq: rebuilt.endEvent.seq,
        endHash: rebuilt.endEvent.id,
        checkpointSeq: rebuilt.checkpointEvent?.seq ?? null,
        checkpointHash: rebuilt.checkpointEvent?.id ?? null,
        membersPage: membersPage
          ? {
              nextCursor: membersPage.nextCursor,
              hasMore: membersPage.hasMore,
              totalApprox: membersPage.totalApprox,
            }
          : undefined,
        stateIncludes: {
          members: membersPage
            ? "partial"
            : includeMembers
              ? "full"
              : "omitted",
          messages: includeMessages ? "full" : "omitted",
          appObjects: includeAppObjects ? "full" : "omitted",
        },
      });
    });
  }

  private async readMembersPage(state: GuildState, query: MemberPageQuery) {
    if (this.store.getMembersPage) {
      return await this.store.getMembersPage(query);
    }

    const limit = normalizeMemberPageLimit(query.limit);
    const members = serializeState(state)
      .members.map(([, member]) => member)
      .sort((left, right) => left.userId.localeCompare(right.userId));
    const startIndex = query.afterUserId
      ? members.findIndex((member) => member.userId > query.afterUserId!)
      : 0;
    const pageStart = startIndex >= 0 ? startIndex : members.length;
    const page = members.slice(pageStart, pageStart + limit + 1);
    const hasMore = page.length > limit;
    const visible = hasMore ? page.slice(0, limit) : page;
    return {
      members: visible,
      nextCursor:
        hasMore && visible.length > 0
          ? visible[visible.length - 1].userId
          : null,
      hasMore,
      totalApprox: members.length,
    };
  }

  private async readReplaySnapshotEvents(guildId: GuildId) {
    const events = this.store.getReplaySnapshotEvents
      ? await this.store.getReplaySnapshotEvents({
          guildId,
          limit: this.snapshotEventLimit,
        })
      : await this.store.getLog(guildId);
    return buildReplaySnapshotEvents(guildId, events);
  }

  private async handleHistoryRequest(socket: WebSocket, payload: unknown) {
    const p = payload as HistoryRequest;
    const guildId = typeof p?.guildId === "string" ? p.guildId : "";
    const subId =
      typeof p?.subId === "string" && p.subId.trim()
        ? p.subId
        : `history-${Date.now()}`;

    if (!guildId.trim()) {
      this.sendFrame(socket, "ERROR", {
        code: "VALIDATION_FAILED",
        message: "GET_HISTORY requires a guildId",
      });
      return;
    }

    const limit = normalizeHistoryLimit(p.limit);
    const query: HistoryQuery = {
      guildId,
      channelId:
        typeof p.channelId === "string" && p.channelId.trim()
          ? p.channelId
          : undefined,
      beforeSeq: normalizeOptionalSeq(p.beforeSeq),
      afterSeq: normalizeOptionalSeq(p.afterSeq),
      limit: limit + 1,
      includeStructural: p.includeStructural === true,
    };

    const author = await this.readAuthorOrError(
      socket,
      "GET_HISTORY",
      payload,
      subId,
      guildId,
    );
    if (author === null) return;

    const headEvent = await this.store.getLastEvent(guildId);
    const cachedState = this.stateCache.get(guildId);
    if (
      this.store.getHistory &&
      this.canServeOpenChannelHistoryFast(cachedState, headEvent, author, query)
    ) {
      const events = await this.store.getHistory(query);
      this.sendHistorySnapshot(
        socket,
        subId,
        guildId,
        query.channelId,
        events,
        limit,
        query.afterSeq,
        headEvent,
      );
      return;
    }

    const rebuilt = await this.rebuildGuildState(guildId);
    if (!rebuilt) {
      this.sendFrame(socket, "ERROR", {
        code: "NOT_FOUND",
        message: "Guild history not found",
        subId,
        guildId,
      });
      return;
    }

    if (!canReadGuild(rebuilt.state, author)) {
      this.sendRequestError(
        socket,
        "FORBIDDEN",
        "You do not have permission to read this guild history",
        { subId, guildId },
      );
      return;
    }

    if (
      query.channelId &&
      !canViewChannel(rebuilt.state, author, query.channelId)
    ) {
      this.sendRequestError(
        socket,
        "FORBIDDEN",
        "You do not have permission to read this channel history",
        { subId, guildId, channelId: query.channelId },
      );
      return;
    }

    const events = this.store.getHistory
      ? await this.store.getHistory(query)
      : await this.store.getLog(guildId);
    const visibleEvents = filterEventsForReader(rebuilt.state, events, author);
    this.sendHistorySnapshot(
      socket,
      subId,
      guildId,
      query.channelId,
      visibleEvents,
      limit,
      query.afterSeq,
      headEvent ?? rebuilt.endEvent,
      rebuilt.checkpointEvent,
    );
  }

  private canServeOpenChannelHistoryFast(
    state: GuildState | undefined,
    headEvent: GuildEvent | undefined,
    author: string | undefined,
    query: HistoryQuery,
  ) {
    if (
      !state ||
      !headEvent ||
      state.headSeq !== headEvent.seq ||
      !query.channelId ||
      query.includeStructural
    ) {
      return false;
    }
    if (state.access !== "public" || (author && state.bans.has(author))) {
      return false;
    }
    const channel = state.channels.get(query.channelId);
    if (!channel) {
      return false;
    }
    return (
      !Array.isArray(channel.permissionOverwrites) ||
      channel.permissionOverwrites.length === 0
    );
  }

  private sendHistorySnapshot(
    socket: WebSocket,
    subId: string,
    guildId: GuildId,
    channelId: ChannelId | undefined,
    candidateEvents: GuildEvent[],
    limit: number,
    afterSeq: number | undefined,
    fallbackTailEvent?: GuildEvent,
    checkpointEvent?: GuildEvent,
  ) {
    const hasMore = candidateEvents.length > limit;
    const pageEvents =
      afterSeq !== undefined
        ? candidateEvents.slice(0, limit)
        : candidateEvents.slice(Math.max(0, candidateEvents.length - limit));
    const oldestSeq = pageEvents.length > 0 ? pageEvents[0].seq : null;
    const newestSeq =
      pageEvents.length > 0 ? pageEvents[pageEvents.length - 1].seq : null;
    const tailEvent =
      pageEvents.length > 0
        ? pageEvents[pageEvents.length - 1]
        : fallbackTailEvent;

    this.sendFrame(socket, "SNAPSHOT", {
      subId,
      guildId,
      channelId,
      events: pageEvents,
      endSeq: tailEvent?.seq ?? -1,
      endHash: tailEvent?.id ?? null,
      oldestSeq,
      newestSeq,
      hasMore,
      checkpointSeq: checkpointEvent?.seq ?? null,
      checkpointHash: checkpointEvent?.id ?? null,
    });
  }

  private async handleLogRangeRequest(socket: WebSocket, payload: unknown) {
    const p = payload as LogRangeRequest;
    const guildId = typeof p?.guildId === "string" ? p.guildId : "";
    const subId =
      typeof p?.subId === "string" && p.subId.trim()
        ? p.subId
        : `log-range-${Date.now()}`;

    if (!guildId.trim()) {
      this.sendFrame(socket, "ERROR", {
        code: "VALIDATION_FAILED",
        message: "GET_LOG_RANGE requires a guildId",
        subId,
      });
      return;
    }

    const afterSeq = normalizeOptionalSeq(p.afterSeq) ?? -1;
    const limit = normalizeLogRangeLimit(p.limit);

    const verifiedAuthor = await this.readAuthorOrError(
      socket,
      "GET_LOG_RANGE",
      payload,
      subId,
      guildId,
    );
    if (verifiedAuthor === null) return;

    if (this.canReadAsPeerRelay(verifiedAuthor)) {
      const endEvent = await this.store.getLastEvent(guildId);
      if (!endEvent) {
        this.sendRequestError(socket, "NOT_FOUND", "Guild log not found", {
          subId,
          guildId,
        });
        return;
      }

      const rawEvents = this.store.getEventRange
        ? await this.store.getEventRange({
            guildId,
            afterSeq,
            limit: limit + 1,
          })
        : (await this.store.getLog(guildId))
            .filter((event) => event.seq > afterSeq)
            .slice(0, limit + 1);
      const hasMore = rawEvents.length > limit;
      const events = rawEvents.slice(0, limit);
      const newestSeq =
        events.length > 0 ? events[events.length - 1].seq : afterSeq;
      const rangeHash = hashObject({
        guildId,
        afterSeq,
        eventIds: events.map((event) => event.id),
      });
      const checkpointEvent = this.checkpointCache.get(guildId);

      this.sendFrame(socket, "LOG_RANGE", {
        subId,
        guildId,
        afterSeq,
        events,
        count: events.length,
        hasMore,
        nextAfterSeq: newestSeq,
        rangeHash,
        endSeq: endEvent.seq,
        endHash: endEvent.id,
        checkpointSeq: checkpointEvent?.seq ?? null,
        checkpointHash: checkpointEvent?.id ?? null,
      });
      return;
    }

    await this.withGuildMutex(guildId, async () => {
      const rebuilt = await this.rebuildGuildState(guildId);
      if (!rebuilt) {
        this.sendRequestError(socket, "NOT_FOUND", "Guild log not found", {
          subId,
          guildId,
        });
        return;
      }

      if (!this.canReadGuildForRequest(rebuilt.state, verifiedAuthor)) {
        this.sendRequestError(
          socket,
          "FORBIDDEN",
          "You do not have permission to read this guild log",
          { subId, guildId },
        );
        return;
      }

      const rawEvents = this.store.getEventRange
        ? await this.store.getEventRange({
            guildId,
            afterSeq,
            limit: limit + 1,
          })
        : (await this.store.getLog(guildId))
            .filter((event) => event.seq > afterSeq)
            .slice(0, limit + 1);
      const hasMore = rawEvents.length > limit;
      const rawPage = rawEvents.slice(0, limit);
      const events = filterEventsForReader(
        rebuilt.state,
        rawPage,
        verifiedAuthor,
      );
      const newestSeq =
        rawPage.length > 0 ? rawPage[rawPage.length - 1].seq : afterSeq;
      const rangeHash = hashObject({
        guildId,
        afterSeq,
        eventIds: events.map((event) => event.id),
      });

      this.sendFrame(socket, "LOG_RANGE", {
        subId,
        guildId,
        afterSeq,
        events,
        count: events.length,
        hasMore,
        nextAfterSeq: newestSeq,
        rangeHash,
        endSeq: rebuilt.endEvent.seq,
        endHash: rebuilt.endEvent.id,
        checkpointSeq: rebuilt.checkpointEvent?.seq ?? null,
        checkpointHash: rebuilt.checkpointEvent?.id ?? null,
      });
    });
  }

  private async handleSearchRequest(socket: WebSocket, payload: unknown) {
    const p = payload as SearchRequest;
    const guildId = typeof p?.guildId === "string" ? p.guildId : "";
    const subId =
      typeof p?.subId === "string" && p.subId.trim()
        ? p.subId
        : `search-${Date.now()}`;
    const query = normalizeSearchQuery(p?.query);

    if (!guildId.trim()) {
      this.sendRequestError(
        socket,
        "VALIDATION_FAILED",
        "SEARCH requires a guildId",
        { subId },
      );
      return;
    }

    if (!query) {
      this.sendRequestError(
        socket,
        "VALIDATION_FAILED",
        "SEARCH requires a non-empty query",
        { subId, guildId },
      );
      return;
    }

    const limit = normalizeSearchLimit(p.limit);
    const request = {
      guildId,
      channelId:
        typeof p.channelId === "string" && p.channelId.trim()
          ? p.channelId
          : undefined,
      query,
      scopes: normalizeSearchScopes(p.scopes),
      beforeSeq: normalizeOptionalSeq(p.beforeSeq),
      afterSeq: normalizeOptionalSeq(p.afterSeq),
      limit,
      includeDeleted: p.includeDeleted === true,
      includeEncrypted: p.includeEncrypted === true,
      includeEvent: p.includeEvent !== false,
    };

    await this.withGuildMutex(guildId, async () => {
      const author = await this.readAuthorOrError(
        socket,
        "SEARCH",
        payload,
        subId,
        guildId,
      );
      if (author === null) return;

      const rebuilt = await this.rebuildGuildState(guildId);
      if (!rebuilt) {
        this.sendRequestError(socket, "NOT_FOUND", "Search guild not found", {
          subId,
          guildId,
        });
        return;
      }

      if (!canReadGuild(rebuilt.state, author)) {
        this.sendRequestError(
          socket,
          "FORBIDDEN",
          "You do not have permission to search this guild",
          { subId, guildId },
        );
        return;
      }

      if (
        request.channelId &&
        !canViewChannel(rebuilt.state, author, request.channelId)
      ) {
        this.sendRequestError(
          socket,
          "FORBIDDEN",
          "You do not have permission to search this channel",
          { subId, guildId, channelId: request.channelId },
        );
        return;
      }

      const indexedMembers = await this.readIndexedMemberSearchResults(request);
      const searchScopes = indexedMembers
        ? new Set([...request.scopes].filter((scope) => scope !== "members"))
        : request.scopes;
      const searchRequest = indexedMembers
        ? { ...request, scopes: searchScopes }
        : request;
      const events = searchRequest.scopes.has("messages")
        ? await this.readSearchCandidateEvents(request)
        : [];
      const built = buildSearchResults(
        rebuilt.state,
        events,
        author,
        searchRequest,
        indexedMembers?.results ?? [],
      );
      const results = built.results;
      const hasMore = built.hasMore || indexedMembers?.hasMore === true;
      const seqs = results
        .map((result) => result.seq)
        .filter((seq): seq is number => typeof seq === "number");

      this.sendFrame(socket, "SEARCH_RESULTS", {
        subId,
        guildId,
        channelId: request.channelId,
        query,
        scopes: Array.from(request.scopes),
        results,
        hasMore,
        oldestSeq: seqs.length > 0 ? Math.min(...seqs) : null,
        newestSeq: seqs.length > 0 ? Math.max(...seqs) : null,
        checkpointSeq: rebuilt.checkpointEvent?.seq ?? null,
        checkpointHash: rebuilt.checkpointEvent?.id ?? null,
      });
    });
  }

  private async readIndexedMemberSearchResults(request: {
    guildId: GuildId;
    query: string;
    scopes: Set<SearchScope>;
    beforeSeq?: number;
    afterSeq?: number;
    limit: number;
  }) {
    if (
      !request.scopes.has("members") ||
      request.beforeSeq !== undefined ||
      request.afterSeq !== undefined ||
      !this.store.searchMembers
    ) {
      return null;
    }

    const page = await this.store.searchMembers({
      guildId: request.guildId,
      query: request.query,
      limit: request.limit,
    });
    return {
      hasMore: page.hasMore,
      results: page.members.map((member) => ({
        kind: "member" as const,
        guildId: request.guildId,
        userId: member.userId,
        preview: previewText(member.nickname || member.bio || member.userId),
      })),
    };
  }

  private async readSearchCandidateEvents(request: {
    guildId: GuildId;
    channelId?: ChannelId;
    beforeSeq?: number;
    afterSeq?: number;
  }) {
    const query = {
      guildId: request.guildId,
      channelId: request.channelId,
      beforeSeq: request.beforeSeq,
      afterSeq: request.afterSeq,
      scanLimit: this.searchScanLimit,
      includeStructural: false,
    };

    if (this.store.getSearchEvents) {
      return await this.store.getSearchEvents(query);
    }

    if (this.store.getHistory) {
      return await this.store.getHistory({
        ...query,
        limit: this.searchScanLimit,
      });
    }

    return await this.store.getLog(request.guildId);
  }

  private publishReplayKey(
    guildId: GuildId,
    author: string,
    clientEventId?: string,
  ) {
    if (!clientEventId || !clientEventId.trim()) {
      return null;
    }
    return `${guildId}\x1f${author}\x1f${clientEventId}`;
  }

  private publishReplayPayloadHash(
    item: Pick<PublishPayload, "body" | "author" | "clientEventId">,
  ) {
    return hashObject({
      body: item.body,
      author: item.author,
      clientEventId: item.clientEventId,
    });
  }

  private recentEventKey(guildId: GuildId, eventId: string) {
    return `${guildId}:${eventId}`;
  }

  private pruneRecentEventIds(cache: Map<string, number>, now = Date.now()) {
    if (cache.size === 0) {
      return;
    }
    const overSizeLimit = cache.size > this.maxRecentPublishAcks;
    const lastPruneAt = this.recentEventPruneAt.get(cache) ?? 0;
    if (!overSizeLimit && now - lastPruneAt < RECENT_EVENT_PRUNE_INTERVAL_MS) {
      return;
    }
    this.recentEventPruneAt.set(cache, now);

    if (this.recentPublishAckTtlMs > 0) {
      const minObservedAt = now - this.recentPublishAckTtlMs;
      for (const [key, observedAt] of cache) {
        if (observedAt >= minObservedAt) {
          break;
        }
        cache.delete(key);
      }
    }

    while (cache.size > this.maxRecentPublishAcks) {
      const oldest = cache.keys().next().value;
      if (!oldest) break;
      cache.delete(oldest);
    }
  }

  private rememberRecentKnownEvents(guildId: GuildId, events: GuildEvent[]) {
    const now = Date.now();
    for (const event of events) {
      this.recentKnownEventIds.set(this.recentEventKey(guildId, event.id), now);
    }
    this.pruneRecentEventIds(this.recentKnownEventIds, now);
  }

  private rememberRecentDeliveredPubSubEvents(
    guildId: GuildId,
    events: GuildEvent[],
  ) {
    const now = Date.now();
    for (const event of events) {
      const key = this.recentEventKey(guildId, event.id);
      this.recentDeliveredPubSubEventIds.set(key, now);
      this.pendingPubSubDeliveryEvents.delete(key);
    }
    this.pruneRecentEventIds(this.recentDeliveredPubSubEventIds, now);
  }

  private prunePendingPubSubDeliveryEvents(now = Date.now()) {
    if (this.pendingPubSubDeliveryEvents.size === 0) {
      return;
    }
    const overSizeLimit =
      this.pendingPubSubDeliveryEvents.size > this.maxRecentPublishAcks;
    if (
      !overSizeLimit &&
      now - this.pendingPubSubPruneAt < RECENT_EVENT_PRUNE_INTERVAL_MS
    ) {
      return;
    }
    this.pendingPubSubPruneAt = now;

    if (this.recentPublishAckTtlMs > 0) {
      const minObservedAt = now - this.recentPublishAckTtlMs;
      for (const [key, entry] of this.pendingPubSubDeliveryEvents) {
        if (entry.observedAt >= minObservedAt) {
          break;
        }
        this.pendingPubSubDeliveryEvents.delete(key);
      }
    }
    while (this.pendingPubSubDeliveryEvents.size > this.maxRecentPublishAcks) {
      const oldest = this.pendingPubSubDeliveryEvents.keys().next().value;
      if (!oldest) break;
      this.pendingPubSubDeliveryEvents.delete(oldest);
    }
  }

  private rememberPendingPubSubDeliveries(
    guildId: GuildId,
    events: GuildEvent[],
  ) {
    const now = Date.now();
    for (const event of events) {
      const key = this.recentEventKey(guildId, event.id);
      if (
        this.recentKnownEventIds.has(key) ||
        this.recentDeliveredPubSubEventIds.has(key)
      ) {
        continue;
      }
      this.pendingPubSubDeliveryEvents.set(key, { event, observedAt: now });
    }
    this.prunePendingPubSubDeliveryEvents(now);
  }

  private flushPendingPubSubDeliveries(
    guildId: GuildId,
    acceptedEvents: GuildEvent[],
  ) {
    this.prunePendingPubSubDeliveryEvents();
    const deliverable: GuildEvent[] = [];
    for (const event of acceptedEvents) {
      const key = this.recentEventKey(guildId, event.id);
      if (
        !this.pendingPubSubDeliveryEvents.has(key) ||
        this.recentDeliveredPubSubEventIds.has(key)
      ) {
        continue;
      }
      this.pendingPubSubDeliveryEvents.delete(key);
      deliverable.push(event);
    }
    return deliverable;
  }

  private filterRecentKnownUndeliveredPubSubEvents(
    guildId: GuildId,
    events: GuildEvent[],
  ) {
    this.pruneRecentEventIds(this.recentKnownEventIds);
    this.pruneRecentEventIds(this.recentDeliveredPubSubEventIds);
    return events.filter((event) => {
      const key = this.recentEventKey(guildId, event.id);
      return (
        this.recentKnownEventIds.has(key) &&
        !this.recentDeliveredPubSubEventIds.has(key)
      );
    });
  }

  private pruneRecentPublishAcks(now = Date.now()) {
    if (this.recentPublishAcks.size > 0) {
      if (this.recentPublishAckTtlMs > 0) {
        const minObservedAt = now - this.recentPublishAckTtlMs;
        for (const [key, entry] of this.recentPublishAcks) {
          if (entry.observedAt >= minObservedAt) {
            break;
          }
          this.recentPublishAcks.delete(key);
        }
      }

      while (this.recentPublishAcks.size > this.maxRecentPublishAcks) {
        const oldest = this.recentPublishAcks.keys().next().value;
        if (!oldest) break;
        this.recentPublishAcks.delete(oldest);
      }
    }
    this.pruneRecentEventIds(this.recentKnownEventIds, now);
    this.pruneRecentEventIds(this.recentDeliveredPubSubEventIds, now);
    this.prunePendingPubSubDeliveryEvents(now);
  }

  private lookupRecentPublishAck(
    guildId: GuildId,
    item: Pick<PublishPayload, "body" | "author" | "clientEventId">,
  ) {
    const key = this.publishReplayKey(guildId, item.author, item.clientEventId);
    if (!key) {
      return { kind: "none" as const };
    }

    this.pruneRecentPublishAcks();
    const entry = this.recentPublishAcks.get(key);
    if (!entry) {
      return { kind: "none" as const };
    }

    const payloadHash = this.publishReplayPayloadHash(item);
    if (entry.payloadHash !== payloadHash) {
      return { kind: "mismatch" as const };
    }

    entry.observedAt = Date.now();
    this.recentPublishAcks.delete(key);
    this.recentPublishAcks.set(key, entry);
    return { kind: "hit" as const, ack: entry.ack };
  }

  private rememberRecentPublishAck(
    guildId: GuildId,
    item: Pick<PublishPayload, "body" | "author" | "clientEventId">,
    ack: PublishAck,
  ) {
    const key = this.publishReplayKey(guildId, item.author, item.clientEventId);
    if (!key) {
      return;
    }

    this.recentPublishAcks.set(key, {
      ack,
      payloadHash: this.publishReplayPayloadHash(item),
      observedAt: Date.now(),
    });
    this.pruneRecentPublishAcks();
  }

  private messagePublishRefCandidate(
    item: Pick<PublishPayload, "body">,
  ): { messageId: string; channelId: string } | undefined {
    const body = item.body as unknown as Record<string, unknown>;
    if (body?.type !== "MESSAGE") {
      return undefined;
    }
    if (
      typeof body.messageId !== "string" ||
      !body.messageId.trim() ||
      typeof body.channelId !== "string" ||
      !body.channelId.trim()
    ) {
      return undefined;
    }

    return { messageId: body.messageId, channelId: body.channelId };
  }

  private existingMessageAckFromRef(
    guildId: GuildId,
    item: Pick<PublishPayload, "author" | "clientEventId">,
    channelId: string,
    ref: SerializableMessageRef | null | undefined,
  ): PublishAck | undefined {
    if (!ref) {
      return undefined;
    }
    if (ref.authorId !== item.author || ref.channelId !== channelId) {
      return undefined;
    }
    if (
      typeof ref.eventId !== "string" ||
      !ref.eventId.trim() ||
      !Number.isFinite(ref.seq)
    ) {
      return undefined;
    }

    return {
      clientEventId: item.clientEventId,
      guildId,
      eventId: ref.eventId,
      seq: Number(ref.seq),
    };
  }

  private async lookupExistingMessageAck(
    guildId: GuildId,
    item: Pick<PublishPayload, "body" | "author" | "clientEventId">,
  ): Promise<PublishAck | undefined> {
    if (!this.store.getMessageRef) {
      return undefined;
    }

    const candidate = this.messagePublishRefCandidate(item);
    if (!candidate) {
      return undefined;
    }

    const ref = await this.store.getMessageRef({
      guildId,
      messageId: candidate.messageId,
    });
    return this.existingMessageAckFromRef(
      guildId,
      item,
      candidate.channelId,
      ref,
    );
  }

  private async lookupExistingMessageAcks(
    guildId: GuildId,
    items: IndexedPublishPayload[],
  ): Promise<Map<number, PublishAck>> {
    const existing = new Map<number, PublishAck>();
    if (!this.store.getMessageRef && !this.store.getMessageRefs) {
      return existing;
    }

    const candidates: Array<{
      item: IndexedPublishPayload;
      messageId: string;
      channelId: string;
    }> = [];
    const messageIds: string[] = [];
    for (const item of items) {
      const candidate = this.messagePublishRefCandidate(item);
      if (!candidate) {
        continue;
      }
      candidates.push({
        item,
        messageId: candidate.messageId,
        channelId: candidate.channelId,
      });
      messageIds.push(candidate.messageId);
    }
    if (candidates.length === 0) {
      return existing;
    }

    let refs: Map<string, SerializableMessageRef>;
    if (this.store.getMessageRefs) {
      refs = await this.store.getMessageRefs({ guildId, messageIds });
    } else {
      refs = new Map<string, SerializableMessageRef>();
      const uniqueMessageIds = Array.from(new Set(messageIds));
      const resolved = await Promise.all(
        uniqueMessageIds.map(async (messageId) => {
          const ref = await this.store.getMessageRef?.({ guildId, messageId });
          return { messageId, ref };
        }),
      );
      for (const { messageId, ref } of resolved) {
        if (ref) {
          refs.set(messageId, ref);
        }
      }
    }

    for (const candidate of candidates) {
      const ack = this.existingMessageAckFromRef(
        guildId,
        candidate.item,
        candidate.channelId,
        refs.get(candidate.messageId),
      );
      if (ack) {
        existing.set(candidate.item.index, ack);
      }
    }

    return existing;
  }

  private async appendSequencedEventsBatch(
    items: IndexedPublishPayload[],
    socket?: WebSocket,
    batchId?: string,
  ): Promise<PublishBatchResult[]> {
    const results: PublishBatchResult[] = [];
    const sendError = (
      item: IndexedPublishPayload,
      code: string,
      message: string,
    ) => {
      if (socket) {
        this.sendFrame(socket, "ERROR", {
          code,
          message,
          batchId,
          clientEventId: item.clientEventId,
        });
      }
    };
    const grouped = new Map<GuildId, IndexedPublishPayload[]>();

    for (const item of items) {
      const body = item?.body;
      const targetGuildId = body?.guildId;
      if (typeof targetGuildId !== "string" || !targetGuildId.trim()) {
        sendError(item, "VALIDATION_FAILED", "Event body requires a guildId");
        continue;
      }
      if (body.type === "CHECKPOINT") {
        sendError(
          item,
          "VALIDATION_FAILED",
          "CHECKPOINT events are relay-maintained and cannot be published by clients",
        );
        continue;
      }

      if (
        typeof item.author !== "string" ||
        !item.author.trim() ||
        typeof item.signature !== "string" ||
        !item.signature.trim() ||
        !Number.isFinite(item.createdAt)
      ) {
        sendError(
          item,
          "VALIDATION_FAILED",
          "Publish requires author, signature, and createdAt",
        );
        continue;
      }

      if (
        !verifyObject(
          item.author,
          { body, author: item.author, createdAt: item.createdAt },
          item.signature,
        )
      ) {
        sendError(item, "INVALID_SIGNATURE", "Signature verification failed");
        continue;
      }

      const replay = this.lookupRecentPublishAck(targetGuildId, item);
      if (replay.kind === "mismatch") {
        sendError(item, "VALIDATION_FAILED", "clientEventId payload mismatch");
        continue;
      }
      if (replay.kind === "hit") {
        results.push({ index: item.index, ack: replay.ack });
        continue;
      }

      const group = grouped.get(targetGuildId) || [];
      group.push({ ...item, signatureVerified: true });
      grouped.set(targetGuildId, group);
    }

    for (const [targetGuildId, group] of grouped) {
      const projectedBytes = group.reduce(
        (sum, item) => sum + this.estimatedPublishBytes(item.body),
        0,
      );
      const storageGate = await this.checkStorageWriteGate(projectedBytes);
      if (!storageGate.ok) {
        for (const item of group) {
          sendError(
            item,
            "RELAY_STORAGE_FULL",
            storageGate.message ?? "Relay storage hard limit reached",
          );
        }
        continue;
      }

      const groupResults = await this.withGuildMutex(
        targetGuildId,
        async () => {
          const accepted: PublishBatchResult[] = [];
          const acceptedEvents: GuildEvent[] = [];
          const newlyDurableAcks: Array<{
            item: IndexedPublishPayload;
            ack: PublishAck;
          }> = [];
          let lastEvent = await this.store.getLastEvent(targetGuildId);
          let nextSeq = lastEvent ? lastEvent.seq + 1 : 0;
          let prevHash = lastEvent ? lastEvent.id : null;
          let state: GuildState | undefined;
          let checkpointEvent: GuildEvent | undefined =
            this.checkpointCache.get(targetGuildId);

          if (nextSeq > 0) {
            state = this.stateCache.get(targetGuildId);
            if (!state || state.headSeq !== nextSeq - 1) {
              let history = this.store.getReplaySnapshotEvents
                ? buildReplaySnapshotEvents(
                    targetGuildId,
                    await this.store.getReplaySnapshotEvents({
                      guildId: targetGuildId,
                      limit: this.snapshotEventLimit,
                    }),
                  )
                : await this.store.getLog(targetGuildId);
              const canReplayFromFirstEvent =
                history[0]?.seq === 0 ||
                (history[0]?.body.type === "CHECKPOINT" &&
                  isValidCheckpointEvent(history[0]));
              if (!canReplayFromFirstEvent) {
                history = await this.store.getLog(targetGuildId);
              }
              if (history.length === 0) {
                for (const item of group) {
                  sendError(
                    item,
                    "VALIDATION_FAILED",
                    "Missing history for non-genesis event",
                  );
                }
                return accepted;
              }

              const rebuilt = rebuildStateFromEvents(history);
              state = rebuilt.state;
              checkpointEvent = rebuilt.checkpointEvent;
            }
          }

          const duplicateAcks = await this.lookupExistingMessageAcks(
            targetGuildId,
            group,
          );

          for (const item of group) {
            const { body, author, signature, createdAt } = item;
            const replay = this.lookupRecentPublishAck(targetGuildId, item);
            if (replay.kind === "mismatch") {
              sendError(
                item,
                "VALIDATION_FAILED",
                "clientEventId payload mismatch",
              );
              continue;
            }
            if (replay.kind === "hit") {
              accepted.push({ index: item.index, ack: replay.ack });
              continue;
            }
            const duplicateAck = duplicateAcks.get(item.index);
            if (duplicateAck) {
              this.rememberRecentPublishAck(targetGuildId, item, duplicateAck);
              accepted.push({ index: item.index, ack: duplicateAck });
              continue;
            }
            const fullEvent: GuildEvent = {
              id: "",
              seq: nextSeq,
              prevHash,
              createdAt,
              author,
              body,
              signature,
            };
            fullEvent.id = computeEventId(fullEvent);

            if (
              !item.signatureVerified &&
              !verifyObject(author, { body, author, createdAt }, signature)
            ) {
              console.error(`Invalid signature for event ${fullEvent.id}`);
              sendError(
                item,
                "INVALID_SIGNATURE",
                "Signature verification failed",
              );
              continue;
            }

            if (nextSeq > 0) {
              if (!state) {
                sendError(
                  item,
                  "VALIDATION_FAILED",
                  "Missing state for non-genesis event",
                );
                continue;
              }

              try {
                if (this.canFastValidateOpenMessage(state, fullEvent)) {
                  state = this.applyFastOpenMessage(state, fullEvent);
                } else {
                  const validationState = await this.hydrateValidationIndexes(
                    state,
                    fullEvent,
                  );
                  validateEvent(validationState, fullEvent);
                  state = applyEvent(validationState, fullEvent, {
                    mutable: true,
                  });
                }
                if (
                  fullEvent.body.type === "CHECKPOINT" &&
                  isValidCheckpointEvent(fullEvent)
                ) {
                  checkpointEvent = fullEvent;
                }
              } catch (e: any) {
                console.error(
                  `Validation failed for guild ${targetGuildId}: ${e.message}`,
                );
                console.error(`Event body:`, JSON.stringify(body));
                sendError(
                  item,
                  "VALIDATION_FAILED",
                  this.clientSafeValidationError(e?.message),
                );
                continue;
              }
            } else {
              if (body.type !== "GUILD_CREATE") {
                console.error(
                  "Validation failed: First event must be GUILD_CREATE",
                );
                sendError(
                  item,
                  "VALIDATION_FAILED",
                  "First event must be GUILD_CREATE",
                );
                continue;
              }
              state = createInitialState(fullEvent);
              checkpointEvent = undefined;
            }

            acceptedEvents.push(fullEvent);
            const ack: PublishAck = {
              clientEventId: item.clientEventId,
              guildId: targetGuildId,
              eventId: fullEvent.id,
              seq: fullEvent.seq,
            };
            accepted.push({
              index: item.index,
              ack,
              event: fullEvent,
            });
            newlyDurableAcks.push({ item, ack });
            lastEvent = fullEvent;
            nextSeq = lastEvent.seq + 1;
            prevHash = lastEvent.id;
          }

          if (acceptedEvents.length > 0) {
            try {
              if (this.store.appendEvents) {
                await this.store.appendEvents(targetGuildId, acceptedEvents);
              } else {
                for (const event of acceptedEvents) {
                  await this.store.append(targetGuildId, event);
                }
              }
              this.storageUnestimatedWrittenBytes += acceptedEvents.reduce(
                (sum, event) => sum + this.estimatedPublishBytes(event),
                0,
              );
            } catch (error) {
              this.stateCache.delete(targetGuildId);
              this.checkpointCache.delete(targetGuildId);
              throw error;
            }
            for (const durable of newlyDurableAcks) {
              this.rememberRecentPublishAck(
                targetGuildId,
                durable.item,
                durable.ack,
              );
            }
            if (state) {
              this.cacheGuildState(targetGuildId, state, checkpointEvent);
            }
            this.rememberRecentKnownEvents(targetGuildId, acceptedEvents);
            this.ensurePubSubHostedGuildReplication(targetGuildId);
            this.scheduleBroadcastBatch(targetGuildId, acceptedEvents);
          }

          return accepted;
        },
      );
      results.push(...groupResults);
    }

    return results;
  }

  private async appendSequencedEvent(
    p: PublishPayload,
    socket?: WebSocket,
    options: { suppressAck?: boolean } = {},
  ): Promise<GuildEvent | undefined> {
    const { body, author, signature, createdAt, clientEventId } = p;
    const targetGuildId =
      body && typeof body === "object" && !Array.isArray(body)
        ? (body as unknown as Record<string, unknown>).guildId
        : undefined;
    const sendError = (code: string, message: string) => {
      if (socket) {
        this.sendFrame(socket, "ERROR", { code, message, clientEventId });
      }
    };

    if (typeof targetGuildId !== "string" || !targetGuildId.trim()) {
      sendError("VALIDATION_FAILED", "Event body requires a guildId");
      return undefined;
    }

    if (body.type === "CHECKPOINT") {
      sendError(
        "VALIDATION_FAILED",
        "CHECKPOINT events are relay-maintained and cannot be published by clients",
      );
      return undefined;
    }

    const storageGate = await this.checkStorageWriteGate(
      this.estimatedPublishBytes(body),
    );
    if (!storageGate.ok) {
      sendError(
        "RELAY_STORAGE_FULL",
        storageGate.message ?? "Relay storage hard limit reached",
      );
      return undefined;
    }

    return await this.withGuildMutex(targetGuildId, async () => {
      const replay = this.lookupRecentPublishAck(targetGuildId, p);
      if (replay.kind === "mismatch") {
        sendError("VALIDATION_FAILED", "clientEventId payload mismatch");
        return undefined;
      }
      if (replay.kind === "hit") {
        if (!options.suppressAck && socket) {
          this.sendFrame(socket, "PUB_ACK", replay.ack);
        }
        return undefined;
      }
      const duplicateAck = await this.lookupExistingMessageAck(
        targetGuildId,
        p,
      );
      if (duplicateAck) {
        this.rememberRecentPublishAck(targetGuildId, p, duplicateAck);
        if (!options.suppressAck && socket) {
          this.sendFrame(socket, "PUB_ACK", duplicateAck);
        }
        return undefined;
      }

      const lastEvent = await this.store.getLastEvent(targetGuildId);

      const seq = lastEvent ? lastEvent.seq + 1 : 0;
      const prevHash = lastEvent ? lastEvent.id : null;

      const fullEvent: GuildEvent = {
        id: "",
        seq,
        prevHash,
        createdAt,
        author,
        body,
        signature,
      };

      fullEvent.id = computeEventId(fullEvent);

      const unsignedForSig = { body, author, createdAt };

      if (!verifyObject(author, unsignedForSig, signature)) {
        console.error(`Invalid signature for event ${fullEvent.id}`);
        sendError("INVALID_SIGNATURE", "Signature verification failed");
        return undefined;
      }

      if (seq > 0) {
        let state = this.stateCache.get(targetGuildId);

        if (!state || state.headSeq !== seq - 1) {
          let history = this.store.getReplaySnapshotEvents
            ? buildReplaySnapshotEvents(
                targetGuildId,
                await this.store.getReplaySnapshotEvents({
                  guildId: targetGuildId,
                  limit: this.snapshotEventLimit,
                }),
              )
            : await this.store.getLog(targetGuildId);
          const canReplayFromFirstEvent =
            history[0]?.seq === 0 ||
            (history[0]?.body.type === "CHECKPOINT" &&
              isValidCheckpointEvent(history[0]));
          if (!canReplayFromFirstEvent) {
            history = await this.store.getLog(targetGuildId);
          }
          if (history.length === 0) {
            console.error(
              "Validation error: Missing history for non-genesis event",
            );
            return undefined;
          }

          const rebuilt = rebuildStateFromEvents(history);
          state = rebuilt.state;
          this.cacheGuildState(targetGuildId, state, rebuilt.checkpointEvent);
        }

        try {
          const fastMessage = this.canFastValidateOpenMessage(state, fullEvent);
          const validationState = fastMessage
            ? state
            : await this.hydrateValidationIndexes(state, fullEvent);
          if (!fastMessage) {
            validateEvent(validationState, fullEvent);
          }
          // Optimistically apply to cache
          const newState = fastMessage
            ? this.applyFastOpenMessage(validationState, fullEvent)
            : applyEvent(validationState, fullEvent, { mutable: true });
          this.cacheGuildState(targetGuildId, newState);
          if (
            fullEvent.body.type === "CHECKPOINT" &&
            isValidCheckpointEvent(fullEvent)
          ) {
            this.checkpointCache.set(targetGuildId, fullEvent);
          }
        } catch (e: any) {
          console.error(
            `Validation failed for guild ${targetGuildId}: ${e.message}`,
          );
          console.error(`Event body:`, JSON.stringify(body));
          sendError(
            "VALIDATION_FAILED",
            this.clientSafeValidationError(e?.message),
          );
          return undefined;
        }
      } else {
        if (body.type !== "GUILD_CREATE") {
          console.error("Validation failed: First event must be GUILD_CREATE");
          sendError("VALIDATION_FAILED", "First event must be GUILD_CREATE");
          return undefined;
        }
        // Initialize cache for new guild
        const state = createInitialState(fullEvent);
        this.cacheGuildState(targetGuildId, state);
        this.checkpointCache.delete(targetGuildId);
      }

      await this.store.append(targetGuildId, fullEvent);
      this.storageUnestimatedWrittenBytes +=
        this.estimatedPublishBytes(fullEvent);
      this.rememberRecentKnownEvents(targetGuildId, [fullEvent]);
      this.ensurePubSubHostedGuildReplication(targetGuildId);
      // console.log(`Relay appended event ${fullEvent.id} type ${body.type} seq ${fullEvent.seq}`);

      if (!options.suppressAck) {
        if (socket) {
          const ack: PublishAck = {
            clientEventId,
            guildId: targetGuildId,
            eventId: fullEvent.id,
            seq: fullEvent.seq,
          };
          this.rememberRecentPublishAck(targetGuildId, p, ack);
          this.sendFrame(socket, "PUB_ACK", ack);
        }
      } else {
        this.rememberRecentPublishAck(targetGuildId, p, {
          clientEventId,
          guildId: targetGuildId,
          eventId: fullEvent.id,
          seq: fullEvent.seq,
        });
      }
      this.scheduleBroadcast(targetGuildId, fullEvent);
      return fullEvent;
    });
  }

  private async hydrateValidationIndexes(
    state: GuildState,
    event: GuildEvent,
  ): Promise<GuildState> {
    const memberState = await this.hydrateValidationMembers(state, event);
    return await this.hydrateValidationMessageRefs(memberState, event);
  }

  private canFastValidateOpenMessage(state: GuildState, event: GuildEvent) {
    const body = event.body as unknown as Record<string, unknown>;
    if (body.type !== "MESSAGE") {
      return false;
    }
    if (state.access === "private" || state.bans.size > 0) {
      return false;
    }
    if (
      state.policies?.posting === "members" &&
      !state.members.has(event.author)
    ) {
      return false;
    }
    if (typeof body.channelId !== "string" || !body.channelId.trim()) {
      return false;
    }
    const channel = state.channels.get(body.channelId);
    if (
      !channel ||
      (Array.isArray(channel.permissionOverwrites) &&
        channel.permissionOverwrites.length > 0)
    ) {
      return false;
    }
    const messageId =
      typeof body.messageId === "string" && body.messageId.trim()
        ? body.messageId
        : event.id;
    return !state.messages.has(messageId);
  }

  private applyFastOpenMessage(state: GuildState, event: GuildEvent) {
    const body = event.body as unknown as Record<string, unknown>;
    const messageId =
      typeof body.messageId === "string" && body.messageId.trim()
        ? body.messageId
        : event.id;
    state.headSeq = event.seq;
    state.headHash = event.id;
    state.messages.set(messageId, {
      channelId: typeof body.channelId === "string" ? body.channelId : "",
      authorId: event.author,
      eventId: event.id,
      seq: event.seq,
    });
    return state;
  }

  private async hydrateValidationMembers(
    state: GuildState,
    event: GuildEvent,
  ): Promise<GuildState> {
    if (!this.store.getMember) {
      return state;
    }

    const body = event.body as Record<string, any>;
    const userIds = new Set<string>([event.author]);
    for (const key of ["userId", "targetUserId", "memberId", "authorId"]) {
      if (typeof body[key] === "string" && body[key].trim()) {
        userIds.add(body[key]);
      }
    }

    let nextMembers: Map<string, Member> | null = null;
    for (const userId of userIds) {
      if (state.members.has(userId)) {
        continue;
      }

      const member = await this.store.getMember({
        guildId: state.guildId,
        userId,
      });
      if (!member) {
        continue;
      }

      if (!nextMembers) {
        nextMembers = new Map(state.members);
      }
      nextMembers.set(userId, this.deserializeMemberForState(member));
    }

    return nextMembers ? { ...state, members: nextMembers } : state;
  }

  private deserializeMemberForState(member: SerializableMember): Member {
    return {
      ...member,
      roles: new Set(
        Array.isArray(member.roles)
          ? member.roles
          : Array.from(member.roles ?? []),
      ),
    };
  }

  private async hydrateValidationMessageRefs(
    state: GuildState,
    event: GuildEvent,
  ): Promise<GuildState> {
    if (!this.store.getMessageRef) {
      return state;
    }

    const body = event.body as Record<string, any>;
    const messageIds = new Set<string>();
    if (body.type === "MESSAGE") {
      const messageId =
        typeof body.messageId === "string" && body.messageId.trim()
          ? body.messageId
          : event.id;
      messageIds.add(messageId);
    } else if (
      body.type === "EDIT_MESSAGE" ||
      body.type === "DELETE_MESSAGE" ||
      body.type === "REACTION_ADD" ||
      body.type === "REACTION_REMOVE"
    ) {
      if (typeof body.messageId === "string" && body.messageId.trim()) {
        messageIds.add(body.messageId);
      }
    }

    const target =
      body.target && typeof body.target === "object"
        ? (body.target as Record<string, any>)
        : undefined;
    if (
      (body.type === "APP_OBJECT_UPSERT" ||
        body.type === "APP_OBJECT_DELETE") &&
      typeof target?.messageId === "string" &&
      target.messageId.trim()
    ) {
      messageIds.add(target.messageId);
    }

    if (messageIds.size === 0) {
      return state;
    }

    let hydrated: GuildState | null = null;
    const ensureHydrated = () => {
      if (!hydrated) {
        hydrated = {
          ...state,
          messages: new Map(state.messages),
        };
      }
      return hydrated;
    };

    for (const messageId of messageIds) {
      if (state.messages.has(messageId)) {
        continue;
      }
      const ref: SerializableMessageRef | null = await this.store.getMessageRef(
        { guildId: state.guildId, messageId },
      );
      if (ref) {
        ensureHydrated().messages.set(messageId, ref);
      }
    }

    return hydrated ?? state;
  }

  private async runOnEventAppended(event: GuildEvent, socket?: WebSocket) {
    await this.runOnEventsAppended([event], socket);
  }

  private async runOnEventsAppended(events: GuildEvent[], socket?: WebSocket) {
    if (events.length === 0) {
      return;
    }
    const appendedGuildIds = new Set<GuildId>();
    for (const event of events) {
      const guildId = event.body.guildId;
      if (guildId) {
        appendedGuildIds.add(guildId);
      }
    }
    for (const plugin of this.plugins) {
      try {
        if (plugin.onEventsAppended) {
          await plugin.onEventsAppended({ events, socket }, this.pluginCtx);
          continue;
        }
        if (plugin.onEventAppended) {
          for (const event of events) {
            await plugin.onEventAppended({ event, socket }, this.pluginCtx);
          }
        }
      } catch (e: any) {
        console.error(`Relay plugin ${plugin.name} failed in append hook:`, e);
      }
    }
    for (const guildId of appendedGuildIds) {
      await this.publishRelayHeadGossip(guildId);
    }
  }

  private cacheGuildState(
    guildId: GuildId,
    state: GuildState,
    checkpointEvent?: GuildEvent,
  ) {
    if (this.stateCache.has(guildId)) {
      this.stateCache.delete(guildId);
    }
    this.trimCachedMessageRefs(state);
    this.stateCache.set(guildId, state);
    if (checkpointEvent !== undefined) {
      this.checkpointCache.set(guildId, checkpointEvent);
    }

    while (this.stateCache.size > this.maxStateCacheGuilds) {
      const oldestGuildId = this.stateCache.keys().next().value as
        | GuildId
        | undefined;
      if (!oldestGuildId) {
        break;
      }
      this.stateCache.delete(oldestGuildId);
      this.checkpointCache.delete(oldestGuildId);
    }
  }

  private trimCachedMessageRefs(state: GuildState) {
    if (
      !Number.isFinite(this.maxCachedMessageRefs) ||
      this.maxCachedMessageRefs < 0
    ) {
      return;
    }

    while (state.messages.size > this.maxCachedMessageRefs) {
      const oldestMessageId = state.messages.keys().next().value as
        | string
        | undefined;
      if (!oldestMessageId) {
        break;
      }
      state.messages.delete(oldestMessageId);
    }
  }

  private clearFanoutQueue(socket: WebSocket) {
    const queue = this.fanoutQueues.get(socket);
    if (queue) {
      queue.pending = false;
    }
    this.fanoutQueues.delete(socket);
  }

  private scheduleFanoutDrainLoop() {
    if (this.fanoutDrainScheduled || this.fanoutDrainActive) {
      return;
    }
    this.fanoutDrainScheduled = true;
    if (this.fanoutDrainDelayMs <= 0) {
      this.fanoutDrainImmediate = setImmediate(() => {
        this.fanoutDrainImmediate = undefined;
        this.fanoutDrainScheduled = false;
        this.drainFanoutQueues();
      });
      return;
    }
    this.fanoutDrainTimer = setTimeout(() => {
      this.fanoutDrainTimer = undefined;
      this.fanoutDrainScheduled = false;
      this.drainFanoutQueues();
    }, this.fanoutDrainDelayMs);
  }

  private scheduleFanoutDrain(socket: WebSocket) {
    const queue = this.fanoutQueues.get(socket);
    if (!queue || queue.pending) {
      return;
    }
    queue.pending = true;
    this.fanoutDrainPending.push(socket);
    this.scheduleFanoutDrainLoop();
  }

  private compactFanoutDrainPending() {
    if (this.fanoutDrainPendingHead === 0) {
      return;
    }
    if (this.fanoutDrainPendingHead >= this.fanoutDrainPending.length) {
      this.fanoutDrainPending = [];
      this.fanoutDrainPendingHead = 0;
      return;
    }
    if (
      this.fanoutDrainPendingHead < 1024 &&
      this.fanoutDrainPendingHead * 2 < this.fanoutDrainPending.length
    ) {
      return;
    }
    this.fanoutDrainPending = this.fanoutDrainPending.slice(
      this.fanoutDrainPendingHead,
    );
    this.fanoutDrainPendingHead = 0;
  }

  private drainFanoutQueues() {
    if (this.fanoutDrainActive) {
      return;
    }
    this.fanoutDrainActive = true;
    try {
      let processed = 0;
      while (
        this.fanoutDrainPendingHead < this.fanoutDrainPending.length &&
        processed < this.maxFanoutDrainSocketsPerTick
      ) {
        const socket = this.fanoutDrainPending[this.fanoutDrainPendingHead++];
        if (!socket) {
          break;
        }
        const queue = this.fanoutQueues.get(socket);
        if (!queue) {
          continue;
        }
        queue.pending = false;
        this.drainFanoutQueue(socket, queue);
        processed += 1;
      }
    } finally {
      this.fanoutDrainActive = false;
    }
    this.compactFanoutDrainPending();
    if (this.fanoutDrainPendingHead < this.fanoutDrainPending.length) {
      this.scheduleFanoutDrainLoop();
    }
  }

  private drainFanoutQueue(
    socket: WebSocket,
    queue = this.fanoutQueues.get(socket),
  ) {
    if (!queue || queue.head >= queue.frames.length) {
      this.clearFanoutQueue(socket);
      return;
    }
    if (socket.readyState !== WebSocket.OPEN) {
      this.clearFanoutQueue(socket);
      return;
    }

    const transport = transportSocket(socket);
    transport?.cork?.();
    try {
      while (
        queue.head < queue.frames.length &&
        socket.bufferedAmount <= this.maxSocketBufferedBytes
      ) {
        const frame = queue.frames[queue.head++];
        if (frame) {
          socket.send(frame);
        }
      }
    } finally {
      transport?.uncork?.();
    }

    if (queue.head < queue.frames.length) {
      if (queue.head > 128 && queue.head * 2 > queue.frames.length) {
        queue.frames = queue.frames.slice(queue.head);
        queue.head = 0;
      }
      this.scheduleFanoutDrain(socket);
    } else {
      queue.frames.length = 0;
      queue.head = 0;
      queue.pending = false;
      this.fanoutQueues.delete(socket);
    }
  }

  private encodeOutboundFrame(
    frame: string,
    wireFormat: RelayWireFormat,
  ): OutboundFrame {
    return encodeCgpWireFrame(frame, wireFormat);
  }

  private encodeOutboundPayload(
    kind: string,
    payload: unknown,
    wireFormat: RelayWireFormat,
  ): OutboundFrame {
    return encodeCgpFrame(kind, payload, wireFormat);
  }

  private sendFrame(socket: WebSocket, kind: string, payload: unknown) {
    if (this.closing) {
      return false;
    }
    if (socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    const wireFormat = this.socketWireFormats.get(socket) ?? this.wireFormat;
    socket.send(encodeCgpFrame(kind, payload, wireFormat));
    return true;
  }

  private sendFanoutFrame(socket: WebSocket, frame: OutboundFrame) {
    if (this.closing) {
      return false;
    }
    if (socket.readyState !== WebSocket.OPEN) {
      return false;
    }

    const existingQueue = this.fanoutQueues.get(socket);
    const existingQueueLength = existingQueue
      ? existingQueue.frames.length - existingQueue.head
      : 0;
    if (
      (!existingQueue || existingQueueLength === 0) &&
      socket.bufferedAmount <= this.maxSocketBufferedBytes
    ) {
      socket.send(frame);
      return true;
    }

    const queue = existingQueue ?? { frames: [], head: 0, pending: false };
    if (queue.frames.length - queue.head >= this.maxQueuedFramesPerSocket) {
      this.clearFanoutQueue(socket);
      this.removeSocketSubscriptions(socket);
      socket.close(1013, "Slow consumer");
      return false;
    }

    queue.frames.push(frame);
    this.fanoutQueues.set(socket, queue);
    this.scheduleFanoutDrain(socket);
    return true;
  }

  private sendFanoutTextFrame(
    socket: WebSocket,
    frame: string,
    encodedByWireFormat?: FanoutFrameCache,
  ) {
    const wireFormat = this.socketWireFormats.get(socket) ?? this.wireFormat;
    let outboundFrame = encodedByWireFormat?.[wireFormat];
    if (!outboundFrame) {
      outboundFrame = this.encodeOutboundFrame(frame, wireFormat);
      if (encodedByWireFormat) {
        encodedByWireFormat[wireFormat] = outboundFrame;
      }
    }
    return this.sendFanoutFrame(socket, outboundFrame);
  }

  private sendFanoutPayloadFrame(
    socket: WebSocket,
    kind: string,
    payload: unknown,
    encodedByWireFormat?: FanoutFrameCache,
  ) {
    const wireFormat = this.socketWireFormats.get(socket) ?? this.wireFormat;
    let outboundFrame = encodedByWireFormat?.[wireFormat];
    if (!outboundFrame) {
      outboundFrame = this.encodeOutboundPayload(kind, payload, wireFormat);
      if (encodedByWireFormat) {
        encodedByWireFormat[wireFormat] = outboundFrame;
      }
    }
    return this.sendFanoutFrame(socket, outboundFrame);
  }

  private mergeSubscriptionCandidates(
    target: Map<WebSocket, Set<string>>,
    source: Map<WebSocket, Set<string>> | undefined,
  ) {
    if (!source) {
      return;
    }
    for (const [socket, subIds] of source) {
      const existing = target.get(socket);
      if (!existing) {
        target.set(socket, subIds);
        continue;
      }
      if (existing === subIds) {
        continue;
      }
      const merged = new Set(existing);
      for (const subId of subIds) {
        merged.add(subId);
      }
      target.set(socket, merged);
    }
  }

  private nonEmptySubscriptionCandidates(
    source: Map<WebSocket, Set<string>> | undefined,
  ) {
    return source && source.size > 0 ? source : undefined;
  }

  private candidateSubscriptionsForEvent(
    guildId: GuildId,
    state: GuildState | undefined,
    event: GuildEvent,
  ) {
    const channelId = state ? eventChannelId(state, event) : undefined;
    if (!channelId) {
      return this.nonEmptySubscriptionCandidates(
        this.guildSubscriptions.get(guildId),
      );
    }

    const allChannelCandidates = this.nonEmptySubscriptionCandidates(
      this.guildAllChannelSubscriptions.get(guildId),
    );
    const channelCandidates = this.nonEmptySubscriptionCandidates(
      this.guildChannelSubscriptions.get(guildId)?.get(channelId),
    );
    if (!allChannelCandidates) {
      return channelCandidates;
    }
    if (!channelCandidates) {
      return allChannelCandidates;
    }

    const candidates = new Map<WebSocket, Set<string>>();
    this.mergeSubscriptionCandidates(candidates, allChannelCandidates);
    this.mergeSubscriptionCandidates(candidates, channelCandidates);
    return candidates;
  }

  private candidateSubscriptionsForEvents(
    guildId: GuildId,
    state: GuildState,
    events: GuildEvent[],
  ) {
    const channelIds = new Set<ChannelId>();
    let hasGuildWideEvent = false;

    for (const event of events) {
      const channelId = eventChannelId(state, event);
      if (!channelId) {
        hasGuildWideEvent = true;
        break;
      }
      channelIds.add(channelId);
    }

    if (hasGuildWideEvent) {
      return this.nonEmptySubscriptionCandidates(
        this.guildSubscriptions.get(guildId),
      );
    }

    const allChannelCandidates = this.nonEmptySubscriptionCandidates(
      this.guildAllChannelSubscriptions.get(guildId),
    );
    const channelSocketsByGuild = this.guildChannelSubscriptions.get(guildId);
    if (channelIds.size === 0) {
      return allChannelCandidates;
    }

    if (channelIds.size === 1) {
      const [channelId] = channelIds;
      const channelCandidates = this.nonEmptySubscriptionCandidates(
        channelSocketsByGuild?.get(channelId),
      );
      if (!allChannelCandidates) {
        return channelCandidates;
      }
      if (!channelCandidates) {
        return allChannelCandidates;
      }
    }

    const candidates = new Map<WebSocket, Set<string>>();
    this.mergeSubscriptionCandidates(candidates, allChannelCandidates);
    for (const channelId of channelIds) {
      this.mergeSubscriptionCandidates(
        candidates,
        channelSocketsByGuild?.get(channelId),
      );
    }
    return candidates.size > 0 ? candidates : undefined;
  }

  private scheduleSocketMapFanout(
    sockets: Map<WebSocket, Set<string>> | undefined,
    deliver: (socket: WebSocket) => void,
    skip?: (socket: WebSocket) => boolean,
  ) {
    if (!sockets || sockets.size === 0) {
      return false;
    }

    const iterator = sockets.keys();
    const drain = () => {
      let processed = 0;
      while (processed < this.maxFanoutDrainSocketsPerTick) {
        const next = iterator.next();
        if (next.done) {
          return;
        }
        processed += 1;
        const socket = next.value;
        if (skip?.(socket)) {
          continue;
        }
        deliver(socket);
      }
      this.scheduleBackgroundImmediate(drain);
    };
    this.scheduleBackgroundImmediate(drain);
    return true;
  }

  private schedulePublicChannelSocketFanout(
    guildId: GuildId,
    channelId: ChannelId,
    deliver: (socket: WebSocket) => void,
  ) {
    const allChannelSockets = this.guildAllChannelSubscriptions.get(guildId);
    const channelSockets = this.guildChannelSubscriptions
      .get(guildId)
      ?.get(channelId);
    if (
      (!allChannelSockets || allChannelSockets.size === 0) &&
      (!channelSockets || channelSockets.size === 0)
    ) {
      return false;
    }
    this.scheduleSocketMapFanout(allChannelSockets, deliver);
    this.scheduleSocketMapFanout(
      channelSockets,
      deliver,
      allChannelSockets ? (socket) => allChannelSockets.has(socket) : undefined,
    );
    return true;
  }

  private publicChannelIdForFastPath(state: GuildState, event: GuildEvent) {
    if (state.access === "private" || state.bans.size > 0) {
      return undefined;
    }
    const channelId = eventChannelId(state, event);
    if (!channelId) {
      return undefined;
    }
    const channel = state.channels.get(channelId);
    if (
      !channel ||
      (Array.isArray(channel.permissionOverwrites) &&
        channel.permissionOverwrites.length > 0)
    ) {
      return undefined;
    }
    return channelId;
  }

  private canUsePublicChannelBatchFastPath(
    state: GuildState,
    events: GuildEvent[],
  ) {
    if (
      state.access === "private" ||
      state.bans.size > 0 ||
      events.length === 0
    ) {
      return false;
    }

    let channelId: ChannelId | undefined;
    for (const event of events) {
      const eventChannel = eventChannelId(state, event);
      if (!eventChannel) {
        return false;
      }
      if (channelId && channelId !== eventChannel) {
        return false;
      }
      channelId = eventChannel;
    }

    const channel = channelId ? state.channels.get(channelId) : undefined;
    return Boolean(
      channel &&
      (!Array.isArray(channel.permissionOverwrites) ||
        channel.permissionOverwrites.length === 0),
    );
  }

  private publicChannelEventBatchesForFastPath(
    state: GuildState,
    events: GuildEvent[],
  ) {
    if (
      state.access === "private" ||
      state.bans.size > 0 ||
      events.length === 0
    ) {
      return undefined;
    }

    const channelEvents = new Map<ChannelId, GuildEvent[]>();
    for (const event of events) {
      const channelId = eventChannelId(state, event);
      if (!channelId) {
        return undefined;
      }
      const channel = state.channels.get(channelId);
      if (
        !channel ||
        (Array.isArray(channel.permissionOverwrites) &&
          channel.permissionOverwrites.length > 0)
      ) {
        return undefined;
      }
      const bucket = channelEvents.get(channelId) ?? [];
      bucket.push(event);
      channelEvents.set(channelId, bucket);
    }

    return channelEvents.size > 0 ? channelEvents : undefined;
  }

  private canUsePublicChannelSplitBatchFastPath(
    guildId: GuildId,
    channelEvents: Map<ChannelId, GuildEvent[]>,
  ) {
    const allChannelSockets = this.guildAllChannelSubscriptions.get(guildId);
    const channelSocketsByGuild = this.guildChannelSubscriptions.get(guildId);
    if (!channelSocketsByGuild) {
      return true;
    }

    // The split path sends one ordered sub-batch per channel. If a socket has
    // multi-channel specific subscriptions, use the generic path so that
    // socket still receives a single merged frame in global event order.
    const seenSpecificSockets = new Set<WebSocket>();
    for (const channelId of channelEvents.keys()) {
      for (const [socket, subIds] of channelSocketsByGuild.get(channelId) ??
        []) {
        if (allChannelSockets?.has(socket)) {
          continue;
        }
        if (seenSpecificSockets.has(socket)) {
          return false;
        }
        const subs = this.subscriptions.get(socket);
        if (!subs) {
          continue;
        }
        for (const subId of subIds) {
          const sub = subs.get(subId);
          if (
            !sub?.channelSet ||
            sub.channelSet.size !== 1 ||
            !sub.channelSet.has(channelId)
          ) {
            return false;
          }
        }
        seenSpecificSockets.add(socket);
      }
    }
    return true;
  }

  private deliverPublicChannelSplitBatch(
    guildId: GuildId,
    events: GuildEvent[],
    channelEvents: Map<ChannelId, GuildEvent[]>,
    frame?: string,
  ) {
    const allChannelSockets = this.guildAllChannelSubscriptions.get(guildId);
    const fullBatchEncodedByWireFormat: FanoutFrameCache = {};

    if (allChannelSockets?.size) {
      this.scheduleSocketMapFanout(allChannelSockets, (socket) => {
        if (frame) {
          this.sendFanoutTextFrame(socket, frame, fullBatchEncodedByWireFormat);
        } else {
          this.sendFanoutPayloadFrame(
            socket,
            "BATCH",
            events,
            fullBatchEncodedByWireFormat,
          );
        }
      });
    }

    const channelSocketsByGuild = this.guildChannelSubscriptions.get(guildId);
    if (!channelSocketsByGuild) {
      return;
    }

    for (const [channelId, bucket] of channelEvents) {
      const channelSockets = channelSocketsByGuild.get(channelId);
      if (!channelSockets || channelSockets.size === 0) {
        continue;
      }
      const encodedByWireFormat: FanoutFrameCache = {};
      if (bucket.length === 1) {
        const event = bucket[0];
        this.scheduleSocketMapFanout(
          channelSockets,
          (socket) => {
            this.sendFanoutPayloadFrame(
              socket,
              "EVENT",
              event,
              encodedByWireFormat,
            );
          },
          allChannelSockets
            ? (socket) => allChannelSockets.has(socket)
            : undefined,
        );
      } else {
        this.scheduleSocketMapFanout(
          channelSockets,
          (socket) => {
            this.sendFanoutPayloadFrame(
              socket,
              "BATCH",
              bucket,
              encodedByWireFormat,
            );
          },
          allChannelSockets
            ? (socket) => allChannelSockets.has(socket)
            : undefined,
        );
      }
    }
  }

  private scheduleSocketFanout<T>(items: T[], deliver: (item: T) => void) {
    if (items.length === 0) {
      return;
    }
    let index = 0;
    const drain = () => {
      const end = Math.min(
        items.length,
        index + this.maxFanoutDrainSocketsPerTick,
      );
      for (; index < end; index += 1) {
        deliver(items[index]);
      }
      if (index < items.length) {
        this.scheduleBackgroundImmediate(drain);
      }
    };
    this.scheduleBackgroundImmediate(drain);
  }

  private deliverBroadcastFrame(
    guildId: GuildId,
    event: GuildEvent,
    frame?: string,
  ) {
    if (this.closing) {
      return;
    }
    const state = this.stateCache.get(guildId);
    const publicChannelId = state
      ? this.publicChannelIdForFastPath(state, event)
      : undefined;
    if (publicChannelId) {
      const encodedByWireFormat: FanoutFrameCache = {};
      if (
        !this.schedulePublicChannelSocketFanout(
          guildId,
          publicChannelId,
          (socket) => {
            frame
              ? this.sendFanoutTextFrame(socket, frame, encodedByWireFormat)
              : this.sendFanoutPayloadFrame(
                  socket,
                  "EVENT",
                  event,
                  encodedByWireFormat,
                );
          },
        )
      ) {
        return;
      }
      return;
    }
    const guildSockets = this.candidateSubscriptionsForEvent(
      guildId,
      state,
      event,
    );
    if (!guildSockets) {
      return;
    }
    const encodedByWireFormat: FanoutFrameCache = {};

    const eligibleSockets: WebSocket[] = [];
    for (const [socket, subIds] of guildSockets) {
      const subs = this.subscriptions.get(socket);
      if (!subs) {
        continue;
      }
      let shouldSend = false;
      for (const subId of subIds) {
        const sub = subs.get(subId);
        if (!sub) {
          continue;
        }
        if (
          state &&
          (!eventVisibleToReader(state, event, sub.author) ||
            !eventMatchesRequestedChannels(
              state,
              event,
              sub.channels,
              sub.channelSet,
            ))
        ) {
          continue;
        }
        shouldSend = true;
        break;
      }
      if (shouldSend) {
        eligibleSockets.push(socket);
      }
    }

    this.scheduleSocketFanout(eligibleSockets, (socket) => {
      frame
        ? this.sendFanoutTextFrame(socket, frame, encodedByWireFormat)
        : this.sendFanoutPayloadFrame(
            socket,
            "EVENT",
            event,
            encodedByWireFormat,
          );
    });
  }

  private deliverBroadcastBatchFrame(
    guildId: GuildId,
    events: GuildEvent[],
    frame?: string,
  ) {
    if (this.closing) {
      return;
    }
    if (events.length === 0) {
      return;
    }
    if (events.length === 1) {
      this.deliverBroadcastFrame(guildId, events[0]);
      return;
    }

    const state = this.stateCache.get(guildId);
    if (!state) {
      for (const event of events) {
        this.deliverBroadcastFrame(guildId, event);
      }
      return;
    }

    const fullBatchEncodedByWireFormat: FanoutFrameCache = {};
    const publicChannelBatches = this.publicChannelEventBatchesForFastPath(
      state,
      events,
    );
    if (
      publicChannelBatches &&
      this.canUsePublicChannelSplitBatchFastPath(guildId, publicChannelBatches)
    ) {
      this.deliverPublicChannelSplitBatch(
        guildId,
        events,
        publicChannelBatches,
        frame,
      );
      return;
    }

    if (this.canUsePublicChannelBatchFastPath(state, events)) {
      const channelId = eventChannelId(state, events[0]);
      if (
        !channelId ||
        !this.schedulePublicChannelSocketFanout(
          guildId,
          channelId,
          (socket) => {
            if (frame) {
              this.sendFanoutTextFrame(
                socket,
                frame,
                fullBatchEncodedByWireFormat,
              );
            } else {
              this.sendFanoutPayloadFrame(
                socket,
                "BATCH",
                events,
                fullBatchEncodedByWireFormat,
              );
            }
          },
        )
      ) {
        return;
      }
      return;
    }

    const guildSockets = this.candidateSubscriptionsForEvents(
      guildId,
      state,
      events,
    );
    if (!guildSockets) {
      return;
    }

    const socketGroups = new Map<
      string,
      {
        socket: WebSocket;
        subIds: Set<string>;
        subs: Map<string, Subscription>;
      }[]
    >();
    for (const [socket, subIds] of guildSockets) {
      const subs = this.subscriptions.get(socket);
      if (!subs) {
        continue;
      }

      const fingerprintParts: string[] = [];
      for (const subId of subIds) {
        const sub = subs.get(subId);
        if (!sub) {
          continue;
        }
        fingerprintParts.push(
          sub.fingerprint ?? this.subscriptionFingerprint(sub),
        );
      }
      if (fingerprintParts.length === 0) {
        continue;
      }
      fingerprintParts.sort();
      const fingerprint = fingerprintParts.join(";");
      const group = socketGroups.get(fingerprint) ?? [];
      group.push({ socket, subIds, subs });
      socketGroups.set(fingerprint, group);
    }

    const partialPayloadCache = new Map<string, GuildEvent[]>();
    const partialFrameEncodedCache = new Map<string, FanoutFrameCache>();
    for (const group of socketGroups.values()) {
      const first = group[0];
      if (!first) continue;

      const visibleEvents: GuildEvent[] = [];
      for (const event of events) {
        for (const subId of first.subIds) {
          const sub = first.subs.get(subId);
          if (!sub) {
            continue;
          }
          if (
            !eventVisibleToReader(state, event, sub.author) ||
            !eventMatchesRequestedChannels(
              state,
              event,
              sub.channels,
              sub.channelSet,
            )
          ) {
            continue;
          }
          visibleEvents.push(event);
          break;
        }
      }

      if (visibleEvents.length === 0) {
        continue;
      }

      let outboundPayload = events;
      let encodedByWireFormat = fullBatchEncodedByWireFormat;
      if (visibleEvents.length !== events.length) {
        const key = visibleEvents.map((event) => event.seq).join(",");
        outboundPayload = partialPayloadCache.get(key) ?? visibleEvents;
        partialPayloadCache.set(key, outboundPayload);
        encodedByWireFormat = partialFrameEncodedCache.get(key) ?? {};
        partialFrameEncodedCache.set(key, encodedByWireFormat);
      }
      this.scheduleSocketFanout(group, ({ socket }) => {
        if (frame && outboundPayload === events) {
          this.sendFanoutTextFrame(socket, frame, encodedByWireFormat);
        } else {
          this.sendFanoutPayloadFrame(
            socket,
            "BATCH",
            outboundPayload,
            encodedByWireFormat,
          );
        }
      });
    }
  }

  private deliverRelayHeadGossip(
    guildId: GuildId,
    head: RelayHead,
    quorum: RelayHeadQuorum,
  ) {
    if (this.closing) {
      return;
    }
    const guildSockets = this.guildSubscriptions.get(guildId);
    if (!guildSockets) {
      return;
    }
    const state = this.stateCache.get(guildId);
    const payload = {
      guildId,
      head,
      quorum: {
        guildId: quorum.guildId,
        validCount: quorum.validHeads.length,
        invalidCount: quorum.invalidHeads.length,
        conflictCount: quorum.conflicts.length,
        canonical: quorum.canonical,
      },
    };
    const encodedByWireFormat: FanoutFrameCache = {};

    for (const [socket, subIds] of guildSockets) {
      const subs = this.subscriptions.get(socket);
      if (!subs) {
        continue;
      }
      let shouldSend = false;
      for (const subId of subIds) {
        const sub = subs.get(subId);
        if (!sub) {
          continue;
        }
        if (state && !canReadGuild(state, sub.author)) {
          continue;
        }
        shouldSend = true;
        break;
      }
      if (shouldSend) {
        this.sendFanoutPayloadFrame(
          socket,
          "RELAY_HEAD_GOSSIP",
          payload,
          encodedByWireFormat,
        );
      }
    }
  }

  private publishPubSubEvents(
    topic: string,
    guildId: GuildId,
    events: GuildEvent[],
    frame?: string,
    options: { liveTopics?: boolean } = {},
  ) {
    if (!this.pubSubAdapter || events.length === 0) {
      return;
    }

    if (events.length > this.maxPubSubBatchEvents) {
      for (
        let index = 0;
        index < events.length;
        index += this.maxPubSubBatchEvents
      ) {
        this.publishPubSubEvents(
          topic,
          guildId,
          events.slice(index, index + this.maxPubSubBatchEvents),
          undefined,
          options,
        );
      }
      return;
    }

    const envelope: RelayPubSubEnvelope =
      events.length === 1
        ? {
            originId: this.relayInstanceId,
            guildId,
            event: events[0],
          }
        : {
            originId: this.relayInstanceId,
            guildId,
            events,
          };
    if (frame && process.env.CGP_RELAY_PUBSUB_INCLUDE_FRAME === "1") {
      envelope.frame = frame;
    }
    if (options.liveTopics !== undefined) {
      envelope.liveTopics = options.liveTopics;
    }

    void Promise.resolve(this.pubSubAdapter.publish(topic, envelope)).catch(
      (e) => {
        console.error(`Relay pubsub publish failed for topic ${topic}:`, e);
      },
    );
  }

  private publishEventToPubSub(
    guildId: GuildId,
    event: GuildEvent,
    frame?: string,
  ) {
    if (!this.pubSubAdapter) {
      return;
    }

    const publishLiveTopics =
      this.pubSubLiveTopics && this.guildSubscriptions.has(guildId);
    this.publishPubSubEvents(
      this.pubSubLogTopic(guildId),
      guildId,
      [event],
      frame,
      { liveTopics: publishLiveTopics },
    );
    if (!publishLiveTopics) {
      return;
    }
    const state = this.stateCache.get(guildId);
    const channelId = state ? eventChannelId(state, event) : undefined;
    if (!channelId) {
      this.publishPubSubEvents(
        this.pubSubTopic(guildId),
        guildId,
        [event],
        frame,
      );
      return;
    }

    this.publishPubSubEvents(
      this.pubSubAllChannelsTopic(guildId),
      guildId,
      [event],
      frame,
    );
    this.publishPubSubEvents(
      this.pubSubChannelTopic(guildId, channelId),
      guildId,
      [event],
      frame,
    );
  }

  private publishBatchToPubSub(
    guildId: GuildId,
    events: GuildEvent[],
    frame?: string,
  ) {
    if (!this.pubSubAdapter || events.length === 0) {
      return;
    }

    const publishLiveTopics =
      this.pubSubLiveTopics && this.guildSubscriptions.has(guildId);
    this.publishPubSubEvents(
      this.pubSubLogTopic(guildId),
      guildId,
      events,
      frame,
      { liveTopics: publishLiveTopics },
    );
    if (!publishLiveTopics) {
      return;
    }
    const state = this.stateCache.get(guildId);
    if (!state) {
      this.publishPubSubEvents(
        this.pubSubTopic(guildId),
        guildId,
        events,
        frame,
      );
      return;
    }

    const eventsByTopic = new Map<string, GuildEvent[]>();
    const addTopicEvent = (topic: string, event: GuildEvent) => {
      const bucket = eventsByTopic.get(topic) ?? [];
      bucket.push(event);
      eventsByTopic.set(topic, bucket);
    };

    for (const event of events) {
      const channelId = eventChannelId(state, event);
      if (!channelId) {
        addTopicEvent(this.pubSubTopic(guildId), event);
        continue;
      }
      addTopicEvent(this.pubSubAllChannelsTopic(guildId), event);
      addTopicEvent(this.pubSubChannelTopic(guildId, channelId), event);
    }

    for (const [topic, topicEvents] of eventsByTopic) {
      this.publishPubSubEvents(
        topic,
        guildId,
        topicEvents,
        topicEvents.length === events.length ? frame : undefined,
      );
    }
  }

  private broadcast(guildId: GuildId, event: GuildEvent) {
    const frame =
      process.env.CGP_RELAY_PUBSUB_INCLUDE_FRAME === "1"
        ? stringifyCgpFrame("EVENT", event)
        : undefined;
    this.deliverBroadcastFrame(guildId, event, frame);
    this.publishEventToPubSub(guildId, event, frame);
  }

  private broadcastBatch(guildId: GuildId, events: GuildEvent[]) {
    if (events.length === 0) {
      return;
    }
    if (events.length === 1) {
      this.broadcast(guildId, events[0]);
      return;
    }

    const frame =
      process.env.CGP_RELAY_PUBSUB_INCLUDE_FRAME === "1"
        ? stringifyCgpFrame("BATCH", events)
        : undefined;
    this.deliverBroadcastBatchFrame(guildId, events, frame);
    this.publishBatchToPubSub(guildId, events, frame);
  }

  private scheduleBackgroundImmediate(task: () => void) {
    const immediate = setImmediate(() => {
      this.scheduledBroadcasts.delete(immediate);
      if (!this.closing) task();
    });
    this.scheduledBroadcasts.add(immediate);
  }

  private scheduleBroadcast(guildId: GuildId, event: GuildEvent) {
    this.scheduleBackgroundImmediate(() => this.broadcast(guildId, event));
  }

  private scheduleBroadcastBatch(guildId: GuildId, events: GuildEvent[]) {
    if (events.length === 0) {
      return;
    }
    this.scheduleBackgroundImmediate(() =>
      this.broadcastBatch(guildId, events),
    );
  }

  private async withGuildMutex<T>(
    guildId: GuildId,
    task: () => Promise<T>,
  ): Promise<T> {
    const prev = this.mutexes.get(guildId) || Promise.resolve();
    const next = prev.catch(() => undefined).then(task);
    this.mutexes.set(
      guildId,
      next.then(
        () => undefined,
        () => undefined,
      ),
    );
    return await next;
  }

  public async close() {
    if (this.closePromise) {
      return this.closePromise;
    }
    this.closePromise = this.closeInternal();
    return this.closePromise;
  }

  private async closeInternal() {
    this.closing = true;
    if (this.peerCatchupTimer) {
      clearInterval(this.peerCatchupTimer);
      this.peerCatchupTimer = undefined;
    }
    clearInterval(this.pruneTimer);
    if (this.checkpointTimer) {
      clearInterval(this.checkpointTimer);
    }

    if (this.fanoutDrainTimer) {
      clearTimeout(this.fanoutDrainTimer);
      this.fanoutDrainTimer = undefined;
    }
    if (this.fanoutDrainImmediate) {
      clearImmediate(this.fanoutDrainImmediate);
      this.fanoutDrainImmediate = undefined;
    }
    for (const immediate of this.scheduledBroadcasts) {
      clearImmediate(immediate);
    }
    this.scheduledBroadcasts.clear();
    this.fanoutDrainScheduled = false;
    this.fanoutDrainPending = [];
    this.fanoutDrainPendingHead = 0;
    this.fanoutQueues.clear();

    for (const socket of this.wss.clients) {
      socket.removeAllListeners("message");
      this.removeSocketSubscriptions(socket);
      this.clearFanoutQueue(socket);
      if (
        socket.readyState === WebSocket.OPEN ||
        socket.readyState === WebSocket.CONNECTING
      ) {
        socket.close(1001, "Relay shutting down");
      }
    }

    await Promise.allSettled(Array.from(this.messageQueues.values()));
    this.messageQueues.clear();

    await new Promise<void>((resolve) => {
      let resolved = false;
      const finish = () => {
        if (!resolved) {
          resolved = true;
          resolve();
        }
      };
      const timeout = setTimeout(() => {
        for (const socket of this.wss.clients) {
          if (socket.readyState !== WebSocket.CLOSED) {
            socket.terminate();
          }
        }
        finish();
      }, 250);
      timeout.unref?.();
      try {
        this.wss.close(() => {
          clearTimeout(timeout);
          finish();
        });
      } catch {
        clearTimeout(timeout);
        finish();
      }
    });

    await new Promise<void>((resolve) => {
      let resolved = false;
      const finish = () => {
        if (!resolved) {
          resolved = true;
          resolve();
        }
      };
      const timeout = setTimeout(finish, 250);
      timeout.unref?.();
      try {
        this.httpServer.close(() => {
          clearTimeout(timeout);
          finish();
        });
      } catch {
        clearTimeout(timeout);
        finish();
      }
    });

    const unsubscribers = Array.from(this.pubSubUnsubscribers.values());
    this.pubSubUnsubscribers.clear();
    this.pubSubPendingGuilds.clear();
    for (const unsubscribe of unsubscribers) {
      try {
        await unsubscribe();
      } catch (e: any) {
        console.error("Relay pubsub unsubscribe failed during close:", e);
      }
    }
    await this.pluginsReady;
    for (const plugin of this.plugins) {
      try {
        await plugin.onClose?.(this.pluginCtx);
      } catch (e: any) {
        console.error(`Relay plugin ${plugin.name} failed to close:`, e);
      }
    }
    this.eventLoopDelay.disable();
    await this.store.close();
  }
}
