import { WebSocketServer, WebSocket, RawData } from "ws";
import { createServer, IncomingMessage, ServerResponse } from "http";
import { createReadStream } from "fs";
import { stat } from "fs/promises";
import path from "path";
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
    validateEvent,
    canReadGuild,
    canViewChannel
} from "@cgp/core";
import { LevelStore } from "./store_level";
import { HistoryQuery, MemberPageQuery, normalizeMemberPageLimit, Store } from "./store";
import {
    RelayPlugin,
    RelayPluginContext,
    RateLimitPolicy,
    createAbuseControlPolicyPlugin,
    createAppSurfacePolicyPlugin,
    createAppObjectPermissionPlugin,
    createRateLimitPolicyPlugin,
    createSafetyReportPlugin,
    createWebhookIngressPlugin
} from "./plugins";

interface Subscription {
    guildId: GuildId;
    channels?: ChannelId[];
    author?: string;
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

interface StateRequest extends SignedReadRequest {
    subId?: string;
    guildId?: GuildId;
    includeMembers?: boolean;
    includeMessages?: boolean;
    includeAppObjects?: boolean;
    memberAfter?: string;
    memberLimit?: number;
}

interface MembersRequest extends SignedReadRequest {
    subId?: string;
    guildId?: GuildId;
    afterUserId?: string;
    limit?: number;
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
}

function positiveIntegerFromEnv(name: string, fallback: number) {
    const raw = process.env[name];
    if (!raw) return fallback;
    const parsed = Number(raw);
    return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function rawDataByteLength(data: RawData) {
    if (typeof data === "string") return Buffer.byteLength(data);
    if (Array.isArray(data)) return data.reduce((total, chunk) => total + chunk.byteLength, 0);
    return data.byteLength;
}

function rawDataToString(data: RawData) {
    if (typeof data === "string") return data;
    if (Array.isArray(data)) return Buffer.concat(data).toString("utf8");
    if (data instanceof ArrayBuffer) return Buffer.from(new Uint8Array(data)).toString("utf8");
    return data.toString("utf8");
}

const DIRECT_MESSAGE_SNAPSHOT_MESSAGE_LIMIT = Math.max(
    1,
    Number(process.env.CGP_RELAY_DM_SNAPSHOT_MESSAGE_LIMIT ?? "96") || 96
);

const DIRECT_MESSAGE_SNAPSHOT_TRANSIENT_TYPES = new Set(["CALL_EVENT", "AGENT_INTENT", "CHECKPOINT"]);
const DIRECT_MESSAGE_SNAPSHOT_DEPENDENT_TYPES = new Set([
    "EDIT_MESSAGE",
    "DELETE_MESSAGE",
    "REACTION_ADD",
    "REACTION_REMOVE",
    "APP_OBJECT_UPSERT",
    "APP_OBJECT_DELETE"
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
    const target = body.target && typeof body.target === "object"
        ? body.target as Record<string, unknown>
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
        if (type === "CHANNEL_CREATE" && (channelId === guildId || channelId.startsWith("dm:"))) {
            return true;
        }
        if (type === "GUILD_CREATE" && /^(DM|GROUP DM):/i.test(name)) {
            return true;
        }
        if (channelId && channelId === stringValue(body.guildId) && channelId === guildId) {
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
                const guildId = stringValue(body.guildId) || `guild:${latestGuildCreateByGuild.size}`;
                latestGuildCreateByGuild.set(guildId, event);
                break;
            }
            case "CHANNEL_CREATE": {
                const channelId = stringValue(body.channelId, body.guildId) || `channel:${latestChannelCreateByChannel.size}`;
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

    const tailMessages = messageFrames.slice(-DIRECT_MESSAGE_SNAPSHOT_MESSAGE_LIMIT);
    const keptMessageIds = new Set(
        tailMessages.map((event) => stringValue((event.body as any)?.messageId, event.id)).filter(Boolean)
    );
    const keptDependentFrames = dependentFrames.filter((event) => keptMessageIds.has(targetMessageId(event)));

    const structuralFrames = [
        ...latestGuildCreateByGuild.values(),
        ...latestChannelCreateByChannel.values(),
        ...latestPresenceByUser.values()
    ].sort(compareEventsByHistoryOrder);
    const timelineFrames = [
        ...passthroughFrames,
        ...tailMessages,
        ...keptDependentFrames
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

function normalizeSearchLimit(limit: unknown) {
    const parsed = Number(limit);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        return 25;
    }

    return Math.min(100, Math.floor(parsed));
}

function normalizeOptionalSeq(value: unknown) {
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed >= 0 ? Math.floor(parsed) : undefined;
}

function normalizeSearchQuery(query: unknown) {
    if (typeof query !== "string") return "";
    return query.trim().replace(/\s+/g, " ").slice(0, 256);
}

const SEARCH_SCOPES: SearchScope[] = ["messages", "channels", "members", "appObjects"];

function normalizeSearchScopes(scopes: unknown) {
    if (!Array.isArray(scopes)) {
        return new Set<SearchScope>(SEARCH_SCOPES);
    }

    const selected = scopes.filter((scope): scope is SearchScope => SEARCH_SCOPES.includes(scope as SearchScope));
    return new Set<SearchScope>(selected.length > 0 ? selected : SEARCH_SCOPES);
}

function searchableText(value: unknown): string {
    if (value === undefined || value === null) return "";
    if (typeof value === "string") return value.toLowerCase();
    if (typeof value === "number" || typeof value === "boolean") return String(value).toLowerCase();
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

function attachmentSearchValues(attachments: unknown, includeEncrypted: boolean) {
    if (!Array.isArray(attachments)) return [];
    const values: unknown[] = [];
    for (const attachment of attachments) {
        if (!attachment || typeof attachment !== "object") continue;
        const ref = attachment as Record<string, unknown>;
        if (ref.encrypted === true && !includeEncrypted) continue;
        values.push(ref.id, ref.name, ref.mimeType, ref.type, ref.hash, ref.url, ref.scheme, ref.external);
    }
    return values;
}

function passesSeqWindow(seq: number | undefined, beforeSeq?: number, afterSeq?: number) {
    if (seq === undefined) {
        return beforeSeq === undefined && afterSeq === undefined;
    }
    if (beforeSeq !== undefined && seq >= beforeSeq) return false;
    if (afterSeq !== undefined && seq <= afterSeq) return false;
    return true;
}

function stripReadSignature(payload: unknown) {
    const source = payload && typeof payload === "object" ? payload as Record<string, unknown> : {};
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

function eventVisibleToReader(state: GuildState, event: GuildEvent, author?: string) {
    const body = event.body as unknown as Record<string, unknown>;
    const type = eventType(event);
    if (
        author &&
        stringValue(body.userId) === author &&
        (type === "BAN_USER" || type === "BAN_ADD" || type === "MEMBER_KICK" || type === "ROLE_REVOKE")
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

function filterEventsForReader(state: GuildState, events: GuildEvent[], author?: string) {
    return events.filter((event) => eventVisibleToReader(state, event, author));
}

function eventMatchesRequestedChannels(state: GuildState, event: GuildEvent, channels?: ChannelId[]) {
    if (!Array.isArray(channels) || channels.length === 0) {
        return true;
    }

    const channelId = eventChannelId(state, event);
    return !channelId || channels.includes(channelId);
}

function filterSerializedStateForReader(
    state: GuildState,
    author?: string,
    options: {
        includeMembers?: boolean;
        includeMessages?: boolean;
        includeAppObjects?: boolean;
    } = {}
) {
    const channels: ReturnType<typeof serializeState>["channels"] = [];
    for (const entry of state.channels) {
        if (canViewChannel(state, author, entry[0])) {
            channels.push(entry);
        }
    }

    const needsChannelVisibility = options.includeMessages !== false || options.includeAppObjects !== false;
    const visibleChannelIds = needsChannelVisibility ? new Set(channels.map(([channelId]) => channelId)) : null;

    const members: ReturnType<typeof serializeState>["members"] = [];
    if (options.includeMembers !== false) {
        for (const [id, member] of state.members) {
            members.push([
                id,
                {
                    ...member,
                    roles: Array.from(member.roles)
                }
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
        policies: state.policies
    };
}

function serializeBoundedCheckpointState(state: GuildState, messageRefLimit: number) {
    const serialized = serializeState(state);
    if (Number.isFinite(messageRefLimit) && messageRefLimit >= 0 && serialized.messages && serialized.messages.length > messageRefLimit) {
        serialized.messages = serialized.messages.slice(serialized.messages.length - messageRefLimit);
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
        const messageId = stringValue(body.messageId, (body.target as any)?.messageId);
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
                deleted: false
            });
            continue;
        }

        const record = records.get(messageId);
        if (!record) continue;

        if (type === "EDIT_MESSAGE") {
            record.latestSeq = event.seq;
            record.latestCreatedAt = event.createdAt;
            record.latestContent = typeof body.newContent === "string" ? body.newContent : record.latestContent;
        } else if (type === "DELETE_MESSAGE") {
            record.latestSeq = event.seq;
            record.latestCreatedAt = event.createdAt;
            record.deleted = true;
        }
    }

    return Array.from(records.values());
}

function buildSearchResults(
    state: GuildState,
    events: GuildEvent[],
    author: string | undefined,
    request: Required<Pick<SearchRequest, "includeDeleted" | "includeEncrypted" | "includeEvent">> & {
        guildId: GuildId;
        channelId?: ChannelId;
        query: string;
        scopes: Set<SearchScope>;
        beforeSeq?: number;
        afterSeq?: number;
        limit: number;
    }
) {
    const results: SearchResult[] = [];
    const visibleEvents = filterEventsForReader(state, events, author);
    const hasEnoughResults = () => results.length > request.limit;

    if (request.scopes.has("messages")) {
        for (const record of collectSearchMessageRecords(visibleEvents)) {
            if (request.channelId && record.channelId !== request.channelId) continue;
            if (!passesSeqWindow(record.latestSeq, request.beforeSeq, request.afterSeq)) continue;
            if (record.deleted && !request.includeDeleted) continue;

            const attachmentValues = attachmentSearchValues(record.attachments, request.includeEncrypted);
            const contentValues = record.encrypted ? [] : [record.latestContent];
            if (!matchesSearchQuery(request.query, record.messageId, record.channelId, record.author, ...contentValues, ...attachmentValues)) {
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
                preview: record.encrypted ? "Encrypted message" : previewText(record.latestContent),
                content: record.encrypted ? undefined : record.latestContent
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
            if (!passesSeqWindow(undefined, request.beforeSeq, request.afterSeq)) continue;
            if (!matchesSearchQuery(request.query, channelId, channel.name, channel.kind, channel.topic, channel.description, channel.categoryId)) {
                continue;
            }
            results.push({
                kind: "channel",
                guildId: request.guildId,
                channelId,
                preview: previewText([channel.name, channel.topic || channel.description].filter(Boolean).join(" - "))
            });
            if (hasEnoughResults()) break;
        }
    }

    if (!hasEnoughResults() && request.scopes.has("members")) {
        for (const [userId, member] of state.members) {
            if (!passesSeqWindow(undefined, request.beforeSeq, request.afterSeq)) continue;
            if (!matchesSearchQuery(request.query, userId, member.nickname, member.bio, Array.from(member.roles))) {
                continue;
            }
            results.push({
                kind: "member",
                guildId: request.guildId,
                userId,
                preview: previewText(member.nickname || member.bio || userId)
            });
            if (hasEnoughResults()) break;
        }
    }

    if (!hasEnoughResults() && request.scopes.has("appObjects")) {
        for (const [, object] of state.appObjects) {
            if (!passesSeqWindow(undefined, request.beforeSeq, request.afterSeq)) continue;
            const channelId = object.channelId || object.target?.channelId;
            if (request.channelId && channelId !== request.channelId) continue;
            if (channelId && !canViewChannel(state, author, channelId)) continue;
            if (!matchesSearchQuery(request.query, object.namespace, object.objectType, object.objectId, object.target, object.value)) {
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
                preview: previewText(`${object.namespace}/${object.objectType}/${object.objectId}`)
            });
            if (hasEnoughResults()) break;
        }
    }

    results.sort((a, b) => {
        const seqDelta = (b.seq ?? -1) - (a.seq ?? -1);
        if (seqDelta !== 0) return seqDelta;
        return (b.createdAt ?? 0) - (a.createdAt ?? 0);
    });

    const limited = results.slice(0, request.limit + 1);
    const hasMore = limited.length > request.limit;
    return {
        results: limited.slice(0, request.limit),
        hasMore
    };
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
    private messageQueues = new Map<WebSocket, Promise<void>>();
    private stateCache = new Map<GuildId, GuildState>();
    private checkpointCache = new Map<GuildId, GuildEvent | undefined>();
    private mutexes = new Map<GuildId, Promise<void>>();
    private maxFrameBytes: number;
    private searchScanLimit: number;
    private snapshotEventLimit: number;
    private checkpointMessageRefLimit: number;

    private pruneTimer: NodeJS.Timeout;
    private checkpointTimer: NodeJS.Timeout;
    private keyPair: { publicKey: string; privateKey: Uint8Array };

    constructor(
        port: number,
        dbPathOrStore: string | Store = "./relay-db",
        plugins: RelayPlugin[] = [],
        options: RelayServerOptions = {}
    ) {
        if (typeof dbPathOrStore === "string") {
            this.store = new LevelStore(dbPathOrStore);
        } else {
            this.store = dbPathOrStore;
        }
        this.maxFrameBytes = options.maxFrameBytes ?? positiveIntegerFromEnv("CGP_RELAY_MAX_FRAME_BYTES", 1024 * 1024);
        this.searchScanLimit = options.searchScanLimit ?? positiveIntegerFromEnv("CGP_RELAY_SEARCH_SCAN_EVENTS", 25000);
        this.snapshotEventLimit = options.snapshotEventLimit ?? positiveIntegerFromEnv("CGP_RELAY_SNAPSHOT_EVENTS", 5000);
        this.checkpointMessageRefLimit = options.checkpointMessageRefLimit ?? positiveIntegerFromEnv("CGP_RELAY_CHECKPOINT_MESSAGE_REFS", 50000);
        const defaultPluginsEnabled = options.enableDefaultPlugins ?? process.env.CGP_RELAY_DEFAULT_PLUGINS !== "0";
        this.plugins = defaultPluginsEnabled
            ? [
                createRateLimitPolicyPlugin(options.rateLimitPolicy),
                createAbuseControlPolicyPlugin(),
                createSafetyReportPlugin(),
                createAppSurfacePolicyPlugin(),
                createWebhookIngressPlugin(),
                createAppObjectPermissionPlugin({
                    rules: [
                        {
                            namespace: "org.cgp.chat",
                            objectType: "message-pin",
                            permissionScope: "messages"
                        }
                    ]
                }),
                ...plugins
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
        this.wss = new WebSocketServer({ server: this.httpServer });

        // Generate a random keypair for the relay (in production, load from config)
        const privateKey = generatePrivateKey();
        this.keyPair = {
            privateKey,
            publicKey: getPublicKey(privateKey)
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
            }
        };
        this.pluginsReady = this.initPlugins();

        this.wss.on("connection", (socket) => {
            this.subscriptions.set(socket, new Map());
            this.messageQueues.set(socket, Promise.resolve());

            socket.on("message", (data) => {
                const currentQueue = this.messageQueues.get(socket) || Promise.resolve();
                const nextTask = currentQueue.then(async () => {
                    try {
                        if (rawDataByteLength(data) > this.maxFrameBytes) {
                            socket.send(JSON.stringify(["ERROR", {
                                code: "PAYLOAD_TOO_LARGE",
                                message: `Frame exceeds ${this.maxFrameBytes} byte relay limit`
                            }]));
                            return;
                        }

                        const raw = rawDataToString(data);
                        const [kind, payload] = JSON.parse(raw);
                        await this.handleMessage(socket, kind, payload);
                    } catch (e: any) {
                        console.error("Error handling message", e);
                        if (e instanceof SyntaxError) {
                            console.error("Invalid payload:", data.toString());
                            socket.send(JSON.stringify(["ERROR", { code: "INVALID_FRAME", message: "Parse error" }]));
                        } else {
                            socket.send(JSON.stringify(["ERROR", { code: "INTERNAL_ERROR", message: e.message }]));
                        }
                    }
                });
                this.messageQueues.set(socket, nextTask);
            });

            socket.on("close", () => {
                this.removeSocketSubscriptions(socket);
                this.messageQueues.delete(socket);
            });
        });

        // Run prune every 60 seconds
        this.pruneTimer = setInterval(() => {
            this.prune().catch(err => console.error("Prune failed:", err));
        }, 60000);

        // Run checkpoint every 60 seconds
        this.checkpointTimer = setInterval(() => {
            this.createCheckpoints().catch(err => console.error("Checkpoint failed:", err));
        }, 60000);

        this.httpServer.listen(port);
        console.log(`Relay listening on port ${port}`);
    }

    public getPort(): number {
        const addr = this.wss.address();
        if (!addr || typeof addr === "string") return NaN;
        return addr.port;
    }

    private setExtensionHeaders(res: ServerResponse, contentType?: string, length?: number) {
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
            this.plugins.find((plugin) => plugin.metadata?.clientExtension === extensionName) ||
            this.plugins.find((plugin) => plugin.name === extensionName)
        );
    }

    private async handleHttp(req: IncomingMessage, res: ServerResponse) {
        const rawUrl = req.url || "/";
        const pathname = rawUrl.split("?")[0] || "/";
        await this.pluginsReady;

        if (await this.handlePluginHttp(req, res, rawUrl, pathname)) {
            return;
        }

        if (await this.handleExtensionHttp(req, res, pathname)) {
            return;
        }

        res.statusCode = 404;
        res.end("Not found");
    }

    private async handlePluginHttp(req: IncomingMessage, res: ServerResponse, rawUrl: string, pathname: string) {
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
        const plugin = this.plugins.find((candidate) => candidate.name === pluginName);
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
                    pathSegments
                },
                this.pluginCtx
            );
            if (!handled && !res.writableEnded) {
                res.statusCode = 404;
                res.end("Plugin route not found");
            }
        } catch (e: any) {
            console.error(`Relay plugin ${plugin.name} failed handling HTTP route ${pathname}:`, e);
            if (!res.writableEnded) {
                res.statusCode = 500;
                res.end(e?.message || "Internal plugin error");
            }
        }

        return true;
    }

    private async handleExtensionHttp(req: IncomingMessage, res: ServerResponse, pathname: string) {
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

    private async publishAsRelay(body: EventBody, createdAt = Date.now()): Promise<GuildEvent | undefined> {
        const author = this.keyPair.publicKey;
        const unsignedForSig = { body, author, createdAt };
        const signature = await sign(this.keyPair.privateKey, hashObject(unsignedForSig));
        const fullEvent = await this.appendSequencedEvent({ body, author, signature, createdAt });
        if (fullEvent) {
            await this.runOnEventAppended(fullEvent);
        }
        return fullEvent;
    }

    public async createCheckpoints() {
        const guilds = await this.store.getGuildIds();

        for (const guildId of guilds) {
            let rebuilt: { state: GuildState; endEvent: GuildEvent; checkpointEvent?: GuildEvent } | null;
            try {
                rebuilt = await this.rebuildGuildState(guildId);
            } catch (e) {
                console.error(`Failed to rebuild state for guild ${guildId} during checkpoint:`, e);
                continue;
            }
            if (!rebuilt) continue;

            // Create checkpoint event
            const { state, endEvent: lastEvent } = rebuilt;
            if (lastEvent.body.type === "CHECKPOINT") {
                continue;
            }

            const serializedState = serializeBoundedCheckpointState(state, this.checkpointMessageRefLimit);
            const stateHash = hashObject(serializedState);

            const body: Checkpoint = {
                type: "CHECKPOINT",
                guildId,
                rootHash: stateHash,
                seq: lastEvent.seq + 1,
                state: serializedState
            };

            const fullEvent: GuildEvent = {
                id: "",
                seq: lastEvent.seq + 1,
                prevHash: lastEvent.id,
                createdAt: Date.now(),
                author: this.keyPair.publicKey,
                body,
                signature: ""
            };

            fullEvent.id = computeEventId(fullEvent);
            fullEvent.signature = await sign(this.keyPair.privateKey, hashObject({ body, author: fullEvent.author, createdAt: fullEvent.createdAt }));

            await this.store.append(guildId, fullEvent);
            this.stateCache.set(guildId, applyEvent(state, fullEvent));
            this.checkpointCache.set(guildId, fullEvent);
            console.log(`Relay created checkpoint for guild ${guildId} at seq ${fullEvent.seq}`);
            this.broadcast(guildId, fullEvent);
        }
    }

    public async prune() {
        const guilds = await this.store.getGuildIds();
        const now = Date.now();

        for (const guildId of guilds) {
            let rebuilt: { state: GuildState; endEvent: GuildEvent; checkpointEvent?: GuildEvent } | null;
            try {
                rebuilt = await this.rebuildGuildState(guildId);
            } catch (e) {
                console.error(`Failed to rebuild state for guild ${guildId} during prune:`, e);
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

            for (const seq of seqsToDelete) {
                await this.store.deleteEvent(guildId, seq);
            }
            if (seqsToDelete.length > 0) {
                this.stateCache.delete(guildId);
                this.checkpointCache.delete(guildId);
            }
        }
    }

    private async handleMessage(socket: WebSocket, kind: string, payload: unknown) {
        console.log(`[RelayServer] handleMessage: ${kind}`);
        await this.pluginsReady;

        for (const plugin of this.plugins) {
            try {
                const handled = await plugin.onFrame?.({ socket, kind, payload }, this.pluginCtx);
                if (handled) return;
            } catch (e: any) {
                console.error(`Relay plugin ${plugin.name} failed handling frame ${kind}:`, e);
                socket.send(JSON.stringify(["ERROR", { code: "PLUGIN_ERROR", message: e.message || String(e) }]));
                return;
            }
        }

        if (kind === "HELLO") {
            const pluginList = this.plugins.map(p => ({
                name: p.name,
                metadata: p.metadata,
                inputs: p.inputs
            }));
            socket.send(JSON.stringify(["HELLO_OK", {
                protocol: "cgp/0.1",
                relayName: "Reference Relay",
                plugins: pluginList
            }]));
        } else if (kind === "PLUGIN_CONFIG") {
            const p = payload as { pluginName: string; config: any };
            const plugin = this.plugins.find(pl => pl.name === p.pluginName);
            if (plugin) {
                try {
                    await plugin.onConfig?.({ socket, config: p.config }, this.pluginCtx);
                    socket.send(JSON.stringify(["PLUGIN_CONFIG_OK", { pluginName: p.pluginName }]));
                } catch (e: any) {
                    socket.send(JSON.stringify(["ERROR", { code: "PLUGIN_CONFIG_ERROR", message: `Plugin configuration failed: ${e.message || String(e)}` }]));
                }
            } else {
                socket.send(JSON.stringify(["ERROR", { code: "PLUGIN_NOT_FOUND", message: `Plugin ${p.pluginName} not found` }]));
            }
        } else if (kind === "SUB") {
            const p = payload as { subId?: string; guildId?: GuildId; channels?: ChannelId[] } & SignedReadRequest;
            const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `sub-${Date.now()}`;
            const guildId = typeof p.guildId === "string" ? p.guildId : "";
            const channels = Array.isArray(p.channels) ? p.channels.filter((channelId): channelId is ChannelId => typeof channelId === "string" && channelId.trim().length > 0) : undefined;
            if (!guildId.trim()) {
                this.sendRequestError(socket, "VALIDATION_FAILED", "SUB requires a guildId", { subId });
                return;
            }

            await this.withGuildMutex(guildId, async () => {
                const author = await this.readAuthorOrError(socket, "SUB", payload, subId, guildId);
                if (author === null) return;

                const rebuilt = await this.rebuildGuildState(guildId);
                if (rebuilt && !canReadGuild(rebuilt.state, author)) {
                    this.sendRequestError(socket, "FORBIDDEN", "You do not have permission to subscribe to this guild", { subId, guildId });
                    return;
                }

                const rawSnapshotEvents = await this.readReplaySnapshotEvents(guildId);
                const snapshotEvents = rebuilt
                    ? filterEventsForReader(rebuilt.state, rawSnapshotEvents, author)
                        .filter((event) => eventMatchesRequestedChannels(rebuilt.state, event, channels))
                    : rawSnapshotEvents;
                this.addSubscription(socket, subId, { guildId, channels, author });
                const tailEvent = rebuilt?.endEvent ?? (snapshotEvents.length > 0 ? snapshotEvents[snapshotEvents.length - 1] : null);
                const oldestSeq = snapshotEvents.length > 0 ? snapshotEvents[0].seq : null;
                const newestSeq = snapshotEvents.length > 0 ? snapshotEvents[snapshotEvents.length - 1].seq : null;
                const checkpointEvent = snapshotEvents.find((event) => event.body.type === "CHECKPOINT");
                socket.send(
                    JSON.stringify([
                        "SNAPSHOT",
                        {
                            subId,
                            guildId,
                            events: snapshotEvents,
                            endSeq: tailEvent?.seq ?? -1,
                            endHash: tailEvent?.id ?? null,
                            oldestSeq,
                            newestSeq,
                            hasMore: oldestSeq !== null && oldestSeq > 0,
                            checkpointSeq: checkpointEvent?.seq ?? null,
                            checkpointHash: checkpointEvent?.id ?? null
                        }
                    ])
                );
            });

        } else if (kind === "GET_HISTORY") {
            await this.handleHistoryRequest(socket, payload);
        } else if (kind === "GET_STATE") {
            await this.handleStateRequest(socket, payload);
        } else if (kind === "SEARCH") {
            await this.handleSearchRequest(socket, payload);
        } else if (kind === "PUBLISH") {
            const p = payload as { body: EventBody; author: string; signature: string; createdAt: number; clientEventId?: string };
            const { body, author, signature, createdAt, clientEventId } = p;

            const fullEvent = await this.appendSequencedEvent({ body, author, signature, createdAt, clientEventId }, socket);
            if (fullEvent) {
                await this.runOnEventAppended(fullEvent, socket);
            }
        } else if (kind === "GET_MEMBERS") {
            const p = payload as MembersRequest;
            const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `members-${Date.now()}`;
            const guildId = typeof p.guildId === "string" ? p.guildId : "";
            if (!guildId.trim()) {
                this.sendRequestError(socket, "VALIDATION_FAILED", "GET_MEMBERS requires a guildId", { subId });
                return;
            }

            await this.withGuildMutex(guildId, async () => {
                const author = await this.readAuthorOrError(socket, "GET_MEMBERS", payload, subId, guildId);
                if (author === null) return;

                const rebuilt = await this.rebuildGuildState(guildId);
                if (!rebuilt) {
                    this.sendRequestError(socket, "NOT_FOUND", "Guild members not found", { subId, guildId });
                    return;
                }

                if (!canReadGuild(rebuilt.state, author)) {
                    this.sendRequestError(socket, "FORBIDDEN", "You do not have permission to read this guild's members", { subId, guildId });
                    return;
                }

                for (const plugin of this.plugins) {
                    try {
                        const members = await plugin.onGetMembers?.({ guildId, author, socket }, this.pluginCtx);
                        if (members) {
                            socket.send(JSON.stringify(["MEMBERS", { subId, guildId, members, nextCursor: null, hasMore: false }]));
                            return;
                        }
                    } catch (e: any) {
                        console.error(`Relay plugin ${plugin.name} failed onGetMembers:`, e);
                    }
                }

                const limit = normalizeMemberPageLimit(p.limit);
                const afterUserId = typeof p.afterUserId === "string" && p.afterUserId.trim() ? p.afterUserId : undefined;
                if (this.store.getMembersPage) {
                    const page = await this.store.getMembersPage({ guildId, afterUserId, limit });
                    socket.send(JSON.stringify(["MEMBERS", { subId, guildId, ...page }]));
                    return;
                }

                const members = serializeState(rebuilt.state).members
                    .map(([, member]) => member)
                    .sort((left, right) => left.userId.localeCompare(right.userId));
                const startIndex = afterUserId ? members.findIndex((member) => member.userId > afterUserId) : 0;
                const pageStart = startIndex >= 0 ? startIndex : members.length;
                const page = members.slice(pageStart, pageStart + limit + 1);
                const hasMore = page.length > limit;
                const visible = hasMore ? page.slice(0, limit) : page;
                socket.send(JSON.stringify([
                    "MEMBERS",
                    {
                        subId,
                        guildId,
                        members: visible,
                        nextCursor: hasMore && visible.length > 0 ? visible[visible.length - 1].userId : null,
                        hasMore,
                        totalApprox: members.length
                    }
                ]));
            });
        }
    }

    private async rebuildGuildState(guildId: GuildId): Promise<{ state: GuildState; endEvent: GuildEvent; checkpointEvent?: GuildEvent } | null> {
        const endEvent = await this.store.getLastEvent(guildId);
        if (!endEvent) {
            this.stateCache.delete(guildId);
            this.checkpointCache.delete(guildId);
            return null;
        }

        const cachedState = this.stateCache.get(guildId);
        if (cachedState?.headSeq === endEvent.seq) {
            if (endEvent.body.type === "CHECKPOINT" && isValidCheckpointEvent(endEvent)) {
                this.checkpointCache.set(guildId, endEvent);
            }
            return {
                state: cachedState,
                endEvent,
                checkpointEvent: this.checkpointCache.get(guildId)
            };
        }

        let events = this.store.getReplaySnapshotEvents
            ? buildReplaySnapshotEvents(guildId, await this.store.getReplaySnapshotEvents({ guildId, limit: this.snapshotEventLimit }))
            : await this.store.getLog(guildId);
        const canReplayFromFirstEvent = events[0]?.seq === 0 ||
            (events[0]?.body.type === "CHECKPOINT" && isValidCheckpointEvent(events[0]));
        if (!canReplayFromFirstEvent) {
            events = await this.store.getLog(guildId);
        }
        if (events.length === 0) {
            return null;
        }

        const { state, checkpointEvent } = rebuildStateFromEvents(events);
        this.stateCache.set(guildId, state);
        this.checkpointCache.set(guildId, checkpointEvent);

        return {
            state,
            endEvent: events[events.length - 1],
            checkpointEvent
        };
    }

    private verifyReadRequest(kind: string, payload: unknown) {
        const p = payload as SignedReadRequest;
        const author = typeof p?.author === "string" && p.author.trim() ? p.author : undefined;
        const signature = typeof p?.signature === "string" && p.signature.trim() ? p.signature : undefined;
        const createdAt = Number(p?.createdAt);

        if (!author && !signature && !Number.isFinite(createdAt)) {
            return undefined;
        }

        if (!author || !signature || !Number.isFinite(createdAt)) {
            throw new Error("Signed read request requires author, createdAt, and signature");
        }

        const maxSkewMs = 5 * 60 * 1000;
        if (Math.abs(Date.now() - createdAt) > maxSkewMs) {
            throw new Error("Signed read request is outside the allowed clock skew");
        }

        const unsignedPayload = stripReadSignature(payload);
        const ok = verify(author, hashObject({ kind, payload: unsignedPayload }), signature);
        if (!ok) {
            throw new Error("Invalid signed read request");
        }

        return author;
    }

    private sendRequestError(socket: WebSocket, code: string, message: string, extra?: Record<string, unknown>) {
        socket.send(JSON.stringify(["ERROR", { code, message, ...(extra ?? {}) }]));
    }

    private addSubscription(socket: WebSocket, subId: string, subscription: Subscription) {
        const subs = this.subscriptions.get(socket);
        if (!subs) {
            return;
        }

        const existing = subs.get(subId);
        if (existing) {
            this.removeSubscriptionIndex(socket, subId, existing.guildId);
        }

        subs.set(subId, subscription);
        let guildSockets = this.guildSubscriptions.get(subscription.guildId);
        if (!guildSockets) {
            guildSockets = new Map<WebSocket, Set<string>>();
            this.guildSubscriptions.set(subscription.guildId, guildSockets);
        }
        const socketSubs = guildSockets.get(socket) ?? new Set<string>();
        socketSubs.add(subId);
        guildSockets.set(socket, socketSubs);
    }

    private removeSubscriptionIndex(socket: WebSocket, subId: string, guildId: GuildId) {
        const guildSockets = this.guildSubscriptions.get(guildId);
        const socketSubs = guildSockets?.get(socket);
        if (!guildSockets || !socketSubs) {
            return;
        }

        socketSubs.delete(subId);
        if (socketSubs.size === 0) {
            guildSockets.delete(socket);
        }
        if (guildSockets.size === 0) {
            this.guildSubscriptions.delete(guildId);
        }
    }

    private removeSocketSubscriptions(socket: WebSocket) {
        const subs = this.subscriptions.get(socket);
        if (subs) {
            for (const [subId, sub] of subs) {
                this.removeSubscriptionIndex(socket, subId, sub.guildId);
            }
        }
        this.subscriptions.delete(socket);
    }

    private async readAuthorOrError(socket: WebSocket, kind: string, payload: unknown, subId?: string, guildId?: string) {
        try {
            return this.verifyReadRequest(kind, payload);
        } catch (e: any) {
            this.sendRequestError(socket, "AUTH_FAILED", e.message || "Read authentication failed", { subId, guildId });
            return null;
        }
    }

    private async handleStateRequest(socket: WebSocket, payload: unknown) {
        const p = payload as StateRequest;
        const guildId = typeof p?.guildId === "string" ? p.guildId : "";
        const subId = typeof p?.subId === "string" && p.subId.trim()
            ? p.subId
            : `state-${Date.now()}`;

        if (!guildId.trim()) {
            socket.send(JSON.stringify(["ERROR", { code: "VALIDATION_FAILED", message: "GET_STATE requires a guildId" }]));
            return;
        }

        await this.withGuildMutex(guildId, async () => {
            const author = await this.readAuthorOrError(socket, "GET_STATE", payload, subId, guildId);
            if (author === null) return;

            const rebuilt = await this.rebuildGuildState(guildId);
            if (!rebuilt) {
                socket.send(JSON.stringify(["ERROR", { code: "NOT_FOUND", message: "Guild state not found", subId, guildId }]));
                return;
            }

            if (!canReadGuild(rebuilt.state, author)) {
                this.sendRequestError(socket, "FORBIDDEN", "You do not have permission to read this guild state", { subId, guildId });
                return;
            }

            const wantsPagedMembers = p.memberLimit !== undefined || (typeof p.memberAfter === "string" && p.memberAfter.trim());
            const memberLimit = normalizeMemberPageLimit(p.memberLimit);
            const memberAfter = typeof p.memberAfter === "string" && p.memberAfter.trim() ? p.memberAfter : undefined;
            const includeMembers = wantsPagedMembers ? false : p.includeMembers !== false;
            const includeMessages = p.includeMessages !== false;
            const includeAppObjects = p.includeAppObjects !== false;
            const serializedState = filterSerializedStateForReader(rebuilt.state, author, {
                includeMembers,
                includeMessages,
                includeAppObjects
            });
            const membersPage = wantsPagedMembers
                ? await this.readMembersPage(rebuilt.state, { guildId, afterUserId: memberAfter, limit: memberLimit })
                : null;
            if (membersPage) {
                serializedState.members = membersPage.members.map((member) => [member.userId, member]);
            }
            socket.send(
                JSON.stringify([
                    "STATE",
                    {
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
                                totalApprox: membersPage.totalApprox
                            }
                            : undefined,
                        stateIncludes: {
                            members: membersPage ? "partial" : includeMembers ? "full" : "omitted",
                            messages: includeMessages ? "full" : "omitted",
                            appObjects: includeAppObjects ? "full" : "omitted"
                        }
                    }
                ])
            );
        });
    }

    private async readMembersPage(state: GuildState, query: MemberPageQuery) {
        if (this.store.getMembersPage) {
            return await this.store.getMembersPage(query);
        }

        const limit = normalizeMemberPageLimit(query.limit);
        const members = serializeState(state).members
            .map(([, member]) => member)
            .sort((left, right) => left.userId.localeCompare(right.userId));
        const startIndex = query.afterUserId ? members.findIndex((member) => member.userId > query.afterUserId!) : 0;
        const pageStart = startIndex >= 0 ? startIndex : members.length;
        const page = members.slice(pageStart, pageStart + limit + 1);
        const hasMore = page.length > limit;
        const visible = hasMore ? page.slice(0, limit) : page;
        return {
            members: visible,
            nextCursor: hasMore && visible.length > 0 ? visible[visible.length - 1].userId : null,
            hasMore,
            totalApprox: members.length
        };
    }

    private async readReplaySnapshotEvents(guildId: GuildId) {
        const events = this.store.getReplaySnapshotEvents
            ? await this.store.getReplaySnapshotEvents({ guildId, limit: this.snapshotEventLimit })
            : await this.store.getLog(guildId);
        return buildReplaySnapshotEvents(guildId, events);
    }

    private async handleHistoryRequest(socket: WebSocket, payload: unknown) {
        const p = payload as HistoryRequest;
        const guildId = typeof p?.guildId === "string" ? p.guildId : "";
        const subId = typeof p?.subId === "string" && p.subId.trim()
            ? p.subId
            : `history-${Date.now()}`;

        if (!guildId.trim()) {
            socket.send(JSON.stringify(["ERROR", { code: "VALIDATION_FAILED", message: "GET_HISTORY requires a guildId" }]));
            return;
        }

        const limit = normalizeHistoryLimit(p.limit);
        const query: HistoryQuery = {
            guildId,
            channelId: typeof p.channelId === "string" && p.channelId.trim() ? p.channelId : undefined,
            beforeSeq: normalizeOptionalSeq(p.beforeSeq),
            afterSeq: normalizeOptionalSeq(p.afterSeq),
            limit: limit + 1,
            includeStructural: p.includeStructural === true
        };

        await this.withGuildMutex(guildId, async () => {
            const author = await this.readAuthorOrError(socket, "GET_HISTORY", payload, subId, guildId);
            if (author === null) return;

            const rebuilt = await this.rebuildGuildState(guildId);
            if (!rebuilt) {
                socket.send(JSON.stringify(["ERROR", { code: "NOT_FOUND", message: "Guild history not found", subId, guildId }]));
                return;
            }

            if (!canReadGuild(rebuilt.state, author)) {
                this.sendRequestError(socket, "FORBIDDEN", "You do not have permission to read this guild history", { subId, guildId });
                return;
            }

            if (query.channelId && !canViewChannel(rebuilt.state, author, query.channelId)) {
                this.sendRequestError(socket, "FORBIDDEN", "You do not have permission to read this channel history", { subId, guildId, channelId: query.channelId });
                return;
            }

            const events = this.store.getHistory
                ? await this.store.getHistory(query)
                : (await this.store.getLog(guildId));
            const visibleEvents = filterEventsForReader(rebuilt.state, events, author);
            const hasMore = visibleEvents.length > limit;
            const pageEvents = query.afterSeq !== undefined
                ? visibleEvents.slice(0, limit)
                : visibleEvents.slice(Math.max(0, visibleEvents.length - limit));
            const oldestSeq = pageEvents.length > 0 ? pageEvents[0].seq : null;
            const newestSeq = pageEvents.length > 0 ? pageEvents[pageEvents.length - 1].seq : null;
            const tailEvent = pageEvents.length > 0 ? pageEvents[pageEvents.length - 1] : await this.store.getLastEvent(guildId);

            socket.send(
                JSON.stringify([
                    "SNAPSHOT",
                    {
                        subId,
                        guildId,
                        channelId: query.channelId,
                        events: pageEvents,
                        endSeq: tailEvent?.seq ?? -1,
                        endHash: tailEvent?.id ?? null,
                        oldestSeq,
                        newestSeq,
                        hasMore,
                        checkpointSeq: rebuilt.checkpointEvent?.seq ?? null,
                        checkpointHash: rebuilt.checkpointEvent?.id ?? null
                    }
                ])
            );
        });
    }

    private async handleSearchRequest(socket: WebSocket, payload: unknown) {
        const p = payload as SearchRequest;
        const guildId = typeof p?.guildId === "string" ? p.guildId : "";
        const subId = typeof p?.subId === "string" && p.subId.trim()
            ? p.subId
            : `search-${Date.now()}`;
        const query = normalizeSearchQuery(p?.query);

        if (!guildId.trim()) {
            this.sendRequestError(socket, "VALIDATION_FAILED", "SEARCH requires a guildId", { subId });
            return;
        }

        if (!query) {
            this.sendRequestError(socket, "VALIDATION_FAILED", "SEARCH requires a non-empty query", { subId, guildId });
            return;
        }

        const limit = normalizeSearchLimit(p.limit);
        const request = {
            guildId,
            channelId: typeof p.channelId === "string" && p.channelId.trim() ? p.channelId : undefined,
            query,
            scopes: normalizeSearchScopes(p.scopes),
            beforeSeq: normalizeOptionalSeq(p.beforeSeq),
            afterSeq: normalizeOptionalSeq(p.afterSeq),
            limit,
            includeDeleted: p.includeDeleted === true,
            includeEncrypted: p.includeEncrypted === true,
            includeEvent: p.includeEvent !== false
        };

        await this.withGuildMutex(guildId, async () => {
            const author = await this.readAuthorOrError(socket, "SEARCH", payload, subId, guildId);
            if (author === null) return;

            const rebuilt = await this.rebuildGuildState(guildId);
            if (!rebuilt) {
                this.sendRequestError(socket, "NOT_FOUND", "Search guild not found", { subId, guildId });
                return;
            }

            if (!canReadGuild(rebuilt.state, author)) {
                this.sendRequestError(socket, "FORBIDDEN", "You do not have permission to search this guild", { subId, guildId });
                return;
            }

            if (request.channelId && !canViewChannel(rebuilt.state, author, request.channelId)) {
                this.sendRequestError(socket, "FORBIDDEN", "You do not have permission to search this channel", { subId, guildId, channelId: request.channelId });
                return;
            }

            const events = request.scopes.has("messages")
                ? await this.readSearchCandidateEvents(request)
                : [];
            const { results, hasMore } = buildSearchResults(rebuilt.state, events, author, request);
            const seqs = results
                .map((result) => result.seq)
                .filter((seq): seq is number => typeof seq === "number");

            socket.send(
                JSON.stringify([
                    "SEARCH_RESULTS",
                    {
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
                        checkpointHash: rebuilt.checkpointEvent?.id ?? null
                    }
                ])
            );
        });
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
            includeStructural: false
        };

        if (this.store.getSearchEvents) {
            return await this.store.getSearchEvents(query);
        }

        if (this.store.getHistory) {
            return await this.store.getHistory({ ...query, limit: this.searchScanLimit });
        }

        return await this.store.getLog(request.guildId);
    }

    private async appendSequencedEvent(
        p: { body: EventBody; author: string; signature: string; createdAt: number; clientEventId?: string },
        socket?: WebSocket
    ): Promise<GuildEvent | undefined> {
        const { body, author, signature, createdAt, clientEventId } = p;
        const targetGuildId = body.guildId;
        const sendError = (code: string, message: string) => {
            socket?.send(JSON.stringify(["ERROR", { code, message, clientEventId }]));
        };

        if (!targetGuildId?.trim()) {
            sendError("VALIDATION_FAILED", "Event body requires a guildId");
            return undefined;
        }

        if (body.type === "CHECKPOINT") {
            sendError("VALIDATION_FAILED", "CHECKPOINT events are relay-maintained and cannot be published by clients");
            return undefined;
        }

        return await this.withGuildMutex(targetGuildId, async () => {
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
                signature
            };

            fullEvent.id = computeEventId({ ...fullEvent });

            const unsignedForSig = { body, author, createdAt };
            const msgHash = hashObject(unsignedForSig);

            if (!verify(author, msgHash, signature)) {
                console.error(`Invalid signature for event ${fullEvent.id}`);
                sendError("INVALID_SIGNATURE", "Signature verification failed");
                return undefined;
            }

            if (seq > 0) {
                let state = this.stateCache.get(targetGuildId);

                if (!state || state.headSeq !== seq - 1) {
                    let history = this.store.getReplaySnapshotEvents
                        ? buildReplaySnapshotEvents(targetGuildId, await this.store.getReplaySnapshotEvents({ guildId: targetGuildId, limit: this.snapshotEventLimit }))
                        : await this.store.getLog(targetGuildId);
                    const canReplayFromFirstEvent = history[0]?.seq === 0 ||
                        (history[0]?.body.type === "CHECKPOINT" && isValidCheckpointEvent(history[0]));
                    if (!canReplayFromFirstEvent) {
                        history = await this.store.getLog(targetGuildId);
                    }
                    if (history.length === 0) {
                        console.error("Validation error: Missing history for non-genesis event");
                        return undefined;
                    }

                    const rebuilt = rebuildStateFromEvents(history);
                    state = rebuilt.state;
                    this.stateCache.set(targetGuildId, state);
                    this.checkpointCache.set(targetGuildId, rebuilt.checkpointEvent);
                }

                try {
                    const validationState = await this.hydrateValidationIndexes(state, fullEvent);
                    validateEvent(validationState, fullEvent);
                    // Optimistically apply to cache
                    const newState = applyEvent(validationState, fullEvent);
                    this.stateCache.set(targetGuildId, newState);
                    if (fullEvent.body.type === "CHECKPOINT" && isValidCheckpointEvent(fullEvent)) {
                        this.checkpointCache.set(targetGuildId, fullEvent);
                    }
                } catch (e: any) {
                    console.error(`Validation failed for guild ${targetGuildId}: ${e.message}`);
                    console.error(`Event body:`, JSON.stringify(body));
                    sendError("VALIDATION_FAILED", e.message);
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
                this.stateCache.set(targetGuildId, state);
                this.checkpointCache.delete(targetGuildId);
            }

            await this.store.append(targetGuildId, fullEvent);
            // console.log(`Relay appended event ${fullEvent.id} type ${body.type} seq ${fullEvent.seq}`);

            this.broadcast(targetGuildId, fullEvent);
            socket?.send(JSON.stringify(["PUB_ACK", {
                clientEventId,
                guildId: targetGuildId,
                eventId: fullEvent.id,
                seq: fullEvent.seq
            }]));
            return fullEvent;
        });
    }

    private async hydrateValidationIndexes(state: GuildState, event: GuildEvent): Promise<GuildState> {
        const memberState = await this.hydrateValidationMembers(state, event);
        return await this.hydrateValidationMessageRefs(memberState, event);
    }

    private async hydrateValidationMembers(state: GuildState, event: GuildEvent): Promise<GuildState> {
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

            const member = await this.store.getMember({ guildId: state.guildId, userId });
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
            roles: new Set(Array.isArray(member.roles) ? member.roles : Array.from(member.roles ?? []))
        };
    }

    private async hydrateValidationMessageRefs(state: GuildState, event: GuildEvent): Promise<GuildState> {
        if (!this.store.getMessageRef) {
            return state;
        }

        const body = event.body as Record<string, any>;
        const messageIds = new Set<string>();
        if (body.type === "MESSAGE") {
            const messageId = typeof body.messageId === "string" && body.messageId.trim() ? body.messageId : event.id;
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

        const target = body.target && typeof body.target === "object" ? body.target as Record<string, any> : undefined;
        if ((body.type === "APP_OBJECT_UPSERT" || body.type === "APP_OBJECT_DELETE") && typeof target?.messageId === "string" && target.messageId.trim()) {
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
                    messages: new Map(state.messages)
                };
            }
            return hydrated;
        };

        for (const messageId of messageIds) {
            if (state.messages.has(messageId)) {
                continue;
            }
            const ref: SerializableMessageRef | null = await this.store.getMessageRef({ guildId: state.guildId, messageId });
            if (ref) {
                ensureHydrated().messages.set(messageId, ref);
            }
        }

        return hydrated ?? state;
    }

    private async runOnEventAppended(event: GuildEvent, socket?: WebSocket) {
        for (const plugin of this.plugins) {
            try {
                await plugin.onEventAppended?.({ event, socket }, this.pluginCtx);
            } catch (e: any) {
                console.error(`Relay plugin ${plugin.name} failed in onEventAppended:`, e);
            }
        }
    }


    private broadcast(guildId: GuildId, event: GuildEvent) {
        let count = 0;
        const state = this.stateCache.get(guildId);
        const frame = JSON.stringify(["EVENT", event]);
        const guildSockets = this.guildSubscriptions.get(guildId);
        if (!guildSockets) {
            return;
        }

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
                if (state && (!eventVisibleToReader(state, event, sub.author) || !eventMatchesRequestedChannels(state, event, sub.channels))) {
                    continue;
                }
                shouldSend = true;
                break;
            }
            if (shouldSend) {
                socket.send(frame);
                count++;
            }
        }
        // console.log(`Broadcasted event ${event.seq} to ${count} clients`);
    }

    private async withGuildMutex<T>(guildId: GuildId, task: () => Promise<T>): Promise<T> {
        const prev = this.mutexes.get(guildId) || Promise.resolve();
        const next = prev.catch(() => undefined).then(task);
        this.mutexes.set(guildId, next.then(() => undefined, () => undefined));
        return await next;
    }

    public async close() {
        clearInterval(this.pruneTimer);
        clearInterval(this.checkpointTimer);
        this.wss.close();
        this.httpServer.close();
        await this.pluginsReady;
        for (const plugin of this.plugins) {
            try {
                await plugin.onClose?.(this.pluginCtx);
            } catch (e: any) {
                console.error(`Relay plugin ${plugin.name} failed to close:`, e);
            }
        }
        await this.store.close();
    }
}
