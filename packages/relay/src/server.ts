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
    validateEvent
} from "@cgp/core";
import { LevelStore } from "./store_level";
import { HistoryQuery, Store } from "./store";
import { RelayPlugin, RelayPluginContext, RateLimitPolicy, createRateLimitPolicyPlugin } from "./plugins";

interface Subscription {
    guildId: GuildId;
    channels?: ChannelId[];
}

interface HistoryRequest {
    subId?: string;
    guildId?: GuildId;
    channelId?: ChannelId;
    beforeSeq?: number;
    afterSeq?: number;
    limit?: number;
    includeStructural?: boolean;
}

export interface RelayServerOptions {
    /**
     * Default plugins are recommended reference relay policy, not mandatory CGP protocol state.
     * Set false when embedding a relay with a custom plugin stack.
     */
    enableDefaultPlugins?: boolean;
    rateLimitPolicy?: Partial<RateLimitPolicy>;
    maxFrameBytes?: number;
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

function normalizeHistoryLimit(limit: unknown) {
    const parsed = Number(limit);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        return 100;
    }

    return Math.min(500, Math.floor(parsed));
}

function normalizeOptionalSeq(value: unknown) {
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed >= 0 ? Math.floor(parsed) : undefined;
}

export class RelayServer {
    private wss: WebSocketServer;
    private httpServer: ReturnType<typeof createServer>;
    private store: Store;
    private plugins: RelayPlugin[];
    private pluginCtx: RelayPluginContext;
    private pluginsReady: Promise<void>;
    private subscriptions = new Map<WebSocket, Map<string, Subscription>>();
    private messageQueues = new Map<WebSocket, Promise<void>>();
    private stateCache = new Map<GuildId, GuildState>();
    private mutexes = new Map<GuildId, Promise<void>>();
    private maxFrameBytes: number;

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
        const defaultPluginsEnabled = options.enableDefaultPlugins ?? process.env.CGP_RELAY_DEFAULT_PLUGINS !== "0";
        this.plugins = defaultPluginsEnabled ? [createRateLimitPolicyPlugin(options.rateLimitPolicy), ...plugins] : [...plugins];
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
                this.subscriptions.delete(socket);
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
            const events = await this.store.getLog(guildId);
            if (events.length === 0) continue;

            // Rebuild state
            let state: GuildState;
            try {
                state = createInitialState(events[0]);
                for (let i = 1; i < events.length; i++) {
                    state = applyEvent(state, events[i]);
                }
            } catch (e) {
                console.error(`Failed to rebuild state for guild ${guildId} during checkpoint:`, e);
                continue;
            }

            // Create checkpoint event
            const lastEvent = events[events.length - 1];
            if (lastEvent.body.type === "CHECKPOINT") {
                continue;
            }

            const serializedState = serializeState(state);
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
            console.log(`Relay created checkpoint for guild ${guildId} at seq ${fullEvent.seq}`);
            this.broadcast(guildId, fullEvent);
        }
    }

    public async prune() {
        const guilds = await this.store.getGuildIds();
        const now = Date.now();

        for (const guildId of guilds) {
            const events = await this.store.getLog(guildId);
            if (events.length === 0) continue;

            let state: GuildState;
            try {
                state = createInitialState(events[0]);
                for (let i = 1; i < events.length; i++) {
                    state = applyEvent(state, events[i]);
                }
            } catch (e) {
                console.error(`Failed to rebuild state for guild ${guildId} during prune:`, e);
                continue;
            }

            const seqsToDelete: number[] = [];
            for (const event of events) {
                if (event.body.type === "MESSAGE") {
                    const body = event.body as Message;
                    const channel = state.channels.get(body.channelId);
                    if (channel && channel.retention && channel.retention.mode === "ttl" && channel.retention.seconds) {
                        const ageSeconds = (now - event.createdAt) / 1000;
                        if (ageSeconds > channel.retention.seconds) {
                            seqsToDelete.push(event.seq);
                        }
                    }
                }
            }

            for (const seq of seqsToDelete) {
                await this.store.deleteEvent(guildId, seq);
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
            const p = payload as { subId: string; guildId: GuildId; channels?: ChannelId[] };
            const { subId, guildId, channels } = p;
            await this.withGuildMutex(guildId, async () => {
                const events = await this.store.getLog(guildId);
                const snapshotEvents = buildSnapshotEvents(guildId, events);
                const subs = this.subscriptions.get(socket);
                if (subs) {
                    subs.set(subId, { guildId, channels });
                }
                const tailEvent = events.length > 0 ? events[events.length - 1] : null;
                socket.send(
                    JSON.stringify([
                        "SNAPSHOT",
                        {
                            subId,
                            guildId,
                            events: snapshotEvents,
                            endSeq: tailEvent?.seq ?? -1,
                            endHash: tailEvent?.id ?? null
                        }
                    ])
                );
            });

        } else if (kind === "GET_HISTORY") {
            await this.handleHistoryRequest(socket, payload);
        } else if (kind === "PUBLISH") {
            const p = payload as { body: EventBody; author: string; signature: string; createdAt: number; clientEventId?: string };
            const { body, author, signature, createdAt, clientEventId } = p;

            const fullEvent = await this.appendSequencedEvent({ body, author, signature, createdAt, clientEventId }, socket);
            if (fullEvent) {
                await this.runOnEventAppended(fullEvent, socket);
            }
        } else if (kind === "GET_MEMBERS") {
            const p = payload as { subId: string; guildId: string };
            const { subId, guildId } = p;

            // Query plugins for members
            for (const plugin of this.plugins) {
                try {
                    const members = await plugin.onGetMembers?.({ guildId, socket }, this.pluginCtx);
                    if (members) {
                        socket.send(JSON.stringify(["MEMBERS", { subId, guildId, members }]));
                        return;
                    }
                } catch (e: any) {
                    console.error(`Relay plugin ${plugin.name} failed onGetMembers:`, e);
                }
            }

            // If no plugin handled it, return empty or error? For now, empty list or ignore.
            socket.send(JSON.stringify(["MEMBERS", { subId, guildId, members: [] }]));
        }
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
            const events = this.store.getHistory
                ? await this.store.getHistory(query)
                : (await this.store.getLog(guildId));
            const hasMore = events.length > limit;
            const pageEvents = query.afterSeq !== undefined
                ? events.slice(0, limit)
                : events.slice(Math.max(0, events.length - limit));
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
                        hasMore
                    }
                ])
            );
        });
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
                    // Cache miss or out of sync, rebuild from history
                    const history = await this.store.getLog(targetGuildId);
                    if (history.length === 0) {
                        console.error("Validation error: Missing history for non-genesis event");
                        return undefined;
                    }

                    state = createInitialState(history[0]);
                    for (let i = 1; i < history.length; i++) {
                        state = applyEvent(state, history[i]);
                    }
                    this.stateCache.set(targetGuildId, state);
                }

                try {
                    validateEvent(state, fullEvent);
                    // Optimistically apply to cache
                    const newState = applyEvent(state, fullEvent);
                    this.stateCache.set(targetGuildId, newState);
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
        for (const [socket, subs] of this.subscriptions) {
            for (const sub of subs.values()) {
                if (sub.guildId === guildId) {
                    socket.send(JSON.stringify(["EVENT", event]));
                    count++;
                }
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
