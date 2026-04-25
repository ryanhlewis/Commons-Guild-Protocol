import { EventEmitter } from "events";
import { randomBytes } from "crypto";
import { WebSocket } from "ws";
import {
    GuildEvent,
    computeEventId,
    GuildId,
    ChannelId,
    createInitialState,
    applyEvent,
    GuildState,
    deserializeState,
    rebuildStateFromEvents,
    verify,
    hashObject,
    sign,
    EventBody,
    GuildCreate,
    ChannelCreate,
    Message,
    AppObjectDelete,
    AppObjectTarget,
    AppObjectUpsert,
    RoleAssign,
    BanUser,
    EditMessage,
    DeleteMessage,
    ForkFrom,
    EphemeralPolicyUpdate,
    EphemeralPolicy,
    GuildPolicies,
    validateEvent,
    getSharedSecret,
    encrypt,
    decrypt,
    generateSymmetricKey,
    SerializableMember,
    SerializableGuildState,
    RelayHead,
    RelayHeadQuorum,
    RelayHeadQuorumOptions,
    assertRelayHeadQuorum,
    summarizeRelayHeadQuorum,
    summarizeVerifiedRelayHeadQuorum,
    verifyRelayHead,
    CgpWireFormat,
    socketDataToCgpFrame,
    encodeCgpFrame,
    encodeCgpWireFrame,
    stringifyCgpFrame,
    InvalidCgpFrameError
} from "@cgp/core";

export type StateIncludeMode = "full" | "partial" | "omitted";

export interface StateIncludes {
    members?: StateIncludeMode;
    messages?: Exclude<StateIncludeMode, "partial">;
    appObjects?: Exclude<StateIncludeMode, "partial">;
}

export interface MemberPageMeta {
    nextCursor: string | null;
    hasMore: boolean;
    totalApprox?: number;
}

export interface StateResponse {
    subId?: string;
    guildId: GuildId;
    state: SerializableGuildState;
    rootHash: string;
    endSeq: number;
    endHash: string;
    checkpointSeq?: number | null;
    checkpointHash?: string | null;
    membersPage?: MemberPageMeta;
    stateIncludes?: StateIncludes;
}

function configureNodeSocket(ws: WebSocket) {
    const transport = (ws as unknown as { _socket?: { setNoDelay?: (noDelay?: boolean) => void } })._socket;
    transport?.setNoDelay?.(true);
}

export interface StateRequestOptions {
    includeMembers?: boolean;
    includeMessages?: boolean;
    includeAppObjects?: boolean;
    memberAfter?: string;
    memberLimit?: number;
}

export interface MembersRequestOptions {
    afterUserId?: string;
    limit?: number;
}

export interface MembersResponse {
    subId?: string;
    guildId: GuildId;
    members: SerializableMember[];
    nextCursor?: string | null;
    hasMore?: boolean;
    totalApprox?: number;
}

export interface RelayHeadResponse {
    subId?: string;
    guildId: GuildId;
    head: RelayHead;
}

export interface RelayHeadsResponse {
    subId?: string;
    guildId: GuildId;
    heads: RelayHead[];
    quorum?: {
        guildId: GuildId;
        validCount: number;
        invalidCount: number;
        conflictCount: number;
        canonical?: { seq: number; hash: string | null; count: number };
        conflicts?: any[];
    };
}

export interface RelayHeadGossip {
    guildId: GuildId;
    head: RelayHead;
    quorum?: {
        guildId: GuildId;
        validCount: number;
        invalidCount: number;
        conflictCount: number;
        canonical?: { seq: number; hash: string | null; count: number };
    };
}

export interface RelayHeadRequestOptions {
    timeoutMs?: number;
    minHeads?: number;
}

export interface HistoryRequest {
    guildId: GuildId;
    channelId?: ChannelId;
    beforeSeq?: number;
    afterSeq?: number;
    limit?: number;
    includeStructural?: boolean;
}

export interface HistoryResponse {
    subId?: string;
    guildId: GuildId;
    channelId?: ChannelId;
    events: GuildEvent[];
    endSeq: number;
    endHash: string | null;
    oldestSeq: number | null;
    newestSeq: number | null;
    hasMore: boolean;
    checkpointSeq?: number | null;
    checkpointHash?: string | null;
}

export type SearchScope = "messages" | "channels" | "members" | "appObjects";
export type SearchResultKind = "message" | "channel" | "member" | "appObject";

export interface SearchRequest {
    guildId: GuildId;
    channelId?: ChannelId;
    query: string;
    scopes?: SearchScope[];
    beforeSeq?: number;
    afterSeq?: number;
    limit?: number;
    includeDeleted?: boolean;
    includeEncrypted?: boolean;
    includeEvent?: boolean;
}

export interface SearchResult {
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

export interface SearchResponse {
    subId?: string;
    guildId: GuildId;
    channelId?: ChannelId;
    query: string;
    scopes: SearchScope[];
    results: SearchResult[];
    hasMore: boolean;
    oldestSeq: number | null;
    newestSeq: number | null;
    checkpointSeq?: number | null;
    checkpointHash?: string | null;
}

export interface PublishAck {
    clientEventId?: string;
    guildId: GuildId;
    eventId: string;
    seq: number;
}

export interface PublishBatchAck {
    batchId?: string;
    results: PublishAck[];
    truncated?: boolean;
}

export interface PublishReliableOptions {
    timeoutMs?: number;
    clientEventId?: string;
}

export interface PublishBatchReliableOptions {
    timeoutMs?: number;
    batchId?: string;
}

export type ClientWireFormat = CgpWireFormat;

export interface AppObjectWriteOptions {
    channelId?: ChannelId;
    target?: AppObjectTarget;
    value?: any;
}

const MAX_OWN_PUBLISH_PROOFS = 5000;
const MAX_SEEN_EVENTS = 8192;

function clientDebugLoggingEnabled() {
    return typeof process !== "undefined" && process.env?.CGP_CLIENT_DEBUG_LOGS === "1";
}

function positiveClientIntegerFromEnv(name: string, fallback: number) {
    const value = typeof process !== "undefined" ? Number(process.env?.[name]) : NaN;
    return Number.isSafeInteger(value) && value > 0 ? value : fallback;
}

function clientWireFormatFromEnv(): ClientWireFormat {
    const value = typeof process !== "undefined" ? process.env?.CGP_CLIENT_WIRE_FORMAT : undefined;
    return value === "binary-json" || value === "binary-v1" || value === "binary-v2" ? value : "json";
}

function mergePartialState(current: GuildState | undefined, incoming: GuildState, includes?: StateIncludes): GuildState {
    if (!current || !includes) {
        return incoming;
    }

    return {
        ...incoming,
        members: includes.members === "omitted"
            ? current.members
            : includes.members === "partial"
                ? new Map([...current.members, ...incoming.members])
                : incoming.members,
        messages: includes.messages === "omitted" ? current.messages : incoming.messages,
        appObjects: includes.appObjects === "omitted" ? current.appObjects : incoming.appObjects
    };
}

function randomNonceHex(byteLength = 16) {
    return randomBytes(byteLength).toString("hex");
}

interface TrustedRelayHeadAnchor {
    seq: number;
    hash: string | null;
    count: number;
    observedAt: number;
}

export class CgpClient extends EventEmitter {
    private relays: string[];
    private writeRelays: string[];
    private sockets: WebSocket[] = [];
    private socketUrls = new Map<WebSocket, string>();
    private keyPair?: { pub: string; priv: Uint8Array };
    private state = new Map<GuildId, GuildState>();
    private desiredSubscriptions = new Set<GuildId>();
    private seenEvents = new Set<string>();
    private pendingEvents = new Map<GuildId, Map<number, GuildEvent>>();
    private relayHeadObservations = new Map<GuildId, Map<string, RelayHead>>();
    private trustedRelayHeads = new Map<GuildId, TrustedRelayHeadAnchor>();
    private groupKeys = new Map<GuildId, string>(); // Hex encoded symmetric keys
    private recoveringGuilds = new Set<GuildId>();
    private server: any;
    public availablePlugins: any[] = [];
    private pluginsBySocket = new Map<WebSocket, any[]>();
    private ownPublishProofs = new Set<string>();
    private ownPublishProofOrder: string[] = [];
    private ownPublishProofOrderHead = 0;
    private seenEventOrder: string[] = [];
    private seenEventOrderHead = 0;
    private isClosed = false;
    private debugLogs: boolean;
    private wireFormat: ClientWireFormat;
    private connectTimeoutMs: number;

    constructor(config: { relays: string[]; keyPair?: { pub: string; priv: Uint8Array }; debugLogs?: boolean; wireFormat?: ClientWireFormat; connectTimeoutMs?: number; writeRelays?: string[] }) {
        super();
        this.relays = config.relays;
        this.writeRelays = Array.isArray(config.writeRelays) && config.writeRelays.length > 0
            ? config.writeRelays
            : config.relays;
        this.keyPair = config.keyPair;
        this.debugLogs = config.debugLogs ?? clientDebugLoggingEnabled();
        this.wireFormat = config.wireFormat ?? clientWireFormatFromEnv();
        this.connectTimeoutMs = config.connectTimeoutMs ?? positiveClientIntegerFromEnv("CGP_CLIENT_CONNECT_TIMEOUT_MS", 5000);
    }

    async connect() {
        const promises = this.relays.map(url => this.connectToRelay(url));
        await Promise.all(promises);
    }

    async configurePlugin(pluginName: string, config: any) {
        const payload = { pluginName, config };
        const hasPluginMap = this.pluginsBySocket.size > 0;
        this.sockets.forEach(s => {
            if (s.readyState !== WebSocket.OPEN) return;
            const plugins = this.pluginsBySocket.get(s);
            if (plugins) {
                const supportsPlugin = plugins.some((p: any) => p && p.name === pluginName);
                if (supportsPlugin) this.sendFrame(s, "PLUGIN_CONFIG", payload);
                return;
            }
            if (!hasPluginMap) {
                this.sendFrame(s, "PLUGIN_CONFIG", payload);
            }
        });
    }

    private async signedReadPayload(kind: string, payload: Record<string, unknown>) {
        if (!this.keyPair) {
            return payload;
        }

        const { pub, priv } = this.keyPair;
        const createdAt = Date.now();
        const unsignedPayload = { ...payload, author: pub, createdAt };
        const signature = await sign(priv, hashObject({ kind, payload: unsignedPayload }));
        return { ...unsignedPayload, signature };
    }

    private sendEncodedFrame(socket: WebSocket, frame: string | Uint8Array) {
        if (socket.readyState !== WebSocket.OPEN) {
            return false;
        }
        socket.send(frame);
        return true;
    }

    private sendRawFrame(socket: WebSocket, frame: string) {
        return this.sendEncodedFrame(socket, encodeCgpWireFrame(frame, this.wireFormat));
    }

    private sendFrame(socket: WebSocket, kind: string, payload: unknown) {
        return this.sendEncodedFrame(socket, encodeCgpFrame(kind, payload, this.wireFormat));
    }

    private sendEncodedFrameToOpenSockets(frame: string | Uint8Array) {
        let sent = false;
        this.sockets.forEach(s => {
            if (this.sendEncodedFrame(s, frame)) {
                sent = true;
            }
        });
        return sent;
    }

    private sendRawFrameToOpenSockets(frame: string) {
        return this.sendEncodedFrameToOpenSockets(encodeCgpWireFrame(frame, this.wireFormat));
    }

    private sendFrameToOpenSockets(kind: string, payload: unknown) {
        return this.sendEncodedFrameToOpenSockets(encodeCgpFrame(kind, payload, this.wireFormat));
    }

    private writerSockets() {
        const writers: WebSocket[] = [];
        for (const relayUrl of this.writeRelays) {
            for (const socket of this.sockets) {
                if (this.socketUrls.get(socket) !== relayUrl) {
                    continue;
                }
                if (socket.readyState === WebSocket.OPEN) {
                    writers.push(socket);
                }
            }
        }

        for (const socket of this.sockets) {
            if (!this.socketUrls.has(socket) && socket.readyState === WebSocket.OPEN) {
                writers.push(socket);
            }
        }
        return writers;
    }

    private sendEncodedFrameToWriterSocket(frame: string | Uint8Array) {
        return this.sendEncodedFrameToWriterSocketWithSocket(frame) !== undefined;
    }

    private sendEncodedFrameToWriterSocketWithSocket(frame: string | Uint8Array) {
        const writer = this.writerSockets()[0];
        if (!writer) {
            return undefined;
        }
        return this.sendEncodedFrame(writer, frame) ? writer : undefined;
    }

    private sendRawFrameToWriterSocket(frame: string) {
        return this.sendEncodedFrameToWriterSocket(encodeCgpWireFrame(frame, this.wireFormat));
    }

    private sendFrameToWriterSocket(kind: string, payload: unknown) {
        return this.sendEncodedFrameToWriterSocket(encodeCgpFrame(kind, payload, this.wireFormat));
    }

    private async sendSubscription(socket: WebSocket, guildId: GuildId) {
        if (socket.readyState !== WebSocket.OPEN) return;
        const subId = Math.random().toString(36).slice(2);
        const payload = await this.signedReadPayload("SUB", { subId, guildId });
        if (socket.readyState !== WebSocket.OPEN) return;
        this.sendFrame(socket, "SUB", payload);
    }

    private replaySubscriptions(socket: WebSocket) {
        for (const guildId of this.desiredSubscriptions) {
            void this.sendSubscription(socket, guildId);
        }
    }

    private recoverGuild(guildId: GuildId) {
        if (!guildId || this.recoveringGuilds.has(guildId)) {
            return;
        }

        this.recoveringGuilds.add(guildId);
        this.state.delete(guildId);
        this.pendingEvents.delete(guildId);

        queueMicrotask(() => {
            try {
                this.sockets.forEach((socket) => void this.sendSubscription(socket, guildId));
            } finally {
                this.recoveringGuilds.delete(guildId);
            }
        });
    }

    private async connectToRelay(url: string, retryCount = 0) {
        return new Promise<void>((resolve) => {
            if (this.isClosed) return resolve();
            const ws = new WebSocket(url, { perMessageDeflate: false });
            let settledInitialConnect = false;
            const settleInitialConnect = () => {
                if (settledInitialConnect) return;
                settledInitialConnect = true;
                clearTimeout(connectTimeout);
                resolve();
            };
            const connectTimeout = setTimeout(() => {
                if (this.debugLogs) console.log(`Timed out connecting to relay ${url}`);
                try {
                    ws.close();
                } catch {
                    // ignored
                }
                settleInitialConnect();
            }, this.connectTimeoutMs);

            ws.onopen = () => {
                if (this.isClosed) {
                    ws.close();
                    settleInitialConnect();
                    return;
                }
                configureNodeSocket(ws);
                if (this.debugLogs) console.log(`Connected to relay ${url}`);
                this.sendFrame(ws, "HELLO", { protocol: "cgp/0.1", wireFormat: this.wireFormat });
                this.socketUrls.set(ws, url);
                this.sockets.push(ws);
                this.replaySubscriptions(ws);
                settleInitialConnect();
            };

            ws.onmessage = (msg) => this.handleMessage(msg.data, ws);

            ws.onerror = (err) => {
                console.error(`WS Error on ${url}`, err);
                settleInitialConnect();
            };

            ws.onclose = () => {
                if (this.debugLogs) console.log(`Disconnected from relay ${url}`);
                const index = this.sockets.indexOf(ws);
                if (index > -1) {
                    this.sockets.splice(index, 1);
                }
                this.socketUrls.delete(ws);
                this.pluginsBySocket.delete(ws);
                this.updateAvailablePlugins();
                settleInitialConnect();

                if (this.isClosed) {
                    return;
                }

                const delay = Math.min(1000 * Math.pow(2, retryCount), 30000);
                if (this.debugLogs) console.log(`Reconnecting to ${url} in ${delay}ms...`);
                setTimeout(() => {
                    this.connectToRelay(url, retryCount + 1);
                }, delay);
            };

            ws.addEventListener('error', settleInitialConnect);
        });
    }

    async listen(port: number) {
        const { WebSocketServer } = await import("ws");
        const wss = new WebSocketServer({ port });

        if (this.debugLogs) console.log(`Client listening on port ${port}`);

        wss.on("connection", (socket) => {
            if (this.debugLogs) console.log(`Incoming connection on port ${port}`);
            this.sockets.push(socket);
            this.replaySubscriptions(socket);
            socket.on("message", (data) => this.handleMessage(data, socket));
            socket.on("close", () => {
                const index = this.sockets.indexOf(socket);
                if (index > -1) this.sockets.splice(index, 1);
                this.socketUrls.delete(socket);
                this.pluginsBySocket.delete(socket);
                this.updateAvailablePlugins();
            });
        });
        this.server = wss;
    }

    async connectToPeer(url: string) {
        return new Promise<void>((resolve, reject) => {
            if (this.debugLogs) console.log(`Connecting to peer ${url}`);
            const ws = new WebSocket(url, { perMessageDeflate: false });
            ws.onopen = () => {
                configureNodeSocket(ws);
                if (this.debugLogs) console.log(`Connected to peer ${url}`);
                this.sendFrame(ws, "HELLO", { protocol: "cgp/0.1", mode: "p2p", wireFormat: this.wireFormat });
                this.sockets.push(ws);
                this.replaySubscriptions(ws);
                if (this.debugLogs) console.log(`Peer sockets count: ${this.sockets.length}`);
                resolve();
            };
            ws.onmessage = (msg) => this.handleMessage(msg.data, ws);
            ws.onerror = (err) => {
                console.error(`Peer WS Error ${url}`, err);
                reject(err);
            };
            ws.onclose = () => {
                const index = this.sockets.indexOf(ws);
                if (index > -1) this.sockets.splice(index, 1);
                this.socketUrls.delete(ws);
                this.pluginsBySocket.delete(ws);
                this.updateAvailablePlugins();
            };
        });
    }
    private messageQueue = Promise.resolve();

    private updateAvailablePlugins() {
        const merged = new Map<string, any>();
        for (const plugins of this.pluginsBySocket.values()) {
            if (!Array.isArray(plugins)) continue;
            for (const plugin of plugins) {
                if (plugin && typeof plugin.name === "string" && !merged.has(plugin.name)) {
                    merged.set(plugin.name, plugin);
                }
            }
        }
        this.availablePlugins = Array.from(merged.values());
        this.emit("plugins_loaded", this.availablePlugins);
    }

    private ownPublishProof(body: EventBody, author: string, createdAt: number, signature: string) {
        return hashObject({ body, author, createdAt, signature });
    }

    private rememberOwnPublishProof(body: EventBody, author: string, createdAt: number, signature: string) {
        const proof = this.ownPublishProof(body, author, createdAt, signature);
        if (this.ownPublishProofs.has(proof)) {
            return;
        }
        this.ownPublishProofs.add(proof);
        this.ownPublishProofOrder.push(proof);
        while (this.ownPublishProofOrder.length - this.ownPublishProofOrderHead > MAX_OWN_PUBLISH_PROOFS) {
            const oldProof = this.ownPublishProofOrder[this.ownPublishProofOrderHead++];
            if (oldProof) {
                this.ownPublishProofs.delete(oldProof);
            }
        }
        if (this.ownPublishProofOrderHead > 1024 && this.ownPublishProofOrderHead * 2 > this.ownPublishProofOrder.length) {
            this.ownPublishProofOrder = this.ownPublishProofOrder.slice(this.ownPublishProofOrderHead);
            this.ownPublishProofOrderHead = 0;
        }
    }

    private consumeOwnPublishProof(body: EventBody, author: string, createdAt: number, signature: string) {
        const proof = this.ownPublishProof(body, author, createdAt, signature);
        if (!this.ownPublishProofs.has(proof)) {
            return false;
        }
        this.ownPublishProofs.delete(proof);
        return true;
    }

    private rememberSeenEvent(eventId: string) {
        if (this.seenEvents.has(eventId)) {
            return false;
        }
        this.seenEvents.add(eventId);
        this.seenEventOrder.push(eventId);
        while (this.seenEventOrder.length - this.seenEventOrderHead > MAX_SEEN_EVENTS) {
            const oldEventId = this.seenEventOrder[this.seenEventOrderHead++];
            if (oldEventId) {
                this.seenEvents.delete(oldEventId);
            }
        }
        if (this.seenEventOrderHead > 1024 && this.seenEventOrderHead * 2 > this.seenEventOrder.length) {
            this.seenEventOrder = this.seenEventOrder.slice(this.seenEventOrderHead);
            this.seenEventOrderHead = 0;
        }
        return true;
    }

    private verifiesSignedEventEnvelope(event: GuildEvent) {
        if (!event || typeof event !== "object") {
            return false;
        }
        if (
            typeof event.id !== "string" ||
            !Number.isSafeInteger(event.seq) ||
            event.seq < 0 ||
            (event.prevHash !== null && typeof event.prevHash !== "string") ||
            typeof event.createdAt !== "number" ||
            typeof event.author !== "string" ||
            typeof event.signature !== "string" ||
            !event.body ||
            typeof event.body !== "object"
        ) {
            return false;
        }
        if (computeEventId(event) !== event.id) {
            if (this.debugLogs) console.log(`Client: Event id mismatch for seq ${event.seq}`);
            return false;
        }
        const unsignedForSig = { body: event.body, author: event.author, createdAt: event.createdAt };
        const msgHash = hashObject(unsignedForSig);
        const alreadyProvedByThisClient =
            event.author === this.keyPair?.pub &&
            this.consumeOwnPublishProof(event.body, event.author, event.createdAt, event.signature);
        if (!alreadyProvedByThisClient && !verify(event.author, msgHash, event.signature)) {
            if (this.debugLogs) console.log(`Client: Invalid signature for event ${event.id}`);
            return false;
        }

        return true;
    }

    private acceptsSignedEvent(event: GuildEvent) {
        if (typeof event?.id === "string" && this.seenEvents.has(event.id)) {
            return false;
        }
        if (!this.verifiesSignedEventEnvelope(event)) {
            return false;
        }
        return true;
    }

    private canRebuildStateFromSnapshotEvents(events: GuildEvent[]) {
        if (events.length === 0) {
            return false;
        }
        const first = events[0];
        if (first.seq !== 0 && first.body.type !== "CHECKPOINT") {
            return false;
        }
        let previous: GuildEvent | undefined;
        for (const event of events) {
            if (!previous) {
                previous = event;
                continue;
            }
            if (event.seq !== previous.seq + 1 || event.prevHash !== previous.id) {
                return false;
            }
            previous = event;
        }
        return true;
    }

    private stateResponseRootMatches(payload: Partial<StateResponse>) {
        if (!payload.state || typeof payload.rootHash !== "string") {
            return false;
        }
        return hashObject(payload.state) === payload.rootHash;
    }

    private relayHeadObservationKey(head: RelayHead) {
        return `${head.relayPublicKey}:${head.relayId}`;
    }

    private recordRelayHeadObservation(head: RelayHead) {
        if (!verifyRelayHead(head)) {
            this.emit("error_frame", {
                guildId: head?.guildId,
                code: "INVALID_RELAY_HEAD",
                message: "Relay head failed signature verification"
            });
            return undefined;
        }
        const observations = this.relayHeadObservations.get(head.guildId) ?? new Map<string, RelayHead>();
        observations.set(this.relayHeadObservationKey(head), head);
        this.relayHeadObservations.set(head.guildId, observations);
        return this.updateTrustedRelayHead(head.guildId, [...observations.values()], true);
    }

    private updateTrustedRelayHead(guildId: GuildId, heads: RelayHead[], headsAreVerified = false) {
        const quorum = headsAreVerified
            ? summarizeVerifiedRelayHeadQuorum(guildId, heads)
            : summarizeRelayHeadQuorum(guildId, heads);
        if (quorum.conflicts.length > 0 || !quorum.canonical) {
            this.emit("error_frame", {
                guildId,
                code: "RELAY_HEAD_CONFLICT",
                message: "Conflicting relay heads observed"
            });
            return quorum;
        }

        const previous = this.trustedRelayHeads.get(guildId);
        const canonical = quorum.canonical;
        if (
            !previous ||
            canonical.seq > previous.seq ||
            (canonical.seq === previous.seq && canonical.hash === previous.hash && canonical.count >= previous.count)
        ) {
            this.trustedRelayHeads.set(guildId, {
                seq: canonical.seq,
                hash: canonical.hash,
                count: canonical.count,
                observedAt: Date.now()
            });
        } else if (canonical.seq === previous.seq && canonical.hash !== previous.hash) {
            this.emit("error_frame", {
                guildId,
                code: "RELAY_HEAD_REGRESSION",
                message: "Relay head disagrees with previously trusted head"
            });
        }
        return quorum;
    }

    private responseAnchorCanReplaceState(guildId: GuildId, endSeq: unknown, endHash: unknown) {
        if (!Number.isSafeInteger(endSeq) || typeof endHash !== "string" || !endHash) {
            return false;
        }
        const seq = Number(endSeq);
        const current = this.state.get(guildId);
        if (current) {
            if (seq < current.headSeq) {
                return false;
            }
            if (seq === current.headSeq && endHash !== current.headHash) {
                return false;
            }
        }

        const trusted = this.trustedRelayHeads.get(guildId);
        if (trusted) {
            if (seq > trusted.seq) {
                return false;
            }
            if (seq === trusted.seq && endHash !== trusted.hash) {
                return false;
            }
            if (!current && seq < trusted.seq) {
                return false;
            }
        }

        return true;
    }

    private async handleMessage(data: unknown, socket?: WebSocket) {
        // Chain the processing to ensure sequential execution
        this.messageQueue = this.messageQueue.then(async () => {
            try {
                const { kind, payload, rawFrame } = await socketDataToCgpFrame(data);
                this.emit("frame", { kind, payload });

                if (kind === "EVENT") {
                    const event = payload as GuildEvent;
                    if (!this.acceptsSignedEvent(event)) {
                        return;
                    }

                    // console.log(`Client received EVENT seq ${event.seq} type ${event.body.type}`);
                    const updateResult = await this.updateState(event);
                    if (updateResult === "applied" || updateResult === "stale") {
                        this.rememberSeenEvent(event.id);
                    }
                    this.emit("event", event);

                    // Gossip: Forward to all other peers
                    let gossipCount = 0;
                    this.sockets.forEach(s => {
                        if (s !== socket && s.readyState === WebSocket.OPEN) {
                            if (this.sendRawFrame(s, rawFrame)) {
                                gossipCount++;
                            }
                        }
                    });
                    // console.log(`Gossiped event ${event.seq} to ${gossipCount} peers`);

                } else if (kind === "SNAPSHOT") {
                    const p = payload as HistoryResponse;
                    const events = Array.isArray(p.events) ? p.events : [];
                    const maybeStateReplacement = events.length > 0 && (events[0]?.seq === 0 || events[0]?.body?.type === "CHECKPOINT");
                    if (maybeStateReplacement && !this.responseAnchorCanReplaceState(p.guildId, p.endSeq, p.endHash)) {
                        this.emit("error_frame", {
                            subId: p.subId,
                            guildId: p.guildId,
                            code: "UNTRUSTED_SNAPSHOT_ANCHOR",
                            message: "Relay snapshot is stale, forked, or ahead of trusted relay head"
                        });
                        return;
                    }
                    const invalidEvent = events.find((event) => !this.verifiesSignedEventEnvelope(event));
                    if (invalidEvent) {
                        this.emit("error_frame", {
                            subId: p.subId,
                            guildId: p.guildId,
                            code: "INVALID_SNAPSHOT",
                            message: "Relay snapshot failed event verification"
                        });
                        return;
                    }
                    const verifiedPayload = { ...p, events };
                    this.emit("snapshot_response", verifiedPayload);
                    const { guildId, endSeq, endHash } = verifiedPayload;
                    // console.log(`Client received SNAPSHOT for ${guildId} with ${events.length} events`);
                    if (this.canRebuildStateFromSnapshotEvents(events)) {
                        try {
                            let state = rebuildStateFromEvents(events).state;
                            for (const event of events) {
                                await this.extractKey(event);
                            }
                            if (typeof endSeq === "number" && endSeq >= state.headSeq && typeof endHash === "string" && endHash) {
                                state.headSeq = endSeq;
                                state.headHash = endHash;
                            }
                            this.state.set(guildId, state);
                        } catch {
                            // Channel-history pages may intentionally omit genesis/checkpoint anchors.
                        }
                    }
                    events.forEach((e: GuildEvent) => this.emit("event", e));

                } else if (kind === "PUBLISH") {
                    const p = payload as { body: EventBody; author: string; signature: string; createdAt: number };
                    const { body, author, signature, createdAt } = p;

                    const unsignedForSig = { body, author, createdAt };
                    const msgHash = hashObject(unsignedForSig);

                    if (!verify(author, msgHash, signature)) {
                        if (this.debugLogs) console.log(`Client: Invalid signature for PUBLISH from peer`);
                        return;
                    }

                    let seq = 0;
                    let prevHash: string | null = null;

                    const targetGuildId = body.guildId;
                    const state = this.state.get(targetGuildId);

                    if (state) {
                        seq = state.headSeq + 1;
                        prevHash = state.headHash;
                    } else if (body.type !== "GUILD_CREATE") {
                        if (this.debugLogs) console.log("Client received PUBLISH for unknown guild and not GUILD_CREATE");
                        return;
                    }

                    const event: GuildEvent = {
                        id: "",
                        seq,
                        prevHash,
                        createdAt,
                        author,
                        body,
                        signature
                    };
                    event.id = computeEventId(event);

                    await this.updateState(event);
                    this.emit("event", event);

                    // Gossip: Forward as EVENT to all peers
                    const eventFrame = encodeCgpFrame("EVENT", event, this.wireFormat);
                    let gossipCount = 0;
                    this.sockets.forEach(s => {
                        if (s.readyState === WebSocket.OPEN) {
                            if (this.sendEncodedFrame(s, eventFrame)) {
                                gossipCount++;
                            }
                        }
                    });
                    // console.log(`Converted PUBLISH to EVENT and gossiped to ${gossipCount} peers`);

                } else if (kind === "HELLO") {
                    // P2P Handshake
                } else if (kind === "HELLO_OK") {
                    const p = payload as any;
                    if (Array.isArray(p.plugins) && socket) {
                        this.pluginsBySocket.set(socket, p.plugins);
                        this.updateAvailablePlugins();
                    }
                } else if (kind === "PLUGIN_CONFIG_OK") {
                    this.emit("plugin_config_ok", payload);
                } else if (kind === "PUB_ACK") {
                    this.emit("publish_ack", payload);
                } else if (kind === "PUB_BATCH_ACK") {
                    this.emit("publish_batch_ack", payload);
                } else if (kind === "BATCH") {
                    const events = payload as GuildEvent[];
                    if (Array.isArray(events)) {
                        if (this.debugLogs) console.log(`Client received BATCH with ${events.length} events`);
                        let acceptedCount = 0;
                        for (const event of events) {
                            if (!this.acceptsSignedEvent(event)) {
                                continue;
                            }
                            const updateResult = await this.updateState(event);
                            if (updateResult === "applied" || updateResult === "stale") {
                                this.rememberSeenEvent(event.id);
                            }
                            this.emit("event", event);
                            acceptedCount++;
                        }
                        if (acceptedCount > 0) {
                            this.sockets.forEach(s => {
                                if (s !== socket && s.readyState === WebSocket.OPEN) {
                                    this.sendRawFrame(s, rawFrame);
                                }
                            });
                        }
                    }
                } else if (kind === "ERROR") {
                    console.error("Client received ERROR:", payload);
                    this.emit("error_frame", payload);
                } else if (kind === "STATE") {
                    const p = payload as Partial<StateResponse>;
                    if (p.state && p.guildId && typeof p.endSeq === "number" && typeof p.endHash === "string") {
                        if (!this.responseAnchorCanReplaceState(p.guildId, p.endSeq, p.endHash)) {
                            this.emit("error_frame", {
                                subId: p.subId,
                                guildId: p.guildId,
                                code: "UNTRUSTED_STATE_ANCHOR",
                                message: "Relay state is stale, forked, or ahead of trusted relay head"
                            });
                            return;
                        }
                        if (!this.stateResponseRootMatches(p)) {
                            this.emit("error_frame", {
                                subId: p.subId,
                                guildId: p.guildId,
                                code: "INVALID_STATE_ROOT",
                                message: "Relay state root did not match state payload"
                            });
                            return;
                        }
                        const incoming = deserializeState(p.state, p.endSeq, p.endHash, Date.now());
                        this.state.set(p.guildId, mergePartialState(this.state.get(p.guildId), incoming, p.stateIncludes));
                    }
                    this.emit("state_response", payload);
                } else if (kind === "SEARCH_RESULTS") {
                    this.emit("search_response", payload);
                } else if (kind === "MEMBERS") {
                    this.emit("members_response", payload);
                } else if (kind === "RELAY_HEAD") {
                    const p = payload as RelayHeadResponse;
                    if (p?.head) {
                        this.recordRelayHeadObservation(p.head);
                    }
                    this.emit("relay_head_response", payload);
                } else if (kind === "RELAY_HEADS") {
                    const p = payload as RelayHeadsResponse;
                    if (p?.guildId && Array.isArray(p.heads)) {
                        this.updateTrustedRelayHead(p.guildId, p.heads.filter((head) => verifyRelayHead(head)), true);
                    }
                    this.emit("relay_heads_response", payload);
                } else if (kind === "RELAY_HEAD_GOSSIP") {
                    const p = payload as RelayHeadGossip;
                    if (p?.head) {
                        this.recordRelayHeadObservation(p.head);
                    }
                    this.emit("relay_head_gossip", payload);
                }

            } catch (e) {
                if (e instanceof InvalidCgpFrameError) {
                    return;
                }
                console.error("Error handling message:", e);
            }
        }).catch(err => console.error("Message queue error:", err));
    }

    private async updateState(event: GuildEvent): Promise<"applied" | "stale" | "gap" | "rejected"> {
        const guildId = event.body.guildId;
        let state = this.state.get(guildId);

        await this.extractKey(event);

        if (!state) {
            if (event.body.type === "GUILD_CREATE") {
                try {
                    // validateEvent({} as any, event); // Skip validation for genesis event
                    // Actually createInitialState validates type
                    state = createInitialState(event);
                    this.state.set(guildId, state);
                    return "applied";
                } catch (e) {
                    console.error("Failed to apply initial event", e);
                    return "rejected";
                }
            } else {
                return "gap";
            }
        } else {
            if (event.seq === state.headSeq + 1 && event.prevHash === state.headHash) {
                try {
                    validateEvent(state, event);
                    state = applyEvent(state, event, { mutable: true });
                    this.state.set(guildId, state);
                    return "applied";
                } catch (e) {
                    console.error("Failed to apply event", e);
                    return "rejected";
                }
            } else if (event.seq <= state.headSeq) {
                // Duplicate or stale gossip should not force recovery. In dense P2P meshes,
                // the same genesis or earlier fork can arrive after a peer already advanced.
                return "stale";
            } else {
                if (this.debugLogs) {
                    console.log(`Gap or Fork detected: Expected ${state.headSeq + 1}/${state.headHash}, got ${event.seq}/${event.prevHash}`);
                }
                this.recoverGuild(guildId);
                return "gap";
            }
        }
    }

    private async extractKey(event: GuildEvent) {
        if (!this.keyPair) return;
        const body = event.body;
        let encryptedGroupKey: string | undefined;
        let targetUser: string | undefined;

        if (body.type === "GUILD_CREATE") {
            encryptedGroupKey = body.encryptedGroupKey;
            targetUser = event.author;
        } else if (body.type === "ROLE_ASSIGN") {
            encryptedGroupKey = body.encryptedGroupKey;
            targetUser = body.userId;
        }

        if (encryptedGroupKey && targetUser === this.keyPair.pub) {
            try {
                const shared = getSharedSecret(this.keyPair.priv, event.author);
                const { ciphertext, iv } = JSON.parse(encryptedGroupKey);
                const keyHex = await decrypt(shared, ciphertext, iv);
                this.groupKeys.set(body.guildId, keyHex);
            } catch (e) {
                console.error("Failed to decrypt group key", e);
            }
        }
    }

    async createGuild(
        name: string,
        description?: string,
        access: "public" | "private" = "public",
        policies?: GuildPolicies
    ): Promise<GuildId> {
        const { pub, priv } = this.keyPair!;
        const createdAt = Date.now();

        let encryptedGroupKey: string | undefined;
        if (access === "private") {
            const groupKey = generateSymmetricKey();
            this.groupKeys.set("", groupKey); // Temp placeholder, will update with ID

            // Encrypt for self
            const shared = getSharedSecret(priv, pub);
            const res = await encrypt(shared, groupKey);
            encryptedGroupKey = JSON.stringify(res);
        }

        const body: GuildCreate = {
            type: "GUILD_CREATE",
            guildId: "",
            name,
            description,
            access,
            policies,
            encryptedGroupKey
        };
        const tempId = hashObject({ type: "guild-id", author: pub, name, createdAt, nonce: randomNonceHex() });
        body.guildId = tempId;

        if (access === "private") {
            const key = this.groupKeys.get("");
            if (key) {
                this.groupKeys.delete("");
                this.groupKeys.set(tempId, key);
            }
        }

        await this.subscribe(tempId);
        await this.publish(body);
        return tempId;
    }

    async createChannel(guildId: GuildId, name: string, kind: "text" | "voice" | "ephemeral-text", ephemeralPolicy?: EphemeralPolicy): Promise<ChannelId> {
        const channelId = hashObject({ type: "channel-id", guildId, name, kind, author: this.keyPair?.pub, createdAt: Date.now(), nonce: randomNonceHex() });
        const body: ChannelCreate = { type: "CHANNEL_CREATE", guildId, channelId, name, kind, retention: ephemeralPolicy };
        await this.publish(body);
        return channelId;
    }

    async sendMessage(guildId: GuildId, channelId: ChannelId, content: string, external?: any): Promise<string> {
        let finalContent = content;
        let iv: string | undefined;
        let encrypted: boolean | undefined;

        const state = this.state.get(guildId);
        const groupKey = this.groupKeys.get(guildId);

        if (groupKey) {
            // Group Encryption
            const result = await encrypt(Buffer.from(groupKey, "hex"), content);
            finalContent = result.ciphertext;
            iv = result.iv;
            encrypted = true;
        } else if (state && state.access === "private" && state.members.size === 2) {
            // Pairwise Fallback (for legacy DMs or if key missing)
            const { pub, priv } = this.keyPair!;
            let otherUser: string | undefined;
            for (const [userId] of state.members) {
                if (userId !== pub) {
                    otherUser = userId;
                    break;
                }
            }

            if (otherUser) {
                const shared = getSharedSecret(priv, otherUser);
                const result = await encrypt(shared, content);
                finalContent = result.ciphertext;
                iv = result.iv;
                encrypted = true;
            }
        }

        const messageId = hashObject({ type: "message-id", guildId, channelId, author: this.keyPair?.pub, createdAt: Date.now(), nonce: randomNonceHex() });
        const body: Message = {
            type: "MESSAGE",
            guildId,
            channelId,
            messageId,
            content: finalContent,
            iv,
            encrypted,
            external
        };
        await this.publish(body);
        return messageId;
    }

    async upsertAppObject(
        guildId: GuildId,
        namespace: string,
        objectType: string,
        objectId: string,
        options: AppObjectWriteOptions = {}
    ): Promise<void> {
        const body: AppObjectUpsert = {
            type: "APP_OBJECT_UPSERT",
            guildId,
            namespace,
            objectType,
            objectId,
            channelId: options.channelId,
            target: options.target,
            value: options.value
        };
        await this.publishReliable(body);
    }

    async deleteAppObject(
        guildId: GuildId,
        namespace: string,
        objectType: string,
        objectId: string,
        options: Omit<AppObjectWriteOptions, "value"> = {}
    ): Promise<void> {
        const body: AppObjectDelete = {
            type: "APP_OBJECT_DELETE",
            guildId,
            namespace,
            objectType,
            objectId,
            channelId: options.channelId,
            target: options.target
        };
        await this.publishReliable(body);
    }

    async assignRole(guildId: GuildId, userId: string, role: string): Promise<void> {
        const { pub, priv } = this.keyPair!;
        let encryptedGroupKey: string | undefined;

        const groupKey = this.groupKeys.get(guildId);
        if (groupKey) {
            const shared = getSharedSecret(priv, userId);
            const res = await encrypt(shared, groupKey);
            encryptedGroupKey = JSON.stringify(res);
        }

        const body: RoleAssign = { type: "ROLE_ASSIGN", guildId, userId, roleId: role, encryptedGroupKey };
        await this.publish(body);
    }

    async banUser(guildId: GuildId, userId: string): Promise<void> {
        const body: BanUser = { type: "BAN_USER", guildId, userId };
        await this.publish(body);
    }

    async editMessage(guildId: GuildId, channelId: ChannelId, messageId: string, content: string): Promise<void> {
        const body: EditMessage = { type: "EDIT_MESSAGE", guildId, channelId, messageId, newContent: content };
        await this.publish(body);
    }

    async deleteMessage(guildId: GuildId, channelId: ChannelId, messageId: string, reason?: string): Promise<void> {
        const body: DeleteMessage = { type: "DELETE_MESSAGE", guildId, channelId, messageId, reason };
        await this.publish(body);
    }

    async forkGuild(originalGuildId: GuildId, parentSeq: number, parentRootHash: string, newName: string): Promise<GuildId> {
        const { pub } = this.keyPair!;
        const createdAt = Date.now();
        const newGuildId = hashObject({ type: "fork-guild-id", author: pub, name: newName, originalGuildId, parentSeq, createdAt, nonce: randomNonceHex() });

        const createBody: GuildCreate = { type: "GUILD_CREATE", guildId: newGuildId, name: newName };
        await this.publish(createBody);

        const forkBody: ForkFrom = {
            type: "FORK_FROM",
            guildId: newGuildId,
            parentGuildId: originalGuildId,
            parentSeq,
            parentRootHash,
            note: `Fork of ${originalGuildId}`
        };
        await this.publish(forkBody);

        return newGuildId;
    }

    async setEphemeralPolicy(guildId: GuildId, channelId: ChannelId, duration: number): Promise<void> {
        const body: EphemeralPolicyUpdate = {
            type: "EPHEMERAL_POLICY_UPDATE",
            guildId,
            channelId,
            retention: { mode: "ttl", seconds: duration }
        };
        await this.publish(body);
    }

    async subscribe(guildId: GuildId): Promise<void> {
        this.desiredSubscriptions.add(guildId);
        this.sockets.forEach((s) => void this.sendSubscription(s, guildId));
    }

    async getMembers(guildId: string): Promise<SerializableMember[]> {
        const response = await this.getMembersPage(guildId);
        return response.members;
    }

    async getMembersPage(guildId: string, options: MembersRequestOptions = {}): Promise<MembersResponse> {
        const subId = Math.random().toString(36).slice(2);
        const payload = await this.signedReadPayload("GET_MEMBERS", { subId, guildId, ...options });

        return new Promise<MembersResponse>((resolve, reject) => {
            const timeout = setTimeout(() => {
                cleanup();
                reject(new Error("Timeout waiting for members"));
            }, 10000);

            const handler = (data: MembersResponse) => {
                if (data.subId === subId) {
                    cleanup();
                    resolve(data);
                }
            };
            const errorHandler = (data: any) => {
                if (data?.subId === subId) {
                    cleanup();
                    reject(new Error(data.message || "Members request failed"));
                }
            };

            const cleanup = () => {
                clearTimeout(timeout);
                this.off("members_response", handler);
                this.off("error_frame", errorHandler);
            };

            this.on("members_response", handler);
            this.on("error_frame", errorHandler);

            this.sendFrameToOpenSockets("GET_MEMBERS", payload);
        });
    }

    async getRelayHeads(guildId: GuildId, options: RelayHeadRequestOptions = {}): Promise<RelayHead[]> {
        const subId = Math.random().toString(36).slice(2);
        const payload = await this.signedReadPayload("GET_HEAD", { subId, guildId });
        const openSockets = this.sockets.filter((socket) => socket.readyState === WebSocket.OPEN);
        const timeoutMs = options.timeoutMs ?? 5000;
        const minHeads = Math.max(1, Math.floor(options.minHeads ?? 1));

        if (openSockets.length === 0) {
            throw new Error("No open relay sockets for head request");
        }

        return new Promise<RelayHead[]>((resolve, reject) => {
            const heads: RelayHead[] = [];
            const errors: any[] = [];
            const expectedResponses = openSockets.length;
            let settled = false;

            const finishIfReady = () => {
                if (settled) return;
                if (heads.length + errors.length >= expectedResponses) {
                    settled = true;
                    cleanup();
                    if (heads.length >= minHeads) {
                        resolve(heads);
                    } else {
                        reject(new Error(errors[0]?.message || "Relay head request failed"));
                    }
                }
            };

            const timeout = setTimeout(() => {
                if (settled) return;
                settled = true;
                cleanup();
                if (heads.length >= minHeads) {
                    resolve(heads);
                } else {
                    reject(new Error("Timeout waiting for relay heads"));
                }
            }, timeoutMs);

            const handler = (data: RelayHeadResponse) => {
                if (data?.subId !== subId || !data.head) return;
                heads.push(data.head);
                finishIfReady();
            };
            const errorHandler = (data: any) => {
                if (data?.subId !== subId) return;
                errors.push(data);
                finishIfReady();
            };

            const cleanup = () => {
                clearTimeout(timeout);
                this.off("relay_head_response", handler);
                this.off("error_frame", errorHandler);
            };

            this.on("relay_head_response", handler);
            this.on("error_frame", errorHandler);

            for (const socket of openSockets) {
                this.sendFrame(socket, "GET_HEAD", payload);
            }
        });
    }

    async getRelayHead(guildId: GuildId, options: RelayHeadRequestOptions = {}): Promise<RelayHead> {
        const heads = await this.getRelayHeads(guildId, { ...options, minHeads: 1 });
        return heads[0];
    }

    async checkRelayHeadConsistency(guildId: GuildId, options: RelayHeadRequestOptions = {}): Promise<RelayHeadQuorum> {
        const heads = await this.getRelayHeads(guildId, options);
        return summarizeRelayHeadQuorum(guildId, heads);
    }

    async getObservedRelayHeads(guildId: GuildId, options: RelayHeadRequestOptions = {}): Promise<RelayHead[]> {
        const subId = Math.random().toString(36).slice(2);
        const payload = await this.signedReadPayload("GET_HEADS", { subId, guildId });
        const timeoutMs = options.timeoutMs ?? 5000;

        return new Promise<RelayHead[]>((resolve, reject) => {
            const timeout = setTimeout(() => {
                cleanup();
                reject(new Error("Timeout waiting for observed relay heads"));
            }, timeoutMs);

            const handler = (data: RelayHeadsResponse) => {
                if (data?.subId !== subId) return;
                cleanup();
                resolve(Array.isArray(data.heads) ? data.heads : []);
            };
            const errorHandler = (data: any) => {
                if (data?.subId !== subId) return;
                cleanup();
                reject(new Error(data.message || "Observed relay heads request failed"));
            };

            const cleanup = () => {
                clearTimeout(timeout);
                this.off("relay_heads_response", handler);
                this.off("error_frame", errorHandler);
            };

            this.on("relay_heads_response", handler);
            this.on("error_frame", errorHandler);

            this.sendFrameToOpenSockets("GET_HEADS", payload);
        });
    }

    async checkObservedRelayHeadConsistency(
        guildId: GuildId,
        options: RelayHeadRequestOptions & RelayHeadQuorumOptions = {}
    ): Promise<RelayHeadQuorum> {
        const heads = await this.getObservedRelayHeads(guildId, options);
        return assertRelayHeadQuorum(guildId, heads, options);
    }

    async getState(guildId: GuildId, options: StateRequestOptions = {}): Promise<StateResponse> {
        const subId = Math.random().toString(36).slice(2);
        const payload = await this.signedReadPayload("GET_STATE", { subId, guildId, ...options });

        return new Promise<StateResponse>((resolve, reject) => {
            const timeout = setTimeout(() => {
                cleanup();
                reject(new Error("Timeout waiting for guild state"));
            }, 10000);

            const handler = (data: StateResponse) => {
                if (data.subId === subId) {
                    cleanup();
                    resolve(data);
                }
            };
            const errorHandler = (data: any) => {
                if (data?.subId === subId) {
                    cleanup();
                    reject(new Error(data.message || "Guild state request failed"));
                }
            };

            const cleanup = () => {
                clearTimeout(timeout);
                this.off("state_response", handler);
                this.off("error_frame", errorHandler);
            };

            this.on("state_response", handler);
            this.on("error_frame", errorHandler);

            this.sendFrameToOpenSockets("GET_STATE", payload);
        });
    }

    async getHistory(request: HistoryRequest): Promise<HistoryResponse> {
        const subId = Math.random().toString(36).slice(2);
        const payload = await this.signedReadPayload("GET_HISTORY", { subId, ...request });

        return new Promise<HistoryResponse>((resolve, reject) => {
            const timeout = setTimeout(() => {
                cleanup();
                reject(new Error("Timeout waiting for guild history"));
            }, 10000);

            const handler = (data: HistoryResponse) => {
                if (data.subId === subId) {
                    cleanup();
                    resolve(data);
                }
            };
            const errorHandler = (data: any) => {
                if (data?.subId === subId) {
                    cleanup();
                    reject(new Error(data.message || "Guild history request failed"));
                }
            };

            const cleanup = () => {
                clearTimeout(timeout);
                this.off("snapshot_response", handler);
                this.off("error_frame", errorHandler);
            };

            this.on("snapshot_response", handler);
            this.on("error_frame", errorHandler);

            this.sendFrameToOpenSockets("GET_HISTORY", payload);
        });
    }

    async search(request: SearchRequest): Promise<SearchResponse> {
        const subId = Math.random().toString(36).slice(2);
        const payload = await this.signedReadPayload("SEARCH", { subId, ...request });

        return new Promise<SearchResponse>((resolve, reject) => {
            const timeout = setTimeout(() => {
                cleanup();
                reject(new Error("Timeout waiting for guild search results"));
            }, 10000);

            const handler = (data: SearchResponse) => {
                if (data.subId === subId) {
                    cleanup();
                    resolve(data);
                }
            };
            const errorHandler = (data: any) => {
                if (data?.subId === subId) {
                    cleanup();
                    reject(new Error(data.message || "Guild search request failed"));
                }
            };

            const cleanup = () => {
                clearTimeout(timeout);
                this.off("search_response", handler);
                this.off("error_frame", errorHandler);
            };

            this.on("search_response", handler);
            this.on("error_frame", errorHandler);

            this.sendFrameToOpenSockets("SEARCH", payload);
        });
    }

    async publish(body: EventBody): Promise<string> {
        if (!this.keyPair) throw new Error("No keypair");
        const { pub, priv } = this.keyPair;
        const createdAt = Date.now();

        const unsigned = { body, author: pub, createdAt };
        const signature = await sign(priv, hashObject(unsigned));
        this.rememberOwnPublishProof(body, pub, createdAt, signature);

        if (this.relays.length === 0) {
            const targetGuildId = body.guildId || (body.type === "GUILD_CREATE" ? computeEventId({ ...unsigned, id: "", seq: 0, prevHash: null, signature } as any) : null);
            if (!targetGuildId) throw new Error("Cannot determine guildId for P2P publish");

            let state = this.state.get(targetGuildId);
            const seq = state ? state.headSeq + 1 : 0;
            const prevHash = state ? state.headHash : null;

            const event: GuildEvent = {
                id: "", seq, prevHash, createdAt, author: pub, body, signature
            };
            event.id = computeEventId(event);

            await this.updateState(event);
            this.emit("event", event);

            const eventFrame = encodeCgpFrame("EVENT", event, this.wireFormat);
            let gossipCount = 0;
            this.sockets.forEach(s => {
                if (this.sendEncodedFrame(s, eventFrame)) {
                    gossipCount++;
                }
            });
            // console.log(`P2P Publish: Gossiped event ${event.seq} to ${gossipCount} peers`);
            return event.id;
        }

        const payload = { body, author: pub, signature, createdAt };
        this.sendFrameToWriterSocket("PUBLISH", payload);

        return "";
    }

    async publishReliable(body: EventBody, options: PublishReliableOptions = {}): Promise<PublishAck> {
        if (!this.keyPair) throw new Error("No keypair");
        if (this.relays.length === 0) {
            const eventId = await this.publish(body);
            const state = this.state.get(body.guildId);
            return {
                clientEventId: options.clientEventId,
                guildId: body.guildId,
                eventId,
                seq: state?.headSeq ?? 0
            };
        }

        const { pub, priv } = this.keyPair;
        const createdAt = Date.now();
        const clientEventId = options.clientEventId || `${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const unsigned = { body, author: pub, createdAt };
        const signature = await sign(priv, hashObject(unsigned));
        this.rememberOwnPublishProof(body, pub, createdAt, signature);
        const payload = { body, author: pub, signature, createdAt, clientEventId };
        const timeoutMs = options.timeoutMs ?? 10000;

        return new Promise<PublishAck>((resolve, reject) => {
            let writer: WebSocket | undefined;
            const timeout = setTimeout(() => {
                cleanup();
                reject(new Error("Timeout waiting for publish acknowledgement"));
            }, timeoutMs);

            const ackHandler = (data: PublishAck) => {
                if (data?.clientEventId === clientEventId) {
                    cleanup();
                    resolve(data);
                }
            };
            const errorHandler = (data: any) => {
                if (data?.clientEventId === clientEventId) {
                    cleanup();
                    reject(new Error(data.message || "Publish rejected by relay"));
                }
            };
            const writerClosedHandler = () => {
                cleanup();
                reject(new Error("Writer relay connection closed before publish acknowledgement"));
            };

            const cleanup = () => {
                clearTimeout(timeout);
                this.off("publish_ack", ackHandler);
                this.off("error_frame", errorHandler);
                writer?.off?.("close", writerClosedHandler);
                writer?.off?.("error", writerClosedHandler);
            };

            this.on("publish_ack", ackHandler);
            this.on("error_frame", errorHandler);

            writer = this.sendEncodedFrameToWriterSocketWithSocket(encodeCgpFrame("PUBLISH", payload, this.wireFormat));

            if (!writer) {
                cleanup();
                reject(new Error("No open relay connection"));
                return;
            }
            writer.once?.("close", writerClosedHandler);
            writer.once?.("error", writerClosedHandler);
        });
    }

    async publishBatchReliable(bodies: EventBody[], options: PublishBatchReliableOptions = {}): Promise<PublishAck[]> {
        if (!this.keyPair) throw new Error("No keypair");
        if (bodies.length === 0) return [];
        if (this.relays.length === 0) {
            const results: PublishAck[] = [];
            for (const body of bodies) {
                results.push(await this.publishReliable(body, { timeoutMs: options.timeoutMs }));
            }
            return results;
        }

        const { pub, priv } = this.keyPair;
        const batchId = options.batchId || `${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const createdAtBase = Date.now();
        const events = await Promise.all(bodies.map(async (body, index) => {
            const createdAt = createdAtBase + index;
            const clientEventId = `${batchId}:${index}`;
            const unsigned = { body, author: pub, createdAt };
            const signature = await sign(priv, hashObject(unsigned));
            this.rememberOwnPublishProof(body, pub, createdAt, signature);
            return { body, author: pub, signature, createdAt, clientEventId };
        }));
        const payload = { batchId, events };
        const timeoutMs = options.timeoutMs ?? 10000;

        return new Promise<PublishAck[]>((resolve, reject) => {
            let writer: WebSocket | undefined;
            const timeout = setTimeout(() => {
                cleanup();
                reject(new Error("Timeout waiting for publish batch acknowledgement"));
            }, timeoutMs);

            const ackHandler = (data: PublishBatchAck) => {
                if (data?.batchId === batchId) {
                    const results = Array.isArray(data.results) ? data.results : [];
                    cleanup();
                    if (data.truncated || results.length !== events.length) {
                        reject(new Error(`Publish batch acknowledged ${results.length}/${events.length} events`));
                        return;
                    }
                    resolve(results);
                }
            };
            const errorHandler = (data: any) => {
                if (data?.batchId === batchId) {
                    cleanup();
                    reject(new Error(data.message || "Publish batch rejected by relay"));
                }
            };
            const writerClosedHandler = () => {
                cleanup();
                reject(new Error("Writer relay connection closed before publish batch acknowledgement"));
            };

            const cleanup = () => {
                clearTimeout(timeout);
                this.off("publish_batch_ack", ackHandler);
                this.off("error_frame", errorHandler);
                writer?.off?.("close", writerClosedHandler);
                writer?.off?.("error", writerClosedHandler);
            };

            this.on("publish_batch_ack", ackHandler);
            this.on("error_frame", errorHandler);

            writer = this.sendEncodedFrameToWriterSocketWithSocket(encodeCgpFrame("PUBLISH_BATCH", payload, this.wireFormat));

            if (!writer) {
                cleanup();
                reject(new Error("No open relay connection"));
                return;
            }
            writer.once?.("close", writerClosedHandler);
            writer.once?.("error", writerClosedHandler);
        });
    }

    getGuildState(guildId: GuildId): GuildState | undefined {
        return this.state.get(guildId);
    }

    async decryptMessage(event: GuildEvent): Promise<string> {
        if (!this.keyPair) throw new Error("No keypair");
        const body = event.body as Message;
        if (!body.encrypted) return body.content;

        // Try Group Key first
        const groupKey = this.groupKeys.get(body.guildId);
        if (groupKey) {
            try {
                // console.log(`Attempting group decryption for msg ${body.messageId}`);
                return await decrypt(Buffer.from(groupKey, "hex"), body.content, body.iv!);
            } catch (e) {
                // console.log(`Group decryption failed for msg ${body.messageId}, trying pairwise...`);
            }
        }

        // Fallback to Pairwise
        const shared = getSharedSecret(this.keyPair.priv, event.author);
        return await decrypt(shared, body.content, body.iv!);
    }

    close() {
        this.isClosed = true;
        this.sockets.forEach(s => s.close());
        this.sockets = [];
        this.socketUrls.clear();
        this.pluginsBySocket.clear();
        this.availablePlugins = [];
        if (this.server) {
            this.server.close();
        }
    }
}
