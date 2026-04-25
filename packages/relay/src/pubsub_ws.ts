import { WebSocket, WebSocketServer } from "ws";
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { encodeCgpPubSubFrame, parseCgpPubSubData, type CgpWireFormat } from "@cgp/core";
import type { RelayPubSubAdapter, RelayPubSubEnvelope, RelayPubSubSubscribeOptions } from "./server";

type Unsubscribe = () => Promise<void> | void;
type PubSubFrame = string | Uint8Array;

interface HandlerEntry {
    handler: (envelope: RelayPubSubEnvelope) => void;
    options?: RelayPubSubSubscribeOptions;
}

interface RetainedEnvelope {
    envelope: RelayPubSubEnvelope;
    frame: PubSubFrame;
    minSeq: number;
    maxSeq: number;
}

interface RetentionRecord {
    topic: string;
    envelope: RelayPubSubEnvelope;
    minSeq: number;
    maxSeq: number;
}

function pubSubWireFormatFromValue(value: unknown): CgpWireFormat | undefined {
    return value === "json" || value === "binary-json" || value === "binary-v1" || value === "binary-v2" ? value : undefined;
}

function defaultPubSubWireFormat(): CgpWireFormat {
    return pubSubWireFormatFromValue(process.env.CGP_PUBSUB_WIRE_FORMAT) ?? "json";
}

function pubSubFrame(kind: string, payload: unknown, wireFormat: CgpWireFormat): PubSubFrame {
    return encodeCgpPubSubFrame(kind as "PUB" | "SUB" | "UNSUB" | "ACK", payload, wireFormat);
}

function envelopeSeqRange(envelope: RelayPubSubEnvelope | undefined): { minSeq: number; maxSeq: number } | undefined {
    const events = Array.isArray(envelope?.events) && envelope.events.length > 0
        ? envelope.events
        : envelope?.event
            ? [envelope.event]
            : [];
    const headSeq = Number(envelope?.head?.headSeq);
    if (events.length === 0 && Number.isFinite(headSeq)) {
        return { minSeq: headSeq, maxSeq: headSeq };
    }
    if (events.length === 0) return undefined;
    let minSeq = Number.POSITIVE_INFINITY;
    let maxSeq = Number.NEGATIVE_INFINITY;
    for (const event of events) {
        if (!Number.isFinite(event.seq)) continue;
        minSeq = Math.min(minSeq, event.seq);
        maxSeq = Math.max(maxSeq, event.seq);
    }
    if (!Number.isFinite(minSeq) || !Number.isFinite(maxSeq)) return undefined;
    return { minSeq, maxSeq };
}

function normalizeSeq(value: unknown): number | undefined {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? Math.floor(parsed) : undefined;
}

function normalizePositiveInteger(value: unknown): number | undefined {
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : undefined;
}

function normalizePositiveIntegerWithDefault(value: unknown, fallback: number) {
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function mergeSubscribeOptions(entries: Iterable<HandlerEntry>): RelayPubSubSubscribeOptions | undefined {
    let afterSeq: number | undefined;
    let replayLimit: number | undefined;
    for (const entry of entries) {
        if (Number.isFinite(entry.options?.afterSeq)) {
            afterSeq = afterSeq === undefined
                ? entry.options!.afterSeq
                : Math.min(afterSeq, entry.options!.afterSeq!);
        }
        if (Number.isFinite(entry.options?.replayLimit) && entry.options!.replayLimit! > 0) {
            replayLimit = replayLimit === undefined
                ? entry.options!.replayLimit
                : Math.max(replayLimit, entry.options!.replayLimit!);
        }
    }
    if (afterSeq === undefined && replayLimit === undefined) return undefined;
    return { afterSeq, replayLimit };
}

export class WebSocketPubSubHub {
    private wss: WebSocketServer;
    private subscriptions = new Map<string, Set<WebSocket>>();
    private socketTopics = new Map<WebSocket, Set<string>>();
    private retained = new Map<string, RetainedEnvelope[]>();
    private retainedOffsets = new Map<string, number>();
    private retainedTopicsLoaded = new Set<string>();
    private retentionStreams = new Map<string, fs.WriteStream>();
    private maxBufferedBytes: number;
    private retainEnvelopesPerTopic: number;
    private retainLoadBytes: number;
    private retainDir?: string;
    private authToken?: string;
    private host: string;
    private wireFormat: CgpWireFormat;
    private fanoutSocketsPerTick: number;

    constructor(port: number, options: { maxBufferedBytes?: number; retainEnvelopesPerTopic?: number; retainDir?: string; authToken?: string; host?: string; wireFormat?: CgpWireFormat; fanoutSocketsPerTick?: number } = {}) {
        this.maxBufferedBytes = options.maxBufferedBytes ?? 8 * 1024 * 1024;
        this.retainEnvelopesPerTopic = options.retainEnvelopesPerTopic ?? Number(process.env.CGP_PUBSUB_RETAIN_ENVELOPES || 50000);
        this.retainLoadBytes = normalizePositiveIntegerWithDefault(process.env.CGP_PUBSUB_RETAIN_LOAD_BYTES, 64 * 1024 * 1024);
        this.retainDir = options.retainDir ?? process.env.CGP_PUBSUB_RETAIN_DIR;
        this.authToken = options.authToken ?? process.env.CGP_PUBSUB_TOKEN;
        this.host = options.host ?? process.env.CGP_PUBSUB_HOST ?? (this.authToken ? "0.0.0.0" : "127.0.0.1");
        this.wireFormat = options.wireFormat ?? defaultPubSubWireFormat();
        this.fanoutSocketsPerTick = options.fanoutSocketsPerTick ?? normalizePositiveIntegerWithDefault(process.env.CGP_PUBSUB_FANOUT_SOCKETS_PER_TICK, 4096);
        if (this.retainDir) {
            fs.mkdirSync(this.retainDir, { recursive: true });
        }
        this.wss = new WebSocketServer({ port, host: this.host, perMessageDeflate: false });

        this.wss.on("connection", (socket) => {
            configureWebSocketTransport(socket);
            this.socketTopics.set(socket, new Set());

            socket.on("message", (raw) => {
                try {
                    const { kind, payload } = parseCgpPubSubData(raw, { includeRawFrame: false }) as { kind: string; payload: any };
                    if (this.authToken && payload?.token !== this.authToken) {
                        socket.close(1008, "Unauthorized pubsub frame");
                        return;
                    }
                    if (kind === "SUB") {
                        this.subscribeSocket(socket, String(payload?.topic ?? ""), {
                            afterSeq: normalizeSeq(payload?.afterSeq),
                            replayLimit: normalizePositiveInteger(payload?.replayLimit)
                        });
                    } else if (kind === "UNSUB") {
                        this.unsubscribeSocket(socket, String(payload?.topic ?? ""));
                    } else if (kind === "PUB") {
                        const accepted = this.publish(String(payload?.topic ?? ""), payload?.envelope);
                        if (typeof payload?.id === "string" && payload.id) {
                            void Promise.resolve(accepted).then(() => {
                                if (socket.readyState === WebSocket.OPEN) {
                                    socket.send(pubSubFrame("ACK", { id: payload.id }, this.wireFormat));
                                }
                            });
                        }
                    }
                } catch {
                    socket.close(1003, "Invalid pubsub frame");
                }
            });

            socket.on("close", () => this.removeSocket(socket));
        });
    }

    private subscribeSocket(socket: WebSocket, topic: string, options: RelayPubSubSubscribeOptions = {}) {
        if (!topic) return;
        this.loadRetainedTopic(topic);
        if (process.env.LOADNET_TRACE_PUBSUB === "1") {
            console.log(JSON.stringify({ role: "pubsub", op: "sub", topic, afterSeq: options.afterSeq }));
        }
        const subscribers = this.subscriptions.get(topic) ?? new Set<WebSocket>();
        subscribers.add(socket);
        this.subscriptions.set(topic, subscribers);

        const topics = this.socketTopics.get(socket) ?? new Set<string>();
        topics.add(topic);
        this.socketTopics.set(socket, topics);
        this.replayRetained(socket, topic, options);
    }

    private unsubscribeSocket(socket: WebSocket, topic: string) {
        if (!topic) return;
        const subscribers = this.subscriptions.get(topic);
        subscribers?.delete(socket);
        if (subscribers?.size === 0) {
            this.subscriptions.delete(topic);
        }
        const topics = this.socketTopics.get(socket);
        topics?.delete(topic);
    }

    private removeSocket(socket: WebSocket) {
        const topics = this.socketTopics.get(socket);
        if (topics) {
            for (const topic of topics) {
                this.unsubscribeSocket(socket, topic);
            }
        }
        this.socketTopics.delete(socket);
    }

    private publish(topic: string, envelope: RelayPubSubEnvelope | undefined): Promise<void> | void {
        if (!topic || !envelope) return;
        this.loadRetainedTopic(topic);
        const subscribers = this.subscriptions.get(topic);
        if (process.env.LOADNET_TRACE_PUBSUB === "1" && topic.endsWith(":log")) {
            console.log(JSON.stringify({ role: "pubsub", op: "pub", topic, subscribers: subscribers?.size ?? 0, events: envelope.events?.length ?? (envelope.event ? 1 : 0) }));
        }
        const frame = pubSubFrame("PUB", { topic, envelope }, this.wireFormat);
        const retained = this.retain(topic, envelope, frame);
        if (!subscribers || subscribers.size === 0) return retained;

        this.fanoutFrame(subscribers, frame);
        return retained;
    }

    private retain(topic: string, envelope: RelayPubSubEnvelope, frame: PubSubFrame): Promise<void> | void {
        const range = envelopeSeqRange(envelope);
        if (!range) return;
        const retained = this.retained.get(topic) ?? [];
        let offset = this.retainedOffsets.get(topic) ?? 0;
        retained.push({ envelope, frame, ...range });
        while (retained.length - offset > this.retainEnvelopesPerTopic) {
            offset += 1;
        }
        if (offset > 1024 && offset * 2 > retained.length) {
            retained.splice(0, offset);
            offset = 0;
        }
        this.retained.set(topic, retained);
        this.retainedOffsets.set(topic, offset);
        const retainedWrite = this.appendRetainedEnvelope(topic, { topic, envelope, ...range });
        if (process.env.CGP_PUBSUB_SYNC_RETENTION === "1") {
            return retainedWrite;
        }
        void Promise.resolve(retainedWrite).catch((err) => {
            console.error(`Pubsub retention write failed for topic ${topic}:`, err);
        });
    }

    private topicRetentionPath(topic: string) {
        if (!this.retainDir) return undefined;
        const digest = createHash("sha256").update(topic).digest("hex");
        return path.join(this.retainDir, `${digest}.jsonl`);
    }

    private readRetentionTail(filePath: string) {
        const stats = fs.statSync(filePath);
        if (stats.size <= this.retainLoadBytes) {
            return fs.readFileSync(filePath, "utf8");
        }

        const start = Math.max(0, stats.size - this.retainLoadBytes);
        const length = stats.size - start;
        const buffer = Buffer.allocUnsafe(length);
        const fd = fs.openSync(filePath, "r");
        try {
            fs.readSync(fd, buffer, 0, length, start);
        } finally {
            fs.closeSync(fd);
        }
        const text = buffer.toString("utf8");
        const firstLineBreak = text.indexOf("\n");
        return firstLineBreak >= 0 ? text.slice(firstLineBreak + 1) : text;
    }

    private loadRetainedTopic(topic: string) {
        if (this.retainedTopicsLoaded.has(topic)) return;
        this.retainedTopicsLoaded.add(topic);
        const filePath = this.topicRetentionPath(topic);
        if (!filePath || !fs.existsSync(filePath)) return;

        const retained: RetainedEnvelope[] = [];
        const lines = this.readRetentionTail(filePath).split(/\r?\n/);
        for (const line of lines) {
            if (!line.trim()) continue;
            try {
                const record = JSON.parse(line) as RetentionRecord;
                if (record.topic !== topic || !record.envelope) continue;
                const range = envelopeSeqRange(record.envelope);
                if (!range) continue;
                retained.push({
                    envelope: record.envelope,
                    frame: pubSubFrame("PUB", { topic, envelope: record.envelope }, this.wireFormat),
                    minSeq: Number.isFinite(record.minSeq) ? record.minSeq : range.minSeq,
                    maxSeq: Number.isFinite(record.maxSeq) ? record.maxSeq : range.maxSeq
                });
            } catch {
                // Ignore corrupt journal lines; relay hash-chain validation still protects consumers.
            }
        }
        if (retained.length > this.retainEnvelopesPerTopic) {
            retained.splice(0, retained.length - this.retainEnvelopesPerTopic);
        }
        if (retained.length > 0) {
            this.retained.set(topic, retained);
            this.retainedOffsets.set(topic, 0);
        }
    }

    private appendRetainedEnvelope(topic: string, record: RetentionRecord): Promise<void> | void {
        const filePath = this.topicRetentionPath(topic);
        if (!filePath) return;
        let stream = this.retentionStreams.get(topic);
        if (!stream) {
            stream = fs.createWriteStream(filePath, { flags: "a", encoding: "utf8" });
            stream.on("error", (err) => {
                console.error(`Pubsub retention write failed for topic ${topic}:`, err);
            });
            this.retentionStreams.set(topic, stream);
        }
        return new Promise((resolve) => {
            stream.write(`${JSON.stringify(record)}\n`, () => resolve());
        });
    }

    private replayRetained(socket: WebSocket, topic: string, options: RelayPubSubSubscribeOptions) {
        if (options.afterSeq === undefined) return;
        const retained = this.retained.get(topic);
        if (!retained || retained.length === 0) return;
        const offset = this.retainedOffsets.get(topic) ?? 0;
        const replayLimit = options.replayLimit ?? retained.length;
        let replayed = 0;
        let low = offset;
        let high = retained.length;
        while (low < high) {
            const mid = Math.floor((low + high) / 2);
            const retainedEnvelope = retained[mid];
            if (!retainedEnvelope || retainedEnvelope.maxSeq <= options.afterSeq) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        for (let index = low; index < retained.length; index += 1) {
            const retainedEnvelope = retained[index];
            if (!retainedEnvelope) continue;
            if (replayed >= replayLimit) break;
            if (!this.sendFrame(socket, retainedEnvelope.frame)) break;
            replayed += 1;
        }
        if (process.env.LOADNET_TRACE_PUBSUB === "1" && replayed > 0) {
            console.log(JSON.stringify({ role: "pubsub", op: "replay", topic, afterSeq: options.afterSeq, replayed }));
        }
    }

    private sendFrame(socket: WebSocket, frame: PubSubFrame) {
        if (socket.readyState !== WebSocket.OPEN) return false;
        if (socket.bufferedAmount > this.maxBufferedBytes) {
            socket.close(1013, "Slow pubsub subscriber");
            return false;
        }
        socket.send(frame);
        return true;
    }

    private fanoutFrame(subscribers: Set<WebSocket>, frame: PubSubFrame) {
        if (subscribers.size <= this.fanoutSocketsPerTick) {
            for (const socket of subscribers) {
                this.sendFrame(socket, frame);
            }
            return;
        }

        const iterator = subscribers.values();
        const drain = () => {
            let sent = 0;
            while (sent < this.fanoutSocketsPerTick) {
                const next = iterator.next();
                if (next.done) {
                    return;
                }
                this.sendFrame(next.value, frame);
                sent += 1;
            }
            setImmediate(drain);
        };
        setImmediate(drain);
    }

    async close() {
        for (const socket of this.wss.clients) {
            socket.close(1001, "Pubsub hub shutting down");
        }
        await new Promise<void>((resolve) => this.wss.close(() => resolve()));
        await Promise.allSettled(Array.from(this.retentionStreams.values()).map((stream) => new Promise<void>((resolve) => {
            stream.end(() => resolve());
        })));
        this.retentionStreams.clear();
        this.subscriptions.clear();
        this.socketTopics.clear();
        this.retained.clear();
        this.retainedOffsets.clear();
    }
}

export class WebSocketRelayPubSubAdapter implements RelayPubSubAdapter {
    private url: string;
    private socket?: WebSocket;
    private handlers = new Map<string, Set<HandlerEntry>>();
    private pendingFrames: PubSubFrame[] = [];
    private pendingPublishes = new Map<string, PubSubFrame>();
    private drainWaiters: Array<() => void> = [];
    private publishCounter = 0;
    private maxPendingPublishes: number;
    private closed = false;
    private reconnectTimer?: NodeJS.Timeout;
    private authToken?: string;
    private wireFormat: CgpWireFormat;

    constructor(url: string, options: { maxPendingPublishes?: number; authToken?: string; wireFormat?: CgpWireFormat } = {}) {
        this.url = url;
        this.maxPendingPublishes = options.maxPendingPublishes ?? Number(process.env.CGP_PUBSUB_MAX_PENDING_PUBLISHES || 100000);
        this.authToken = options.authToken ?? process.env.CGP_PUBSUB_TOKEN;
        this.wireFormat = options.wireFormat ?? defaultPubSubWireFormat();
        this.connect();
    }

    private withAuth(payload: Record<string, unknown>) {
        return this.authToken ? { ...payload, token: this.authToken } : payload;
    }

    publish(topic: string, envelope: RelayPubSubEnvelope) {
        const id = `${Date.now().toString(36)}-${this.publishCounter++}`;
        const frame = pubSubFrame("PUB", this.withAuth({ id, topic, envelope }), this.wireFormat);
        if (this.pendingPublishes.size >= this.maxPendingPublishes) {
            const oldest = this.pendingPublishes.keys().next().value;
            if (oldest) {
                this.pendingPublishes.delete(oldest);
            }
        }
        this.pendingPublishes.set(id, frame);
        this.sendPublish(frame);
    }

    async drain(timeoutMs = 5000) {
        if (this.pendingPublishes.size === 0) {
            return;
        }
        await new Promise<void>((resolve, reject) => {
            const timer = setTimeout(() => {
                this.drainWaiters = this.drainWaiters.filter((waiter) => waiter !== done);
                reject(new Error("Timed out waiting for pubsub publish ACKs"));
            }, timeoutMs);
            timer.unref?.();
            const done = () => {
                clearTimeout(timer);
                resolve();
            };
            this.drainWaiters.push(done);
        });
    }

    subscribe(topic: string, handler: (envelope: RelayPubSubEnvelope) => void, options?: RelayPubSubSubscribeOptions): Unsubscribe {
        const handlers = this.handlers.get(topic) ?? new Set<HandlerEntry>();
        const entry: HandlerEntry = { handler, options };
        handlers.add(entry);
        this.handlers.set(topic, handlers);
        this.send(pubSubFrame("SUB", this.withAuth({ topic, ...options }), this.wireFormat));

        return () => {
            const current = this.handlers.get(topic);
            current?.delete(entry);
            if (!current || current.size === 0) {
                this.handlers.delete(topic);
                this.send(pubSubFrame("UNSUB", this.withAuth({ topic }), this.wireFormat));
            }
        };
    }

    async close() {
        this.closed = true;
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = undefined;
        }
        this.pendingFrames = [];
        this.pendingPublishes.clear();
        this.resolveDrainWaiters();
        const socket = this.socket;
        this.socket = undefined;
        if (socket && socket.readyState === WebSocket.OPEN) {
            socket.close(1000, "Adapter closed");
        }
    }

    private connect() {
        if (this.closed) return;
        const socket = new WebSocket(this.url, { perMessageDeflate: false });
        this.socket = socket;

        socket.on("open", () => {
            configureWebSocketTransport(socket);
            for (const [topic, handlers] of this.handlers) {
                socket.send(pubSubFrame("SUB", this.withAuth({ topic, ...mergeSubscribeOptions(handlers) }), this.wireFormat));
            }
            for (const frame of this.pendingPublishes.values()) {
                socket.send(frame);
            }
            const queued = this.pendingFrames;
            this.pendingFrames = [];
            for (const frame of queued) {
                socket.send(frame);
            }
        });

        socket.on("message", (raw) => {
            try {
                const { kind, payload } = parseCgpPubSubData(raw, { includeRawFrame: false }) as { kind: string; payload: any };
                if (kind === "ACK" && typeof payload?.id === "string") {
                    this.pendingPublishes.delete(payload.id);
                    if (this.pendingPublishes.size === 0) {
                        this.resolveDrainWaiters();
                    }
                    return;
                }
                if (kind !== "PUB") return;
                const topic = String(payload?.topic ?? "");
                const envelope = payload?.envelope as RelayPubSubEnvelope | undefined;
                if (!topic || !envelope) return;
                for (const entry of this.handlers.get(topic) ?? []) {
                    entry.handler(envelope);
                }
            } catch {
                // Ignore malformed hub frames. The relay itself still validates events.
            }
        });

        socket.on("close", () => this.scheduleReconnect(socket));
        socket.on("error", () => this.scheduleReconnect(socket));
    }

    private scheduleReconnect(socket: WebSocket) {
        if (this.closed || this.socket !== socket || this.reconnectTimer) {
            return;
        }
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = undefined;
            this.connect();
        }, 500);
    }

    private send(frame: PubSubFrame) {
        const socket = this.socket;
        if (socket?.readyState === WebSocket.OPEN) {
            socket.send(frame);
            return;
        }
        if (this.pendingFrames.length < 10_000) {
            this.pendingFrames.push(frame);
        }
    }

    private sendPublish(frame: PubSubFrame) {
        const socket = this.socket;
        if (socket?.readyState === WebSocket.OPEN) {
            socket.send(frame);
        }
    }

    private resolveDrainWaiters() {
        const waiters = this.drainWaiters;
        this.drainWaiters = [];
        for (const waiter of waiters) {
            waiter();
        }
    }
}

function configureWebSocketTransport(socket: WebSocket) {
    const transport = (socket as any)?._socket;
    if (transport && typeof transport.setNoDelay === "function") {
        transport.setNoDelay(true);
    }
}

function normalizeUrlList(value: string | string[]) {
    const raw = Array.isArray(value) ? value : value.split(",");
    const urls: string[] = [];
    const seen = new Set<string>();
    for (const entry of raw) {
        const url = String(entry || "").trim();
        if (!url || seen.has(url)) continue;
        seen.add(url);
        urls.push(url);
    }
    return urls;
}

function topicShardIndex(topic: string, shards: number) {
    if (shards <= 1) {
        return 0;
    }
    const digest = createHash("sha256").update(topic).digest();
    return digest.readUInt32BE(0) % shards;
}

export class ShardedWebSocketRelayPubSubAdapter implements RelayPubSubAdapter {
    private adapters: WebSocketRelayPubSubAdapter[];

    constructor(urls: string[] | string, options: { maxPendingPublishes?: number; authToken?: string; wireFormat?: CgpWireFormat } = {}) {
        const normalized = normalizeUrlList(urls);
        if (normalized.length === 0) {
            throw new Error("ShardedWebSocketRelayPubSubAdapter requires at least one pubsub URL");
        }
        this.adapters = normalized.map((url) => new WebSocketRelayPubSubAdapter(url, options));
    }

    publish(topic: string, envelope: RelayPubSubEnvelope) {
        return this.adapterForTopic(topic).publish(topic, envelope);
    }

    subscribe(topic: string, handler: (envelope: RelayPubSubEnvelope) => void, options?: RelayPubSubSubscribeOptions): Unsubscribe {
        return this.adapterForTopic(topic).subscribe(topic, handler, options);
    }

    async close() {
        await Promise.allSettled(this.adapters.map((adapter) => adapter.close()));
    }

    async drain(timeoutMs = 5000) {
        await Promise.all(this.adapters.map((adapter) => adapter.drain(timeoutMs)));
    }

    private adapterForTopic(topic: string) {
        return this.adapters[topicShardIndex(topic, this.adapters.length)];
    }
}
