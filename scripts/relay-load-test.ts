import fs from "fs";
import os from "os";
import path from "path";
import { performance } from "perf_hooks";
import { WebSocket } from "ws";
import type { RawData } from "ws";
import { RelayServer, LevelStore, MemoryStore, LocalRelayPubSubAdapter } from "@cgp/relay";
import type { Store } from "@cgp/relay";
import {
    computeEventId,
    encodeCgpFrame,
    generatePrivateKey,
    getPublicKey,
    hashObject,
    parseCgpWireData,
    sign
} from "@cgp/core";
import type { EventBody, GuildEvent, GuildId, ChannelId } from "@cgp/core";

type Scenario = "storage" | "fanout" | "all";
type StoreKind = "level" | "memory";
type WireFormat = "json" | "binary-json" | "binary-v1" | "binary-v2";
type FanoutParseMode = "json" | "count";

interface LoadProfile {
    members: number;
    messages: number;
    subscribers: number;
    publishMessages: number;
    batchSize: number;
    channelCount: number;
    samplePages: number;
    store: StoreKind;
}

const PROFILES: Record<string, LoadProfile> = {
    smoke: {
        members: 1_000,
        messages: 5_000,
        subscribers: 25,
        publishMessages: 200,
        batchSize: 1_000,
        channelCount: 1,
        samplePages: 10,
        store: "level"
    },
    "100k": {
        members: 100_000,
        messages: 1_000_000,
        subscribers: 1_000,
        publishMessages: 10_000,
        batchSize: 10_000,
        channelCount: 8,
        samplePages: 100,
        store: "level"
    },
    "1m": {
        members: 1_000_000,
        messages: 5_000_000,
        subscribers: 5_000,
        publishMessages: 50_000,
        batchSize: 10_000,
        channelCount: 16,
        samplePages: 250,
        store: "level"
    }
};

interface Metric {
    name: string;
    count?: number;
    ms: number;
    ratePerSec?: number;
    extra?: Record<string, unknown>;
}

interface LoadReport {
    profile: string;
    scenario: Scenario;
    config: LoadProfile & {
        dbPath: string;
        computeEventHashes: boolean;
        wireFormat: WireFormat;
        fanoutParse: FanoutParseMode;
        relays: number;
    };
    metrics: Metric[];
    memory: NodeJS.MemoryUsage;
    startedAt: string;
    finishedAt: string;
}

const args = process.argv.slice(2);
const metrics: Metric[] = [];
const positionalArgs = collectPositionalArgs();

function collectPositionalArgs() {
    const positional: string[] = [];
    for (let index = 0; index < args.length; index += 1) {
        const arg = args[index];
        if (arg.startsWith("--")) {
            if (!arg.includes("=") && args[index + 1] && !args[index + 1].startsWith("--")) {
                index += 1;
            }
            continue;
        }
        positional.push(arg);
    }
    return positional;
}

function argValue(name: string, fallback?: string) {
    const long = `--${name}`;
    const withEquals = `${long}=`;
    const equalsArg = args.find((arg) => arg.startsWith(withEquals));
    if (equalsArg) return equalsArg.slice(withEquals.length);
    const index = args.indexOf(long);
    if (index >= 0 && args[index + 1] && !args[index + 1].startsWith("--")) {
        return args[index + 1];
    }
    return process.env[`CGP_LOAD_${name.replace(/-/g, "_").toUpperCase()}`] ?? fallback;
}

function boolArg(name: string, fallback = false) {
    const value = argValue(name);
    if (value === undefined) return args.includes(`--${name}`) || fallback;
    return ["1", "true", "yes", "on"].includes(value.toLowerCase());
}

function numberArg(name: string, fallback: number, positionalIndex?: number) {
    const raw = argValue(name, positionalIndex === undefined ? undefined : positionalArgs[positionalIndex]);
    if (!raw) return fallback;
    const parsed = Number(raw.replace(/_/g, ""));
    return Number.isFinite(parsed) && parsed >= 0 ? Math.floor(parsed) : fallback;
}

function choiceArg<T extends string>(name: string, allowed: readonly T[], fallback: T, positionalIndex?: number): T {
    const raw = argValue(name, positionalIndex === undefined ? undefined : positionalArgs[positionalIndex]);
    return allowed.includes(raw as T) ? raw as T : fallback;
}

function describeUsage() {
    console.log(`
CGP relay load test

Usage:
  npm run load:relay -- smoke all
  npm run load:relay -- 100k storage 100000 1000000
  npm run load:relay -- 1m storage 1000000 5000000
  npm run load:relay -- smoke fanout 0 0 1000 10000 1000

Options:
  --profile smoke|100k|1m
  --scenario storage|fanout|all
  --members N
  --messages N
  --subscribers N
  --publish-messages N
  --batch-size N
  --channels N
  --sample-pages N
  --store level|memory
  --db PATH
  --output PATH
  --keep-db
  --compute-event-hashes
--wire-format json|binary-json|binary-v1|binary-v2
  --fanout-parse json|count
  --relays N
`);
}

if (args.includes("--help") || args.includes("-h")) {
    describeUsage();
    process.exit(0);
}

function mergeConfig(profileName: string): LoadProfile {
    const base = PROFILES[profileName] ?? PROFILES.smoke;
    return {
        members: numberArg("members", base.members, 2),
        messages: numberArg("messages", base.messages, 3),
        subscribers: numberArg("subscribers", base.subscribers, 4),
        publishMessages: numberArg("publish-messages", base.publishMessages, 5),
        batchSize: Math.max(1, numberArg("batch-size", base.batchSize, 6)),
        channelCount: Math.max(1, numberArg("channels", base.channelCount, 7)),
        samplePages: Math.max(1, numberArg("sample-pages", base.samplePages, 8)),
        store: choiceArg("store", ["level", "memory"] as const, base.store, 9)
    };
}

async function timed(name: string, count: number | undefined, fn: () => Promise<Record<string, unknown> | void>) {
    const start = performance.now();
    const extra = await fn();
    const ms = performance.now() - start;
    const metric: Metric = {
        name,
        count,
        ms: Number(ms.toFixed(3)),
        ratePerSec: count && ms > 0 ? Number((count / (ms / 1000)).toFixed(2)) : undefined,
        extra: extra || undefined
    };
    metrics.push(metric);
    console.log(formatMetric(metric));
    return metric;
}

function formatMetric(metric: Metric) {
    const parts = [
        metric.name.padEnd(34),
        `${metric.ms.toFixed(3)} ms`.padStart(14)
    ];
    if (metric.count !== undefined) parts.push(`${metric.count} items`.padStart(14));
    if (metric.ratePerSec !== undefined) parts.push(`${metric.ratePerSec}/s`.padStart(14));
    if (metric.extra) parts.push(JSON.stringify(metric.extra));
    return parts.join("  ");
}

function fakeHex(index: number, width = 64) {
    return index.toString(16).padStart(width, "0").slice(-width);
}

function fakeUserId(index: number) {
    return `02${fakeHex(index, 64).slice(0, 64)}`.slice(0, 66);
}

function messageId(index: number) {
    return fakeHex(index + 1, 64);
}

function eventIdFor(seq: number, event: GuildEvent, computeHashes: boolean) {
    return computeHashes ? computeEventId(event) : fakeHex(seq + 1, 64);
}

function makeSequencer(guildId: GuildId, author: string, computeHashes: boolean) {
    let seq = 0;
    let prevHash: string | null = null;
    const createdAtBase = Date.now();
    return (body: EventBody, eventAuthor = author): GuildEvent => {
        const event: GuildEvent = {
            id: "",
            seq,
            prevHash,
            createdAt: createdAtBase + seq,
            author: eventAuthor,
            body,
            signature: "00"
        };
        event.id = eventIdFor(seq, event, computeHashes);
        prevHash = event.id;
        seq += 1;
        return event;
    };
}

async function appendBatch(store: Store, guildId: GuildId, events: GuildEvent[]) {
    if (events.length === 0) return;
    if (store.appendEvents) {
        await store.appendEvents(guildId, events);
        return;
    }
    for (const event of events) {
        await store.append(guildId, event);
    }
}

async function createStore(kind: StoreKind, dbPath: string): Promise<Store> {
    if (kind === "memory") {
        return new MemoryStore();
    }
    const store = new LevelStore(dbPath);
    await (store as any).db?.open?.();
    return store;
}

async function runStorageScenario(config: LoadProfile, dbPath: string, computeEventHashes: boolean) {
    if (config.store === "level" && fs.existsSync(dbPath)) {
        fs.rmSync(dbPath, { recursive: true, force: true });
    }
    const store = await createStore(config.store, dbPath);
    const guildId = hashObject({ load: "guild" });
    const owner = fakeUserId(0);
    const channels = Array.from({ length: config.channelCount }, (_, index) => hashObject({ channel: index }));
    const nextEvent = makeSequencer(guildId, owner, computeEventHashes);

    await timed("storage.bootstrap", 1 + channels.length, async () => {
        const bootstrap = [
            nextEvent({ type: "GUILD_CREATE", guildId, name: "Load Test Guild", policies: { posting: "public" } } as EventBody),
            ...channels.map((channelId, index) => nextEvent({
                type: "CHANNEL_CREATE",
                guildId,
                channelId,
                name: `load-${index}`,
                kind: "text"
            } as EventBody))
        ];
        await appendBatch(store, guildId, bootstrap);
    });

    await timed("storage.append-members", config.members, async () => {
        let batch: GuildEvent[] = [];
        for (let index = 1; index <= config.members; index += 1) {
            const userId = fakeUserId(index);
            batch.push(nextEvent({
                type: "MEMBER_UPDATE",
                guildId,
                userId,
                nickname: `user-${index}`,
                bio: `load member ${index}`
            } as EventBody, userId));
            if (batch.length >= config.batchSize) {
                await appendBatch(store, guildId, batch);
                batch = [];
            }
        }
        await appendBatch(store, guildId, batch);
    });

    await timed("storage.append-messages", config.messages, async () => {
        let batch: GuildEvent[] = [];
        for (let index = 0; index < config.messages; index += 1) {
            const channelId = channels[index % channels.length] as ChannelId;
            batch.push(nextEvent({
                type: "MESSAGE",
                guildId,
                channelId,
                messageId: messageId(index),
                content: `load message ${index}`
            } as EventBody, fakeUserId((index % Math.max(1, config.members)) + 1)));
            if (batch.length >= config.batchSize) {
                await appendBatch(store, guildId, batch);
                batch = [];
            }
        }
        await appendBatch(store, guildId, batch);
    });

    await timed("storage.get-last-event", undefined, async () => {
        const event = await store.getLastEvent(guildId);
        return { seq: event?.seq ?? null };
    });

    await timed("storage.history-tail-page", config.samplePages, async () => {
        let newestSeq: number | undefined;
        for (let index = 0; index < config.samplePages; index += 1) {
            const events = store.getHistory
                ? await store.getHistory({ guildId, channelId: channels[index % channels.length], limit: 100 })
                : [];
            newestSeq = events[events.length - 1]?.seq;
        }
        return { lastSeq: newestSeq ?? null };
    });

    await timed("storage.member-page-seek", config.samplePages, async () => {
        let cursor: string | undefined;
        for (let index = 0; index < config.samplePages; index += 1) {
            const afterUserId = fakeUserId(Math.floor((index / config.samplePages) * Math.max(1, config.members)));
            const page = store.getMembersPage
                ? await store.getMembersPage({ guildId, afterUserId, limit: 100 })
                : { members: [], nextCursor: null };
            cursor = page.nextCursor ?? undefined;
        }
        return { lastCursor: cursor ?? null };
    });

    await timed("storage.member-search", Math.min(config.samplePages, 25), async () => {
        if (!store.searchMembers) return { skipped: true };
        let hits = 0;
        const samples = Math.min(config.samplePages, 25);
        for (let index = 0; index < samples; index += 1) {
            const target = Math.max(1, Math.floor((index / samples) * Math.max(1, config.members)));
            const page = await store.searchMembers({ guildId, query: `user-${target}`, limit: 10 });
            hits += page.members.length;
        }
        return { hits };
    });

    await timed("storage.message-ref-random", Math.min(config.samplePages, 100), async () => {
        if (!store.getMessageRef) return { skipped: true };
        let hits = 0;
        const samples = Math.min(config.samplePages, 100);
        for (let index = 0; index < samples; index += 1) {
            const target = Math.floor((index / samples) * Math.max(1, config.messages));
            const ref = await store.getMessageRef({ guildId, messageId: messageId(target) });
            if (ref) hits += 1;
        }
        return { hits };
    });

    await store.close();
    return { guildId, channels };
}

function waitForSocketOpen(socket: WebSocket) {
    if (socket.readyState === WebSocket.OPEN) {
        return Promise.resolve();
    }
    return new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error("Socket open timeout")), 10000);
        socket.once("open", () => {
            clearTimeout(timeout);
            resolve();
        });
        socket.once("error", reject);
    });
}

async function connectSocketWithRetry(url: string, retries = 5): Promise<WebSocket> {
    let lastError: unknown;
    for (let attempt = 0; attempt <= retries; attempt += 1) {
        const socket = quietSocket(new WebSocket(url, { perMessageDeflate: false }));
        try {
            await waitForSocketOpen(socket);
            return socket;
        } catch (error) {
            lastError = error;
            closeSocket(socket);
            await new Promise(resolve => setTimeout(resolve, 50 * (attempt + 1)));
        }
    }
    throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

function quietSocket(socket: WebSocket) {
    socket.on("error", () => {
        // Load tests intentionally push connection limits; report failures through awaited promises instead.
    });
    return socket;
}

function closeSocket(socket: WebSocket | undefined) {
    if (!socket) return;
    try {
        if (socket.readyState === WebSocket.OPEN) {
            socket.close();
        } else if (socket.readyState === WebSocket.CONNECTING) {
            socket.terminate();
        }
    } catch {
        // Best-effort cleanup in load tests.
    }
}

function sendFrame(socket: WebSocket, wireFormat: WireFormat, kind: string, payload: unknown) {
    socket.send(encodeCgpFrame(kind, payload, wireFormat));
}

function waitForAllSnapshots(sockets: WebSocket[], expected: number) {
    return new Promise<void>((resolve, reject) => {
        const readySockets = new Set<WebSocket>();
        const timeout = setTimeout(() => reject(new Error(`Timed out waiting for ${expected} distinct snapshots, got ${readySockets.size}`)), 30000);
        const cleanup = () => {
            clearTimeout(timeout);
            for (const socket of sockets) {
                socket.off("message", onMessage);
            }
        };
        const onMessage = function onMessage(this: WebSocket, raw: RawData) {
            const { kind } = parseCgpWireData(raw, { includeRawFrame: false });
            if (kind === "SNAPSHOT") {
                readySockets.add(this);
                if (readySockets.size >= expected) {
                    cleanup();
                    resolve();
                }
            }
        };
        for (const socket of sockets) {
            socket.on("message", onMessage);
        }
    });
}

function waitForFanoutMessages(
    sockets: WebSocket[],
    expectedBySocket: number[],
    publishBatchSize: number,
    parseMode: FanoutParseMode
) {
    return new Promise<{ received: number }>((resolve, reject) => {
        const expected = expectedBySocket.reduce((sum, count) => sum + count, 0);
        let received = 0;
        const socketIndexes = new Map<WebSocket, number>();
        sockets.forEach((socket, index) => socketIndexes.set(socket, index));
        const perSocketReceived = new Map<WebSocket, number>();
        const timeout = setTimeout(() => {
            cleanup();
            const minSocket = Math.min(...sockets.map((socket) => perSocketReceived.get(socket) ?? 0));
            reject(new Error(`Timed out waiting for fanout events. Received ${received}/${expected}; slowest socket received ${minSocket}`));
        }, Math.max(30000, Math.max(...expectedBySocket, 1) * 100));
        const cleanup = () => {
            clearTimeout(timeout);
            for (const socket of sockets) {
                socket.off("message", onMessage);
            }
        };
        const onMessage = function onMessage(this: WebSocket, raw: RawData) {
            const { kind, payload } = parseCgpWireData(raw, { includeRawFrame: false }) as { kind: string; payload: any };
            const socketIndex = socketIndexes.get(this) ?? 0;
            const expectedForSocket = expectedBySocket[socketIndex] ?? 0;
            if (expectedForSocket <= 0) {
                return;
            }
            const current = perSocketReceived.get(this) ?? 0;
            if (current >= expectedForSocket) {
                return;
            }

            if (parseMode === "count") {
                const eventCount = kind === "EVENT" ? 1 : kind === "BATCH" && Array.isArray(payload) ? payload.length : 0;
                const increment = Math.min(eventCount, expectedForSocket - current);
                if (increment <= 0) {
                    return;
                }
                perSocketReceived.set(this, current + increment);
                received += increment;
                if (received >= expected) {
                    cleanup();
                    resolve({ received });
                }
                return;
            }

            if (kind === "EVENT") {
                perSocketReceived.set(this, current + 1);
                received += 1;
            } else if (kind === "BATCH") {
                const increment = Array.isArray(payload) ? payload.length : 0;
                perSocketReceived.set(this, current + increment);
                received += increment;
            }
            if (received >= expected) {
                cleanup();
                resolve({ received });
            }
        };
        for (const socket of sockets) {
            socket.on("message", onMessage);
        }
    });
}

async function signedPayload(body: EventBody, author: string, priv: Uint8Array, createdAt: number) {
    return {
        body,
        author,
        createdAt,
        signature: await sign(priv, hashObject({ body, author, createdAt }))
    };
}

async function runFanoutScenario(config: LoadProfile) {
    const port = numberArg("port", 18_747 + Math.floor(Math.random() * 1000));
    const wireFormat = choiceArg("wire-format", ["json", "binary-json", "binary-v1", "binary-v2"] as const, "json", 2);
    const fanoutParse = choiceArg("fanout-parse", ["json", "count"] as const, "json", 4);
    const relayCount = Math.max(1, numberArg("relays", 1));
    const pubSubAdapter = relayCount > 1 ? new LocalRelayPubSubAdapter() : undefined;
    const relays = Array.from({ length: relayCount }, (_, index) => new RelayServer(port + index, new MemoryStore(), [], {
            enableDefaultPlugins: false,
            maxFrameBytes: 512 * 1024 * 1024,
            maxPublishBatchSize: Math.max(config.batchSize, config.publishMessages),
            wireFormat,
            pubSubAdapter,
            instanceId: `load-relay-${index}`
        }));
    await new Promise(resolve => setTimeout(resolve, 100));
    const relayUrls = relays.map((_, index) => `ws://127.0.0.1:${port + index}`);
    const ownerPriv = generatePrivateKey();
    const owner = getPublicKey(ownerPriv);
    const guildId = hashObject({ load: "fanout", port }) as GuildId;
    const channelIds = Array.from({ length: Math.max(1, config.channelCount) }, (_, index) => (
        hashObject({ load: "fanout-channel", port, index }) as ChannelId
    ));
    const nextEvent = makeSequencer(guildId, owner, true);

    const bootstrapEvents = [
        nextEvent({ type: "GUILD_CREATE", guildId, name: "Fanout Load Guild", policies: { posting: "public" } } as EventBody),
        ...channelIds.map((channelId, index) => (
            nextEvent({ type: "CHANNEL_CREATE", guildId, channelId, name: `fanout-${index}`, kind: "text" } as EventBody)
        ))
    ];
    for (const relay of relays) {
        await appendBatch((relay as any).store, guildId, bootstrapEvents);
    }

    const subscribers: WebSocket[] = [];
    let publisher: WebSocket | undefined;
    try {
        await timed("fanout.connect-subscribers", config.subscribers, async () => {
            const connectBatchSize = Math.min(250, Math.max(1, numberArg("connect-batch", 50)));
            for (let offset = 0; offset < config.subscribers; offset += connectBatchSize) {
                const count = Math.min(connectBatchSize, config.subscribers - offset);
                const batch = await Promise.all(Array.from({ length: count }, (_, index) => {
                    const subscriberIndex = offset + index;
                    return connectSocketWithRetry(relayUrls[subscriberIndex % relayUrls.length]);
                }));
                subscribers.push(...batch);
            }
        });

        await timed("fanout.subscribe", config.subscribers, async () => {
            const snapshotsReady = waitForAllSnapshots(subscribers, config.subscribers);
            for (let index = 0; index < subscribers.length; index += 1) {
                sendFrame(subscribers[index], wireFormat, "SUB", {
                    subId: `sub-${index}`,
                    guildId,
                    channels: [channelIds[index % channelIds.length]]
                });
            }
            await snapshotsReady;
        });

        publisher = await connectSocketWithRetry(relayUrls[0]);
        const prepared = [];
        const prepareStart = performance.now();
        const createdAt = Date.now();
        const messagesByChannel = new Array(channelIds.length).fill(0);
        for (let index = 0; index < config.publishMessages; index += 1) {
            const channelIndex = index % channelIds.length;
            messagesByChannel[channelIndex] += 1;
            prepared.push(await signedPayload({
                type: "MESSAGE",
                guildId,
                channelId: channelIds[channelIndex],
                messageId: messageId(index),
                content: `fanout message ${index}`
            } as EventBody, owner, ownerPriv, createdAt + index));
        }
        const prepareMs = performance.now() - prepareStart;
        const prepareMetric: Metric = {
            name: "fanout.prepare-signatures",
            count: config.publishMessages,
            ms: Number(prepareMs.toFixed(3)),
            ratePerSec: prepareMs > 0 ? Number((config.publishMessages / (prepareMs / 1000)).toFixed(2)) : undefined
        };
        metrics.push(prepareMetric);
        console.log(formatMetric(prepareMetric));

        if (fanoutParse === "count" && channelIds.length > 1) {
            throw new Error("--fanout-parse=count only supports single-channel fanout because it intentionally avoids parsing partial batch sizes");
        }

        const expectedBySocket = subscribers.map((_, index) => messagesByChannel[index % channelIds.length] ?? 0);
        const expectedDeliveries = expectedBySocket.reduce((sum, count) => sum + count, 0);
        const fanoutReady = waitForFanoutMessages(subscribers, expectedBySocket, config.batchSize, fanoutParse);
        await timed("fanout.publish-batches", config.publishMessages, async () => {
            for (let index = 0; index < prepared.length; index += config.batchSize) {
                const events = prepared.slice(index, index + config.batchSize);
                sendFrame(publisher!, wireFormat, "PUBLISH_BATCH", { batchId: `batch-${index}`, events });
            }
        });
        await timed("fanout.delivery", expectedDeliveries, async () => {
            const result = await fanoutReady;
            return result;
        });
    } finally {
        closeSocket(publisher);
        for (const socket of subscribers) {
            closeSocket(socket);
        }
        await Promise.all(relays.map((relay) => relay.close()));
        pubSubAdapter?.close();
    }
}

async function main() {
    const profile = argValue("profile", positionalArgs[0] ?? "smoke")!;
    const scenario = choiceArg("scenario", ["storage", "fanout", "all"] as const, "all", 1);
    const config = mergeConfig(profile);
    const computeEventHashes = boolArg("compute-event-hashes", profile !== "smoke");
    const wireFormat = choiceArg("wire-format", ["json", "binary-json", "binary-v1", "binary-v2"] as const, "json", 2);
    const fanoutParse = choiceArg("fanout-parse", ["json", "count"] as const, "json", 4);
    const relays = Math.max(1, numberArg("relays", 1));
    const keepDb = boolArg("keep-db", false);
    const dbPath = path.resolve(argValue("db", path.join(os.tmpdir(), `cgp-load-${Date.now()}-${process.pid}-${Math.random().toString(36).slice(2)}`))!);
    const startedAt = new Date().toISOString();

    console.log(`CGP load test profile=${profile} scenario=${scenario} store=${config.store}`);
    console.log(JSON.stringify({ ...config, dbPath, computeEventHashes, wireFormat, fanoutParse, relays }, null, 2));

    try {
        if (scenario === "storage" || scenario === "all") {
            await runStorageScenario(config, dbPath, computeEventHashes);
        }
        if (scenario === "fanout" || scenario === "all") {
            await runFanoutScenario(config);
        }
    } finally {
        if (!keepDb && config.store === "level" && fs.existsSync(dbPath)) {
            fs.rmSync(dbPath, { recursive: true, force: true });
        }
    }

    const report: LoadReport = {
        profile,
        scenario,
        config: { ...config, dbPath, computeEventHashes, wireFormat, fanoutParse, relays },
        metrics,
        memory: process.memoryUsage(),
        startedAt,
        finishedAt: new Date().toISOString()
    };

    const output = argValue("output", positionalArgs[3]);
    if (output) {
        fs.mkdirSync(path.dirname(path.resolve(output)), { recursive: true });
        fs.writeFileSync(output, JSON.stringify(report, null, 2));
        console.log(`Wrote report: ${path.resolve(output)}`);
    }

    console.log("Final memory:", JSON.stringify(report.memory));
}

main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
});
