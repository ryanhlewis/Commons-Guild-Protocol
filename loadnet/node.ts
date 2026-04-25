import fs from "fs";
import path from "path";
import { execFileSync } from "child_process";
import WebSocket from "ws";
import { CgpClient } from "@cgp/client";
import { encodeCgpFrame, EventBody, generatePrivateKey, getPublicKey, hashObject, parseCgpWireData, sign, summarizeRelayHeadQuorum, verifyRelayHead } from "@cgp/core";
import type { CgpWireFormat, RelayHead } from "@cgp/core";
import { MemoryStore, RelayServer, ShardedWebSocketRelayPubSubAdapter, WebSocketPubSubHub, WebSocketRelayPubSubAdapter } from "@cgp/relay";
import { defaultProfile, normalizeProfile, LoadnetProfile } from "./profile";

interface ScenarioFile {
    guildId: string;
    channelIds: string[];
    ownerPub: string;
    ownerPrivHex: string;
    profile: LoadnetProfile;
    createdAt: string;
}

const DATA_DIR = process.env.LOADNET_DATA_DIR || "/data";
const READY_DIR = path.join(DATA_DIR, "ready");
const METRICS_DIR = path.join(DATA_DIR, "metrics");
const NETEM_DIR = path.join(DATA_DIR, "netem");
const SCENARIO_PATH = path.join(DATA_DIR, "scenario.json");
const RUN_ID = process.env.LOADNET_RUN_ID || "default";

function envNumber(name: string, fallback: number) {
    const value = Number(process.env[name]);
    return Number.isFinite(value) ? value : fallback;
}

function profileFromEnv() {
    try {
        return normalizeProfile(JSON.parse(process.env.LOADNET_PROFILE || "{}"));
    } catch {
        return defaultProfile;
    }
}

function loadnetWireFormat(profile?: LoadnetProfile): CgpWireFormat {
    const value = process.env.LOADNET_WIRE_FORMAT ?? profile?.wireFormat;
    return value === "binary-json" || value === "binary-v1" || value === "binary-v2" ? value : "json";
}

function sendFrame(socket: WebSocket, kind: string, payload: unknown, wireFormat = loadnetWireFormat()) {
    socket.send(encodeCgpFrame(kind, payload, wireFormat));
}

function configureNodeSocket(ws: WebSocket) {
    const transport = (ws as unknown as { _socket?: { setNoDelay?: (noDelay?: boolean) => void } })._socket;
    transport?.setNoDelay?.(true);
}

function hex(bytes: Uint8Array) {
    return Buffer.from(bytes).toString("hex");
}

function bytes(hexValue: string) {
    return new Uint8Array(Buffer.from(hexValue, "hex"));
}

function ensureDirs() {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.mkdirSync(READY_DIR, { recursive: true });
    fs.mkdirSync(METRICS_DIR, { recursive: true });
    fs.mkdirSync(NETEM_DIR, { recursive: true });
}

function writeJson(filePath: string, value: unknown) {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, JSON.stringify(value, null, 2));
}

async function sleep(ms: number) {
    await new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForFile(filePath: string, timeoutMs = 60000) {
    const started = Date.now();
    while (!fs.existsSync(filePath)) {
        if (Date.now() - started > timeoutMs) {
            throw new Error(`Timed out waiting for ${filePath}`);
        }
        await sleep(250);
    }
}

async function waitForReadyFiles(expected: number, timeoutMs = 60000) {
    if (expected <= 0) return;
    const started = Date.now();
    while (true) {
        const count = fs.existsSync(READY_DIR)
            ? fs.readdirSync(READY_DIR)
                .filter((name) => name.endsWith(".ready"))
                .map((name) => JSON.parse(fs.readFileSync(path.join(READY_DIR, name), "utf8")))
                .filter((ready) => (ready.runId || "default") === RUN_ID)
                .length
            : 0;
        if (count >= expected) return;
        if (Date.now() - started > timeoutMs) {
            throw new Error(`Timed out waiting for subscriber workers: ${count}/${expected}`);
        }
        await sleep(250);
    }
}

async function requestRelayHead(relayUrl: string, guildId: string): Promise<{ relayUrl: string; head?: RelayHead; error?: string }> {
    return await new Promise((resolve) => {
        const socket = new WebSocket(relayUrl, { perMessageDeflate: false });
        const subId = `head-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const timeout = setTimeout(() => {
            socket.close();
            resolve({ relayUrl, error: "timeout" });
        }, 10_000);

        socket.once("open", () => {
            configureNodeSocket(socket);
            sendFrame(socket, "GET_HEAD", { subId, guildId });
        });
        socket.once("error", (error) => {
            clearTimeout(timeout);
            socket.close();
            resolve({ relayUrl, error: error instanceof Error ? error.message : String(error) });
        });
        socket.on("message", (raw) => {
            try {
                const { kind, payload } = parseCgpWireData(raw, { includeRawFrame: false }) as { kind: string; payload: any };
                if (kind === "RELAY_HEAD" && payload?.subId === subId) {
                    clearTimeout(timeout);
                    socket.close();
                    resolve({ relayUrl, head: payload.head });
                }
                if (kind === "ERROR" && payload?.subId === subId) {
                    clearTimeout(timeout);
                    socket.close();
                    resolve({ relayUrl, error: payload?.message || payload?.code || "head request failed" });
                }
            } catch (error: any) {
                clearTimeout(timeout);
                socket.close();
                resolve({ relayUrl, error: error?.message || String(error) });
            }
        });
    });
}

async function waitForRelayAtHead(relayUrls: string[], guildId: string, minHeadSeq: number, timeoutMs: number) {
    const started = Date.now();
    let best: { relayUrl: string; head: RelayHead } | undefined;
    let lastErrors: Array<{ relayUrl: string; error?: string }> = [];

    while (Date.now() - started < timeoutMs) {
        const results = await Promise.all(relayUrls.map((relayUrl) => requestRelayHead(relayUrl, guildId)));
        lastErrors = results
            .filter((result) => result.error)
            .map((result) => ({ relayUrl: result.relayUrl, error: result.error }));
        const heads = results.flatMap((result) => result.head ? [{ relayUrl: result.relayUrl, head: result.head }] : []);
        const validHeads = heads.filter((entry) => verifyRelayHead(entry.head));
        const quorum = summarizeRelayHeadQuorum(guildId, validHeads.map((entry) => entry.head));
        if (quorum.conflicts.length > 0) {
            throw new Error(`Relay head equivocation detected during final backfill: ${quorum.conflicts[0]?.reason ?? "conflict"}`);
        }

        best = validHeads
            .filter((entry) => entry.head.guildId === guildId)
            .sort((left, right) => Number(right.head.headSeq) - Number(left.head.headSeq))[0];
        if (best && Number(best.head.headSeq) >= minHeadSeq) {
            return best;
        }

        await sleep(500);
    }

    const bestSeq = best ? Number(best.head.headSeq) : null;
    throw new Error(`Timed out waiting for relay head >= ${minHeadSeq}; best=${bestSeq}, errors=${JSON.stringify(lastErrors.slice(0, 3))}`);
}

async function collectRelayHeads(profile: LoadnetProfile) {
    if (!fs.existsSync(SCENARIO_PATH)) {
        return undefined;
    }
    const scenario = JSON.parse(fs.readFileSync(SCENARIO_PATH, "utf8")) as ScenarioFile;
    const results = await Promise.all(
        Array.from({ length: profile.relays }, (_, index) => requestRelayHead(`ws://relay-${index}:7447`, scenario.guildId))
    );
    const heads = results.flatMap((result) => result.head ? [result.head] : []);
    const quorum = summarizeRelayHeadQuorum(scenario.guildId, heads);
    return {
        guildId: scenario.guildId,
        results,
        validCount: quorum.validHeads.length,
        invalidCount: results.filter((result) => result.head && !verifyRelayHead(result.head)).length,
        errorCount: results.filter((result) => result.error).length,
        conflictCount: quorum.conflicts.length,
        canonical: quorum.canonical,
        conflicts: quorum.conflicts.map((conflict) => ({
            guildId: conflict.guildId,
            seq: conflict.seq,
            reason: conflict.reason,
            leftRelayId: conflict.left.relayId,
            leftHash: conflict.left.headHash,
            rightRelayId: conflict.right.relayId,
            rightHash: conflict.right.headHash
        }))
    };
}

async function waitForClusterRelayHeads(profile: LoadnetProfile, expectedHeadSeq: number, timeoutMs: number) {
    const started = Date.now();
    let latest: Awaited<ReturnType<typeof collectRelayHeads>> | undefined;
    while (Date.now() - started < timeoutMs) {
        latest = await collectRelayHeads(profile);
        const results = latest?.results ?? [];
        const headSeqs = results
            .map((result: any) => Number(result.head?.headSeq))
            .filter((seq: number) => Number.isFinite(seq));
        const maxSeq = headSeqs.length > 0 ? Math.max(...headSeqs) : -1;
        const minSeq = headSeqs.length > 0 ? Math.min(...headSeqs) : -1;
        if (
            latest &&
            latest.validCount >= profile.relays &&
            latest.invalidCount === 0 &&
            latest.errorCount === 0 &&
            latest.conflictCount === 0 &&
            minSeq >= expectedHeadSeq &&
            maxSeq === minSeq
        ) {
            return latest;
        }
        await sleep(1000);
    }
    return latest;
}

async function waitForWebSocket(url: string, timeoutMs = 60000) {
    const started = Date.now();
    while (true) {
        try {
            const socket = new WebSocket(url, { perMessageDeflate: false });
            await new Promise<void>((resolve, reject) => {
                const timer = setTimeout(() => reject(new Error("connect timeout")), 1000);
                socket.once("open", () => {
                    clearTimeout(timer);
                    socket.close();
                    resolve();
                });
                socket.once("error", reject);
            });
            return;
        } catch {
            if (Date.now() - started > timeoutMs) {
                throw new Error(`Timed out waiting for websocket ${url}`);
            }
            await sleep(500);
        }
    }
}

async function publishRawBatch(url: string, batchId: string, events: Array<{ body: EventBody; author: string; signature: string; createdAt: number }>, wireFormat = loadnetWireFormat()) {
    const socket = new WebSocket(url, { perMessageDeflate: false });
    await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`Timed out opening ${url}`)), 5000);
        socket.once("open", () => {
            clearTimeout(timer);
            configureNodeSocket(socket);
            resolve();
        });
        socket.once("error", reject);
    });

    await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`Timed out waiting for ${batchId} ack from ${url}`)), 15000);
        socket.on("message", (raw) => {
            const { kind, payload } = parseCgpWireData(raw, { includeRawFrame: false }) as { kind: string; payload: any };
            if (kind === "PUB_BATCH_ACK" && payload?.batchId === batchId) {
                clearTimeout(timer);
                socket.close();
                resolve();
            } else if (kind === "ERROR" && payload?.batchId === batchId) {
                clearTimeout(timer);
                socket.close();
                reject(new Error(payload?.message || "Bootstrap publish failed"));
            }
        });
        sendFrame(socket, "PUBLISH_BATCH", { batchId, events }, wireFormat);
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

async function applyNetem() {
    fs.mkdirSync(NETEM_DIR, { recursive: true });
    const latencyMs = envNumber("LOADNET_LATENCY_MS", 0);
    const jitterMs = envNumber("LOADNET_JITTER_MS", 0);
    const lossPercent = envNumber("LOADNET_LOSS_PERCENT", 0);
    const role = process.argv[2] || process.env.LOADNET_ROLE || "unknown";
    const workerId = process.env.LOADNET_WORKER_ID ?? process.env.LOADNET_RELAY_INDEX ?? "0";
    const statusPath = path.join(NETEM_DIR, `${role}-${workerId}.json`);
    if (latencyMs <= 0 && jitterMs <= 0 && lossPercent <= 0) {
        writeJson(statusPath, { role, workerId, required: false, applied: false });
        return;
    }

    const args = ["qdisc", "replace", "dev", "eth0", "root", "netem"];
    if (latencyMs > 0 || jitterMs > 0) {
        args.push("delay", `${latencyMs}ms`);
        if (jitterMs > 0) args.push(`${jitterMs}ms`);
    }
    if (lossPercent > 0) {
        args.push("loss", `${lossPercent}%`);
    }

    const retries = Math.max(1, envNumber("LOADNET_NETEM_RETRIES", 20));
    const retryDelayMs = Math.max(50, envNumber("LOADNET_NETEM_RETRY_DELAY_MS", 250));
    let warning: string | undefined;

    for (let attempt = 1; attempt <= retries; attempt += 1) {
        try {
            execFileSync("tc", args, { stdio: "ignore" });
            console.log(JSON.stringify({ role: "netem", args, attempt }));
            writeJson(statusPath, { role, workerId, required: true, applied: true, args, attempt, retries });
            return;
        } catch (error: any) {
            warning = error?.message || String(error);
            if (attempt < retries) {
                await sleep(retryDelayMs);
                continue;
            }
        }
    }

    writeJson(statusPath, { role, workerId, required: true, applied: false, warning, retries, retryDelayMs });
    if (process.env.LOADNET_ALLOW_NETEM_FAILURE === "1") {
        console.warn(JSON.stringify({ role: "netem", warning, retries, retryDelayMs }));
        return;
    }
    throw new Error(`Failed to apply required netem profile: ${warning}`);
}

async function runPubSub() {
    await applyNetem();
    const port = envNumber("LOADNET_PUBSUB_PORT", 7600);
    const retainEnvelopesPerTopic = envNumber("CGP_PUBSUB_RETAIN_ENVELOPES", 100000);
    const pubSubShard = process.env.LOADNET_PUBSUB_SHARD || "0";
    const baseRetainDir = process.env.CGP_PUBSUB_RETAIN_DIR || path.join(DATA_DIR, "pubsub-retain");
    const retainDir = path.join(baseRetainDir, `shard-${pubSubShard}`);
    const wireFormat = loadnetWireFormat();
    const fanoutSocketsPerTick = envNumber("CGP_PUBSUB_FANOUT_SOCKETS_PER_TICK", 4096);
    const hub = new WebSocketPubSubHub(port, { retainEnvelopesPerTopic, retainDir, wireFormat, fanoutSocketsPerTick });
    console.log(JSON.stringify({ role: "pubsub", port, shard: pubSubShard, retainEnvelopesPerTopic, retainDir, wireFormat, fanoutSocketsPerTick }));
    process.on("SIGTERM", () => void hub.close().then(() => process.exit(0)));
    await new Promise(() => undefined);
}

async function runRelay() {
    await applyNetem();
    const port = envNumber("CGP_RELAY_PORT", 7447);
    const dbPath = process.env.CGP_RELAY_DB || path.join(DATA_DIR, `relay-${process.env.LOADNET_RELAY_INDEX || "0"}`);
    const pubsubUrls = (process.env.CGP_RELAY_PUBSUB_URLS || process.env.CGP_RELAY_PUBSUB_URL || "")
        .split(",")
        .map((entry) => entry.trim())
        .filter(Boolean);
    const peerUrls = (process.env.CGP_RELAY_PEERS || "").split(",").map((entry) => entry.trim()).filter(Boolean);
    const store = process.env.LOADNET_RELAY_STORE === "memory" ? new MemoryStore() : dbPath;
    const wireFormat = loadnetWireFormat();
    const pubSubAdapter = pubsubUrls.length > 1
        ? new ShardedWebSocketRelayPubSubAdapter(pubsubUrls, { wireFormat })
        : pubsubUrls.length === 1
            ? new WebSocketRelayPubSubAdapter(pubsubUrls[0], { wireFormat })
            : undefined;
    const relay = new RelayServer(port, store, [], {
        pubSubAdapter,
        peerUrls,
        enableDefaultPlugins: process.env.CGP_RELAY_DEFAULT_PLUGINS !== "0",
        instanceId: `loadnet-relay-${process.env.LOADNET_RELAY_INDEX || "0"}`,
        wireFormat
    });
    console.log(JSON.stringify({ role: "relay", port, dbPath, pubsubUrls, peerUrls: peerUrls.length, wireFormat }));
    process.on("SIGTERM", () => void relay.close().then(() => process.exit(0)));
    await new Promise(() => undefined);
}

async function runSeed() {
    await applyNetem();
    ensureDirs();
    const profile = profileFromEnv();
    const wireFormat = loadnetWireFormat(profile);
    const relays = (process.env.LOADNET_RELAYS || "ws://relay-0:7447").split(",").map((entry) => entry.trim()).filter(Boolean);
    for (const relay of relays) {
        await waitForWebSocket(relay);
    }

    const priv = generatePrivateKey();
    const pub = getPublicKey(priv);
    const createdAt = Date.now();
    const guildId = hashObject({ loadnet: "guild", pub, createdAt });
    const channelIds = Array.from({ length: profile.channels }, (_, index) => hashObject({ loadnet: "channel", guildId, index }));
    const bootstrapBodies: EventBody[] = [
        {
            type: "GUILD_CREATE",
            guildId,
            name: "CGP Loadnet Guild",
            description: "loadnet synthetic guild",
            access: "public",
            policies: { posting: "public" }
        } as EventBody,
        ...channelIds.map((channelId, index) => ({
            type: "CHANNEL_CREATE",
            guildId,
            channelId,
            name: `load-${index}`,
            kind: "text"
        } as EventBody))
    ];
    const bootstrapEvents = await Promise.all(bootstrapBodies.map((body, index) => signedPayload(body, pub, priv, createdAt + index)));
    for (const relay of relays) {
        await publishRawBatch(relay, `loadnet-bootstrap-${createdAt}`, bootstrapEvents, wireFormat);
    }

    const scenario: ScenarioFile = {
        guildId,
        channelIds,
        ownerPub: pub,
        ownerPrivHex: hex(priv),
        profile,
        createdAt: new Date().toISOString()
    };
    writeJson(SCENARIO_PATH, scenario);
    console.log(JSON.stringify({ role: "seed", guildId, channels: channelIds.length, wireFormat }));
}

function expectedMessagesForChannel(totalMessages: number, channelCount: number, channelIndex: number) {
    if (totalMessages <= channelIndex) return 0;
    return Math.floor((totalMessages - 1 - channelIndex) / channelCount) + 1;
}

async function runSubscriber() {
    await applyNetem();
    ensureDirs();
    const profile = profileFromEnv();
    const wireFormat = loadnetWireFormat(profile);
    const workerId = envNumber("LOADNET_WORKER_ID", 0);
    const subscribers = envNumber("LOADNET_SUBSCRIBERS", profile.subscribersPerWorker);
    const relayUrl = process.env.LOADNET_RELAY_URL || "ws://relay-0:7447";
    const relayUrls = (process.env.LOADNET_RELAYS || relayUrl).split(",").map((entry) => entry.trim()).filter(Boolean);
    const churnEveryMs = envNumber("LOADNET_CHURN_EVERY_MS", profile.churnEveryMs);
    const backfillOnReconnect = process.env.LOADNET_BACKFILL_ON_RECONNECT !== "0" && churnEveryMs > 0;
    const finalBackfill = process.env.LOADNET_FINAL_BACKFILL !== "0";
    const slowConsumerPercent = envNumber("LOADNET_SLOW_CONSUMER_PERCENT", profile.slowConsumerPercent);
    const slowConsumerDelayMs = envNumber("LOADNET_SLOW_CONSUMER_DELAY_MS", profile.slowConsumerDelayMs);
    await waitForFile(SCENARIO_PATH);
    const scenario = JSON.parse(fs.readFileSync(SCENARIO_PATH, "utf8")) as ScenarioFile;

    let received = 0;
    let reconnects = 0;
    let finalBackfillRequests = 0;
    let stopping = false;
    const sockets: WebSocket[] = [];
    const manualReconnects = new Set<number>();
    const seenMessagesBySubscriber = Array.from({ length: subscribers }, () => new Set<string>());
    const lastSeqBySubscriber = Array.from({ length: subscribers }, () => -1);
    const backfillAfterSeqBySubscriber: Array<number | undefined> = Array.from({ length: subscribers }, () => undefined);
    const relayAttemptBySubscriber = Array.from({ length: subscribers }, () => 0);
    const connectionGenerationBySubscriber = Array.from({ length: subscribers }, () => 0);
    const historyWaiters = new Map<string, () => void>();
    const startedAt = Date.now();
    const expectedTotalMessages = profile.publisherWorkers * profile.messagesPerPublisher;
    let deliveredMessages = 0;
    let backfillDeliveredMessages = 0;
    let duplicateDeliveries = 0;
    let backfillDuplicateDeliveries = 0;

    const expectedForSubscriberIndex = (index: number) => {
        const globalIndex = workerId * subscribers + index;
        const channelIndex = globalIndex % scenario.channelIds.length;
        return expectedMessagesForChannel(expectedTotalMessages, scenario.channelIds.length, channelIndex);
    };

    const recoveryPageLimitForSubscriber = (index: number, fallbackLimit: number) => {
        const expected = expectedForSubscriberIndex(index);
        const missing = Math.max(0, expected - seenMessagesBySubscriber[index].size);
        return Math.max(32, Math.min(500, Math.min(fallbackLimit, missing + 64)));
    };

    const finalPageLimitForSubscriber = (index: number, fallbackLimit: number) => {
        const expected = expectedForSubscriberIndex(index);
        const missing = Math.max(0, expected - seenMessagesBySubscriber[index].size);
        return Math.max(64, Math.min(500, Math.min(fallbackLimit, missing * 4 + 64)));
    };

    const processEvents = (subscriberIndex: number, events: any[], source: "live" | "backfill") => {
        const seenMessages = seenMessagesBySubscriber[subscriberIndex];
        for (const event of events) {
            if (Number.isFinite(event?.seq)) {
                lastSeqBySubscriber[subscriberIndex] = Math.max(lastSeqBySubscriber[subscriberIndex], Number(event.seq));
            }
            if (event?.body?.type !== "MESSAGE") {
                continue;
            }
            const messageId = typeof event.body.messageId === "string" && event.body.messageId.trim()
                ? event.body.messageId
                : event.id;
            if (source === "backfill") {
                backfillDeliveredMessages += 1;
            } else {
                deliveredMessages += 1;
            }
            if (!messageId || seenMessages.has(messageId)) {
                if (source === "backfill") {
                    backfillDuplicateDeliveries += 1;
                } else {
                    duplicateDeliveries += 1;
                }
                continue;
            }
            seenMessages.add(messageId);
            received += 1;
        }
    };

    const isSlowConsumer = (subscriberIndex: number) => {
        if (slowConsumerPercent <= 0 || slowConsumerDelayMs <= 0) {
            return false;
        }
        const globalIndex = workerId * subscribers + subscriberIndex;
        const slowEvery = Math.max(1, Math.floor(100 / slowConsumerPercent));
        return globalIndex % slowEvery === 0;
    };

    const processLiveEvents = (subscriberIndex: number, events: any[], generation: number) => {
        if (events.length === 0) {
            return;
        }
        if (isSlowConsumer(subscriberIndex)) {
            const pending = new Promise<void>((resolve) => {
                setTimeout(() => {
                    if (connectionGenerationBySubscriber[subscriberIndex] === generation) {
                        processEvents(subscriberIndex, events, "live");
                    }
                    resolve();
                }, slowConsumerDelayMs);
            });
            pendingSlowConsumerTimers.add(pending);
            void pending.finally(() => pendingSlowConsumerTimers.delete(pending));
            return;
        }
        processEvents(subscriberIndex, events, "live");
    };
    const pendingSlowConsumerTimers = new Set<Promise<void>>();

    let retryConnectOne: (index: number) => Promise<void>;

    const connectOne = async (index: number) => {
        const generation = connectionGenerationBySubscriber[index] + 1;
        connectionGenerationBySubscriber[index] = generation;
        const globalIndex = workerId * subscribers + index;
        const channelIndex = globalIndex % scenario.channelIds.length;
        const channelId = scenario.channelIds[channelIndex];
        const attempt = relayAttemptBySubscriber[index];
        relayAttemptBySubscriber[index] = attempt + 1;
        const targetRelayUrl = relayUrls[(globalIndex + attempt) % relayUrls.length] || relayUrl;
        const socket = new WebSocket(targetRelayUrl, { perMessageDeflate: false });
        socket.on("error", () => undefined);
        sockets[index] = socket;
        await new Promise<void>((resolve, reject) => {
            let settled = false;
            const timer = setTimeout(() => {
                if (settled) return;
                settled = true;
                socket.close();
                reject(new Error(`Timed out opening subscriber socket: ${targetRelayUrl}`));
            }, 5000);
            const done = (fn: () => void) => {
                if (settled) return;
                settled = true;
                clearTimeout(timer);
                fn();
            };
            socket.once("open", () => done(() => {
                configureNodeSocket(socket);
                resolve();
            }));
            socket.once("error", (error) => done(() => reject(error)));
            socket.once("close", () => done(() => reject(new Error(`socket closed before open: ${targetRelayUrl}`))));
        });
        let snapshotSeen = false;
        socket.on("message", (raw) => {
            if (sockets[index] !== socket || connectionGenerationBySubscriber[index] !== generation) {
                return;
            }
            const { kind, payload } = parseCgpWireData(raw, { includeRawFrame: false }) as { kind: string; payload: any };
            if (kind === "SNAPSHOT") {
                if (typeof payload?.subId === "string" && payload.subId.startsWith("history-")) {
                    processEvents(index, Array.isArray(payload.events) ? payload.events : [], "backfill");
                    backfillAfterSeqBySubscriber[index] = undefined;
                    const waiter = historyWaiters.get(payload.subId);
                    if (waiter) {
                        historyWaiters.delete(payload.subId);
                        waiter();
                    }
                }
                snapshotSeen = true;
                return;
            }
            const events = kind === "EVENT" ? [payload] : kind === "BATCH" && Array.isArray(payload) ? payload : [];
            processLiveEvents(index, events, generation);
        });
        socket.on("close", () => {
            if (stopping || sockets[index] !== socket || connectionGenerationBySubscriber[index] !== generation) {
                return;
            }
            if (manualReconnects.delete(index)) {
                return;
            }
            if (Date.now() - startedAt > profile.durationMs + 5000) {
                return;
            }
            if (backfillAfterSeqBySubscriber[index] === undefined) {
                backfillAfterSeqBySubscriber[index] = Math.max(-1, lastSeqBySubscriber[index]);
            }
            reconnects += 1;
            void retryConnectOne(index).catch(() => undefined);
        });
        sendFrame(socket, "SUB", {
            subId: `worker-${workerId}-${index}`,
            guildId: scenario.guildId,
            channels: [channelId]
        }, wireFormat);
        const waitStarted = Date.now();
        while (!snapshotSeen && Date.now() - waitStarted < 10000) {
            await sleep(25);
        }
        if (connectionGenerationBySubscriber[index] !== generation) {
            socket.close();
            throw new Error(`subscriber ${index} connection generation superseded`);
        }
        const sendHistoryRequest = (afterSeq: number | undefined, limit: number, waitForResponse = false) => {
            const subId = `history-${workerId}-${index}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
            const payload: Record<string, unknown> = {
                subId,
                guildId: scenario.guildId,
                channelId,
                limit
            };
            if (afterSeq !== undefined) {
                payload.afterSeq = afterSeq;
            }
            sendFrame(socket, "GET_HISTORY", payload, wireFormat);
            if (!waitForResponse) {
                return Promise.resolve();
            }
            return new Promise<void>((resolve) => {
                const timer = setTimeout(() => {
                    historyWaiters.delete(subId);
                    resolve();
                }, 10_000);
                historyWaiters.set(subId, () => {
                    clearTimeout(timer);
                    resolve();
                });
            });
        };

        const backfillAfterSeq = backfillAfterSeqBySubscriber[index];
        if (backfillOnReconnect && backfillAfterSeq !== undefined) {
            const reconnectLimit = envNumber("LOADNET_RECONNECT_BACKFILL_PAGE_LIMIT", recoveryPageLimitForSubscriber(index, 500));
            void sendHistoryRequest(backfillAfterSeq, reconnectLimit);
        }
        return { socket, channelId, sendHistoryRequest };
    };

    retryConnectOne = async (index: number) => {
        const started = Date.now();
        let lastError: unknown;
        while (!stopping && Date.now() - started < 60000) {
            try {
                await connectOne(index);
                return;
            } catch (error) {
                lastError = error;
                await sleep(250);
            }
        }
        throw lastError instanceof Error ? lastError : new Error(`subscriber ${index} failed to reconnect`);
    };

    let nextConnectIndex = 0;
    const connectWorkers = Array.from({ length: Math.min(32, Math.max(1, subscribers)) }, async () => {
        while (nextConnectIndex < subscribers) {
            const index = nextConnectIndex;
            nextConnectIndex += 1;
            await retryConnectOne(index);
        }
    });
    await Promise.all(connectWorkers);

    writeJson(path.join(READY_DIR, `subscriber-${workerId}.ready`), { runId: RUN_ID, workerId, subscribers, relayUrls });

    let churnTimer: NodeJS.Timeout | undefined;
    if (churnEveryMs > 0 && subscribers > 0) {
        churnTimer = setInterval(() => {
            const index = Math.floor(Math.random() * subscribers);
            const oldSocket = sockets[index];
            if (backfillAfterSeqBySubscriber[index] === undefined) {
                backfillAfterSeqBySubscriber[index] = Math.max(-1, lastSeqBySubscriber[index]);
            }
            manualReconnects.add(index);
            oldSocket?.close();
            reconnects += 1;
            void retryConnectOne(index).catch(() => undefined);
        }, churnEveryMs);
    }

    const expectedBySubscriber = Array.from({ length: subscribers }, (_, index) => {
        return expectedForSubscriberIndex(index);
    });
    const expectedByWorker = expectedBySubscriber.reduce((sum, count) => sum + count, 0);

    while (Date.now() - startedAt < profile.durationMs) {
        if (churnEveryMs === 0 && received >= expectedByWorker) {
            break;
        }
        await sleep(100);
    }

    if (churnTimer) clearInterval(churnTimer);
    if (finalBackfill) {
        const expectedFinalHeadSeq = expectedTotalMessages + scenario.channelIds.length;
        const headWaitMs = envNumber("LOADNET_FINAL_BACKFILL_HEAD_WAIT_MS", Math.max(120_000, profile.durationMs));
        const finalBackfillTarget = await waitForRelayAtHead(relayUrls, scenario.guildId, expectedFinalHeadSeq, headWaitMs);
        const finalBackfillRelays = [
            finalBackfillTarget.relayUrl,
            ...relayUrls.filter((url) => url !== finalBackfillTarget.relayUrl)
        ];
        const finalHistoryPageLimit = envNumber("LOADNET_FINAL_BACKFILL_PAGE_LIMIT", 500);
        const requestFinalHistory = async (
            relayUrl: string,
            index: number,
            channelId: string,
            attempt: number,
            beforeSeq?: number,
            limit = finalHistoryPageLimit
        ): Promise<{ events: number; oldestSeq: number | null; newestSeq: number | null; hasMore: boolean }> => {
            const subId = `history-final-${workerId}-${index}-${Date.now()}-${attempt}`;
            const socket = new WebSocket(relayUrl, { perMessageDeflate: false });
            await new Promise<void>((resolve, reject) => {
                const timer = setTimeout(() => reject(new Error(`Timed out opening final backfill socket ${relayUrl}`)), 5000);
                socket.once("open", () => {
                    clearTimeout(timer);
                    configureNodeSocket(socket);
                    resolve();
                });
                socket.once("error", reject);
            });

            return await new Promise((resolve, reject) => {
                const timer = setTimeout(() => {
                    socket.close();
                    resolve({ events: 0, oldestSeq: null, newestSeq: null, hasMore: false });
                }, 15_000);
                socket.on("message", (raw) => {
                    try {
                        const { kind, payload } = parseCgpWireData(raw, { includeRawFrame: false }) as { kind: string; payload: any };
                        if (kind === "SNAPSHOT" && payload?.subId === subId) {
                            clearTimeout(timer);
                            const events = Array.isArray(payload.events) ? payload.events : [];
                            processEvents(index, events, "backfill");
                            socket.close();
                            resolve({
                                events: events.length,
                                oldestSeq: Number.isFinite(Number(payload.oldestSeq)) ? Number(payload.oldestSeq) : null,
                                newestSeq: Number.isFinite(Number(payload.newestSeq)) ? Number(payload.newestSeq) : null,
                                hasMore: payload.hasMore === true
                            });
                        } else if (kind === "ERROR" && payload?.subId === subId) {
                            clearTimeout(timer);
                            socket.close();
                            reject(new Error(payload?.message || payload?.code || "final backfill failed"));
                        }
                    } catch (error) {
                        clearTimeout(timer);
                        socket.close();
                        reject(error);
                    }
                });
                socket.once("error", (error) => {
                    clearTimeout(timer);
                    socket.close();
                    reject(error);
                });
                const payload: Record<string, unknown> = {
                    subId,
                    guildId: scenario.guildId,
                    channelId,
                    limit
                };
                if (beforeSeq !== undefined) {
                    payload.beforeSeq = beforeSeq;
                }
                sendFrame(socket, "GET_HISTORY", payload, wireFormat);
            });
        };

        const backfillOneSubscriber = async (index: number) => {
            const globalIndex = workerId * subscribers + index;
            const channelIndex = globalIndex % scenario.channelIds.length;
            const channelId = scenario.channelIds[channelIndex];
            const expectedForSubscriber = expectedMessagesForChannel(expectedTotalMessages, scenario.channelIds.length, channelIndex);
            let beforeSeq: number | undefined;
            let lastOldestSeq: number | undefined;
            for (let attempt = 0; attempt < 16 && seenMessagesBySubscriber[index].size < expectedForSubscriber; attempt += 1) {
                const targetRelayUrl = finalBackfillRelays[attempt % finalBackfillRelays.length] ?? finalBackfillTarget.relayUrl;
                finalBackfillRequests += 1;
                const previousSize = seenMessagesBySubscriber[index].size;
                const limit = finalPageLimitForSubscriber(index, finalHistoryPageLimit);
                const page = await requestFinalHistory(targetRelayUrl, index, channelId, attempt, beforeSeq, limit).catch(() => undefined);
                if (!page || page.events <= 0 || page.oldestSeq === null || page.oldestSeq === lastOldestSeq) {
                    await sleep(250);
                    continue;
                }
                lastOldestSeq = page.oldestSeq;
                beforeSeq = page.oldestSeq;
                const deliveredNew = seenMessagesBySubscriber[index].size > previousSize;
                if (seenMessagesBySubscriber[index].size < expectedForSubscriber && page.events < limit && deliveredNew) {
                    break;
                }
                if (seenMessagesBySubscriber[index].size < expectedForSubscriber) {
                    await sleep(250);
                }
            }
        };

        let nextBackfillIndex = 0;
        const workers = Array.from({ length: Math.min(16, Math.max(1, subscribers)) }, async () => {
            while (nextBackfillIndex < subscribers) {
                const index = nextBackfillIndex;
                nextBackfillIndex += 1;
                await backfillOneSubscriber(index);
            }
        });
        await Promise.all(workers);
    }
    stopping = true;
    for (const socket of sockets) socket?.close();
    const slowConsumerDrainStarted = Date.now();
    if (pendingSlowConsumerTimers.size > 0) {
        await Promise.allSettled(Array.from(pendingSlowConsumerTimers));
    }
    const slowConsumerDrainMs = Date.now() - slowConsumerDrainStarted;
    const perSubscriberReceived = seenMessagesBySubscriber.map((seen) => seen.size);
    writeJson(path.join(METRICS_DIR, `subscriber-${workerId}.json`), {
        runId: RUN_ID,
        role: "subscriber",
        workerId,
        subscribers,
        expected: expectedByWorker,
        received,
        deliveredMessages,
        duplicateDeliveries,
        backfillDeliveredMessages,
        backfillDuplicateDeliveries,
        expectedBySubscriber,
        perSubscriberReceived,
        minSubscriberReceived: perSubscriberReceived.length > 0 ? Math.min(...perSubscriberReceived) : 0,
        maxSubscriberReceived: perSubscriberReceived.length > 0 ? Math.max(...perSubscriberReceived) : 0,
        underReceivedSubscribers: perSubscriberReceived.filter((count, index) => count < (expectedBySubscriber[index] ?? 0)).length,
        reconnects,
        finalBackfillRequests,
        slowConsumers: Array.from({ length: subscribers }, (_, index) => isSlowConsumer(index)).filter(Boolean).length,
        slowConsumerDelayMs,
        slowConsumerDrainMs,
        ms: Date.now() - startedAt
    });
    console.log(JSON.stringify({ role: "subscriber", workerId, subscribers, expected: expectedByWorker, received, reconnects, finalBackfillRequests, slowConsumerDelayMs }));
}

async function runPublisher() {
    await applyNetem();
    ensureDirs();
    const profile = profileFromEnv();
    const wireFormat = loadnetWireFormat(profile);
    const workerId = envNumber("LOADNET_WORKER_ID", 0);
    const relayUrl = process.env.LOADNET_RELAY_URL || "ws://relay-0:7447";
    const relayUrls = (process.env.LOADNET_RELAYS || relayUrl).split(",").map((entry) => entry.trim()).filter(Boolean);
    const writeRelayUrls = (process.env.LOADNET_WRITE_RELAYS || relayUrls[0] || relayUrl).split(",").map((entry) => entry.trim()).filter(Boolean);
    const messages = envNumber("LOADNET_MESSAGES", profile.messagesPerPublisher);
    const batchSize = envNumber("LOADNET_BATCH_SIZE", profile.batchSize);
    const expectedReady = envNumber("LOADNET_EXPECTED_READY", profile.subscriberWorkers);
    const readyTimeoutMs = envNumber("LOADNET_READY_TIMEOUT_MS", Math.max(60_000, profile.durationMs + 60_000));
    await waitForFile(SCENARIO_PATH);
    await waitForReadyFiles(expectedReady, readyTimeoutMs);
    const scenario = JSON.parse(fs.readFileSync(SCENARIO_PATH, "utf8")) as ScenarioFile;

    const priv = bytes(scenario.ownerPrivHex);
    const pub = scenario.ownerPub;
    const publishTimeoutMs = envNumber("LOADNET_PUBLISH_TIMEOUT_MS", 10_000);
    const batchRetries = Math.max(1, envNumber("LOADNET_PUBLISH_BATCH_RETRIES", 8));
    const retryDelayMs = Math.max(100, envNumber("LOADNET_PUBLISH_RETRY_DELAY_MS", 750));
    const reconnectTimeoutMs = envNumber("LOADNET_PUBLISH_RECONNECT_TIMEOUT_MS", 10_000);

    const connectClient = async () => {
        const next = new CgpClient({ relays: relayUrls, writeRelays: writeRelayUrls, keyPair: { pub, priv }, wireFormat });
        await Promise.race([
            next.connect(),
            new Promise<never>((_, reject) => {
                setTimeout(() => reject(new Error(`Timed out connecting publisher client after ${reconnectTimeoutMs}ms`)), reconnectTimeoutMs);
            })
        ]);
        return next;
    };

    let client = await connectClient();

    const startedAt = Date.now();
    const batchLatencies: number[] = [];
    let publishRetries = 0;
    const publishRetryReasons = new Map<string, number>();
    for (let offset = 0; offset < messages; offset += batchSize) {
        const bodies: EventBody[] = [];
        const count = Math.min(batchSize, messages - offset);
        for (let index = 0; index < count; index += 1) {
            const messageIndex = workerId * messages + offset + index;
            bodies.push({
                type: "MESSAGE",
                guildId: scenario.guildId,
                channelId: scenario.channelIds[messageIndex % scenario.channelIds.length],
                messageId: `loadnet-${workerId}-${messageIndex}`,
                content: `loadnet message ${workerId}/${messageIndex}`
            } as EventBody);
        }
        const batchStarted = Date.now();
        const batchId = `loadnet-${workerId}-${offset}`;
        let published = false;
        for (let attempt = 1; attempt <= batchRetries; attempt += 1) {
            try {
                await client.publishBatchReliable(bodies, { timeoutMs: publishTimeoutMs, batchId });
                published = true;
                break;
            } catch (error: any) {
                if (attempt >= batchRetries) {
                    throw new Error(`Failed to publish batch ${batchId} after ${batchRetries} attempts: ${error?.message || String(error)}`);
                }
                const reason = error?.message || String(error);
                publishRetries += 1;
                publishRetryReasons.set(reason, (publishRetryReasons.get(reason) || 0) + 1);
                console.warn(JSON.stringify({ role: "publisher-retry", workerId, batchId, attempt, error: reason }));
                try {
                    client.close();
                } catch {
                    // ignored
                }
                await sleep(Math.min(10_000, retryDelayMs * attempt));
                client = await connectClient();
            }
        }
        if (!published) {
            throw new Error(`Failed to publish batch ${batchId}`);
        }
        batchLatencies.push(Date.now() - batchStarted);
    }

    client.close();
    const ms = Date.now() - startedAt;
    batchLatencies.sort((a, b) => a - b);
    const percentile = (p: number) => batchLatencies[Math.min(batchLatencies.length - 1, Math.floor(batchLatencies.length * p))] ?? 0;
    writeJson(path.join(METRICS_DIR, `publisher-${workerId}.json`), {
        runId: RUN_ID,
        role: "publisher",
        workerId,
        messages,
        batches: batchLatencies.length,
        publishRetries,
        publishRetryReasons: Object.fromEntries(publishRetryReasons),
        ms,
        messagesPerSec: ms > 0 ? messages / (ms / 1000) : messages,
        batchLatencyMs: {
            p50: percentile(0.5),
            p95: percentile(0.95),
            p99: percentile(0.99)
        }
    });
    console.log(JSON.stringify({ role: "publisher", workerId, messages, ms }));
}

async function runCollector() {
    ensureDirs();
    const profile = profileFromEnv();
    const expected = envNumber("LOADNET_EXPECTED_METRICS", 1);
    const timeoutMs = envNumber("LOADNET_COLLECTOR_TIMEOUT_MS", Math.max(120000, profile.durationMs + 180000));
    const startedAt = Date.now();
    while (true) {
        const files = fs.readdirSync(METRICS_DIR).filter((name) => name.endsWith(".json"));
        const metrics = files
            .map((file) => JSON.parse(fs.readFileSync(path.join(METRICS_DIR, file), "utf8")))
            .filter((metric) => (metric.runId || "default") === RUN_ID);
        if (metrics.length >= expected || Date.now() - startedAt > timeoutMs) {
            const netem = fs.existsSync(NETEM_DIR)
                ? fs.readdirSync(NETEM_DIR)
                    .filter((name) => name.endsWith(".json"))
                    .map((file) => JSON.parse(fs.readFileSync(path.join(NETEM_DIR, file), "utf8")))
                : [];
            const expectedHeadSeq = profile.publisherWorkers * profile.messagesPerPublisher + profile.channels;
            const headWaitMs = envNumber("LOADNET_COLLECTOR_HEAD_WAIT_MS", Math.max(120_000, profile.durationMs));
            const relayHeads = await waitForClusterRelayHeads(profile, expectedHeadSeq, headWaitMs);
            const summary = {
                runId: RUN_ID,
                profile,
                metrics,
                metricsCount: metrics.length,
                expectedMetrics: expected,
                collectorComplete: metrics.length >= expected,
                netem,
                totals: {
                    published: metrics.filter((m) => m.role === "publisher").reduce((sum, m) => sum + (m.messages || 0), 0),
                    received: metrics.filter((m) => m.role === "subscriber").reduce((sum, m) => sum + (m.received || 0), 0),
                    expectedReceived: metrics.filter((m) => m.role === "subscriber").reduce((sum, m) => sum + (m.expected || 0), 0)
                },
                relayHeads,
                collectedAt: new Date().toISOString()
            };
            writeJson(path.join(DATA_DIR, "summary.json"), summary);
            fs.mkdirSync("/results", { recursive: true });
            writeJson(path.join("/results", `summary-${Date.now()}.json`), summary);
            console.log(JSON.stringify(summary));
            return;
        }
        await sleep(500);
    }
}

async function main() {
    const role = process.argv[2] || process.env.LOADNET_ROLE || "help";
    if (role === "pubsub") return runPubSub();
    if (role === "relay") return runRelay();
    if (role === "seed") return runSeed();
    if (role === "subscriber") return runSubscriber();
    if (role === "publisher") return runPublisher();
    if (role === "collector") return runCollector();
    if (role === "help" || role === "--help" || role === "-h") {
        console.log("usage: tsx loadnet/node.ts <pubsub|relay|seed|subscriber|publisher|collector>");
        return;
    }
    throw new Error(`Unknown loadnet role: ${role}`);
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
