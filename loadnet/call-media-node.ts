import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { WebSocket } from "ws";
import {
    CgpClient,
    CgpWebTransportDatagramClient
} from "@cgp/client";
import {
    encodeCgpFrame,
    generatePrivateKey,
    getPublicKey,
    hashObject,
    parseCgpWireData,
    sign,
    type CgpWireFormat,
    type EventBody,
    type GuildEvent
} from "@cgp/core";

const DATA_DIR = process.env.LOADNET_DATA_DIR || "/data";
const METRICS_DIR = path.join(DATA_DIR, "metrics");
const NETEM_DIR = path.join(DATA_DIR, "netem");
const RUN_ID = process.env.LOADNET_RUN_ID || "call-media";
const SESSION_PATH = path.join(DATA_DIR, "call-media-session.json");
const PUBLISH_START_PATH = path.join(DATA_DIR, "coordination", "call-media-publish-start.json");
const PUBLISH_COMPLETE_PATH = path.join(DATA_DIR, "coordination", "call-media-publish-complete.json");

type MediaKind = "fallback-audio" | "fallback-video";
type DeliveryMode = "exact" | "realtime";
type MediaProfile = "audio" | "camera" | "screen" | "mixed" | "stress";
type RealtimeTransportMode = "websocket" | "webtransport";

interface ExpectedMedia {
    messageId: string;
    kind: MediaKind;
    streamKind?: "camera" | "screen";
    mediaHash: string;
    byteLength: number;
    roomId: string;
}

interface CallMediaSession {
    guildId: string;
    channelId: string;
    readinessMessageId: string;
    messages: number;
    rooms: number;
    deliveryMode: DeliveryMode;
    mediaDeadlineMs: number;
    minFreshDeliveryRatio: number;
    mediaProfile: MediaProfile;
}

interface ObserverResult {
    ok: boolean;
    error?: string;
    observerId: number;
    messages: number;
    observedEvents: number;
    verifiedMessages: number;
    corruptPayloads: number;
    duplicateMessages: number;
    channelReady: boolean;
    missingMessages: string[];
    latenciesMs: number[];
    observedLatencyMs: {
        p50: number;
        p95: number;
        p99: number;
    };
    freshDeliveryRatio: number;
}

function envNumber(name: string, fallback: number) {
    const value = Number(process.env[name]);
    return Number.isFinite(value) ? value : fallback;
}

function ensureDirs() {
    fs.mkdirSync(METRICS_DIR, { recursive: true });
    fs.mkdirSync(NETEM_DIR, { recursive: true });
}

function writeJson(filePath: string, value: unknown) {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    const temporaryPath = `${filePath}.${process.pid}.tmp`;
    fs.writeFileSync(temporaryPath, JSON.stringify(value, null, 2));
    fs.renameSync(temporaryPath, filePath);
}

async function sleep(ms: number) {
    await new Promise((resolve) => setTimeout(resolve, ms));
}

function roleName() {
    return process.argv[2] || process.env.LOADNET_ROLE || "worker";
}

function realtimeTransportMode(): RealtimeTransportMode {
    return process.env.LOADNET_CALL_MEDIA_TRANSPORT === "webtransport"
        ? "webtransport"
        : "websocket";
}

function metricsFileName() {
    return `${realtimeTransportMode()}-call-media.json`;
}

async function applyNetem() {
    const role = roleName();
    const workerId = process.env.LOADNET_WORKER_ID || "0";
    const latencyMs = envNumber("LOADNET_LATENCY_MS", 0);
    const jitterMs = envNumber("LOADNET_JITTER_MS", 0);
    const lossPercent = envNumber("LOADNET_LOSS_PERCENT", 0);
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
    let warning = "";
    for (let attempt = 1; attempt <= retries; attempt += 1) {
        try {
            execFileSync("tc", args, { stdio: "ignore" });
            writeJson(statusPath, { role, workerId, required: true, applied: true, args, attempt, retries });
            return;
        } catch (error: any) {
            warning = error?.message || String(error);
            await sleep(100);
        }
    }

    throw new Error(`Failed to apply required netem profile: ${warning}`);
}

function keyPair() {
    const priv = generatePrivateKey();
    return { priv, pub: getPublicKey(priv) };
}

function sha256(buffer: Buffer) {
    return createHash("sha256").update(buffer).digest("hex");
}

function deterministicBytes(seed: string, size: number) {
    const output = Buffer.alloc(size);
    let offset = 0;
    let counter = 0;
    while (offset < size) {
        const chunk = createHash("sha256").update(`${seed}:${counter}`).digest();
        const copied = Math.min(chunk.byteLength, size - offset);
        chunk.copy(output, offset, 0, copied);
        offset += copied;
        counter += 1;
    }
    return output;
}

function percentile(values: number[], p: number) {
    if (values.length === 0) return 0;
    const sorted = [...values].sort((left, right) => left - right);
    return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))] ?? 0;
}

function configureNodeSocket(ws: WebSocket) {
    const transport = (ws as unknown as { _socket?: { setNoDelay?: (noDelay?: boolean) => void } })._socket;
    transport?.setNoDelay?.(true);
}

async function openWebSocket(url: string, timeoutMs = 10000) {
    const started = Date.now();
    let lastError: unknown;
    while (Date.now() - started < timeoutMs) {
        try {
            const socket = new WebSocket(url, { perMessageDeflate: false });
            await new Promise<void>((resolve, reject) => {
                const timer = setTimeout(() => {
                    socket.close();
                    reject(new Error(`Timed out opening ${url}`));
                }, 2000);
                socket.once("open", () => {
                    clearTimeout(timer);
                    configureNodeSocket(socket);
                    resolve();
                });
                socket.once("error", (error) => {
                    clearTimeout(timer);
                    reject(error);
                });
            });
            socket.on("error", () => undefined);
            return socket;
        } catch (error) {
            lastError = error;
            await sleep(250);
        }
    }
    throw lastError instanceof Error ? lastError : new Error(`Timed out opening ${url}`);
}

async function signedPayload(body: EventBody, author: string, priv: Uint8Array, createdAt: number) {
    return {
        body,
        author,
        createdAt,
        signature: await sign(priv, hashObject({ body, author, createdAt }))
    };
}

async function publishBootstrapBatch(
    relayUrl: string,
    batchId: string,
    events: Array<{ body: EventBody; author: string; signature: string; createdAt: number }>,
    wireFormat: CgpWireFormat
) {
    const socket = await openWebSocket(relayUrl);
    try {
        await new Promise<void>((resolve, reject) => {
            const timer = setTimeout(() => {
                cleanup();
                reject(new Error(`Timed out waiting for bootstrap ack from ${relayUrl}`));
            }, 10000);
            const cleanup = () => {
                clearTimeout(timer);
                socket.off("message", onMessage);
                socket.off("error", onError);
                socket.off("close", onClose);
            };
            const onMessage = (raw: WebSocket.RawData) => {
                try {
                    const { kind, payload } = parseCgpWireData(raw, { includeRawFrame: false }) as { kind: string; payload: any };
                    if (kind === "PUB_BATCH_ACK" && payload?.batchId === batchId) {
                        cleanup();
                        resolve();
                    } else if (kind === "ERROR" && payload?.batchId === batchId) {
                        cleanup();
                        reject(new Error(payload?.message || "Bootstrap publish failed"));
                    }
                } catch (error) {
                    cleanup();
                    reject(error);
                }
            };
            const onError = (error: Error) => {
                cleanup();
                reject(error);
            };
            const onClose = () => {
                cleanup();
                reject(new Error(`Bootstrap socket closed before ack from ${relayUrl}`));
            };
            socket.on("message", onMessage);
            socket.once("error", onError);
            socket.once("close", onClose);
            socket.send(encodeCgpFrame("PUBLISH_BATCH", { batchId, events }, wireFormat));
        });
    } finally {
        socket.close();
    }
}

async function waitFor(label: string, predicate: () => boolean | Promise<boolean>, timeoutMs = 15000) {
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
        if (await predicate()) return;
        await sleep(100);
    }
    throw new Error(`Timed out waiting for ${label}`);
}

function readJson<T>(filePath: string): T {
    return JSON.parse(fs.readFileSync(filePath, "utf8")) as T;
}

function observerPath(kind: "connected" | "ready" | "result", observerId: number) {
    const directory = kind === "result" ? METRICS_DIR : path.join(DATA_DIR, "coordination");
    return path.join(directory, `call-media-observer-${observerId}-${kind}.json`);
}

async function subscribeClientToChannels(client: CgpClient, guildId: string, channels: string[], wireFormat: CgpWireFormat) {
    const sockets = ((client as unknown as { sockets?: WebSocket[] }).sockets || [])
        .filter((socket) => socket.readyState === WebSocket.OPEN);
    if (sockets.length === 0) {
        throw new Error("No open observer relay socket for channel subscription");
    }

    for (const socket of sockets) {
        const subId = `call-media-channel-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        socket.send(encodeCgpFrame("SUB", { subId, guildId, channels }, wireFormat));
    }
}

function setClientSocketMaxListeners(client: CgpClient, maxListeners: number) {
    const sockets = ((client as unknown as { sockets?: WebSocket[] }).sockets || [])
        .filter((socket) => socket.readyState === WebSocket.OPEN);
    for (const socket of sockets) {
        socket.setMaxListeners(maxListeners);
    }
}

function mediaPlan(index: number, guildId: string, channelId: string, rooms: number): { expected: ExpectedMedia; body: any } {
    const roomId = `loadnet-call-room-${index % Math.max(1, rooms)}`;
    const configuredProfile = process.env.LOADNET_CALL_MEDIA_PROFILE;
    const mediaProfile: MediaProfile =
        configuredProfile === "audio" ||
        configuredProfile === "camera" ||
        configuredProfile === "screen" ||
        configuredProfile === "stress"
            ? configuredProfile
            : "mixed";
    const profileSlot = mediaProfile === "stress"
        ? index % 10
        : mediaProfile === "camera"
            ? index % 32
            : mediaProfile === "screen"
                ? index % 16
                : index % 20;
    const isAudio = mediaProfile === "audio" ||
        (mediaProfile === "stress"
            ? profileSlot < 6
            : mediaProfile === "camera"
                ? profileSlot < 31
                : mediaProfile === "screen"
                    ? profileSlot < 15
                    : profileSlot < 18);
    const messageId = `call-media-${index}`;
    if (isAudio) {
        const pcm = deterministicBytes(`${RUN_ID}:audio:${index}`, envNumber("LOADNET_CALL_MEDIA_AUDIO_BYTES", 164));
        const mediaHash = sha256(pcm);
        return {
            expected: { messageId, kind: "fallback-audio", mediaHash, byteLength: pcm.byteLength, roomId },
            body: {
                type: "CALL_EVENT",
                guildId,
                channelId,
                roomId,
                payload: {
                    kind: "fallback-audio",
                    messageId,
                    roomId,
                    fromUserId: `loadnet-user-${index % 64}`,
                    transport: realtimeTransportMode() === "webtransport"
                        ? "relay-webtransport-datagram"
                        : "relay-websocket",
                    sequence: index,
                    capturedAt: 0,
                    sampleRate: 16000,
                    audioCodec: "ima-adpcm",
                    audioBase64: pcm.toString("base64"),
                    sampleCount: 320,
                    mediaHash,
                    byteLength: pcm.byteLength
                }
            }
        };
    }

    const streamKind = mediaProfile === "screen" || (mediaProfile === "mixed" && profileSlot === 19)
        ? "screen"
        : "camera";
    const size = streamKind === "camera"
        ? envNumber("LOADNET_CALL_MEDIA_CAMERA_BYTES", 4096)
        : envNumber("LOADNET_CALL_MEDIA_SCREEN_BYTES", 8192);
    const frame = deterministicBytes(`${RUN_ID}:${streamKind}:${index}`, size);
    const mediaHash = sha256(frame);
    return {
        expected: { messageId, kind: "fallback-video", streamKind, mediaHash, byteLength: frame.byteLength, roomId },
        body: {
            type: "CALL_EVENT",
            guildId,
            channelId,
            roomId,
            payload: {
                kind: "fallback-video",
                messageId,
                roomId,
                fromUserId: `loadnet-user-${index % 64}`,
                transport: realtimeTransportMode() === "webtransport"
                    ? "relay-webtransport-datagram"
                    : "relay-websocket",
                sequence: index,
                capturedAt: 0,
                streamKind,
                frame: `data:image/jpeg;base64,${frame.toString("base64")}`,
                width: streamKind === "camera" ? 320 : 960,
                height: streamKind === "camera" ? 180 : 540,
                mediaHash,
                byteLength: frame.byteLength
            }
        }
    };
}

function payloadBytes(payload: any) {
    if (payload?.kind === "fallback-audio") {
        const encoded = typeof payload.audioBase64 === "string" ? payload.audioBase64 : payload.pcmBase64;
        if (typeof encoded === "string") return Buffer.from(encoded, "base64");
    }
    if (payload?.kind === "fallback-video" && typeof payload.frame === "string") {
        const base64 = payload.frame.includes(",") ? payload.frame.split(",").pop()! : payload.frame;
        return Buffer.from(base64, "base64");
    }
    return Buffer.alloc(0);
}

function validateMediaEvent(body: any, expected: ExpectedMedia) {
    const payload = body.payload || {};
    const bytes = payloadBytes(payload);
    const actualHash = sha256(bytes);
    return payload.kind === expected.kind &&
        payload.streamKind === expected.streamKind &&
        body.roomId === expected.roomId &&
        bytes.byteLength === expected.byteLength &&
        actualHash === expected.mediaHash &&
        payload.mediaHash === expected.mediaHash;
}

async function runObserver() {
    ensureDirs();
    await applyNetem();

    const observerId = Math.max(0, Math.floor(envNumber("LOADNET_WORKER_ID", 0)));
    const observeRelay = process.env.LOADNET_OBSERVE_RELAY || "ws://relay-1:7447";
    const wireFormat = process.env.LOADNET_WIRE_FORMAT === "binary-json" ||
        process.env.LOADNET_WIRE_FORMAT === "binary-v1" ||
        process.env.LOADNET_WIRE_FORMAT === "binary-v2"
        ? process.env.LOADNET_WIRE_FORMAT
        : "binary-v1";
    const observeTimeoutMs = Math.max(5000, envNumber("LOADNET_CALL_MEDIA_OBSERVE_TIMEOUT_MS", 30000));
    const resultPath = observerPath("result", observerId);
    const transportMode = realtimeTransportMode();
    const observerKeys = keyPair();
    let client: CgpClient | undefined;
    let realtimeClient: CgpWebTransportDatagramClient | undefined;
    let session: CallMediaSession | undefined;
    let observedEvents = 0;
    let corruptPayloads = 0;
    let channelReady = false;
    const verified = new Set<string>();
    const duplicateIds = new Set<string>();
    const latenciesMs: number[] = [];

    const buildResult = (ok: boolean, error?: unknown): ObserverResult => {
        const expectedIds = session
            ? Array.from({ length: session.messages }, (_, index) => `call-media-${index}`)
            : [];
        return {
            ok,
            error: error instanceof Error ? error.message : error ? String(error) : undefined,
            observerId,
            messages: session?.messages ?? 0,
            observedEvents,
            verifiedMessages: verified.size,
            corruptPayloads,
            duplicateMessages: duplicateIds.size,
            channelReady,
            missingMessages: expectedIds.filter((id) => !verified.has(id)),
            latenciesMs,
            observedLatencyMs: {
                p50: percentile(latenciesMs, 0.5),
                p95: percentile(latenciesMs, 0.95),
                p99: percentile(latenciesMs, 0.99)
            },
            freshDeliveryRatio: session && session.messages > 0 ? verified.size / session.messages : 0
        };
    };

    try {
        await waitFor("call-media publisher session", () => fs.existsSync(SESSION_PATH), 30000);
        session = readJson<CallMediaSession>(SESSION_PATH);
        const expectedById = new Map<string, ExpectedMedia>();
        for (let index = 0; index < session.messages; index += 1) {
            const plan = mediaPlan(index, session.guildId, session.channelId, session.rooms);
            expectedById.set(plan.expected.messageId, plan.expected);
        }

        const handleCallEvent = (event: GuildEvent) => {
            const body = event.body as any;
            if (body?.type !== "CALL_EVENT") return;
            const payload = body.payload || {};
            const expected = expectedById.get(String(payload.messageId || ""));
            if (!expected) return;
            observedEvents += 1;
            if (verified.has(expected.messageId)) {
                duplicateIds.add(expected.messageId);
                return;
            }
            if (!validateMediaEvent(body, expected)) {
                corruptPayloads += 1;
                return;
            }
            verified.add(expected.messageId);
            if (typeof payload.capturedAt === "number") {
                latenciesMs.push(Date.now() - payload.capturedAt);
            }
        };

        client = new CgpClient({
            relays: [observeRelay],
            keyPair: observerKeys,
            wireFormat,
            connectTimeoutMs: 10000
        });
        client.on("event", (event: GuildEvent) => {
            const body = event.body as any;
            if (body?.type === "MESSAGE" && body.messageId === session?.readinessMessageId) {
                if (!channelReady) {
                    channelReady = true;
                    writeJson(observerPath("ready", observerId), { observerId, readyAt: Date.now() });
                }
                return;
            }
            if (transportMode === "websocket") {
                handleCallEvent(event);
            }
        });

        await client.connect();
        await waitFor("cross-relay channel history", async () => {
            const history = await client?.getHistory({ guildId: session!.guildId, limit: 10 }).catch(() => undefined);
            return Boolean(history?.events?.some((event) => (event.body as any)?.channelId === session!.channelId));
        });
        await subscribeClientToChannels(client, session.guildId, [session.channelId], wireFormat);
        if (transportMode === "webtransport") {
            const realtimeUrl = process.env.LOADNET_OBSERVE_REALTIME_RELAY ||
                "https://relay-1:7448/cgp/realtime";
            realtimeClient = new CgpWebTransportDatagramClient({
                url: realtimeUrl,
                certificateHash: process.env.LOADNET_WEBTRANSPORT_CERT_HASH,
                connectTimeoutMs: 10000
            });
            realtimeClient.on("event", handleCallEvent);
            await realtimeClient.connect();
            await realtimeClient.subscribeTransient(
                session.guildId,
                observerKeys,
                [session.channelId]
            );
        }
        writeJson(observerPath("connected", observerId), { observerId, connectedAt: Date.now() });

        await waitFor("observer readiness event", () => channelReady, observeTimeoutMs);
        await waitFor("publisher completion", () => fs.existsSync(PUBLISH_COMPLETE_PATH), Math.max(observeTimeoutMs, 30000));
        if (session.deliveryMode === "realtime") {
            const publishComplete = readJson<{ completedAt: number }>(PUBLISH_COMPLETE_PATH);
            const remainingFreshWindowMs = publishComplete.completedAt + session.mediaDeadlineMs + 250 - Date.now();
            if (remainingFreshWindowMs > 0) await sleep(remainingFreshWindowMs);
        } else if (verified.size < expectedById.size) {
            await waitFor(
                `all websocket call media payloads for observer ${observerId}`,
                () => verified.size >= expectedById.size,
                observeTimeoutMs
            );
        }

        const freshDeliveryRatio = expectedById.size > 0 ? verified.size / expectedById.size : 0;
        const deliveryPassed = session.deliveryMode === "realtime"
            ? freshDeliveryRatio >= session.minFreshDeliveryRatio
            : verified.size === expectedById.size;
        const result = buildResult(deliveryPassed && corruptPayloads === 0 && channelReady);
        writeJson(resultPath, result);
        if (!result.ok) process.exitCode = 1;
    } catch (error) {
        writeJson(resultPath, buildResult(false, error));
        console.error(error);
        process.exitCode = 1;
    } finally {
        await realtimeClient?.close();
        client?.close();
    }
}

async function runWorker() {
    ensureDirs();
    await applyNetem();

    const relays = (process.env.LOADNET_RELAYS || "ws://relay-0:7447,ws://relay-1:7447")
        .split(",")
        .map((entry) => entry.trim())
        .filter(Boolean);
    const writeRelay = process.env.LOADNET_WRITE_RELAY || relays[0] || "ws://relay-0:7447";
    const observeRelay = process.env.LOADNET_OBSERVE_RELAY || relays[1] || writeRelay;
    const wireFormat = process.env.LOADNET_WIRE_FORMAT === "binary-json" ||
        process.env.LOADNET_WIRE_FORMAT === "binary-v1" ||
        process.env.LOADNET_WIRE_FORMAT === "binary-v2"
        ? process.env.LOADNET_WIRE_FORMAT
        : "binary-v1";
    const messages = Math.max(1, Math.floor(envNumber("LOADNET_CALL_MEDIA_MESSAGES", 300)));
    const rooms = Math.max(1, Math.floor(envNumber("LOADNET_CALL_MEDIA_ROOMS", 12)));
    const publishConcurrency = Math.max(1, Math.floor(envNumber("LOADNET_CALL_MEDIA_CONCURRENCY", 8)));
    const publishRatePerSecond = Math.max(0, envNumber("LOADNET_CALL_MEDIA_RATE_PER_SECOND", 0));
    const deliveryMode: DeliveryMode = process.env.LOADNET_CALL_MEDIA_DELIVERY_MODE === "realtime"
        ? "realtime"
        : "exact";
    const mediaDeadlineMs = Math.max(100, envNumber("LOADNET_CALL_MEDIA_DEADLINE_MS", 2500));
    const minFreshDeliveryRatio = Math.max(0, Math.min(1, envNumber("LOADNET_CALL_MEDIA_MIN_FRESH_RATIO", 0.95)));
    const configuredMediaProfile = process.env.LOADNET_CALL_MEDIA_PROFILE;
    const mediaProfile: MediaProfile =
        configuredMediaProfile === "audio" ||
        configuredMediaProfile === "camera" ||
        configuredMediaProfile === "screen" ||
        configuredMediaProfile === "stress"
            ? configuredMediaProfile
            : "mixed";
    const observerSockets = Math.max(1, Math.floor(envNumber("LOADNET_CALL_MEDIA_OBSERVERS", 2)));
    const publishTimeoutMs = Math.max(1000, envNumber("LOADNET_CALL_MEDIA_PUBLISH_TIMEOUT_MS", 10000));
    const observeTimeoutMs = Math.max(5000, envNumber("LOADNET_CALL_MEDIA_OBSERVE_TIMEOUT_MS", 30000));
    const transportMode = realtimeTransportMode();

    const senderKeys = keyPair();
    const ownerKeys = keyPair();
    const sender = new CgpClient({ relays: [writeRelay], keyPair: senderKeys, wireFormat });
    const realtimeSenders: CgpWebTransportDatagramClient[] = [];
    sender.setMaxListeners(Math.max(10, publishConcurrency + 4));

    const expectedById = new Map<string, ExpectedMedia>();
    let audioMessages = 0;
    let cameraMessages = 0;
    let screenMessages = 0;
    const publishLatencies: number[] = [];
    const audioPublishLatencies: number[] = [];
    const cameraPublishLatencies: number[] = [];
    const screenPublishLatencies: number[] = [];
    let publishedMessages = 0;
    let droppedPublishes = 0;
    let publishDurationMs = 0;
    let observerResults: ObserverResult[] = [];

    const buildSummary = (ok: boolean, error?: unknown) => {
        const totalBytes = [...expectedById.values()].reduce((sum, entry) => sum + entry.byteLength, 0);
        const observedLatencies = observerResults.flatMap((result) => result.latenciesMs);
        const missingDeliveries = observerResults.reduce((sum, result) => sum + result.missingMessages.length, 0) +
            Math.max(0, observerSockets - observerResults.length) * messages;
        const missingMessages = observerResults.flatMap((result) =>
            result.missingMessages.map((messageId) => `${result.observerId}:${messageId}`)
        ).slice(0, 100);
        return {
            ok,
            error: error instanceof Error ? error.message : error ? String(error) : undefined,
            runId: RUN_ID,
            role: `${transportMode}-call-media`,
            protocol: transportMode === "webtransport"
                ? "cgp-webtransport-datagram-call-media"
                : "cgp-transient-call-media",
            wireFormat,
            relays: { writeRelay, observeRelay },
            messages,
            rooms,
            observerSockets,
            publishConcurrency,
            publishRatePerSecond,
            deliveryMode,
            mediaDeadlineMs: deliveryMode === "realtime" ? mediaDeadlineMs : undefined,
            minFreshDeliveryRatio: deliveryMode === "realtime" ? minFreshDeliveryRatio : undefined,
            mediaProfile,
            publishDurationMs,
            publishMessagesPerSecond: publishDurationMs > 0
                ? Math.round((publishedMessages * 100000) / publishDurationMs) / 100
                : 0,
            publishedMessages,
            droppedPublishes,
            audioMessages,
            cameraMessages,
            screenMessages,
            totalBytes,
            expectedDeliveries: messages * observerSockets,
            observedEvents: observerResults.reduce((sum, result) => sum + result.observedEvents, 0),
            verifiedMessages: observerResults.length > 0
                ? Math.min(...observerResults.map((result) => result.verifiedMessages))
                : 0,
            verifiedDeliveries: observerResults.reduce((sum, result) => sum + result.verifiedMessages, 0),
            channelReadyEvents: observerResults.filter((result) => result.channelReady).length,
            corruptPayloads: observerResults.reduce((sum, result) => sum + result.corruptPayloads, 0),
            duplicateMessages: observerResults.reduce((sum, result) => sum + result.duplicateMessages, 0),
            missingDeliveries,
            freshDeliveryRatio: messages * observerSockets > 0
                ? observerResults.reduce((sum, result) => sum + result.verifiedMessages, 0) / (messages * observerSockets)
                : 0,
            missingMessages,
            observerResults: observerResults.map(({ latenciesMs: _, missingMessages: missing, ...result }) => ({
                ...result,
                missingMessages: missing.slice(0, 20)
            })),
            publishLatencyMs: {
                p50: percentile(publishLatencies, 0.5),
                p95: percentile(publishLatencies, 0.95),
                p99: percentile(publishLatencies, 0.99)
            },
            publishLatencyByLaneMs: {
                audioDatagram: {
                    p50: percentile(audioPublishLatencies, 0.5),
                    p95: percentile(audioPublishLatencies, 0.95),
                    p99: percentile(audioPublishLatencies, 0.99)
                },
                cameraStream: {
                    p50: percentile(cameraPublishLatencies, 0.5),
                    p95: percentile(cameraPublishLatencies, 0.95),
                    p99: percentile(cameraPublishLatencies, 0.99)
                },
                screenStream: {
                    p50: percentile(screenPublishLatencies, 0.5),
                    p95: percentile(screenPublishLatencies, 0.95),
                    p99: percentile(screenPublishLatencies, 0.99)
                }
            },
            observedLatencyMs: {
                p50: percentile(observedLatencies, 0.5),
                p95: percentile(observedLatencies, 0.95),
                p99: percentile(observedLatencies, 0.99)
            }
        };
    };

    const writeSummary = (summary: ReturnType<typeof buildSummary>) => {
        writeJson(path.join(METRICS_DIR, metricsFileName()), summary);
        writeJson(path.join(DATA_DIR, "summary.json"), summary);
    };

    try {
        const guildId = hashObject({ loadnet: `${transportMode}-call-media`, runId: RUN_ID, owner: ownerKeys.pub });
        const channelId = hashObject({ loadnet: `${transportMode}-call-media-channel`, guildId });
        const createdAt = Date.now();
        const bootstrapBodies: EventBody[] = [
            {
                type: "GUILD_CREATE",
                guildId,
                name: `${transportMode === "webtransport" ? "WebTransport" : "WebSocket"} Call Media Loadnet`,
                description: "loadnet transient media guild",
                access: "public",
                policies: { posting: "public" }
            } as EventBody,
            {
                type: "CHANNEL_CREATE",
                guildId,
                channelId,
                name: "voice-media",
                kind: "voice"
            } as EventBody
        ];
        const bootstrapEvents = await Promise.all(bootstrapBodies.map((body, index) =>
            signedPayload(body, ownerKeys.pub, ownerKeys.priv, createdAt + index)
        ));
        await Promise.all([...new Set([writeRelay, observeRelay])].map((relay, index) =>
            publishBootstrapBatch(relay, `call-media-bootstrap-${RUN_ID}-${index}`, bootstrapEvents, wireFormat)
        ));

        await sender.connect();
        if (transportMode === "webtransport") {
            const realtimeRelayUrls = (
                process.env.LOADNET_REALTIME_RELAYS ||
                process.env.LOADNET_WRITE_REALTIME_RELAY ||
                "https://relay-0:7448/cgp/realtime"
            ).split(",").map((entry) => entry.trim()).filter(Boolean);
            const candidates = realtimeRelayUrls.map((url) =>
                new CgpWebTransportDatagramClient({
                    url,
                    certificateHash: process.env.LOADNET_WEBTRANSPORT_CERT_HASH,
                    connectTimeoutMs: 10000
                })
            );
            const connections = await Promise.allSettled(
                candidates.map((candidate) => candidate.connect())
            );
            for (let index = 0; index < candidates.length; index += 1) {
                if (connections[index]?.status === "fulfilled") {
                    realtimeSenders.push(candidates[index]);
                } else {
                    await candidates[index].close();
                }
            }
            if (realtimeSenders.length === 0) {
                throw new Error("No WebTransport realtime relay is available");
            }
        }
        setClientSocketMaxListeners(sender, Math.max(10, publishConcurrency * 2 + 8));
        const readinessMessageId = `call-media-ready-${RUN_ID}`;
        writeJson(SESSION_PATH, {
            guildId,
            channelId,
            readinessMessageId,
            messages,
            rooms,
            deliveryMode,
            mediaDeadlineMs,
            minFreshDeliveryRatio,
            mediaProfile
        } satisfies CallMediaSession);
        await waitFor(
            "observer subscriptions",
            () => Array.from({ length: observerSockets }, (_, observerId) =>
                fs.existsSync(observerPath("connected", observerId))
            ).every(Boolean),
            observeTimeoutMs
        );
        await sender.publishReliable({
            type: "MESSAGE",
            guildId,
            channelId,
            messageId: readinessMessageId,
            content: "channel-ready"
        }, { timeoutMs: publishTimeoutMs, clientEventId: readinessMessageId });
        await waitFor(
            "observer channel readiness events",
            () => Array.from({ length: observerSockets }, (_, observerId) =>
                fs.existsSync(observerPath("ready", observerId))
            ).every(Boolean),
            observeTimeoutMs
        );

        const bodies = Array.from({ length: messages }, (_, index) => {
            const plan = mediaPlan(index, guildId, channelId, rooms);
            expectedById.set(plan.expected.messageId, plan.expected);
            if (plan.expected.kind === "fallback-audio") audioMessages += 1;
            if (plan.expected.streamKind === "camera") cameraMessages += 1;
            if (plan.expected.streamKind === "screen") screenMessages += 1;
            return plan.body;
        });

        let nextIndex = 0;
        const publishStarted = Date.now();
        writeJson(PUBLISH_START_PATH, {
            startedAt: publishStarted,
            realtimeRelays: realtimeSenders.length
        });
        const publishWorker = async () => {
            while (nextIndex < bodies.length) {
                const index = nextIndex;
                nextIndex += 1;
                if (publishRatePerSecond > 0) {
                    const scheduledAt = publishStarted + (index * 1000) / publishRatePerSecond;
                    const delayMs = scheduledAt - Date.now();
                    if (delayMs > 0) await sleep(delayMs);
                }
                const started = Date.now();
                bodies[index].payload.capturedAt = started;
                if (deliveryMode === "realtime") {
                    bodies[index].expiresAt = started + mediaDeadlineMs;
                }
                try {
                    const sent = transportMode === "webtransport"
                        ? await (async () => {
                            const candidates = realtimeSenders
                                .filter((candidate) => candidate.connected)
                                .sort((left, right) => left.bufferedAmount - right.bufferedAmount)
                                .slice(0, 2);
                            const payload = await realtimeSenders[0].prepareTransient(
                                bodies[index],
                                senderKeys,
                                `call-media-${index}`
                            );
                            try {
                                return await Promise.any(candidates.map(async (candidate) => {
                                    const sent = await candidate.publishPreparedTransient(payload);
                                    if (!sent) {
                                        throw new Error("Realtime relay is backpressured");
                                    }
                                    return true;
                                }));
                            } catch {
                                return false;
                            }
                        })()
                        : deliveryMode === "realtime"
                        ? await sender.publishTransientUnreliable(bodies[index], {
                            clientEventId: `call-media-${index}`,
                            maxBufferedBytes: 512 * 1024
                        })
                        : Boolean(await sender.publishTransientReliable(bodies[index], {
                            timeoutMs: publishTimeoutMs,
                            clientEventId: `call-media-${index}`
                        }));
                    if (!sent) {
                        droppedPublishes += 1;
                        continue;
                    }
                    publishedMessages += 1;
                    const publishLatency = Date.now() - started;
                    publishLatencies.push(publishLatency);
                    const expected = expectedById.get(`call-media-${index}`);
                    if (expected?.kind === "fallback-audio") {
                        audioPublishLatencies.push(publishLatency);
                    } else if (expected?.streamKind === "camera") {
                        cameraPublishLatencies.push(publishLatency);
                    } else if (expected?.streamKind === "screen") {
                        screenPublishLatencies.push(publishLatency);
                    }
                } catch (error) {
                    if (deliveryMode !== "realtime") throw error;
                    droppedPublishes += 1;
                }
            }
        };
        await Promise.all(Array.from({ length: Math.min(publishConcurrency, bodies.length) }, publishWorker));
        publishDurationMs = Date.now() - publishStarted;
        writeJson(PUBLISH_COMPLETE_PATH, { publishedMessages, completedAt: Date.now() });

        await waitFor(
            "all websocket call media observer results",
            () => Array.from({ length: observerSockets }, (_, observerId) =>
                fs.existsSync(observerPath("result", observerId))
            ).every(Boolean),
            observeTimeoutMs
        );
        observerResults = Array.from({ length: observerSockets }, (_, observerId) =>
            readJson<ObserverResult>(observerPath("result", observerId))
        );
        const allObserversVerified = observerResults.length === observerSockets &&
            observerResults.every((result) => result.ok);
        const summary = buildSummary(
            allObserversVerified &&
            (deliveryMode === "realtime" || publishedMessages === messages)
        );
        writeSummary(summary);
        if (!summary.ok) {
            process.exitCode = 1;
        }
    } catch (error) {
        observerResults = Array.from({ length: observerSockets }, (_, observerId) => {
            const resultPath = observerPath("result", observerId);
            return fs.existsSync(resultPath) ? readJson<ObserverResult>(resultPath) : undefined;
        }).filter((result): result is ObserverResult => Boolean(result));
        const summary = buildSummary(false, error);
        writeSummary(summary);
        console.error(error);
        process.exitCode = 1;
    } finally {
        await Promise.allSettled(realtimeSenders.map((candidate) => candidate.close()));
        sender.close();
    }
}

async function main() {
    const role = roleName();
    if (role === "worker") return runWorker();
    if (role === "observer") return runObserver();
    console.log("usage: tsx loadnet/call-media-node.ts worker|observer");
}

main().catch((error) => {
    console.error(error);
    process.exit(1);
});
