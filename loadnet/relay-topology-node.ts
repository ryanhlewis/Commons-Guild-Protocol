import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { WebSocket } from "ws";
import { CgpClient } from "@cgp/client";
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
const RUN_ID = process.env.LOADNET_RUN_ID || "relay-topology";

type RelayClass = "user" | "open-proxy";
type MediaKind = "fallback-audio" | "fallback-video";

interface RelayRoute {
    name: string;
    url: string;
    relayClass: RelayClass;
    channelId: string;
}

interface LogicalUser {
    id: string;
    natClosed: boolean;
    relay: RelayRoute;
}

interface ExpectedMedia {
    messageId: string;
    relayName: string;
    relayClass: RelayClass;
    mediaHash: string;
    byteLength: number;
    targetUserId: string;
    sourceUserId: string;
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
    fs.writeFileSync(filePath, JSON.stringify(value, null, 2));
}

async function sleep(ms: number) {
    await new Promise((resolve) => setTimeout(resolve, ms));
}

function roleName() {
    return process.argv[2] || process.env.LOADNET_ROLE || "worker";
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
    if (lossPercent > 0) args.push("loss", `${lossPercent}%`);

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

async function subscribeClientToChannel(client: CgpClient, guildId: string, channelId: string, wireFormat: CgpWireFormat) {
    const sockets = ((client as unknown as { sockets?: WebSocket[] }).sockets || [])
        .filter((socket) => socket.readyState === WebSocket.OPEN);
    if (sockets.length === 0) throw new Error("No open relay socket for topology subscription");
    for (const socket of sockets) {
        const subId = `relay-topology-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        socket.send(encodeCgpFrame("SUB", { subId, guildId, channels: [channelId] }, wireFormat));
    }
}

function payloadBytes(payload: any) {
    if (payload?.kind === "fallback-audio" && typeof payload.pcmBase64 === "string") {
        return Buffer.from(payload.pcmBase64, "base64");
    }
    if (payload?.kind === "fallback-video" && typeof payload.frame === "string") {
        const base64 = payload.frame.includes(",") ? payload.frame.split(",").pop()! : payload.frame;
        return Buffer.from(base64, "base64");
    }
    return Buffer.alloc(0);
}

function parseRelayList(name: string, relayClass: RelayClass) {
    return (process.env[name] || "")
        .split(",")
        .map((entry) => entry.trim())
        .filter(Boolean)
        .map((entry, index) => {
            const [rawName, url] = entry.split("=");
            const relayName = rawName || `${relayClass}-${index}`;
            return {
                name: relayName,
                url: url || `ws://${relayName}:7447`,
                relayClass,
                channelId: ""
            };
        });
}

function buildUsers(userRelays: RelayRoute[], openRelays: RelayRoute[], users: number, natClosedPercent: number) {
    const natClosedUsers = Math.min(users, Math.max(0, Math.round(users * natClosedPercent / 100)));
    return Array.from({ length: users }, (_, index): LogicalUser => {
        const natClosed = index < natClosedUsers;
        const relay = natClosed
            ? openRelays[index % openRelays.length]
            : userRelays[index % userRelays.length];
        return {
            id: `loadnet-user-${index}`,
            natClosed,
            relay
        };
    });
}

function mediaPlan(index: number, guildId: string, source: LogicalUser, target: LogicalUser, audioBytes: number, videoBytes: number) {
    const messageId = `topology-media-${index}`;
    const mediaKind: MediaKind = index % 4 === 0 ? "fallback-video" : "fallback-audio";
    const size = mediaKind === "fallback-audio" ? audioBytes : videoBytes;
    const bytes = deterministicBytes(`${RUN_ID}:${messageId}:${source.id}:${target.id}`, size);
    const mediaHash = sha256(bytes);
    const payload = mediaKind === "fallback-audio"
        ? {
            kind: "fallback-audio",
            sampleRate: 16000,
            pcmBase64: bytes.toString("base64")
        }
        : {
            kind: "fallback-video",
            streamKind: "camera",
            frame: `data:image/jpeg;base64,${bytes.toString("base64")}`,
            width: 320,
            height: 180
        };

    return {
        expected: {
            messageId,
            relayName: target.relay.name,
            relayClass: target.relay.relayClass,
            mediaHash,
            byteLength: bytes.byteLength,
            targetUserId: target.id,
            sourceUserId: source.id
        } satisfies ExpectedMedia,
        body: {
            type: "CALL_EVENT",
            guildId,
            channelId: target.relay.channelId,
            roomId: `route-${target.relay.name}`,
            payload: {
                ...payload,
                messageId,
                fromUserId: source.id,
                toUserId: target.id,
                transport: "relay-websocket-topology",
                capturedAt: Date.now(),
                mediaHash,
                byteLength: bytes.byteLength,
                route: {
                    sourceRelay: source.relay.name,
                    targetRelay: target.relay.name,
                    targetRelayClass: target.relay.relayClass,
                    targetNatClosed: target.natClosed
                }
            }
        } as EventBody
    };
}

function increment(map: Map<string, number>, key: string, amount = 1) {
    map.set(key, (map.get(key) || 0) + amount);
}

function mapObject(map: Map<string, number>) {
    return Object.fromEntries([...map.entries()].sort(([left], [right]) => left.localeCompare(right)));
}

async function runWorker() {
    ensureDirs();
    await applyNetem();

    const wireFormat = process.env.LOADNET_WIRE_FORMAT === "binary-json" ||
        process.env.LOADNET_WIRE_FORMAT === "binary-v1" ||
        process.env.LOADNET_WIRE_FORMAT === "binary-v2"
        ? process.env.LOADNET_WIRE_FORMAT
        : "binary-v1";
    const messages = Math.max(1, Math.floor(envNumber("LOADNET_TOPOLOGY_MESSAGES", 512)));
    const users = Math.max(2, Math.floor(envNumber("LOADNET_TOPOLOGY_USERS", 64)));
    const natClosedPercent = Math.min(100, Math.max(0, envNumber("LOADNET_TOPOLOGY_NAT_CLOSED_PERCENT", 25)));
    const publishConcurrency = Math.max(1, Math.floor(envNumber("LOADNET_TOPOLOGY_CONCURRENCY", 16)));
    const publishTimeoutMs = Math.max(1000, envNumber("LOADNET_TOPOLOGY_PUBLISH_TIMEOUT_MS", 10000));
    const observeTimeoutMs = Math.max(5000, envNumber("LOADNET_TOPOLOGY_OBSERVE_TIMEOUT_MS", 45000));
    const audioBytes = Math.max(64, Math.floor(envNumber("LOADNET_TOPOLOGY_AUDIO_BYTES", 640)));
    const videoBytes = Math.max(64, Math.floor(envNumber("LOADNET_TOPOLOGY_VIDEO_BYTES", 4096)));
    const userRelays = parseRelayList("LOADNET_USER_RELAYS", "user");
    const openRelays = parseRelayList("LOADNET_OPEN_RELAYS", "open-proxy");
    if (userRelays.length === 0) throw new Error("LOADNET_USER_RELAYS must include at least one relay");
    if (openRelays.length === 0) throw new Error("LOADNET_OPEN_RELAYS must include at least one relay");
    const relays = [...userRelays, ...openRelays];

    const ownerKeys = keyPair();
    const senderKeys = keyPair();
    const guildId = hashObject({ loadnet: "relay-topology", runId: RUN_ID, owner: ownerKeys.pub });
    for (const relay of relays) {
        relay.channelId = hashObject({ loadnet: "relay-topology-channel", guildId, relayName: relay.name });
    }
    const logicalUsers = buildUsers(userRelays, openRelays, users, natClosedPercent);
    const clients = new Map<string, CgpClient>();
    const observers = new Map<string, CgpClient>();

    const expectedById = new Map<string, ExpectedMedia>();
    const expectedMessagesByRelay = new Map<string, number>();
    const expectedBytesByRelay = new Map<string, number>();
    const observedMessagesByRelay = new Map<string, number>();
    const observedBytesByRelay = new Map<string, number>();
    const sourceMessagesByRelay = new Map<string, number>();
    const verified = new Set<string>();
    const duplicateIds = new Set<string>();
    let corruptPayloads = 0;
    let readinessEvents = 0;
    const publishLatencies: number[] = [];
    const observedLatencies: number[] = [];
    let publishedMessages = 0;

    for (const relay of relays) {
        clients.set(relay.name, new CgpClient({ relays: [relay.url], keyPair: senderKeys, wireFormat, connectTimeoutMs: 10000 }));
        const observer = new CgpClient({ relays: [relay.url], keyPair: keyPair(), wireFormat, connectTimeoutMs: 10000 });
        observer.on("event", (event: GuildEvent) => {
            const body = event.body as any;
            if (body?.type === "MESSAGE" && String(body.messageId || "").startsWith(`topology-ready-${RUN_ID}-`)) {
                readinessEvents += 1;
                return;
            }
            if (body?.type !== "CALL_EVENT") return;
            const payload = body.payload || {};
            const expected = expectedById.get(String(payload.messageId || ""));
            if (!expected) return;
            if (verified.has(expected.messageId)) {
                duplicateIds.add(expected.messageId);
                return;
            }
            const bytes = payloadBytes(payload);
            const actualHash = sha256(bytes);
            if (
                body.channelId !== relays.find((entry) => entry.name === expected.relayName)?.channelId ||
                payload.route?.targetRelay !== expected.relayName ||
                payload.toUserId !== expected.targetUserId ||
                bytes.byteLength !== expected.byteLength ||
                actualHash !== expected.mediaHash ||
                payload.mediaHash !== expected.mediaHash
            ) {
                corruptPayloads += 1;
                return;
            }
            verified.add(expected.messageId);
            increment(observedMessagesByRelay, expected.relayName);
            increment(observedBytesByRelay, expected.relayName, expected.byteLength);
            if (typeof payload.capturedAt === "number") {
                observedLatencies.push(Date.now() - payload.capturedAt);
            }
        });
        observers.set(relay.name, observer);
    }

    const buildSummary = (ok: boolean, error?: unknown) => {
        const totalBytes = [...expectedById.values()].reduce((sum, entry) => sum + entry.byteLength, 0);
        const expectedOpenProxyMessages = [...expectedById.values()].filter((entry) => entry.relayClass === "open-proxy").length;
        const observedOpenProxyMessages = [...observedMessagesByRelay.entries()]
            .filter(([relayName]) => openRelays.some((relay) => relay.name === relayName))
            .reduce((sum, [, count]) => sum + count, 0);
        const natClosedUsers = logicalUsers.filter((user) => user.natClosed).length;
        return {
            ok,
            error: error instanceof Error ? error.message : error ? String(error) : undefined,
            runId: RUN_ID,
            role: "relay-topology",
            protocol: "cgp-transient-call-media-topology",
            wireFormat,
            messages,
            users,
            natClosedUsers,
            natClosedPercent,
            userRelays: userRelays.length,
            openRelays: openRelays.length,
            publishedMessages,
            verifiedMessages: verified.size,
            corruptPayloads,
            duplicateMessages: duplicateIds.size,
            missingMessages: [...expectedById.keys()].filter((id) => !verified.has(id)),
            totalBytes,
            expectedOpenProxyMessages,
            observedOpenProxyMessages,
            expectedOpenProxyRatio: expectedOpenProxyMessages / messages,
            observedOpenProxyRatio: observedOpenProxyMessages / Math.max(1, verified.size),
            expectedMessagesByRelay: mapObject(expectedMessagesByRelay),
            observedMessagesByRelay: mapObject(observedMessagesByRelay),
            expectedBytesByRelay: mapObject(expectedBytesByRelay),
            observedBytesByRelay: mapObject(observedBytesByRelay),
            sourceMessagesByRelay: mapObject(sourceMessagesByRelay),
            readinessEvents,
            publishLatencyMs: {
                p50: percentile(publishLatencies, 0.5),
                p95: percentile(publishLatencies, 0.95),
                p99: percentile(publishLatencies, 0.99)
            },
            observedLatencyMs: {
                p50: percentile(observedLatencies, 0.5),
                p95: percentile(observedLatencies, 0.95),
                p99: percentile(observedLatencies, 0.99)
            }
        };
    };

    const writeSummary = (summary: ReturnType<typeof buildSummary>) => {
        writeJson(path.join(METRICS_DIR, "relay-topology.json"), summary);
        writeJson(path.join(DATA_DIR, "summary.json"), summary);
    };

    try {
        const createdAt = Date.now();
        const bootstrapBodies: EventBody[] = [
            {
                type: "GUILD_CREATE",
                guildId,
                name: "Relay Topology Loadnet",
                description: "loadnet user relay and open proxy topology guild",
                access: "public",
                policies: { posting: "public" }
            } as EventBody,
            ...relays.map((relay) => ({
                type: "CHANNEL_CREATE",
                guildId,
                channelId: relay.channelId,
                name: `route-${relay.name}`,
                kind: "voice"
            } as EventBody))
        ];
        const bootstrapEvents = await Promise.all(bootstrapBodies.map((body, index) =>
            signedPayload(body, ownerKeys.pub, ownerKeys.priv, createdAt + index)
        ));
        await Promise.all(relays.map((relay, index) =>
            publishBootstrapBatch(relay.url, `relay-topology-bootstrap-${RUN_ID}-${index}`, bootstrapEvents, wireFormat)
        ));

        await Promise.all([...clients.values(), ...observers.values()].map((client) => client.connect()));
        for (const relay of relays) {
            await subscribeClientToChannel(observers.get(relay.name)!, guildId, relay.channelId, wireFormat);
        }

        await Promise.all(relays.map((relay) =>
            clients.get(relay.name)!.publishReliable({
                type: "MESSAGE",
                guildId,
                channelId: relay.channelId,
                messageId: `topology-ready-${RUN_ID}-${relay.name}`,
                content: "ready"
            }, { timeoutMs: publishTimeoutMs, clientEventId: `topology-ready-${relay.name}` })
        ));
        await waitFor("relay topology channel readiness", () => readinessEvents >= relays.length, observeTimeoutMs);

        const bodies = Array.from({ length: messages }, (_, index) => {
            const target = logicalUsers[index % logicalUsers.length];
            const source = logicalUsers[(index * 13 + 7) % logicalUsers.length];
            const plan = mediaPlan(index, guildId, source, target, audioBytes, videoBytes);
            expectedById.set(plan.expected.messageId, plan.expected);
            increment(expectedMessagesByRelay, target.relay.name);
            increment(expectedBytesByRelay, target.relay.name, plan.expected.byteLength);
            increment(sourceMessagesByRelay, source.relay.name);
            return { source, body: plan.body };
        });

        let nextIndex = 0;
        const publishWorker = async () => {
            while (nextIndex < bodies.length) {
                const index = nextIndex;
                nextIndex += 1;
                const plan = bodies[index];
                const started = Date.now();
                await clients.get(plan.source.relay.name)!.publishTransientReliable(plan.body, {
                    timeoutMs: publishTimeoutMs,
                    clientEventId: `topology-media-${index}`
                });
                publishedMessages += 1;
                publishLatencies.push(Date.now() - started);
            }
        };
        await Promise.all(Array.from({ length: Math.min(publishConcurrency, bodies.length) }, publishWorker));
        await waitFor("all relay topology media payloads", () => verified.size >= expectedById.size, observeTimeoutMs);

        const summary = buildSummary(verified.size === expectedById.size && corruptPayloads === 0);
        const observedMatchesExpected = JSON.stringify(summary.expectedMessagesByRelay) === JSON.stringify(summary.observedMessagesByRelay);
        const openProxyMatchesExpected = summary.expectedOpenProxyMessages === summary.observedOpenProxyMessages;
        const ratioBound = Math.max(1 / users, 0.02);
        const ratioOk = Math.abs(summary.observedOpenProxyRatio - summary.natClosedUsers / users) <= ratioBound;
        summary.ok = summary.ok && observedMatchesExpected && openProxyMatchesExpected && ratioOk;
        if (!observedMatchesExpected || !openProxyMatchesExpected || !ratioOk) {
            summary.error = `Topology scaling assertion failed: observedMatchesExpected=${observedMatchesExpected} openProxyMatchesExpected=${openProxyMatchesExpected} ratioOk=${ratioOk}`;
        }
        writeSummary(summary);
        if (!summary.ok) process.exitCode = 1;
    } catch (error) {
        const summary = buildSummary(false, error);
        writeSummary(summary);
        console.error(error);
        process.exitCode = 1;
    } finally {
        for (const client of clients.values()) client.close();
        for (const observer of observers.values()) observer.close();
    }
}

async function main() {
    const role = roleName();
    if (role === "worker") return runWorker();
    console.log("usage: tsx loadnet/relay-topology-node.ts worker");
}

main().catch((error) => {
    console.error(error);
    process.exit(1);
});
