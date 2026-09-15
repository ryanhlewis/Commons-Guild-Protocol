import dgram from "node:dgram";
import { lookup } from "node:dns/promises";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { RealtimeForwardQueue } from "./topology/realtime-forward-queue.ts";
import { RealtimePacketWindow } from "./topology/realtime-packet-window.ts";
import { federationCopiesForEpoch, recoveryCopiesForEpoch } from "./topology/recovery-redundancy.ts";
import { shouldInjectSyntheticBaselineDrop } from "./topology/failure-injection.ts";
import { routeCost } from "./topology/media-topology.ts";
import {
    decodeTopologyPacket,
    deliveryKey,
    deterministicRtpPayload,
    encodeTopologyPacket,
    type TopologyPacket
} from "./topology/packet-wire.ts";
import type {
    StreamKind,
    TopologyLabScenario,
    TopologyName,
    TopologyRouteEpoch,
    TopologyRoutePlan
} from "./topology/types.ts";

const DATA_DIR = process.env.LOADNET_DATA_DIR || "/data";
const READY_DIR = path.join(DATA_DIR, "ready");
const METRICS_DIR = path.join(DATA_DIR, "metrics");
const NETEM_DIR = path.join(DATA_DIR, "netem");
const CHAOS_REQUEST_DIR = path.join(DATA_DIR, "chaos", "requests");
const CHAOS_APPLIED_DIR = path.join(DATA_DIR, "chaos", "applied");
const SCENARIO_PATH = path.join(DATA_DIR, "topology-lab-scenario.json");
const RUN_ID = process.env.LOADNET_RUN_ID || "topology-lab";
const MEDIA_PORT = envNumber("LOADNET_TOPOLOGY_MEDIA_PORT", 5900);
const WORKER_PORT = envNumber("LOADNET_TOPOLOGY_WORKER_PORT", 5901);
const CLIENT_SINKS = Math.max(1, Math.min(256, Math.floor(envNumber("LOADNET_TOPOLOGY_CLIENT_SINKS", 16))));
const CLIENT_SOCKET_SHARDS = Math.max(1, Math.min(64, Math.floor(envNumber("LOADNET_TOPOLOGY_SINK_SOCKET_SHARDS", 8))));

const TOPOLOGY_GENERATION: Record<TopologyName, number> = {
    "turn-mesh": 1,
    "single-sfu": 2,
    "federated-sfu": 3,
    "cascaded-sfu": 4,
    "host-star-game": 5,
    "spatial-sharded-game": 6,
    "resilient-sfu": 7
};

function envNumber(name: string, fallback: number) {
    const value = Number(process.env[name]);
    return Number.isFinite(value) ? value : fallback;
}

function ensureDirs() {
    fs.mkdirSync(READY_DIR, { recursive: true });
    fs.mkdirSync(METRICS_DIR, { recursive: true });
    fs.mkdirSync(NETEM_DIR, { recursive: true });
    fs.mkdirSync(CHAOS_REQUEST_DIR, { recursive: true });
    fs.mkdirSync(CHAOS_APPLIED_DIR, { recursive: true });
}

function writeJson(filePath: string, value: unknown) {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, JSON.stringify(value, null, 2));
}

function writeJsonAtomic(filePath: string, value: unknown) {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    const temporaryPath = `${filePath}.${process.pid}.tmp`;
    fs.writeFileSync(temporaryPath, JSON.stringify(value));
    fs.renameSync(temporaryPath, filePath);
}

function sleep(ms: number) {
    return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

function percentile(values: number[], p: number) {
    if (values.length === 0) return 0;
    const sorted = [...values].sort((left, right) => left - right);
    return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))] ?? 0;
}

function roleName() {
    return process.argv[2] || process.env.LOADNET_ROLE || "help";
}

async function applyNetem() {
    const role = roleName();
    const workerId = process.env.LOADNET_WORKER_ID || "0";
    const latencyMs = envNumber("LOADNET_LATENCY_MS", 0);
    const jitterMs = envNumber("LOADNET_JITTER_MS", 0);
    const lossPercent = envNumber("LOADNET_LOSS_PERCENT", 0);
    const limitPackets = Math.max(1000, Math.floor(envNumber("LOADNET_NETEM_LIMIT_PACKETS", 100000)));
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
    args.push("limit", String(limitPackets));
    let warning = "";
    for (let attempt = 1; attempt <= 20; attempt += 1) {
        try {
            execFileSync("tc", args, { stdio: "ignore" });
            writeJson(statusPath, { role, workerId, required: true, applied: true, args, attempt });
            return;
        } catch (error: any) {
            warning = error?.message || String(error);
            await sleep(100);
        }
    }
    throw new Error(`Failed to apply topology lab netem: ${warning}`);
}

async function waitForFile(filePath: string, timeoutMs = 20000) {
    const deadline = Date.now() + timeoutMs;
    while (!fs.existsSync(filePath)) {
        if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${filePath}`);
        await sleep(50);
    }
}

function readScenario() {
    return JSON.parse(fs.readFileSync(SCENARIO_PATH, "utf8")) as TopologyLabScenario;
}

function routeEpochFor(plan: TopologyRoutePlan, epoch: number): TopologyRouteEpoch {
    const selected = Object.values(plan.routeEpochs ?? {})
        .filter((candidate) => candidate.epoch <= epoch)
        .sort((left, right) => right.epoch - left.epoch)[0];
    return selected ?? {
        epoch: 1,
        failedRelayIds: [],
        departedParticipantIds: [],
        sourceRelayByParticipant: plan.sourceRelayByParticipant,
        recipientRelayByParticipant: plan.recipientRelayByParticipant,
        relayOverlays: plan.relayOverlays
    };
}

function setSocketBuffers(socket: dgram.Socket) {
    try {
        socket.setRecvBufferSize(16 * 1024 * 1024);
        socket.setSendBufferSize(16 * 1024 * 1024);
    } catch {
        // Docker kernels may cap these values; the loadnet records delivery loss either way.
    }
}

async function resolveHostAddresses(hosts: string[]) {
    const addresses = new Map<string, string>();
    for (const host of [...new Set(hosts)]) {
        let lastError: unknown;
        for (let attempt = 0; attempt < 40; attempt += 1) {
            try {
                addresses.set(host, (await lookup(host, { family: 4 })).address);
                lastError = undefined;
                break;
            } catch (error) {
                lastError = error;
                await sleep(50);
            }
        }
        if (lastError) throw new Error(`Unable to resolve topology host ${host}: ${String(lastError)}`);
    }
    return addresses;
}

interface RelayMetrics {
    relayId: string;
    clientIngress: number;
    federationIngress: number;
    federationEgress: number;
    clientEgress: number;
    malformedPackets: number;
    intentionalDrops: number;
    sendErrors: number;
    staleEpochDrops: number;
    wrongRouteDrops: number;
    departedDeliverySuppressions: number;
    queueHighWater: number;
    queueDepth: number;
    supersededQueueDrops: number;
    pendingSends: number;
    pendingSendHighWater: number;
    redundantFederationEgress: number;
    duplicateFederationDrops: number;
    duplicateSourceDrops: number;
    byEpoch: Record<string, {
        clientIngress: number;
        federationIngress: number;
        outboundEnqueued: number;
        outboundRejected: number;
        federationEgress: number;
        clientEgress: number;
    }>;
    byTopology: Record<string, {
        clientIngress: number;
        federationIngress: number;
        federationEgress: number;
        sourceFederationEgress: number;
        transitFederationEgress: number;
        clientEgress: number;
        staleEpochDrops: number;
        wrongRouteDrops: number;
        departedDeliverySuppressions: number;
    }>;
}

async function runRelay() {
    ensureDirs();
    await applyNetem();
    await waitForFile(SCENARIO_PATH);
    const scenario = readScenario();
    const relayId = process.env.LOADNET_RELAY_ID || "media-relay-0";
    const relayIndex = scenario.relayIds.indexOf(relayId);
    if (relayIndex < 0) throw new Error(`Unknown topology lab relay ${relayId}`);
    const socket = dgram.createSocket("udp4");
    const metrics: RelayMetrics = {
        relayId,
        clientIngress: 0,
        federationIngress: 0,
        federationEgress: 0,
        clientEgress: 0,
        malformedPackets: 0,
        intentionalDrops: 0,
        sendErrors: 0,
        staleEpochDrops: 0,
        wrongRouteDrops: 0,
        departedDeliverySuppressions: 0,
        queueHighWater: 0,
        queueDepth: 0,
        supersededQueueDrops: 0,
        pendingSends: 0,
        pendingSendHighWater: 0,
        redundantFederationEgress: 0,
        duplicateFederationDrops: 0,
        duplicateSourceDrops: 0,
        byEpoch: {},
        byTopology: {}
    };
    const outbound = new RealtimeForwardQueue<{ buffer: Buffer; host: string; port: number; onSent?: () => void }>();
    const hostAddresses = await resolveHostAddresses([
        ...scenario.relayIds,
        ...Array.from({ length: CLIENT_SINKS }, (_, sinkIndex) => `client-sink-${sinkIndex}`)
    ]);
    let draining = false;
    let pendingSends = 0;
    const federationCopies = Math.max(1, envNumber("LOADNET_TOPOLOGY_RECOVERY_FEDERATION_COPIES", 2));
    const maximumSeenFederatedPackets = Math.max(4096, envNumber("LOADNET_TOPOLOGY_FEDERATION_DEDUPE_WINDOW", 65536));
    const seenFederatedPackets = new RealtimePacketWindow(maximumSeenFederatedPackets);
    const seenSourcePackets = new RealtimePacketWindow(maximumSeenFederatedPackets);
    const relayMetricPath = path.join(METRICS_DIR, `${relayId}.json`);

    const counters = (topology: TopologyName) => metrics.byTopology[topology] ??= {
        clientIngress: 0,
        federationIngress: 0,
        federationEgress: 0,
        sourceFederationEgress: 0,
        transitFederationEgress: 0,
        clientEgress: 0,
        staleEpochDrops: 0,
        wrongRouteDrops: 0,
        departedDeliverySuppressions: 0
    };
    const epochCounters = (packet: TopologyPacket) => metrics.byEpoch[`${packet.topology}:${packet.epoch}`] ??= {
        clientIngress: 0,
        federationIngress: 0,
        outboundEnqueued: 0,
        outboundRejected: 0,
        federationEgress: 0,
        clientEgress: 0
    };
    const flush = () => {
        metrics.queueDepth = outbound.length;
        writeJsonAtomic(relayMetricPath, { runId: RUN_ID, role: "topology-media-relay", ...metrics });
    };
    const drain = () => {
        if (draining) return;
        draining = true;
        const step = () => {
            const batchSize = Math.max(1, envNumber("LOADNET_TOPOLOGY_RELAY_SEND_BATCH", 24));
            const maxInFlight = Math.max(batchSize, envNumber("LOADNET_TOPOLOGY_RELAY_MAX_IN_FLIGHT", 64));
            for (
                let index = 0;
                index < batchSize && outbound.length > 0 && pendingSends < maxInFlight;
                index += 1
            ) {
                const item = outbound.dequeue()!;
                pendingSends += 1;
                metrics.pendingSends = pendingSends;
                metrics.pendingSendHighWater = Math.max(metrics.pendingSendHighWater, pendingSends);
                socket.send(item.buffer, item.port, item.host, (error) => {
                    pendingSends -= 1;
                    metrics.pendingSends = pendingSends;
                    if (error) metrics.sendErrors += 1;
                    else item.onSent?.();
                    if (outbound.length > 0 && !draining) drain();
                });
            }
            if (outbound.length > 0 && pendingSends < maxInFlight) {
                setImmediate(step);
            } else {
                draining = false;
            }
        };
        setImmediate(step);
    };
    const enqueue = (packet: TopologyPacket, host: string, port: number, onSent?: () => void) => {
        const queued = outbound.enqueue(
            { buffer: encodeTopologyPacket(packet), host: hostAddresses.get(host) ?? host, port, onSent },
            {
                scopeId: RUN_ID,
                generation: TOPOLOGY_GENERATION[packet.topology],
                epoch: packet.topology === "resilient-sfu" ? packet.epoch : 1
            }
        );
        metrics.supersededQueueDrops += queued.droppedSuperseded;
        if (!queued.accepted) {
            epochCounters(packet).outboundRejected += 1;
            return;
        }
        epochCounters(packet).outboundEnqueued += 1;
        metrics.queueHighWater = Math.max(metrics.queueHighWater, outbound.length);
        drain();
    };
    const deliver = (packet: TopologyPacket, recipientId: number) => {
        const sinkHost = `client-sink-${recipientId % CLIENT_SINKS}`;
        const socketShard = Math.floor(recipientId / CLIENT_SINKS) % CLIENT_SOCKET_SHARDS;
        enqueue({ ...packet, type: "delivery", recipientId, originRelayIndex: relayIndex }, sinkHost, WORKER_PORT + socketShard, () => {
            metrics.clientEgress += 1;
            counters(packet.topology).clientEgress += 1;
            epochCounters(packet).clientEgress += 1;
        });
    };
    const enqueueFederated = (packet: TopologyPacket, targetRelay: string, onSent: () => void) => {
        const copies = federationCopiesForEpoch(packet.topology, packet.epoch, {
            routeEpoch: scenario.resilience.routeEpoch,
            recoveryCopies: federationCopies
        });
        for (let copyIndex = 0; copyIndex < copies; copyIndex += 1) {
            enqueue(
                { ...packet, type: "federated", recipientId: undefined, originRelayIndex: relayIndex },
                targetRelay,
                MEDIA_PORT,
                () => {
                    if (copyIndex > 0) metrics.redundantFederationEgress += 1;
                    onSent();
                }
            );
        }
    };
    const acceptFederatedPacket = (packet: TopologyPacket) => {
        const key = `${packet.topology}:${packet.epoch}:${packet.streamKind}:${packet.sourceId}:${packet.sequence}`;
        if (!seenFederatedPackets.accept(key)) {
            metrics.duplicateFederationDrops += 1;
            return false;
        }
        return true;
    };

    const latestEpochByStream = new Map<string, number>();
    socket.on("message", (raw) => {
        try {
            const packet = decodeTopologyPacket(raw);
            const plan = scenario.topologies[packet.topology];
            if (!plan) throw new Error(`Unknown topology ${packet.topology}`);
            const routeEpoch = routeEpochFor(plan, packet.epoch);
            const streamKey = `${packet.topology}:${packet.streamKind}:${packet.sourceId}`;
            const latestEpoch = latestEpochByStream.get(streamKey) ?? 0;
            if (packet.topology === "resilient-sfu" && packet.epoch < latestEpoch) {
                metrics.staleEpochDrops += 1;
                counters(packet.topology).staleEpochDrops += 1;
                return;
            }
            if (packet.topology === "resilient-sfu" && packet.epoch > latestEpoch) {
                latestEpochByStream.set(streamKey, packet.epoch);
            }
            const departedParticipants = new Set(routeEpoch.departedParticipantIds);
            if (departedParticipants.has(packet.sourceId)) {
                metrics.wrongRouteDrops += 1;
                counters(packet.topology).wrongRouteDrops += 1;
                return;
            }
            if (packet.type === "upload") {
                metrics.clientIngress += 1;
                counters(packet.topology).clientIngress += 1;
                epochCounters(packet).clientIngress += 1;
                if (
                    packet.topology === "resilient-sfu" &&
                    packet.epoch >= scenario.resilience.routeEpoch &&
                    !seenSourcePackets.accept(`${packet.epoch}:${packet.streamKind}:${packet.sourceId}:${packet.sequence}`)
                ) {
                    metrics.duplicateSourceDrops += 1;
                    return;
                }
                if (shouldInjectSyntheticBaselineDrop(
                    packet.topology,
                    packet.epoch,
                    relayId,
                    scenario.failedRelayId
                )) {
                    metrics.intentionalDrops += 1;
                    return;
                }
                if (
                    packet.topology === "resilient-sfu" &&
                    routeEpoch.sourceRelayByParticipant[packet.sourceId] !== relayId
                ) {
                    metrics.wrongRouteDrops += 1;
                    counters(packet.topology).wrongRouteDrops += 1;
                    return;
                }
                if (packet.topology === "turn-mesh") {
                    if (packet.recipientId === undefined) throw new Error("TURN mesh packet is missing recipient");
                    deliver(packet, packet.recipientId);
                    return;
                }
                const configuredSubscribers = plan.subscriptions[`${packet.streamKind}:${packet.sourceId}`] ?? [];
                const subscribers = configuredSubscribers.filter((recipient) => !departedParticipants.has(recipient));
                const suppressed = configuredSubscribers.length - subscribers.length;
                metrics.departedDeliverySuppressions += suppressed;
                counters(packet.topology).departedDeliverySuppressions += suppressed;
                const overlay = routeEpoch.relayOverlays?.[`${packet.streamKind}:${packet.sourceId}`];
                if (packet.topology === "cascaded-sfu" || (packet.topology === "resilient-sfu" && overlay)) {
                    for (const recipient of subscribers) {
                        if (routeEpoch.recipientRelayByParticipant[recipient] === relayId) deliver(packet, recipient);
                    }
                    for (const childRelay of overlay?.childrenByRelay[relayId] ?? []) {
                        enqueueFederated(packet, childRelay, () => {
                            metrics.federationEgress += 1;
                            counters(packet.topology).federationEgress += 1;
                            counters(packet.topology).sourceFederationEgress += 1;
                            epochCounters(packet).federationEgress += 1;
                        });
                    }
                    return;
                }
                const recipientsByRelay = new Map<string, number[]>();
                for (const recipient of subscribers) {
                    const targetRelay = routeEpoch.recipientRelayByParticipant[recipient];
                    if (!targetRelay) continue;
                    const recipients = recipientsByRelay.get(targetRelay) ?? [];
                    recipients.push(recipient);
                    recipientsByRelay.set(targetRelay, recipients);
                }
                for (const [targetRelay, recipients] of recipientsByRelay) {
                    if (targetRelay === relayId) {
                        for (const recipient of recipients) deliver(packet, recipient);
                    } else {
                        enqueueFederated(packet, targetRelay, () => {
                            metrics.federationEgress += 1;
                            counters(packet.topology).federationEgress += 1;
                            counters(packet.topology).sourceFederationEgress += 1;
                            epochCounters(packet).federationEgress += 1;
                        });
                    }
                }
                return;
            }
            if (packet.type === "federated") {
                if (!acceptFederatedPacket(packet)) return;
                metrics.federationIngress += 1;
                counters(packet.topology).federationIngress += 1;
                epochCounters(packet).federationIngress += 1;
                const subscribers = (plan.subscriptions[`${packet.streamKind}:${packet.sourceId}`] ?? [])
                    .filter((recipient) => !departedParticipants.has(recipient));
                for (const recipient of subscribers) {
                    if (routeEpoch.recipientRelayByParticipant[recipient] === relayId) deliver(packet, recipient);
                }
                const overlay = routeEpoch.relayOverlays?.[`${packet.streamKind}:${packet.sourceId}`];
                if (packet.topology === "cascaded-sfu" || (packet.topology === "resilient-sfu" && overlay)) {
                    for (const childRelay of overlay?.childrenByRelay[relayId] ?? []) {
                        enqueueFederated(packet, childRelay, () => {
                            metrics.federationEgress += 1;
                            counters(packet.topology).federationEgress += 1;
                            counters(packet.topology).transitFederationEgress += 1;
                            epochCounters(packet).federationEgress += 1;
                        });
                    }
                }
            }
        } catch {
            metrics.malformedPackets += 1;
        }
    });

    await new Promise<void>((resolve, reject) => {
        socket.once("error", reject);
        socket.bind(MEDIA_PORT, () => {
            socket.off("error", reject);
            setSocketBuffers(socket);
            resolve();
        });
    });
    flush();
    writeJson(path.join(READY_DIR, `${relayId}.ready`), { runId: RUN_ID, relayId, port: MEDIA_PORT });
    const flushTimer = setInterval(flush, 250);
    const close = () => {
        clearInterval(flushTimer);
        flush();
        socket.close();
        process.exit(0);
    };
    process.once("SIGTERM", close);
    process.once("SIGINT", close);
}

interface StreamPlan {
    kind: StreamKind;
    sourceId: number;
    frames: number;
}

interface StreamFrame {
    stream: StreamPlan;
    sequence: number;
}

function interleaveStreamFrames(streams: StreamPlan[]) {
    const frames: StreamFrame[] = [];
    const maximumFrames = Math.max(0, ...streams.map((stream) => stream.frames));
    for (let sequence = 0; sequence < maximumFrames; sequence += 1) {
        for (const stream of streams) {
            if (sequence < stream.frames) frames.push({ stream, sequence });
        }
    }
    return frames;
}

interface SinkTopologySnapshot {
    receivedDeliveries: number;
    duplicates: number;
    corruptPackets: number;
    unexpectedPackets: number;
    lastReceivedAt: number;
    latencies: number[];
    staleDeliveries: number;
    receivedByEpoch: Record<string, number>;
    firstReceivedAtByEpoch: Record<string, number>;
    lastReceivedAtByEpoch: Record<string, number>;
    latenciesByEpoch: Record<string, number[]>;
}

function verifyDeterministicRtpPayloadHeader(packet: TopologyPacket) {
    return packet.payload.byteLength >= 12 &&
        packet.payload[0] === 0x80 &&
        packet.payload[1] === (packet.streamKind === "audio" ? 111 : packet.streamKind === "video" ? 96 : 112) &&
        packet.payload.readUInt16BE(2) === (packet.sequence & 0xffff) &&
        packet.payload.readUInt32BE(8) === (((packet.sourceId + 1) * 2654435761) >>> 0);
}

async function runSink() {
    ensureDirs();
    await applyNetem();
    await waitForFile(SCENARIO_PATH);
    const scenario = readScenario();
    const sinkIndex = Math.max(0, Math.floor(envNumber("LOADNET_SINK_INDEX", 0)));
    if (sinkIndex >= CLIENT_SINKS) throw new Error(`Invalid client sink index ${sinkIndex}/${CLIENT_SINKS}`);
    const sockets = Array.from({ length: CLIENT_SOCKET_SHARDS }, () => dgram.createSocket("udp4"));
    const states = new Map<TopologyName, {
        received: Set<string>;
        duplicates: number;
        corruptPackets: number;
        unexpectedPackets: number;
        staleDeliveries: number;
        lastReceivedAt: number;
        latencies: number[];
        receivedByEpoch: Record<string, number>;
        firstReceivedAtByEpoch: Record<string, number>;
        lastReceivedAtByEpoch: Record<string, number>;
        latenciesByEpoch: Record<string, number[]>;
    }>();
    const subscriptionCache = new Map<string, Set<number>>();
    const expectedPayloadCache = new Map<string, Buffer>();
    let malformedPackets = 0;
    const stateFor = (topology: TopologyName) => {
        let state = states.get(topology);
        if (!state) {
            state = {
                received: new Set<string>(),
                duplicates: 0,
                corruptPackets: 0,
                unexpectedPackets: 0,
                staleDeliveries: 0,
                lastReceivedAt: 0,
                latencies: [],
                receivedByEpoch: {},
                firstReceivedAtByEpoch: {},
                lastReceivedAtByEpoch: {},
                latenciesByEpoch: {}
            };
            states.set(topology, state);
        }
        return state;
    };
    const metricPath = path.join(METRICS_DIR, `client-sink-${sinkIndex}.json`);
    const flush = () => {
        const byTopology: Record<string, SinkTopologySnapshot> = {};
        for (const [topology, state] of states) {
            byTopology[topology] = {
                receivedDeliveries: state.received.size,
                duplicates: state.duplicates,
                corruptPackets: state.corruptPackets,
                unexpectedPackets: state.unexpectedPackets,
                lastReceivedAt: state.lastReceivedAt,
                latencies: state.latencies,
                staleDeliveries: state.staleDeliveries,
                receivedByEpoch: state.receivedByEpoch,
                firstReceivedAtByEpoch: state.firstReceivedAtByEpoch,
                lastReceivedAtByEpoch: state.lastReceivedAtByEpoch,
                latenciesByEpoch: state.latenciesByEpoch
            };
        }
        writeJsonAtomic(metricPath, {
            runId: RUN_ID,
            role: "topology-client-sink",
            sinkIndex,
            sinkCount: CLIENT_SINKS,
            malformedPackets,
            byTopology
        });
    };
    const onMessage = (raw: Buffer) => {
        let packet: TopologyPacket;
        try {
            packet = decodeTopologyPacket(raw);
        } catch {
            malformedPackets += 1;
            return;
        }
        if (packet.type !== "delivery" || packet.recipientId === undefined) return;
        const state = stateFor(packet.topology);
        if (packet.recipientId % CLIENT_SINKS !== sinkIndex) {
            state.unexpectedPackets += 1;
            return;
        }
        const routeEpoch = routeEpochFor(scenario.topologies[packet.topology], packet.epoch);
        if (routeEpoch.departedParticipantIds.includes(packet.recipientId)) {
            state.staleDeliveries += 1;
            return;
        }
        const cacheKey = `${packet.topology}:${packet.streamKind}:${packet.sourceId}`;
        let subscribers = subscriptionCache.get(cacheKey);
        if (!subscribers) {
            subscribers = new Set(scenario.topologies[packet.topology]?.subscriptions[`${packet.streamKind}:${packet.sourceId}`] ?? []);
            subscriptionCache.set(cacheKey, subscribers);
        }
        if (!subscribers.has(packet.recipientId)) {
            state.unexpectedPackets += 1;
            return;
        }
        const key = deliveryKey(packet);
        if (state.received.has(key)) {
            state.duplicates += 1;
            return;
        }
        const payloadCacheKey = `${packet.streamKind}:${packet.sourceId}:${packet.sequence}:${packet.payload.byteLength}`;
        let expectedPayload = expectedPayloadCache.get(payloadCacheKey);
        if (!expectedPayload) {
            expectedPayload = deterministicRtpPayload(
                RUN_ID,
                packet.streamKind,
                packet.sourceId,
                packet.sequence,
                packet.payload.byteLength
            );
            expectedPayloadCache.set(payloadCacheKey, expectedPayload);
        }
        if (!packet.payload.equals(expectedPayload) || !verifyDeterministicRtpPayloadHeader(packet)) {
            state.corruptPackets += 1;
            return;
        }
        state.received.add(key);
        const receivedAt = Date.now();
        const epochKey = String(packet.epoch);
        const latency = Math.max(0, receivedAt - packet.sentAt);
        state.latencies.push(latency);
        (state.latenciesByEpoch[epochKey] ??= []).push(latency);
        state.receivedByEpoch[epochKey] = (state.receivedByEpoch[epochKey] ?? 0) + 1;
        state.firstReceivedAtByEpoch[epochKey] ??= receivedAt;
        state.lastReceivedAtByEpoch[epochKey] = receivedAt;
        state.lastReceivedAt = receivedAt;
    };
    for (const socket of sockets) socket.on("message", onMessage);
    await Promise.all(sockets.map((socket, socketShard) => new Promise<void>((resolve, reject) => {
        socket.once("error", reject);
        socket.bind(WORKER_PORT + socketShard, () => {
            socket.off("error", reject);
            setSocketBuffers(socket);
            resolve();
        });
    })));
    flush();
    writeJson(path.join(READY_DIR, `client-sink-${sinkIndex}.ready`), {
        runId: RUN_ID,
        sinkIndex,
        ports: sockets.map((_, socketShard) => WORKER_PORT + socketShard)
    });
    const flushTimer = setInterval(flush, 500);
    const close = () => {
        clearInterval(flushTimer);
        flush();
        for (const socket of sockets) socket.close();
        process.exit(0);
    };
    process.once("SIGTERM", close);
    process.once("SIGINT", close);
}

interface PhaseMetrics {
    topology: TopologyName;
    expectedDeliveries: number;
    receivedDeliveries: number;
    deliveryRatio: number;
    sourceUploads: number;
    plannedSourceUploads: number;
    plannedFederationCopies: number;
    duplicates: number;
    corruptPackets: number;
    unexpectedPackets: number;
    migrationRetries: number;
    redundantSourceUploads: number;
    latencyMs: { p50: number; p95: number; p99: number };
    latencyMsByEpoch: Record<string, { p50: number; p95: number; p99: number }>;
    postRecoveryLatencyP95Ms?: number;
    durationMs: number;
    expectedByEpoch: Record<string, number>;
    receivedByEpoch: Record<string, number>;
    postFailureDeliveryRatio?: number;
    failoverDeliveryRatio?: number;
    postChurnDeliveryRatio?: number;
    recoveryMs?: number;
    failureAppliedAt?: number;
    churnAppliedAt?: number;
    hardKilledRelays?: string[];
    hardClosedClientSinks?: number[];
    departedParticipants?: number;
    staleDeliveries: number;
    survivorExpectedByEpoch: Record<string, number>;
    survivorReceivedByEpoch: Record<string, number>;
    survivorDeliveryRatio?: number;
}

function streamPlansFor(scenario: TopologyLabScenario, topology: TopologyName): StreamPlan[] {
    if (
        topology === "turn-mesh" ||
        topology === "single-sfu" ||
        topology === "federated-sfu" ||
        topology === "cascaded-sfu" ||
        topology === "resilient-sfu"
    ) {
        const resilientFrames = Math.max(6, Math.floor(envNumber("LOADNET_TOPOLOGY_RESILIENCE_FRAMES", 6)));
        const mediaStreams = [
            ...scenario.mediaSources.audio.map((sourceId) => ({
                kind: "audio" as const,
                sourceId,
                frames: topology === "resilient-sfu" ? Math.max(scenario.streamFrames.audio, resilientFrames) : scenario.streamFrames.audio
            })),
            ...scenario.mediaSources.video.map((sourceId) => ({
                kind: "video" as const,
                sourceId,
                frames: topology === "resilient-sfu" ? Math.max(scenario.streamFrames.video, resilientFrames) : scenario.streamFrames.video
            }))
        ];
        if (topology !== "resilient-sfu") return mediaStreams;
        return [
            ...mediaStreams,
            ...scenario.mediaSources.game.map((sourceId) => ({
                kind: "game" as const,
                sourceId,
                frames: Math.max(scenario.streamFrames.game, resilientFrames)
            }))
        ];
    }
    return scenario.mediaSources.game.map((sourceId) => ({
        kind: "game" as const,
        sourceId,
        frames: scenario.streamFrames.game
    }));
}

interface ChaosApplied {
    id: string;
    services: string[];
    requestedAt: number;
    appliedAt: number;
}

async function requestHardKills(id: string, services: string[]) {
    if (services.length === 0) {
        const now = Date.now();
        return { id, services: [], requestedAt: now, appliedAt: now } satisfies ChaosApplied;
    }
    const requestPath = path.join(CHAOS_REQUEST_DIR, `${id}.json`);
    const appliedPath = path.join(CHAOS_APPLIED_DIR, `${id}.json`);
    writeJsonAtomic(requestPath, { id, action: "kill", services, requestedAt: Date.now() });
    await waitForFile(appliedPath, 30_000);
    return JSON.parse(fs.readFileSync(appliedPath, "utf8")) as ChaosApplied;
}

function payloadSize(kind: StreamKind) {
    if (kind === "audio") return Math.max(64, envNumber("LOADNET_TOPOLOGY_AUDIO_BYTES", 320));
    if (kind === "video") return Math.max(64, envNumber("LOADNET_TOPOLOGY_VIDEO_BYTES", 900));
    return Math.max(64, envNumber("LOADNET_TOPOLOGY_GAME_BYTES", 160));
}

async function runWorker() {
    ensureDirs();
    await applyNetem();
    await waitForFile(SCENARIO_PATH);
    const scenario = readScenario();
    for (const relayId of scenario.relayIds) await waitForFile(path.join(READY_DIR, `${relayId}.ready`));
    for (let sinkIndex = 0; sinkIndex < CLIENT_SINKS; sinkIndex += 1) {
        await waitForFile(path.join(READY_DIR, `client-sink-${sinkIndex}.ready`));
    }
    const outboundSocket = dgram.createSocket("udp4");
    const relayAddresses = await resolveHostAddresses(scenario.relayIds);
    await new Promise<void>((resolve, reject) => {
        outboundSocket.once("error", reject);
        outboundSocket.bind(0, () => {
            outboundSocket.off("error", reject);
            setSocketBuffers(outboundSocket);
            resolve();
        });
    });

    const readSinkAggregate = (topology: TopologyName) => {
        const aggregate = {
            receivedDeliveries: 0,
            duplicates: 0,
            corruptPackets: 0,
            unexpectedPackets: 0,
            staleDeliveries: 0,
            malformedPackets: 0,
            lastReceivedAt: 0,
            latencies: [] as number[],
            receivedByEpoch: {} as Record<string, number>,
            firstReceivedAtByEpoch: {} as Record<string, number>,
            lastReceivedAtByEpoch: {} as Record<string, number>,
            latenciesByEpoch: {} as Record<string, number[]>,
            survivorReceivedByEpoch: {} as Record<string, number>
        };
        for (let sinkIndex = 0; sinkIndex < CLIENT_SINKS; sinkIndex += 1) {
            const metricPath = path.join(METRICS_DIR, `client-sink-${sinkIndex}.json`);
            try {
                const metric = JSON.parse(fs.readFileSync(metricPath, "utf8"));
                const state = metric.byTopology?.[topology] as SinkTopologySnapshot | undefined;
                aggregate.malformedPackets += Number(metric.malformedPackets || 0);
                if (!state) continue;
                aggregate.receivedDeliveries += state.receivedDeliveries;
                aggregate.duplicates += state.duplicates;
                aggregate.corruptPackets += state.corruptPackets;
                aggregate.unexpectedPackets += state.unexpectedPackets;
                aggregate.staleDeliveries += state.staleDeliveries ?? 0;
                aggregate.lastReceivedAt = Math.max(aggregate.lastReceivedAt, state.lastReceivedAt);
                aggregate.latencies.push(...state.latencies);
                for (const [epoch, count] of Object.entries(state.receivedByEpoch ?? {})) {
                    aggregate.receivedByEpoch[epoch] = (aggregate.receivedByEpoch[epoch] ?? 0) + Number(count);
                    if (!scenario.resilience.departedSinkIndices.includes(sinkIndex)) {
                        aggregate.survivorReceivedByEpoch[epoch] =
                            (aggregate.survivorReceivedByEpoch[epoch] ?? 0) + Number(count);
                    }
                }
                for (const [epoch, receivedAt] of Object.entries(state.firstReceivedAtByEpoch ?? {})) {
                    const value = Number(receivedAt);
                    aggregate.firstReceivedAtByEpoch[epoch] = Math.min(
                        aggregate.firstReceivedAtByEpoch[epoch] ?? Number.POSITIVE_INFINITY,
                        value
                    );
                }
                for (const [epoch, receivedAt] of Object.entries(state.lastReceivedAtByEpoch ?? {})) {
                    aggregate.lastReceivedAtByEpoch[epoch] = Math.max(
                        aggregate.lastReceivedAtByEpoch[epoch] ?? 0,
                        Number(receivedAt)
                    );
                }
                for (const [epoch, latencies] of Object.entries(state.latenciesByEpoch ?? {})) {
                    (aggregate.latenciesByEpoch[epoch] ??= []).push(...(latencies as number[]));
                }
            } catch {
                // A sink may be between its atomic rename and the first topology packet.
            }
        }
        return aggregate;
    };

    let workerSendErrors = 0;
    const send = (packet: TopologyPacket, relayId: string) => new Promise<void>((resolve) => {
        outboundSocket.send(encodeTopologyPacket(packet), MEDIA_PORT, relayAddresses.get(relayId) ?? relayId, (error) => {
            if (error) workerSendErrors += 1;
            resolve();
        });
    });
    const defaultTopologies: TopologyName[] = [
        "turn-mesh",
        "single-sfu",
        "federated-sfu",
        "cascaded-sfu",
        "host-star-game",
        "spatial-sharded-game",
        "resilient-sfu"
    ];
    const requestedTopologies = (process.env.LOADNET_TOPOLOGY_PHASES || "")
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
    const topologies = requestedTopologies.length > 0
        ? defaultTopologies.filter((topology) => requestedTopologies.includes(topology))
        : defaultTopologies;
    const phaseMetrics: PhaseMetrics[] = [];
    const sendBatch = Math.max(1, envNumber("LOADNET_TOPOLOGY_WORKER_SEND_BATCH", 16));
    const observeTimeoutMs = Math.max(2000, envNumber("LOADNET_TOPOLOGY_OBSERVE_TIMEOUT_MS", 30000));
    const settleMs = Math.max(100, envNumber("LOADNET_TOPOLOGY_SETTLE_MS", 750));
    const minimumDeliveryRatio = Math.min(1, Math.max(0, envNumber("LOADNET_TOPOLOGY_MIN_DELIVERY_RATIO", 1)));
    const maximumRecoveryMs = Math.max(100, envNumber("LOADNET_TOPOLOGY_MAX_RECOVERY_MS", 1_500));
    const maximumPostRecoveryP95Ms = Math.max(1, envNumber("LOADNET_TOPOLOGY_MAX_RECOVERED_P95_MS", 1500));
    const epochDrainTimeoutMs = Math.max(1000, envNumber("LOADNET_TOPOLOGY_EPOCH_DRAIN_TIMEOUT_MS", 7_500));
    const epochQuietMs = Math.max(500, envNumber("LOADNET_TOPOLOGY_EPOCH_QUIET_MS", 650));
    const frameIntervalMs = Math.max(0, envNumber("LOADNET_TOPOLOGY_FRAME_INTERVAL_MS", 20));

    const waitForEpochDelivery = async (
        topology: TopologyName,
        epoch: number,
        expected: number,
        minimumRatio: number
    ) => {
        const deadline = Date.now() + epochDrainTimeoutMs;
        let aggregate = readSinkAggregate(topology);
        let previousReceived = aggregate.survivorReceivedByEpoch[String(epoch)] ?? 0;
        let lastProgressAt = Date.now();
        while (Date.now() < deadline) {
            await sleep(100);
            aggregate = readSinkAggregate(topology);
            const received = aggregate.survivorReceivedByEpoch[String(epoch)] ?? 0;
            if (received > previousReceived) {
                previousReceived = received;
                lastProgressAt = Date.now();
            }
            const ratio = expected > 0 ? received / expected : 1;
            if (ratio >= minimumRatio && Date.now() - lastProgressAt >= epochQuietMs) break;
        }
        return aggregate;
    };

    for (const topology of topologies) {
        const plan = scenario.topologies[topology];
        const streams = streamPlansFor(scenario, topology);
        const cost = routeCost(plan, streams);
        let sourceUploads = 0;
        let migrationRetries = 0;
        let redundantSourceUploads = 0;
        let sincePause = 0;
        let expectedDeliveries = 0;
        const expectedByEpoch: Record<string, number> = {};
        const survivorExpectedByEpoch: Record<string, number> = {};
        const eventualDepartures = new Set(scenario.resilience.departedParticipantIds);
        let currentEpoch = 1;
        const streamFrames = interleaveStreamFrames(streams);
        const maximumSequence = Math.max(1, ...streams.map((stream) => stream.frames));
        const failureAtSequence = Math.max(1, Math.floor(maximumSequence / 3));
        const churnAtSequence = Math.max(failureAtSequence + 1, Math.floor(maximumSequence * 2 / 3));
        let previousSequence = -1;
        let failureApplied: ChaosApplied | undefined;
        let churnApplied: ChaosApplied | undefined;
        const startedAt = Date.now();
        for (const { stream, sequence } of streamFrames) {
                if (sequence !== previousSequence) {
                    if (previousSequence >= 0 && frameIntervalMs > 0) await sleep(frameIntervalMs);
                    previousSequence = sequence;
                }
                if (topology === "resilient-sfu" && !failureApplied && sequence >= failureAtSequence) {
                    failureApplied = await requestHardKills(
                        `${RUN_ID}-relay-failure`,
                        scenario.resilience.failedRelayIds
                    );
                    await sleep(scenario.resilience.detectionMs);
                    currentEpoch = scenario.resilience.routeEpoch;
                    migrationRetries = scenario.resilience.remappedParticipants;
                }
                if (topology === "resilient-sfu" && !churnApplied && sequence >= churnAtSequence) {
                    await waitForEpochDelivery(
                        topology,
                        scenario.resilience.routeEpoch,
                        survivorExpectedByEpoch[String(scenario.resilience.routeEpoch)] ?? 0,
                        minimumDeliveryRatio
                    );
                    churnApplied = await requestHardKills(
                        `${RUN_ID}-client-churn`,
                        scenario.resilience.departedSinkIndices.map((sinkIndex) => `client-sink-${sinkIndex}`)
                    );
                    currentEpoch = scenario.resilience.churnEpoch;
                }
                const routeEpoch = routeEpochFor(plan, currentEpoch);
                const departedParticipants = new Set(routeEpoch.departedParticipantIds);
                if (departedParticipants.has(stream.sourceId)) continue;
                const subscribers = (plan.subscriptions[`${stream.kind}:${stream.sourceId}`] ?? [])
                    .filter((recipientId) => !departedParticipants.has(recipientId));
                expectedDeliveries += subscribers.length;
                expectedByEpoch[String(currentEpoch)] = (expectedByEpoch[String(currentEpoch)] ?? 0) + subscribers.length;
                const survivorDeliveries = topology === "resilient-sfu"
                    ? subscribers.filter((recipientId) => !eventualDepartures.has(recipientId)).length
                    : subscribers.length;
                survivorExpectedByEpoch[String(currentEpoch)] =
                    (survivorExpectedByEpoch[String(currentEpoch)] ?? 0) + survivorDeliveries;
                const payload = deterministicRtpPayload(RUN_ID, stream.kind, stream.sourceId, sequence, payloadSize(stream.kind));
                if (topology === "turn-mesh") {
                    for (const recipientId of subscribers) {
                        await send({
                            type: "upload",
                            topology,
                            streamKind: stream.kind,
                            epoch: currentEpoch,
                            sourceId: stream.sourceId,
                            sequence,
                            recipientId,
                            originRelayIndex: 0xffff,
                            sentAt: Date.now(),
                            payload
                        }, routeEpoch.sourceRelayByParticipant[stream.sourceId]);
                        sourceUploads += 1;
                        sincePause += 1;
                        if (sincePause >= sendBatch) {
                            sincePause = 0;
                            await sleep(1);
                        }
                    }
                    continue;
                }
                const primaryRelay = routeEpoch.sourceRelayByParticipant[stream.sourceId];
                const shouldRetryMigration = topology === "federated-sfu" &&
                    scenario.failedRelayId === primaryRelay &&
                    sequence === Math.floor(stream.frames / 2);
                const basePacket: TopologyPacket = {
                    type: "upload",
                    topology,
                    streamKind: stream.kind,
                    epoch: shouldRetryMigration ? 2 : currentEpoch,
                    sourceId: stream.sourceId,
                    sequence,
                    originRelayIndex: 0xffff,
                    sentAt: Date.now(),
                    payload
                };
                const sourceCopies = recoveryCopiesForEpoch(topology, currentEpoch, {
                    routeEpoch: scenario.resilience.routeEpoch,
                    recoveryCopies: Math.max(1, envNumber("LOADNET_TOPOLOGY_RECOVERY_SOURCE_COPIES", 2))
                });
                for (let copyIndex = 0; copyIndex < sourceCopies; copyIndex += 1) {
                    await send(basePacket, primaryRelay);
                    sourceUploads += 1;
                    if (copyIndex > 0) redundantSourceUploads += 1;
                }
                if (shouldRetryMigration) {
                    await sleep(Math.max(10, envNumber("LOADNET_TOPOLOGY_MIGRATION_DETECT_MS", 40)));
                    const primaryIndex = scenario.relayIds.indexOf(primaryRelay);
                    const backupRelay = scenario.relayIds[(primaryIndex + 1) % scenario.relayIds.length];
                    await send({ ...basePacket, epoch: 3, sentAt: Date.now() }, backupRelay);
                    sourceUploads += 1;
                    migrationRetries += 1;
                }
                sincePause += 1;
                if (sincePause >= sendBatch) {
                    sincePause = 0;
                    await sleep(1);
                }
        }

        let aggregate: ReturnType<typeof readSinkAggregate>;
        if (topology === "resilient-sfu") {
            aggregate = await waitForEpochDelivery(
                topology,
                scenario.resilience.churnEpoch,
                survivorExpectedByEpoch[String(scenario.resilience.churnEpoch)] ?? 0,
                minimumDeliveryRatio
            );
        } else {
            const deadline = Date.now() + observeTimeoutMs;
            aggregate = readSinkAggregate(topology);
            let previousReceived = aggregate.receivedDeliveries;
            let lastProgressAt = Date.now();
            while (Date.now() < deadline && aggregate.receivedDeliveries < expectedDeliveries) {
                await sleep(100);
                aggregate = readSinkAggregate(topology);
                if (aggregate.receivedDeliveries > previousReceived) {
                    previousReceived = aggregate.receivedDeliveries;
                    lastProgressAt = Date.now();
                }
                const ratio = expectedDeliveries > 0 ? aggregate.receivedDeliveries / expectedDeliveries : 1;
                if (minimumDeliveryRatio < 1 && ratio >= minimumDeliveryRatio && Date.now() - lastProgressAt >= settleMs) break;
            }
        }
        const epochDeliveryRatio = (epoch: number) => {
            const expected = survivorExpectedByEpoch[String(epoch)] ?? 0;
            const received = aggregate.survivorReceivedByEpoch[String(epoch)] ?? 0;
            return expected > 0 ? received / expected : 1;
        };
        const failoverDeliveryRatio = epochDeliveryRatio(scenario.resilience.routeEpoch);
        const postChurnDeliveryRatio = epochDeliveryRatio(scenario.resilience.churnEpoch);
        const postFailureDeliveryRatio = Math.min(failoverDeliveryRatio, postChurnDeliveryRatio);
        const latencyMsByEpoch = Object.fromEntries(Object.entries(aggregate.latenciesByEpoch).map(([epoch, latencies]) => [
            epoch,
            {
                p50: percentile(latencies, 0.5),
                p95: percentile(latencies, 0.95),
                p99: percentile(latencies, 0.99)
            }
        ]));
        const postRecoveryLatencyP95Ms = Math.max(
            latencyMsByEpoch[String(scenario.resilience.routeEpoch)]?.p95 ?? 0,
            latencyMsByEpoch[String(scenario.resilience.churnEpoch)]?.p95 ?? 0
        );
        const firstRecoveredAt = aggregate.firstReceivedAtByEpoch[String(scenario.resilience.routeEpoch)];
        const recoveryMs = topology === "resilient-sfu" && failureApplied && firstRecoveredAt
            ? Math.max(0, firstRecoveredAt - failureApplied.appliedAt)
            : undefined;
        phaseMetrics.push({
            topology,
            expectedDeliveries,
            receivedDeliveries: aggregate.receivedDeliveries,
            deliveryRatio: expectedDeliveries > 0 ? aggregate.receivedDeliveries / expectedDeliveries : 1,
            sourceUploads,
            plannedSourceUploads: cost.sourceUploads,
            plannedFederationCopies: cost.federationCopies,
            duplicates: aggregate.duplicates,
            corruptPackets: aggregate.corruptPackets + aggregate.malformedPackets,
            unexpectedPackets: aggregate.unexpectedPackets,
            migrationRetries,
            redundantSourceUploads,
            expectedByEpoch,
            receivedByEpoch: aggregate.receivedByEpoch,
            postFailureDeliveryRatio: topology === "resilient-sfu"
                ? postFailureDeliveryRatio
                : undefined,
            failoverDeliveryRatio: topology === "resilient-sfu" ? failoverDeliveryRatio : undefined,
            postChurnDeliveryRatio: topology === "resilient-sfu" ? postChurnDeliveryRatio : undefined,
            recoveryMs,
            failureAppliedAt: failureApplied?.appliedAt,
            churnAppliedAt: churnApplied?.appliedAt,
            hardKilledRelays: topology === "resilient-sfu" ? scenario.resilience.failedRelayIds : undefined,
            hardClosedClientSinks: topology === "resilient-sfu" ? scenario.resilience.departedSinkIndices : undefined,
            departedParticipants: topology === "resilient-sfu" ? scenario.resilience.departedParticipantIds.length : undefined,
            staleDeliveries: aggregate.staleDeliveries,
            survivorExpectedByEpoch,
            survivorReceivedByEpoch: aggregate.survivorReceivedByEpoch,
            survivorDeliveryRatio: topology === "resilient-sfu"
                ? Object.values(survivorExpectedByEpoch).reduce((sum, count) => sum + count, 0) > 0
                    ? Object.values(aggregate.survivorReceivedByEpoch).reduce((sum, count) => sum + count, 0) /
                        Object.values(survivorExpectedByEpoch).reduce((sum, count) => sum + count, 0)
                    : 1
                : undefined,
            latencyMs: {
                p50: percentile(aggregate.latencies, 0.5),
                p95: percentile(aggregate.latencies, 0.95),
                p99: percentile(aggregate.latencies, 0.99)
            },
            latencyMsByEpoch,
            postRecoveryLatencyP95Ms: topology === "resilient-sfu" ? postRecoveryLatencyP95Ms : undefined,
            durationMs: Date.now() - startedAt
        });
        await sleep(settleMs);
    }

    outboundSocket.close();
    const ok = workerSendErrors === 0 && phaseMetrics.every((phase) =>
        (phase.topology === "resilient-sfu" || phase.deliveryRatio >= minimumDeliveryRatio) &&
        phase.corruptPackets === 0 &&
        phase.unexpectedPackets === 0 &&
        phase.staleDeliveries === 0 &&
        (phase.topology !== "resilient-sfu" || (
            phase.postFailureDeliveryRatio !== undefined &&
            phase.postFailureDeliveryRatio >= minimumDeliveryRatio &&
            phase.recoveryMs !== undefined &&
            phase.recoveryMs <= maximumRecoveryMs &&
            phase.postRecoveryLatencyP95Ms !== undefined &&
            phase.postRecoveryLatencyP95Ms <= maximumPostRecoveryP95Ms
        ))
    );
    const summary = {
        ok,
        runId: RUN_ID,
        role: "topology-lab-worker",
        protocol: "opaque-rtp-over-udp-topology-lab-v1",
        participants: scenario.participants.length,
        relays: scenario.relayIds.length,
        clientSinks: CLIENT_SINKS,
        minimumDeliveryRatio,
        maximumRecoveryMs,
        maximumPostRecoveryP95Ms,
        workerSendErrors,
        phases: phaseMetrics
    };
    writeJson(path.join(METRICS_DIR, "topology-lab-worker.json"), summary);
    writeJson(path.join(DATA_DIR, "summary.json"), summary);
    if (!ok) process.exitCode = 1;
}

async function main() {
    const role = roleName();
    if (role === "relay") return runRelay();
    if (role === "sink") return runSink();
    if (role === "worker") return runWorker();
    console.log("usage: tsx loadnet/topology-lab-node.ts <relay|sink|worker>");
}

main().catch((error) => {
    console.error(error);
    process.exit(1);
});
