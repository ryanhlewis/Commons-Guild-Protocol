import { execFileSync, spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { recoveryRedundancyRatio } from "./topology/recovery-redundancy.ts";
import { buildTopologyLabScenario } from "./topology/scenario.ts";

const numericArgumentNames = [
    "users",
    "relays",
    "relay-participant-capacity",
    "audio-sources",
    "video-sources",
    "game-sources",
    "audio-top-k",
    "video-tiles",
    "audio-frames",
    "video-frames",
    "game-frames",
    "world-size",
    "cell-size",
    "interest-radius",
    "latency-ms",
    "jitter-ms",
    "loss-percent",
    "netem-limit-packets",
    "observe-timeout-ms",
    "min-delivery-ratio",
    "relay-send-batch",
    "worker-send-batch",
    "client-sinks",
    "sink-socket-shards",
    "chaos-relays",
    "chaos-client-sinks",
    "resilience-detection-ms",
    "max-recovery-ms",
    "max-recovered-p95-ms",
    "resilience-frames",
    "relay-max-in-flight",
    "recovery-federation-copies",
    "recovery-source-copies",
    "frame-interval-ms",
    "epoch-drain-timeout-ms",
    "target-relay-utilization"
];
const positionalNumbers = process.argv.slice(2).filter((arg) => /^\d+(\.\d+)?$/.test(arg));
const npmNumericFallbacks = new Map<string, string>();
for (const name of numericArgumentNames) {
    const npmValue = process.env[`npm_config_${name.replace(/-/g, "_")}`];
    if (npmValue === "true" && positionalNumbers.length > 0) npmNumericFallbacks.set(name, positionalNumbers.shift()!);
}

function argValue(name: string, fallback?: string) {
    const prefix = `--${name}=`;
    const found = process.argv.find((arg) => arg.startsWith(prefix));
    if (found) return found.slice(prefix.length);
    const index = process.argv.indexOf(`--${name}`);
    if (index >= 0 && process.argv[index + 1] && !process.argv[index + 1].startsWith("--")) return process.argv[index + 1];
    const npmValue = process.env[`npm_config_${name.replace(/-/g, "_")}`];
    return npmValue && npmValue !== "true" ? npmValue : fallback;
}

function numberArg(name: string, fallback: number) {
    const value = Number(argValue(name, npmNumericFallbacks.get(name)));
    return Number.isFinite(value) ? value : fallback;
}

function hasFlag(name: string) {
    const npmValue = process.env[`npm_config_${name.replace(/-/g, "_")}`];
    return process.argv.includes(`--${name}`) || npmValue === "true" || npmValue === "1";
}

function safeName(value: string, label: string) {
    if (!/^[a-zA-Z0-9_.-]+$/.test(value)) throw new Error(`${label} contains unsupported characters`);
    return value;
}

function safeImageName(value: string) {
    if (!/^[a-zA-Z0-9_.:/-]+$/.test(value)) throw new Error("image contains unsupported characters");
    return value;
}

function run(command: string, args: string[] = [], stdio: "inherit" | "pipe" = "inherit") {
    console.log(`> ${[command, ...args].join(" ")}`);
    const output = execFileSync(command, args, { cwd: process.cwd(), shell: false, stdio });
    return output ? output.toString().trim() : "";
}

function waitContainer(containerName: string, timeoutMs: number): Promise<number> {
    return new Promise((resolve, reject) => {
        const child = spawn("docker", ["wait", containerName], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
        let stdout = "";
        let stderr = "";
        let settled = false;
        const timer = setTimeout(() => {
            child.kill();
            if (!settled) {
                settled = true;
                reject(new Error(`Timed out waiting for ${containerName}`));
            }
        }, timeoutMs);
        child.stdout.on("data", (chunk) => stdout += chunk.toString());
        child.stderr.on("data", (chunk) => stderr += chunk.toString());
        child.once("error", (error) => {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            reject(error);
        });
        child.once("close", (code) => {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            if (code !== 0) reject(new Error(stderr.trim() || `docker wait exited with ${code}`));
            else resolve(Number(stdout.trim()));
        });
    });
}

function composePath(value: string) {
    return path.resolve(value).replace(/\\/g, "/");
}

function scalar(value: unknown) {
    if (typeof value === "number" || typeof value === "boolean") return String(value);
    return `'${String(value).replace(/'/g, "''")}'`;
}

function toYaml(value: any, indent = 0): string {
    const pad = " ".repeat(indent);
    if (Array.isArray(value)) {
        return value.map((item) => `${pad}- ${typeof item === "object" && item !== null ? `\n${toYaml(item, indent + 2)}` : scalar(item)}`).join("\n") + "\n";
    }
    if (value && typeof value === "object") {
        return Object.entries(value).map(([key, item]) => item && typeof item === "object"
            ? `${pad}${key}:\n${toYaml(item, indent + 2)}`
            : `${pad}${key}: ${scalar(item)}`).join("\n") + "\n";
    }
    return `${pad}${scalar(value)}\n`;
}

function writeCompose(
    composeFile: string,
    image: string,
    dataDir: string,
    config: Record<string, string>,
    relayIds: string[],
    clientSinks: number
) {
    const base = {
        image,
        entrypoint: ["npx", "tsx", "loadnet/topology-lab-node.ts"],
        cap_add: ["NET_ADMIN"],
        networks: ["loadnet"],
        volumes: ["topology-lab-data:/data"],
        logging: {
            driver: "json-file",
            options: {
                "max-size": process.env.LOADNET_LOG_MAX_SIZE || "5m",
                "max-file": process.env.LOADNET_LOG_MAX_FILE || "2"
            }
        }
    };
    const services: Record<string, any> = {};
    for (const [index, relayId] of relayIds.entries()) {
        services[relayId] = {
            ...base,
            command: ["relay"],
            environment: {
                ...config,
                LOADNET_ROLE: "relay",
                LOADNET_WORKER_ID: String(index),
                LOADNET_RELAY_ID: relayId
            }
        };
    }
    const sinkServiceNames: string[] = [];
    for (let sinkIndex = 0; sinkIndex < clientSinks; sinkIndex += 1) {
        const sinkName = `client-sink-${sinkIndex}`;
        sinkServiceNames.push(sinkName);
        services[sinkName] = {
            ...base,
            command: ["sink"],
            environment: {
                ...config,
                LOADNET_ROLE: "sink",
                LOADNET_WORKER_ID: String(sinkIndex),
                LOADNET_SINK_INDEX: String(sinkIndex)
            }
        };
    }
    services.worker = {
        ...base,
        command: ["worker"],
        depends_on: [...relayIds, ...sinkServiceNames],
        environment: {
            ...config,
            LOADNET_ROLE: "worker",
            LOADNET_WORKER_ID: "0"
        }
    };
    const compose = {
        name: "cgp-loadnet-topology-lab",
        services,
        networks: { loadnet: { driver: "bridge" } },
        volumes: {
            "topology-lab-data": {
                driver: "local",
                driver_opts: { type: "none", o: "bind", device: composePath(dataDir) }
            }
        }
    };
    fs.writeFileSync(composeFile, toYaml(compose));
}

function readJson(filePath: string) {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function loadNetem(dataDir: string) {
    const netemDir = path.join(dataDir, "netem");
    return fs.existsSync(netemDir)
        ? fs.readdirSync(netemDir).filter((name) => name.endsWith(".json")).map((name) => readJson(path.join(netemDir, name)))
        : [];
}

function stopCompose(composeFile: string) {
    try {
        run("docker", ["compose", "-f", composeFile, "down", "--remove-orphans", "--volumes", "--timeout", "5"], "pipe");
    } catch (error) {
        console.error(error);
    }
}

interface ChaosRequest {
    id: string;
    action: "kill";
    services: string[];
    requestedAt: number;
}

function sleep(ms: number) {
    return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

function writeJsonAtomic(filePath: string, value: unknown) {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    const temporaryPath = `${filePath}.${process.pid}.tmp`;
    fs.writeFileSync(temporaryPath, JSON.stringify(value, null, 2));
    fs.renameSync(temporaryPath, filePath);
}

async function runChaosController(dataDir: string, composeFile: string, shouldStop: () => boolean) {
    const requestDir = path.join(dataDir, "chaos", "requests");
    const appliedDir = path.join(dataDir, "chaos", "applied");
    fs.mkdirSync(requestDir, { recursive: true });
    fs.mkdirSync(appliedDir, { recursive: true });
    const processed = new Set<string>();
    const evidence: Array<ChaosRequest & { appliedAt: number }> = [];
    while (!shouldStop() || fs.readdirSync(requestDir).some((name) => name.endsWith(".json") && !processed.has(name))) {
        const requests = fs.readdirSync(requestDir).filter((name) => name.endsWith(".json") && !processed.has(name)).sort();
        for (const name of requests) {
            const request = readJson(path.join(requestDir, name)) as ChaosRequest;
            if (request.action !== "kill" || !Array.isArray(request.services) || request.services.length === 0) {
                throw new Error(`Invalid chaos request ${name}`);
            }
            for (const service of request.services) {
                if (!/^(media-relay-\d+|client-sink-\d+)$/.test(service)) {
                    throw new Error(`Unsupported chaos service ${service}`);
                }
            }
            run("docker", ["compose", "-f", composeFile, "kill", "-s", "SIGKILL", ...request.services], "pipe");
            const applied = { ...request, appliedAt: Date.now() };
            writeJsonAtomic(path.join(appliedDir, name), applied);
            evidence.push(applied);
            processed.add(name);
        }
        if (!shouldStop()) await sleep(25);
    }
    return evidence;
}

async function main() {
    const runId = safeName(argValue("run-id", `topology-lab-${Date.now()}`)!, "run-id");
    const composeFile = path.resolve(argValue("compose-file", "loadnet/docker-compose.topology-lab.generated.yml")!);
    const image = safeImageName(argValue("image", "cgp-loadnet:local")!);
    const dataDir = path.resolve("loadnet", "run-data", "topology-lab");
    const resultsDir = path.resolve("loadnet", "results");
    const users = Math.max(2, Math.min(60000, Math.floor(numberArg("users", 256))));
    const relays = Math.max(2, Math.min(32, Math.floor(numberArg("relays", 4))));
    const relayParticipantCapacity = Math.max(1, Math.floor(numberArg("relay-participant-capacity", 1000000)));
    const audioSources = Math.max(1, Math.floor(numberArg("audio-sources", 8)));
    const videoSources = Math.max(1, Math.floor(numberArg("video-sources", 8)));
    const gameSources = Math.max(1, Math.floor(numberArg("game-sources", 16)));
    const audioTopK = Math.max(1, Math.floor(numberArg("audio-top-k", 4)));
    const videoTiles = Math.max(1, Math.floor(numberArg("video-tiles", 4)));
    const audioFrames = Math.max(1, Math.floor(numberArg("audio-frames", 4)));
    const videoFrames = Math.max(1, Math.floor(numberArg("video-frames", 2)));
    const gameFrames = Math.max(1, Math.floor(numberArg("game-frames", 1)));
    const worldSize = Math.max(10, numberArg("world-size", 1000));
    const cellSize = Math.max(1, numberArg("cell-size", 100));
    const interestRadius = Math.max(1, numberArg("interest-radius", 80));
    const latencyMs = Math.max(0, numberArg("latency-ms", 20));
    const jitterMs = Math.max(0, numberArg("jitter-ms", 5));
    const lossPercent = Math.min(100, Math.max(0, numberArg("loss-percent", 0)));
    const netemLimitPackets = Math.max(1000, Math.floor(numberArg("netem-limit-packets", 100000)));
    const observeTimeoutMs = Math.max(5000, Math.floor(numberArg("observe-timeout-ms", 30000)));
    const relaySendBatch = Math.max(1, Math.floor(numberArg("relay-send-batch", 4)));
    const workerSendBatch = Math.max(1, Math.floor(numberArg("worker-send-batch", 4)));
    const clientSinks = Math.max(1, Math.min(64, Math.floor(numberArg("client-sinks", Math.min(8, users)))));
    const sinkSocketShards = Math.max(1, Math.min(64, Math.floor(numberArg("sink-socket-shards", 8))));
    const chaosRelays = Math.max(1, Math.min(relays - 1, Math.floor(numberArg("chaos-relays", Math.min(2, relays - 1)))));
    const chaosClientSinks = Math.max(0, Math.min(clientSinks - 1, Math.floor(numberArg(
        "chaos-client-sinks",
        Math.max(1, Math.floor(clientSinks * 0.125))
    ))));
    const resilienceDetectionMs = Math.max(10, Math.floor(numberArg("resilience-detection-ms", 150)));
    const maximumRecoveryMs = Math.max(100, Math.floor(numberArg("max-recovery-ms", 1500)));
    const maximumPostRecoveryP95Ms = Math.max(1, Math.floor(numberArg("max-recovered-p95-ms", 1500)));
    const resilienceFrames = Math.max(6, Math.floor(numberArg("resilience-frames", 6)));
    const relayMaxInFlight = Math.max(1, Math.floor(numberArg("relay-max-in-flight", 8)));
    const recoveryFederationCopies = Math.max(1, Math.floor(numberArg("recovery-federation-copies", 2)));
    const recoverySourceCopies = Math.max(1, Math.floor(numberArg("recovery-source-copies", 2)));
    const frameIntervalMs = Math.max(0, Math.floor(numberArg("frame-interval-ms", 20)));
    const epochDrainTimeoutMs = Math.max(1000, Math.floor(numberArg("epoch-drain-timeout-ms", 7500)));
    const minimumDeliveryRatio = numberArg(
        "min-delivery-ratio",
        lossPercent === 0 ? 1 : Math.max(0.6, Math.pow(1 - lossPercent / 100, 3) - 0.15)
    );
    const skipBuild = hasFlag("skip-build");
    const resilienceOnly = hasFlag("resilience-only");
    const targetRelayUtilization = Math.min(1, Math.max(0.1, numberArg("target-relay-utilization", 0.85)));
    const scenario = buildTopologyLabScenario({
        runId,
        users,
        relays,
        audioSources,
        videoSources,
        gameSources,
        audioTopK,
        videoTiles,
        audioFrames,
        videoFrames,
        gameFrames,
        worldSize,
        cellSize,
        interestRadius,
        relayParticipantCapacity,
        clientSinks,
        chaosClientSinks,
        chaosRelays,
        recoveryFederationCopies,
        resilienceDetectionMs,
        adaptiveRelayCohort: true,
        targetRelayUtilization
    });

    fs.rmSync(dataDir, { recursive: true, force: true });
    fs.mkdirSync(dataDir, { recursive: true });
    fs.mkdirSync(resultsDir, { recursive: true });
    fs.writeFileSync(path.join(dataDir, "topology-lab-scenario.json"), JSON.stringify(scenario));
    const config = {
        LOADNET_RUN_ID: runId,
        LOADNET_DATA_DIR: "/data",
        LOADNET_LATENCY_MS: String(latencyMs),
        LOADNET_JITTER_MS: String(jitterMs),
        LOADNET_LOSS_PERCENT: String(lossPercent),
        LOADNET_NETEM_LIMIT_PACKETS: String(netemLimitPackets),
        LOADNET_TOPOLOGY_GAME_SOURCES: String(gameSources),
        LOADNET_TOPOLOGY_OBSERVE_TIMEOUT_MS: String(observeTimeoutMs),
        LOADNET_TOPOLOGY_MIN_DELIVERY_RATIO: String(minimumDeliveryRatio),
        LOADNET_TOPOLOGY_RELAY_SEND_BATCH: String(relaySendBatch),
        LOADNET_TOPOLOGY_WORKER_SEND_BATCH: String(workerSendBatch),
        LOADNET_TOPOLOGY_CLIENT_SINKS: String(clientSinks),
        LOADNET_TOPOLOGY_SINK_SOCKET_SHARDS: String(sinkSocketShards),
        LOADNET_TOPOLOGY_MAX_RECOVERY_MS: String(maximumRecoveryMs),
        LOADNET_TOPOLOGY_MAX_RECOVERED_P95_MS: String(maximumPostRecoveryP95Ms),
        LOADNET_TOPOLOGY_RESILIENCE_FRAMES: String(resilienceFrames),
        LOADNET_TOPOLOGY_RELAY_MAX_IN_FLIGHT: String(relayMaxInFlight),
        LOADNET_TOPOLOGY_RECOVERY_FEDERATION_COPIES: String(recoveryFederationCopies),
        LOADNET_TOPOLOGY_RECOVERY_SOURCE_COPIES: String(recoverySourceCopies),
        LOADNET_TOPOLOGY_FRAME_INTERVAL_MS: String(frameIntervalMs),
        LOADNET_TOPOLOGY_EPOCH_DRAIN_TIMEOUT_MS: String(epochDrainTimeoutMs),
        LOADNET_TOPOLOGY_PHASES: resilienceOnly ? "resilient-sfu" : ""
    };
    writeCompose(composeFile, image, dataDir, config, scenario.relayIds, clientSinks);

    let exitCode = 0;
    try {
        if (!skipBuild) run("docker", ["build", "--pull=false", "-t", image, "-f", "loadnet/Dockerfile", "."]);
        const upArgs = ["compose", "-f", composeFile, "up", "-d", "--remove-orphans"];
        if (skipBuild) upArgs.push("--no-build");
        run("docker", upArgs);
        let stopChaosController = false;
        const chaosController = runChaosController(dataDir, composeFile, () => stopChaosController);
        const workerWait = waitContainer("cgp-loadnet-topology-lab-worker-1", Math.max(120000, observeTimeoutMs * 8));
        const workerExit = await Promise.race([
            workerWait,
            chaosController.then(() => { throw new Error("Chaos controller stopped before the topology worker"); })
        ]);
        stopChaosController = true;
        const chaosEvidence = await chaosController;
        await new Promise((resolve) => setTimeout(resolve, 500));
        const workerPath = path.join(dataDir, "metrics", "topology-lab-worker.json");
        if (!fs.existsSync(workerPath)) throw new Error(`Topology worker metrics are missing; exit=${workerExit}`);
        const worker = readJson(workerPath);
        const relayMetrics = scenario.relayIds.map((relayId) => readJson(path.join(dataDir, "metrics", `${relayId}.json`)));
        const sinkMetrics = Array.from({ length: clientSinks }, (_, sinkIndex) =>
            readJson(path.join(dataDir, "metrics", `client-sink-${sinkIndex}.json`))
        );
        const netem = loadNetem(dataDir);
        const appliedNetem = netem.filter((entry: any) => entry.required === true && entry.applied === true);
        const netemRequired = latencyMs > 0 || jitterMs > 0 || lossPercent > 0;
        const expectedNetemServices = scenario.relayIds.length + clientSinks + 1;
        if (netemRequired && appliedNetem.length < expectedNetemServices) {
            throw new Error(`Topology netem applied for ${appliedNetem.length}/${expectedNetemServices} services`);
        }
        if (workerExit !== 0 || !worker.ok) throw new Error(`Topology lab verification failed: ${JSON.stringify(worker).slice(0, 1600)}`);
        const phases = Object.fromEntries(worker.phases.map((phase: any) => [phase.topology, phase]));
        if (!resilienceOnly && phases["federated-sfu"].sourceUploads >= phases["turn-mesh"].sourceUploads) {
            throw new Error("Federated SFU did not reduce publisher uploads versus TURN mesh");
        }
        if (!resilienceOnly && phases["spatial-sharded-game"].expectedDeliveries >= phases["host-star-game"].expectedDeliveries) {
            throw new Error("Spatial game sharding did not reduce fanout versus host star");
        }
        if (!resilienceOnly && scenario.failedRelayId && phases["federated-sfu"].migrationRetries < 1) {
            throw new Error("Federated SFU migration path was not exercised");
        }
        const resilientPhase = phases["resilient-sfu"];
        if (!resilientPhase) throw new Error("Resilient SFU phase was not executed");
        if (resilientPhase.recoveryMs > maximumRecoveryMs) {
            throw new Error(`Resilient SFU recovery exceeded ${maximumRecoveryMs}ms: ${resilientPhase.recoveryMs}ms`);
        }
        if (resilientPhase.postFailureDeliveryRatio < minimumDeliveryRatio) {
            throw new Error("Resilient SFU post-failure delivery ratio missed its SLO");
        }
        if (resilientPhase.postRecoveryLatencyP95Ms > maximumPostRecoveryP95Ms) {
            throw new Error(
                `Resilient SFU recovered p95 exceeded ${maximumPostRecoveryP95Ms}ms: ` +
                `${resilientPhase.postRecoveryLatencyP95Ms}ms`
            );
        }
        const failedRelaySet = new Set(scenario.resilience.failedRelayIds);
        const resilientPlan = scenario.topologies["resilient-sfu"];
        const affectedParticipants = (resilientPlan.sourceRelayCandidatesByParticipant ?? [])
            .filter((candidates) => failedRelaySet.has(candidates[0])).length;
        if (scenario.resilience.remappedParticipants !== affectedParticipants) {
            throw new Error("Relay failure moved participants whose primary relay remained healthy");
        }
        if (scenario.resilience.minimumRecoveredRelayHeadroom < 0) {
            throw new Error("Recovered relay assignment exceeds declared participant capacity");
        }
        const recoveredRoute = resilientPlan.routeEpochs?.[String(scenario.resilience.routeEpoch)];
        if (!recoveredRoute) throw new Error("Resilient SFU route epoch is missing");
        const recoveredOverlays = Object.values(recoveredRoute.relayOverlays ?? {});
        const resilientArchitectureEvidence = {
            availableRelays: scenario.architecture.availableRelayCount,
            activeRelays: scenario.architecture.activeRelayCount,
            standbyRelays: scenario.architecture.standbyRelayCount,
            recoveredActiveRelays: new Set(recoveredRoute.sourceRelayByParticipant).size,
            candidateReplicas: Math.min(...(resilientPlan.sourceRelayCandidatesByParticipant ?? []).map(
                (candidates) => candidates.length
            )),
            maximumOverlayFanout: Math.max(0, ...recoveredOverlays.flatMap((overlay) =>
                Object.values(overlay.childrenByRelay).map((children) => children.length)
            )),
            maximumOverlayDepth: Math.max(0, ...recoveredOverlays.flatMap((overlay) =>
                Object.values(overlay.depthByRelay)
            )),
            logicalOverlayEdges: recoveredOverlays.reduce(
                (sum, overlay) => sum + Object.values(overlay.childrenByRelay).reduce(
                    (edgeSum, children) => edgeSum + children.length,
                    0
                ),
                0
            ),
            maximumRecoveredRelayLoad: scenario.resilience.maximumRecoveredRelayLoad,
            minimumRecoveredRelayHeadroom: scenario.resilience.minimumRecoveredRelayHeadroom,
            worstCaseSurvivorCapacity: scenario.architecture.worstCaseSurvivorCapacity,
            targetRelayUtilization: scenario.architecture.targetRelayUtilization
        };
        if (resilientArchitectureEvidence.maximumOverlayFanout > 4) {
            throw new Error("Recovered relay tree exceeded bounded source fanout");
        }
        const expectedChaosEvents = 1 + Number(scenario.resilience.departedSinkIndices.length > 0);
        if (chaosEvidence.length < expectedChaosEvents || !chaosEvidence.some((event) => event.services.some((service) => service.startsWith("media-relay-")))) {
            throw new Error("Hard relay/client process failure was not exercised");
        }
        const intentionalDrops = relayMetrics.reduce((sum: number, relay: any) => sum + relay.intentionalDrops, 0);
        if (!resilienceOnly && scenario.failedRelayId && intentionalDrops < 1) throw new Error("Relay failure injection was not observed");
        const redundantFederationEgress = relayMetrics.reduce(
            (sum: number, relay: any) => sum + Number(relay.redundantFederationEgress || 0),
            0
        );
        const duplicateFederationDrops = relayMetrics.reduce(
            (sum: number, relay: any) => sum + Number(relay.duplicateFederationDrops || 0),
            0
        );
        const duplicateSourceDrops = relayMetrics.reduce(
            (sum: number, relay: any) => sum + Number(relay.duplicateSourceDrops || 0),
            0
        );
        if (recoveryFederationCopies > 1 && (redundantFederationEgress < 1 || duplicateFederationDrops < 1)) {
            throw new Error("Repair-epoch federation redundancy was not exercised end to end");
        }
        if (recoverySourceCopies > 1 && (resilientPhase.redundantSourceUploads < 1 || duplicateSourceDrops < 1)) {
            throw new Error("Repair-epoch publisher redundancy was not exercised end to end");
        }
        const recoveryProtectionEvidence = {
            configuredCopies: recoveryFederationCopies,
            configuredSourceCopies: recoverySourceCopies,
            redundantSourceUploads: resilientPhase.redundantSourceUploads,
            duplicateSourceDrops,
            redundantFederationEgress,
            duplicateFederationDrops,
            redundancyRatio: recoveryRedundancyRatio(
                Math.max(1, resilientPhase.plannedFederationCopies),
                resilientPhase.plannedFederationCopies + redundantFederationEgress
            )
        };
        const sourceFederationEgress = (topology: "federated-sfu" | "cascaded-sfu") => relayMetrics.reduce(
            (sum: number, relay: any) => sum + Number(relay.byTopology?.[topology]?.sourceFederationEgress || 0),
            0
        );
        const transitFederationEgress = relayMetrics.reduce(
            (sum: number, relay: any) => sum + Number(relay.byTopology?.["cascaded-sfu"]?.transitFederationEgress || 0),
            0
        );
        const directSourceFederationEgress = resilienceOnly ? 0 : sourceFederationEgress("federated-sfu");
        const cascadedSourceFederationEgress = resilienceOnly ? 0 : sourceFederationEgress("cascaded-sfu");
        const cascadeDemandRelays = Math.max(0, ...Object.values(scenario.topologies["cascaded-sfu"].relayOverlays ?? {})
            .map((overlay: any) => Object.keys(overlay.depthByRelay).length));
        if (!resilienceOnly && cascadeDemandRelays > 5 && cascadedSourceFederationEgress >= directSourceFederationEgress) {
            throw new Error("Cascade did not reduce source-relay federation fanout");
        }
        const architectureEvidence = {
            cascadeDemandRelays,
            directSourceFederationEgress,
            cascadedSourceFederationEgress,
            cascadedTransitFederationEgress: transitFederationEgress
        };

        const summary = {
            runId,
            profile: {
                users,
                relays,
                relayParticipantCapacity,
                audioSources,
                videoSources,
                gameSources,
                audioTopK,
                videoTiles,
                audioFrames,
                videoFrames,
                gameFrames,
                worldSize,
                cellSize,
                interestRadius,
                latencyMs,
                jitterMs,
                lossPercent,
                netemLimitPackets,
                minimumDeliveryRatio,
                relaySendBatch,
                workerSendBatch,
                clientSinks,
                sinkSocketShards,
                chaosRelays,
                chaosClientSinks,
                resilienceDetectionMs,
                maximumRecoveryMs,
                maximumPostRecoveryP95Ms,
                resilienceFrames,
                resilienceOnly,
                relayMaxInFlight,
                recoveryFederationCopies,
                recoverySourceCopies,
                frameIntervalMs,
                epochDrainTimeoutMs,
                targetRelayUtilization
            },
            worker,
            relayMetrics,
            sinkMetrics,
            netem,
            architectureEvidence,
            resilientArchitectureEvidence,
            resilienceEvidence: scenario.resilience,
            recoveryProtectionEvidence,
            chaosEvidence,
            intentionalDrops,
            collectorComplete: true,
            collectedAt: new Date().toISOString()
        };
        const summaryPath = path.join(resultsDir, `topology-lab-summary-${Date.now()}.json`);
        fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
        console.log(JSON.stringify({
            ok: true,
            summary: path.resolve(summaryPath),
            users,
            relays,
            minimumDeliveryRatio,
            phases: worker.phases,
            intentionalDrops,
            architectureEvidence,
            resilientArchitectureEvidence,
            resilienceEvidence: scenario.resilience,
            recoveryProtectionEvidence,
            chaosEvidence,
            netemApplied: appliedNetem.length
        }, null, 2));
    } catch (error) {
        console.error(error);
        exitCode = 1;
    } finally {
        stopCompose(composeFile);
    }
    process.exit(exitCode);
}

void main();
