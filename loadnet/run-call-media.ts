import { execFileSync, spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { ensureWebTransportLoadnetCertificate } from "./webtransport-certificate.js";

const positionalArgs = collectPositionalArgs();
const numericPositionalArgs = positionalArgs.filter((arg) => /^\d+(\.\d+)?$/.test(arg));
const npmNumericFallbacks = numericFallbacksFor([
    "messages",
    "rooms",
    "observers",
    "concurrency",
    "rate-per-second",
    "latency-ms",
    "jitter-ms",
    "loss-percent",
    "publish-timeout-ms",
    "observe-timeout-ms",
    "max-publish-p99-ms",
    "max-observed-p99-ms"
]);
const numericPositionalArg = npmNumericFallbacks.get("messages") || numericPositionalArgs[0];
const positionalComposeArg = positionalArgs.find((arg) => /\.ya?ml$/i.test(arg));
const positionalImageArg = positionalArgs.find((arg) => arg !== positionalComposeArg && !/^\d+$/.test(arg));

function collectPositionalArgs() {
    const args = process.argv.slice(2);
    const positional: string[] = [];
    for (let index = 0; index < args.length; index += 1) {
        const arg = args[index];
        if (arg.startsWith("--")) {
            if (!arg.includes("=") && args[index + 1] && !args[index + 1].startsWith("--")) index += 1;
            continue;
        }
        positional.push(arg);
    }
    return positional;
}

function numericFallbacksFor(names: string[]) {
    const values = [...numericPositionalArgs];
    const fallbacks = new Map<string, string>();
    for (const name of names) {
        const npmConfigName = `npm_config_${name.replace(/-/g, "_")}`;
        if (process.env[npmConfigName] === "true" && values.length > 0) {
            fallbacks.set(name, values.shift()!);
        }
    }
    return fallbacks;
}

function argValue(name: string, fallback?: string) {
    const prefix = `--${name}=`;
    const found = process.argv.find((arg) => arg.startsWith(prefix));
    if (found) return found.slice(prefix.length);
    const index = process.argv.indexOf(`--${name}`);
    if (index >= 0 && process.argv[index + 1] && !process.argv[index + 1].startsWith("--")) {
        return process.argv[index + 1];
    }
    const npmConfigName = `npm_config_${name.replace(/-/g, "_")}`;
    const npmValue = process.env[npmConfigName];
    return npmValue && npmValue !== "true" ? npmValue : fallback;
}

function hasFlag(name: string) {
    const npmConfigName = `npm_config_${name.replace(/-/g, "_")}`;
    const npmValue = process.env[npmConfigName];
    return process.argv.includes(`--${name}`) || npmValue === "true" || npmValue === "1";
}

function numberArg(name: string, fallback: number) {
    const value = Number(argValue(name, npmNumericFallbacks.get(name) || (name === "messages" ? numericPositionalArg : undefined)));
    return Number.isFinite(value) ? value : fallback;
}

function safeName(value: string, label: string) {
    if (!/^[a-zA-Z0-9_.-]+$/.test(value)) throw new Error(`${label} may only contain letters, numbers, dot, underscore, and dash`);
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

async function sleep(ms: number) {
    await new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForFile(filePath: string, timeoutMs: number) {
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
        if (fs.existsSync(filePath)) return;
        await sleep(100);
    }
    throw new Error(`Timed out waiting for ${filePath}`);
}

function waitContainer(containerName: string, timeoutMs: number): Promise<number> {
    return new Promise((resolve, reject) => {
        const child = spawn("docker", ["wait", containerName], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
        let stdout = "";
        let stderr = "";
        let settled = false;
        const timer = setTimeout(() => {
            try {
                child.kill();
            } catch {
                // ignored
            }
            if (!settled) {
                settled = true;
                reject(new Error(`Timed out waiting for ${containerName}`));
            }
        }, timeoutMs);
        child.stdout.on("data", (chunk) => {
            stdout += chunk.toString();
        });
        child.stderr.on("data", (chunk) => {
            stderr += chunk.toString();
        });
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
            if (code !== 0) {
                reject(new Error(stderr.trim() || `docker wait exited with ${code}`));
                return;
            }
            resolve(Number(stdout.trim()));
        });
    });
}

function composePath(value: string) {
    return path.resolve(value).replace(/\\/g, "/");
}

function scalar(value: unknown): string {
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

function commonService(image: string, dataDirVolume = "call-media-data:/data") {
    return {
        image,
        cap_add: ["NET_ADMIN"],
        networks: ["loadnet"],
        volumes: [dataDirVolume],
        logging: {
            driver: "json-file",
            options: {
                "max-size": process.env.LOADNET_LOG_MAX_SIZE || "5m",
                "max-file": process.env.LOADNET_LOG_MAX_FILE || "2"
            }
        }
    };
}

function writeCompose(composeFile: string, image: string, runId: string, dataDir: string, config: Record<string, string>) {
    const base = commonService(image);
    const sharedEnv = {
        LOADNET_RUN_ID: runId,
        LOADNET_DATA_DIR: "/data",
        LOADNET_WIRE_FORMAT: config.LOADNET_WIRE_FORMAT,
        CGP_CLIENT_WIRE_FORMAT: config.LOADNET_WIRE_FORMAT,
        CGP_RELAY_WIRE_FORMAT: config.LOADNET_WIRE_FORMAT,
        CGP_PUBSUB_WIRE_FORMAT: config.LOADNET_WIRE_FORMAT,
        LOADNET_CALL_MEDIA_TRANSPORT: config.LOADNET_CALL_MEDIA_TRANSPORT,
        LOADNET_WEBTRANSPORT_CERT_HASH: config.LOADNET_WEBTRANSPORT_CERT_HASH
    };
    const clientNetemEnv = {
        ...sharedEnv,
        LOADNET_LATENCY_MS: config.LOADNET_LATENCY_MS,
        LOADNET_JITTER_MS: config.LOADNET_JITTER_MS,
        LOADNET_LOSS_PERCENT: config.LOADNET_LOSS_PERCENT
    };
    const serverNetemEnv = {
        ...sharedEnv,
        LOADNET_LATENCY_MS: config.LOADNET_NETEM_SCOPE === "all" ? config.LOADNET_LATENCY_MS : "0",
        LOADNET_JITTER_MS: config.LOADNET_NETEM_SCOPE === "all" ? config.LOADNET_JITTER_MS : "0",
        LOADNET_LOSS_PERCENT: config.LOADNET_NETEM_SCOPE === "all" ? config.LOADNET_LOSS_PERCENT : "0"
    };
    const workerEnvironment = {
        ...clientNetemEnv,
        LOADNET_CALL_MEDIA_MESSAGES: config.LOADNET_CALL_MEDIA_MESSAGES,
        LOADNET_CALL_MEDIA_ROOMS: config.LOADNET_CALL_MEDIA_ROOMS,
        LOADNET_CALL_MEDIA_OBSERVERS: config.LOADNET_CALL_MEDIA_OBSERVERS,
        LOADNET_CALL_MEDIA_RATE_PER_SECOND: config.LOADNET_CALL_MEDIA_RATE_PER_SECOND,
        LOADNET_CALL_MEDIA_DELIVERY_MODE: config.LOADNET_CALL_MEDIA_DELIVERY_MODE,
        LOADNET_CALL_MEDIA_DEADLINE_MS: config.LOADNET_CALL_MEDIA_DEADLINE_MS,
        LOADNET_CALL_MEDIA_MIN_FRESH_RATIO: config.LOADNET_CALL_MEDIA_MIN_FRESH_RATIO,
        LOADNET_CALL_MEDIA_PROFILE: config.LOADNET_CALL_MEDIA_PROFILE,
        LOADNET_CALL_MEDIA_PUBLISH_TIMEOUT_MS: config.LOADNET_CALL_MEDIA_PUBLISH_TIMEOUT_MS,
        LOADNET_CALL_MEDIA_OBSERVE_TIMEOUT_MS: config.LOADNET_CALL_MEDIA_OBSERVE_TIMEOUT_MS,
        LOADNET_OBSERVE_RELAY: config.LOADNET_OBSERVE_RELAY,
        LOADNET_OBSERVE_REALTIME_RELAY: "https://relay-1:7448/cgp/realtime"
    };
    const webTransportRelayEnvironment = config.LOADNET_CALL_MEDIA_TRANSPORT === "webtransport"
        ? {
            CGP_RELAY_WEBTRANSPORT_PORT: "7448",
            CGP_RELAY_WEBTRANSPORT_HOST: "0.0.0.0",
            CGP_RELAY_WEBTRANSPORT_CERT_PATH: "/data/webtransport-cert.pem",
            CGP_RELAY_WEBTRANSPORT_KEY_PATH: "/data/webtransport-key.pem",
            CGP_RELAY_WEBTRANSPORT_ADVERTISE_CERT_HASH: "1"
        }
        : {};
    const observerServices = Object.fromEntries(
        Array.from({ length: Number(config.LOADNET_CALL_MEDIA_OBSERVERS) }, (_, observerId) => [
            `observer-${observerId}`,
            {
                ...base,
                entrypoint: ["npx", "tsx", "loadnet/call-media-node.ts"],
                command: ["observer"],
                depends_on: ["relay-0", "relay-1"],
                environment: {
                    ...workerEnvironment,
                    LOADNET_ROLE: "observer",
                    LOADNET_WORKER_ID: String(observerId)
                }
            }
        ])
    );
    const compose = {
        name: "cgp-loadnet-call-media",
        services: {
            "pubsub-0": {
                ...base,
                command: ["pubsub"],
                environment: {
                    ...serverNetemEnv,
                    LOADNET_ROLE: "pubsub",
                    LOADNET_WORKER_ID: "0",
                    LOADNET_PUBSUB_PORT: "7600",
                    LOADNET_PUBSUB_SHARD: "0",
                    LOADNET_PUBSUB_SHARDS: "1",
                    CGP_PUBSUB_TOKEN: "loadnet-pubsub-token",
                    CGP_PUBSUB_RETAIN_DIR: "/data/pubsub-retain"
                }
            },
            "relay-0": {
                ...base,
                command: ["relay"],
                depends_on: ["pubsub-0"],
                environment: {
                    ...serverNetemEnv,
                    LOADNET_ROLE: "relay",
                    LOADNET_RELAY_INDEX: "0",
                    CGP_RELAY_PORT: "7447",
                    CGP_RELAY_DB: "/data/relay-0",
                    CGP_RELAY_PUBSUB_URLS: "ws://pubsub-0:7600",
                    CGP_PUBSUB_TOKEN: "loadnet-pubsub-token",
                    CGP_RELAY_DEFAULT_PLUGINS: "0",
                    CGP_RELAY_CHECKPOINT_INTERVAL_MS: "0",
                    ...webTransportRelayEnvironment,
                    CGP_RELAY_WEBTRANSPORT_PUBLIC_URL: "https://relay-0:7448/cgp/realtime"
                }
            },
            "relay-1": {
                ...base,
                command: ["relay"],
                depends_on: ["pubsub-0"],
                environment: {
                    ...serverNetemEnv,
                    LOADNET_ROLE: "relay",
                    LOADNET_RELAY_INDEX: "1",
                    CGP_RELAY_PORT: "7447",
                    CGP_RELAY_DB: "/data/relay-1",
                    CGP_RELAY_PUBSUB_URLS: "ws://pubsub-0:7600",
                    CGP_PUBSUB_TOKEN: "loadnet-pubsub-token",
                    CGP_RELAY_DEFAULT_PLUGINS: "0",
                    CGP_RELAY_CHECKPOINT_INTERVAL_MS: "0",
                    ...webTransportRelayEnvironment,
                    CGP_RELAY_WEBTRANSPORT_PUBLIC_URL: "https://relay-1:7448/cgp/realtime"
                }
            },
            worker: {
                ...base,
                entrypoint: ["npx", "tsx", "loadnet/call-media-node.ts"],
                command: ["worker"],
                depends_on: ["relay-0", "relay-1"],
                environment: {
                    ...clientNetemEnv,
                    LOADNET_ROLE: "worker",
                    LOADNET_WORKER_ID: "0",
                    LOADNET_RELAYS: "ws://relay-0:7447,ws://relay-1:7447",
                    LOADNET_WRITE_RELAY: "ws://relay-0:7447",
                    LOADNET_WRITE_REALTIME_RELAY: "https://relay-0:7448/cgp/realtime",
                    LOADNET_REALTIME_RELAYS: "https://relay-0:7448/cgp/realtime,https://relay-1:7448/cgp/realtime",
                    ...workerEnvironment,
                    LOADNET_CALL_MEDIA_CONCURRENCY: config.LOADNET_CALL_MEDIA_CONCURRENCY,
                }
            },
            ...observerServices
        },
        networks: {
            loadnet: { driver: "bridge" }
        },
        volumes: {
            "call-media-data": {
                driver: "local",
                driver_opts: {
                    type: "none",
                    o: "bind",
                    device: composePath(dataDir)
                }
            }
        }
    };

    fs.mkdirSync(path.dirname(path.resolve(composeFile)), { recursive: true });
    fs.writeFileSync(composeFile, toYaml(compose));
}

function readJsonIfExists(filePath: string) {
    return fs.existsSync(filePath) ? JSON.parse(fs.readFileSync(filePath, "utf8")) : undefined;
}

function loadNetem(dataDir: string) {
    const netemDir = path.join(dataDir, "netem");
    return fs.existsSync(netemDir)
        ? fs.readdirSync(netemDir).filter((name) => name.endsWith(".json")).map((name) => JSON.parse(fs.readFileSync(path.join(netemDir, name), "utf8")))
        : [];
}

function logService(composeFile: string, service: string) {
    try {
        run("docker", ["compose", "-f", composeFile, "logs", "--tail", "160", service]);
    } catch (error) {
        console.error(error);
    }
}

function stopCompose(composeFile: string) {
    try {
        run("docker", ["compose", "-f", composeFile, "down", "--remove-orphans", "--volumes", "--timeout", "5"], "pipe");
    } catch (error) {
        console.error(error);
    }
}

async function main() {
    const transport = argValue("transport", "websocket") === "webtransport"
        ? "webtransport"
        : "websocket";
    const runId = safeName(argValue("run-id", `${transport}-call-media-${Date.now()}`)!, "run-id");
    const composeFile = argValue("compose-file", positionalComposeArg || "loadnet/docker-compose.call-media.generated.yml")!;
    const image = safeImageName(argValue("image", positionalImageArg || "cgp-loadnet:local")!);
    const dataDir = path.resolve("loadnet", "run-data", `${transport}-call-media`);
    const resultsDir = path.resolve("loadnet", "results");
    const skipBuild = hasFlag("skip-build");
    const messages = Math.max(1, Math.floor(numberArg("messages", 300)));
    const rooms = Math.max(1, Math.floor(numberArg("rooms", 12)));
    const observers = Math.max(1, Math.floor(numberArg("observers", 2)));
    const concurrency = Math.max(1, Math.floor(numberArg("concurrency", 8)));
    const ratePerSecond = Math.max(0, numberArg("rate-per-second", 0));
    const deliveryMode = transport === "webtransport" ||
        argValue("delivery-mode", "exact") === "realtime"
        ? "realtime"
        : "exact";
    const netemScope = argValue("netem-scope", "clients") === "all" ? "all" : "clients";
    const mediaDeadlineMs = Math.max(100, numberArg("media-deadline-ms", 2500));
    const minFreshDeliveryRatio = Math.max(0, Math.min(1, numberArg("min-fresh-delivery-ratio", 0.95)));
    const requestedMediaProfile = argValue("media-profile", "mixed");
    const mediaProfile = requestedMediaProfile === "audio" ||
        requestedMediaProfile === "camera" ||
        requestedMediaProfile === "screen" ||
        requestedMediaProfile === "stress"
        ? requestedMediaProfile
        : "mixed";
    const latencyMs = numberArg("latency-ms", 35);
    const jitterMs = numberArg("jitter-ms", 10);
    const lossPercent = numberArg("loss-percent", 0);
    const publishTimeoutMs = Math.max(1000, numberArg("publish-timeout-ms", 10000));
    const observeTimeoutMs = Math.max(5000, numberArg("observe-timeout-ms", 30000));
    const maxPublishP99Ms = Math.max(1, numberArg("max-publish-p99-ms", 1500));
    const maxObservedP99Ms = Math.max(1, numberArg("max-observed-p99-ms", 2000));
    const wireFormat = argValue("wire-format", "binary-v1")!;
    const observeRelay = argValue("observe-relay", "ws://relay-1:7447")!;
    const relayFailureAtMs = Math.max(0, numberArg("relay-failure-at-ms", 0));
    const relayRecoveryAfterMs = Math.max(0, numberArg("relay-recovery-after-ms", 0));
    fs.rmSync(dataDir, { recursive: true, force: true });
    fs.mkdirSync(dataDir, { recursive: true });
    const webTransportCertificate = transport === "webtransport"
        ? ensureWebTransportLoadnetCertificate(dataDir)
        : undefined;
    const waitMs = Math.max(45000, messages * 200, publishTimeoutMs + observeTimeoutMs + 30000);
    const config = {
        LOADNET_WIRE_FORMAT: wireFormat,
        LOADNET_CALL_MEDIA_TRANSPORT: transport,
        LOADNET_WEBTRANSPORT_CERT_HASH: webTransportCertificate?.sha256 || "",
        LOADNET_LATENCY_MS: String(latencyMs),
        LOADNET_JITTER_MS: String(jitterMs),
        LOADNET_LOSS_PERCENT: String(lossPercent),
        LOADNET_CALL_MEDIA_MESSAGES: String(messages),
        LOADNET_CALL_MEDIA_ROOMS: String(rooms),
        LOADNET_CALL_MEDIA_OBSERVERS: String(observers),
        LOADNET_CALL_MEDIA_RATE_PER_SECOND: String(ratePerSecond),
        LOADNET_CALL_MEDIA_DELIVERY_MODE: deliveryMode,
        LOADNET_CALL_MEDIA_DEADLINE_MS: String(mediaDeadlineMs),
        LOADNET_CALL_MEDIA_MIN_FRESH_RATIO: String(minFreshDeliveryRatio),
        LOADNET_CALL_MEDIA_PROFILE: mediaProfile,
        LOADNET_NETEM_SCOPE: netemScope,
        LOADNET_CALL_MEDIA_CONCURRENCY: String(concurrency),
        LOADNET_CALL_MEDIA_PUBLISH_TIMEOUT_MS: String(publishTimeoutMs),
        LOADNET_CALL_MEDIA_OBSERVE_TIMEOUT_MS: String(observeTimeoutMs),
        LOADNET_OBSERVE_RELAY: observeRelay
    };

    fs.mkdirSync(resultsDir, { recursive: true });
    writeCompose(composeFile, image, runId, dataDir, config);

    let exitCode = 0;
    try {
        if (!skipBuild) {
            const dockerfile = transport === "webtransport"
                ? "loadnet/Dockerfile.webtransport"
                : "loadnet/Dockerfile";
            run("docker", ["build", "--pull=false", "-t", image, "-f", dockerfile, "."]);
        }
        const upArgs = ["compose", "-f", composeFile, "up", "-d", "--remove-orphans"];
        if (skipBuild) upArgs.push("--no-build");
        run("docker", upArgs);

        const relayFailure = relayFailureAtMs > 0
            ? (async () => {
                await waitForFile(
                    path.join(dataDir, "coordination", "call-media-publish-start.json"),
                    observeTimeoutMs
                );
                await sleep(relayFailureAtMs);
                run("docker", ["compose", "-f", composeFile, "stop", "--timeout", "1", "relay-0"], "pipe");
                if (relayRecoveryAfterMs > 0) {
                    await sleep(relayRecoveryAfterMs);
                    run("docker", ["compose", "-f", composeFile, "start", "relay-0"], "pipe");
                }
            })()
            : Promise.resolve();
        const workerExit = await waitContainer("cgp-loadnet-call-media-worker-1", waitMs);
        await relayFailure;
        if (workerExit !== 0) {
            const worker = readJsonIfExists(path.join(dataDir, "metrics", `${transport}-call-media.json`));
            logService(composeFile, "worker");
            if (worker) {
                const failurePath = path.join(resultsDir, `${transport}-call-media-failure-${Date.now()}.json`);
                fs.writeFileSync(failurePath, JSON.stringify({
                    runId,
                    profile: { messages, rooms, observers, concurrency, ratePerSecond, deliveryMode, netemScope, mediaProfile, mediaDeadlineMs, minFreshDeliveryRatio, latencyMs, jitterMs, lossPercent, publishTimeoutMs, observeTimeoutMs, maxPublishP99Ms, maxObservedP99Ms, wireFormat, observeRelay },
                    worker,
                    netem: loadNetem(dataDir),
                    collectedAt: new Date().toISOString()
                }, null, 2));
                console.error(`Failure metrics: ${path.resolve(failurePath)}`);
                throw new Error(`${transport} call media worker failed: ${JSON.stringify(worker).slice(0, 1000)}`);
            }
            throw new Error(`${transport} call media worker failed: ${workerExit}`);
        }

        const worker = readJsonIfExists(path.join(dataDir, "metrics", `${transport}-call-media.json`));
        if (!worker) throw new Error(`${transport} call media metrics are missing`);
        const netem = loadNetem(dataDir);
        const netemRequired = latencyMs > 0 || jitterMs > 0 || lossPercent > 0;
        const requiredNetem = netem.filter((entry: any) => entry.required === true);
        const appliedNetem = requiredNetem.filter((entry: any) => entry.applied === true);
        const expectedNetemServices = (netemScope === "all" ? 4 : 1) + observers;
        if (netemRequired && appliedNetem.length < expectedNetemServices) {
            throw new Error(`${transport} call media netem applied for ${appliedNetem.length}/${expectedNetemServices} services`);
        }
        if (!worker.ok) {
            throw new Error(`${transport} call media verification failed: ${JSON.stringify(worker).slice(0, 1000)}`);
        }

        const summary = {
            runId,
            profile: {
                name: `${transport}-call-media-smoke`,
                transport,
                relayFailureAtMs,
                relayRecoveryAfterMs,
                messages,
                rooms,
                observers,
                concurrency,
                ratePerSecond,
                deliveryMode,
                netemScope,
                mediaProfile,
                mediaDeadlineMs,
                minFreshDeliveryRatio,
                latencyMs,
                jitterMs,
                lossPercent,
                publishTimeoutMs,
                observeTimeoutMs,
                maxPublishP99Ms,
                maxObservedP99Ms,
                wireFormat,
                observeRelay
            },
            worker,
            netem,
            gates: {
                publishP99: {
                    actualMs: worker.publishLatencyMs?.p99,
                    maximumMs: maxPublishP99Ms,
                    passed: worker.publishLatencyMs?.p99 <= maxPublishP99Ms
                },
                observedP99: {
                    actualMs: worker.observedLatencyMs?.p99,
                    maximumMs: maxObservedP99Ms,
                    passed: worker.observedLatencyMs?.p99 <= maxObservedP99Ms
                }
            },
            collectorComplete: true,
            collectedAt: new Date().toISOString()
        };
        const summaryPath = path.join(resultsDir, `${transport}-call-media-summary-${Date.now()}.json`);
        fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
        if (worker.publishLatencyMs?.p99 > maxPublishP99Ms) {
            throw new Error(`${transport} call media publish p99 ${worker.publishLatencyMs.p99}ms exceeds ${maxPublishP99Ms}ms`);
        }
        if (worker.observedLatencyMs?.p99 > maxObservedP99Ms) {
            throw new Error(`${transport} call media observed p99 ${worker.observedLatencyMs.p99}ms exceeds ${maxObservedP99Ms}ms`);
        }
        console.log(JSON.stringify({
            ok: true,
            summary: path.resolve(summaryPath),
            protocol: worker.protocol,
            messages: worker.messages,
            verifiedMessages: worker.verifiedMessages,
            verifiedDeliveries: worker.verifiedDeliveries,
            expectedDeliveries: worker.expectedDeliveries,
            freshDeliveryRatio: worker.freshDeliveryRatio,
            droppedPublishes: worker.droppedPublishes,
            totalBytes: worker.totalBytes,
            audioMessages: worker.audioMessages,
            cameraMessages: worker.cameraMessages,
            screenMessages: worker.screenMessages,
            publishP99Ms: worker.publishLatencyMs?.p99,
            observedP99Ms: worker.observedLatencyMs?.p99,
            maxPublishP99Ms,
            maxObservedP99Ms,
            netemApplied: appliedNetem.length,
            netemRequired: requiredNetem.length
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
