import { execFileSync, spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const positionalArgs = collectPositionalArgs();
const numericPositionalArgs = positionalArgs.filter((arg) => /^\d+(\.\d+)?$/.test(arg));
const npmNumericFallbacks = numericFallbacksFor([
    "users",
    "messages",
    "user-relays",
    "open-relays",
    "nat-closed-percent",
    "concurrency",
    "latency-ms",
    "jitter-ms",
    "loss-percent",
    "audio-bytes",
    "video-bytes",
    "publish-timeout-ms",
    "observe-timeout-ms"
]);
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
    const value = Number(argValue(name, npmNumericFallbacks.get(name)));
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

function commonService(image: string, dataDirVolume = "relay-topology-data:/data") {
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

function relayService(base: Record<string, any>, netemEnv: Record<string, string>, name: string, workerId: string, relayIndex: number) {
    return {
        ...base,
        command: ["relay"],
        depends_on: ["pubsub-0"],
        environment: {
            ...netemEnv,
            LOADNET_ROLE: "relay",
            LOADNET_WORKER_ID: workerId,
            LOADNET_RELAY_INDEX: String(relayIndex),
            CGP_RELAY_PORT: "7447",
            CGP_RELAY_DB: `/data/${name}`,
            CGP_RELAY_PUBSUB_URLS: "ws://pubsub-0:7600",
            CGP_PUBSUB_TOKEN: "loadnet-pubsub-token",
            CGP_RELAY_DEFAULT_PLUGINS: "0",
            CGP_RELAY_CHECKPOINT_INTERVAL_MS: "0"
        }
    };
}

function writeCompose(composeFile: string, image: string, runId: string, dataDir: string, config: Record<string, string>) {
    const base = commonService(image);
    const netemEnv = {
        LOADNET_RUN_ID: runId,
        LOADNET_DATA_DIR: "/data",
        LOADNET_WIRE_FORMAT: config.LOADNET_WIRE_FORMAT,
        CGP_CLIENT_WIRE_FORMAT: config.LOADNET_WIRE_FORMAT,
        CGP_RELAY_WIRE_FORMAT: config.LOADNET_WIRE_FORMAT,
        CGP_PUBSUB_WIRE_FORMAT: config.LOADNET_WIRE_FORMAT,
        LOADNET_LATENCY_MS: config.LOADNET_LATENCY_MS,
        LOADNET_JITTER_MS: config.LOADNET_JITTER_MS,
        LOADNET_LOSS_PERCENT: config.LOADNET_LOSS_PERCENT
    };
    const userRelays = Number(config.LOADNET_TOPOLOGY_USER_RELAYS);
    const openRelays = Number(config.LOADNET_TOPOLOGY_OPEN_RELAYS);
    const services: Record<string, any> = {
        "pubsub-0": {
            ...base,
            command: ["pubsub"],
            environment: {
                ...netemEnv,
                LOADNET_ROLE: "pubsub",
                LOADNET_WORKER_ID: "0",
                LOADNET_PUBSUB_PORT: "7600",
                LOADNET_PUBSUB_SHARD: "0",
                LOADNET_PUBSUB_SHARDS: "1",
                CGP_PUBSUB_TOKEN: "loadnet-pubsub-token",
                CGP_PUBSUB_RETAIN_DIR: "/data/pubsub-retain"
            }
        }
    };
    const userRelayRefs: string[] = [];
    const openRelayRefs: string[] = [];
    const relayServiceNames: string[] = [];
    for (let index = 0; index < userRelays; index += 1) {
        const name = `user-relay-${index}`;
        relayServiceNames.push(name);
        userRelayRefs.push(`${name}=ws://${name}:7447`);
        services[name] = relayService(base, netemEnv, name, `user-${index}`, index);
    }
    for (let index = 0; index < openRelays; index += 1) {
        const name = `open-relay-${index}`;
        relayServiceNames.push(name);
        openRelayRefs.push(`${name}=ws://${name}:7447`);
        services[name] = relayService(base, netemEnv, name, `open-${index}`, userRelays + index);
    }
    services.worker = {
        ...base,
        entrypoint: ["npx", "tsx", "loadnet/relay-topology-node.ts"],
        command: ["worker"],
        depends_on: ["pubsub-0", ...relayServiceNames],
        environment: {
            ...netemEnv,
            LOADNET_ROLE: "worker",
            LOADNET_WORKER_ID: "0",
            LOADNET_USER_RELAYS: userRelayRefs.join(","),
            LOADNET_OPEN_RELAYS: openRelayRefs.join(","),
            LOADNET_TOPOLOGY_MESSAGES: config.LOADNET_TOPOLOGY_MESSAGES,
            LOADNET_TOPOLOGY_USERS: config.LOADNET_TOPOLOGY_USERS,
            LOADNET_TOPOLOGY_NAT_CLOSED_PERCENT: config.LOADNET_TOPOLOGY_NAT_CLOSED_PERCENT,
            LOADNET_TOPOLOGY_CONCURRENCY: config.LOADNET_TOPOLOGY_CONCURRENCY,
            LOADNET_TOPOLOGY_AUDIO_BYTES: config.LOADNET_TOPOLOGY_AUDIO_BYTES,
            LOADNET_TOPOLOGY_VIDEO_BYTES: config.LOADNET_TOPOLOGY_VIDEO_BYTES,
            LOADNET_TOPOLOGY_PUBLISH_TIMEOUT_MS: config.LOADNET_TOPOLOGY_PUBLISH_TIMEOUT_MS,
            LOADNET_TOPOLOGY_OBSERVE_TIMEOUT_MS: config.LOADNET_TOPOLOGY_OBSERVE_TIMEOUT_MS
        }
    };

    const compose = {
        name: "cgp-loadnet-relay-topology",
        services,
        networks: {
            loadnet: { driver: "bridge" }
        },
        volumes: {
            "relay-topology-data": {
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
    const runId = safeName(argValue("run-id", `relay-topology-${Date.now()}`)!, "run-id");
    const composeFile = argValue("compose-file", positionalComposeArg || "loadnet/docker-compose.relay-topology.generated.yml")!;
    const image = safeImageName(argValue("image", positionalImageArg || "cgp-loadnet:local")!);
    const dataDir = path.resolve("loadnet", "run-data", "relay-topology");
    const resultsDir = path.resolve("loadnet", "results");
    const skipBuild = hasFlag("skip-build");
    const users = Math.max(2, Math.floor(numberArg("users", 64)));
    const messages = Math.max(1, Math.floor(numberArg("messages", 512)));
    const userRelays = Math.max(1, Math.floor(numberArg("user-relays", 8)));
    const openRelays = Math.max(1, Math.floor(numberArg("open-relays", 2)));
    const natClosedPercent = Math.min(100, Math.max(0, numberArg("nat-closed-percent", 25)));
    const concurrency = Math.max(1, Math.floor(numberArg("concurrency", 16)));
    const latencyMs = numberArg("latency-ms", 35);
    const jitterMs = numberArg("jitter-ms", 10);
    const lossPercent = numberArg("loss-percent", 0);
    const audioBytes = Math.max(64, Math.floor(numberArg("audio-bytes", 640)));
    const videoBytes = Math.max(64, Math.floor(numberArg("video-bytes", 4096)));
    const publishTimeoutMs = Math.max(1000, Math.floor(numberArg("publish-timeout-ms", 10000)));
    const observeTimeoutMs = Math.max(5000, Math.floor(numberArg("observe-timeout-ms", 45000)));
    const wireFormat = argValue("wire-format", "binary-v1")!;
    const waitMs = Math.max(60000, messages * 300);
    const config = {
        LOADNET_WIRE_FORMAT: wireFormat,
        LOADNET_LATENCY_MS: String(latencyMs),
        LOADNET_JITTER_MS: String(jitterMs),
        LOADNET_LOSS_PERCENT: String(lossPercent),
        LOADNET_TOPOLOGY_MESSAGES: String(messages),
        LOADNET_TOPOLOGY_USERS: String(users),
        LOADNET_TOPOLOGY_USER_RELAYS: String(userRelays),
        LOADNET_TOPOLOGY_OPEN_RELAYS: String(openRelays),
        LOADNET_TOPOLOGY_NAT_CLOSED_PERCENT: String(natClosedPercent),
        LOADNET_TOPOLOGY_CONCURRENCY: String(concurrency),
        LOADNET_TOPOLOGY_AUDIO_BYTES: String(audioBytes),
        LOADNET_TOPOLOGY_VIDEO_BYTES: String(videoBytes),
        LOADNET_TOPOLOGY_PUBLISH_TIMEOUT_MS: String(publishTimeoutMs),
        LOADNET_TOPOLOGY_OBSERVE_TIMEOUT_MS: String(observeTimeoutMs)
    };

    fs.rmSync(dataDir, { recursive: true, force: true });
    fs.mkdirSync(dataDir, { recursive: true });
    fs.mkdirSync(resultsDir, { recursive: true });
    writeCompose(composeFile, image, runId, dataDir, config);

    let exitCode = 0;
    try {
        if (!skipBuild) {
            run("docker", ["build", "--pull=false", "-t", image, "-f", "loadnet/Dockerfile", "."]);
        }
        const upArgs = ["compose", "-f", composeFile, "up", "-d", "--remove-orphans"];
        if (skipBuild) upArgs.push("--no-build");
        run("docker", upArgs);

        const workerExit = await waitContainer("cgp-loadnet-relay-topology-worker-1", waitMs);
        if (workerExit !== 0) {
            const worker = readJsonIfExists(path.join(dataDir, "metrics", "relay-topology.json"));
            logService(composeFile, "worker");
            if (worker) {
                throw new Error(`Relay topology worker failed: ${JSON.stringify(worker).slice(0, 1000)}`);
            }
            throw new Error(`Relay topology worker failed: ${workerExit}`);
        }

        const worker = readJsonIfExists(path.join(dataDir, "metrics", "relay-topology.json"));
        if (!worker) throw new Error("Relay topology metrics are missing");
        const netem = loadNetem(dataDir);
        const netemRequired = latencyMs > 0 || jitterMs > 0 || lossPercent > 0;
        const requiredNetem = netem.filter((entry: any) => entry.required === true);
        const appliedNetem = requiredNetem.filter((entry: any) => entry.applied === true);
        const expectedNetemServices = 2 + userRelays + openRelays;
        if (netemRequired && appliedNetem.length < expectedNetemServices) {
            throw new Error(`Relay topology netem applied for ${appliedNetem.length}/${expectedNetemServices} services`);
        }
        if (!worker.ok) {
            throw new Error(`Relay topology verification failed: ${JSON.stringify(worker).slice(0, 1000)}`);
        }

        const summary = {
            runId,
            profile: {
                name: "relay-topology-smoke",
                users,
                messages,
                userRelays,
                openRelays,
                natClosedPercent,
                concurrency,
                latencyMs,
                jitterMs,
                lossPercent,
                audioBytes,
                videoBytes,
                publishTimeoutMs,
                observeTimeoutMs,
                wireFormat
            },
            worker,
            netem,
            collectorComplete: true,
            collectedAt: new Date().toISOString()
        };
        const summaryPath = path.join(resultsDir, `relay-topology-summary-${Date.now()}.json`);
        fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
        console.log(JSON.stringify({
            ok: true,
            summary: path.resolve(summaryPath),
            protocol: worker.protocol,
            users: worker.users,
            messages: worker.messages,
            verifiedMessages: worker.verifiedMessages,
            natClosedUsers: worker.natClosedUsers,
            expectedOpenProxyMessages: worker.expectedOpenProxyMessages,
            observedOpenProxyMessages: worker.observedOpenProxyMessages,
            expectedOpenProxyRatio: worker.expectedOpenProxyRatio,
            observedOpenProxyRatio: worker.observedOpenProxyRatio,
            publishP99Ms: worker.publishLatencyMs?.p99,
            observedP99Ms: worker.observedLatencyMs?.p99,
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
