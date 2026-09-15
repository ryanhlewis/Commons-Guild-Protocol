import { execFileSync, spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const positionalArgs = collectPositionalArgs();
const numericPositionalArgs = positionalArgs.filter((arg) => /^\d+(\.\d+)?$/.test(arg));
const npmNumericFallbacks = numericFallbacksFor([
    "duration-ms",
    "frame-ms",
    "sample-rate",
    "frequency-hz",
    "latency-ms",
    "jitter-ms",
    "loss-percent"
]);
const numericPositionalArg = npmNumericFallbacks.get("duration-ms") || numericPositionalArgs[0];
const positionalComposeArg = positionalArgs.find((arg) => /\.ya?ml$/i.test(arg));
const positionalImageArg = positionalArgs.find((arg) => arg !== positionalComposeArg && !/^\d+$/.test(arg));

function collectPositionalArgs() {
    const args = process.argv.slice(2);
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
    const value = Number(argValue(name, npmNumericFallbacks.get(name) || (name === "duration-ms" ? numericPositionalArg : undefined)));
    return Number.isFinite(value) ? value : fallback;
}

function safeName(value: string, label: string) {
    if (!/^[a-zA-Z0-9_.-]+$/.test(value)) {
        throw new Error(`${label} may only contain letters, numbers, dot, underscore, and dash`);
    }
    return value;
}

function safeImageName(value: string) {
    if (!/^[a-zA-Z0-9_.:/-]+$/.test(value)) {
        throw new Error("image contains unsupported characters");
    }
    return value;
}

function bin(name: "docker") {
    return name;
}

function run(command: string, args: string[] = [], stdio: "inherit" | "pipe" = "inherit") {
    console.log(`> ${[command, ...args].join(" ")}`);
    const output = execFileSync(command, args, {
        cwd: process.cwd(),
        shell: false,
        stdio
    });
    return output ? output.toString().trim() : "";
}

function waitContainer(containerName: string, timeoutMs: number): Promise<number> {
    return new Promise((resolve, reject) => {
        const child = spawn(bin("docker"), ["wait", containerName], {
            cwd: process.cwd(),
            stdio: ["ignore", "pipe", "pipe"]
        });
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
        return Object.entries(value).map(([key, item]) => {
            if (item && typeof item === "object") {
                return `${pad}${key}:\n${toYaml(item, indent + 2)}`;
            }
            return `${pad}${key}: ${scalar(item)}`;
        }).join("\n") + "\n";
    }
    return `${pad}${scalar(value)}\n`;
}

function writeCompose(composeFile: string, image: string, runId: string, dataDir: string, config: Record<string, string>) {
    const serviceBase = {
        image,
        entrypoint: ["npx", "tsx", "loadnet/rtp-audio-node.ts"],
        cap_add: ["NET_ADMIN"],
        networks: ["loadnet"],
        volumes: ["rtp-audio-data:/data"],
        logging: {
            driver: "json-file",
            options: {
                "max-size": process.env.LOADNET_LOG_MAX_SIZE || "5m",
                "max-file": process.env.LOADNET_LOG_MAX_FILE || "2"
            }
        }
    };
    const environment = {
        LOADNET_RUN_ID: runId,
        LOADNET_DATA_DIR: "/data",
        ...config
    };
    const compose = {
        name: "cgp-loadnet-rtp-audio",
        services: {
            receiver: {
                ...serviceBase,
                command: ["receiver"],
                environment: {
                    ...environment,
                    LOADNET_ROLE: "receiver",
                    LOADNET_WORKER_ID: "0"
                }
            },
            sender: {
                ...serviceBase,
                command: ["sender"],
                depends_on: ["receiver"],
                environment: {
                    ...environment,
                    LOADNET_ROLE: "sender",
                    LOADNET_WORKER_ID: "0",
                    LOADNET_RTP_AUDIO_TARGET_HOST: "receiver"
                }
            }
        },
        networks: {
            loadnet: {
                driver: "bridge"
            }
        },
        volumes: {
            "rtp-audio-data": {
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
        ? fs.readdirSync(netemDir)
            .filter((name) => name.endsWith(".json"))
            .map((name) => JSON.parse(fs.readFileSync(path.join(netemDir, name), "utf8")))
        : [];
}

function logService(composeFile: string, service: string) {
    try {
        run(bin("docker"), ["compose", "-f", composeFile, "logs", "--tail", "120", service]);
    } catch (error) {
        console.error(error);
    }
}

function stopCompose(composeFile: string) {
    try {
        run(bin("docker"), ["compose", "-f", composeFile, "down", "--remove-orphans", "--volumes", "--timeout", "5"], "pipe");
    } catch (error) {
        console.error(error);
    }
}

async function main() {
    const runId = safeName(argValue("run-id", `rtp-audio-${Date.now()}`)!, "run-id");
    const composeFile = argValue("compose-file", positionalComposeArg || "loadnet/docker-compose.rtp-audio.generated.yml")!;
    const image = safeImageName(argValue("image", positionalImageArg || "cgp-loadnet:local")!);
    const dataDir = path.resolve("loadnet", "run-data", "rtp-audio");
    const resultsDir = path.resolve("loadnet", "results");
    const skipBuild = hasFlag("skip-build");
    const durationMs = numberArg("duration-ms", 800);
    const frameMs = numberArg("frame-ms", 20);
    const sampleRate = numberArg("sample-rate", 48000);
    const frequencyHz = numberArg("frequency-hz", 997);
    const latencyMs = numberArg("latency-ms", 35);
    const jitterMs = numberArg("jitter-ms", 10);
    const lossPercent = numberArg("loss-percent", 0);
    const waitMs = Math.max(30000, durationMs + 20000);
    const config = {
        LOADNET_RTP_AUDIO_DURATION_MS: String(durationMs),
        LOADNET_RTP_AUDIO_FRAME_MS: String(frameMs),
        LOADNET_RTP_AUDIO_SAMPLE_RATE: String(sampleRate),
        LOADNET_RTP_AUDIO_FREQUENCY_HZ: String(frequencyHz),
        LOADNET_LATENCY_MS: String(latencyMs),
        LOADNET_JITTER_MS: String(jitterMs),
        LOADNET_LOSS_PERCENT: String(lossPercent)
    };

    fs.rmSync(dataDir, { recursive: true, force: true });
    fs.mkdirSync(dataDir, { recursive: true });
    fs.mkdirSync(resultsDir, { recursive: true });
    writeCompose(composeFile, image, runId, dataDir, config);

    let exitCode = 0;
    try {
        if (!skipBuild) {
            run(bin("docker"), ["build", "--pull=false", "-t", image, "-f", "loadnet/Dockerfile", "."]);
        }
        const upArgs = ["compose", "-f", composeFile, "up", "-d", "--remove-orphans"];
        if (skipBuild) upArgs.push("--no-build");
        run(bin("docker"), upArgs);

        const receiverExit = await waitContainer("cgp-loadnet-rtp-audio-receiver-1", waitMs);
        const senderExit = await waitContainer("cgp-loadnet-rtp-audio-sender-1", waitMs);
        if (receiverExit !== 0 || senderExit !== 0) {
            logService(composeFile, "receiver");
            logService(composeFile, "sender");
            throw new Error(`RTP audio loadnet containers failed: receiver=${receiverExit}, sender=${senderExit}`);
        }

        const receiver = readJsonIfExists(path.join(dataDir, "metrics", "receiver.json"));
        const sender = readJsonIfExists(path.join(dataDir, "metrics", "sender.json"));
        if (!receiver || !sender) {
            throw new Error("RTP audio loadnet metrics are missing");
        }
        const netem = loadNetem(dataDir);
        const netemRequired = latencyMs > 0 || jitterMs > 0 || lossPercent > 0;
        const requiredNetem = netem.filter((entry: any) => entry.required === true);
        const appliedNetem = requiredNetem.filter((entry: any) => entry.applied === true);
        if (netemRequired && appliedNetem.length < 2) {
            throw new Error(`RTP audio netem applied for ${appliedNetem.length}/2 services`);
        }
        if (!receiver.ok) {
            throw new Error(`RTP audio verification failed: ${JSON.stringify(receiver).slice(0, 1000)}`);
        }

        const summary = {
            runId,
            profile: {
                name: "rtp-audio-smoke",
                durationMs,
                frameMs,
                sampleRate,
                frequencyHz,
                latencyMs,
                jitterMs,
                lossPercent
            },
            receiver,
            sender,
            netem,
            collectorComplete: true,
            collectedAt: new Date().toISOString()
        };
        const summaryPath = path.join(resultsDir, `rtp-audio-summary-${Date.now()}.json`);
        fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
        console.log(JSON.stringify({
            ok: true,
            summary: path.resolve(summaryPath),
            protocol: receiver.protocol,
            sentFrames: sender.sentFrames,
            receivedFrames: receiver.receivedFrames,
            expectedFrames: receiver.expectedFrames,
            receivedSha256: receiver.receivedSha256,
            estimatedFrequencyHz: receiver.estimatedFrequencyHz,
            rms: receiver.rms,
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
