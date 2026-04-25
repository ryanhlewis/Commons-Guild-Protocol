import { execFileSync, spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { normalizeProfile, type LoadnetProfile } from "./profile";

const positionalArgs = collectPositionalArgs();

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

function argValue(name: string, fallback?: string) {
    const prefix = `--${name}=`;
    const found = process.argv.find((arg) => arg.startsWith(prefix));
    if (found) return found.slice(prefix.length);
    const index = process.argv.indexOf(`--${name}`);
    return index >= 0 ? process.argv[index + 1] : fallback;
}

function hasFlag(name: string) {
    return process.argv.includes(`--${name}`);
}

function safeName(value: string, label: string) {
    if (!/^[a-zA-Z0-9_.-]+$/.test(value)) {
        throw new Error(`${label} may only contain letters, numbers, dot, underscore, and dash`);
    }
    return value;
}

function bin(name: "npm" | "docker") {
    if (process.platform === "win32" && name === "npm") {
        return `${name}.cmd`;
    }
    return name;
}

function tsxBinArgs(script: string, args: string[] = []) {
    return {
        command: process.execPath,
        args: [path.resolve("node_modules", "tsx", "dist", "cli.mjs"), script, ...args]
    };
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

function readProfile(profileName: string) {
    profileName = safeName(profileName, "profile");
    const profilePath = path.resolve("loadnet", "profiles", profileName.endsWith(".json") ? profileName : `${profileName}.json`);
    const raw = fs.existsSync(profilePath) ? JSON.parse(fs.readFileSync(profilePath, "utf8")) : { name: profileName };
    const profile = normalizeProfile(raw);
    const wireFormat = argValue("wire-format");
    if (wireFormat !== undefined) {
        if (wireFormat !== "json" && wireFormat !== "binary-json" && wireFormat !== "binary-v1" && wireFormat !== "binary-v2") {
            throw new Error("wire-format must be json, binary-json, binary-v1, or binary-v2");
        }
        profile.wireFormat = wireFormat;
    }
    return profile;
}

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

interface SummaryCandidate {
    path: string;
    mtimeMs: number;
}

function summaryCandidates() {
    const resultsDir = path.resolve("loadnet", "results");
    if (!fs.existsSync(resultsDir)) {
        return [] as SummaryCandidate[];
    }
    return fs.readdirSync(resultsDir)
        .filter((name) => /^summary-\d+\.json$/.test(name))
        .map((name) => {
            const summaryPath = path.join(resultsDir, name);
            return {
                path: summaryPath,
                mtimeMs: fs.statSync(summaryPath).mtimeMs
            };
        })
        .sort((left, right) => right.mtimeMs - left.mtimeMs);
}

function summaryRunId(summaryPath: string) {
    try {
        const parsed = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
        return typeof parsed?.runId === "string" ? parsed.runId : undefined;
    } catch {
        return undefined;
    }
}

async function waitForFreshSummary(
    runId: string,
    startedAtMs: number,
    knownPaths: Set<string>,
    timeoutMs = 15000
) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() <= deadline) {
        const match = summaryCandidates().find((candidate) => {
            if (candidate.mtimeMs + 1 < startedAtMs && knownPaths.has(candidate.path)) {
                return false;
            }
            return summaryRunId(candidate.path) === runId;
        });
        if (match) {
            return match.path;
        }
        await sleep(250);
    }
    throw new Error(`Timed out waiting for a fresh loadnet summary for runId=${runId}`);
}

function stopComposeProject(composeFile: string, projectName: string) {
    try {
        run(bin("docker"), ["compose", "-f", composeFile, "down", "--remove-orphans", "--volumes", "--timeout", "5"], "pipe");
        return;
    } catch (error) {
        console.error(error);
    }

    try {
        const containers = run(
            bin("docker"),
            ["ps", "-aq", "--filter", `label=com.docker.compose.project=${projectName}`],
            "pipe"
        )
            .split(/\s+/)
            .map((value) => value.trim())
            .filter(Boolean);
        if (containers.length > 0) {
            run(bin("docker"), ["rm", "-f", ...containers], "pipe");
        }
    } catch (error) {
        console.error(error);
    }

    try {
        run(bin("docker"), ["network", "rm", `${projectName}_loadnet`], "pipe");
    } catch {
        // ignored
    }

    try {
        run(bin("docker"), ["volume", "rm", `${projectName}_loadnet-data`], "pipe");
    } catch {
        // ignored
    }
}

function logContainerTail(containerId: string | undefined, tail = 200) {
    if (!containerId) {
        return;
    }
    try {
        run(bin("docker"), ["logs", "--tail", `${tail}`, containerId]);
    } catch (error) {
        console.error(error);
    }
}

function waitContainer(containerId: string, timeoutMs = 0): Promise<number> {
    return new Promise((resolve, reject) => {
        const child = spawn(bin("docker"), ["wait", containerId], {
            cwd: process.cwd(),
            stdio: ["ignore", "pipe", "pipe"]
        });
        let settled = false;
        let timeout: NodeJS.Timeout | null = null;

        const complete = (fn: () => void) => {
            if (settled) return;
            settled = true;
            if (timeout) {
                clearTimeout(timeout);
                timeout = null;
            }
            fn();
        };

        if (timeoutMs > 0) {
            timeout = setTimeout(() => {
                try {
                    child.kill();
                } catch {
                    // ignored
                }
                complete(() => reject(new Error(`Timed out waiting for container ${containerId} after ${timeoutMs}ms`)));
            }, timeoutMs);
        }
        let stdout = "";
        let stderr = "";
        child.stdout.on("data", (chunk) => {
            stdout += chunk.toString();
        });
        child.stderr.on("data", (chunk) => {
            stderr += chunk.toString();
        });
        child.once("error", (error) => complete(() => reject(error)));
        child.once("close", (code) => {
            complete(() => {
                if (code !== 0) {
                    reject(new Error(stderr.trim() || `docker wait exited with ${code}`));
                    return;
                }
                resolve(Number(stdout.trim()));
            });
        });
    });
}

function computeDefaultMaxRunMs(profile: LoadnetProfile) {
    const relayRestartWindowMs = Math.max(0, profile.relayRestartEveryMs * profile.relayRestartCount);
    const pubSubRestartWindowMs = Math.max(0, profile.pubSubRestartEveryMs * profile.pubSubRestartCount);
    const partitionWindowMs = Math.max(
        0,
        profile.partitionEveryMs * profile.partitionCount + profile.partitionDurationMs * profile.partitionCount
    );
    const disruptionWindowMs = Math.max(relayRestartWindowMs, pubSubRestartWindowMs, partitionWindowMs);
    return Math.max(60_000, profile.durationMs + disruptionWindowMs + 180_000);
}

async function restartRelays(profile: LoadnetProfile, composeFile: string, shouldStop: () => boolean) {
    if (profile.relayRestartEveryMs <= 0 || profile.relayRestartCount <= 0 || profile.relays <= 0) {
        return;
    }
    for (let restart = 0; restart < profile.relayRestartCount; restart += 1) {
        await sleep(profile.relayRestartEveryMs);
        if (shouldStop()) {
            return;
        }
        const relayIndex = restart % profile.relays;
        const container = run(bin("docker"), ["compose", "-f", composeFile, "ps", "-q", `relay-${relayIndex}`], "pipe");
        if (!container) {
            throw new Error(`relay-${relayIndex} container not found for restart`);
        }
        console.log(JSON.stringify({ role: "host-restarter", relay: relayIndex, restart: restart + 1, container }));
        run(bin("docker"), ["restart", container], "pipe");
    }
}

async function restartPubSub(profile: LoadnetProfile, composeFile: string, shouldStop: () => boolean) {
    if (profile.pubSubRestartEveryMs <= 0 || profile.pubSubRestartCount <= 0) {
        return;
    }
    for (let restart = 0; restart < profile.pubSubRestartCount; restart += 1) {
        await sleep(profile.pubSubRestartEveryMs);
        if (shouldStop()) {
            return;
        }
        const shard = restart % profile.pubSubShards;
        const serviceName = `pubsub-${shard}`;
        const container = run(bin("docker"), ["compose", "-f", composeFile, "ps", "-q", serviceName], "pipe");
        if (!container) {
            throw new Error(`${serviceName} container not found for restart`);
        }
        console.log(JSON.stringify({ role: "host-restarter", pubsub: true, shard, restart: restart + 1, container }));
        run(bin("docker"), ["restart", container], "pipe");
    }
}

async function partitionRelays(
    profile: LoadnetProfile,
    composeFile: string,
    networkName: string,
    shouldStop: () => boolean
) {
    if (
        profile.partitionEveryMs <= 0 ||
        profile.partitionDurationMs <= 0 ||
        profile.partitionCount <= 0 ||
        profile.relays <= 1
    ) {
        return;
    }

    const span = Math.min(profile.relays, Math.max(1, profile.partitionRelaySpan));
    for (let partition = 0; partition < profile.partitionCount; partition += 1) {
        await sleep(profile.partitionEveryMs);
        if (shouldStop()) {
            return;
        }

        const start = partition % profile.relays;
        const relayIndexes = Array.from({ length: span }, (_, offset) => (start + offset) % profile.relays);
        const targets = relayIndexes
            .map((relayIndex) => {
                const container = run(bin("docker"), ["compose", "-f", composeFile, "ps", "-q", `relay-${relayIndex}`], "pipe");
                return container ? { relayIndex, container } : null;
            })
            .filter((entry): entry is { relayIndex: number; container: string } => Boolean(entry));

        if (targets.length === 0) {
            continue;
        }

        console.log(
            JSON.stringify({
                role: "host-partitioner",
                partition: partition + 1,
                network: networkName,
                relays: targets.map((target) => target.relayIndex)
            })
        );

        for (const target of targets) {
            try {
                run(bin("docker"), ["network", "disconnect", networkName, target.container], "pipe");
            } catch (error) {
                console.error(`Failed to disconnect relay-${target.relayIndex} from ${networkName}`, error);
            }
        }

        await sleep(profile.partitionDurationMs);
        if (shouldStop()) {
            return;
        }

        for (const target of targets) {
            try {
                run(
                    bin("docker"),
                    ["network", "connect", "--alias", `relay-${target.relayIndex}`, networkName, target.container],
                    "pipe"
                );
            } catch (error) {
                console.error(`Failed to reconnect relay-${target.relayIndex} to ${networkName}`, error);
            }
        }
    }
}

function compactRelayStores(profile: LoadnetProfile, composeFile: string) {
    for (let relay = 0; relay < profile.relays; relay += 1) {
        run(bin("docker"), ["compose", "-f", composeFile, "stop", `relay-${relay}`]);
        run(bin("docker"), [
            "compose",
            "-f",
            composeFile,
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "node",
            `relay-${relay}`,
            "node_modules/tsx/dist/cli.mjs",
            "scripts/relay-ops.ts",
            "compact-db",
            "--db",
            `/data/relay-${relay}`,
            "--verify"
        ]);
    }
}

async function main() {
    const profileName = argValue("profile", positionalArgs[0] ?? "smoke")!;
    const profile = readProfile(profileName);
    const skipBuild = hasFlag("skip-build");
    const wireFormat = argValue("wire-format");
    const composeFile = argValue("compose-file", positionalArgs[1] ?? "loadnet/docker-compose.generated.yml")!;
    const image = argValue("image", positionalArgs[2] ?? "cgp-loadnet:local")!;
    const maxRunMs = Math.max(0, Number(argValue("max-run-ms", positionalArgs[3] ?? `${computeDefaultMaxRunMs(profile)}`) ?? 0));
    const compactAfterRun = hasFlag("compact-after-run");
    const projectName = `cgp-loadnet-${profile.name}`;
    const networkName = `${projectName}_loadnet`;
    const runId = safeName(argValue("run-id", process.env.LOADNET_RUN_ID || `${profile.name}-${Date.now()}`)!, "run-id");
    const summariesBeforeRun = new Set(summaryCandidates().map((candidate) => candidate.path));
    const startedAtMs = Date.now();

    let exitCode = 0;
    let stopRestarter = false;
    let restarterError: unknown;
    let restarters: Promise<void>[] = [];
    let collector = "";

    const stopOnSignal = () => {
        stopRestarter = true;
    };
    process.once("SIGINT", stopOnSignal);
    process.once("SIGTERM", stopOnSignal);

    try {
        {
            const composeArgs = ["--profile", profileName, "--output", composeFile, "--image", image, "--run-id", runId];
            if (wireFormat !== undefined) {
                composeArgs.push("--wire-format", wireFormat);
            }
            const tsx = tsxBinArgs("loadnet/generate-compose.ts", composeArgs);
            run(tsx.command, tsx.args);
        }
        if (!skipBuild) {
            run(bin("docker"), ["build", "--pull=false", "-t", image, "-f", "loadnet/Dockerfile", "."]);
        }
        const composeUpArgs = ["compose", "-f", composeFile, "up", "-d", "--remove-orphans"];
        if (skipBuild) {
            composeUpArgs.push("--no-build");
        }
        run(bin("docker"), composeUpArgs);
        restarters = [
            restartRelays(profile, composeFile, () => stopRestarter),
            restartPubSub(profile, composeFile, () => stopRestarter),
            partitionRelays(profile, composeFile, networkName, () => stopRestarter)
        ].map((task) => task.catch((error) => {
            restarterError = error;
        }));
        collector = run(bin("docker"), ["compose", "-f", composeFile, "ps", "-q", "collector"], "pipe");
        if (!collector) {
            throw new Error("collector container not found");
        }
        const collectorExit = await waitContainer(collector, maxRunMs);
        stopRestarter = true;
        await Promise.all(restarters);
        if (restarterError) throw restarterError;
        if (collectorExit !== 0) {
            logContainerTail(collector);
            exitCode = collectorExit;
        } else {
            const summaryPath = await waitForFreshSummary(runId, startedAtMs, summariesBeforeRun);
            console.log(JSON.stringify({ role: "host-runner", runId, summaryPath }));
            const tsx = tsxBinArgs("loadnet/verify-summary.ts", ["--summary", summaryPath]);
            run(tsx.command, tsx.args);
            if (compactAfterRun) {
                compactRelayStores(profile, composeFile);
            }
        }
    } catch (error) {
        console.error(error);
        logContainerTail(collector);
        exitCode = exitCode || 1;
    } finally {
        process.removeListener("SIGINT", stopOnSignal);
        process.removeListener("SIGTERM", stopOnSignal);
        stopRestarter = true;
        if (restarters.length > 0) {
            await Promise.all(restarters.map((restarter) => restarter.catch((error) => {
                console.error(error);
                exitCode = exitCode || 1;
            })));
        }
        try {
            stopComposeProject(composeFile, projectName);
        } catch (error) {
            console.error(error);
            exitCode = exitCode || 1;
        }
    }

    process.exit(exitCode);
}

void main();
