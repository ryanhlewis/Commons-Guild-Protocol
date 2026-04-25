import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

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

function wireFormatArg() {
    const value = argValue("wire-format", "binary-v1")!;
    if (value !== "json" && value !== "binary-json" && value !== "binary-v1" && value !== "binary-v2") {
        throw new Error("wire-format must be json, binary-json, binary-v1, or binary-v2");
    }
    return value;
}

function run(command: string, args: string[] = []) {
    console.log(`\n> ${[command, ...args].join(" ")}`);
    execFileSync(command, args, {
        cwd: process.cwd(),
        shell: false,
        stdio: "inherit"
    });
}

function runTsx(script: string, args: string[] = []) {
    run(process.execPath, [path.resolve("node_modules", "tsx", "dist", "cli.mjs"), script, ...args]);
}

const reportDir = path.resolve(argValue("report-dir", path.join(".tmp", "cgp-brutal"))!);
const storageProfile = safeName(argValue("storage-profile", "100k")!, "storage-profile");
const fanoutProfile = safeName(argValue("fanout-profile", "100k")!, "fanout-profile");
const wireFormat = wireFormatArg();
const dockerProfiles = (argValue("docker-profiles", hasFlag("with-docker") ? "1k,churn,restart,pubsub-restart,slow-consumers" : "") || "")
    .split(",")
    .map((profile) => profile.trim())
    .filter(Boolean);
for (const profile of dockerProfiles) {
    safeName(profile, "docker profile");
}

fs.mkdirSync(reportDir, { recursive: true });

runTsx("scripts/relay-load-test.ts", ["--profile", storageProfile, "--scenario", "storage", "--output", path.join(reportDir, `storage-${storageProfile}.json`)]);
runTsx("scripts/verify-load-report.ts", ["--report", path.join(reportDir, `storage-${storageProfile}.json`), "--max-rss-mb", "1024", "--min-message-append-rate", "10000"]);

runTsx("scripts/relay-load-test.ts", ["--profile", fanoutProfile, "--scenario", "fanout", "--subscribers", "1000", "--publish-messages", "1000", "--batch-size", "250", "--channels", "1", "--store", "memory", "--relays", "4", "--wire-format", wireFormat, "--fanout-parse", "count", "--output", path.join(reportDir, `fanout-${fanoutProfile}.json`)]);
runTsx("scripts/verify-load-report.ts", ["--report", path.join(reportDir, `fanout-${fanoutProfile}.json`), "--max-rss-mb", "1024", "--min-message-append-rate", "1000", "--min-fanout-delivery-rate", "10000"]);

for (const profile of dockerProfiles) {
    runTsx("loadnet/run.ts", ["--profile", profile, "--wire-format", wireFormat]);
    runTsx("loadnet/verify-summary.ts");
}

console.log(`\nCGP brutal gate passed. Report directory: ${reportDir}`);
