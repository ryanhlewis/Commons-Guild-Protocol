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

function tsxBinArgs(script: string, args: string[] = []) {
    return {
        command: process.execPath,
        args: [path.resolve("node_modules", "tsx", "dist", "cli.mjs"), script, ...args]
    };
}

function npmBinArgs(args: string[] = []) {
    const npmCli = process.env.npm_execpath;
    if (!npmCli) {
        return { command: "npm", args };
    }
    return { command: process.execPath, args: [npmCli, ...args] };
}

function run(command: string, args: string[] = []) {
    console.log(`\n> ${[command, ...args].join(" ")}`);
    execFileSync(command, args, {
        cwd: process.cwd(),
        shell: false,
        stdio: "inherit"
    });
}

const profile = safeName(argValue("profile", "smoke")!, "profile");
const scenario = safeName(argValue("scenario", "all")!, "scenario");
const reportDir = path.resolve(argValue("report-dir", path.join(".tmp", "cgp-gate"))!);
const reportPath = path.join(reportDir, `load-${profile}-${scenario}.json`);
const wireFormat = wireFormatArg();
fs.mkdirSync(reportDir, { recursive: true });

for (const args of [
    ["run", "security:scan"],
    ["run", "conformance:relay"],
    ["run", "test:fuzz"],
    ["test", "--", "--run"],
    ["run", "build", "--workspace=@cgp/core"],
    ["run", "build", "--workspace=@cgp/client"],
    ["run", "build", "--workspace=@cgp/relay"],
    ["run", "build", "--workspace=@cgp/directory"]
]) {
    const npm = npmBinArgs(args);
    run(npm.command, npm.args);
}

if (!hasFlag("skip-load")) {
    {
        const tsx = tsxBinArgs("scripts/relay-load-test.ts", ["--profile", profile, "--scenario", scenario, "--wire-format", wireFormat, "--output", reportPath]);
        run(tsx.command, tsx.args);
    }
    {
        const tsx = tsxBinArgs("scripts/verify-load-report.ts", ["--report", reportPath]);
        run(tsx.command, tsx.args);
    }
}

console.log(`\nCGP CI gate passed. Report directory: ${reportDir}`);
