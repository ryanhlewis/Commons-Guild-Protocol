import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

interface Finding {
    area: "audit" | "package-script" | "lockfile";
    severity: "info" | "low" | "moderate" | "high" | "critical";
    message: string;
    file?: string;
}

const root = process.cwd();
const suspiciousScriptPattern =
    /\b(curl|wget|powershell|pwsh|Invoke-WebRequest|iwr|bitsadmin|certutil|Start-Process|bash\s+-c|sh\s+-c|node\s+-e|eval|base64|FromBase64String|http:\/\/|https:\/\/)\b/i;
const lifecycleScriptNames = new Set(["preinstall", "install", "postinstall", "prepare", "prepack", "postpack"]);
const allowedResolvedHosts = new Set(["registry.npmjs.org"]);

function readJson(file: string) {
    return JSON.parse(fs.readFileSync(file, "utf8"));
}

function packageJsonFiles() {
    const files = [path.join(root, "package.json")];
    const packagesDir = path.join(root, "packages");
    if (fs.existsSync(packagesDir)) {
        for (const entry of fs.readdirSync(packagesDir, { withFileTypes: true })) {
            if (!entry.isDirectory()) continue;
            const file = path.join(packagesDir, entry.name, "package.json");
            if (fs.existsSync(file)) files.push(file);
        }
    }
    return files;
}

function scanPackageScripts(findings: Finding[]) {
    for (const file of packageJsonFiles()) {
        const pkg = readJson(file);
        const scripts = pkg.scripts ?? {};
        for (const [name, value] of Object.entries(scripts)) {
            if (typeof value !== "string") continue;
            const isLifecycle = lifecycleScriptNames.has(name);
            const looksSuspicious = suspiciousScriptPattern.test(value);
            if (isLifecycle || looksSuspicious) {
                findings.push({
                    area: "package-script",
                    severity: isLifecycle ? "high" : "moderate",
                    file: path.relative(root, file),
                    message: `${pkg.name ?? file} script "${name}" requires review: ${value}`
                });
            }
        }
    }
}

function scanLockfile(findings: Finding[]) {
    const lockfile = path.join(root, "package-lock.json");
    if (!fs.existsSync(lockfile)) return;
    const lock = readJson(lockfile);
    const packages = lock.packages ?? {};
    for (const [name, meta] of Object.entries<Record<string, any>>(packages)) {
        const resolved = meta?.resolved;
        if (typeof resolved !== "string" || !resolved.startsWith("http")) continue;
        let url: URL;
        try {
            url = new URL(resolved);
        } catch {
            findings.push({
                area: "lockfile",
                severity: "moderate",
                file: "package-lock.json",
                message: `${name || "(root)"} has an unparsable resolved URL: ${resolved}`
            });
            continue;
        }
        if (!allowedResolvedHosts.has(url.hostname)) {
            findings.push({
                area: "lockfile",
                severity: "high",
                file: "package-lock.json",
                message: `${name || "(root)"} resolves from unexpected host ${url.hostname}`
            });
        }
        if (!meta.integrity) {
            findings.push({
                area: "lockfile",
                severity: "moderate",
                file: "package-lock.json",
                message: `${name || "(root)"} has a remote tarball without an integrity hash`
            });
        }
    }
}

function scanAudit(findings: Finding[]) {
    let stdout = "";
    const npmCommand = process.platform === "win32" ? "npm.cmd" : "npm";
    try {
        stdout = execFileSync(npmCommand, ["audit", "--json"], {
            cwd: root,
            encoding: "utf8",
            shell: process.platform === "win32",
            stdio: ["ignore", "pipe", "pipe"]
        });
    } catch (error: any) {
        stdout = error?.stdout?.toString?.() ?? "";
    }
    if (!stdout.trim()) {
        findings.push({ area: "audit", severity: "high", message: "npm audit did not return JSON output" });
        return;
    }
    const audit = JSON.parse(stdout);
    const vulnerabilities = audit.metadata?.vulnerabilities ?? {};
    const total = Number(vulnerabilities.total ?? 0);
    if (total > 0) {
        const severity = vulnerabilities.critical > 0
            ? "critical"
            : vulnerabilities.high > 0
                ? "high"
                : vulnerabilities.moderate > 0
                    ? "moderate"
                    : "low";
        findings.push({
            area: "audit",
            severity,
            message: `npm audit reports ${total} vulnerable package(s): ${JSON.stringify(vulnerabilities)}`
        });
    }
}

const findings: Finding[] = [];
scanPackageScripts(findings);
scanLockfile(findings);
scanAudit(findings);

if (findings.length === 0) {
    console.log("Security scan passed: npm audit clean, package scripts clean, lockfile hosts/integrity clean.");
    process.exit(0);
}

console.error("Security scan found issues:");
for (const finding of findings) {
    const file = finding.file ? ` ${finding.file}` : "";
    console.error(`- [${finding.severity}] ${finding.area}${file}: ${finding.message}`);
}
process.exit(1);
