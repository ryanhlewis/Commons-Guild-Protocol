import { execFileSync, spawn } from "node:child_process";
import { createServer } from "node:http";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { createSign, randomBytes } from "node:crypto";

type Args = {
    owner: string;
    repo: string;
    port: number;
    createRepo: boolean;
    publicRepo: boolean;
    outDir: string;
    publicUrl: string;
};

function printHelp() {
    console.log(`Usage:
  npm run github:backup-app -- -- [options]

Options:
  --owner <login-or-org>     GitHub owner for the backup repository. Defaults to gh auth user.
  --repo <name>              Backup repository name. Defaults to hollow-backup.
  --create-repo              Create the repository with gh if it does not exist.
  --public                   Create the repository as public. Default is private.
  --port <port>              Local callback port. Defaults to 17876.
  --public-url <url>         Public tunnel URL that forwards to this helper.
  --out-dir <path>           Output directory for app credentials. Defaults to .github-app.
  --help                     Show this help.

Example:
  npm run github:backup-app -- -- --repo hollow-backup --create-repo
`);
}

function parseArgs(): Args {
    const args = process.argv.slice(2);
    if (args.includes("--help") || args.includes("-h")) {
        printHelp();
        process.exit(0);
    }
    const read = (name: string) => {
        const prefixed = args.find((arg) => arg.startsWith(`--${name}=`));
        if (prefixed) return prefixed.slice(name.length + 3);
        const index = args.indexOf(`--${name}`);
        return index >= 0 ? args[index + 1] || "" : "";
    };
    const readNpmConfig = (name: string) =>
        process.env[`npm_config_${name.replace(/-/g, "_")}`] || "";
    const readBoolean = (name: string) =>
        args.includes(`--${name}`) ||
        /^(1|true|yes)$/i.test(readNpmConfig(name));
    const positionalRepo = args.find((arg) => !arg.startsWith("-")) || "";
    return {
        owner: read("owner") || readNpmConfig("owner"),
        repo: read("repo") || readNpmConfig("repo") || positionalRepo || "hollow-backup",
        port: Number(read("port") || readNpmConfig("port") || 17876),
        createRepo: readBoolean("create-repo"),
        publicRepo: readBoolean("public"),
        outDir: path.resolve(read("out-dir") || readNpmConfig("out-dir") || ".github-app"),
        publicUrl: normalizePublicUrl(read("public-url") || readNpmConfig("public-url") || process.env.CGP_GITHUB_SETUP_PUBLIC_URL || ""),
    };
}

function normalizePublicUrl(value: string) {
    const trimmed = value.trim().replace(/\/+$/, "");
    if (!trimmed) return "";
    const parsed = new URL(trimmed);
    if (parsed.protocol !== "https:") {
        throw new Error("--public-url must be an https URL.");
    }
    return parsed.toString().replace(/\/+$/, "");
}

function ghJson(args: string[]) {
    return JSON.parse(execFileSync("gh", args, { encoding: "utf8" }));
}

function ghText(args: string[]) {
    return execFileSync("gh", args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
}

function ensureRepo(owner: string, repo: string, publicRepo: boolean) {
    try {
        ghText(["repo", "view", `${owner}/${repo}`, "--json", "name"]);
        return "existing";
    } catch {
        const visibility = publicRepo ? "--public" : "--private";
        execFileSync("gh", [
            "repo",
            "create",
            `${owner}/${repo}`,
            visibility,
            "--description",
            "Hollow CGP backup mirror",
            "--disable-issues",
            "--disable-wiki",
        ], { stdio: "inherit" });
        return "created";
    }
}

function repoStatus(owner: string, repo: string, createRepo: boolean, publicRepo: boolean) {
    if (createRepo) {
        return ensureRepo(owner, repo, publicRepo);
    }
    try {
        ghText(["repo", "view", `${owner}/${repo}`, "--json", "name"]);
        return "existing";
    } catch {
        return "not-created";
    }
}

function openBrowser(url: string) {
    if (process.env.CGP_GITHUB_SETUP_NO_OPEN === "1") return;
    const command = process.platform === "win32"
        ? ["cmd", ["/c", "start", "", url]]
        : process.platform === "darwin"
            ? ["open", [url]]
            : ["xdg-open", [url]];
    try {
        const child = spawn(command[0], command[1], { detached: true, stdio: "ignore" });
        child.unref();
    } catch {
        // The printed URL is enough when desktop open is unavailable.
    }
}

function html(body: string) {
    return `<!doctype html><meta charset="utf-8"><title>Hollow GitHub Backup Setup</title><body style="font:14px system-ui,sans-serif;max-width:760px;margin:40px auto;line-height:1.45;background:#0f1115;color:#eef2ff"><h1>Hollow GitHub Backup Setup</h1>${body}</body>`;
}

function normalizeLocalReturnToUrl(value: string | null) {
    const trimmed = value?.trim() || "";
    if (!trimmed) return "";
    try {
        const url = new URL(trimmed);
        const hostname = url.hostname.toLowerCase();
        if (
            (url.protocol === "http:" || url.protocol === "https:") &&
            (hostname === "localhost" || hostname === "127.0.0.1" || hostname === "::1" || hostname === "[::1]")
        ) {
            return url.toString();
        }
        if (url.protocol === "tauri:" || url.protocol === "hollow:") {
            return url.toString();
        }
    } catch {
        return "";
    }
    return "";
}

function setupUrlWithReturnTo(baseSetupUrl: string, returnTo: string) {
    if (!returnTo) return baseSetupUrl;
    const url = new URL(baseSetupUrl);
    url.searchParams.set("returnTo", returnTo);
    return url.toString();
}

function githubSetupReturnUrl(
    returnTo: string,
    params: {
        repository: string;
        appId?: string | number;
        appInstallationId?: string;
        appPrivateKeyFile?: string;
    },
) {
    if (!returnTo) return "";
    const url = new URL(returnTo);
    url.searchParams.set("github_repository", params.repository);
    if (params.appInstallationId) {
        url.searchParams.set("github_installation_id", params.appInstallationId);
    }
    if (params.appId) {
        url.searchParams.set("github_app_id", String(params.appId));
    }
    if (params.appPrivateKeyFile) {
        url.searchParams.set("github_app_private_key_file", params.appPrivateKeyFile);
    }
    return url.toString();
}

function base64UrlJson(value: unknown) {
    return Buffer.from(JSON.stringify(value)).toString("base64url");
}

function createGitHubAppJwt(appId: string, privateKey: string) {
    const nowSeconds = Math.floor(Date.now() / 1000);
    const input = `${base64UrlJson({ alg: "RS256", typ: "JWT" })}.${base64UrlJson({
        iat: nowSeconds - 60,
        exp: nowSeconds + 9 * 60,
        iss: appId,
    })}`;
    const signer = createSign("RSA-SHA256");
    signer.update(input);
    signer.end();
    return `${input}.${signer.sign(privateKey).toString("base64url")}`;
}

async function createInstallationToken(appId: string, privateKey: string, installationId: string) {
    const response = await fetch(
        `https://api.github.com/app/installations/${encodeURIComponent(installationId)}/access_tokens`,
        {
            method: "POST",
            headers: {
                accept: "application/vnd.github+json",
                authorization: `Bearer ${createGitHubAppJwt(appId, privateKey)}`,
                "content-type": "application/json",
                "user-agent": "cgp-github-backup-setup",
                "x-github-api-version": "2022-11-28",
            },
            body: JSON.stringify({ permissions: { contents: "write" } }),
        },
    );
    const body = await response.json().catch(() => ({})) as Record<string, unknown>;
    if (!response.ok) {
        throw new Error(`GitHub installation token failed: HTTP ${response.status} ${JSON.stringify(body)}`);
    }
    const token = typeof body.token === "string" ? body.token : "";
    if (!token) throw new Error("GitHub installation token response had no token.");
    return token;
}

async function putGitHubFile(options: {
    owner: string;
    repo: string;
    branch: string;
    token: string;
    filePath: string;
    content: Buffer;
    message: string;
}) {
    const encodedPath = options.filePath.split("/").map(encodeURIComponent).join("/");
    const apiUrl = `https://api.github.com/repos/${encodeURIComponent(options.owner)}/${encodeURIComponent(options.repo)}/contents/${encodedPath}`;
    const headers = {
        accept: "application/vnd.github+json",
        authorization: `Bearer ${options.token}`,
        "content-type": "application/json",
        "user-agent": "cgp-github-backup-setup",
        "x-github-api-version": "2022-11-28",
    };
    let sha: string | undefined;
    const existing = await fetch(`${apiUrl}?ref=${encodeURIComponent(options.branch)}`, { headers });
    if (existing.ok) {
        const body = await existing.json() as Record<string, unknown>;
        sha = typeof body.sha === "string" ? body.sha : undefined;
    }
    const response = await fetch(apiUrl, {
        method: "PUT",
        headers,
        body: JSON.stringify({
            message: options.message,
            branch: options.branch,
            content: options.content.toString("base64"),
            ...(sha ? { sha } : {}),
        }),
    });
    const body = await response.json().catch(() => ({})) as Record<string, unknown>;
    if (!response.ok) {
        throw new Error(`GitHub contents write failed: HTTP ${response.status} ${JSON.stringify(body)}`);
    }
}

function backupReadme(owner: string, repo: string) {
    return `# Hollow CGP Backup

This repository is a user-controlled backup target for Hollow and Commons Guild Protocol relay data.

Hollow writes here through a GitHub App installed only on this selected repository. The app uses short-lived installation tokens, so the relay does not need a broad user token after setup.

## Contents

- \`cgp/manifest.json\` tracks exported mirror chunks and hosted guild ranges.
- \`cgp/chunks/*.jsonl.gz\` stores signed append-only CGP events.
- \`ipfs/\` may contain small content-addressed bootstrap objects when a relay uses the faux-IPFS GitHub backend.
- Setup check files may be written during verification and can be deleted after the backup is confirmed.

Large media should normally live in IPFS, R2, S3-compatible storage, or another content-addressed media backend. GitHub should hold hashes, references, manifests, and small bootstrap objects rather than production-scale media blobs.

## Restore

A CGP relay can ingest the mirror manifest, verify each event hash/signature, and replay chunks only when the sequence chain is continuous. Divergent histories should fail instead of overwriting local state.

## Security

Do not commit recovery keys, relay operator secrets, GitHub App private keys, local env files, or third-party service tokens to this repository.

Repository: \`${owner}/${repo}\`
`;
}

async function seedBackupReadme(options: {
    owner: string;
    repo: string;
    branch: string;
    appId: string;
    privateKeyPath: string;
    installationId: string;
}) {
    const privateKey = await readFile(options.privateKeyPath, "utf8");
    const token = await createInstallationToken(options.appId, privateKey, options.installationId);
    await putGitHubFile({
        owner: options.owner,
        repo: options.repo,
        branch: options.branch,
        token,
        filePath: "README.md",
        content: Buffer.from(backupReadme(options.owner, options.repo), "utf8"),
        message: "Add Hollow CGP backup README",
    });
}

async function main() {
    const args = parseArgs();
    const user = ghJson(["api", "user", "--jq", "{login:.login}"]);
    const owner = args.owner || user.login;
    if (!owner) throw new Error("Could not determine GitHub owner. Pass --owner <user-or-org>.");
    const repoState = repoStatus(owner, args.repo, args.createRepo, args.publicRepo);
    await mkdir(args.outDir, { recursive: true });

    const state = randomBytes(18).toString("hex");
    const origin = `http://127.0.0.1:${args.port}`;
    const publicOrigin = args.publicUrl || origin;
    const callbackUrl = `${publicOrigin}/callback`;
    const baseSetupUrl = `${publicOrigin}/setup`;
    const hookUrl = args.publicUrl
        ? `${publicOrigin}/webhook-disabled`
        : `https://github.com/${owner}/${args.repo}`;
    const manifestForSetupUrl = (setupUrl: string) => ({
        name: `Hollow Backup ${owner}`,
        url: `https://github.com/${owner}/${args.repo}`,
        redirect_url: callbackUrl,
        callback_urls: [callbackUrl],
        setup_url: setupUrl,
        setup_on_update: true,
        public: false,
        hook_attributes: {
            active: false,
            url: hookUrl,
        },
        default_permissions: {
            contents: "write",
            metadata: "read",
        },
        default_events: [],
    });

    let convertedApp: any;
    let installationId = "";
    let lastReturnTo = "";

    const writeEnv = async () => {
        if (!convertedApp) return;
        const privateKeyPath = path.join(args.outDir, "github-app.private-key.pem");
        const appJsonPath = path.join(args.outDir, "github-app.json");
        const envPath = path.join(args.outDir, "cgp-github-mirror.env");
        await writeFile(privateKeyPath, convertedApp.pem || "", "utf8");
        await writeFile(appJsonPath, JSON.stringify({
            id: convertedApp.id,
            slug: convertedApp.slug,
            client_id: convertedApp.client_id,
            webhook_secret: convertedApp.webhook_secret,
            owner,
            repo: args.repo,
            installation_id: installationId || undefined,
        }, null, 2), "utf8");
        await writeFile(envPath, [
            `CGP_GITHUB_MIRROR_REPOSITORY=${owner}/${args.repo}`,
            "CGP_GITHUB_MIRROR_BRANCH=main",
            "CGP_GITHUB_MIRROR_BASE_PATH=cgp",
            "CGP_GITHUB_MIRROR_AUTO=1",
            "CGP_GITHUB_MIRROR_FREQUENCY=batch",
            `CGP_GITHUB_MIRROR_APP_ID=${convertedApp.id}`,
            `CGP_GITHUB_MIRROR_APP_PRIVATE_KEY_FILE=${privateKeyPath}`,
            installationId ? `CGP_GITHUB_MIRROR_APP_INSTALLATION_ID=${installationId}` : "# CGP_GITHUB_MIRROR_APP_INSTALLATION_ID=<install-the-app-and-refresh-setup-url>",
            "",
            "# Optional: store small faux-IPFS/bootstrap objects in the same selected repo.",
            "# CGP_IPFS_FAUX_STORAGE=github",
            `# CGP_IPFS_FAUX_GITHUB_REPOSITORY=${owner}/${args.repo}`,
            "# CGP_IPFS_FAUX_GITHUB_BRANCH=main",
            "# CGP_IPFS_FAUX_GITHUB_BASE_PATH=ipfs",
            `# CGP_IPFS_FAUX_GITHUB_APP_ID=${convertedApp.id}`,
            `# CGP_IPFS_FAUX_GITHUB_APP_PRIVATE_KEY_FILE=${privateKeyPath}`,
            installationId ? `# CGP_IPFS_FAUX_GITHUB_APP_INSTALLATION_ID=${installationId}` : "# CGP_IPFS_FAUX_GITHUB_APP_INSTALLATION_ID=<install-the-app-and-refresh-setup-url>",
            "",
        ].join("\n"), "utf8");
        return { privateKeyPath, appJsonPath, envPath };
    };

    const server = createServer(async (req, res) => {
        const url = new URL(req.url || "/", origin);
        try {
            if (url.pathname === "/") {
                const returnTo = normalizeLocalReturnToUrl(url.searchParams.get("returnTo"));
                if (returnTo) {
                    lastReturnTo = returnTo;
                }
                const manifest = manifestForSetupUrl(setupUrlWithReturnTo(baseSetupUrl, returnTo));
                res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
                res.end(html(`
                    <p>Repository target: <code>${owner}/${args.repo}</code> (${repoState})</p>
                    <p>GitHub callback: <code>${callbackUrl}</code></p>
                    <form action="https://github.com/settings/apps/new?state=${state}" method="post">
                        <input type="hidden" name="manifest" value='${JSON.stringify(manifest).replace(/'/g, "&#39;")}'>
                        <button style="font:inherit;padding:10px 14px;border:0;border-radius:6px;background:#58a6ff;color:#06101f" type="submit">Create GitHub App</button>
                    </form>
                    <p>After GitHub creates the app, this helper stores the app id and private key in <code>${args.outDir}</code>.</p>
                `));
                return;
            }
            if (url.pathname === "/webhook-disabled") {
                res.writeHead(204);
                res.end();
                return;
            }
            if (url.pathname === "/callback") {
                if (url.searchParams.get("state") !== state) {
                    res.writeHead(400);
                    res.end("state mismatch");
                    return;
                }
                const code = url.searchParams.get("code");
                if (!code) {
                    res.writeHead(400);
                    res.end("missing code");
                    return;
                }
                const response = await fetch(`https://api.github.com/app-manifests/${encodeURIComponent(code)}/conversions`, {
                    method: "POST",
                    headers: {
                        accept: "application/vnd.github+json",
                        "user-agent": "cgp-github-backup-setup",
                        "x-github-api-version": "2022-11-28",
                    },
                });
                convertedApp = await response.json();
                if (!response.ok) throw new Error(`GitHub manifest conversion failed: ${JSON.stringify(convertedApp)}`);
                const written = await writeEnv();
                const installUrl = `https://github.com/apps/${convertedApp.slug}/installations/new`;
                res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
                res.end(html(`
                    <p>GitHub App created: <code>${convertedApp.slug}</code>.</p>
                    <p>Saved app credentials to <code>${written?.appJsonPath}</code> and <code>${written?.privateKeyPath}</code>.</p>
                    <p><a style="color:#58a6ff" href="${installUrl}">Install the app</a>, choose <strong>Only select repositories</strong>, and select <code>${owner}/${args.repo}</code>.</p>
                `));
                return;
            }
            if (url.pathname === "/setup") {
                installationId = url.searchParams.get("installation_id") || installationId;
                const written = await writeEnv();
                let readmeStatus = "not written";
                if (convertedApp && installationId && written?.privateKeyPath) {
                    await seedBackupReadme({
                        owner,
                        repo: args.repo,
                        branch: "main",
                        appId: String(convertedApp.id),
                        privateKeyPath: written.privateKeyPath,
                        installationId,
                    });
                    readmeStatus = "written";
                }
                const returnTo = normalizeLocalReturnToUrl(url.searchParams.get("returnTo")) || lastReturnTo;
                const returnUrl = githubSetupReturnUrl(returnTo, {
                    repository: `${owner}/${args.repo}`,
                    appId: convertedApp?.id,
                    appInstallationId: installationId,
                    appPrivateKeyFile: written?.privateKeyPath,
                });
                if (returnUrl) {
                    res.writeHead(302, { location: returnUrl });
                    res.end();
                    return;
                }
                res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
                res.end(html(`
                    <p>Installation recorded: <code>${installationId || "missing"}</code>.</p>
                    <p>Relay env file: <code>${written?.envPath}</code></p>
                    <p>Repository README: <code>${readmeStatus}</code></p>
                    <p>You can stop this setup helper now.</p>
                `));
                return;
            }
            res.writeHead(404);
            res.end("not found");
        } catch (error: any) {
            res.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
            res.end(error?.stack || error?.message || String(error));
        }
    });

    await new Promise<void>((resolve) => server.listen(args.port, "127.0.0.1", resolve));
    const url = `${publicOrigin}/`;
    console.log(`GitHub backup app setup running at ${url}`);
    console.log(`Local helper listening at ${origin}/`);
    console.log(`Target repo: ${owner}/${args.repo} (${repoState})`);
    console.log("Keep this process running until GitHub redirects back after app installation.");
    openBrowser(url);
}

main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
});
