import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import {
    computeEventId,
    generatePrivateKey,
    getPublicKey,
    hashObject,
    sign,
    type EventBody,
    type GuildEvent,
} from "@cgp/core";
import {
    createFauxIpfsBackendPlugin,
    createGitHubRelayMirrorPlugin,
    type CgpIpfsBackend,
    type RelayPluginContext,
} from "@cgp/relay/src/plugins";
import { MemoryStore } from "@cgp/relay/src/store";

type Args = {
    envFile: string;
    basePath: string;
    mirrorDir: string;
};

function parseArgs(): Args {
    const args = process.argv.slice(2);
    const read = (name: string) => {
        const prefixed = args.find((arg) => arg.startsWith(`--${name}=`));
        if (prefixed) return prefixed.slice(name.length + 3);
        const index = args.indexOf(`--${name}`);
        return index >= 0 ? args[index + 1] || "" : "";
    };
    return {
        envFile: path.resolve(read("env-file") || ".github-app/cgp-github-mirror.env"),
        basePath: read("base-path") || "cgp/live-plugin-check",
        mirrorDir: path.resolve(read("mirror-dir") || ".github-app/live-plugin-mirror"),
    };
}

function parseEnvFile(raw: string) {
    const env: Record<string, string> = {};
    for (const line of raw.split(/\r?\n/)) {
        if (!line || line.trim().startsWith("#")) continue;
        const index = line.indexOf("=");
        if (index <= 0) continue;
        env[line.slice(0, index)] = line.slice(index + 1);
    }
    return env;
}

function requireEnv(env: Record<string, string>, name: string) {
    const value = env[name]?.trim();
    if (!value) {
        throw new Error(`Missing ${name} in GitHub backup env file.`);
    }
    return value;
}

async function signedEvent(
    privateKey: Uint8Array,
    author: string,
    seq: number,
    previous: GuildEvent | undefined,
    body: EventBody,
): Promise<GuildEvent> {
    const event: GuildEvent = {
        id: "",
        seq,
        prevHash: previous?.id ?? null,
        createdAt: Date.now() + seq,
        author,
        body,
        signature: "",
    };
    event.id = computeEventId(event);
    event.signature = sign(
        privateKey,
        hashObject({ body: event.body, author: event.author, createdAt: event.createdAt }),
    );
    return event;
}

async function main() {
    const args = parseArgs();
    const env = parseEnvFile(await fs.readFile(args.envFile, "utf8"));
    const repository = requireEnv(env, "CGP_GITHUB_MIRROR_REPOSITORY");
    const branch = env.CGP_GITHUB_MIRROR_BRANCH || "main";
    const appId = requireEnv(env, "CGP_GITHUB_MIRROR_APP_ID");
    const appPrivateKeyFile = requireEnv(env, "CGP_GITHUB_MIRROR_APP_PRIVATE_KEY_FILE");
    const appInstallationId = requireEnv(env, "CGP_GITHUB_MIRROR_APP_INSTALLATION_ID");

    await fs.rm(args.mirrorDir, { recursive: true, force: true });
    const privateKey = generatePrivateKey();
    const author = getPublicKey(privateKey);
    const guildId = "github-app-live-check";
    const genesis = await signedEvent(privateKey, author, 0, undefined, {
        type: "GUILD_CREATE",
        guildId,
        name: "GitHub App Live Check",
    });
    const channel = await signedEvent(privateKey, author, 1, genesis, {
        type: "CHANNEL_CREATE",
        guildId,
        channelId: "checks",
        name: "checks",
        kind: "text",
    });
    const message = await signedEvent(privateKey, author, 2, channel, {
        type: "MESSAGE",
        guildId,
        channelId: "checks",
        messageId: hashObject({ live: Date.now() }),
        content: "GitHub App mirror plugin live check",
    });
    const events = [genesis, channel, message];
    const store = new MemoryStore();
    await store.appendEvents(guildId, events);

    const plugin = createGitHubRelayMirrorPlugin({
        mirrorDir: args.mirrorDir,
        repository,
        branch,
        basePath: args.basePath,
        appId,
        appPrivateKeyFile,
        appInstallationId,
        autoMirror: true,
        frequency: "per-event",
    });
    const ctx = {
        relayPublicKey: author,
        store,
        publishAsRelay: async () => undefined,
        broadcast: () => undefined,
        getLog: async (id: string) => store.getLog(id),
    } as RelayPluginContext;

    await plugin.onInit?.(ctx);
    await plugin.onEventsAppended?.({ events }, ctx);

    const manifestPath = path.join(args.mirrorDir, args.basePath, "manifest.json");
    const manifestBytes = await fs.readFile(manifestPath);
    const manifest = JSON.parse(manifestBytes.toString("utf8"));
    const fauxBackends = new Map<string, CgpIpfsBackend>();
    const fauxPlugin = createFauxIpfsBackendPlugin({
        id: "github-app-faux-check",
        storage: "github",
        githubRepository: repository,
        githubBranch: branch,
        githubBasePath: "ipfs/live-plugin-check",
        githubAppId: appId,
        githubAppPrivateKeyFile: appPrivateKeyFile,
        githubAppInstallationId: appInstallationId,
        exposeHttpRoutes: false,
        maxAddBytes: 1024 * 1024,
    });
    await fauxPlugin.onInit?.({
        ...ctx,
        ipfsBackends: fauxBackends,
    });
    const fauxBackend = fauxBackends.get("github-app-faux-check");
    if (!fauxBackend) {
        throw new Error("Faux IPFS GitHub App backend did not register.");
    }
    const fauxBytes = Buffer.from(`GitHub App faux IPFS live check ${new Date().toISOString()}`);
    const fauxResult = await fauxBackend.addFile({
        bytes: fauxBytes,
        name: "github-app-faux-check.txt",
        mimeType: "text/plain",
    });
    console.log(JSON.stringify({
        ok: true,
        repository,
        branch,
        basePath: args.basePath,
        chunks: manifest.chunks?.length || 0,
        events: Array.isArray(manifest.chunks)
            ? manifest.chunks.reduce((sum: number, chunk: any) => sum + Number(chunk.events || 0), 0)
            : 0,
        manifestSha256: createHash("sha256").update(manifestBytes).digest("hex"),
        fauxIpfs: {
            providerId: fauxResult.providerId,
            cid: fauxResult.cid,
            bytes: fauxResult.bytes,
            storage: fauxResult.storage,
        },
    }, null, 2));
}

main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
});
