import { createHash, generateKeyPairSync } from "node:crypto";
import { createServer, type ServerResponse } from "node:http";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { gunzipSync } from "node:zlib";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
    computeEventId,
    generatePrivateKey,
    getPublicKey,
    hashObject,
    sign,
    type EventBody,
    type GuildEvent,
} from "@cgp/core";
import { createGitHubRelayMirrorPlugin, type RelayPluginContext } from "@cgp/relay/src/plugins";
import { RelayServer } from "@cgp/relay/src/server";
import { MemoryStore } from "@cgp/relay/src/store";

function sha256(data: Buffer | string) {
    return createHash("sha256").update(data).digest("hex");
}

function decodeMirrorChunk(bytes: Buffer, chunkPath: string) {
    return chunkPath.endsWith(".gz") ? gunzipSync(bytes) : bytes;
}

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForPort(relay: RelayServer) {
    const started = Date.now();
    while (Date.now() - started < 5000) {
        const port = relay.getPort();
        if (Number.isFinite(port) && port > 0) return port;
        await sleep(25);
    }
    throw new Error("Timed out waiting for relay port");
}

async function signedEvent(
    privateKey: Uint8Array,
    author: string,
    seq: number,
    previous: GuildEvent | undefined,
    body: EventBody,
): Promise<GuildEvent> {
    const createdAt = 1_700_000_000_000 + seq;
    const event: GuildEvent = {
        id: "",
        seq,
        prevHash: previous?.id ?? null,
        createdAt,
        author,
        body,
        signature: "",
    };
    event.id = computeEventId(event);
    event.signature = await sign(privateKey, hashObject({ body, author, createdAt }));
    return event;
}

function sendBuffer(res: ServerResponse, body: Buffer, contentType = "application/octet-stream") {
    res.statusCode = 200;
    res.setHeader("content-type", contentType);
    res.setHeader("content-length", body.byteLength);
    res.end(body);
}

describe("GitHub relay mirror plugin", () => {
    const tempDirs: string[] = [];
    const relays: RelayServer[] = [];

    afterEach(async () => {
        await Promise.all(relays.splice(0).map((relay) => relay.close().catch(() => undefined)));
        await Promise.all(tempDirs.splice(0).map((dir) => fs.rm(dir, { recursive: true, force: true })));
        vi.unstubAllGlobals();
    });

    it("writes GitHub-compatible mirror chunks and restores them into an empty relay store", async () => {
        const mirrorDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-github-mirror-"));
        tempDirs.push(mirrorDir);
        const privateKey = generatePrivateKey();
        const author = getPublicKey(privateKey);
        const guildId = hashObject({ test: "github-mirror" });
        const channelId = hashObject({ test: "github-mirror-channel" });
        const genesis = await signedEvent(privateKey, author, 0, undefined, {
            type: "GUILD_CREATE",
            guildId,
            name: "Mirror Test",
        });
        const channel = await signedEvent(privateKey, author, 1, genesis, {
            type: "CHANNEL_CREATE",
            guildId,
            channelId,
            name: "general",
            kind: "text",
        });
        const message = await signedEvent(privateKey, author, 2, channel, {
            type: "MESSAGE",
            guildId,
            channelId,
            messageId: hashObject({ test: "github-mirror-message" }),
            content: "mirrored",
        });
        const events = [genesis, channel, message];
        const sourceStore = new MemoryStore();
        await sourceStore.appendEvents(guildId, events);
        const plugin = createGitHubRelayMirrorPlugin({
            mirrorDir,
            autoMirror: true,
            frequency: "batch",
            batchSize: 2,
            allowUnauthenticatedHttpWrites: true,
        });
        const sourceCtx = {
            relayPublicKey: author,
            store: sourceStore,
            publishAsRelay: async () => undefined,
            broadcast: () => undefined,
            getLog: async (id: string) => sourceStore.getLog(id),
        } as RelayPluginContext;
        await plugin.onInit?.(sourceCtx);
        await plugin.onEventsAppended?.({ events }, sourceCtx);

        const manifestPath = path.join(mirrorDir, "cgp", "manifest.json");
        const manifest = JSON.parse(await fs.readFile(manifestPath, "utf8"));
        expect(manifest.chunks).toHaveLength(1);
        expect(manifest.chunks[0].events).toBe(3);
        expect(manifest.chunks[0].compression).toBe("gzip");
        expect(manifest.chunks[0].path.endsWith(".jsonl.gz")).toBe(true);

        const root = path.join(mirrorDir, "cgp");
        let chunkRequests = 0;
        const origin = createServer(async (req, res) => {
            const url = new URL(req.url || "/", "http://localhost");
            const safeRel = url.pathname.replace(/^\/+/, "");
            const resolved = path.join(root, safeRel || "manifest.json");
            const relative = path.relative(root, resolved);
            if (relative.startsWith("..") || path.isAbsolute(relative)) {
                res.statusCode = 400;
                res.end("bad path");
                return;
            }
            try {
                if (safeRel.startsWith("chunks/")) {
                    chunkRequests += 1;
                }
                const body = await fs.readFile(resolved);
                sendBuffer(res, body, resolved.endsWith(".json") ? "application/json" : "text/plain");
            } catch {
                res.statusCode = 404;
                res.end("not found");
            }
        });
        await new Promise<void>((resolve) => origin.listen(0, "127.0.0.1", resolve));
        const address = origin.address();
        if (!address || typeof address === "string") throw new Error("origin did not bind");
        const targetStore = new MemoryStore();
        const targetMirrorDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-github-mirror-target-"));
        tempDirs.push(targetMirrorDir);
        const targetPlugin = createGitHubRelayMirrorPlugin({
            mirrorDir: targetMirrorDir,
            sources: [{ url: `http://127.0.0.1:${address.port}/manifest.json` }],
            autoIngest: true,
            allowUnauthenticatedHttpWrites: true,
        });
        const broadcasted: GuildEvent[] = [];
        const targetCtx = {
            relayPublicKey: author,
            store: targetStore,
            publishAsRelay: async () => undefined,
            broadcast: (_guildId: string, event: GuildEvent) => broadcasted.push(event),
            getLog: async (id: string) => targetStore.getLog(id),
        } as RelayPluginContext;

        try {
            await targetPlugin.onInit?.(targetCtx);
            const restored = await targetStore.getLog(guildId);
            expect(restored.map((event) => event.id)).toEqual(events.map((event) => event.id));
            expect(broadcasted).toHaveLength(3);

            const secondPlugin = createGitHubRelayMirrorPlugin({
                mirrorDir: targetMirrorDir,
                sources: [{ url: `http://127.0.0.1:${address.port}/manifest.json` }],
                autoIngest: true,
                allowUnauthenticatedHttpWrites: true,
            });
            await secondPlugin.onInit?.(targetCtx);
            expect(chunkRequests).toBe(1);
            const targetManifest = JSON.parse(await fs.readFile(path.join(targetMirrorDir, "cgp", "manifest.json"), "utf8"));
            expect(targetManifest.ingestedChunks).toHaveLength(1);
            expect(targetManifest.ingestedChunks[0].sha256).toBe(manifest.chunks[0].sha256);
        } finally {
            await new Promise<void>((resolve) => origin.close(() => resolve()));
        }
    });

    it("mirrors events appended through the relay plugin append path used by bridge plugins", async () => {
        const mirrorDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-bridge-mirror-"));
        tempDirs.push(mirrorDir);
        const store = new MemoryStore();
        let capturedCtx: RelayPluginContext | undefined;
        const capturePlugin = {
            name: "bridge-capture",
            onInit: (ctx: RelayPluginContext) => {
                capturedCtx = ctx;
            },
        };
        const relay = new RelayServer(
            0,
            store,
            [
                createGitHubRelayMirrorPlugin({
                    mirrorDir,
                    autoMirror: true,
                    frequency: "per-event",
                    allowUnauthenticatedHttpWrites: true,
                }),
                capturePlugin,
            ],
            { enableDefaultPlugins: false },
        );
        relays.push(relay);
        await waitForPort(relay);
        const started = Date.now();
        while (!capturedCtx && Date.now() - started < 5000) {
            await sleep(25);
        }
        expect(capturedCtx?.appendEventsFromPlugin).toBeTypeOf("function");

        const privateKey = generatePrivateKey();
        const author = getPublicKey(privateKey);
        const guildId = hashObject({ test: "bridge-plugin-mirror" });
        const channelId = hashObject({ test: "bridge-plugin-channel" });
        const genesis = await signedEvent(privateKey, author, 99, undefined, {
            type: "GUILD_CREATE",
            guildId,
            name: "Bridge Mirror Test",
            external: { provider: "external-chat", kind: "GUILD", sourceGuildId: "source-guild-1" },
        } as any);
        const channel = await signedEvent(privateKey, author, 99, genesis, {
            type: "CHANNEL_CREATE",
            guildId,
            channelId,
            name: "general",
            kind: "text",
            external: { provider: "external-chat", sourceChannelId: "source-channel-1", sourceGuildId: "source-guild-1" },
        } as any);
        const message = await signedEvent(privateKey, author, 99, channel, {
            type: "MESSAGE",
            guildId,
            channelId,
            messageId: hashObject({ provider: "external-chat", kind: "message", sourceMessageId: "source-message-1" }),
            content: "hello from the existing bridge",
            external: {
                provider: "external-chat",
                direction: "inbound",
                sourceMessageId: "source-message-1",
                sourceChannelId: "source-channel-1",
                attachments: ["https://cdn.example.invalid/attachments/cat.png"],
            },
        } as any);

        const appended = await capturedCtx!.appendEventsFromPlugin!([genesis, channel, message]);
        expect(appended.map((event) => event.seq)).toEqual([0, 1, 2]);

        const log = await store.getLog(guildId);
        expect(log.map((event) => event.id)).toEqual(appended.map((event) => event.id));
        expect((log[2].body as any).external.provider).toBe("external-chat");

        const manifest = JSON.parse(await fs.readFile(path.join(mirrorDir, "cgp", "manifest.json"), "utf8"));
        expect(manifest.chunks.length).toBeGreaterThan(0);
        expect(manifest.guilds[0].headSeq).toBe(2);
        expect(sha256(await fs.readFile(path.join(mirrorDir, "cgp", manifest.chunks[0].path)))).toBe(manifest.chunks[0].sha256);
    });

    it("uploads profile events with usernames and avatars through the GitHub contents API", async () => {
        const mirrorDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-github-upload-"));
        tempDirs.push(mirrorDir);
        const privateKey = generatePrivateKey();
        const author = getPublicKey(privateKey);
        const guildId = hashObject({ test: "github-profile-upload" });
        const memberId = hashObject({ test: "github-profile-member" });
        const genesis = await signedEvent(privateKey, author, 0, undefined, {
            type: "GUILD_CREATE",
            guildId,
            name: "Profile Upload Test",
        });
        const member = await signedEvent(privateKey, author, 1, genesis, {
            type: "GUILD_MEMBER_ADD",
            guildId,
            userId: memberId,
            nickname: "Alice Example",
            avatar: "https://cdn.example.invalid/avatars/u3/aliceavatar.png",
            roles: [],
        } as any);
        const events = [genesis, member];
        const uploads: Array<{ url: string; body: any }> = [];
        vi.stubGlobal("fetch", async (url: string | URL | Request, init?: RequestInit) => {
            const href = String(url);
            if (init?.method === "PUT") {
                uploads.push({ url: href, body: JSON.parse(String(init.body)) });
                return new Response(JSON.stringify({ content: { path: href } }), { status: 200 });
            }
            return new Response("{}", { status: 404 });
        });

        const store = new MemoryStore();
        await store.appendEvents(guildId, events);
        const plugin = createGitHubRelayMirrorPlugin({
            mirrorDir,
            repository: "cgp-test/profile-mirror",
            token: "test-token",
            branch: "main",
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

        const chunkUpload = uploads.find((upload) => upload.url.includes("/contents/cgp/chunks/"));
        const manifestUpload = uploads.find((upload) => upload.url.endsWith("/contents/cgp/manifest.json"));
        expect(chunkUpload).toBeDefined();
        expect(manifestUpload).toBeDefined();
        const uploadedChunk = Buffer.from(chunkUpload!.body.content, "base64");
        const uploadedJsonl = decodeMirrorChunk(uploadedChunk, chunkUpload!.url).toString("utf8");
        expect(uploadedJsonl).toContain("\"nickname\":\"Alice Example\"");
        expect(uploadedJsonl).toContain("\"avatar\":\"https://cdn.example.invalid/avatars/u3/aliceavatar.png\"");
        expect(manifestUpload!.body.message).toBe("CGP relay mirror manifest.json");
    });

    it("mints a scoped GitHub App installation token before uploading mirror chunks", async () => {
        const mirrorDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-github-app-upload-"));
        tempDirs.push(mirrorDir);
        const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
        const privateKeyPem = privateKey.export({ type: "pkcs1", format: "pem" }).toString();
        const privateKeyBytes = generatePrivateKey();
        const author = getPublicKey(privateKeyBytes);
        const guildId = hashObject({ test: "github-app-upload" });
        const genesis = await signedEvent(privateKeyBytes, author, 0, undefined, {
            type: "GUILD_CREATE",
            guildId,
            name: "GitHub App Upload Test",
        });
        const events = [genesis];
        const installationTokenRequests: any[] = [];
        const uploads: Array<{ url: string; authorization: string; body: any }> = [];
        vi.stubGlobal("fetch", async (url: string | URL | Request, init?: RequestInit) => {
            const href = String(url);
            const authorization = String((init?.headers as any)?.authorization || "");
            if (href.endsWith("/app/installations/123456/access_tokens") && init?.method === "POST") {
                installationTokenRequests.push({
                    authorization,
                    body: JSON.parse(String(init.body)),
                });
                return new Response(JSON.stringify({
                    token: "installation-token-abc",
                    expires_at: new Date(Date.now() + 60 * 60_000).toISOString(),
                }), { status: 201 });
            }
            if (init?.method === "PUT") {
                uploads.push({ url: href, authorization, body: JSON.parse(String(init.body)) });
                return new Response(JSON.stringify({ content: { path: href } }), { status: 200 });
            }
            return new Response("{}", { status: 404 });
        });

        const store = new MemoryStore();
        await store.appendEvents(guildId, events);
        const plugin = createGitHubRelayMirrorPlugin({
            mirrorDir,
            repository: "cgp-test/app-mirror",
            appId: "999",
            appInstallationId: "123456",
            appPrivateKey: privateKeyPem,
            branch: "main",
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

        expect(installationTokenRequests).toHaveLength(1);
        expect(installationTokenRequests[0].authorization).toMatch(/^Bearer [^.]+\.[^.]+\.[^.]+$/);
        expect(installationTokenRequests[0].body).toMatchObject({ permissions: { contents: "write" } });
        expect(uploads.length).toBeGreaterThanOrEqual(2);
        expect(uploads.every((upload) => upload.authorization === "Bearer installation-token-abc")).toBe(true);
        expect(uploads.some((upload) => upload.url.includes("/contents/cgp/chunks/"))).toBe(true);
        expect(uploads.some((upload) => upload.url.endsWith("/contents/cgp/manifest.json"))).toBe(true);
    });
});
