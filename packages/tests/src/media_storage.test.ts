import { describe, expect, it } from "vitest";
import { CgpClient } from "@cgp/client";
import { generatePrivateKey, getPublicKey, hashObject } from "@cgp/core";
import { createFauxIpfsBackendPlugin, createHeliaIpfsPlugin, createMediaStoragePolicyPlugin } from "@cgp/relay/src/plugins";
import { RelayServer } from "@cgp/relay/src/server";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

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

function keyPair() {
    const priv = generatePrivateKey();
    return { pub: getPublicKey(priv), priv };
}

function mediaPlugin() {
    return createMediaStoragePolicyPlugin({
        maxAttachmentBytes: 1024 * 1024,
        providers: [
            {
                id: "meme-ipfs",
                kind: "ipfs",
                priority: 100,
                maxBytes: 1024 * 1024,
                acceptsMimeTypes: ["image/*"],
                acceptsTags: ["meme"],
                rejectsTags: ["adult", "nsfw", "porn"],
                adult: "deny",
                retention: "operator-defined",
                mission: "free meme pinning"
            },
            {
                id: "adult-ipfs",
                kind: "ipfs",
                priority: 90,
                maxBytes: 1024 * 1024,
                acceptsMimeTypes: ["image/*"],
                acceptsTags: ["adult", "nsfw"],
                requiresTags: ["adult"],
                adult: "only",
                retention: "operator-defined",
                mission: "opt-in adult media host"
            },
            {
                id: "general-https",
                kind: "https",
                priority: 10,
                maxBytes: 1024 * 1024,
                acceptsMimeTypes: ["*/*"],
                adult: "allow",
                retention: "operator-defined"
            }
        ]
    });
}

describe("media storage policy plugin", () => {
    it("routes upload intents to tag-specific providers", async () => {
        const dbPath = `./test-media-route-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const relay = new RelayServer(0, dbPath, [mediaPlugin()], { enableDefaultPlugins: false });
        try {
            const port = await waitForPort(relay);
            const response = await fetch(`http://localhost:${port}/plugins/cgp.media.storage/route`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    mimeType: "image/webp",
                    size: 2048,
                    tags: ["meme"],
                    encrypted: true
                })
            });
            const payload = await response.json() as any;
            expect(response.status).toBe(200);
            expect(payload.selected.id).toBe("meme-ipfs");
            expect(payload.upload.requiredAttachment.external.storage.providerId).toBe("meme-ipfs");
        } finally {
            await relay.close();
            await fs.rm(dbPath, { recursive: true, force: true });
        }
    });

    it("uploads accepted media bytes through a registered Helia backend", async () => {
        const dbPath = `./test-media-upload-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const heliaDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-media-helia-"));
        const relay = new RelayServer(
            0,
            dbPath,
            [
                createHeliaIpfsPlugin({ storeDir: heliaDir, exposeHttpRoutes: true }),
                mediaPlugin(),
            ],
            { enableDefaultPlugins: false },
        );
        try {
            const port = await waitForPort(relay);
            const bytes = Buffer.from("real meme image bytes");
            const uploadResponse = await fetch(`http://localhost:${port}/plugins/cgp.media.storage/upload`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    bytesBase64: bytes.toString("base64"),
                    name: "meme.webp",
                    mimeType: "image/webp",
                    tags: ["meme"],
                    encrypted: true,
                    providerId: "meme-ipfs",
                }),
            });
            const uploaded = await uploadResponse.json() as any;
            expect(uploadResponse.status, JSON.stringify(uploaded)).toBe(201);
            expect(uploaded.selected.id).toBe("meme-ipfs");
            expect(uploaded.cid).toMatch(/^baf/);
            expect(uploaded.attachment).toMatchObject({
                url: `ipfs://${uploaded.cid}`,
                scheme: "ipfs",
                mimeType: "image/webp",
                size: bytes.byteLength,
                external: {
                    storage: {
                        providerId: "meme-ipfs",
                        kind: "ipfs",
                        ipfsBackendId: "helia",
                    },
                },
            });

            const heliaResponse = await fetch(`http://localhost:${port}/plugins/cgp.ipfs.helia/ipfs/${uploaded.cid}`);
            expect(heliaResponse.status).toBe(200);
            expect(Buffer.from(await heliaResponse.arrayBuffer()).toString("utf8")).toBe(bytes.toString("utf8"));
        } finally {
            await relay.close();
            await fs.rm(dbPath, { recursive: true, force: true });
            await fs.rm(heliaDir, { recursive: true, force: true });
        }
    }, 15_000);

    it("uploads accepted media bytes through a faux IPFS backend", async () => {
        const dbPath = `./test-media-faux-upload-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const fauxDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-media-faux-"));
        const relay = new RelayServer(
            0,
            dbPath,
            [
                createFauxIpfsBackendPlugin({
                    id: "faux",
                    storage: "local",
                    storeDir: fauxDir,
                    exposeHttpRoutes: true,
                }),
                createMediaStoragePolicyPlugin({
                    maxAttachmentBytes: 1024 * 1024,
                    providers: [
                        {
                            id: "meme-faux",
                            kind: "ipfs",
                            ipfsBackendId: "faux",
                            priority: 100,
                            maxBytes: 1024 * 1024,
                            acceptsMimeTypes: ["image/*"],
                            acceptsTags: ["meme"],
                            adult: "deny",
                            retention: "operator-defined",
                        },
                    ],
                }),
            ],
            { enableDefaultPlugins: false },
        );
        try {
            const port = await waitForPort(relay);
            const bytes = Buffer.from("faux meme image bytes");
            const uploadResponse = await fetch(`http://localhost:${port}/plugins/cgp.media.storage/upload`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    bytesBase64: bytes.toString("base64"),
                    name: "meme-faux.webp",
                    mimeType: "image/webp",
                    tags: ["meme"],
                    encrypted: true,
                    providerId: "meme-faux",
                }),
            });
            const uploaded = await uploadResponse.json() as any;
            expect(uploadResponse.status, JSON.stringify(uploaded)).toBe(201);
            expect(uploaded.selected.id).toBe("meme-faux");
            expect(uploaded.attachment).toMatchObject({
                url: `ipfs://${uploaded.cid}`,
                scheme: "ipfs",
                external: {
                    storage: {
                        providerId: "meme-faux",
                        kind: "ipfs",
                        ipfsBackendId: "faux",
                    },
                },
            });

            const fauxResponse = await fetch(`http://localhost:${port}/plugins/cgp.ipfs.faux/ipfs/${uploaded.cid}`);
            expect(fauxResponse.status).toBe(200);
            expect(fauxResponse.headers.get("x-cgp-faux-ipfs")).toBe("1");
            expect(Buffer.from(await fauxResponse.arrayBuffer()).toString("utf8")).toBe(bytes.toString("utf8"));
        } finally {
            await relay.close();
            await fs.rm(dbPath, { recursive: true, force: true });
            await fs.rm(fauxDir, { recursive: true, force: true });
        }
    });

    it("accepts matching media providers and rejects mismatched providers", async () => {
        const dbPath = `./test-media-policy-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const relay = new RelayServer(0, dbPath, [mediaPlugin()], { enableDefaultPlugins: false });
        const port = await waitForPort(relay);
        const client = new CgpClient({ relays: [`ws://localhost:${port}`], keyPair: keyPair() });
        try {
            await client.connect();
            const guildId = hashObject({ test: "media-policy", nonce: Date.now() });
            const channelId = "general";
            await client.publishReliable({ type: "GUILD_CREATE", guildId, name: "Media Policy" }, { timeoutMs: 3000 });
            await client.publishReliable({ type: "CHANNEL_CREATE", guildId, channelId, name: "general", kind: "text" }, { timeoutMs: 3000 });

            await expect(client.publishReliable({
                type: "MESSAGE",
                guildId,
                channelId,
                messageId: hashObject({ kind: "accepted-media", guildId }),
                content: "meme",
                attachments: [
                    {
                        url: "ipfs://bafymeme",
                        scheme: "ipfs",
                        type: "image",
                        mimeType: "image/webp",
                        size: 2048,
                        encrypted: true,
                        external: {
                            tags: ["meme"],
                            storage: { providerId: "meme-ipfs", kind: "ipfs", tags: ["meme"] }
                        }
                    }
                ]
            } as any, { timeoutMs: 3000 })).resolves.toMatchObject({ guildId });

            await expect(client.publishReliable({
                type: "MESSAGE",
                guildId,
                channelId,
                messageId: hashObject({ kind: "blocked-media", guildId }),
                content: "adult-tagged media cannot use the meme host",
                attachments: [
                    {
                        url: "ipfs://bafyadult",
                        scheme: "ipfs",
                        type: "image",
                        mimeType: "image/webp",
                        size: 2048,
                        encrypted: true,
                        adult: true,
                        external: {
                            tags: ["adult"],
                            storage: { providerId: "meme-ipfs", kind: "ipfs", tags: ["adult"] }
                        }
                    }
                ]
            } as any, { timeoutMs: 3000 })).rejects.toThrow(/does not accept/i);
        } finally {
            client.close();
            await relay.close();
            await fs.rm(dbPath, { recursive: true, force: true });
        }
    });
});
