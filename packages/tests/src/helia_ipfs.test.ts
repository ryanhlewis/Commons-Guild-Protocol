import { createHash, generateKeyPairSync } from "node:crypto";
import { createServer, type ServerResponse } from "node:http";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
    createFauxIpfsBackendPlugin,
    createHeliaIpfsPlugin,
    createStaticShardSeedPlugin,
    type CgpIpfsBackend,
    type RelayPlugin,
} from "@cgp/relay/src/plugins";
import { RelayServer } from "@cgp/relay/src/server";
import { MemoryStore } from "@cgp/relay/src/store";
import { hashObject } from "@cgp/core";

function sha256(data: Buffer | string) {
    return createHash("sha256").update(data).digest("hex");
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

async function removeDirEventually(dir: string) {
    let lastError: unknown;
    for (let attempt = 0; attempt < 10; attempt += 1) {
        try {
            await fs.rm(dir, { recursive: true, force: true });
            return;
        } catch (error) {
            lastError = error;
            await sleep(50 + attempt * 50);
        }
    }
    throw lastError;
}

function sendBuffer(res: ServerResponse, pathName: string, body: Buffer) {
    res.statusCode = 200;
    res.setHeader(
        "content-type",
        pathName.endsWith(".json") ? "application/json; charset=utf-8" : "application/octet-stream",
    );
    res.setHeader("content-length", body.byteLength);
    res.end(body);
}

async function requestBuffer(req: NodeJS.ReadableStream) {
    const chunks: Buffer[] = [];
    for await (const chunk of req) {
        chunks.push(Buffer.from(chunk as Buffer));
    }
    return Buffer.concat(chunks);
}

describe("embedded Helia IPFS relay plugin", () => {
    const relays: RelayServer[] = [];
    const tempDirs: string[] = [];

    afterEach(async () => {
        await Promise.all(relays.splice(0).map((relay) => relay.close().catch(() => undefined)));
        await Promise.all(tempDirs.splice(0).map((dir) => removeDirEventually(dir)));
    });

    it("adds and pins bytes through the relay HTTP API", async () => {
        const storeDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-helia-ipfs-"));
        tempDirs.push(storeDir);
        const relay = new RelayServer(
            0,
            new MemoryStore(),
            [createHeliaIpfsPlugin({ storeDir, exposeHttpRoutes: true })],
            { enableDefaultPlugins: false },
        );
        relays.push(relay);

        const port = await waitForPort(relay);
        const bytes = Buffer.from("hello helia from cgp tests");
        const response = await fetch(`http://127.0.0.1:${port}/plugins/cgp.ipfs.helia/add`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                bytesBase64: bytes.toString("base64"),
                name: "hello.txt",
                mimeType: "text/plain",
                sha256: sha256(bytes),
            }),
        });
        const payload = await response.json() as any;
        expect(response.status, JSON.stringify(payload)).toBe(201);
        expect(payload).toMatchObject({
            ok: true,
            providerId: "helia",
            backend: "helia",
            bytes: bytes.byteLength,
            sha256: sha256(bytes),
        });
        expect(payload.cid).toMatch(/^baf/);
        expect(payload.gatewayUrl).toContain(payload.cid);

        const statusResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.ipfs.helia/status`);
        const status = await statusResponse.json() as any;
        expect(status.backend).toMatchObject({
            id: "helia",
            kind: "helia",
            started: true,
            pins: 1,
        });
        expect(status.backend.peerId).toMatch(/^12D3/);

        const catResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.ipfs.helia/ipfs/${payload.cid}`);
        expect(catResponse.status).toBe(200);
        expect(catResponse.headers.get("content-type")).toBe("text/plain");
        expect(await catResponse.text()).toBe(bytes.toString("utf8"));
    }, 15_000);

    it("fetches a pinned game shard across two networked Helia relay plugins", async () => {
        const providerStore = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-helia-provider-"));
        const consumerStore = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-helia-consumer-"));
        tempDirs.push(providerStore, consumerStore);
        const provider = new RelayServer(
            0,
            new MemoryStore(),
            [
                createHeliaIpfsPlugin({
                    storeDir: providerStore,
                    mode: "network",
                    listenAddrs: ["/ip4/127.0.0.1/tcp/0"],
                    useDefaultBootstrap: false,
                    bootstrapAddrs: [],
                    enableDht: true,
                    enableTrustlessGateway: false,
                    provideOnAdd: false,
                    exposeHttpRoutes: true,
                }),
            ],
            { enableDefaultPlugins: false },
        );
        const consumer = new RelayServer(
            0,
            new MemoryStore(),
            [
                createHeliaIpfsPlugin({
                    storeDir: consumerStore,
                    mode: "network",
                    listenAddrs: [],
                    useDefaultBootstrap: false,
                    bootstrapAddrs: [],
                    enableDht: false,
                    enableTrustlessGateway: false,
                    provideOnAdd: false,
                    exposeHttpRoutes: true,
                }),
            ],
            { enableDefaultPlugins: false },
        );
        relays.push(provider, consumer);

        const providerPort = await waitForPort(provider);
        const consumerPort = await waitForPort(consumer);
        const bytes = Buffer.from("networked game shard bytes");
        const addResponse = await fetch(`http://127.0.0.1:${providerPort}/plugins/cgp.ipfs.helia/add`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                bytesBase64: bytes.toString("base64"),
                name: "source-000.zip",
                mimeType: "application/zip",
                sha256: sha256(bytes),
                metadata: {
                    kind: "cgp-static-shard",
                    release: "network-game@0.1.0",
                    shardId: "source-000",
                },
            }),
        });
        const added = await addResponse.json() as any;
        expect(addResponse.status, JSON.stringify(added)).toBe(201);

        const providerStatusResponse = await fetch(`http://127.0.0.1:${providerPort}/plugins/cgp.ipfs.helia/status`);
        const providerStatus = await providerStatusResponse.json() as any;
        const providerAddr = providerStatus.backend.multiaddrs?.[0];
        expect(providerAddr, JSON.stringify(providerStatus)).toMatch(/\/p2p\//);

        const connectResponse = await fetch(`http://127.0.0.1:${consumerPort}/plugins/cgp.ipfs.helia/peers/connect`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ multiaddr: providerAddr }),
        });
        const connected = await connectResponse.json() as any;
        expect(connectResponse.status, JSON.stringify(connected)).toBe(200);

        const catResponse = await fetch(
            `http://127.0.0.1:${consumerPort}/plugins/cgp.ipfs.helia/ipfs/${added.cid}?offline=0&timeoutMs=10000`,
        );
        const catBody = Buffer.from(await catResponse.arrayBuffer());
        expect(catResponse.status, catBody.toString("utf8")).toBe(200);
        expect(catBody.toString("utf8")).toBe(bytes.toString("utf8"));
    });

    it("mirrors a forkable game release into CGP objects and Helia-hosted shard CIDs", async () => {
        const shard = Buffer.from("helia hosted forkable game source shard");
        const gameManifest = Buffer.from(JSON.stringify({
            kind: "hollow-game",
            id: "helia-game",
            title: "Helia Game",
            version: "0.1.0",
        }));
        const release = {
            kind: "cgp-static-shard-release",
            schemaVersion: 1,
            id: "helia-game",
            title: "Helia Game",
            type: "game",
            version: "0.1.0",
            manifests: {
                game: {
                    path: "manifests/hollow.game.json",
                    sha256: sha256(gameManifest),
                },
            },
            shards: [
                {
                    id: "source-000",
                    kind: "source",
                    path: "source-000.zip",
                    bytes: shard.byteLength,
                    sha256: sha256(shard),
                },
            ],
        };
        const releaseBody = Buffer.from(JSON.stringify(release));
        const files = new Map<string, Buffer>([
            ["/release.json", releaseBody],
            ["/manifests/hollow.game.json", gameManifest],
            ["/source-000.zip", shard],
        ]);
        const origin = createServer((req, res) => {
            const pathName = new URL(req.url || "/", "http://localhost").pathname;
            const body = files.get(pathName);
            if (!body) {
                res.statusCode = 404;
                res.end("not found");
                return;
            }
            sendBuffer(res, pathName, body);
        });
        await new Promise<void>((resolve) => origin.listen(0, "127.0.0.1", resolve));
        const address = origin.address();
        if (!address || typeof address === "string") throw new Error("origin did not bind");

        const shardStoreDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-static-helia-game-"));
        const heliaStoreDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-static-helia-ipfs-"));
        tempDirs.push(shardStoreDir, heliaStoreDir);
        const store = new MemoryStore();
        const relay = new RelayServer(
            0,
            store,
            [
                createHeliaIpfsPlugin({
                    storeDir: heliaStoreDir,
                    exposeHttpRoutes: true,
                }),
                createStaticShardSeedPlugin({
                    sources: [{
                        url: `http://127.0.0.1:${address.port}/release.json`,
                        kind: "release",
                        expectedSha256: sha256(releaseBody),
                    }],
                    storeDir: shardStoreDir,
                    autoIngest: true,
                    pinToIpfs: true,
                    ipfsBackendId: "helia",
                    extractPlayable: false,
                }),
            ],
            { enableDefaultPlugins: false },
        );
        relays.push(relay);

        try {
            const port = await waitForPort(relay);
            const statusResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/status`);
            const status = await statusResponse.json() as any;
            const mirroredShard = status.releases[0].shards[0];
            expect(mirroredShard.ipfsCid).toMatch(/^baf/);

            const heliaResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.ipfs.helia/ipfs/${mirroredShard.ipfsCid}`);
            expect(heliaResponse.status).toBe(200);
            expect(Buffer.from(await heliaResponse.arrayBuffer()).toString("utf8")).toBe(shard.toString("utf8"));

            const guildId = hashObject({ kind: "cgp-static-shard-game-guild", id: "helia-game" });
            const log = await store.getLog(guildId);
            const releaseObject = log.find((event) =>
                event.body.type === "APP_OBJECT_UPSERT" &&
                (event.body as any).objectType === "game-release" &&
                (event.body as any).objectId === "helia-game@0.1.0"
            );
            const value = (releaseObject?.body as any)?.value;
            expect(value?.ipfsHosting).toMatchObject({
                scheme: "ipfs",
                available: true,
            });
            expect(value?.ipfsHosting.shards[0]).toMatchObject({
                id: "source-000",
                cid: mirroredShard.ipfsCid,
                uri: `ipfs://${mirroredShard.ipfsCid}`,
            });
            expect(value?.forkSource).toMatchObject({
                strategy: "snapshot-shards",
                guarantee: "release-and-shard-sha256",
                releaseSha256: sha256(releaseBody),
            });
            expect(value?.forkSource.shards[0]).toMatchObject({
                id: "source-000",
                ipfsCid: mirroredShard.ipfsCid,
                ipfsUri: `ipfs://${mirroredShard.ipfsCid}`,
            });
        } finally {
            await new Promise<void>((resolve) => origin.close(() => resolve()));
        }
    });

    it("lets static shard seeding pin through a registered CGP IPFS backend", async () => {
        const shard = Buffer.from("static shard through cgp ipfs backend");
        const release = {
            kind: "cgp-static-shard-release",
            schemaVersion: 1,
            id: "backend-game",
            title: "Backend Game",
            type: "game",
            version: "0.1.0",
            shards: [
                {
                    id: "source-000",
                    kind: "source",
                    path: "source-000.zip",
                    bytes: shard.byteLength,
                    sha256: sha256(shard),
                },
            ],
        };
        const releaseBody = Buffer.from(JSON.stringify(release));
        const files = new Map<string, Buffer>([
            ["/release.json", releaseBody],
            ["/source-000.zip", shard],
        ]);
        const origin = createServer((req, res) => {
            const pathName = new URL(req.url || "/", "http://localhost").pathname;
            const body = files.get(pathName);
            if (!body) {
                res.statusCode = 404;
                res.end("not found");
                return;
            }
            sendBuffer(res, pathName, body);
        });
        await new Promise<void>((resolve) => origin.listen(0, "127.0.0.1", resolve));
        const address = origin.address();
        if (!address || typeof address === "string") throw new Error("origin did not bind");

        const added: Array<{ path?: string; sha256?: string; metadata?: Record<string, string | number | boolean> }> = [];
        const backend: CgpIpfsBackend = {
            id: "test-ipfs",
            kind: "external",
            addFile: async (input) => {
                added.push({
                    path: input.path,
                    sha256: input.sha256,
                    metadata: input.metadata,
                });
                return {
                    providerId: "test-ipfs",
                    backend: "external",
                    cid: "bafytestbackend0",
                    bytes: shard.byteLength,
                    sha256: input.sha256 || "",
                    gatewayUrl: "https://gateway.example/ipfs/bafytestbackend0",
                };
            },
            pin: async () => undefined,
            status: async () => ({
                id: "test-ipfs",
                kind: "external",
                started: true,
            }),
        };
        const backendPlugin: RelayPlugin = {
            name: "test.ipfs.backend",
            onInit: async (ctx) => {
                ctx.ipfsBackends?.set(backend.id, backend);
            },
        };
        const storeDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-shard-ipfs-backend-"));
        tempDirs.push(storeDir);
        const relay = new RelayServer(
            0,
            new MemoryStore(),
            [
                backendPlugin,
                createStaticShardSeedPlugin({
                    sources: [{
                        url: `http://127.0.0.1:${address.port}/release.json`,
                        kind: "release",
                        expectedSha256: sha256(releaseBody),
                    }],
                    storeDir,
                    autoIngest: true,
                    pinToIpfs: true,
                    ipfsBackendId: "test-ipfs",
                    extractPlayable: false,
                }),
            ],
            { enableDefaultPlugins: false },
        );
        relays.push(relay);

        try {
            const port = await waitForPort(relay);
            const statusResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/status`);
            const status = await statusResponse.json() as any;
            expect(status.releases[0].shards[0]).toMatchObject({
                id: "source-000",
                ipfsCid: "bafytestbackend0",
                ipfsGatewayUrl: "https://gateway.example/ipfs/bafytestbackend0",
            });
            expect(added).toHaveLength(1);
            expect(added[0].sha256).toBe(sha256(shard));
            expect(added[0].metadata).toMatchObject({
                kind: "cgp-static-shard",
                release: "backend-game@0.1.0",
                shardId: "source-000",
                shardKind: "source",
            });
        } finally {
            await new Promise<void>((resolve) => origin.close(() => resolve()));
        }
    });

    it("exposes a faux IPFS backend over R2/S3-style object URLs", async () => {
        const objects = new Map<string, Buffer>();
        const origin = createServer(async (req, res) => {
            const pathName = new URL(req.url || "/", "http://localhost").pathname;
            const match = /^\/objects\/([^/]+)$/.exec(pathName);
            if (!match) {
                res.statusCode = 404;
                res.end("not found");
                return;
            }
            const cid = match[1];
            if (req.method === "PUT") {
                objects.set(cid, await requestBuffer(req));
                res.statusCode = 200;
                res.end("ok");
                return;
            }
            if (req.method === "GET") {
                const body = objects.get(cid);
                if (!body) {
                    res.statusCode = 404;
                    res.end("not found");
                    return;
                }
                sendBuffer(res, pathName, body);
                return;
            }
            res.statusCode = 405;
            res.end("method not allowed");
        });
        await new Promise<void>((resolve) => origin.listen(0, "127.0.0.1", resolve));
        const address = origin.address();
        if (!address || typeof address === "string") throw new Error("origin did not bind");

        const baseUrl = `http://127.0.0.1:${address.port}`;
        const relay = new RelayServer(
            0,
            new MemoryStore(),
            [
                createFauxIpfsBackendPlugin({
                    id: "faux",
                    storage: "r2",
                    putUrlTemplate: `${baseUrl}/objects/{cid}`,
                    getUrlTemplate: `${baseUrl}/objects/{cid}`,
                    gatewayUrl: `${baseUrl}/objects/{cid}`,
                    exposeHttpRoutes: true,
                }),
            ],
            { enableDefaultPlugins: false },
        );
        relays.push(relay);

        try {
            const port = await waitForPort(relay);
            const bytes = Buffer.from("serverless object store bytes");
            const addResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.ipfs.faux/add`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    bytesBase64: bytes.toString("base64"),
                    name: "object.bin",
                    mimeType: "application/octet-stream",
                    sha256: sha256(bytes),
                }),
            });
            const added = await addResponse.json() as any;
            expect(addResponse.status, JSON.stringify(added)).toBe(201);
            expect(added.backend).toBe("faux");
            expect(added.cid).toMatch(/^baf/);
            expect(added.storage).toMatchObject({ kind: "r2", syntheticIpfs: true });
            expect(objects.get(added.cid)?.toString("utf8")).toBe(bytes.toString("utf8"));

            const ipfsResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.ipfs.faux/ipfs/${added.cid}`);
            expect(ipfsResponse.status).toBe(200);
            expect(ipfsResponse.headers.get("x-cgp-faux-ipfs")).toBe("1");
            expect(Buffer.from(await ipfsResponse.arrayBuffer()).toString("utf8")).toBe(bytes.toString("utf8"));

            const statusResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.ipfs.faux/status`);
            const status = await statusResponse.json() as any;
            expect(status.backend).toMatchObject({
                id: "faux",
                kind: "faux",
                storage: "r2",
                objects: 1,
            });
        } finally {
            await new Promise<void>((resolve) => origin.close(() => resolve()));
        }
    });

    it("signs native R2/S3-compatible requests and can fetch objects after relay restart", async () => {
        const objects = new Map<string, Buffer>();
        const signedRequests: Array<{ method?: string; path?: string; authorization?: string; amzDate?: string }> = [];
        const origin = createServer(async (req, res) => {
            const pathName = new URL(req.url || "/", "http://localhost").pathname;
            const match = /^\/hollow-bucket\/media\/([^/]+)$/.exec(pathName);
            if (!match) {
                res.statusCode = 404;
                res.end("not found");
                return;
            }
            signedRequests.push({
                method: req.method,
                path: pathName,
                authorization: String(req.headers.authorization || ""),
                amzDate: String(req.headers["x-amz-date"] || ""),
            });
            const cid = match[1];
            if (req.method === "PUT") {
                objects.set(cid, await requestBuffer(req));
                res.statusCode = 200;
                res.end("ok");
                return;
            }
            if (req.method === "GET") {
                const body = objects.get(cid);
                if (!body) {
                    res.statusCode = 404;
                    res.end("not found");
                    return;
                }
                sendBuffer(res, pathName, body);
                return;
            }
            res.statusCode = 405;
            res.end("method not allowed");
        });
        await new Promise<void>((resolve) => origin.listen(0, "127.0.0.1", resolve));
        const address = origin.address();
        if (!address || typeof address === "string") throw new Error("origin did not bind");

        const baseUrl = `http://127.0.0.1:${address.port}`;
        const makePlugin = () => createFauxIpfsBackendPlugin({
            id: "r2-native",
            storage: "r2",
            s3Endpoint: baseUrl,
            s3Bucket: "hollow-bucket",
            s3Region: "auto",
            s3AccessKeyId: "test-access",
            s3SecretAccessKey: "test-secret",
            s3ForcePathStyle: true,
            s3PublicBaseUrl: `${baseUrl}/public`,
            keyPrefix: "media",
            exposeHttpRoutes: true,
        });
        const relay = new RelayServer(
            0,
            new MemoryStore(),
            [makePlugin()],
            { enableDefaultPlugins: false },
        );
        relays.push(relay);

        try {
            const port = await waitForPort(relay);
            const bytes = Buffer.from("native r2 signed bytes");
            const addResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.ipfs.faux/add`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    bytesBase64: bytes.toString("base64"),
                    name: "r2.bin",
                    mimeType: "application/octet-stream",
                    sha256: sha256(bytes),
                }),
            });
            const added = await addResponse.json() as any;
            expect(addResponse.status, JSON.stringify(added)).toBe(201);
            expect(added.storage).toMatchObject({
                kind: "r2",
                key: `media/${added.cid}`,
                syntheticIpfs: true,
            });
            expect(added.gatewayUrl).toBe(`${baseUrl}/public/media/${added.cid}`);
            expect(signedRequests[0]).toMatchObject({
                method: "PUT",
                path: `/hollow-bucket/media/${added.cid}`,
            });
            expect(signedRequests[0].authorization).toContain("AWS4-HMAC-SHA256");
            expect(signedRequests[0].amzDate).toMatch(/^\d{8}T\d{6}Z$/);

            const restartedRelay = new RelayServer(
                0,
                new MemoryStore(),
                [makePlugin()],
                { enableDefaultPlugins: false },
            );
            relays.push(restartedRelay);
            const restartedPort = await waitForPort(restartedRelay);
            const ipfsResponse = await fetch(`http://127.0.0.1:${restartedPort}/plugins/cgp.ipfs.faux/ipfs/${added.cid}`);
            expect(ipfsResponse.status).toBe(200);
            expect(ipfsResponse.headers.get("x-cgp-faux-ipfs")).toBe("1");
            expect(Buffer.from(await ipfsResponse.arrayBuffer()).toString("utf8")).toBe(bytes.toString("utf8"));
            expect(signedRequests.some((entry) => entry.method === "GET" && entry.path === `/hollow-bucket/media/${added.cid}`)).toBe(true);
        } finally {
            await new Promise<void>((resolve) => origin.close(() => resolve()));
        }
    });

    it("can back faux IPFS objects with the GitHub contents API", async () => {
        const originalFetch = globalThis.fetch;
        const githubObjects = new Map<string, Buffer>();
        const uploads: string[] = [];
        vi.stubGlobal("fetch", async (url: string | URL | Request, init?: RequestInit) => {
            const href = String(url);
            if (!href.startsWith("https://api.github.com/")) {
                return originalFetch(url, init);
            }
            const objectUrl = href.split("?")[0];
            if (init?.method === "PUT") {
                const body = JSON.parse(String(init.body));
                githubObjects.set(objectUrl, Buffer.from(String(body.content), "base64"));
                uploads.push(objectUrl);
                return new Response(JSON.stringify({ content: { path: objectUrl } }), { status: 200 });
            }
            const stored = githubObjects.get(objectUrl);
            if (!stored) {
                return new Response("{}", { status: 404 });
            }
            return new Response(JSON.stringify({ content: stored.toString("base64") }), { status: 200 });
        });

        const relay = new RelayServer(
            0,
            new MemoryStore(),
            [
                createFauxIpfsBackendPlugin({
                    id: "github-faux",
                    storage: "github",
                    githubRepository: "cgp-test/faux-ipfs",
                    githubToken: "test-token",
                    githubBranch: "main",
                    githubBasePath: "objects",
                    exposeHttpRoutes: true,
                }),
            ],
            { enableDefaultPlugins: false },
        );
        relays.push(relay);

        try {
            const port = await waitForPort(relay);
            const bytes = Buffer.from("github-backed object bytes");
            const addResponse = await originalFetch(`http://127.0.0.1:${port}/plugins/cgp.ipfs.faux/add`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    bytesBase64: bytes.toString("base64"),
                    name: "github-object.bin",
                    mimeType: "application/octet-stream",
                    sha256: sha256(bytes),
                }),
            });
            const added = await addResponse.json() as any;
            expect(addResponse.status, JSON.stringify(added)).toBe(201);
            expect(added.backend).toBe("faux");
            expect(added.storage).toMatchObject({ kind: "github", syntheticIpfs: true });
            expect(uploads[0]).toContain("/repos/cgp-test/faux-ipfs/contents/objects/");

            const ipfsResponse = await originalFetch(`http://127.0.0.1:${port}/plugins/cgp.ipfs.faux/ipfs/${added.cid}`);
            expect(ipfsResponse.status).toBe(200);
            expect(ipfsResponse.headers.get("x-cgp-faux-ipfs")).toBe("1");
            expect(Buffer.from(await ipfsResponse.arrayBuffer()).toString("utf8")).toBe(bytes.toString("utf8"));
        } finally {
            vi.unstubAllGlobals();
        }
    });

    it("can back faux IPFS GitHub storage with GitHub App installation auth", async () => {
        const originalFetch = globalThis.fetch;
        const { privateKey } = generateKeyPairSync("rsa", {
            modulusLength: 2048,
            privateKeyEncoding: { type: "pkcs8", format: "pem" },
            publicKeyEncoding: { type: "spki", format: "pem" },
        });
        const githubObjects = new Map<string, Buffer>();
        const installationTokenRequests: Array<{ auth: string; body: any }> = [];
        const uploadTokens: string[] = [];
        vi.stubGlobal("fetch", async (url: string | URL | Request, init?: RequestInit) => {
            const href = String(url);
            if (!href.startsWith("https://api.github.com/")) {
                return originalFetch(url, init);
            }
            if (href.endsWith("/app/installations/987654/access_tokens")) {
                installationTokenRequests.push({
                    auth: String((init?.headers as Record<string, string>)?.authorization || ""),
                    body: JSON.parse(String(init?.body || "{}")),
                });
                return new Response(JSON.stringify({
                    token: "faux-installation-token",
                    expires_at: new Date(Date.now() + 60 * 60_000).toISOString(),
                }), { status: 201 });
            }
            const objectUrl = href.split("?")[0];
            if (init?.method === "PUT") {
                uploadTokens.push(String((init.headers as Record<string, string>).authorization || ""));
                const body = JSON.parse(String(init.body));
                githubObjects.set(objectUrl, Buffer.from(String(body.content), "base64"));
                return new Response(JSON.stringify({ content: { path: objectUrl } }), { status: 200 });
            }
            const stored = githubObjects.get(objectUrl);
            if (!stored) {
                return new Response("{}", { status: 404 });
            }
            return new Response(JSON.stringify({ content: stored.toString("base64") }), { status: 200 });
        });

        const relay = new RelayServer(
            0,
            new MemoryStore(),
            [
                createFauxIpfsBackendPlugin({
                    id: "github-app-faux",
                    storage: "github",
                    githubRepository: "cgp-test/faux-ipfs",
                    githubAppId: "12345",
                    githubAppPrivateKey: String(privateKey),
                    githubAppInstallationId: "987654",
                    githubBranch: "main",
                    githubBasePath: "objects",
                    exposeHttpRoutes: true,
                }),
            ],
            { enableDefaultPlugins: false },
        );
        relays.push(relay);

        try {
            const port = await waitForPort(relay);
            const bytes = Buffer.from("github-app-backed object bytes");
            const addResponse = await originalFetch(`http://127.0.0.1:${port}/plugins/cgp.ipfs.faux/add`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    bytesBase64: bytes.toString("base64"),
                    name: "github-app-object.bin",
                    mimeType: "application/octet-stream",
                    sha256: sha256(bytes),
                }),
            });
            const added = await addResponse.json() as any;
            expect(addResponse.status, JSON.stringify(added)).toBe(201);
            expect(installationTokenRequests).toHaveLength(1);
            expect(installationTokenRequests[0].auth.split(" ")[1].split(".")).toHaveLength(3);
            expect(installationTokenRequests[0].body).toEqual({ permissions: { contents: "write" } });
            expect(uploadTokens).toEqual(["Bearer faux-installation-token"]);

            const ipfsResponse = await originalFetch(`http://127.0.0.1:${port}/plugins/cgp.ipfs.faux/ipfs/${added.cid}`);
            expect(ipfsResponse.status).toBe(200);
            expect(Buffer.from(await ipfsResponse.arrayBuffer()).toString("utf8")).toBe(bytes.toString("utf8"));
            expect(installationTokenRequests).toHaveLength(1);
        } finally {
            vi.unstubAllGlobals();
        }
    });
});
