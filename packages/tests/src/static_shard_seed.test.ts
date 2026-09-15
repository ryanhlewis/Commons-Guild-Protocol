import { createHash } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
    DeviceAuthorityRegistry,
    generatePrivateKey,
    getPublicKey,
    hashObject,
    sign,
    type DeviceAuthorization,
} from "@cgp/core";
import {
    STATIC_SHARD_PUBLISHER_PROTOCOL,
    createStaticShardSeedPlugin,
    staticShardReleaseSigningPayload,
    verifyStaticShardReleasePublisher,
} from "@cgp/relay/src/plugins";
import { RelayServer } from "@cgp/relay/src/server";
import { MemoryStore } from "@cgp/relay/src/store";
import { zipSync } from "fflate";

function sha256(data: Buffer | string) {
    return createHash("sha256").update(data).digest("hex");
}

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function zipTextFiles(files: Record<string, string>) {
    const encoded: Record<string, Uint8Array> = {};
    for (const [name, value] of Object.entries(files)) {
        encoded[name] = new TextEncoder().encode(value);
    }
    return Buffer.from(zipSync(encoded));
}

async function signStaticShardRelease(
    release: Record<string, unknown>,
    privateKey: Uint8Array,
) {
    const publicKey = getPublicKey(privateKey);
    const unsigned = {
        ...release,
        publisher: {
            protocol: STATIC_SHARD_PUBLISHER_PROTOCOL,
            publicKey,
        },
    };
    const signature = await sign(
        privateKey,
        hashObject(staticShardReleaseSigningPayload(unsigned)),
    );
    return {
        ...unsigned,
        publisher: {
            ...unsigned.publisher,
            signature,
        },
    };
}

async function signDelegatedStaticShardRelease(release: Record<string, unknown>) {
    const now = Date.now();
    const accountPrivateKey = generatePrivateKey();
    const authorityPrivateKey = generatePrivateKey();
    const devicePrivateKey = generatePrivateKey();
    const accountPublicKey = getPublicKey(accountPrivateKey);
    const authorityPublicKey = getPublicKey(authorityPrivateKey);
    const devicePublicKey = getPublicKey(devicePrivateKey);
    const bindingUnsigned = {
        protocol: "cgp/device-authority/1" as const,
        accountPublicKey,
        authorityPublicKey,
        generation: 1,
        activatedAt: now - 1_000,
    };
    const certificateUnsigned = {
        protocol: "cgp/device-certificate/1" as const,
        accountPublicKey,
        authorityPublicKey,
        devicePublicKey,
        serial: "42".repeat(16),
        label: "Publisher test device",
        capabilities: ["publish", "read"] as const,
        issuedAt: now - 500,
        expiresAt: now + 60_000,
    };
    const revocationUnsigned = {
        protocol: "cgp/device-revocation/1" as const,
        accountPublicKey,
        authorityPublicKey,
        generation: 1,
        epoch: 0,
        updatedAt: now - 250,
        revokedSerials: [],
    };
    const authorization = {
        protocol: "cgp/device-authorization/1" as const,
        binding: {
            ...bindingUnsigned,
            signature: await sign(accountPrivateKey, hashObject(bindingUnsigned)),
        },
        certificate: {
            ...certificateUnsigned,
            capabilities: [...certificateUnsigned.capabilities],
            signature: await sign(authorityPrivateKey, hashObject(certificateUnsigned)),
        },
        revocation: {
            ...revocationUnsigned,
            signature: await sign(authorityPrivateKey, hashObject(revocationUnsigned)),
        },
    } satisfies DeviceAuthorization;
    const unsigned = {
        ...release,
        publisher: {
            protocol: STATIC_SHARD_PUBLISHER_PROTOCOL,
            publicKey: accountPublicKey,
        },
    };
    const payloadHash = hashObject(staticShardReleaseSigningPayload(unsigned));
    const signature = await sign(
        devicePrivateKey,
        hashObject({ payload: payloadHash, deviceAuthorization: authorization }),
    );
    return {
        release: {
            ...unsigned,
            publisher: {
                ...unsigned.publisher,
                signature,
                deviceAuthorization: authorization,
            },
        },
        accountPublicKey,
        authorization,
        authorityPrivateKey,
        devicePrivateKey,
    };
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

function sendBuffer(res: ServerResponse, pathName: string, body: Buffer) {
    res.statusCode = 200;
    res.setHeader(
        "content-type",
        pathName.endsWith(".json")
            ? "application/json; charset=utf-8"
            : pathName.endsWith(".zip")
                ? "application/zip"
                : "application/octet-stream",
    );
    res.setHeader("content-length", body.byteLength);
    res.end(body);
}

async function drainRequest(req: IncomingMessage) {
    for await (const _chunk of req) {
        // Drain upload body.
    }
}

describe("static shard seed plugin", () => {
    const relays: RelayServer[] = [];
    const tempDirs: string[] = [];

    afterEach(async () => {
        await Promise.all(relays.splice(0).map((relay) => relay.close().catch(() => undefined)));
        await Promise.all(tempDirs.splice(0).map((dir) => fs.rm(dir, { recursive: true, force: true })));
    });

    it("shares Hollow's canonical publisher payload test vector", () => {
        const publicKey = `02${"11".repeat(32)}`;
        const value = {
            kind: "cgp-static-shard-release",
            schemaVersion: 1,
            id: "example-game",
            title: "Example Game",
            version: "0.1.0",
            manifests: {},
            shards: [],
            publisher: { protocol: STATIC_SHARD_PUBLISHER_PROTOCOL, publicKey },
        };
        expect(hashObject(staticShardReleaseSigningPayload(value))).toBe(
            "3ec2019051d857282b3da036a793c53c8efc2dcc782b6e50dcbd9b5cadc0b7d1",
        );
    });

    it("accepts a release signed by a root-authorized publishing device", async () => {
        const delegated = await signDelegatedStaticShardRelease({
            kind: "cgp-static-shard-release",
            schemaVersion: 1,
            id: "delegated-game",
            title: "Delegated Game",
            type: "game",
            version: "0.1.0",
            manifests: {},
            shards: [],
        });

        expect(verifyStaticShardReleasePublisher(delegated.release)).toMatchObject({
            protocol: STATIC_SHARD_PUBLISHER_PROTOCOL,
            publicKey: delegated.accountPublicKey,
            deviceAuthorization: {
                protocol: "cgp/device-authorization/1",
            },
        });

        const tampered = structuredClone(delegated.release);
        tampered.title = "Attacker title";
        expect(() => verifyStaticShardReleasePublisher(tampered)).toThrow(
            "delegated publisher signature is invalid",
        );

        const registry = new DeviceAuthorityRegistry();
        expect(verifyStaticShardReleasePublisher(delegated.release, {
            deviceAuthorityRegistry: registry,
        })).toBeDefined();
        const revocationUnsigned = {
            protocol: "cgp/device-revocation/1" as const,
            accountPublicKey: delegated.accountPublicKey,
            authorityPublicKey: delegated.authorization.binding.authorityPublicKey,
            generation: delegated.authorization.binding.generation,
            epoch: 1,
            updatedAt: Date.now(),
            revokedSerials: [],
        };
        const newerAuthorization = {
            ...delegated.authorization,
            revocation: {
                ...revocationUnsigned,
                signature: await sign(
                    delegated.authorityPrivateKey,
                    hashObject(revocationUnsigned),
                ),
            },
        };
        const nextUnsigned = {
            ...delegated.release,
            version: "0.1.1",
            publisher: {
                protocol: STATIC_SHARD_PUBLISHER_PROTOCOL,
                publicKey: delegated.accountPublicKey,
            },
        };
        const nextPayloadHash = hashObject(staticShardReleaseSigningPayload(nextUnsigned));
        const nextSignature = await sign(
            delegated.devicePrivateKey,
            hashObject({ payload: nextPayloadHash, deviceAuthorization: newerAuthorization }),
        );
        const nextRelease = {
            ...nextUnsigned,
            publisher: {
                ...nextUnsigned.publisher,
                signature: nextSignature,
                deviceAuthorization: newerAuthorization,
            },
        };
        expect(verifyStaticShardReleasePublisher(nextRelease, {
            deviceAuthorityRegistry: registry,
        })).toBeDefined();
        expect(() => verifyStaticShardReleasePublisher(delegated.release, {
            deviceAuthorityRegistry: registry,
        })).toThrow("stale revocation epoch");
    });

    it("uploads and restores a delegated-device release under its account owner", async () => {
        const shard = zipTextFiles({ "index.html": "<!doctype html><title>Device Game</title>" });
        const manifest = Buffer.from(JSON.stringify({
            id: "device-game",
            title: "Device Game",
            version: "0.1.0",
            entry: { type: "web", localPath: "index.html" },
        }));
        const delegated = await signDelegatedStaticShardRelease({
            kind: "cgp-static-shard-release",
            schemaVersion: 1,
            id: "device-game",
            title: "Device Game",
            type: "game",
            version: "0.1.0",
            manifests: {
                game: { path: "manifests/hollow.game.json", sha256: sha256(manifest) },
            },
            shardPolicy: { maxShardBytes: 25 * 1024 * 1024, format: "zip", hash: "sha256" },
            shards: [{
                id: "source-000",
                kind: "source",
                path: "source-000.zip",
                bytes: shard.byteLength,
                sha256: sha256(shard),
            }],
        });
        const releaseBody = Buffer.from(JSON.stringify(delegated.release));
        const storeDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-delegated-upload-"));
        tempDirs.push(storeDir);
        const startRelay = () => {
            const relay = new RelayServer(
                0,
                new MemoryStore(),
                [createStaticShardSeedPlugin({ storeDir, autoIngest: false })],
                { enableDefaultPlugins: false },
            );
            relays.push(relay);
            return relay;
        };
        const relay = startRelay();
        const port = await waitForPort(relay);
        const uploadResponse = await fetch(
            `http://127.0.0.1:${port}/plugins/cgp.static-shards/upload`,
            {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    releaseBase64: releaseBody.toString("base64"),
                    releaseSha256: sha256(releaseBody),
                    files: [
                        {
                            path: "manifests/hollow.game.json",
                            bytesBase64: manifest.toString("base64"),
                            sha256: sha256(manifest),
                        },
                        {
                            path: "source-000.zip",
                            bytesBase64: shard.toString("base64"),
                            sha256: sha256(shard),
                        },
                    ],
                }),
            },
        );
        const uploaded = await uploadResponse.json() as any;
        expect(uploadResponse.status, JSON.stringify(uploaded)).toBe(201);
        expect(uploaded.release.publisher).toMatchObject({
            publicKey: delegated.accountPublicKey,
            deviceAuthorization: { protocol: "cgp/device-authorization/1" },
        });

        await relay.close();
        const restarted = startRelay();
        const restartedPort = await waitForPort(restarted);
        const catalog = await (
            await fetch(`http://127.0.0.1:${restartedPort}/plugins/cgp.static-shards/catalog?id=device-game`)
        ).json() as any;
        expect(catalog.releases[0].publisher).toMatchObject({
            publicKey: delegated.accountPublicKey,
            deviceAuthorization: { protocol: "cgp/device-authorization/1" },
        });
    });

    it("mirrors verified static shards, pins them through IPFS, and registers a game guild", async () => {
        const sourceShard = zipTextFiles({
            "index.html": "<!doctype html><title>Avera</title><script src=\"game.js\"></script>",
            "game.js": "window.__averaLoaded = true;",
            "_next/static/chunks/webpack.js": "window.__averaNextChunkLoaded = true;"
        });
        const assetShard = zipTextFiles({
            "assets/readme.txt": "asset shard bytes",
            "assets/thumb.png": "fake png bytes",
            "models/Wuhu_Island.glb": "fake model bytes"
        });
        const gameManifest = Buffer.from(JSON.stringify({
            id: "avera",
            title: "Avera",
            version: "0.1.0",
            summary: "Forkable voxel game runtime.",
            entry: { type: "web", localPath: "index.html" },
            launch: {
                web: {
                    params: {
                        world: "samples/starter-haven.litematic",
                        playerView: "1",
                        hideChrome: "1"
                    }
                }
            },
            media: { thumbnail: "assets/thumb.png" },
            display: { aspectRatio: "1 / 1", viewport: { width: 750, height: 750 } },
            creator: { id: "avera-studio", username: "avera-studio", name: "Avera Studio" },
            tags: ["voxel", "sandbox"]
        }));
        const publisherPrivateKey = generatePrivateKey();
        const publisherPublicKey = getPublicKey(publisherPrivateKey);
        const release = await signStaticShardRelease({
            kind: "cgp-static-shard-release",
            schemaVersion: 1,
            id: "avera",
            title: "Avera",
            type: "game",
            version: "0.1.0",
            manifests: {
                game: {
                    path: "manifests/hollow.game.json",
                    sha256: sha256(gameManifest)
                }
            },
            shardPolicy: {
                maxShardBytes: 25 * 1024 * 1024,
                format: "zip",
                hash: "sha256"
            },
            shards: [
                {
                    id: "source-000",
                    kind: "source",
                    path: "source-000.zip",
                    bytes: sourceShard.byteLength,
                    sha256: sha256(sourceShard)
                },
                {
                    id: "assets-000",
                    kind: "assets",
                    path: "assets-000.zip",
                    bytes: assetShard.byteLength,
                    sha256: sha256(assetShard)
                }
            ]
        }, publisherPrivateKey);
        const releaseBody = Buffer.from(JSON.stringify(release));
        const releaseHash = sha256(releaseBody);
        const catalog = Buffer.from(JSON.stringify({
            kind: "cgp-static-shard-catalog",
            schemaVersion: 1,
            projects: [
                {
                    id: "avera",
                    title: "Avera",
                    type: "game",
                    latest: "0.1.0",
                    latestManifest: "games/avera/0.1.0/release.json",
                    latestSha256: releaseHash
                }
            ],
            games: {
                avera: {
                    latest: "0.1.0",
                    latestManifest: "games/avera/0.1.0/release.json",
                    latestSha256: releaseHash
                }
            }
        }));
        const files = new Map<string, Buffer>([
            ["/index.json", catalog],
            ["/games/avera/0.1.0/release.json", releaseBody],
            ["/games/avera/0.1.0/manifests/hollow.game.json", gameManifest],
            ["/games/avera/0.1.0/source-000.zip", sourceShard],
            ["/games/avera/0.1.0/assets-000.zip", assetShard],
        ]);
        const ipfsPins: string[] = [];
        const origin = createServer(async (req, res) => {
            const pathName = new URL(req.url || "/", "http://localhost").pathname;
            if (req.method === "POST" && pathName === "/api/v0/add") {
                await drainRequest(req);
                const cid = `bafytest${ipfsPins.length}`;
                ipfsPins.push(cid);
                res.statusCode = 200;
                res.setHeader("content-type", "application/json");
                res.end(`${JSON.stringify({ Name: "file", Hash: cid, Size: "1" })}\n`);
                return;
            }
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
        if (!address || typeof address === "string") throw new Error("server did not bind");
        const baseUrl = `http://127.0.0.1:${address.port}`;
        const storeDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-static-shards-"));
        tempDirs.push(storeDir);
        const store = new MemoryStore();
        const relay = new RelayServer(
            0,
            store,
            [
                createStaticShardSeedPlugin({
                    sources: [{ url: `${baseUrl}/index.json`, kind: "catalog" }],
                    storeDir,
                    autoIngest: true,
                    pinToIpfs: true,
                    ipfsApiUrl: `${baseUrl}/api/v0`,
                }),
            ],
            { enableDefaultPlugins: false },
        );
        relays.push(relay);

        try {
            const port = await waitForPort(relay);
            const statusResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/status`);
            const status = await statusResponse.json() as any;
            expect(statusResponse.status).toBe(200);
            expect(status.releases).toHaveLength(1);
            expect(status.releases[0].releaseSha256).toBe(releaseHash);
            expect(status.releases[0]).toMatchObject({
                publisherVerification: "verified",
                publisher: {
                    protocol: STATIC_SHARD_PUBLISHER_PROTOCOL,
                    publicKey: publisherPublicKey,
                },
            });
            expect(status.releases[0].shards.map((shard: any) => shard.ipfsCid)).toEqual([
                "bafytest0",
                "bafytest1",
            ]);
            expect(ipfsPins).toHaveLength(2);

            const mirroredSource = await fetch(`http://127.0.0.1:${port}${status.releases[0].shards[0].servePath}`);
            expect(Buffer.from(await mirroredSource.arrayBuffer())).toEqual(sourceShard);
            expect(status.releases[0].entryPath).toBe("index.html");
            expect(status.releases[0].playUrl).toBe(`http://127.0.0.1:${port}/plugins/cgp.static-shards/play/avera/0.1.0/index.html?world=samples%2Fstarter-haven.litematic&playerView=1&hideChrome=1`);
            expect(status.releases[0].thumbnailUrl).toBe(`http://127.0.0.1:${port}/plugins/cgp.static-shards/play/avera/0.1.0/assets/thumb.png`);
            expect(status.releases[0].creatorId).toBe("avera-studio");
            expect(status.releases[0].display).toEqual({ aspectRatio: "1 / 1", viewport: { width: 750, height: 750 } });
            expect(status.releases[0].unpacked).toMatchObject({ files: 6 });

            const playableEntry = await fetch(status.releases[0].playUrl);
            expect(playableEntry.status).toBe(200);
            expect(await playableEntry.text()).toContain("<title>Avera</title>");

            const escapedNextChunk = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/_next/static/chunks/webpack.js`, {
                headers: { referer: status.releases[0].playUrl }
            });
            expect(escapedNextChunk.status).toBe(200);
            expect(await escapedNextChunk.text()).toContain("__averaNextChunkLoaded");

            const escapedModel = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/models/Wuhu_Island.glb`, {
                headers: { referer: status.releases[0].playUrl }
            });
            expect(escapedModel.status).toBe(200);
            expect(await escapedModel.text()).toContain("fake model bytes");

            const guildId = hashObject({ kind: "cgp-static-shard-game-guild", id: "avera" });
            const log = await store.getLog(guildId);
            expect(log.map((event) => event.body.type)).toContain("GUILD_CREATE");
            expect(log.map((event) => event.body.type)).toContain("CHANNEL_CREATE");
            const releaseObject = log.find((event) =>
                event.body.type === "APP_OBJECT_UPSERT" &&
                (event.body as any).namespace === "org.cgp.games" &&
                (event.body as any).objectType === "game-release" &&
                (event.body as any).objectId === "avera@0.1.0"
            );
            expect((releaseObject?.body as any)?.value.releaseSha256).toBe(releaseHash);
            expect((releaseObject?.body as any)?.value.playUrl).toBe(`${status.releases[0].entryServePath}?world=samples%2Fstarter-haven.litematic&playerView=1&hideChrome=1`);
            expect((releaseObject?.body as any)?.value.launchQuery).toBe("world=samples%2Fstarter-haven.litematic&playerView=1&hideChrome=1");
            expect((releaseObject?.body as any)?.value.display).toEqual({ aspectRatio: "1 / 1", viewport: { width: 750, height: 750 } });
            const homeProfileObject = log.find((event) =>
                event.body.type === "APP_OBJECT_UPSERT" &&
                (event.body as any).namespace === "app.hollow.home" &&
                (event.body as any).objectType === "game-profile" &&
                (event.body as any).objectId === "game:avera"
            );
            expect((homeProfileObject?.body as any)?.value.playUrl).toBe(`${status.releases[0].entryServePath}?world=samples%2Fstarter-haven.litematic&playerView=1&hideChrome=1`);
            expect((homeProfileObject?.body as any)?.value.creatorId).toBe("avera-studio");
            expect((homeProfileObject?.body as any)?.value.display).toEqual({ aspectRatio: "1 / 1", viewport: { width: 750, height: 750 } });
            const homeCreatorObject = log.find((event) =>
                event.body.type === "APP_OBJECT_UPSERT" &&
                (event.body as any).namespace === "app.hollow.home" &&
                (event.body as any).objectType === "creator-profile" &&
                (event.body as any).objectId === "creator:avera-studio"
            );
            expect((homeCreatorObject?.body as any)?.value.name).toBe("Avera Studio");
        } finally {
            await new Promise<void>((resolve) => origin.close(() => resolve()));
        }
    });

    it("accepts a desktop client upload of release bytes, manifests, and ZIP shards", async () => {
        const sourceShard = zipTextFiles({
            "__hollow_chunks/model/0.part": "large ",
            "__hollow_chunks/model/1.part": "asset",
            "dist/index.html": "<!doctype html><title>Uploaded Game</title>",
            "dist/engine/index.html": "<!doctype html><title>Nested Engine</title>",
            "dist/assets/thumb.png": "thumbnail bytes",
            "dist/_astro/theme-toggle.js": "window.__hollowAstroTheme = true;"
        });
        const gameManifest = Buffer.from(JSON.stringify({
            network: {protocol:'hollow-gamenet/1', compatibilityId:'uploaded-network-v1'},
            host: {requestedFeatures:['microphone']},
            id: "uploaded-game",
            title: "Uploaded Game",
            version: "0.1.0",
            entry: { type: "web", localPath: "dist/index.html" },
            media: { thumbnail: "dist/assets/thumb.png" },
            creator: { id: "upload-studio", username: "upload-studio", name: "Upload Studio" }
        }));
        const publisherPrivateKey = generatePrivateKey();
        const publisherPublicKey = getPublicKey(publisherPrivateKey);
        const release = await signStaticShardRelease({
            kind: "cgp-static-shard-release",
            schemaVersion: 1,
            id: "uploaded-game",
            title: "Uploaded Game",
            type: "game",
            version: "0.1.0",
            fileChunks: [{path:'dist/assets/model.bin', bytes:11, sha256:sha256(Buffer.from('large asset')), parts:['__hollow_chunks/model/0.part','__hollow_chunks/model/1.part']}],
            manifests: {
                game: {
                    path: "manifests/hollow.game.json",
                    sha256: sha256(gameManifest)
                }
            },
            shardPolicy: {
                maxShardBytes: 25 * 1024 * 1024,
                format: "zip",
                hash: "sha256"
            },
            shards: [
                {
                    id: "source-000",
                    kind: "source",
                    path: "source-000.zip",
                    bytes: sourceShard.byteLength,
                    sha256: sha256(sourceShard)
                }
            ]
        }, publisherPrivateKey);
        const releaseBody = Buffer.from(JSON.stringify(release));
        const storeDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-static-upload-"));
        tempDirs.push(storeDir);
        const relay = new RelayServer(
            0,
            new MemoryStore(),
            [createStaticShardSeedPlugin({
                storeDir,
                autoIngest: false,
                registryCompactEvery: 3,
                httpIngestToken: "operator-secret",
            })],
            { enableDefaultPlugins: false },
        );
        relays.push(relay);

        const port = await waitForPort(relay);
        const deniedIngestResponse = await fetch(
            `http://127.0.0.1:${port}/plugins/cgp.static-shards/ingest`,
            {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: "{}",
            },
        );
        expect(deniedIngestResponse.status).toBe(403);
        const authorizedInvalidIngestResponse = await fetch(
            `http://127.0.0.1:${port}/plugins/cgp.static-shards/ingest`,
            {
                method: "POST",
                headers: {
                    "content-type": "application/json",
                    authorization: "Bearer operator-secret",
                },
                body: "{}",
            },
        );
        expect(authorizedInvalidIngestResponse.status).toBe(409);
        const uploadBody = {
            releaseBase64: releaseBody.toString("base64"),
            releaseSha256: sha256(releaseBody),
            files: [
                {
                    path: "manifests/hollow.game.json",
                    bytesBase64: gameManifest.toString("base64"),
                    sha256: sha256(gameManifest)
                },
                {
                    path: "source-000.zip",
                    bytesBase64: sourceShard.toString("base64"),
                    sha256: sha256(sourceShard)
                }
            ]
        };
        const stage = async (action: string, body: unknown) => fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/${action}`, {method:'POST', headers:{'content-type':'application/json'}, body:JSON.stringify(body)});
        const envelope = {releaseBase64: uploadBody.releaseBase64, releaseSha256: uploadBody.releaseSha256};
        const missingBefore = await (await stage('upload-status', envelope)).json() as any;
        expect(missingBefore.missing, JSON.stringify(missingBefore)).toHaveLength(2);
        for (const file of uploadBody.files) {
            const staged = await stage('upload-blob', {...envelope, files:[file]});
            expect(staged.status, await staged.text()).toBe(200);
        }
        const missingAfter = await (await stage('upload-status', envelope)).json() as any;
        expect(missingAfter.missing).toEqual([]);
        const rejectedBlob = await stage('upload-blob', {...envelope, files:[{path:'unlisted', bytesBase64:Buffer.from('bad').toString('base64')}]});
        expect(rejectedBlob.status).toBe(400);
        const uploadResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/upload`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(envelope)
        });
        const uploaded = await uploadResponse.json() as any;
        expect(uploadResponse.status, JSON.stringify(uploaded)).toBe(201);
        expect(uploaded.release).toMatchObject({
            network: {compatibilityId:'uploaded-network-v1'},
            host: {requestedFeatures:['microphone']},
            id: "uploaded-game",
            title: "Uploaded Game",
            releaseSha256: sha256(releaseBody),
            creatorId: "upload-studio",
            publisherVerification: "verified",
            publisher: {
                protocol: STATIC_SHARD_PUBLISHER_PROTOCOL,
                publicKey: publisherPublicKey,
            },
        });
        expect(uploaded.release.playUrl).toBe(`http://127.0.0.1:${port}/plugins/cgp.static-shards/play/uploaded-game/0.1.0/dist/index.html`);
        expect(uploaded.release.thumbnailUrl).toBe(`http://127.0.0.1:${port}/plugins/cgp.static-shards/play/uploaded-game/0.1.0/dist/assets/thumb.png`);

        const unsignedReleaseBody = Buffer.from(JSON.stringify({
            ...release,
            id: "unsigned-upload",
            publisher: undefined,
        }));
        const unsignedUploadResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/upload`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                ...uploadBody,
                releaseBase64: unsignedReleaseBody.toString("base64"),
                releaseSha256: sha256(unsignedReleaseBody),
            })
        });
        const unsignedUpload = await unsignedUploadResponse.json() as any;
        expect(unsignedUploadResponse.status).toBe(409);
        expect(unsignedUpload.error).toContain("requires a signed");

        const tamperedReleaseBody = Buffer.from(JSON.stringify({
            ...release,
            id: "tampered-upload",
            title: "Changed after signing",
        }));
        const tamperedUploadResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/upload`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                ...uploadBody,
                releaseBase64: tamperedReleaseBody.toString("base64"),
                releaseSha256: sha256(tamperedReleaseBody),
            })
        });
        const tamperedUpload = await tamperedUploadResponse.json() as any;
        expect(tamperedUploadResponse.status).toBe(409);
        expect(tamperedUpload.error).toContain("signature is invalid");

        const identicalRetryResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/upload`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(uploadBody)
        });
        expect(identicalRetryResponse.status).toBe(201);

        const conflictingRelease = await signStaticShardRelease({
            ...release,
            title: "Conflicting replacement"
        }, publisherPrivateKey);
        const conflictingReleaseBody = Buffer.from(JSON.stringify(conflictingRelease));
        const conflictingResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/upload`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                ...uploadBody,
                releaseBase64: conflictingReleaseBody.toString("base64"),
                releaseSha256: sha256(conflictingReleaseBody)
            })
        });
        const conflicting = await conflictingResponse.json() as any;
        expect(conflictingResponse.status).toBe(409);
        expect(conflicting.error).toContain("immutable");

        const playableEntry = await fetch(uploaded.release.playUrl);
        expect(playableEntry.status).toBe(200);
        expect(await playableEntry.text()).toContain("Uploaded Game");

        const stalePlayableEntry = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/play/uploaded-game/0.1.0/index.html`);
        expect(stalePlayableEntry.status).toBe(200);
        expect(await stalePlayableEntry.text()).toContain("Uploaded Game");

        const nestedPlayableEntry = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/play/uploaded-game/0.1.0/dist/engine/index.html`);
        expect(nestedPlayableEntry.status).toBe(200);
        expect(await nestedPlayableEntry.text()).toContain("Nested Engine");

        const stalePlayableAsset = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/play/uploaded-game/0.1.0/assets/thumb.png`);
        expect(stalePlayableAsset.status).toBe(200);
        expect(await stalePlayableAsset.text()).toContain("thumbnail bytes");
        const reconstructedAsset = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/play/uploaded-game/0.1.0/dist/assets/model.bin`);
        expect(reconstructedAsset.status).toBe(200);
        expect(await reconstructedAsset.text()).toBe('large asset');

        const rootRelativeAsset = await fetch(`http://127.0.0.1:${port}/_astro/theme-toggle.js`, {
            headers: { referer: uploaded.release.playUrl }
        });
        expect(rootRelativeAsset.status).toBe(200);
        expect(await rootRelativeAsset.text()).toContain("__hollowAstroTheme");

        const rootRelativeWorkerAsset = await fetch(`http://127.0.0.1:${port}/assets/thumb.png`);
        expect(rootRelativeWorkerAsset.status).toBe(200);
        expect(await rootRelativeWorkerAsset.text()).toContain("thumbnail bytes");
        expect(
            (await fs.readFile(path.join(storeDir, "index.json.ndjson"), "utf8"))
                .split(/\r?\n/)
                .filter(Boolean),
        ).toHaveLength(1);

        const secondRelease = await signStaticShardRelease({
            ...release,
            id: "uploaded-game-two",
            title: "Uploaded Game Two",
            version: "0.2.0",
        }, publisherPrivateKey);
        const secondReleaseBody = Buffer.from(JSON.stringify(secondRelease));
        const secondUploadResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/upload`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                releaseBase64: secondReleaseBody.toString("base64"),
                releaseSha256: sha256(secondReleaseBody),
                files: [
                    {
                        path: "manifests/hollow.game.json",
                        bytesBase64: gameManifest.toString("base64"),
                        sha256: sha256(gameManifest)
                    },
                    {
                        path: "source-000.zip",
                        bytesBase64: sourceShard.toString("base64"),
                        sha256: sha256(sourceShard)
                    }
                ]
            })
        });
        expect(secondUploadResponse.status).toBe(201);

        const firstCatalogPageResponse = await fetch(
            `http://127.0.0.1:${port}/plugins/cgp.static-shards/catalog?limit=1`,
        );
        const firstCatalogPage = await firstCatalogPageResponse.json() as any;
        expect(firstCatalogPageResponse.status).toBe(200);
        expect(firstCatalogPage).toMatchObject({
            kind: "cgp-relay-static-shard-catalog-page",
            schemaVersion: 2,
            total: 2,
            page: { limit: 1, count: 1, hasMore: true },
        });
        expect(firstCatalogPage.releases.map((entry: any) => entry.id)).toEqual(["uploaded-game-two"]);
        expect(typeof firstCatalogPage.page.nextCursor).toBe("string");

        const updatedRelease = await signStaticShardRelease({
            ...release,
            title: "Uploaded Game Updated",
            version: "0.3.0",
        }, publisherPrivateKey);
        const updatedReleaseBody = Buffer.from(JSON.stringify(updatedRelease));
        const updatedUploadResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/upload`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                releaseBase64: updatedReleaseBody.toString("base64"),
                releaseSha256: sha256(updatedReleaseBody),
                files: [
                    {
                        path: "manifests/hollow.game.json",
                        bytesBase64: gameManifest.toString("base64"),
                        sha256: sha256(gameManifest)
                    },
                    {
                        path: "source-000.zip",
                        bytesBase64: sourceShard.toString("base64"),
                        sha256: sha256(sourceShard)
                    }
                ]
            })
        });
        expect(updatedUploadResponse.status).toBe(201);

        const attackerPrivateKey = generatePrivateKey();
        const attackerRelease = await signStaticShardRelease({
            ...release,
            version: "0.4.0",
        }, attackerPrivateKey);
        const attackerReleaseBody = Buffer.from(JSON.stringify(attackerRelease));
        const attackerUploadResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/upload`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                ...uploadBody,
                releaseBase64: attackerReleaseBody.toString("base64"),
                releaseSha256: sha256(attackerReleaseBody),
            })
        });
        const attackerUpload = await attackerUploadResponse.json() as any;
        expect(attackerUploadResponse.status).toBe(409);
        expect(attackerUpload.error).toContain("owned by a different CGP publisher key");

        const secondCatalogPageResponse = await fetch(
            `http://127.0.0.1:${port}/plugins/cgp.static-shards/catalog?cursor=${encodeURIComponent(firstCatalogPage.page.nextCursor)}`,
        );
        const secondCatalogPage = await secondCatalogPageResponse.json() as any;
        expect(secondCatalogPageResponse.status).toBe(200);
        expect(secondCatalogPage.releases.map((entry: any) => entry.id)).toEqual(["uploaded-game"]);
        expect(secondCatalogPage.releases[0]).toMatchObject({
            title: "Uploaded Game",
            version: "0.3.0",
        });
        expect(secondCatalogPage.total).toBe(2);
        expect(secondCatalogPage.page).toMatchObject({ limit: 64, count: 1, hasMore: false, nextCursor: null });

        const lookupResponse = await fetch(
            `http://127.0.0.1:${port}/plugins/cgp.static-shards/catalog?id=uploaded-game`,
        );
        const lookup = await lookupResponse.json() as any;
        expect(lookupResponse.status).toBe(200);
        expect(lookup.releases.map((entry: any) => entry.id)).toEqual(["uploaded-game"]);
        expect(lookup.releases[0].version).toBe("0.3.0");
        expect(lookup.releases[0].publisher.publicKey).toBe(publisherPublicKey);
        expect(lookup.releases[0].publisherVerification).toBe("verified");

        const searchResponse = await fetch(
            `http://127.0.0.1:${port}/plugins/cgp.static-shards/catalog?q=${encodeURIComponent("game two")}&limit=8`,
        );
        const search = await searchResponse.json() as any;
        expect(searchResponse.status).toBe(200);
        expect(search).toMatchObject({
            kind: "cgp-relay-static-shard-catalog-search",
            schemaVersion: 2,
            query: "game two",
            page: { limit: 8, count: 1, hasMore: false },
        });
        expect(search.releases.map((entry: any) => entry.id)).toEqual(["uploaded-game-two"]);

        const prefixSearchResponse = await fetch(
            `http://127.0.0.1:${port}/plugins/cgp.static-shards/catalog?q=upload&limit=1`,
        );
        const prefixSearch = await prefixSearchResponse.json() as any;
        expect(prefixSearch.releases.map((entry: any) => entry.id)).toEqual(["uploaded-game"]);
        expect(prefixSearch.releases[0].version).toBe("0.3.0");
        expect(prefixSearch.page).toMatchObject({ count: 1, hasMore: true });

        const invalidCursorResponse = await fetch(
            `http://127.0.0.1:${port}/plugins/cgp.static-shards/catalog?limit=1&cursor=missing`,
        );
        expect(invalidCursorResponse.status).toBe(400);

        await relay.close();
        const journalLines = (await fs.readFile(path.join(storeDir, "index.json.ndjson"), "utf8"))
            .split(/\r?\n/)
            .filter(Boolean);
        expect(journalLines).toHaveLength(0);
        const compactedRegistry = JSON.parse(await fs.readFile(path.join(storeDir, "index.json"), "utf8"));
        expect(compactedRegistry.releases).toHaveLength(3);
        await fs.appendFile(path.join(storeDir, "index.json.ndjson"), '{"torn":', "utf8");

        const restartedStore = new MemoryStore();
        let failListingProfile = false;
        const restartedPlugin = createStaticShardSeedPlugin({ storeDir, autoIngest: false });
        const initializePlugin = restartedPlugin.onInit!;
        restartedPlugin.onInit = async ctx => initializePlugin({...ctx, publishAsRelay: async body => {
            if (failListingProfile && (body as any).objectType === 'game-profile') {
                failListingProfile = false;
                throw new Error('Injected listing publication failure');
            }
            return ctx.publishAsRelay(body);
        }});
        const restartedRelay = new RelayServer(
            0,
            restartedStore,
            [restartedPlugin],
            { enableDefaultPlugins: false },
        );
        relays.push(restartedRelay);
        const restartedPort = await waitForPort(restartedRelay);
        const restartedCatalogResponse = await fetch(
            `http://127.0.0.1:${restartedPort}/plugins/cgp.static-shards/catalog?limit=2`,
        );
        const restartedCatalog = await restartedCatalogResponse.json() as any;
        expect(restartedCatalogResponse.status).toBe(200);
        expect(restartedCatalog.total).toBe(2);
        expect(restartedCatalog.releases.map((entry: any) => entry.id)).toEqual([
            "uploaded-game-two",
            "uploaded-game",
        ]);
        expect(restartedCatalog.releases[1].version).toBe("0.3.0");
        expect(restartedCatalog.releases[1].publisher.publicKey).toBe(publisherPublicKey);
        const restartedSearchResponse = await fetch(
            `http://127.0.0.1:${restartedPort}/plugins/cgp.static-shards/catalog?q=upload&limit=2`,
        );
        const restartedSearch = await restartedSearchResponse.json() as any;
        expect(restartedSearch.releases.map((entry: any) => entry.id)).toEqual([
            "uploaded-game",
            "uploaded-game-two",
        ]);
        const restartedAttackerResponse = await fetch(
            `http://127.0.0.1:${restartedPort}/plugins/cgp.static-shards/upload`,
            {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    ...uploadBody,
                    releaseBase64: attackerReleaseBody.toString("base64"),
                    releaseSha256: sha256(attackerReleaseBody),
                }),
            },
        );
        const restartedAttacker = await restartedAttackerResponse.json() as any;
        expect(restartedAttackerResponse.status).toBe(409);
        expect(restartedAttacker.error).toContain("owned by a different CGP publisher key");
        expect(await fs.readFile(path.join(storeDir, "index.json.ndjson"), "utf8")).toBe("");

        const raceReleaseBase = {
            ...release,
            id: "publisher-race-game",
            version: "1.0.0",
        };
        const raceReleaseA = await signStaticShardRelease(
            raceReleaseBase,
            publisherPrivateKey,
        );
        const raceReleaseB = await signStaticShardRelease(
            raceReleaseBase,
            attackerPrivateKey,
        );
        const raceBody = (raceRelease: Record<string, unknown>) => {
            const bytes = Buffer.from(JSON.stringify(raceRelease));
            return JSON.stringify({
                ...uploadBody,
                releaseBase64: bytes.toString("base64"),
                releaseSha256: sha256(bytes),
            });
        };
        const raceResponses = await Promise.all([
            fetch(`http://127.0.0.1:${restartedPort}/plugins/cgp.static-shards/upload`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: raceBody(raceReleaseA),
            }),
            fetch(`http://127.0.0.1:${restartedPort}/plugins/cgp.static-shards/upload`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: raceBody(raceReleaseB),
            }),
        ]);
        expect(raceResponses.map((response) => response.status).sort()).toEqual([201, 409]);
        const raceLookup = await (
            await fetch(`http://127.0.0.1:${restartedPort}/plugins/cgp.static-shards/catalog?id=publisher-race-game`)
        ).json() as any;
        expect([
            publisherPublicKey,
            getPublicKey(attackerPrivateKey),
        ]).toContain(raceLookup.releases[0].publisher.publicKey);
        const listingStage = (body: unknown) => fetch(`http://127.0.0.1:${restartedPort}/plugins/cgp.static-shards/listing`, {method:'POST', headers:{'content-type':'application/json'}, body:JSON.stringify(body)});
        const listing = await signStaticShardRelease({kind:'cgp-game-listing-update/1', id:'uploaded-game', version:'0.1.0', releaseSha256: sha256(releaseBody), expectedRevision:0, title:'Updated listing', description:'New description', thumbnail:'dist/assets/thumb.png'}, publisherPrivateKey);
        const listingResponse = await listingStage(listing);
        const listingPayload = await listingResponse.json() as any;
        expect(listingResponse.status, JSON.stringify(listingPayload)).toBe(200);
        expect(listingPayload.release.title).toBe('Updated listing');
        expect(listingPayload.release.releaseSha256).toBe(sha256(releaseBody));
        expect(listingPayload.release.version).toBe('0.1.0');
        expect(listingPayload.release.isCurrentRelease).toBe(false);
        expect((await listingStage(listing)).status).toBe(200);
        const staleListing = await signStaticShardRelease({...listing,title:'Stale edit'}, publisherPrivateKey);
        expect((await listingStage(staleListing)).status).toBe(409);
        const listingGuild = hashObject({kind:'cgp-static-shard-game-guild', id:'uploaded-game'});
        const listingEvents = (await restartedStore.getLog(listingGuild)).map(event => event.body as any);
        expect(listingEvents.find(event => event.objectType === 'game-release' && event.objectId === 'uploaded-game@0.1.0')?.value.title).toBe('Updated listing');
        expect(listingEvents.filter(event => event.objectType === 'game-profile')).toHaveLength(0);
        const currentListing = await signStaticShardRelease({...listing, version:'0.3.0', releaseSha256:sha256(updatedReleaseBody), title:'Current listing'}, publisherPrivateKey);
        failListingProfile = true;
        expect((await listingStage(currentListing)).status).toBe(409);
        const refreshedListing = await (await fetch(`http://127.0.0.1:${restartedPort}/plugins/cgp.static-shards/listing?id=uploaded-game&version=0.3.0`)).json() as any;
        expect(refreshedListing.release).toMatchObject({title:'Current listing',listingRevision:1,isCurrentRelease:true});
        expect((await listingStage(currentListing)).status).toBe(200);
        const currentEvents = (await restartedStore.getLog(listingGuild)).map(event => event.body as any);
        expect(currentEvents.find(event => event.objectType === 'game-profile')?.value).toMatchObject({title:'Current listing', version:'0.3.0'});
    });

    it("loads and searches a large persisted catalog without scanning every release", async () => {
        const releaseCount = 20_000;
        const storeDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-static-large-catalog-"));
        tempDirs.push(storeDir);
        const releases = Array.from({ length: releaseCount }, (_, index) => {
            const suffix = String(index).padStart(5, "0");
            return {
                id: `catalog-game-${suffix}`,
                title: `Catalog Game ${suffix}`,
                description: `Bounded prefix lookup fixture ${suffix}`,
                type: "game",
                version: "1.0.0",
                releaseUrl: `https://static.example/catalog-game-${suffix}/release.json`,
                releaseSha256: sha256(`catalog-game-${suffix}`),
                storedAt: index + 1,
                manifests: [],
                shards: []
            };
        });
        await fs.writeFile(path.join(storeDir, "index.json"), JSON.stringify({
            kind: "cgp-relay-static-shard-registry",
            schemaVersion: 1,
            releases
        }));

        const relay = new RelayServer(
            0,
            new MemoryStore(),
            [createStaticShardSeedPlugin({ storeDir, autoIngest: false })],
            { enableDefaultPlugins: false },
        );
        relays.push(relay);
        const port = await waitForPort(relay);

        const pageResponse = await fetch(
            `http://127.0.0.1:${port}/plugins/cgp.static-shards/catalog?limit=3`,
        );
        const page = await pageResponse.json() as any;
        expect(pageResponse.status).toBe(200);
        expect(page.total).toBe(releaseCount);
        expect(page.releases.map((entry: any) => entry.id)).toEqual([
            "catalog-game-19999",
            "catalog-game-19998",
            "catalog-game-19997",
        ]);

        const searchResponse = await fetch(
            `http://127.0.0.1:${port}/plugins/cgp.static-shards/catalog?q=${encodeURIComponent("catalog 19999")}&limit=4`,
        );
        const search = await searchResponse.json() as any;
        expect(searchResponse.status).toBe(200);
        expect(search.releases.map((entry: any) => entry.id)).toEqual([
            "catalog-game-19999",
        ]);
    }, 20_000);

    it.each(["index.html", "index#%?.html"])("redirects verified external playable filenames without URL reinterpretation (%s)", async (entryName) => {
        const entryPath = `dist/${entryName}`;
        const externalEntry = `http://static.example/games/redirect-game/0.1.0/dist/${encodeURIComponent(entryName)}`;
        const sourceShard = zipTextFiles({
            [entryPath]: "<!doctype html><title>Redirect Game</title>",
            "dist/thumb.png": "thumbnail bytes"
        });
        const gameManifest = Buffer.from(JSON.stringify({
            id: "redirect-game",
            title: "Redirect Game",
            version: "0.1.0",
            entry: { type: "web", localPath: entryPath },
            media: { thumbnail: "dist/thumb.png" },
            launch: { web: { query: "playerView=1" } }
        }));
        const release = {
            kind: "cgp-static-shard-release",
            schemaVersion: 1,
            id: "redirect-game",
            title: "Redirect Game",
            type: "game",
            version: "0.1.0",
            hosting: {
                mode: "redirect",
                distBaseUrl: "http://static.example/games/redirect-game/0.1.0/"
            },
            manifests: {
                game: {
                    path: "manifests/hollow.game.json",
                    sha256: sha256(gameManifest)
                }
            },
            shards: [
                {
                    id: "source-000",
                    kind: "source",
                    path: "source-000.zip",
                    bytes: sourceShard.byteLength,
                    sha256: sha256(sourceShard)
                }
            ]
        };
        const releaseBody = Buffer.from(JSON.stringify(release));
        const files = new Map<string, Buffer>([
            ["/release.json", releaseBody],
            ["/manifests/hollow.game.json", gameManifest],
            ["/source-000.zip", sourceShard],
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
        if (!address || typeof address === "string") throw new Error("server did not bind");
        const storeDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-static-redirect-"));
        tempDirs.push(storeDir);
        const relay = new RelayServer(
            0,
            new MemoryStore(),
            [
                createStaticShardSeedPlugin({
                    sources: [{
                        url: `http://127.0.0.1:${address.port}/release.json`,
                        kind: "release",
                        expectedSha256: sha256(releaseBody),
                    }],
                    storeDir,
                    autoIngest: true,
                    servePlayableMode: "redirect",
                    allowVerifiedRedirects: true,
                    extractPlayable: true,
                }),
            ],
            { enableDefaultPlugins: false },
        );
        relays.push(relay);

        try {
            const port = await waitForPort(relay);
            const statusResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/status`);
            const status = await statusResponse.json() as any;
            expect(status.releases[0]).toMatchObject({
                id: "redirect-game",
                serveMode: "redirect",
                assetServing: "external",
                publisherVerification: "legacy-operator-seed",
                externalPlayUrl: `${externalEntry}?playerView=1`,
                thumbnailUrl: "http://static.example/games/redirect-game/0.1.0/dist/thumb.png",
            });
            expect(status.releases[0].unpacked).toBeUndefined();

            const playResponse = await fetch(status.releases[0].playUrl, { redirect: "manual" });
            expect(playResponse.status).toBe(302);
            expect(playResponse.headers.get("location")).toBe(`${externalEntry}?playerView=1`);

            const stalePlayResponse = await fetch(`http://127.0.0.1:${port}/plugins/cgp.static-shards/play/redirect-game/0.1.0/${encodeURIComponent(entryName)}?world=samples%2Fstarter-haven.litematic`, { redirect: "manual" });
            expect(stalePlayResponse.status).toBe(302);
            expect(stalePlayResponse.headers.get("location")).toBe(`${externalEntry}?world=samples%2Fstarter-haven.litematic`);
        } finally {
            await new Promise<void>((resolve) => origin.close(() => resolve()));
        }
    });
});
