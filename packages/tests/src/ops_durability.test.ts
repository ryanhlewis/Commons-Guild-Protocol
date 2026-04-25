import { afterEach, describe, expect, it } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile, execFileSync } from "node:child_process";
import { promisify } from "node:util";
import * as secp from "@noble/secp256k1";
import WebSocket from "ws";
import { CgpClient } from "@cgp/client";
import { LevelStore, LocalRelayPubSubAdapter, RelayServer } from "@cgp/relay";
import { EventBody, hashObject, sign } from "@cgp/core";

const baseDir = path.join(os.tmpdir(), "cgp-ops-durability-tests");
const npxBin = process.platform === "win32" ? "npx.cmd" : "npx";
const execFileAsync = promisify(execFile);

function clean(filePath: string) {
    if (fs.existsSync(filePath)) {
        fs.rmSync(filePath, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
    }
}

function keyPair() {
    const priv = secp.utils.randomPrivateKey();
    const pub = Buffer.from(secp.getPublicKey(priv, true)).toString("hex");
    return { priv, pub };
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 5000) {
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
        if (await predicate()) return;
        await new Promise((resolve) => setTimeout(resolve, 100));
    }
    throw new Error("Timed out waiting for condition");
}

function syntheticEvent(seq: number, body: EventBody, author = "author"): any {
    const event = {
        id: "",
        seq,
        prevHash: seq === 0 ? null : `prev-${seq - 1}`,
        createdAt: Date.now() + seq,
        author,
        body,
        signature: `sig-${seq}`
    };
    event.id = hashObject({ seq, body, author });
    return event;
}

async function publishRawBatch(
    url: string,
    batchId: string,
    bodies: EventBody[],
    keys: { pub: string; priv: Uint8Array },
    createdAt = Date.now()
) {
    const events = await Promise.all(bodies.map((body, index) => {
        const eventCreatedAt = createdAt + index;
        return sign(keys.priv, hashObject({ body, author: keys.pub, createdAt: eventCreatedAt })).then((signature) => ({
            body,
            author: keys.pub,
            createdAt: eventCreatedAt,
            signature
        }));
    }));

    const socket = new WebSocket(url);
    await new Promise<void>((resolve, reject) => {
        socket.once("open", resolve);
        socket.once("error", reject);
    });
    await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => {
            socket.close();
            reject(new Error(`Timed out waiting for ${batchId}`));
        }, 5000);
        socket.on("message", (raw) => {
            const [kind, payload] = JSON.parse(raw.toString());
            if (kind === "PUB_BATCH_ACK" && payload?.batchId === batchId) {
                clearTimeout(timer);
                socket.close();
                resolve();
            }
            if (kind === "ERROR" && payload?.batchId === batchId) {
                clearTimeout(timer);
                socket.close();
                reject(new Error(payload?.message || "batch failed"));
            }
        });
        socket.send(JSON.stringify(["PUBLISH_BATCH", { batchId, events }]));
    });
}

describe("ops durability and failover", () => {
    afterEach(() => {
        clean(baseDir);
    });

    it("keeps same-batch message refs correct without full derived replay", async () => {
        const dbPath = path.join(baseDir, `same-batch-ref-${Date.now()}`);
        clean(dbPath);
        const store = new LevelStore(dbPath);
        const guildId = "same-batch-guild";
        const channelId = "same-batch-channel";
        const messageId = "same-batch-message";

        try {
            await (store as any).db.open();
            await store.appendEvents(guildId, [
                syntheticEvent(0, { type: "GUILD_CREATE", guildId, name: "Batch Guild" } as EventBody),
                syntheticEvent(1, { type: "CHANNEL_CREATE", guildId, channelId, name: "batch", kind: "text" } as EventBody),
                syntheticEvent(2, { type: "MESSAGE", guildId, channelId, messageId, content: "batched" } as EventBody),
                syntheticEvent(3, { type: "REACTION_ADD", guildId, channelId, messageId, reaction: "ok" } as EventBody),
                syntheticEvent(4, { type: "DELETE_MESSAGE", guildId, channelId, messageId } as EventBody)
            ]);

            expect(await store.getMessageRef({ guildId, messageId })).toMatchObject({
                channelId,
                authorId: "author",
                deleted: true,
                reactions: { ok: ["author"] }
            });
        } finally {
            await store.close();
        }
    });

    it("keeps same-batch member role mutations correct without full derived replay", async () => {
        const dbPath = path.join(baseDir, `same-batch-member-${Date.now()}`);
        clean(dbPath);
        const store = new LevelStore(dbPath);
        const guildId = "same-batch-member-guild";
        const author = "member-author";

        try {
            await (store as any).db.open();
            await store.appendEvents(guildId, [
                syntheticEvent(0, { type: "GUILD_CREATE", guildId, name: "Batch Member Guild" } as EventBody, author),
                syntheticEvent(1, { type: "ROLE_ASSIGN", guildId, userId: author, roleId: "admin" } as EventBody, author),
                syntheticEvent(2, { type: "ROLE_ASSIGN", guildId, userId: author, roleId: "moderator" } as EventBody, author),
                syntheticEvent(3, { type: "ROLE_REVOKE", guildId, userId: author, roleId: "admin" } as EventBody, author)
            ]);

            const page = await store.getMembersPage({ guildId, limit: 10 });
            expect(page.members).toEqual([
                expect.objectContaining({
                    userId: author,
                    roles: ["moderator", "owner"]
                })
            ]);
        } finally {
            await store.close();
        }
    });

    it("uses the role-member index to apply role deletes", async () => {
        const dbPath = path.join(baseDir, `role-index-${Date.now()}`);
        clean(dbPath);
        const store = new LevelStore(dbPath);
        const guildId = "role-index-guild";
        const author = "owner-user";

        try {
            await (store as any).db.open();
            await store.appendEvents(guildId, [
                syntheticEvent(0, { type: "GUILD_CREATE", guildId, name: "Role Index Guild" } as EventBody, author),
                syntheticEvent(1, { type: "ROLE_ASSIGN", guildId, userId: author, roleId: "admin" } as EventBody, author),
                syntheticEvent(2, { type: "ROLE_ASSIGN", guildId, userId: "helper-user", roleId: "admin" } as EventBody, author),
                syntheticEvent(3, { type: "ROLE_ASSIGN", guildId, userId: "member-user", roleId: "member" } as EventBody, author)
            ]);
            await store.appendEvents(guildId, [
                syntheticEvent(4, { type: "ROLE_DELETE", guildId, roleId: "admin" } as EventBody, author)
            ]);

            const members = new Map((await store.getMembersPage({ guildId, limit: 10 })).members.map((member) => [
                member.userId,
                member.roles
            ]));
            expect(members.get(author)).toEqual(["owner"]);
            expect(members.get("helper-user")).toEqual([]);
            expect(members.get("member-user")).toEqual(["member"]);
        } finally {
            await store.close();
        }
    });

    it("hydrates history, members, and message refs after relay restart", async () => {
        const dbPath = path.join(baseDir, `restart-${Date.now()}`);
        clean(dbPath);
        const port = 8305;
        const keys = keyPair();
        let relay = new RelayServer(port, dbPath, [], { enableDefaultPlugins: false });
        let client = new CgpClient({ relays: [`ws://localhost:${port}`], keyPair: keys });

        await client.connect();
        const guildId = await client.createGuild("Restart Durable Guild");
        const channelId = await client.createChannel(guildId, "durable", "text");
        const firstMessageId = await client.sendMessage(guildId, channelId, "survives restart");
        await client.sendMessage(guildId, channelId, "also survives restart");
        await client.publishReliable({ type: "REACTION_ADD", guildId, channelId, messageId: firstMessageId, reaction: "ok" });
        for (let index = 0; index < 12; index += 1) {
            await client.assignRole(guildId, `member-${String(index).padStart(3, "0")}`, "member");
        }
        await new Promise((resolve) => setTimeout(resolve, 300));

        client.close();
        await relay.close();

        relay = new RelayServer(port, dbPath, [], { enableDefaultPlugins: false });
        client = new CgpClient({ relays: [`ws://localhost:${port}`], keyPair: keys });
        await client.connect();

        const history = await client.getHistory({ guildId, channelId, limit: 10 });
        expect(history.events.some((event: any) => event.body.content === "survives restart")).toBe(true);
        expect(history.events.some((event: any) => event.body.content === "also survives restart")).toBe(true);

        const members = await client.getMembersPage(guildId, { limit: 5 });
        expect(members.members).toHaveLength(5);
        expect(members.hasMore).toBe(true);

        const store = (relay as any).store as LevelStore;
        const ref = await store.getMessageRef({ guildId, messageId: firstMessageId });
        expect(ref).toMatchObject({
            channelId,
            authorId: keys.pub,
            reactions: { ok: [keys.pub] }
        });

        client.close();
        await relay.close();
    });

    it("serves backfill from a follower relay after sequencer shutdown", async () => {
        const dbA = path.join(baseDir, `failover-a-${Date.now()}`);
        const dbB = path.join(baseDir, `failover-b-${Date.now()}`);
        clean(dbA);
        clean(dbB);
        const portA = 8315;
        const portB = 8316;
        const pubSub = new LocalRelayPubSubAdapter();
        const relayA = new RelayServer(portA, dbA, [], {
            enableDefaultPlugins: false,
            instanceId: "ops-sequencer-a",
            pubSubAdapter: pubSub
        });
        const relayB = new RelayServer(portB, dbB, [], {
            enableDefaultPlugins: false,
            instanceId: "ops-follower-b",
            pubSubAdapter: pubSub
        });
        const keys = keyPair();
        const publisher = new CgpClient({ relays: [`ws://localhost:${portA}`], keyPair: keys });

        try {
            const guildId = hashObject({ test: "ops-failover", t: Date.now() });
            const channelId = hashObject({ test: "ops-failover-channel", guildId });
            const bootstrapBodies: EventBody[] = [
                {
                    type: "GUILD_CREATE",
                    guildId,
                    name: "Failover Guild",
                    policies: { posting: "public" }
                } as EventBody,
                {
                    type: "CHANNEL_CREATE",
                    guildId,
                    channelId,
                    name: "handoff",
                    kind: "text"
                } as EventBody
            ];
            const createdAt = Date.now();
            await publishRawBatch(`ws://localhost:${portA}`, "bootstrap-a", bootstrapBodies, keys, createdAt);
            await publishRawBatch(`ws://localhost:${portB}`, "bootstrap-b", bootstrapBodies, keys, createdAt);

            await publisher.connect();
            await publisher.sendMessage(guildId, channelId, "before failover");

            const followerStore = (relayB as any).store as LevelStore;
            await waitFor(async () => {
                const history = await followerStore.getHistory({ guildId, channelId, limit: 10 });
                return history.some((event) => event.body.type === "MESSAGE" && (event.body as any).content === "before failover");
            });

            publisher.close();
            await relayA.close();

            const reader = new CgpClient({ relays: [`ws://localhost:${portB}`], keyPair: keys });
            await reader.connect();
            const history = await reader.getHistory({ guildId, channelId, limit: 10 });
            expect(history.events.some((event: any) => event.body.content === "before failover")).toBe(true);
            reader.close();
        } finally {
            publisher.close();
            await relayA.close().catch(() => undefined);
            await relayB.close().catch(() => undefined);
            pubSub.close();
        }
    }, 15000);

    it("backs up and restores a relay database from canonical guild logs", async () => {
        const sourceDb = path.join(baseDir, `backup-source-${Date.now()}`);
        const targetDb = path.join(baseDir, `backup-target-${Date.now()}`);
        const backupFile = path.join(baseDir, `relay-backup-${Date.now()}.json`);
        clean(sourceDb);
        clean(targetDb);
        const port = 8330;
        const keys = keyPair();
        const relay = new RelayServer(port, sourceDb, [], { enableDefaultPlugins: false });
        const client = new CgpClient({ relays: [`ws://localhost:${port}`], keyPair: keys });

        try {
            await client.connect();
            const guildId = await client.createGuild("Backup Guild");
            const channelId = await client.createChannel(guildId, "backup", "text");
            await client.sendMessage(guildId, channelId, "backup one");
            await client.sendMessage(guildId, channelId, "backup two");
            const liveStore = (relay as any).store as LevelStore;
            await waitFor(async () => (await liveStore.getLastEvent(guildId))?.seq === 3);
            client.close();
            await relay.close();

            execFileSync(npxBin, ["tsx", "scripts/relay-ops.ts", "backup-db", "--db", sourceDb, "--output", backupFile], {
                cwd: process.cwd(),
                stdio: "pipe",
                shell: process.platform === "win32"
            });
            execFileSync(npxBin, ["tsx", "scripts/relay-ops.ts", "restore-db", "--db", targetDb, "--input", backupFile], {
                cwd: process.cwd(),
                stdio: "pipe",
                shell: process.platform === "win32"
            });

            const sourceStore = new LevelStore(sourceDb);
            const targetStore = new LevelStore(targetDb);
            try {
                const sourceHead = await sourceStore.getLastEvent(guildId);
                const targetHead = await targetStore.getLastEvent(guildId);
                expect(targetHead?.seq).toBe(sourceHead?.seq);
                expect(targetHead?.id).toBe(sourceHead?.id);
                const restoredHistory = await targetStore.getHistory({ guildId, channelId, limit: 10 });
                expect(restoredHistory.some((event) => event.body.type === "MESSAGE" && (event.body as any).content === "backup one")).toBe(true);
                expect(restoredHistory.some((event) => event.body.type === "MESSAGE" && (event.body as any).content === "backup two")).toBe(true);
            } finally {
                await sourceStore.close();
                await targetStore.close();
            }
        } finally {
            client.close();
            await relay.close().catch(() => undefined);
        }
    }, 20000);

    it("streams JSONL relay backups and restores them without loading full logs", async () => {
        const sourceDb = path.join(baseDir, `jsonl-backup-source-${Date.now()}`);
        const targetDb = path.join(baseDir, `jsonl-backup-target-${Date.now()}`);
        const backupFile = path.join(baseDir, `relay-backup-${Date.now()}.jsonl`);
        clean(sourceDb);
        clean(targetDb);
        const port = 8331;
        const keys = keyPair();
        const relay = new RelayServer(port, sourceDb, [], { enableDefaultPlugins: false });
        const client = new CgpClient({ relays: [`ws://localhost:${port}`], keyPair: keys });

        try {
            await client.connect();
            const guildId = await client.createGuild("Streaming Backup Guild");
            const channelId = await client.createChannel(guildId, "backup", "text");
            for (let index = 0; index < 24; index += 1) {
                await client.sendMessage(guildId, channelId, `streamed backup ${index}`);
            }
            const liveStore = (relay as any).store as LevelStore;
            await waitFor(async () => (await liveStore.getLastEvent(guildId))?.seq === 25);
            client.close();
            await relay.close();

            execFileSync(npxBin, ["tsx", "scripts/relay-ops.ts", "backup-jsonl", "--db", sourceDb, "--output", backupFile], {
                cwd: process.cwd(),
                stdio: "pipe",
                shell: process.platform === "win32"
            });
            execFileSync(npxBin, ["tsx", "scripts/relay-ops.ts", "restore-jsonl", "--db", targetDb, "--input", backupFile], {
                cwd: process.cwd(),
                stdio: "pipe",
                shell: process.platform === "win32"
            });

            const sourceStore = new LevelStore(sourceDb);
            const targetStore = new LevelStore(targetDb);
            try {
                const sourceHead = await sourceStore.getLastEvent(guildId);
                const targetHead = await targetStore.getLastEvent(guildId);
                expect(targetHead?.seq).toBe(sourceHead?.seq);
                expect(targetHead?.id).toBe(sourceHead?.id);
                const restoredHistory = await targetStore.getHistory({ guildId, channelId, limit: 30 });
                expect(restoredHistory.filter((event) => event.body.type === "MESSAGE")).toHaveLength(24);
            } finally {
                await sourceStore.close();
                await targetStore.close();
            }
        } finally {
            client.close();
            await relay.close().catch(() => undefined);
        }
    }, 20000);

    it("exposes operator health, readiness, and metrics endpoints", async () => {
        const port = 8332;
        const relay = new RelayServer(port, new LevelStore(path.join(baseDir, `ops-http-${Date.now()}`)), [], {
            enableDefaultPlugins: false,
            instanceId: "ops-http-relay"
        });

        try {
            const health = await fetch(`http://localhost:${port}/healthz`);
            expect(health.status).toBe(200);
            await expect(health.json()).resolves.toMatchObject({
                ok: true,
                status: "ok",
                relayId: "ops-http-relay"
            });

            const ready = await fetch(`http://localhost:${port}/readyz`);
            expect(ready.status).toBe(200);
            await expect(ready.json()).resolves.toMatchObject({
                ok: true,
                status: "ready",
                relayId: "ops-http-relay"
            });

            const metrics = await fetch(`http://localhost:${port}/metrics`);
            expect(metrics.status).toBe(200);
            const body = await metrics.text();
            expect(body).toContain("cgp_relay_uptime_seconds");
            expect(body).toContain("cgp_relay_websocket_clients");
            expect(body).toContain("cgp_relay_event_loop_delay_p99_seconds");
        } finally {
            await relay.close().catch(() => undefined);
        }
    }, 10000);

    it("syncs missing canonical events from another relay in large log ranges", async () => {
        const sourceDb = path.join(baseDir, `sync-source-${Date.now()}`);
        const targetDb = path.join(baseDir, `sync-target-${Date.now()}`);
        clean(sourceDb);
        clean(targetDb);
        const port = 8333;
        const keys = keyPair();
        const relay = new RelayServer(port, sourceDb, [], { enableDefaultPlugins: false });
        const client = new CgpClient({ relays: [`ws://localhost:${port}`], keyPair: keys });

        try {
            await client.connect();
            const guildId = await client.createGuild("Relay Sync Guild");
            const channelId = await client.createChannel(guildId, "sync", "text");
            for (let index = 0; index < 35; index += 1) {
                await client.sendMessage(guildId, channelId, `sync message ${index}`);
            }
            const liveStore = (relay as any).store as LevelStore;
            await waitFor(async () => (await liveStore.getLastEvent(guildId))?.seq === 36);

            await execFileAsync(npxBin, [
                "tsx",
                "scripts/relay-ops.ts",
                "sync-from-relay",
                "--db",
                targetDb,
                "--guild",
                guildId,
                "--relay",
                `ws://127.0.0.1:${port}`,
                "--trust-source-head",
                "--limit",
                "9",
                "--private-key-hex",
                Buffer.from(keys.priv).toString("hex")
            ], {
                cwd: process.cwd(),
                shell: process.platform === "win32",
                timeout: 20000
            });

            const targetStore = new LevelStore(targetDb);
            try {
                const sourceHead = await liveStore.getLastEvent(guildId);
                const targetHead = await targetStore.getLastEvent(guildId);
                expect(targetHead?.seq).toBe(sourceHead?.seq);
                expect(targetHead?.id).toBe(sourceHead?.id);
                const restoredHistory = await targetStore.getHistory({ guildId, channelId, limit: 40 });
                expect(restoredHistory.filter((event) => event.body.type === "MESSAGE")).toHaveLength(35);
            } finally {
                await targetStore.close();
            }
        } finally {
            client.close();
            await relay.close().catch(() => undefined);
        }
    }, 30000);

    it("repairs a lagging relay from a canonical relay-head quorum", async () => {
        const sourceDb = path.join(baseDir, `quorum-source-${Date.now()}`);
        const mirrorDb = path.join(baseDir, `quorum-mirror-${Date.now()}`);
        const targetDb = path.join(baseDir, `quorum-target-${Date.now()}`);
        clean(sourceDb);
        clean(mirrorDb);
        clean(targetDb);
        const sourcePort = 8334;
        const mirrorPort = 8335;
        const keys = keyPair();
        const sourceRelay = new RelayServer(sourcePort, sourceDb, [], { enableDefaultPlugins: false });
        let mirrorRelay: RelayServer | undefined;
        const client = new CgpClient({ relays: [`ws://localhost:${sourcePort}`], keyPair: keys });

        try {
            await client.connect();
            const guildId = await client.createGuild("Relay Quorum Repair Guild");
            const channelId = await client.createChannel(guildId, "repair", "text");
            for (let index = 0; index < 18; index += 1) {
                await client.sendMessage(guildId, channelId, `repair message ${index}`);
            }
            const sourceStore = (sourceRelay as any).store as LevelStore;
            await waitFor(async () => (await sourceStore.getLastEvent(guildId))?.seq === 19);

            await execFileAsync(npxBin, [
                "tsx",
                "scripts/relay-ops.ts",
                "sync-from-relay",
                "--db",
                mirrorDb,
                "--guild",
                guildId,
                "--relay",
                `ws://127.0.0.1:${sourcePort}`,
                "--trust-source-head",
                "--limit",
                "6",
                "--private-key-hex",
                Buffer.from(keys.priv).toString("hex")
            ], {
                cwd: process.cwd(),
                shell: process.platform === "win32",
                timeout: 20000
            });

            mirrorRelay = new RelayServer(mirrorPort, mirrorDb, [], { enableDefaultPlugins: false });

            await execFileAsync(npxBin, [
                "tsx",
                "scripts/relay-ops.ts",
                "repair-from-quorum",
                "--db",
                targetDb,
                "--guild",
                guildId,
                "--relays",
                `ws://127.0.0.1:${sourcePort},ws://127.0.0.1:${mirrorPort}`,
                "--min-valid-heads",
                "2",
                "--min-canonical-count",
                "2",
                "--limit",
                "5",
                "--private-key-hex",
                Buffer.from(keys.priv).toString("hex")
            ], {
                cwd: process.cwd(),
                shell: process.platform === "win32",
                timeout: 20000
            });

            const targetStore = new LevelStore(targetDb);
            try {
                const sourceHead = await sourceStore.getLastEvent(guildId);
                const targetHead = await targetStore.getLastEvent(guildId);
                expect(targetHead?.seq).toBe(sourceHead?.seq);
                expect(targetHead?.id).toBe(sourceHead?.id);
                const restoredHistory = await targetStore.getHistory({ guildId, channelId, limit: 25 });
                expect(restoredHistory.filter((event) => event.body.type === "MESSAGE")).toHaveLength(18);
            } finally {
                await targetStore.close();
            }
        } finally {
            client.close();
            if (mirrorRelay) {
                await mirrorRelay.close().catch(() => undefined);
            }
            await sourceRelay.close().catch(() => undefined);
        }
    }, 40000);
});
