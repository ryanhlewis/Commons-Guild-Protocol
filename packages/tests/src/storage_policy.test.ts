import { describe, expect, it } from "vitest";
import { CgpClient } from "@cgp/client/src/client";
import { computeEventId, generatePrivateKey, getPublicKey, hashObject, type GuildEvent } from "@cgp/core";
import { RelayServer } from "@cgp/relay/src/server";
import { LevelStore } from "@cgp/relay/src/store_level";
import fs from "node:fs/promises";

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

function testEvent(seq: number, body: any): GuildEvent {
    const event = {
        id: "",
        seq,
        prevHash: seq > 0 ? `prev-${seq}` : null,
        createdAt: 1_700_000_000_000 + seq,
        author: "storage-test",
        signature: `sig-${seq}`,
        body
    } as GuildEvent;
    event.id = computeEventId(event);
    return event;
}

describe("relay storage policy", () => {
    it("estimates LevelStore disk usage", async () => {
        const dbPath = `./test-storage-estimate-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const store = new LevelStore(dbPath);
        try {
            await (store as any).db.open();
            await store.appendEvents("guild-storage-estimate", [
                testEvent(0, { type: "GUILD_CREATE", guildId: "guild-storage-estimate", name: "Storage Estimate" }),
                testEvent(1, { type: "CHANNEL_CREATE", guildId: "guild-storage-estimate", channelId: "general", name: "general", kind: "text" }),
                testEvent(2, { type: "MESSAGE", guildId: "guild-storage-estimate", channelId: "general", messageId: "m1", content: "hello" })
            ]);

            const estimate = await store.estimateStorage();
            expect(estimate.bytes).toBeGreaterThan(0);
            expect(estimate.files).toBeGreaterThan(0);
            expect(estimate.path).toContain("test-storage-estimate");
        } finally {
            await store.close();
            await fs.rm(dbPath, { recursive: true, force: true });
        }
    });

    it("rejects writes beyond the hard storage limit", async () => {
        const dbPath = `./test-storage-hard-limit-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const relay = new RelayServer(0, dbPath, [], {
            enableDefaultPlugins: false,
            storagePolicy: {
                softLimitBytes: 0,
                hardLimitBytes: 1,
                estimateIntervalMs: 1000
            }
        });
        const client = new CgpClient({ relays: [`ws://localhost:${await waitForPort(relay)}`], keyPair: keyPair() });
        try {
            await client.connect();
            const guildId = hashObject({ test: "storage-hard-limit", nonce: Date.now() });
            await expect(client.publishReliable({
                type: "GUILD_CREATE",
                guildId,
                name: "Storage Hard Limit"
            }, { timeoutMs: 3000 })).rejects.toThrow(/storage limit/i);
        } finally {
            client.close();
            await relay.close();
            await fs.rm(dbPath, { recursive: true, force: true });
        }
    });

    it("reports not_ready after crossing the soft storage limit", async () => {
        const dbPath = `./test-storage-soft-limit-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const relay = new RelayServer(0, dbPath, [], {
            enableDefaultPlugins: false,
            storagePolicy: {
                softLimitBytes: 1,
                hardLimitBytes: 0,
                estimateIntervalMs: 1000
            }
        });
        const port = await waitForPort(relay);
        const client = new CgpClient({ relays: [`ws://localhost:${port}`], keyPair: keyPair() });
        try {
            await client.connect();
            const guildId = hashObject({ test: "storage-soft-limit", nonce: Date.now() });
            await client.publishReliable({
                type: "GUILD_CREATE",
                guildId,
                name: "Storage Soft Limit"
            }, { timeoutMs: 3000 });

            const response = await fetch(`http://localhost:${port}/readyz`);
            const payload = await response.json() as any;
            expect(response.status).toBe(503);
            expect(payload.status).toBe("not_ready");
            expect(payload.storage.overSoftLimit).toBe(true);
            expect(payload.storage.bytes).toBeGreaterThan(0);
        } finally {
            client.close();
            await relay.close();
            await fs.rm(dbPath, { recursive: true, force: true });
        }
    });
});
