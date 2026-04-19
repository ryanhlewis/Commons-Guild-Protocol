import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { RelayServer } from "@cgp/relay/src/server"; // Import directly from src to avoid build step in dev
import { MemoryStore } from "@cgp/relay/src/store";
import { CgpClient } from "@cgp/client/src/client";
import { hashObject, sign } from "@cgp/core";
import * as secp from "@noble/secp256k1";
import WebSocket from "ws";

describe("CGP relay basic messaging", () => {
    let relay: RelayServer;
    const PORT = 7448;
    const relayUrl = `ws://localhost:${PORT}`;
    const DB_PATH = "./test-relay-db-" + Date.now();

    beforeAll(async () => {
        relay = new RelayServer(PORT, DB_PATH);
    });

    afterAll(async () => {
        await relay.close();
        // Wait for DB to close
        await new Promise(r => setTimeout(r, 100));
        const fs = await import("fs/promises");
        try {
            await fs.rm(DB_PATH, { recursive: true, force: true });
        } catch (e) {
            console.error("Failed to cleanup test DB", e);
        }
    });

    it("sends a message between two peers", async () => {
        const privKeyA = secp.utils.randomPrivateKey();
        const pubKeyA = Buffer.from(secp.getPublicKey(privKeyA, true)).toString("hex");

        const privKeyB = secp.utils.randomPrivateKey();
        const pubKeyB = Buffer.from(secp.getPublicKey(privKeyB, true)).toString("hex");

        const alice = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKeyA, priv: privKeyA } });
        const bob = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKeyB, priv: privKeyB } });

        await Promise.all([alice.connect(), bob.connect()]);

        // Alice creates a guild
        const guildId = await alice.createGuild("Test Guild");

        // Bob subscribes
        bob.subscribe(guildId);
        await new Promise((res) => setTimeout(res, 100));

        const messages: any[] = [];
        bob.on("event", (ev) => {
            if (ev.body.type === "MESSAGE") {
                messages.push(ev.body.content);
            }
        });

        // Alice creates a channel (REQUIRED for validation)
        await alice.publish({
            type: "CHANNEL_CREATE",
            guildId,
            channelId: "general",
            name: "general",
            kind: "text"
        });

        await new Promise((res) => setTimeout(res, 200)); // Wait for channel to be processed

        // Alice sends a message
        await alice.publish({
            type: "MESSAGE",
            guildId,
            channelId: "general", // matches created channel
            content: "hello from alice"
        });

        // wait for async propagation
        await new Promise((res) => setTimeout(res, 1000));

        expect(messages).toContain("hello from alice");
    });

    it("supports high-level API (channels, roles, bans)", async () => {
        const privKeyA = secp.utils.randomPrivateKey();
        const pubKeyA = Buffer.from(secp.getPublicKey(privKeyA, true)).toString("hex");
        const alice = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKeyA, priv: privKeyA } });
        await alice.connect();

        const guildId = await alice.createGuild("API Test Guild");

        // Create channel
        const channelId = await alice.createChannel(guildId, "general", "text");
        expect(channelId).toBeDefined();

        // Send message
        const msgId = await alice.sendMessage(guildId, channelId, "Hello API");
        expect(msgId).toBeDefined();

        // Assign role
        await alice.assignRole(guildId, "some_user", "admin");

        // Ban user
        await alice.banUser(guildId, "bad_user", "spam");

        // Wait for propagation (local relay is fast but async)
        await new Promise((res) => setTimeout(res, 200));
    });

    it("serves paginated channel history snapshots", async () => {
        const privKey = secp.utils.randomPrivateKey();
        const pubKey = Buffer.from(secp.getPublicKey(privKey, true)).toString("hex");
        const alice = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKey, priv: privKey } });
        await alice.connect();

        const guildId = await alice.createGuild("History Guild");
        const channelId = await alice.createChannel(guildId, "history", "text");
        await new Promise((res) => setTimeout(res, 200));

        for (let i = 0; i < 5; i++) {
            await alice.sendMessage(guildId, channelId, `history-${i}`);
        }
        await new Promise((res) => setTimeout(res, 300));

        const ws = new WebSocket(relayUrl);
        await new Promise<void>((resolve) => ws.onopen = () => resolve());

        const waitForSnapshot = (subId: string) => new Promise<any>((resolve) => {
            const handler = (raw: WebSocket.RawData) => {
                const data = JSON.parse(raw.toString());
                if (data[0] === "SNAPSHOT" && data[1]?.subId === subId) {
                    ws.off("message", handler);
                    resolve(data[1]);
                }
            };
            ws.on("message", handler);
        });

        const firstPage = waitForSnapshot("history-page-1");
        ws.send(JSON.stringify(["GET_HISTORY", { subId: "history-page-1", guildId, channelId, limit: 2 }]));
        const first = await firstPage;
        expect(first.events.map((event: any) => event.body.content)).toEqual(["history-3", "history-4"]);
        expect(first.hasMore).toBe(true);

        const secondPage = waitForSnapshot("history-page-2");
        ws.send(JSON.stringify(["GET_HISTORY", { subId: "history-page-2", guildId, channelId, beforeSeq: first.oldestSeq, limit: 2 }]));
        const second = await secondPage;
        expect(second.events.map((event: any) => event.body.content)).toEqual(["history-1", "history-2"]);
        expect(second.hasMore).toBe(true);

        ws.close();
        alice.close();
    });

    it("returns correlated publish acknowledgements and validation errors", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const attackerPrivKey = secp.utils.randomPrivateKey();
        const attackerPubKey = Buffer.from(secp.getPublicKey(attackerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        await owner.connect();

        const guildId = await owner.createGuild("Ack Guild");
        const channelId = await owner.createChannel(guildId, "ack", "text");
        await new Promise((res) => setTimeout(res, 200));

        const ws = new WebSocket(relayUrl);
        await new Promise<void>((resolve) => ws.onopen = () => resolve());

        const waitForFrame = (kind: string, clientEventId: string) => new Promise<any>((resolve) => {
            const handler = (raw: WebSocket.RawData) => {
                const data = JSON.parse(raw.toString());
                if (data[0] === kind && data[1]?.clientEventId === clientEventId) {
                    ws.off("message", handler);
                    resolve(data[1]);
                }
            };
            ws.on("message", handler);
        });

        const ackBody = {
            type: "MESSAGE",
            guildId,
            channelId,
            messageId: `ack-message-${Date.now()}`,
            content: "ack me"
        };
        const ackCreatedAt = Date.now();
        const ackClientEventId = "publish-ack-test";
        const ackSignature = await sign(ownerPrivKey, hashObject({ body: ackBody, author: ownerPubKey, createdAt: ackCreatedAt }));
        const ackPromise = waitForFrame("PUB_ACK", ackClientEventId);
        ws.send(JSON.stringify([
            "PUBLISH",
            {
                body: ackBody,
                author: ownerPubKey,
                createdAt: ackCreatedAt,
                signature: ackSignature,
                clientEventId: ackClientEventId
            }
        ]));
        const ack = await ackPromise;
        expect(ack.guildId).toBe(guildId);
        expect(ack.eventId).toBeDefined();
        expect(typeof ack.seq).toBe("number");

        const errorBody = {
            type: "CHANNEL_CREATE",
            guildId,
            channelId: `attacker-channel-${Date.now()}`,
            name: "attacker",
            kind: "text"
        };
        const errorCreatedAt = Date.now();
        const errorClientEventId = "publish-error-test";
        const errorSignature = await sign(
            attackerPrivKey,
            hashObject({ body: errorBody, author: attackerPubKey, createdAt: errorCreatedAt })
        );
        const errorPromise = waitForFrame("ERROR", errorClientEventId);
        ws.send(JSON.stringify([
            "PUBLISH",
            {
                body: errorBody,
                author: attackerPubKey,
                createdAt: errorCreatedAt,
                signature: errorSignature,
                clientEventId: errorClientEventId
            }
        ]));
        const error = await errorPromise;
        expect(error.code).toBe("VALIDATION_FAILED");
        expect(error.message).toContain("permission");

        ws.close();
        owner.close();
    });

    it("keeps rate limiting as a configurable relay plugin, not mandatory core protocol", async () => {
        const pluginlessPort = PORT + 1;
        const pluginlessRelay = new RelayServer(pluginlessPort, new MemoryStore(), [], {
            enableDefaultPlugins: false
        });
        const ws = new WebSocket(`ws://localhost:${pluginlessPort}`);

        try {
            await new Promise<void>((resolve) => ws.onopen = () => resolve());
            const helloPromise = new Promise<any>((resolve) => {
                ws.onmessage = (msg) => {
                    const data = JSON.parse(msg.data.toString());
                    if (data[0] === "HELLO_OK") {
                        resolve(data[1]);
                    }
                };
            });
            ws.send(JSON.stringify(["HELLO", { protocol: "cgp/0.1" }]));
            const hello = await helloPromise;
            expect(hello.plugins).toEqual([]);
        } finally {
            ws.close();
            await pluginlessRelay.close();
        }
    });

    it("enforces validation rules", async () => {
        const privKeyA = secp.utils.randomPrivateKey();
        const pubKeyA = Buffer.from(secp.getPublicKey(privKeyA, true)).toString("hex");
        const alice = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKeyA, priv: privKeyA } });

        const privKeyB = secp.utils.randomPrivateKey();
        const pubKeyB = Buffer.from(secp.getPublicKey(privKeyB, true)).toString("hex");
        const bob = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKeyB, priv: privKeyB } });

        await Promise.all([alice.connect(), bob.connect()]);

        const guildId = await alice.createGuild("Validation Guild");

        bob.subscribe(guildId);
        await new Promise(r => setTimeout(r, 100));

        const receivedEvents: any[] = [];
        bob.on("event", e => receivedEvents.push(e.body));

        // 1. Bob tries to create channel (should fail - not owner)
        await bob.publish({
            type: "CHANNEL_CREATE",
            guildId,
            channelId: "bob_chan",
            name: "Bob Channel",
            kind: "text"
        });

        await new Promise(r => setTimeout(r, 200));
        expect(receivedEvents.some(e => e.channelId === "bob_chan")).toBe(false);

        // 2. Alice creates channel (should succeed)
        const chanId = await alice.createChannel(guildId, "general", "text");
        await new Promise(r => setTimeout(r, 200));
        expect(receivedEvents.some(e => e.channelId === chanId)).toBe(true);

        // 3. Alice bans Bob
        await alice.banUser(guildId, pubKeyB, "spam");
        await new Promise(r => setTimeout(r, 200));
        expect(receivedEvents.some(e => e.type === "BAN_USER" && e.userId === pubKeyB)).toBe(true);

        // 4. Bob tries to send message (should fail - banned)
        await bob.sendMessage(guildId, chanId, "I am banned");
        await new Promise(r => setTimeout(r, 200));
        expect(receivedEvents.some(e => e.content === "I am banned")).toBe(false);
    });
});
