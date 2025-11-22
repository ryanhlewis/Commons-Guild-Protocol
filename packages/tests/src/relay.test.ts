import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { RelayServer } from "@cgp/relay/src/server"; // Import directly from src to avoid build step in dev
import { CgpClient } from "@cgp/client/src/client";
import * as secp from "@noble/secp256k1";

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
