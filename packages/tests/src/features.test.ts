import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { RelayServer } from "@cgp/relay/src/server";
import { CgpClient } from "@cgp/client/src/client";
import * as secp from "@noble/secp256k1";

describe("CGP v1 Features", () => {
    let relay: RelayServer;
    const PORT = 7450;
    const relayUrl = `ws://localhost:${PORT}`;
    const DB_PATH = "./test-features-db-" + Date.now();

    beforeAll(async () => {
        relay = new RelayServer(PORT, DB_PATH);
    });

    afterAll(async () => {
        relay.close();
        await new Promise(r => setTimeout(r, 100));
        const fs = await import("fs/promises");
        try {
            await fs.rm(DB_PATH, { recursive: true, force: true });
        } catch (e) {
            console.error("Failed to cleanup test DB", e);
        }
    });

    it("supports message editing and deletion", async () => {
        const privKey = secp.utils.randomPrivateKey();
        const pubKey = Buffer.from(secp.getPublicKey(privKey, true)).toString("hex");
        const client = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKey, priv: privKey } });
        await client.connect();

        const guildId = await client.createGuild("Edit Test Guild");
        const channelId = await client.createChannel(guildId, "general", "text");
        await new Promise(r => setTimeout(r, 100));

        // 1. Send Message
        const msgId = await client.sendMessage(guildId, channelId, "Original Content");

        // Capture events
        const events: any[] = [];
        client.on("event", e => events.push(e.body));
        client.subscribe(guildId);
        await new Promise(r => setTimeout(r, 100));

        // 2. Edit Message
        await client.editMessage(guildId, channelId, msgId, "Edited Content");
        await new Promise(r => setTimeout(r, 100));

        const editEvent = events.find(e => e.type === "EDIT_MESSAGE" && e.messageId === msgId);
        expect(editEvent).toBeDefined();
        expect(editEvent.newContent).toBe("Edited Content");

        // 3. Delete Message
        await client.deleteMessage(guildId, channelId, msgId, "Regret");
        await new Promise(r => setTimeout(r, 100));

        const deleteEvent = events.find(e => e.type === "DELETE_MESSAGE" && e.messageId === msgId);
        expect(deleteEvent).toBeDefined();
        expect(deleteEvent.reason).toBe("Regret");
    });

    it("supports forking a guild", async () => {
        const privKey = secp.utils.randomPrivateKey();
        const pubKey = Buffer.from(secp.getPublicKey(privKey, true)).toString("hex");
        const client = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKey, priv: privKey } });
        await client.connect();

        const originalGuildId = await client.createGuild("Original Guild");
        await new Promise(r => setTimeout(r, 100));

        // Fork it
        const forkGuildId = await client.forkGuild(originalGuildId, 10, "hash_of_seq_10", "Forked Guild");
        await new Promise(r => setTimeout(r, 100));

        expect(forkGuildId).not.toBe(originalGuildId);

        // Verify FORK_FROM event in new guild
        const events: any[] = [];
        client.on("event", e => events.push(e.body));
        client.subscribe(forkGuildId);
        await new Promise(r => setTimeout(r, 100));

        const forkEvent = events.find(e => e.type === "FORK_FROM");
        expect(forkEvent).toBeDefined();
        expect(forkEvent.parentGuildId).toBe(originalGuildId);
        expect(forkEvent.note).toBe(`Fork of ${originalGuildId}`);
    });
});
