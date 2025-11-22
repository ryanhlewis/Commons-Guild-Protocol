import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { RelayServer } from "@cgp/relay/src/server";
import { CgpClient } from "@cgp/client/src/client";
import * as secp from "@noble/secp256k1";

describe("Client State Tracking", () => {
    let relay: RelayServer;
    const PORT = 7453;
    const relayUrl = `ws://localhost:${PORT}`;
    const DB_PATH = "./test-client-state-db-" + Date.now();

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

    it("tracks guild state updates", async () => {
        const privKey = secp.utils.randomPrivateKey();
        const pubKey = Buffer.from(secp.getPublicKey(privKey, true)).toString("hex");
        const client = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKey, priv: privKey } });
        await client.connect();

        // 1. Create Guild
        const guildId = await client.createGuild("State Test Guild");
        await new Promise(r => setTimeout(r, 100));

        let state = client.getGuildState(guildId);
        expect(state).toBeDefined();
        expect(state?.name).toBe("State Test Guild");
        expect(state?.ownerId).toBe(pubKey);

        // 2. Create Channel
        const channelId = await client.createChannel(guildId, "general", "text");
        await new Promise(r => setTimeout(r, 100));

        state = client.getGuildState(guildId);
        expect(state?.channels.has(channelId)).toBe(true);
        expect(state?.channels.get(channelId)?.name).toBe("general");

        // 3. Assign Role
        const otherUser = "02" + "1".repeat(64); // Fake pubkey
        await client.assignRole(guildId, otherUser, "admin");
        await new Promise(r => setTimeout(r, 100));

        state = client.getGuildState(guildId);
        const member = state?.members.get(otherUser);
        expect(member).toBeDefined();
        expect(member?.roles.has("admin")).toBe(true);

        // 4. Ban User
        await client.banUser(guildId, otherUser, "Spam");
        await new Promise(r => setTimeout(r, 100));

        state = client.getGuildState(guildId);
        expect(state?.bans.has(otherUser)).toBe(true);
    });

    it("rebuilds state from snapshot", async () => {
        // Use the guild from previous test (we need to know the ID, but it's local to that test block)
        // So let's create a new one here.
        const privKey = secp.utils.randomPrivateKey();
        const pubKey = Buffer.from(secp.getPublicKey(privKey, true)).toString("hex");
        const client1 = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKey, priv: privKey } });
        await client1.connect();

        const guildId = await client1.createGuild("Snapshot Guild");
        const channelId = await client1.createChannel(guildId, "snapshot-channel", "text");
        await new Promise(r => setTimeout(r, 200));

        // Client 2 connects and subscribes
        const client2 = new CgpClient({ relays: [relayUrl] }); // Read-only
        await client2.connect();

        // Subscribe
        client2.subscribe(guildId);
        await new Promise(r => setTimeout(r, 200));

        const state = client2.getGuildState(guildId);
        expect(state).toBeDefined();
        expect(state?.name).toBe("Snapshot Guild");
        expect(state?.channels.has(channelId)).toBe(true);
    });
});
