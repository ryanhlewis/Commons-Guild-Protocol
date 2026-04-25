import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { RelayServer } from "@cgp/relay/src/server";
import { CgpClient } from "@cgp/client/src/client";
import * as secp from "@noble/secp256k1";

async function waitFor(predicate: () => boolean, timeoutMs = 5000) {
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
        if (predicate()) return;
        await new Promise((resolve) => setTimeout(resolve, 50));
    }
    throw new Error("Timed out waiting for client state");
}

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
        await waitFor(() => client.getGuildState(guildId)?.name === "State Test Guild");

        let state = client.getGuildState(guildId);
        expect(state).toBeDefined();
        expect(state?.name).toBe("State Test Guild");
        expect(state?.ownerId).toBe(pubKey);

        // 2. Create Channel
        const channelId = await client.createChannel(guildId, "general", "text");
        await waitFor(() => client.getGuildState(guildId)?.channels.has(channelId) === true);

        state = client.getGuildState(guildId);
        expect(state?.channels.has(channelId)).toBe(true);
        expect(state?.channels.get(channelId)?.name).toBe("general");

        // 3. Assign Role
        const otherUser = "02" + "1".repeat(64); // Fake pubkey
        await client.assignRole(guildId, otherUser, "admin");
        await waitFor(() => client.getGuildState(guildId)?.members.get(otherUser)?.roles.has("admin") === true);

        state = client.getGuildState(guildId);
        const member = state?.members.get(otherUser);
        expect(member).toBeDefined();
        expect(member?.roles.has("admin")).toBe(true);

        // 4. Ban User
        await client.banUser(guildId, otherUser, "Spam");
        await waitFor(() => client.getGuildState(guildId)?.bans.has(otherUser) === true);

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
        await waitFor(() => client1.getGuildState(guildId)?.channels.has(channelId) === true);

        // Client 2 connects and subscribes
        const client2 = new CgpClient({ relays: [relayUrl] }); // Read-only
        await client2.connect();

        // Subscribe
        client2.subscribe(guildId);
        await waitFor(() => client2.getGuildState(guildId)?.channels.has(channelId) === true);

        const state = client2.getGuildState(guildId);
        expect(state).toBeDefined();
        expect(state?.name).toBe("Snapshot Guild");
        expect(state?.channels.has(channelId)).toBe(true);
    });

    it("publishes generic app objects through client helpers", async () => {
        const privKey = secp.utils.randomPrivateKey();
        const pubKey = Buffer.from(secp.getPublicKey(privKey, true)).toString("hex");
        const client = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKey, priv: privKey } });
        await client.connect();

        const guildId = await client.createGuild("App Object Helper Guild");
        await waitFor(() => client.getGuildState(guildId)?.name === "App Object Helper Guild");

        await client.upsertAppObject(guildId, "org.cgp.apps", "agent-profile", pubKey, {
            target: { userId: pubKey },
            value: {
                displayName: "Helper Agent [bot]",
                bot: true,
                agent: true
            }
        });
        await waitFor(() => Array.from(client.getGuildState(guildId)?.appObjects.values() ?? []).some((object) =>
            object.namespace === "org.cgp.apps" &&
            object.objectType === "agent-profile" &&
            object.objectId === pubKey &&
            object.value?.bot === true
        ));

        let state = client.getGuildState(guildId);
        expect(Array.from(state?.appObjects.values() ?? []).some((object) =>
            object.namespace === "org.cgp.apps" &&
            object.objectType === "agent-profile" &&
            object.objectId === pubKey &&
            object.value?.bot === true
        )).toBe(true);

        await client.deleteAppObject(guildId, "org.cgp.apps", "agent-profile", pubKey, {
            target: { userId: pubKey }
        });
        await waitFor(() => !Array.from(client.getGuildState(guildId)?.appObjects.values() ?? []).some((object) =>
            object.namespace === "org.cgp.apps" &&
            object.objectType === "agent-profile" &&
            object.objectId === pubKey
        ));

        state = client.getGuildState(guildId);
        expect(Array.from(state?.appObjects.values() ?? []).some((object) =>
            object.namespace === "org.cgp.apps" &&
            object.objectType === "agent-profile" &&
            object.objectId === pubKey
        )).toBe(false);

        client.close();
    });

    it("rejects stale or forked state replacement anchors", async () => {
        const privKey = secp.utils.randomPrivateKey();
        const pubKey = Buffer.from(secp.getPublicKey(privKey, true)).toString("hex");
        const client = new CgpClient({ relays: [], keyPair: { pub: pubKey, priv: privKey } });

        const guildId = await client.createGuild("Anchor Guild");
        const state = client.getGuildState(guildId);
        expect(state).toBeDefined();

        expect((client as any).responseAnchorCanReplaceState(guildId, state!.headSeq, "bad-hash")).toBe(false);
        expect((client as any).responseAnchorCanReplaceState(guildId, state!.headSeq - 1, state!.headHash)).toBe(false);

        (client as any).trustedRelayHeads.set(guildId, {
            seq: state!.headSeq + 10,
            hash: "future-trusted-head",
            count: 2,
            observedAt: Date.now()
        });
        expect((client as any).responseAnchorCanReplaceState(guildId, state!.headSeq, state!.headHash)).toBe(true);

        (client as any).state.delete(guildId);
        expect((client as any).responseAnchorCanReplaceState(guildId, state!.headSeq, state!.headHash)).toBe(false);
    });
});
