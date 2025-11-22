import { describe, it, expect, afterEach } from "vitest";
import { CgpClient } from "@cgp/client";
import { generatePrivateKey, getPublicKey } from "@cgp/core";

describe("P2P Communication", () => {
    let clientA: CgpClient;
    let clientB: CgpClient;

    afterEach(() => {
        clientA?.close();
        clientB?.close();
    });

    it("allows direct client-to-client messaging", async () => {
        const privA = generatePrivateKey();
        const pubA = getPublicKey(privA);
        const privB = generatePrivateKey();
        const pubB = getPublicKey(privB);

        // Client A acts as a server
        clientA = new CgpClient({ relays: [], keyPair: { pub: pubA, priv: privA } });
        await clientA.listen(9001);

        // Client B connects to Client A
        clientB = new CgpClient({ relays: [], keyPair: { pub: pubB, priv: privB } });
        await clientB.connectToPeer("ws://localhost:9001");

        // Client A creates a guild
        const guildId = await clientA.createGuild("P2P Guild");

        // Client B needs to know about the guild to receive events?
        // In current impl, `handleMessage` emits "event" regardless of subscription if it receives it.
        // But usually we filter.
        // Let's see if Client B receives the GUILD_CREATE event via gossip/broadcast.
        // Wait, `createGuild` calls `publish`. `publish` sends to `sockets`.
        // `sockets` includes connected peers (in `listen` and `connectToPeer`).
        // So Client A should send to Client B.

        const receivedEvents: any[] = [];
        clientB.on("event", (e) => receivedEvents.push(e));

        // Wait for event propagation
        await new Promise(resolve => setTimeout(resolve, 500));

        expect(receivedEvents.length).toBeGreaterThan(0);
        expect(receivedEvents[0].body.type).toBe("GUILD_CREATE");
        expect(receivedEvents[0].body.guildId).toBe(guildId);

        // Client B sends a message to the guild
        // Client B needs to create a channel first? Or Client A creates it.
        const channelId = await clientA.createChannel(guildId, "general", "text");

        await new Promise(resolve => setTimeout(resolve, 500));

        // Client B sends message
        await clientB.sendMessage(guildId, channelId, "Hello P2P!");

        // Client A should receive it
        const receivedEventsA: any[] = [];
        clientA.on("event", (e) => receivedEventsA.push(e));

        // Wait for propagation
        await new Promise(resolve => setTimeout(resolve, 500));

        // Note: Client A's `on` listener was added AFTER the message might have arrived?
        // No, we added it now. But Client B sent it just now.
        // Actually, `sendMessage` is async.

        // Let's check if Client A state has the message
        const stateA = clientA.getGuildState(guildId);
        // We need to wait a bit more or check state.
        // But `sendMessage` sends to `sockets`. Client B's sockets include Client A (from `connectToPeer`).
        // So Client A should receive it.

        // Wait and check
        await new Promise(resolve => setTimeout(resolve, 500));

        // Check if Client A received the message event
        // Since we attached listener late, we might have missed it if it was fast.
        // But we can check state if we implement state update on P2P.
        // `handleMessage` calls `updateState`.

        // Let's check if we can find the message in Client A's state (if we implemented message storage in state, which we didn't fully, only channels/members).
        // Actually `GuildState` doesn't store messages (it's too big).
        // So we rely on the listener.

        // Let's retry sending from B to A with listener attached.
        await clientB.sendMessage(guildId, channelId, "Hello again P2P!");
        await new Promise(resolve => setTimeout(resolve, 500));

        const msgEvent = receivedEventsA.find(e => e.body.type === "MESSAGE" && e.body.content === "Hello again P2P!");
        expect(msgEvent).toBeDefined();
    });
});
