import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { CgpClient } from "@cgp/client";
import { generatePrivateKey, getPublicKey } from "@cgp/core";

describe("Group E2EE", () => {
    let owner: CgpClient;
    let member: CgpClient;
    let relay: any;
    const port = 8003;
    const relayUrl = `ws://localhost:${port}`;

    beforeAll(async () => {
        // Start a relay
        const { RelayServer } = await import("@cgp/relay");
        relay = new RelayServer(port);
        console.log("Relay started");

        // Create clients
        const ownerPriv = generatePrivateKey();
        owner = new CgpClient({
            relays: [relayUrl],
            keyPair: { pub: getPublicKey(ownerPriv), priv: ownerPriv }
        });

        const memberPriv = generatePrivateKey();
        member = new CgpClient({
            relays: [relayUrl],
            keyPair: { pub: getPublicKey(memberPriv), priv: memberPriv }
        });

        await owner.connect();
        await member.connect();
    });

    afterAll(() => {
        owner.close();
        member.close();
        relay.close();
    });

    it("should encrypt messages with a group key in a private guild", async () => {
        // 1. Owner creates a private guild
        const guildId = await owner.createGuild("Secret Guild", "Shh", "private");
        await owner.subscribe(guildId); // Owner subscribes
        await new Promise(r => setTimeout(r, 100));

        // 2. Owner sends a message (should be encrypted with Group Key)
        const channelId = await owner.createChannel(guildId, "general", "text");
        await new Promise(r => setTimeout(r, 100));

        await owner.sendMessage(guildId, channelId, "Hello Group!");
        await new Promise(r => setTimeout(r, 100));

        // 3. Member joins (simulated by Owner assigning role)

        // 4. Owner assigns role to Member (distributing the key)
        await owner.assignRole(guildId, member["keyPair"]!.pub, "member");
        await new Promise(r => setTimeout(r, 200));

        // Attach listener BEFORE subscribing to catch snapshot events
        let receivedMessage = "";
        member.on("event", async (e) => {
            if (e.body.type === "MESSAGE") {
                const decrypted = await member.decryptMessage(e);
                if (decrypted === "Hello Group!") {
                    receivedMessage = decrypted;
                }
            }
        });

        // 5. Member subscribes (simulating accepting invite or clicking link)
        await member.subscribe(guildId);
        await new Promise(r => setTimeout(r, 500)); // Wait for snapshot and processing

        // 6. Member should now be able to decrypt the history
        const memberState = member.getGuildState(guildId);
        expect(memberState).toBeDefined();

        expect(receivedMessage).toBe("Hello Group!");
    });

    it("should allow member to send encrypted messages", async () => {
        const guildId = [...owner["state"].keys()][0];
        const state = owner.getGuildState(guildId)!;
        const channelId = [...state.channels.keys()][0];

        let ownerReceived = "";
        owner.on("event", async (e) => {
            if (e.body.type === "MESSAGE" && e.author === member["keyPair"]!.pub) {
                const decrypted = await owner.decryptMessage(e);
                ownerReceived = decrypted;
            }
        });

        await member.sendMessage(guildId, channelId, "Reply from Member");
        await new Promise(r => setTimeout(r, 500));

        expect(ownerReceived).toBe("Reply from Member");
    });
});
