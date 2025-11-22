import { describe, it, expect, afterEach, beforeEach } from "vitest";
import { RelayServer } from "@cgp/relay";
import { CgpClient } from "@cgp/client";
import { ChannelCreate, GuildEvent, generatePrivateKey, getPublicKey } from "@cgp/core";
import fs from "fs";
import path from "path";

const DB_PATH = "./test-relay-db-ephemeral";

describe("Ephemeral Channels", () => {
    let relay: RelayServer;
    let client: CgpClient;

    beforeEach(async () => {
        if (fs.existsSync(DB_PATH)) fs.rmSync(DB_PATH, { recursive: true, force: true });
        relay = new RelayServer(8086, DB_PATH);

        const priv = generatePrivateKey();
        const pub = getPublicKey(priv);

        client = new CgpClient({
            relays: ["ws://localhost:8086"],
            keyPair: { pub, priv }
        });
        await client.connect();
    });

    afterEach(async () => {
        client.close();
        await relay.close();
        // Wait for LevelDB to close
        await new Promise(resolve => setTimeout(resolve, 100));
        if (fs.existsSync(DB_PATH)) fs.rmSync(DB_PATH, { recursive: true, force: true });
    });

    it("prunes messages after TTL expires", async () => {
        // 1. Create Guild
        const guildId = await client.createGuild("Ephemeral Guild");

        // 2. Create Ephemeral Channel (TTL = 1 second)
        const channelId = await client.createChannel(guildId, "temp-chat", "ephemeral-text", {
            mode: "ttl",
            seconds: 1
        });

        // 3. Send Message
        const msgId = await client.sendMessage(guildId, channelId, "This message will self-destruct");

        // Wait for relay to process
        await new Promise(resolve => setTimeout(resolve, 500));

        // 4. Verify message exists in relay
        let log = await (relay as any).store.getLog(guildId);
        expect(log.find((e: GuildEvent) => e.body.type === "MESSAGE" && (e.body as any).messageId === msgId)).toBeDefined();

        // 5. Wait for TTL to expire
        await new Promise(resolve => setTimeout(resolve, 1500));

        // 6. Trigger Prune
        await relay.prune();

        // 7. Verify message is gone
        log = await (relay as any).store.getLog(guildId);
        expect(log.find((e: GuildEvent) => e.body.type === "MESSAGE" && (e.body as any).messageId === msgId)).toBeUndefined();

        // 8. Verify GuildCreate and ChannelCreate are still there
        expect(log.find((e: GuildEvent) => e.body.type === "GUILD_CREATE")).toBeDefined();
        expect(log.find((e: GuildEvent) => e.body.type === "CHANNEL_CREATE")).toBeDefined();
    });

    it("does not prune messages before TTL expires", async () => {
        // 1. Create Guild
        const guildId = await client.createGuild("Persistent Guild");

        // 2. Create Ephemeral Channel (TTL = 5 seconds)
        const channelId = await client.createChannel(guildId, "long-temp-chat", "ephemeral-text", {
            mode: "ttl",
            seconds: 5
        });

        // 3. Send Message
        const msgId = await client.sendMessage(guildId, channelId, "I should stay for a bit");

        // Wait for relay to process
        await new Promise(resolve => setTimeout(resolve, 500));

        // 4. Wait a little (but less than TTL)
        await new Promise(resolve => setTimeout(resolve, 1000));

        // 5. Trigger Prune
        await relay.prune();

        // 6. Verify message is STILL there
        const log = await (relay as any).store.getLog(guildId);
        expect(log.find((e: GuildEvent) => e.body.type === "MESSAGE" && (e.body as any).messageId === msgId)).toBeDefined();
    });
});
