import { describe, it, expect, afterEach, beforeEach } from "vitest";
import { RelayServer } from "@cgp/relay";
import { CgpClient } from "@cgp/client";
import { generatePrivateKey, getPublicKey } from "@cgp/core";
import fs from "fs";

const DB_PATH = "./test-relay-db-reconnect";
const PORT = 8087;

describe("Client Reconnection", () => {
    let relay: RelayServer | undefined;
    let client: CgpClient;

    beforeEach(async () => {
        if (fs.existsSync(DB_PATH)) fs.rmSync(DB_PATH, { recursive: true, force: true });
        relay = new RelayServer(PORT, DB_PATH);
    });

    afterEach(async () => {
        client?.close();
        if (relay) await relay.close();
        // Wait for LevelDB to close
        await new Promise(resolve => setTimeout(resolve, 100));
        if (fs.existsSync(DB_PATH)) fs.rmSync(DB_PATH, { recursive: true, force: true });
    });

    it("reconnects when relay restarts", async () => {
        const priv = generatePrivateKey();
        const pub = getPublicKey(priv);

        client = new CgpClient({
            relays: [`ws://localhost:${PORT}`],
            keyPair: { pub, priv }
        });

        // 1. Initial Connect
        await client.connect();

        // Verify connection by creating a guild
        const guildId = await client.createGuild("Reconnect Guild");
        expect(guildId).toBeDefined();

        // 2. Stop Relay
        await relay!.close();
        relay = undefined;

        // Wait for client to detect disconnect
        await new Promise(resolve => setTimeout(resolve, 500));

        // 3. Restart Relay
        if (fs.existsSync(DB_PATH)) fs.rmSync(DB_PATH, { recursive: true, force: true });
        relay = new RelayServer(PORT, DB_PATH);

        // Wait for client to reconnect (backoff might be 1s)
        console.log("Waiting for reconnection...");
        await new Promise(resolve => setTimeout(resolve, 2000));

        // 4. Verify Reconnection by sending a message (or creating channel)
        // Note: The new relay won't have the old guild unless we persisted it (we deleted DB).
        // So we should try to create a NEW guild or just check if we can send anything.
        // Since we deleted the DB, the old guild is gone.
        // Let's create a new guild.

        let newGuildId = "";
        try {
            newGuildId = await client.createGuild("After Reconnect");
        } catch (e) {
            console.error("Failed to create guild after reconnect", e);
        }

        expect(newGuildId).toBeTruthy();
    });
});
