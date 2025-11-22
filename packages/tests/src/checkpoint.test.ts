import { RelayServer } from "@cgp/relay";
import { CgpClient } from "@cgp/client";
import { GuildEvent, createInitialState, applyEvent, deserializeState } from "@cgp/core";
import fs from "fs";
import path from "path";
import os from "os";
import { describe, test, expect, beforeAll, afterAll } from "vitest";

const TEST_PORT = 8085;
const DB_PATH = path.join(os.tmpdir(), "cgp-relay-checkpoint-test");

describe("Checkpoint Tests", () => {
    let relay: RelayServer;
    let client: CgpClient;
    let guildId: string;
    let keyPair: { publicKey: string; privateKey: Uint8Array };

    beforeAll(async () => {
        if (fs.existsSync(DB_PATH)) {
            fs.rmSync(DB_PATH, { recursive: true, force: true });
        }
        relay = new RelayServer(TEST_PORT, DB_PATH);

        // Generate client keys
        const { generatePrivateKey, getPublicKey } = await import("@cgp/core");
        const priv = generatePrivateKey();
        const pub = getPublicKey(priv);
        keyPair = { publicKey: pub, privateKey: priv };

        client = new CgpClient({
            relays: [`ws://localhost:${TEST_PORT}`],
            keyPair: { pub, priv }
        });
        await client.connect();
        await new Promise(resolve => setTimeout(resolve, 100));
    });

    afterAll(async () => {
        client.close();
        await relay.close();
        if (fs.existsSync(DB_PATH)) {
            fs.rmSync(DB_PATH, { recursive: true, force: true });
        }
    });

    test("creates and verifies a checkpoint", async () => {
        // 1. Create Guild
        guildId = await client.createGuild("Checkpoint Guild");

        // 2. Add some events
        const channelId = await client.createChannel(guildId, "general", "text");
        await client.sendMessage(guildId, channelId, "Hello World");
        await client.assignRole(guildId, keyPair.publicKey, "admin");

        // Wait for events to be processed
        await new Promise(resolve => setTimeout(resolve, 500));

        // 3. Force Checkpoint creation
        await relay.createCheckpoints();

        // 4. Verify Checkpoint Event
        const log = await (relay as any).store.getLog(guildId);
        const checkpointEvent = log.find((e: GuildEvent) => e.body.type === "CHECKPOINT");

        expect(checkpointEvent).toBeDefined();
        expect(checkpointEvent.body.type).toBe("CHECKPOINT");
        expect(checkpointEvent.body.state).toBeDefined();

        // 5. Verify State Reconstruction from Checkpoint
        const serializedState = checkpointEvent.body.state;
        const reconstructedState = deserializeState(serializedState, checkpointEvent.seq, checkpointEvent.id, checkpointEvent.createdAt);

        expect(reconstructedState.guildId).toBe(guildId);
        expect(reconstructedState.name).toBe("Checkpoint Guild");
        expect(reconstructedState.channels.has(channelId)).toBe(true);
        expect(reconstructedState.members.get(keyPair.publicKey)?.roles.has("admin")).toBe(true);
    });
});
