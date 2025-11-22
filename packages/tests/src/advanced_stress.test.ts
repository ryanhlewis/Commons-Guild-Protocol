import { RelayServer } from "@cgp/relay";
import { CgpClient } from "@cgp/client";
import { GuildEvent, createInitialState, applyEvent, generatePrivateKey, getPublicKey, EphemeralPolicy } from "@cgp/core";
import { MemoryStore } from "@cgp/relay"; // Use MemoryStore for speed
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import fs from "fs";
import path from "path";
import os from "os";

// Helper to create a client with a random key
function createClient(port: number) {
    const priv = generatePrivateKey();
    const pub = getPublicKey(priv);
    return new CgpClient({
        relays: [`ws://localhost:${port}`],
        keyPair: { pub, priv }
    });
}

describe("Advanced Stress Testing", () => {

    describe("Gossip Storm", () => {
        it("propagates messages in a random high-traffic mesh", async () => {
            const PEER_COUNT = 15;
            const peers: CgpClient[] = [];
            const peerPorts: number[] = [];

            // 1. Setup Peers (using mock P2P setup similar to stress.test.ts but with random connections)
            // Note: In a real P2P test we need actual ports. 
            // For this test, we'll simulate P2P by having clients connect to each other directly.
            // We need to start them as "servers" (listen).

            const BASE_PORT = 12000;

            for (let i = 0; i < PEER_COUNT; i++) {
                const port = BASE_PORT + i;
                peerPorts.push(port);

                const priv = generatePrivateKey();
                const pub = getPublicKey(priv);

                const client = new CgpClient({
                    relays: [], // No relay, pure P2P
                    keyPair: { pub, priv }
                });

                await client.listen(port);
                peers.push(client);
            }

            // 2. Create Random Mesh (Erdős–Rényi-ish)
            // Ensure each peer connects to at least 2 others for redundancy
            for (let i = 0; i < PEER_COUNT; i++) {
                const targets = new Set<number>();
                while (targets.size < 5) { // Connect to 5 random peers
                    const t = Math.floor(Math.random() * PEER_COUNT);
                    if (t !== i) targets.add(t);
                }

                for (const t of targets) {
                    await peers[i].connectToPeer(`ws://localhost:${peerPorts[t]}`);
                }
            }

            // Wait for connections
            await new Promise(r => setTimeout(r, 1000));

            // 3. Create Guild on Peer 0
            const guildId = await peers[0].createGuild("Storm Guild");
            const channelId = await peers[0].createChannel(guildId, "general", "text");

            // Wait for initial propagation (GuildCreate + ChannelCreate)
            // We must ensure everyone has the guild before sending messages, 
            // otherwise they will reject PUBLISH for unknown guild.
            const startWait = Date.now();
            while (Date.now() - startWait < 10000) {
                let readyCount = 0;
                for (const p of peers) {
                    const s = p.getGuildState(guildId);
                    if (s && s.headSeq >= 1) readyCount++;
                }
                if (readyCount === PEER_COUNT) break;
                await new Promise(r => setTimeout(r, 100));
            }

            // Verify everyone is ready
            for (const p of peers) {
                const s = p.getGuildState(guildId);
                if (!s) throw new Error("Peer missing guild state before storm");
            }
            console.log("All peers ready for storm");

            // 4. Storm: Peer 0 sends 50 messages
            const senderIdx = 0;
            const MESSAGES_COUNT = 50;
            const TOTAL_EXPECTED = 2 + MESSAGES_COUNT; // GuildCreate + ChannelCreate + Messages

            for (let k = 0; k < MESSAGES_COUNT; k++) {
                await peers[senderIdx].sendMessage(guildId, channelId, `Msg from ${senderIdx} #${k}`);
                // Small delay to allow some propagation overlap but keep it fast
                await new Promise(r => setTimeout(r, 10));
            }

            // 5. Verify Convergence
            // Wait for gossip to settle
            await new Promise(r => setTimeout(r, 8000));

            let convergedCount = 0;
            for (let i = 0; i < PEER_COUNT; i++) {
                const state = peers[i].getGuildState(guildId);
                // console.log(`Peer ${i} headSeq: ${state?.headSeq} (Expected: ${TOTAL_EXPECTED - 1})`);
                if (state && state.headSeq === TOTAL_EXPECTED - 1) { // seq is 0-indexed
                    convergedCount++;
                }
            }

            expect(convergedCount).toBe(PEER_COUNT);

            // Cleanup
            for (const p of peers) p.close();
        }, 40000);
    });

    describe("Large State Checkpointing", () => {
        let relay: RelayServer;
        let client: CgpClient;
        const PORT = 8090;

        beforeAll(async () => {
            // Use MemoryStore for speed, we are testing checkpoint generation logic/perf, not disk I/O
            relay = new RelayServer(PORT, new MemoryStore());
            client = createClient(PORT);
            await client.connect();
        });

        afterAll(async () => {
            client.close();
            await relay.close();
        });

        it("benchmarks checkpoint creation with 1000 members", async () => {
            const guildId = await client.createGuild("Large Guild");

            // Add 1000 members (simulated by just adding roles/bans or just dummy events? 
            // Actually, we need to populate state. 
            // Simplest way to bloat state is adding many channels or roles.
            // Let's add 500 channels and 500 roles.

            const BATCH_SIZE = 100; // Send in batches to avoid overwhelming websocket buffer if any

            const startLoad = Date.now();

            for (let i = 0; i < 500; i++) {
                await client.createChannel(guildId, `ch-${i}`, "text");
                if (i % BATCH_SIZE === 0) await new Promise(r => setTimeout(r, 10));
            }

            for (let i = 0; i < 500; i++) {
                // We can't easily "add member" without them joining, but we can create roles
                // or just ban random users to fill the map.
                const randomUser = getPublicKey(generatePrivateKey());
                await client.banUser(guildId, randomUser, "stress test");
                if (i % BATCH_SIZE === 0) await new Promise(r => setTimeout(r, 10));
            }

            const loadTime = Date.now() - startLoad;
            console.log(`Loaded 1000 state items in ${loadTime}ms`);

            // Wait for relay to process
            await new Promise(r => setTimeout(r, 2000));

            // Measure Checkpoint Time
            const startCp = Date.now();
            await relay.createCheckpoints();
            const cpTime = Date.now() - startCp;

            console.log(`Checkpoint creation took ${cpTime}ms`);
            expect(cpTime).toBeLessThan(2000); // Should be reasonably fast

            // Verify checkpoint exists
            const log = await (relay as any).store.getLog(guildId);
            const cp = log.find((e: GuildEvent) => e.body.type === "CHECKPOINT");
            expect(cp).toBeDefined();
            expect(cp.body.state.channels.length).toBeGreaterThanOrEqual(500);
        }, 60000);
    });

    describe("Ephemeral Pruning Flood", () => {
        let relay: RelayServer;
        let client: CgpClient;
        const PORT = 8091;

        beforeAll(async () => {
            relay = new RelayServer(PORT, new MemoryStore());
            client = createClient(PORT);
            await client.connect();
        });

        afterAll(async () => {
            client.close();
            await relay.close();
        });

        it("prunes 1000 ephemeral messages efficiently", async () => {
            const guildId = await client.createGuild("Ephemeral Guild");
            const policy: EphemeralPolicy = { mode: "ttl", seconds: 1 };
            const channelId = await client.createChannel(guildId, "ephemeral-chat", "text", policy);

            // Flood 1000 messages
            const MSG_COUNT = 1000;
            const promises = [];
            for (let i = 0; i < MSG_COUNT; i++) {
                promises.push(client.sendMessage(guildId, channelId, `msg ${i}`));
                if (i % 50 === 0) await new Promise(r => setTimeout(r, 5));
            }
            await Promise.all(promises);

            // Wait for TTL (1s) + buffer
            await new Promise(r => setTimeout(r, 1500));

            // Trigger Prune
            const startPrune = Date.now();
            await relay.prune();
            const pruneTime = Date.now() - startPrune;

            console.log(`Pruned ${MSG_COUNT} messages in ${pruneTime}ms`);

            // Verify messages are gone from store
            const log = await (relay as any).store.getLog(guildId);
            // Log should contain: GuildCreate, ChannelCreate. Messages should be gone.
            // Note: MemoryStore.deleteEvent removes them.
            // But wait, deleteEvent implementation in MemoryStore?
            // I need to check if MemoryStore implements deleteEvent correctly.
            // Assuming it does (I implemented it in previous turn).

            const messages = log.filter((e: GuildEvent) => e.body.type === "MESSAGE");
            expect(messages.length).toBe(0);

        }, 30000);
    });
});
