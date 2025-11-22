import { describe, it, expect, afterEach, beforeEach } from "vitest";
import { RelayServer } from "@cgp/relay/src/server";
import { MemoryStore } from "@cgp/relay/src/store";
import { CgpClient } from "@cgp/client/src/client";
import { generatePrivateKey, getPublicKey } from "@cgp/core";
import fs from "fs";

const RELAY_PORT = 8087;
const RELAY_URL = `ws://localhost:${RELAY_PORT}`;
const DB_PATH = "./test-stress-db";

describe("Stress Testing", () => {
    let relay: RelayServer;
    let clients: CgpClient[] = [];

    beforeEach(async () => {
        // Use MemoryStore for stress testing to avoid disk I/O bottlenecks
        relay = new RelayServer(RELAY_PORT, new MemoryStore());
        await new Promise(resolve => setTimeout(resolve, 100)); // Wait for relay to start
    });

    afterEach(async () => {
        for (const client of clients) {
            client.close();
        }
        clients = [];
        await relay.close();
    });

    it("handles concurrent messages from multiple clients", async () => {
        const CLIENT_COUNT = 5;
        const MESSAGES_PER_CLIENT = 50;
        const TOTAL_MESSAGES = CLIENT_COUNT * MESSAGES_PER_CLIENT;

        // 1. Create Guild Owner
        const ownerPriv = generatePrivateKey();
        const ownerPub = getPublicKey(ownerPriv);
        const ownerClient = new CgpClient({
            relays: [RELAY_URL],
            keyPair: { pub: ownerPub, priv: ownerPriv }
        });
        clients.push(ownerClient);
        await ownerClient.connect();

        const guildId = await ownerClient.createGuild("Stress Guild");
        const channelId = await ownerClient.createChannel(guildId, "general", "text");

        // 2. Create Other Clients
        const otherClients: CgpClient[] = [];
        for (let i = 0; i < CLIENT_COUNT - 1; i++) {
            const priv = generatePrivateKey();
            const pub = getPublicKey(priv);
            const client = new CgpClient({
                relays: [RELAY_URL],
                keyPair: { pub, priv }
            });
            clients.push(client);
            otherClients.push(client);
            await client.connect();
            client.subscribe(guildId);
        }

        // 3. Spam Messages concurrently
        const startTime = Date.now();
        const promises: Promise<any>[] = [];

        // Owner sends
        for (let j = 0; j < MESSAGES_PER_CLIENT; j++) {
            promises.push(ownerClient.sendMessage(guildId, channelId, `Owner msg ${j}`));
        }

        // Others send
        for (const client of otherClients) {
            for (let j = 0; j < MESSAGES_PER_CLIENT; j++) {
                promises.push(client.sendMessage(guildId, channelId, `Client msg ${j}`));
            }
        }

        await Promise.all(promises);
        const endTime = Date.now();
        console.log(`Sent ${TOTAL_MESSAGES} messages in ${endTime - startTime}ms`);

        // 4. Wait for convergence
        // Wait until ownerClient receives all events
        const expectedSeq = 2 + TOTAL_MESSAGES - 1;
        const waitForSync = async (client: CgpClient, name: string) => {
            let retries = 0;
            while (retries < 300) {
                const state = client.getGuildState(guildId);
                if (state && state.headSeq >= expectedSeq) return;
                if (retries % 10 === 0) console.log(`[${name}] Waiting for sync. Current: ${state?.headSeq}, Expected: ${expectedSeq}`);
                await new Promise(r => setTimeout(r, 100));
                retries++;
            }
            throw new Error(`Client ${name} failed to sync. Current seq: ${client.getGuildState(guildId)?.headSeq}`);
        };

        await Promise.all(clients.map((c, i) => waitForSync(c, `Client ${i}`)));

        // 5. Verify State
        for (const client of clients) {
            const state = client.getGuildState(guildId);
            expect(state).toBeDefined();
            expect(state!.headSeq).toBe(expectedSeq);
        }

    }, 30000); // Increase timeout

    it("handles high volume of directory registrations", async () => {
        const { DirectoryService } = await import("@cgp/directory/src/index");
        const { hashObject, generatePrivateKey, getPublicKey, sign } = await import("@cgp/core");
        const fs = await import("fs");

        const DIR_DB_PATH = "./test-stress-dir-db";
        if (fs.existsSync(DIR_DB_PATH)) fs.rmSync(DIR_DB_PATH, { recursive: true, force: true });

        const service = new DirectoryService(DIR_DB_PATH);
        const REGISTRATION_COUNT = 100; // Can increase for heavier load

        const startTime = Date.now();
        const promises: Promise<void>[] = [];

        for (let i = 0; i < REGISTRATION_COUNT; i++) {
            const p = (async () => {
                const handle = `user_${i}`;
                const priv = generatePrivateKey();
                const pub = getPublicKey(priv);
                const guildId = hashObject({ name: handle });
                const timestamp = Date.now();
                const msg = `REGISTER:${handle}:${guildId}:${timestamp}`;
                const sig = await sign(priv, hashObject(msg));
                await service.register(handle, guildId, pub, sig, timestamp);
            })();
            promises.push(p);
        }

        await Promise.all(promises);
        const endTime = Date.now();
        console.log(`Registered ${REGISTRATION_COUNT} users in ${endTime - startTime}ms`);

        // Verify random lookup
        const randomIndex = Math.floor(Math.random() * REGISTRATION_COUNT);
        const entry = await service.getEntry(`user_${randomIndex}`);
        expect(entry).toBeDefined();

        await service.close();
        if (fs.existsSync(DIR_DB_PATH)) fs.rmSync(DIR_DB_PATH, { recursive: true, force: true });
    });

    it("benchmarks state reconstruction with large event history", async () => {
        const { createInitialState, applyEvent, computeEventId } = await import("@cgp/core");

        const EVENT_COUNT = 10000;
        const guildId = "bench-guild";
        const ownerId = "owner";
        const createdAt = Date.now();

        // 1. Generate History
        const events: any[] = [];

        // Genesis
        const genesis: any = {
            id: "",
            seq: 0,
            prevHash: null,
            createdAt,
            author: ownerId,
            body: { type: "GUILD_CREATE", guildId, name: "Bench Guild", ownerId },
            signature: ""
        };
        genesis.id = computeEventId(genesis);
        events.push(genesis);

        // Channel Create
        const channelEvent: any = {
            id: "",
            seq: 1,
            prevHash: events[0].id,
            createdAt,
            author: ownerId,
            body: { type: "CHANNEL_CREATE", guildId, channelId: "general", name: "general", kind: "text" },
            signature: ""
        };
        channelEvent.id = computeEventId(channelEvent);
        events.push(channelEvent);

        // Spam Messages
        for (let i = 0; i < EVENT_COUNT; i++) {
            const prev = events[events.length - 1];
            const evt: any = {
                id: "",
                seq: prev.seq + 1,
                prevHash: prev.id,
                createdAt,
                author: ownerId,
                body: { type: "MESSAGE", guildId, channelId: "general", content: `msg ${i}` },
                signature: ""
            };
            evt.id = computeEventId(evt);
            events.push(evt);
        }

        console.log(`Generated ${events.length} events`);

        // 2. Measure Rebuild Time
        const startTime = Date.now();
        let state = createInitialState(events[0]);
        for (let i = 1; i < events.length; i++) {
            state = applyEvent(state, events[i]);
        }
        const endTime = Date.now();

        const duration = endTime - startTime;
        console.log(`Rebuilt state from ${events.length} events in ${duration}ms`);
        console.log(`Average: ${duration / events.length}ms per event`);

        expect(state.headSeq).toBe(events.length - 1);
        expect(duration).toBeLessThan(5000); // Should be reasonably fast (e.g. < 0.5ms per event)
    });

    it("propagates gossip in a larger peer mesh", async () => {
        const PEER_COUNT = 10;
        const peers: CgpClient[] = [];
        const peerPorts: number[] = [];

        // 1. Create Peers
        for (let i = 0; i < PEER_COUNT; i++) {
            const priv = generatePrivateKey();
            const pub = getPublicKey(priv);
            const client = new CgpClient({
                relays: [], // No relays, pure P2P
                keyPair: { pub, priv }
            });
            const port = 9000 + i;
            await client.listen(port);
            peers.push(client);
            peerPorts.push(port);
        }

        // 2. Connect Peers in a Ring (or random mesh)
        // Ring topology: 0->1, 1->2, ..., N->0
        for (let i = 0; i < PEER_COUNT; i++) {
            const nextIndex = (i + 1) % PEER_COUNT;
            const nextPort = peerPorts[nextIndex];
            await peers[i].connectToPeer(`ws://localhost:${nextPort}`);
        }

        // 3. Publish Event from Peer 0
        const guildId = await peers[0].createGuild("Gossip Guild");
        const channelId = await peers[0].createChannel(guildId, "general", "text");

        // Wait for initial propagation
        await new Promise(r => setTimeout(r, 500));

        // Subscribe everyone
        for (const peer of peers) {
            if (peer !== peers[0]) peer.subscribe(guildId);
        }

        // 4. Send Message
        const msgContent = "Hello Gossip World";
        await peers[0].sendMessage(guildId, channelId, msgContent);

        // 5. Verify Propagation
        // Wait for gossip
        await new Promise(r => setTimeout(r, 5000));

        let receivedCount = 0;
        for (const peer of peers) {
            const state = peer.getGuildState(guildId);
            // We can't easily check message content in state, but we can check headSeq.
            // Guild Create (0) + Channel Create (1) + Message (2) -> headSeq should be 2
            if (state && state.headSeq === 2) {
                receivedCount++;
            }
        }

        console.log(`Gossip propagation: ${receivedCount}/${PEER_COUNT} peers received the message`);
        expect(receivedCount).toBe(PEER_COUNT);

        // Cleanup
        for (const peer of peers) {
            peer.close();
        }
    }, 30000);
});
