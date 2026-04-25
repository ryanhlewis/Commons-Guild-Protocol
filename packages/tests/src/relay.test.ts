import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { LocalRelayPubSubAdapter, RelayServer } from "@cgp/relay/src/server"; // Import directly from src to avoid build step in dev
import { MemoryStore } from "@cgp/relay/src/store";
import { LevelStore } from "@cgp/relay/src/store_level";
import { CgpClient } from "@cgp/client/src/client";
import { GuildEvent, computeEventId, hashObject, sign } from "@cgp/core";
import * as secp from "@noble/secp256k1";
import fs from "fs/promises";
import WebSocket from "ws";

class RecordingPubSubAdapter extends LocalRelayPubSubAdapter {
    public publishedTopics: string[] = [];

    publish(topic: string, envelope: any) {
        this.publishedTopics.push(topic);
        return super.publish(topic, envelope);
    }
}

function testEvent(seq: number, body: any, author = "test-author"): GuildEvent {
    const event = {
        id: "",
        seq,
        prevHash: seq > 0 ? `prev-${seq}` : null,
        createdAt: 1_700_000_000_000 + seq,
        author,
        body,
        signature: `sig-${seq}`
    } as GuildEvent;
    event.id = computeEventId(event);
    return event;
}

function seqKeyForTest(seq: number) {
    return seq.toString().padStart(10, "0");
}

describe("CGP relay basic messaging", () => {
    let relay: RelayServer;
    const PORT = 7448;
    const relayUrl = `ws://localhost:${PORT}`;
    const DB_PATH = "./test-relay-db-" + Date.now();

    beforeAll(async () => {
        relay = new RelayServer(PORT, DB_PATH);
    });

    afterAll(async () => {
        await relay.close();
        // Wait for DB to close
        await new Promise(r => setTimeout(r, 100));
        const fs = await import("fs/promises");
        try {
            await fs.rm(DB_PATH, { recursive: true, force: true });
        } catch (e) {
            console.error("Failed to cleanup test DB", e);
        }
    });

    it("serves channel history directly from LevelStore channel indexes with marker fallback", async () => {
        const dbPath = `./test-level-channel-index-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const store = new LevelStore(dbPath);
        const guildId = "guild-fast-history";
        const channelId = "general";
        try {
            await (store as any).db.open();
            await store.appendEvents(guildId, [
                testEvent(0, { type: "GUILD_CREATE", guildId, name: "Fast History" }),
                testEvent(1, { type: "CHANNEL_CREATE", guildId, channelId, name: "general", kind: "text" }),
                testEvent(2, { type: "MESSAGE", guildId, channelId, messageId: "m1", content: "one" }),
                testEvent(3, { type: "MESSAGE", guildId, channelId, messageId: "m2", content: "two" })
            ]);

            const fastHistory = await store.getHistory({ guildId, channelId, limit: 10 });
            expect(fastHistory.map((event) => event.body.type)).toEqual(["CHANNEL_CREATE", "MESSAGE", "MESSAGE"]);

            await (store as any).db.put(`guild:${guildId}:chan:${encodeURIComponent(channelId)}:seq:${seqKeyForTest(2)}`, "1");
            const fallbackHistory = await store.getHistory({ guildId, channelId, afterSeq: 1, limit: 10 });
            expect(fallbackHistory.map((event) => (event.body as any).messageId)).toEqual(["m1", "m2"]);
        } finally {
            await store.close();
            await fs.rm(dbPath, { recursive: true, force: true });
        }
    });

    it("sends a message between two peers", async () => {
        const privKeyA = secp.utils.randomPrivateKey();
        const pubKeyA = Buffer.from(secp.getPublicKey(privKeyA, true)).toString("hex");

        const privKeyB = secp.utils.randomPrivateKey();
        const pubKeyB = Buffer.from(secp.getPublicKey(privKeyB, true)).toString("hex");

        const alice = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKeyA, priv: privKeyA } });
        const bob = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKeyB, priv: privKeyB } });

        await Promise.all([alice.connect(), bob.connect()]);

        // Alice creates a guild
        const guildId = await alice.createGuild("Test Guild");

        // Bob subscribes
        bob.subscribe(guildId);
        await new Promise((res) => setTimeout(res, 100));

        const messages: any[] = [];
        bob.on("event", (ev) => {
            if (ev.body.type === "MESSAGE") {
                messages.push(ev.body.content);
            }
        });

        // Alice creates a channel (REQUIRED for validation)
        await alice.publish({
            type: "CHANNEL_CREATE",
            guildId,
            channelId: "general",
            name: "general",
            kind: "text"
        });

        await new Promise((res) => setTimeout(res, 200)); // Wait for channel to be processed

        // Alice sends a message
        await alice.publish({
            type: "MESSAGE",
            guildId,
            channelId: "general", // matches created channel
            content: "hello from alice"
        });

        // wait for async propagation
        await new Promise((res) => setTimeout(res, 1000));

        expect(messages).toContain("hello from alice");
    });

    it("supports high-level API (channels, roles, bans)", async () => {
        const privKeyA = secp.utils.randomPrivateKey();
        const pubKeyA = Buffer.from(secp.getPublicKey(privKeyA, true)).toString("hex");
        const alice = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKeyA, priv: privKeyA } });
        await alice.connect();

        const guildId = await alice.createGuild("API Test Guild");

        // Create channel
        const channelId = await alice.createChannel(guildId, "general", "text");
        expect(channelId).toBeDefined();

        // Send message
        const msgId = await alice.sendMessage(guildId, channelId, "Hello API");
        expect(msgId).toBeDefined();

        // Assign role
        await alice.assignRole(guildId, "some_user", "admin");

        // Ban user
        await alice.banUser(guildId, "bad_user", "spam");

        // Wait for propagation (local relay is fast but async)
        await new Promise((res) => setTimeout(res, 200));
    });

    it("rejects forged live events before dedupe state is poisoned", async () => {
        const privKey = secp.utils.randomPrivateKey();
        const pubKey = Buffer.from(secp.getPublicKey(privKey, true)).toString("hex");
        const client = new CgpClient({ relays: [], keyPair: { pub: pubKey, priv: privKey } });
        const createdAt = Date.now();
        const guildId = hashObject({ name: "forged-client-event", createdAt });
        const body = {
            type: "GUILD_CREATE",
            guildId,
            name: "Forged Client Event"
        } as any;
        const signature = await sign(privKey, hashObject({ body, author: pubKey, createdAt }));
        const event: GuildEvent = {
            id: "",
            seq: 0,
            prevHash: null,
            createdAt,
            author: pubKey,
            body,
            signature
        };
        event.id = computeEventId(event);

        await (client as any).handleMessage(JSON.stringify(["EVENT", { ...event, id: hashObject("forged-id") }]));
        await (client as any).messageQueue;
        expect(client.getGuildState(guildId)).toBeUndefined();

        await (client as any).handleMessage(JSON.stringify(["EVENT", event]));
        await (client as any).messageQueue;
        expect(client.getGuildState(guildId)?.guildId).toBe(guildId);
    });

    it("uses the first configured live relay as the deterministic writer", () => {
        const client = new CgpClient({ relays: ["ws://primary", "ws://secondary"] });
        const sentFrames: string[] = [];
        const primary = {
            readyState: WebSocket.OPEN,
            send: (frame: string) => sentFrames.push(`primary:${frame}`)
        };
        const secondary = {
            readyState: WebSocket.OPEN,
            send: (frame: string) => sentFrames.push(`secondary:${frame}`)
        };

        (client as any).sockets = [secondary, primary];
        (client as any).socketUrls = new Map([
            [secondary, "ws://secondary"],
            [primary, "ws://primary"]
        ]);

        expect((client as any).sendRawFrameToWriterSocket("test-frame")).toBe(true);
        expect(sentFrames).toEqual(["primary:test-frame"]);
    });

    it("serves paginated channel history snapshots", async () => {
        const privKey = secp.utils.randomPrivateKey();
        const pubKey = Buffer.from(secp.getPublicKey(privKey, true)).toString("hex");
        const alice = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKey, priv: privKey } });
        await alice.connect();

        const guildId = await alice.createGuild("History Guild");
        const channelId = await alice.createChannel(guildId, "history", "text");
        await new Promise((res) => setTimeout(res, 200));

        for (let i = 0; i < 5; i++) {
            await alice.sendMessage(guildId, channelId, `history-${i}`);
        }
        await new Promise((res) => setTimeout(res, 300));

        const ws = new WebSocket(relayUrl);
        await new Promise<void>((resolve) => ws.onopen = () => resolve());

        const waitForSnapshot = (subId: string) => new Promise<any>((resolve) => {
            const handler = (raw: WebSocket.RawData) => {
                const data = JSON.parse(raw.toString());
                if (data[0] === "SNAPSHOT" && data[1]?.subId === subId) {
                    ws.off("message", handler);
                    resolve(data[1]);
                }
            };
            ws.on("message", handler);
        });

        const firstPage = waitForSnapshot("history-page-1");
        ws.send(JSON.stringify(["GET_HISTORY", { subId: "history-page-1", guildId, channelId, limit: 2 }]));
        const first = await firstPage;
        expect(first.events.map((event: any) => event.body.content)).toEqual(["history-3", "history-4"]);
        expect(first.hasMore).toBe(true);

        const secondPage = waitForSnapshot("history-page-2");
        ws.send(JSON.stringify(["GET_HISTORY", { subId: "history-page-2", guildId, channelId, beforeSeq: first.oldestSeq, limit: 2 }]));
        const second = await secondPage;
        expect(second.events.map((event: any) => event.body.content)).toEqual(["history-1", "history-2"]);
        expect(second.hasMore).toBe(true);

        ws.close();
        alice.close();
    });

    it("fans out live events across relay instances through a pubsub adapter", async () => {
        const pubSub = new LocalRelayPubSubAdapter();
        const storeA = new MemoryStore();
        const storeB = new MemoryStore();
        const relayA = new RelayServer(PORT + 4, storeA, [], {
            instanceId: "relay-pubsub-a",
            pubSubAdapter: pubSub
        });
        const relayB = new RelayServer(PORT + 5, storeB, [], {
            instanceId: "relay-pubsub-b",
            pubSubAdapter: pubSub,
            wireFormat: "binary-json"
        });

        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({
            relays: [`ws://localhost:${PORT + 4}`],
            keyPair: { pub: ownerPubKey, priv: ownerPrivKey }
        });
        const observer = new CgpClient({
            relays: [`ws://localhost:${PORT + 5}`],
            keyPair: { pub: ownerPubKey, priv: ownerPrivKey }
        });

        try {
            await Promise.all([owner.connect(), observer.connect()]);
            const guildId = await owner.createGuild("PubSub Guild");
            const channelId = await owner.createChannel(guildId, "live", "text");
            await new Promise((res) => setTimeout(res, 200));

            const received: string[] = [];
            observer.on("event", (event) => {
                if (event.body.type === "MESSAGE") {
                    received.push((event.body as any).content);
                }
            });
            observer.subscribe(guildId);
            await new Promise((res) => setTimeout(res, 200));
            const replayStarted = Date.now();
            while (storeB.getLog(guildId).length < 2 && Date.now() - replayStarted < 3000) {
                await new Promise((res) => setTimeout(res, 50));
            }

            await owner.sendMessage(guildId, channelId, "pubsub live delivery");
            await new Promise((res) => setTimeout(res, 1000));
            expect(received.filter((content) => content === "pubsub live delivery")).toEqual(["pubsub live delivery"]);
        } finally {
            owner.close();
            observer.close();
            await relayA.close();
            await relayB.close();
            pubSub.close();
        }
    });

    it("routes pubsub fanout through channel topics for channel-specific subscribers", async () => {
        const pubSub = new RecordingPubSubAdapter();
        const storeA = new MemoryStore();
        const storeB = new MemoryStore();
        const relayA = new RelayServer(PORT + 70, storeA, [], {
            instanceId: "relay-channel-pubsub-a",
            pubSubAdapter: pubSub,
            pubSubLiveTopics: true
        });
        const relayB = new RelayServer(PORT + 71, storeB, [], {
            instanceId: "relay-channel-pubsub-b",
            pubSubAdapter: pubSub,
            pubSubLiveTopics: true
        });

        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({
            relays: [`ws://localhost:${PORT + 70}`],
            keyPair: { pub: ownerPubKey, priv: ownerPrivKey }
        });
        let observer: WebSocket | undefined;

        try {
            await owner.connect();
            const guildId = await owner.createGuild("Channel PubSub Guild");
            const channelOne = await owner.createChannel(guildId, "one", "text");
            const channelTwo = await owner.createChannel(guildId, "two", "text");
            await new Promise((res) => setTimeout(res, 200));

            observer = new WebSocket(`ws://localhost:${PORT + 71}`);
            await new Promise<void>((resolve, reject) => {
                observer!.once("open", () => resolve());
                observer!.once("error", reject);
            });

            const received: string[] = [];
            observer.on("message", (raw) => {
                const [kind, payload] = JSON.parse(raw.toString());
                const events = kind === "EVENT" ? [payload] : kind === "BATCH" && Array.isArray(payload) ? payload : [];
                for (const event of events) {
                    if (event?.body?.type === "MESSAGE") {
                        received.push(event.body.content);
                    }
                }
            });
            observer.send(JSON.stringify(["SUB", {
                subId: "channel-two-only",
                guildId,
                channels: [channelTwo]
            }]));
            await new Promise((res) => setTimeout(res, 200));
            const replayStarted = Date.now();
            while (storeB.getLog(guildId).length < 3 && Date.now() - replayStarted < 3000) {
                await new Promise((res) => setTimeout(res, 50));
            }

            pubSub.publishedTopics = [];
            await owner.sendMessage(guildId, channelOne, "channel one should stay local");
            await owner.sendMessage(guildId, channelTwo, "channel two should fan out");
            await new Promise((res) => setTimeout(res, 500));

            expect(received).not.toContain("channel one should stay local");
            expect(received).toContain("channel two should fan out");
            expect(pubSub.publishedTopics).not.toContain(`guild:${guildId}`);
            expect(pubSub.publishedTopics).toContain(`guild:${guildId}:log`);
            expect(pubSub.publishedTopics).toContain(`guild:${guildId}:channels`);
            expect(pubSub.publishedTopics).toContain(`guild:${guildId}:channel:${encodeURIComponent(channelOne)}`);
            expect(pubSub.publishedTopics).toContain(`guild:${guildId}:channel:${encodeURIComponent(channelTwo)}`);
        } finally {
            observer?.close();
            owner.close();
            await relayA.close();
            await relayB.close();
            pubSub.close();
        }
    });

    it("replicates canonical log events into independent relay stores", async () => {
        const pubSub = new LocalRelayPubSubAdapter();
        const storeA = new MemoryStore();
        const storeB = new MemoryStore();
        const relayA = new RelayServer(PORT + 80, storeA, [], {
            instanceId: "relay-durable-a",
            pubSubAdapter: pubSub
        });
        const relayB = new RelayServer(PORT + 81, storeB, [], {
            instanceId: "relay-durable-b",
            pubSubAdapter: pubSub
        });

        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const bootstrap = new CgpClient({
            relays: [`ws://localhost:${PORT + 80}`, `ws://localhost:${PORT + 81}`],
            keyPair: { pub: ownerPubKey, priv: ownerPrivKey }
        });
        const publisher = new CgpClient({
            relays: [`ws://localhost:${PORT + 80}`],
            keyPair: { pub: ownerPubKey, priv: ownerPrivKey }
        });

        try {
            await bootstrap.connect();
            const guildId = await bootstrap.createGuild("Durable PubSub Guild");
            const channelId = await bootstrap.createChannel(guildId, "replicated", "text");
            await new Promise((res) => setTimeout(res, 300));

            await publisher.connect();
            await publisher.publishReliable({
                type: "MESSAGE",
                guildId,
                channelId,
                messageId: `replicated-${Date.now()}`,
                content: "durably replicated"
            }, { timeoutMs: 5000 });

            await new Promise((res) => setTimeout(res, 500));
            const replicatedHistory = await storeB.getHistory({ guildId, channelId, limit: 10 });
            expect(replicatedHistory.some((event) =>
                event.body.type === "MESSAGE" && (event.body as any).content === "durably replicated"
            )).toBe(true);
        } finally {
            bootstrap.close();
            publisher.close();
            await relayA.close();
            await relayB.close();
            pubSub.close();
        }
    });

    it("delivers replicated pubsub messages exactly once to channel subscribers", async () => {
        const pubSub = new LocalRelayPubSubAdapter();
        const storeA = new MemoryStore();
        const storeB = new MemoryStore();
        const relayA = new RelayServer(PORT + 82, storeA, [], {
            instanceId: "relay-single-delivery-a",
            pubSubAdapter: pubSub,
            wireFormat: "binary-v1"
        });
        const relayB = new RelayServer(PORT + 83, storeB, [], {
            instanceId: "relay-single-delivery-b",
            pubSubAdapter: pubSub,
            wireFormat: "binary-v1"
        });

        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({
            relays: [`ws://localhost:${PORT + 82}`],
            keyPair: { pub: ownerPubKey, priv: ownerPrivKey }
        });
        const observer = new CgpClient({
            relays: [`ws://localhost:${PORT + 83}`],
            keyPair: { pub: ownerPubKey, priv: ownerPrivKey },
            wireFormat: "binary-v1"
        });

        try {
            await Promise.all([owner.connect(), observer.connect()]);
            const guildId = await owner.createGuild("Single Delivery Guild");
            const channelId = await owner.createChannel(guildId, "live", "text");
            await new Promise((res) => setTimeout(res, 250));

            const received: string[] = [];
            observer.on("event", (event) => {
                if (event.body.type === "MESSAGE") {
                    received.push((event.body as any).content);
                }
            });
            observer.subscribe(guildId);
            await new Promise((res) => setTimeout(res, 250));
            const replayStarted = Date.now();
            while (storeB.getLog(guildId).length < 2 && Date.now() - replayStarted < 3000) {
                await new Promise((res) => setTimeout(res, 50));
            }

            await owner.sendMessage(guildId, channelId, "single delivery only");
            await new Promise((res) => setTimeout(res, 1000));

            expect(received.filter((content) => content === "single delivery only")).toEqual(["single delivery only"]);
        } finally {
            owner.close();
            observer.close();
            await relayA.close();
            await relayB.close();
            pubSub.close();
        }
    });

    it("catches up relay history over binary peer control reads", async () => {
        const relayAPrivKey = Buffer.from(secp.utils.randomPrivateKey()).toString("hex");
        const relayASeedPub = Buffer.from(secp.getPublicKey(relayAPrivKey, true)).toString("hex");
        const relayBPrivKey = Buffer.from(secp.utils.randomPrivateKey()).toString("hex");
        const relayBPubKey = Buffer.from(secp.getPublicKey(relayBPrivKey, true)).toString("hex");
        const storeA = new MemoryStore();
        const storeB = new MemoryStore();
        const relayA = new RelayServer(PORT + 84, storeA, [], {
            instanceId: "relay-peer-source-a",
            relayPrivateKeyHex: relayAPrivKey,
            trustedPeerReadPublicKeys: [relayBPubKey],
            wireFormat: "binary-v1",
            enableDefaultPlugins: false
        });
        const relayB = new RelayServer(PORT + 85, storeB, [], {
            instanceId: "relay-peer-follower-b",
            relayPrivateKeyHex: relayBPrivKey,
            trustedPeerReadPublicKeys: [relayASeedPub],
            peerUrls: [`ws://localhost:${PORT + 84}`],
            peerCatchupIntervalMs: 0,
            wireFormat: "binary-v1",
            enableDefaultPlugins: false
        });

        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({
            relays: [`ws://localhost:${PORT + 84}`],
            keyPair: { pub: ownerPubKey, priv: ownerPrivKey },
            wireFormat: "binary-v1"
        });

        try {
            await owner.connect();
            const guildId = await owner.createGuild("Binary Peer Catchup Guild");
            const channelId = await owner.createChannel(guildId, "replicated", "text");
            await owner.sendMessage(guildId, channelId, "binary peer catchup");
            await new Promise((res) => setTimeout(res, 300));

            await (relayB as any).requestPeerLogCatchup(guildId, 2);

            let replicated = false;
            const started = Date.now();
            while (Date.now() - started < 5000) {
                const history = await storeB.getHistory({ guildId, channelId, limit: 10 });
                replicated = history.some((event) =>
                    event.body.type === "MESSAGE" && (event.body as any).content === "binary peer catchup"
                );
                if (replicated) {
                    break;
                }
                await new Promise((res) => setTimeout(res, 100));
            }

            expect(replicated).toBe(true);
        } finally {
            owner.close();
            await relayA.close();
            await relayB.close();
        }
    });

    it("official client wraps history requests and reliable publish acknowledgements", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const attackerPrivKey = secp.utils.randomPrivateKey();
        const attackerPubKey = Buffer.from(secp.getPublicKey(attackerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        const attacker = new CgpClient({ relays: [relayUrl], keyPair: { pub: attackerPubKey, priv: attackerPrivKey } });
        await Promise.all([owner.connect(), attacker.connect()]);

        const guildId = await owner.createGuild("Client History Guild");
        const channelId = await owner.createChannel(guildId, "client-history", "text");
        await new Promise((res) => setTimeout(res, 200));

        const ack = await owner.publishReliable({
            type: "MESSAGE",
            guildId,
            channelId,
            messageId: `client-ack-${Date.now()}`,
            content: "reliable publish body"
        });
        expect(ack.guildId).toBe(guildId);
        expect(ack.eventId).toBeDefined();
        expect(ack.seq).toBeGreaterThanOrEqual(2);

        await owner.sendMessage(guildId, channelId, "history helper tail");
        const history = await owner.getHistory({ guildId, channelId, limit: 2 });
        expect(history.guildId).toBe(guildId);
        expect(history.channelId).toBe(channelId);
        expect(history.events.map((event) => event.body.type)).toEqual(["MESSAGE", "MESSAGE"]);
        expect(history.events.some((event: any) => event.body.content === "reliable publish body")).toBe(true);
        expect(owner.getGuildState(guildId)?.channels.has(channelId)).toBe(true);

        await expect(attacker.publishReliable({
            type: "CHANNEL_CREATE",
            guildId,
            channelId: `unauthorized-${Date.now()}`,
            name: "unauthorized",
            kind: "text"
        }, { timeoutMs: 5000 })).rejects.toThrow(/permission/i);

        owner.close();
        attacker.close();
    });

    it("serves permission-filtered durable search results", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        await owner.connect();

        const guildId = await owner.createGuild("Search Guild");
        const channelId = await owner.createChannel(guildId, "knowledge-base", "text");
        await new Promise((res) => setTimeout(res, 200));

        const liveMessageId = `search-live-${Date.now()}`;
        await owner.publishReliable({
            type: "MESSAGE",
            guildId,
            channelId,
            messageId: liveMessageId,
            content: "alpha moderation workflow"
        });
        await owner.publishReliable({
            type: "EDIT_MESSAGE",
            guildId,
            channelId,
            messageId: liveMessageId,
            newContent: "alpha operator audit workflow"
        });

        const deletedMessageId = `search-deleted-${Date.now()}`;
        await owner.publishReliable({
            type: "MESSAGE",
            guildId,
            channelId,
            messageId: deletedMessageId,
            content: "delete-visible-token"
        });
        await owner.publishReliable({
            type: "DELETE_MESSAGE",
            guildId,
            channelId,
            messageId: deletedMessageId,
            reason: "search test"
        });

        const encryptedMessageId = `search-encrypted-${Date.now()}`;
        await owner.publishReliable({
            type: "MESSAGE",
            guildId,
            channelId,
            messageId: encryptedMessageId,
            content: "super-secret-token",
            encrypted: true
        });

        await owner.upsertAppObject(guildId, "org.cgp.chat", "message-pin", `pin-${liveMessageId}`, {
            channelId,
            target: { channelId, messageId: liveMessageId },
            value: { label: "Launch checklist pin" }
        });

        const editedSearch = await owner.search({ guildId, query: "operator audit", scopes: ["messages"], limit: 10 });
        expect(editedSearch.results.map((result) => result.messageId)).toContain(liveMessageId);
        expect(editedSearch.results[0].preview).toContain("operator audit");

        const staleContentSearch = await owner.search({ guildId, query: "moderation workflow", scopes: ["messages"], limit: 10 });
        expect(staleContentSearch.results.map((result) => result.messageId)).not.toContain(liveMessageId);

        const deletedHidden = await owner.search({ guildId, query: "delete-visible-token", scopes: ["messages"], limit: 10 });
        expect(deletedHidden.results).toHaveLength(0);

        const deletedIncluded = await owner.search({ guildId, query: "delete-visible-token", scopes: ["messages"], includeDeleted: true, limit: 10 });
        expect(deletedIncluded.results.map((result) => result.messageId)).toContain(deletedMessageId);

        const encryptedSearch = await owner.search({ guildId, query: "super-secret-token", scopes: ["messages"], limit: 10 });
        expect(encryptedSearch.results).toHaveLength(0);

        const channelSearch = await owner.search({ guildId, query: "knowledge", scopes: ["channels"], limit: 10 });
        expect(channelSearch.results.map((result) => result.channelId)).toContain(channelId);

        const appObjectSearch = await owner.search({ guildId, query: "Launch checklist", scopes: ["appObjects"], limit: 10 });
        expect(appObjectSearch.results.map((result) => result.objectId)).toContain(`pin-${liveMessageId}`);

        const memberSearch = await owner.search({ guildId, query: ownerPubKey.slice(0, 16), scopes: ["members"], limit: 10 });
        expect(memberSearch.results.map((result) => result.userId)).toContain(ownerPubKey);

        owner.close();
    });

    it("accepts high-throughput batch publishes with one batch acknowledgement", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        await owner.connect();

        const guildId = await owner.createGuild("Batch Publish Guild");
        const channelId = await owner.createChannel(guildId, "batch", "text");
        await new Promise((res) => setTimeout(res, 200));

        const acks = await owner.publishBatchReliable([
            { type: "MESSAGE", guildId, channelId, messageId: `batch-1-${Date.now()}`, content: "first batch message" },
            { type: "MESSAGE", guildId, channelId, messageId: `batch-2-${Date.now()}`, content: "second batch message" },
            { type: "MESSAGE", guildId, channelId, messageId: `batch-3-${Date.now()}`, content: "third batch message" }
        ]);

        expect(acks).toHaveLength(3);
        expect(acks.map((ack) => ack.seq)).toEqual([...acks.map((ack) => ack.seq)].sort((a, b) => a - b));
        owner.close();
    });

    it("fans out batch publishes as one live batch frame", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        const observer = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        await Promise.all([owner.connect(), observer.connect()]);

        const guildId = await owner.createGuild("Batch Fanout Guild");
        const channelId = await owner.createChannel(guildId, "batch-fanout", "text");
        await new Promise((res) => setTimeout(res, 200));

        let batchFrameCount = 0;
        const received: string[] = [];
        observer.on("frame", (frame: any) => {
            if (frame?.kind === "BATCH") {
                batchFrameCount++;
            }
        });
        observer.on("event", (event: GuildEvent) => {
            if (event.body.type === "MESSAGE") {
                received.push((event.body as any).content);
            }
        });
        observer.subscribe(guildId);
        await new Promise((res) => setTimeout(res, 200));

        await owner.publishBatchReliable([
            { type: "MESSAGE", guildId, channelId, messageId: `fanout-batch-1-${Date.now()}`, content: "fanout one" },
            { type: "MESSAGE", guildId, channelId, messageId: `fanout-batch-2-${Date.now()}`, content: "fanout two" },
            { type: "MESSAGE", guildId, channelId, messageId: `fanout-batch-3-${Date.now()}`, content: "fanout three" }
        ]);

        const start = Date.now();
        while (received.length < 3 && Date.now() - start < 3000) {
            await new Promise((res) => setTimeout(res, 50));
        }

        expect(batchFrameCount).toBeGreaterThanOrEqual(1);
        expect(received).toEqual(expect.arrayContaining(["fanout one", "fanout two", "fanout three"]));
        owner.close();
        observer.close();
    });

    it("fans out mixed guild-wide and channel batches to channel-scoped subscribers", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        let observer: WebSocket | undefined;

        try {
            await owner.connect();
            const guildId = await owner.createGuild("Mixed Batch Fanout Guild");
            const channelId = await owner.createChannel(guildId, "mixed-batch", "text");
            await new Promise((res) => setTimeout(res, 200));

            observer = new WebSocket(relayUrl);
            await new Promise<void>((resolve, reject) => {
                observer!.once("open", () => resolve());
                observer!.once("error", reject);
            });

            const receivedTypes: string[] = [];
            const receivedMessages: string[] = [];
            observer.on("message", (raw) => {
                const [kind, payload] = JSON.parse(raw.toString());
                const events = kind === "EVENT" ? [payload] : kind === "BATCH" && Array.isArray(payload) ? payload : [];
                for (const event of events) {
                    if (!event?.body?.type) continue;
                    receivedTypes.push(event.body.type);
                    if (event.body.type === "MESSAGE") {
                        receivedMessages.push(event.body.content);
                    }
                }
            });

            observer.send(JSON.stringify(["SUB", {
                subId: "mixed-batch-channel-only",
                guildId,
                channels: [channelId]
            }]));
            await new Promise((res) => setTimeout(res, 200));
            receivedTypes.length = 0;
            receivedMessages.length = 0;

            await owner.publishBatchReliable([
                {
                    type: "ROLE_UPSERT",
                    guildId,
                    roleId: `mixed-role-${Date.now()}`,
                    name: "Mixed Role",
                    permissions: []
                },
                {
                    type: "MESSAGE",
                    guildId,
                    channelId,
                    messageId: `mixed-message-${Date.now()}`,
                    content: "mixed channel message"
                }
            ]);

            const start = Date.now();
            while ((receivedTypes.length < 2 || !receivedMessages.includes("mixed channel message")) && Date.now() - start < 3000) {
                await new Promise((res) => setTimeout(res, 50));
            }

            expect(receivedTypes).toContain("ROLE_UPSERT");
            expect(receivedMessages).toContain("mixed channel message");
        } finally {
            observer?.close();
            owner.close();
        }
    });

    it("supports binary-json wire frames for client publish and relay responses", async () => {
        const binaryPort = PORT + 60;
        const binaryRelay = new RelayServer(binaryPort, new MemoryStore(), [], { wireFormat: "binary-json" });
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({
            relays: [`ws://localhost:${binaryPort}`],
            keyPair: { pub: ownerPubKey, priv: ownerPrivKey },
            wireFormat: "binary-json"
        });

        try {
            await owner.connect();
            const guildId = await owner.createGuild("Binary Wire Guild");
            const channelId = await owner.createChannel(guildId, "binary", "text");
            const ack = await owner.publishReliable({
                type: "MESSAGE",
                guildId,
                channelId,
                messageId: `binary-${Date.now()}`,
                content: "binary wire message"
            });
            expect(ack.guildId).toBe(guildId);

            const history = await owner.getHistory({ guildId, channelId, limit: 1 });
            expect(history.events.map((event: any) => event.body.content)).toContain("binary wire message");
        } finally {
            owner.close();
            await binaryRelay.close();
        }
    });

    it("negotiates relay wire format per socket", async () => {
        const port = PORT + 61;
        const relay = new RelayServer(port, new MemoryStore(), [], { wireFormat: "json" });
        const relayUrl = `ws://localhost:${port}`;
        const binarySocket = new WebSocket(relayUrl);
        const jsonSocket = new WebSocket(relayUrl);

        try {
            await Promise.all([
                new Promise<void>((resolve, reject) => {
                    binarySocket.once("open", () => resolve());
                    binarySocket.once("error", reject);
                }),
                new Promise<void>((resolve, reject) => {
                    jsonSocket.once("open", () => resolve());
                    jsonSocket.once("error", reject);
                })
            ]);

            const binaryHello = new Promise<boolean>((resolve, reject) => {
                const timer = setTimeout(() => reject(new Error("Timed out waiting for binary HELLO_OK")), 5000);
                binarySocket.on("message", (data, isBinary) => {
                    const [kind] = JSON.parse(data.toString());
                    if (kind !== "HELLO_OK") return;
                    clearTimeout(timer);
                    resolve(Boolean(isBinary));
                });
            });
            const jsonHello = new Promise<boolean>((resolve, reject) => {
                const timer = setTimeout(() => reject(new Error("Timed out waiting for json HELLO_OK")), 5000);
                jsonSocket.on("message", (data, isBinary) => {
                    const [kind] = JSON.parse(data.toString());
                    if (kind !== "HELLO_OK") return;
                    clearTimeout(timer);
                    resolve(Boolean(isBinary));
                });
            });

            binarySocket.send(Buffer.from(JSON.stringify(["HELLO", { protocol: "cgp/0.1", wireFormat: "binary-json" }]), "utf8"));
            jsonSocket.send(JSON.stringify(["HELLO", { protocol: "cgp/0.1", wireFormat: "json" }]));

            expect(await binaryHello).toBe(true);
            expect(await jsonHello).toBe(false);
        } finally {
            binarySocket.close();
            jsonSocket.close();
            await relay.close();
        }
    });

    it("applies relay rate limits to publish batches", async () => {
        const limitedPort = PORT + 50;
        const limitedRelay = new RelayServer(limitedPort, new MemoryStore(), [], {
            rateLimitPolicy: {
                rateWindowMs: 50,
                socketPublishesPerWindow: 2,
                authorPublishesPerWindow: 2,
                guildPublishesPerWindow: 2
            }
        });
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [`ws://localhost:${limitedPort}`], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });

        try {
            await owner.connect();
            const guildId = await owner.createGuild("Batch Limited Guild");
            const channelId = await owner.createChannel(guildId, "batch-limited", "text");
            await new Promise((resolve) => setTimeout(resolve, 80));

            await expect(owner.publishBatchReliable([
                { type: "MESSAGE", guildId, channelId, messageId: `limited-batch-1-${Date.now()}`, content: "limited one" },
                { type: "MESSAGE", guildId, channelId, messageId: `limited-batch-2-${Date.now()}`, content: "limited two" },
                { type: "MESSAGE", guildId, channelId, messageId: `limited-batch-3-${Date.now()}`, content: "limited three" }
            ], { timeoutMs: 1000 })).rejects.toThrow(/rate/i);
        } finally {
            owner.close();
            await limitedRelay.close();
        }
    });

    it("keeps MemoryStore member search indexed across member updates and removals", () => {
        const store = new MemoryStore();
        const guildId = "memory-member-search-guild";
        const ownerId = "owner-fast-index-user";
        const memberId = "indexed-fast-search-user";
        const event = (seq: number, body: any, author = ownerId): GuildEvent => ({
            id: `memory-search-${seq}`,
            guildId,
            seq,
            prevHash: seq > 0 ? `memory-search-${seq - 1}` : null,
            author,
            signature: "test-signature",
            createdAt: seq,
            body
        });

        store.append(guildId, event(0, { type: "GUILD_CREATE", guildId, name: "Memory Search" }));
        store.append(guildId, event(1, { type: "ROLE_ASSIGN", guildId, userId: memberId, roleId: "bluecrew" }));
        store.append(guildId, event(2, {
            type: "MEMBER_UPDATE",
            guildId,
            userId: memberId,
            nickname: "Scale Operator",
            bio: "Shard maintainer"
        }));

        expect(store.searchMembers({ guildId, query: "scale", limit: 10 }).members.map((member) => member.userId))
            .toContain(memberId);
        expect(store.searchMembers({ guildId, query: "blue", limit: 10 }).members.map((member) => member.userId))
            .toContain(memberId);

        store.append(guildId, event(3, { type: "ROLE_REVOKE", guildId, userId: memberId, roleId: "bluecrew" }));
        expect(store.searchMembers({ guildId, query: "bluecrew", limit: 10 }).members.map((member) => member.userId))
            .not.toContain(memberId);

        store.append(guildId, event(4, { type: "BAN_ADD", guildId, userId: memberId, reason: "test" }));
        expect(store.searchMembers({ guildId, query: "scale", limit: 10 }).members.map((member) => member.userId))
            .not.toContain(memberId);
    });

    it("serves canonical replayed guild state snapshots", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const memberPrivKey = secp.utils.randomPrivateKey();
        const memberPubKey = Buffer.from(secp.getPublicKey(memberPrivKey, true)).toString("hex");
        const bannedPrivKey = secp.utils.randomPrivateKey();
        const bannedPubKey = Buffer.from(secp.getPublicKey(bannedPrivKey, true)).toString("hex");
        const alice = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        await alice.connect();

        const guildId = await alice.createGuild("State Guild");
        const channelId = await alice.createChannel(guildId, "state-room", "text");
        const messageId = await alice.sendMessage(guildId, channelId, "state-backed app object target");
        await alice.publish({
            type: "REACTION_ADD",
            guildId,
            channelId,
            messageId,
            reaction: "+1"
        });
        await alice.publish({
            type: "APP_OBJECT_UPSERT",
            guildId,
            channelId,
            namespace: "org.example.chat",
            objectType: "message-pin",
            objectId: `message-pin:${channelId}:${messageId}`,
            target: { channelId, messageId },
            value: { pinned: true }
        });
        await alice.assignRole(guildId, memberPubKey, "member");
        await alice.banUser(guildId, bannedPubKey, "spam");
        await new Promise((res) => setTimeout(res, 300));

        const ws = new WebSocket(relayUrl);
        await new Promise<void>((resolve) => ws.onopen = () => resolve());

        const statePromise = new Promise<any>((resolve) => {
            const handler = (raw: WebSocket.RawData) => {
                const data = JSON.parse(raw.toString());
                if (data[0] === "STATE" && data[1]?.subId === "state-snapshot") {
                    ws.off("message", handler);
                    resolve(data[1]);
                }
            };
            ws.on("message", handler);
        });

        ws.send(JSON.stringify(["GET_STATE", { subId: "state-snapshot", guildId }]));
        const state = await statePromise;

        expect(state.guildId).toBe(guildId);
        expect(typeof state.rootHash).toBe("string");
        expect(state.endSeq).toBeGreaterThanOrEqual(5);
        expect(state.state.name).toBe("State Guild");
        expect(state.state.channels.some(([id]: [string, any]) => id === channelId)).toBe(true);
        expect(state.state.messages.some(([id]: [string, any]) => id === messageId)).toBe(true);
        expect(state.state.messages.some(([id, message]: [string, any]) =>
            id === messageId &&
            Array.isArray(message.reactions?.["+1"]) &&
            message.reactions["+1"].includes(ownerPubKey)
        )).toBe(true);
        expect(state.state.appObjects.some(([, object]: [string, any]) =>
            object.namespace === "org.example.chat" &&
            object.objectType === "message-pin" &&
            object.target?.messageId === messageId &&
            object.value?.pinned === true
        )).toBe(true);
        expect(state.state.members.some(([id]: [string, any]) => id === ownerPubKey)).toBe(true);
        expect(state.state.members.some(([id]: [string, any]) => id === memberPubKey)).toBe(true);
        expect(state.state.bans.some(([id]: [string, any]) => id === bannedPubKey)).toBe(true);

        ws.close();
        alice.close();
    });

    it("uses relay checkpoints as compact subscription replay anchors", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        await owner.connect();

        const guildId = await owner.createGuild("Checkpoint Replay Guild");
        const oldChannelId = await owner.createChannel(guildId, "old-room", "text");
        await owner.sendMessage(guildId, oldChannelId, "before checkpoint");
        await new Promise((res) => setTimeout(res, 200));

        await relay.createCheckpoints();
        const newChannelId = await owner.createChannel(guildId, "new-room", "text");
        await new Promise((res) => setTimeout(res, 200));

        const ws = new WebSocket(relayUrl);
        await new Promise<void>((resolve) => ws.onopen = () => resolve());

        const snapshotPromise = new Promise<any>((resolve) => {
            const handler = (raw: WebSocket.RawData) => {
                const data = JSON.parse(raw.toString());
                if (data[0] === "SNAPSHOT" && data[1]?.subId === "checkpoint-sub") {
                    ws.off("message", handler);
                    resolve(data[1]);
                }
            };
            ws.on("message", handler);
        });

        ws.send(JSON.stringify(["SUB", { subId: "checkpoint-sub", guildId }]));
        const snapshot = await snapshotPromise;
        expect(snapshot.events[0].body.type).toBe("CHECKPOINT");
        expect(snapshot.events.some((event: any) => event.body.type === "GUILD_CREATE")).toBe(false);
        expect(snapshot.events.some((event: any) => event.body.channelId === newChannelId)).toBe(true);
        expect(snapshot.checkpointSeq).toBe(snapshot.events[0].seq);
        expect(snapshot.hasMore).toBe(true);

        const joiningClient = new CgpClient({ relays: [relayUrl] });
        await joiningClient.connect();
        await joiningClient.subscribe(guildId);
        await new Promise((res) => setTimeout(res, 300));
        const joinedState = joiningClient.getGuildState(guildId);
        expect(joinedState?.channels.has(oldChannelId)).toBe(true);
        expect(joinedState?.channels.has(newChannelId)).toBe(true);

        ws.close();
        owner.close();
        joiningClient.close();
    });

    it("rejects client-published checkpoints", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        await owner.connect();

        const guildId = await owner.createGuild("Client Checkpoint Rejection Guild");
        await expect(owner.publishReliable({
            type: "CHECKPOINT",
            guildId,
            rootHash: "bad",
            seq: 999,
            state: {}
        } as any, { timeoutMs: 5000 })).rejects.toThrow(/relay-maintained/i);

        owner.close();
    });

    it("returns correlated publish acknowledgements and validation errors", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const attackerPrivKey = secp.utils.randomPrivateKey();
        const attackerPubKey = Buffer.from(secp.getPublicKey(attackerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        await owner.connect();

        const guildId = await owner.createGuild("Ack Guild");
        const channelId = await owner.createChannel(guildId, "ack", "text");
        await new Promise((res) => setTimeout(res, 200));

        const ws = new WebSocket(relayUrl);
        await new Promise<void>((resolve) => ws.onopen = () => resolve());

        const waitForFrame = (kind: string, clientEventId: string) => new Promise<any>((resolve) => {
            const handler = (raw: WebSocket.RawData) => {
                const data = JSON.parse(raw.toString());
                if (data[0] === kind && data[1]?.clientEventId === clientEventId) {
                    ws.off("message", handler);
                    resolve(data[1]);
                }
            };
            ws.on("message", handler);
        });

        const ackBody = {
            type: "MESSAGE",
            guildId,
            channelId,
            messageId: `ack-message-${Date.now()}`,
            content: "ack me"
        };
        const ackCreatedAt = Date.now();
        const ackClientEventId = "publish-ack-test";
        const ackSignature = await sign(ownerPrivKey, hashObject({ body: ackBody, author: ownerPubKey, createdAt: ackCreatedAt }));
        const ackPromise = waitForFrame("PUB_ACK", ackClientEventId);
        ws.send(JSON.stringify([
            "PUBLISH",
            {
                body: ackBody,
                author: ownerPubKey,
                createdAt: ackCreatedAt,
                signature: ackSignature,
                clientEventId: ackClientEventId
            }
        ]));
        const ack = await ackPromise;
        expect(ack.guildId).toBe(guildId);
        expect(ack.eventId).toBeDefined();
        expect(typeof ack.seq).toBe("number");

        const errorBody = {
            type: "CHANNEL_CREATE",
            guildId,
            channelId: `attacker-channel-${Date.now()}`,
            name: "attacker",
            kind: "text"
        };
        const errorCreatedAt = Date.now();
        const errorClientEventId = "publish-error-test";
        const errorSignature = await sign(
            attackerPrivKey,
            hashObject({ body: errorBody, author: attackerPubKey, createdAt: errorCreatedAt })
        );
        const errorPromise = waitForFrame("ERROR", errorClientEventId);
        ws.send(JSON.stringify([
            "PUBLISH",
            {
                body: errorBody,
                author: attackerPubKey,
                createdAt: errorCreatedAt,
                signature: errorSignature,
                clientEventId: errorClientEventId
            }
        ]));
        const error = await errorPromise;
        expect(error.code).toBe("VALIDATION_FAILED");
        expect(error.message).toContain("permission");

        ws.close();
        owner.close();
    });

    it("keeps rate limiting as a configurable relay plugin, not mandatory core protocol", async () => {
        const pluginlessPort = PORT + 1;
        const pluginlessRelay = new RelayServer(pluginlessPort, new MemoryStore(), [], {
            enableDefaultPlugins: false
        });
        const ws = new WebSocket(`ws://localhost:${pluginlessPort}`);

        try {
            await new Promise<void>((resolve) => ws.onopen = () => resolve());
            const helloPromise = new Promise<any>((resolve) => {
                ws.onmessage = (msg) => {
                    const data = JSON.parse(msg.data.toString());
                    if (data[0] === "HELLO_OK") {
                        resolve(data[1]);
                    }
                };
            });
            ws.send(JSON.stringify(["HELLO", { protocol: "cgp/0.1" }]));
            const hello = await helloPromise;
            expect(hello.plugins).toEqual([]);
        } finally {
            ws.close();
            await pluginlessRelay.close();
        }
    });

    it("enforces default generic chat app-object permissions for message pins", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const attackerPrivKey = secp.utils.randomPrivateKey();
        const attackerPubKey = Buffer.from(secp.getPublicKey(attackerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        const attacker = new CgpClient({ relays: [relayUrl], keyPair: { pub: attackerPubKey, priv: attackerPrivKey } });
        await Promise.all([owner.connect(), attacker.connect()]);

        const guildId = await owner.createGuild("Default App Object Policy Guild");
        const channelId = await owner.createChannel(guildId, "pins", "text");
        await new Promise((res) => setTimeout(res, 200));
        const messageId = `pin-target-${Date.now()}`;
        await owner.publishReliable({
            type: "MESSAGE",
            guildId,
            channelId,
            messageId,
            content: "pin permission target"
        });

        const pinBody = {
            type: "APP_OBJECT_UPSERT" as const,
            guildId,
            channelId,
            namespace: "org.cgp.chat",
            objectType: "message-pin",
            objectId: `message-pin:${channelId}:${messageId}`,
            target: { channelId, messageId },
            value: { pinned: true }
        };

        await expect(attacker.publishReliable(pinBody, { timeoutMs: 5000 })).rejects.toThrow(/permission/i);
        await expect(owner.publishReliable(pinBody, { timeoutMs: 5000 })).resolves.toMatchObject({
            guildId
        });

        owner.close();
        attacker.close();
    });

    it("enforces default portable app surface permissions", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const attackerPrivKey = secp.utils.randomPrivateKey();
        const attackerPubKey = Buffer.from(secp.getPublicKey(attackerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        const attacker = new CgpClient({ relays: [relayUrl], keyPair: { pub: attackerPubKey, priv: attackerPrivKey } });
        await Promise.all([owner.connect(), attacker.connect()]);

        const guildId = await owner.createGuild("Default App Surface Guild");
        const channelId = await owner.createChannel(guildId, "bot-lab", "text");
        await new Promise((res) => setTimeout(res, 200));

        const manifestBody = {
            type: "APP_OBJECT_UPSERT" as const,
            guildId,
            namespace: "org.cgp.apps",
            objectType: "app-manifest",
            objectId: "assistant",
            value: {
                name: "Assistant",
                bot: true,
                agent: true,
                commands: [{ name: "ask", description: "Ask the assistant" }]
            }
        };

        await expect(attacker.publishReliable(manifestBody, { timeoutMs: 5000 })).rejects.toThrow(/permission/i);
        await expect(owner.publishReliable(manifestBody, { timeoutMs: 5000 })).resolves.toMatchObject({ guildId });

        await expect(owner.publishReliable({
            type: "APP_OBJECT_UPSERT" as const,
            guildId,
            channelId,
            namespace: "org.cgp.apps",
            objectType: "webhook",
            objectId: "deploy",
            target: { channelId },
            value: { name: "Deploy Hook", token: "do-not-log-secrets" }
        }, { timeoutMs: 5000 })).rejects.toThrow(/plaintext token/i);

        await expect(owner.publishReliable({
            type: "APP_OBJECT_UPSERT" as const,
            guildId,
            channelId,
            namespace: "org.cgp.apps",
            objectType: "webhook",
            objectId: "deploy",
            target: { channelId },
            value: { name: "Deploy Hook", credentialRef: "relay-local:deploy" }
        }, { timeoutMs: 5000 })).resolves.toMatchObject({ guildId });

        owner.close();
        attacker.close();
    });

    it("enforces channel permission overwrites on the relay", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const memberPrivKey = secp.utils.randomPrivateKey();
        const memberPubKey = Buffer.from(secp.getPublicKey(memberPrivKey, true)).toString("hex");
        const attackerPrivKey = secp.utils.randomPrivateKey();
        const attackerPubKey = Buffer.from(secp.getPublicKey(attackerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        const member = new CgpClient({ relays: [relayUrl], keyPair: { pub: memberPubKey, priv: memberPrivKey } });
        const attacker = new CgpClient({ relays: [relayUrl], keyPair: { pub: attackerPubKey, priv: attackerPrivKey } });
        await Promise.all([owner.connect(), member.connect(), attacker.connect()]);

        const guildId = await owner.createGuild("Overwrite Enforcement Guild");
        const channelId = await owner.createChannel(guildId, "staff", "text");
        await owner.publishReliable({
            type: "ROLE_UPSERT",
            guildId,
            roleId: "speaker",
            name: "Speaker",
            permissions: []
        });
        await owner.publishReliable({
            type: "ROLE_ASSIGN",
            guildId,
            userId: memberPubKey,
            roleId: "speaker"
        });
        await owner.publishReliable({
            type: "CHANNEL_UPSERT",
            guildId,
            channelId,
            permissionOverwrites: [
                {
                    id: "@everyone",
                    kind: "role",
                    allow: ["viewChannels"],
                    deny: ["sendMessages"]
                },
                {
                    id: "speaker",
                    kind: "role",
                    allow: ["sendMessages"],
                    deny: []
                }
            ]
        });
        await new Promise((res) => setTimeout(res, 200));

        await expect(attacker.publishReliable({
            type: "MESSAGE",
            guildId,
            channelId,
            messageId: `blocked-overwrite-${Date.now()}`,
            content: "should not pass"
        }, { timeoutMs: 5000 })).rejects.toThrow(/sendMessages/i);

        await expect(member.publishReliable({
            type: "MESSAGE",
            guildId,
            channelId,
            messageId: `allowed-overwrite-${Date.now()}`,
            content: "role overwrite allowed"
        }, { timeoutMs: 5000 })).resolves.toMatchObject({ guildId });

        owner.close();
        member.close();
        attacker.close();
    });

    it("requires signed membership for private guild reads", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const strangerPrivKey = secp.utils.randomPrivateKey();
        const strangerPubKey = Buffer.from(secp.getPublicKey(strangerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        const stranger = new CgpClient({ relays: [relayUrl], keyPair: { pub: strangerPubKey, priv: strangerPrivKey } });
        await Promise.all([owner.connect(), stranger.connect()]);

        const guildId = await owner.createGuild("Private Read Guild", undefined, "private");
        const channelId = await owner.createChannel(guildId, "vault", "text");
        const messageId = await owner.sendMessage(guildId, channelId, "private relay history");
        await new Promise((res) => setTimeout(res, 300));

        const ws = new WebSocket(relayUrl);
        await new Promise<void>((resolve) => ws.onopen = () => resolve());
        const errorPromise = new Promise<any>((resolve) => {
            const handler = (raw: WebSocket.RawData) => {
                const data = JSON.parse(raw.toString());
                if (data[0] === "ERROR" && data[1]?.subId === "private-state") {
                    ws.off("message", handler);
                    resolve(data[1]);
                }
            };
            ws.on("message", handler);
        });
        ws.send(JSON.stringify(["GET_STATE", { subId: "private-state", guildId }]));
        await expect(errorPromise).resolves.toMatchObject({ code: "FORBIDDEN" });

        await expect(stranger.getState(guildId)).rejects.toThrow(/permission/i);
        await expect(owner.getState(guildId)).resolves.toMatchObject({ guildId });
        const history = await owner.getHistory({ guildId, channelId, limit: 10 });
        expect(history.events.some((event: any) => event.body.type === "MESSAGE" && event.body.messageId === messageId)).toBe(true);

        ws.close();
        owner.close();
        stranger.close();
    });

    it("serves canonical member lists only to authorized readers", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const strangerPrivKey = secp.utils.randomPrivateKey();
        const strangerPubKey = Buffer.from(secp.getPublicKey(strangerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        const stranger = new CgpClient({ relays: [relayUrl], keyPair: { pub: strangerPubKey, priv: strangerPrivKey } });
        await Promise.all([owner.connect(), stranger.connect()]);

        const guildId = await owner.createGuild("Private Members Guild", undefined, "private");
        await new Promise((res) => setTimeout(res, 200));

        await expect(stranger.getMembers(guildId)).rejects.toThrow(/permission/i);
        await expect(owner.getMembers(guildId)).resolves.toEqual(
            expect.arrayContaining([
                expect.objectContaining({
                    userId: ownerPubKey,
                    roles: expect.arrayContaining(["owner"])
                })
            ])
        );

        const ws = new WebSocket(relayUrl);
        await new Promise<void>((resolve) => ws.onopen = () => resolve());
        const errorPromise = new Promise<any>((resolve) => {
            const handler = (raw: WebSocket.RawData) => {
                const data = JSON.parse(raw.toString());
                if (data[0] === "ERROR" && data[1]?.subId === "private-members") {
                    ws.off("message", handler);
                    resolve(data[1]);
                }
            };
            ws.on("message", handler);
        });
        ws.send(JSON.stringify(["GET_MEMBERS", { subId: "private-members", guildId }]));
        await expect(errorPromise).resolves.toMatchObject({ code: "FORBIDDEN" });

        ws.close();
        owner.close();
        stranger.close();
    });

    it("serves members and guild state as bounded pages", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        await owner.connect();

        const guildId = await owner.createGuild("Paged Members Guild");
        for (let index = 0; index < 12; index += 1) {
            await owner.assignRole(guildId, `member-${String(index).padStart(3, "0")}`, "member");
        }
        await new Promise((res) => setTimeout(res, 300));

        const firstPage = await owner.getMembersPage(guildId, { limit: 5 });
        expect(firstPage.members).toHaveLength(5);
        expect(firstPage.hasMore).toBe(true);
        expect(firstPage.nextCursor).toBe(firstPage.members.at(-1)?.userId);

        const secondPage = await owner.getMembersPage(guildId, { limit: 5, afterUserId: firstPage.nextCursor ?? undefined });
        expect(secondPage.members.length).toBeGreaterThan(0);
        expect(secondPage.members[0].userId.localeCompare(firstPage.nextCursor ?? "")).toBeGreaterThan(0);

        const partialState = await owner.getState(guildId, { memberLimit: 4, includeMessages: false });
        expect(partialState.stateIncludes).toMatchObject({ members: "partial", messages: "omitted" });
        expect(partialState.membersPage).toMatchObject({ hasMore: true });
        expect(partialState.state.members).toHaveLength(4);
        expect(partialState.state.messages ?? []).toHaveLength(0);

        owner.close();
    });

    it("filters hidden channel history for unauthenticated readers", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        await owner.connect();

        const guildId = await owner.createGuild("Hidden Channel Guild");
        const channelId = await owner.createChannel(guildId, "ops", "text");
        await owner.publishReliable({
            type: "CHANNEL_UPSERT",
            guildId,
            channelId,
            permissionOverwrites: [
                {
                    id: "@everyone",
                    kind: "role",
                    deny: ["viewChannels"],
                    allow: []
                }
            ]
        });
        await owner.sendMessage(guildId, channelId, "hidden relay history");
        await new Promise((res) => setTimeout(res, 300));

        const ws = new WebSocket(relayUrl);
        await new Promise<void>((resolve) => ws.onopen = () => resolve());
        const errorPromise = new Promise<any>((resolve) => {
            const handler = (raw: WebSocket.RawData) => {
                const data = JSON.parse(raw.toString());
                if (data[0] === "ERROR" && data[1]?.subId === "hidden-history") {
                    ws.off("message", handler);
                    resolve(data[1]);
                }
            };
            ws.on("message", handler);
        });
        ws.send(JSON.stringify(["GET_HISTORY", { subId: "hidden-history", guildId, channelId, limit: 10 }]));
        await expect(errorPromise).resolves.toMatchObject({ code: "FORBIDDEN" });

        const history = await owner.getHistory({ guildId, channelId, limit: 10 });
        expect(history.events.some((event: any) => event.body.content === "hidden relay history")).toBe(true);

        ws.close();
        owner.close();
    });

    it("treats publish batch retries with the same batchId as idempotent", async () => {
        const ownerPrivKey = secp.utils.randomPrivateKey();
        const ownerPubKey = Buffer.from(secp.getPublicKey(ownerPrivKey, true)).toString("hex");
        const owner = new CgpClient({ relays: [relayUrl], keyPair: { pub: ownerPubKey, priv: ownerPrivKey } });
        await owner.connect();

        const guildId = await owner.createGuild("Idempotent Batch Guild");
        const channelId = await owner.createChannel(guildId, "general", "text");
        const bodies = [
            {
                type: "MESSAGE",
                guildId,
                channelId,
                messageId: "idempotent-0",
                content: "retry-me-0"
            },
            {
                type: "MESSAGE",
                guildId,
                channelId,
                messageId: "idempotent-1",
                content: "retry-me-1"
            }
        ] as any[];

        const first = await owner.publishBatchReliable(bodies, { batchId: "retry-batch", timeoutMs: 20_000 });
        const second = await owner.publishBatchReliable(bodies, { batchId: "retry-batch", timeoutMs: 20_000 });
        expect(first).toHaveLength(2);
        expect(second).toHaveLength(2);
        expect(second.map((ack) => ack.seq)).toEqual(first.map((ack) => ack.seq));
        expect(second.map((ack) => ack.eventId)).toEqual(first.map((ack) => ack.eventId));

        const history = await owner.getHistory({ guildId, channelId, limit: 50 });
        const published = history.events.filter((event: any) =>
            event.body?.type === "MESSAGE" &&
            (event.body?.messageId === "idempotent-0" || event.body?.messageId === "idempotent-1")
        );
        expect(published).toHaveLength(2);

        owner.close();
    });

    it("enforces validation rules", async () => {
        const privKeyA = secp.utils.randomPrivateKey();
        const pubKeyA = Buffer.from(secp.getPublicKey(privKeyA, true)).toString("hex");
        const alice = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKeyA, priv: privKeyA } });

        const privKeyB = secp.utils.randomPrivateKey();
        const pubKeyB = Buffer.from(secp.getPublicKey(privKeyB, true)).toString("hex");
        const bob = new CgpClient({ relays: [relayUrl], keyPair: { pub: pubKeyB, priv: privKeyB } });

        await Promise.all([alice.connect(), bob.connect()]);

        const guildId = await alice.createGuild("Validation Guild");

        bob.subscribe(guildId);
        await new Promise(r => setTimeout(r, 100));

        const receivedEvents: any[] = [];
        bob.on("event", e => receivedEvents.push(e.body));

        // 1. Bob tries to create channel (should fail - not owner)
        await bob.publish({
            type: "CHANNEL_CREATE",
            guildId,
            channelId: "bob_chan",
            name: "Bob Channel",
            kind: "text"
        });

        await new Promise(r => setTimeout(r, 200));
        expect(receivedEvents.some(e => e.channelId === "bob_chan")).toBe(false);

        // 2. Alice creates channel (should succeed)
        const chanId = await alice.createChannel(guildId, "general", "text");
        await new Promise(r => setTimeout(r, 200));
        expect(receivedEvents.some(e => e.channelId === chanId)).toBe(true);

        // 3. Alice bans Bob
        await alice.banUser(guildId, pubKeyB, "spam");
        await new Promise(r => setTimeout(r, 200));
        expect(receivedEvents.some(e => e.type === "BAN_USER" && e.userId === pubKeyB)).toBe(true);

        // 4. Bob tries to send message (should fail - banned)
        await bob.sendMessage(guildId, chanId, "I am banned");
        await new Promise(r => setTimeout(r, 200));
        expect(receivedEvents.some(e => e.content === "I am banned")).toBe(false);
    });
});
