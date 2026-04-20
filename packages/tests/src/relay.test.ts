import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { RelayServer } from "@cgp/relay/src/server"; // Import directly from src to avoid build step in dev
import { MemoryStore } from "@cgp/relay/src/store";
import { CgpClient } from "@cgp/client/src/client";
import { hashObject, sign } from "@cgp/core";
import * as secp from "@noble/secp256k1";
import WebSocket from "ws";

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
