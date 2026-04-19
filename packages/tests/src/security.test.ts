import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { RelayServer } from "@cgp/relay";
import { CgpClient } from "@cgp/client";
import { MemoryStore, createAppObjectPermissionPlugin } from "@cgp/relay";
import { generatePrivateKey, getPublicKey, hashObject, sign, GuildEvent, computeEventId } from "@cgp/core";
import WebSocket from "ws";

describe("Security & Robustness", () => {
    let relay: RelayServer;
    let ownerClient: CgpClient;
    let attackerClient: CgpClient;
    const PORT = 8095;
    let guildId: string;
    let ownerKeys: { pub: string; priv: Uint8Array };
    let attackerKeys: { pub: string; priv: Uint8Array };

    beforeAll(async () => {
        relay = new RelayServer(PORT, new MemoryStore(), [
            createAppObjectPermissionPlugin({
                rules: [
                    {
                        namespace: "org.example.chat",
                        objectType: "message-pin",
                        permissionScope: "messages"
                    }
                ]
            })
        ]);

        const ownerPriv = generatePrivateKey();
        ownerKeys = { pub: getPublicKey(ownerPriv), priv: ownerPriv };
        ownerClient = new CgpClient({ relays: [`ws://localhost:${PORT}`], keyPair: ownerKeys });
        await ownerClient.connect();

        const attackerPriv = generatePrivateKey();
        attackerKeys = { pub: getPublicKey(attackerPriv), priv: attackerPriv };
        attackerClient = new CgpClient({ relays: [`ws://localhost:${PORT}`], keyPair: attackerKeys });
        await attackerClient.connect();

        // Create a legitimate guild
        guildId = await ownerClient.createGuild("Secure Guild");
        await new Promise(r => setTimeout(r, 500));
    });

    afterAll(async () => {
        ownerClient.close();
        attackerClient.close();
        await relay.close();
    });

    async function publishRaw(
        body: Record<string, any>,
        keys: { pub: string; priv: Uint8Array } = ownerKeys,
        waitMs = 350
    ) {
        const ws = new WebSocket(`ws://localhost:${PORT}`);
        await new Promise<void>(resolve => ws.onopen = () => resolve());
        const createdAt = Date.now();
        const signature = await sign(keys.priv, hashObject({ body, author: keys.pub, createdAt }));
        const errorPromise = new Promise<any | null>(resolve => {
            const timer = setTimeout(() => resolve(null), waitMs);
            ws.onmessage = (msg) => {
                const data = JSON.parse(msg.data.toString());
                if (data[0] === "ERROR") {
                    clearTimeout(timer);
                    resolve(data[1]);
                }
            };
        });
        ws.send(JSON.stringify(["PUBLISH", { body, author: keys.pub, createdAt, signature }]));
        const error = await errorPromise;
        ws.close();
        if (!error) {
            await new Promise(resolve => setTimeout(resolve, 150));
        }
        return error;
    }

    it("rejects events with invalid signatures", async () => {
        // Attacker tries to forge a message from Owner
        const body = {
            type: "MESSAGE",
            guildId,
            channelId: "fake-channel",
            messageId: "fake-msg",
            content: "I am the owner"
        };

        const createdAt = Date.now();
        const author = ownerKeys.pub; // Impersonating owner

        // Sign with ATTACKER'S key (invalid for author field)
        const unsignedForSig = { body, author, createdAt };
        const msgHash = hashObject(unsignedForSig);
        const signature = await sign(attackerKeys.priv, msgHash); // Wrong key!

        const payload = { body, author, createdAt, signature };

        // We need to send raw frame to bypass client's own checks (if any)
        // Attacker client usually signs with its own key, so we manually construct the frame
        const ws = new WebSocket(`ws://localhost:${PORT}`);
        await new Promise<void>(resolve => ws.onopen = () => resolve());

        const errorPromise = new Promise<any>(resolve => {
            ws.onmessage = (msg) => {
                const data = JSON.parse(msg.data.toString());
                if (data[0] === "ERROR") resolve(data[1]);
            };
        });

        ws.send(JSON.stringify(["PUBLISH", payload]));

        const error = await errorPromise;
        expect(error.code).toBe("INVALID_SIGNATURE");
        ws.close();
    });

    it("prevents unauthorized users from performing admin actions", async () => {
        // Attacker tries to create a channel (requires admin/owner)
        // Attacker signs correctly with THEIR key, but they are not admin.

        const ws = new WebSocket(`ws://localhost:${PORT}`);
        await new Promise<void>(resolve => ws.onopen = () => resolve());

        const errorPromise = new Promise<any>(resolve => {
            ws.onmessage = (msg) => {
                const data = JSON.parse(msg.data.toString());
                if (data[0] === "ERROR") resolve(data[1]);
            };
        });

        const body = {
            type: "CHANNEL_CREATE",
            guildId,
            channelId: "hacked-channel",
            name: "hacked",
            kind: "text"
        };

        const createdAt = Date.now();
        const author = attackerKeys.pub;
        const unsignedForSig = { body, author, createdAt };
        const msgHash = hashObject(unsignedForSig);
        const signature = await sign(attackerKeys.priv, msgHash);

        const payload = { body, author, createdAt, signature };
        ws.send(JSON.stringify(["PUBLISH", payload]));

        const error = await errorPromise;
        expect(error.code).toBe("VALIDATION_FAILED");
        expect(error.message).toContain("permission");
        ws.close();
    });

    it("prevents unauthorized users from publishing application admin events", async () => {
        const ws = new WebSocket(`ws://localhost:${PORT}`);
        await new Promise<void>(resolve => ws.onopen = () => resolve());

        const errorPromise = new Promise<any>(resolve => {
            ws.onmessage = (msg) => {
                const data = JSON.parse(msg.data.toString());
                if (data[0] === "ERROR") resolve(data[1]);
            };
        });

        const body = {
            type: "GUILD_UPDATE",
            guildId,
            name: "Hijacked Guild"
        };
        const createdAt = Date.now();
        const author = attackerKeys.pub;
        const signature = await sign(attackerKeys.priv, hashObject({ body, author, createdAt }));
        ws.send(JSON.stringify(["PUBLISH", { body, author, createdAt, signature }]));

        const error = await errorPromise;
        expect(error.code).toBe("VALIDATION_FAILED");
        expect(error.message).toContain("permission");
        ws.close();
    });

    it("applies BAN_ADD events to relay-authoritative ban state", async () => {
        const victimPriv = generatePrivateKey();
        const victimPub = getPublicKey(victimPriv);
        const channelId = await ownerClient.createChannel(guildId, "custom-ban-channel", "text");
        await new Promise(r => setTimeout(r, 200));

        const banBody = {
            type: "BAN_ADD",
            guildId,
            userId: victimPub,
            reason: "custom moderation event"
        };
        const banCreatedAt = Date.now();
        const banSignature = await sign(ownerKeys.priv, hashObject({ body: banBody, author: ownerKeys.pub, createdAt: banCreatedAt }));
        const ownerWs = new WebSocket(`ws://localhost:${PORT}`);
        await new Promise<void>(resolve => ownerWs.onopen = () => resolve());
        ownerWs.send(JSON.stringify(["PUBLISH", { body: banBody, author: ownerKeys.pub, createdAt: banCreatedAt, signature: banSignature }]));
        await new Promise(r => setTimeout(r, 300));
        ownerWs.close();

        const victimWs = new WebSocket(`ws://localhost:${PORT}`);
        await new Promise<void>(resolve => victimWs.onopen = () => resolve());
        const errorPromise = new Promise<any>(resolve => {
            victimWs.onmessage = (msg) => {
                const data = JSON.parse(msg.data.toString());
                if (data[0] === "ERROR") resolve(data[1]);
            };
        });
        const messageBody = {
            type: "MESSAGE",
            guildId,
            channelId,
            messageId: hashObject(`blocked-custom-ban-${Date.now()}`),
            content: "custom ban should block this"
        };
        const messageCreatedAt = Date.now();
        const messageSignature = await sign(victimPriv, hashObject({ body: messageBody, author: victimPub, createdAt: messageCreatedAt }));
        victimWs.send(JSON.stringify(["PUBLISH", { body: messageBody, author: victimPub, createdAt: messageCreatedAt, signature: messageSignature }]));

        const error = await errorPromise;
        expect(error.code).toBe("VALIDATION_FAILED");
        expect(error.message).toContain("banned");
        victimWs.close();
    });

    it("enforces app-object pin permissions at the relay", async () => {
        const channelId = await ownerClient.createChannel(guildId, "pin-policy-channel", "text");
        const messageId = hashObject(`pin-policy-message-${Date.now()}`);
        await publishRaw({
            type: "MESSAGE",
            guildId,
            channelId,
            messageId,
            content: "Only moderators should be able to pin this."
        });

        const attackerError = await publishRaw({
            type: "APP_OBJECT_UPSERT",
            guildId,
            channelId,
            namespace: "org.example.chat",
            objectType: "message-pin",
            objectId: `message-pin:${channelId}:${messageId}`,
            target: { channelId, messageId },
            value: { pinned: true }
        }, attackerKeys);
        expect(attackerError?.code).toBe("VALIDATION_FAILED");
        expect(attackerError?.message).toContain("permission");

        const ownerError = await publishRaw({
            type: "APP_OBJECT_UPSERT",
            guildId,
            channelId,
            namespace: "org.example.chat",
            objectType: "message-pin",
            objectId: `message-pin:${channelId}:${messageId}`,
            target: { channelId, messageId },
            value: { pinned: true }
        });
        expect(ownerError).toBeNull();

        const log = await (relay as any).store.getLog(guildId);
        expect(log.some((e: GuildEvent) => (e.body as any).objectId === `message-pin:${channelId}:${messageId}`)).toBe(true);
    });

    it("uses custom role permissions when validating application admin events", async () => {
        const channelId = hashObject(`custom-role-channel-${Date.now()}`);
        const rejected = await publishRaw({
            type: "CHANNEL_UPSERT",
            guildId,
            channelId,
            name: "should-not-exist-yet",
            kind: "text"
        }, attackerKeys);
        expect(rejected?.code).toBe("VALIDATION_FAILED");
        expect(rejected?.message).toContain("permission");

        const roleId = `relay-channel-manager-${Date.now()}`;
        expect(await publishRaw({
            type: "ROLE_UPSERT",
            guildId,
            roleId,
            name: "Relay Channel Manager",
            permissions: ["MANAGE_CHANNELS"]
        })).toBeNull();
        expect(await publishRaw({
            type: "MEMBER_UPDATE",
            guildId,
            userId: attackerKeys.pub,
            roleIds: [roleId]
        })).toBeNull();

        const accepted = await publishRaw({
            type: "CHANNEL_UPSERT",
            guildId,
            channelId,
            name: "role-managed",
            kind: "text"
        }, attackerKeys);
        expect(accepted).toBeNull();

        const messageId = hashObject(`custom-role-message-${Date.now()}`);
        const messageError = await publishRaw({
            type: "MESSAGE",
            guildId,
            channelId,
            messageId,
            content: "Channel created through relay-authoritative role permissions."
        }, attackerKeys);
        expect(messageError).toBeNull();
    });

    it("rejects duplicate channel ids in a guild", async () => {
        const channelId = await ownerClient.createChannel(guildId, "unique-channel", "text");
        await new Promise(r => setTimeout(r, 200));

        const body = {
            type: "CHANNEL_CREATE",
            guildId,
            channelId,
            name: "duplicate-channel",
            kind: "text"
        };
        const createdAt = Date.now();
        const author = ownerKeys.pub;
        const signature = await sign(ownerKeys.priv, hashObject({ body, author, createdAt }));
        const payload = { body, author, createdAt, signature };

        const ws = new WebSocket(`ws://localhost:${PORT}`);
        await new Promise<void>(resolve => ws.onopen = () => resolve());

        const errorPromise = new Promise<any>(resolve => {
            ws.onmessage = (msg) => {
                const data = JSON.parse(msg.data.toString());
                if (data[0] === "ERROR") resolve(data[1]);
            };
        });

        ws.send(JSON.stringify(["PUBLISH", payload]));

        const error = await errorPromise;
        expect(error.code).toBe("VALIDATION_FAILED");
        expect(error.message).toContain("already exists");
        ws.close();
    });

    it("rejects GUILD_CREATE after genesis for an existing guild log", async () => {
        const body = {
            type: "GUILD_CREATE",
            guildId,
            name: "Shadow Genesis"
        };
        const createdAt = Date.now();
        const author = ownerKeys.pub;
        const signature = await sign(ownerKeys.priv, hashObject({ body, author, createdAt }));
        const payload = { body, author, createdAt, signature };

        const ws = new WebSocket(`ws://localhost:${PORT}`);
        await new Promise<void>(resolve => ws.onopen = () => resolve());

        const errorPromise = new Promise<any>(resolve => {
            ws.onmessage = (msg) => {
                const data = JSON.parse(msg.data.toString());
                if (data[0] === "ERROR") resolve(data[1]);
            };
        });

        ws.send(JSON.stringify(["PUBLISH", payload]));

        const error = await errorPromise;
        expect(error.code).toBe("VALIDATION_FAILED");
        expect(error.message).toContain("seq 0");
        ws.close();
    });

    it("enforces members-only posting policy for public guilds", async () => {
        const restrictedGuildId = await ownerClient.createGuild("Members Only Public Guild", undefined, "public", { posting: "members" });
        const channelId = await ownerClient.createChannel(restrictedGuildId, "members", "text");
        await new Promise(r => setTimeout(r, 200));

        const body = {
            type: "MESSAGE",
            guildId: restrictedGuildId,
            channelId,
            messageId: hashObject(`blocked-${Date.now()}`),
            content: "I should not be accepted"
        };
        const createdAt = Date.now();
        const author = attackerKeys.pub;
        const signature = await sign(attackerKeys.priv, hashObject({ body, author, createdAt }));
        const payload = { body, author, createdAt, signature };

        const ws = new WebSocket(`ws://localhost:${PORT}`);
        await new Promise<void>(resolve => ws.onopen = () => resolve());

        const errorPromise = new Promise<any>(resolve => {
            ws.onmessage = (msg) => {
                const data = JSON.parse(msg.data.toString());
                if (data[0] === "ERROR") resolve(data[1]);
            };
        });

        ws.send(JSON.stringify(["PUBLISH", payload]));
        const error = await errorPromise;
        expect(error.code).toBe("VALIDATION_FAILED");
        expect(error.message).toContain("membership");
        ws.close();

        await ownerClient.assignRole(restrictedGuildId, attackerKeys.pub, "member");
        await new Promise(r => setTimeout(r, 200));
        await attackerClient.sendMessage(restrictedGuildId, channelId, "member message accepted");
        await new Promise(r => setTimeout(r, 500));

        const log = await (relay as any).store.getLog(restrictedGuildId);
        expect(log.some((e: GuildEvent) => (e.body as any).content === "I should not be accepted")).toBe(false);
        expect(log.some((e: GuildEvent) => (e.body as any).content === "member message accepted")).toBe(true);
    });

    it("rejects malformed payloads", async () => {
        const ws = new WebSocket(`ws://localhost:${PORT}`);
        await new Promise<void>(resolve => ws.onopen = () => resolve());

        const errorPromise = new Promise<any>(resolve => {
            ws.onmessage = (msg) => {
                const data = JSON.parse(msg.data.toString());
                if (data[0] === "ERROR") resolve(data[1]);
            };
        });

        // Send garbage JSON
        ws.send("THIS IS NOT JSON");

        const error = await errorPromise;
        expect(error.code).toBe("INVALID_FRAME");
        ws.close();
    });

    it("rejects events for non-existent guilds (unless GUILD_CREATE)", async () => {
        const ws = new WebSocket(`ws://localhost:${PORT}`);
        await new Promise<void>(resolve => ws.onopen = () => resolve());

        const body = {
            type: "MESSAGE",
            guildId: "non-existent-guild",
            channelId: "c",
            messageId: "m",
            content: "hello"
        };

        const createdAt = Date.now();
        const author = attackerKeys.pub;
        const unsignedForSig = { body, author, createdAt };
        const msgHash = hashObject(unsignedForSig);
        const signature = await sign(attackerKeys.priv, msgHash);

        const payload = { body, author, createdAt, signature };
        ws.send(JSON.stringify(["PUBLISH", payload]));

        // We don't expect an error frame necessarily, as the relay might just log and return.
        // Instead, we verify that the event was NOT appended to the store.

        await new Promise(r => setTimeout(r, 500));
        const log = await (relay as any).store.getLog("non-existent-guild");
        expect(log.length).toBe(0);

        ws.close();
    });

    describe("Advanced Attacks", () => {
        it("handles concurrent publishes without sequence collisions (Race Condition)", async () => {
            // Attack: Send 10 messages in parallel.
            // If Relay doesn't lock/serialize, they might all read the same 'headSeq' and write to the same 'seq'.
            // Result: Data loss (last write wins).

            const PARALLEL_COUNT = 10;
            const promises = [];
            const channelId = await ownerClient.createChannel(guildId, "race-channel", "text");

            // Wait for channel creation to settle
            await new Promise(r => setTimeout(r, 500));

            const startSeq = ownerClient.getGuildState(guildId)!.headSeq;

            for (let i = 0; i < PARALLEL_COUNT; i++) {
                promises.push(ownerClient.sendMessage(guildId, channelId, `Race msg ${i}`));
            }

            await Promise.all(promises);
            await new Promise(r => setTimeout(r, 1000));

            const state = ownerClient.getGuildState(guildId);
            // We expect headSeq to have increased by PARALLEL_COUNT
            const expectedSeq = startSeq + PARALLEL_COUNT;

            // If race condition exists, headSeq will be less than expected (some overwrote others)
            expect(state!.headSeq).toBe(expectedSeq);

            // Also verify all messages are in the log
            const log = await (relay as any).store.getLog(guildId);
            const raceMsgs = log.filter((e: GuildEvent) => e.body.type === "MESSAGE" && (e.body as any).content.startsWith("Race msg"));
            expect(raceMsgs.length).toBe(PARALLEL_COUNT);
        });

        it("prunes messages with ancient timestamps immediately (Retention Bypass)", async () => {
            // Attack: User sends message with createdAt = 0 (1970).
            // If channel has TTL, this message is effectively "expired" upon arrival.
            // It should be accepted, but then pruned immediately.
            // This allows an attacker to send "ghost" messages that might be seen briefly but vanish.

            const policy = { mode: "ttl", seconds: 60 }; // 1 minute TTL
            const channelId = await ownerClient.createChannel(guildId, "ghost-channel", "text", policy as any);
            await new Promise(r => setTimeout(r, 200));

            // Manually craft ancient message
            const body = {
                type: "MESSAGE",
                guildId,
                channelId,
                messageId: hashObject("ghost"),
                content: "I am a ghost"
            };
            const createdAt = 0; // 1970
            const author = ownerKeys.pub;
            const signature = await sign(ownerKeys.priv, hashObject({ body, author, createdAt }));
            const payload = { body, author, createdAt, signature };

            const ws = new WebSocket(`ws://localhost:${PORT}`);
            await new Promise<void>(resolve => ws.onopen = () => resolve());
            ws.send(JSON.stringify(["PUBLISH", payload]));
            ws.close();

            await new Promise(r => setTimeout(r, 500));

            // Verify it exists initially (or maybe not if we check log)
            let log = await (relay as any).store.getLog(guildId);
            let ghost = log.find((e: GuildEvent) => (e.body as any).content === "I am a ghost");
            expect(ghost).toBeDefined();

            // Run Prune
            await relay.prune();

            // Verify it is gone
            log = await (relay as any).store.getLog(guildId);
            ghost = log.find((e: GuildEvent) => (e.body as any).content === "I am a ghost");
            expect(ghost).toBeUndefined();
        });

        it("deduplicates replayed PUBLISH frames", async () => {
            // Attack: Capture a valid PUBLISH frame and replay it 10 times.
            // Since Relay assigns Seq, these become 10 valid events with identical bodies.
            // This is a spam vector. Ideally, Relay should deduplicate based on hash(body, author, createdAt).

            const body = {
                type: "MESSAGE",
                guildId,
                channelId: "fake-channel", // doesn't matter for this test, or use valid one
                messageId: "spam-msg",
                content: "Spam"
            };
            // Use a valid channel to avoid validation error
            const channelId = await ownerClient.createChannel(guildId, "spam-channel", "text");
            await new Promise(r => setTimeout(r, 200));
            body.channelId = channelId;

            const createdAt = Date.now();
            const author = ownerKeys.pub;
            const signature = await sign(ownerKeys.priv, hashObject({ body, author, createdAt }));
            const payload = { body, author, createdAt, signature };
            const frame = JSON.stringify(["PUBLISH", payload]);

            const ws = new WebSocket(`ws://localhost:${PORT}`);
            await new Promise<void>(resolve => ws.onopen = () => resolve());

            // Send 5 times
            for (let i = 0; i < 5; i++) {
                ws.send(frame);
                await new Promise(r => setTimeout(r, 10));
            }
            ws.close();

            await new Promise(r => setTimeout(r, 1000));

            const log = await (relay as any).store.getLog(guildId);
            const spam = log.filter((e: GuildEvent) => (e.body as any).content === "Spam");

            // Replays reuse the same messageId, so relay validation must reject duplicates.
            expect(spam.length).toBe(1);
        });
    });
});
