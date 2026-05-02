import { describe, expect, it } from "vitest";
import { CgpClient } from "@cgp/client/src/client";
import { GuildEvent } from "@cgp/core";
import { LocalRelayPubSubAdapter, RelayServer } from "@cgp/relay/src/server";
import { MemoryStore } from "@cgp/relay/src/store";
import * as secp from "@noble/secp256k1";

interface TestKeyPair {
    pub: string;
    priv: Uint8Array;
}

interface CallEventBody {
    type: "CALL_EVENT";
    guildId: string;
    channelId: string;
    roomId: string;
    payload: Record<string, any>;
}

function createKeyPair(): TestKeyPair {
    const priv = secp.utils.randomPrivateKey();
    const pub = Buffer.from(secp.getPublicKey(priv, true)).toString("hex");
    return { pub, priv };
}

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor<T>(label: string, predicate: () => T | undefined | false | Promise<T | undefined | false>, timeoutMs = 5000): Promise<T> {
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
        const value = await predicate();
        if (value) {
            return value;
        }
        await sleep(50);
    }
    throw new Error(`Timed out waiting for ${label}`);
}

async function relayUrl(relay: RelayServer) {
    const port = await waitFor("relay port", () => {
        const value = relay.getPort();
        return Number.isFinite(value) && value > 0 ? value : undefined;
    });
    return `ws://localhost:${port}`;
}

async function waitForLog(store: MemoryStore, guildId: string, minLength: number) {
    return await waitFor(`guild ${guildId} log length ${minLength}`, () => {
        const log = store.getLog(guildId);
        return log.length >= minLength ? log : undefined;
    });
}

function observeCallEvents(client: CgpClient) {
    const events: GuildEvent[] = [];
    client.on("event", (event: GuildEvent) => {
        if ((event.body as any).type === "CALL_EVENT") {
            events.push(event);
        }
    });
    return events;
}

async function waitForCallEvent(
    events: GuildEvent[],
    label: string,
    predicate: (body: CallEventBody, event: GuildEvent) => boolean
) {
    return await waitFor(label, () => {
        return events.find((event) => {
            const body = event.body as any;
            return body?.type === "CALL_EVENT" && predicate(body as CallEventBody, event);
        });
    });
}

function stableVerificationHex(input: string) {
    let left = 0x811c9dc5;
    let right = 0x9e3779b9;
    for (let index = 0; index < input.length; index += 1) {
        const code = input.charCodeAt(index);
        left ^= code;
        left = Math.imul(left, 0x01000193);
        right ^= code + index;
        right = Math.imul(right, 0x85ebca6b);
    }
    return `${(left >>> 0).toString(16).padStart(8, "0")}${(right >>> 0).toString(16).padStart(8, "0")}`;
}

function callVerificationCode(roomId: string, leftIdentityKey: string, rightIdentityKey: string) {
    const pair = [leftIdentityKey, rightIdentityKey].sort().join(":");
    const hash = stableVerificationHex(`${roomId}:${pair}`).slice(0, 12).toUpperCase();
    return `${hash.slice(0, 4)} ${hash.slice(4, 8)} ${hash.slice(8, 12)}`;
}

let messageCounter = 0;

function callEventBody(
    guildId: string,
    channelId: string,
    roomId: string,
    fromUserId: string,
    payload: Record<string, any>
): CallEventBody {
    messageCounter += 1;
    const kind = typeof payload.kind === "string" ? payload.kind : "custom";
    return {
        type: "CALL_EVENT",
        guildId,
        channelId,
        roomId,
        payload: {
            messageId: `call-${kind}-${messageCounter}`,
            roomId,
            fromUserId,
            timestamp: Date.now() + messageCounter,
            ...payload
        }
    };
}

async function publishCall(
    client: CgpClient,
    guildId: string,
    channelId: string,
    roomId: string,
    fromUserId: string,
    payload: Record<string, any>
) {
    return await client.publishReliable(
        callEventBody(guildId, channelId, roomId, fromUserId, payload) as any,
        { timeoutMs: 5000 }
    );
}

async function publishTransientCall(
    client: CgpClient,
    guildId: string,
    channelId: string,
    roomId: string,
    fromUserId: string,
    payload: Record<string, any>
) {
    return await client.publishTransientReliable(
        callEventBody(guildId, channelId, roomId, fromUserId, payload) as any,
        { timeoutMs: 5000 }
    );
}

describe("call verification loadnet relay pass", () => {
    it("delivers DM, guild voice, guest invite, and reconnect call signaling through relays", async () => {
        const pubSub = new LocalRelayPubSubAdapter();
        const storeA = new MemoryStore();
        const storeB = new MemoryStore();
        const relayA = new RelayServer(0, storeA, [], {
            instanceId: "call-loadnet-a",
            pubSubAdapter: pubSub,
            wireFormat: "binary-v1"
        });
        const relayB = new RelayServer(0, storeB, [], {
            instanceId: "call-loadnet-b",
            pubSubAdapter: pubSub,
            wireFormat: "binary-v1"
        });

        const ownerKeys = createKeyPair();
        const aliceKeys = createKeyPair();
        const bobKeys = createKeyPair();
        const guestKeys = createKeyPair();
        const monitorKeys = createKeyPair();
        let reconnectedMonitor: CgpClient | undefined;

        const urlA = await relayUrl(relayA);
        const urlB = await relayUrl(relayB);
        const owner = new CgpClient({ relays: [urlA], keyPair: ownerKeys, wireFormat: "binary-v1" });
        const alice = new CgpClient({ relays: [urlA], keyPair: aliceKeys, wireFormat: "binary-v1" });
        const bob = new CgpClient({ relays: [urlA], keyPair: bobKeys, wireFormat: "binary-v1" });
        const guest = new CgpClient({ relays: [urlA], keyPair: guestKeys, wireFormat: "binary-v1" });
        const monitor = new CgpClient({ relays: [urlB], keyPair: monitorKeys, wireFormat: "binary-v1" });

        try {
            await Promise.all([owner.connect(), alice.connect(), bob.connect(), guest.connect(), monitor.connect()]);

            const dmGuildId = await owner.createGuild("Call Verification DM");
            await waitForLog(storeA, dmGuildId, 1);
            const dmChannelId = await owner.createChannel(dmGuildId, "dm-call", "voice");
            await waitForLog(storeA, dmGuildId, 2);

            const voiceGuildId = await owner.createGuild("Call Verification Guild");
            await waitForLog(storeA, voiceGuildId, 1);
            const voiceChannelId = await owner.createChannel(voiceGuildId, "Guild Voice", "voice");
            await waitForLog(storeA, voiceGuildId, 2);

            const observed = observeCallEvents(monitor);
            await monitor.subscribe(dmGuildId);
            await monitor.subscribe(voiceGuildId);
            await waitForLog(storeB, dmGuildId, 2);
            await waitForLog(storeB, voiceGuildId, 2);

            const dmRoomId = `dm:${dmChannelId}`;
            await publishCall(alice, dmGuildId, dmChannelId, dmRoomId, aliceKeys.pub, {
                kind: "join",
                toUserId: bobKeys.pub,
                name: "Alice",
                avatar: "",
                audioEnabled: true,
                videoEnabled: true,
                screenEnabled: false,
                identityKey: aliceKeys.pub
            });
            await publishCall(bob, dmGuildId, dmChannelId, dmRoomId, bobKeys.pub, {
                kind: "presence",
                toUserId: aliceKeys.pub,
                name: "Bob",
                avatar: "",
                audioEnabled: true,
                videoEnabled: false,
                screenEnabled: false,
                identityKey: bobKeys.pub
            });
            await publishCall(alice, dmGuildId, dmChannelId, dmRoomId, aliceKeys.pub, {
                kind: "offer",
                toUserId: bobKeys.pub,
                description: { type: "offer", sdp: "v=0\r\ns=dm-call\r\n" }
            });
            await publishCall(bob, dmGuildId, dmChannelId, dmRoomId, bobKeys.pub, {
                kind: "answer",
                toUserId: aliceKeys.pub,
                description: { type: "answer", sdp: "v=0\r\ns=dm-call\r\n" }
            });
            await publishCall(alice, dmGuildId, dmChannelId, dmRoomId, aliceKeys.pub, {
                kind: "ice",
                toUserId: bobKeys.pub,
                candidate: {
                    candidate: "candidate:0 1 udp 2122260223 127.0.0.1 54400 typ host",
                    sdpMid: "0",
                    sdpMLineIndex: 0
                }
            });

            const dmJoin = await waitForCallEvent(observed, "DM join relay delivery", (body) =>
                body.roomId === dmRoomId &&
                body.payload.kind === "join" &&
                body.payload.identityKey === aliceKeys.pub
            );
            await waitForCallEvent(observed, "DM WebRTC answer relay delivery", (body) =>
                body.roomId === dmRoomId &&
                body.payload.kind === "answer" &&
                body.payload.toUserId === aliceKeys.pub
            );
            await waitForCallEvent(observed, "DM ICE relay delivery", (body) =>
                body.roomId === dmRoomId &&
                body.payload.kind === "ice" &&
                body.payload.toUserId === bobKeys.pub
            );
            const dmCode = callVerificationCode(dmRoomId, aliceKeys.pub, bobKeys.pub);
            expect(dmCode).toEqual(callVerificationCode(dmRoomId, bobKeys.pub, aliceKeys.pub));
            expect(dmCode).toMatch(/^[0-9A-F]{4} [0-9A-F]{4} [0-9A-F]{4}$/);
            expect(((dmJoin.body as CallEventBody).payload as any).fromUserId).toBe(aliceKeys.pub);

            const voiceRoomId = `server:${voiceGuildId}:channel:${voiceChannelId}`;
            await publishCall(alice, voiceGuildId, voiceChannelId, voiceRoomId, aliceKeys.pub, {
                kind: "join",
                name: "Alice",
                avatar: "",
                audioEnabled: true,
                videoEnabled: true,
                screenEnabled: false,
                identityKey: aliceKeys.pub
            });
            await publishCall(bob, voiceGuildId, voiceChannelId, voiceRoomId, bobKeys.pub, {
                kind: "presence",
                name: "Bob",
                avatar: "",
                audioEnabled: true,
                videoEnabled: true,
                screenEnabled: false,
                identityKey: bobKeys.pub
            });
            await publishCall(alice, voiceGuildId, voiceChannelId, voiceRoomId, aliceKeys.pub, {
                kind: "media-state",
                audioEnabled: false,
                videoEnabled: true,
                screenEnabled: false
            });
            const durableVoiceSeqBeforeTransient = storeA.getLog(voiceGuildId).length;
            await publishTransientCall(alice, voiceGuildId, voiceChannelId, voiceRoomId, aliceKeys.pub, {
                kind: "fallback-audio",
                transport: "relay-websocket",
                sequence: 1,
                capturedAt: Date.now(),
                sampleRate: 16000,
                pcmBase64: Buffer.alloc(640, 2).toString("base64")
            });
            await publishTransientCall(alice, voiceGuildId, voiceChannelId, voiceRoomId, aliceKeys.pub, {
                kind: "fallback-video",
                transport: "relay-websocket",
                sequence: 1,
                capturedAt: Date.now(),
                streamKind: "camera",
                frame: `data:image/jpeg;base64,${Buffer.alloc(8192, 3).toString("base64")}`,
                width: 320,
                height: 180
            });
            await publishTransientCall(alice, voiceGuildId, voiceChannelId, voiceRoomId, aliceKeys.pub, {
                kind: "fallback-video",
                transport: "relay-websocket",
                sequence: 1,
                capturedAt: Date.now(),
                streamKind: "screen",
                frame: `data:image/jpeg;base64,${Buffer.alloc(16384, 4).toString("base64")}`,
                width: 960,
                height: 540
            });

            await waitForCallEvent(observed, "guild voice join relay delivery", (body) =>
                body.roomId === voiceRoomId &&
                body.payload.kind === "join" &&
                body.payload.identityKey === aliceKeys.pub
            );
            await waitForCallEvent(observed, "guild voice media-state relay delivery", (body) =>
                body.roomId === voiceRoomId &&
                body.payload.kind === "media-state" &&
                body.payload.audioEnabled === false
            );
            await waitForCallEvent(observed, "relay WebSocket fallback audio delivery", (body) =>
                body.roomId === voiceRoomId &&
                body.payload.kind === "fallback-audio" &&
                body.payload.transport === "relay-websocket" &&
                body.payload.sampleRate === 16000
            );
            await waitForCallEvent(observed, "relay WebSocket camera fallback delivery", (body) =>
                body.roomId === voiceRoomId &&
                body.payload.kind === "fallback-video" &&
                body.payload.transport === "relay-websocket" &&
                body.payload.streamKind === "camera"
            );
            await waitForCallEvent(observed, "relay WebSocket screen fallback delivery", (body) =>
                body.roomId === voiceRoomId &&
                body.payload.kind === "fallback-video" &&
                body.payload.transport === "relay-websocket" &&
                body.payload.streamKind === "screen"
            );
            expect(storeA.getLog(voiceGuildId)).toHaveLength(durableVoiceSeqBeforeTransient);
            const voiceCode = callVerificationCode(voiceRoomId, aliceKeys.pub, bobKeys.pub);
            expect(voiceCode).toEqual(callVerificationCode(voiceRoomId, bobKeys.pub, aliceKeys.pub));
            expect(voiceCode).not.toEqual(dmCode);

            await publishCall(guest, voiceGuildId, voiceChannelId, voiceRoomId, "uid-guest", {
                kind: "join",
                name: "Guest",
                avatar: "",
                audioEnabled: true,
                videoEnabled: false,
                screenEnabled: false,
                identityKey: guestKeys.pub,
                inviteId: "invite-call-loadnet"
            });
            const guestJoin = await waitForCallEvent(observed, "guest invite call relay delivery", (body) =>
                body.roomId === voiceRoomId &&
                body.payload.kind === "join" &&
                body.payload.fromUserId === "uid-guest" &&
                body.payload.identityKey === guestKeys.pub
            );
            expect(((guestJoin.body as CallEventBody).payload as any).inviteId).toBe("invite-call-loadnet");
            expect(callVerificationCode(voiceRoomId, aliceKeys.pub, guestKeys.pub)).toMatch(/^[0-9A-F]{4} [0-9A-F]{4} [0-9A-F]{4}$/);

            const lastSeenVoiceSeq = Math.max(
                ...observed
                    .filter((event) => (event.body as any).guildId === voiceGuildId)
                    .map((event) => event.seq)
            );
            monitor.close();
            await sleep(100);

            const offlineAck = await publishCall(bob, voiceGuildId, voiceChannelId, voiceRoomId, bobKeys.pub, {
                kind: "presence",
                name: "Bob",
                avatar: "",
                audioEnabled: false,
                videoEnabled: true,
                screenEnabled: false,
                identityKey: bobKeys.pub,
                reconnectProbe: "offline-presence"
            });
            await waitForLog(storeB, voiceGuildId, offlineAck.seq + 1);

            reconnectedMonitor = new CgpClient({ relays: [urlB], keyPair: createKeyPair(), wireFormat: "binary-v1" });
            await reconnectedMonitor.connect();
            const history = await reconnectedMonitor.getHistory({
                guildId: voiceGuildId,
                channelId: voiceChannelId,
                afterSeq: lastSeenVoiceSeq,
                limit: 10
            });
            expect(history.events.some((event) => {
                const body = event.body as any;
                return body.type === "CALL_EVENT" &&
                    body.roomId === voiceRoomId &&
                    body.payload?.reconnectProbe === "offline-presence" &&
                    body.payload?.identityKey === bobKeys.pub;
            })).toBe(true);

            const reconnectedObserved = observeCallEvents(reconnectedMonitor);
            await reconnectedMonitor.subscribe(voiceGuildId);
            await waitForLog(storeB, voiceGuildId, offlineAck.seq + 1);
            await publishCall(alice, voiceGuildId, voiceChannelId, voiceRoomId, aliceKeys.pub, {
                kind: "offer",
                toUserId: bobKeys.pub,
                description: { type: "offer", sdp: "v=0\r\ns=reconnected-call\r\n" },
                reconnectProbe: "live-after-reconnect"
            });
            await waitForCallEvent(reconnectedObserved, "live call delivery after reconnect", (body) =>
                body.roomId === voiceRoomId &&
                body.payload.kind === "offer" &&
                body.payload.reconnectProbe === "live-after-reconnect"
            );
        } finally {
            owner.close();
            alice.close();
            bob.close();
            guest.close();
            monitor.close();
            reconnectedMonitor?.close();
            await relayA.close();
            await relayB.close();
            pubSub.close();
        }
    }, 20000);
});
