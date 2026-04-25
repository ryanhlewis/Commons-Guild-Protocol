import { afterAll, beforeAll, describe, expect, it } from "vitest";
import WebSocket from "ws";
import { CgpClient } from "@cgp/client";
import { MemoryStore, RelayServer } from "@cgp/relay";
import {
    computeEventId,
    generatePrivateKey,
    getPublicKey,
    GuildEvent,
    hashObject,
    sign,
    validateChain
} from "@cgp/core";

function rng(seed: number) {
    let value = seed >>> 0;
    return () => {
        value += 0x6d2b79f5;
        let t = value;
        t = Math.imul(t ^ (t >>> 15), t | 1);
        t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
}

function pick<T>(random: () => number, values: T[]): T {
    return values[Math.floor(random() * values.length)]!;
}

async function openSocket(url: string) {
    const ws = new WebSocket(url);
    await new Promise<void>((resolve, reject) => {
        ws.once("open", resolve);
        ws.once("error", reject);
    });
    return ws;
}

function readFrame(ws: WebSocket, timeoutMs = 750): Promise<any[] | null> {
    return new Promise((resolve) => {
        const timer = setTimeout(() => {
            cleanup();
            resolve(null);
        }, timeoutMs);
        const onMessage = (raw: WebSocket.RawData) => {
            cleanup();
            try {
                resolve(JSON.parse(raw.toString()));
            } catch {
                resolve(null);
            }
        };
        const onClose = () => {
            cleanup();
            resolve(null);
        };
        const cleanup = () => {
            clearTimeout(timer);
            ws.off("message", onMessage);
            ws.off("close", onClose);
        };
        ws.once("message", onMessage);
        ws.once("close", onClose);
    });
}

describe("deterministic relay fuzzing", () => {
    const port = 8196;
    const relayUrl = `ws://localhost:${port}`;
    let relay: RelayServer;
    let store: MemoryStore;

    beforeAll(() => {
        store = new MemoryStore();
        relay = new RelayServer(port, store, [], {
            enableDefaultPlugins: false,
            maxFrameBytes: 2048,
            maxPublishBatchSize: 16
        });
    });

    afterAll(async () => {
        await relay.close();
    });

    it("rejects malformed transport frames without killing the connection or relay", async () => {
        const ws = await openSocket(relayUrl);
        const frames: Array<string | Buffer> = [
            "not json",
            "{}",
            "[]",
            "[42,{}]",
            "[\"\",{}]",
            "[\"UNKNOWN\",{}]",
            "[\"SUB\"]",
            "[\"PUBLISH\"]",
            "[\"PUBLISH_BATCH\"]",
            "[\"GET_HISTORY\",[]]",
            "[\"GET_STATE\",null]",
            "[\"GET_MEMBERS\",\"not-object\"]",
            Buffer.from([0xff, 0x00, 0x81, 0x7b]),
            JSON.stringify(["PUBLISH", { body: { type: "MESSAGE" } }]),
            JSON.stringify(["PUBLISH", { body: { type: "MESSAGE", guildId: "g" }, author: 42 }]),
            JSON.stringify(["PUBLISH_BATCH", { events: "not-array" }]),
            "x".repeat(4096)
        ];

        for (const frame of frames) {
            const next = readFrame(ws);
            ws.send(frame);
            const response = await next;
            expect(response?.[0]).toBe("ERROR");
            expect([
                "INVALID_FRAME",
                "UNKNOWN_FRAME",
                "VALIDATION_FAILED",
                "INVALID_SIGNATURE",
                "PAYLOAD_TOO_LARGE"
            ]).toContain(response?.[1]?.code);
        }

        const hello = readFrame(ws);
        ws.send(JSON.stringify(["HELLO", {}]));
        expect(await hello).toMatchObject(["HELLO_OK", { protocol: "cgp/0.1" }]);
        ws.close();
    });

    it("keeps canonical state unchanged across random invalid publish mutations", async () => {
        const priv = generatePrivateKey();
        const pub = getPublicKey(priv);
        const client = new CgpClient({ relays: [relayUrl], keyPair: { priv, pub } });
        await client.connect();
        const guildId = await client.createGuild("Fuzz Guild");
        const channelId = await client.createChannel(guildId, "fuzz", "text");
        await new Promise((resolve) => setTimeout(resolve, 150));
        const before = await store.getLog(guildId);

        const random = rng(0xc0ffee);
        const ws = await openSocket(relayUrl);
        const candidateTypes = [
            "MESSAGE",
            "EDIT_MESSAGE",
            "DELETE_MESSAGE",
            "REACTION_ADD",
            "ROLE_ASSIGN",
            "CHANNEL_CREATE",
            "BAN_USER",
            "APP_OBJECT_UPSERT",
            "CHECKPOINT"
        ];

        for (let i = 0; i < 200; i += 1) {
            const body: Record<string, unknown> = {
                type: pick(random, candidateTypes),
                guildId,
                channelId: random() > 0.2 ? channelId : undefined,
                messageId: `fuzz-${i}`,
                content: random() > 0.5 ? `payload-${i}` : { nested: ["bad", i] },
                userId: random() > 0.5 ? pub : `user-${i}`,
                roleId: random() > 0.5 ? "admin" : undefined,
                value: random() > 0.5 ? { n: i } : undefined
            };
            const payload = {
                body,
                author: random() > 0.1 ? pub : `bad-author-${i}`,
                createdAt: Date.now() + Math.floor(random() * 1000),
                signature: random() > 0.5 ? "00" : "not-a-signature"
            };
            const next = readFrame(ws);
            ws.send(JSON.stringify(["PUBLISH", payload]));
            const response = await next;
            expect(response?.[0]).toBe("ERROR");
        }

        ws.close();
        client.close();
        const after = await store.getLog(guildId);
        expect(after).toHaveLength(before.length);
        expect(after.map((event) => event.id)).toEqual(before.map((event) => event.id));
    });

    it("rejects randomized hash-chain mutations and ignores transient id/signature fields in event ids", async () => {
        const author = "author";
        const guildId = "guild";
        const random = rng(0x51a7e);
        const events: GuildEvent[] = [];

        for (let seq = 0; seq < 64; seq += 1) {
            const body = seq === 0
                ? { type: "GUILD_CREATE", guildId, name: "Chain Fuzz" }
                : { type: "MESSAGE", guildId, channelId: "general", messageId: `m-${seq}`, content: `m-${seq}` };
            const unsigned = {
                seq,
                prevHash: seq === 0 ? null : events[seq - 1]!.id,
                createdAt: 1000 + seq,
                author,
                body
            };
            events.push({
                ...unsigned,
                id: computeEventId(unsigned),
                signature: `sig-${seq}`
            });
        }

        expect(validateChain(events)).toBe(true);

        for (let i = 0; i < 128; i += 1) {
            const index = 1 + Math.floor(random() * (events.length - 1));
            const mutated = events.map((event) => ({ ...event, body: { ...event.body } }));
            const mutation = pick(random, ["seq", "prevHash", "body", "id"]);
            if (mutation === "seq") mutated[index]!.seq += 1;
            if (mutation === "prevHash") mutated[index]!.prevHash = `bad-${i}`;
            if (mutation === "body") (mutated[index]!.body as any).content = `tampered-${i}`;
            if (mutation === "id") mutated[index]!.id = hashObject({ bad: i });
            expect(validateChain(mutated)).toBe(false);

            const polluted = { ...events[index]!, id: `polluted-${i}`, signature: `changed-${i}` };
            expect(computeEventId(polluted)).toBe(events[index]!.id);
        }
    });
});
