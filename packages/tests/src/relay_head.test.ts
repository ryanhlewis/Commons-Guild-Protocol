import { afterEach, describe, expect, it } from "vitest";
import * as secp from "@noble/secp256k1";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import WebSocket from "ws";
import { CgpClient } from "@cgp/client";
import { LocalRelayPubSubAdapter, MemoryStore, RelayServer, WebSocketPubSubHub, WebSocketRelayPubSubAdapter } from "@cgp/relay";
import {
    RelayHead,
    assertRelayHeadQuorum,
    findRelayHeadConflicts,
    getPublicKey,
    hashObject,
    relayHeadId,
    sign,
    verifyRelayHead
} from "@cgp/core";

function keyPair() {
    const priv = secp.utils.randomPrivateKey();
    const pub = Buffer.from(secp.getPublicKey(priv, true)).toString("hex");
    return { priv, pub };
}

async function waitForFrame(url: string, request: unknown, frameKind: string, subId: string) {
    const socket = new WebSocket(url);
    await new Promise<void>((resolve, reject) => {
        socket.once("open", resolve);
        socket.once("error", reject);
    });

    return await new Promise<any>((resolve, reject) => {
        const timeout = setTimeout(() => {
            socket.close();
            reject(new Error(`Timed out waiting for ${frameKind}`));
        }, 5000);
        socket.on("message", (raw) => {
            const [kind, payload] = JSON.parse(raw.toString());
            if (kind === frameKind && payload?.subId === subId) {
                clearTimeout(timeout);
                socket.close();
                resolve(payload);
            }
            if (kind === "ERROR" && payload?.subId === subId) {
                clearTimeout(timeout);
                socket.close();
                resolve({ error: payload });
            }
        });
        socket.send(JSON.stringify(request));
    });
}

async function fakeHead(
    priv: Uint8Array,
    input: Omit<RelayHead, "relayPublicKey" | "signature" | "protocol" | "observedAt"> & { observedAt?: number }
): Promise<RelayHead> {
    const relayPublicKey = getPublicKey(priv);
    const unsigned = {
        protocol: "cgp/0.1" as const,
        relayPublicKey,
        ...input,
        observedAt: input.observedAt ?? Date.now()
    };
    return {
        ...unsigned,
        signature: await sign(priv, relayHeadId(unsigned))
    };
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 5000) {
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
        if (await predicate()) return;
        await new Promise((resolve) => setTimeout(resolve, 100));
    }
    throw new Error("Timed out waiting for condition");
}

describe("signed relay heads", () => {
    const relays: RelayServer[] = [];
    const clients: CgpClient[] = [];

    afterEach(async () => {
        for (const client of clients.splice(0)) {
            client.close();
        }
        for (const relay of relays.splice(0)) {
            await relay.close().catch(() => undefined);
        }
    });

    it("serves a signed relay head that clients can verify", async () => {
        const port = 8520;
        const relay = new RelayServer(port, new MemoryStore(), [], {
            enableDefaultPlugins: false,
            instanceId: "head-test-relay"
        });
        relays.push(relay);
        const keys = keyPair();
        const client = new CgpClient({ relays: [`ws://localhost:${port}`], keyPair: keys });
        clients.push(client);
        await client.connect();

        const guildId = await client.createGuild("Head Guild");
        const channelId = await client.createChannel(guildId, "heads", "text");
        await client.sendMessage(guildId, channelId, "signed head coverage");

        const head = await client.getRelayHead(guildId);
        expect(head.relayId).toBe("head-test-relay");
        expect(head.guildId).toBe(guildId);
        expect(head.headSeq).toBeGreaterThanOrEqual(2);
        expect(head.headHash).toMatch(/^[0-9a-f]{64}$/);
        expect(verifyRelayHead(head)).toBe(true);

        const quorum = await client.checkRelayHeadConsistency(guildId);
        expect(quorum.validHeads).toHaveLength(1);
        expect(quorum.conflicts).toHaveLength(0);
        expect(quorum.canonical?.hash).toBe(head.headHash);
    });

    it("rejects forged relay heads and detects same-seq equivocation", async () => {
        const privA = secp.utils.randomPrivateKey();
        const privB = secp.utils.randomPrivateKey();
        const guildId = hashObject({ test: "relay-head-conflict" });
        const left = await fakeHead(privA, {
            relayId: "relay-a",
            guildId,
            headSeq: 7,
            headHash: hashObject({ branch: "left" }),
            prevHash: hashObject({ seq: 6 }),
            checkpointSeq: null,
            checkpointHash: null
        });
        const right = await fakeHead(privB, {
            relayId: "relay-b",
            guildId,
            headSeq: 7,
            headHash: hashObject({ branch: "right" }),
            prevHash: hashObject({ seq: 6 }),
            checkpointSeq: null,
            checkpointHash: null
        });
        const forged = { ...left, headHash: right.headHash };

        expect(verifyRelayHead(left)).toBe(true);
        expect(verifyRelayHead(right)).toBe(true);
        expect(verifyRelayHead(forged)).toBe(false);
        expect(findRelayHeadConflicts([left, right])).toMatchObject([
            { guildId, seq: 7, reason: "same-seq-different-hash" }
        ]);
    });

    it("scopes relay-head quorum to the requested guild and supports freshness policy", async () => {
        const privA = secp.utils.randomPrivateKey();
        const privB = secp.utils.randomPrivateKey();
        const guildId = hashObject({ guild: "requested-head-quorum" });
        const otherGuildId = hashObject({ guild: "other-head-quorum" });
        const now = Date.now();
        const requested = await fakeHead(privA, {
            relayId: "relay-requested",
            guildId,
            headSeq: 3,
            headHash: "requested",
            prevHash: "prev",
            checkpointSeq: null,
            checkpointHash: null,
            observedAt: now
        });
        const otherGuild = await fakeHead(privB, {
            relayId: "relay-other",
            guildId: otherGuildId,
            headSeq: 99,
            headHash: "other",
            prevHash: "prev",
            checkpointSeq: null,
            checkpointHash: null,
            observedAt: now
        });

        const quorum = assertRelayHeadQuorum(guildId, [requested, otherGuild], {
            minValidHeads: 1,
            minCanonicalCount: 1,
            nowMs: now,
            maxAgeMs: 1000,
            maxFutureSkewMs: 1000
        });
        expect(quorum.validHeads.map((head) => head.guildId)).toEqual([guildId]);
        expect(quorum.invalidHeads.map((head) => head.guildId)).toEqual([otherGuildId]);
        expect(quorum.canonical?.hash).toBe("requested");

        expect(() => assertRelayHeadQuorum(guildId, [requested], {
            nowMs: now + 2000,
            maxAgeMs: 1000
        })).toThrow(/valid heads/);
    });

    it("only selects peer catch-up sources that match canonical signed-head quorum", async () => {
        const relay = new RelayServer(0, new MemoryStore(), [], {
            enableDefaultPlugins: false,
            peerUrls: ["ws://peer-a", "ws://peer-b"],
            peerCatchupMinCanonicalHeads: 2
        });
        relays.push(relay);

        const keyA = keyPair();
        const keyB = keyPair();
        const guildId = hashObject({ guild: "peer-catchup-quorum" });
        const canonicalA = await fakeHead(keyA.priv, {
            relayId: "peer-a",
            guildId,
            headSeq: 10,
            headHash: "canonical",
            prevHash: "prev",
            checkpointSeq: null,
            checkpointHash: null
        });
        const canonicalB = await fakeHead(keyB.priv, {
            relayId: "peer-b",
            guildId,
            headSeq: 10,
            headHash: "canonical",
            prevHash: "prev",
            checkpointSeq: null,
            checkpointHash: null
        });
        const forkB = await fakeHead(keyB.priv, {
            relayId: "peer-b",
            guildId,
            headSeq: 10,
            headHash: "fork",
            prevHash: "prev",
            checkpointSeq: null,
            checkpointHash: null
        });

        (relay as any).requestPeerRelayHead = async (peerUrl: string) => ({
            peerUrl,
            head: peerUrl.endsWith("peer-a") ? canonicalA : forkB
        });
        await expect((relay as any).peerCanonicalCatchupSources(guildId)).resolves.toEqual([]);

        (relay as any).requestPeerRelayHead = async (peerUrl: string) => ({
            peerUrl,
            head: peerUrl.endsWith("peer-a") ? canonicalA : canonicalB
        });
        const sources = await (relay as any).peerCanonicalCatchupSources(guildId);
        expect(sources.map((source: any) => source.peerUrl).sort()).toEqual(["ws://peer-a", "ws://peer-b"]);
    });

    it("uses ahead peer heads when the exact canonical quorum is only the local prefix", async () => {
        const relay = new RelayServer(0, new MemoryStore(), [], {
            enableDefaultPlugins: false,
            peerUrls: ["ws://peer-a", "ws://peer-b", "ws://peer-c"],
            peerCatchupMinCanonicalHeads: 2
        });
        relays.push(relay);

        const localKey = keyPair();
        const keyA = keyPair();
        const keyB = keyPair();
        const keyC = keyPair();
        const guildId = hashObject({ guild: "peer-catchup-prefix-extension" });
        const lowerHash = hashObject({ branch: "known-prefix" });
        const lowerPrevHash = hashObject({ seq: 9 });
        const aheadHash = hashObject({ branch: "known-prefix-extension" });
        const aheadPrevHash = lowerHash;
        const ownHead = await fakeHead(localKey.priv, {
            relayId: "local",
            guildId,
            headSeq: 10,
            headHash: lowerHash,
            prevHash: lowerPrevHash,
            checkpointSeq: null,
            checkpointHash: null
        });
        const lowerA = await fakeHead(keyA.priv, {
            relayId: "peer-a",
            guildId,
            headSeq: 10,
            headHash: lowerHash,
            prevHash: lowerPrevHash,
            checkpointSeq: null,
            checkpointHash: null
        });
        const lowerB = await fakeHead(keyB.priv, {
            relayId: "peer-b",
            guildId,
            headSeq: 10,
            headHash: lowerHash,
            prevHash: lowerPrevHash,
            checkpointSeq: null,
            checkpointHash: null
        });
        const aheadC = await fakeHead(keyC.priv, {
            relayId: "peer-c",
            guildId,
            headSeq: 20,
            headHash: aheadHash,
            prevHash: aheadPrevHash,
            checkpointSeq: null,
            checkpointHash: null
        });

        (relay as any).signRelayHead = async () => ownHead;
        (relay as any).requestPeerRelayHead = async (peerUrl: string) => ({
            peerUrl,
            head: peerUrl.endsWith("peer-a") ? lowerA : peerUrl.endsWith("peer-b") ? lowerB : aheadC
        });

        const sources = await (relay as any).peerCanonicalCatchupSources(guildId);
        expect(sources.map((source: any) => source.peerUrl)).toEqual(["ws://peer-c"]);
    });

    it("does not leak private guild heads to unsigned readers", async () => {
        const port = 8521;
        const relay = new RelayServer(port, new MemoryStore(), [], {
            enableDefaultPlugins: false,
            instanceId: "private-head-relay"
        });
        relays.push(relay);
        const keys = keyPair();
        const owner = new CgpClient({ relays: [`ws://localhost:${port}`], keyPair: keys });
        clients.push(owner);
        await owner.connect();

        const guildId = await owner.createGuild("Private Head Guild", undefined, "private");
        const ownerHead = await owner.getRelayHead(guildId);
        expect(verifyRelayHead(ownerHead)).toBe(true);

        const response = await waitForFrame(
            `ws://localhost:${port}`,
            ["GET_HEAD", { subId: "unsigned-head", guildId }],
            "RELAY_HEAD",
            "unsigned-head"
        );
        expect(response.error).toMatchObject({
            code: "FORBIDDEN",
            guildId
        });
    });

    it("gossips signed heads between relays and serves observed quorum", async () => {
        const pubSub = new LocalRelayPubSubAdapter();
        const store = new MemoryStore();
        const relayA = new RelayServer(8522, store, [], {
            enableDefaultPlugins: false,
            instanceId: "head-gossip-a",
            pubSubAdapter: pubSub
        });
        const relayB = new RelayServer(8523, store, [], {
            enableDefaultPlugins: false,
            instanceId: "head-gossip-b",
            pubSubAdapter: pubSub
        });
        relays.push(relayA, relayB);
        const keys = keyPair();
        const writer = new CgpClient({ relays: ["ws://localhost:8522"], keyPair: keys });
        const reader = new CgpClient({ relays: ["ws://localhost:8523"], keyPair: keys });
        clients.push(writer, reader);

        await Promise.all([writer.connect(), reader.connect()]);
        const guildId = await writer.createGuild("Head Gossip Guild");
        const channelId = await writer.createChannel(guildId, "heads", "text");
        await reader.subscribe(guildId);
        await writer.sendMessage(guildId, channelId, "gossip this head");

        await waitFor(async () => {
            const heads = await reader.getObservedRelayHeads(guildId);
            return heads.some((head) => head.relayId === "head-gossip-a")
                && heads.some((head) => head.relayId === "head-gossip-b");
        });

        const quorum = await reader.checkObservedRelayHeadConsistency(guildId, {
            minValidHeads: 2,
            minCanonicalCount: 2,
            requireNoConflicts: true
        });
        expect(quorum.validHeads.map((head) => head.relayId).sort()).toEqual(["head-gossip-a", "head-gossip-b"]);
        expect(quorum.conflicts).toHaveLength(0);
        expect(quorum.canonical?.count).toBe(2);
        pubSub.close();
    });

    it("replays retained pubsub log envelopes after a relay restarts", async () => {
        const hubPort = 8524;
        const relayAPort = 8525;
        const relayBPort = 8526;
        const hub = new WebSocketPubSubHub(hubPort, { retainEnvelopesPerTopic: 1000 });
        let adapterA: WebSocketRelayPubSubAdapter | undefined = new WebSocketRelayPubSubAdapter(`ws://localhost:${hubPort}`);
        let adapterB: WebSocketRelayPubSubAdapter | undefined = new WebSocketRelayPubSubAdapter(`ws://localhost:${hubPort}`);
        const storeA = new MemoryStore();
        const storeB = new MemoryStore();
        const relayA = new RelayServer(relayAPort, storeA, [], {
            enableDefaultPlugins: false,
            instanceId: "replay-a",
            pubSubAdapter: adapterA
        });
        let relayB: RelayServer | undefined = new RelayServer(relayBPort, storeB, [], {
            enableDefaultPlugins: false,
            instanceId: "replay-b",
            pubSubAdapter: adapterB
        });
        relays.push(relayA, relayB);

        const keys = keyPair();
        const writer = new CgpClient({ relays: [`ws://localhost:${relayAPort}`], keyPair: keys });
        const reader = new CgpClient({ relays: [`ws://localhost:${relayBPort}`], keyPair: keys });
        clients.push(writer, reader);

        try {
            await Promise.all([writer.connect(), reader.connect()]);
            const guildId = await writer.createGuild("Replay Guild");
            const channelId = await writer.createChannel(guildId, "replay", "text");
            await reader.subscribe(guildId);

            await waitFor(async () => (await storeB.getLastEvent(guildId))?.seq === 1);

            await relayB.close();
            await adapterB.close();
            adapterB = undefined;

            for (let i = 0; i < 8; i += 1) {
                await writer.sendMessage(guildId, channelId, `missed while down ${i}`);
            }

            await waitFor(async () => (await storeA.getLastEvent(guildId))?.seq === 9);
            await adapterA?.drain(5000);
            const relayAHead = await storeA.getLastEvent(guildId);
            expect(relayAHead?.seq).toBe(9);
            expect((await storeB.getLastEvent(guildId))?.seq).toBe(1);

            adapterB = new WebSocketRelayPubSubAdapter(`ws://localhost:${hubPort}`);
            relayB = new RelayServer(relayBPort, storeB, [], {
                enableDefaultPlugins: false,
                instanceId: "replay-b-restarted",
                pubSubAdapter: adapterB
            });
            relays.push(relayB);

            await waitFor(async () => {
                const head = await storeB.getLastEvent(guildId);
                return head?.seq === relayAHead?.seq && head?.id === relayAHead?.id;
            }, 15000);
        } finally {
            await adapterA?.close().catch(() => undefined);
            await adapterB?.close().catch(() => undefined);
            await hub.close().catch(() => undefined);
        }
    }, 15000);

    it("replicates and replays pubsub envelopes over compact binary-v1 wire", async () => {
        const hubPort = 8530;
        const relayAPort = 8531;
        const relayBPort = 8532;
        const hub = new WebSocketPubSubHub(hubPort, { retainEnvelopesPerTopic: 1000, wireFormat: "binary-v1" });
        let adapterA: WebSocketRelayPubSubAdapter | undefined = new WebSocketRelayPubSubAdapter(`ws://localhost:${hubPort}`, { wireFormat: "binary-v1" });
        let adapterB: WebSocketRelayPubSubAdapter | undefined = new WebSocketRelayPubSubAdapter(`ws://localhost:${hubPort}`, { wireFormat: "binary-v1" });
        const storeA = new MemoryStore();
        const storeB = new MemoryStore();
        const relayA = new RelayServer(relayAPort, storeA, [], {
            enableDefaultPlugins: false,
            instanceId: "binary-pubsub-a",
            pubSubAdapter: adapterA,
            wireFormat: "binary-v1"
        });
        let relayB: RelayServer | undefined = new RelayServer(relayBPort, storeB, [], {
            enableDefaultPlugins: false,
            instanceId: "binary-pubsub-b",
            pubSubAdapter: adapterB,
            wireFormat: "binary-v1"
        });
        relays.push(relayA, relayB);

        const keys = keyPair();
        const writer = new CgpClient({ relays: [`ws://localhost:${relayAPort}`], keyPair: keys, wireFormat: "binary-v1" });
        const reader = new CgpClient({ relays: [`ws://localhost:${relayBPort}`], keyPair: keys, wireFormat: "binary-v1" });
        clients.push(writer, reader);

        try {
            await Promise.all([writer.connect(), reader.connect()]);
            const guildId = await writer.createGuild("Binary Pubsub Guild");
            const channelId = await writer.createChannel(guildId, "binary", "text");
            await reader.subscribe(guildId);
            await waitFor(async () => (await storeB.getLastEvent(guildId))?.seq === 1);

            await writer.sendMessage(guildId, channelId, "binary pubsub live delivery");
            await waitFor(async () => (await storeB.getLastEvent(guildId))?.seq === 2);

            await relayB.close();
            await adapterB.close();
            adapterB = undefined;

            await writer.sendMessage(guildId, channelId, "binary pubsub retained delivery");
            await waitFor(async () => (await storeA.getLastEvent(guildId))?.seq === 3);
            await adapterA?.drain(5000);

            adapterB = new WebSocketRelayPubSubAdapter(`ws://localhost:${hubPort}`, { wireFormat: "binary-v1" });
            relayB = new RelayServer(relayBPort, storeB, [], {
                enableDefaultPlugins: false,
                instanceId: "binary-pubsub-b-restarted",
                pubSubAdapter: adapterB,
                wireFormat: "binary-v1"
            });
            relays.push(relayB);

            await waitFor(async () => (await storeB.getLastEvent(guildId))?.seq === 3, 10000);
        } finally {
            await adapterA?.close().catch(() => undefined);
            await adapterB?.close().catch(() => undefined);
            await hub.close().catch(() => undefined);
        }
    }, 15000);

    it("replays disk-retained pubsub log envelopes after the pubsub hub restarts", async () => {
        const hubPort = 8527;
        const relayAPort = 8528;
        const relayBPort = 8529;
        const retainDir = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-pubsub-retain-"));
        let hub: WebSocketPubSubHub | undefined = new WebSocketPubSubHub(hubPort, {
            retainEnvelopesPerTopic: 1000,
            retainDir
        });
        let adapterA: WebSocketRelayPubSubAdapter | undefined = new WebSocketRelayPubSubAdapter(`ws://localhost:${hubPort}`);
        let adapterB: WebSocketRelayPubSubAdapter | undefined = new WebSocketRelayPubSubAdapter(`ws://localhost:${hubPort}`);
        const storeA = new MemoryStore();
        const storeB = new MemoryStore();
        const relayA = new RelayServer(relayAPort, storeA, [], {
            enableDefaultPlugins: false,
            instanceId: "disk-replay-a",
            pubSubAdapter: adapterA
        });
        let relayB: RelayServer | undefined = new RelayServer(relayBPort, storeB, [], {
            enableDefaultPlugins: false,
            instanceId: "disk-replay-b",
            pubSubAdapter: adapterB
        });
        relays.push(relayA, relayB);

        const keys = keyPair();
        const writer = new CgpClient({ relays: [`ws://localhost:${relayAPort}`], keyPair: keys });
        const reader = new CgpClient({ relays: [`ws://localhost:${relayBPort}`], keyPair: keys });
        clients.push(writer, reader);

        try {
            await Promise.all([writer.connect(), reader.connect()]);
            const guildId = await writer.createGuild("Disk Replay Guild");
            const channelId = await writer.createChannel(guildId, "replay", "text");
            await reader.subscribe(guildId);
            await waitFor(async () => (await storeB.getLastEvent(guildId))?.seq === 1);

            await relayB.close();
            await adapterB.close();
            adapterB = undefined;

            for (let i = 0; i < 6; i += 1) {
                await writer.sendMessage(guildId, channelId, `persisted replay ${i}`);
            }
            await waitFor(async () => (await storeA.getLastEvent(guildId))?.seq === 7);
            await adapterA?.drain(5000);
            const relayAHead = await storeA.getLastEvent(guildId);
            expect(relayAHead?.seq).toBeGreaterThan(1);

            await hub.close();
            hub = undefined;
            hub = new WebSocketPubSubHub(hubPort, {
                retainEnvelopesPerTopic: 1000,
                retainDir
            });

            adapterB = new WebSocketRelayPubSubAdapter(`ws://localhost:${hubPort}`);
            relayB = new RelayServer(relayBPort, storeB, [], {
                enableDefaultPlugins: false,
                instanceId: "disk-replay-b-restarted",
                pubSubAdapter: adapterB
            });
            relays.push(relayB);

            await waitFor(async () => {
                const head = await storeB.getLastEvent(guildId);
                return head?.seq === relayAHead?.seq;
            }, 10000);
            expect((await storeB.getLastEvent(guildId))?.id).toBe(relayAHead?.id);
        } finally {
            await adapterA?.close().catch(() => undefined);
            await adapterB?.close().catch(() => undefined);
            await hub?.close().catch(() => undefined);
            await fs.rm(retainDir, { recursive: true, force: true }).catch(() => undefined);
        }
    }, 20000);
});
