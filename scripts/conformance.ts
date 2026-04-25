import { WebSocket } from "ws";
import { CgpClient } from "@cgp/client";
import { MemoryStore, RelayServer } from "@cgp/relay";
import {
    generatePrivateKey,
    getPublicKey,
    verifyRelayHead
} from "@cgp/core";

function argValue(name: string, fallback?: string) {
    const prefix = `--${name}=`;
    const found = process.argv.find((arg) => arg.startsWith(prefix));
    if (found) return found.slice(prefix.length);
    const index = process.argv.indexOf(`--${name}`);
    return index >= 0 ? process.argv[index + 1] : fallback;
}

function keyPair() {
    const priv = generatePrivateKey();
    const pub = getPublicKey(priv);
    return { priv, pub };
}

async function waitForSocket(url: string) {
    const socket = new WebSocket(url);
    await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error(`Timed out connecting to ${url}`)), 5000);
        socket.once("open", () => {
            clearTimeout(timeout);
            resolve();
        });
        socket.once("error", (err) => {
            clearTimeout(timeout);
            reject(err);
        });
    });
    return socket;
}

async function requestFrame(url: string, request: unknown, expectedKind: string) {
    const socket = await waitForSocket(url);
    return await new Promise<any>((resolve, reject) => {
        const timeout = setTimeout(() => {
            socket.close();
            reject(new Error(`Timed out waiting for ${expectedKind}`));
        }, 5000);
        socket.on("message", (raw) => {
            const frame = JSON.parse(raw.toString());
            if (Array.isArray(frame) && frame[0] === expectedKind) {
                clearTimeout(timeout);
                socket.close();
                resolve(frame[1]);
            }
        });
        socket.send(JSON.stringify(request));
    });
}

async function requestError(url: string, rawFrame: string) {
    const socket = await waitForSocket(url);
    return await new Promise<any>((resolve, reject) => {
        const timeout = setTimeout(() => {
            socket.close();
            reject(new Error("Timed out waiting for ERROR"));
        }, 5000);
        socket.on("message", (raw) => {
            const frame = JSON.parse(raw.toString());
            if (Array.isArray(frame) && frame[0] === "ERROR") {
                clearTimeout(timeout);
                socket.close();
                resolve(frame[1]);
            }
        });
        socket.send(rawFrame);
    });
}

async function main() {
    const port = Number(argValue("port", String(8600 + Math.floor(Math.random() * 500))));
    const providedRelay = argValue("relay");
    const relay = providedRelay
        ? undefined
        : new RelayServer(port, new MemoryStore(), [], {
            enableDefaultPlugins: false,
            instanceId: "conformance-local"
        });
    const relayUrl = providedRelay ?? `ws://localhost:${port}`;
    const keys = keyPair();
    const client = new CgpClient({ relays: [relayUrl], keyPair: keys });

    try {
        const hello = await requestFrame(relayUrl, ["HELLO", { protocol: "cgp/0.1" }], "HELLO_OK");
        if (hello?.protocol !== "cgp/0.1") {
            throw new Error("HELLO_OK missing protocol cgp/0.1");
        }
        if (typeof hello?.relayPublicKey !== "string" || hello.relayPublicKey.length === 0) {
            throw new Error("HELLO_OK missing relayPublicKey");
        }
        if (typeof hello?.relayId !== "string" || hello.relayId.length === 0) {
            throw new Error("HELLO_OK missing relayId");
        }
        const malformed = await requestError(relayUrl, JSON.stringify({ not: "a-frame" }));
        if (malformed?.code !== "INVALID_FRAME") {
            throw new Error("Malformed frame did not produce INVALID_FRAME");
        }

        await client.connect();
        const guildId = await client.createGuild(`CGP Conformance ${Date.now()}`, "temporary conformance guild");
        const channelId = await client.createChannel(guildId, "conformance", "text");
        await client.sendMessage(guildId, channelId, "conformance message");

        const head = await client.getRelayHead(guildId);
        if (!verifyRelayHead(head)) {
            throw new Error("GET_HEAD returned an invalid relay signature");
        }
        if (head.relayPublicKey !== hello.relayPublicKey) {
            throw new Error("GET_HEAD relayPublicKey does not match HELLO_OK");
        }

        const quorum = await client.checkRelayHeadConsistency(guildId);
        if (quorum.conflicts.length > 0 || quorum.invalidHeads.length > 0) {
            throw new Error("Relay head quorum reported conflicts or invalid heads");
        }
        const observedQuorum = await client.checkObservedRelayHeadConsistency(guildId, {
            minValidHeads: 1,
            minCanonicalCount: 1,
            requireNoConflicts: true
        });
        if (!observedQuorum.canonical || observedQuorum.canonical.hash !== head.headHash) {
            throw new Error("Observed relay-head quorum does not match direct GET_HEAD");
        }

        const history = await client.getHistory({ guildId, channelId, limit: 10 });
        if (!history.events.some((event: any) => event.body?.content === "conformance message")) {
            throw new Error("GET_HISTORY did not return the published conformance message");
        }

        const state = await client.getState(guildId, { includeMessages: false, includeAppObjects: false, memberLimit: 10 });
        if (state.guildId !== guildId || state.endSeq < head.headSeq) {
            throw new Error("GET_STATE did not return a state at least as recent as GET_HEAD");
        }

        console.log(JSON.stringify({
            ok: true,
            relayUrl,
            relayId: hello.relayId,
            relayPublicKey: hello.relayPublicKey,
            guildId,
            headSeq: head.headSeq,
            headHash: head.headHash
        }, null, 2));
    } finally {
        client.close();
        await relay?.close().catch(() => undefined);
    }
}

void main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
});
