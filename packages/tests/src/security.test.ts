import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { RelayServer } from "@cgp/relay/src/server";
import { CgpClient } from "@cgp/client/src/client";
import * as secp from "@noble/secp256k1";
import { computeEventId, hashObject, sign } from "@cgp/core";
import { WebSocket, WebSocketServer } from "ws";

describe("CGP Security", () => {
    let relay: RelayServer;
    const PORT = 7451;
    const relayUrl = `ws://localhost:${PORT}`;
    const DB_PATH = "./test-security-db-" + Date.now();

    beforeAll(async () => {
        relay = new RelayServer(PORT, DB_PATH);
    });

    afterAll(async () => {
        relay.close();
        await new Promise(r => setTimeout(r, 100));
        const fs = await import("fs/promises");
        try {
            await fs.rm(DB_PATH, { recursive: true, force: true });
        } catch (e) {
            console.error("Failed to cleanup test DB", e);
        }
    });

    it("relay rejects event with invalid signature", async () => {
        const privKey = secp.utils.randomPrivateKey();
        const pubKey = Buffer.from(secp.getPublicKey(privKey, true)).toString("hex");

        // Manually connect via WS to send bad payload
        const ws = new WebSocket(relayUrl);
        await new Promise<void>(resolve => ws.onopen = () => resolve());

        // Create a valid-looking event but sign it with wrong key or tamper signature
        const body = {
            type: "GUILD_CREATE",
            name: "Hacker Guild",
            timestamp: Date.now()
        };
        const author = pubKey;
        const createdAt = Date.now();

        // Sign with correct key first
        const unsigned = { body, author, createdAt };
        const validSig = await sign(privKey, hashObject(unsigned));

        // Tamper with signature
        const invalidSig = validSig.replace("a", "b"); // Naive tampering

        const payload = {
            body,
            author,
            createdAt,
            signature: invalidSig
        };

        const responsePromise = new Promise<any>(resolve => {
            ws.onmessage = (msg) => {
                const data = JSON.parse(msg.data.toString());
                resolve(data);
            };
        });

        ws.send(JSON.stringify(["PUBLISH", payload]));

        const response = await responsePromise;
        expect(response[0]).toBe("ERROR");
        expect(response[1].code).toBe("INVALID_SIGNATURE");

        ws.close();
    });

    it("client rejects event with invalid signature", async () => {
        // Let's spin up a fake relay that sends bad events.
        const FAKE_PORT = 7452;
        const wss = new WebSocketServer({ port: FAKE_PORT });

        const client = new CgpClient({ relays: [`ws://localhost:${FAKE_PORT}`] });

        const connected = new Promise<void>(resolve => wss.on("connection", (ws) => {
            // Send a bad event immediately
            const privKey = secp.utils.randomPrivateKey();
            const pubKey = Buffer.from(secp.getPublicKey(privKey, true)).toString("hex");

            const body = { type: "GUILD_CREATE", name: "Bad Guild", timestamp: Date.now() };
            const author = pubKey;
            const createdAt = Date.now();
            const unsigned = { body, author, createdAt };
            // Don't sign, just put garbage
            const signature = "deadbeef";

            const fullEvent = {
                id: "",
                seq: 0,
                prevHash: null,
                createdAt,
                author,
                body,
                signature
            };
            fullEvent.id = computeEventId(fullEvent);

            ws.send(JSON.stringify(["EVENT", fullEvent]));
            resolve();
        }));

        await client.connect();
        await connected;

        // Client should NOT emit this event
        let emitted = false;
        client.on("event", () => { emitted = true; });

        await new Promise(r => setTimeout(r, 200));
        expect(emitted).toBe(false);

        wss.close();
    });
});
