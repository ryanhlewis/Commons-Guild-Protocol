
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { RelayServer } from "@cgp/relay/src/server";
import { createEncryptionPolicyPlugin, createRateLimitPolicyPlugin } from "@cgp/relay/src/plugins";
import type { RelayPlugin } from "@cgp/relay/src/plugins";
import WebSocket from "ws";

describe("Generic Plugin System", () => {
    let relay: RelayServer;
    const PORT = 7449;
    const relayUrl = `ws://localhost:${PORT}`;
    const DB_PATH = "./test-relay-db-plugin-" + Date.now();

    let lastConfig: any = null;
    let lastConfigSocket: WebSocket | undefined = undefined;

    const mockPlugin: RelayPlugin = {
        name: "mock-plugin",
        metadata: {
            name: "Mock Plugin",
            description: "A test plugin",
            version: "1.0.0"
        },
        inputs: [
            { name: "token", type: "string", required: true, description: "Auth token" }
        ],
        onConfig: async ({ socket, config }, ctx) => {
            lastConfig = config;
            lastConfigSocket = socket;
        }
    };

    beforeAll(async () => {
        relay = new RelayServer(PORT, DB_PATH, [mockPlugin]);
    });

    afterAll(async () => {
        await relay.close();
        const fs = await import("fs/promises");
        try {
            await fs.rm(DB_PATH, { recursive: true, force: true });
        } catch { }
    });

    it("announces plugin metadata in HELLO_OK", async () => {
        const ws = new WebSocket(relayUrl);
        await new Promise((resolve) => ws.on("open", resolve));

        const helloPromise = new Promise<any>((resolve) => {
            ws.on("message", (data) => {
                const [kind, payload] = JSON.parse(data.toString());
                if (kind === "HELLO_OK") resolve(payload);
            });
        });

        ws.send(JSON.stringify(["HELLO", { protocol: "cgp/0.1" }]));

        const payload = await helloPromise;
        expect(payload.plugins).toBeDefined();
        const rateLimitPlugin = payload.plugins.find((plugin: any) => plugin.name === "cgp.relay.rate-limit");
        const announcedMock = payload.plugins.find((plugin: any) => plugin.name === "mock-plugin");
        expect(rateLimitPlugin).toBeDefined();
        expect(rateLimitPlugin.metadata.policy.rateWindowMs).toBeGreaterThan(0);
        expect(rateLimitPlugin.metadata.policy.socketPublishesPerWindow).toBeGreaterThan(0);
        expect(announcedMock).toBeDefined();
        expect(announcedMock.metadata.name).toBe("Mock Plugin");
        expect(announcedMock.inputs).toHaveLength(1);
        expect(announcedMock.inputs[0].name).toBe("token");

        ws.close();
    });

    it("handles PLUGIN_CONFIG frame", async () => {
        const ws = new WebSocket(relayUrl);
        await new Promise((resolve) => ws.on("open", resolve));

        // Connect first
        ws.send(JSON.stringify(["HELLO", { protocol: "cgp/0.1" }]));
        await new Promise((r) => setTimeout(r, 50));

        const configPromise = new Promise<any>((resolve) => {
            ws.on("message", (data) => {
                const [kind, payload] = JSON.parse(data.toString());
                if (kind === "PLUGIN_CONFIG_OK") resolve(payload);
            });
        });

        const config = { token: "abc-123" };
        ws.send(JSON.stringify(["PLUGIN_CONFIG", { pluginName: "mock-plugin", config }]));

        const res = await configPromise;
        expect(res.pluginName).toBe("mock-plugin");

        // Verify server side hook
        expect(lastConfig).toEqual(config);
        expect(lastConfigSocket).toBeDefined();
        // expect(lastConfigSocket).toBe(ws); // hard to verify strict equality with WS client wrapper but effectively same connection

        ws.close();
    });

    it("returns error for unknown plugin", async () => {
        const ws = new WebSocket(relayUrl);
        await new Promise((resolve) => ws.on("open", resolve));
        ws.send(JSON.stringify(["HELLO", { protocol: "cgp/0.1" }]));

        const errorPromise = new Promise<any>((resolve) => {
            ws.on("message", (data) => {
                const [kind, payload] = JSON.parse(data.toString());
                if (kind === "ERROR") resolve(payload);
            });
        });

        ws.send(JSON.stringify(["PLUGIN_CONFIG", { pluginName: "ghost-plugin", config: {} }]));

        const err = await errorPromise;
        expect(err.code).toBe("PLUGIN_NOT_FOUND");

        ws.close();
    });

    it("enforces rate limits through the rate-limit plugin hook", async () => {
        const plugin = createRateLimitPolicyPlugin({
            rateWindowMs: 60_000,
            socketPublishesPerWindow: 1,
            authorPublishesPerWindow: 100,
            guildPublishesPerWindow: 100
        });
        const sent: any[] = [];
        const socket = { send: (message: string) => sent.push(JSON.parse(message)) } as unknown as WebSocket;
        const payload = { author: "user-a", body: { guildId: "guild-a" } };

        expect(await plugin.onFrame?.({ socket, kind: "PUBLISH", payload }, {} as any)).toBe(false);
        expect(await plugin.onFrame?.({ socket, kind: "PUBLISH", payload }, {} as any)).toBe(true);
        expect(sent[0][0]).toBe("ERROR");
        expect(sent[0][1].code).toBe("RATE_LIMITED");
    });

    it("can require encrypted message envelopes as an optional policy plugin", async () => {
        const plugin = createEncryptionPolicyPlugin({ requireEncryptedMessages: true });
        const sent: any[] = [];
        const socket = { send: (message: string) => sent.push(JSON.parse(message)) } as unknown as WebSocket;

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: "user-a",
                body: { type: "MESSAGE", guildId: "guild-a", channelId: "general", content: "plaintext" }
            }
        }, {} as any)).toBe(true);

        expect(sent[0][0]).toBe("ERROR");
        expect(sent[0][1].code).toBe("ENCRYPTION_REQUIRED");

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: "user-a",
                body: { type: "MESSAGE", guildId: "guild-a", channelId: "general", content: "ciphertext", encrypted: true, iv: "iv" }
            }
        }, {} as any)).toBe(false);
    });
});
