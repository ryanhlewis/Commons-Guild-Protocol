
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { RelayServer } from "@cgp/relay/src/server";
import {
    createAbuseControlPolicyPlugin,
    createAppSurfacePolicyPlugin,
    createEncryptionPolicyPlugin,
    createProofOfWorkPolicyPlugin,
    createRateLimitPolicyPlugin,
    createSafetyReportPlugin
} from "@cgp/relay/src/plugins";
import type { RelayPlugin } from "@cgp/relay/src/plugins";
import { hashObject, type GuildEvent } from "@cgp/core";
import { createHash } from "crypto";
import WebSocket from "ws";

function leadingZeroBits(bytes: Buffer) {
    let total = 0;
    for (const byte of bytes) {
        if (byte === 0) {
            total += 8;
            continue;
        }
        for (let bit = 7; bit >= 0; bit--) {
            if (((byte >> bit) & 1) === 0) {
                total += 1;
                continue;
            }
            return total;
        }
    }
    return total;
}

function proofForPublish(payload: { author: string; createdAt: number; body: unknown }, difficultyBits: number) {
    const issuedAt = Date.now();
    for (let counter = 0; counter < 100_000; counter++) {
        const nonce = `nonce-${counter}`;
        const challenge = hashObject({
            algorithm: "sha256-leading-zero-bits-v1",
            difficultyBits,
            nonce,
            issuedAt,
            author: payload.author,
            createdAt: payload.createdAt,
            body: payload.body
        });
        const digest = createHash("sha256").update(challenge).digest();
        if (leadingZeroBits(digest) >= difficultyBits) {
            return {
                algorithm: "sha256-leading-zero-bits-v1",
                difficultyBits,
                nonce,
                issuedAt
            };
        }
    }
    throw new Error("Failed to generate proof in test budget");
}

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

    it("blocks obvious abuse bursts through the abuse-control plugin hook", async () => {
        const plugin = createAbuseControlPolicyPlugin({
            windowMs: 60_000,
            maxMessageChars: 12,
            maxMentionsPerMessage: 1,
            duplicateMessagesPerWindow: 1,
            commandInvocationsPerWindow: 1
        });
        const sent: any[] = [];
        const socket = { send: (message: string) => sent.push(JSON.parse(message)) } as unknown as WebSocket;

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: "user-a",
                body: { type: "MESSAGE", guildId: "guild-a", channelId: "general", content: "same" }
            }
        }, {} as any)).toBe(false);
        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: "user-a",
                body: { type: "MESSAGE", guildId: "guild-a", channelId: "general", content: "same" }
            }
        }, {} as any)).toBe(true);
        expect(sent.at(-1)?.[1].code).toBe("ABUSE_RATE_LIMITED");

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: "user-a",
                body: { type: "MESSAGE", guildId: "guild-a", channelId: "general", content: "@one @two" }
            }
        }, {} as any)).toBe(true);
        expect(sent.at(-1)?.[1].code).toBe("ABUSE_POLICY_BLOCKED");

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: "user-a",
                body: {
                    type: "APP_OBJECT_UPSERT",
                    guildId: "guild-a",
                    namespace: "org.cgp.apps",
                    objectType: "command-invocation",
                    objectId: "command-invocation:1",
                    value: { commandName: "help" }
                }
            }
        }, {} as any)).toBe(false);
        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: "user-a",
                body: {
                    type: "APP_OBJECT_UPSERT",
                    guildId: "guild-a",
                    namespace: "org.cgp.apps",
                    objectType: "command-invocation",
                    objectId: "command-invocation:2",
                    value: { commandName: "help" }
                }
            }
        }, {} as any)).toBe(true);
        expect(sent.at(-1)?.[1].code).toBe("ABUSE_RATE_LIMITED");
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

    it("can require proof-of-work as an optional relay-local anti-Sybil policy", async () => {
        const difficultyBits = 8;
        const plugin = createProofOfWorkPolicyPlugin({ difficultyBits, eventTypes: ["MESSAGE"] });
        const sent: any[] = [];
        const socket = { send: (message: string) => sent.push(JSON.parse(message)) } as unknown as WebSocket;
        const payload = {
            author: "user-a",
            createdAt: Date.now(),
            body: { type: "MESSAGE", guildId: "guild-a", channelId: "general", content: "hello" }
        };

        expect(await plugin.onFrame?.({ socket, kind: "PUBLISH", payload }, {} as any)).toBe(true);
        expect(sent.at(-1)?.[1].code).toBe("PROOF_OF_WORK_REQUIRED");

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                ...payload,
                proof: proofForPublish(payload, difficultyBits)
            }
        }, {} as any)).toBe(false);

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: "user-a",
                createdAt: Date.now(),
                body: { type: "MEMBER_UPDATE", guildId: "guild-a", userId: "user-a", status: "online" }
            }
        }, {} as any)).toBe(false);
    });

    it("validates generic safety report objects without making reports a core event type", async () => {
        const plugin = createSafetyReportPlugin({
            allowedCategories: ["spam", "abuse"]
        });
        const sent: any[] = [];
        const socket = { send: (message: string) => sent.push(JSON.parse(message)) } as unknown as WebSocket;
        const guildId = "guild-safety";
        const reporter = "reporter-key";
        const genesis: GuildEvent = {
            id: "genesis",
            seq: 0,
            prevHash: null,
            createdAt: 1,
            author: reporter,
            signature: "sig",
            body: {
                type: "GUILD_CREATE",
                guildId,
                name: "Safety Test",
                access: "private"
            }
        };
        const ctx = { getLog: async () => [genesis] } as any;

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: reporter,
                body: {
                    type: "APP_OBJECT_UPSERT",
                    guildId,
                    namespace: "org.cgp.safety",
                    objectType: "report",
                    objectId: "report-1",
                    target: { channelId: "general", messageId: "message-1" },
                    value: { category: "spam", reason: "bot flood" }
                }
            }
        }, ctx)).toBe(false);

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: reporter,
                body: {
                    type: "APP_OBJECT_UPSERT",
                    guildId,
                    namespace: "org.cgp.safety",
                    objectType: "report",
                    objectId: "report-2",
                    value: { category: "spam" }
                }
            }
        }, ctx)).toBe(true);
        expect(sent.at(-1)?.[1].code).toBe("VALIDATION_FAILED");

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: "outsider-key",
                body: {
                    type: "APP_OBJECT_UPSERT",
                    guildId,
                    namespace: "org.cgp.safety",
                    objectType: "report",
                    objectId: "report-3",
                    target: { userId: "bad-user" },
                    value: { category: "abuse" }
                }
            }
        }, ctx)).toBe(true);
        expect(sent.at(-1)?.[1].message).toContain("not allowed");
    });

    it("validates portable app, bot, and webhook app objects as relay policy", async () => {
        const plugin = createAppSurfacePolicyPlugin();
        const sent: any[] = [];
        const socket = { send: (message: string) => sent.push(JSON.parse(message)) } as unknown as WebSocket;
        const owner = "owner-key";
        const agent = "agent-key";
        const guildId = "guild-apps";
        const genesis: GuildEvent = {
            id: "genesis",
            seq: 0,
            prevHash: null,
            createdAt: 1,
            author: owner,
            signature: "sig",
            body: {
                type: "GUILD_CREATE",
                guildId,
                name: "Apps Test"
            }
        };
        const ctx = { getLog: async () => [genesis] } as any;

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: agent,
                body: {
                    type: "APP_OBJECT_UPSERT",
                    guildId,
                    namespace: "org.cgp.apps",
                    objectType: "agent-profile",
                    objectId: agent,
                    target: { userId: agent },
                    value: { displayName: "DAUNT [bot]", bot: true, agent: true }
                }
            }
        }, ctx)).toBe(false);

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: agent,
                body: {
                    type: "APP_OBJECT_UPSERT",
                    guildId,
                    namespace: "org.cgp.apps",
                    objectType: "app-manifest",
                    objectId: "agent-app",
                    value: { name: "Agent App", commands: [{ name: "ask", description: "Ask the agent" }] }
                }
            }
        }, ctx)).toBe(true);
        expect(sent.at(-1)?.[1].message).toContain("permission");

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: owner,
                body: {
                    type: "APP_OBJECT_UPSERT",
                    guildId,
                    namespace: "org.cgp.apps",
                    objectType: "app-manifest",
                    objectId: "agent-app",
                    value: { name: "Agent App", commands: [{ name: "Ask", description: "Bad casing" }] }
                }
            }
        }, ctx)).toBe(true);
        expect(sent.at(-1)?.[1].message).toContain("lowercase");

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: owner,
                body: {
                    type: "APP_OBJECT_UPSERT",
                    guildId,
                    namespace: "org.cgp.apps",
                    objectType: "webhook",
                    objectId: "deploy-hook",
                    value: { name: "Deploy Hook", secret: "plain-secret" }
                }
            }
        }, ctx)).toBe(true);
        expect(sent.at(-1)?.[1].message).toContain("credentialRef");

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: owner,
                body: {
                    type: "APP_OBJECT_UPSERT",
                    guildId,
                    namespace: "org.cgp.apps",
                    objectType: "slash-command",
                    objectId: "ask",
                    value: { name: "ask", description: "Ask the bot" }
                }
            }
        }, ctx)).toBe(false);

        const channelEvent: GuildEvent = {
            id: "channel-general",
            seq: 1,
            prevHash: "genesis",
            createdAt: 2,
            author: owner,
            signature: "sig",
            body: {
                type: "CHANNEL_CREATE",
                guildId,
                channelId: "general",
                name: "general",
                kind: "text"
            }
        };
        const slashCommandEvent: GuildEvent = {
            id: "slash-ask",
            seq: 2,
            prevHash: "channel-general",
            createdAt: 3,
            author: owner,
            signature: "sig",
            body: {
                type: "APP_OBJECT_UPSERT",
                guildId,
                namespace: "org.cgp.apps",
                objectType: "slash-command",
                objectId: "slash-command:agent-app:ask",
                target: { appId: "agent-app" },
                value: {
                    appId: "agent-app",
                    name: "ask",
                    description: "Ask the bot",
                    channelIds: ["general"],
                    options: [{ name: "prompt", type: "string", required: true }]
                }
            }
        };
        const ctxWithCommand = { getLog: async () => [genesis, channelEvent, slashCommandEvent] } as any;

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: agent,
                body: {
                    type: "APP_OBJECT_UPSERT",
                    guildId,
                    channelId: "general",
                    namespace: "org.cgp.apps",
                    objectType: "command-invocation",
                    objectId: "command-invocation:ok",
                    target: { channelId: "general", appId: "agent-app" },
                    value: {
                        commandName: "ask",
                        arguments: "status",
                        appId: "agent-app",
                        responseMode: "ephemeral",
                        options: { prompt: "status" }
                    }
                }
            }
        }, ctxWithCommand)).toBe(false);

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: agent,
                body: {
                    type: "APP_OBJECT_UPSERT",
                    guildId,
                    channelId: "general",
                    namespace: "org.cgp.apps",
                    objectType: "command-invocation",
                    objectId: "command-invocation:missing-option",
                    target: { channelId: "general", appId: "agent-app" },
                    value: {
                        commandName: "ask",
                        arguments: "",
                        appId: "agent-app",
                        responseMode: "ephemeral",
                        options: {}
                    }
                }
            }
        }, ctxWithCommand)).toBe(true);
        expect(sent.at(-1)?.[1].message).toContain("Command option prompt is required.");

        expect(await plugin.onFrame?.({
            socket,
            kind: "PUBLISH",
            payload: {
                author: agent,
                body: {
                    type: "APP_OBJECT_UPSERT",
                    guildId,
                    channelId: "general",
                    namespace: "org.cgp.apps",
                    objectType: "command-invocation",
                    objectId: "command-invocation:missing",
                    target: { channelId: "general", appId: "agent-app" },
                    value: { commandName: "unknown", arguments: "", appId: "agent-app" }
                }
            }
        }, ctxWithCommand)).toBe(true);
        expect(sent.at(-1)?.[1].message).toContain("not registered");
    });
});
