import { EventEmitter } from "events";
import { describe, expect, it, vi } from "vitest";
import {
    APP_AGENT_PROFILE_OBJECT_TYPE,
    APP_COMMAND_INVOCATION_OBJECT_TYPE,
    APP_COMMAND_RESPONSE_OBJECT_TYPE,
    APP_MANIFEST_OBJECT_TYPE,
    APP_SLASH_COMMAND_OBJECT_TYPE,
    CGP_APPS_NAMESPACE,
    createBotRuntime,
    registerBotApp
} from "@cgp/bot/src/index";
import type { GuildEvent } from "@cgp/core";

class FakeCgpClient extends EventEmitter {
    upserts: any[] = [];
    messages: any[] = [];

    async upsertAppObject(guildId: string, namespace: string, objectType: string, objectId: string, options: any = {}) {
        this.upserts.push({ guildId, namespace, objectType, objectId, options });
    }

    async sendMessage(guildId: string, channelId: string, content: string, external?: any) {
        this.messages.push({ guildId, channelId, content, external });
        return `message:${this.messages.length}`;
    }
}

function commandInvocationEvent(overrides: Record<string, unknown> = {}): GuildEvent {
    return {
        id: `event-${Math.random()}`,
        seq: 1,
        prevHash: null,
        createdAt: Date.now(),
        author: "02" + "1".repeat(64),
        signature: "sig",
        body: {
            type: "APP_OBJECT_UPSERT",
            guildId: "guild-1",
            channelId: "channel-1",
            namespace: CGP_APPS_NAMESPACE,
            objectType: APP_COMMAND_INVOCATION_OBJECT_TYPE,
            objectId: "command-invocation:1",
            target: {
                channelId: "channel-1",
                appId: "app-1",
                commandName: "help"
            },
            value: {
                commandName: "help",
                arguments: "roles",
                appId: "app-1",
                invokerId: "02" + "2".repeat(64),
                responseMode: "ephemeral",
                ...overrides
            }
        }
    } as GuildEvent;
}

describe("@cgp/bot runtime", () => {
    it("registers a manifest, slash commands, and optional agent profile", async () => {
        const client = new FakeCgpClient();

        await registerBotApp(client as any, {
            guildId: "guild-1",
            appId: "app-1",
            name: "Test Bot",
            description: "Command bot",
            botUserId: "02" + "a".repeat(64),
            commands: [
                {
                    name: "/help",
                    description: "Show help",
                    defaultEphemeral: true,
                    options: [
                        {
                            name: "topic",
                            description: "Help topic",
                            type: "string",
                            required: false,
                            choices: [{ name: "Roles", value: "roles" }]
                        }
                    ]
                }
            ],
            agentProfile: {
                username: "testbot",
                capabilities: ["commands"]
            }
        });

        expect(client.upserts.map((entry) => entry.objectType)).toEqual([
            APP_AGENT_PROFILE_OBJECT_TYPE,
            APP_MANIFEST_OBJECT_TYPE,
            APP_SLASH_COMMAND_OBJECT_TYPE
        ]);
        expect(client.upserts[1].options.value.commands[0]).toMatchObject({
            name: "help",
            appId: "app-1",
            defaultEphemeral: true,
            options: [
                expect.objectContaining({
                    name: "topic",
                    type: "string",
                    choices: [{ name: "Roles", value: "roles" }]
                })
            ]
        });
        expect(client.upserts[2].options.value.options[0]).toMatchObject({
            name: "topic",
            type: "string"
        });
        expect(client.upserts[2].objectId).toBe("slash-command:app-1:help");
    });

    it("turns hidden command invocations into ephemeral command-response objects", async () => {
        const client = new FakeCgpClient();
        const handler = vi.fn(async () => "Only visible to you");
        createBotRuntime(client as any, {
            appId: "app-1",
            commands: [{ name: "help", handler }]
        });

        client.emit("event", commandInvocationEvent());
        await new Promise((resolve) => setTimeout(resolve, 0));

        expect(handler).toHaveBeenCalledWith(expect.objectContaining({ commandName: "help", arguments: "roles" }));
        const response = client.upserts.find((entry) => entry.objectType === APP_COMMAND_RESPONSE_OBJECT_TYPE);
        expect(response).toBeDefined();
        expect(response.options.channelId).toBe("channel-1");
        expect(response.options.value).toMatchObject({
            content: "Only visible to you",
            commandName: "help",
            visibility: "ephemeral"
        });
        expect(client.messages).toHaveLength(0);
    });

    it("can publish command responses as normal channel messages", async () => {
        const client = new FakeCgpClient();
        createBotRuntime(client as any, {
            appId: "app-1",
            commands: [
                {
                    name: "announce",
                    handler: () => ({ content: "Public result", responseMode: "public" })
                }
            ]
        });

        client.emit(
            "event",
            commandInvocationEvent({
                commandName: "announce",
                responseMode: "public"
            })
        );
        await new Promise((resolve) => setTimeout(resolve, 0));

        expect(client.messages).toEqual([
            expect.objectContaining({
                guildId: "guild-1",
                channelId: "channel-1",
                content: "Public result",
                external: expect.objectContaining({
                    kind: "command-response",
                    commandName: "announce",
                    responseMode: "public"
                })
            })
        ]);
    });

    it("returns an ephemeral validation response for invalid command options", async () => {
        const client = new FakeCgpClient();
        const handler = vi.fn();
        createBotRuntime(client as any, {
            appId: "app-1",
            commands: [
                {
                    name: "ask",
                    options: [{ name: "prompt", type: "string", required: true }],
                    handler
                }
            ]
        });

        client.emit(
            "event",
            commandInvocationEvent({
                commandName: "ask",
                arguments: "",
                options: {}
            })
        );
        await new Promise((resolve) => setTimeout(resolve, 0));

        expect(handler).not.toHaveBeenCalled();
        const response = client.upserts.find((entry) => entry.objectType === APP_COMMAND_RESPONSE_OBJECT_TYPE);
        expect(response.options.value).toMatchObject({
            content: "Missing required option: prompt.",
            error: true,
            code: "INVALID_OPTIONS"
        });
    });

    it("returns an ephemeral error response when a command throws", async () => {
        const client = new FakeCgpClient();
        createBotRuntime(client as any, {
            appId: "app-1",
            commands: [
                {
                    name: "help",
                    handler: () => {
                        throw new Error("boom");
                    }
                }
            ]
        });

        client.emit("event", commandInvocationEvent());
        await new Promise((resolve) => setTimeout(resolve, 0));

        const response = client.upserts.find((entry) => entry.objectType === APP_COMMAND_RESPONSE_OBJECT_TYPE);
        expect(response.options.value).toMatchObject({
            content: "/help failed.",
            error: true,
            code: "COMMAND_ERROR"
        });
    });

    it("returns an ephemeral timeout response when a command does not finish", async () => {
        const client = new FakeCgpClient();
        createBotRuntime(client as any, {
            appId: "app-1",
            commandTimeoutMs: 1,
            commands: [
                {
                    name: "help",
                    handler: () => new Promise(() => undefined)
                }
            ]
        });

        client.emit("event", commandInvocationEvent());
        await new Promise((resolve) => setTimeout(resolve, 20));

        const response = client.upserts.find((entry) => entry.objectType === APP_COMMAND_RESPONSE_OBJECT_TYPE);
        expect(response.options.value).toMatchObject({
            content: "/help timed out before it could respond.",
            error: true,
            code: "COMMAND_TIMEOUT"
        });
    });
});
