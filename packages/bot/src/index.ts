import { randomUUID } from "crypto";
import type { CgpClient } from "@cgp/client";
import { hashObject, type ChannelId, type GuildEvent, type GuildId, type UserId } from "@cgp/core";

export const CGP_APPS_NAMESPACE = "org.cgp.apps";
export const APP_AGENT_PROFILE_OBJECT_TYPE = "agent-profile";
export const APP_MANIFEST_OBJECT_TYPE = "app-manifest";
export const APP_SLASH_COMMAND_OBJECT_TYPE = "slash-command";
export const APP_WEBHOOK_OBJECT_TYPE = "webhook";
export const APP_COMMAND_INVOCATION_OBJECT_TYPE = "command-invocation";
export const APP_COMMAND_RESPONSE_OBJECT_TYPE = "command-response";

export type CgpCommandOptionType = "string" | "integer" | "number" | "boolean" | "user" | "channel" | "role" | "mentionable";

export interface CgpBotCommandOptionChoice {
    name: string;
    value: string | number | boolean;
}

export interface CgpBotCommandOption {
    name: string;
    description?: string;
    type: CgpCommandOptionType;
    required?: boolean;
    autocomplete?: boolean;
    choices?: CgpBotCommandOptionChoice[];
}

type CgpBotClient = Pick<CgpClient, "upsertAppObject" | "sendMessage" | "on"> & {
    off?: (eventName: string | symbol, listener: (...args: any[]) => void) => unknown;
    removeListener?: (eventName: string | symbol, listener: (...args: any[]) => void) => unknown;
};

export interface CgpBotCommandDefinition {
    name: string;
    description?: string;
    usage?: string;
    channelIds?: ChannelId[];
    roleIds?: string[];
    defaultEphemeral?: boolean;
    endpoint?: string;
    credentialRef?: string;
    options?: CgpBotCommandOption[];
    timeoutMs?: number;
    handler?: CgpBotCommandHandler;
}

export interface CgpBotAppRegistration {
    guildId: GuildId;
    appId: string;
    name: string;
    description?: string;
    botUserId?: UserId;
    bot?: boolean;
    agent?: boolean;
    enabled?: boolean;
    endpoint?: string;
    credentialRef?: string;
    commands: CgpBotCommandDefinition[];
    agentProfile?: {
        userId?: UserId;
        displayName?: string;
        username?: string;
        description?: string;
        avatar?: string;
        banner?: string;
        capabilities?: string[];
        bot?: boolean;
        agent?: boolean;
    };
}

export interface CgpCommandInvocation {
    event: GuildEvent;
    guildId: GuildId;
    channelId: ChannelId;
    objectId: string;
    invocationId: string;
    commandName: string;
    arguments: string;
    appId?: string;
    integrationId?: string;
    invokerId?: UserId;
    responseMode: "ephemeral" | "public";
    options: Record<string, unknown>;
    submittedAt?: string;
    value: Record<string, unknown>;
    target: Record<string, unknown>;
}

export interface CgpCommandResponse {
    content: string;
    responseMode?: "ephemeral" | "public";
    ephemeral?: boolean;
    visibleTo?: UserId;
    error?: boolean;
    code?: string;
    external?: Record<string, unknown>;
}

export type CgpBotCommandHandler = (
    invocation: CgpCommandInvocation
) => string | CgpCommandResponse | void | Promise<string | CgpCommandResponse | void>;

export interface CgpBotRuntimeOptions {
    appId: string;
    commands: CgpBotCommandDefinition[];
    onError?: (error: unknown, invocation?: CgpCommandInvocation) => void;
    onCommand?: CgpBotCommandHandler;
    commandTimeoutMs?: number;
    respondOnError?: boolean;
    errorResponseContent?: string | ((error: unknown, invocation: CgpCommandInvocation) => string);
    timeoutResponseContent?: string | ((invocation: CgpCommandInvocation) => string);
}

export interface CgpBotRuntime {
    dispose: () => void;
    publishResponse: (invocation: CgpCommandInvocation, response: string | CgpCommandResponse) => Promise<void>;
}

function asRecord(value: unknown): Record<string, unknown> {
    return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function stringValue(...values: unknown[]) {
    for (const value of values) {
        if (typeof value === "string" && value.trim()) {
            return value.trim();
        }
    }
    return "";
}

function stringList(value: unknown) {
    if (!Array.isArray(value)) {
        return [];
    }
    return value.filter((entry): entry is string => typeof entry === "string" && entry.trim().length > 0);
}

function normalizeOptionName(value: unknown) {
    return stringValue(value)
        .replace(/^[-/]+/, "")
        .toLowerCase()
        .replace(/[^a-z0-9_-]/g, "")
        .slice(0, 32);
}

function objectValue(value: unknown) {
    return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function booleanValue(...values: unknown[]) {
    for (const value of values) {
        if (typeof value === "boolean") {
            return value;
        }
    }
    return undefined;
}

function sanitizeCommandName(value: unknown) {
    return stringValue(value).replace(/^\/+/, "").toLowerCase();
}

function normalizeResponseMode(...values: unknown[]): "ephemeral" | "public" {
    for (const value of values) {
        if (value === "public") {
            return "public";
        }
        if (value === "ephemeral") {
            return "ephemeral";
        }
    }
    return "ephemeral";
}

function commandObjectId(appId: string, commandName: string) {
    return `${APP_SLASH_COMMAND_OBJECT_TYPE}:${appId}:${sanitizeCommandName(commandName)}`;
}

function commandOptionsValue(options: CgpBotCommandOption[] | undefined) {
    return (options ?? [])
        .map((option) => ({
            name: normalizeOptionName(option.name),
            description: option.description,
            type: option.type,
            required: option.required === true,
            autocomplete: option.autocomplete === true,
            choices: Array.isArray(option.choices)
                ? option.choices
                    .filter((choice) => stringValue(choice.name))
                    .map((choice) => ({
                        name: stringValue(choice.name),
                        value: choice.value
                    }))
                : undefined
        }))
        .filter((option) => option.name && option.type);
}

function commandManifestValue(command: CgpBotCommandDefinition, appId: string) {
    const commandName = sanitizeCommandName(command.name);
    return {
        name: commandName,
        description: command.description ?? "",
        usage: command.usage,
        appId,
        channelIds: command.channelIds,
        roleIds: command.roleIds,
        defaultEphemeral: command.defaultEphemeral ?? true,
        endpoint: command.endpoint,
        credentialRef: command.credentialRef,
        options: commandOptionsValue(command.options)
    };
}

function normalizeResponse(response: string | CgpCommandResponse, invocation: CgpCommandInvocation): CgpCommandResponse {
    if (typeof response === "string") {
        return {
            content: response,
            responseMode: invocation.responseMode,
            visibleTo: invocation.invokerId
        };
    }

    const responseMode = response.responseMode ?? (response.ephemeral === false ? "public" : invocation.responseMode);
    return {
        ...response,
        responseMode,
        visibleTo: response.visibleTo ?? invocation.invokerId
    };
}

function optionIsValidForSchema(option: CgpBotCommandOption, value: unknown) {
    if (value === undefined || value === null || value === "") {
        return option.required !== true;
    }

    switch (option.type) {
        case "boolean":
            return typeof value === "boolean";
        case "integer":
            return Number.isInteger(value);
        case "number":
            return typeof value === "number" && Number.isFinite(value);
        default:
            return typeof value === "string" && value.trim().length > 0;
    }
}

function optionValueMatchesChoices(option: CgpBotCommandOption, value: unknown) {
    if (!option.choices?.length || value === undefined || value === null || value === "") {
        return true;
    }
    return option.choices.some((choice) => choice.value === value);
}

function validateInvocationOptions(command: CgpBotCommandDefinition, invocation: CgpCommandInvocation) {
    const options = commandOptionsValue(command.options);
    if (options.length === 0) {
        return "";
    }

    for (const option of options) {
        const value = invocation.options[option.name];
        if (!optionIsValidForSchema(option, value)) {
            return option.required
                ? `Missing required option: ${option.name}.`
                : `Invalid value for option: ${option.name}.`;
        }
        if (!optionValueMatchesChoices(option, value)) {
            return `Unsupported value for option: ${option.name}.`;
        }
    }

    return "";
}

function commandErrorContent(error: unknown, invocation: CgpCommandInvocation, options: CgpBotRuntimeOptions) {
    if (typeof options.errorResponseContent === "function") {
        return options.errorResponseContent(error, invocation);
    }
    if (typeof options.errorResponseContent === "string") {
        return options.errorResponseContent;
    }
    return `/${invocation.commandName} failed.`;
}

function commandTimeoutContent(invocation: CgpCommandInvocation, options: CgpBotRuntimeOptions) {
    if (typeof options.timeoutResponseContent === "function") {
        return options.timeoutResponseContent(invocation);
    }
    if (typeof options.timeoutResponseContent === "string") {
        return options.timeoutResponseContent;
    }
    return `/${invocation.commandName} timed out before it could respond.`;
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number) {
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
        return promise.then((value) => ({ timedOut: false as const, value }));
    }

    return new Promise<{ timedOut: false; value: T } | { timedOut: true }>((resolve, reject) => {
        const timeout = setTimeout(() => resolve({ timedOut: true }), timeoutMs);
        promise
            .then((value) => {
                clearTimeout(timeout);
                resolve({ timedOut: false, value });
            })
            .catch((error) => {
                clearTimeout(timeout);
                reject(error);
            });
    });
}

function parseCommandInvocation(event: GuildEvent): CgpCommandInvocation | null {
    const body = asRecord(event.body);
    if (stringValue(body.type).toUpperCase() !== "APP_OBJECT_UPSERT") {
        return null;
    }
    if (stringValue(body.namespace) !== CGP_APPS_NAMESPACE) {
        return null;
    }
    if (stringValue(body.objectType) !== APP_COMMAND_INVOCATION_OBJECT_TYPE) {
        return null;
    }

    const target = asRecord(body.target);
    const value = asRecord(body.value);
    const guildId = stringValue(body.guildId);
    const channelId = stringValue(body.channelId, target.channelId);
    const objectId = stringValue(body.objectId);
    const commandName = sanitizeCommandName(value.commandName || target.commandName || objectId.split(":").pop());
    if (!guildId || !channelId || !objectId || !commandName) {
        return null;
    }

    return {
        event,
        guildId,
        channelId,
        objectId,
        invocationId: stringValue(value.invocationId, objectId),
        commandName,
        arguments: stringValue(value.arguments, value.args, value.query),
        appId: stringValue(value.appId, target.appId) || undefined,
        integrationId: stringValue(value.integrationId, target.integrationId) || undefined,
        invokerId: stringValue(value.invokerId, target.userId) || undefined,
        responseMode: normalizeResponseMode(value.responseMode, value.visibility),
        options: objectValue(value.options),
        submittedAt: stringValue(value.submittedAt, value.createdAt) || undefined,
        value,
        target
    };
}

export async function registerBotApp(client: CgpBotClient, registration: CgpBotAppRegistration) {
    const commands = registration.commands
        .map((command) => ({ ...command, name: sanitizeCommandName(command.name) }))
        .filter((command) => command.name);

    if (registration.agentProfile) {
        const profile = registration.agentProfile;
        const userId = profile.userId ?? registration.botUserId ?? registration.appId;
        await client.upsertAppObject(registration.guildId, CGP_APPS_NAMESPACE, APP_AGENT_PROFILE_OBJECT_TYPE, userId, {
            target: { userId },
            value: {
                schemaVersion: 1,
                userId,
                displayName: profile.displayName ?? registration.name,
                username: profile.username,
                description: profile.description ?? registration.description,
                avatar: profile.avatar,
                banner: profile.banner,
                capabilities: profile.capabilities,
                bot: profile.bot ?? registration.bot ?? true,
                agent: profile.agent ?? registration.agent ?? false
            }
        });
    }

    await client.upsertAppObject(registration.guildId, CGP_APPS_NAMESPACE, APP_MANIFEST_OBJECT_TYPE, registration.appId, {
        target: { appId: registration.appId, userId: registration.botUserId },
        value: {
            schemaVersion: 1,
            appId: registration.appId,
            name: registration.name,
            description: registration.description,
            enabled: registration.enabled ?? true,
            bot: registration.bot ?? true,
            agent: registration.agent ?? false,
            botUserId: registration.botUserId,
            endpoint: registration.endpoint,
            credentialRef: registration.credentialRef,
            commands: commands.map((command) => commandManifestValue(command, registration.appId))
        }
    });

    for (const command of commands) {
        await client.upsertAppObject(
            registration.guildId,
            CGP_APPS_NAMESPACE,
            APP_SLASH_COMMAND_OBJECT_TYPE,
            commandObjectId(registration.appId, command.name),
            {
                target: { appId: registration.appId },
                value: {
                    schemaVersion: 1,
                    ...commandManifestValue(command, registration.appId),
                    appName: registration.name
                }
            }
        );
    }
}

export function createBotRuntime(client: CgpBotClient, options: CgpBotRuntimeOptions): CgpBotRuntime {
    const commands = new Map(
        options.commands
            .map((command) => ({ ...command, name: sanitizeCommandName(command.name) }))
            .filter((command) => command.name)
            .map((command) => [command.name, command])
    );
    const seenInvocationIds = new Set<string>();

    async function publishResponse(invocation: CgpCommandInvocation, rawResponse: string | CgpCommandResponse) {
        const response = normalizeResponse(rawResponse, invocation);
        const content = response.content.trim();
        if (!content) {
            return;
        }

        const createdAt = new Date().toISOString();
        const responseMode = response.responseMode ?? "ephemeral";
        if (responseMode === "public") {
            await client.sendMessage(invocation.guildId, invocation.channelId, content, {
                kind: "command-response",
                commandName: invocation.commandName,
                invocationId: invocation.invocationId,
                appId: options.appId,
                external: response.external,
                responseMode,
                error: response.error === true || undefined,
                code: response.code
            });
            return;
        }

        const objectId = `${APP_COMMAND_RESPONSE_OBJECT_TYPE}:${hashObject({
            guildId: invocation.guildId,
            channelId: invocation.channelId,
            invocationId: invocation.invocationId,
            appId: options.appId,
            createdAt,
            nonce: randomUUID()
        })}`;

        await client.upsertAppObject(invocation.guildId, CGP_APPS_NAMESPACE, APP_COMMAND_RESPONSE_OBJECT_TYPE, objectId, {
            channelId: invocation.channelId,
            target: {
                channelId: invocation.channelId,
                userId: response.visibleTo,
                appId: options.appId,
                invocationId: invocation.invocationId,
                commandName: invocation.commandName
            },
            value: {
                schemaVersion: 1,
                content,
                appId: options.appId,
                commandName: invocation.commandName,
                invocationId: invocation.invocationId,
                visibility: "ephemeral",
                visibleTo: response.visibleTo,
                error: response.error === true || undefined,
                code: response.code,
                createdAt,
                external: response.external
            }
        });
    }

    async function handleEvent(event: GuildEvent) {
        const invocation = parseCommandInvocation(event);
        if (!invocation) {
            return;
        }
        if (invocation.appId && invocation.appId !== options.appId) {
            return;
        }

        const command = commands.get(invocation.commandName);
        if (!command) {
            return;
        }

        const dedupeId = stringValue(event.id, invocation.invocationId, invocation.objectId);
        if (dedupeId && seenInvocationIds.has(dedupeId)) {
            return;
        }
        if (dedupeId) {
            seenInvocationIds.add(dedupeId);
            if (seenInvocationIds.size > 2000) {
                const oldestInvocationId = seenInvocationIds.values().next().value;
                if (oldestInvocationId) {
                    seenInvocationIds.delete(oldestInvocationId);
                }
            }
        }

        try {
            const optionError = validateInvocationOptions(command, invocation);
            if (optionError) {
                await publishResponse(invocation, {
                    content: optionError,
                    responseMode: "ephemeral",
                    error: true,
                    code: "INVALID_OPTIONS"
                });
                return;
            }

            const handler = command.handler ?? options.onCommand;
            const timeoutMs = Math.max(0, Math.floor(command.timeoutMs ?? options.commandTimeoutMs ?? 8_000));
            const handled = await withTimeout(Promise.resolve(handler?.(invocation)), timeoutMs);
            if (handled.timedOut) {
                await publishResponse(invocation, {
                    content: commandTimeoutContent(invocation, options),
                    responseMode: "ephemeral",
                    error: true,
                    code: "COMMAND_TIMEOUT"
                });
                return;
            }
            const result = handled.value;
            if (result) {
                await publishResponse(invocation, result);
            }
        } catch (error) {
            options.onError?.(error, invocation);
            if (options.respondOnError !== false) {
                await publishResponse(invocation, {
                    content: commandErrorContent(error, invocation, options),
                    responseMode: "ephemeral",
                    error: true,
                    code: "COMMAND_ERROR"
                });
            }
        }
    }

    const listener = (event: GuildEvent) => {
        void handleEvent(event);
    };
    client.on("event", listener);

    return {
        dispose() {
            if (typeof client.off === "function") {
                client.off("event", listener);
                return;
            }
            if (typeof client.removeListener === "function") {
                client.removeListener("event", listener);
            }
        },
        publishResponse
    };
}
