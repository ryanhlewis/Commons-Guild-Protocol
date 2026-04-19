import type { IncomingMessage, ServerResponse } from "http";
import type { WebSocket } from "ws";
import {
    applyEvent,
    canModerateScope,
    createInitialState,
    type ChannelId,
    type EventBody,
    type GuildEvent,
    type GuildId,
    type PermissionScope,
    type SerializableMember
} from "@cgp/core";
import type { Store } from "./store";

interface RateBucket {
    windowStartedAt: number;
    count: number;
}

export interface RateLimitPolicy {
    rateWindowMs: number;
    socketPublishesPerWindow: number;
    authorPublishesPerWindow: number;
    guildPublishesPerWindow: number;
}

export interface EncryptionPolicy {
    /**
     * When true, MESSAGE events matching this policy must carry an encrypted payload envelope.
     * The relay only validates envelope shape; it never receives or verifies plaintext keys.
     */
    requireEncryptedMessages?: boolean;
    /**
     * Set false only for relays that require plaintext moderation or indexing. Defaults to true.
     */
    allowEncryptedMessages?: boolean;
    /**
     * Empty or omitted means the policy applies to all guilds.
     */
    guildIds?: GuildId[];
    /**
     * Empty or omitted means the policy applies to all channels in matching guilds.
     */
    channelIds?: ChannelId[];
}

export interface AppObjectPermissionRule {
    namespace: string;
    objectType?: string;
    permissionScope: PermissionScope;
}

export interface AppObjectPermissionPolicy {
    rules: AppObjectPermissionRule[];
}

export interface RelayPluginContext {
    relayPublicKey: string;
    store: Store;
    publishAsRelay: (body: EventBody, createdAt?: number) => Promise<GuildEvent | undefined>;
    broadcast: (guildId: string, event: GuildEvent) => void;
    getLog: (guildId: GuildId) => Promise<GuildEvent[]>;
}

export interface RelayPluginHttpArgs {
    req: IncomingMessage;
    res: ServerResponse;
    rawUrl: string;
    pathname: string;
    pathSegments: string[];
}

export interface PluginInputSchema {
    name: string;
    type: "string" | "number" | "boolean" | "object";
    required: boolean;
    sensitive?: boolean;
    description: string;
    placeholder?: string;
    scope?: "relay" | "client" | "both";
}

export interface PluginMetadata {
    name: string;
    description?: string;
    icon?: string; // URL or base64
    version?: string;
    policy?: Record<string, unknown>;
    clientExtension?: string;
    clientExtensionPluginId?: string;
    clientExtensionAutoEnableInTauri?: boolean;
    clientExtensionDescription?: string;
    clientExtensionUrl?: string;
    clientExtensionManifestUrl?: string;
    clientExtensionRequiresBrowserExtension?: boolean;
    clientExtensionBrowserInstallUrl?: string;
    clientExtensionBrowserInstallLabel?: string;
    clientExtensionBrowserInstallHint?: string;
}

export interface RelayPlugin {
    name: string;
    metadata?: PluginMetadata;
    inputs?: PluginInputSchema[];
    staticDir?: string;

    onInit?: (ctx: RelayPluginContext) => void | Promise<void>;
    onConfig?: (args: { socket: WebSocket; config: any }, ctx: RelayPluginContext) => void | Promise<void>;
    onGetMembers?: (args: { guildId: string; socket?: WebSocket }, ctx: RelayPluginContext) => Promise<SerializableMember[] | undefined>;
    onFrame?: (args: { socket: WebSocket; kind: string; payload: unknown }, ctx: RelayPluginContext) => boolean | Promise<boolean>;
    onHttp?: (args: RelayPluginHttpArgs, ctx: RelayPluginContext) => boolean | Promise<boolean>;
    onEventAppended?: (args: { event: GuildEvent; socket?: WebSocket }, ctx: RelayPluginContext) => void | Promise<void>;
    onClose?: (ctx: RelayPluginContext) => void | Promise<void>;
}

function positiveIntegerFromEnv(name: string, fallback: number) {
    const raw = process.env[name];
    if (!raw) return fallback;
    const parsed = Number(raw);
    return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function takeRateToken(buckets: Map<string, RateBucket>, key: string, limit: number, windowMs: number) {
    if (limit <= 0) return true;

    const now = Date.now();
    const current = buckets.get(key);

    if (!current || now - current.windowStartedAt >= windowMs) {
        buckets.set(key, { windowStartedAt: now, count: 1 });
        pruneRateBuckets(buckets, windowMs, now);
        return true;
    }

    if (current.count >= limit) {
        return false;
    }

    current.count += 1;
    return true;
}

function pruneRateBuckets(buckets: Map<string, RateBucket>, windowMs: number, now = Date.now()) {
    if (buckets.size < 20_000) {
        return;
    }

    for (const [key, bucket] of buckets) {
        if (now - bucket.windowStartedAt >= windowMs) {
            buckets.delete(key);
        }
    }
}

function publishClientEventId(payload: unknown) {
    return typeof (payload as { clientEventId?: unknown })?.clientEventId === "string"
        ? (payload as { clientEventId: string }).clientEventId
        : undefined;
}

function sendRateLimitError(socket: WebSocket, message: string, payload?: unknown) {
    socket.send(JSON.stringify(["ERROR", { code: "RATE_LIMITED", message, clientEventId: publishClientEventId(payload) }]));
}

export function createRateLimitPolicyPlugin(policy: Partial<RateLimitPolicy> = {}): RelayPlugin {
    const resolved: RateLimitPolicy = {
        rateWindowMs: policy.rateWindowMs ?? positiveIntegerFromEnv("CGP_RELAY_RATE_WINDOW_MS", 10_000),
        socketPublishesPerWindow: policy.socketPublishesPerWindow ?? positiveIntegerFromEnv("CGP_RELAY_SOCKET_PUBLISH_LIMIT", 1_500),
        authorPublishesPerWindow: policy.authorPublishesPerWindow ?? positiveIntegerFromEnv("CGP_RELAY_AUTHOR_PUBLISH_LIMIT", 1_500),
        guildPublishesPerWindow: policy.guildPublishesPerWindow ?? positiveIntegerFromEnv("CGP_RELAY_GUILD_PUBLISH_LIMIT", 5_000)
    };
    const buckets = new Map<string, RateBucket>();
    const socketIds = new WeakMap<WebSocket, number>();
    let nextSocketId = 1;

    return {
        name: "cgp.relay.rate-limit",
        metadata: {
            name: "Reference relay rate limiting",
            description: "Default relay-local anti-abuse policy. This is operational policy, not core CGP state.",
            version: "1",
            policy: { ...resolved }
        },
        onFrame: ({ socket, kind, payload }) => {
            if (kind !== "PUBLISH") {
                return false;
            }

            let socketId = socketIds.get(socket);
            if (!socketId) {
                socketId = nextSocketId++;
                socketIds.set(socket, socketId);
            }

            if (!takeRateToken(buckets, `socket:${socketId}`, resolved.socketPublishesPerWindow, resolved.rateWindowMs)) {
                sendRateLimitError(socket, "Socket publish rate limit exceeded", payload);
                return true;
            }

            const publish = payload as { author?: unknown; body?: { guildId?: unknown } };
            const author = typeof publish?.author === "string" ? publish.author : "";
            const guildId = typeof publish?.body?.guildId === "string" ? publish.body.guildId : "";

            if (author && !takeRateToken(buckets, `author:${author}`, resolved.authorPublishesPerWindow, resolved.rateWindowMs)) {
                sendRateLimitError(socket, "Author publish rate limit exceeded", payload);
                return true;
            }

            if (guildId && !takeRateToken(buckets, `guild:${guildId}`, resolved.guildPublishesPerWindow, resolved.rateWindowMs)) {
                sendRateLimitError(socket, "Guild publish rate limit exceeded", payload);
                return true;
            }

            return false;
        }
    };
}

function sendPolicyError(socket: WebSocket, code: string, message: string, payload?: unknown) {
    socket.send(JSON.stringify(["ERROR", { code, message, clientEventId: publishClientEventId(payload) }]));
}

function listApplies<T extends string>(allowed: T[] | undefined, value: T | undefined) {
    return !allowed?.length || (!!value && allowed.includes(value));
}

export function createEncryptionPolicyPlugin(policy: EncryptionPolicy = {}): RelayPlugin {
    const resolved: Required<Pick<EncryptionPolicy, "requireEncryptedMessages" | "allowEncryptedMessages">> &
        Pick<EncryptionPolicy, "guildIds" | "channelIds"> = {
        requireEncryptedMessages: policy.requireEncryptedMessages ?? false,
        allowEncryptedMessages: policy.allowEncryptedMessages ?? true,
        guildIds: policy.guildIds,
        channelIds: policy.channelIds
    };

    return {
        name: "cgp.security.encryption-policy",
        metadata: {
            name: "Encryption payload policy",
            description: "Relay-local policy for accepting or requiring opaque encrypted MESSAGE payloads. It is not a key server.",
            version: "1",
            policy: { ...resolved }
        },
        onFrame: ({ socket, kind, payload }) => {
            if (kind !== "PUBLISH") {
                return false;
            }

            const publish = payload as { body?: { type?: unknown; guildId?: unknown; channelId?: unknown; encrypted?: unknown; iv?: unknown; content?: unknown } };
            const body = publish?.body;
            if (body?.type !== "MESSAGE") {
                return false;
            }

            const guildId = typeof body.guildId === "string" ? body.guildId : undefined;
            const channelId = typeof body.channelId === "string" ? body.channelId : undefined;
            if (!listApplies(resolved.guildIds, guildId) || !listApplies(resolved.channelIds, channelId)) {
                return false;
            }

            const isEncrypted = body.encrypted === true;
            if (isEncrypted && !resolved.allowEncryptedMessages) {
                sendPolicyError(socket, "ENCRYPTED_PAYLOAD_REJECTED", "This relay does not accept encrypted message payloads for this guild/channel.", payload);
                return true;
            }

            if (resolved.requireEncryptedMessages) {
                const hasEnvelope = isEncrypted && typeof body.iv === "string" && body.iv.length > 0 && typeof body.content === "string" && body.content.length > 0;
                if (!hasEnvelope) {
                    sendPolicyError(socket, "ENCRYPTION_REQUIRED", "This relay requires encrypted message payloads for this guild/channel.", payload);
                    return true;
                }
            }

            return false;
        }
    };
}

function appObjectRuleMatches(rule: AppObjectPermissionRule, body: { namespace?: unknown; objectType?: unknown }) {
    return (
        typeof body.namespace === "string" &&
        body.namespace === rule.namespace &&
        (!rule.objectType || (typeof body.objectType === "string" && body.objectType === rule.objectType))
    );
}

export function createAppObjectPermissionPlugin(policy: AppObjectPermissionPolicy): RelayPlugin {
    const rules = policy.rules.filter((rule) => rule.namespace.trim().length > 0);

    return {
        name: "cgp.relay.app-object-permissions",
        metadata: {
            name: "Application object permissions",
            description: "Relay-local permission rules for application-defined APP_OBJECT events.",
            version: "1",
            policy: { rules }
        },
        onFrame: async ({ socket, kind, payload }, ctx) => {
            if (kind !== "PUBLISH" || rules.length === 0) {
                return false;
            }

            const publish = payload as { author?: unknown; body?: { type?: unknown; guildId?: unknown; namespace?: unknown; objectType?: unknown } };
            const body = publish?.body;
            if (body?.type !== "APP_OBJECT_UPSERT" && body?.type !== "APP_OBJECT_DELETE") {
                return false;
            }

            const rule = rules.find((candidate) => appObjectRuleMatches(candidate, body));
            if (!rule) {
                return false;
            }

            const guildId = typeof body.guildId === "string" ? body.guildId : "";
            const author = typeof publish.author === "string" ? publish.author : "";
            const history = guildId ? await ctx.getLog(guildId) : [];
            if (!guildId || !author || history.length === 0) {
                return false;
            }

            let state = createInitialState(history[0]);
            for (let index = 1; index < history.length; index++) {
                state = applyEvent(state, history[index]);
            }

            if (!canModerateScope(state, author, rule.permissionScope)) {
                sendPolicyError(socket, "VALIDATION_FAILED", `User ${author} does not have permission for ${body.type}`, payload);
                return true;
            }

            return false;
        }
    };
}
