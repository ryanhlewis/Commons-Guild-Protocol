import type { IncomingMessage, ServerResponse } from "http";
import { createHash, randomUUID } from "crypto";
import type { WebSocket } from "ws";
import {
    applyEvent,
    canModerateScope,
    canUseChannelPermission,
    createInitialState,
    hashObject,
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

export interface AppSurfacePolicy {
    /**
     * Namespace for portable app/bot/webhook records. Defaults to org.cgp.apps.
     */
    namespace?: string;
    /**
     * Object type for app manifests. Defaults to app-manifest.
     */
    manifestObjectType?: string;
    /**
     * Object type for slash-style command registrations. Defaults to slash-command.
     */
    commandObjectType?: string;
    /**
     * Object type for webhook registrations. Defaults to webhook.
     */
    webhookObjectType?: string;
    /**
     * Object type for command execution requests. Defaults to command-invocation.
     */
    commandInvocationObjectType?: string;
    /**
     * Object type for command response receipts. Defaults to command-response.
     */
    commandResponseObjectType?: string;
    /**
     * Object type for self-declared agent/bot profile records. Defaults to agent-profile.
     */
    agentProfileObjectType?: string;
    /**
     * Maximum commands allowed inside an app manifest. Defaults to 50.
     */
    maxManifestCommands?: number;
    /**
     * Maximum argument text stored on command invocation objects. Defaults to 4000.
     */
    maxCommandArgumentLength?: number;
    /**
     * Allow users/agents to write their own agent-profile object without app admin permission.
     * Changing someone else's profile still requires member moderation permission.
     */
    allowSelfAgentProfiles?: boolean;
}

export interface AbuseControlPolicy {
    /**
     * Sliding window for duplicate and burst checks. Defaults to 10 seconds.
     */
    windowMs?: number;
    /**
     * Maximum characters in MESSAGE.content. Set 0 to disable. Defaults to 6000.
     */
    maxMessageChars?: number;
    /**
     * Maximum mentions in a single MESSAGE. Set 0 to disable. Defaults to 20.
     */
    maxMentionsPerMessage?: number;
    /**
     * Maximum identical MESSAGE.content publishes per author/channel/window. Set 0 to disable. Defaults to 4.
     */
    duplicateMessagesPerWindow?: number;
    /**
     * Maximum command invocation objects per author/guild/window. Set 0 to disable. Defaults to 20.
     */
    commandInvocationsPerWindow?: number;
}

export interface WebhookIngressPolicy {
    /**
     * Namespace used for portable app/bot/webhook records. Defaults to org.cgp.apps.
     */
    namespace?: string;
    /**
     * Object type for webhook registrations. Defaults to webhook.
     */
    webhookObjectType?: string;
    /**
     * Allow webhook deliveries when the webhook record has no credentialRef. Defaults to false.
     */
    allowUnsignedWebhooks?: boolean;
    /**
     * Maximum accepted JSON request body. Defaults to 256 KiB.
     */
    maxBodyBytes?: number;
    /**
     * Maximum text content length for delivered webhook messages. Defaults to 4000.
     */
    maxContentChars?: number;
}

export interface SafetyReportPolicy {
    /**
     * Namespace used for generic safety report objects. Defaults to org.cgp.safety.
     */
    namespace?: string;
    /**
     * Object type used for reports inside the namespace. Defaults to report.
     */
    objectType?: string;
    /**
     * Require the reporter to be allowed to participate in the guild under the
     * guild's access/posting policy and current ban list. Defaults to true.
     */
    requireParticipantReporter?: boolean;
    /**
     * Optional allow-list for value.category.
     */
    allowedCategories?: string[];
    /**
     * Require either value.category or value.reason. Defaults to true.
     */
    requireReasonOrCategory?: boolean;
}

export interface ProofOfWorkPolicy {
    /**
     * Leading zero bits required in sha256(challenge). Set 0 to disable.
     */
    difficultyBits: number;
    /**
     * Maximum age of proof. Defaults to 5 minutes.
     */
    ttlMs?: number;
    /**
     * Optional event types this policy applies to. Empty means all PUBLISH event types.
     */
    eventTypes?: string[];
    /**
     * Empty or omitted means the policy applies to all guilds.
     */
    guildIds?: GuildId[];
    /**
     * Empty or omitted means the policy applies to all channels in matching guilds.
     */
    channelIds?: ChannelId[];
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
    onGetMembers?: (args: { guildId: string; author?: string; socket?: WebSocket }, ctx: RelayPluginContext) => Promise<SerializableMember[] | undefined>;
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

function countMentions(content: string) {
    const atMentions = content.match(/(^|\s)@[a-zA-Z0-9_.-]{2,64}\b/g)?.length ?? 0;
    const idMentions = content.match(/<@!?[a-zA-Z0-9:_-]{6,}>/g)?.length ?? 0;
    return atMentions + idMentions;
}

function shortContentHash(content: string) {
    return createHash("sha256").update(content.trim().replace(/\s+/g, " ").toLowerCase()).digest("hex").slice(0, 24);
}

export function createAbuseControlPolicyPlugin(policy: AbuseControlPolicy = {}): RelayPlugin {
    const resolved = {
        windowMs: Math.max(1000, Math.floor(policy.windowMs ?? positiveIntegerFromEnv("CGP_RELAY_ABUSE_WINDOW_MS", 10_000))),
        maxMessageChars: Math.max(0, Math.floor(policy.maxMessageChars ?? positiveIntegerFromEnv("CGP_RELAY_MAX_MESSAGE_CHARS", 6000))),
        maxMentionsPerMessage: Math.max(0, Math.floor(policy.maxMentionsPerMessage ?? positiveIntegerFromEnv("CGP_RELAY_MAX_MENTIONS", 20))),
        duplicateMessagesPerWindow: Math.max(0, Math.floor(policy.duplicateMessagesPerWindow ?? positiveIntegerFromEnv("CGP_RELAY_DUPLICATE_MESSAGE_LIMIT", 4))),
        commandInvocationsPerWindow: Math.max(0, Math.floor(policy.commandInvocationsPerWindow ?? positiveIntegerFromEnv("CGP_RELAY_COMMAND_INVOCATION_LIMIT", 20)))
    };
    const duplicateBuckets = new Map<string, RateBucket>();
    const commandBuckets = new Map<string, RateBucket>();

    return {
        name: "cgp.relay.abuse-controls",
        metadata: {
            name: "Reference abuse controls",
            description: "Relay-local anti-spam policy for message length, mention storms, duplicate floods, and command invocation bursts.",
            version: "1",
            policy: { ...resolved }
        },
        onFrame: ({ socket, kind, payload }) => {
            if (kind !== "PUBLISH") {
                return false;
            }

            const publish = payload as { author?: unknown; body?: Record<string, unknown> };
            const author = typeof publish?.author === "string" ? publish.author : "";
            const body = isRecord(publish?.body) ? publish.body : undefined;
            if (!author || !body) {
                return false;
            }

            const guildId = stringField(body, "guildId");
            const channelId = stringField(body, "channelId");
            if (body.type === "MESSAGE") {
                const content = stringField(body, "content");
                if (resolved.maxMessageChars > 0 && content.length > resolved.maxMessageChars) {
                    sendPolicyError(socket, "ABUSE_POLICY_BLOCKED", `Message content exceeds ${resolved.maxMessageChars} characters.`, payload);
                    return true;
                }
                if (resolved.maxMentionsPerMessage > 0 && countMentions(content) > resolved.maxMentionsPerMessage) {
                    sendPolicyError(socket, "ABUSE_POLICY_BLOCKED", `Message exceeds ${resolved.maxMentionsPerMessage} mentions.`, payload);
                    return true;
                }
                if (
                    resolved.duplicateMessagesPerWindow > 0 &&
                    content &&
                    !takeRateToken(
                        duplicateBuckets,
                        `dup:${guildId}:${channelId}:${author}:${shortContentHash(content)}`,
                        resolved.duplicateMessagesPerWindow,
                        resolved.windowMs
                    )
                ) {
                    sendPolicyError(socket, "ABUSE_RATE_LIMITED", "Duplicate message flood detected.", payload);
                    return true;
                }
            }

            if (
                body.type === "APP_OBJECT_UPSERT" &&
                body.namespace === "org.cgp.apps" &&
                body.objectType === "command-invocation" &&
                resolved.commandInvocationsPerWindow > 0 &&
                !takeRateToken(commandBuckets, `command:${guildId}:${author}`, resolved.commandInvocationsPerWindow, resolved.windowMs)
            ) {
                sendPolicyError(socket, "ABUSE_RATE_LIMITED", "Command invocation rate limit exceeded.", payload);
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

function isRecord(value: unknown): value is Record<string, unknown> {
    return !!value && typeof value === "object" && !Array.isArray(value);
}

function stringField(source: Record<string, unknown> | undefined, key: string) {
    const value = source?.[key];
    return typeof value === "string" ? value.trim() : "";
}

function numberField(source: Record<string, unknown> | undefined, key: string) {
    const value = source?.[key];
    return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function booleanField(source: Record<string, unknown> | undefined, key: string) {
    const value = source?.[key];
    return typeof value === "boolean" ? value : undefined;
}

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

export function createProofOfWorkPolicyPlugin(policy: ProofOfWorkPolicy): RelayPlugin {
    const difficultyBits = Math.max(0, Math.floor(policy.difficultyBits));
    const ttlMs = policy.ttlMs ?? 5 * 60_000;
    const eventTypes = new Set((policy.eventTypes ?? []).map((type) => type.trim().toUpperCase()).filter(Boolean));

    return {
        name: "cgp.relay.proof-of-work",
        metadata: {
            name: "Proof-of-work publish gate",
            description: "Optional relay-local anti-Sybil policy for open relays. This is not mandatory CGP protocol state.",
            version: "1",
            policy: {
                difficultyBits,
                ttlMs,
                eventTypes: [...eventTypes],
                guildIds: policy.guildIds,
                channelIds: policy.channelIds
            }
        },
        onFrame: ({ socket, kind, payload }) => {
            if (kind !== "PUBLISH" || difficultyBits <= 0) {
                return false;
            }

            const publish = payload as {
                author?: unknown;
                createdAt?: unknown;
                body?: { type?: unknown; guildId?: unknown; channelId?: unknown };
                proof?: unknown;
            };
            const body = publish?.body;
            const type = typeof body?.type === "string" ? body.type.toUpperCase() : "";
            const guildId = typeof body?.guildId === "string" ? body.guildId : undefined;
            const channelId = typeof body?.channelId === "string" ? body.channelId : undefined;
            if (
                (eventTypes.size > 0 && !eventTypes.has(type)) ||
                !listApplies(policy.guildIds, guildId) ||
                !listApplies(policy.channelIds, channelId)
            ) {
                return false;
            }

            const proof = isRecord(publish.proof) ? publish.proof : undefined;
            const algorithm = stringField(proof, "algorithm") || "sha256-leading-zero-bits-v1";
            const nonce = stringField(proof, "nonce");
            const issuedAt = Number(proof?.issuedAt);
            const proofDifficulty = Math.floor(Number(proof?.difficultyBits ?? difficultyBits));
            const now = Date.now();
            if (
                algorithm !== "sha256-leading-zero-bits-v1" ||
                !nonce ||
                nonce.length > 256 ||
                !Number.isFinite(issuedAt) ||
                Math.abs(now - issuedAt) > ttlMs ||
                proofDifficulty < difficultyBits
            ) {
                sendPolicyError(socket, "PROOF_OF_WORK_REQUIRED", "Valid proof-of-work is required for this publish.", payload);
                return true;
            }

            const challenge = hashObject({
                algorithm,
                difficultyBits: proofDifficulty,
                nonce,
                issuedAt,
                author: publish.author,
                createdAt: publish.createdAt,
                body
            });
            const digest = createHash("sha256").update(challenge).digest();
            if (leadingZeroBits(digest) < difficultyBits) {
                sendPolicyError(socket, "PROOF_OF_WORK_REQUIRED", "Proof-of-work difficulty target was not met.", payload);
                return true;
            }

            return false;
        }
    };
}

function hasReportTarget(target: Record<string, unknown> | undefined) {
    return !!target && ["messageId", "userId", "channelId"].some((key) => stringField(target, key).length > 0);
}

function replayGuildState(history: GuildEvent[]) {
    if (history.length === 0) {
        return null;
    }

    try {
        let state = createInitialState(history[0]);
        for (let index = 1; index < history.length; index++) {
            state = applyEvent(state, history[index]);
        }
        return state;
    } catch {
        return null;
    }
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

function rejectSensitiveAppFields(value: Record<string, unknown> | undefined) {
    if (!value) {
        return undefined;
    }

    const sensitiveKeys = ["secret", "token", "apiKey", "api_key", "password", "privateKey", "private_key"];
    for (const key of sensitiveKeys) {
        if (typeof value[key] === "string" && (value[key] as string).trim()) {
            return `App objects must not store plaintext ${key}; store a credentialRef or relay-local secret instead.`;
        }
    }

    return undefined;
}

function commandNameIsValid(name: string) {
    return /^[a-z0-9][a-z0-9_-]{0,31}$/.test(name);
}

function commandOptionTypeIsValid(value: string) {
    return ["string", "integer", "number", "boolean", "user", "channel", "role", "mentionable"].includes(value);
}

function validateCommandOptionRecord(value: Record<string, unknown>) {
    const name = stringField(value, "name");
    if (!commandNameIsValid(name)) {
        return "Command option name must be 1-32 lowercase letters, numbers, underscores, or hyphens.";
    }

    const type = stringField(value, "type");
    if (!commandOptionTypeIsValid(type)) {
        return "Command option type is invalid.";
    }

    const description = stringField(value, "description");
    if (description.length > 200) {
        return "Command option description must be 200 characters or less.";
    }

    const required = booleanField(value, "required");
    if (required === undefined && "required" in value) {
        return "Command option required must be a boolean when present.";
    }

    const autocomplete = booleanField(value, "autocomplete");
    if (autocomplete === undefined && "autocomplete" in value) {
        return "Command option autocomplete must be a boolean when present.";
    }

    const choices = value.choices;
    if (choices !== undefined) {
        if (!Array.isArray(choices) || choices.length > 25) {
            return "Command option choices must be an array of at most 25 entries.";
        }
        for (const choice of choices) {
            if (!isRecord(choice)) {
                return "Command option choices must be JSON objects.";
            }
            const choiceName = stringField(choice, "name");
            if (!choiceName || choiceName.length > 100) {
                return "Command option choice name is required and must be 100 characters or less.";
            }
            const choiceValue = choice.value;
            if (!["string", "number", "boolean"].includes(typeof choiceValue)) {
                return "Command option choice value must be a string, number, or boolean.";
            }
        }
    }

    return rejectSensitiveAppFields(value);
}

function validateCommandOptions(value: Record<string, unknown>) {
    const options = value.options;
    if (options === undefined) {
        return undefined;
    }
    if (!Array.isArray(options) || options.length > 25) {
        return "Command options must be an array of at most 25 entries.";
    }
    const seen = new Set<string>();
    for (const option of options) {
        if (!isRecord(option)) {
            return "Command options must be JSON objects.";
        }
        const error = validateCommandOptionRecord(option);
        if (error) {
            return error;
        }
        const name = stringField(option, "name");
        if (seen.has(name)) {
            return `Command option ${name} is duplicated.`;
        }
        seen.add(name);
    }
    return undefined;
}

function validateCommandRecord(value: Record<string, unknown> | undefined) {
    if (!value) {
        return "Command records require a JSON value.";
    }

    const name = stringField(value, "name");
    if (!commandNameIsValid(name)) {
        return "Command name must be 1-32 lowercase letters, numbers, underscores, or hyphens.";
    }

    const description = stringField(value, "description");
    if (description.length > 200) {
        return "Command description must be 200 characters or less.";
    }
    if (stringField(value, "endpoint").length > 2048) {
        return "Command endpoint must be 2048 characters or less.";
    }
    if (stringField(value, "credentialRef").length > 256) {
        return "Command credentialRef must be 256 characters or less.";
    }

    const optionError = validateCommandOptions(value);
    if (optionError) {
        return optionError;
    }

    return rejectSensitiveAppFields(value);
}

function validateManifestRecord(value: Record<string, unknown> | undefined, maxCommands: number) {
    if (!value) {
        return "App manifests require a JSON value.";
    }

    const name = stringField(value, "name");
    if (!name || name.length > 80) {
        return "App manifest name is required and must be 80 characters or less.";
    }

    const description = stringField(value, "description");
    if (description.length > 280) {
        return "App manifest description must be 280 characters or less.";
    }

    const homepageUrl = stringField(value, "homepageUrl");
    if (homepageUrl.length > 2048) {
        return "App manifest homepageUrl must be 2048 characters or less.";
    }
    if (stringField(value, "endpoint").length > 2048) {
        return "App manifest endpoint must be 2048 characters or less.";
    }
    if (stringField(value, "credentialRef").length > 256) {
        return "App manifest credentialRef must be 256 characters or less.";
    }

    const bot = booleanField(value, "bot");
    const agent = booleanField(value, "agent");
    if (bot === undefined && "bot" in value) {
        return "App manifest bot must be a boolean when present.";
    }
    if (agent === undefined && "agent" in value) {
        return "App manifest agent must be a boolean when present.";
    }

    const commands = value.commands;
    if (commands !== undefined) {
        if (!Array.isArray(commands)) {
            return "App manifest commands must be an array when present.";
        }
        if (commands.length > maxCommands) {
            return `App manifest cannot register more than ${maxCommands} commands.`;
        }
        for (const command of commands) {
            if (!isRecord(command)) {
                return "App manifest commands must be JSON objects.";
            }
            const commandError = validateCommandRecord(command);
            if (commandError) {
                return commandError;
            }
        }
    }

    return rejectSensitiveAppFields(value);
}

function validateWebhookRecord(value: Record<string, unknown> | undefined) {
    if (!value) {
        return "Webhook records require a JSON value.";
    }

    const name = stringField(value, "name");
    if (!name || name.length > 80) {
        return "Webhook name is required and must be 80 characters or less.";
    }

    const endpoint = stringField(value, "endpoint");
    const credentialRef = stringField(value, "credentialRef");
    if (endpoint.length > 2048) {
        return "Webhook endpoint must be 2048 characters or less.";
    }
    if (credentialRef.length > 256) {
        return "Webhook credentialRef must be 256 characters or less.";
    }

    return rejectSensitiveAppFields(value);
}

function validateCommandInvocationRecord(value: Record<string, unknown> | undefined, maxArgumentLength: number) {
    if (!value) {
        return "Command invocation records require a JSON value.";
    }

    const name = stringField(value, "commandName") || stringField(value, "name");
    if (!commandNameIsValid(name)) {
        return "Command invocation name must be 1-32 lowercase letters, numbers, underscores, or hyphens.";
    }

    const argumentText = stringField(value, "arguments");
    if (argumentText.length > maxArgumentLength) {
        return `Command invocation arguments must be ${maxArgumentLength} characters or less.`;
    }

    const responseMode = stringField(value, "responseMode");
    if (responseMode && responseMode !== "ephemeral" && responseMode !== "public") {
        return "Command invocation responseMode must be ephemeral or public.";
    }

    const options = value.options;
    if (options !== undefined) {
        if (!isRecord(options)) {
            return "Command invocation options must be a JSON object when present.";
        }
        for (const [name, optionValue] of Object.entries(options)) {
            if (!commandNameIsValid(name)) {
                return "Command invocation option names must use command option syntax.";
            }
            if (!["string", "number", "boolean"].includes(typeof optionValue)) {
                return "Command invocation option values must be strings, numbers, or booleans.";
            }
            if (typeof optionValue === "string" && optionValue.length > maxArgumentLength) {
                return `Command invocation option ${name} must be ${maxArgumentLength} characters or less.`;
            }
        }
    }

    return rejectSensitiveAppFields(value);
}

function validateCommandResponseRecord(value: Record<string, unknown> | undefined) {
    if (!value) {
        return "Command response records require a JSON value.";
    }

    const visibility = stringField(value, "visibility") || stringField(value, "responseMode");
    if (visibility && visibility !== "ephemeral" && visibility !== "public") {
        return "Command response visibility must be ephemeral or public.";
    }

    const content = stringField(value, "content");
    if (content.length > 4000) {
        return "Command response content must be 4000 characters or less.";
    }

    return rejectSensitiveAppFields(value);
}

function commandNameFromObjectRecord(record: Record<string, unknown> | undefined) {
    if (!record) return "";
    return (stringField(record, "commandName") || stringField(record, "name")).replace(/^\/+/, "").toLowerCase();
}

function optionValueMatchesType(type: string, value: unknown) {
    if (value === undefined || value === null || value === "") {
        return true;
    }
    switch (type) {
        case "boolean":
            return typeof value === "boolean";
        case "integer":
            return Number.isInteger(value);
        case "number":
            return typeof value === "number" && Number.isFinite(value);
        default:
            return typeof value === "string";
    }
}

function invocationMatchesCommandOptions(invocation: Record<string, unknown> | undefined, commandValue: Record<string, unknown> | undefined) {
    if (!invocation || !commandValue || !Array.isArray(commandValue.options)) {
        return undefined;
    }
    const providedOptions = isRecord(invocation.options) ? invocation.options : {};
    for (const option of commandValue.options) {
        if (!isRecord(option)) {
            continue;
        }
        const name = stringField(option, "name");
        if (!name) {
            continue;
        }
        const value = providedOptions[name];
        const required = booleanField(option, "required") === true;
        if (required && (value === undefined || value === null || value === "")) {
            return `Command option ${name} is required.`;
        }
        const type = stringField(option, "type") || "string";
        if (!optionValueMatchesType(type, value)) {
            return `Command option ${name} must be ${type}.`;
        }
        const choices = Array.isArray(option.choices) ? option.choices : [];
        if (choices.length > 0 && value !== undefined && value !== null && value !== "") {
            const validChoice = choices.some((choice) => isRecord(choice) && choice.value === value);
            if (!validChoice) {
                return `Command option ${name} has an unsupported choice.`;
            }
        }
    }
    return undefined;
}

function commandRegistrationMatches(
    record: { objectType: string; value?: unknown; objectId?: string; target?: unknown },
    commandObjectType: string,
    appId: string,
    commandName: string
) {
    if (record.objectType !== commandObjectType || !isRecord(record.value)) {
        return false;
    }
    const target = isRecord(record.target) ? record.target : undefined;
    const registeredAppId = stringField(record.value, "appId") || stringField(target, "appId");
    const registeredName = commandNameFromObjectRecord(record.value);
    if (appId && registeredAppId && registeredAppId !== appId) {
        return false;
    }
    return registeredName === commandName;
}

function findCommandRegistration(
    state: ReturnType<typeof replayGuildState>,
    commandObjectType: string,
    appId: string,
    commandName: string
) {
    if (!state) return undefined;
    for (const record of state.appObjects.values()) {
        if (commandRegistrationMatches(record, commandObjectType, appId, commandName)) {
            return record;
        }
    }
    return undefined;
}

function findManifestCommandRegistration(
    state: ReturnType<typeof replayGuildState>,
    manifestObjectType: string,
    appId: string,
    commandName: string
) {
    if (!state) return undefined;
    for (const record of state.appObjects.values()) {
        if (record.objectType !== manifestObjectType || !isRecord(record.value)) {
            continue;
        }
        const target = isRecord(record.target) ? record.target : undefined;
        const manifestAppId = stringField(record.value, "appId") || stringField(target, "appId") || record.objectId;
        if (appId && manifestAppId && manifestAppId !== appId) {
            continue;
        }
        const commands = record.value.commands;
        if (!Array.isArray(commands)) {
            continue;
        }
        const command = commands.find((entry): entry is Record<string, unknown> => {
            return isRecord(entry) && commandNameFromObjectRecord(entry) === commandName;
        });
        if (command) {
            return {
                ...record,
                value: command
            };
        }
    }
    return undefined;
}

function memberHasAnyListedRole(state: ReturnType<typeof replayGuildState>, author: string, roleIds: string[]) {
    if (!state || roleIds.length === 0) {
        return true;
    }
    const member = state.members.get(author);
    if (!member) {
        return false;
    }
    return Array.from(member.roles).some((roleId) => roleIds.includes(roleId));
}

function validateAgentProfileRecord(value: Record<string, unknown> | undefined) {
    if (!value) {
        return "Agent profile records require a JSON value.";
    }

    const displayName = stringField(value, "displayName");
    if (displayName.length > 80) {
        return "Agent profile displayName must be 80 characters or less.";
    }

    const description = stringField(value, "description");
    if (description.length > 280) {
        return "Agent profile description must be 280 characters or less.";
    }

    const bot = booleanField(value, "bot");
    const agent = booleanField(value, "agent");
    if (bot === undefined && "bot" in value) {
        return "Agent profile bot must be a boolean when present.";
    }
    if (agent === undefined && "agent" in value) {
        return "Agent profile agent must be a boolean when present.";
    }

    const version = numberField(value, "schemaVersion");
    if (version !== undefined && version < 0) {
        return "Agent profile schemaVersion must be a positive number.";
    }

    return rejectSensitiveAppFields(value);
}

function getTargetUser(body: { target?: unknown; value?: unknown; objectId?: unknown }) {
    const target = isRecord(body.target) ? body.target : undefined;
    const value = isRecord(body.value) ? body.value : undefined;
    return stringField(target, "userId") || stringField(value, "userId") || (typeof body.objectId === "string" ? body.objectId : "");
}

export function createAppSurfacePolicyPlugin(policy: AppSurfacePolicy = {}): RelayPlugin {
    const namespace = policy.namespace?.trim() || "org.cgp.apps";
    const manifestObjectType = policy.manifestObjectType?.trim() || "app-manifest";
    const commandObjectType = policy.commandObjectType?.trim() || "slash-command";
    const webhookObjectType = policy.webhookObjectType?.trim() || "webhook";
    const commandInvocationObjectType = policy.commandInvocationObjectType?.trim() || "command-invocation";
    const commandResponseObjectType = policy.commandResponseObjectType?.trim() || "command-response";
    const agentProfileObjectType = policy.agentProfileObjectType?.trim() || "agent-profile";
    const maxManifestCommands = Math.max(0, Math.floor(policy.maxManifestCommands ?? 50));
    const maxCommandArgumentLength = Math.max(0, Math.floor(policy.maxCommandArgumentLength ?? 4000));
    const allowSelfAgentProfiles = policy.allowSelfAgentProfiles ?? true;

    return {
        name: "cgp.apps.surface-policy",
        metadata: {
            name: "App, bot, and webhook surface policy",
            description: "Relay-local validation and permission policy for portable app-scoped integration records.",
            version: "1",
            policy: {
                namespace,
                manifestObjectType,
                commandObjectType,
                webhookObjectType,
                commandInvocationObjectType,
                commandResponseObjectType,
                agentProfileObjectType,
                maxManifestCommands,
                maxCommandArgumentLength,
                allowSelfAgentProfiles
            }
        },
        onFrame: async ({ socket, kind, payload }, ctx) => {
            if (kind !== "PUBLISH") {
                return false;
            }

            const publish = payload as {
                author?: unknown;
                body?: {
                    type?: unknown;
                    guildId?: unknown;
                    namespace?: unknown;
                    objectType?: unknown;
                    objectId?: unknown;
                    target?: unknown;
                    value?: unknown;
                };
            };
            const body = publish?.body;
            if (
                (body?.type !== "APP_OBJECT_UPSERT" && body?.type !== "APP_OBJECT_DELETE") ||
                body.namespace !== namespace
            ) {
                return false;
            }

            const objectType = typeof body.objectType === "string" ? body.objectType : "";
            const guildId = typeof body.guildId === "string" ? body.guildId : "";
            const author = typeof publish.author === "string" ? publish.author : "";
            if (!guildId || !author || typeof body.objectId !== "string" || !body.objectId.trim()) {
                sendPolicyError(socket, "VALIDATION_FAILED", "App objects require guildId, author, and objectId.", payload);
                return true;
            }

            const history = await ctx.getLog(guildId);
            const state = replayGuildState(history);
            if (!state) {
                return false;
            }

            if (objectType === agentProfileObjectType) {
                const targetUser = getTargetUser(body);
                const isSelfProfile = allowSelfAgentProfiles && (!targetUser || targetUser === author);
                if (!isSelfProfile && !canModerateScope(state, author, "members")) {
                    sendPolicyError(socket, "VALIDATION_FAILED", "Changing another user's agent profile requires member moderation permission.", payload);
                    return true;
                }
                if (body.type === "APP_OBJECT_UPSERT") {
                    const error = validateAgentProfileRecord(isRecord(body.value) ? body.value : undefined);
                    if (error) {
                        sendPolicyError(socket, "VALIDATION_FAILED", error, payload);
                        return true;
                    }
                }
                return false;
            }

            if (objectType === commandInvocationObjectType) {
                if (body.type !== "APP_OBJECT_UPSERT") {
                    sendPolicyError(socket, "VALIDATION_FAILED", "Command invocations cannot be deleted through the invocation surface.", payload);
                    return true;
                }
                const value = isRecord(body.value) ? body.value : undefined;
                const error = validateCommandInvocationRecord(value, maxCommandArgumentLength);
                if (error) {
                    sendPolicyError(socket, "VALIDATION_FAILED", error, payload);
                    return true;
                }

                const target = isRecord(body.target) ? body.target : undefined;
                const channelId = stringField(body as Record<string, unknown>, "channelId") || stringField(target, "channelId");
                if (!channelId || !canUseChannelPermission(state, author, channelId, "sendMessages")) {
                    sendPolicyError(socket, "VALIDATION_FAILED", "Command invocations require send permission in the target channel.", payload);
                    return true;
                }

                const commandName = commandNameFromObjectRecord(value);
                const appId = stringField(value, "appId") || stringField(target, "appId");
                const commandRegistration =
                    findCommandRegistration(state, commandObjectType, appId, commandName) ??
                    findManifestCommandRegistration(state, manifestObjectType, appId, commandName);
                if (!commandRegistration) {
                    sendPolicyError(socket, "VALIDATION_FAILED", `Command /${commandName} is not registered for this guild.`, payload);
                    return true;
                }
                const commandValue = isRecord(commandRegistration.value) ? commandRegistration.value : undefined;
                const optionError = invocationMatchesCommandOptions(value, commandValue);
                if (optionError) {
                    sendPolicyError(socket, "VALIDATION_FAILED", optionError, payload);
                    return true;
                }

                const allowedChannels = Array.isArray(commandValue?.channelIds)
                    ? commandValue.channelIds.filter((entry): entry is string => typeof entry === "string")
                    : [];
                if (allowedChannels.length > 0 && !allowedChannels.includes(channelId)) {
                    sendPolicyError(socket, "VALIDATION_FAILED", `Command /${commandName} is not enabled in this channel.`, payload);
                    return true;
                }
                const allowedRoles = Array.isArray(commandValue?.roleIds)
                    ? commandValue.roleIds.filter((entry): entry is string => typeof entry === "string")
                    : [];
                if (!memberHasAnyListedRole(state, author, allowedRoles)) {
                    sendPolicyError(socket, "VALIDATION_FAILED", `Command /${commandName} is not enabled for this member.`, payload);
                    return true;
                }

                return false;
            }

            if (objectType === commandResponseObjectType) {
                if (body.type === "APP_OBJECT_UPSERT") {
                    const error = validateCommandResponseRecord(isRecord(body.value) ? body.value : undefined);
                    if (error) {
                        sendPolicyError(socket, "VALIDATION_FAILED", error, payload);
                        return true;
                    }
                }
                const target = isRecord(body.target) ? body.target : undefined;
                const channelId = stringField(body as Record<string, unknown>, "channelId") || stringField(target, "channelId");
                if (channelId && !canUseChannelPermission(state, author, channelId, "sendMessages")) {
                    sendPolicyError(socket, "VALIDATION_FAILED", "Command responses require send permission in the target channel.", payload);
                    return true;
                }
                return false;
            }

            const requiredScope: PermissionScope | undefined =
                objectType === manifestObjectType || objectType === commandObjectType
                    ? "apps"
                    : objectType === webhookObjectType
                        ? "webhooks"
                        : undefined;
            if (!requiredScope) {
                return false;
            }

            if (!canModerateScope(state, author, requiredScope)) {
                sendPolicyError(socket, "VALIDATION_FAILED", `User ${author} does not have permission for ${body.type}`, payload);
                return true;
            }

            if (body.type === "APP_OBJECT_DELETE") {
                return false;
            }

            const value = isRecord(body.value) ? body.value : undefined;
            const error =
                objectType === manifestObjectType
                    ? validateManifestRecord(value, maxManifestCommands)
                    : objectType === commandObjectType
                        ? validateCommandRecord(value)
                        : validateWebhookRecord(value);
            if (error) {
                sendPolicyError(socket, "VALIDATION_FAILED", error, payload);
                return true;
            }

            return false;
        }
    };
}

function sendJson(res: ServerResponse, statusCode: number, payload: Record<string, unknown>) {
    res.statusCode = statusCode;
    res.setHeader("content-type", "application/json; charset=utf-8");
    res.end(JSON.stringify(payload));
}

async function readJsonRequestBody(req: IncomingMessage, maxBytes: number) {
    const chunks: Buffer[] = [];
    let total = 0;
    for await (const chunk of req) {
        const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        total += buffer.byteLength;
        if (total > maxBytes) {
            throw new Error(`Request body exceeds ${maxBytes} bytes.`);
        }
        chunks.push(buffer);
    }
    const raw = Buffer.concat(chunks).toString("utf8").trim();
    if (!raw) {
        return {};
    }
    const parsed = JSON.parse(raw);
    if (!isRecord(parsed)) {
        throw new Error("Request body must be a JSON object.");
    }
    return parsed;
}

function bearerOrHeaderToken(req: IncomingMessage) {
    const direct = req.headers["x-cgp-webhook-token"];
    if (typeof direct === "string" && direct.trim()) {
        return direct.trim();
    }
    const auth = req.headers.authorization;
    const match = typeof auth === "string" ? /^Bearer\s+(.+)$/i.exec(auth.trim()) : null;
    return match?.[1]?.trim() ?? "";
}

function secretEnvNameForCredentialRef(credentialRef: string) {
    return `CGP_WEBHOOK_SECRET_${credentialRef.replace(/[^a-zA-Z0-9]+/g, "_").replace(/^_+|_+$/g, "").toUpperCase()}`;
}

function resolveWebhookCredentialSecret(credentialRef: string) {
    if (!credentialRef) {
        return "";
    }
    if (credentialRef.startsWith("env:")) {
        return process.env[credentialRef.slice("env:".length).trim()] ?? "";
    }
    return process.env[secretEnvNameForCredentialRef(credentialRef)] ?? "";
}

function findWebhookRecord(state: ReturnType<typeof replayGuildState>, webhookObjectType: string, webhookId: string) {
    if (!state) return undefined;
    const candidates = new Set([webhookId, webhookId.startsWith("webhook:") ? webhookId.slice("webhook:".length) : `webhook:${webhookId}`]);
    for (const record of state.appObjects.values()) {
        if (record.objectType === webhookObjectType && candidates.has(record.objectId)) {
            return record;
        }
    }
    return undefined;
}

export function createWebhookIngressPlugin(policy: WebhookIngressPolicy = {}): RelayPlugin {
    const namespace = policy.namespace?.trim() || "org.cgp.apps";
    const webhookObjectType = policy.webhookObjectType?.trim() || "webhook";
    const allowUnsignedWebhooks = policy.allowUnsignedWebhooks ?? false;
    const maxBodyBytes = Math.max(1024, Math.floor(policy.maxBodyBytes ?? positiveIntegerFromEnv("CGP_WEBHOOK_MAX_BODY_BYTES", 256 * 1024)));
    const maxContentChars = Math.max(1, Math.floor(policy.maxContentChars ?? positiveIntegerFromEnv("CGP_WEBHOOK_MAX_CONTENT_CHARS", 4000)));

    return {
        name: "cgp.apps.webhook-ingress",
        metadata: {
            name: "Webhook ingress",
            description: "Relay-local HTTP ingress for registered org.cgp.apps webhook objects.",
            version: "1",
            policy: {
                namespace,
                webhookObjectType,
                allowUnsignedWebhooks,
                maxBodyBytes,
                maxContentChars
            }
        },
        onHttp: async ({ req, res, pathSegments }, ctx) => {
            if (pathSegments[0] !== "cgp.apps.webhook-ingress") {
                return false;
            }

            if (req.method === "OPTIONS") {
                res.statusCode = 204;
                res.end();
                return true;
            }
            if (req.method !== "POST") {
                sendJson(res, 405, { ok: false, error: "Webhook ingress requires POST." });
                return true;
            }

            const guildId = pathSegments[1] ?? "";
            const webhookId = pathSegments[2] ?? "";
            if (!guildId || !webhookId) {
                sendJson(res, 400, { ok: false, error: "Webhook route requires guildId and webhookId." });
                return true;
            }

            const history = await ctx.getLog(guildId);
            const state = replayGuildState(history);
            const webhook = findWebhookRecord(state, webhookObjectType, webhookId);
            const value = isRecord(webhook?.value) ? webhook.value : undefined;
            const target = isRecord(webhook?.target) ? webhook.target : undefined;
            if (!webhook || !value || webhook.namespace !== namespace) {
                sendJson(res, 404, { ok: false, error: "Webhook is not registered on this relay." });
                return true;
            }
            if (booleanField(value, "enabled") === false) {
                sendJson(res, 403, { ok: false, error: "Webhook is disabled." });
                return true;
            }

            const credentialRef = stringField(value, "credentialRef");
            const token = bearerOrHeaderToken(req);
            if (credentialRef) {
                const expected = resolveWebhookCredentialSecret(credentialRef);
                if (!expected) {
                    sendJson(res, 403, { ok: false, error: "Webhook credentialRef is not configured on this relay." });
                    return true;
                }
                if (token !== expected) {
                    sendJson(res, 403, { ok: false, error: "Invalid webhook token." });
                    return true;
                }
            } else if (!allowUnsignedWebhooks) {
                sendJson(res, 403, { ok: false, error: "Webhook requires a credentialRef." });
                return true;
            }

            let body: Record<string, unknown>;
            try {
                body = await readJsonRequestBody(req, maxBodyBytes);
            } catch (error) {
                sendJson(res, 400, { ok: false, error: error instanceof Error ? error.message : "Invalid request body." });
                return true;
            }

            const content = stringField(body, "content");
            if (!content) {
                sendJson(res, 400, { ok: false, error: "Webhook content is required." });
                return true;
            }
            if (content.length > maxContentChars) {
                sendJson(res, 400, { ok: false, error: `Webhook content exceeds ${maxContentChars} characters.` });
                return true;
            }

            const channelId = webhook.channelId || stringField(target, "channelId") || stringField(value, "channelId");
            if (!channelId) {
                sendJson(res, 409, { ok: false, error: "Webhook has no target channel." });
                return true;
            }

            const event = await ctx.publishAsRelay(
                {
                    type: "MESSAGE",
                    guildId,
                    channelId,
                    messageId: hashObject({
                        kind: "webhook-delivery",
                        guildId,
                        channelId,
                        webhookId: webhook.objectId,
                        nonce: randomUUID()
                    }),
                    content,
                    external: {
                        kind: "webhook",
                        webhookId: webhook.objectId,
                        username: stringField(body, "username") || stringField(value, "name") || undefined,
                        avatarUrl: stringField(body, "avatarUrl") || stringField(body, "avatar_url") || undefined
                    }
                },
                Date.now()
            );
            if (!event) {
                sendJson(res, 409, { ok: false, error: "Relay could not publish the webhook delivery." });
                return true;
            }

            sendJson(res, 202, { ok: true, guildId, channelId, eventId: event.id, seq: event.seq });
            return true;
        }
    };
}

export function createSafetyReportPlugin(policy: SafetyReportPolicy = {}): RelayPlugin {
    const namespace = policy.namespace?.trim() || "org.cgp.safety";
    const objectType = policy.objectType?.trim() || "report";
    const allowedCategories = Array.from(new Set((policy.allowedCategories ?? []).map((category) => category.trim()).filter(Boolean)));
    const requireParticipantReporter = policy.requireParticipantReporter ?? true;
    const requireReasonOrCategory = policy.requireReasonOrCategory ?? true;

    return {
        name: "cgp.safety.reports",
        metadata: {
            name: "Safety report validation",
            description: "Relay-local policy for validating generic APP_OBJECT safety reports before they are appended.",
            version: "1",
            policy: {
                namespace,
                objectType,
                requireParticipantReporter,
                allowedCategories,
                requireReasonOrCategory
            }
        },
        onFrame: async ({ socket, kind, payload }, ctx) => {
            if (kind !== "PUBLISH") {
                return false;
            }

            const publish = payload as {
                author?: unknown;
                body?: {
                    type?: unknown;
                    guildId?: unknown;
                    namespace?: unknown;
                    objectType?: unknown;
                    objectId?: unknown;
                    target?: unknown;
                    value?: unknown;
                };
            };
            const body = publish?.body;
            if (
                body?.type !== "APP_OBJECT_UPSERT" ||
                body.namespace !== namespace ||
                body.objectType !== objectType
            ) {
                return false;
            }

            const guildId = typeof body.guildId === "string" ? body.guildId : "";
            const author = typeof publish.author === "string" ? publish.author : "";
            const objectId = typeof body.objectId === "string" ? body.objectId.trim() : "";
            const target = isRecord(body.target) ? body.target : undefined;
            const value = isRecord(body.value) ? body.value : undefined;
            const category = stringField(value, "category");
            const reason = stringField(value, "reason");

            if (!guildId || !author || !objectId) {
                sendPolicyError(socket, "VALIDATION_FAILED", "Safety reports require guildId, author, and objectId.", payload);
                return true;
            }

            if (!hasReportTarget(target)) {
                sendPolicyError(socket, "VALIDATION_FAILED", "Safety reports require a target messageId, userId, or channelId.", payload);
                return true;
            }

            if (requireReasonOrCategory && !category && !reason) {
                sendPolicyError(socket, "VALIDATION_FAILED", "Safety reports require a category or reason.", payload);
                return true;
            }

            if (category && allowedCategories.length > 0 && !allowedCategories.includes(category)) {
                sendPolicyError(socket, "VALIDATION_FAILED", `Unsupported safety report category: ${category}`, payload);
                return true;
            }

            if (requireParticipantReporter) {
                const state = replayGuildState(await ctx.getLog(guildId));
                if (!state) {
                    return false;
                }

                const memberRequired = state.access === "private" || state.policies.posting === "members";
                if (state.bans.has(author) || (memberRequired && !state.members.has(author))) {
                    sendPolicyError(socket, "VALIDATION_FAILED", "Reporter is not allowed to participate in this guild.", payload);
                    return true;
                }
            }

            return false;
        }
    };
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

            const state = replayGuildState(history);
            if (!state) {
                return false;
            }

            if (!canModerateScope(state, author, rule.permissionScope)) {
                sendPolicyError(socket, "VALIDATION_FAILED", `User ${author} does not have permission for ${body.type}`, payload);
                return true;
            }

            return false;
        }
    };
}
