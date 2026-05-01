import type { IncomingMessage, ServerResponse } from "http";
import { request as httpRequest } from "http";
import { request as httpsRequest } from "https";
import { spawn } from "node:child_process";
import { createHash, randomUUID } from "crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import type { WebSocket } from "ws";
import webPush from "web-push";
import {
  applyEvent,
  canModerateScope,
  canUseChannelPermission,
  createInitialState,
  hashObject,
  verify,
  type ChannelId,
  type EventBody,
  type GuildEvent,
  type GuildId,
  type GuildState,
  type PermissionScope,
  type SerializableMember,
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
  /**
   * Maximum guardian recovery requests per author/guild/window. Set 0 to disable. Defaults to 3.
   */
  recoveryRequestsPerWindow?: number;
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

export interface RelayPushPolicy {
  /**
   * JSON file used to persist device push registrations across relay restarts.
   * Defaults to CGP_RELAY_PUSH_REGISTRY or ./relay-push-registry.json.
   */
  registryPath?: string;
  /**
   * Public URL clients should reconnect to after tapping a wake notification.
   * Optional because local/dev relays are often discovered out of band.
   */
  publicRelayUrl?: string;
  /**
   * Maximum devices a single account can register with this relay.
   */
  maxDevicesPerAccount?: number;
  /**
   * Maximum guild/channel IDs a single device can subscribe for wake hints.
   */
  maxGuildIdsPerDevice?: number;
  /**
   * Maximum age for signed register/unregister payloads.
   */
  maxSignatureAgeMs?: number;
  /**
   * Timeout for delivery attempts to UnifiedPush/WebPush endpoints.
   */
  deliveryTimeoutMs?: number;
  /**
   * WebPush VAPID settings. Some UnifiedPush distributors require VAPID.
   */
  vapidSubject?: string;
  vapidPublicKey?: string;
  vapidPrivateKey?: string;
  /**
   * Send wake hints for the author's own events too. Defaults false.
   */
  deliverSelfEvents?: boolean;
}

export type MediaStorageProviderKind =
  | "ipfs"
  | "https"
  | "relay-cache"
  | "local"
  | "external";
export type MediaAdultPolicy = "allow" | "deny" | "only";

export interface MediaStorageProvider {
  id: string;
  kind: MediaStorageProviderKind;
  label?: string;
  description?: string;
  endpoint?: string;
  gatewayUrl?: string;
  priority?: number;
  maxBytes?: number;
  acceptsMimeTypes?: string[];
  acceptsTags?: string[];
  requiresTags?: string[];
  rejectsTags?: string[];
  adult?: MediaAdultPolicy;
  encryptedOnly?: boolean;
  lossless?: boolean;
  retention?: "best-effort" | "pinned" | "paid" | "operator-defined";
  mission?: string;
}

export interface MediaRouteRequest {
  guildId?: string;
  channelId?: string;
  type?: string;
  mimeType?: string;
  size?: number;
  tags?: string[];
  adult?: boolean;
  nsfw?: boolean;
  encrypted?: boolean;
  lossless?: boolean;
}

export interface MediaStoragePolicy {
  providers?: MediaStorageProvider[];
  maxAttachmentBytes?: number;
  maxAttachmentsPerMessage?: number;
  maxInlineBytes?: number;
  allowInlineContent?: boolean;
  allowedSchemes?: string[];
  requireKnownProvider?: boolean;
  requireEncryptedMedia?: boolean;
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
  publishAsRelay: (
    body: EventBody,
    createdAt?: number,
  ) => Promise<GuildEvent | undefined>;
  broadcast: (guildId: string, event: GuildEvent) => void;
  getLog: (guildId: GuildId) => Promise<GuildEvent[]>;
  getState?: (guildId: GuildId) => Promise<GuildState | null>;
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
  onConfig?: (
    args: { socket: WebSocket; config: any },
    ctx: RelayPluginContext,
  ) => void | Promise<void>;
  onGetMembers?: (
    args: { guildId: string; author?: string; socket?: WebSocket },
    ctx: RelayPluginContext,
  ) => Promise<SerializableMember[] | undefined>;
  onFrame?: (
    args: { socket: WebSocket; kind: string; payload: unknown },
    ctx: RelayPluginContext,
  ) => boolean | Promise<boolean>;
  onHttp?: (
    args: RelayPluginHttpArgs,
    ctx: RelayPluginContext,
  ) => boolean | Promise<boolean>;
  onEventsAppended?: (
    args: { events: GuildEvent[]; socket?: WebSocket },
    ctx: RelayPluginContext,
  ) => void | Promise<void>;
  onEventAppended?: (
    args: { event: GuildEvent; socket?: WebSocket },
    ctx: RelayPluginContext,
  ) => void | Promise<void>;
  onClose?: (ctx: RelayPluginContext) => void | Promise<void>;
}

export type SandboxedPluginHook =
  | "onInit"
  | "onConfig"
  | "onGetMembers"
  | "onFrame"
  | "onHttp"
  | "onEventsAppended"
  | "onEventAppended"
  | "onClose";

export interface SandboxedCommandPluginOptions {
  name: string;
  command: string;
  args?: string[];
  cwd?: string;
  env?: Record<string, string>;
  hooks?: SandboxedPluginHook[];
  timeoutMs?: number;
  maxStdoutBytes?: number;
  maxStderrBytes?: number;
  maxHttpBodyBytes?: number;
  metadata?: PluginMetadata;
  inputs?: PluginInputSchema[];
  staticDir?: string;
}

const SANDBOX_PROTOCOL = "cgp.relay.sandboxed-plugin.v1";
const DEFAULT_SANDBOX_HOOKS: SandboxedPluginHook[] = [
  "onInit",
  "onConfig",
  "onGetMembers",
  "onFrame",
  "onHttp",
  "onEventsAppended",
  "onEventAppended",
  "onClose",
];

export function createSandboxedCommandPlugin(
  options: SandboxedCommandPluginOptions,
): RelayPlugin {
  const enabledHooks = new Set<SandboxedPluginHook>(
    options.hooks ?? DEFAULT_SANDBOX_HOOKS,
  );
  const plugin: RelayPlugin = {
    name: options.name,
    metadata: options.metadata ?? {
      name: options.name,
      description: "Sandboxed relay plugin executed as an isolated command.",
      version: "1",
    },
    inputs: options.inputs,
    staticDir: options.staticDir,
  };

  if (enabledHooks.has("onInit")) {
    plugin.onInit = async (ctx) => {
      await runSandboxedPluginHook(options, "onInit", ctx, {});
    };
  }

  if (enabledHooks.has("onConfig")) {
    plugin.onConfig = async ({ config }, ctx) => {
      await runSandboxedPluginHook(options, "onConfig", ctx, { config });
    };
  }

  if (enabledHooks.has("onGetMembers")) {
    plugin.onGetMembers = async ({ guildId, author }, ctx) => {
      const result = await runSandboxedPluginHook(
        options,
        "onGetMembers",
        ctx,
        { guildId, author },
      );
      return Array.isArray(result.members)
        ? (result.members as SerializableMember[])
        : undefined;
    };
  }

  if (enabledHooks.has("onFrame")) {
    plugin.onFrame = async ({ socket, kind, payload }, ctx) => {
      const result = await runSandboxedPluginHook(options, "onFrame", ctx, {
        kind,
        payload,
      });
      const handled = booleanField(result, "handled") === true;
      if (handled) {
        sendSandboxedPluginErrorFrame(socket, result, payload);
      }
      return handled;
    };
  }

  if (enabledHooks.has("onHttp")) {
    plugin.onHttp = async (
      { req, res, rawUrl, pathname, pathSegments },
      ctx,
    ) => {
      const body = await readSandboxedHttpBody(
        req,
        options.maxHttpBodyBytes ?? 256 * 1024,
      );
      const result = await runSandboxedPluginHook(options, "onHttp", ctx, {
        method: req.method ?? "GET",
        url: req.url ?? rawUrl,
        rawUrl,
        pathname,
        pathSegments,
        headers: sanitizeHttpHeaders(req.headers),
        body,
      });
      if (booleanField(result, "handled") !== true) {
        return false;
      }
      writeSandboxedHttpResponse(res, result);
      return true;
    };
  }

  if (enabledHooks.has("onEventsAppended")) {
    plugin.onEventsAppended = async ({ events }, ctx) => {
      await runSandboxedPluginHook(options, "onEventsAppended", ctx, {
        events,
      });
    };
  }

  if (enabledHooks.has("onEventAppended")) {
    plugin.onEventAppended = async ({ event }, ctx) => {
      await runSandboxedPluginHook(options, "onEventAppended", ctx, { event });
    };
  }

  if (enabledHooks.has("onClose")) {
    plugin.onClose = async (ctx) => {
      await runSandboxedPluginHook(options, "onClose", ctx, {});
    };
  }

  return plugin;
}

async function runSandboxedPluginHook(
  options: SandboxedCommandPluginOptions,
  hook: SandboxedPluginHook,
  ctx: RelayPluginContext,
  args: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const timeoutMs = Math.max(1, Math.floor(options.timeoutMs ?? 2000));
  const maxStdoutBytes = Math.max(
    1,
    Math.floor(options.maxStdoutBytes ?? 1024 * 1024),
  );
  const maxStderrBytes = Math.max(
    1,
    Math.floor(options.maxStderrBytes ?? 64 * 1024),
  );
  const request = {
    protocol: SANDBOX_PROTOCOL,
    pluginName: options.name,
    hook,
    relayPublicKey: ctx.relayPublicKey,
    args,
  };

  return await new Promise<Record<string, unknown>>((resolve, reject) => {
    const child = spawn(options.command, options.args ?? [], {
      cwd: options.cwd,
      env: sandboxedPluginEnv(options.env),
      windowsHide: true,
      stdio: ["pipe", "pipe", "pipe"],
    });
    let settled = false;
    let stdout = Buffer.alloc(0);
    let stderr = Buffer.alloc(0);

    const finish = (error?: Error, result?: Record<string, unknown>) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.removeAllListeners();
      child.stdout.removeAllListeners();
      child.stderr.removeAllListeners();
      child.stdin.removeAllListeners();
      if (error) {
        reject(error);
        return;
      }
      resolve(result ?? {});
    };

    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      finish(
        new Error(
          `Sandboxed plugin ${options.name} hook ${hook} timed out after ${timeoutMs}ms`,
        ),
      );
    }, timeoutMs);
    timer.unref();

    child.stdout.on("data", (chunk: Buffer) => {
      if (settled) return;
      if (stdout.length + chunk.length > maxStdoutBytes) {
        child.kill("SIGKILL");
        finish(
          new Error(
            `Sandboxed plugin ${options.name} hook ${hook} exceeded stdout limit`,
          ),
        );
        return;
      }
      stdout = Buffer.concat([stdout, chunk]);
    });

    child.stderr.on("data", (chunk: Buffer) => {
      if (settled) return;
      if (stderr.length + chunk.length > maxStderrBytes) {
        child.kill("SIGKILL");
        finish(
          new Error(
            `Sandboxed plugin ${options.name} hook ${hook} exceeded stderr limit`,
          ),
        );
        return;
      }
      stderr = Buffer.concat([stderr, chunk]);
    });

    child.once("error", (error) => {
      finish(error);
    });

    child.once("close", (code, signal) => {
      if (settled) return;
      const stderrText = stderr.toString("utf8").trim();
      if (code !== 0 || signal) {
        const suffix = stderrText ? `: ${stderrText.slice(0, 500)}` : "";
        finish(
          new Error(
            `Sandboxed plugin ${options.name} hook ${hook} failed (${signal ?? code})${suffix}`,
          ),
        );
        return;
      }
      const raw = stdout.toString("utf8").trim();
      if (!raw) {
        finish(undefined, {});
        return;
      }
      try {
        const parsed: unknown = JSON.parse(raw);
        if (!isRecord(parsed)) {
          finish(
            new Error(
              `Sandboxed plugin ${options.name} hook ${hook} returned non-object JSON`,
            ),
          );
          return;
        }
        finish(undefined, parsed);
      } catch (error: any) {
        finish(
          new Error(
            `Sandboxed plugin ${options.name} hook ${hook} returned invalid JSON: ${error?.message ?? String(error)}`,
          ),
        );
      }
    });

    child.stdin.once("error", (error) => {
      finish(error);
    });
    child.stdin.end(JSON.stringify(request));
  });
}

function sandboxedPluginEnv(
  overlay: Record<string, string> | undefined,
): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const key of ["PATH", "Path", "SystemRoot", "COMSPEC", "TEMP", "TMP"]) {
    const value = process.env[key];
    if (value !== undefined) {
      env[key] = value;
    }
  }
  env.NODE_NO_WARNINGS = process.env.NODE_NO_WARNINGS ?? "1";
  for (const [key, value] of Object.entries(overlay ?? {})) {
    env[key] = value;
  }
  return env;
}

async function readSandboxedHttpBody(req: IncomingMessage, maxBytes: number) {
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    total += buffer.length;
    if (total > maxBytes) {
      throw new Error(
        `Sandboxed plugin HTTP request body exceeded ${maxBytes} bytes`,
      );
    }
    chunks.push(buffer);
  }
  return Buffer.concat(chunks).toString("utf8");
}

function sanitizeHttpHeaders(headers: IncomingMessage["headers"]) {
  const result: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    if (typeof value === "string") {
      result[key] = value;
    } else if (Array.isArray(value)) {
      result[key] = value.join(", ");
    }
  }
  return result;
}

function writeSandboxedHttpResponse(
  res: ServerResponse,
  result: Record<string, unknown>,
) {
  const rawStatusCode = result.statusCode;
  res.statusCode =
    typeof rawStatusCode === "number" &&
    Number.isInteger(rawStatusCode) &&
    rawStatusCode >= 100 &&
    rawStatusCode <= 599
      ? rawStatusCode
      : 200;

  const headers = isRecord(result.headers) ? result.headers : {};
  for (const [key, value] of Object.entries(headers)) {
    if (key.toLowerCase() === "content-length") {
      continue;
    }
    if (
      typeof value === "string" ||
      typeof value === "number" ||
      typeof value === "boolean"
    ) {
      res.setHeader(key, String(value));
    }
  }

  const body = result.body;
  if (body === undefined || body === null) {
    res.end();
    return;
  }
  if (typeof body === "string") {
    res.end(body);
    return;
  }
  if (!res.hasHeader("content-type")) {
    res.setHeader("content-type", "application/json; charset=utf-8");
  }
  res.end(JSON.stringify(body));
}

function sendSandboxedPluginErrorFrame(
  socket: WebSocket,
  result: Record<string, unknown>,
  payload: unknown,
) {
  const rawError = isRecord(result.error)
    ? result.error
    : isRecord(result.errorFrame)
      ? result.errorFrame
      : undefined;
  if (!rawError) {
    return;
  }
  socket.send(
    JSON.stringify([
      "ERROR",
      {
        code: stringField(rawError, "code") || "SANDBOX_PLUGIN_REJECTED",
        message:
          stringField(rawError, "message") ||
          "Sandboxed plugin rejected the frame.",
        clientEventId: publishClientEventId(payload),
        batchId: publishBatchId(payload),
      },
    ]),
  );
}

interface RelayPushRegistration {
  version: number;
  provider: string;
  endpointKind: string;
  endpoint: string;
  instance?: string;
  pubKey?: string;
  auth?: string;
  temporary?: boolean;
  deviceId: string;
  relayUrl?: string;
  account: string;
  wakeOnly: boolean;
  includePreviews: boolean;
  includeDirectMessages: boolean;
  requireRelayHead: boolean;
  quorumPolicy?: Record<string, unknown>;
  guildIds: string[];
  author: string;
  createdAt: number;
  signature: string;
  registeredAt: number;
  lastDeliveryAt?: number;
  lastDeliveryError?: string;
}

interface RelayPushRegistry {
  version: 1;
  registrations: RelayPushRegistration[];
}

const RELAY_PUSH_PROTOCOL = "hollow.relay-push/1";
const RELAY_PUSH_PLUGIN_NAME = "cgp.relay.push";
const RELAY_PUSH_WAKE_TYPES = new Set([
  "MESSAGE",
  "CALL_INVITE",
  "CALL_JOIN",
  "CALL_EVENT",
]);

export function createRelayPushPlugin(
  policy: RelayPushPolicy = {},
): RelayPlugin {
  const registryPath =
    policy.registryPath?.trim() ||
    process.env.CGP_RELAY_PUSH_REGISTRY?.trim() ||
    "./relay-push-registry.json";
  const publicRelayUrl =
    policy.publicRelayUrl?.trim() || process.env.CGP_RELAY_PUBLIC_URL?.trim();
  const maxDevicesPerAccount = Math.max(
    1,
    Math.floor(
      policy.maxDevicesPerAccount ??
        positiveIntegerFromEnv("CGP_RELAY_PUSH_MAX_DEVICES", 12),
    ),
  );
  const maxGuildIdsPerDevice = Math.max(
    1,
    Math.floor(
      policy.maxGuildIdsPerDevice ??
        positiveIntegerFromEnv("CGP_RELAY_PUSH_MAX_GUILDS", 512),
    ),
  );
  const maxSignatureAgeMs = Math.max(
    5_000,
    Math.floor(
      policy.maxSignatureAgeMs ??
        positiveIntegerFromEnv("CGP_RELAY_PUSH_SIGNATURE_TTL_MS", 15 * 60_000),
    ),
  );
  const deliveryTimeoutMs = Math.max(
    1_000,
    Math.floor(
      policy.deliveryTimeoutMs ??
        positiveIntegerFromEnv("CGP_RELAY_PUSH_TIMEOUT_MS", 8_000),
    ),
  );
  const vapidSubject =
    policy.vapidSubject?.trim() ||
    process.env.CGP_RELAY_PUSH_VAPID_SUBJECT?.trim() ||
    "mailto:relay@hollow.local";
  const vapidPublicKey =
    policy.vapidPublicKey?.trim() ||
    process.env.CGP_RELAY_PUSH_VAPID_PUBLIC_KEY?.trim();
  const vapidPrivateKey =
    policy.vapidPrivateKey?.trim() ||
    process.env.CGP_RELAY_PUSH_VAPID_PRIVATE_KEY?.trim();
  const deliverSelfEvents =
    policy.deliverSelfEvents ?? process.env.CGP_RELAY_PUSH_SELF_EVENTS === "1";
  const registrations = new Map<string, RelayPushRegistration>();
  let loaded = false;
  let persistChain = Promise.resolve();

  const registrationKey = (account: string, deviceId: string) =>
    `${account}:${deviceId}`;

  const persist = () => {
    const snapshot: RelayPushRegistry = {
      version: 1,
      registrations: Array.from(registrations.values()).sort((left, right) => {
        const account = left.account.localeCompare(right.account);
        return account || left.deviceId.localeCompare(right.deviceId);
      }),
    };
    persistChain = persistChain
      .then(async () => {
        const resolved = path.resolve(registryPath);
        await mkdir(path.dirname(resolved), { recursive: true });
        const tmp = `${resolved}.${process.pid}.${Date.now()}.tmp`;
        await writeFile(tmp, JSON.stringify(snapshot, null, 2), "utf8");
        await rename(tmp, resolved);
      })
      .catch((error) => {
        console.error("Relay push registry persist failed:", error);
      });
    return persistChain;
  };

  const load = async () => {
    if (loaded) return;
    loaded = true;
    try {
      const raw = await readFile(path.resolve(registryPath), "utf8");
      const parsed = JSON.parse(raw) as Partial<RelayPushRegistry>;
      if (!Array.isArray(parsed.registrations)) return;
      for (const entry of parsed.registrations) {
        if (!isRecord(entry)) continue;
        const normalized = normalizeRelayPushRegistration(
          entry,
          maxGuildIdsPerDevice,
        );
        if (!normalized) continue;
        registrations.set(
          registrationKey(normalized.account, normalized.deviceId),
          normalized,
        );
      }
    } catch (error: any) {
      if (error?.code !== "ENOENT") {
        console.error("Relay push registry load failed:", error);
      }
    }
  };

  const register = async (config: Record<string, unknown>) => {
    const registration = verifyRelayPushSignedPayload(
      "cgp.relay.push.register",
      config.registration,
      maxSignatureAgeMs,
    );
    const normalized = normalizeRelayPushRegistration(
      registration,
      maxGuildIdsPerDevice,
    );
    if (!normalized) {
      throw new Error("Invalid relay push registration");
    }
    if (normalized.account !== normalized.author) {
      throw new Error("Relay push account must match signing author");
    }
    const owned = Array.from(registrations.values()).filter(
      (entry) => entry.account === normalized.account,
    );
    const existingKey = registrationKey(
      normalized.account,
      normalized.deviceId,
    );
    if (
      !registrations.has(existingKey) &&
      owned.length >= maxDevicesPerAccount
    ) {
      owned
        .sort((left, right) => left.registeredAt - right.registeredAt)
        .slice(0, owned.length - maxDevicesPerAccount + 1)
        .forEach((entry) =>
          registrations.delete(registrationKey(entry.account, entry.deviceId)),
        );
    }
    registrations.set(existingKey, normalized);
    await persist();
  };

  const unregister = async (config: Record<string, unknown>) => {
    const registration = verifyRelayPushSignedPayload(
      "cgp.relay.push.unregister",
      config.registration,
      maxSignatureAgeMs,
    );
    const account =
      stringField(registration, "account") ||
      stringField(registration, "author");
    const deviceId = stringField(registration, "deviceId");
    if (
      !account ||
      !deviceId ||
      account !== stringField(registration, "author")
    ) {
      throw new Error("Invalid relay push unregister request");
    }
    registrations.delete(registrationKey(account, deviceId));
    await persist();
  };

  const deliver = async (event: GuildEvent) => {
    if (!shouldRelayPushWake(event)) return;
    const payload = buildRelayPushPayload(event, publicRelayUrl);
    const content = JSON.stringify(payload);
    const stale: string[] = [];
    for (const registration of registrations.values()) {
      if (!deliverSelfEvents && registration.account === event.author) continue;
      if (!registrationMatchesEvent(registration, event)) continue;
      try {
        await sendRelayPush(registration, content, {
          timeoutMs: deliveryTimeoutMs,
          vapidSubject,
          vapidPublicKey,
          vapidPrivateKey,
        });
        registration.lastDeliveryAt = Date.now();
        delete registration.lastDeliveryError;
      } catch (error: any) {
        registration.lastDeliveryError = error?.message || String(error);
        const statusCode = Number(error?.statusCode ?? error?.status);
        if (statusCode === 404 || statusCode === 410) {
          stale.push(
            registrationKey(registration.account, registration.deviceId),
          );
        } else {
          const body = event.body as unknown as Record<string, unknown>;
          console.warn(
            `Relay push delivery failed for ${body.guildId}/${body.channelId ?? ""}:`,
            registration.lastDeliveryError,
          );
        }
      }
    }
    if (stale.length > 0) {
      for (const key of stale) registrations.delete(key);
      await persist();
    }
  };

  return {
    name: RELAY_PUSH_PLUGIN_NAME,
    metadata: {
      name: "Relay push",
      description:
        "Decentralized wake notifications via client-provided UnifiedPush/WebPush endpoints.",
      version: "1",
      policy: {
        protocol: RELAY_PUSH_PROTOCOL,
        maxDevicesPerAccount,
        maxGuildIdsPerDevice,
        wakeOnly: true,
        encryptedWebPush: Boolean(vapidPublicKey && vapidPrivateKey),
      },
    },
    inputs: [
      {
        name: "registryPath",
        type: "string",
        required: false,
        description: "Path to the relay push registration JSON file.",
        placeholder: registryPath,
        scope: "relay",
      },
      {
        name: "publicRelayUrl",
        type: "string",
        required: false,
        description: "Public relay URL included in wake hints.",
        placeholder: publicRelayUrl || "ws://relay.example",
        scope: "relay",
      },
      {
        name: "vapidPrivateKey",
        type: "string",
        required: false,
        sensitive: true,
        description:
          "Optional WebPush VAPID private key for distributors that require VAPID.",
        scope: "relay",
      },
    ],
    onInit: load,
    onConfig: async ({ config }) => {
      await load();
      if (!isRecord(config)) {
        throw new Error("Relay push config must be an object");
      }
      const action = stringField(config, "action");
      const protocol = stringField(config, "protocol");
      if (protocol !== RELAY_PUSH_PROTOCOL) {
        throw new Error(
          `Unsupported relay push protocol ${protocol || "(missing)"}`,
        );
      }
      if (action === "register") {
        await register(config);
        return;
      }
      if (action === "unregister") {
        await unregister(config);
        return;
      }
      throw new Error(`Unsupported relay push action ${action || "(missing)"}`);
    },
    onEventsAppended: async ({ events }) => {
      await load();
      for (const event of events) {
        await deliver(event);
      }
    },
    onClose: async () => {
      await persistChain;
    },
  };
}

function verifyRelayPushSignedPayload(
  kind: string,
  value: unknown,
  maxSignatureAgeMs: number,
): Record<string, unknown> {
  if (!isRecord(value)) {
    throw new Error("Relay push signed payload must be an object");
  }
  const signature = stringField(value, "signature");
  const author = stringField(value, "author");
  const createdAt = numberField(value, "createdAt");
  if (!signature || !author || !createdAt) {
    throw new Error(
      "Relay push signed payload is missing author, createdAt, or signature",
    );
  }
  const skew = Math.abs(Date.now() - createdAt);
  if (skew > maxSignatureAgeMs) {
    throw new Error("Relay push signed payload expired");
  }
  const payload: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value)) {
    if (key !== "signature" && entry !== undefined && entry !== null) {
      payload[key] = entry;
    }
  }
  if (!verify(author, hashObject({ kind, payload }), signature)) {
    throw new Error("Relay push signed payload signature is invalid");
  }
  return value;
}

function normalizeRelayPushRegistration(
  value: Record<string, unknown>,
  maxGuildIds: number,
): RelayPushRegistration | undefined {
  const provider = stringField(value, "provider") || "unifiedpush";
  const endpointKind = stringField(value, "endpointKind") || "webpush";
  const endpoint = stringField(value, "endpoint");
  const deviceId = stringField(value, "deviceId");
  const account = stringField(value, "account") || stringField(value, "author");
  const author = stringField(value, "author");
  const signature = stringField(value, "signature");
  const createdAt = numberField(value, "createdAt") ?? Date.now();
  const url = safeUrl(endpoint);
  if (!url || !deviceId || !account || !author || !signature) {
    return undefined;
  }
  const guildIds = Array.from(
    new Set(stringArrayField(value, "guildIds")),
  ).slice(0, maxGuildIds);
  const quorumPolicy = isRecord(value.quorumPolicy)
    ? value.quorumPolicy
    : undefined;
  return {
    version: Math.max(1, Math.floor(numberField(value, "version") ?? 1)),
    provider,
    endpointKind,
    endpoint: url.toString(),
    instance: stringField(value, "instance") || undefined,
    pubKey: stringField(value, "pubKey") || undefined,
    auth: stringField(value, "auth") || undefined,
    temporary: booleanField(value, "temporary") === true,
    deviceId,
    relayUrl: stringField(value, "relayUrl") || undefined,
    account,
    wakeOnly: booleanField(value, "wakeOnly") !== false,
    includePreviews: booleanField(value, "includePreviews") === true,
    includeDirectMessages:
      booleanField(value, "includeDirectMessages") !== false,
    requireRelayHead: booleanField(value, "requireRelayHead") !== false,
    quorumPolicy,
    guildIds,
    author,
    createdAt,
    signature,
    registeredAt: numberField(value, "registeredAt") ?? Date.now(),
    lastDeliveryAt: numberField(value, "lastDeliveryAt"),
    lastDeliveryError: stringField(value, "lastDeliveryError") || undefined,
  };
}

function shouldRelayPushWake(event: GuildEvent) {
  const body = event.body as unknown as Record<string, unknown>;
  const type = typeof body.type === "string" ? body.type : "";
  if (!RELAY_PUSH_WAKE_TYPES.has(type)) return false;
  if (type !== "CALL_EVENT") return true;
  const payload = isRecord(body.payload) ? body.payload : {};
  const callKind = stringField(payload, "kind") || stringField(body, "kind");
  const targetUserId = relayPushTargetUserId(body, payload);
  if (targetUserId) return true;
  const guildId = stringField(body, "guildId");
  const channelId = stringField(body, "channelId");
  const direct =
    guildId === "@me" ||
    guildId.startsWith("dm:") ||
    channelId.startsWith("dm:");
  return direct || callKind === "invite" || callKind === "join";
}

function registrationMatchesEvent(
  registration: RelayPushRegistration,
  event: GuildEvent,
) {
  const body = event.body as unknown as Record<string, unknown>;
  const guildId = stringField(body, "guildId");
  const channelId = stringField(body, "channelId");
  const localServerId = stringField(body, "localServerId");
  const direct =
    guildId === "@me" ||
    guildId.startsWith("dm:") ||
    channelId.startsWith("dm:");
  if (direct && !registration.includeDirectMessages) {
    return false;
  }
  const targetUserId = relayPushTargetUserId(body);
  if (targetUserId) {
    return registration.account === targetUserId;
  }
  if (registration.guildIds.length === 0) {
    return true;
  }
  return (
    registration.guildIds.includes(guildId) ||
    registration.guildIds.includes(channelId) ||
    registration.guildIds.includes(localServerId) ||
    (direct && registration.guildIds.includes("@me"))
  );
}

function buildRelayPushPayload(event: GuildEvent, publicRelayUrl?: string) {
  const body = event.body as unknown as Record<string, unknown>;
  const type = stringField(body, "type");
  const payload = isRecord(body.payload) ? body.payload : {};
  const channelId = stringField(body, "channelId");
  const guildId = stringField(body, "guildId");
  const messageId =
    stringField(body, "messageId") || stringField(payload, "messageId");
  const callKind = stringField(payload, "kind") || stringField(body, "kind");
  const targetUserId = relayPushTargetUserId(body, payload);
  const kind =
    type === "MESSAGE"
      ? "message"
      : type.startsWith("CALL") || type === "CALL_EVENT"
        ? "incoming-call"
        : "sync";
  return {
    protocol: RELAY_PUSH_PROTOCOL,
    kind,
    eventType: type,
    guildId,
    serverId: stringField(body, "localServerId") || guildId,
    channelId,
    messageId: messageId || undefined,
    callKind: callKind || undefined,
    targetUserId: targetUserId || undefined,
    author: event.author,
    eventId: event.id,
    seq: event.seq,
    prevHash: event.prevHash,
    createdAt: event.createdAt,
    relayUrl: publicRelayUrl || undefined,
    relayHead: {
      guildId,
      headSeq: event.seq,
      headHash: event.id,
      prevHash: event.prevHash,
    },
  };
}

function relayPushTargetUserId(
  body: Record<string, unknown>,
  payload?: Record<string, unknown>,
) {
  const callPayload =
    payload ?? (isRecord(body.payload) ? body.payload : undefined);
  return (
    stringField(callPayload, "toUserId") ||
    stringField(callPayload, "targetUserId") ||
    stringField(body, "toUserId") ||
    stringField(body, "targetUserId")
  );
}

async function sendRelayPush(
  registration: RelayPushRegistration,
  payload: string,
  options: {
    timeoutMs: number;
    vapidSubject: string;
    vapidPublicKey?: string;
    vapidPrivateKey?: string;
  },
) {
  if (
    registration.pubKey &&
    registration.auth &&
    options.vapidPublicKey &&
    options.vapidPrivateKey
  ) {
    webPush.setVapidDetails(
      options.vapidSubject,
      options.vapidPublicKey,
      options.vapidPrivateKey,
    );
    await webPush.sendNotification(
      {
        endpoint: registration.endpoint,
        keys: {
          p256dh: registration.pubKey,
          auth: registration.auth,
        },
      },
      payload,
      {
        TTL: 60,
        urgency: "high",
        timeout: options.timeoutMs,
      },
    );
    return;
  }
  await postRelayPush(registration.endpoint, payload, options.timeoutMs);
}

function postRelayPush(endpoint: string, payload: string, timeoutMs: number) {
  return new Promise<void>((resolve, reject) => {
    const url = safeUrl(endpoint);
    if (!url) {
      reject(new Error("Invalid relay push endpoint URL"));
      return;
    }
    const client = url.protocol === "https:" ? httpsRequest : httpRequest;
    const req = client(
      url,
      {
        method: "POST",
        headers: {
          "content-type": "application/json; charset=utf-8",
          "content-length": Buffer.byteLength(payload),
          ttl: "60",
          urgency: "high",
        },
      },
      (res) => {
        res.resume();
        res.on("end", () => {
          const statusCode = res.statusCode ?? 0;
          if (statusCode >= 200 && statusCode < 300) {
            resolve();
            return;
          }
          const error = new Error(
            `Relay push endpoint returned HTTP ${statusCode}`,
          ) as Error & { statusCode?: number };
          error.statusCode = statusCode;
          reject(error);
        });
      },
    );
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error("Relay push delivery timed out"));
    });
    req.on("error", reject);
    req.write(payload);
    req.end();
  });
}

function safeUrl(value: string) {
  try {
    const url = new URL(value);
    if (url.protocol !== "https:" && url.protocol !== "http:") {
      return undefined;
    }
    return url;
  } catch {
    return undefined;
  }
}

function positiveIntegerFromEnv(name: string, fallback: number) {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function takeRateToken(
  buckets: Map<string, RateBucket>,
  key: string,
  limit: number,
  windowMs: number,
) {
  return takeRateTokens(buckets, key, limit, windowMs, 1);
}

function takeRateTokens(
  buckets: Map<string, RateBucket>,
  key: string,
  limit: number,
  windowMs: number,
  count: number,
) {
  if (limit <= 0) return true;
  const amount = Math.max(1, Math.floor(count));

  const now = Date.now();
  const current = buckets.get(key);

  if (!current || now - current.windowStartedAt >= windowMs) {
    if (amount > limit) {
      return false;
    }
    buckets.set(key, { windowStartedAt: now, count: amount });
    pruneRateBuckets(buckets, windowMs, now);
    return true;
  }

  if (current.count + amount > limit) {
    return false;
  }

  current.count += amount;
  return true;
}

function pruneRateBuckets(
  buckets: Map<string, RateBucket>,
  windowMs: number,
  now = Date.now(),
) {
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
  return typeof (payload as { clientEventId?: unknown })?.clientEventId ===
    "string"
    ? (payload as { clientEventId: string }).clientEventId
    : undefined;
}

function publishBatchId(payload: unknown) {
  return typeof (payload as { batchId?: unknown })?.batchId === "string"
    ? (payload as { batchId: string }).batchId
    : undefined;
}

function publishPayloads(kind: string, payload: unknown) {
  if (kind === "PUBLISH") {
    return [payload];
  }
  if (kind === "PUBLISH_BATCH") {
    const events = (payload as { events?: unknown })?.events;
    return Array.isArray(events) ? events : [];
  }
  return [];
}

function sendRateLimitError(
  socket: WebSocket,
  message: string,
  payload?: unknown,
) {
  socket.send(
    JSON.stringify([
      "ERROR",
      {
        code: "RATE_LIMITED",
        message,
        clientEventId: publishClientEventId(payload),
        batchId: publishBatchId(payload),
      },
    ]),
  );
}

export function createRateLimitPolicyPlugin(
  policy: Partial<RateLimitPolicy> = {},
): RelayPlugin {
  const resolved: RateLimitPolicy = {
    rateWindowMs:
      policy.rateWindowMs ??
      positiveIntegerFromEnv("CGP_RELAY_RATE_WINDOW_MS", 10_000),
    socketPublishesPerWindow:
      policy.socketPublishesPerWindow ??
      positiveIntegerFromEnv("CGP_RELAY_SOCKET_PUBLISH_LIMIT", 1_500),
    authorPublishesPerWindow:
      policy.authorPublishesPerWindow ??
      positiveIntegerFromEnv("CGP_RELAY_AUTHOR_PUBLISH_LIMIT", 1_500),
    guildPublishesPerWindow:
      policy.guildPublishesPerWindow ??
      positiveIntegerFromEnv("CGP_RELAY_GUILD_PUBLISH_LIMIT", 5_000),
  };
  const buckets = new Map<string, RateBucket>();
  const socketIds = new WeakMap<WebSocket, number>();
  let nextSocketId = 1;

  return {
    name: "cgp.relay.rate-limit",
    metadata: {
      name: "Reference relay rate limiting",
      description:
        "Default relay-local anti-abuse policy. This is operational policy, not core CGP state.",
      version: "1",
      policy: { ...resolved },
    },
    onFrame: ({ socket, kind, payload }) => {
      const publishes = publishPayloads(kind, payload);
      if (publishes.length === 0) {
        return false;
      }

      let socketId = socketIds.get(socket);
      if (!socketId) {
        socketId = nextSocketId++;
        socketIds.set(socket, socketId);
      }

      if (
        !takeRateTokens(
          buckets,
          `socket:${socketId}`,
          resolved.socketPublishesPerWindow,
          resolved.rateWindowMs,
          publishes.length,
        )
      ) {
        sendRateLimitError(
          socket,
          "Socket publish rate limit exceeded",
          payload,
        );
        return true;
      }

      const authorCounts = new Map<string, number>();
      const guildCounts = new Map<string, number>();
      for (const publish of publishes as Array<{
        author?: unknown;
        body?: { guildId?: unknown };
      }>) {
        const author =
          typeof publish?.author === "string" ? publish.author : "";
        const guildId =
          typeof publish?.body?.guildId === "string"
            ? publish.body.guildId
            : "";
        if (author)
          authorCounts.set(author, (authorCounts.get(author) ?? 0) + 1);
        if (guildId)
          guildCounts.set(guildId, (guildCounts.get(guildId) ?? 0) + 1);
      }

      for (const [author, count] of authorCounts) {
        if (
          !takeRateTokens(
            buckets,
            `author:${author}`,
            resolved.authorPublishesPerWindow,
            resolved.rateWindowMs,
            count,
          )
        ) {
          sendRateLimitError(
            socket,
            "Author publish rate limit exceeded",
            payload,
          );
          return true;
        }
      }

      for (const [guildId, count] of guildCounts) {
        if (
          !takeRateTokens(
            buckets,
            `guild:${guildId}`,
            resolved.guildPublishesPerWindow,
            resolved.rateWindowMs,
            count,
          )
        ) {
          sendRateLimitError(
            socket,
            "Guild publish rate limit exceeded",
            payload,
          );
          return true;
        }
      }

      return false;
    },
  };
}

function countMentions(content: string) {
  const atMentions =
    content.match(/(^|\s)@[a-zA-Z0-9_.-]{2,64}\b/g)?.length ?? 0;
  const idMentions = content.match(/<@!?[a-zA-Z0-9:_-]{6,}>/g)?.length ?? 0;
  return atMentions + idMentions;
}

function shortContentHash(content: string) {
  return createHash("sha256")
    .update(content.trim().replace(/\s+/g, " ").toLowerCase())
    .digest("hex")
    .slice(0, 24);
}

export function createAbuseControlPolicyPlugin(
  policy: AbuseControlPolicy = {},
): RelayPlugin {
  const resolved = {
    windowMs: Math.max(
      1000,
      Math.floor(
        policy.windowMs ??
          positiveIntegerFromEnv("CGP_RELAY_ABUSE_WINDOW_MS", 10_000),
      ),
    ),
    maxMessageChars: Math.max(
      0,
      Math.floor(
        policy.maxMessageChars ??
          positiveIntegerFromEnv("CGP_RELAY_MAX_MESSAGE_CHARS", 6000),
      ),
    ),
    maxMentionsPerMessage: Math.max(
      0,
      Math.floor(
        policy.maxMentionsPerMessage ??
          positiveIntegerFromEnv("CGP_RELAY_MAX_MENTIONS", 20),
      ),
    ),
    duplicateMessagesPerWindow: Math.max(
      0,
      Math.floor(
        policy.duplicateMessagesPerWindow ??
          positiveIntegerFromEnv("CGP_RELAY_DUPLICATE_MESSAGE_LIMIT", 4),
      ),
    ),
    commandInvocationsPerWindow: Math.max(
      0,
      Math.floor(
        policy.commandInvocationsPerWindow ??
          positiveIntegerFromEnv("CGP_RELAY_COMMAND_INVOCATION_LIMIT", 20),
      ),
    ),
    recoveryRequestsPerWindow: Math.max(
      0,
      Math.floor(
        policy.recoveryRequestsPerWindow ??
          positiveIntegerFromEnv("CGP_RELAY_RECOVERY_REQUEST_LIMIT", 3),
      ),
    ),
  };
  const duplicateBuckets = new Map<string, RateBucket>();
  const commandBuckets = new Map<string, RateBucket>();
  const recoveryBuckets = new Map<string, RateBucket>();

  return {
    name: "cgp.relay.abuse-controls",
    metadata: {
      name: "Reference abuse controls",
      description:
        "Relay-local anti-spam policy for message length, mention storms, duplicate floods, command bursts, and recovery request floods.",
      version: "1",
      policy: { ...resolved },
    },
    onFrame: ({ socket, kind, payload }) => {
      const publishes = publishPayloads(kind, payload);
      if (publishes.length === 0) {
        return false;
      }

      for (const publish of publishes as Array<{
        author?: unknown;
        body?: Record<string, unknown>;
      }>) {
        const author =
          typeof publish?.author === "string" ? publish.author : "";
        const body = isRecord(publish?.body) ? publish.body : undefined;
        if (!author || !body) {
          continue;
        }

        const guildId = stringField(body, "guildId");
        const channelId = stringField(body, "channelId");
        if (body.type === "MESSAGE") {
          const content = stringField(body, "content");
          if (
            resolved.maxMessageChars > 0 &&
            content.length > resolved.maxMessageChars
          ) {
            sendPolicyError(
              socket,
              "ABUSE_POLICY_BLOCKED",
              `Message content exceeds ${resolved.maxMessageChars} characters.`,
              payload,
            );
            return true;
          }
          if (
            resolved.maxMentionsPerMessage > 0 &&
            countMentions(content) > resolved.maxMentionsPerMessage
          ) {
            sendPolicyError(
              socket,
              "ABUSE_POLICY_BLOCKED",
              `Message exceeds ${resolved.maxMentionsPerMessage} mentions.`,
              payload,
            );
            return true;
          }
          if (
            resolved.duplicateMessagesPerWindow > 0 &&
            content &&
            !takeRateToken(
              duplicateBuckets,
              `dup:${guildId}:${channelId}:${author}:${shortContentHash(content)}`,
              resolved.duplicateMessagesPerWindow,
              resolved.windowMs,
            )
          ) {
            sendPolicyError(
              socket,
              "ABUSE_RATE_LIMITED",
              "Duplicate message flood detected.",
              payload,
            );
            return true;
          }
        }

        if (
          body.type === "APP_OBJECT_UPSERT" &&
          body.namespace === "org.cgp.apps" &&
          body.objectType === "command-invocation" &&
          resolved.commandInvocationsPerWindow > 0 &&
          !takeRateToken(
            commandBuckets,
            `command:${guildId}:${author}`,
            resolved.commandInvocationsPerWindow,
            resolved.windowMs,
          )
        ) {
          sendPolicyError(
            socket,
            "ABUSE_RATE_LIMITED",
            "Command invocation rate limit exceeded.",
            payload,
          );
          return true;
        }

        if (
          body.type === "APP_OBJECT_UPSERT" &&
          body.namespace === "org.cgp.recovery" &&
          body.objectType === "recovery-request" &&
          resolved.recoveryRequestsPerWindow > 0
        ) {
          const target = isRecord(body.value)
            ? stringField(body.value, "recoveryTopic") ||
              stringField(body.value, "accountHandle")
            : "";
          const bucketKey = `recovery:${guildId}:${author}:${target || "generic"}`;
          if (
            !takeRateToken(
              recoveryBuckets,
              bucketKey,
              resolved.recoveryRequestsPerWindow,
              resolved.windowMs,
            )
          ) {
            sendPolicyError(
              socket,
              "ABUSE_RATE_LIMITED",
              "Recovery request rate limit exceeded.",
              payload,
            );
            return true;
          }
        }
      }

      return false;
    },
  };
}

function sendPolicyError(
  socket: WebSocket,
  code: string,
  message: string,
  payload?: unknown,
) {
  socket.send(
    JSON.stringify([
      "ERROR",
      {
        code,
        message,
        clientEventId: publishClientEventId(payload),
        batchId: publishBatchId(payload),
      },
    ]),
  );
}

function listApplies<T extends string>(
  allowed: T[] | undefined,
  value: T | undefined,
) {
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
  return typeof value === "number" && Number.isFinite(value)
    ? value
    : undefined;
}

function booleanField(
  source: Record<string, unknown> | undefined,
  key: string,
) {
  const value = source?.[key];
  return typeof value === "boolean" ? value : undefined;
}

function stringArrayField(
  source: Record<string, unknown> | undefined,
  key: string,
) {
  const value = source?.[key];
  return Array.isArray(value)
    ? value
        .filter((entry): entry is string => typeof entry === "string")
        .map((entry) => entry.trim())
        .filter(Boolean)
    : [];
}

function normalizeMediaToken(value: string) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_.:-]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function normalizeMediaTags(tags: unknown) {
  if (!Array.isArray(tags)) return [];
  const normalized = new Set<string>();
  for (const tag of tags) {
    if (typeof tag !== "string") continue;
    const value = normalizeMediaToken(tag);
    if (value) normalized.add(value);
  }
  return Array.from(normalized);
}

function parseMediaProvidersFromEnv(): MediaStorageProvider[] | undefined {
  const raw = process.env.CGP_MEDIA_PROVIDERS_JSON;
  if (!raw?.trim()) return undefined;
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return undefined;
    }
    return parsed
      .filter((entry): entry is Record<string, unknown> => isRecord(entry))
      .map((entry) => normalizeMediaProvider(entry))
      .filter((entry): entry is MediaStorageProvider => Boolean(entry));
  } catch {
    return undefined;
  }
}

function normalizeProviderStringList(value: unknown) {
  if (!Array.isArray(value)) return undefined;
  const values = value
    .filter((entry): entry is string => typeof entry === "string")
    .map((entry) => entry.trim())
    .filter(Boolean);
  return values.length > 0 ? values : undefined;
}

function normalizeMediaProvider(
  entry: Record<string, unknown>,
): MediaStorageProvider | undefined {
  const id = stringField(entry, "id");
  const kind = stringField(entry, "kind") as MediaStorageProviderKind;
  if (
    !id ||
    !["ipfs", "https", "relay-cache", "local", "external"].includes(kind)
  ) {
    return undefined;
  }
  const adult = stringField(entry, "adult") as MediaAdultPolicy;
  const retention = stringField(
    entry,
    "retention",
  ) as MediaStorageProvider["retention"];
  const provider: MediaStorageProvider = {
    id,
    kind,
    label: stringField(entry, "label") || undefined,
    description: stringField(entry, "description") || undefined,
    endpoint: stringField(entry, "endpoint") || undefined,
    gatewayUrl: stringField(entry, "gatewayUrl") || undefined,
    priority: numberField(entry, "priority"),
    maxBytes: numberField(entry, "maxBytes"),
    acceptsMimeTypes: normalizeProviderStringList(entry.acceptsMimeTypes),
    acceptsTags: normalizeProviderStringList(entry.acceptsTags)?.map(
      normalizeMediaToken,
    ),
    requiresTags: normalizeProviderStringList(entry.requiresTags)?.map(
      normalizeMediaToken,
    ),
    rejectsTags: normalizeProviderStringList(entry.rejectsTags)?.map(
      normalizeMediaToken,
    ),
    adult:
      adult === "allow" || adult === "deny" || adult === "only"
        ? adult
        : undefined,
    encryptedOnly: booleanField(entry, "encryptedOnly"),
    lossless: booleanField(entry, "lossless"),
    retention:
      retention === "best-effort" ||
      retention === "pinned" ||
      retention === "paid" ||
      retention === "operator-defined"
        ? retention
        : undefined,
    mission: stringField(entry, "mission") || undefined,
  };
  return provider;
}

function defaultMediaProviders(
  maxAttachmentBytes: number,
): MediaStorageProvider[] {
  const providers: MediaStorageProvider[] = [
    {
      id: "ipfs-public-media",
      kind: "ipfs",
      label: "Public IPFS media pinning",
      description:
        "Best-effort encrypted or public media pins for general community images and files.",
      endpoint: process.env.CGP_MEDIA_IPFS_API_URL,
      gatewayUrl:
        process.env.CGP_MEDIA_IPFS_GATEWAY_URL || "https://ipfs.io/ipfs/{cid}",
      priority: 50,
      maxBytes: maxAttachmentBytes,
      acceptsMimeTypes: [
        "image/*",
        "video/*",
        "audio/*",
        "application/octet-stream",
      ],
      acceptsTags: [
        "general",
        "image",
        "meme",
        "art",
        "anime",
        "photo",
        "dog",
        "file",
      ],
      rejectsTags: ["adult", "porn", "illegal"],
      adult: "deny",
      retention: "operator-defined",
      mission: "general community media",
    },
    {
      id: "https-origin",
      kind: "https",
      label: "External HTTPS origin",
      description: "Client- or guild-provided object storage/CDN URL.",
      priority: 10,
      maxBytes: maxAttachmentBytes,
      acceptsMimeTypes: ["*/*"],
      adult: "allow",
      retention: "operator-defined",
      mission: "bring-your-own storage",
    },
  ];

  if (process.env.CGP_MEDIA_ADULT_PROVIDER === "1") {
    providers.push({
      id: "ipfs-adult-media",
      kind: "ipfs",
      label: "Adult-only IPFS media pinning",
      description:
        "Opt-in provider for relays that explicitly host adult-tagged media.",
      endpoint:
        process.env.CGP_MEDIA_ADULT_IPFS_API_URL ||
        process.env.CGP_MEDIA_IPFS_API_URL,
      gatewayUrl:
        process.env.CGP_MEDIA_ADULT_IPFS_GATEWAY_URL ||
        process.env.CGP_MEDIA_IPFS_GATEWAY_URL,
      priority: 60,
      maxBytes: maxAttachmentBytes,
      acceptsMimeTypes: ["image/*", "video/*"],
      acceptsTags: ["adult", "nsfw", "porn"],
      requiresTags: ["adult"],
      adult: "only",
      retention: "operator-defined",
      mission: "adult media host",
    });
  }

  return providers;
}

function mimeMatches(pattern: string, mimeType: string) {
  const normalizedPattern = pattern.trim().toLowerCase();
  const normalizedMime = mimeType.trim().toLowerCase();
  if (!normalizedPattern || normalizedPattern === "*/*") return true;
  if (normalizedPattern.endsWith("/*")) {
    return normalizedMime.startsWith(normalizedPattern.slice(0, -1));
  }
  return normalizedPattern === normalizedMime;
}

function providerMatchesMediaRequest(
  provider: MediaStorageProvider,
  request: MediaRouteRequest,
) {
  const size = Number(request.size ?? 0);
  const mimeType = typeof request.mimeType === "string" ? request.mimeType : "";
  const tags = new Set(normalizeMediaTags(request.tags));
  const adult =
    request.adult === true ||
    request.nsfw === true ||
    tags.has("adult") ||
    tags.has("nsfw") ||
    tags.has("porn");

  if (provider.maxBytes && size > provider.maxBytes) {
    return false;
  }
  if (provider.encryptedOnly && request.encrypted !== true) {
    return false;
  }
  if (provider.lossless && request.lossless === false) {
    return false;
  }
  if (provider.adult === "deny" && adult) {
    return false;
  }
  if (provider.adult === "only" && !adult) {
    return false;
  }
  if (
    mimeType &&
    provider.acceptsMimeTypes?.length &&
    !provider.acceptsMimeTypes.some((pattern) => mimeMatches(pattern, mimeType))
  ) {
    return false;
  }
  if (provider.requiresTags?.some((tag) => !tags.has(tag))) {
    return false;
  }
  if (provider.rejectsTags?.some((tag) => tags.has(tag))) {
    return false;
  }
  if (provider.acceptsTags?.length) {
    if (tags.size === 0 || !provider.acceptsTags.some((tag) => tags.has(tag))) {
      return false;
    }
  }
  return true;
}

function mediaProvidersForRequest(
  providers: MediaStorageProvider[],
  request: MediaRouteRequest,
) {
  return providers
    .filter((provider) => providerMatchesMediaRequest(provider, request))
    .sort(
      (left, right) =>
        (right.priority ?? 0) - (left.priority ?? 0) ||
        left.id.localeCompare(right.id),
    );
}

function schemeFromAttachment(attachment: Record<string, unknown>) {
  const direct = stringField(attachment, "scheme");
  if (direct) return direct.toLowerCase();
  const url = stringField(attachment, "url");
  const match = /^([a-z][a-z0-9+.-]*):/i.exec(url);
  return match?.[1]?.toLowerCase() || "";
}

function mediaStorageMetadata(attachment: Record<string, unknown>) {
  const external = isRecord(attachment.external)
    ? attachment.external
    : undefined;
  const storage = isRecord(external?.storage)
    ? external.storage
    : isRecord(attachment.storage)
      ? attachment.storage
      : undefined;
  return storage;
}

function routeRequestFromAttachment(
  attachment: Record<string, unknown>,
): MediaRouteRequest {
  const external = isRecord(attachment.external)
    ? attachment.external
    : undefined;
  const storage = mediaStorageMetadata(attachment);
  return {
    type: stringField(attachment, "type") || stringField(storage, "type"),
    mimeType:
      stringField(attachment, "mimeType") || stringField(storage, "mimeType"),
    size: numberField(attachment, "size") ?? numberField(storage, "size"),
    tags: [
      ...normalizeMediaTags(external?.tags),
      ...normalizeMediaTags(storage?.tags),
    ],
    adult: booleanField(attachment, "adult") ?? booleanField(storage, "adult"),
    nsfw: booleanField(attachment, "nsfw") ?? booleanField(storage, "nsfw"),
    encrypted:
      booleanField(attachment, "encrypted") ??
      booleanField(storage, "encrypted"),
    lossless: booleanField(storage, "lossless"),
  };
}

function providerIdFromAttachment(attachment: Record<string, unknown>) {
  const storage = mediaStorageMetadata(attachment);
  return (
    stringField(storage, "providerId") ||
    stringField(storage, "provider") ||
    stringField(attachment, "providerId")
  );
}

export function createMediaStoragePolicyPlugin(
  policy: MediaStoragePolicy = {},
): RelayPlugin {
  const maxAttachmentBytes = Math.max(
    1,
    Math.floor(
      policy.maxAttachmentBytes ??
        positiveIntegerFromEnv(
          "CGP_MEDIA_MAX_ATTACHMENT_BYTES",
          25 * 1024 * 1024,
        ),
    ),
  );
  const providers =
    policy.providers ??
    parseMediaProvidersFromEnv() ??
    defaultMediaProviders(maxAttachmentBytes);
  const providerById = new Map(
    providers.map((provider) => [provider.id, provider]),
  );
  const maxAttachmentsPerMessage = Math.max(
    1,
    Math.floor(
      policy.maxAttachmentsPerMessage ??
        positiveIntegerFromEnv("CGP_MEDIA_MAX_ATTACHMENTS_PER_MESSAGE", 10),
    ),
  );
  const maxInlineBytes = Math.max(
    0,
    Math.floor(
      policy.maxInlineBytes ??
        positiveIntegerFromEnv("CGP_MEDIA_MAX_INLINE_BYTES", 0),
    ),
  );
  const allowInlineContent =
    policy.allowInlineContent ??
    process.env.CGP_MEDIA_ALLOW_INLINE_CONTENT === "1";
  const allowedSchemes = new Set(
    (
      policy.allowedSchemes ??
      (process.env.CGP_MEDIA_ALLOWED_SCHEMES || "ipfs,https,cgp-media").split(
        ",",
      )
    )
      .map((scheme) => scheme.trim().toLowerCase())
      .filter(Boolean),
  );
  const requireKnownProvider =
    policy.requireKnownProvider ??
    process.env.CGP_MEDIA_REQUIRE_KNOWN_PROVIDER !== "0";
  const requireEncryptedMedia =
    policy.requireEncryptedMedia ??
    process.env.CGP_MEDIA_REQUIRE_ENCRYPTED === "1";

  const publicProvider = (provider: MediaStorageProvider) => ({
    id: provider.id,
    kind: provider.kind,
    label: provider.label,
    description: provider.description,
    endpoint: provider.endpoint,
    gatewayUrl: provider.gatewayUrl,
    priority: provider.priority,
    maxBytes: provider.maxBytes,
    acceptsMimeTypes: provider.acceptsMimeTypes,
    acceptsTags: provider.acceptsTags,
    requiresTags: provider.requiresTags,
    rejectsTags: provider.rejectsTags,
    adult: provider.adult,
    encryptedOnly: provider.encryptedOnly,
    lossless: provider.lossless,
    retention: provider.retention,
    mission: provider.mission,
  });

  return {
    name: "cgp.media.storage",
    metadata: {
      name: "Media storage routing",
      description:
        "Relay-local media placement policy for IPFS pins, external object storage, and topic-specific media hosts.",
      version: "1",
      policy: {
        maxAttachmentBytes,
        maxAttachmentsPerMessage,
        maxInlineBytes,
        allowInlineContent,
        allowedSchemes: Array.from(allowedSchemes),
        requireKnownProvider,
        requireEncryptedMedia,
        providers: providers.map(publicProvider),
      },
    },
    onHttp: async ({ req, res, pathSegments }) => {
      if (pathSegments[0] !== "cgp.media.storage") {
        return false;
      }

      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
      res.setHeader(
        "Access-Control-Allow-Headers",
        "Content-Type, Authorization",
      );

      if (req.method === "OPTIONS") {
        res.statusCode = 204;
        res.end();
        return true;
      }

      const action = pathSegments[1] || "providers";
      if (action === "providers" && req.method === "GET") {
        sendJson(res, 200, {
          ok: true,
          providers: providers.map(publicProvider),
          policy: {
            maxAttachmentBytes,
            maxAttachmentsPerMessage,
            maxInlineBytes,
            allowInlineContent,
            allowedSchemes: Array.from(allowedSchemes),
            requireKnownProvider,
            requireEncryptedMedia,
          },
        });
        return true;
      }

      if (
        (action === "route" || action === "upload-intent") &&
        req.method === "POST"
      ) {
        let request: MediaRouteRequest;
        try {
          request = (await readJsonRequestBody(
            req,
            64 * 1024,
          )) as MediaRouteRequest;
        } catch (error: any) {
          sendJson(res, 400, {
            ok: false,
            error: error?.message || "Invalid media route request.",
          });
          return true;
        }

        const candidates = mediaProvidersForRequest(providers, {
          ...request,
          tags: normalizeMediaTags(request.tags),
        });
        if (candidates.length === 0) {
          sendJson(res, 409, {
            ok: false,
            error: "No configured media provider accepts this object.",
            request,
          });
          return true;
        }

        const selected = candidates[0];
        sendJson(res, 200, {
          ok: true,
          selected: publicProvider(selected),
          candidates: candidates.map(publicProvider),
          upload: {
            mode:
              selected.kind === "ipfs" && selected.endpoint
                ? "ipfs-api"
                : selected.kind,
            endpoint: selected.endpoint,
            gatewayUrl: selected.gatewayUrl,
            maxBytes: selected.maxBytes ?? maxAttachmentBytes,
            requiredAttachment: {
              scheme:
                selected.kind === "ipfs"
                  ? "ipfs"
                  : selected.kind === "https"
                    ? "https"
                    : "cgp-media",
              external: {
                storage: {
                  providerId: selected.id,
                  kind: selected.kind,
                  retention: selected.retention,
                },
              },
            },
          },
        });
        return true;
      }

      sendJson(res, 404, { ok: false, error: "Unknown media storage route." });
      return true;
    },
    onFrame: ({ socket, kind, payload }) => {
      const publishes = publishPayloads(kind, payload);
      if (publishes.length === 0) {
        return false;
      }

      for (const publish of publishes as Array<{
        body?: Record<string, unknown>;
      }>) {
        const body = isRecord(publish?.body) ? publish.body : undefined;
        if (body?.type !== "MESSAGE") {
          continue;
        }
        const attachments = Array.isArray(body.attachments)
          ? body.attachments
          : [];
        if (attachments.length === 0) {
          continue;
        }
        if (attachments.length > maxAttachmentsPerMessage) {
          sendPolicyError(
            socket,
            "MEDIA_POLICY_BLOCKED",
            `Message has ${attachments.length}/${maxAttachmentsPerMessage} allowed attachments.`,
            payload,
          );
          return true;
        }

        for (const attachmentValue of attachments) {
          if (!isRecord(attachmentValue)) {
            sendPolicyError(
              socket,
              "MEDIA_POLICY_BLOCKED",
              "Attachment entries must be objects.",
              payload,
            );
            return true;
          }
          const attachment = attachmentValue;
          const inlineContent = stringField(attachment, "content");
          if (inlineContent) {
            const inlineBytes = Buffer.byteLength(inlineContent, "utf8");
            if (!allowInlineContent || inlineBytes > maxInlineBytes) {
              sendPolicyError(
                socket,
                "MEDIA_POLICY_BLOCKED",
                `Inline attachment content is limited to ${maxInlineBytes} bytes on this relay.`,
                payload,
              );
              return true;
            }
          }

          const route = routeRequestFromAttachment(attachment);
          const size = Number(
            route.size ??
              (inlineContent ? Buffer.byteLength(inlineContent, "utf8") : 0),
          );
          if (Number.isFinite(size) && size > maxAttachmentBytes) {
            sendPolicyError(
              socket,
              "MEDIA_POLICY_BLOCKED",
              `Attachment size ${size} exceeds ${maxAttachmentBytes} bytes.`,
              payload,
            );
            return true;
          }
          if (requireEncryptedMedia && route.encrypted !== true) {
            sendPolicyError(
              socket,
              "MEDIA_POLICY_BLOCKED",
              "This relay requires encrypted media attachments.",
              payload,
            );
            return true;
          }

          const scheme = schemeFromAttachment(attachment);
          if (
            scheme &&
            allowedSchemes.size > 0 &&
            !allowedSchemes.has(scheme)
          ) {
            sendPolicyError(
              socket,
              "MEDIA_POLICY_BLOCKED",
              `Unsupported media URL scheme: ${scheme}.`,
              payload,
            );
            return true;
          }

          const providerId = providerIdFromAttachment(attachment);
          if (providerId) {
            const provider = providerById.get(providerId);
            if (!provider) {
              sendPolicyError(
                socket,
                "MEDIA_POLICY_BLOCKED",
                `Unknown media provider: ${providerId}.`,
                payload,
              );
              return true;
            }
            if (!providerMatchesMediaRequest(provider, route)) {
              sendPolicyError(
                socket,
                "MEDIA_POLICY_BLOCKED",
                `Media provider ${providerId} does not accept this attachment class.`,
                payload,
              );
              return true;
            }
          } else if (requireKnownProvider) {
            const candidates = mediaProvidersForRequest(providers, route);
            if (candidates.length === 0) {
              sendPolicyError(
                socket,
                "MEDIA_POLICY_BLOCKED",
                "No configured media provider accepts this attachment class.",
                payload,
              );
              return true;
            }
          }
        }
      }

      return false;
    },
  };
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

export function createProofOfWorkPolicyPlugin(
  policy: ProofOfWorkPolicy,
): RelayPlugin {
  const difficultyBits = Math.max(0, Math.floor(policy.difficultyBits));
  const ttlMs = policy.ttlMs ?? 5 * 60_000;
  const eventTypes = new Set(
    (policy.eventTypes ?? [])
      .map((type) => type.trim().toUpperCase())
      .filter(Boolean),
  );

  return {
    name: "cgp.relay.proof-of-work",
    metadata: {
      name: "Proof-of-work publish gate",
      description:
        "Optional relay-local anti-Sybil policy for open relays. This is not mandatory CGP protocol state.",
      version: "1",
      policy: {
        difficultyBits,
        ttlMs,
        eventTypes: [...eventTypes],
        guildIds: policy.guildIds,
        channelIds: policy.channelIds,
      },
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
      const type =
        typeof body?.type === "string" ? body.type.toUpperCase() : "";
      const guildId =
        typeof body?.guildId === "string" ? body.guildId : undefined;
      const channelId =
        typeof body?.channelId === "string" ? body.channelId : undefined;
      if (
        (eventTypes.size > 0 && !eventTypes.has(type)) ||
        !listApplies(policy.guildIds, guildId) ||
        !listApplies(policy.channelIds, channelId)
      ) {
        return false;
      }

      const proof = isRecord(publish.proof) ? publish.proof : undefined;
      const algorithm =
        stringField(proof, "algorithm") || "sha256-leading-zero-bits-v1";
      const nonce = stringField(proof, "nonce");
      const issuedAt = Number(proof?.issuedAt);
      const proofDifficulty = Math.floor(
        Number(proof?.difficultyBits ?? difficultyBits),
      );
      const now = Date.now();
      if (
        algorithm !== "sha256-leading-zero-bits-v1" ||
        !nonce ||
        nonce.length > 256 ||
        !Number.isFinite(issuedAt) ||
        Math.abs(now - issuedAt) > ttlMs ||
        proofDifficulty < difficultyBits
      ) {
        sendPolicyError(
          socket,
          "PROOF_OF_WORK_REQUIRED",
          "Valid proof-of-work is required for this publish.",
          payload,
        );
        return true;
      }

      const challenge = hashObject({
        algorithm,
        difficultyBits: proofDifficulty,
        nonce,
        issuedAt,
        author: publish.author,
        createdAt: publish.createdAt,
        body,
      });
      const digest = createHash("sha256").update(challenge).digest();
      if (leadingZeroBits(digest) < difficultyBits) {
        sendPolicyError(
          socket,
          "PROOF_OF_WORK_REQUIRED",
          "Proof-of-work difficulty target was not met.",
          payload,
        );
        return true;
      }

      return false;
    },
  };
}

function hasReportTarget(target: Record<string, unknown> | undefined) {
  return (
    !!target &&
    ["messageId", "userId", "channelId"].some(
      (key) => stringField(target, key).length > 0,
    )
  );
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

async function relayGuildState(ctx: RelayPluginContext, guildId: GuildId) {
  return (
    (await ctx.getState?.(guildId)) ??
    replayGuildState(await ctx.getLog(guildId))
  );
}

export function createEncryptionPolicyPlugin(
  policy: EncryptionPolicy = {},
): RelayPlugin {
  const resolved: Required<
    Pick<
      EncryptionPolicy,
      "requireEncryptedMessages" | "allowEncryptedMessages"
    >
  > &
    Pick<EncryptionPolicy, "guildIds" | "channelIds"> = {
    requireEncryptedMessages: policy.requireEncryptedMessages ?? false,
    allowEncryptedMessages: policy.allowEncryptedMessages ?? true,
    guildIds: policy.guildIds,
    channelIds: policy.channelIds,
  };

  return {
    name: "cgp.security.encryption-policy",
    metadata: {
      name: "Encryption payload policy",
      description:
        "Relay-local policy for accepting or requiring opaque encrypted MESSAGE payloads. It is not a key server.",
      version: "1",
      policy: { ...resolved },
    },
    onFrame: ({ socket, kind, payload }) => {
      const publishes = publishPayloads(kind, payload);
      if (publishes.length === 0) {
        return false;
      }

      for (const publish of publishes as Array<{
        body?: {
          type?: unknown;
          guildId?: unknown;
          channelId?: unknown;
          encrypted?: unknown;
          iv?: unknown;
          content?: unknown;
        };
      }>) {
        const body = publish?.body;
        if (body?.type !== "MESSAGE") {
          continue;
        }

        const guildId =
          typeof body.guildId === "string" ? body.guildId : undefined;
        const channelId =
          typeof body.channelId === "string" ? body.channelId : undefined;
        if (
          !listApplies(resolved.guildIds, guildId) ||
          !listApplies(resolved.channelIds, channelId)
        ) {
          continue;
        }

        const isEncrypted = body.encrypted === true;
        if (isEncrypted && !resolved.allowEncryptedMessages) {
          sendPolicyError(
            socket,
            "ENCRYPTED_PAYLOAD_REJECTED",
            "This relay does not accept encrypted message payloads for this guild/channel.",
            payload,
          );
          return true;
        }

        if (resolved.requireEncryptedMessages) {
          const hasEnvelope =
            isEncrypted &&
            typeof body.iv === "string" &&
            body.iv.length > 0 &&
            typeof body.content === "string" &&
            body.content.length > 0;
          if (!hasEnvelope) {
            sendPolicyError(
              socket,
              "ENCRYPTION_REQUIRED",
              "This relay requires encrypted message payloads for this guild/channel.",
              payload,
            );
            return true;
          }
        }
      }

      return false;
    },
  };
}

function appObjectRuleMatches(
  rule: AppObjectPermissionRule,
  body: { namespace?: unknown; objectType?: unknown },
) {
  return (
    typeof body.namespace === "string" &&
    body.namespace === rule.namespace &&
    (!rule.objectType ||
      (typeof body.objectType === "string" &&
        body.objectType === rule.objectType))
  );
}

function rejectSensitiveAppFields(value: Record<string, unknown> | undefined) {
  if (!value) {
    return undefined;
  }

  const sensitiveKeys = [
    "secret",
    "token",
    "apiKey",
    "api_key",
    "password",
    "privateKey",
    "private_key",
  ];
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
  return [
    "string",
    "integer",
    "number",
    "boolean",
    "user",
    "channel",
    "role",
    "mentionable",
  ].includes(value);
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

function validateManifestRecord(
  value: Record<string, unknown> | undefined,
  maxCommands: number,
) {
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

function validateCommandInvocationRecord(
  value: Record<string, unknown> | undefined,
  maxArgumentLength: number,
) {
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
  if (
    responseMode &&
    responseMode !== "ephemeral" &&
    responseMode !== "public"
  ) {
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
      if (
        typeof optionValue === "string" &&
        optionValue.length > maxArgumentLength
      ) {
        return `Command invocation option ${name} must be ${maxArgumentLength} characters or less.`;
      }
    }
  }

  return rejectSensitiveAppFields(value);
}

function validateCommandResponseRecord(
  value: Record<string, unknown> | undefined,
) {
  if (!value) {
    return "Command response records require a JSON value.";
  }

  const visibility =
    stringField(value, "visibility") || stringField(value, "responseMode");
  if (visibility && visibility !== "ephemeral" && visibility !== "public") {
    return "Command response visibility must be ephemeral or public.";
  }

  const content = stringField(value, "content");
  if (content.length > 4000) {
    return "Command response content must be 4000 characters or less.";
  }

  return rejectSensitiveAppFields(value);
}

function commandNameFromObjectRecord(
  record: Record<string, unknown> | undefined,
) {
  if (!record) return "";
  return (stringField(record, "commandName") || stringField(record, "name"))
    .replace(/^\/+/, "")
    .toLowerCase();
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

function invocationMatchesCommandOptions(
  invocation: Record<string, unknown> | undefined,
  commandValue: Record<string, unknown> | undefined,
) {
  if (!invocation || !commandValue || !Array.isArray(commandValue.options)) {
    return undefined;
  }
  const providedOptions = isRecord(invocation.options)
    ? invocation.options
    : {};
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
    if (
      choices.length > 0 &&
      value !== undefined &&
      value !== null &&
      value !== ""
    ) {
      const validChoice = choices.some(
        (choice) => isRecord(choice) && choice.value === value,
      );
      if (!validChoice) {
        return `Command option ${name} has an unsupported choice.`;
      }
    }
  }
  return undefined;
}

function commandRegistrationMatches(
  record: {
    objectType: string;
    value?: unknown;
    objectId?: string;
    target?: unknown;
  },
  commandObjectType: string,
  appId: string,
  commandName: string,
) {
  if (record.objectType !== commandObjectType || !isRecord(record.value)) {
    return false;
  }
  const target = isRecord(record.target) ? record.target : undefined;
  const registeredAppId =
    stringField(record.value, "appId") || stringField(target, "appId");
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
  commandName: string,
) {
  if (!state) return undefined;
  for (const record of state.appObjects.values()) {
    if (
      commandRegistrationMatches(record, commandObjectType, appId, commandName)
    ) {
      return record;
    }
  }
  return undefined;
}

function findManifestCommandRegistration(
  state: ReturnType<typeof replayGuildState>,
  manifestObjectType: string,
  appId: string,
  commandName: string,
) {
  if (!state) return undefined;
  for (const record of state.appObjects.values()) {
    if (record.objectType !== manifestObjectType || !isRecord(record.value)) {
      continue;
    }
    const target = isRecord(record.target) ? record.target : undefined;
    const manifestAppId =
      stringField(record.value, "appId") ||
      stringField(target, "appId") ||
      record.objectId;
    if (appId && manifestAppId && manifestAppId !== appId) {
      continue;
    }
    const commands = record.value.commands;
    if (!Array.isArray(commands)) {
      continue;
    }
    const command = commands.find((entry): entry is Record<string, unknown> => {
      return (
        isRecord(entry) && commandNameFromObjectRecord(entry) === commandName
      );
    });
    if (command) {
      return {
        ...record,
        value: command,
      };
    }
  }
  return undefined;
}

function memberHasAnyListedRole(
  state: ReturnType<typeof replayGuildState>,
  author: string,
  roleIds: string[],
) {
  if (!state || roleIds.length === 0) {
    return true;
  }
  const member = state.members.get(author);
  if (!member) {
    return false;
  }
  return Array.from(member.roles).some((roleId) => roleIds.includes(roleId));
}

function validateAgentProfileRecord(
  value: Record<string, unknown> | undefined,
) {
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

function getTargetUser(body: {
  target?: unknown;
  value?: unknown;
  objectId?: unknown;
}) {
  const target = isRecord(body.target) ? body.target : undefined;
  const value = isRecord(body.value) ? body.value : undefined;
  return (
    stringField(target, "userId") ||
    stringField(value, "userId") ||
    (typeof body.objectId === "string" ? body.objectId : "")
  );
}

export function createAppSurfacePolicyPlugin(
  policy: AppSurfacePolicy = {},
): RelayPlugin {
  const namespace = policy.namespace?.trim() || "org.cgp.apps";
  const manifestObjectType =
    policy.manifestObjectType?.trim() || "app-manifest";
  const commandObjectType = policy.commandObjectType?.trim() || "slash-command";
  const webhookObjectType = policy.webhookObjectType?.trim() || "webhook";
  const commandInvocationObjectType =
    policy.commandInvocationObjectType?.trim() || "command-invocation";
  const commandResponseObjectType =
    policy.commandResponseObjectType?.trim() || "command-response";
  const agentProfileObjectType =
    policy.agentProfileObjectType?.trim() || "agent-profile";
  const maxManifestCommands = Math.max(
    0,
    Math.floor(policy.maxManifestCommands ?? 50),
  );
  const maxCommandArgumentLength = Math.max(
    0,
    Math.floor(policy.maxCommandArgumentLength ?? 4000),
  );
  const allowSelfAgentProfiles = policy.allowSelfAgentProfiles ?? true;

  return {
    name: "cgp.apps.surface-policy",
    metadata: {
      name: "App, bot, and webhook surface policy",
      description:
        "Relay-local validation and permission policy for portable app-scoped integration records.",
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
        allowSelfAgentProfiles,
      },
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
        (body?.type !== "APP_OBJECT_UPSERT" &&
          body?.type !== "APP_OBJECT_DELETE") ||
        body.namespace !== namespace
      ) {
        return false;
      }

      const objectType =
        typeof body.objectType === "string" ? body.objectType : "";
      const guildId = typeof body.guildId === "string" ? body.guildId : "";
      const author = typeof publish.author === "string" ? publish.author : "";
      if (
        !guildId ||
        !author ||
        typeof body.objectId !== "string" ||
        !body.objectId.trim()
      ) {
        sendPolicyError(
          socket,
          "VALIDATION_FAILED",
          "App objects require guildId, author, and objectId.",
          payload,
        );
        return true;
      }

      const state = await relayGuildState(ctx, guildId);
      if (!state) {
        return false;
      }

      if (objectType === agentProfileObjectType) {
        const targetUser = getTargetUser(body);
        const isSelfProfile =
          allowSelfAgentProfiles && (!targetUser || targetUser === author);
        if (!isSelfProfile && !canModerateScope(state, author, "members")) {
          sendPolicyError(
            socket,
            "VALIDATION_FAILED",
            "Changing another user's agent profile requires member moderation permission.",
            payload,
          );
          return true;
        }
        if (body.type === "APP_OBJECT_UPSERT") {
          const error = validateAgentProfileRecord(
            isRecord(body.value) ? body.value : undefined,
          );
          if (error) {
            sendPolicyError(socket, "VALIDATION_FAILED", error, payload);
            return true;
          }
        }
        return false;
      }

      if (objectType === commandInvocationObjectType) {
        if (body.type !== "APP_OBJECT_UPSERT") {
          sendPolicyError(
            socket,
            "VALIDATION_FAILED",
            "Command invocations cannot be deleted through the invocation surface.",
            payload,
          );
          return true;
        }
        const value = isRecord(body.value) ? body.value : undefined;
        const error = validateCommandInvocationRecord(
          value,
          maxCommandArgumentLength,
        );
        if (error) {
          sendPolicyError(socket, "VALIDATION_FAILED", error, payload);
          return true;
        }

        const target = isRecord(body.target) ? body.target : undefined;
        const channelId =
          stringField(body as Record<string, unknown>, "channelId") ||
          stringField(target, "channelId");
        if (
          !channelId ||
          !canUseChannelPermission(state, author, channelId, "sendMessages")
        ) {
          sendPolicyError(
            socket,
            "VALIDATION_FAILED",
            "Command invocations require send permission in the target channel.",
            payload,
          );
          return true;
        }

        const commandName = commandNameFromObjectRecord(value);
        const appId =
          stringField(value, "appId") || stringField(target, "appId");
        const commandRegistration =
          findCommandRegistration(
            state,
            commandObjectType,
            appId,
            commandName,
          ) ??
          findManifestCommandRegistration(
            state,
            manifestObjectType,
            appId,
            commandName,
          );
        if (!commandRegistration) {
          sendPolicyError(
            socket,
            "VALIDATION_FAILED",
            `Command /${commandName} is not registered for this guild.`,
            payload,
          );
          return true;
        }
        const commandValue = isRecord(commandRegistration.value)
          ? commandRegistration.value
          : undefined;
        const optionError = invocationMatchesCommandOptions(
          value,
          commandValue,
        );
        if (optionError) {
          sendPolicyError(socket, "VALIDATION_FAILED", optionError, payload);
          return true;
        }

        const allowedChannels = Array.isArray(commandValue?.channelIds)
          ? commandValue.channelIds.filter(
              (entry): entry is string => typeof entry === "string",
            )
          : [];
        if (
          allowedChannels.length > 0 &&
          !allowedChannels.includes(channelId)
        ) {
          sendPolicyError(
            socket,
            "VALIDATION_FAILED",
            `Command /${commandName} is not enabled in this channel.`,
            payload,
          );
          return true;
        }
        const allowedRoles = Array.isArray(commandValue?.roleIds)
          ? commandValue.roleIds.filter(
              (entry): entry is string => typeof entry === "string",
            )
          : [];
        if (!memberHasAnyListedRole(state, author, allowedRoles)) {
          sendPolicyError(
            socket,
            "VALIDATION_FAILED",
            `Command /${commandName} is not enabled for this member.`,
            payload,
          );
          return true;
        }

        return false;
      }

      if (objectType === commandResponseObjectType) {
        if (body.type === "APP_OBJECT_UPSERT") {
          const error = validateCommandResponseRecord(
            isRecord(body.value) ? body.value : undefined,
          );
          if (error) {
            sendPolicyError(socket, "VALIDATION_FAILED", error, payload);
            return true;
          }
        }
        const target = isRecord(body.target) ? body.target : undefined;
        const channelId =
          stringField(body as Record<string, unknown>, "channelId") ||
          stringField(target, "channelId");
        if (
          channelId &&
          !canUseChannelPermission(state, author, channelId, "sendMessages")
        ) {
          sendPolicyError(
            socket,
            "VALIDATION_FAILED",
            "Command responses require send permission in the target channel.",
            payload,
          );
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
        sendPolicyError(
          socket,
          "VALIDATION_FAILED",
          `User ${author} does not have permission for ${body.type}`,
          payload,
        );
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
    },
  };
}

function sendJson(
  res: ServerResponse,
  statusCode: number,
  payload: Record<string, unknown>,
) {
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
  const match =
    typeof auth === "string" ? /^Bearer\s+(.+)$/i.exec(auth.trim()) : null;
  return match?.[1]?.trim() ?? "";
}

function secretEnvNameForCredentialRef(credentialRef: string) {
  return `CGP_WEBHOOK_SECRET_${credentialRef
    .replace(/[^a-zA-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toUpperCase()}`;
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

function findWebhookRecord(
  state: ReturnType<typeof replayGuildState>,
  webhookObjectType: string,
  webhookId: string,
) {
  if (!state) return undefined;
  const candidates = new Set([
    webhookId,
    webhookId.startsWith("webhook:")
      ? webhookId.slice("webhook:".length)
      : `webhook:${webhookId}`,
  ]);
  for (const record of state.appObjects.values()) {
    if (
      record.objectType === webhookObjectType &&
      candidates.has(record.objectId)
    ) {
      return record;
    }
  }
  return undefined;
}

export function createWebhookIngressPlugin(
  policy: WebhookIngressPolicy = {},
): RelayPlugin {
  const namespace = policy.namespace?.trim() || "org.cgp.apps";
  const webhookObjectType = policy.webhookObjectType?.trim() || "webhook";
  const allowUnsignedWebhooks = policy.allowUnsignedWebhooks ?? false;
  const maxBodyBytes = Math.max(
    1024,
    Math.floor(
      policy.maxBodyBytes ??
        positiveIntegerFromEnv("CGP_WEBHOOK_MAX_BODY_BYTES", 256 * 1024),
    ),
  );
  const maxContentChars = Math.max(
    1,
    Math.floor(
      policy.maxContentChars ??
        positiveIntegerFromEnv("CGP_WEBHOOK_MAX_CONTENT_CHARS", 4000),
    ),
  );

  return {
    name: "cgp.apps.webhook-ingress",
    metadata: {
      name: "Webhook ingress",
      description:
        "Relay-local HTTP ingress for registered org.cgp.apps webhook objects.",
      version: "1",
      policy: {
        namespace,
        webhookObjectType,
        allowUnsignedWebhooks,
        maxBodyBytes,
        maxContentChars,
      },
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
        sendJson(res, 405, {
          ok: false,
          error: "Webhook ingress requires POST.",
        });
        return true;
      }

      const guildId = pathSegments[1] ?? "";
      const webhookId = pathSegments[2] ?? "";
      if (!guildId || !webhookId) {
        sendJson(res, 400, {
          ok: false,
          error: "Webhook route requires guildId and webhookId.",
        });
        return true;
      }

      const state = await relayGuildState(ctx, guildId);
      const webhook = findWebhookRecord(state, webhookObjectType, webhookId);
      const value = isRecord(webhook?.value) ? webhook.value : undefined;
      const target = isRecord(webhook?.target) ? webhook.target : undefined;
      if (!webhook || !value || webhook.namespace !== namespace) {
        sendJson(res, 404, {
          ok: false,
          error: "Webhook is not registered on this relay.",
        });
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
          sendJson(res, 403, {
            ok: false,
            error: "Webhook credentialRef is not configured on this relay.",
          });
          return true;
        }
        if (token !== expected) {
          sendJson(res, 403, { ok: false, error: "Invalid webhook token." });
          return true;
        }
      } else if (!allowUnsignedWebhooks) {
        sendJson(res, 403, {
          ok: false,
          error: "Webhook requires a credentialRef.",
        });
        return true;
      }

      let body: Record<string, unknown>;
      try {
        body = await readJsonRequestBody(req, maxBodyBytes);
      } catch (error) {
        sendJson(res, 400, {
          ok: false,
          error:
            error instanceof Error ? error.message : "Invalid request body.",
        });
        return true;
      }

      const content = stringField(body, "content");
      if (!content) {
        sendJson(res, 400, {
          ok: false,
          error: "Webhook content is required.",
        });
        return true;
      }
      if (content.length > maxContentChars) {
        sendJson(res, 400, {
          ok: false,
          error: `Webhook content exceeds ${maxContentChars} characters.`,
        });
        return true;
      }

      const channelId =
        webhook.channelId ||
        stringField(target, "channelId") ||
        stringField(value, "channelId");
      if (!channelId) {
        sendJson(res, 409, {
          ok: false,
          error: "Webhook has no target channel.",
        });
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
            nonce: randomUUID(),
          }),
          content,
          external: {
            kind: "webhook",
            webhookId: webhook.objectId,
            username:
              stringField(body, "username") ||
              stringField(value, "name") ||
              undefined,
            avatarUrl:
              stringField(body, "avatarUrl") ||
              stringField(body, "avatar_url") ||
              undefined,
          },
        },
        Date.now(),
      );
      if (!event) {
        sendJson(res, 409, {
          ok: false,
          error: "Relay could not publish the webhook delivery.",
        });
        return true;
      }

      sendJson(res, 202, {
        ok: true,
        guildId,
        channelId,
        eventId: event.id,
        seq: event.seq,
      });
      return true;
    },
  };
}

export function createSafetyReportPlugin(
  policy: SafetyReportPolicy = {},
): RelayPlugin {
  const namespace = policy.namespace?.trim() || "org.cgp.safety";
  const objectType = policy.objectType?.trim() || "report";
  const allowedCategories = Array.from(
    new Set(
      (policy.allowedCategories ?? [])
        .map((category) => category.trim())
        .filter(Boolean),
    ),
  );
  const requireParticipantReporter = policy.requireParticipantReporter ?? true;
  const requireReasonOrCategory = policy.requireReasonOrCategory ?? true;

  return {
    name: "cgp.safety.reports",
    metadata: {
      name: "Safety report validation",
      description:
        "Relay-local policy for validating generic APP_OBJECT safety reports before they are appended.",
      version: "1",
      policy: {
        namespace,
        objectType,
        requireParticipantReporter,
        allowedCategories,
        requireReasonOrCategory,
      },
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
      const objectId =
        typeof body.objectId === "string" ? body.objectId.trim() : "";
      const target = isRecord(body.target) ? body.target : undefined;
      const value = isRecord(body.value) ? body.value : undefined;
      const category = stringField(value, "category");
      const reason = stringField(value, "reason");

      if (!guildId || !author || !objectId) {
        sendPolicyError(
          socket,
          "VALIDATION_FAILED",
          "Safety reports require guildId, author, and objectId.",
          payload,
        );
        return true;
      }

      if (!hasReportTarget(target)) {
        sendPolicyError(
          socket,
          "VALIDATION_FAILED",
          "Safety reports require a target messageId, userId, or channelId.",
          payload,
        );
        return true;
      }

      if (requireReasonOrCategory && !category && !reason) {
        sendPolicyError(
          socket,
          "VALIDATION_FAILED",
          "Safety reports require a category or reason.",
          payload,
        );
        return true;
      }

      if (
        category &&
        allowedCategories.length > 0 &&
        !allowedCategories.includes(category)
      ) {
        sendPolicyError(
          socket,
          "VALIDATION_FAILED",
          `Unsupported safety report category: ${category}`,
          payload,
        );
        return true;
      }

      if (requireParticipantReporter) {
        const state = await relayGuildState(ctx, guildId);
        if (!state) {
          return false;
        }

        const memberRequired =
          state.access === "private" || state.policies.posting === "members";
        if (
          state.bans.has(author) ||
          (memberRequired && !state.members.has(author))
        ) {
          sendPolicyError(
            socket,
            "VALIDATION_FAILED",
            "Reporter is not allowed to participate in this guild.",
            payload,
          );
          return true;
        }
      }

      return false;
    },
  };
}

export function createAppObjectPermissionPlugin(
  policy: AppObjectPermissionPolicy,
): RelayPlugin {
  const rules = policy.rules.filter((rule) => rule.namespace.trim().length > 0);

  return {
    name: "cgp.relay.app-object-permissions",
    metadata: {
      name: "Application object permissions",
      description:
        "Relay-local permission rules for application-defined APP_OBJECT events.",
      version: "1",
      policy: { rules },
    },
    onFrame: async ({ socket, kind, payload }, ctx) => {
      if (kind !== "PUBLISH" || rules.length === 0) {
        return false;
      }

      const publish = payload as {
        author?: unknown;
        body?: {
          type?: unknown;
          guildId?: unknown;
          namespace?: unknown;
          objectType?: unknown;
        };
      };
      const body = publish?.body;
      if (
        body?.type !== "APP_OBJECT_UPSERT" &&
        body?.type !== "APP_OBJECT_DELETE"
      ) {
        return false;
      }

      const rule = rules.find((candidate) =>
        appObjectRuleMatches(candidate, body),
      );
      if (!rule) {
        return false;
      }

      const guildId = typeof body.guildId === "string" ? body.guildId : "";
      const author = typeof publish.author === "string" ? publish.author : "";
      if (!guildId || !author) {
        return false;
      }

      const state = await relayGuildState(ctx, guildId);
      if (!state) {
        return false;
      }

      if (!canModerateScope(state, author, rule.permissionScope)) {
        sendPolicyError(
          socket,
          "VALIDATION_FAILED",
          `User ${author} does not have permission for ${body.type}`,
          payload,
        );
        return true;
      }

      return false;
    },
  };
}
