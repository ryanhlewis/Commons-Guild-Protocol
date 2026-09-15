import { reassembleStaticFiles } from "./static_shard_chunks.js";
import { StaticShardUploadCache } from "./static_shard_upload_cache.js";
import type { IncomingMessage, ServerResponse } from "http";
import { request as httpRequest } from "http";
import { request as httpsRequest } from "https";
import { once } from "node:events";
import { spawn } from "node:child_process";
import { createHash, createHmac, createSign, randomUUID, timingSafeEqual } from "crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { appendFile, mkdir, readFile, rename, rm, stat, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { gzipSync, gunzipSync } from "node:zlib";
import type { WebSocket } from "ws";
import webPush from "web-push";
import { unzipSync } from "fflate";
import {
  applyEvent,
  canModerateScope,
  canUseChannelPermission,
  computeEventId,
  createInitialState,
  DeviceAuthorityRegistry,
  hashObject,
  verify,
  verifyDeviceAuthorizedObject,
  type ChannelId,
  type DeviceAuthorization,
  type EventBody,
  type GuildEvent,
  type GuildId,
  type GuildState,
  type PermissionScope,
  type RelayWriteProposal,
  type RelayWriteQuorumPolicy,
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
   * When true, MESSAGE events in guilds whose signed state is private must
   * carry an encrypted payload envelope.
   */
  requireEncryptedPrivateGuildMessages?: boolean;
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

export interface HollowRoomRelayPolicy {
  /**
   * Public WebSocket URL advertised to Hollow game iframes.
   * Defaults to CGP_HOLLOW_RELAY_PUBLIC_WS_URL, CGP_RELAY_PUBLIC_URL, or the request host.
   */
  publicWsUrl?: string;
  /**
   * Human readable relay label shown in Hollow settings.
   */
  label?: string;
  /**
   * Maximum peers per game room. Defaults to 64.
   */
  maxPeersPerRoom?: number;
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
  ipfsBackendId?: string;
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
  providerId?: string;
  name?: string;
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

export interface ExpressionSearchProviderPolicy {
  id?: string;
  label?: string;
  description?: string;
  endpoint?: string;
  supportedTypes?: string[];
  acceptsMimeTypes?: string[];
  tags?: string[];
  attribution?: string;
}

export type CgpIpfsBackendKind = "helia" | "kubo" | "faux" | "external";

export interface CgpIpfsAddFileInput {
  path?: string;
  bytes?: Uint8Array;
  name?: string;
  mimeType?: string;
  sha256?: string;
  pin?: boolean;
  tags?: string[];
  metadata?: Record<string, string | number | boolean>;
}

export interface CgpIpfsAddFileResult {
  providerId: string;
  backend: CgpIpfsBackendKind;
  cid: string;
  bytes: number;
  sha256: string;
  gatewayUrl?: string;
  storage?: Record<string, unknown>;
}

export type HeliaIpfsMode = "local" | "network";

export interface CgpIpfsBackendStatus {
  id: string;
  kind: CgpIpfsBackendKind;
  started: boolean;
  mode?: HeliaIpfsMode;
  storage?: string;
  storeDir?: string;
  gatewayUrl?: string;
  objects?: number;
  s3Configured?: boolean;
  githubRepository?: string;
  peerId?: string;
  listenAddrs?: string[];
  multiaddrs?: string[];
  protocols?: string[];
  connections?: number;
  peers?: number;
  pins?: number;
  provided?: number;
  error?: string;
}

export interface CgpIpfsBackend {
  id: string;
  kind: CgpIpfsBackendKind;
  addFile(input: CgpIpfsAddFileInput): Promise<CgpIpfsAddFileResult>;
  pin(cid: string, metadata?: Record<string, string | number | boolean>): Promise<void>;
  status(): Promise<CgpIpfsBackendStatus>;
  close?(): Promise<void> | void;
}

export interface HeliaIpfsPolicy {
  id?: string;
  storeDir?: string;
  gatewayUrl?: string;
  maxAddBytes?: number;
  /**
   * local stores and serves CIDs only through this relay. network also starts
   * libp2p transports, Bitswap, routing, and provider announcements.
   */
  mode?: HeliaIpfsMode;
  listenAddrs?: string[];
  announceAddrs?: string[];
  bootstrapAddrs?: string[];
  useDefaultBootstrap?: boolean;
  enableTcp?: boolean;
  enableWebSockets?: boolean;
  enableDht?: boolean;
  enableBitswap?: boolean;
  enableTrustlessGateway?: boolean;
  provideOnAdd?: boolean;
  provideTimeoutMs?: number;
  /**
   * Start Helia during relay init. Defaults false so regular relays do not open
   * IPFS networking until bytes are actually pinned.
   */
  autoStart?: boolean;
  /**
   * Allow HTTP routes under /plugins/cgp.ipfs.helia. Internal plugin calls work
   * even when this is false.
   */
  exposeHttpRoutes?: boolean;
}

export type FauxIpfsStorageKind = "local" | "http" | "s3" | "r2" | "github";

export interface FauxIpfsBackendPolicy {
  id?: string;
  /**
   * local writes relay-local files. http/s3/r2 use PUT/GET URL templates.
   * github writes objects through the GitHub Contents API.
   */
  storage?: FauxIpfsStorageKind;
  storeDir?: string;
  gatewayUrl?: string;
  maxAddBytes?: number;
  exposeHttpRoutes?: boolean;
  keyPrefix?: string;
  putUrlTemplate?: string;
  getUrlTemplate?: string;
  headers?: Record<string, string>;
  s3Endpoint?: string;
  s3Bucket?: string;
  s3Region?: string;
  s3AccessKeyId?: string;
  s3SecretAccessKey?: string;
  s3SessionToken?: string;
  s3ForcePathStyle?: boolean;
  s3PublicBaseUrl?: string;
  r2AccountId?: string;
  githubRepository?: string;
  githubOwner?: string;
  githubRepo?: string;
  githubBranch?: string;
  githubToken?: string;
  githubAppId?: string;
  githubAppPrivateKey?: string;
  githubAppPrivateKeyFile?: string;
  githubAppInstallationId?: string;
  githubBasePath?: string;
  requestTimeoutMs?: number;
}

export type StaticShardSeedKind = "catalog" | "release";

export interface StaticShardSeedSource {
  url: string;
  kind?: StaticShardSeedKind;
  expectedSha256?: string;
}

export interface StaticShardSeedPolicy {
  /**
   * Static shard catalogs or release manifests to ingest. Defaults to
   * CGP_STATIC_SHARD_SEEDS_JSON when present.
   */
  sources?: StaticShardSeedSource[];
  /**
   * Local verified copy of source/media shards. Defaults to CGP_STATIC_SHARD_STORE_DIR
   * or ./relay-shards.
   */
  storeDir?: string;
  /**
   * Ingest configured sources during plugin init. Defaults true when sources are configured.
   */
  autoIngest?: boolean;
  maxShardBytes?: number;
  uploadMaxBytes?: number;
  maxManifestBytes?: number;
  requestTimeoutMs?: number;
  /** Append-only registry entries written before an atomic snapshot compaction. */
  registryCompactEvery?: number;
  /**
   * Require HTTP uploads to carry a valid CGP publisher proof. Defaults true.
   * Configured relay seed sources remain operator-trusted for legacy mirrors.
   */
  requireSignedUploads?: boolean;
  /** Shared bearer/x-cgp-static-shard-token credential for POST /ingest. */
  httpIngestToken?: string;
  /**
   * Permit unauthenticated remote URL ingestion. Defaults false because this
   * route grants the relay outbound fetch and persistent storage authority.
   */
  allowUnauthenticatedHttpIngest?: boolean;
  /**
   * Pin verified shard ZIPs through a Kubo-compatible HTTP API. This never invents CIDs:
   * without an IPFS API response the relay records only SHA-256 verified local storage.
   */
  pinToIpfs?: boolean;
  ipfsApiUrl?: string;
  ipfsBackendId?: string;
  /**
   * Extract verified ZIP shards into a relay-served playable tree. Defaults true;
   * operators can disable it with CGP_STATIC_SHARD_EXTRACT_PLAYABLE=0.
   */
  extractPlayable?: boolean;
  servePlayableMode?: StaticShardPlayableMode;
  allowVerifiedRedirects?: boolean;
  maxExtractedBytes?: number;
  maxExtractedFiles?: number;
  publicHttpUrl?: string;
  publishHollowHomeObjects?: boolean;
  hollowHomeNamespace?: string;
  namespace?: string;
  gameReleaseObjectType?: string;
  shardObjectType?: string;
  createGameGuilds?: boolean;
}

export type StaticShardPlayableMode = "extract" | "redirect" | "auto";

export type GitHubRelayMirrorFrequency =
  | "manual"
  | "per-event"
  | "batch"
  | "interval";

export type GitHubRelayMirrorSourceKind = "manifest" | "jsonl";
export type GitHubRelayMirrorCompression = "none" | "gzip";

export interface GitHubRelayMirrorSource {
  /**
   * Raw GitHub, Pages, or other HTTP(S) URL containing a CGP mirror manifest
   * or relay JSONL backup.
   */
  url: string;
  kind?: GitHubRelayMirrorSourceKind;
  expectedSha256?: string;
}

export interface GitHubRelayMirrorScope {
  /**
   * Empty means every guild hosted by this relay is eligible. Account-wide
   * backups should still be initiated by a client that can prove read access.
   */
  guildIds?: GuildId[];
}

export interface GitHubRelayMirrorPolicy {
  /**
   * Local GitHub-compatible working tree. This is useful for tests, dry runs,
   * and relays that push with an external git daemon.
   */
  mirrorDir?: string;
  /**
   * Path prefix inside the GitHub repo, e.g. cgp/backups/main-relay.
   */
  basePath?: string;
  repository?: string; // owner/repo
  owner?: string;
  repo?: string;
  branch?: string;
  token?: string;
  appId?: string;
  appPrivateKey?: string;
  appPrivateKeyFile?: string;
  appInstallationId?: string;
  /**
   * Optional shared secret required for HTTP writes/imports. Send as bearer or
   * x-cgp-github-mirror-token. Without this, write routes are closed unless
   * allowUnauthenticatedHttpWrites is explicitly true.
   */
  adminToken?: string;
  allowUnauthenticatedHttpWrites?: boolean;
  sources?: GitHubRelayMirrorSource[];
  autoIngest?: boolean;
  autoMirror?: boolean;
  frequency?: GitHubRelayMirrorFrequency;
  compression?: GitHubRelayMirrorCompression;
  batchSize?: number;
  intervalMs?: number;
  scope?: GitHubRelayMirrorScope;
  maxSourceBytes?: number;
  requestTimeoutMs?: number;
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
  writeQuorumPolicy?: RelayWriteQuorumPolicy;
  store: Store;
  ipfsBackends?: Map<string, CgpIpfsBackend>;
  publishAsRelay: (
    body: EventBody,
    createdAt?: number,
  ) => Promise<GuildEvent | undefined>;
  publishSignedEvent?: (event: {
    body: EventBody;
    author: string;
    signature: string;
    createdAt: number;
    clientEventId?: string;
  }) => Promise<GuildEvent | undefined>;
  /**
   * Publish a trusted plugin policy event through sequencer and write-quorum
   * consensus while bypassing the guild's current author membership check.
   * The body must claim the configured write certifier. This is intended for
   * narrowly validated recovery/rotation events, not application messages.
   */
  publishPolicyAuthorizedEvent?: (event: {
    body: EventBody;
    author: string;
    signature: string;
    createdAt: number;
    clientEventId?: string;
  }) => Promise<GuildEvent | undefined>;
  appendEventsFromPlugin?: (
    events: GuildEvent[],
    options?: { broadcast?: boolean; runHooks?: boolean },
  ) => Promise<GuildEvent[]>;
  activateGuildReplication?: (guildId: GuildId) => Promise<void>;
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
  expressionProvider?: Record<string, unknown> | string;
  hollowIntegration?: Record<string, unknown> | string;
  clientExtension?: string;
  clientExtensionPluginId?: string;
  clientExtensionAutoEnable?: boolean;
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
  /**
   * Validate a narrowly scoped policy-authorized proposal independently on
   * each witness. This hook is intentionally unavailable to sandboxed plugins.
   */
  onValidatePolicyAuthorizedEvent?: (
    args: { proposal: RelayWriteProposal },
    ctx: RelayPluginContext,
  ) => boolean | Promise<boolean>;
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

interface HollowRoomPeer {
  peerId: string;
  socket: WebSocket;
  metadata?: Record<string, unknown>;
  joinedAt: number;
}

interface HollowRoomMembership {
  roomId: string;
  peerId: string;
}

function normalizeHollowRoomId(value: unknown) {
  const roomId = typeof value === "string" ? value.trim() : "";
  if (!roomId || roomId.length > 160) {
    return "";
  }
  return /^[A-Za-z0-9._:-]+$/.test(roomId) ? roomId : "";
}

function sanitizeHollowRoomMetadata(value: unknown) {
  if (!isRecord(value)) {
    return undefined;
  }

  const metadata: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value).slice(0, 16)) {
    const normalizedKey = key.trim().slice(0, 64);
    if (!normalizedKey) {
      continue;
    }
    if (
      typeof entry === "string" ||
      typeof entry === "number" ||
      typeof entry === "boolean"
    ) {
      metadata[normalizedKey] =
        typeof entry === "string" ? entry.trim().slice(0, 256) : entry;
    }
  }
  return Object.keys(metadata).length > 0 ? metadata : undefined;
}

function normalizeWebSocketUrl(value: string) {
  const trimmed = value.trim();
  if (!trimmed) {
    return "";
  }
  if (/^wss?:\/\//i.test(trimmed)) {
    return trimmed.replace(/\/+$/, "");
  }
  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed
      .replace(/^https:\/\//i, "wss://")
      .replace(/^http:\/\//i, "ws://")
      .replace(/\/+$/, "");
  }
  return trimmed;
}

function httpUrlFromWebSocketUrl(value: string) {
  return value
    .replace(/^wss:\/\//i, "https://")
    .replace(/^ws:\/\//i, "http://")
    .replace(/\/+$/, "");
}

function requestWebSocketUrl(req: IncomingMessage) {
  const host = String(req.headers.host || "").trim();
  if (!host) {
    return "";
  }

  const forwardedProto = String(req.headers["x-forwarded-proto"] || "")
    .split(",")[0]
    ?.trim()
    .toLowerCase();
  const secure = forwardedProto === "https" || forwardedProto === "wss";
  return `${secure ? "wss" : "ws"}://${host}`;
}

function writeHollowRelayJson(
  res: ServerResponse,
  statusCode: number,
  payload: unknown,
) {
  const body = JSON.stringify(payload);
  res.statusCode = statusCode;
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Content-Length", Buffer.byteLength(body));
  res.end(body);
}

function sendHollowRoomFrame(
  socket: WebSocket,
  kind: string,
  body: Record<string, unknown>,
) {
  if (socket.readyState !== 1) {
    return;
  }
  socket.send(JSON.stringify([kind, body]));
}

export function createHollowRoomRelayPlugin(
  policy: HollowRoomRelayPolicy = {},
): RelayPlugin {
  const rooms = new Map<string, Map<string, HollowRoomPeer>>();
  const socketMemberships = new WeakMap<WebSocket, HollowRoomMembership>();
  const closeListeners = new WeakSet<WebSocket>();
  const maxPeersPerRoom =
    policy.maxPeersPerRoom ??
    positiveIntegerFromEnv("CGP_HOLLOW_RELAY_MAX_PEERS_PER_ROOM", 64);
  const label =
    policy.label?.trim() ||
    process.env.CGP_HOLLOW_RELAY_LABEL?.trim() ||
    "Local Hollow Relay";

  const leaveRoom = (socket: WebSocket) => {
    const membership = socketMemberships.get(socket);
    if (!membership) {
      return;
    }

    socketMemberships.delete(socket);
    const room = rooms.get(membership.roomId);
    if (!room) {
      return;
    }

    const leaving = room.get(membership.peerId);
    room.delete(membership.peerId);
    if (room.size === 0) {
      rooms.delete(membership.roomId);
    }

    for (const peer of room.values()) {
      sendHollowRoomFrame(peer.socket, "PEER_LEFT", {
        roomId: membership.roomId,
        peerId: membership.peerId,
        metadata: leaving?.metadata,
      });
    }
  };

  const roomSnapshot = (room: Map<string, HollowRoomPeer>) =>
    [...room.values()].map((peer) => ({
      peerId: peer.peerId,
      metadata: peer.metadata,
      joinedAt: peer.joinedAt,
    }));

  const activeConnections = () =>
    [...rooms.values()].reduce((total, room) => total + room.size, 0);

  const relaySnapshot = (req?: IncomingMessage) => {
    const publicWsUrl = normalizeWebSocketUrl(
      policy.publicWsUrl ||
        process.env.CGP_HOLLOW_RELAY_PUBLIC_WS_URL ||
        process.env.CGP_RELAY_PUBLIC_URL ||
        (req ? requestWebSocketUrl(req) : ""),
    );
    const publicUrl = publicWsUrl ? httpUrlFromWebSocketUrl(publicWsUrl) : "";
    return {
      relayId: `hollow-room:${publicWsUrl || "local"}`,
      relayPublicKey: undefined,
      provider: "cgp-relay",
      label,
      publicUrl,
      publicWsUrl,
      localHttpUrl: publicUrl,
      localWsUrl: publicWsUrl,
      healthy: Boolean(publicWsUrl),
      source: "local",
      rooms: rooms.size,
      connections: activeConnections(),
      startedAt: Date.now(),
      lastSeenAt: Date.now(),
      transport: "hollow-room",
    };
  };

  return {
    name: "hollow-relay",
    metadata: {
      name: "Hollow room relay",
      description:
        "Low-latency room relay for game invites, Minecraft Web Client sessions, and WebSocket fallback paths.",
      version: "1",
      policy: {
        transport: "hollow-room",
        maxPeersPerRoom,
      },
    },
    inputs: [
      {
        name: "publicWsUrl",
        type: "string",
        required: false,
        description: "Public WebSocket URL advertised to Hollow game clients.",
        placeholder: "ws://relay.example",
        scope: "relay",
      },
    ],
    onFrame: ({ socket, kind, payload }) => {
      if (kind !== "JOIN" && kind !== "DIRECT" && kind !== "LEAVE") {
        return false;
      }

      const body = isRecord(payload) ? payload : {};
      const roomId = normalizeHollowRoomId(body.roomId);
      if (!roomId) {
        sendHollowRoomFrame(socket, "ERROR", {
          code: "INVALID_ROOM",
          message: "JOIN/DIRECT requires a valid roomId.",
        });
        return true;
      }

      if (kind === "LEAVE") {
        leaveRoom(socket);
        sendHollowRoomFrame(socket, "LEFT", { roomId });
        return true;
      }

      if (kind === "JOIN") {
        leaveRoom(socket);
        let room = rooms.get(roomId);
        if (!room) {
          room = new Map();
          rooms.set(roomId, room);
        }

        if (room.size >= maxPeersPerRoom) {
          sendHollowRoomFrame(socket, "ERROR", {
            code: "ROOM_FULL",
            message: "Room is full.",
            roomId,
          });
          return true;
        }

        const peerId = randomUUID();
        const metadata = sanitizeHollowRoomMetadata(body.metadata);
        const existingPeers = roomSnapshot(room);
        const peer: HollowRoomPeer = {
          peerId,
          socket,
          metadata,
          joinedAt: Date.now(),
        };
        room.set(peerId, peer);
        socketMemberships.set(socket, { roomId, peerId });
        if (!closeListeners.has(socket)) {
          closeListeners.add(socket);
          socket.once("close", () => leaveRoom(socket));
          socket.once("error", () => leaveRoom(socket));
        }

        sendHollowRoomFrame(socket, "JOINED", {
          roomId,
          peerId,
          peers: existingPeers,
        });
        for (const existing of room.values()) {
          if (existing.peerId === peerId) {
            continue;
          }
          sendHollowRoomFrame(existing.socket, "PEER_JOINED", {
            roomId,
            peerId,
            metadata,
          });
        }
        return true;
      }

      const membership = socketMemberships.get(socket);
      if (!membership || membership.roomId !== roomId) {
        sendHollowRoomFrame(socket, "ERROR", {
          code: "NOT_IN_ROOM",
          message: "Socket is not joined to that room.",
          roomId,
        });
        return true;
      }

      const toPeerId = typeof body.toPeerId === "string" ? body.toPeerId : "";
      const directPayload =
        typeof body.payload === "string" ? body.payload : undefined;
      const target = rooms.get(roomId)?.get(toPeerId);
      if (!toPeerId || directPayload === undefined || !target) {
        sendHollowRoomFrame(socket, "ERROR", {
          code: "PEER_NOT_FOUND",
          message: "Target peer is not in this room.",
          roomId,
          toPeerId,
        });
        return true;
      }

      sendHollowRoomFrame(target.socket, "DIRECT", {
        roomId,
        fromPeerId: membership.peerId,
        payload: directPayload,
      });
      return true;
    },
    onHttp: ({ req, res, pathSegments }) => {
      if (pathSegments[0] !== "hollow-relay") {
        return false;
      }

      if (req.method === "OPTIONS") {
        writeHollowRelayJson(res, 204, {});
        return true;
      }

      const route = pathSegments[1] || "";
      const snapshot = relaySnapshot(req);
      if (route === "relays") {
        writeHollowRelayJson(res, 200, {
          relays: snapshot.publicWsUrl ? [snapshot] : [],
        });
        return true;
      }

      if (route === "local") {
        if (pathSegments[2] === "refresh" && req.method !== "POST") {
          writeHollowRelayJson(res, 405, {
            error: "Use POST to refresh the Hollow relay advertisement.",
          });
          return true;
        }
        writeHollowRelayJson(res, 200, {
          enabled: true,
          active: Boolean(snapshot.publicWsUrl),
          relay: snapshot,
          tunnel: snapshot,
        });
        return true;
      }

      return false;
    },
    onClose: () => {
      rooms.clear();
    },
  };
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
  const recentSignedAttempts = new Map<string, number>();

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
        clientEventId?: unknown;
        createdAt?: unknown;
        signature?: unknown;
        body?: Record<string, unknown>;
      }>) {
        const author =
          typeof publish?.author === "string" ? publish.author : "";
        const body = isRecord(publish?.body) ? publish.body : undefined;
        if (!author || !body) {
          continue;
        }

        const clientEventId =
          typeof publish.clientEventId === "string"
            ? publish.clientEventId
            : "";
        const signature =
          typeof publish.signature === "string" ? publish.signature : "";
        const createdAt =
          typeof publish.createdAt === "number" &&
          Number.isFinite(publish.createdAt)
            ? publish.createdAt
            : undefined;
        const signedAttemptKey =
          clientEventId && signature && createdAt !== undefined
            ? `${author}\u0000${clientEventId}\u0000${createdAt}\u0000${signature}`
            : "";
        const now = Date.now();
        const priorSignedAttempt = signedAttemptKey
          ? recentSignedAttempts.get(signedAttemptKey)
          : undefined;
        const exactSignedReplay =
          priorSignedAttempt !== undefined &&
          now - priorSignedAttempt <= resolved.windowMs;
        if (signedAttemptKey) {
          recentSignedAttempts.delete(signedAttemptKey);
          recentSignedAttempts.set(signedAttemptKey, now);
          if (recentSignedAttempts.size > 20_000) {
            for (const key of recentSignedAttempts.keys()) {
              if (recentSignedAttempts.size <= 10_000) {
                break;
              }
              recentSignedAttempts.delete(key);
            }
          }
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
            !exactSignedReplay &&
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
          !exactSignedReplay &&
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
            !exactSignedReplay &&
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
    ipfsBackendId: normalizeMediaToken(stringField(entry, "ipfsBackendId")) || undefined,
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
      ipfsBackendId:
        normalizeMediaToken(
          process.env.CGP_MEDIA_IPFS_BACKEND_ID ||
            process.env.CGP_STATIC_SHARD_IPFS_BACKEND_ID ||
            "helia",
        ) || undefined,
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
      ipfsBackendId:
        normalizeMediaToken(
          process.env.CGP_MEDIA_ADULT_IPFS_BACKEND_ID ||
            process.env.CGP_MEDIA_IPFS_BACKEND_ID ||
            process.env.CGP_STATIC_SHARD_IPFS_BACKEND_ID ||
            "helia",
        ) || undefined,
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

function preferredIpfsBackendForProvider(
  provider: MediaStorageProvider,
  ctx: RelayPluginContext,
) {
  if (provider.kind !== "ipfs") return undefined;
  const backends = ctx.ipfsBackends;
  if (!backends || backends.size === 0) return undefined;
  if (provider.ipfsBackendId) {
    return backends.get(provider.ipfsBackendId);
  }
  return backends.get("helia") ?? backends.values().next().value;
}

function mediaUploadEndpointForProvider(
  provider: MediaStorageProvider,
  ctx: RelayPluginContext,
) {
  if (provider.kind !== "ipfs" || provider.endpoint) {
    return provider.endpoint;
  }
  return preferredIpfsBackendForProvider(provider, ctx)
    ? "/plugins/cgp.media.storage/upload"
    : undefined;
}

export function createExpressionSearchProviderPlugin(
  policy: ExpressionSearchProviderPolicy = {},
): RelayPlugin {
  const endpoint = (
    policy.endpoint || process.env.CGP_EXPRESSION_PROVIDER_ENDPOINT || ""
  ).trim();
  let parsedEndpoint: URL;
  try {
    parsedEndpoint = new URL(endpoint);
  } catch {
    throw new Error("Expression search provider requires a valid HTTP(S) endpoint.");
  }
  if (parsedEndpoint.protocol !== "http:" && parsedEndpoint.protocol !== "https:") {
    throw new Error("Expression search provider endpoint must use HTTP or HTTPS.");
  }

  const supportedTypes =
    policy.supportedTypes?.filter(Boolean) ??
    listFromEnv("CGP_EXPRESSION_PROVIDER_TYPES");
  const acceptsMimeTypes =
    policy.acceptsMimeTypes?.filter(Boolean) ??
    listFromEnv("CGP_EXPRESSION_PROVIDER_MIME_TYPES");
  const tags =
    policy.tags?.filter(Boolean) ?? listFromEnv("CGP_EXPRESSION_PROVIDER_TAGS");
  const label =
    policy.label || process.env.CGP_EXPRESSION_PROVIDER_LABEL || "Expression provider";
  const descriptor = {
    schemaVersion: 1,
    id:
      policy.id ||
      process.env.CGP_EXPRESSION_PROVIDER_ID ||
      `expression-${createHash("sha256").update(endpoint).digest("hex").slice(0, 12)}`,
    label,
    description:
      policy.description ||
      process.env.CGP_EXPRESSION_PROVIDER_DESCRIPTION ||
      "External expression search provider advertised by this relay.",
    endpoint,
    supportedTypes: supportedTypes.length > 0 ? supportedTypes : ["gif"],
    acceptsMimeTypes:
      acceptsMimeTypes.length > 0
        ? acceptsMimeTypes
        : ["video/webm", "video/mp4", "image/webp"],
    tags,
    attribution:
      policy.attribution || process.env.CGP_EXPRESSION_PROVIDER_ATTRIBUTION || label,
  };

  return {
    name: "cgp.expression.search",
    metadata: {
      name: label,
      description: descriptor.description,
      version: "1.0.0",
      expressionProvider: descriptor,
      policy: {
        external: true,
        endpoint,
      },
    },
  };
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
    ipfsBackendId: provider.ipfsBackendId,
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
    onHttp: async ({ req, res, pathSegments }, ctx) => {
      if (pathSegments[0] !== "cgp.media.storage") {
        return false;
      }

      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS");
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

        const requestedProviderId = normalizeMediaToken(request.providerId || "");
        const requestedProvider = requestedProviderId
          ? providerById.get(requestedProviderId)
          : undefined;
        const routeRequest = {
          ...request,
          tags: normalizeMediaTags(request.tags),
        };
        const candidates = requestedProvider
          ? providerMatchesMediaRequest(requestedProvider, routeRequest)
            ? [requestedProvider]
            : []
          : mediaProvidersForRequest(providers, routeRequest);
        if (candidates.length === 0) {
          sendJson(res, 409, {
            ok: false,
            error: "No configured media provider accepts this object.",
            request,
          });
          return true;
        }

        const selected = candidates[0];
        const cgpIpfsBackend = preferredIpfsBackendForProvider(selected, ctx);
        const uploadEndpoint = mediaUploadEndpointForProvider(selected, ctx);
        sendJson(res, 200, {
          ok: true,
          selected: publicProvider(selected),
          candidates: candidates.map(publicProvider),
          upload: {
            mode:
              selected.kind === "ipfs" && selected.endpoint
                ? "ipfs-api"
                : selected.kind === "ipfs" && cgpIpfsBackend
                  ? "cgp-ipfs-backend"
                : selected.kind,
            endpoint: uploadEndpoint,
            ipfsBackendId: cgpIpfsBackend?.id,
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
                  ipfsBackendId: selected.kind === "ipfs" ? cgpIpfsBackend?.id : undefined,
                  retention: selected.retention,
                },
              },
            },
          },
        });
        return true;
      }

      if (action === "upload" && req.method === "POST") {
        let body: Record<string, unknown>;
        try {
          body = await readJsonRequestBody(req, Math.ceil(maxAttachmentBytes * 1.4) + 8192);
        } catch (error: any) {
          sendJson(res, 400, {
            ok: false,
            error: error?.message || "Invalid media upload request.",
          });
          return true;
        }

        const base64 = stringField(body, "bytesBase64");
        if (!base64) {
          sendJson(res, 400, { ok: false, error: "Media upload requires bytesBase64." });
          return true;
        }

        let bytes: Buffer;
        try {
          bytes = Buffer.from(base64, "base64");
        } catch {
          sendJson(res, 400, { ok: false, error: "Invalid base64 payload." });
          return true;
        }
        const sha256 = createHash("sha256").update(bytes).digest("hex");
        const requestedSha256 = stringField(body, "sha256").toLowerCase();
        if (requestedSha256 && requestedSha256 !== sha256) {
          sendJson(res, 409, {
            ok: false,
            error: `Media upload hash mismatch: ${sha256} != ${requestedSha256}.`,
          });
          return true;
        }

        const uploadRequest: MediaRouteRequest = {
          guildId: stringField(body, "guildId") || undefined,
          channelId: stringField(body, "channelId") || undefined,
          providerId: stringField(body, "providerId") || undefined,
          name: stringField(body, "name") || undefined,
          type: stringField(body, "type") || undefined,
          mimeType: stringField(body, "mimeType") || "application/octet-stream",
          size: bytes.byteLength,
          tags: normalizeMediaTags(body.tags),
          adult: booleanField(body, "adult"),
          nsfw: booleanField(body, "nsfw"),
          encrypted: booleanField(body, "encrypted"),
          lossless: booleanField(body, "lossless"),
        };
        const requestedProviderId = normalizeMediaToken(uploadRequest.providerId || "");
        const requestedProvider = requestedProviderId
          ? providerById.get(requestedProviderId)
          : undefined;
        const candidates = requestedProvider
          ? providerMatchesMediaRequest(requestedProvider, uploadRequest)
            ? [requestedProvider]
            : []
          : mediaProvidersForRequest(providers, uploadRequest);
        const selected = candidates.find((candidate) =>
          Boolean(preferredIpfsBackendForProvider(candidate, ctx)),
        );
        if (!selected) {
          sendJson(res, 409, {
            ok: false,
            error: requestedProvider
              ? `Media provider ${requestedProvider.id} does not accept this upload or has no CGP IPFS backend.`
              : "No configured IPFS media provider accepts this upload with an available CGP backend.",
            request: uploadRequest,
          });
          return true;
        }

        const backend = preferredIpfsBackendForProvider(selected, ctx);
        if (!backend) {
          sendJson(res, 409, {
            ok: false,
            error: `Media provider ${selected.id} has no available CGP IPFS backend.`,
          });
          return true;
        }

        try {
          const result = await backend.addFile({
            bytes,
            name: uploadRequest.name,
            mimeType: uploadRequest.mimeType,
            sha256,
            pin: booleanField(body, "pin") ?? true,
            tags: uploadRequest.tags,
            metadata: {
              kind: "cgp-media-upload",
              providerId: selected.id,
              mimeType: uploadRequest.mimeType || "",
              tags: uploadRequest.tags?.join(",") || "",
              encrypted: uploadRequest.encrypted === true,
              adult: uploadRequest.adult === true || uploadRequest.nsfw === true,
            },
          });
          const attachment = {
            id: stringField(body, "attachmentId") || `ipfs-${result.cid}`,
            url: `ipfs://${result.cid}`,
            scheme: "ipfs",
            type: uploadRequest.type,
            name: uploadRequest.name,
            mimeType: uploadRequest.mimeType,
            size: result.bytes,
            hash: result.sha256,
            sha256: result.sha256,
            encrypted: uploadRequest.encrypted,
            adult: uploadRequest.adult,
            nsfw: uploadRequest.nsfw,
            external: {
              tags: uploadRequest.tags,
              storage: {
                providerId: selected.id,
                kind: selected.kind,
                ipfsBackendId: backend.id,
                cid: result.cid,
                gatewayUrl: result.gatewayUrl,
                retention: selected.retention,
              },
            },
          };
          sendJson(res, 201, {
            ok: true,
            selected: publicProvider(selected),
            ipfsBackendId: backend.id,
            cid: result.cid,
            gatewayUrl: result.gatewayUrl,
            bytes: result.bytes,
            sha256: result.sha256,
            attachment,
          });
        } catch (error: any) {
          sendJson(res, 409, {
            ok: false,
            error: error?.message || "Media upload failed.",
          });
        }
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

type DynamicImport = (specifier: string) => Promise<any>;
const importEsmFromCommonJs = new Function(
  "specifier",
  "return import(specifier)",
) as DynamicImport;

async function importEsm(specifier: string) {
  if (process.env.VITEST) {
    return await import(specifier);
  }
  return await importEsmFromCommonJs(specifier);
}

const DEFAULT_HELIA_BOOTSTRAP_ADDRS = [
  "/dnsaddr/bootstrap.libp2p.io/ipfs/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
  "/dnsaddr/bootstrap.libp2p.io/ipfs/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
];

function envFlag(name: string, fallback: boolean) {
  const raw = process.env[name]?.trim().toLowerCase();
  if (!raw) return fallback;
  if (["1", "true", "yes", "on"].includes(raw)) return true;
  if (["0", "false", "no", "off"].includes(raw)) return false;
  return fallback;
}

function listFromEnv(name: string) {
  return (process.env[name] || "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function normalizeMultiaddrs(addrs: string[] | undefined) {
  const seen = new Set<string>();
  const normalized: string[] = [];
  for (const addr of addrs ?? []) {
    const value = addr.trim();
    if (!value || seen.has(value)) continue;
    seen.add(value);
    normalized.push(value);
  }
  return normalized;
}

function gatewayUrlForCid(gatewayUrl: string | undefined, cid: string) {
  if (!gatewayUrl || !cid) return undefined;
  return gatewayUrl.includes("{cid}")
    ? gatewayUrl.replace("{cid}", cid)
    : `${gatewayUrl.replace(/\/+$/, "")}/${cid}`;
}

function templateObjectStorageUrl(
  template: string | undefined,
  values: Record<string, string | number | boolean | undefined>,
) {
  if (!template?.trim()) return undefined;
  return template.replace(/\{([A-Za-z0-9_.-]+)\}/g, (_match, key) => {
    const value = String(values[key] ?? "");
    return key === "key"
      ? value.split("/").map(encodeURIComponent).join("/")
      : encodeURIComponent(value);
  });
}

function parseHeaderMapFromEnv(name: string) {
  const raw = process.env[name];
  if (!raw?.trim()) return undefined;
  try {
    const parsed = JSON.parse(raw);
    if (!isRecord(parsed)) return undefined;
    const headers: Record<string, string> = {};
    for (const [key, value] of Object.entries(parsed)) {
      if (typeof value === "string" && key.trim()) {
        headers[key.trim()] = value;
      }
    }
    return Object.keys(headers).length > 0 ? headers : undefined;
  } catch {
    return undefined;
  }
}

function normalizeIpfsMetadata(
  metadata?: Record<string, string | number | boolean>,
) {
  const normalized: Record<string, string | number | boolean> = {};
  for (const [key, value] of Object.entries(metadata ?? {}).slice(0, 32)) {
    const normalizedKey = normalizeMediaToken(key).slice(0, 80);
    if (!normalizedKey) continue;
    if (
      typeof value === "string" ||
      typeof value === "number" ||
      typeof value === "boolean"
    ) {
      normalized[normalizedKey] =
        typeof value === "string" ? value.slice(0, 512) : value;
    }
  }
  return normalized;
}

async function drainAsyncIterable(iterable: AsyncIterable<unknown>) {
  for await (const _entry of iterable) {
    // Drain generator side effects.
  }
}

async function streamUint8Iterable(
  res: ServerResponse,
  iterable: AsyncIterable<Uint8Array>,
) {
  for await (const chunk of iterable) {
    if (res.destroyed) {
      return;
    }
    if (!res.write(Buffer.from(chunk))) {
      await once(res, "drain");
    }
  }
  res.end();
}

async function bufferForIpfsInput(input: CgpIpfsAddFileInput) {
  const bytes =
    input.bytes !== undefined
      ? Buffer.from(input.bytes)
      : input.path
        ? await readFile(input.path)
        : undefined;
  if (!bytes) {
    throw new Error("IPFS add requires either bytes or path.");
  }
  const sha256 = createHash("sha256").update(bytes).digest("hex");
  if (input.sha256 && input.sha256.toLowerCase() !== sha256) {
    throw new Error(`IPFS add hash mismatch: ${sha256} != ${input.sha256}`);
  }
  return { bytes, sha256 };
}

interface FauxIpfsRecord {
  cid: string;
  sha256: string;
  bytes: number;
  key: string;
  name?: string;
  mimeType?: string;
  gatewayUrl?: string;
  createdAt: number;
  metadata?: Record<string, string | number | boolean>;
}

function normalizeFauxIpfsStorage(value: string | undefined): FauxIpfsStorageKind {
  const normalized = value?.trim().toLowerCase();
  return normalized === "http" ||
    normalized === "s3" ||
    normalized === "r2" ||
    normalized === "github"
    ? normalized
    : "local";
}

function normalizeFauxCid(value: string) {
  const cid = value.trim();
  return /^[A-Za-z0-9]+$/.test(cid) ? cid : "";
}

async function rawCidForBytes(bytes: Buffer) {
  const [{ CID }, { sha256: multiformatSha256 }] = await Promise.all([
    importEsm("multiformats/cid"),
    importEsm("multiformats/hashes/sha2"),
  ]);
  const digest = await multiformatSha256.digest(bytes);
  return CID.createV1(0x55, digest).toString();
}

function fauxIpfsRecordKey(prefix: string, cid: string) {
  return [safeStaticRelativePath(prefix), cid].filter(Boolean).join("/");
}

function fauxIpfsTemplateValues(record: FauxIpfsRecord) {
  return {
    cid: record.cid,
    sha256: record.sha256,
    key: record.key,
    name: record.name || record.cid,
    bytes: record.bytes,
  };
}

function gatewayUrlForFauxRecord(gatewayUrl: string | undefined, record: FauxIpfsRecord) {
  if (!gatewayUrl) return undefined;
  return /\{(?:cid|sha256|key|name|bytes)\}/.test(gatewayUrl)
    ? templateObjectStorageUrl(gatewayUrl, fauxIpfsTemplateValues(record))
    : gatewayUrlForCid(gatewayUrl, record.cid);
}

async function putHttpObject(
  url: string,
  bytes: Buffer,
  headers: Record<string, string>,
  record: FauxIpfsRecord,
  timeoutMs: number,
) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "PUT",
      headers: {
        "content-type": record.mimeType || "application/octet-stream",
        ...headers,
      },
      body: bytes.buffer.slice(
        bytes.byteOffset,
        bytes.byteOffset + bytes.byteLength,
      ) as ArrayBuffer,
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`Object store PUT returned HTTP ${response.status}: ${await response.text()}`);
    }
  } finally {
    clearTimeout(timer);
  }
}

async function fetchHttpObject(url: string, headers: Record<string, string>, timeoutMs: number) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "GET",
      headers,
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`Object store GET returned HTTP ${response.status}: ${await response.text()}`);
    }
    return Buffer.from(await response.arrayBuffer());
  } finally {
    clearTimeout(timer);
  }
}

async function fetchGitHubContent(options: {
  owner: string;
  repo: string;
  branch: string;
  token: string;
  path: string;
  timeoutMs: number;
}) {
  const encodedPath = options.path.split("/").map(encodeURIComponent).join("/");
  const apiUrl = `https://api.github.com/repos/${encodeURIComponent(options.owner)}/${encodeURIComponent(options.repo)}/contents/${encodedPath}?ref=${encodeURIComponent(options.branch)}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), options.timeoutMs);
  try {
    const response = await fetch(apiUrl, {
      headers: {
        accept: "application/vnd.github+json",
        authorization: `Bearer ${options.token}`,
        "user-agent": "cgp-faux-ipfs-backend",
        "x-github-api-version": "2022-11-28",
      },
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`GitHub contents API returned HTTP ${response.status}: ${await response.text()}`);
    }
    const body = await response.json() as Record<string, unknown>;
    const content = stringField(body, "content").replace(/\s+/g, "");
    if (!content) {
      throw new Error(`GitHub object ${options.path} had no base64 content.`);
    }
    return Buffer.from(content, "base64");
  } finally {
    clearTimeout(timer);
  }
}

interface S3CompatibleConfig {
  endpoint: string;
  bucket: string;
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  forcePathStyle: boolean;
}

function dateForAwsSigV4(now = new Date()) {
  const amzDate = now.toISOString().replace(/[:-]|\.\d{3}/g, "");
  return {
    amzDate,
    dateStamp: amzDate.slice(0, 8),
  };
}

function hashHex(data: string | Buffer) {
  return createHash("sha256").update(data).digest("hex");
}

function hmacSha256(key: string | Buffer, data: string) {
  return createHmac("sha256", key).update(data).digest();
}

function awsSigV4EncodePath(value: string) {
  return value
    .split("/")
    .map((part) => encodeURIComponent(part).replace(/[!'()*]/g, (char) =>
      `%${char.charCodeAt(0).toString(16).toUpperCase()}`,
    ))
    .join("/");
}

function s3ObjectUrl(config: S3CompatibleConfig, key: string) {
  const endpoint = new URL(config.endpoint.endsWith("/") ? config.endpoint : `${config.endpoint}/`);
  const bucket = encodeURIComponent(config.bucket);
  const encodedKey = awsSigV4EncodePath(key);
  if (config.forcePathStyle) {
    endpoint.pathname = `/${bucket}/${encodedKey}`;
  } else {
    endpoint.hostname = `${config.bucket}.${endpoint.hostname}`;
    endpoint.pathname = `/${encodedKey}`;
  }
  endpoint.search = "";
  return endpoint;
}

async function signedS3CompatibleRequest(
  config: S3CompatibleConfig,
  method: "GET" | "PUT",
  record: FauxIpfsRecord,
  body?: Buffer,
  timeoutMs = 30_000,
) {
  const url = s3ObjectUrl(config, record.key);
  const payload = body ?? Buffer.alloc(0);
  const payloadHash = hashHex(payload);
  const { amzDate, dateStamp } = dateForAwsSigV4();
  const headers: Record<string, string> = {
    host: url.host,
    "x-amz-content-sha256": payloadHash,
    "x-amz-date": amzDate,
  };
  if (method === "PUT") {
    headers["content-type"] = record.mimeType || "application/octet-stream";
  }
  if (config.sessionToken) {
    headers["x-amz-security-token"] = config.sessionToken;
  }
  const signedHeaderNames = Object.keys(headers).sort();
  const canonicalHeaders = signedHeaderNames
    .map((name) => `${name}:${headers[name].trim().replace(/\s+/g, " ")}\n`)
    .join("");
  const signedHeaders = signedHeaderNames.join(";");
  const canonicalRequest = [
    method,
    url.pathname || "/",
    "",
    canonicalHeaders,
    signedHeaders,
    payloadHash,
  ].join("\n");
  const credentialScope = `${dateStamp}/${config.region}/s3/aws4_request`;
  const stringToSign = [
    "AWS4-HMAC-SHA256",
    amzDate,
    credentialScope,
    hashHex(canonicalRequest),
  ].join("\n");
  const dateKey = hmacSha256(`AWS4${config.secretAccessKey}`, dateStamp);
  const regionKey = hmacSha256(dateKey, config.region);
  const serviceKey = hmacSha256(regionKey, "s3");
  const signingKey = hmacSha256(serviceKey, "aws4_request");
  const signature = createHmac("sha256", signingKey).update(stringToSign).digest("hex");
  headers.authorization =
    `AWS4-HMAC-SHA256 Credential=${config.accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method,
      headers,
      body: method === "PUT" && body
        ? body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength) as ArrayBuffer
        : undefined,
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`S3-compatible ${method} returned HTTP ${response.status}: ${await response.text()}`);
    }
    return response;
  } finally {
    clearTimeout(timer);
  }
}

export function createFauxIpfsBackendPlugin(
  policy: FauxIpfsBackendPolicy = {},
): RelayPlugin {
  const id = normalizeMediaToken(policy.id || process.env.CGP_IPFS_FAUX_ID || "faux");
  const storage = normalizeFauxIpfsStorage(
    policy.storage || process.env.CGP_IPFS_FAUX_STORAGE,
  );
  const storeDir = path.resolve(
    policy.storeDir ||
      process.env.CGP_IPFS_FAUX_STORE_DIR ||
      "./relay-ipfs-faux",
  );
  const registryPath = path.join(storeDir, "index.json");
  const objectDir = path.join(storeDir, "objects");
  const keyPrefix =
    safeStaticRelativePath(policy.keyPrefix || process.env.CGP_IPFS_FAUX_KEY_PREFIX || "") ||
    "";
  const maxAddBytes = Math.max(
    1,
    Math.floor(
      policy.maxAddBytes ??
        positiveIntegerFromEnv("CGP_IPFS_FAUX_MAX_ADD_BYTES", 25 * 1024 * 1024),
    ),
  );
  const exposeHttpRoutes =
    policy.exposeHttpRoutes ?? process.env.CGP_IPFS_FAUX_HTTP_ROUTES !== "0";
  const configuredGatewayUrl =
    policy.gatewayUrl ||
    process.env.CGP_IPFS_FAUX_GATEWAY_URL ||
    policy.getUrlTemplate ||
    process.env.CGP_IPFS_FAUX_GET_URL_TEMPLATE ||
    "";
  const putUrlTemplate =
    policy.putUrlTemplate || process.env.CGP_IPFS_FAUX_PUT_URL_TEMPLATE;
  const getUrlTemplate =
    policy.getUrlTemplate || process.env.CGP_IPFS_FAUX_GET_URL_TEMPLATE;
  const headers =
    policy.headers || parseHeaderMapFromEnv("CGP_IPFS_FAUX_HEADERS_JSON") || {};
  const r2AccountId =
    policy.r2AccountId || process.env.CGP_IPFS_FAUX_R2_ACCOUNT_ID || "";
  const s3Endpoint =
    policy.s3Endpoint ||
    process.env.CGP_IPFS_FAUX_S3_ENDPOINT ||
    (storage === "r2" && r2AccountId
      ? `https://${r2AccountId}.r2.cloudflarestorage.com`
      : "");
  const s3Bucket =
    policy.s3Bucket ||
    process.env.CGP_IPFS_FAUX_S3_BUCKET ||
    (storage === "r2" ? process.env.CGP_IPFS_FAUX_R2_BUCKET || "" : "");
  const s3Region =
    policy.s3Region ||
    process.env.CGP_IPFS_FAUX_S3_REGION ||
    (storage === "r2" ? "auto" : "us-east-1");
  const s3AccessKeyId =
    policy.s3AccessKeyId ||
    process.env.CGP_IPFS_FAUX_S3_ACCESS_KEY_ID ||
    (storage === "r2" ? process.env.CGP_IPFS_FAUX_R2_ACCESS_KEY_ID || "" : "");
  const s3SecretAccessKey =
    policy.s3SecretAccessKey ||
    process.env.CGP_IPFS_FAUX_S3_SECRET_ACCESS_KEY ||
    (storage === "r2" ? process.env.CGP_IPFS_FAUX_R2_SECRET_ACCESS_KEY || "" : "");
  const s3SessionToken =
    policy.s3SessionToken || process.env.CGP_IPFS_FAUX_S3_SESSION_TOKEN || "";
  const s3ForcePathStyle =
    policy.s3ForcePathStyle ??
    envFlag("CGP_IPFS_FAUX_S3_FORCE_PATH_STYLE", storage === "r2");
  const s3PublicBaseUrl =
    policy.s3PublicBaseUrl ||
    process.env.CGP_IPFS_FAUX_S3_PUBLIC_BASE_URL ||
    (storage === "r2" ? process.env.CGP_IPFS_FAUX_R2_PUBLIC_BASE_URL || "" : "");
  const gatewayUrl =
    configuredGatewayUrl ||
    (s3PublicBaseUrl ? `${s3PublicBaseUrl.replace(/\/+$/, "")}/{key}` : "") ||
    "/plugins/cgp.ipfs.faux/ipfs/{cid}";
  const s3Config: S3CompatibleConfig | undefined =
    (storage === "s3" || storage === "r2") &&
    s3Endpoint &&
    s3Bucket &&
    s3AccessKeyId &&
    s3SecretAccessKey
      ? {
          endpoint: s3Endpoint,
          bucket: s3Bucket,
          region: s3Region,
          accessKeyId: s3AccessKeyId,
          secretAccessKey: s3SecretAccessKey,
          sessionToken: s3SessionToken || undefined,
          forcePathStyle: s3ForcePathStyle,
        }
      : undefined;
  const requestTimeoutMs = Math.max(
    1000,
    Math.floor(
      policy.requestTimeoutMs ??
        positiveIntegerFromEnv("CGP_IPFS_FAUX_REQUEST_TIMEOUT_MS", 30_000),
    ),
  );
  const [githubOwner, githubRepo] = normalizeGitHubRepository(
    policy.githubOwner || process.env.CGP_IPFS_FAUX_GITHUB_OWNER || "",
    policy.githubRepo || process.env.CGP_IPFS_FAUX_GITHUB_REPO || "",
    policy.githubRepository ||
      process.env.CGP_IPFS_FAUX_GITHUB_REPOSITORY ||
      "",
  );
  const githubBranch =
    policy.githubBranch || process.env.CGP_IPFS_FAUX_GITHUB_BRANCH || "main";
  const githubToken =
    policy.githubToken ||
    process.env.CGP_IPFS_FAUX_GITHUB_TOKEN ||
    process.env.CGP_GITHUB_MIRROR_TOKEN ||
    process.env.CGP_GITHUB_TOKEN ||
    "";
  const githubAppId =
    policy.githubAppId ||
    process.env.CGP_IPFS_FAUX_GITHUB_APP_ID ||
    process.env.CGP_GITHUB_MIRROR_APP_ID ||
    process.env.CGP_GITHUB_APP_ID ||
    "";
  const githubAppPrivateKey =
    normalizeGitHubAppPrivateKey(
      policy.githubAppPrivateKey ||
        process.env.CGP_IPFS_FAUX_GITHUB_APP_PRIVATE_KEY ||
        process.env.CGP_GITHUB_MIRROR_APP_PRIVATE_KEY ||
        process.env.CGP_GITHUB_APP_PRIVATE_KEY ||
        "",
    );
  const githubAppPrivateKeyFile =
    policy.githubAppPrivateKeyFile ||
    process.env.CGP_IPFS_FAUX_GITHUB_APP_PRIVATE_KEY_FILE ||
    process.env.CGP_GITHUB_MIRROR_APP_PRIVATE_KEY_FILE ||
    process.env.CGP_GITHUB_APP_PRIVATE_KEY_FILE ||
    "";
  const githubAppInstallationId =
    policy.githubAppInstallationId ||
    process.env.CGP_IPFS_FAUX_GITHUB_APP_INSTALLATION_ID ||
    process.env.CGP_GITHUB_MIRROR_APP_INSTALLATION_ID ||
    process.env.CGP_GITHUB_APP_INSTALLATION_ID ||
    "";
  const githubBasePath =
    safeStaticRelativePath(
      policy.githubBasePath || process.env.CGP_IPFS_FAUX_GITHUB_BASE_PATH || "ipfs",
    ) || "ipfs";

  const records = new Map<string, FauxIpfsRecord>();
  let started = false;
  let lastError = "";
  let resolvedGitHubAppPrivateKey = githubAppPrivateKey;
  let githubInstallationToken:
    | { token: string; expiresAt: number }
    | undefined;

  const hasFauxGitHubAuth = () =>
    Boolean(
      githubToken ||
        (githubAppId &&
          githubAppInstallationId &&
          (resolvedGitHubAppPrivateKey || githubAppPrivateKeyFile)),
    );

  const resolveFauxGitHubToken = async () => {
    if (githubToken) return githubToken;
    if (!githubAppId || !githubAppInstallationId) return "";
    if (!resolvedGitHubAppPrivateKey && githubAppPrivateKeyFile) {
      resolvedGitHubAppPrivateKey = normalizeGitHubAppPrivateKey(
        await readFile(path.resolve(githubAppPrivateKeyFile), "utf8"),
      );
    }
    if (!resolvedGitHubAppPrivateKey) return "";
    const now = Date.now();
    if (githubInstallationToken && githubInstallationToken.expiresAt - now > 60_000) {
      return githubInstallationToken.token;
    }
    const next = await createGitHubInstallationAccessToken({
      appId: githubAppId,
      privateKey: resolvedGitHubAppPrivateKey,
      installationId: githubAppInstallationId,
      timeoutMs: requestTimeoutMs,
    });
    githubInstallationToken = next;
    return next.token;
  };

  const defaultRecordForCid = (cid: string): FauxIpfsRecord => {
    const current = records.get(cid);
    if (current) return current;
    const record: FauxIpfsRecord = {
      cid,
      sha256: "",
      bytes: 0,
      key: fauxIpfsRecordKey(keyPrefix, cid),
      gatewayUrl: undefined,
      createdAt: 0,
    };
    record.gatewayUrl = gatewayUrlForFauxRecord(gatewayUrl, record);
    return record;
  };

  const persist = async () => {
    if (storage !== "local") return;
    await mkdir(storeDir, { recursive: true });
    await writeFile(
      registryPath,
      JSON.stringify({ kind: "cgp-faux-ipfs-registry", records: Array.from(records.values()) }, null, 2),
    );
  };

  const loadRegistry = async () => {
    if (storage !== "local") return;
    await mkdir(objectDir, { recursive: true });
    try {
      const parsed = JSON.parse(await readFile(registryPath, "utf8"));
      const entries = Array.isArray(parsed?.records) ? parsed.records : [];
      for (const entry of entries) {
        if (!isRecord(entry)) continue;
        const cid = normalizeFauxCid(stringField(entry, "cid"));
        const sha256 = stringField(entry, "sha256").toLowerCase();
        const key = safeStaticRelativePath(stringField(entry, "key"));
        if (!cid || !/^[0-9a-f]{64}$/.test(sha256) || !key) continue;
        records.set(cid, {
          cid,
          sha256,
          key,
          bytes: Math.max(0, Math.floor(numberField(entry, "bytes") || 0)),
          name: stringField(entry, "name") || undefined,
          mimeType: stringField(entry, "mimeType") || undefined,
          gatewayUrl: stringField(entry, "gatewayUrl") || undefined,
          createdAt: Math.max(0, Math.floor(numberField(entry, "createdAt") || Date.now())),
          metadata: isRecord(entry.metadata)
            ? normalizeIpfsMetadata(entry.metadata as Record<string, string | number | boolean>)
            : undefined,
        });
      }
    } catch {
      // Missing local registry is fine for a new relay.
    }
  };

  const writeObject = async (record: FauxIpfsRecord, bytes: Buffer) => {
    if (storage === "local") {
      await mkdir(objectDir, { recursive: true });
      const filePath = path.join(objectDir, record.cid);
      await writeFile(filePath, bytes);
      return;
    }
    if (storage === "github") {
      const token = await resolveFauxGitHubToken();
      if (!githubOwner || !githubRepo || !token) {
        throw new Error("Faux IPFS GitHub storage requires repository and token or GitHub App installation auth.");
      }
      await putGitHubContent({
        owner: githubOwner,
        repo: githubRepo,
        branch: githubBranch,
        token,
        path: joinGitHubMirrorPath(githubBasePath, record.key),
        bytes,
        message: `CGP faux IPFS object ${record.cid}`,
        timeoutMs: requestTimeoutMs,
      });
      return;
    }
    if ((storage === "s3" || storage === "r2") && s3Config && !putUrlTemplate) {
      await signedS3CompatibleRequest(
        s3Config,
        "PUT",
        record,
        bytes,
        requestTimeoutMs,
      );
      return;
    }
    const putUrl = templateObjectStorageUrl(putUrlTemplate, fauxIpfsTemplateValues(record));
    if (!putUrl) {
      throw new Error(
        `Faux IPFS ${storage} storage requires either S3-compatible credentials or putUrlTemplate.`,
      );
    }
    await putHttpObject(putUrl, bytes, headers, record, requestTimeoutMs);
  };

  const readObject = async (record: FauxIpfsRecord) => {
    if (storage === "local") {
      const filePath = path.join(objectDir, record.cid);
      assertInsideDirectory(objectDir, filePath);
      return await readFile(filePath);
    }
    if (storage === "github" && !getUrlTemplate) {
      const token = await resolveFauxGitHubToken();
      if (!githubOwner || !githubRepo || !token) {
        throw new Error("Faux IPFS GitHub reads require repository and token, GitHub App installation auth, or getUrlTemplate.");
      }
      return await fetchGitHubContent({
        owner: githubOwner,
        repo: githubRepo,
        branch: githubBranch,
        token,
        path: joinGitHubMirrorPath(githubBasePath, record.key),
        timeoutMs: requestTimeoutMs,
      });
    }
    if ((storage === "s3" || storage === "r2") && s3Config && !getUrlTemplate) {
      const response = await signedS3CompatibleRequest(
        s3Config,
        "GET",
        record,
        undefined,
        requestTimeoutMs,
      );
      return Buffer.from(await response.arrayBuffer());
    }
    const getUrl = templateObjectStorageUrl(getUrlTemplate || gatewayUrl, fauxIpfsTemplateValues(record));
    if (!getUrl || !safeUrl(getUrl)) {
      throw new Error(`Faux IPFS ${storage} storage has no fetchable getUrlTemplate.`);
    }
    return await fetchHttpObject(getUrl, headers, requestTimeoutMs);
  };

  const backend: CgpIpfsBackend = {
    id,
    kind: "faux",
    addFile: async (input) => {
      const { bytes, sha256 } = await bufferForIpfsInput(input);
      if (bytes.byteLength > maxAddBytes) {
        throw new Error(
          `Faux IPFS add exceeds ${maxAddBytes} byte limit (${bytes.byteLength}).`,
        );
      }
      const cid = await rawCidForBytes(bytes);
      const record: FauxIpfsRecord = {
        cid,
        sha256,
        bytes: bytes.byteLength,
        key: fauxIpfsRecordKey(keyPrefix, cid),
        name: input.name,
        mimeType: input.mimeType,
        createdAt: Date.now(),
        metadata: normalizeIpfsMetadata({
          provider: id,
          backend: "faux",
          storage,
          name: input.name || "",
          sha256,
          bytes: bytes.byteLength,
          mimeType: input.mimeType || "",
          ...(input.metadata ?? {}),
        }),
      };
      record.gatewayUrl = gatewayUrlForFauxRecord(gatewayUrl, record);
      if (!records.has(cid)) {
        await writeObject(record, bytes);
      }
      records.set(cid, record);
      await persist();
      return {
        providerId: id,
        backend: "faux",
        cid,
        bytes: bytes.byteLength,
        sha256,
        gatewayUrl: record.gatewayUrl,
        storage: {
          kind: storage,
          key: record.key,
          syntheticIpfs: true,
        },
      };
    },
    pin: async (cid, metadata) => {
      const normalizedCid = normalizeFauxCid(cid);
      if (!normalizedCid) {
        throw new Error("Faux IPFS pin requires a valid CID.");
      }
      const current = records.get(normalizedCid);
      records.set(normalizedCid, {
        cid: normalizedCid,
        sha256: current?.sha256 || "",
        bytes: current?.bytes || 0,
        key: current?.key || fauxIpfsRecordKey(keyPrefix, normalizedCid),
        name: current?.name,
        mimeType: current?.mimeType,
        gatewayUrl: current?.gatewayUrl || gatewayUrlForFauxRecord(gatewayUrl, {
          cid: normalizedCid,
          sha256: current?.sha256 || "",
          bytes: current?.bytes || 0,
          key: current?.key || fauxIpfsRecordKey(keyPrefix, normalizedCid),
          createdAt: current?.createdAt || Date.now(),
        }),
        createdAt: current?.createdAt || Date.now(),
        metadata: normalizeIpfsMetadata({
          ...(current?.metadata ?? {}),
          ...(metadata ?? {}),
        }),
      });
      await persist();
    },
    status: async () => ({
      id,
      kind: "faux",
      started,
      storage,
      storeDir: storage === "local" ? storeDir : undefined,
      gatewayUrl,
      objects: records.size,
      s3Configured: Boolean(s3Config),
      githubRepository: githubOwner && githubRepo ? `${githubOwner}/${githubRepo}` : undefined,
      githubAuthMode: githubToken
        ? "token"
        : hasFauxGitHubAuth()
          ? "app-installation"
          : "none",
      error: lastError || undefined,
    }),
  };

  return {
    name: "cgp.ipfs.faux",
    metadata: {
      name: "Faux IPFS object backend",
      description:
        "Content-addressed compatibility backend that lets serverless relays use local files, R2/S3-style object URLs, or GitHub while exposing IPFS-shaped CGP storage.",
      version: "1",
      policy: {
        id,
        storage,
        gatewayUrl,
        maxAddBytes,
        exposeHttpRoutes,
        keyPrefix,
        s3Configured: Boolean(s3Config),
        s3Bucket: s3Bucket || undefined,
        s3Endpoint: s3Endpoint || undefined,
        r2AccountId: r2AccountId || undefined,
        s3PublicBaseUrl: s3PublicBaseUrl || undefined,
        githubRepository: githubOwner && githubRepo ? `${githubOwner}/${githubRepo}` : undefined,
        githubUploadConfigured: Boolean(githubOwner && githubRepo && hasFauxGitHubAuth()),
        githubAuthMode: githubToken
          ? "token"
          : hasFauxGitHubAuth()
            ? "app-installation"
            : "none",
      },
    },
    inputs: [
      {
        name: "storage",
        type: "string",
        required: false,
        description: "local, http, s3, r2, or github.",
        placeholder: "r2",
        scope: "relay",
      },
      {
        name: "s3Endpoint",
        type: "string",
        required: false,
        description: "S3-compatible endpoint URL. R2 can use r2AccountId instead.",
        placeholder: "https://s3.us-east-1.amazonaws.com",
        scope: "relay",
      },
      {
        name: "r2AccountId",
        type: "string",
        required: false,
        description: "Cloudflare R2 account id for deriving the S3 endpoint.",
        placeholder: "account-id",
        scope: "relay",
      },
      {
        name: "s3Bucket",
        type: "string",
        required: false,
        description: "S3 or R2 bucket name.",
        placeholder: "hollow-media",
        scope: "relay",
      },
      {
        name: "s3AccessKeyId",
        type: "string",
        required: false,
        sensitive: true,
        description: "S3-compatible access key id.",
        scope: "relay",
      },
      {
        name: "s3SecretAccessKey",
        type: "string",
        required: false,
        sensitive: true,
        description: "S3-compatible secret access key.",
        scope: "relay",
      },
      {
        name: "githubRepository",
        type: "string",
        required: false,
        description: "GitHub owner/repo used when storage is github.",
        placeholder: "owner/repo",
        scope: "relay",
      },
      {
        name: "githubToken",
        type: "string",
        required: false,
        sensitive: true,
        description: "Fallback GitHub token with contents write access.",
        scope: "relay",
      },
      {
        name: "githubAppInstallationId",
        type: "string",
        required: false,
        sensitive: true,
        description: "GitHub App installation id for scoped object storage.",
        scope: "relay",
      },
      {
        name: "githubAppId",
        type: "string",
        required: false,
        description: "GitHub App id used to mint short-lived installation tokens.",
        scope: "relay",
      },
      {
        name: "githubAppPrivateKey",
        type: "string",
        required: false,
        sensitive: true,
        description: "GitHub App PEM private key, or use githubAppPrivateKeyFile.",
        scope: "relay",
      },
      {
        name: "githubAppPrivateKeyFile",
        type: "string",
        required: false,
        sensitive: true,
        description: "Path to a GitHub App PEM private key file.",
        scope: "relay",
      },
    ],
    onInit: async (ctx) => {
      await loadRegistry();
      started = true;
      ctx.ipfsBackends?.set(id, backend);
    },
    onHttp: async ({ req, res, pathSegments }) => {
      if (pathSegments[0] !== "cgp.ipfs.faux") {
        return false;
      }
      if (!exposeHttpRoutes) {
        sendJson(res, 404, { ok: false, error: "Faux IPFS HTTP routes are disabled." });
        return true;
      }
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS");
      res.setHeader("Access-Control-Allow-Headers", "Content-Type");
      if (req.method === "OPTIONS") {
        res.statusCode = 204;
        res.end();
        return true;
      }
      const action = pathSegments[1] || "status";
      if (action === "status" && req.method === "GET") {
        sendJson(res, 200, { ok: true, backend: await backend.status() });
        return true;
      }
      if (action === "add" && req.method === "POST") {
        try {
          const body = await readJsonRequestBody(req, Math.ceil(maxAddBytes * 1.4) + 4096);
          const base64 = stringField(body, "bytesBase64");
          if (!base64) {
            sendJson(res, 400, { ok: false, error: "Faux IPFS add requires bytesBase64." });
            return true;
          }
          const bytes = Buffer.from(base64, "base64");
          const result = await backend.addFile({
            bytes,
            name: stringField(body, "name") || undefined,
            mimeType: stringField(body, "mimeType") || undefined,
            sha256: stringField(body, "sha256") || undefined,
            pin: booleanField(body, "pin") ?? true,
            metadata: isRecord(body.metadata)
              ? normalizeIpfsMetadata(body.metadata as Record<string, string | number | boolean>)
              : undefined,
          });
          sendJson(res, 201, { ok: true, ...result });
        } catch (error: any) {
          lastError = error?.message || String(error);
          sendJson(res, 409, { ok: false, error: lastError || "Faux IPFS add failed." });
        }
        return true;
      }
      if (action === "pin" && req.method === "POST") {
        try {
          const body = await readJsonRequestBody(req, 32 * 1024);
          await backend.pin(stringField(body, "cid"), isRecord(body.metadata)
            ? normalizeIpfsMetadata(body.metadata as Record<string, string | number | boolean>)
            : undefined);
          sendJson(res, 200, { ok: true });
        } catch (error: any) {
          lastError = error?.message || String(error);
          sendJson(res, 409, { ok: false, error: lastError || "Faux IPFS pin failed." });
        }
        return true;
      }
      if (action === "ipfs" && (req.method === "GET" || req.method === "HEAD")) {
        const cid = normalizeFauxCid(pathSegments[2] || "");
        const record = cid ? records.get(cid) ?? defaultRecordForCid(cid) : undefined;
        if (!cid || !record) {
          sendJson(res, 404, { ok: false, error: "CID was not found in this faux IPFS backend." });
          return true;
        }
        try {
          const bytes = req.method === "HEAD" ? Buffer.alloc(0) : await readObject(record);
          res.statusCode = 200;
          res.setHeader("Content-Type", record.mimeType || "application/octet-stream");
          res.setHeader("Cache-Control", "public, max-age=31536000, immutable");
          res.setHeader("X-IPFS-CID", record.cid);
          res.setHeader("X-CGP-Faux-IPFS", "1");
          res.setHeader("Content-Length", req.method === "HEAD" ? record.bytes : bytes.byteLength);
          if (req.method === "HEAD") {
            res.end();
          } else {
            res.end(bytes);
          }
        } catch (error: any) {
          lastError = error?.message || String(error);
          sendJson(res, 404, { ok: false, error: lastError || "Faux IPFS object was not found." });
        }
        return true;
      }
      sendJson(res, 404, { ok: false, error: "Unknown faux IPFS route." });
      return true;
    },
    onClose: async (ctx) => {
      started = false;
      ctx.ipfsBackends?.delete(id);
      await persist().catch(() => undefined);
    },
  };
}

export function createHeliaIpfsPlugin(
  policy: HeliaIpfsPolicy = {},
): RelayPlugin {
  const id = normalizeMediaToken(policy.id || process.env.CGP_IPFS_HELIA_ID || "helia");
  const envMode = process.env.CGP_IPFS_HELIA_MODE?.trim().toLowerCase();
  const mode: HeliaIpfsMode =
    policy.mode ||
    (envMode === "network" || process.env.CGP_IPFS_HELIA_NETWORK === "1"
      ? "network"
      : "local");
  const storeDir = path.resolve(
    policy.storeDir ||
      process.env.CGP_IPFS_HELIA_STORE_DIR ||
      "./relay-ipfs-helia",
  );
  const gatewayUrl =
    policy.gatewayUrl ||
    process.env.CGP_IPFS_HELIA_GATEWAY_URL ||
    process.env.CGP_MEDIA_IPFS_GATEWAY_URL ||
    "https://ipfs.io/ipfs/{cid}";
  const maxAddBytes = Math.max(
    1,
    Math.floor(
      policy.maxAddBytes ??
        positiveIntegerFromEnv("CGP_IPFS_HELIA_MAX_ADD_BYTES", 25 * 1024 * 1024),
    ),
  );
  const autoStart =
    policy.autoStart ?? process.env.CGP_IPFS_HELIA_AUTOSTART === "1";
  const exposeHttpRoutes =
    policy.exposeHttpRoutes ?? process.env.CGP_IPFS_HELIA_HTTP_ROUTES !== "0";
  const configuredListenAddrs =
    policy.listenAddrs ?? listFromEnv("CGP_IPFS_HELIA_LISTEN_ADDRS");
  const listenAddrs = normalizeMultiaddrs(
    configuredListenAddrs.length > 0
      ? configuredListenAddrs
      : mode === "network"
        ? ["/ip4/0.0.0.0/tcp/0", "/ip4/0.0.0.0/tcp/0/ws"]
        : [],
  );
  const announceAddrs = normalizeMultiaddrs(
    policy.announceAddrs ?? listFromEnv("CGP_IPFS_HELIA_ANNOUNCE_ADDRS"),
  );
  const useDefaultBootstrap =
    policy.useDefaultBootstrap ??
    envFlag("CGP_IPFS_HELIA_USE_DEFAULT_BOOTSTRAP", mode === "network");
  const bootstrapAddrs = normalizeMultiaddrs([
    ...(policy.bootstrapAddrs ?? listFromEnv("CGP_IPFS_HELIA_BOOTSTRAP_ADDRS")),
    ...(useDefaultBootstrap ? DEFAULT_HELIA_BOOTSTRAP_ADDRS : []),
  ]);
  const enableTcp = policy.enableTcp ?? envFlag("CGP_IPFS_HELIA_TCP", true);
  const enableWebSockets =
    policy.enableWebSockets ?? envFlag("CGP_IPFS_HELIA_WEBSOCKETS", true);
  const enableDht =
    policy.enableDht ?? envFlag("CGP_IPFS_HELIA_DHT", mode === "network");
  const enableBitswap =
    policy.enableBitswap ?? envFlag("CGP_IPFS_HELIA_BITSWAP", mode === "network");
  const enableTrustlessGateway =
    policy.enableTrustlessGateway ??
    envFlag("CGP_IPFS_HELIA_TRUSTLESS_GATEWAY", mode === "network");
  const provideOnAdd =
    policy.provideOnAdd ?? envFlag("CGP_IPFS_HELIA_PROVIDE_ON_ADD", mode === "network");
  const provideTimeoutMs = Math.max(
    1000,
    Math.floor(
      policy.provideTimeoutMs ??
        positiveIntegerFromEnv("CGP_IPFS_HELIA_PROVIDE_TIMEOUT_MS", 5000),
    ),
  );

  let helia: any;
  let unix: any;
  let cidModule: any;
  let multiaddrModule: any;
  let libp2pInstance: any;
  let blockstoreInstance: any;
  let datastoreInstance: any;
  let startPromise: Promise<void> | undefined;
  const providedCids = new Set<string>();
  let lastError = "";
  let started = false;

  const ensureStarted = async () => {
    if (started) return;
    if (startPromise) {
      await startPromise;
      return;
    }
    startPromise = (async () => {
      await mkdir(storeDir, { recursive: true });
      const [
        { Helia },
        { unixfs },
        { FsBlockstore },
        { LevelDatastore },
        { createLibp2p },
        multiaddrMod,
        cidMod,
      ] = await Promise.all([
        importEsm("@helia/utils"),
        importEsm("@helia/unixfs"),
        importEsm("blockstore-fs"),
        importEsm("datastore-level"),
        importEsm("libp2p"),
        importEsm("@multiformats/multiaddr"),
        importEsm("multiformats/cid"),
      ]);
      const blockstore = new FsBlockstore(path.join(storeDir, "blocks"));
      const datastore = new LevelDatastore(path.join(storeDir, "datastore"));
      blockstoreInstance = blockstore;
      datastoreInstance = datastore;
      await blockstore.open?.();
      await datastore.open?.();
      const transports: any[] = [];
      const connectionEncrypters: any[] = [];
      const streamMuxers: any[] = [];
      const peerDiscovery: any[] = [];
      const services: Record<string, any> = {};
      const blockBrokers: any[] = [];
      const routers: any[] = [];
      if (mode === "network") {
        const [
          { tcp },
          { webSockets },
          { noise },
          { yamux },
          { identify, identifyPush },
          { bootstrap },
          { kadDHT, removePrivateAddressesMapper },
          { ping },
          { bitswap, trustlessGateway },
          { libp2pRouting, httpGatewayRouting },
        ] = await Promise.all([
          importEsm("@libp2p/tcp"),
          importEsm("@libp2p/websockets"),
          importEsm("@chainsafe/libp2p-noise"),
          importEsm("@chainsafe/libp2p-yamux"),
          importEsm("@libp2p/identify"),
          importEsm("@libp2p/bootstrap"),
          importEsm("@libp2p/kad-dht"),
          importEsm("@libp2p/ping"),
          importEsm("@helia/block-brokers"),
          importEsm("@helia/routers"),
        ]);
        if (enableTcp) transports.push(tcp());
        if (enableWebSockets) transports.push(webSockets());
        connectionEncrypters.push(noise());
        streamMuxers.push(yamux());
        services.identify = identify();
        services.identifyPush = identifyPush();
        services.ping = ping();
        if (enableDht) {
          services.aminoDHT = kadDHT({
            protocol: "/ipfs/kad/1.0.0",
            peerInfoMapper: removePrivateAddressesMapper,
            clientMode: false,
          });
        }
        if (bootstrapAddrs.length > 0) {
          peerDiscovery.push(bootstrap({ list: bootstrapAddrs }));
        }
        if (enableBitswap) blockBrokers.push(bitswap());
        if (enableTrustlessGateway) {
          blockBrokers.push(trustlessGateway());
          routers.push(httpGatewayRouting());
        }
      }
      const libp2p = await createLibp2p({
        datastore,
        start: false,
        addresses: { listen: listenAddrs, announce: announceAddrs },
        transports,
        connectionEncrypters,
        streamMuxers,
        peerDiscovery,
        services,
      });
      if (mode === "network") {
        const { libp2pRouting } = await importEsm("@helia/routers");
        routers.unshift(libp2pRouting(libp2p));
      }
      libp2pInstance = libp2p;
      helia = new Helia({
        libp2p,
        blockstore,
        datastore,
        blockBrokers,
        routers,
      });
      await helia.start();
      unix = unixfs(helia);
      multiaddrModule = multiaddrMod;
      cidModule = cidMod;
      started = true;
      lastError = "";
    })().catch((error) => {
      lastError = error instanceof Error ? error.message : String(error);
      startPromise = undefined;
      throw error;
    });
    await startPromise;
  };

  const provideCid = async (cid: any, force = false) => {
    if (mode !== "network" || (!provideOnAdd && !force) || !helia?.routing?.provide) {
      return false;
    }
    const cidString = cid.toString();
    if (!force && providedCids.has(cidString)) {
      return true;
    }
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), provideTimeoutMs);
      try {
        await helia.routing.provide(cid, { signal: controller.signal });
      } finally {
        clearTimeout(timer);
      }
      providedCids.add(cidString);
      return true;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      return false;
    }
  };

  const backend: CgpIpfsBackend = {
    id,
    kind: "helia",
    addFile: async (input) => {
      const { bytes, sha256 } = await bufferForIpfsInput(input);
      if (bytes.byteLength > maxAddBytes) {
        throw new Error(
          `IPFS add exceeds ${maxAddBytes} byte limit (${bytes.byteLength}).`,
        );
      }
      await ensureStarted();
      const cid = await unix.addBytes(bytes);
      const cidString = cid.toString();
      if (input.pin !== false) {
        try {
          await drainAsyncIterable(
            helia.pins.add(cid, {
              metadata: normalizeIpfsMetadata({
                provider: id,
                backend: "helia",
                name: input.name || "",
                sha256,
                bytes: bytes.byteLength,
                mimeType: input.mimeType || "",
                ...(input.metadata ?? {}),
              }),
            }),
          );
        } catch (error) {
          if (!/already pinned/i.test(error instanceof Error ? error.message : String(error))) {
            throw error;
          }
        }
      }
      await provideCid(cid);
      return {
        providerId: id,
        backend: "helia",
        cid: cidString,
        bytes: bytes.byteLength,
        sha256,
        gatewayUrl: gatewayUrlForCid(gatewayUrl, cidString),
      };
    },
    pin: async (cid, metadata) => {
      await ensureStarted();
      const parsed = cidModule.CID.parse(cid);
      await drainAsyncIterable(
        helia.pins.add(parsed, {
          metadata: normalizeIpfsMetadata({
            provider: id,
            backend: "helia",
            ...(metadata ?? {}),
          }),
        }),
      );
      await provideCid(parsed);
    },
    status: async () => {
      let pins: number | undefined;
      if (started && helia?.pins) {
        pins = 0;
        for await (const _pin of helia.pins.ls()) {
          pins += 1;
        }
      }
      return {
        id,
        kind: "helia",
        started,
        mode,
        storeDir,
        peerId: started ? helia?.libp2p?.peerId?.toString?.() : undefined,
        listenAddrs,
        multiaddrs: started
          ? helia?.libp2p?.getMultiaddrs?.()?.map((addr: any) => addr.toString())
          : undefined,
        protocols: started ? helia?.libp2p?.getProtocols?.() : undefined,
        connections: started ? helia?.libp2p?.getConnections?.()?.length ?? 0 : undefined,
        peers: started ? helia?.libp2p?.getPeers?.()?.length ?? 0 : undefined,
        pins,
        provided: providedCids.size,
        error: lastError || undefined,
      };
    },
    close: async () => {
      const currentHelia = helia;
      const currentDatastore = datastoreInstance;
      const currentBlockstore = blockstoreInstance;
      started = false;
      startPromise = undefined;
      helia = undefined;
      unix = undefined;
      cidModule = undefined;
      multiaddrModule = undefined;
      libp2pInstance = undefined;
      blockstoreInstance = undefined;
      datastoreInstance = undefined;
      providedCids.clear();
      if (currentHelia?.stop) {
        await currentHelia.stop();
      }
      if (currentDatastore?.close) {
        await currentDatastore.close();
      }
      if (currentBlockstore?.close) {
        await currentBlockstore.close();
      }
    },
  };

  return {
    name: "cgp.ipfs.helia",
    metadata: {
      name: "Embedded Helia IPFS",
      description:
        "Relay-local TypeScript IPFS backend for content-addressed media and static shard pinning.",
      version: "1",
      policy: {
        id,
        storeDir,
        gatewayUrl,
        maxAddBytes,
        mode,
        listenAddrs,
        announceAddrs,
        bootstrapAddrs,
        enableTcp,
        enableWebSockets,
        enableDht,
        enableBitswap,
        enableTrustlessGateway,
        provideOnAdd,
        autoStart,
        exposeHttpRoutes,
      },
    },
    onInit: async (ctx) => {
      ctx.ipfsBackends?.set(id, backend);
      if (autoStart) {
        await ensureStarted();
      }
    },
    onHttp: async ({ req, res, pathSegments }) => {
      if (pathSegments[0] !== "cgp.ipfs.helia") {
        return false;
      }
      if (!exposeHttpRoutes) {
        sendJson(res, 404, { ok: false, error: "Helia HTTP routes are disabled." });
        return true;
      }
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS");
      res.setHeader("Access-Control-Allow-Headers", "Content-Type");
      if (req.method === "OPTIONS") {
        res.statusCode = 204;
        res.end();
        return true;
      }
      const action = pathSegments[1] || "status";
      if (action === "status" && req.method === "GET") {
        sendJson(res, 200, { ok: true, backend: await backend.status() });
        return true;
      }
      if (action === "peers" && req.method === "GET") {
        await ensureStarted();
        sendJson(res, 200, {
          ok: true,
          peers:
            libp2pInstance?.getPeers?.()?.map((peer: any) => peer.toString()) ?? [],
          connections:
            libp2pInstance?.getConnections?.()?.map((connection: any) => ({
              peer: connection.remotePeer?.toString?.(),
              remoteAddr: connection.remoteAddr?.toString?.(),
              status: connection.status,
            })) ?? [],
        });
        return true;
      }
      if (action === "peers" && pathSegments[2] === "connect" && req.method === "POST") {
        if (mode !== "network") {
          sendJson(res, 409, {
            ok: false,
            error: "Helia peer connections require network mode.",
          });
          return true;
        }
        let body: Record<string, unknown>;
        try {
          body = await readJsonRequestBody(req, 16 * 1024);
        } catch (error: any) {
          sendJson(res, 400, {
            ok: false,
            error: error?.message || "Invalid peer connect request.",
          });
          return true;
        }
        const multiaddrValue = stringField(body, "multiaddr");
        if (!multiaddrValue) {
          sendJson(res, 400, { ok: false, error: "multiaddr is required." });
          return true;
        }
        try {
          await ensureStarted();
          const ma = multiaddrModule.multiaddr(multiaddrValue);
          const connection = await libp2pInstance.dial(ma, {
            signal: AbortSignal.timeout(
              positiveIntegerFromEnv("CGP_IPFS_HELIA_DIAL_TIMEOUT_MS", 10_000),
            ),
          });
          sendJson(res, 200, {
            ok: true,
            peer: connection.remotePeer?.toString?.(),
            remoteAddr: connection.remoteAddr?.toString?.(),
            status: connection.status,
          });
        } catch (error: any) {
          sendJson(res, 409, {
            ok: false,
            error: error?.message || "Helia peer connect failed.",
          });
        }
        return true;
      }
      if (action === "add" && req.method === "POST") {
        let body: Record<string, unknown>;
        try {
          body = await readJsonRequestBody(req, Math.ceil(maxAddBytes * 1.4) + 4096);
        } catch (error: any) {
          sendJson(res, 400, {
            ok: false,
            error: error?.message || "Invalid Helia add request.",
          });
          return true;
        }
        const base64 = stringField(body, "bytesBase64");
        if (!base64) {
          sendJson(res, 400, {
            ok: false,
            error: "Helia add requires bytesBase64.",
          });
          return true;
        }
        let bytes: Buffer;
        try {
          bytes = Buffer.from(base64, "base64");
        } catch {
          sendJson(res, 400, { ok: false, error: "Invalid base64 payload." });
          return true;
        }
        try {
          const result = await backend.addFile({
            bytes,
            name: stringField(body, "name") || undefined,
            mimeType: stringField(body, "mimeType") || undefined,
            sha256: stringField(body, "sha256") || undefined,
            pin: booleanField(body, "pin") ?? true,
            metadata: isRecord(body.metadata)
              ? normalizeIpfsMetadata(body.metadata as Record<string, string | number | boolean>)
              : undefined,
          });
          sendJson(res, 201, { ok: true, ...result });
        } catch (error: any) {
          sendJson(res, 409, {
            ok: false,
            error: error?.message || "Helia add failed.",
          });
        }
        return true;
      }
      if (action === "provide" && req.method === "POST") {
        if (mode !== "network") {
          sendJson(res, 409, {
            ok: false,
            error: "Helia providing requires network mode.",
          });
          return true;
        }
        let body: Record<string, unknown>;
        try {
          body = await readJsonRequestBody(req, 16 * 1024);
        } catch (error: any) {
          sendJson(res, 400, {
            ok: false,
            error: error?.message || "Invalid provide request.",
          });
          return true;
        }
        const cidValue = stringField(body, "cid");
        if (!cidValue) {
          sendJson(res, 400, { ok: false, error: "cid is required." });
          return true;
        }
        try {
          await ensureStarted();
          const parsed = cidModule.CID.parse(cidValue);
          const provided = await provideCid(parsed, true);
          sendJson(res, provided ? 200 : 409, { ok: provided, cid: cidValue });
        } catch (error: any) {
          sendJson(res, 409, {
            ok: false,
            error: error?.message || "Helia provide failed.",
          });
        }
        return true;
      }
      if (action === "ipfs" && req.method === "GET") {
        const cidValue = pathSegments[2] || "";
        if (!cidValue) {
          sendJson(res, 400, { ok: false, error: "CID is required." });
          return true;
        }
        try {
          await ensureStarted();
          const parsed = cidModule.CID.parse(cidValue);
          const requestUrl = new URL(req.url || "/", "http://localhost");
          const pathInsideDag = pathSegments.slice(3).join("/") || undefined;
          const offlineParam = requestUrl.searchParams.get("offline");
          const offline =
            offlineParam === null
              ? true
              : !["0", "false", "no"].includes(offlineParam.toLowerCase());
          const timeoutMs = Math.max(
            1000,
            Math.floor(
              Number(requestUrl.searchParams.get("timeoutMs")) ||
                positiveIntegerFromEnv("CGP_IPFS_HELIA_FETCH_TIMEOUT_MS", 15_000),
            ),
          );
          const signal = offline ? undefined : AbortSignal.timeout(timeoutMs);
          const pin = await helia.pins.get(parsed).catch(() => undefined);
          const metadata = isRecord(pin?.metadata) ? pin.metadata : {};
          const mimeType =
            stringField(metadata, "mimeType") ||
            stringField(metadata, "mimetype") ||
            stringField(metadata, "contentType") ||
            "application/octet-stream";
          const stats = await unix.stat(parsed, {
            offline,
            path: pathInsideDag,
            signal,
          });
          if (stats.type === "directory") {
            sendJson(res, 409, {
              ok: false,
              error: "CID points to a UnixFS directory; directory listing is not implemented.",
            });
            return true;
          }
          res.statusCode = 200;
          res.setHeader("Content-Type", mimeType);
          res.setHeader("Cache-Control", "public, max-age=31536000, immutable");
          res.setHeader("X-IPFS-CID", cidValue);
          if (
            typeof stats.size === "bigint" &&
            stats.size <= BigInt(Number.MAX_SAFE_INTEGER)
          ) {
            res.setHeader("Content-Length", Number(stats.size));
          }
          await streamUint8Iterable(
            res,
            unix.cat(parsed, { offline, path: pathInsideDag, signal }),
          );
        } catch (error: any) {
          if (!res.headersSent) {
            sendJson(res, 404, {
              ok: false,
              error: error?.message || "CID was not found in this Helia store.",
            });
          } else if (!res.destroyed) {
            res.destroy(error);
          }
        }
        return true;
      }
      sendJson(res, 404, { ok: false, error: "Unknown Helia route." });
      return true;
    },
    onClose: async () => {
      await backend.close?.();
    },
  };
}

interface StaticShardManifestRef {
  path: string;
  sha256?: string;
  [key: string]: unknown;
}

interface StaticShardRef {
  id: string;
  kind: string;
  path: string;
  bytes?: number;
  sha256: string;
  [key: string]: unknown;
}

export const STATIC_SHARD_PUBLISHER_PROTOCOL = "cgp/static-shard-publisher/1";

export interface StaticShardPublisherProof {
  protocol: typeof STATIC_SHARD_PUBLISHER_PROTOCOL;
  publicKey: string;
  signature: string;
  payloadHash: string;
  deviceAuthorization?: DeviceAuthorization;
}

/** Canonical object signed by a static-shard publisher. */
export function staticShardReleaseSigningPayload(value: unknown) {
  if (!isRecord(value)) {
    throw new Error("Static shard release must be a JSON object.");
  }
  const publisher = isRecord(value.publisher) ? value.publisher : {};
  const publicKey = stringField(publisher, "publicKey").toLowerCase();
  return {
    protocol: STATIC_SHARD_PUBLISHER_PROTOCOL,
    publicKey,
    release: {
      ...value,
      publisher: {
        protocol: STATIC_SHARD_PUBLISHER_PROTOCOL,
        publicKey,
      },
    },
  };
}

/**
 * Returns undefined only when no publisher claim exists. A malformed or
 * invalid claim is always rejected instead of being downgraded to unsigned.
 */
export function verifyStaticShardReleasePublisher(
  value: unknown,
  options: {
    deviceAuthorityRegistry?: DeviceAuthorityRegistry;
    now?: number;
  } = {},
): StaticShardPublisherProof | undefined {
  if (!isRecord(value)) {
    throw new Error("Static shard release must be a JSON object.");
  }
  if (value.publisher === undefined || value.publisher === null) return undefined;
  if (!isRecord(value.publisher)) {
    throw new Error("Static shard publisher proof must be an object.");
  }
  const protocol = stringField(value.publisher, "protocol");
  const publicKey = stringField(value.publisher, "publicKey").toLowerCase();
  const signature = stringField(value.publisher, "signature").toLowerCase();
  const deviceAuthorization = value.publisher.deviceAuthorization as
    | DeviceAuthorization
    | undefined;
  if (protocol !== STATIC_SHARD_PUBLISHER_PROTOCOL) {
    throw new Error(`Unsupported static shard publisher protocol: ${protocol || "missing"}.`);
  }
  if (!/^(02|03)[0-9a-f]{64}$/.test(publicKey)) {
    throw new Error("Static shard publisher publicKey must be a compressed CGP secp256k1 key.");
  }
  if (!/^[0-9a-f]{128}$/.test(signature)) {
    throw new Error("Static shard publisher signature must be a compact secp256k1 signature.");
  }
  const payloadHash = hashObject(staticShardReleaseSigningPayload(value));
  if (value.publisher.deviceAuthorization !== undefined) {
    if (!isRecord(value.publisher.deviceAuthorization)) {
      throw new Error("Static shard publisher device authorization must be an object.");
    }
    const authorized = options.deviceAuthorityRegistry
      ? options.deviceAuthorityRegistry.verify(
          payloadHash,
          signature,
          publicKey,
          deviceAuthorization as DeviceAuthorization,
          "publish",
          options.now,
        )
      : verifyDeviceAuthorizedObject(
          payloadHash,
          signature,
          deviceAuthorization as DeviceAuthorization,
          {
            accountPublicKey: publicKey,
            requiredCapability: "publish",
            now: options.now,
          },
        );
    if (!authorized.ok) {
      throw new Error(
        `Static shard delegated publisher signature is invalid: ${authorized.error ?? "authorization failed"}.`,
      );
    }
  } else if (options.deviceAuthorityRegistry?.get(publicKey)) {
    throw new Error(
      "Static shard direct account signatures are disabled after device authority activation.",
    );
  } else if (!verify(publicKey, payloadHash, signature)) {
    throw new Error("Static shard publisher signature is invalid.");
  }
  return {
    protocol: STATIC_SHARD_PUBLISHER_PROTOCOL,
    publicKey,
    signature,
    payloadHash,
    ...(deviceAuthorization ? { deviceAuthorization } : {}),
  };
}

interface StaticShardReleaseManifest {
  kind?: string;
  id: string;
  title?: string;
  type?: string;
  version: string;
  generatedAt?: string;
  source?: unknown;
  ipfs?: unknown;
  hosting?: StaticShardHosting;
  manifests?: Record<string, StaticShardManifestRef>;
  shardPolicy?: {
    maxShardBytes?: number;
    targetShardBytes?: number;
    format?: string;
    hash?: string;
    addressing?: string;
    note?: string;
  };
  shards: StaticShardRef[];
  install?: unknown;
  publisher?: {
    protocol?: string;
    publicKey?: string;
    signature?: string;
  };
  [key: string]: unknown;
}

interface StaticShardHosting {
  mode?: StaticShardPlayableMode;
  distBaseUrl?: string;
  entryUrl?: string;
  fallbackMode?: StaticShardPlayableMode;
}

interface IngestedStaticShardRelease {
  network?: Record<string, unknown>;
  host?: Record<string, unknown>;
  listingRevision?: number;
  listingUpdatedAt?: number;
  listingClaim?: Record<string, unknown>;
  id: string;
  title: string;
  description?: string;
  thumbnail?: string;
  icon?: string;
  tags?: string[];
  creatorId?: string;
  creatorName?: string;
  creatorUsername?: string;
  creatorAvatar?: string;
  creatorBio?: string;
  source?: Record<string, unknown>;
  sourceBranch?: string;
  display?: Record<string, unknown>;
  launchQuery?: string;
  hosting?: StaticShardHosting;
  type: string;
  version: string;
  releaseUrl: string;
  releaseSha256: string;
  storedAt: number;
  publisher?: StaticShardPublisherProof;
  guildId?: string;
  channelId?: string;
  entryPath?: string;
  playServePath?: string;
  entryServePath?: string;
  externalEntryUrl?: string;
  externalPlayUrl?: string;
  serveMode?: StaticShardPlayableMode;
  unpacked?: {
    files: number;
    bytes: number;
    extractedAt: number;
  };
  manifests: Array<{
    id: string;
    path: string;
    sha256?: string;
    bytes: number;
    servePath: string;
  }>;
  shards: Array<{
    id: string;
    kind: string;
    path: string;
    bytes: number;
    sha256: string;
    servePath: string;
    ipfsCid?: string;
    ipfsGatewayUrl?: string;
  }>;
}

function parseStaticShardSourcesFromEnv() {
  const sources: StaticShardSeedSource[] = [];
  const rawJson = process.env.CGP_STATIC_SHARD_SEEDS_JSON;
  if (rawJson?.trim()) {
    try {
      const parsed = JSON.parse(rawJson);
      if (Array.isArray(parsed)) {
        for (const entry of parsed) {
          const source = normalizeStaticShardSeedSource(entry);
          if (source) sources.push(source);
        }
      }
    } catch {
      // Ignore invalid env config; operators can inspect /plugins/cgp.static-shards/status.
    }
  }
  const rawUrls = process.env.CGP_STATIC_SHARD_SEED_URLS;
  if (rawUrls?.trim()) {
    for (const url of rawUrls.split(",")) {
      const source = normalizeStaticShardSeedSource(url);
      if (source) sources.push(source);
    }
  }
  const seen = new Set<string>();
  return sources.filter((source) => {
    const key = `${source.kind ?? ""}:${source.url}:${source.expectedSha256 ?? ""}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function normalizeStaticShardSeedSource(
  value: unknown,
): StaticShardSeedSource | undefined {
  const entry =
    typeof value === "string" ? { url: value } : isRecord(value) ? value : {};
  const url = stringField(entry, "url");
  if (!url || !safeUrl(url)) return undefined;
  const rawKind = stringField(entry, "kind");
  const kind =
    rawKind === "catalog" || rawKind === "release"
      ? rawKind
      : inferStaticShardSeedKind(url);
  const expectedSha256 = stringField(entry, "expectedSha256").toLowerCase();
  return {
    url,
    kind,
    expectedSha256: /^[0-9a-f]{64}$/.test(expectedSha256)
      ? expectedSha256
      : undefined,
  };
}

function inferStaticShardSeedKind(url: string): StaticShardSeedKind {
  return /\/release\.json(?:[?#].*)?$/i.test(url) ? "release" : "catalog";
}

function normalizeStaticShardPlayableMode(value: unknown): StaticShardPlayableMode | undefined {
  const mode = typeof value === "string" ? value.trim().toLowerCase() : "";
  return mode === "extract" || mode === "redirect" || mode === "auto"
    ? mode
    : undefined;
}

function normalizeStaticShardHosting(value: unknown): StaticShardHosting | undefined {
  if (!isRecord(value)) return undefined;
  const distBaseUrl = stringField(value, "distBaseUrl");
  const entryUrl = stringField(value, "entryUrl");
  const hosting: StaticShardHosting = {
    mode: normalizeStaticShardPlayableMode(value.mode),
    distBaseUrl: distBaseUrl && safeUrl(distBaseUrl) ? distBaseUrl : undefined,
    entryUrl: entryUrl && safeUrl(entryUrl) ? entryUrl : undefined,
    fallbackMode: normalizeStaticShardPlayableMode(value.fallbackMode),
  };
  return hosting.mode || hosting.distBaseUrl || hosting.entryUrl || hosting.fallbackMode
    ? hosting
    : undefined;
}

function normalizeStaticShardRelease(
  value: unknown,
): StaticShardReleaseManifest {
  if (!isRecord(value)) {
    throw new Error("Static shard release must be a JSON object.");
  }
  const id = normalizeMediaToken(stringField(value, "id"));
  const version = stringField(value, "version");
  const shards = Array.isArray(value.shards) ? value.shards : [];
  if (!id || !version || shards.length === 0) {
    throw new Error("Static shard release requires id, version, and shards.");
  }
  const normalizedShards: StaticShardRef[] = shards.map((entry, index) => {
    if (!isRecord(entry)) {
      throw new Error(`Shard ${index} must be an object.`);
    }
    const shardId = normalizeMediaToken(stringField(entry, "id")) || `shard-${index}`;
    const shardPath = safeStaticRelativePath(stringField(entry, "path"));
    const sha256 = stringField(entry, "sha256").toLowerCase();
    if (!shardPath || !/^[0-9a-f]{64}$/.test(sha256)) {
      throw new Error(`Shard ${shardId} requires a safe path and sha256.`);
    }
    const bytes = numberField(entry, "bytes");
    return {
      ...entry,
      id: shardId,
      kind: normalizeMediaToken(stringField(entry, "kind")) || "unknown",
      path: shardPath,
      sha256,
      bytes,
    };
  });

  const manifests: Record<string, StaticShardManifestRef> = {};
  if (isRecord(value.manifests)) {
    for (const [key, manifestValue] of Object.entries(value.manifests)) {
      if (!isRecord(manifestValue)) continue;
      const manifestPath = safeStaticRelativePath(
        stringField(manifestValue, "path"),
      );
      if (!manifestPath) continue;
      const sha256 = stringField(manifestValue, "sha256").toLowerCase();
      manifests[normalizeMediaToken(key) || key] = {
        ...manifestValue,
        path: manifestPath,
        sha256: /^[0-9a-f]{64}$/.test(sha256) ? sha256 : undefined,
      };
    }
  }

  const shardPolicy = isRecord(value.shardPolicy)
    ? {
        maxShardBytes: numberField(value.shardPolicy, "maxShardBytes"),
        targetShardBytes: numberField(value.shardPolicy, "targetShardBytes"),
        format: stringField(value.shardPolicy, "format") || undefined,
        hash: stringField(value.shardPolicy, "hash") || undefined,
        addressing: stringField(value.shardPolicy, "addressing") || undefined,
        note: stringField(value.shardPolicy, "note") || undefined,
      }
    : undefined;

  return {
    ...value,
    id,
    title: stringField(value, "title") || id,
    type: normalizeMediaToken(stringField(value, "type")) || "game",
    version,
    generatedAt: stringField(value, "generatedAt") || undefined,
    hosting: normalizeStaticShardHosting(value.hosting),
    manifests,
    shardPolicy,
    shards: normalizedShards,
  };
}

interface StaticShardUploadedFile {
  path: string;
  buffer: Buffer;
  bytes: number;
  sha256: string;
}

function decodeStaticShardBase64(
  value: unknown,
  label: string,
  maxBytes: number,
) {
  const encoded = typeof value === "string" ? value.trim() : "";
  if (!encoded || !/^[A-Za-z0-9+/]+={0,2}$/.test(encoded)) {
    throw new Error(`${label} must be base64-encoded bytes.`);
  }
  const buffer = Buffer.from(encoded, "base64");
  if (buffer.byteLength > maxBytes) {
    throw new Error(`${label} exceeds ${maxBytes} bytes.`);
  }
  return buffer;
}

function staticShardUploadReleaseBytes(
  body: Record<string, unknown>,
  maxBytes: number,
) {
  const encoded =
    stringField(body, "releaseBase64") ||
    stringField(body, "releaseJsonBase64");
  if (encoded) {
    return decodeStaticShardBase64(encoded, "releaseBase64", maxBytes);
  }
  if (isRecord(body.release)) {
    return Buffer.from(JSON.stringify(body.release));
  }
  throw new Error("Static shard upload requires releaseBase64 or release.");
}

function staticShardUploadFileList(
  body: Record<string, unknown>,
  maxFileBytes: number,
) {
  const rawEntries = [
    ...(Array.isArray(body.files) ? body.files : []),
    ...(Array.isArray(body.manifests) ? body.manifests : []),
    ...(Array.isArray(body.shards) ? body.shards : []),
  ];
  const files = new Map<string, StaticShardUploadedFile>();
  for (const [index, rawEntry] of rawEntries.entries()) {
    if (!isRecord(rawEntry)) {
      throw new Error(`Upload file ${index} must be an object.`);
    }
    const filePath = safeStaticRelativePath(stringField(rawEntry, "path"));
    if (!filePath) {
      throw new Error(`Upload file ${index} requires a safe relative path.`);
    }
    const buffer = decodeStaticShardBase64(
      rawEntry.bytesBase64 ?? rawEntry.dataBase64,
      `Upload file ${filePath}`,
      maxFileBytes,
    );
    const sha256 = createHash("sha256").update(buffer).digest("hex");
    const expectedSha256 = stringField(rawEntry, "sha256").toLowerCase();
    if (expectedSha256 && expectedSha256 !== sha256) {
      throw new Error(
        `Upload file hash mismatch for ${filePath}: ${sha256} != ${expectedSha256}.`,
      );
    }
    files.set(filePath, {
      path: filePath,
      buffer,
      bytes: buffer.byteLength,
      sha256,
    });
  }
  return files;
}

function safeStaticRelativePath(value: string) {
  const trimmed = value.trim().replace(/\\/g, "/").replace(/^\/+/, "");
  if (!trimmed || /^[a-z]:/i.test(trimmed)) return "";
  const parts = trimmed.split("/").filter(Boolean);
  if (parts.some((part) => part === "." || part === "..")) return "";
  return parts.join("/");
}

function staticShardReleaseKey(id: string, version: string) {
  return `${id}@${version}`;
}

function staticShardGameGuildId(id: string) {
  return hashObject({ kind: "cgp-static-shard-game-guild", id });
}

function staticShardReleaseChannelId(id: string) {
  return hashObject({ kind: "cgp-static-shard-game-release-channel", id });
}

function staticShardStoreReleasePrefix(id: string, version: string) {
  return `games/${safeStaticRelativePath(id)}/${safeStaticRelativePath(version)}`;
}

function staticShardServePath(relPath: string) {
  return `/plugins/cgp.static-shards/files/${relPath}`;
}

function staticShardPlayServePath(id: string, version: string, relPath = "") {
  const safeRel = safeStaticRelativePath(relPath);
  const base = `/plugins/cgp.static-shards/play/${encodeURIComponent(id)}/${encodeURIComponent(version)}/`;
  return safeRel
    ? `${base}${safeRel.split("/").map(encodeURIComponent).join("/")}`
    : base;
}

function staticShardReleaseUnpackedRoot(storeDir: string, id: string, version: string) {
  return path.join(storeDir, staticShardStoreReleasePrefix(id, version), "unpacked");
}

function staticShardContentType(filePath: string) {
  const ext = path.extname(filePath).toLowerCase();
  switch (ext) {
    case ".json":
      return "application/json; charset=utf-8";
    case ".html":
      return "text/html; charset=utf-8";
    case ".js":
    case ".mjs":
      return "application/javascript; charset=utf-8";
    case ".css":
      return "text/css; charset=utf-8";
    case ".wasm":
      return "application/wasm";
    case ".png":
      return "image/png";
    case ".jpg":
    case ".jpeg":
      return "image/jpeg";
    case ".webp":
      return "image/webp";
    case ".gif":
      return "image/gif";
    case ".svg":
      return "image/svg+xml";
    case ".ico":
      return "image/x-icon";
    case ".mp3":
      return "audio/mpeg";
    case ".ogg":
      return "audio/ogg";
    case ".wav":
      return "audio/wav";
    case ".mp4":
      return "video/mp4";
    case ".webm":
      return "video/webm";
    case ".woff":
      return "font/woff";
    case ".woff2":
      return "font/woff2";
    case ".sha256":
    case ".txt":
    case ".jsonl":
      return "text/plain; charset=utf-8";
    case ".gz":
      return "application/gzip";
    case ".zip":
      return "application/zip";
    default:
      return "application/octet-stream";
  }
}

function assertInsideDirectory(root: string, candidate: string) {
  const relative = path.relative(path.resolve(root), path.resolve(candidate));
  if (relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error("Resolved static shard path escapes the configured store.");
  }
}

function resolveStaticShardUrl(baseUrl: string, relativePath: string) {
  const url = new URL(relativePath, baseUrl);
  if (url.protocol !== "https:" && url.protocol !== "http:") {
    throw new Error(`Unsupported static shard URL protocol: ${url.protocol}`);
  }
  return url.toString();
}

function decodeUrlPathname(value: string) {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function normalizeHttpBaseUrl(value: string) {
  const trimmed = value.trim();
  if (!trimmed) return "";
  const converted = trimmed
    .replace(/^wss:\/\//i, "https://")
    .replace(/^ws:\/\//i, "http://")
    .replace(/\/+$/, "");
  return /^https?:\/\//i.test(converted) ? converted : "";
}

function requestHttpBaseUrl(req?: IncomingMessage) {
  const host = String(req?.headers.host || "").trim();
  if (!host) return "";
  const forwardedProto = String(req?.headers["x-forwarded-proto"] || "")
    .split(",")[0]
    ?.trim()
    .toLowerCase();
  const secure = forwardedProto === "https" || forwardedProto === "wss";
  return `${secure ? "https" : "http"}://${host}`;
}

function absoluteStaticShardUrl(baseUrl: string, servePath?: string) {
  if (!servePath) return "";
  const normalizedBase = normalizeHttpBaseUrl(baseUrl);
  if (!normalizedBase) return servePath;
  return new URL(servePath, `${normalizedBase}/`).toString();
}

function externalStaticShardUrl(
  hosting: StaticShardHosting | undefined,
  relPath: string | undefined,
) {
  if (!hosting) return undefined;
  const safeRel = safeStaticRelativePath(relPath || "");
  if (hosting.entryUrl && (!safeRel || relPath === undefined)) {
    return hosting.entryUrl;
  }
  const base = hosting.distBaseUrl || (hosting.entryUrl ? new URL(".", hosting.entryUrl).toString() : "");
  if (!base || !safeUrl(base)) return undefined;
  try {
    // Relative paths are filenames, not URLs: preserve literal %, #, and ?.
    const encodedRel = safeRel.split("/").map(encodeURIComponent).join("/");
    const url = new URL(encodedRel, base.endsWith("/") ? base : `${base}/`);
    return url.protocol === "http:" || url.protocol === "https:"
      ? url.toString()
      : undefined;
  } catch {
    return undefined;
  }
}

function appendQueryToUrl(url: string | undefined, query: string | undefined) {
  const normalizedQuery = query?.trim().replace(/^\?+/, "");
  if (!url || !normalizedQuery) return url;
  return `${url}${url.includes("?") ? "&" : "?"}${normalizedQuery}`;
}

function staticShardPlayablePathCandidates(
  release: Pick<IngestedStaticShardRelease, "entryPath">,
  relPath: string | undefined,
) {
  const candidates: string[] = [];
  const safeRel = safeStaticRelativePath(relPath || "");
  const entryPath = safeStaticRelativePath(release.entryPath || "");
  const entryDir = entryPath ? path.posix.dirname(entryPath) : "";
  const safeEntryDir = entryDir && entryDir !== "." ? entryDir : "";
  const push = (value: string | undefined) => {
    const safe = safeStaticRelativePath(value || "");
    if (safe && !candidates.includes(safe)) {
      candidates.push(safe);
    }
  };

  if (safeEntryDir && safeRel && !safeRel.startsWith(`${safeEntryDir}/`)) {
    push(path.posix.join(safeEntryDir, safeRel));
  }
  // A bare legacy entry request (for example /index.html when the actual
  // entry is dist/index.html) may fall back to the manifest entry. Never do
  // that for an explicitly nested path: composed bundles commonly contain
  // more than one index.html and the nested file must win.
  if (
    entryPath
    && safeRel
    && path.posix.dirname(safeRel) === "."
    && path.posix.basename(entryPath) === path.posix.basename(safeRel)
  ) {
    push(entryPath);
  }
  push(safeRel);
  if (!safeRel) {
    push(entryPath);
  }
  return candidates;
}

async function readStaticShardRegistry(registryPath: string) {
  try {
    const parsed = JSON.parse(await readFile(registryPath, "utf8"));
    if (!isRecord(parsed) || !Array.isArray(parsed.releases)) return [];
    return parsed.releases.filter((entry): entry is IngestedStaticShardRelease =>
      isRecord(entry) &&
      typeof entry.id === "string" &&
      typeof entry.version === "string" &&
      Array.isArray(entry.shards),
    );
  } catch {
    return [];
  }
}

async function readStaticShardRegistryJournal(journalPath: string) {
  try {
    const lines = (await readFile(journalPath, "utf8"))
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);
    const releases: IngestedStaticShardRelease[] = [];
    let invalidEntries = 0;
    for (const line of lines) {
      try {
        const entry = JSON.parse(line);
        if (
          isRecord(entry) &&
          typeof entry.id === "string" &&
          typeof entry.version === "string" &&
          Array.isArray(entry.shards)
        ) {
          releases.push(entry as unknown as IngestedStaticShardRelease);
        } else {
          invalidEntries += 1;
        }
      } catch {
        invalidEntries += 1;
      }
    }
    return { releases, entries: releases.length, invalidEntries };
  } catch {
    return { releases: [], entries: 0, invalidEntries: 0 };
  }
}

async function writeStaticShardRegistry(
  registryPath: string,
  releases: IngestedStaticShardRelease[],
) {
  await mkdir(path.dirname(registryPath), { recursive: true });
  const temporaryPath = `${registryPath}.${process.pid}.${randomUUID()}.tmp`;
  try {
    await writeFile(
      temporaryPath,
      JSON.stringify(
        {
          kind: "cgp-relay-static-shard-registry",
          schemaVersion: 1,
          updatedAt: Date.now(),
          releases,
        },
        null,
        2,
      ),
    );
    await rename(temporaryPath, registryPath);
  } finally {
    await rm(temporaryPath, { force: true }).catch(() => undefined);
  }
}

function requestUrlBuffer(
  urlValue: string,
  options: { maxBytes: number; timeoutMs: number; accept?: string },
) {
  return new Promise<{ buffer: Buffer; contentType?: string }>((resolve, reject) => {
    const url = safeUrl(urlValue);
    if (!url) {
      reject(new Error(`Invalid URL: ${urlValue}`));
      return;
    }
    const client = url.protocol === "https:" ? httpsRequest : httpRequest;
    const req = client(
      url,
      {
        method: "GET",
        headers: options.accept ? { accept: options.accept } : undefined,
      },
      (res) => {
        const statusCode = res.statusCode ?? 0;
        if (statusCode < 200 || statusCode >= 300) {
          res.resume();
          reject(new Error(`GET ${urlValue} returned HTTP ${statusCode}`));
          return;
        }
        const chunks: Buffer[] = [];
        let total = 0;
        res.on("data", (chunk: Buffer) => {
          total += chunk.byteLength;
          if (total > options.maxBytes) {
            req.destroy(
              new Error(`GET ${urlValue} exceeded ${options.maxBytes} bytes`),
            );
            return;
          }
          chunks.push(chunk);
        });
        res.on("end", () => {
          resolve({
            buffer: Buffer.concat(chunks),
            contentType:
              typeof res.headers["content-type"] === "string"
                ? res.headers["content-type"]
                : undefined,
          });
        });
      },
    );
    req.setTimeout(options.timeoutMs, () => {
      req.destroy(new Error(`GET ${urlValue} timed out`));
    });
    req.on("error", reject);
    req.end();
  });
}

async function requestStaticShardJson(
  url: string,
  options: { maxBytes: number; timeoutMs: number },
) {
  const { buffer } = await requestUrlBuffer(url, {
    ...options,
    accept: "application/json",
  });
  let json: unknown;
  try {
    json = JSON.parse(buffer.toString("utf8"));
  } catch (error: any) {
    throw new Error(`Invalid JSON from ${url}: ${error?.message ?? String(error)}`);
  }
  return {
    json,
    bytes: buffer.byteLength,
    sha256: createHash("sha256").update(buffer).digest("hex"),
    buffer,
  };
}

async function downloadStaticShardFile(
  urlValue: string,
  destination: string,
  options: { maxBytes: number; timeoutMs: number },
) {
  const url = safeUrl(urlValue);
  if (!url) {
    throw new Error(`Invalid URL: ${urlValue}`);
  }
  await mkdir(path.dirname(destination), { recursive: true });
  return await new Promise<{ bytes: number; sha256: string }>((resolve, reject) => {
    const tmpPath = `${destination}.tmp-${randomUUID()}`;
    const output = createWriteStream(tmpPath, { flags: "wx" });
    const hash = createHash("sha256");
    let bytes = 0;
    let completed = false;
    const fail = (error: Error) => {
      if (completed) return;
      completed = true;
      output.destroy();
      void unlink(tmpPath).catch(() => undefined);
      reject(error);
    };
    output.on("error", fail);

    const client = url.protocol === "https:" ? httpsRequest : httpRequest;
    const req = client(url, { method: "GET" }, (res) => {
      const statusCode = res.statusCode ?? 0;
      if (statusCode < 200 || statusCode >= 300) {
        res.resume();
        fail(new Error(`GET ${urlValue} returned HTTP ${statusCode}`));
        return;
      }
      res.on("data", (chunk: Buffer) => {
        if (completed) return;
        bytes += chunk.byteLength;
        if (bytes > options.maxBytes) {
          req.destroy(
            new Error(`GET ${urlValue} exceeded ${options.maxBytes} bytes`),
          );
          return;
        }
        hash.update(chunk);
        if (!output.write(chunk)) {
          res.pause();
          output.once("drain", () => res.resume());
        }
      });
      res.on("end", () => {
        if (completed) return;
        output.end(async () => {
          if (completed) return;
          completed = true;
          try {
            await rename(tmpPath, destination);
            resolve({ bytes, sha256: hash.digest("hex") });
          } catch (error: any) {
            reject(error);
          }
        });
      });
    });
    req.setTimeout(options.timeoutMs, () => {
      req.destroy(new Error(`GET ${urlValue} timed out`));
    });
    req.on("error", fail);
    req.end();
  });
}

async function addStaticShardFileToIpfs(
  apiUrlValue: string,
  filePath: string,
  timeoutMs: number,
) {
  const apiUrl = safeUrl(apiUrlValue);
  if (!apiUrl) {
    throw new Error("Invalid IPFS API URL.");
  }
  if (!apiUrl.pathname.endsWith("/add")) {
    apiUrl.pathname = `${apiUrl.pathname.replace(/\/+$/, "")}/add`;
  }
  apiUrl.searchParams.set("pin", "true");
  apiUrl.searchParams.set("cid-version", "1");

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const data = await readFile(filePath);
    const form = new FormData();
    form.append("file", new Blob([data]), path.basename(filePath));
    const response = await fetch(apiUrl, {
      method: "POST",
      body: form as any,
      signal: controller.signal,
    });
    const body = await response.text();
    if (!response.ok) {
      throw new Error(`IPFS add returned HTTP ${response.status}: ${body.slice(0, 200)}`);
    }
    const lines = body
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);
    for (let index = lines.length - 1; index >= 0; index -= 1) {
      try {
        const parsed = JSON.parse(lines[index]);
        const cid = stringField(parsed, "Hash") || stringField(parsed, "Cid");
        if (cid) return cid;
      } catch {
        // Try earlier JSONL lines.
      }
    }
    throw new Error("IPFS add response did not contain a CID.");
  } finally {
    clearTimeout(timer);
  }
}

function nestedStaticShardString(value: unknown, pathParts: string[]) {
  let current: unknown = value;
  for (const part of pathParts) {
    if (!isRecord(current)) return "";
    current = current[part];
  }
  return typeof current === "string" ? current.trim() : "";
}

function normalizeStaticShardLaunchQueryFromValue(value: unknown) {
  if (typeof value === "string") {
    return value.trim().replace(/^\?+/, "");
  }
  if (!isRecord(value)) return "";
  const params = new URLSearchParams();
  for (const [key, rawValue] of Object.entries(value)) {
    const normalizedKey = key.trim();
    if (!normalizedKey || rawValue == null) continue;
    if (Array.isArray(rawValue)) {
      for (const item of rawValue) {
        if (item == null) continue;
        params.append(normalizedKey, String(item));
      }
      continue;
    }
    params.set(normalizedKey, String(rawValue));
  }
  return params.toString();
}

function normalizeStaticShardLaunchQuery(
  release: StaticShardReleaseManifest,
  manifest: Record<string, unknown>,
) {
  const launch = isRecord(manifest.launch) ? manifest.launch : {};
  const launchWeb = isRecord(launch.web) ? launch.web : {};
  const entry = isRecord(manifest.entry) ? manifest.entry : {};
  const install = isRecord(release.install) ? release.install : {};
  const installLaunch = isRecord(install.launch) ? install.launch : {};

  const rawQueries = [
    manifest.launchQuery,
    launch.query,
    launchWeb.query,
    entry.query,
    (release as Record<string, unknown>).launchQuery,
    install.query,
    installLaunch.query,
  ];
  for (const value of rawQueries) {
    const query = normalizeStaticShardLaunchQueryFromValue(value);
    if (query) return query;
  }

  const paramsSources = [
    launch.params,
    launchWeb.params,
    entry.params,
    install.params,
    installLaunch.params,
  ];
  const params = new URLSearchParams();
  for (const source of paramsSources) {
    if (!isRecord(source)) continue;
    for (const [key, rawValue] of Object.entries(source)) {
      const normalizedKey = key.trim();
      if (!normalizedKey || rawValue == null) continue;
      if (Array.isArray(rawValue)) {
        for (const item of rawValue) {
          if (item != null) params.append(normalizedKey, String(item));
        }
      } else {
        params.set(normalizedKey, String(rawValue));
      }
    }
  }
  return params.toString();
}

function staticShardDefaultCreatorId(gameId: string) {
  const normalized = gameId.trim().toLowerCase().replace(/[^a-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "");
  return normalized ? `${normalized}-studio` : "hollow-game-studio";
}

function staticShardDefaultCreatorName(gameTitle: string, creatorId: string) {
  const title = gameTitle.trim();
  if (title) return `${title} Studio`;
  return creatorId
    .replace(/[-_.]+/g, " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function normalizeStaticShardGameMetadata(
  release: StaticShardReleaseManifest,
  manifestJson: unknown,
) {
  const manifest = isRecord(manifestJson) ? manifestJson : {};
  const entryPath = [
    nestedStaticShardString(manifest, ["entry", "localPath"]),
    nestedStaticShardString(manifest, ["entry", "path"]),
    nestedStaticShardString(manifest, ["launch", "web", "localPath"]),
    nestedStaticShardString(manifest, ["launch", "web", "entry"]),
    nestedStaticShardString(manifest, ["web", "entry"]),
    stringField(manifest, "entry"),
    nestedStaticShardString(release.install, ["entry"]),
    nestedStaticShardString(release.install, ["localPath"]),
  ]
    .map(safeStaticRelativePath)
    .find(Boolean);

  const title = stringField(manifest, "title") || release.title || release.id;
  const description =
    stringField(manifest, "description") ||
    stringField(manifest, "summary") ||
    stringField(release as Record<string, unknown>, "description") ||
    stringField(release as Record<string, unknown>, "summary");
  const thumbnail =
    safeStaticRelativePath(stringField(manifest, "thumbnail")) ||
    safeStaticRelativePath(nestedStaticShardString(manifest, ["media", "thumbnail"])) ||
    stringField(release as Record<string, unknown>, "thumbnail");
  const icon =
    safeStaticRelativePath(stringField(manifest, "icon")) ||
    safeStaticRelativePath(nestedStaticShardString(manifest, ["media", "icon"])) ||
    stringField(release as Record<string, unknown>, "icon");
  const hollow = isRecord(manifest.hollow) ? manifest.hollow : {};
  const creator = isRecord(manifest.creator) ? manifest.creator : {};
  const creatorId =
    stringField(manifest, "creatorId") ||
    stringField(creator, "id") ||
    stringField(release as Record<string, unknown>, "creatorId") ||
    staticShardDefaultCreatorId(release.id);
  const creatorName =
    stringField(manifest, "creatorName") ||
    stringField(creator, "name") ||
    stringField(release as Record<string, unknown>, "creatorName") ||
    staticShardDefaultCreatorName(title, creatorId);
  const creatorUsername =
    stringField(manifest, "creatorUsername") ||
    stringField(creator, "username") ||
    stringField(release as Record<string, unknown>, "creatorUsername") ||
    creatorId;
  const creatorAvatar =
    safeStaticRelativePath(stringField(manifest, "creatorAvatar")) ||
    safeStaticRelativePath(stringField(creator, "avatar")) ||
    safeStaticRelativePath(nestedStaticShardString(manifest, ["media", "creatorAvatar"])) ||
    stringField(release as Record<string, unknown>, "creatorAvatar");
  const creatorBio =
    stringField(manifest, "creatorBio") ||
    stringField(creator, "bio") ||
    stringField(release as Record<string, unknown>, "creatorBio");
  const display = normalizeStaticShardDisplayMetadata(manifest, release as Record<string, unknown>);

  return {
    network: isRecord(manifest.network) ? manifest.network : isRecord(release.network) ? release.network : undefined,
    host: isRecord(manifest.host) ? manifest.host : isRecord(release.host) ? release.host : undefined,
    title,
    description: description || undefined,
    thumbnail: thumbnail || undefined,
    icon: icon || undefined,
    tags: [
      ...stringArrayField(manifest, "tags"),
      ...stringArrayField(release as Record<string, unknown>, "tags"),
    ].filter(Boolean),
    creatorId,
    creatorName,
    creatorUsername,
    creatorAvatar: creatorAvatar || undefined,
    creatorBio: creatorBio || undefined,
    display,
    sourceBranch: stringField(hollow, "sourceBranch") || undefined,
    launchQuery: normalizeStaticShardLaunchQuery(release, manifest) || undefined,
    entryPath: entryPath || undefined,
  };
}

function normalizeStaticShardDisplayMetadata(
  manifest: Record<string, unknown>,
  release: Record<string, unknown>,
) {
  const manifestDisplay = isRecord(manifest.display) ? manifest.display : {};
  const manifestViewport = isRecord(manifestDisplay.viewport)
    ? manifestDisplay.viewport
    : isRecord(manifest.viewport)
      ? manifest.viewport
      : {};
  const releaseDisplay = isRecord(release.display) ? release.display : {};
  const releaseViewport = isRecord(releaseDisplay.viewport)
    ? releaseDisplay.viewport
    : isRecord(release.viewport)
      ? release.viewport
      : {};

  const aspectRatio =
    stringField(manifest, "aspectRatio") ||
    stringField(manifestDisplay, "aspectRatio") ||
    stringField(manifestViewport, "aspectRatio") ||
    stringField(release, "aspectRatio") ||
    stringField(releaseDisplay, "aspectRatio") ||
    stringField(releaseViewport, "aspectRatio");
  const viewportWidth =
    numberField(manifest, "viewportWidth") ||
    numberField(manifestDisplay, "viewportWidth") ||
    numberField(manifestDisplay, "width") ||
    numberField(manifestViewport, "width") ||
    numberField(release, "viewportWidth") ||
    numberField(releaseDisplay, "viewportWidth") ||
    numberField(releaseDisplay, "width") ||
    numberField(releaseViewport, "width");
  const viewportHeight =
    numberField(manifest, "viewportHeight") ||
    numberField(manifestDisplay, "viewportHeight") ||
    numberField(manifestDisplay, "height") ||
    numberField(manifestViewport, "height") ||
    numberField(release, "viewportHeight") ||
    numberField(releaseDisplay, "viewportHeight") ||
    numberField(releaseDisplay, "height") ||
    numberField(releaseViewport, "height");

  const display: Record<string, unknown> = {};
  if (aspectRatio) display.aspectRatio = aspectRatio;
  if (viewportWidth && viewportHeight) {
    display.viewport = { width: viewportWidth, height: viewportHeight };
  }
  return Object.keys(display).length ? display : undefined;
}

async function readStaticShardGameMetadata(
  release: StaticShardReleaseManifest,
  manifestPath: string,
) {
  try {
    const parsed = JSON.parse(await readFile(manifestPath, "utf8"));
    return normalizeStaticShardGameMetadata(release, parsed);
  } catch {
    return normalizeStaticShardGameMetadata(release, undefined);
  }
}

async function ensureStaticShardDirectory(root: string, directory: string) {
  assertInsideDirectory(root, directory);
  const relative = path.relative(path.resolve(root), path.resolve(directory));
  if (!relative || relative === ".") {
    await mkdir(root, { recursive: true });
    return;
  }

  let current = path.resolve(root);
  for (const part of relative.split(path.sep).filter(Boolean)) {
    current = path.join(current, part);
    assertInsideDirectory(root, current);
    try {
      const existing = await stat(current);
      if (existing.isDirectory()) continue;
      await rm(current, { recursive: true, force: true });
    } catch {
      // Missing paths are created below.
    }
    await mkdir(current, { recursive: false }).catch((error: any) => {
      if (error?.code !== "EEXIST") throw error;
    });
  }
}

async function extractStaticShardZip(
  zipPath: string,
  unpackedRoot: string,
  options: { maxExtractedBytes: number; maxExtractedFiles: number },
) {
  const zipBytes = await readFile(zipPath);
  let plannedBytes = 0;
  let plannedFiles = 0;
  const entries = unzipSync(new Uint8Array(zipBytes), {
    filter(file) {
      const safeName = safeStaticRelativePath(file.name);
      if (!safeName || file.name.endsWith("/")) {
        return false;
      }
      plannedFiles += 1;
      plannedBytes += file.originalSize;
      if (plannedFiles > options.maxExtractedFiles) {
        throw new Error(
          `ZIP ${path.basename(zipPath)} exceeds ${options.maxExtractedFiles} extracted files.`,
        );
      }
      if (plannedBytes > options.maxExtractedBytes) {
        throw new Error(
          `ZIP ${path.basename(zipPath)} exceeds ${options.maxExtractedBytes} extracted bytes.`,
        );
      }
      return true;
    },
  });

  let files = 0;
  let bytes = 0;
  for (const [entryName, data] of Object.entries(entries)) {
    const relPath = safeStaticRelativePath(entryName);
    if (!relPath) continue;
    const destination = path.join(unpackedRoot, relPath);
    assertInsideDirectory(unpackedRoot, destination);
    await ensureStaticShardDirectory(unpackedRoot, path.dirname(destination));
    try {
      const existing = await stat(destination);
      if (existing.isDirectory()) {
        continue;
      }
    } catch {
      // No existing file.
    }
    await writeFile(destination, Buffer.from(data));
    files += 1;
    bytes += data.byteLength;
  }
  return { files, bytes };
}

function findStaticShardAppObject(
  state: GuildState | null | undefined,
  namespace: string,
  objectType: string,
  objectId: string,
) {
  if (!state?.appObjects) return undefined;
  for (const record of state.appObjects.values()) {
    if (
      record.namespace === namespace &&
      record.objectType === objectType &&
      record.objectId === objectId
    ) {
      return record;
    }
  }
  return undefined;
}

export function createStaticShardSeedPlugin(
  policy: StaticShardSeedPolicy = {},
): RelayPlugin {
  const sources = policy.sources ?? parseStaticShardSourcesFromEnv();
  const storeDir = path.resolve(
    policy.storeDir ||
      process.env.CGP_STATIC_SHARD_STORE_DIR ||
      "./relay-shards",
  );
  const registryPath = path.join(storeDir, "index.json");
  const maxShardBytes = Math.max(
    1,
    Math.floor(
      policy.maxShardBytes ??
        positiveIntegerFromEnv("CGP_STATIC_SHARD_MAX_BYTES", 25 * 1024 * 1024),
    ),
  );
  const maxManifestBytes = Math.max(
    1,
    Math.floor(
      policy.maxManifestBytes ??
        positiveIntegerFromEnv(
          "CGP_STATIC_SHARD_MAX_MANIFEST_BYTES",
          8 * 1024 * 1024,
        ),
    ),
  );
  const uploadMaxBytes = Math.max(
    maxManifestBytes,
    Math.floor(
      policy.uploadMaxBytes ??
        positiveIntegerFromEnv(
          "CGP_STATIC_SHARD_UPLOAD_MAX_BYTES",
          maxShardBytes * 12,
        ),
    ),
  );
  const requestTimeoutMs = Math.max(
    1000,
    Math.floor(
      policy.requestTimeoutMs ??
        positiveIntegerFromEnv("CGP_STATIC_SHARD_REQUEST_TIMEOUT_MS", 30_000),
    ),
  );
  const autoIngest =
    policy.autoIngest ??
    (sources.length > 0 && process.env.CGP_STATIC_SHARD_AUTO_INGEST !== "0");
  const pinToIpfs =
    policy.pinToIpfs ?? process.env.CGP_STATIC_SHARD_IPFS_PIN === "1";
  const ipfsApiUrl = policy.ipfsApiUrl || process.env.CGP_MEDIA_IPFS_API_URL;
  const ipfsBackendId =
    policy.ipfsBackendId || process.env.CGP_STATIC_SHARD_IPFS_BACKEND_ID;
  const ipfsGatewayUrl =
    process.env.CGP_MEDIA_IPFS_GATEWAY_URL || "https://ipfs.io/ipfs/{cid}";
  const extractPlayable =
    policy.extractPlayable ??
    process.env.CGP_STATIC_SHARD_EXTRACT_PLAYABLE !== "0";
  const servePlayableMode =
    policy.servePlayableMode ??
    normalizeStaticShardPlayableMode(process.env.CGP_STATIC_SHARD_PLAYABLE_MODE) ??
    "extract";
  const allowVerifiedRedirects =
    policy.allowVerifiedRedirects ??
    envFlag("CGP_STATIC_SHARD_ALLOW_REDIRECTS", servePlayableMode !== "extract");
  const maxExtractedBytes = Math.max(
    1,
    Math.floor(
      policy.maxExtractedBytes ??
        positiveIntegerFromEnv(
          "CGP_STATIC_SHARD_MAX_EXTRACTED_BYTES",
          maxShardBytes * 8,
        ),
    ),
  );
  const maxExtractedFiles = Math.max(
    1,
    Math.floor(
      policy.maxExtractedFiles ??
        positiveIntegerFromEnv("CGP_STATIC_SHARD_MAX_EXTRACTED_FILES", 20_000),
    ),
  );
  const publicHttpUrl =
    normalizeHttpBaseUrl(
      policy.publicHttpUrl ||
        process.env.CGP_STATIC_SHARD_PUBLIC_HTTP_URL ||
        process.env.CGP_RELAY_PUBLIC_URL ||
        "",
    );
  const publishHollowHomeObjects =
    policy.publishHollowHomeObjects ??
    process.env.CGP_STATIC_SHARD_PUBLISH_HOLLOW_HOME !== "0";
  const hollowHomeNamespace =
    policy.hollowHomeNamespace?.trim() || "app.hollow.home";
  const namespace = policy.namespace?.trim() || "org.cgp.games";
  const gameReleaseObjectType =
    policy.gameReleaseObjectType?.trim() || "game-release";
  const shardObjectType = policy.shardObjectType?.trim() || "static-shard";
  const createGameGuilds = policy.createGameGuilds ?? true;
  const releases = new Map<string, IngestedStaticShardRelease>();
  // Discovery pages contain one stable slot per game. A new version updates
  // the value served from that slot without moving it, so opaque cursors do
  // not skip a game or emit it twice while releases are being appended.
  const gameCatalogIds: string[] = [];
  const gameCatalogIndex = new Map<string, number>();
  const latestReleaseKeyByGameId = new Map<string, string>();
  const searchTokensByGameId = new Map<string, string[]>();
  const gameIdsBySearchToken = new Map<string, Set<string>>();
  interface SearchTokenTrieNode {
    children: Map<string, SearchTokenTrieNode>;
    token?: string;
  }
  const searchTokenTrie: SearchTokenTrieNode = { children: new Map() };
  const registryJournalPath = `${registryPath}.ndjson`;
  const registryCompactEvery = Math.max(
    1,
    Math.floor(
      policy.registryCompactEvery ??
        positiveIntegerFromEnv("CGP_STATIC_SHARD_REGISTRY_COMPACT_EVERY", 4096),
    ),
  );
  const requireSignedUploads =
    policy.requireSignedUploads ??
    process.env.CGP_STATIC_SHARD_REQUIRE_SIGNED_UPLOADS !== "0";
  const httpIngestToken = String(
    policy.httpIngestToken ?? process.env.CGP_STATIC_SHARD_INGEST_TOKEN ?? "",
  ).trim();
  const allowUnauthenticatedHttpIngest =
    policy.allowUnauthenticatedHttpIngest ??
    process.env.CGP_STATIC_SHARD_ALLOW_UNAUTHENTICATED_INGEST === "1";
  const publisherKeyByGameId = new Map<string, string>();
  const publisherDeviceAuthorities = new DeviceAuthorityRegistry(100_000);
  const legacyUnsignedGameIds = new Set<string>();
  const errors: Array<{ source: string; error: string; at: number }> = [];
  let ctxRef: RelayPluginContext | undefined;
  let initPromise: Promise<void> | undefined;
  let registryJournalEntries = 0;
  let registryPersistQueue = Promise.resolve();
  const gameIngestTails = new Map<string, Promise<void>>();

  const validPersistedPublisherProof = (
    value: unknown,
  ): value is StaticShardPublisherProof => {
    if (!isRecord(value)) return false;
    const protocol = stringField(value, "protocol");
    const publicKey = stringField(value, "publicKey").toLowerCase();
    const signature = stringField(value, "signature").toLowerCase();
    const payloadHash = stringField(value, "payloadHash").toLowerCase();
    const deviceAuthorization = value.deviceAuthorization as
      | DeviceAuthorization
      | undefined;
    const publisherVerification = deviceAuthorization
      ? publisherDeviceAuthorities.verify(
          payloadHash,
          signature,
          publicKey,
          deviceAuthorization,
          "publish",
        )
      : {
          ok:
            !publisherDeviceAuthorities.get(publicKey) &&
            verify(publicKey, payloadHash, signature),
        };
    return (
      protocol === STATIC_SHARD_PUBLISHER_PROTOCOL &&
      /^(02|03)[0-9a-f]{64}$/.test(publicKey) &&
      /^[0-9a-f]{128}$/.test(signature) &&
      /^[0-9a-f]{64}$/.test(payloadHash) &&
      publisherVerification.ok === true
    );
  };

  const assertPublisherContinuity = (
    gameId: string,
    publisher: StaticShardPublisherProof | undefined,
    options: { requireSigned: boolean },
  ) => {
    if (options.requireSigned && !publisher) {
      throw new Error(
        `Static shard upload ${gameId} requires a signed ${STATIC_SHARD_PUBLISHER_PROTOCOL} publisher proof.`,
      );
    }
    const existingPublisherKey = publisherKeyByGameId.get(gameId);
    if (existingPublisherKey) {
      if (!publisher || publisher.publicKey !== existingPublisherKey) {
        throw new Error(
          `Static shard game ${gameId} is owned by a different CGP publisher key.`,
        );
      }
      return;
    }
    if (legacyUnsignedGameIds.has(gameId) && publisher) {
      throw new Error(
        `Static shard game ${gameId} is an unsigned legacy mirror and cannot be claimed through public upload.`,
      );
    }
  };

  const rememberPublisher = (release: IngestedStaticShardRelease) => {
    if (release.publisher) {
      if (!validPersistedPublisherProof(release.publisher)) {
        throw new Error(
          `Persisted static shard release ${release.id}@${release.version} has an invalid publisher proof.`,
        );
      }
      assertPublisherContinuity(release.id, release.publisher, { requireSigned: false });
      publisherKeyByGameId.set(release.id, release.publisher.publicKey);
      return;
    }
    if (publisherKeyByGameId.has(release.id)) {
      throw new Error(
        `Persisted static shard release ${release.id}@${release.version} breaks publisher continuity.`,
      );
    }
    legacyUnsignedGameIds.add(release.id);
  };

  const requestHasIngestAuthority = (req: IncomingMessage) => {
    if (allowUnauthenticatedHttpIngest) return true;
    if (!httpIngestToken) return false;
    const authorization =
      typeof req.headers.authorization === "string" ? req.headers.authorization : "";
    const bearer = /^Bearer\s+(.+)$/i.exec(authorization)?.[1]?.trim() || "";
    const headerToken =
      typeof req.headers["x-cgp-static-shard-token"] === "string"
        ? req.headers["x-cgp-static-shard-token"].trim()
        : "";
    const supplied = bearer || headerToken;
    const expectedBytes = Buffer.from(httpIngestToken);
    const suppliedBytes = Buffer.from(supplied);
    return (
      expectedBytes.length === suppliedBytes.length &&
      expectedBytes.length > 0 &&
      timingSafeEqual(expectedBytes, suppliedBytes)
    );
  };

  const withGameIngestLock = async <T>(
    gameId: string,
    action: () => Promise<T>,
  ) => {
    const previous = gameIngestTails.get(gameId) ?? Promise.resolve();
    let openGate: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      openGate = () => resolve();
    });
    const tail = previous.catch(() => undefined).then(() => gate);
    gameIngestTails.set(gameId, tail);
    await previous.catch(() => undefined);
    try {
      return await action();
    } finally {
      openGate();
      if (gameIngestTails.get(gameId) === tail) {
        gameIngestTails.delete(gameId);
      }
    }
  };

  const searchTokensForRelease = (release: IngestedStaticShardRelease) => {
    const fields = [
      release.id,
      release.title,
      release.description,
      release.creatorId,
      release.creatorName,
      release.creatorUsername,
      ...(release.tags ?? []),
    ];
    const tokens = new Set<string>();
    for (const field of fields) {
      const normalized = String(field || "").trim().toLowerCase().slice(0, 512);
      if (!normalized) continue;
      const compact = normalizeMediaToken(normalized).slice(0, 128);
      if (compact) tokens.add(compact);
      for (const token of normalized.split(/[^a-z0-9]+/g)) {
        if (token.length >= 2) tokens.add(token.slice(0, 128));
      }
    }
    return [...tokens].slice(0, 64);
  };

  const addSearchToken = (token: string) => {
    let node = searchTokenTrie;
    for (const character of token) {
      let child = node.children.get(character);
      if (!child) {
        child = { children: new Map() };
        node.children.set(character, child);
      }
      node = child;
    }
    node.token = token;
  };

  const removeSearchToken = (token: string) => {
    const path: Array<{ node: SearchTokenTrieNode; character: string }> = [];
    let node = searchTokenTrie;
    for (const character of token) {
      const child = node.children.get(character);
      if (!child) return;
      path.push({ node, character });
      node = child;
    }
    delete node.token;
    for (let index = path.length - 1; index >= 0; index -= 1) {
      const { node: parent, character } = path[index];
      const child = parent.children.get(character);
      if (!child || child.token || child.children.size > 0) break;
      parent.children.delete(character);
    }
  };

  const searchTokensWithPrefix = (prefix: string, limit: number) => {
    let node = searchTokenTrie;
    for (const character of prefix) {
      const child = node.children.get(character);
      if (!child) return [];
      node = child;
    }
    const matches: string[] = [];
    const stack = [node];
    while (stack.length > 0 && matches.length < limit) {
      const current = stack.pop()!;
      if (current.token && gameIdsBySearchToken.has(current.token)) {
        matches.push(current.token);
      }
      for (const child of current.children.values()) stack.push(child);
    }
    return matches;
  };

  const indexLatestRelease = (release: IngestedStaticShardRelease, key: string) => {
    const previousKey = latestReleaseKeyByGameId.get(release.id);
    const previousRelease = previousKey ? releases.get(previousKey) : undefined;
    if (previousRelease && previousRelease.storedAt > release.storedAt) return;

    for (const token of searchTokensByGameId.get(release.id) ?? []) {
      const gameIds = gameIdsBySearchToken.get(token);
      gameIds?.delete(release.id);
      if (gameIds?.size === 0) {
        gameIdsBySearchToken.delete(token);
        removeSearchToken(token);
      }
    }
    const tokens = searchTokensForRelease(release);
    for (const token of tokens) {
      let gameIds = gameIdsBySearchToken.get(token);
      if (!gameIds) {
        gameIds = new Set<string>();
        gameIdsBySearchToken.set(token, gameIds);
        addSearchToken(token);
      }
      gameIds.add(release.id);
    }
    searchTokensByGameId.set(release.id, tokens);
    latestReleaseKeyByGameId.set(release.id, key);
  };

  const rememberRelease = (release: IngestedStaticShardRelease) => {
    const key = staticShardReleaseKey(release.id, release.version);
    rememberPublisher(release);
    if (!latestReleaseKeyByGameId.has(release.id)) {
      gameCatalogIndex.set(release.id, gameCatalogIds.length);
      gameCatalogIds.push(release.id);
    }
    releases.set(key, release);
    indexLatestRelease(release, key);
  };

  const existingImmutableRelease = (
    id: string,
    version: string,
    releaseSha256: string,
  ) => {
    const key = staticShardReleaseKey(id, version);
    const existing = releases.get(key);
    if (!existing) return undefined;
    if (existing.releaseSha256 !== releaseSha256) {
      throw new Error(
        `Static shard release ${key} is immutable and already has different content. Publish a new version instead.`,
      );
    }
    return existing;
  };

  const persist = async (release: IngestedStaticShardRelease) => {
    const operation = registryPersistQueue.then(async () => {
      await mkdir(path.dirname(registryJournalPath), { recursive: true });
      await appendFile(registryJournalPath, `${JSON.stringify(release)}\n`, "utf8");
      registryJournalEntries += 1;
      if (registryJournalEntries < registryCompactEvery) return;
      await writeStaticShardRegistry(registryPath, Array.from(releases.values()));
      await writeFile(registryJournalPath, "", "utf8");
      registryJournalEntries = 0;
    });
    registryPersistQueue = operation.catch(() => undefined);
    await operation;
  };

  const loadPersistedReleases = async () => {
    for (const release of await readStaticShardRegistry(registryPath)) {
      rememberRelease(release);
    }
    const journal = await readStaticShardRegistryJournal(registryJournalPath);
    for (const release of journal.releases) {
      rememberRelease(release);
    }
    registryJournalEntries = journal.entries;
    if (journal.invalidEntries > 0) {
      const repaired = journal.releases.length > 0
        ? `${journal.releases.map((release) => JSON.stringify(release)).join("\n")}\n`
        : "";
      await writeFile(registryJournalPath, repaired, "utf8");
    }
  };

  const preferredIpfsBackend = () => {
    const backends = ctxRef?.ipfsBackends;
    if (!backends || backends.size === 0) return undefined;
    if (ipfsBackendId) {
      return backends.get(ipfsBackendId);
    }
    return backends.get("helia") ?? backends.values().next().value;
  };

  const rememberError = (source: string, error: unknown) => {
    errors.push({
      source,
      error: error instanceof Error ? error.message : String(error),
      at: Date.now(),
    });
    while (errors.length > 50) errors.shift();
  };

  const publicBaseForRequest = (req?: IncomingMessage) =>
    normalizeHttpBaseUrl(publicHttpUrl || requestHttpBaseUrl(req));

  const releaseUrlForServePath = (
    release: IngestedStaticShardRelease,
    servePath?: string,
    req?: IncomingMessage,
  ) => absoluteStaticShardUrl(publicBaseForRequest(req), servePath);

  const appendReleaseLaunchQuery = (
    url: string | undefined,
    release: IngestedStaticShardRelease,
  ) => appendQueryToUrl(url, release.launchQuery);

  const externalReleaseUrl = (
    release: IngestedStaticShardRelease,
    relPath: string | undefined,
  ) => {
    if (!release.hosting) return undefined;
    for (const candidate of staticShardPlayablePathCandidates(release, relPath)) {
      if (candidate === release.entryPath && release.hosting.entryUrl) {
        return release.hosting.entryUrl;
      }
      const externalUrl = externalStaticShardUrl(release.hosting, candidate);
      if (externalUrl) {
        return externalUrl;
      }
    }
    return undefined;
  };

  const releaseHasExternalPlayable = (release: IngestedStaticShardRelease) =>
    Boolean(externalReleaseUrl(release, release.entryPath || undefined));

  const releaseEffectiveServeMode = (release: IngestedStaticShardRelease) => {
    const requested = release.hosting?.mode || servePlayableMode;
    if (
      allowVerifiedRedirects &&
      (requested === "redirect" || requested === "auto") &&
      releaseHasExternalPlayable(release)
    ) {
      return "redirect" as StaticShardPlayableMode;
    }
    if (release.unpacked || requested === "extract") {
      return "extract" as StaticShardPlayableMode;
    }
    return requested === "redirect" ? "extract" : requested;
  };

  const releasePlayUrl = (
    release: IngestedStaticShardRelease,
    req?: IncomingMessage,
  ) => appendReleaseLaunchQuery(
    releaseUrlForServePath(release, release.entryServePath || release.playServePath, req),
    release,
  );

  const releaseAssetUrl = (
    release: IngestedStaticShardRelease,
    value: string | undefined,
    req?: IncomingMessage,
  ) => {
    const raw = String(value || "").trim();
    if (!raw) return undefined;
    if (/^(https?:|data:|ipfs:)/i.test(raw)) return raw;
    const safeRel = safeStaticRelativePath(raw);
    if (!safeRel) return undefined;
    if (allowVerifiedRedirects && releaseEffectiveServeMode(release) === "redirect") {
      return externalReleaseUrl(release, safeRel);
    }
    return releaseUrlForServePath(release, staticShardPlayServePath(release.id, release.version, safeRel), req);
  };

  const publicReleasePayload = (
    release: IngestedStaticShardRelease,
    req?: IncomingMessage,
  ) => ({
    ...release,
    isCurrentRelease: latestReleaseKeyByGameId.get(release.id) === staticShardReleaseKey(release.id, release.version),
    publisherVerification: release.publisher
      ? "verified"
      : "legacy-operator-seed",
    playUrl: releasePlayUrl(release, req) || undefined,
    embedPlayUrl: releasePlayUrl(release, req) || undefined,
    externalPlayUrl: appendReleaseLaunchQuery(
      externalReleaseUrl(release, release.entryPath || undefined),
      release,
    ),
    serveMode: releaseEffectiveServeMode(release),
    assetServing: releaseEffectiveServeMode(release) === "redirect" ? "external" : "relay",
    thumbnailUrl: releaseAssetUrl(release, release.thumbnail, req),
    iconUrl: releaseAssetUrl(release, release.icon, req),
  });

  const registerReleaseInCgp = async (release: IngestedStaticShardRelease) => {
    const ctx = ctxRef;
    if (!ctx || !createGameGuilds) return release;
    const guildId = staticShardGameGuildId(release.id);
    const channelId = staticShardReleaseChannelId(release.id);
    let state = await ctx.getState?.(guildId);
    if (!state) {
      await ctx.publishAsRelay({
        type: "GUILD_CREATE",
        guildId,
        name: release.title,
        description:
          `CGP mirror for ${release.title} static shard releases.`,
        flags: { allowForksBy: "any" },
      } as EventBody);
      await ctx.publishAsRelay({
        type: "CHANNEL_CREATE",
        guildId,
        channelId,
        name: "releases",
        kind: "text",
        topic:
          "Verified static source/media shards mirrored by this CGP relay.",
      } as EventBody);
      state = await ctx.getState?.(guildId);
    }
    // A guild can arrive through federation before its release channel (or a
    // prior relay can fail between those two writes). Repair that partial
    // state before emitting channel-scoped objects so retries converge.
    if (!state?.channels?.has(channelId)) {
      await ctx.publishAsRelay({
        type: "CHANNEL_CREATE",
        guildId,
        channelId,
        name: "releases",
        kind: "text",
        topic:
          "Verified static source/media shards mirrored by this CGP relay.",
      } as EventBody);
      state = await ctx.getState?.(guildId);
    }

    const releaseObjectId = staticShardReleaseKey(release.id, release.version);
    const latestKey = latestReleaseKeyByGameId.get(release.id);
    const latest = latestKey ? releases.get(latestKey) : undefined;
    const playableUrl = releasePlayUrl(release) || undefined;
    const existingRelease = findStaticShardAppObject(
      state,
      namespace,
      gameReleaseObjectType,
      releaseObjectId,
    );
    const existingSha = isRecord(existingRelease?.value)
      ? stringField(existingRelease?.value, "releaseSha256")
      : "";
    if (existingSha !== release.releaseSha256 ||
        (Number(existingRelease?.value?.listingRevision) || 0) !== (release.listingRevision || 0)) {
      await ctx.publishAsRelay({
        type: "APP_OBJECT_UPSERT",
        guildId,
        namespace,
        objectType: gameReleaseObjectType,
        objectId: releaseObjectId,
        channelId,
        value: {
          id: release.id,
          isCurrentRelease: !latest || latest.storedAt <= release.storedAt,
          title: release.title,
          description: release.description,
          thumbnail: releaseAssetUrl(release, release.thumbnail),
          icon: releaseAssetUrl(release, release.icon),
          tags: release.tags,
          creatorId: release.creatorId,
          creatorName: release.creatorName,
          creatorUsername: release.creatorUsername,
          creatorAvatar: releaseAssetUrl(release, release.creatorAvatar),
          creatorBio: release.creatorBio,
          display: release.display,
          network: release.network,
          host: release.host,
          listingRevision: release.listingRevision,
          listingClaim: release.listingClaim,
          source: release.source,
          type: release.type,
          version: release.version,
          entryPath: release.entryPath,
          playServePath: release.playServePath,
          entryServePath: release.entryServePath,
          externalEntryUrl: release.externalEntryUrl,
          externalPlayUrl: release.externalPlayUrl,
          hosting: release.hosting,
          serveMode: releaseEffectiveServeMode(release),
          assetServing: releaseEffectiveServeMode(release) === "redirect" ? "external" : "relay",
          launchQuery: release.launchQuery,
          playUrl: playableUrl,
          embedPlayUrl: playableUrl,
          releaseUrl: release.releaseUrl,
          releaseSha256: release.releaseSha256,
          publisher: release.publisher,
          publisherVerification: release.publisher
            ? "verified"
            : "legacy-operator-seed",
          shardCount: release.shards.length,
          unpacked: release.unpacked,
          manifests: release.manifests,
          shards: release.shards,
          ipfsHosting: {
            scheme: "ipfs",
            available: release.shards.some((shard) => Boolean(shard.ipfsCid)),
            shards: release.shards.map((shard) => ({
              id: shard.id,
              kind: shard.kind,
              bytes: shard.bytes,
              sha256: shard.sha256,
              cid: shard.ipfsCid,
              uri: shard.ipfsCid ? `ipfs://${shard.ipfsCid}` : undefined,
              gatewayUrl: shard.ipfsGatewayUrl,
              relayServePath: shard.servePath,
            })),
          },
          forkSource: {
            strategy: "snapshot-shards",
            guarantee: "release-and-shard-sha256",
            releaseSha256: release.releaseSha256,
            releaseUrl: release.releaseUrl,
            manifests: release.manifests.map((manifest) => ({
              id: manifest.id,
              path: manifest.path,
              bytes: manifest.bytes,
              sha256: manifest.sha256,
              relayServePath: manifest.servePath,
            })),
            shards: release.shards.map((shard) => ({
              id: shard.id,
              kind: shard.kind,
              path: shard.path,
              bytes: shard.bytes,
              sha256: shard.sha256,
              relayServePath: shard.servePath,
              ipfsCid: shard.ipfsCid,
              ipfsUri: shard.ipfsCid ? `ipfs://${shard.ipfsCid}` : undefined,
            })),
          },
          mirroredAt: release.storedAt,
        },
      } as EventBody);
    }

    if (publishHollowHomeObjects && release.type === "game" &&
        (!latest || latest.storedAt <= release.storedAt)) {
      const creatorId = release.creatorId || staticShardDefaultCreatorId(release.id);
      const creatorName =
        release.creatorName || staticShardDefaultCreatorName(release.title, creatorId);
      const creatorUsername = release.creatorUsername || creatorId;
      await ctx.publishAsRelay({
        type: "APP_OBJECT_UPSERT",
        guildId,
        namespace: hollowHomeNamespace,
        objectType: "game-profile",
        objectId: `game:${release.id}`,
        channelId,
        target: { gameId: release.id },
        value: {
          title: release.title,
          description: release.description,
          thumbnail: releaseAssetUrl(release, release.thumbnail),
          icon: releaseAssetUrl(release, release.icon),
          tags: release.tags,
          playUrl: playableUrl,
          embedPlayUrl: playableUrl,
          externalPlayUrl: release.externalPlayUrl,
          display: release.display,
          compatibilityId: release.network?.compatibilityId,
          requestedFeatures: release.host?.requestedFeatures,
          version: release.version,
          releaseSha256: release.releaseSha256,
          serveMode: releaseEffectiveServeMode(release),
          guildId,
          creatorId,
          updatedAt: new Date(release.listingUpdatedAt || release.storedAt).toISOString(),
        },
      } as EventBody);
      await ctx.publishAsRelay({
        type: "APP_OBJECT_UPSERT",
        guildId,
        namespace: hollowHomeNamespace,
        objectType: "creator-profile",
        objectId: `creator:${creatorId}`,
        channelId,
        target: { creatorId },
        value: {
          username: creatorUsername,
          name: creatorName,
          avatar: releaseAssetUrl(release, release.creatorAvatar),
          bio: release.creatorBio,
          updatedAt: new Date(release.storedAt).toISOString(),
        },
      } as EventBody);
      await ctx.publishAsRelay({
        type: "APP_OBJECT_UPSERT",
        guildId,
        namespace: hollowHomeNamespace,
        objectType: "game-source",
        objectId: `source:${release.id}`,
        channelId,
        target: { gameId: release.id },
        value: {
          repositoryUrl:
            stringField(release.source, "repositoryUrl") ||
            stringField(release.source, "url") ||
            release.releaseUrl,
          cloneUrl: stringField(release.source, "cloneUrl") || undefined,
          branch:
            stringField(release.source, "branch") ||
            release.sourceBranch ||
            release.version,
          host: stringField(release.source, "host") || "cgp-static-shards",
          license: undefined,
          updatedAt: new Date(release.storedAt).toISOString(),
        },
      } as EventBody);
    }

    for (const shard of release.shards) {
      const shardObjectId = `${releaseObjectId}:${shard.id}`;
      await ctx.publishAsRelay({
        type: "APP_OBJECT_UPSERT",
        guildId,
        namespace,
        objectType: shardObjectType,
        objectId: shardObjectId,
        channelId,
        value: {
          gameId: release.id,
          version: release.version,
          ...shard,
          releaseSha256: release.releaseSha256,
          mirroredAt: release.storedAt,
        },
      } as EventBody);
    }

    release.guildId = guildId;
    release.channelId = channelId;
    return release;
  };

  const ingestRelease = async (source: StaticShardSeedSource) => {
    const releaseUrl = source.url;
    const releaseDownload = await requestStaticShardJson(releaseUrl, {
      maxBytes: maxManifestBytes,
      timeoutMs: requestTimeoutMs,
    });
    if (
      source.expectedSha256 &&
      releaseDownload.sha256 !== source.expectedSha256
    ) {
      throw new Error(
        `Release hash mismatch for ${releaseUrl}: ${releaseDownload.sha256} != ${source.expectedSha256}`,
      );
    }
    const publisher = verifyStaticShardReleasePublisher(releaseDownload.json, {
      deviceAuthorityRegistry: publisherDeviceAuthorities,
    });
    const release = normalizeStaticShardRelease(releaseDownload.json);
    return withGameIngestLock(release.id, async () => {
    assertPublisherContinuity(release.id, publisher, { requireSigned: false });
    const existingRelease = existingImmutableRelease(
      release.id,
      release.version,
      releaseDownload.sha256,
    );
    if (existingRelease) return registerReleaseInCgp(existingRelease);
    const releaseMaxShardBytes =
      release.shardPolicy?.maxShardBytes &&
      release.shardPolicy.maxShardBytes > 0
        ? Math.min(maxShardBytes, release.shardPolicy.maxShardBytes)
        : maxShardBytes;
    const releasePrefix = staticShardStoreReleasePrefix(
      release.id,
      release.version,
    );
    const releaseDir = path.join(storeDir, releasePrefix);
    assertInsideDirectory(storeDir, releaseDir);
    await mkdir(releaseDir, { recursive: true });
    const unpackedRoot = staticShardReleaseUnpackedRoot(storeDir, release.id, release.version);
    assertInsideDirectory(storeDir, unpackedRoot);
    await writeFile(path.join(releaseDir, "release.json"), releaseDownload.buffer);
    await writeFile(
      path.join(releaseDir, "release.json.sha256"),
      `${releaseDownload.sha256}  release.json\n`,
    );

    const ingestedManifests: IngestedStaticShardRelease["manifests"] = [];
    let gameMetadata = normalizeStaticShardGameMetadata(release, undefined);
    for (const [manifestId, manifest] of Object.entries(
      release.manifests ?? {},
    )) {
      const manifestRel = safeStaticRelativePath(manifest.path);
      if (!manifestRel) continue;
      const destination = path.join(releaseDir, manifestRel);
      assertInsideDirectory(releaseDir, destination);
      const downloaded = await downloadStaticShardFile(
        resolveStaticShardUrl(releaseUrl, manifestRel),
        destination,
        { maxBytes: maxManifestBytes, timeoutMs: requestTimeoutMs },
      );
      if (manifest.sha256 && downloaded.sha256 !== manifest.sha256) {
        throw new Error(
          `Manifest hash mismatch for ${manifestRel}: ${downloaded.sha256} != ${manifest.sha256}`,
        );
      }
      ingestedManifests.push({
        id: manifestId,
        path: manifestRel,
        sha256: manifest.sha256,
        bytes: downloaded.bytes,
        servePath: staticShardServePath(`${releasePrefix}/${manifestRel}`),
      });
      if (
        manifestId === "game" ||
        /(^|\/)hollow\.game\.json$/i.test(manifestRel)
      ) {
        gameMetadata = await readStaticShardGameMetadata(release, destination);
      }
    }

    const releaseHosting = release.hosting;
    const hasExternalPlayable = Boolean(
      externalStaticShardUrl(releaseHosting, gameMetadata.entryPath || undefined),
    );
    const extractThisRelease =
      extractPlayable &&
      !(
        allowVerifiedRedirects &&
        servePlayableMode === "redirect" &&
        hasExternalPlayable
      );
    if (extractThisRelease) {
      await rm(unpackedRoot, { recursive: true, force: true });
      await mkdir(unpackedRoot, { recursive: true });
    }

    const ingestedShards: IngestedStaticShardRelease["shards"] = [];
    let extractedFiles = 0;
    let extractedBytes = 0;
    for (const shard of release.shards) {
      if (shard.bytes && shard.bytes > releaseMaxShardBytes) {
        throw new Error(
          `Shard ${shard.id} declares ${shard.bytes} bytes beyond ${releaseMaxShardBytes} byte limit.`,
        );
      }
      const destination = path.join(releaseDir, shard.path);
      assertInsideDirectory(releaseDir, destination);
      const downloaded = await downloadStaticShardFile(
        resolveStaticShardUrl(releaseUrl, shard.path),
        destination,
        { maxBytes: releaseMaxShardBytes, timeoutMs: requestTimeoutMs },
      );
      if (downloaded.sha256 !== shard.sha256) {
        throw new Error(
          `Shard hash mismatch for ${shard.id}: ${downloaded.sha256} != ${shard.sha256}`,
        );
      }
      await writeFile(
        `${destination}.sha256`,
        `${downloaded.sha256}  ${path.basename(shard.path)}\n`,
      );
      if (extractThisRelease && path.extname(shard.path).toLowerCase() === ".zip") {
        if (extractedFiles >= maxExtractedFiles || extractedBytes >= maxExtractedBytes) {
          throw new Error(
            `Release ${release.id}@${release.version} exceeds playable extraction limits.`,
          );
        }
        const extracted = await extractStaticShardZip(destination, unpackedRoot, {
          maxExtractedBytes: Math.max(1, maxExtractedBytes - extractedBytes),
          maxExtractedFiles: Math.max(1, maxExtractedFiles - extractedFiles),
        });
        extractedFiles += extracted.files;
        extractedBytes += extracted.bytes;
      }
      let ipfsCid: string | undefined;
      let ipfsPinnedGatewayUrl: string | undefined;
      if (pinToIpfs) {
        const backend = preferredIpfsBackend();
        if (backend) {
          const added = await backend.addFile({
            path: destination,
            name: path.basename(shard.path),
            sha256: downloaded.sha256,
            pin: true,
            metadata: {
              kind: "cgp-static-shard",
              release: staticShardReleaseKey(release.id, release.version),
              shardId: shard.id,
              shardKind: shard.kind,
            },
          });
          ipfsCid = added.cid;
          ipfsPinnedGatewayUrl = added.gatewayUrl;
        } else if (ipfsApiUrl) {
          ipfsCid = await addStaticShardFileToIpfs(
            ipfsApiUrl,
            destination,
            requestTimeoutMs,
          );
        } else {
          throw new Error(
            "Static shard IPFS pinning is enabled but no CGP IPFS backend or Kubo API URL is configured.",
          );
        }
      }
      ingestedShards.push({
        id: shard.id,
        kind: shard.kind,
        path: shard.path,
        bytes: downloaded.bytes,
        sha256: downloaded.sha256,
        servePath: staticShardServePath(`${releasePrefix}/${shard.path}`),
        ipfsCid,
        ipfsGatewayUrl: ipfsCid
          ? ipfsPinnedGatewayUrl ?? gatewayUrlForCid(ipfsGatewayUrl, ipfsCid)
          : undefined,
      });
    }

    if (extractThisRelease) await reassembleStaticFiles(unpackedRoot, release.fileChunks, maxExtractedBytes, maxExtractedFiles);
    const ingested: IngestedStaticShardRelease = {
      id: release.id,
      title: gameMetadata.title || release.title || release.id,
      description: gameMetadata.description,
      thumbnail: gameMetadata.thumbnail,
      icon: gameMetadata.icon,
      tags: gameMetadata.tags?.length ? gameMetadata.tags : undefined,
      creatorId: gameMetadata.creatorId,
      creatorName: gameMetadata.creatorName,
      creatorUsername: gameMetadata.creatorUsername,
      creatorAvatar: gameMetadata.creatorAvatar,
      creatorBio: gameMetadata.creatorBio,
      display: gameMetadata.display,
      source: isRecord(release.source) ? release.source : undefined,
      network: gameMetadata.network,
      host: gameMetadata.host,
      sourceBranch: gameMetadata.sourceBranch,
      launchQuery: gameMetadata.launchQuery,
      hosting: releaseHosting,
      type: release.type || "game",
      version: release.version,
      releaseUrl,
      releaseSha256: releaseDownload.sha256,
      storedAt: Date.now(),
      publisher,
      entryPath: gameMetadata.entryPath,
      playServePath: staticShardPlayServePath(release.id, release.version),
      entryServePath: gameMetadata.entryPath
        ? staticShardPlayServePath(release.id, release.version, gameMetadata.entryPath)
        : undefined,
      externalEntryUrl: externalStaticShardUrl(releaseHosting, gameMetadata.entryPath || undefined),
      externalPlayUrl: appendQueryToUrl(
        externalStaticShardUrl(releaseHosting, gameMetadata.entryPath || undefined),
        gameMetadata.launchQuery,
      ),
      serveMode:
        allowVerifiedRedirects && hasExternalPlayable && servePlayableMode !== "extract"
          ? "redirect"
          : "extract",
      unpacked: extractedFiles > 0
        ? {
            files: extractedFiles,
            bytes: extractedBytes,
            extractedAt: Date.now(),
          }
        : undefined,
      manifests: ingestedManifests,
      shards: ingestedShards,
    };
    await registerReleaseInCgp(ingested);
    const previousRelease = releases.get(staticShardReleaseKey(ingested.id, ingested.version));
    rememberRelease(ingested);
    if (previousRelease?.releaseSha256 !== ingested.releaseSha256) {
      await persist(ingested);
    }
    return ingested;
    });
  };

  const ingestCatalog = async (source: StaticShardSeedSource) => {
    const catalogDownload = await requestStaticShardJson(source.url, {
      maxBytes: maxManifestBytes,
      timeoutMs: requestTimeoutMs,
    });
    if (
      source.expectedSha256 &&
      catalogDownload.sha256 !== source.expectedSha256
    ) {
      throw new Error(
        `Catalog hash mismatch for ${source.url}: ${catalogDownload.sha256} != ${source.expectedSha256}`,
      );
    }
    const catalog = isRecord(catalogDownload.json) ? catalogDownload.json : {};
    const releaseSources: StaticShardSeedSource[] = [];
    if (Array.isArray(catalog.projects)) {
      for (const project of catalog.projects) {
        if (!isRecord(project)) continue;
        const latestManifest = stringField(project, "latestManifest");
        if (!latestManifest) continue;
        releaseSources.push({
          url: resolveStaticShardUrl(source.url, latestManifest),
          kind: "release",
          expectedSha256: stringField(project, "latestSha256").toLowerCase(),
        });
      }
    }
    if (isRecord(catalog.games)) {
      for (const value of Object.values(catalog.games)) {
        if (!isRecord(value)) continue;
        const latestManifest = stringField(value, "latestManifest");
        if (!latestManifest) continue;
        releaseSources.push({
          url: resolveStaticShardUrl(source.url, latestManifest),
          kind: "release",
          expectedSha256: stringField(value, "latestSha256").toLowerCase(),
        });
      }
    }
    const unique = new Map<string, StaticShardSeedSource>();
    for (const releaseSource of releaseSources) {
      if (
        releaseSource.expectedSha256 &&
        !/^[0-9a-f]{64}$/.test(releaseSource.expectedSha256)
      ) {
        releaseSource.expectedSha256 = undefined;
      }
      unique.set(releaseSource.url, releaseSource);
    }
    const ingested: IngestedStaticShardRelease[] = [];
    for (const releaseSource of unique.values()) {
      ingested.push(await ingestRelease(releaseSource));
    }
    return ingested;
  };

  const ingestSource = async (source: unknown) => {
    const normalized = normalizeStaticShardSeedSource(source);
    if (!normalized) {
      throw new Error("Invalid static shard seed source.");
    }
    if (normalized.kind === "release") {
      return [await ingestRelease(normalized)];
    }
    return await ingestCatalog(normalized);
  };

  const uploadCache = new StaticShardUploadCache(path.join(storeDir, "pending-upload-blobs"));
  const uploadReferences = (release: ReturnType<typeof normalizeStaticShardRelease>) => [
    ...Object.values(release.manifests ?? {}), ...release.shards
  ];
  const stageUpload = async (body: Record<string, unknown>, write: boolean) => {
    const raw = JSON.parse(staticShardUploadReleaseBytes(body, maxManifestBytes).toString("utf8"));
    const publisher = verifyStaticShardReleasePublisher(raw, { deviceAuthorityRegistry: publisherDeviceAuthorities });
    const release = normalizeStaticShardRelease(raw);
    assertPublisherContinuity(release.id, publisher, { requireSigned: true });
    const references = uploadReferences(release);
    if (references.length > maxExtractedFiles) throw new Error("Too many upload references.");
    if (write) {
      const files = staticShardUploadFileList(body, Math.max(maxShardBytes, maxManifestBytes));
      if (files.size !== 1) throw new Error("Upload exactly one content blob at a time.");
      for (const file of files.values()) {
        if (!references.some(ref => ref.path === file.path && ref.sha256 === file.sha256)) throw new Error("Blob is not authorized by this signed release.");
        await uploadCache.put(file.sha256, file.buffer);
      }
      return { ok: true };
    }
    const missing: string[] = [];
    for (const ref of references) if (!ref.sha256 || !await uploadCache.get(ref.sha256)) missing.push(ref.sha256 || "");
    return { ok: true, protocol: "cgp-static-shard-upload/2", missing };
  };

  const ingestUpload = async (body: Record<string, unknown>, req?: IncomingMessage) => {
    const releaseBuffer = staticShardUploadReleaseBytes(body, maxManifestBytes);
    const releaseSha256 = createHash("sha256").update(releaseBuffer).digest("hex");
    const expectedReleaseSha256 =
      stringField(body, "expectedSha256").toLowerCase() ||
      stringField(body, "releaseSha256").toLowerCase();
    if (expectedReleaseSha256 && expectedReleaseSha256 !== releaseSha256) {
      throw new Error(
        `Uploaded release hash mismatch: ${releaseSha256} != ${expectedReleaseSha256}.`,
      );
    }
    let releaseJson: unknown;
    try {
      releaseJson = JSON.parse(releaseBuffer.toString("utf8"));
    } catch (error: any) {
      throw new Error(`Uploaded release JSON is invalid: ${error?.message ?? String(error)}`);
    }
    const publisher = verifyStaticShardReleasePublisher(releaseJson, {
      deviceAuthorityRegistry: publisherDeviceAuthorities,
    });
    const release = normalizeStaticShardRelease(releaseJson);
    return withGameIngestLock(release.id, async () => {
    assertPublisherContinuity(release.id, publisher, {
      requireSigned: requireSignedUploads,
    });
    const existingRelease = existingImmutableRelease(
      release.id,
      release.version,
      releaseSha256,
    );
    if (existingRelease) return registerReleaseInCgp(existingRelease);
    const releaseMaxShardBytes =
      release.shardPolicy?.maxShardBytes &&
      release.shardPolicy.maxShardBytes > 0
        ? Math.min(maxShardBytes, release.shardPolicy.maxShardBytes)
        : maxShardBytes;
    const uploadFiles = staticShardUploadFileList(
      body,
      Math.max(releaseMaxShardBytes, maxManifestBytes),
    );
    const getUpload = async (relative: string, hash?: string) => {
      const provided = uploadFiles.get(relative);
      if (provided) return provided;
      if (!hash) return undefined;
      const buffer = await uploadCache.get(hash);
      return buffer ? { path: relative, buffer, bytes: buffer.length, sha256: hash } : undefined;
    };
    const releasePrefix = staticShardStoreReleasePrefix(
      release.id,
      release.version,
    );
    const releaseDir = path.join(storeDir, releasePrefix);
    assertInsideDirectory(storeDir, releaseDir);
    await mkdir(releaseDir, { recursive: true });
    const unpackedRoot = staticShardReleaseUnpackedRoot(storeDir, release.id, release.version);
    assertInsideDirectory(storeDir, unpackedRoot);
    await writeFile(path.join(releaseDir, "release.json"), releaseBuffer);
    await writeFile(
      path.join(releaseDir, "release.json.sha256"),
      `${releaseSha256}  release.json\n`,
    );

    const ingestedManifests: IngestedStaticShardRelease["manifests"] = [];
    let gameMetadata = normalizeStaticShardGameMetadata(release, undefined);
    for (const [manifestId, manifest] of Object.entries(
      release.manifests ?? {},
    )) {
      const manifestRel = safeStaticRelativePath(manifest.path);
      if (!manifestRel) continue;
      const upload = await getUpload(manifestRel, manifest.sha256);
      if (!upload) {
        throw new Error(`Upload is missing manifest ${manifestRel}.`);
      }
      if (upload.bytes > maxManifestBytes) {
        throw new Error(`Manifest ${manifestRel} exceeds ${maxManifestBytes} bytes.`);
      }
      if (manifest.sha256 && upload.sha256 !== manifest.sha256) {
        throw new Error(
          `Manifest hash mismatch for ${manifestRel}: ${upload.sha256} != ${manifest.sha256}.`,
        );
      }
      const destination = path.join(releaseDir, manifestRel);
      assertInsideDirectory(releaseDir, destination);
      await mkdir(path.dirname(destination), { recursive: true });
      await writeFile(destination, upload.buffer);
      ingestedManifests.push({
        id: manifestId,
        path: manifestRel,
        sha256: manifest.sha256,
        bytes: upload.bytes,
        servePath: staticShardServePath(`${releasePrefix}/${manifestRel}`),
      });
      if (
        manifestId === "game" ||
        /(^|\/)hollow\.game\.json$/i.test(manifestRel)
      ) {
        gameMetadata = await readStaticShardGameMetadata(release, destination);
      }
    }

    const releaseHosting = release.hosting;
    const hasExternalPlayable = Boolean(
      externalStaticShardUrl(releaseHosting, gameMetadata.entryPath || undefined),
    );
    const extractThisRelease =
      extractPlayable &&
      !(
        allowVerifiedRedirects &&
        servePlayableMode === "redirect" &&
        hasExternalPlayable
      );
    if (extractThisRelease) {
      await rm(unpackedRoot, { recursive: true, force: true });
      await mkdir(unpackedRoot, { recursive: true });
    }

    const ingestedShards: IngestedStaticShardRelease["shards"] = [];
    let extractedFiles = 0;
    let extractedBytes = 0;
    for (const shard of release.shards) {
      if (shard.bytes && shard.bytes > releaseMaxShardBytes) {
        throw new Error(
          `Shard ${shard.id} declares ${shard.bytes} bytes beyond ${releaseMaxShardBytes} byte limit.`,
        );
      }
      const shardRel = safeStaticRelativePath(shard.path);
      const upload = await getUpload(shardRel, shard.sha256);
      if (!upload) {
        throw new Error(`Upload is missing shard ${shardRel}.`);
      }
      if (upload.bytes > releaseMaxShardBytes) {
        throw new Error(`Shard ${shard.id} exceeds ${releaseMaxShardBytes} bytes.`);
      }
      if (upload.sha256 !== shard.sha256) {
        throw new Error(
          `Shard hash mismatch for ${shard.id}: ${upload.sha256} != ${shard.sha256}.`,
        );
      }
      const destination = path.join(releaseDir, shard.path);
      assertInsideDirectory(releaseDir, destination);
      await mkdir(path.dirname(destination), { recursive: true });
      await writeFile(destination, upload.buffer);
      await writeFile(
        `${destination}.sha256`,
        `${upload.sha256}  ${path.basename(shard.path)}\n`,
      );
      if (extractThisRelease && path.extname(shard.path).toLowerCase() === ".zip") {
        if (extractedFiles >= maxExtractedFiles || extractedBytes >= maxExtractedBytes) {
          throw new Error(
            `Release ${release.id}@${release.version} exceeds playable extraction limits.`,
          );
        }
        const extracted = await extractStaticShardZip(destination, unpackedRoot, {
          maxExtractedBytes: Math.max(1, maxExtractedBytes - extractedBytes),
          maxExtractedFiles: Math.max(1, maxExtractedFiles - extractedFiles),
        });
        extractedFiles += extracted.files;
        extractedBytes += extracted.bytes;
      }
      let ipfsCid: string | undefined;
      let ipfsPinnedGatewayUrl: string | undefined;
      if (pinToIpfs) {
        const backend = preferredIpfsBackend();
        if (backend) {
          const added = await backend.addFile({
            path: destination,
            name: path.basename(shard.path),
            sha256: upload.sha256,
            pin: true,
            metadata: {
              kind: "cgp-static-shard",
              release: staticShardReleaseKey(release.id, release.version),
              shardId: shard.id,
              shardKind: shard.kind,
            },
          });
          ipfsCid = added.cid;
          ipfsPinnedGatewayUrl = added.gatewayUrl;
        } else if (ipfsApiUrl) {
          ipfsCid = await addStaticShardFileToIpfs(
            ipfsApiUrl,
            destination,
            requestTimeoutMs,
          );
        } else {
          throw new Error(
            "Static shard IPFS pinning is enabled but no CGP IPFS backend or Kubo API URL is configured.",
          );
        }
      }
      ingestedShards.push({
        id: shard.id,
        kind: shard.kind,
        path: shard.path,
        bytes: upload.bytes,
        sha256: upload.sha256,
        servePath: staticShardServePath(`${releasePrefix}/${shard.path}`),
        ipfsCid,
        ipfsGatewayUrl: ipfsCid
          ? ipfsPinnedGatewayUrl ?? gatewayUrlForCid(ipfsGatewayUrl, ipfsCid)
          : undefined,
      });
    }

    if (extractThisRelease) await reassembleStaticFiles(unpackedRoot, release.fileChunks, maxExtractedBytes, maxExtractedFiles);

    const uploadedReleaseServePath = staticShardServePath(`${releasePrefix}/release.json`);
    const ingested: IngestedStaticShardRelease = {
      id: release.id,
      title: gameMetadata.title || release.title || release.id,
      description: gameMetadata.description,
      thumbnail: gameMetadata.thumbnail,
      icon: gameMetadata.icon,
      tags: gameMetadata.tags?.length ? gameMetadata.tags : undefined,
      creatorId: gameMetadata.creatorId,
      creatorName: gameMetadata.creatorName,
      creatorUsername: gameMetadata.creatorUsername,
      creatorAvatar: gameMetadata.creatorAvatar,
      creatorBio: gameMetadata.creatorBio,
      display: gameMetadata.display,
      source: isRecord(release.source) ? release.source : undefined,
      network: gameMetadata.network,
      host: gameMetadata.host,
      sourceBranch: gameMetadata.sourceBranch,
      launchQuery: gameMetadata.launchQuery,
      hosting: releaseHosting,
      type: release.type || "game",
      version: release.version,
      releaseUrl: absoluteStaticShardUrl(publicBaseForRequest(req), uploadedReleaseServePath) || uploadedReleaseServePath,
      releaseSha256,
      storedAt: Date.now(),
      publisher,
      entryPath: gameMetadata.entryPath,
      playServePath: staticShardPlayServePath(release.id, release.version),
      entryServePath: gameMetadata.entryPath
        ? staticShardPlayServePath(release.id, release.version, gameMetadata.entryPath)
        : undefined,
      externalEntryUrl: externalStaticShardUrl(releaseHosting, gameMetadata.entryPath || undefined),
      externalPlayUrl: appendQueryToUrl(
        externalStaticShardUrl(releaseHosting, gameMetadata.entryPath || undefined),
        gameMetadata.launchQuery,
      ),
      serveMode:
        allowVerifiedRedirects && hasExternalPlayable && servePlayableMode !== "extract"
          ? "redirect"
          : "extract",
      unpacked: extractedFiles > 0
        ? {
            files: extractedFiles,
            bytes: extractedBytes,
            extractedAt: Date.now(),
          }
        : undefined,
      manifests: ingestedManifests,
      shards: ingestedShards,
    };
    await registerReleaseInCgp(ingested);
    const previousRelease = releases.get(staticShardReleaseKey(ingested.id, ingested.version));
    rememberRelease(ingested);
    if (previousRelease?.releaseSha256 !== ingested.releaseSha256) {
      await persist(ingested);
    }
    return ingested;
    });
  };

  const ingestConfiguredSources = async () => {
    await mkdir(storeDir, { recursive: true });
    await loadPersistedReleases();
    for (const source of sources) {
      try {
        await ingestSource(source);
      } catch (error) {
        rememberError(source.url, error);
        console.error(
          `Static shard seed ingest failed for ${source.url}:`,
          error,
        );
      }
    }
  };

  const registryPayload = (req?: IncomingMessage) => ({
    kind: "cgp-relay-static-shard-registry",
    schemaVersion: 1,
    storeDir,
    releases: Array.from(releases.values()).map((release) =>
      publicReleasePayload(release, req),
    ),
  });

  const catalogPagePayload = (req: IncomingMessage) => {
    const requestUrl = new URL(req.url || "/", "http://localhost");
    const rawLimit = requestUrl.searchParams.get("limit");
    const requestedLimit = rawLimit === null ? Number.NaN : Number(rawLimit);
    const limit = Math.min(
      256,
      Math.max(1, Number.isFinite(requestedLimit) ? Math.floor(requestedLimit) : 64),
    );
    const encodedCursor = requestUrl.searchParams.get("cursor")?.trim() || "";
    let nextIndex = gameCatalogIds.length - 1;
    if (encodedCursor) {
      let cursorGameId = "";
      try {
        cursorGameId = Buffer.from(encodedCursor, "base64url").toString("utf8");
      } catch {
        cursorGameId = "";
      }
      const cursorIndex = gameCatalogIndex.get(cursorGameId);
      if (cursorIndex === undefined) {
        return { ok: false as const, error: "Invalid or expired catalog cursor." };
      }
      nextIndex = cursorIndex - 1;
    }

    const pageReleases: Record<string, unknown>[] = [];
    let lastGameId = "";
    while (nextIndex >= 0 && pageReleases.length < limit) {
      const gameId = gameCatalogIds[nextIndex];
      nextIndex -= 1;
      const release = latestReleaseForGameId(gameId);
      if (!release) continue;
      pageReleases.push(publicReleasePayload(release, req));
      lastGameId = gameId;
    }
    const hasMore = nextIndex >= 0;
    return {
      ok: true as const,
      kind: "cgp-relay-static-shard-catalog-page",
      schemaVersion: 2,
      publicHttpUrl: publicBaseForRequest(req),
      total: gameCatalogIds.length,
      releases: pageReleases,
      page: {
        limit,
        count: pageReleases.length,
        hasMore,
        nextCursor: hasMore && lastGameId
          ? Buffer.from(lastGameId, "utf8").toString("base64url")
          : null,
      },
    };
  };

  const latestReleaseForGameId = (gameId: string) => {
    const key = latestReleaseKeyByGameId.get(normalizeMediaToken(gameId));
    return key ? releases.get(key) : undefined;
  };

  const catalogLookupPayload = (req: IncomingMessage, rawGameId: string) => {
    const gameId = normalizeMediaToken(rawGameId);
    const release = gameId ? latestReleaseForGameId(gameId) : undefined;
    return {
      ok: true,
      kind: "cgp-relay-static-shard-catalog-lookup",
      schemaVersion: 2,
      publicHttpUrl: publicBaseForRequest(req),
      gameId,
      releases: release ? [publicReleasePayload(release, req)] : [],
    };
  };

  const catalogSearchPayload = (
    req: IncomingMessage,
    rawQuery: string,
    rawLimit: string | null,
  ) => {
    const query = rawQuery.trim().toLowerCase().slice(0, 128);
    const compactQuery = normalizeMediaToken(query);
    const wordTerms = query.split(/[^a-z0-9]+/g).filter((term) => term.length >= 2);
    const terms = [...new Set(
      wordTerms.length > 1 ? wordTerms : [compactQuery, ...wordTerms],
    )].filter(Boolean).slice(0, 8);
    const requestedLimit = rawLimit === null ? Number.NaN : Number(rawLimit);
    const limit = Math.min(
      64,
      Math.max(1, Number.isFinite(requestedLimit) ? Math.floor(requestedLimit) : 24),
    );
    let candidates: Set<string> | undefined;
    for (const term of terms) {
      const matches = new Set<string>();
      const exactMatches = gameIdsBySearchToken.get(term);
      for (const gameId of exactMatches ?? []) matches.add(gameId);
      for (const token of searchTokensWithPrefix(term, 256)) {
        for (const gameId of gameIdsBySearchToken.get(token) ?? []) {
          matches.add(gameId);
          if (matches.size >= 4096) break;
        }
        if (matches.size >= 4096) break;
      }
      if (!candidates) candidates = matches;
      else candidates = new Set([...candidates].filter((gameId) => matches.has(gameId)));
      if (candidates.size === 0) break;
    }
    if (compactQuery && latestReleaseKeyByGameId.has(compactQuery)) {
      (candidates ??= new Set()).add(compactQuery);
    }

    const scored = [...(candidates ?? [])]
      .map((gameId) => latestReleaseForGameId(gameId))
      .filter((release): release is IngestedStaticShardRelease => Boolean(release))
      .map((release) => {
        const id = release.id.toLowerCase();
        const title = release.title.toLowerCase();
        const tokens = searchTokensByGameId.get(release.id) ?? [];
        const score =
          id === compactQuery ? 0 :
          title === query ? 1 :
          id.startsWith(compactQuery) || title.startsWith(query) ? 2 :
          terms.every((term) => tokens.includes(term)) ? 3 : 4;
        return { release, score };
      })
      .sort(
        (left, right) =>
          left.score - right.score ||
          right.release.storedAt - left.release.storedAt ||
          left.release.id.localeCompare(right.release.id),
      );
    return {
      ok: true,
      kind: "cgp-relay-static-shard-catalog-search",
      schemaVersion: 2,
      publicHttpUrl: publicBaseForRequest(req),
      query,
      total: scored.length,
      releases: scored.slice(0, limit).map(({ release }) => publicReleasePayload(release, req)),
      page: {
        limit,
        count: Math.min(limit, scored.length),
        hasMore: scored.length > limit,
        nextCursor: null,
      },
    };
  };

  const serveStoredFile = async (
    req: IncomingMessage,
    res: ServerResponse,
    relPath: string,
  ) => {
    const safeRel = safeStaticRelativePath(relPath);
    if (!safeRel) {
      sendJson(res, 400, { ok: false, error: "Invalid shard path." });
      return;
    }
    const resolved = path.join(storeDir, safeRel);
    try {
      assertInsideDirectory(storeDir, resolved);
      const file = await stat(resolved);
      if (!file.isFile()) {
        sendJson(res, 404, { ok: false, error: "Shard file not found." });
        return;
      }
      res.statusCode = 200;
      res.setHeader("content-type", staticShardContentType(resolved));
      res.setHeader("content-length", file.size);
      res.setHeader("cache-control", "public, max-age=31536000, immutable");
      if (req.method === "HEAD") {
        res.end();
        return;
      }
      createReadStream(resolved).pipe(res);
    } catch {
      sendJson(res, 404, { ok: false, error: "Shard file not found." });
    }
  };

  const servePlayableFile = async (
    req: IncomingMessage,
    res: ServerResponse,
    id: string,
    version: string,
    relPath: string,
  ) => {
    const key = staticShardReleaseKey(id, version);
    const release = releases.get(key);
    if (!release) {
      sendJson(res, 404, { ok: false, error: "Static shard release not found." });
      return;
    }

    const safeRel = safeStaticRelativePath(relPath);
    const relForExternal = safeRel || release.entryPath || undefined;
    if (allowVerifiedRedirects && releaseEffectiveServeMode(release) === "redirect") {
      const externalUrl = appendQueryToUrl(
        externalReleaseUrl(release, relForExternal),
        new URL(req.url || "/", "http://localhost").searchParams.toString(),
      );
      if (externalUrl) {
        res.statusCode = 302;
        res.setHeader("location", externalUrl);
        res.end();
        return;
      }
    }

    if (!release.unpacked || !release.entryPath) {
      sendJson(res, 409, {
        ok: false,
        error: "Static shard release has no extracted playable entry.",
      });
      return;
    }

    if (!safeRel) {
      res.statusCode = 302;
      res.setHeader("location", releaseUrlForServePath(release, release.entryServePath, req));
      res.end();
      return;
    }

    const unpackedRoot = staticShardReleaseUnpackedRoot(storeDir, release.id, release.version);
    for (const candidate of staticShardPlayablePathCandidates(release, safeRel)) {
      const resolved = path.join(unpackedRoot, candidate);
      try {
        assertInsideDirectory(unpackedRoot, resolved);
        const file = await stat(resolved);
        if (!file.isFile()) {
          continue;
        }
        res.statusCode = 200;
        res.setHeader("content-type", staticShardContentType(resolved));
        res.setHeader("content-length", file.size);
        res.setHeader("cache-control", "public, max-age=31536000, immutable");
        if (req.method === "HEAD") {
          res.end();
          return;
        }
        createReadStream(resolved).pipe(res);
        return;
      } catch {
        // Try entry-directory fallback candidates before returning 404.
      }
    }
    sendJson(res, 404, { ok: false, error: "Playable file not found." });
  };

  const servePlayableAssetFromRelease = async (
    req: IncomingMessage,
    res: ServerResponse,
    release: IngestedStaticShardRelease,
    relPath: string,
  ) => {
    if (!release.unpacked) {
      return false;
    }
    const unpackedRoot = staticShardReleaseUnpackedRoot(storeDir, release.id, release.version);
    for (const candidate of staticShardPlayablePathCandidates(release, relPath)) {
      const resolved = path.join(unpackedRoot, candidate);
      try {
        assertInsideDirectory(unpackedRoot, resolved);
        const file = await stat(resolved);
        if (!file.isFile()) {
          continue;
        }
        res.statusCode = 200;
        res.setHeader("content-type", staticShardContentType(resolved));
        res.setHeader("content-length", file.size);
        res.setHeader("cache-control", "public, max-age=31536000, immutable");
        if (req.method === "HEAD") {
          res.end();
          return true;
        }
        createReadStream(resolved).pipe(res);
        return true;
      } catch {
        // Try entry-directory fallback candidates before giving up.
      }
    }
    return false;
  };

  const releaseFromPlayableReferer = (req: IncomingMessage) => {
    const referer =
      typeof req.headers.referer === "string"
        ? req.headers.referer
        : typeof req.headers.referrer === "string"
          ? req.headers.referrer
          : "";
    if (!referer) {
      return undefined;
    }
    try {
      const refererPath = new URL(referer, "http://localhost").pathname;
      const match = /^\/plugins\/cgp\.static-shards\/play\/([^/]+)\/([^/]+)\//.exec(
        refererPath,
      );
      if (match) {
        const id = decodeUrlPathname(match[1]);
        const version = decodeUrlPathname(match[2]);
        return releases.get(staticShardReleaseKey(id, version));
      }
    } catch {
      return undefined;
    }
    return undefined;
  };

  const servePlayableAssetByContext = async (
    req: IncomingMessage,
    res: ServerResponse,
    requestedPath: string,
  ) => {
    if (req.method !== "GET" && req.method !== "HEAD") {
      return false;
    }
    const safeRequestedPath = safeStaticRelativePath(requestedPath);
    if (!safeRequestedPath) {
      return false;
    }

    const refererRelease = releaseFromPlayableReferer(req);
    if (
      refererRelease &&
      (await servePlayableAssetFromRelease(req, res, refererRelease, safeRequestedPath))
    ) {
      return true;
    }

    const matches: IngestedStaticShardRelease[] = [];
    for (const release of releases.values()) {
      if (!release.unpacked) {
        continue;
      }
      const unpackedRoot = staticShardReleaseUnpackedRoot(storeDir, release.id, release.version);
      for (const candidate of staticShardPlayablePathCandidates(release, safeRequestedPath)) {
        const resolved = path.join(unpackedRoot, candidate);
        try {
          assertInsideDirectory(unpackedRoot, resolved);
          const file = await stat(resolved);
          if (file.isFile()) {
            matches.push(release);
            if (matches.length > 1) {
              return false;
            }
            break;
          }
        } catch {
          // Ignore releases that do not contain the requested root-relative asset.
        }
      }
    }
    return matches.length === 1
      ? servePlayableAssetFromRelease(req, res, matches[0], safeRequestedPath)
      : false;
  };

  const servePlayableRootRelativeAsset = async (
    req: IncomingMessage,
    res: ServerResponse,
    requestPathname: string,
  ) => {
    const requestedPath = safeStaticRelativePath(
      decodeUrlPathname(requestPathname),
    );
    if (!requestedPath || requestedPath.startsWith("plugins/")) {
      return false;
    }
    return servePlayableAssetByContext(req, res, requestedPath);
  };

  return {
    name: "cgp.static-shards",
    metadata: {
      name: "Static shard seed mirror",
      description:
        "Relay-local ingestion for CGP static shard catalogs, git-like game source mirrors, and optional IPFS pinning.",
      version: "1",
      policy: {
        storeDir,
        sourceCount: sources.length,
        autoIngest,
        maxShardBytes,
        uploadMaxBytes,
        maxManifestBytes,
        pinToIpfs,
        ipfsApiConfigured: Boolean(ipfsApiUrl),
        ipfsBackendId,
        extractPlayable,
        servePlayableMode,
        allowVerifiedRedirects,
        maxExtractedBytes,
        maxExtractedFiles,
        publicHttpUrl,
        requireSignedUploads,
        httpIngestEnabled:
          allowUnauthenticatedHttpIngest || Boolean(httpIngestToken),
        allowUnauthenticatedHttpIngest,
        publishHollowHomeObjects,
        hollowHomeNamespace,
        namespace,
        gameReleaseObjectType,
        shardObjectType,
        createGameGuilds,
      },
    },
    onInit: async (ctx) => {
      ctxRef = ctx;
      initPromise = autoIngest
        ? ingestConfiguredSources()
        : (async () => {
            await loadPersistedReleases();
          })();
      await initPromise;
    },
    onHttp: async ({ req, res, pathname, pathSegments }) => {
      if (pathSegments[0] !== "cgp.static-shards") {
        await initPromise;
        if (await servePlayableRootRelativeAsset(req, res, pathname)) {
          return true;
        }
        return false;
      }
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS");
      res.setHeader(
        "Access-Control-Allow-Headers",
        "Content-Type, Authorization, X-CGP-Static-Shard-Token",
      );
      if (req.method === "OPTIONS") {
        res.statusCode = 204;
        res.end();
        return true;
      }
      await initPromise;
      const action = pathSegments[1] || "status";
      if (action === "status" && req.method === "GET") {
        sendJson(res, 200, {
          ok: true,
          storeDir,
          sourceCount: sources.length,
          autoIngest,
          pinToIpfs,
          uploadMaxBytes,
          ipfsApiConfigured: Boolean(ipfsApiUrl),
          ipfsBackendId,
          extractPlayable,
          servePlayableMode,
          allowVerifiedRedirects,
          maxExtractedBytes,
          maxExtractedFiles,
          publicHttpUrl: publicBaseForRequest(req),
          requireSignedUploads,
          httpIngestEnabled:
            allowUnauthenticatedHttpIngest || Boolean(httpIngestToken),
          allowUnauthenticatedHttpIngest,
          signedGames: publisherKeyByGameId.size,
          legacyUnsignedGames: legacyUnsignedGameIds.size,
          releases: Array.from(releases.values()).map((release) =>
            publicReleasePayload(release, req),
          ),
          errors,
        });
        return true;
      }
      if (action === "catalog" && req.method === "GET") {
        const requestUrl = new URL(req.url || "/", "http://localhost");
        const gameId = requestUrl.searchParams.get("id")?.trim() || "";
        const query = requestUrl.searchParams.get("q")?.trim() || "";
        if (gameId) {
          sendJson(res, 200, catalogLookupPayload(req, gameId));
          return true;
        }
        if (query) {
          sendJson(
            res,
            200,
            catalogSearchPayload(req, query, requestUrl.searchParams.get("limit")),
          );
          return true;
        }
        if (requestUrl.searchParams.has("limit") || requestUrl.searchParams.has("cursor")) {
          const page = catalogPagePayload(req);
          sendJson(res, page.ok ? 200 : 400, page);
          return true;
        }
        sendJson(res, 200, { ok: true, ...registryPayload(req) });
        return true;
      }
      if (action === "ingest" && req.method === "POST") {
        if (!requestHasIngestAuthority(req)) {
          sendJson(res, 403, {
            ok: false,
            error:
              "Static shard URL ingestion requires relay operator authorization.",
          });
          return true;
        }
        let body: Record<string, unknown>;
        try {
          body = await readJsonRequestBody(req, 64 * 1024);
        } catch (error: any) {
          sendJson(res, 400, {
            ok: false,
            error: error?.message || "Invalid ingest request.",
          });
          return true;
        }
        try {
          const ingested = await ingestSource(body);
          sendJson(res, 202, { ok: true, releases: ingested });
        } catch (error: any) {
          rememberError(stringField(body, "url"), error);
          sendJson(res, 409, {
            ok: false,
            error: error?.message || "Static shard ingest failed.",
          });
        }
        return true;
      }
      if (action === "listing" && req.method === "GET") {
        const query = new URL(req.url || "/", "http://localhost").searchParams;
        const release = releases.get(staticShardReleaseKey(query.get("id") || "", query.get("version") || ""));
        sendJson(res, release ? 200 : 404, release ? {ok:true, release:publicReleasePayload(release, req)} : {ok:false, error:"Listing not found."});
        return true;
      }
      if (action === "listing" && req.method === "POST") {
        try {
          const claim = await readJsonRequestBody(req, maxManifestBytes);
          if (claim.kind !== "cgp-game-listing-update/1") throw new Error("Invalid listing update protocol.");
          const publisher = verifyStaticShardReleasePublisher(claim, { deviceAuthorityRegistry: publisherDeviceAuthorities });
          const id = stringField(claim, "id");
          const version = stringField(claim, "version");
          const updated = await withGameIngestLock(id, async () => {
            assertPublisherContinuity(id, publisher, {requireSigned: true});
            const existing = releases.get(staticShardReleaseKey(id, version));
            if (!existing || existing.releaseSha256 !== claim.releaseSha256) throw new Error("Listing target release was not found.");
            if (existing.listingClaim && hashObject(staticShardReleaseSigningPayload(existing.listingClaim)) === hashObject(staticShardReleaseSigningPayload(claim))) {
              await registerReleaseInCgp(existing);
              return existing;
            }
            if (claim.expectedRevision !== (existing.listingRevision || 0)) throw new Error("Listing changed. Refresh the listing, compare your draft, then save again.");
            const title = stringField(claim, "title").trim();
            if (!title || title.length > 200) throw new Error("Listing title must be 1–200 characters.");
            const next: IngestedStaticShardRelease = {...existing, title,
              description: stringField(claim, "description").slice(0, 8000),
              thumbnail: stringField(claim, "thumbnail").slice(0, 2048) || undefined,
              listingRevision: (existing.listingRevision || 0) + 1, listingUpdatedAt: Date.now(), listingClaim: claim};
            await persist(next); rememberRelease(next); await registerReleaseInCgp(next);
            return next;
          });
          sendJson(res, 200, {ok: true, release: publicReleasePayload(updated, req)});
        } catch (error: any) { sendJson(res, 409, {ok: false, error: error?.message || "Listing update failed."}); }
        return true;
      }
      if ((action === "upload-status" || action === "upload-blob") && req.method === "POST") {
        try {
          const body = await readJsonRequestBody(req, action === "upload-status" ? maxManifestBytes * 2 : Math.max(maxShardBytes, maxManifestBytes) * 2 + maxManifestBytes * 2);
          sendJson(res, 200, await stageUpload(body, action === "upload-blob"));
        } catch (error: any) { sendJson(res, 400, { ok: false, error: error?.message || "Unable to stage upload." }); }
        return true;
      }
      if (action === "upload" && req.method === "POST") {
        let body: Record<string, unknown>;
        try {
          body = await readJsonRequestBody(req, uploadMaxBytes);
        } catch (error: any) {
          sendJson(res, 400, {
            ok: false,
            error: error?.message || "Invalid static shard upload request.",
          });
          return true;
        }
        try {
          const ingested = await ingestUpload(body, req);
          sendJson(res, 201, {
            ok: true,
            release: publicReleasePayload(ingested, req),
            releases: [publicReleasePayload(ingested, req)],
          });
        } catch (error: any) {
          rememberError("upload", error);
          sendJson(res, 409, {
            ok: false,
            error: error?.message || "Static shard upload failed.",
          });
        }
        return true;
      }
      if (action === "files" && (req.method === "GET" || req.method === "HEAD")) {
        await serveStoredFile(req, res, pathSegments.slice(2).join("/"));
        return true;
      }
      if (action === "play" && (req.method === "GET" || req.method === "HEAD")) {
        await servePlayableFile(
          req,
          res,
          pathSegments[2] || "",
          pathSegments[3] || "",
          pathSegments.slice(4).join("/"),
        );
        return true;
      }
      const pluginEscapedAsset = safeStaticRelativePath(
        decodeUrlPathname(pathSegments.slice(1).join("/")),
      );
      if (
        pluginEscapedAsset &&
        (await servePlayableAssetByContext(req, res, pluginEscapedAsset))
      ) {
        return true;
      }
      sendJson(res, 404, { ok: false, error: "Unknown static shard route." });
      return true;
    },
    onClose: async () => {
      await registryPersistQueue;
    },
  };
}

interface GitHubMirrorGuildRange {
  guildId: string;
  startSeq: number;
  endSeq: number;
  startHash: string;
  endHash: string;
  events: number;
}

interface GitHubMirrorChunkRecord {
  path: string;
  bytes: number;
  sha256: string;
  compression?: GitHubRelayMirrorCompression;
  uncompressedBytes?: number;
  uncompressedSha256?: string;
  exportedAt: string;
  reason: string;
  events: number;
  guilds: GitHubMirrorGuildRange[];
}

interface GitHubMirrorGuildRecord {
  guildId: string;
  events: number;
  headSeq: number;
  headHash: string | null;
  updatedAt: string;
}

interface GitHubMirrorIngestedChunkRecord {
  source: string;
  path: string;
  sha256: string;
  uncompressedSha256?: string;
  events: number;
  appended: number;
  skipped: number;
  ingestedAt: string;
}

interface GitHubMirrorManifest {
  kind: "cgp.github-relay-mirror";
  schemaVersion: 1;
  format: "cgp.github-relay-mirror.v1";
  exportedAt: string;
  updatedAt: string;
  relayPublicKey: string;
  basePath: string;
  chunks: GitHubMirrorChunkRecord[];
  guilds: GitHubMirrorGuildRecord[];
  ingestedChunks?: GitHubMirrorIngestedChunkRecord[];
}

export function createGitHubRelayMirrorPlugin(
  policy: GitHubRelayMirrorPolicy = {},
): RelayPlugin {
  const configuredRepository = policy.repository || process.env.CGP_GITHUB_MIRROR_REPOSITORY || "";
  const [repoOwner, repoName] = normalizeGitHubRepository(
    policy.owner || process.env.CGP_GITHUB_MIRROR_OWNER || "",
    policy.repo || process.env.CGP_GITHUB_MIRROR_REPO || "",
    configuredRepository,
  );
  const branch = policy.branch || process.env.CGP_GITHUB_MIRROR_BRANCH || "main";
  const token =
    policy.token ||
    process.env.CGP_GITHUB_MIRROR_TOKEN ||
    process.env.CGP_GITHUB_TOKEN ||
    "";
  const appId =
    policy.appId ||
    process.env.CGP_GITHUB_MIRROR_APP_ID ||
    process.env.CGP_GITHUB_APP_ID ||
    "";
  const appPrivateKey =
    normalizeGitHubAppPrivateKey(
      policy.appPrivateKey ||
        process.env.CGP_GITHUB_MIRROR_APP_PRIVATE_KEY ||
        process.env.CGP_GITHUB_APP_PRIVATE_KEY ||
        "",
    );
  const appPrivateKeyFile =
    policy.appPrivateKeyFile ||
    process.env.CGP_GITHUB_MIRROR_APP_PRIVATE_KEY_FILE ||
    process.env.CGP_GITHUB_APP_PRIVATE_KEY_FILE ||
    "";
  const appInstallationId =
    policy.appInstallationId ||
    process.env.CGP_GITHUB_MIRROR_APP_INSTALLATION_ID ||
    process.env.CGP_GITHUB_APP_INSTALLATION_ID ||
    "";
  const adminToken =
    policy.adminToken || process.env.CGP_GITHUB_MIRROR_ADMIN_TOKEN || "";
  const allowUnauthenticatedHttpWrites =
    policy.allowUnauthenticatedHttpWrites ??
    envFlag("CGP_GITHUB_MIRROR_OPEN_HTTP", false);
  const sources = policy.sources ?? parseGitHubMirrorSourcesFromEnv();
  const autoIngest =
    policy.autoIngest ??
    (sources.length > 0 && process.env.CGP_GITHUB_MIRROR_AUTO_INGEST !== "0");
  const autoMirror =
    policy.autoMirror ?? envFlag("CGP_GITHUB_MIRROR_AUTO", false);
  const frequency =
    normalizeGitHubMirrorFrequency(
      policy.frequency || process.env.CGP_GITHUB_MIRROR_FREQUENCY || "",
    ) || "batch";
  const chunkCompression = normalizeGitHubMirrorCompression(
    policy.compression || process.env.CGP_GITHUB_MIRROR_COMPRESSION || "gzip",
  );
  const batchSize = Math.max(
    1,
    Math.floor(
      policy.batchSize ??
        positiveIntegerFromEnv("CGP_GITHUB_MIRROR_BATCH_SIZE", 50),
    ),
  );
  const intervalMs = Math.max(
    1000,
    Math.floor(
      policy.intervalMs ??
        positiveIntegerFromEnv("CGP_GITHUB_MIRROR_INTERVAL_MS", 24 * 60 * 60_000),
    ),
  );
  const maxSourceBytes = Math.max(
    1024,
    Math.floor(
      policy.maxSourceBytes ??
        positiveIntegerFromEnv(
          "CGP_GITHUB_MIRROR_MAX_SOURCE_BYTES",
          100 * 1024 * 1024,
        ),
    ),
  );
  const requestTimeoutMs = Math.max(
    1000,
    Math.floor(
      policy.requestTimeoutMs ??
        positiveIntegerFromEnv("CGP_GITHUB_MIRROR_REQUEST_TIMEOUT_MS", 30_000),
    ),
  );
  const basePath = safeStaticRelativePath(
    policy.basePath || process.env.CGP_GITHUB_MIRROR_BASE_PATH || "cgp",
  );
  const mirrorDir = path.resolve(
    policy.mirrorDir || process.env.CGP_GITHUB_MIRROR_DIR || "./relay-github-mirror",
  );
  const mirrorRoot = path.join(mirrorDir, basePath);
  const manifestPath = path.join(mirrorRoot, "manifest.json");
  const scopedGuildIds = new Set(
    (policy.scope?.guildIds ?? listFromEnv("CGP_GITHUB_MIRROR_GUILDS"))
      .map((guildId) => guildId.trim())
      .filter(Boolean),
  );
  const pendingEvents: GuildEvent[] = [];
  const errors: Array<{ source: string; error: string; at: number }> = [];
  let ctxRef: RelayPluginContext | undefined;
  let manifest: GitHubMirrorManifest | undefined;
  let flushPromise: Promise<GitHubMirrorChunkRecord | undefined> | undefined;
  let interval: ReturnType<typeof setInterval> | undefined;
  let resolvedGitHubAppPrivateKey = appPrivateKey;
  let installationToken:
    | { token: string; expiresAt: number }
    | undefined;

  const rememberError = (source: string, error: unknown) => {
    errors.push({
      source,
      error: error instanceof Error ? error.message : String(error),
      at: Date.now(),
    });
    while (errors.length > 50) errors.shift();
  };

  const readManifest = async () => {
    if (manifest) return manifest;
    try {
      const parsed = JSON.parse(await readFile(manifestPath, "utf8"));
      if (isGitHubMirrorManifest(parsed)) {
        parsed.ingestedChunks = normalizeGitHubMirrorIngestedChunks(parsed.ingestedChunks);
        manifest = parsed;
        return manifest;
      }
    } catch {
      // Missing or invalid local mirror state starts a fresh manifest.
    }
    manifest = {
      kind: "cgp.github-relay-mirror",
      schemaVersion: 1,
      format: "cgp.github-relay-mirror.v1",
      exportedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      relayPublicKey: ctxRef?.relayPublicKey || "",
      basePath,
      chunks: [],
      guilds: [],
      ingestedChunks: [],
    };
    return manifest;
  };

  const writeManifest = async () => {
    const current = await readManifest();
    current.updatedAt = new Date().toISOString();
    current.relayPublicKey = ctxRef?.relayPublicKey || current.relayPublicKey;
    current.basePath = basePath;
    await writeAtomicJson(manifestPath, current);
    await uploadGitHubMirrorFile("manifest.json", Buffer.from(JSON.stringify(current, null, 2)));
  };

  const uploadGitHubMirrorFile = async (relativePath: string, bytes: Buffer) => {
    const uploadToken = await resolveGitHubMirrorToken();
    if (!repoOwner || !repoName || !uploadToken) {
      return;
    }
    const repoPath = joinGitHubMirrorPath(basePath, relativePath);
    await putGitHubContent({
      owner: repoOwner,
      repo: repoName,
      branch,
      token: uploadToken,
      path: repoPath,
      bytes,
      message: `CGP relay mirror ${relativePath}`,
      timeoutMs: requestTimeoutMs,
    });
  };

  const resolveGitHubMirrorToken = async () => {
    if (token) return token;
    if (!appId || !appInstallationId) return "";
    if (!resolvedGitHubAppPrivateKey && appPrivateKeyFile) {
      resolvedGitHubAppPrivateKey = normalizeGitHubAppPrivateKey(
        await readFile(path.resolve(appPrivateKeyFile), "utf8"),
      );
    }
    if (!resolvedGitHubAppPrivateKey) return "";
    const now = Date.now();
    if (installationToken && installationToken.expiresAt - now > 60_000) {
      return installationToken.token;
    }
    const next = await createGitHubInstallationAccessToken({
      appId,
      privateKey: resolvedGitHubAppPrivateKey,
      installationId: appInstallationId,
      timeoutMs: requestTimeoutMs,
    });
    installationToken = next;
    return next.token;
  };

  const flushPending = async (reason: string) => {
    if (flushPromise) return await flushPromise;
    if (pendingEvents.length === 0) return undefined;
    const batch = pendingEvents.splice(0, pendingEvents.length);
    flushPromise = writeMirrorChunk(batch, reason).finally(() => {
      flushPromise = undefined;
    });
    return await flushPromise;
  };

  const writeMirrorChunk = async (events: GuildEvent[], reason: string) => {
    if (events.length === 0) return undefined;
    const exportedAt = new Date().toISOString();
    const first = events[0];
    const last = events[events.length - 1];
    const chunkName = `${Date.now()}-${first.seq}-${String(last.id).slice(0, 12)}-${randomUUID()}.jsonl${chunkCompression === "gzip" ? ".gz" : ""}`;
    const relPath = `chunks/${chunkName}`;
    const localPath = path.join(mirrorRoot, relPath);
    assertInsideDirectory(mirrorRoot, localPath);
    const lines = [
      JSON.stringify({
        type: "mirror-header",
        format: "cgp.github-relay-mirror-chunk.v1",
        exportedAt,
        relayPublicKey: ctxRef?.relayPublicKey,
        reason,
      }),
      ...events.map((event) =>
        JSON.stringify({ type: "event", guildId: event.body.guildId, event }),
      ),
      JSON.stringify({
        type: "mirror-footer",
        format: "cgp.github-relay-mirror-chunk.v1",
        exportedAt,
        events: events.length,
      }),
    ];
    const plainBody = Buffer.from(`${lines.join("\n")}\n`, "utf8");
    const body = chunkCompression === "gzip" ? gzipSync(plainBody, { level: 9 }) : plainBody;
    const sha256 = createHash("sha256").update(body).digest("hex");
    const uncompressedSha256 = createHash("sha256").update(plainBody).digest("hex");
    await mkdir(path.dirname(localPath), { recursive: true });
    const tmpPath = `${localPath}.tmp-${randomUUID()}`;
    await writeFile(tmpPath, body);
    await rename(tmpPath, localPath);

    const guilds = gitHubMirrorGuildRanges(events);
    const chunk: GitHubMirrorChunkRecord = {
      path: relPath,
      bytes: body.byteLength,
      sha256,
      compression: chunkCompression === "gzip" ? "gzip" : undefined,
      uncompressedBytes: chunkCompression === "gzip" ? plainBody.byteLength : undefined,
      uncompressedSha256: chunkCompression === "gzip" ? uncompressedSha256 : undefined,
      exportedAt,
      reason,
      events: events.length,
      guilds,
    };
    const current = await readManifest();
    current.chunks.push(chunk);
    mergeGitHubMirrorGuilds(current, events);
    await uploadGitHubMirrorFile(relPath, body).catch((error) => {
      rememberError(`github:${relPath}`, error);
    });
    await writeManifest();
    return chunk;
  };

  const appendMirroredEvents = async (events: GuildEvent[]) => {
    const scoped = events.filter((event) => {
      const guildId = event.body.guildId;
      return guildId && (scopedGuildIds.size === 0 || scopedGuildIds.has(guildId));
    });
    if (!autoMirror || scoped.length === 0 || frequency === "manual") {
      return;
    }
    pendingEvents.push(...scoped);
    if (frequency === "per-event" || pendingEvents.length >= batchSize) {
      await flushPending(frequency);
    }
  };

  const ingestSource = async (source: GitHubRelayMirrorSource) => {
    const url = safeUrl(source.url);
    if (!url) throw new Error("GitHub mirror source must be an HTTP(S) URL.");
    const kind = source.kind || (url.pathname.endsWith(".jsonl") || url.pathname.endsWith(".jsonl.gz") ? "jsonl" : "manifest");
    const downloaded = await requestUrlBuffer(url.toString(), {
      maxBytes: maxSourceBytes,
      timeoutMs: requestTimeoutMs,
      accept: kind === "jsonl" ? "application/gzip, text/plain, application/jsonl, */*" : "application/json",
    });
    const digest = createHash("sha256").update(downloaded.buffer).digest("hex");
    if (source.expectedSha256 && digest !== source.expectedSha256) {
      throw new Error(`Source hash mismatch: ${digest} != ${source.expectedSha256}`);
    }
    if (kind === "jsonl") {
      const existing = await findIngestedGitHubMirrorChunk(url.toString(), "", digest);
      if (existing) {
        return {
          source: url.toString(),
          kind,
          events: existing.events,
          appended: 0,
          skipped: existing.events,
          guilds: [],
          chunksDownloaded: 0,
          chunksSkipped: 1,
        };
      }
      const result = await ingestGitHubMirrorJsonl(
        decodeGitHubMirrorJsonlBuffer(downloaded.buffer, url.toString()),
        url.toString(),
      );
      await rememberIngestedGitHubMirrorChunk(url.toString(), {
        path: "",
        sha256: digest,
        events: result.events,
        appended: result.appended,
        skipped: result.skipped,
      });
      return result;
    }
    const parsed = JSON.parse(downloaded.buffer.toString("utf8"));
    if (!isGitHubMirrorManifest(parsed)) {
      throw new Error("Source is not a CGP GitHub relay mirror manifest.");
    }
    const totals = {
      sources: 1,
      events: 0,
      appended: 0,
      skipped: 0,
      chunksDownloaded: 0,
      chunksSkipped: 0,
      guilds: new Set<string>(),
    };
    for (const chunk of parsed.chunks) {
      const chunkUrl = resolveStaticShardUrl(url.toString(), chunk.path);
      const ingested = await findIngestedGitHubMirrorChunk(url.toString(), chunk.path, chunk.sha256);
      if (ingested) {
        totals.events += chunk.events || ingested.events || 0;
        totals.skipped += chunk.events || ingested.events || 0;
        totals.chunksSkipped += 1;
        for (const range of chunk.guilds || []) {
          if (range.guildId) totals.guilds.add(range.guildId);
        }
        continue;
      }
      const chunkDownload = await requestUrlBuffer(chunkUrl, {
        maxBytes: maxSourceBytes,
        timeoutMs: requestTimeoutMs,
        accept: "application/gzip, text/plain, application/jsonl, */*",
      });
      totals.chunksDownloaded += 1;
      const chunkDigest = createHash("sha256").update(chunkDownload.buffer).digest("hex");
      if (chunkDigest !== chunk.sha256) {
        throw new Error(`Chunk hash mismatch for ${chunk.path}: ${chunkDigest} != ${chunk.sha256}`);
      }
      const result = await ingestGitHubMirrorJsonl(
        decodeGitHubMirrorJsonlBuffer(chunkDownload.buffer, chunkUrl, chunk),
        chunkUrl,
      );
      await rememberIngestedGitHubMirrorChunk(url.toString(), {
        path: chunk.path,
        sha256: chunk.sha256,
        uncompressedSha256: chunk.uncompressedSha256,
        events: result.events,
        appended: result.appended,
        skipped: result.skipped,
      });
      totals.events += result.events;
      totals.appended += result.appended;
      totals.skipped += result.skipped;
      for (const guildId of result.guilds) totals.guilds.add(guildId);
    }
    return {
      source: url.toString(),
      kind,
      events: totals.events,
      appended: totals.appended,
      skipped: totals.skipped,
      chunksDownloaded: totals.chunksDownloaded,
      chunksSkipped: totals.chunksSkipped,
      guilds: Array.from(totals.guilds),
    };
  };

  const findIngestedGitHubMirrorChunk = async (
    source: string,
    chunkPath: string,
    sha256: string,
  ) => {
    const current = await readManifest();
    return (current.ingestedChunks || []).find((entry) =>
      entry.source === source &&
      entry.path === chunkPath &&
      entry.sha256 === sha256,
    );
  };

  const rememberIngestedGitHubMirrorChunk = async (
    source: string,
    record: Omit<GitHubMirrorIngestedChunkRecord, "source" | "ingestedAt">,
  ) => {
    const current = await readManifest();
    const ingestedChunks = normalizeGitHubMirrorIngestedChunks(current.ingestedChunks)
      .filter((entry) =>
        !(entry.source === source &&
          entry.path === record.path &&
          entry.sha256 === record.sha256),
      );
    ingestedChunks.push({
      source,
      ...record,
      ingestedAt: new Date().toISOString(),
    });
    current.ingestedChunks = ingestedChunks.slice(-5000);
    await writeManifest();
  };

  const ingestGitHubMirrorJsonl = async (buffer: Buffer, source: string) => {
    const ctx = ctxRef;
    if (!ctx) throw new Error("Relay plugin is not initialized.");
    const previousByGuild = new Map<string, GuildEvent | undefined>();
    const existingByGuild = new Map<string, GuildEvent[]>();
    const pendingByGuild = new Map<string, GuildEvent[]>();
    let events = 0;
    let appended = 0;
    let skipped = 0;

    const existingLog = async (guildId: string) => {
      let log = existingByGuild.get(guildId);
      if (!log) {
        log = await ctx.getLog(guildId);
        existingByGuild.set(guildId, log);
      }
      return log;
    };

    for (const line of buffer.toString("utf8").split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      const record = JSON.parse(trimmed);
      if (!isRecord(record) || record.type !== "event") {
        continue;
      }
      const event = record.event as GuildEvent;
      const guildId =
        typeof record.guildId === "string"
          ? record.guildId
          : typeof event?.body?.guildId === "string"
            ? event.body.guildId
            : "";
      if (!guildId || !event) {
        throw new Error(`Malformed mirror event in ${source}`);
      }
      const existing = await existingLog(guildId);
      const previous =
        previousByGuild.get(guildId) ||
        (event.seq > 0 ? existing[event.seq - 1] : undefined);
      verifyGitHubMirrorEvent(event, previous, guildId);
      previousByGuild.set(guildId, event);
      events += 1;

      if (event.seq < existing.length) {
        if (existing[event.seq]?.id !== event.id) {
          throw new Error(`Target relay has divergent guild ${guildId} at seq ${event.seq}`);
        }
        skipped += 1;
        continue;
      }
      const queue = pendingByGuild.get(guildId) || [];
      if (event.seq !== existing.length + queue.length) {
        throw new Error(`Mirror source has a gap for guild ${guildId} at seq ${event.seq}`);
      }
      queue.push(event);
      pendingByGuild.set(guildId, queue);
    }

    for (const [guildId, queue] of pendingByGuild) {
      if (queue.length === 0) continue;
      if (ctx.store.appendEvents) {
        await ctx.store.appendEvents(guildId, queue);
      } else {
        for (const event of queue) await ctx.store.append(guildId, event);
      }
      appended += queue.length;
      for (const event of queue) ctx.broadcast(guildId, event);
    }

    return {
      source,
      kind: "jsonl" as const,
      events,
      appended,
      skipped,
      guilds: Array.from(new Set([...existingByGuild.keys(), ...pendingByGuild.keys()])),
    };
  };

  const requireWriteAccess = (req: IncomingMessage) => {
    if (allowUnauthenticatedHttpWrites) return true;
    if (!adminToken) return false;
    const auth = String(req.headers.authorization || "");
    const bearer = /^Bearer\s+(.+)$/i.exec(auth.trim())?.[1]?.trim();
    const header = String(req.headers["x-cgp-github-mirror-token"] || "").trim();
    return bearer === adminToken || header === adminToken;
  };

  return {
    name: "cgp.github.mirror",
    metadata: {
      name: "GitHub relay mirror",
      description:
        "Relay-local GitHub-compatible CGP log mirror and restore source. Media remains hash/IPFS metadata instead of git blobs.",
      version: "1",
      policy: {
        mirrorDir,
        basePath,
        repository: repoOwner && repoName ? `${repoOwner}/${repoName}` : "",
        branch,
        sourceCount: sources.length,
        autoIngest,
        autoMirror,
        frequency,
        batchSize,
        intervalMs,
        scopedGuilds: Array.from(scopedGuildIds),
        githubUploadConfigured: Boolean(
          repoOwner &&
            repoName &&
            (token || (appId && appInstallationId && (resolvedGitHubAppPrivateKey || appPrivateKeyFile))),
        ),
        githubAuthMode: token
          ? "token"
          : appId && appInstallationId && (resolvedGitHubAppPrivateKey || appPrivateKeyFile)
            ? "app-installation"
            : "none",
        httpWritesRequireToken: !allowUnauthenticatedHttpWrites,
      },
      hollowIntegration: {
        schemaVersion: 1,
        id: "github-backup",
        kind: "account-backup",
        title: "GitHub backup",
        connectLabel: "Connect GitHub",
        removeLabel: "Remove GitHub",
        setup: {
          url: process.env.CGP_GITHUB_BACKUP_SETUP_URL || "http://127.0.0.1:17876",
          localCommand: "npm run github:backup-app -- -- --repo hollow-backup --create-repo",
          returnParams: {
            repository: ["github_repo", "github_repository", "repo"],
            appInstallationId: ["github_installation_id", "app_installation_id", "installation_id"],
            appId: ["github_app_id", "app_id"],
            appPrivateKeyFile: ["github_app_private_key_file", "app_private_key_file"],
          },
        },
      },
    },
    inputs: [
      {
        name: "repository",
        type: "string",
        required: false,
        description: "GitHub owner/repo for pushed mirror chunks.",
        placeholder: "owner/repo",
        scope: "relay",
      },
      {
        name: "branch",
        type: "string",
        required: false,
        description: "Git branch used for mirror commits.",
        placeholder: "main",
        scope: "relay",
      },
      {
        name: "basePath",
        type: "string",
        required: false,
        description: "Repo folder for CGP mirror manifests and chunks.",
        placeholder: "cgp/backups/main-relay",
        scope: "relay",
      },
      {
        name: "frequency",
        type: "string",
        required: false,
        description: "manual, per-event, batch, or interval.",
        placeholder: "batch",
        scope: "relay",
      },
      {
        name: "batchSize",
        type: "number",
        required: false,
        description: "Events per mirror chunk when frequency is batch.",
        placeholder: "50",
        scope: "relay",
      },
      {
        name: "token",
        type: "string",
        required: false,
        sensitive: true,
        description: "Fallback GitHub token with repository contents write access.",
        scope: "relay",
      },
      {
        name: "appInstallationId",
        type: "string",
        required: false,
        sensitive: true,
        description: "GitHub App installation id for scoped backup repository writes.",
        scope: "relay",
      },
      {
        name: "appId",
        type: "string",
        required: false,
        description: "GitHub App id used to mint short-lived installation tokens.",
        scope: "relay",
      },
      {
        name: "appPrivateKey",
        type: "string",
        required: false,
        sensitive: true,
        description: "GitHub App PEM private key, or use appPrivateKeyFile.",
        scope: "relay",
      },
      {
        name: "sources",
        type: "object",
        required: false,
        description: "Mirror manifests or JSONL backups to ingest on startup.",
        scope: "relay",
      },
    ],
    onInit: async (ctx) => {
      ctxRef = ctx;
      await readManifest();
      if (autoIngest) {
        for (const source of sources) {
          try {
            await ingestSource(source);
          } catch (error) {
            rememberError(source.url, error);
          }
        }
      }
      if (autoMirror && frequency === "interval") {
        interval = setInterval(() => {
          void flushPending("interval").catch((error) =>
            rememberError("interval", error),
          );
        }, intervalMs);
      }
    },
    onEventsAppended: async ({ events }) => {
      await appendMirroredEvents(events);
    },
    onHttp: async ({ req, res, pathSegments }) => {
      if (pathSegments[0] !== "cgp.github.mirror") {
        return false;
      }
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS");
      res.setHeader(
        "Access-Control-Allow-Headers",
        "Content-Type, Authorization, x-cgp-github-mirror-token",
      );
      if (req.method === "OPTIONS") {
        res.statusCode = 204;
        res.end();
        return true;
      }

      const action = pathSegments[1] || "status";
      if (action === "status" && req.method === "GET") {
        const current = await readManifest();
        sendJson(res, 200, {
          ok: true,
          mirrorDir,
          basePath,
          repository: repoOwner && repoName ? `${repoOwner}/${repoName}` : "",
          branch,
          autoIngest,
          autoMirror,
          frequency,
          batchSize,
          intervalMs,
          pendingEvents: pendingEvents.length,
          chunks: current.chunks.length,
          ingestedChunks: current.ingestedChunks?.length || 0,
          guilds: current.guilds,
          latestChunk: current.chunks.at(-1),
          errors,
        });
        return true;
      }

      if (action === "manifest" && req.method === "GET") {
        sendJson(res, 200, await readManifest() as unknown as Record<string, unknown>);
        return true;
      }

      if (action === "files" && (req.method === "GET" || req.method === "HEAD")) {
        await serveGitHubMirrorFile(req, res, mirrorRoot, pathSegments.slice(2).join("/"));
        return true;
      }

      if (!requireWriteAccess(req)) {
        sendJson(res, 403, {
          ok: false,
          error:
            "GitHub mirror write route is locked. Configure CGP_GITHUB_MIRROR_ADMIN_TOKEN or allowUnauthenticatedHttpWrites for local tests.",
        });
        return true;
      }

      if (action === "flush" && req.method === "POST") {
        try {
          const chunk = await flushPending("manual");
          sendJson(res, 202, { ok: true, chunk, pendingEvents: pendingEvents.length });
        } catch (error: any) {
          rememberError("flush", error);
          sendJson(res, 409, { ok: false, error: error?.message || "Flush failed." });
        }
        return true;
      }

      if (action === "ingest" && req.method === "POST") {
        try {
          const body = await readJsonRequestBody(req, 64 * 1024);
          const result = await ingestSource({
            url: stringField(body, "url"),
            kind: normalizeGitHubMirrorSourceKind(stringField(body, "kind")),
            expectedSha256: stringField(body, "expectedSha256") || undefined,
          });
          sendJson(res, 202, { ok: true, result });
        } catch (error: any) {
          rememberError("ingest", error);
          sendJson(res, 409, { ok: false, error: error?.message || "Ingest failed." });
        }
        return true;
      }

      sendJson(res, 404, { ok: false, error: "Unknown GitHub mirror route." });
      return true;
    },
    onClose: async () => {
      if (interval) {
        clearInterval(interval);
        interval = undefined;
      }
      if (pendingEvents.length > 0) {
        await flushPending("close").catch((error) => rememberError("close", error));
      }
    },
  };
}

function normalizeGitHubRepository(
  owner: string,
  repo: string,
  repository: string,
): [string, string] {
  if (owner && repo) return [owner.trim(), repo.trim()];
  const match = /^([^/\s]+)\/([^/\s]+)$/.exec(repository.trim());
  return match ? [match[1], match[2]] : ["", ""];
}

function normalizeGitHubMirrorFrequency(value: string): GitHubRelayMirrorFrequency | undefined {
  const normalized = value.trim().toLowerCase();
  return normalized === "manual" ||
    normalized === "per-event" ||
    normalized === "batch" ||
    normalized === "interval"
    ? normalized
    : undefined;
}

function normalizeGitHubMirrorSourceKind(value: string): GitHubRelayMirrorSourceKind | undefined {
  const normalized = value.trim().toLowerCase();
  return normalized === "manifest" || normalized === "jsonl" ? normalized : undefined;
}

function normalizeGitHubMirrorCompression(value: string): GitHubRelayMirrorCompression {
  const normalized = value.trim().toLowerCase();
  return normalized === "none" || normalized === "0" || normalized === "false"
    ? "none"
    : "gzip";
}

function parseGitHubMirrorSourcesFromEnv(): GitHubRelayMirrorSource[] {
  const raw = process.env.CGP_GITHUB_MIRROR_SOURCES_JSON;
  if (raw) {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        const sources: GitHubRelayMirrorSource[] = [];
        for (const entry of parsed) {
          const source = isRecord(entry) ? {
            url: stringField(entry, "url"),
            kind: normalizeGitHubMirrorSourceKind(stringField(entry, "kind")),
            expectedSha256: stringField(entry, "expectedSha256") || undefined,
          } : undefined;
          if (source?.url) sources.push(source);
        }
        return sources;
      }
    } catch {
      return [];
    }
  }
  return listFromEnv("CGP_GITHUB_MIRROR_SOURCES").map((url) => ({ url }));
}

function isGitHubMirrorManifest(value: unknown): value is GitHubMirrorManifest {
  if (!isRecord(value)) return false;
  if (value.kind !== "cgp.github-relay-mirror") return false;
  if (value.format !== "cgp.github-relay-mirror.v1") return false;
  return Array.isArray(value.chunks) && Array.isArray(value.guilds);
}

function normalizeGitHubMirrorIngestedChunks(value: unknown): GitHubMirrorIngestedChunkRecord[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((entry) => {
    if (!isRecord(entry)) return [];
    const source = stringField(entry, "source");
    const chunkPath = stringField(entry, "path");
    const sha256 = stringField(entry, "sha256").toLowerCase();
    const uncompressedSha256 = stringField(entry, "uncompressedSha256").toLowerCase();
    if (!source || !/^[0-9a-f]{64}$/.test(sha256)) return [];
    return [{
      source,
      path: chunkPath,
      sha256,
      uncompressedSha256: /^[0-9a-f]{64}$/.test(uncompressedSha256)
        ? uncompressedSha256
        : undefined,
      events: Math.max(0, Math.floor(numberField(entry, "events") || 0)),
      appended: Math.max(0, Math.floor(numberField(entry, "appended") || 0)),
      skipped: Math.max(0, Math.floor(numberField(entry, "skipped") || 0)),
      ingestedAt: stringField(entry, "ingestedAt") || new Date(0).toISOString(),
    }];
  });
}

function decodeGitHubMirrorJsonlBuffer(
  buffer: Buffer,
  source: string,
  chunk?: Partial<GitHubMirrorChunkRecord>,
) {
  const compression = chunk?.compression || (source.endsWith(".gz") ? "gzip" : "none");
  const plain = compression === "gzip" ? gunzipSync(buffer) : buffer;
  if (chunk?.uncompressedSha256) {
    const digest = createHash("sha256").update(plain).digest("hex");
    if (digest !== chunk.uncompressedSha256) {
      throw new Error(`Uncompressed chunk hash mismatch for ${source}: ${digest} != ${chunk.uncompressedSha256}`);
    }
  }
  if (chunk?.uncompressedBytes !== undefined && plain.byteLength !== chunk.uncompressedBytes) {
    throw new Error(`Uncompressed chunk size mismatch for ${source}: ${plain.byteLength} != ${chunk.uncompressedBytes}`);
  }
  return plain;
}

async function writeAtomicJson(filePath: string, value: unknown) {
  await mkdir(path.dirname(filePath), { recursive: true });
  const tmpPath = `${filePath}.tmp-${randomUUID()}`;
  await writeFile(tmpPath, JSON.stringify(value, null, 2));
  await rename(tmpPath, filePath);
}

function joinGitHubMirrorPath(basePath: string, relativePath: string) {
  return [safeStaticRelativePath(basePath), safeStaticRelativePath(relativePath)]
    .filter(Boolean)
    .join("/");
}

function gitHubMirrorGuildRanges(events: GuildEvent[]): GitHubMirrorGuildRange[] {
  const byGuild = new Map<string, GuildEvent[]>();
  for (const event of events) {
    const guildId = event.body.guildId;
    const list = byGuild.get(guildId) || [];
    list.push(event);
    byGuild.set(guildId, list);
  }
  return Array.from(byGuild.entries()).map(([guildId, list]) => {
    const first = list[0];
    const last = list[list.length - 1];
    return {
      guildId,
      startSeq: first.seq,
      endSeq: last.seq,
      startHash: first.id,
      endHash: last.id,
      events: list.length,
    };
  });
}

function mergeGitHubMirrorGuilds(manifest: GitHubMirrorManifest, events: GuildEvent[]) {
  const records = new Map(manifest.guilds.map((record) => [record.guildId, record]));
  for (const event of events) {
    const guildId = event.body.guildId;
    const current = records.get(guildId) || {
      guildId,
      events: 0,
      headSeq: -1,
      headHash: null,
      updatedAt: new Date(0).toISOString(),
    };
    records.set(guildId, {
      ...current,
      events: current.events + 1,
      headSeq: Math.max(current.headSeq, event.seq),
      headHash: event.seq >= current.headSeq ? event.id : current.headHash,
      updatedAt: new Date().toISOString(),
    });
  }
  manifest.guilds = Array.from(records.values()).sort((left, right) =>
    left.guildId.localeCompare(right.guildId),
  );
}

function verifyGitHubMirrorEvent(
  event: GuildEvent,
  previous: GuildEvent | undefined,
  guildId: string,
) {
  if (event.body.guildId !== guildId) {
    throw new Error(`Mirror event guild mismatch: ${event.body.guildId} != ${guildId}`);
  }
  if (previous) {
    if (event.seq !== previous.seq + 1) {
      throw new Error(`Mirror event seq ${event.seq} does not follow ${previous.seq}`);
    }
    if (event.prevHash !== previous.id) {
      throw new Error(`Mirror event prevHash mismatch at seq ${event.seq}`);
    }
  } else {
    if (event.seq !== 0 || event.prevHash !== null) {
      throw new Error(`Mirror source for guild ${guildId} does not start at genesis.`);
    }
  }
  if (computeEventId(event) !== event.id) {
    throw new Error(`Mirror event id mismatch at seq ${event.seq}`);
  }
  if (
    !verify(
      event.author,
      hashObject({ body: event.body, author: event.author, createdAt: event.createdAt }),
      event.signature,
    )
  ) {
    throw new Error(`Mirror event signature mismatch at seq ${event.seq}`);
  }
}

function normalizeGitHubAppPrivateKey(value: string) {
  const trimmed = value.trim();
  if (!trimmed) return "";
  const withNewlines = trimmed.replace(/\\n/g, "\n");
  if (withNewlines.includes("BEGIN") && withNewlines.includes("PRIVATE KEY")) {
    return withNewlines;
  }
  try {
    const decoded = Buffer.from(withNewlines, "base64").toString("utf8").trim();
    if (decoded.includes("BEGIN") && decoded.includes("PRIVATE KEY")) {
      return decoded;
    }
  } catch {
    // Leave malformed values to GitHub JWT signing for a precise error.
  }
  return withNewlines;
}

function base64UrlJson(value: unknown) {
  return Buffer.from(JSON.stringify(value)).toString("base64url");
}

function createGitHubAppJwt(appId: string, privateKey: string) {
  const nowSeconds = Math.floor(Date.now() / 1000);
  const header = base64UrlJson({ alg: "RS256", typ: "JWT" });
  const payload = base64UrlJson({
    iat: nowSeconds - 60,
    exp: nowSeconds + 9 * 60,
    iss: appId,
  });
  const signingInput = `${header}.${payload}`;
  const signer = createSign("RSA-SHA256");
  signer.update(signingInput);
  signer.end();
  const signature = signer.sign(privateKey).toString("base64url");
  return `${signingInput}.${signature}`;
}

async function createGitHubInstallationAccessToken(options: {
  appId: string;
  privateKey: string;
  installationId: string;
  timeoutMs: number;
}) {
  const apiUrl = `https://api.github.com/app/installations/${encodeURIComponent(options.installationId)}/access_tokens`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), options.timeoutMs);
  try {
    const response = await fetch(apiUrl, {
      method: "POST",
      headers: {
        accept: "application/vnd.github+json",
        authorization: `Bearer ${createGitHubAppJwt(options.appId, options.privateKey)}`,
        "content-type": "application/json",
        "user-agent": "cgp-github-relay-mirror",
        "x-github-api-version": "2022-11-28",
      },
      signal: controller.signal,
      body: JSON.stringify({ permissions: { contents: "write" } }),
    });
    const body = await response.json().catch(() => ({})) as Record<string, unknown>;
    if (!response.ok) {
      throw new Error(`GitHub installation token API returned HTTP ${response.status}: ${JSON.stringify(body)}`);
    }
    const token = typeof body.token === "string" ? body.token : "";
    if (!token) {
      throw new Error("GitHub installation token response had no token.");
    }
    const expiresAtRaw = typeof body.expires_at === "string" ? Date.parse(body.expires_at) : 0;
    return {
      token,
      expiresAt: Number.isFinite(expiresAtRaw) && expiresAtRaw > 0
        ? expiresAtRaw
        : Date.now() + 50 * 60_000,
    };
  } finally {
    clearTimeout(timer);
  }
}

async function putGitHubContent(options: {
  owner: string;
  repo: string;
  branch: string;
  token: string;
  path: string;
  bytes: Buffer;
  message: string;
  timeoutMs: number;
}) {
  const encodedPath = options.path.split("/").map(encodeURIComponent).join("/");
  const apiUrl = `https://api.github.com/repos/${encodeURIComponent(options.owner)}/${encodeURIComponent(options.repo)}/contents/${encodedPath}`;
  const headers = {
    accept: "application/vnd.github+json",
    authorization: `Bearer ${options.token}`,
    "content-type": "application/json",
    "user-agent": "cgp-github-relay-mirror",
    "x-github-api-version": "2022-11-28",
  };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), options.timeoutMs);
  try {
    let sha: string | undefined;
    const existing = await fetch(`${apiUrl}?ref=${encodeURIComponent(options.branch)}`, {
      headers,
      signal: controller.signal,
    });
    if (existing.ok) {
      const body = await existing.json() as Record<string, unknown>;
      sha = typeof body.sha === "string" ? body.sha : undefined;
    }
    const response = await fetch(apiUrl, {
      method: "PUT",
      headers,
      signal: controller.signal,
      body: JSON.stringify({
        message: options.message,
        branch: options.branch,
        content: options.bytes.toString("base64"),
        sha,
      }),
    });
    if (!response.ok) {
      throw new Error(`GitHub contents API returned HTTP ${response.status}: ${await response.text()}`);
    }
  } finally {
    clearTimeout(timer);
  }
}

async function serveGitHubMirrorFile(
  req: IncomingMessage,
  res: ServerResponse,
  root: string,
  relPath: string,
) {
  const safeRel = safeStaticRelativePath(relPath);
  if (!safeRel) {
    sendJson(res, 400, { ok: false, error: "Invalid mirror file path." });
    return;
  }
  const resolved = path.join(root, safeRel);
  try {
    assertInsideDirectory(root, resolved);
    const file = await stat(resolved);
    if (!file.isFile()) {
      sendJson(res, 404, { ok: false, error: "Mirror file not found." });
      return;
    }
    res.statusCode = 200;
    res.setHeader("content-type", staticShardContentType(resolved));
    res.setHeader("content-length", file.size);
    if (req.method === "HEAD") {
      res.end();
      return;
    }
    createReadStream(resolved).pipe(res);
  } catch {
    sendJson(res, 404, { ok: false, error: "Mirror file not found." });
  }
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
      | "requireEncryptedMessages"
      | "requireEncryptedPrivateGuildMessages"
      | "allowEncryptedMessages"
    >
  > &
    Pick<EncryptionPolicy, "guildIds" | "channelIds"> = {
    requireEncryptedMessages: policy.requireEncryptedMessages ?? false,
    requireEncryptedPrivateGuildMessages:
      policy.requireEncryptedPrivateGuildMessages ?? false,
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
    onFrame: async ({ socket, kind, payload }, ctx) => {
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
          external?: unknown;
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

        const privateGuildRequiresEncryption =
          resolved.requireEncryptedPrivateGuildMessages &&
          typeof guildId === "string" &&
          (await relayGuildState(ctx, guildId))?.access === "private";
        const requireEncryptedMessages =
          resolved.requireEncryptedMessages ||
          privateGuildRequiresEncryption;
        if (requireEncryptedMessages) {
          const external = isRecord(body.external) ? body.external : undefined;
          const encryption =
            external && isRecord(external.encryption)
              ? external.encryption
              : undefined;
          const hasDeclaredScheme =
            typeof encryption?.scheme === "string" &&
            encryption.scheme.trim().length > 0;
          const hasNonce =
            typeof body.iv === "string" && body.iv.length > 0;
          const hasEnvelope =
            isEncrypted &&
            typeof body.content === "string" &&
            body.content.length > 0 &&
            (hasNonce || hasDeclaredScheme);
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
