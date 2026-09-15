import { LocalRelayPubSubAdapter, RelayServer } from "./server";
import type {
  RelayPubSubAdapter,
  RelayPubSubEnvelope,
  RelayPubSubSubscribeOptions,
  RelayServerOptions,
  RelayStoragePolicy,
  RelayWireFormat,
} from "./server";
import {
  RedundantWebSocketRelayPubSubAdapter,
  ShardedWebSocketRelayPubSubAdapter,
  WebSocketPubSubHub,
  WebSocketRelayPubSubAdapter,
} from "./pubsub_ws";
import {
  CgpWebTransportRelayServer,
  CgpWebTransportSocket,
  webTransportOptionsFromEnv,
} from "./webtransport_realtime";
import type {
  CgpWebTransportAdvertisement,
  CgpWebTransportOptions,
} from "./webtransport_realtime";
import {
  RelayWriteQuorumCoordinator,
  normalizeRelayWriteQuorumConfig,
  relayWriteProposalId,
  relayWriteQuorumConfigFromEnv,
} from "./write_quorum";
import {
  RelaySequencerConsensusCoordinator,
  normalizeRelaySequencerConsensusConfig,
  relaySequencerConsensusConfigFromEnv,
  relaySequencingRequestId,
} from "./sequencer_consensus";
import type {
  RelaySequencerConsensusConfig,
  RelaySequencerHead,
  RelaySequencerMessage,
  RelaySequencerPersistentState,
  RelaySequencingRequest,
  RelaySequencingToken,
} from "./sequencer_consensus";
import type {
  RelayWriteProposal,
  RelayWriteQuorumConfig,
  RelayWriteQuorumVote,
  RelayWriteQuorumVoteUnsigned,
} from "./write_quorum";

import { Store, MemoryStore } from "./store";
import { LevelStore } from "./store_level";
import {
  createAbuseControlPolicyPlugin,
  createAppSurfacePolicyPlugin,
  createAppObjectPermissionPlugin,
  createEncryptionPolicyPlugin,
  createExpressionSearchProviderPlugin,
  createFauxIpfsBackendPlugin,
  createGitHubRelayMirrorPlugin,
  createHollowRoomRelayPlugin,
  createHeliaIpfsPlugin,
  createMediaStoragePolicyPlugin,
  createProofOfWorkPolicyPlugin,
  createRateLimitPolicyPlugin,
  createRelayPushPlugin,
  createSafetyReportPlugin,
  createSandboxedCommandPlugin,
  createStaticShardSeedPlugin,
  staticShardReleaseSigningPayload,
  verifyStaticShardReleasePublisher,
  STATIC_SHARD_PUBLISHER_PROTOCOL,
  createWebhookIngressPlugin,
} from "./plugins";
import type {
  AbuseControlPolicy,
  AppObjectPermissionPolicy,
  AppObjectPermissionRule,
  AppSurfacePolicy,
  CgpIpfsAddFileInput,
  CgpIpfsAddFileResult,
  CgpIpfsBackend,
  CgpIpfsBackendKind,
  CgpIpfsBackendStatus,
  EncryptionPolicy,
  ExpressionSearchProviderPolicy,
  FauxIpfsBackendPolicy,
  FauxIpfsStorageKind,
  GitHubRelayMirrorFrequency,
  GitHubRelayMirrorPolicy,
  GitHubRelayMirrorScope,
  GitHubRelayMirrorSource,
  GitHubRelayMirrorSourceKind,
  HeliaIpfsPolicy,
  HollowRoomRelayPolicy,
  MediaStoragePolicy,
  MediaStorageProvider,
  MediaStorageProviderKind,
  MediaAdultPolicy,
  MediaRouteRequest,
  ProofOfWorkPolicy,
  RelayPlugin,
  RelayPluginContext,
  RelayPluginHttpArgs,
  RelayPushPolicy,
  RateLimitPolicy,
  SafetyReportPolicy,
  SandboxedCommandPluginOptions,
  SandboxedPluginHook,
  StaticShardSeedKind,
  StaticShardSeedPolicy,
  StaticShardSeedSource,
  StaticShardPublisherProof,
  WebhookIngressPolicy,
} from "./plugins";

export {
  RelayServer,
  LocalRelayPubSubAdapter,
  RedundantWebSocketRelayPubSubAdapter,
  ShardedWebSocketRelayPubSubAdapter,
  WebSocketPubSubHub,
  WebSocketRelayPubSubAdapter,
  CgpWebTransportRelayServer,
  CgpWebTransportSocket,
  webTransportOptionsFromEnv,
  RelayWriteQuorumCoordinator,
  normalizeRelayWriteQuorumConfig,
  relayWriteProposalId,
  relayWriteQuorumConfigFromEnv,
  RelaySequencerConsensusCoordinator,
  normalizeRelaySequencerConsensusConfig,
  relaySequencerConsensusConfigFromEnv,
  relaySequencingRequestId,
  Store,
  MemoryStore,
  LevelStore,
  createAbuseControlPolicyPlugin,
  createAppSurfacePolicyPlugin,
  createAppObjectPermissionPlugin,
  createEncryptionPolicyPlugin,
  createExpressionSearchProviderPlugin,
  createFauxIpfsBackendPlugin,
  createGitHubRelayMirrorPlugin,
  createHollowRoomRelayPlugin,
  createHeliaIpfsPlugin,
  createMediaStoragePolicyPlugin,
  createProofOfWorkPolicyPlugin,
  createRateLimitPolicyPlugin,
  createRelayPushPlugin,
  createSafetyReportPlugin,
  createSandboxedCommandPlugin,
  createStaticShardSeedPlugin,
  staticShardReleaseSigningPayload,
  verifyStaticShardReleasePublisher,
  STATIC_SHARD_PUBLISHER_PROTOCOL,
  createWebhookIngressPlugin,
};
export type {
  AbuseControlPolicy,
  AppObjectPermissionPolicy,
  AppObjectPermissionRule,
  AppSurfacePolicy,
  CgpIpfsAddFileInput,
  CgpIpfsAddFileResult,
  CgpIpfsBackend,
  CgpIpfsBackendKind,
  CgpIpfsBackendStatus,
  EncryptionPolicy,
  ExpressionSearchProviderPolicy,
  FauxIpfsBackendPolicy,
  FauxIpfsStorageKind,
  GitHubRelayMirrorFrequency,
  GitHubRelayMirrorPolicy,
  GitHubRelayMirrorScope,
  GitHubRelayMirrorSource,
  GitHubRelayMirrorSourceKind,
  HeliaIpfsPolicy,
  HollowRoomRelayPolicy,
  MediaStoragePolicy,
  MediaStorageProvider,
  MediaStorageProviderKind,
  MediaAdultPolicy,
  MediaRouteRequest,
  ProofOfWorkPolicy,
  RelayPlugin,
  RelayPluginContext,
  RelayPluginHttpArgs,
  RelayPushPolicy,
  RateLimitPolicy,
  RelayPubSubAdapter,
  RelayPubSubEnvelope,
  RelayPubSubSubscribeOptions,
  RelayWireFormat,
  RelayStoragePolicy,
  SafetyReportPolicy,
  SandboxedCommandPluginOptions,
  SandboxedPluginHook,
  StaticShardSeedKind,
  StaticShardSeedPolicy,
  StaticShardSeedSource,
  StaticShardPublisherProof,
  WebhookIngressPolicy,
  RelayServerOptions,
  CgpWebTransportAdvertisement,
  CgpWebTransportOptions,
  RelayWriteProposal,
  RelayWriteQuorumConfig,
  RelayWriteQuorumVote,
  RelayWriteQuorumVoteUnsigned,
  RelaySequencerConsensusConfig,
  RelaySequencerHead,
  RelaySequencerMessage,
  RelaySequencerPersistentState,
  RelaySequencingRequest,
  RelaySequencingToken,
};

if (require.main === module) {
  void (async () => {
    const args = process.argv.slice(2);
    const shouldClean = args.includes("--clean") || args.includes("-c");

    const PORT = parseInt(
      process.env.CGP_RELAY_PORT || process.env.PORT || "7447",
      10,
    );
    const DB_PATH = process.env.CGP_RELAY_DB || "./relay-db";
    const PLUGINS_SPEC = process.env.CGP_RELAY_PLUGINS;
    const PUBSUB_URL = process.env.CGP_RELAY_PUBSUB_URL;
    const PUBSUB_URLS = process.env.CGP_RELAY_PUBSUB_URLS;
    // Clean database if --clean flag is passed
    if (shouldClean) {
      const fs = await import("fs");
      const path = await import("path");
      const dbPath = path.resolve(DB_PATH);
      if (fs.existsSync(dbPath)) {
        console.log(`Cleaning database at ${dbPath}...`);
        fs.rmSync(dbPath, { recursive: true, force: true });
        console.log("Database cleaned.");
      }
    }
    const PLUGIN_CONFIG_RAW = process.env.CGP_RELAY_PLUGIN_CONFIG;

    let pluginConfig: Record<string, any> = {};
    if (PLUGIN_CONFIG_RAW) {
      try {
        pluginConfig = JSON.parse(PLUGIN_CONFIG_RAW);
      } catch (e: any) {
        console.error(
          "Failed to parse CGP_RELAY_PLUGIN_CONFIG as JSON:",
          e.message || String(e),
        );
      }
    }

    const plugins: RelayPlugin[] = [];
    const pluginNames = Array.from(
      new Set(parsePluginList(PLUGINS_SPEC || "")),
    );
    if (pluginNames.length > 0) {
      for (const name of pluginNames) {
        try {
          const plugin = await loadPlugin(name, pluginConfig[name]);
          plugins.push(plugin);
          console.log(`Loaded relay plugin: ${plugin.name} (${name})`);
        } catch (e: any) {
          console.error(
            `Failed to load relay plugin ${name}:`,
            e.message || String(e),
          );
        }
      }
    }

    const pubSubUrls = (PUBSUB_URLS || PUBSUB_URL || "")
      .split(",")
      .map((entry) => entry.trim())
      .filter(Boolean);
    const pubSubAdapter =
      pubSubUrls.length > 1
        ? process.env.CGP_RELAY_PUBSUB_MODE === "redundant"
          ? new RedundantWebSocketRelayPubSubAdapter(pubSubUrls)
          : new ShardedWebSocketRelayPubSubAdapter(pubSubUrls)
        : pubSubUrls.length === 1
          ? new WebSocketRelayPubSubAdapter(pubSubUrls[0])
          : undefined;
    new RelayServer(PORT, DB_PATH, plugins, {
      pubSubAdapter,
      instanceId: process.env.CGP_RELAY_INSTANCE_ID,
      listenHost: process.env.CGP_RELAY_HOST,
    });
  })();
}

export function parsePluginList(spec: string): string[] {
  const trimmed = spec.trim();
  if (!trimmed) return [];

  if (trimmed.startsWith("[")) {
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed))
        return parsed
          .map(String)
          .map((s) => s.trim())
          .filter(Boolean);
    } catch {
      // fallthrough
    }
  }

  return trimmed
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

export async function loadPlugin(
  moduleName: string,
  config?: any,
): Promise<RelayPlugin> {
  const builtIn = builtInRelayPluginFactory(moduleName);
  if (builtIn) {
    return builtIn(config);
  }

  const mod: any = await import(moduleName);

  const namespaces = [mod, mod?.default].filter(
    (entry) => entry && typeof entry === "object",
  );
  const functionCandidate = namespaces
    .flatMap((entry: any) => [
      entry.default,
      entry.createRelayPlugin,
      entry.createPlugin,
      entry.plugin,
      entry.createHollowFederatedRelayPlugin,
    ])
    .find((entry) => typeof entry === "function");

  if (functionCandidate) {
    const plugin = functionCandidate(config);
    if (!plugin || typeof plugin.name !== "string") {
      throw new Error(
        `Plugin factory did not return a valid plugin for ${moduleName}`,
      );
    }
    return plugin as RelayPlugin;
  }

  const objectCandidate = namespaces
    .flatMap((entry: any) => [entry.plugin, entry.default, entry])
    .find((entry) => entry && typeof entry === "object" && typeof entry.name === "string");

  if (objectCandidate) {
    return objectCandidate as RelayPlugin;
  }

  throw new Error(`Unsupported plugin module shape for ${moduleName}`);
}

function builtInRelayPluginFactory(
  name: string,
): ((config?: any) => RelayPlugin) | undefined {
  switch (name.trim()) {
    case "cgp.ipfs.faux":
    case "faux-ipfs":
      return createFauxIpfsBackendPlugin;
    case "cgp.ipfs.helia":
    case "helia":
      return createHeliaIpfsPlugin;
    case "cgp.media.storage":
    case "media-storage":
      return createMediaStoragePolicyPlugin;
    case "cgp.expression.search":
    case "expression-search":
      return createExpressionSearchProviderPlugin;
    case "cgp.static-shards":
    case "static-shards":
      return createStaticShardSeedPlugin;
    case "cgp.github.mirror":
    case "github-mirror":
      return createGitHubRelayMirrorPlugin;
    case "hollow-relay":
      return createHollowRoomRelayPlugin;
    case "cgp.relay.push":
    case "relay-push":
      return createRelayPushPlugin;
    case "cgp.rate-limit":
    case "rate-limit":
      return createRateLimitPolicyPlugin;
    case "cgp.abuse-control":
    case "abuse-control":
      return createAbuseControlPolicyPlugin;
    case "cgp.encryption-policy":
    case "encryption-policy":
      return createEncryptionPolicyPlugin;
    case "cgp.app-surface-policy":
    case "app-surface-policy":
      return createAppSurfacePolicyPlugin;
    case "cgp.app-object-permissions":
    case "app-object-permissions":
      return createAppObjectPermissionPlugin;
    case "cgp.safety-report":
    case "safety-report":
      return createSafetyReportPlugin;
    case "cgp.proof-of-work":
    case "proof-of-work":
      return createProofOfWorkPolicyPlugin;
    case "cgp.webhook-ingress":
    case "webhook-ingress":
      return createWebhookIngressPlugin;
    case "cgp.sandboxed-command":
    case "sandboxed-command":
      return createSandboxedCommandPlugin;
    default:
      return undefined;
  }
}
