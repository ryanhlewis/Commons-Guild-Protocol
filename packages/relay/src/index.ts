import { LocalRelayPubSubAdapter, RelayServer } from "./server";
import type { RelayPubSubAdapter, RelayPubSubEnvelope, RelayPubSubSubscribeOptions, RelayServerOptions, RelayWireFormat } from "./server";
import { ShardedWebSocketRelayPubSubAdapter, WebSocketPubSubHub, WebSocketRelayPubSubAdapter } from "./pubsub_ws";

import { Store, MemoryStore } from "./store";
import { LevelStore } from "./store_level";
import {
    createAbuseControlPolicyPlugin,
    createAppSurfacePolicyPlugin,
    createAppObjectPermissionPlugin,
    createEncryptionPolicyPlugin,
    createProofOfWorkPolicyPlugin,
    createRateLimitPolicyPlugin,
    createSafetyReportPlugin,
    createWebhookIngressPlugin
} from "./plugins";
import type {
    AbuseControlPolicy,
    AppObjectPermissionPolicy,
    AppObjectPermissionRule,
    AppSurfacePolicy,
    EncryptionPolicy,
    ProofOfWorkPolicy,
    RelayPlugin,
    RelayPluginContext,
    RelayPluginHttpArgs,
    RateLimitPolicy,
    SafetyReportPolicy,
    WebhookIngressPolicy
} from "./plugins";

export {
    RelayServer,
    LocalRelayPubSubAdapter,
    ShardedWebSocketRelayPubSubAdapter,
    WebSocketPubSubHub,
    WebSocketRelayPubSubAdapter,
    Store,
    MemoryStore,
    LevelStore,
    createAbuseControlPolicyPlugin,
    createAppSurfacePolicyPlugin,
    createAppObjectPermissionPlugin,
    createEncryptionPolicyPlugin,
    createProofOfWorkPolicyPlugin,
    createRateLimitPolicyPlugin,
    createSafetyReportPlugin,
    createWebhookIngressPlugin
};
export type {
    AbuseControlPolicy,
    AppObjectPermissionPolicy,
    AppObjectPermissionRule,
    AppSurfacePolicy,
    EncryptionPolicy,
    ProofOfWorkPolicy,
    RelayPlugin,
    RelayPluginContext,
    RelayPluginHttpArgs,
    RateLimitPolicy,
    RelayPubSubAdapter,
    RelayPubSubEnvelope,
    RelayPubSubSubscribeOptions,
    RelayWireFormat,
    SafetyReportPolicy,
    WebhookIngressPolicy,
    RelayServerOptions
};

if (require.main === module) {
    void (async () => {
        const args = process.argv.slice(2);
        const shouldClean = args.includes('--clean') || args.includes('-c');

        const PORT = parseInt(process.env.CGP_RELAY_PORT || process.env.PORT || "7447", 10);
        const DB_PATH = process.env.CGP_RELAY_DB || "./relay-db";
        const PLUGINS_SPEC = process.env.CGP_RELAY_PLUGINS;
        const PUBSUB_URL = process.env.CGP_RELAY_PUBSUB_URL;
        const PUBSUB_URLS = process.env.CGP_RELAY_PUBSUB_URLS;
        // Clean database if --clean flag is passed
        if (shouldClean) {
            const fs = await import('fs');
            const path = await import('path');
            const dbPath = path.resolve(DB_PATH);
            if (fs.existsSync(dbPath)) {
                console.log(`Cleaning database at ${dbPath}...`);
                fs.rmSync(dbPath, { recursive: true, force: true });
                console.log('Database cleaned.');
            }
        }
        const PLUGIN_CONFIG_RAW = process.env.CGP_RELAY_PLUGIN_CONFIG;

        let pluginConfig: Record<string, any> = {};
        if (PLUGIN_CONFIG_RAW) {
            try {
                pluginConfig = JSON.parse(PLUGIN_CONFIG_RAW);
            } catch (e: any) {
                console.error("Failed to parse CGP_RELAY_PLUGIN_CONFIG as JSON:", e.message || String(e));
            }
        }

        const plugins: RelayPlugin[] = [];
        const pluginNames = Array.from(new Set(parsePluginList(PLUGINS_SPEC || "")));
        if (pluginNames.length > 0) {
            for (const name of pluginNames) {
                try {
                    const plugin = await loadPlugin(name, pluginConfig[name]);
                    plugins.push(plugin);
                    console.log(`Loaded relay plugin: ${plugin.name} (${name})`);
                } catch (e: any) {
                    console.error(`Failed to load relay plugin ${name}:`, e.message || String(e));
                }
            }
        }

        const pubSubUrls = (PUBSUB_URLS || PUBSUB_URL || "")
            .split(",")
            .map((entry) => entry.trim())
            .filter(Boolean);
        const pubSubAdapter = pubSubUrls.length > 1
            ? new ShardedWebSocketRelayPubSubAdapter(pubSubUrls)
            : pubSubUrls.length === 1
                ? new WebSocketRelayPubSubAdapter(pubSubUrls[0])
                : undefined;
        new RelayServer(PORT, DB_PATH, plugins, { pubSubAdapter });
    })();
}

function parsePluginList(spec: string): string[] {
    const trimmed = spec.trim();
    if (!trimmed) return [];

    if (trimmed.startsWith("[")) {
        try {
            const parsed = JSON.parse(trimmed);
            if (Array.isArray(parsed)) return parsed.map(String).map(s => s.trim()).filter(Boolean);
        } catch {
            // fallthrough
        }
    }

    return trimmed.split(",").map(s => s.trim()).filter(Boolean);
}

async function loadPlugin(moduleName: string, config?: any): Promise<RelayPlugin> {
    const mod: any = await import(moduleName);

    const candidate =
        mod?.default ?? mod?.createRelayPlugin ?? mod?.createPlugin ?? mod?.plugin ?? mod;

    if (typeof candidate === "function") {
        const plugin = candidate(config);
        if (!plugin || typeof plugin.name !== "string") {
            throw new Error(`Plugin factory did not return a valid plugin for ${moduleName}`);
        }
        return plugin as RelayPlugin;
    }

    if (candidate && typeof candidate === "object" && typeof candidate.name === "string") {
        return candidate as RelayPlugin;
    }

    throw new Error(`Unsupported plugin module shape for ${moduleName}`);
}
