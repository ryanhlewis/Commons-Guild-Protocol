import { RelayServer } from "./server";

import { Store, MemoryStore } from "./store";
import { LevelStore } from "./store_level";
import type { RelayPlugin, RelayPluginContext } from "./plugins";

export { RelayServer, Store, MemoryStore, LevelStore };
export type { RelayPlugin, RelayPluginContext };

if (require.main === module) {
    void (async () => {
        const args = process.argv.slice(2);
        const shouldClean = args.includes('--clean') || args.includes('-c');

        const PORT = parseInt(process.env.CGP_RELAY_PORT || process.env.PORT || "7447", 10);
        const DB_PATH = process.env.CGP_RELAY_DB || "./relay-db";
        const PLUGINS_SPEC = process.env.CGP_RELAY_PLUGINS;

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
        if (PLUGINS_SPEC) {
            const pluginNames = parsePluginList(PLUGINS_SPEC);
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

        new RelayServer(PORT, DB_PATH, plugins);
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
