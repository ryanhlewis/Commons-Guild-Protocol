import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { RelayServer } from "@cgp/relay/src/server";
import { RelayPlugin } from "@cgp/relay/src/plugins";
import { CgpClient } from "@cgp/client/src/client";

describe("Client Plugin Discovery", () => {
    let relay: RelayServer;
    const PORT = 7451;
    const relayUrl = `ws://localhost:${PORT}`;
    const DB_PATH = "./test-relay-db-client-plugin-" + Date.now();

    const mockPlugin: RelayPlugin = {
        name: "mock-plugin",
        metadata: {
            name: "Mock Plugin",
            description: "A test plugin",
            version: "1.0.0"
        },
        inputs: [
            { name: "token", type: "string", required: true, description: "Auth token" }
        ]
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

    it("emits plugins_loaded and populates availablePlugins", async () => {
        const client = new CgpClient({ relays: [relayUrl] });

        const pluginsPromise = new Promise<any[]>((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error("Timed out waiting for plugins")), 3000);
            client.on("plugins_loaded", (plugins: any[]) => {
                clearTimeout(timeout);
                resolve(plugins);
            });
        });

        await client.connect();
        const plugins = await pluginsPromise;

        expect(Array.isArray(plugins)).toBe(true);
        expect(plugins).toHaveLength(1);
        expect(plugins[0].name).toBe("mock-plugin");
        expect(client.availablePlugins).toHaveLength(1);

        client.close();
    });
});
