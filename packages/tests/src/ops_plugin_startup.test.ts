import { describe, expect, it, vi } from "vitest";
import { MemoryStore, RelayServer } from "@cgp/relay";

describe("operator endpoints during plugin startup", () => {
    it("reports liveness and not-ready while plugin initialization is stalled", async () => {
        let release!: () => void;
        const pending = new Promise<void>((resolve) => { release = resolve; });
        const relay = new RelayServer(0, new MemoryStore(), [{
            name: "controlled-startup",
            onInit: async () => pending,
        }], { enableDefaultPlugins: false, listenHost: "127.0.0.1" });
        try {
            await vi.waitFor(() => expect(relay.getPort()).toBeGreaterThan(0));
            const url = `http://127.0.0.1:${relay.getPort()}`;
            const health = await fetch(`${url}/healthz`, { signal: AbortSignal.timeout(2000) });
            expect(health.status).toBe(200);
            expect((await health.json()).pluginsReady).toBe(false);
            const ready = await fetch(`${url}/readyz`, { signal: AbortSignal.timeout(2000) });
            expect(ready.status).toBe(503);
            expect((await ready.json()).pluginsReady).toBe(false);
            const metrics = await fetch(`${url}/metrics`, { signal: AbortSignal.timeout(2000) });
            expect(metrics.status).toBe(200);
            release();
            await vi.waitFor(async () => {
                const response = await fetch(`${url}/readyz`);
                expect(response.status).toBe(200);
                expect((await response.json()).pluginsReady).toBe(true);
            });
        } finally {
            release();
            await relay.close();
        }
    });

    it("does not report ready when a plugin fails initialization", async () => {
        const logging = vi.spyOn(console, "error").mockImplementation(() => undefined);
        const relay = new RelayServer(0, new MemoryStore(), [{
            name: "controlled-failure",
            onInit: async () => { throw new Error("controlled initialization failure"); },
        }], { enableDefaultPlugins: false, listenHost: "127.0.0.1" });
        try {
            await vi.waitFor(() => expect(relay.getPort()).toBeGreaterThan(0));
            await (relay as any).pluginsReady;
            const response = await fetch(`http://127.0.0.1:${relay.getPort()}/readyz`);
            expect(response.status).toBe(503);
            expect((await response.json()).pluginsReady).toBe(false);
        } finally {
            await relay.close();
            logging.mockRestore();
        }
    });
});
