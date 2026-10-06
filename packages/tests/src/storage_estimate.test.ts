import { afterEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { LevelStore, MemoryStore, RelayServer } from "@cgp/relay";

const roots: string[] = [];

afterEach(async () => {
    await Promise.all(roots.splice(0).map((root) =>
        fs.rm(root, { recursive: true, force: true }),
    ));
});

describe("LevelStore storage estimates", () => {
    it("counts every regular file and byte across nested and large flat directories", async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-storage-estimate-"));
        roots.push(root);
        const nested = path.join(root, "nested", "deeper");
        await fs.mkdir(nested, { recursive: true });

        let expectedBytes = 0;
        let expectedFiles = 0;
        const write = async (filePath: string, contents: string) => {
            await fs.writeFile(filePath, contents);
            expectedBytes += Buffer.byteLength(contents);
            expectedFiles += 1;
        };

        await Promise.all(Array.from({ length: 137 }, (_, index) =>
            write(path.join(root, `flat-${index}.dat`), `entry-${index}-x`.repeat(index % 9 + 1)),
        ));
        await write(path.join(root, "nested", "one.bin"), "one\0two\0three");
        await write(path.join(nested, "two.bin"), "nested payload");

        const store = new LevelStore(root);
        try {
            const estimate = await store.estimateStorage();
            expect(estimate).toMatchObject({ bytes: expectedBytes, files: expectedFiles, path: root });
            expect(estimate.error).toBeUndefined();
        } finally {
            await store.close();
        }
    });

    it("fails closed when a listed file disappears during the size scan", async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-storage-estimate-race-"));
        roots.push(root);
        const disappearingFile = path.join(root, "compacting.log");
        await fs.writeFile(disappearingFile, "must not be counted as an empty store");
        const store = new LevelStore(root);
        const originalStat = fs.stat.bind(fs);
        const statSpy = vi.spyOn(fs, "stat").mockImplementation(async (filePath: any, options?: any) => {
            if (filePath === disappearingFile) {
                await fs.unlink(disappearingFile);
            }
            return await originalStat(filePath, options);
        });
        try {
            const estimate = await store.estimateStorage();
            expect(estimate.error).toMatch(/ENOENT/);
            expect(estimate.bytes).toBe(0);
        } finally {
            statSpy.mockRestore();
            await store.close();
        }
    });

    it("uses cached quota totals during a stale refresh and still rejects over-limit writes", async () => {
        const store = new MemoryStore();
        const relay = new RelayServer(0, store, [], {
            enableDefaultPlugins: false,
            storagePolicy: { hardLimitBytes: 100, softLimitBytes: 0, estimateIntervalMs: 1000 },
        });
        let release: ((value: { bytes: number; files: number; checkedAt: number }) => void) | undefined;
        let scans = 0;
        try {
            const internals = relay as any;
            internals.storageEstimateCompleted = true;
            internals.storageStatus = {
                ...internals.emptyStorageStatus(),
                bytes: 95,
                checkedAt: Date.now() - 2000,
            };
            store.estimateStorage = async () => {
                scans += 1;
                return await new Promise((resolve) => { release = resolve; });
            };

            const underLimit = await Promise.race([
                internals.checkStorageWriteGate(4),
                new Promise((resolve) => setTimeout(() => resolve("blocked"), 1000)),
            ]);
            expect(underLimit).toMatchObject({ ok: true, projectedTotal: 99 });
            expect(scans).toBe(1);
            const overLimit = await internals.checkStorageWriteGate(5);
            expect(overLimit).toMatchObject({ ok: false, projectedTotal: 100 });
        } finally {
            release?.({ bytes: 95, files: 2, checkedAt: Date.now() });
            await relay.close().catch(() => undefined);
        }
    });
});
