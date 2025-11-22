import { describe, it, expect, afterEach, beforeEach } from "vitest";
import { DirectoryService } from "@cgp/directory/src/index";
import { hashObject } from "@cgp/core";
import fs from "fs";

const DB_PATH = "./test-directory-db";

describe("Directory Service", () => {
    let service: DirectoryService;

    beforeEach(() => {
        if (fs.existsSync(DB_PATH)) fs.rmSync(DB_PATH, { recursive: true, force: true });
        service = new DirectoryService(DB_PATH);
    });

    afterEach(async () => {
        await service.close();
        // Wait for LevelDB to close
        await new Promise(resolve => setTimeout(resolve, 100));
        if (fs.existsSync(DB_PATH)) fs.rmSync(DB_PATH, { recursive: true, force: true });
    });

    it("registers and looks up a guild", async () => {
        const handle = "my-guild";
        const guildId = hashObject({ name: "test" });
        const guildPubkey = "02" + "a".repeat(64);
        const relays = ["ws://localhost:8080", "ws://relay.example.com"];

        await service.register(handle, guildId, guildPubkey, relays);

        const entry = await service.getEntry(handle);
        expect(entry).toBeDefined();
        expect(entry?.guildId).toBe(guildId);
        expect(entry?.relays).toEqual(relays);
    });

    it("provides a merkle proof", async () => {
        await service.register("g1", "id1", "pk1");
        await service.register("g2", "id2", "pk2");

        const proof = await service.getProof("g1");
        expect(proof).toBeDefined();
        expect(proof!.length).toBeGreaterThan(0);

        const root = await service.getRoot();
        expect(root).toBeDefined();
    });
});
