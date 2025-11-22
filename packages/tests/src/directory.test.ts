import { describe, it, expect, afterEach, beforeEach } from "vitest";
import { DirectoryService } from "@cgp/directory/src/index";
import { hashObject, generatePrivateKey, getPublicKey, sign } from "@cgp/core";
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
        const privKey = generatePrivateKey();
        const guildPubkey = getPublicKey(privKey);
        const relays = ["ws://localhost:8080", "ws://relay.example.com"];
        const timestamp = Date.now();

        const msg = `REGISTER:${handle}:${guildId}:${timestamp}`;
        const msgHash = hashObject(msg);
        const signature = await sign(privKey, msgHash);

        await service.register(handle, guildId, guildPubkey, signature, timestamp, relays);

        const entry = await service.getEntry(handle);
        expect(entry).toBeDefined();
        expect(entry?.guildId).toBe(guildId);
        expect(entry?.relays).toEqual(relays);
    });

    it("provides a merkle proof", async () => {
        const privKey1 = generatePrivateKey();
        const pub1 = getPublicKey(privKey1);
        const ts1 = Date.now();
        const msg1 = `REGISTER:g1:id1:${ts1}`;
        const sig1 = await sign(privKey1, hashObject(msg1));
        await service.register("g1", "id1", pub1, sig1, ts1);

        const privKey2 = generatePrivateKey();
        const pub2 = getPublicKey(privKey2);
        const ts2 = Date.now();
        const msg2 = `REGISTER:g2:id2:${ts2}`;
        const sig2 = await sign(privKey2, hashObject(msg2));
        await service.register("g2", "id2", pub2, sig2, ts2);

        const proof = await service.getProof("g1");
        expect(proof).toBeDefined();
        expect(proof!.length).toBeGreaterThan(0);

        const root = await service.getRoot();
        expect(root).toBeDefined();
    });
});
