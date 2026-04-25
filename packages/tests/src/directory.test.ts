import { describe, it, expect, afterEach, beforeEach } from "vitest";
import { DirectoryService, verifyDirectoryLookupProof, verifyDirectoryLookupQuorum } from "@cgp/directory/src/index";
import { hashObject, generatePrivateKey, getPublicKey, sign } from "@cgp/core";
import { MerkleTree } from "merkletreejs";
import { sha256 } from "@noble/hashes/sha256";
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

        const entry = await service.getEntry("g1");
        const verified = MerkleTree.verify(proof, Buffer.from(hashObject(entry), "hex"), root, sha256, { sortPairs: true });
        expect(verified).toBe(true);

        const tampered = { ...entry, guildId: "evil-id" };
        const tamperedLeaf = Buffer.from(hashObject(tampered), "hex");
        expect(MerkleTree.verify(proof, tamperedLeaf, root, sha256, { sortPairs: true })).toBe(false);

        const lookup = await service.getLookupProof("g1");
        expect(lookup).toBeDefined();
        expect(verifyDirectoryLookupProof(lookup!, {
            expectedHandle: "g1"
        })).toBe(false);
        expect(verifyDirectoryLookupProof(lookup!, {
            expectedHandle: "g1",
            trustedOperatorPubkeys: [service.operatorPubkey]
        })).toBe(true);
        expect(verifyDirectoryLookupProof(lookup!, {
            expectedHandle: "g2",
            trustedOperatorPubkeys: [service.operatorPubkey]
        })).toBe(false);
    });

    it("rejects invalid and stale registrations", async () => {
        const privKey = generatePrivateKey();
        const pub = getPublicKey(privKey);
        const guildId = hashObject({ name: "strict-directory" });
        const timestamp = Date.now();
        const signature = await sign(privKey, hashObject(`REGISTER:valid:${guildId}:${timestamp}`));

        await expect(service.register("valid", guildId, pub, signature, timestamp)).resolves.toBeUndefined();
        await expect(service.register("tampered", guildId, pub, signature, timestamp)).rejects.toThrow(/Invalid signature/);

        const staleTimestamp = Date.now() - 10 * 60 * 1000;
        const staleSignature = await sign(privKey, hashObject(`REGISTER:stale:${guildId}:${staleTimestamp}`));
        await expect(service.register("stale", guildId, pub, staleSignature, staleTimestamp)).rejects.toThrow(/Timestamp/);
    });

    it("requires an operator quorum for portable lookup trust", async () => {
        const secondDbPath = `${DB_PATH}-second`;
        if (fs.existsSync(secondDbPath)) fs.rmSync(secondDbPath, { recursive: true, force: true });
        const secondService = new DirectoryService(secondDbPath);
        try {
            const handle = "quorum";
            const guildId = hashObject({ name: "directory-quorum" });
            const privKey = generatePrivateKey();
            const pub = getPublicKey(privKey);
            const timestamp = Date.now();
            const signature = await sign(privKey, hashObject(`REGISTER:${handle}:${guildId}:${timestamp}`));

            await service.register(handle, guildId, pub, signature, timestamp);
            await secondService.register(handle, guildId, pub, signature, timestamp);

            const lookups = [
                await service.getLookupProof(handle),
                await secondService.getLookupProof(handle)
            ].filter(Boolean) as any[];

            expect(verifyDirectoryLookupQuorum(lookups, {
                expectedHandle: handle,
                trustedOperatorPubkeys: [service.operatorPubkey, secondService.operatorPubkey],
                minOperatorProofs: 2
            })).toBe(true);

            expect(verifyDirectoryLookupQuorum([lookups[0]], {
                expectedHandle: handle,
                trustedOperatorPubkeys: [service.operatorPubkey, secondService.operatorPubkey],
                minOperatorProofs: 2
            })).toBe(false);
        } finally {
            await secondService.close();
            await new Promise(resolve => setTimeout(resolve, 100));
            if (fs.existsSync(secondDbPath)) fs.rmSync(secondDbPath, { recursive: true, force: true });
        }
    });
});
