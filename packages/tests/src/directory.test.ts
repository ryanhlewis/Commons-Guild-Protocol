import { describe, it, expect, afterEach, beforeEach } from "vitest";
import { DirectoryService, verifyDirectoryLookupProof, verifyDirectoryLookupQuorum } from "@cgp/directory/src/index";
import { hashObject, generatePrivateKey, getPublicKey, sign, directoryRegistrationPayload } from "@cgp/core";
import { MerkleTree } from "merkletreejs";
import { sha256 } from "@noble/hashes/sha256";
import fs from "fs";
import os from "os";
import path from "path";

const DB_PATH = "./test-directory-db";

describe("Directory Service", () => {
    let service: DirectoryService;

    beforeEach(() => {
        if (fs.existsSync(DB_PATH)) fs.rmSync(DB_PATH, { recursive: true, force: true });
        service = new DirectoryService(DB_PATH, {workBits:0});
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

        const msg = directoryRegistrationPayload(handle, guildId, guildPubkey, timestamp, relays);
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
        const msg1 = directoryRegistrationPayload("g1", "id1", pub1, ts1);
        const sig1 = await sign(privKey1, hashObject(msg1));
        await service.register("g1", "id1", pub1, sig1, ts1);

        const privKey2 = generatePrivateKey();
        const pub2 = getPublicKey(privKey2);
        const ts2 = Date.now();
        const msg2 = directoryRegistrationPayload("g2", "id2", pub2, ts2);
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
        const signature = await sign(privKey, hashObject(directoryRegistrationPayload("valid", guildId, pub, timestamp)));

        await expect(service.register("valid", guildId, pub, signature, timestamp)).resolves.toBeUndefined();
        await expect(service.register("tampered", guildId, pub, signature, timestamp)).rejects.toThrow(/Invalid signature/);

        const staleTimestamp = Date.now() - 10 * 60 * 1000;
        const staleSignature = await sign(privKey, hashObject(directoryRegistrationPayload("stale", guildId, pub, staleTimestamp)));
        await expect(service.register("stale", guildId, pub, staleSignature, staleTimestamp)).rejects.toThrow(/Timestamp/);
    });

    it("requires an operator quorum for portable lookup trust", async () => {
        const secondDbPath = `${DB_PATH}-second`;
        if (fs.existsSync(secondDbPath)) fs.rmSync(secondDbPath, { recursive: true, force: true });
        const secondService = new DirectoryService(secondDbPath, {workBits:0});
        try {
            const handle = "quorum";
            const guildId = hashObject({ name: "directory-quorum" });
            const privKey = generatePrivateKey();
            const pub = getPublicKey(privKey);
            const timestamp = Date.now();
            const signature = await sign(privKey, hashObject(directoryRegistrationPayload(handle, guildId, pub, timestamp)));

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

    it("keeps directory discovery available when a minority is stale and defaults to a strict majority", async () => {
        const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cgp-directory-degraded-'));
        const services = [0, 1, 2].map((index) => new DirectoryService(path.join(root, `db-${index}`), {workBits:0}));
        try {
            const handle = "degraded";
            const guildId = hashObject({ name: "canonical-directory-entry" });
            const divergentGuildId = hashObject({ name: "stale-directory-entry" });
            const guildKey = generatePrivateKey();
            const guildPubkey = getPublicKey(guildKey);
            const timestamp = Date.now();
            const canonicalSignature = await sign(guildKey, hashObject(directoryRegistrationPayload(handle, guildId, guildPubkey, timestamp)));
            const divergentSignature = await sign(guildKey, hashObject(directoryRegistrationPayload(handle, divergentGuildId, guildPubkey, timestamp)));

            await services[0].register(handle, guildId, guildPubkey, canonicalSignature, timestamp);
            await services[1].register(handle, guildId, guildPubkey, canonicalSignature, timestamp);
            await services[2].register(handle, divergentGuildId, guildPubkey, divergentSignature, timestamp);

            const lookups = await Promise.all(services.map((service) => service.getLookupProof(handle)));
            const trustedOperatorPubkeys = services.map((service) => service.operatorPubkey);
            expect(verifyDirectoryLookupQuorum(lookups.filter(Boolean) as any[], {
                expectedHandle: handle,
                trustedOperatorPubkeys
            })).toBe(true);
            expect(verifyDirectoryLookupQuorum(lookups.slice(0, 1).filter(Boolean) as any[], {
                expectedHandle: handle,
                trustedOperatorPubkeys: trustedOperatorPubkeys.slice(0, 2)
            })).toBe(false);
            expect(verifyDirectoryLookupQuorum(lookups.slice(0, 1).filter(Boolean) as any[], {
                expectedHandle: handle,
                trustedOperatorPubkeys: trustedOperatorPubkeys.slice(0, 1)
            })).toBe(true);

            const equivocalGuildId = hashObject({ name: "equivocal-directory-entry" });
            const equivocalTimestamp = timestamp + 1;
            const equivocalSignature = await sign(guildKey, hashObject(directoryRegistrationPayload(handle, equivocalGuildId, guildPubkey, equivocalTimestamp)));
            await services[0].register(handle, equivocalGuildId, guildPubkey, equivocalSignature, equivocalTimestamp);
            const conflictingProof = await services[0].getLookupProof(handle);
            expect(verifyDirectoryLookupQuorum([...lookups.filter(Boolean), conflictingProof!] as any[], {
                expectedHandle: handle,
                trustedOperatorPubkeys
            })).toBe(false);
        } finally {
            await Promise.all(services.map((service) => service.close()));
            await new Promise(resolve => setTimeout(resolve, 100));
            fs.rmSync(root, { recursive: true, force: true });
        }
    });
});
