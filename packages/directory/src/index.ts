import express from "express";
import { MerkleTree } from "merkletreejs";
import { sha256 } from "@noble/hashes/sha256";
import { GuildId, PublicKeyHex, hashObject, verify, sign, generatePrivateKey, getPublicKey } from "@cgp/core";
import { Level } from "level";

export interface DirectoryValue {
    guildId: GuildId;
    guildPubkey: PublicKeyHex;
    handle: string;
    relays?: string[];
    registeredAt?: number;
    registrationSignature?: string;
}

export interface DirectorySnapshot {
    root: string;
    size: number;
    timestamp: number;
    operatorPubkey: PublicKeyHex;
    signature: string;
}

export interface DirectoryLookupProof {
    entry: DirectoryValue;
    proof: string[];
    snapshot: DirectorySnapshot;
}

export interface DirectoryQuorumVerificationOptions {
    expectedHandle?: string;
    trustedOperatorPubkeys: string[];
    minOperatorProofs?: number;
    maxSnapshotAgeMs?: number;
}

// Persistent directory service
export class DirectoryService {
    private db: Level<string, string>;
    private tree: MerkleTree;
    private ready: Promise<void>;
    private treeSize = 0;
    private operatorPrivateKey: Uint8Array;
    public readonly operatorPubkey: PublicKeyHex;

    constructor(dbPath: string = "./directory-db", options: { operatorPrivateKey?: Uint8Array } = {}) {
        this.db = new Level(dbPath);
        this.tree = new MerkleTree([], sha256);
        this.operatorPrivateKey = options.operatorPrivateKey ?? generatePrivateKey();
        this.operatorPubkey = getPublicKey(this.operatorPrivateKey);
        this.ready = this.rebuildTree();
    }

    async register(handle: string, guildId: GuildId, guildPubkey: PublicKeyHex, signature: string, timestamp: number, relays?: string[]) {
        await this.ready;

        // Verify signature
        // Message: "REGISTER:<handle>:<guildId>:<timestamp>"
        const msg = `REGISTER:${handle}:${guildId}:${timestamp}`;
        const msgHash = hashObject(msg);
        if (!verify(guildPubkey, msgHash, signature)) {
            throw new Error("Invalid signature");
        }

        // Check timestamp to prevent replay attacks (allow 5 minute window)
        const now = Date.now();
        if (Math.abs(now - timestamp) > 5 * 60 * 1000) {
            throw new Error("Timestamp out of bounds");
        }

        const value: DirectoryValue = { handle, guildId, guildPubkey, relays, registeredAt: timestamp, registrationSignature: signature };
        await this.db.put(handle, JSON.stringify(value));
        await this.rebuildTree();
    }

    async getEntry(handle: string): Promise<DirectoryValue | undefined> {
        await this.ready;
        try {
            const val = await this.db.get(handle);
            return JSON.parse(val);
        } catch (e: any) {
            if (e.code === 'LEVEL_NOT_FOUND' || e.code === 'KEY_NOT_FOUND' || e.notFound) return undefined;
            throw e;
        }
    }

    async getProof(handle: string) {
        await this.ready;
        const entry = await this.getEntry(handle);
        if (!entry) return null;
        const leaf = this.hashEntry(entry);
        return this.tree.getHexProof(leaf);
    }

    async getRoot() {
        await this.ready;
        return this.tree.getHexRoot();
    }

    async getSnapshot(): Promise<DirectorySnapshot> {
        await this.ready;
        const root = this.tree.getHexRoot();
        const size = this.treeSize;
        const timestamp = Date.now();
        const operatorPubkey = this.operatorPubkey;
        const signature = await sign(this.operatorPrivateKey, hashObject({ root, size, timestamp, operatorPubkey }));
        return { root, size, timestamp, operatorPubkey, signature };
    }

    async getLookupProof(handle: string): Promise<DirectoryLookupProof | null> {
        await this.ready;
        const entry = await this.getEntry(handle);
        if (!entry) return null;
        return {
            entry,
            proof: this.tree.getHexProof(this.hashEntry(entry)),
            snapshot: await this.getSnapshot()
        };
    }

    private async rebuildTree() {
        const leaves: Buffer[] = [];
        // Scan all entries
        for await (const [, value] of this.db.iterator()) {
            const entry = JSON.parse(value);
            leaves.push(this.hashEntry(entry));
        }

        this.tree = new MerkleTree(leaves, sha256, { sortPairs: true });
        this.treeSize = leaves.length;
    }

    private hashEntry(entry: DirectoryValue) {
        return Buffer.from(hashObject(entry), "hex");
    }

    async close() {
        await this.db.close();
    }
}

export function verifyDirectoryLookupProof(
    lookup: DirectoryLookupProof,
    options: { expectedHandle?: string; trustedOperatorPubkeys?: string[]; maxSnapshotAgeMs?: number } = {}
) {
    if (!lookup?.entry || !Array.isArray(lookup.proof) || !lookup.snapshot) {
        return false;
    }
    if (options.expectedHandle !== undefined && lookup.entry.handle !== options.expectedHandle) {
        return false;
    }
    if (!options.trustedOperatorPubkeys || options.trustedOperatorPubkeys.length === 0) {
        return false;
    }
    if (!options.trustedOperatorPubkeys.includes(lookup.snapshot.operatorPubkey)) {
        return false;
    }
    if (options.maxSnapshotAgeMs !== undefined && Date.now() - lookup.snapshot.timestamp > options.maxSnapshotAgeMs) {
        return false;
    }
    const snapshotHash = hashObject({
        root: lookup.snapshot.root,
        size: lookup.snapshot.size,
        timestamp: lookup.snapshot.timestamp,
        operatorPubkey: lookup.snapshot.operatorPubkey
    });
    if (!verify(lookup.snapshot.operatorPubkey, snapshotHash, lookup.snapshot.signature)) {
        return false;
    }
    if (!Number.isSafeInteger(lookup.snapshot.size) || lookup.snapshot.size <= 0) {
        return false;
    }
    const leaf = Buffer.from(hashObject(lookup.entry), "hex");
    return MerkleTree.verify(lookup.proof, leaf, lookup.snapshot.root, sha256, { sortPairs: true });
}

export function verifyDirectoryLookupQuorum(
    lookups: DirectoryLookupProof[],
    options: DirectoryQuorumVerificationOptions
) {
    if (!Array.isArray(lookups) || lookups.length === 0) {
        return false;
    }
    const minOperatorProofs = Math.max(1, Math.floor(options.minOperatorProofs ?? Math.ceil(options.trustedOperatorPubkeys.length / 2)));
    const accepted = new Map<string, DirectoryLookupProof>();
    let canonicalEntryHash: string | undefined;

    for (const lookup of lookups) {
        if (!verifyDirectoryLookupProof(lookup, options)) {
            continue;
        }
        const entryHash = hashObject(lookup.entry);
        if (canonicalEntryHash === undefined) {
            canonicalEntryHash = entryHash;
        } else if (canonicalEntryHash !== entryHash) {
            return false;
        }
        accepted.set(lookup.snapshot.operatorPubkey, lookup);
    }

    return accepted.size >= minOperatorProofs;
}

export const app = express();
const port = (() => {
    const raw = process.env.PORT ?? process.env.CGP_DIRECTORY_PORT ?? '3000';
    const parsed = Number.parseInt(raw, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 3000;
})();
const isMainModule = require.main === module;

let service: DirectoryService;

if (isMainModule) {
    service = new DirectoryService();

    app.use(express.json());

    app.post("/register", async (req, res) => {
        const { handle, guildId, guildPubkey, signature, timestamp, relays } = req.body;
        if (!handle || !guildId || !guildPubkey || !signature || !timestamp) {
            return res.status(400).json({ error: "Missing fields" });
        }
        try {
            await service.register(handle, guildId, guildPubkey, signature, timestamp, relays);
            res.json({ success: true });
        } catch (e: any) {
            res.status(400).json({ error: e.message });
        }
    });

    app.get("/lookup", async (req, res) => {
        const handle = req.query.handle as string;
        const entry = await service.getEntry(handle);
        if (!entry) {
            return res.status(404).json({ error: "Not found" });
        }
        const lookup = await service.getLookupProof(handle);
        res.json({ ...lookup, proof: lookup?.proof, root: lookup?.snapshot.root });
    });

    app.get("/root", async (req, res) => {
        const snapshot = await service.getSnapshot();
        res.json({ root: snapshot.root, snapshot });
    });

    app.listen(port, () => {
        console.log(`Directory service listening on port ${port}`);
    });
}
