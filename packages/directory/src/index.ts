import express from "express";
import { MerkleTree } from "merkletreejs";
import { sha256 } from "@noble/hashes/sha256";
import { GuildId, PublicKeyHex, HashHex, hashObject } from "@cgp/core";
import { Level } from "level";

interface DirectoryValue {
    guildId: GuildId;
    guildPubkey: PublicKeyHex;
    handle: string;
    relays?: string[];
}

// Persistent directory service
export class DirectoryService {
    private db: Level<string, string>;
    private tree: MerkleTree;
    private ready: Promise<void>;

    constructor(dbPath: string = "./directory-db") {
        this.db = new Level(dbPath);
        this.tree = new MerkleTree([], sha256);
        this.ready = this.rebuildTree();
    }

    async register(handle: string, guildId: GuildId, guildPubkey: PublicKeyHex, relays?: string[]) {
        await this.ready;
        const value: DirectoryValue = { handle, guildId, guildPubkey, relays };
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

    private async rebuildTree() {
        const leaves: Buffer[] = [];
        // Scan all entries
        for await (const [key, value] of this.db.iterator()) {
            const entry = JSON.parse(value);
            leaves.push(this.hashEntry(entry));
        }

        this.tree = new MerkleTree(leaves, sha256, { sortPairs: true });
    }

    private hashEntry(entry: DirectoryValue) {
        return Buffer.from(hashObject(entry), "hex");
    }

    async close() {
        await this.db.close();
    }
}

export const app = express();
const port = 3000;

let service: DirectoryService;

if (import.meta.url === `file://${process.argv[1]}`) {
    service = new DirectoryService();

    app.use(express.json());

    app.post("/register", async (req, res) => {
        const { handle, guildId, guildPubkey, relays } = req.body;
        if (!handle || !guildId || !guildPubkey) {
            return res.status(400).json({ error: "Missing fields" });
        }
        await service.register(handle, guildId, guildPubkey, relays);
        res.json({ success: true });
    });

    app.get("/lookup", async (req, res) => {
        const handle = req.query.handle as string;
        const entry = await service.getEntry(handle);
        if (!entry) {
            return res.status(404).json({ error: "Not found" });
        }
        const proof = await service.getProof(handle);
        const root = await service.getRoot();
        res.json({ entry, proof, root });
    });

    app.get("/root", async (req, res) => {
        res.json({ root: await service.getRoot() });
    });

    app.listen(port, () => {
        console.log(`Directory service listening on port ${port}`);
    });
}
