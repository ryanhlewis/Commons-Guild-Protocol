import { MerkleTree } from "merkletreejs";
import { sha256 } from "@noble/hashes/sha256";
import { hashObject, GuildId, PublicKeyHex } from "@cgp/core";

export interface DirectoryEntry {
    guildId: GuildId;
    guildPubkey: PublicKeyHex;
    handle: string;
}

export class DirectoryClient {
    private serviceUrl: string;

    constructor(serviceUrl: string) {
        this.serviceUrl = serviceUrl;
    }

    async lookup(handle: string): Promise<DirectoryEntry | null> {
        try {
            const res = await fetch(`${this.serviceUrl}/lookup?handle=${encodeURIComponent(handle)}`);
            if (res.status === 404) return null;
            if (!res.ok) throw new Error(`Directory lookup failed: ${res.statusText}`);

            const data = await res.json();
            const { entry, proof, root } = data;

            // Verify proof
            // Leaf is hashObject(entry)
            const leaf = Buffer.from(hashObject(entry), "hex");

            // We need to verify that the proof connects the leaf to the root
            // using the same hash function (sha256)
            const verified = MerkleTree.verify(proof, leaf, root, sha256);

            if (!verified) {
                throw new Error("Invalid Merkle proof for directory entry");
            }

            return entry;
        } catch (e) {
            console.error("Directory lookup error:", e);
            throw e;
        }
    }
}
