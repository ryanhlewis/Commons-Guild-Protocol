import express from "express";
import { MerkleTree } from "merkletreejs";
import { sha256 } from "@noble/hashes/sha256";
import { GuildId, PublicKeyHex, hashObject, verify, sign, generatePrivateKey, getPublicKey, directoryRegistrationPayload,
    DeviceAuthorityRegistry, deviceAuthorityContinues, verifyDeviceAuthorizedObject, type DeviceAuthorization } from "@cgp/core";
import { Level } from "level";

export interface DirectoryValue {
    guildId: GuildId;
    guildPubkey: PublicKeyHex;
    handle: string;
    relays?: string[];
    registeredAt?: number;
    registrationSignature?: string;
    registrationVersion?: number;
    deviceAuthorization?: DeviceAuthorization;
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
    private mutations: Promise<void> = Promise.resolve();
    private entries = new Map<string, DirectoryValue>();
    private authorities = new DeviceAuthorityRegistry();
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

    async register(handle: string, guildId: GuildId, guildPubkey: PublicKeyHex, signature: string, timestamp: number, relays?: string[], deviceAuthorization?: DeviceAuthorization) {
        await this.ready;
        if (typeof handle !== 'string' || !/^[a-z0-9][a-z0-9/_-]{0,63}$/.test(handle) ||
            typeof guildId !== 'string' || !guildId || guildId.length > 192 ||
            typeof guildPubkey !== 'string' || !/^(02|03)[a-f0-9]{64}$/i.test(guildPubkey) ||
            !Number.isSafeInteger(timestamp) ||
            (relays !== undefined && (!Array.isArray(relays) || relays.length > 32 || relays.some(relay => {
                if (typeof relay !== 'string' || relay.length > 2048) return true;
                try { const url = new URL(relay); return !['http:', 'https:', 'ws:', 'wss:'].includes(url.protocol) || Boolean(url.username || url.password); }
                catch { return true; }
            })))) throw new Error('Invalid directory registration fields');
        guildPubkey = guildPubkey.toLowerCase();
        const routes = [...(relays ?? [])];
        const payload = directoryRegistrationPayload(handle, guildId, guildPubkey, timestamp, routes);
        if (!deviceAuthorization && !verify(guildPubkey, hashObject(payload), signature)) {
            throw new Error("Invalid signature");
        }

        // Check timestamp to prevent replay attacks (allow 5 minute window)
        const now = Date.now();
        if (Math.abs(now - timestamp) > 5 * 60 * 1000) {
            throw new Error("Timestamp out of bounds");
        }

        const value: DirectoryValue = { handle, guildId, guildPubkey, relays: routes, registeredAt: timestamp, registrationSignature: signature,
            registrationVersion: deviceAuthorization ? 3 : 2, ...(deviceAuthorization ? { deviceAuthorization } : {}) };
        const operation = this.mutations.then(async () => {
            const pin = this.authorities.get(guildPubkey);
            if (deviceAuthorization) {
                if (!deviceAuthorityContinues(deviceAuthorization.binding, pin)) throw new Error('Directory authorization conflicts with the pinned authority');
                const checked = verifyDeviceAuthorizedObject(payload, signature, deviceAuthorization, { accountPublicKey: guildPubkey,
                    requiredCapability: 'publish', minimumRevocationEpoch: pin?.revocationEpoch, now });
                if (!checked.ok) throw new Error(checked.error || 'Directory device authorization is invalid');
            } else if (pin) throw new Error('Direct account registration is disabled after device authority activation');
            const previous = this.entries.get(handle);
            if (previous) {
                if (previous.guildPubkey.toLowerCase() !== guildPubkey) throw new Error('Handle is owned by another key');
                if (hashObject(previous) === hashObject(value)) return; // Safe idempotent retry.
                if (timestamp <= (previous.registeredAt ?? 0)) throw new Error('Registration update must be newer');
            }
            const entries = new Map(this.entries);
            entries.set(handle, value);
            // Build before persistence: a failed write leaves the published snapshot intact.
            const tree = this.treeFor(entries);
            await this.db.put(handle, JSON.stringify(value));
            if (deviceAuthorization) this.authorities.verify(payload, signature, guildPubkey, deviceAuthorization, 'publish', now);
            // No await between publishing entries and tree; readers see one snapshot.
            this.entries = entries;
            this.tree = tree;
            this.treeSize = entries.size;
        });
        this.mutations = operation.catch(() => undefined);
        await operation;
    }

    async getEntry(handle: string): Promise<DirectoryValue | undefined> {
        await this.ready;
        const entry = this.entries.get(handle);
        return entry ? structuredClone(entry) : undefined;
    }

    async getProof(handle: string) {
        await this.ready;
        const entry = this.entries.get(handle);
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
        return this.snapshot();
    }

    private async snapshot(): Promise<DirectorySnapshot> {
        const root = this.tree.getHexRoot();
        const size = this.treeSize;
        const timestamp = Date.now();
        const operatorPubkey = this.operatorPubkey;
        const signature = await sign(this.operatorPrivateKey, hashObject({ root, size, timestamp, operatorPubkey }));
        return { root, size, timestamp, operatorPubkey, signature };
    }

    async getLookupProof(handle: string): Promise<DirectoryLookupProof | null> {
        await this.ready;
        const entry = this.entries.get(handle);
        if (!entry) return null;
        return {
            entry: structuredClone(entry),
            proof: this.tree.getHexProof(this.hashEntry(entry)),
            snapshot: await this.snapshot()
        };
    }

    private async rebuildTree() {
        const entries = new Map<string, DirectoryValue>();
        // Scan all entries
        for await (const [, value] of this.db.iterator()) {
            const entry = JSON.parse(value);
            entries.set(entry.handle, entry);
        }

        this.entries = entries;
        for (const entry of [...entries.values()].filter(entry => entry.registrationVersion === 3 && entry.deviceAuthorization)
            .sort((a, b) => a.deviceAuthorization!.binding.generation - b.deviceAuthorization!.binding.generation ||
                a.deviceAuthorization!.revocation.epoch - b.deviceAuthorization!.revocation.epoch || Number(a.registeredAt) - Number(b.registeredAt))) {
            const checked = this.authorities.verify(directoryRegistrationPayload(entry.handle, entry.guildId, entry.guildPubkey, entry.registeredAt!, entry.relays),
                entry.registrationSignature!, entry.guildPubkey, entry.deviceAuthorization, 'publish', entry.registeredAt!);
            if (!checked.ok) throw new Error('Stored directory device authorization failed verification');
        }
        this.tree = this.treeFor(entries);
        this.treeSize = entries.size;
    }

    private treeFor(entries: Map<string, DirectoryValue>) {
        return new MerkleTree([...entries].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([, entry]) => this.hashEntry(entry)), sha256, { sortPairs: true });
    }

    private hashEntry(entry: DirectoryValue) {
        return Buffer.from(hashObject(entry), "hex");
    }

    async close() {
        await this.ready;
        await this.mutations;
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
    const registration = directoryRegistrationPayload(lookup.entry.handle, lookup.entry.guildId, lookup.entry.guildPubkey, lookup.entry.registeredAt!, lookup.entry.relays);
    if (lookup.entry.registrationVersion === 3) {
        if (!lookup.entry.deviceAuthorization || !verifyDeviceAuthorizedObject(registration, lookup.entry.registrationSignature!, lookup.entry.deviceAuthorization,
            { accountPublicKey: lookup.entry.guildPubkey, requiredCapability: 'publish', now: lookup.entry.registeredAt! }).ok) return false;
    } else if (lookup.entry.registrationVersion === 2 && !verify(lookup.entry.guildPubkey, hashObject(registration), lookup.entry.registrationSignature!)) return false;
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
        const { handle, guildId, guildPubkey, signature, timestamp, relays, deviceAuthorization } = req.body;
        if (!handle || !guildId || !guildPubkey || !signature || !timestamp) {
            return res.status(400).json({ error: "Missing fields" });
        }
        try {
            await service.register(handle, guildId, guildPubkey, signature, timestamp, relays, deviceAuthorization);
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
