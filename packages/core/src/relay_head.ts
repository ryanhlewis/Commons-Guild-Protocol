import { hashObject, verify } from "./crypto";
import { HashHex, RelayHead, RelayHeadUnsigned } from "./types";

export interface RelayHeadConflict {
    guildId: string;
    seq: number;
    reason: "same-seq-different-hash" | "same-relay-equivocation" | "same-checkpoint-seq-different-hash";
    left: RelayHead;
    right: RelayHead;
}

export interface RelayHeadQuorumOptions {
    minValidHeads?: number;
    minCanonicalCount?: number;
    requireNoConflicts?: boolean;
    nowMs?: number;
    maxAgeMs?: number;
    maxFutureSkewMs?: number;
}

export interface RelayHeadQuorum {
    guildId: string;
    heads: RelayHead[];
    validHeads: RelayHead[];
    invalidHeads: RelayHead[];
    conflicts: RelayHeadConflict[];
    canonical?: {
        seq: number;
        hash: HashHex | null;
        count: number;
    };
}

function normalizeNullableHash(value: unknown): HashHex | null {
    return typeof value === "string" && value.length > 0 ? value : null;
}

export function relayHeadSigningPayload(head: RelayHeadUnsigned): RelayHeadUnsigned {
    return {
        protocol: "cgp/0.1",
        relayId: head.relayId,
        relayPublicKey: head.relayPublicKey,
        guildId: head.guildId,
        headSeq: head.headSeq,
        headHash: normalizeNullableHash(head.headHash),
        prevHash: normalizeNullableHash(head.prevHash),
        checkpointSeq: typeof head.checkpointSeq === "number" ? head.checkpointSeq : null,
        checkpointHash: normalizeNullableHash(head.checkpointHash),
        observedAt: head.observedAt
    };
}

export function relayHeadId(head: RelayHeadUnsigned): HashHex {
    return hashObject(relayHeadSigningPayload(head));
}

function relayHeadWithinFreshnessPolicy(head: RelayHead, options: RelayHeadQuorumOptions = {}) {
    const nowMs = options.nowMs ?? Date.now();
    const maxAgeMs = options.maxAgeMs;
    const maxFutureSkewMs = options.maxFutureSkewMs;
    if (typeof maxAgeMs === "number" && Number.isFinite(maxAgeMs) && maxAgeMs >= 0 && nowMs - head.observedAt > maxAgeMs) {
        return false;
    }
    if (typeof maxFutureSkewMs === "number" && Number.isFinite(maxFutureSkewMs) && maxFutureSkewMs >= 0 && head.observedAt - nowMs > maxFutureSkewMs) {
        return false;
    }
    return true;
}

export function verifyRelayHead(head: RelayHead, options: RelayHeadQuorumOptions = {}): boolean {
    if (!head || head.protocol !== "cgp/0.1") return false;
    if (typeof head.relayId !== "string" || head.relayId.length === 0) return false;
    if (typeof head.relayPublicKey !== "string" || head.relayPublicKey.length === 0) return false;
    if (typeof head.guildId !== "string" || head.guildId.length === 0) return false;
    if (!Number.isSafeInteger(head.headSeq) || head.headSeq < 0) return false;
    if (typeof head.observedAt !== "number" || !Number.isFinite(head.observedAt) || head.observedAt <= 0) return false;
    if (!relayHeadWithinFreshnessPolicy(head, options)) return false;
    if (typeof head.signature !== "string" || head.signature.length === 0) return false;
    return verify(head.relayPublicKey, relayHeadId(head), head.signature);
}

export function findRelayHeadConflicts(heads: RelayHead[]): RelayHeadConflict[] {
    const conflicts: RelayHeadConflict[] = [];
    const seenBySeq = new Map<string, RelayHead>();
    const seenByCheckpointSeq = new Map<string, RelayHead>();

    for (const head of heads) {
        const key = `${head.guildId}:${head.headSeq}`;
        const previous = seenBySeq.get(key);
        if (!previous) {
            seenBySeq.set(key, head);
        } else if (previous.headHash !== head.headHash) {
            conflicts.push({
                guildId: head.guildId,
                seq: head.headSeq,
                reason: previous.relayPublicKey === head.relayPublicKey
                    ? "same-relay-equivocation"
                    : "same-seq-different-hash",
                left: previous,
                right: head
            });
        }

        if (typeof head.checkpointSeq !== "number" || !head.checkpointHash) {
            continue;
        }
        const checkpointKey = `${head.guildId}:${head.checkpointSeq}`;
        const previousCheckpoint = seenByCheckpointSeq.get(checkpointKey);
        if (!previousCheckpoint) {
            seenByCheckpointSeq.set(checkpointKey, head);
        } else if (previousCheckpoint.checkpointHash !== head.checkpointHash) {
            conflicts.push({
                guildId: head.guildId,
                seq: head.checkpointSeq,
                reason: "same-checkpoint-seq-different-hash",
                left: previousCheckpoint,
                right: head
            });
        }
    }

    return conflicts;
}

export function summarizeRelayHeadQuorum(guildId: string, heads: RelayHead[], options: RelayHeadQuorumOptions = {}): RelayHeadQuorum {
    const validHeads: RelayHead[] = [];
    const invalidHeads: RelayHead[] = [];

    for (const head of heads) {
        if (head?.guildId !== guildId || !verifyRelayHead(head, options)) {
            invalidHeads.push(head);
            continue;
        }
        validHeads.push(head);
    }

    return summarizeVerifiedRelayHeadQuorum(guildId, validHeads, invalidHeads);
}

export function summarizeVerifiedRelayHeadQuorum(
    guildId: string,
    validHeads: RelayHead[],
    invalidHeads: RelayHead[] = []
): RelayHeadQuorum {
    const scopedValidHeads = validHeads.filter((head) => head.guildId === guildId);
    const buckets = new Map<string, { seq: number; hash: HashHex | null; count: number }>();
    let canonical: { seq: number; hash: HashHex | null; count: number } | undefined;

    for (const head of scopedValidHeads) {
        const key = `${head.headSeq}:${head.headHash ?? "null"}`;
        let bucket = buckets.get(key);
        if (bucket) {
            bucket.count += 1;
        } else {
            bucket = { seq: head.headSeq, hash: head.headHash, count: 1 };
            buckets.set(key, bucket);
        }
        if (
            !canonical ||
            bucket.count > canonical.count ||
            (bucket.count === canonical.count && bucket.seq > canonical.seq)
        ) {
            canonical = bucket;
        }
    }

    const conflicts = findRelayHeadConflicts(scopedValidHeads);

    return {
        guildId,
        heads: invalidHeads.length > 0 ? [...scopedValidHeads, ...invalidHeads] : scopedValidHeads,
        validHeads: scopedValidHeads,
        invalidHeads,
        conflicts,
        canonical
    };
}

export function assertRelayHeadQuorum(
    guildId: string,
    heads: RelayHead[],
    options: RelayHeadQuorumOptions = {}
): RelayHeadQuorum {
    const quorum = summarizeRelayHeadQuorum(guildId, heads, options);
    const minValidHeads = Math.max(1, Math.floor(options.minValidHeads ?? 1));
    const minCanonicalCount = Math.max(1, Math.floor(options.minCanonicalCount ?? minValidHeads));
    const requireNoConflicts = options.requireNoConflicts ?? true;

    if (quorum.validHeads.length < minValidHeads) {
        throw new Error(`Relay head quorum failed: ${quorum.validHeads.length}/${minValidHeads} valid heads`);
    }
    if (!quorum.canonical || quorum.canonical.count < minCanonicalCount) {
        throw new Error(`Relay head quorum failed: canonical count below ${minCanonicalCount}`);
    }
    if (requireNoConflicts && quorum.conflicts.length > 0) {
        throw new Error(`Relay head quorum failed: ${quorum.conflicts.length} conflicts`);
    }
    return quorum;
}
