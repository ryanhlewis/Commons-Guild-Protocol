import {
    GuildEvent,
    RelayWriteQuorumPolicy,
    SfuAuthorityMember,
    SfuAuthoritySet,
} from "./types.js";

const PUBLIC_KEY_PATTERN = /^(?:02|03)[0-9a-f]{64}$/;
const MAX_AUTHORITY_MEMBERS = 128;
const MAX_CERTIFIER_MEMBERS = 64;
const MAX_POLICY_LIFETIME_MS = 366 * 24 * 60 * 60 * 1000;
const MAX_ACTIVATION_DELAY_MS = 24 * 60 * 60 * 1000;
const MAX_CLOCK_SKEW_MS = 5 * 60 * 1000;

function normalizedPublicKeys(values: unknown) {
    if (!Array.isArray(values)) return [];
    return values.map((value) =>
        typeof value === "string" ? value.trim().toLowerCase() : "",
    );
}

function boundedText(value: unknown, maximum: number) {
    return typeof value === "string" &&
        value.length > 0 &&
        value.length <= maximum &&
        value === value.trim()
        ? value
        : "";
}

export function normalizeRelayWriteQuorumPolicy(
    value: unknown,
): RelayWriteQuorumPolicy {
    const record =
        value && typeof value === "object" && !Array.isArray(value)
            ? (value as Record<string, unknown>)
            : {};
    const epoch = boundedText(record.epoch, 128);
    const members = normalizedPublicKeys(record.members);
    const requiredVotes = Number(record.requiredVotes);
    if (
        record.protocol !== "cgp/write-quorum/1" ||
        !epoch ||
        members.length < 2 ||
        members.length > MAX_CERTIFIER_MEMBERS ||
        new Set(members).size !== members.length ||
        members.some((member) => !PUBLIC_KEY_PATTERN.test(member)) ||
        !Number.isSafeInteger(requiredVotes) ||
        requiredVotes < Math.floor(members.length / 2) + 1 ||
        requiredVotes > members.length
    ) {
        throw new Error("Invalid SFU authority certifier policy");
    }
    return {
        protocol: "cgp/write-quorum/1",
        epoch,
        members,
        requiredVotes,
    };
}

function normalizeAuthority(value: unknown): SfuAuthorityMember {
    const record =
        value && typeof value === "object" && !Array.isArray(value)
            ? (value as Record<string, unknown>)
            : {};
    const nodeId = boundedText(record.nodeId, 128);
    const clusterId = boundedText(record.clusterId, 128);
    const role =
        record.role === "authority" || record.role === "forward-only"
            ? record.role
            : "";
    const routeAuthorityPublicKey =
        typeof record.routeAuthorityPublicKey === "string"
            ? record.routeAuthorityPublicKey.trim().toLowerCase()
            : "";
    if (
        !nodeId ||
        !clusterId ||
        !role ||
        (role === "authority" &&
            !PUBLIC_KEY_PATTERN.test(routeAuthorityPublicKey)) ||
        (role === "forward-only" && routeAuthorityPublicKey)
    ) {
        throw new Error("Invalid SFU authority member");
    }
    return {
        nodeId,
        clusterId,
        role,
        routeAuthorityPublicKey:
            role === "authority" ? routeAuthorityPublicKey : undefined,
    };
}

export function normalizeSfuAuthoritySet(
    value: unknown,
    createdAt?: number,
): SfuAuthoritySet {
    const record =
        value && typeof value === "object" && !Array.isArray(value)
            ? (value as Record<string, unknown>)
            : {};
    const guildId = boundedText(record.guildId, 256);
    const epoch = Number(record.epoch);
    const previousEpoch =
        record.previousEpoch === null ? null : Number(record.previousEpoch);
    const notBefore = Number(record.notBefore);
    const overlapUntil = Number(record.overlapUntil);
    const expiresAt = Number(record.expiresAt);
    const authorities = Array.isArray(record.authorities)
        ? record.authorities.map(normalizeAuthority)
        : [];
    const authorityIds = authorities.map(
        (authority) => `${authority.clusterId}\u0000${authority.nodeId}`,
    );
    const routeKeys = authorities
        .map((authority) => authority.routeAuthorityPublicKey)
        .filter((key): key is string => Boolean(key));
    if (
        record.type !== "SFU_AUTHORITY_SET" ||
        !guildId ||
        !Number.isSafeInteger(epoch) ||
        epoch < 1 ||
        (previousEpoch !== null &&
            (!Number.isSafeInteger(previousEpoch) ||
                previousEpoch < 1 ||
                previousEpoch !== epoch - 1)) ||
        !Number.isSafeInteger(notBefore) ||
        !Number.isSafeInteger(overlapUntil) ||
        !Number.isSafeInteger(expiresAt) ||
        overlapUntil < notBefore ||
        expiresAt <= overlapUntil ||
        expiresAt - notBefore > MAX_POLICY_LIFETIME_MS ||
        authorities.length < 1 ||
        authorities.length > MAX_AUTHORITY_MEMBERS ||
        new Set(authorityIds).size !== authorityIds.length ||
        new Set(routeKeys).size !== routeKeys.length ||
        !authorities.some((authority) => authority.role === "authority")
    ) {
        throw new Error("Invalid SFU authority set");
    }
    if (
        createdAt !== undefined &&
        (notBefore < createdAt - MAX_CLOCK_SKEW_MS ||
            notBefore > createdAt + MAX_ACTIVATION_DELAY_MS)
    ) {
        throw new Error("SFU authority activation is outside the allowed window");
    }
    return {
        type: "SFU_AUTHORITY_SET",
        guildId,
        epoch,
        previousEpoch,
        notBefore,
        overlapUntil,
        expiresAt,
        certifier: normalizeRelayWriteQuorumPolicy(record.certifier),
        authorities,
    };
}

export function validateSfuAuthorityRotation(
    body: unknown,
    currentSets: SfuAuthoritySet[],
    event: Pick<GuildEvent, "createdAt">,
) {
    const next = normalizeSfuAuthoritySet(body, event.createdAt);
    const current = currentSets[currentSets.length - 1];
    if (!current) {
        if (next.epoch !== 1 || next.previousEpoch !== null) {
            throw new Error("The first SFU authority set must use epoch 1");
        }
        return next;
    }
    if (
        next.epoch !== current.epoch + 1 ||
        next.previousEpoch !== current.epoch
    ) {
        throw new Error("SFU authority epochs must rotate monotonically");
    }
    if (next.notBefore > current.expiresAt) {
        throw new Error("SFU authority rotation cannot leave an uncovered gap");
    }
    return next;
}

export function activeSfuAuthoritySets(
    sets: SfuAuthoritySet[],
    now = Date.now(),
) {
    const ordered = [...sets].sort((left, right) => left.epoch - right.epoch);
    const current = [...ordered]
        .reverse()
        .find((set) => set.notBefore <= now && set.expiresAt > now);
    if (!current) return [];
    const previous = ordered.find(
        (set) =>
            set.epoch === current.previousEpoch &&
            now <= current.overlapUntil &&
            set.expiresAt > now,
    );
    return previous ? [previous, current] : [current];
}
