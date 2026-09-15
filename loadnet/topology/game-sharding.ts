import type { GameTopologyName, ParticipantLocation, TopologyRoutePlan } from "./types.ts";

export interface SpatialGameConfig {
    participants: ParticipantLocation[];
    relayIds: string[];
    cellSize: number;
    interestRadius: number;
    maxRecipientsPerSource?: number;
}

export interface AuthorityHandoff {
    participantId: number;
    fromRelay: string;
    toRelay: string;
    phases: ["prepare", "dual-publish", "commit"];
}

export interface AuthorityLeaseHandoff extends AuthorityHandoff {
    previousEpoch: number;
    nextEpoch: number;
    leaseExpiresAt: number;
}

function cellCoordinate(value: number, cellSize: number) {
    return Math.floor(value / Math.max(1, cellSize));
}

export function shardIdForPosition(x: number, y: number, cellSize: number) {
    return `${cellCoordinate(x, cellSize)}:${cellCoordinate(y, cellSize)}`;
}

function hash32(value: string) {
    let hash = 2166136261;
    for (const char of value) {
        hash ^= char.charCodeAt(0);
        hash = Math.imul(hash, 16777619);
    }
    hash ^= hash >>> 16;
    hash = Math.imul(hash, 0x85ebca6b);
    hash ^= hash >>> 13;
    hash = Math.imul(hash, 0xc2b2ae35);
    hash ^= hash >>> 16;
    return hash >>> 0;
}

export function relayForShard(shardId: string, relayIds: string[]) {
    if (relayIds.length === 0) throw new Error("At least one game relay is required");
    let selected = relayIds[0];
    let selectedScore = -1;
    for (const relayId of relayIds) {
        const score = hash32(`${shardId}\0${relayId}`);
        if (score > selectedScore || (score === selectedScore && relayId < selected)) {
            selected = relayId;
            selectedScore = score;
        }
    }
    return selected;
}

export function spatialSubscriptions(participants: ParticipantLocation[], interestRadius: number, maxRecipientsPerSource = Number.POSITIVE_INFINITY) {
    const radiusSquared = interestRadius * interestRadius;
    const subscriptions: Record<string, number[]> = {};
    for (const source of participants) {
        subscriptions[`game:${source.participantId}`] = participants
            .filter((recipient) => recipient.participantId !== source.participantId)
            .map((recipient) => {
                const dx = recipient.x - source.x;
                const dy = recipient.y - source.y;
                return { recipient, distanceSquared: dx * dx + dy * dy };
            })
            .filter(({ distanceSquared }) => distanceSquared <= radiusSquared)
            .sort((left, right) => left.distanceSquared - right.distanceSquared || left.recipient.participantId - right.recipient.participantId)
            .slice(0, Math.max(0, maxRecipientsPerSource))
            .map(({ recipient }) => recipient.participantId);
    }
    return subscriptions;
}

export function buildGameRoutePlans(config: SpatialGameConfig): Record<GameTopologyName, TopologyRoutePlan> {
    const hostRelay = config.relayIds[0];
    if (!hostRelay) throw new Error("At least one game relay is required");
    const participantCount = config.participants.length;
    const allToAll: Record<string, number[]> = {};
    for (const source of config.participants) {
        allToAll[`game:${source.participantId}`] = config.participants
            .filter((recipient) => recipient.participantId !== source.participantId)
            .map((recipient) => recipient.participantId);
    }
    const shardRelays = config.participants.map((participant) => relayForShard(
        shardIdForPosition(participant.x, participant.y, config.cellSize),
        config.relayIds
    ));
    return {
        "host-star-game": {
            name: "host-star-game",
            sourceRelayByParticipant: Array(participantCount).fill(hostRelay),
            recipientRelayByParticipant: Array(participantCount).fill(hostRelay),
            subscriptions: allToAll
        },
        "spatial-sharded-game": {
            name: "spatial-sharded-game",
            sourceRelayByParticipant: shardRelays,
            recipientRelayByParticipant: shardRelays,
            subscriptions: spatialSubscriptions(config.participants, config.interestRadius, config.maxRecipientsPerSource)
        }
    };
}

export function planAuthorityHandoff(
    participant: ParticipantLocation,
    nextPosition: Pick<ParticipantLocation, "x" | "y">,
    relayIds: string[],
    cellSize: number
): AuthorityHandoff | undefined {
    const fromRelay = relayForShard(shardIdForPosition(participant.x, participant.y, cellSize), relayIds);
    const toRelay = relayForShard(shardIdForPosition(nextPosition.x, nextPosition.y, cellSize), relayIds);
    if (fromRelay === toRelay) return undefined;
    return {
        participantId: participant.participantId,
        fromRelay,
        toRelay,
        phases: ["prepare", "dual-publish", "commit"]
    };
}

export function planAuthorityLeaseHandoff(
    participant: ParticipantLocation,
    nextPosition: Pick<ParticipantLocation, "x" | "y">,
    relayIds: string[],
    cellSize: number,
    currentEpoch: number,
    now: number,
    options: { boundaryHysteresis?: number; leaseMs?: number } = {}
): AuthorityLeaseHandoff | undefined {
    const hysteresis = Math.max(0, Math.min(cellSize / 2, options.boundaryHysteresis ?? cellSize * 0.1));
    const nextCellX = cellCoordinate(nextPosition.x, cellSize);
    const nextCellY = cellCoordinate(nextPosition.y, cellSize);
    const localX = nextPosition.x - nextCellX * cellSize;
    const localY = nextPosition.y - nextCellY * cellSize;
    if (localX < hysteresis || localX > cellSize - hysteresis || localY < hysteresis || localY > cellSize - hysteresis) {
        return undefined;
    }
    const handoff = planAuthorityHandoff(participant, nextPosition, relayIds, cellSize);
    if (!handoff) return undefined;
    return {
        ...handoff,
        previousEpoch: currentEpoch,
        nextEpoch: currentEpoch + 1,
        leaseExpiresAt: now + Math.max(1000, options.leaseMs ?? 10000)
    };
}
