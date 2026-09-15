import { evaluateRelay, type RelayRequirement } from "./relay-capabilities.ts";
import type { ParticipantLocation, RelayCapabilities } from "./types.ts";

export interface RelayReplicaAssignment {
    participantId: number;
    relayIds: string[];
}

export interface ResilientAssignmentPlan {
    candidatesByParticipant: string[][];
    primaryByParticipant: string[];
    activeRelayIds: string[];
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

function utilization(relay: RelayCapabilities) {
    const capacity = relay.capacity;
    return Math.max(
        capacity.participantLoad / Math.max(1, capacity.maxParticipants),
        capacity.ingressLoadMbps / Math.max(1, capacity.maxIngressMbps),
        capacity.egressLoadMbps / Math.max(1, capacity.maxEgressMbps)
    );
}

function rendezvousCost(key: string, relay: RelayCapabilities, evaluationScore: number) {
    const unit = (hash32(`${key}\0${relay.relayId}`) + 1) / 4_294_967_297;
    const headroom = Math.max(0.02, 1 - utilization(relay));
    const localityWeight = 1 / (1 + Math.max(0, evaluationScore) / 100);
    const weight = Math.max(0.001, headroom * headroom * localityWeight);
    return -Math.log(unit) / weight;
}

export function rankRelayReplicas(
    key: string,
    participant: ParticipantLocation,
    relays: RelayCapabilities[],
    requirement: RelayRequirement,
    count = 3
) {
    return relays
        .map((relay) => ({ relay, evaluation: evaluateRelay(relay, participant, requirement) }))
        .filter(({ evaluation }) => evaluation.eligible)
        .map(({ relay, evaluation }) => ({ relay, cost: rendezvousCost(key, relay, evaluation.score) }))
        .sort((left, right) => left.cost - right.cost || left.relay.relayId.localeCompare(right.relay.relayId))
        .slice(0, Math.max(1, count));
}

export function buildResilientRelayAssignments(
    participants: ParticipantLocation[],
    relays: RelayCapabilities[],
    requirement: RelayRequirement,
    replicaCount = 3
): ResilientAssignmentPlan {
    const mutableRelays = relays.map((relay) => ({ ...relay, capacity: { ...relay.capacity } }));
    const candidatesByParticipant: string[][] = [];
    const primaryByParticipant: string[] = [];
    for (const participant of [...participants].sort((left, right) => left.participantId - right.participantId)) {
        const candidates = rankRelayReplicas(
            `participant:${participant.participantId}`,
            participant,
            mutableRelays,
            requirement,
            Math.min(replicaCount, mutableRelays.length)
        );
        if (candidates.length === 0) throw new Error(`No resilient relay candidates for participant ${participant.participantId}`);
        const relayIds = candidates.map(({ relay }) => relay.relayId);
        candidatesByParticipant[participant.participantId] = relayIds;
        primaryByParticipant[participant.participantId] = relayIds[0];
        candidates[0].relay.capacity.participantLoad += 1;
    }
    return {
        candidatesByParticipant,
        primaryByParticipant,
        activeRelayIds: [...new Set(primaryByParticipant)].sort()
    };
}

export function selectAvailableRelay(candidates: readonly string[], unavailableRelayIds: ReadonlySet<string>) {
    return candidates.find((relayId) => !unavailableRelayIds.has(relayId));
}

export function remapRelayAssignments(
    candidatesByParticipant: string[][],
    unavailableRelayIds: ReadonlySet<string>,
    participantCapacityByRelay?: ReadonlyMap<string, number>
) {
    const primary = candidatesByParticipant.map((candidates) => candidates[0]);
    const assignments = [...primary];
    const loads = new Map<string, number>();
    for (const relayId of primary) {
        if (relayId && !unavailableRelayIds.has(relayId)) loads.set(relayId, (loads.get(relayId) ?? 0) + 1);
    }
    for (let participantId = 0; participantId < candidatesByParticipant.length; participantId += 1) {
        if (!unavailableRelayIds.has(primary[participantId])) continue;
        const relayId = candidatesByParticipant[participantId].find((candidate) => {
            if (unavailableRelayIds.has(candidate)) return false;
            const capacity = participantCapacityByRelay?.get(candidate) ?? Number.POSITIVE_INFINITY;
            return (loads.get(candidate) ?? 0) < capacity;
        });
        if (!relayId) throw new Error(`No surviving relay capacity for participant ${participantId}`);
        assignments[participantId] = relayId;
        loads.set(relayId, (loads.get(relayId) ?? 0) + 1);
    }
    const changedParticipants = assignments.reduce((count, relayId, participantId) => count + Number(relayId !== primary[participantId]), 0);
    return { assignments, changedParticipants, loads };
}

export function assignmentMovement(before: readonly string[], after: readonly string[]) {
    if (before.length !== after.length) throw new Error("Assignment sets must have equal length");
    const changed = before.reduce((count, relayId, index) => count + Number(relayId !== after[index]), 0);
    return { changed, ratio: before.length > 0 ? changed / before.length : 0 };
}
