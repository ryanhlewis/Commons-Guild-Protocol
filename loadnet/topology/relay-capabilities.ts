import type { AuthorityTrust, ParticipantLocation, RelayCapabilities } from "./types.ts";

export interface RelayRequirement {
    protocolVersion: string;
    requireSfu?: boolean;
    requireFederation?: boolean;
    requireGameAuthority?: boolean;
    requireOpaqueE2ee?: boolean;
    requireSframe?: boolean;
    requireMls?: boolean;
    requireSvc?: boolean;
    requireFastPath?: boolean;
    minimumAuthorityTrust?: AuthorityTrust;
    minimumEgressMbps?: number;
}

export interface RelayEvaluation {
    relay: RelayCapabilities;
    eligible: boolean;
    score: number;
    rejectionReasons: string[];
}

function utilization(relay: RelayCapabilities) {
    const capacity = relay.capacity;
    return Math.max(
        capacity.participantLoad / Math.max(1, capacity.maxParticipants),
        capacity.ingressLoadMbps / Math.max(1, capacity.maxIngressMbps),
        capacity.egressLoadMbps / Math.max(1, capacity.maxEgressMbps)
    );
}

const authorityTrustRank: Record<AuthorityTrust, number> = {
    "forward-only": 0,
    community: 1,
    managed: 2
};

export function evaluateRelay(relay: RelayCapabilities, participant: ParticipantLocation, requirement: RelayRequirement): RelayEvaluation {
    const rejectionReasons: string[] = [];
    if (!relay.healthy) rejectionReasons.push("unhealthy");
    if (!relay.udp) rejectionReasons.push("udp-unavailable");
    if (!relay.protocolVersions.includes(requirement.protocolVersion)) rejectionReasons.push("protocol-version");
    if (requirement.requireSfu && !relay.sfu) rejectionReasons.push("sfu-unavailable");
    if (requirement.requireFederation && !relay.federation) rejectionReasons.push("federation-unavailable");
    if (requirement.requireGameAuthority && !relay.gameAuthority) rejectionReasons.push("game-authority-unavailable");
    if (requirement.requireOpaqueE2ee && !relay.opaqueE2eeForwarding) rejectionReasons.push("opaque-e2ee-unavailable");
    if (requirement.requireSframe && !relay.sframe) rejectionReasons.push("sframe-unavailable");
    if (requirement.requireMls && !relay.mls) rejectionReasons.push("mls-unavailable");
    if (requirement.requireSvc && (!relay.svc || !relay.dependencyDescriptor)) rejectionReasons.push("svc-unavailable");
    if (requirement.requireFastPath && !relay.mediaFastPath) rejectionReasons.push("media-fast-path-unavailable");
    if (requirement.minimumAuthorityTrust && authorityTrustRank[relay.authorityTrust] < authorityTrustRank[requirement.minimumAuthorityTrust]) {
        rejectionReasons.push("authority-trust");
    }
    if (relay.capacity.participantLoad >= relay.capacity.maxParticipants) rejectionReasons.push("participant-capacity");
    if (relay.capacity.maxEgressMbps - relay.capacity.egressLoadMbps < (requirement.minimumEgressMbps ?? 0)) {
        rejectionReasons.push("egress-capacity");
    }

    const regionPenalty = relay.region === participant.region ? 0 : 35;
    const operatorPenalty = relay.operator === "user" ? 0 : relay.operator === "community" ? 3 : 6;
    const loadPenalty = utilization(relay) * 100;
    return {
        relay,
        eligible: rejectionReasons.length === 0,
        score: relay.rttMs + regionPenalty + operatorPenalty + loadPenalty,
        rejectionReasons
    };
}

export function selectRelayCandidates(
    relays: RelayCapabilities[],
    participant: ParticipantLocation,
    requirement: RelayRequirement,
    count = 2
) {
    return relays
        .map((relay) => evaluateRelay(relay, participant, requirement))
        .filter((evaluation) => evaluation.eligible)
        .sort((left, right) => left.score - right.score || left.relay.relayId.localeCompare(right.relay.relayId))
        .slice(0, Math.max(1, count));
}

export function assignParticipantsToRelays(
    participants: ParticipantLocation[],
    relays: RelayCapabilities[],
    requirement: RelayRequirement,
    targetUtilization = 0.7
) {
    const mutable = relays.map((relay) => ({
        ...relay,
        capacity: { ...relay.capacity }
    }));
    const assignments: string[] = [];
    const activeRelays = new Set<string>();
    for (const participant of participants) {
        const candidates = selectRelayCandidates(mutable, participant, requirement, mutable.length);
        const regionalCandidates = candidates.filter((candidate) => candidate.relay.region === participant.region);
        const selected = regionalCandidates.find((candidate) =>
            activeRelays.has(candidate.relay.relayId) &&
            utilization(candidate.relay) < targetUtilization
        ) ?? regionalCandidates.find((candidate) => !activeRelays.has(candidate.relay.relayId))
            ?? regionalCandidates[0]
            ?? candidates.find((candidate) =>
            activeRelays.has(candidate.relay.relayId) &&
            utilization(candidate.relay) < targetUtilization
        ) ?? candidates[0];
        if (!selected) throw new Error(`No eligible relay for participant ${participant.participantId}`);
        assignments[participant.participantId] = selected.relay.relayId;
        activeRelays.add(selected.relay.relayId);
        selected.relay.capacity.participantLoad += 1;
    }
    return assignments;
}
