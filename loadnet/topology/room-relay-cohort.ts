import type { RelayCapabilities } from "./types.ts";

export interface RoomRelayCohortOptions {
    failureReserve: number;
    minimumSurvivors?: number;
    targetUtilization?: number;
}

export interface RoomRelayCohort {
    active: RelayCapabilities[];
    standby: RelayCapabilities[];
    worstCaseSurvivorCapacity: number;
    targetUtilization: number;
}

function availableParticipantCapacity(relay: RelayCapabilities) {
    return Math.max(0, relay.capacity.maxParticipants - relay.capacity.participantLoad);
}

function worstCaseSurvivorCapacity(relays: RelayCapabilities[], failureReserve: number) {
    const capacities = relays.map(availableParticipantCapacity).sort((left, right) => right - left);
    return capacities.slice(Math.min(failureReserve, capacities.length)).reduce((sum, capacity) => sum + capacity, 0);
}

export function selectRoomRelayCohort(
    relays: RelayCapabilities[],
    participantCount: number,
    options: RoomRelayCohortOptions
): RoomRelayCohort {
    const failureReserve = Math.max(0, Math.floor(options.failureReserve));
    const minimumSurvivors = Math.max(1, Math.floor(options.minimumSurvivors ?? 1));
    const targetUtilization = Math.min(1, Math.max(0.1, options.targetUtilization ?? 0.85));
    const candidates = relays
        .filter((relay) => relay.healthy && relay.udp && relay.sfu && relay.federation && relay.mediaFastPath)
        .sort((left, right) =>
            availableParticipantCapacity(right) - availableParticipantCapacity(left) ||
            left.rttMs - right.rttMs ||
            left.relayId.localeCompare(right.relayId)
        );
    for (let count = Math.max(1, failureReserve + minimumSurvivors); count <= candidates.length; count += 1) {
        const active = candidates.slice(0, count);
        const survivorCapacity = worstCaseSurvivorCapacity(active, failureReserve);
        if (survivorCapacity * targetUtilization < participantCount) continue;
        const activeIds = new Set(active.map((relay) => relay.relayId));
        return {
            active,
            standby: candidates.filter((relay) => !activeIds.has(relay.relayId)),
            worstCaseSurvivorCapacity: survivorCapacity,
            targetUtilization
        };
    }
    const totalSurvivorCapacity = worstCaseSurvivorCapacity(candidates, failureReserve);
    throw new Error(
        `Insufficient relay capacity: ${participantCount} participants require ` +
        `${Math.ceil(participantCount / targetUtilization)} post-failure slots, ` +
        `but only ${totalSurvivorCapacity} survive ${failureReserve} relay failures`
    );
}
