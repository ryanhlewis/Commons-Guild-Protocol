import { buildMediaSubscriptions } from "./media-topology.ts";
import { buildDemandDrivenRelayTree, type RelayNetworkPoint } from "./relay-overlay.ts";
import { repairRelayTree } from "./resilient-overlay.ts";
import { assignmentMovement, remapRelayAssignments } from "./resilient-placement.ts";
import type { RelayOverlay, TopologyRouteEpoch, TopologyRoutePlan } from "./types.ts";

export interface ResilientMediaRouteConfig {
    participantCount: number;
    relayIds: string[];
    relayNetwork: RelayNetworkPoint[];
    relayParticipantCapacityById: Record<string, number>;
    homeRelayCandidates: string[][];
    audioSources: number[];
    videoSources: number[];
    gameSources: number[];
    gameSubscriptions: Record<string, number[]>;
    audioTopK: number;
    videoTiles: number;
    failedRelayIds: string[];
    departedParticipantIds: number[];
    routeEpoch?: number;
    churnEpoch?: number;
    cascadeMaxChildren?: number;
    useCascade?: boolean;
}

export interface ResilientMediaEvidence {
    remappedParticipants: number;
    assignmentMovementRatio: number;
    repairedTreeParents: number;
    cascade: boolean;
    maximumRecoveredRelayLoad: number;
    minimumRecoveredRelayHeadroom: number;
}

function streamKeys(config: ResilientMediaRouteConfig) {
    return [
        ...config.audioSources.map((sourceId) => `audio:${sourceId}`),
        ...config.videoSources.map((sourceId) => `video:${sourceId}`),
        ...config.gameSources.map((sourceId) => `game:${sourceId}`)
    ];
}

function buildOverlays(
    config: ResilientMediaRouteConfig,
    subscriptions: Record<string, number[]>,
    sourceRelays: string[],
    recipientRelays: string[],
    departedParticipantIds: ReadonlySet<number>
) {
    const overlays: Record<string, RelayOverlay> = {};
    for (const key of streamKeys(config)) {
        const sourceId = Number(key.slice(key.indexOf(":") + 1));
        if (departedParticipantIds.has(sourceId)) continue;
        const sourceRelayId = sourceRelays[sourceId];
        const destinations = (subscriptions[key] ?? [])
            .filter((participantId) => !departedParticipantIds.has(participantId))
            .map((participantId) => recipientRelays[participantId]);
        overlays[key] = buildDemandDrivenRelayTree(sourceRelayId, destinations, config.relayNetwork, {
            maxChildren: config.cascadeMaxChildren ?? 4
        });
    }
    return overlays;
}

function repairOverlays(
    config: ResilientMediaRouteConfig,
    subscriptions: Record<string, number[]>,
    original: Record<string, RelayOverlay>,
    sourceRelays: string[],
    recipientRelays: string[],
    failedRelayIds: ReadonlySet<string>,
    departedParticipantIds: ReadonlySet<number>
) {
    const overlays: Record<string, RelayOverlay> = {};
    let changedParents = 0;
    for (const key of streamKeys(config)) {
        const sourceId = Number(key.slice(key.indexOf(":") + 1));
        if (departedParticipantIds.has(sourceId)) continue;
        const sourceRelayId = sourceRelays[sourceId];
        const requiredRelayIds = [
            sourceRelayId,
            ...(subscriptions[key] ?? [])
                .filter((participantId) => !departedParticipantIds.has(participantId))
                .map((participantId) => recipientRelays[participantId])
        ];
        const previous = original[key];
        if (!previous) {
            overlays[key] = buildDemandDrivenRelayTree(sourceRelayId, requiredRelayIds, config.relayNetwork, {
                maxChildren: config.cascadeMaxChildren ?? 4
            });
            continue;
        }
        const repaired = repairRelayTree(previous, failedRelayIds, config.relayNetwork, {
            preferredRootRelayId: sourceRelayId,
            requiredRelayIds,
            maxChildren: config.cascadeMaxChildren ?? 4
        });
        overlays[key] = repaired.overlay;
        changedParents += repaired.changedParents;
    }
    return { overlays, changedParents };
}

export function buildResilientMediaRoutePlan(config: ResilientMediaRouteConfig): {
    plan: TopologyRoutePlan;
    evidence: ResilientMediaEvidence;
} {
    const routeEpoch = config.routeEpoch ?? 2;
    const churnEpoch = config.churnEpoch ?? routeEpoch + 1;
    const primary = config.homeRelayCandidates.map((candidates, participantId) => {
        const relayId = candidates[0];
        if (!relayId) throw new Error(`Participant ${participantId} has no primary relay`);
        return relayId;
    });
    const subscriptions = {
        ...buildMediaSubscriptions({
            participantCount: config.participantCount,
            relayIds: config.relayIds,
            homeRelays: primary,
            audioSources: config.audioSources,
            videoSources: config.videoSources,
            audioTopK: config.audioTopK,
            videoTiles: config.videoTiles
        }),
        ...Object.fromEntries(config.gameSources.map((sourceId) => [
            `game:${sourceId}`,
            [...(config.gameSubscriptions[`game:${sourceId}`] ?? [])]
        ]))
    };
    const failed = new Set(config.failedRelayIds);
    const relayCapacity = new Map(Object.entries(config.relayParticipantCapacityById));
    const remapped = remapRelayAssignments(config.homeRelayCandidates, failed, relayCapacity);
    const departed = new Set(config.departedParticipantIds);
    const useCascade = config.useCascade ?? config.relayIds.length > 5;
    const baseOverlays = useCascade
        ? buildOverlays(config, subscriptions, primary, primary, new Set())
        : undefined;
    let routeOverlays: Record<string, RelayOverlay> | undefined;
    let churnOverlays: Record<string, RelayOverlay> | undefined;
    let repairedTreeParents = 0;
    if (useCascade && baseOverlays) {
        const repaired = repairOverlays(
            config,
            subscriptions,
            baseOverlays,
            remapped.assignments,
            remapped.assignments,
            failed,
            new Set()
        );
        routeOverlays = repaired.overlays;
        repairedTreeParents += repaired.changedParents;
        const churnRepaired = repairOverlays(
            config,
            subscriptions,
            routeOverlays,
            remapped.assignments,
            remapped.assignments,
            new Set(),
            departed
        );
        churnOverlays = churnRepaired.overlays;
        repairedTreeParents += churnRepaired.changedParents;
    }
    const routeEpochs: Record<string, TopologyRouteEpoch> = {
        [routeEpoch]: {
            epoch: routeEpoch,
            failedRelayIds: [...failed],
            departedParticipantIds: [],
            sourceRelayByParticipant: [...remapped.assignments],
            recipientRelayByParticipant: [...remapped.assignments],
            relayOverlays: routeOverlays
        },
        [churnEpoch]: {
            epoch: churnEpoch,
            failedRelayIds: [...failed],
            departedParticipantIds: [...departed],
            sourceRelayByParticipant: [...remapped.assignments],
            recipientRelayByParticipant: [...remapped.assignments],
            relayOverlays: churnOverlays ?? routeOverlays
        }
    };
    return {
        plan: {
            name: "resilient-sfu",
            sourceRelayByParticipant: [...primary],
            recipientRelayByParticipant: [...primary],
            subscriptions,
            relayOverlays: baseOverlays,
            sourceRelayCandidatesByParticipant: config.homeRelayCandidates.map((candidates) => [...candidates]),
            recipientRelayCandidatesByParticipant: config.homeRelayCandidates.map((candidates) => [...candidates]),
            routeEpochs
        },
        evidence: {
            remappedParticipants: remapped.changedParticipants,
            assignmentMovementRatio: assignmentMovement(primary, remapped.assignments).ratio,
            repairedTreeParents,
            cascade: useCascade,
            maximumRecoveredRelayLoad: Math.max(0, ...remapped.loads.values()),
            minimumRecoveredRelayHeadroom: Math.min(...[...remapped.loads].map(
                ([relayId, load]) => (relayCapacity.get(relayId) ?? 0) - load
            ))
        }
    };
}
