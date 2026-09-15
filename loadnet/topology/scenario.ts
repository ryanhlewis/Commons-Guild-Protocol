import { buildGameRoutePlans } from "./game-sharding.ts";
import { selectGameNetcode } from "./game-networking.ts";
import { buildMediaRoutePlans } from "./media-topology.ts";
import { buildResilientMediaRoutePlan } from "./resilient-media.ts";
import { buildResilientRelayAssignments } from "./resilient-placement.ts";
import { selectRoomRelayCohort } from "./room-relay-cohort.ts";
import { selectMediaRelayOverlay, selectMediaRoomMode } from "./media-session.ts";
import { assignParticipantsToRelays } from "./relay-capabilities.ts";
import type { ParticipantLocation, RelayCapabilities, TopologyLabScenario } from "./types.ts";

export interface ScenarioConfig {
    runId: string;
    users: number;
    relays: number;
    audioSources: number;
    videoSources: number;
    gameSources?: number;
    audioTopK: number;
    videoTiles: number;
    audioFrames: number;
    videoFrames: number;
    gameFrames: number;
    worldSize: number;
    cellSize: number;
    interestRadius: number;
    relayParticipantCapacity?: number;
    clientSinks?: number;
    chaosClientSinks?: number;
    resilienceSeed?: number;
    resilienceDetectionMs?: number;
    chaosRelays?: number;
    recoveryFederationCopies?: number;
    adaptiveRelayCohort?: boolean;
    targetRelayUtilization?: number;
}

export function buildParticipants(count: number, worldSize: number): ParticipantLocation[] {
    const side = Math.ceil(Math.sqrt(count));
    return Array.from({ length: count }, (_, participantId) => ({
        participantId,
        region: `region-${participantId % 4}`,
        x: ((participantId % side) + 0.5) * worldSize / side,
        y: (Math.floor(participantId / side) + 0.5) * worldSize / side
    }));
}

export function buildLabRelayCapabilities(count: number, relayParticipantCapacity?: number): RelayCapabilities[] {
    return Array.from({ length: count }, (_, index) => ({
        relayId: `media-relay-${index}`,
        region: `region-${index % 4}`,
        operator: "user" as const,
        rttMs: 8 + (index % 5) * 4,
        healthy: true,
        udp: true,
        turn: true,
        sfu: true,
        federation: true,
        opaqueE2eeForwarding: true,
        sframe: true,
        mls: true,
        svc: true,
        dependencyDescriptor: true,
        mediaFastPath: true,
        gameAuthority: true,
        authorityTrust: "forward-only" as const,
        protocolVersions: ["hollow-media/1", "hollow-game/1"],
        capacity: {
            maxParticipants: relayParticipantCapacity ?? Math.max(1000, Math.ceil(10000 / Math.max(1, count))),
            maxIngressMbps: 1000,
            maxEgressMbps: 1000,
            participantLoad: 0,
            ingressLoadMbps: 0,
            egressLoadMbps: 0
        }
    }));
}

export function buildTopologyLabScenario(config: ScenarioConfig): TopologyLabScenario {
    const participants = buildParticipants(config.users, config.worldSize);
    const availableRelays = buildLabRelayCapabilities(config.relays, config.relayParticipantCapacity);
    const requestedChaosRelayCount = Math.min(
        Math.max(1, availableRelays.length - 1),
        Math.max(1, Math.floor(config.chaosRelays ?? Math.min(2, availableRelays.length - 1)))
    );
    const relayCohort = config.adaptiveRelayCohort
        ? selectRoomRelayCohort(availableRelays, participants.length, {
            failureReserve: requestedChaosRelayCount,
            minimumSurvivors: config.recoveryFederationCopies,
            targetUtilization: config.targetRelayUtilization
        })
        : {
            active: availableRelays,
            standby: [],
            worstCaseSurvivorCapacity: availableRelays.reduce(
                (sum, relay) => sum + relay.capacity.maxParticipants,
                0
            ),
            targetUtilization: config.targetRelayUtilization ?? 1
        };
    const relays = relayCohort.active;
    const relayIds = relays.map((relay) => relay.relayId);
    const homeRelays = assignParticipantsToRelays(participants, relays, {
        protocolVersion: "hollow-media/1",
        requireSfu: true,
        requireFederation: config.relays > 1,
        requireOpaqueE2ee: true,
        requireSframe: true,
        requireMls: true,
        requireSvc: true,
        requireFastPath: true,
        minimumEgressMbps: 1
    });
    const resilientAssignments = buildResilientRelayAssignments(participants, relays, {
        protocolVersion: "hollow-media/1",
        requireSfu: true,
        requireFederation: config.relays > 1,
        requireOpaqueE2ee: true,
        requireSframe: true,
        requireMls: true,
        requireSvc: true,
        requireFastPath: true,
        minimumEgressMbps: 1
    }, Math.min(Math.max(3, requestedChaosRelayCount + 1), relays.length));
    const audioSources = participants.slice(0, Math.min(config.audioSources, participants.length)).map((entry) => entry.participantId);
    const videoSources = participants.slice(0, Math.min(config.videoSources, participants.length)).map((entry) => entry.participantId);
    const gameSources = participants.slice(0, Math.min(config.gameSources ?? 16, participants.length)).map((entry) => entry.participantId);
    const activeRelayCount = new Set(homeRelays).size;
    const mediaPlans = buildMediaRoutePlans({
        participantCount: participants.length,
        relayIds,
        homeRelays,
        audioSources,
        videoSources,
        audioTopK: config.audioTopK,
        videoTiles: config.videoTiles,
        relayNetwork: relays
    });
    const relayLoads = new Map<string, number>();
    for (const relayId of resilientAssignments.primaryByParticipant) {
        relayLoads.set(relayId, (relayLoads.get(relayId) ?? 0) + 1);
    }
    const chaosRelayCount = Math.min(requestedChaosRelayCount, relays.length - 1);
    const resilienceFailedRelayIds = [...relayLoads]
        .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
        .slice(0, chaosRelayCount)
        .map(([relayId]) => relayId);
    const clientSinks = Math.max(1, Math.floor(config.clientSinks ?? Math.min(8, participants.length)));
    const chaosSinkCount = Math.min(
        Math.max(0, clientSinks - 1),
        Math.max(0, Math.floor(config.chaosClientSinks ?? Math.max(1, clientSinks * 0.125)))
    );
    let randomState = (config.resilienceSeed ?? 0x484f4c4c) >>> 0;
    const sinkCandidates = Array.from({ length: clientSinks }, (_, index) => index);
    for (let index = sinkCandidates.length - 1; index > 0; index -= 1) {
        randomState = (Math.imul(randomState, 1664525) + 1013904223) >>> 0;
        const swapIndex = randomState % (index + 1);
        [sinkCandidates[index], sinkCandidates[swapIndex]] = [sinkCandidates[swapIndex], sinkCandidates[index]];
    }
    const departedSinkIndices = sinkCandidates.slice(0, chaosSinkCount).sort((left, right) => left - right);
    const departedSinkSet = new Set(departedSinkIndices);
    const departedParticipantIds = participants
        .filter((participant) => departedSinkSet.has(participant.participantId % clientSinks))
        .map((participant) => participant.participantId);
    const gamePlans = buildGameRoutePlans({
        participants,
        relayIds,
        cellSize: config.cellSize,
        interestRadius: config.interestRadius
    });
    const resilientMedia = buildResilientMediaRoutePlan({
        participantCount: participants.length,
        relayIds,
        relayNetwork: relays,
        relayParticipantCapacityById: Object.fromEntries(relays.map((relay) => [
            relay.relayId,
            relay.capacity.maxParticipants
        ])),
        homeRelayCandidates: resilientAssignments.candidatesByParticipant,
        audioSources,
        videoSources,
        gameSources,
        gameSubscriptions: gamePlans["spatial-sharded-game"].subscriptions,
        audioTopK: config.audioTopK,
        videoTiles: config.videoTiles,
        failedRelayIds: resilienceFailedRelayIds,
        departedParticipantIds,
        routeEpoch: 2,
        churnEpoch: 3,
        useCascade: activeRelayCount > 5
    });
    return {
        version: 3,
        runId: config.runId,
        relayIds,
        participants,
        mediaSources: { audio: audioSources, video: videoSources, game: gameSources },
        topologies: { ...mediaPlans, ...gamePlans, "resilient-sfu": resilientMedia.plan },
        streamFrames: {
            audio: config.audioFrames,
            video: config.videoFrames,
            game: config.gameFrames
        },
        architecture: {
            mediaMode: selectMediaRoomMode({
                participants: participants.length,
                simultaneousPublishers: audioSources.length + videoSources.length,
                expectedAudience: participants.length,
                endToEndEncryption: true
            }),
            mediaOverlay: selectMediaRelayOverlay({
                demandRelayCount: activeRelayCount,
                maxSourceFanout: 4,
                estimatedExtraHopMs: 25,
                latencyBudgetMs: 150
            }),
            gameMode: selectGameNetcode({
                maxPlayers: participants.length,
                deterministicSimulation: false,
                supportsRollback: true,
                competitive: false,
                persistentWorld: participants.length > 64,
                tickRate: 30
            }).mode,
            opaqueUserRelays: true,
            availableRelayCount: availableRelays.length,
            activeRelayCount: relays.length,
            standbyRelayCount: relayCohort.standby.length,
            worstCaseSurvivorCapacity: relayCohort.worstCaseSurvivorCapacity,
            targetRelayUtilization: relayCohort.targetUtilization
        },
        failedRelayId: relayIds.length > 1
            ? homeRelays[audioSources[1] ?? audioSources[0]]
            : undefined,
        resilience: {
            seed: config.resilienceSeed ?? 0x484f4c4c,
            failedRelayIds: resilienceFailedRelayIds,
            departedSinkIndices,
            departedParticipantIds,
            detectionMs: Math.max(10, config.resilienceDetectionMs ?? 150),
            routeEpoch: 2,
            churnEpoch: 3,
            ...resilientMedia.evidence
        }
    };
}
