export type RelayOperator = "user" | "community" | "managed";
export type AuthorityTrust = "forward-only" | "community" | "managed";

export interface RelayCapacity {
    maxParticipants: number;
    maxIngressMbps: number;
    maxEgressMbps: number;
    participantLoad: number;
    ingressLoadMbps: number;
    egressLoadMbps: number;
}

export interface RelayCapabilities {
    relayId: string;
    region: string;
    operator: RelayOperator;
    rttMs: number;
    healthy: boolean;
    udp: boolean;
    turn: boolean;
    sfu: boolean;
    federation: boolean;
    opaqueE2eeForwarding: boolean;
    sframe: boolean;
    mls: boolean;
    svc: boolean;
    dependencyDescriptor: boolean;
    mediaFastPath: boolean;
    gameAuthority: boolean;
    authorityTrust: AuthorityTrust;
    protocolVersions: string[];
    capacity: RelayCapacity;
}

export interface ParticipantLocation {
    participantId: number;
    region: string;
    x: number;
    y: number;
}

export type MediaTopologyName = "turn-mesh" | "single-sfu" | "federated-sfu" | "cascaded-sfu" | "resilient-sfu";
export type GameTopologyName = "host-star-game" | "spatial-sharded-game";
export type TopologyName = MediaTopologyName | GameTopologyName;
export type StreamKind = "audio" | "video" | "game";

export interface TopologyRoutePlan {
    name: TopologyName;
    sourceRelayByParticipant: string[];
    recipientRelayByParticipant: string[];
    subscriptions: Record<string, number[]>;
    relayOverlays?: Record<string, RelayOverlay>;
    sourceRelayCandidatesByParticipant?: string[][];
    recipientRelayCandidatesByParticipant?: string[][];
    routeEpochs?: Record<string, TopologyRouteEpoch>;
}

export interface TopologyRouteEpoch {
    epoch: number;
    failedRelayIds: string[];
    departedParticipantIds: number[];
    sourceRelayByParticipant: string[];
    recipientRelayByParticipant: string[];
    relayOverlays?: Record<string, RelayOverlay>;
}

export interface RelayOverlay {
    rootRelayId: string;
    childrenByRelay: Record<string, string[]>;
    depthByRelay: Record<string, number>;
}

export interface TopologyLabScenario {
    version: 3;
    runId: string;
    relayIds: string[];
    participants: ParticipantLocation[];
    mediaSources: {
        audio: number[];
        video: number[];
        game: number[];
    };
    topologies: Record<TopologyName, TopologyRoutePlan>;
    streamFrames: Record<StreamKind, number>;
    architecture: {
        mediaMode: "direct" | "interactive-sfu" | "stage-tree";
        mediaOverlay: "direct-federation" | "cascade";
        gameMode: "deterministic-rollback" | "authoritative-rollback" | "authoritative-snapshot";
        opaqueUserRelays: boolean;
        availableRelayCount: number;
        activeRelayCount: number;
        standbyRelayCount: number;
        worstCaseSurvivorCapacity: number;
        targetRelayUtilization: number;
    };
    failedRelayId?: string;
    resilience: {
        seed: number;
        failedRelayIds: string[];
        departedSinkIndices: number[];
        departedParticipantIds: number[];
        detectionMs: number;
        routeEpoch: number;
        churnEpoch: number;
        remappedParticipants: number;
        assignmentMovementRatio: number;
        repairedTreeParents: number;
        cascade: boolean;
        maximumRecoveredRelayLoad: number;
        minimumRecoveredRelayHeadroom: number;
    };
}

export interface RouteCost {
    sourceUploads: number;
    federationCopies: number;
    clientDeliveries: number;
    totalPackets: number;
}
