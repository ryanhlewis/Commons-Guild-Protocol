import { buildDemandDrivenRelayTree, type RelayNetworkPoint } from "./relay-overlay.ts";
import type { MediaTopologyName, RouteCost, TopologyRoutePlan } from "./types.ts";

type BaselineMediaTopologyName = Exclude<MediaTopologyName, "resilient-sfu">;

export interface MediaSubscriptionConfig {
    participantCount: number;
    relayIds: string[];
    homeRelays: string[];
    audioSources: number[];
    videoSources: number[];
    audioTopK: number;
    videoTiles: number;
    relayNetwork?: RelayNetworkPoint[];
    cascadeMaxChildren?: number;
}

function sourceKey(kind: "audio" | "video", sourceId: number) {
    return `${kind}:${sourceId}`;
}

function rotatedSubscriptions(participantId: number, sources: number[], limit: number) {
    if (sources.length === 0 || limit <= 0) return [];
    const selected: number[] = [];
    for (let offset = 0; offset < sources.length && selected.length < limit; offset += 1) {
        const source = sources[(participantId + offset) % sources.length];
        if (source !== participantId && !selected.includes(source)) selected.push(source);
    }
    return selected;
}

export function buildMediaSubscriptions(config: MediaSubscriptionConfig) {
    const subscriptions: Record<string, number[]> = {};
    for (const source of config.audioSources) subscriptions[sourceKey("audio", source)] = [];
    for (const source of config.videoSources) subscriptions[sourceKey("video", source)] = [];

    for (let participant = 0; participant < config.participantCount; participant += 1) {
        for (const source of rotatedSubscriptions(participant, config.audioSources, config.audioTopK)) {
            subscriptions[sourceKey("audio", source)].push(participant);
        }
        for (const source of rotatedSubscriptions(participant, config.videoSources, config.videoTiles)) {
            subscriptions[sourceKey("video", source)].push(participant);
        }
    }
    return subscriptions;
}

export function buildMediaRoutePlans(config: MediaSubscriptionConfig): Record<BaselineMediaTopologyName, TopologyRoutePlan> {
    const subscriptions = buildMediaSubscriptions(config);
    const singleRelay = config.relayIds[0];
    if (!singleRelay) throw new Error("At least one relay is required");
    const relayNetwork = config.relayNetwork ?? config.relayIds.map((relayId, index) => ({
        relayId,
        region: `region-${index}`,
        rttMs: 10 + index
    }));
    const relayOverlays: NonNullable<TopologyRoutePlan["relayOverlays"]> = {};
    for (const kind of ["audio", "video"] as const) {
        const sources = kind === "audio" ? config.audioSources : config.videoSources;
        for (const source of sources) {
            const key = sourceKey(kind, source);
            const sourceRelay = config.homeRelays[source];
            const destinationRelays = (subscriptions[key] ?? []).map((recipient) => config.homeRelays[recipient]);
            relayOverlays[key] = buildDemandDrivenRelayTree(sourceRelay, destinationRelays, relayNetwork, {
                maxChildren: config.cascadeMaxChildren ?? 4
            });
        }
    }
    return {
        "turn-mesh": {
            name: "turn-mesh",
            sourceRelayByParticipant: [...config.homeRelays],
            recipientRelayByParticipant: [...config.homeRelays],
            subscriptions
        },
        "single-sfu": {
            name: "single-sfu",
            sourceRelayByParticipant: Array(config.participantCount).fill(singleRelay),
            recipientRelayByParticipant: Array(config.participantCount).fill(singleRelay),
            subscriptions
        },
        "federated-sfu": {
            name: "federated-sfu",
            sourceRelayByParticipant: [...config.homeRelays],
            recipientRelayByParticipant: [...config.homeRelays],
            subscriptions
        },
        "cascaded-sfu": {
            name: "cascaded-sfu",
            sourceRelayByParticipant: [...config.homeRelays],
            recipientRelayByParticipant: [...config.homeRelays],
            subscriptions,
            relayOverlays
        }
    };
}

export function routeCost(
    plan: TopologyRoutePlan,
    streams: Array<{ kind: "audio" | "video" | "game"; sourceId: number; frames: number }>
): RouteCost {
    let sourceUploads = 0;
    let federationCopies = 0;
    let clientDeliveries = 0;
    for (const stream of streams) {
        const subscribers = plan.subscriptions[`${stream.kind}:${stream.sourceId}`] ?? [];
        clientDeliveries += subscribers.length * stream.frames;
        if (plan.name === "turn-mesh") {
            sourceUploads += subscribers.length * stream.frames;
            continue;
        }
        sourceUploads += stream.frames;
        if (plan.name === "cascaded-sfu") {
            const overlay = plan.relayOverlays?.[`${stream.kind}:${stream.sourceId}`];
            const edges = overlay
                ? Object.values(overlay.childrenByRelay).reduce((sum, children) => sum + children.length, 0)
                : 0;
            federationCopies += edges * stream.frames;
        } else {
            const sourceRelay = plan.sourceRelayByParticipant[stream.sourceId];
            const remoteRelays = new Set(
                subscribers
                    .map((recipient) => plan.recipientRelayByParticipant[recipient])
                    .filter((relayId) => relayId && relayId !== sourceRelay)
            );
            federationCopies += remoteRelays.size * stream.frames;
        }
    }
    return {
        sourceUploads,
        federationCopies,
        clientDeliveries,
        totalPackets: sourceUploads + federationCopies + clientDeliveries
    };
}
