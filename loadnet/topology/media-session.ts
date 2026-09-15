export type MediaRoomMode = "direct" | "interactive-sfu" | "stage-tree";
export type AudioSubscriptionMode = "all" | "none" | "include" | "exclude";

export interface MediaRoomProfile {
    participants: number;
    simultaneousPublishers: number;
    expectedAudience: number;
    endToEndEncryption: boolean;
}

export interface MediaRelayOverlayProfile {
    demandRelayCount: number;
    maxSourceFanout: number;
    estimatedExtraHopMs: number;
    latencyBudgetMs: number;
}

export interface AudioSourceActivity {
    participantId: number;
    score: number;
    muted?: boolean;
}

export interface ReceiverAudioPolicy {
    receiverId: number;
    mode: AudioSubscriptionMode;
    participantIds?: number[];
    maxActiveSpeakers: number;
}

export interface VideoLayer {
    id: string;
    width: number;
    height: number;
    maxBitrateKbps: number;
    spatialLayer: number;
    temporalLayer: number;
}

export interface VideoTileDemand {
    visible: boolean;
    viewportWidth: number;
    viewportHeight: number;
    availableBitrateKbps: number;
    priority: number;
}

export interface FastPathRoute {
    streamId: string;
    nextRelayIds: readonly string[];
    localRecipientIds: readonly number[];
}

export interface MediaFastPathSnapshot {
    epoch: number;
    routes: Readonly<Record<string, Readonly<FastPathRoute>>>;
}

export function selectMediaRoomMode(profile: MediaRoomProfile): MediaRoomMode {
    if (profile.participants <= 4 && profile.simultaneousPublishers <= 4 && !profile.endToEndEncryption) {
        return "direct";
    }
    if (profile.expectedAudience > 64 || profile.participants > 128 || profile.simultaneousPublishers > 32) {
        return "stage-tree";
    }
    return "interactive-sfu";
}

export function selectMediaRelayOverlay(profile: MediaRelayOverlayProfile): "direct-federation" | "cascade" {
    const remoteRelays = Math.max(0, profile.demandRelayCount - 1);
    if (remoteRelays <= Math.max(1, profile.maxSourceFanout)) return "direct-federation";
    if (profile.estimatedExtraHopMs >= profile.latencyBudgetMs * 0.35) return "direct-federation";
    return "cascade";
}

export function selectReceiverAudioSources(sources: AudioSourceActivity[], policy: ReceiverAudioPolicy) {
    const configured = new Set(policy.participantIds ?? []);
    return sources
        .filter((source) => source.participantId !== policy.receiverId)
        .filter((source) => {
            if (policy.mode === "none") return false;
            if (policy.mode === "include") return configured.has(source.participantId);
            if (policy.mode === "exclude") return !configured.has(source.participantId);
            return true;
        })
        .filter((source) => !source.muted)
        .sort((left, right) => right.score - left.score || left.participantId - right.participantId)
        .slice(0, Math.max(0, policy.maxActiveSpeakers))
        .map((source) => source.participantId);
}

export function prewarmedAudioPublications(sources: AudioSourceActivity[]) {
    return sources.map((source) => ({
        participantId: source.participantId,
        state: source.muted ? "dtx-warm" as const : "active" as const
    }));
}

export function selectVideoLayer(layers: VideoLayer[], demand: VideoTileDemand): VideoLayer | undefined {
    if (!demand.visible || demand.availableBitrateKbps <= 0) return undefined;
    const viewportPixels = Math.max(1, demand.viewportWidth * demand.viewportHeight);
    const priorityBudget = demand.availableBitrateKbps * Math.max(0.25, Math.min(1, demand.priority));
    return [...layers]
        .filter((layer) => layer.maxBitrateKbps <= priorityBudget)
        .filter((layer) => layer.width * layer.height <= viewportPixels * 1.5)
        .sort((left, right) =>
            right.spatialLayer - left.spatialLayer ||
            right.temporalLayer - left.temporalLayer ||
            right.maxBitrateKbps - left.maxBitrateKbps
        )[0];
}

export function compileMediaFastPath(epoch: number, routes: FastPathRoute[]): MediaFastPathSnapshot {
    if (!Number.isSafeInteger(epoch) || epoch < 1) throw new Error("Fast-path epoch must be a positive integer");
    const compiled: Record<string, Readonly<FastPathRoute>> = {};
    for (const route of routes) {
        if (compiled[route.streamId]) throw new Error(`Duplicate fast-path stream ${route.streamId}`);
        compiled[route.streamId] = Object.freeze({
            streamId: route.streamId,
            nextRelayIds: Object.freeze([...new Set(route.nextRelayIds)]),
            localRecipientIds: Object.freeze([...new Set(route.localRecipientIds)])
        });
    }
    return Object.freeze({ epoch, routes: Object.freeze(compiled) });
}
