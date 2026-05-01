export interface LoadnetProfile {
    name: string;
    wireFormat: "json" | "binary-json" | "binary-v1" | "binary-v2";
    relays: number;
    pubSubShards: number;
    subscriberWorkers: number;
    subscribersPerWorker: number;
    publisherWorkers: number;
    messagesPerPublisher: number;
    batchSize: number;
    channels: number;
    latencyMs: number;
    jitterMs: number;
    lossPercent: number;
    churnEveryMs: number;
    relayRestartEveryMs: number;
    relayRestartCount: number;
    pubSubRestartEveryMs: number;
    pubSubRestartCount: number;
    partitionEveryMs: number;
    partitionDurationMs: number;
    partitionCount: number;
    partitionRelaySpan: number;
    slowConsumerPercent: number;
    slowConsumerDelayMs: number;
    durationMs: number;
    voiceChannels: number;
    partyChannels: number;
    gameChannels: number;
    fullClientWorkers: number;
    fullClientsPerWorker: number;
    fullClientActiveSigners: number;
    fullClientActionsPerWorker: number;
    fullClientBatchSize: number;
    fullClientObserverSockets: number;
    fullClientParties: number;
    fullClientGameRooms: number;
    fullClientCallRooms: number;
    fullClientInviteLinks: number;
    fullClientFallbackRelayPercent: number;
    userHostedRelays: number;
    fullClientFinalBackfill: boolean;
    fullClientBackfillPageLimit: number;
    fullClientMediaEvery: number;
    fullClientMediaBytes: number;
    fullClientMediaTags: string[];
    relayStorageSoftLimitBytes: number;
    relayStorageHardLimitBytes: number;
    relayStorageEstimateIntervalMs: number;
}

export const defaultProfile: LoadnetProfile = {
    name: "smoke",
    wireFormat: "json",
    relays: 3,
    pubSubShards: 1,
    subscriberWorkers: 4,
    subscribersPerWorker: 25,
    publisherWorkers: 1,
    messagesPerPublisher: 200,
    batchSize: 50,
    channels: 4,
    latencyMs: 25,
    jitterMs: 5,
    lossPercent: 0,
    churnEveryMs: 0,
    relayRestartEveryMs: 0,
    relayRestartCount: 0,
    pubSubRestartEveryMs: 0,
    pubSubRestartCount: 0,
    partitionEveryMs: 0,
    partitionDurationMs: 0,
    partitionCount: 0,
    partitionRelaySpan: 1,
    slowConsumerPercent: 0,
    slowConsumerDelayMs: 0,
    durationMs: 30000,
    voiceChannels: 0,
    partyChannels: 0,
    gameChannels: 0,
    fullClientWorkers: 0,
    fullClientsPerWorker: 0,
    fullClientActiveSigners: 256,
    fullClientActionsPerWorker: 0,
    fullClientBatchSize: 100,
    fullClientObserverSockets: 4,
    fullClientParties: 8,
    fullClientGameRooms: 4,
    fullClientCallRooms: 4,
    fullClientInviteLinks: 8,
    fullClientFallbackRelayPercent: 25,
    userHostedRelays: 16,
    fullClientFinalBackfill: true,
    fullClientBackfillPageLimit: 500,
    fullClientMediaEvery: 0,
    fullClientMediaBytes: 256 * 1024,
    fullClientMediaTags: ["meme", "anime", "dog", "art", "photo"],
    relayStorageSoftLimitBytes: 0,
    relayStorageHardLimitBytes: 0,
    relayStorageEstimateIntervalMs: 15000
};

export function normalizeProfile(input: Partial<LoadnetProfile> = {}): LoadnetProfile {
    const wireFormat = input.wireFormat === "binary-json" || input.wireFormat === "binary-v1" || input.wireFormat === "binary-v2"
        ? input.wireFormat
        : "json";
    const fullClientWorkers = Math.max(0, Math.floor(Number(input.fullClientWorkers ?? defaultProfile.fullClientWorkers)));
    const stringArray = (value: unknown, fallback: string[]) => {
        if (!Array.isArray(value)) return fallback;
        const normalized = value
            .filter((entry): entry is string => typeof entry === "string")
            .map((entry) => entry.trim().toLowerCase())
            .filter(Boolean);
        return normalized.length > 0 ? normalized : fallback;
    };
    const boolValue = (value: unknown, fallback: boolean) => {
        if (typeof value === "boolean") return value;
        if (typeof value === "string") {
            if (value === "1" || value.toLowerCase() === "true") return true;
            if (value === "0" || value.toLowerCase() === "false") return false;
        }
        return fallback;
    };
    return {
        ...defaultProfile,
        ...input,
        name: String(input.name || defaultProfile.name),
        wireFormat,
        relays: Math.max(1, Math.floor(Number(input.relays ?? defaultProfile.relays))),
        pubSubShards: Math.max(1, Math.floor(Number(input.pubSubShards ?? defaultProfile.pubSubShards))),
        subscriberWorkers: Math.max(0, Math.floor(Number(input.subscriberWorkers ?? defaultProfile.subscriberWorkers))),
        subscribersPerWorker: Math.max(0, Math.floor(Number(input.subscribersPerWorker ?? defaultProfile.subscribersPerWorker))),
        publisherWorkers: Math.max(1, Math.floor(Number(input.publisherWorkers ?? defaultProfile.publisherWorkers))),
        messagesPerPublisher: Math.max(0, Math.floor(Number(input.messagesPerPublisher ?? defaultProfile.messagesPerPublisher))),
        batchSize: Math.max(1, Math.floor(Number(input.batchSize ?? defaultProfile.batchSize))),
        channels: Math.max(1, Math.floor(Number(input.channels ?? defaultProfile.channels))),
        latencyMs: Math.max(0, Number(input.latencyMs ?? defaultProfile.latencyMs)),
        jitterMs: Math.max(0, Number(input.jitterMs ?? defaultProfile.jitterMs)),
        lossPercent: Math.max(0, Number(input.lossPercent ?? defaultProfile.lossPercent)),
        churnEveryMs: Math.max(0, Math.floor(Number(input.churnEveryMs ?? defaultProfile.churnEveryMs))),
        relayRestartEveryMs: Math.max(0, Math.floor(Number(input.relayRestartEveryMs ?? defaultProfile.relayRestartEveryMs))),
        relayRestartCount: Math.max(0, Math.floor(Number(input.relayRestartCount ?? defaultProfile.relayRestartCount))),
        pubSubRestartEveryMs: Math.max(0, Math.floor(Number(input.pubSubRestartEveryMs ?? defaultProfile.pubSubRestartEveryMs))),
        pubSubRestartCount: Math.max(0, Math.floor(Number(input.pubSubRestartCount ?? defaultProfile.pubSubRestartCount))),
        partitionEveryMs: Math.max(0, Math.floor(Number(input.partitionEveryMs ?? defaultProfile.partitionEveryMs))),
        partitionDurationMs: Math.max(0, Math.floor(Number(input.partitionDurationMs ?? defaultProfile.partitionDurationMs))),
        partitionCount: Math.max(0, Math.floor(Number(input.partitionCount ?? defaultProfile.partitionCount))),
        partitionRelaySpan: Math.max(1, Math.floor(Number(input.partitionRelaySpan ?? defaultProfile.partitionRelaySpan))),
        slowConsumerPercent: Math.min(100, Math.max(0, Number(input.slowConsumerPercent ?? defaultProfile.slowConsumerPercent))),
        slowConsumerDelayMs: Math.max(0, Math.floor(Number(input.slowConsumerDelayMs ?? defaultProfile.slowConsumerDelayMs))),
        durationMs: Math.max(1000, Math.floor(Number(input.durationMs ?? defaultProfile.durationMs))),
        voiceChannels: Math.max(0, Math.floor(Number(input.voiceChannels ?? defaultProfile.voiceChannels))),
        partyChannels: Math.max(0, Math.floor(Number(input.partyChannels ?? defaultProfile.partyChannels))),
        gameChannels: Math.max(0, Math.floor(Number(input.gameChannels ?? defaultProfile.gameChannels))),
        fullClientWorkers,
        fullClientsPerWorker: Math.max(fullClientWorkers > 0 ? 1 : 0, Math.floor(Number(input.fullClientsPerWorker ?? defaultProfile.fullClientsPerWorker))),
        fullClientActiveSigners: Math.max(1, Math.floor(Number(input.fullClientActiveSigners ?? defaultProfile.fullClientActiveSigners))),
        fullClientActionsPerWorker: Math.max(0, Math.floor(Number(input.fullClientActionsPerWorker ?? defaultProfile.fullClientActionsPerWorker))),
        fullClientBatchSize: Math.max(1, Math.floor(Number(input.fullClientBatchSize ?? defaultProfile.fullClientBatchSize))),
        fullClientObserverSockets: Math.max(0, Math.floor(Number(input.fullClientObserverSockets ?? defaultProfile.fullClientObserverSockets))),
        fullClientParties: Math.max(1, Math.floor(Number(input.fullClientParties ?? defaultProfile.fullClientParties))),
        fullClientGameRooms: Math.max(1, Math.floor(Number(input.fullClientGameRooms ?? defaultProfile.fullClientGameRooms))),
        fullClientCallRooms: Math.max(1, Math.floor(Number(input.fullClientCallRooms ?? defaultProfile.fullClientCallRooms))),
        fullClientInviteLinks: Math.max(1, Math.floor(Number(input.fullClientInviteLinks ?? defaultProfile.fullClientInviteLinks))),
        fullClientFallbackRelayPercent: Math.min(100, Math.max(0, Number(input.fullClientFallbackRelayPercent ?? defaultProfile.fullClientFallbackRelayPercent))),
        userHostedRelays: Math.max(0, Math.floor(Number(input.userHostedRelays ?? defaultProfile.userHostedRelays))),
        fullClientFinalBackfill: boolValue(input.fullClientFinalBackfill, defaultProfile.fullClientFinalBackfill),
        fullClientBackfillPageLimit: Math.max(64, Math.floor(Number(input.fullClientBackfillPageLimit ?? defaultProfile.fullClientBackfillPageLimit))),
        fullClientMediaEvery: Math.max(0, Math.floor(Number(input.fullClientMediaEvery ?? defaultProfile.fullClientMediaEvery))),
        fullClientMediaBytes: Math.max(1, Math.floor(Number(input.fullClientMediaBytes ?? defaultProfile.fullClientMediaBytes))),
        fullClientMediaTags: stringArray(input.fullClientMediaTags, defaultProfile.fullClientMediaTags),
        relayStorageSoftLimitBytes: Math.max(0, Math.floor(Number(input.relayStorageSoftLimitBytes ?? defaultProfile.relayStorageSoftLimitBytes))),
        relayStorageHardLimitBytes: Math.max(0, Math.floor(Number(input.relayStorageHardLimitBytes ?? defaultProfile.relayStorageHardLimitBytes))),
        relayStorageEstimateIntervalMs: Math.max(1000, Math.floor(Number(input.relayStorageEstimateIntervalMs ?? defaultProfile.relayStorageEstimateIntervalMs)))
    };
}
