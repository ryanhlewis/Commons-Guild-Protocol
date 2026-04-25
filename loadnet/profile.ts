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
    durationMs: 30000
};

export function normalizeProfile(input: Partial<LoadnetProfile> = {}): LoadnetProfile {
    const wireFormat = input.wireFormat === "binary-json" || input.wireFormat === "binary-v1" || input.wireFormat === "binary-v2"
        ? input.wireFormat
        : "json";
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
        durationMs: Math.max(1000, Math.floor(Number(input.durationMs ?? defaultProfile.durationMs)))
    };
}
