import type { TopologyName } from "./types.ts";

export function shouldInjectSyntheticBaselineDrop(
    topology: TopologyName,
    epoch: number,
    relayId: string,
    failedRelayId?: string
) {
    return topology === "federated-sfu" && epoch === 2 && relayId === failedRelayId;
}
