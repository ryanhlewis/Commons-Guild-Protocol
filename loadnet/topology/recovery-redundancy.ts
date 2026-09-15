import type { TopologyName } from "./types.ts";

export interface RecoveryRedundancyPolicy {
    routeEpoch: number;
    recoveryCopies: number;
}

export function recoveryCopiesForEpoch(
    topology: TopologyName,
    epoch: number,
    policy: RecoveryRedundancyPolicy
) {
    if (topology !== "resilient-sfu" || epoch < policy.routeEpoch) return 1;
    return Math.max(1, Math.floor(policy.recoveryCopies));
}

export const federationCopiesForEpoch = recoveryCopiesForEpoch;

export function recoveryRedundancyRatio(logicalCopies: number, physicalCopies: number) {
    if (logicalCopies <= 0) return 0;
    return Math.max(0, physicalCopies - logicalCopies) / logicalCopies;
}
