import type { RelayOverlay } from "./types.ts";

export interface RelayNetworkPoint {
    relayId: string;
    region: string;
    rttMs: number;
}

export interface RelayTreeOptions {
    maxChildren?: number;
    maxDepth?: number;
}

function linkCost(left: RelayNetworkPoint, right: RelayNetworkPoint) {
    const regionPenalty = left.region === right.region ? 0 : 35;
    return Math.abs(left.rttMs - right.rttMs) + regionPenalty;
}

export function buildDemandDrivenRelayTree(
    sourceRelayId: string,
    destinationRelayIds: string[],
    relays: RelayNetworkPoint[],
    options: RelayTreeOptions = {}
): RelayOverlay {
    const maxChildren = Math.max(1, Math.floor(options.maxChildren ?? 4));
    const maxDepth = Math.max(1, Math.floor(options.maxDepth ?? 8));
    const relayById = new Map(relays.map((relay) => [relay.relayId, relay]));
    if (!relayById.has(sourceRelayId)) throw new Error(`Unknown source relay ${sourceRelayId}`);
    const pending = [...new Set(destinationRelayIds)]
        .filter((relayId) => relayId !== sourceRelayId)
        .map((relayId) => {
            const relay = relayById.get(relayId);
            if (!relay) throw new Error(`Unknown destination relay ${relayId}`);
            return relay;
        })
        .sort((left, right) => left.relayId.localeCompare(right.relayId));
    const childrenByRelay: Record<string, string[]> = { [sourceRelayId]: [] };
    const depthByRelay: Record<string, number> = { [sourceRelayId]: 0 };
    const attached = [relayById.get(sourceRelayId)!];

    while (pending.length > 0) {
        let best: { childIndex: number; parent: RelayNetworkPoint; score: number } | undefined;
        for (let childIndex = 0; childIndex < pending.length; childIndex += 1) {
            const child = pending[childIndex];
            for (const parent of attached) {
                const parentDepth = depthByRelay[parent.relayId];
                if (parentDepth >= maxDepth || (childrenByRelay[parent.relayId]?.length ?? 0) >= maxChildren) continue;
                const score = linkCost(parent, child) + parentDepth * 8;
                if (!best || score < best.score || (score === best.score && parent.relayId < best.parent.relayId)) {
                    best = { childIndex, parent, score };
                }
            }
        }
        if (!best) throw new Error("Relay tree cannot satisfy its fanout/depth limits");
        const [child] = pending.splice(best.childIndex, 1);
        (childrenByRelay[best.parent.relayId] ??= []).push(child.relayId);
        childrenByRelay[child.relayId] = [];
        depthByRelay[child.relayId] = depthByRelay[best.parent.relayId] + 1;
        attached.push(child);
    }

    return { rootRelayId: sourceRelayId, childrenByRelay, depthByRelay };
}

export function relayTreeStats(overlay: RelayOverlay) {
    const fanouts = Object.values(overlay.childrenByRelay).map((children) => children.length);
    return {
        relays: Object.keys(overlay.depthByRelay).length,
        edges: fanouts.reduce((sum, fanout) => sum + fanout, 0),
        maxFanout: Math.max(0, ...fanouts),
        maxDepth: Math.max(0, ...Object.values(overlay.depthByRelay))
    };
}
