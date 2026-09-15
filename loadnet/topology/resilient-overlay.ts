import type { RelayNetworkPoint, RelayTreeOptions } from "./relay-overlay.ts";
import type { RelayOverlay } from "./types.ts";

export interface RelayTreeRepairOptions extends RelayTreeOptions {
    preferredRootRelayId?: string;
    requiredRelayIds?: string[];
}

export interface RelayTreeRepairResult {
    overlay: RelayOverlay;
    changedParents: number;
    removedRelays: number;
}

function parentMap(overlay: RelayOverlay) {
    const parents = new Map<string, string>();
    for (const [parent, children] of Object.entries(overlay.childrenByRelay)) {
        for (const child of children) parents.set(child, parent);
    }
    return parents;
}

function linkCost(left: RelayNetworkPoint, right: RelayNetworkPoint) {
    return Math.abs(left.rttMs - right.rttMs) + (left.region === right.region ? 0 : 35);
}

export function repairRelayTree(
    original: RelayOverlay,
    failedRelayIds: ReadonlySet<string>,
    relays: RelayNetworkPoint[],
    options: RelayTreeRepairOptions = {}
): RelayTreeRepairResult {
    const maxChildren = Math.max(1, Math.floor(options.maxChildren ?? 4));
    const maxDepth = Math.max(1, Math.floor(options.maxDepth ?? 8));
    const relayById = new Map(relays.map((relay) => [relay.relayId, relay]));
    const required = new Set(options.requiredRelayIds ?? Object.keys(original.depthByRelay));
    if (options.preferredRootRelayId) required.add(options.preferredRootRelayId);
    const survivors = [...required]
        .filter((relayId) => !failedRelayIds.has(relayId) && relayById.has(relayId))
        .sort();
    if (survivors.length === 0) throw new Error("Relay tree has no surviving nodes");
    const rootRelayId = options.preferredRootRelayId && survivors.includes(options.preferredRootRelayId)
        ? options.preferredRootRelayId
        : survivors.includes(original.rootRelayId)
            ? original.rootRelayId
            : [...survivors].sort((left, right) => {
                const leftRelay = relayById.get(left)!;
                const rightRelay = relayById.get(right)!;
                return leftRelay.rttMs - rightRelay.rttMs || left.localeCompare(right);
            })[0];
    const originalParents = parentMap(original);
    const childrenByRelay: Record<string, string[]> = Object.fromEntries(survivors.map((relayId) => [relayId, []]));
    const depthByRelay: Record<string, number> = { [rootRelayId]: 0 };
    const connected = new Set([rootRelayId]);
    const pending = new Set(survivors.filter((relayId) => relayId !== rootRelayId));

    const attach = (child: string, parent: string) => {
        childrenByRelay[parent].push(child);
        childrenByRelay[parent].sort();
        depthByRelay[child] = depthByRelay[parent] + 1;
        connected.add(child);
        pending.delete(child);
    };

    while (pending.size > 0) {
        let preserved = false;
        for (const child of [...pending].sort()) {
            const parent = originalParents.get(child);
            if (!parent || !connected.has(parent)) continue;
            if (depthByRelay[parent] >= maxDepth || childrenByRelay[parent].length >= maxChildren) continue;
            attach(child, parent);
            preserved = true;
        }
        if (preserved) continue;

        let best: { child: string; parent: string; score: number } | undefined;
        for (const child of [...pending].sort()) {
            const childRelay = relayById.get(child)!;
            for (const parent of [...connected].sort()) {
                if (depthByRelay[parent] >= maxDepth || childrenByRelay[parent].length >= maxChildren) continue;
                const score = linkCost(relayById.get(parent)!, childRelay) + depthByRelay[parent] * 8;
                if (!best || score < best.score || (score === best.score && `${parent}:${child}` < `${best.parent}:${best.child}`)) {
                    best = { child, parent, score };
                }
            }
        }
        if (!best) throw new Error("Relay tree repair cannot satisfy fanout/depth limits");
        attach(best.child, best.parent);
    }

    const repairedParents = parentMap({ rootRelayId, childrenByRelay, depthByRelay });
    const changedParents = survivors.reduce((count, relayId) => {
        if (relayId === rootRelayId) return count + Number(relayId !== original.rootRelayId);
        return count + Number(originalParents.get(relayId) !== repairedParents.get(relayId));
    }, 0);
    return {
        overlay: { rootRelayId, childrenByRelay, depthByRelay },
        changedParents,
        removedRelays: Object.keys(original.depthByRelay).filter((relayId) => !survivors.includes(relayId)).length
    };
}
