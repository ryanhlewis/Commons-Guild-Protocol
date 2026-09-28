// DESIGN MODEL ONLY. No network, signatures, epoch migration or production integration.
export type Policy = {
    old: string[];
    next: string[];
    threshold: number;
};
function subsets<T>(items: T[]): T[][] {
    return Array.from({ length: 2 ** items.length }, (_, mask) => items.filter((_, index) => mask & (1 << index)));
}
function majority(voters: string[], members: string[], threshold: number) {
    return new Set(voters.filter(voter => members.includes(voter))).size >= threshold;
}
export function jointCertificate(policy: Policy, voters: string[], authorized: boolean, barrierReady: boolean) {
    return authorized && barrierReady && majority(voters, policy.old, policy.threshold) && majority(voters, policy.next, policy.threshold);
}
function castVote(node: string, transition: string, durable: Map<string, string>, volatile: Map<string, string>, persist = true) {
    const previous = volatile.get(node) ?? durable.get(node);
    if (previous && previous !== transition)
        return false;
    if (persist)
        durable.set(node, transition);
    volatile.set(node, transition);
    return true;
}
export function runEpochTransitionModel() {
    let explored = 0, certificates = 0;
    for (const policy of [
        { old: ['a', 'b', 'c'], next: ['b', 'c', 'd'], threshold: 2 },
        { old: ['a', 'b', 'c'], next: ['d', 'e', 'f'], threshold: 2 },
    ]) {
        const nodes = [...new Set([...policy.old, ...policy.next])];
        const possibleVotes = subsets(nodes);
        for (const firstVotes of possibleVotes) {
            for (const crashed of subsets(nodes)) {
                // Each acknowledged vote is persisted before response. Restart drops only volatile state.
                for (const competingRequests of possibleVotes) {
                    const durable = new Map<string, string>(), volatile = new Map<string, string>();
                    for (const node of firstVotes)
                        castVote(node, 'transition-one', durable, volatile);
                    for (const node of crashed)
                        volatile.delete(node);
                    explored++;
                    const secondVotes = competingRequests.filter(node => castVote(node, 'transition-two', durable, volatile));
                    const firstValid = jointCertificate(policy, firstVotes, true, true);
                    const secondValid = jointCertificate(policy, secondVotes, true, true);
                    if (firstValid && secondValid)
                        throw Error('Conflicting finalized transitions after restart');
                    if (!firstValid)
                        continue;
                    certificates++;
                    // Old signers durably retire before acknowledging the joint barrier.
                    for (const oldWriters of subsets(policy.old)) {
                        const eligible = oldWriters.filter(node => durable.get(node) !== 'transition-one');
                        if (majority(eligible, policy.old, policy.threshold))
                            throw Error('Retired epoch still has a writable majority');
                    }
                    if (jointCertificate(policy, firstVotes, false, true))
                        throw Error('Unauthenticated transition accepted');
                    if (jointCertificate(policy, firstVotes, true, false))
                        throw Error('Uncaught-up barrier accepted');
                }
            }
        }
    }
    const policy = { old: ['a', 'b', 'c'], next: ['d', 'e', 'f'], threshold: 2 };
    if (jointCertificate(policy, ['a', 'b'], true, true))
        throw Error('Old-only quorum activated new epoch');
    // Negative control: erasing persisted fences lets exactly the same voters certify a conflict.
    const voters = ['a', 'b', 'd', 'e'];
    const lostDurable = new Map<string, string>(), lostVolatile = new Map<string, string>();
    const unsafeFirst = voters.filter(node => castVote(node, 'one', lostDurable, lostVolatile, false));
    lostVolatile.clear(); // Crash loses acknowledged votes when no durable write happened.
    const unsafeSecond = voters.filter(node => castVote(node, 'two', lostDurable, lostVolatile, false));
    const unsafeCounterexample = jointCertificate(policy, unsafeFirst, true, true) && jointCertificate(policy, unsafeSecond, true, true);
    if (!unsafeCounterexample)
        throw Error('Negative control failed');
    return {
        ok: true, explored, certificates, unsafeFenceResetCounterexample: true,
        scope: 'bounded abstract design only; assumes authenticated votes, exact global barrier, honest crash-fault voters and durable retirement; no runtime migration',
        checks: ['joint old/new majorities', 'conflicting transitions excluded by durable per-voter fence', 'all crash subsets preserve fence', 'retired old epoch cannot form majority', 'authorization and catch-up barrier required', 'old-only activation rejected', 'erasing fences admits conflict: negative control'],
    };
}
