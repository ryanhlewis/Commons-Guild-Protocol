export type MigrationPhase = "prepare" | "dual-publish" | "committed";

export interface RelayMigration {
    streamId: string;
    epoch: number;
    fromRelayId: string;
    toRelayId: string;
    startedAt: number;
    dualPublishUntil: number;
    phase: MigrationPhase;
    hardFailure: boolean;
}

export function beginRelayMigration(input: {
    streamId: string;
    currentEpoch: number;
    fromRelayId: string;
    toRelayId: string;
    now: number;
    dualPublishMs?: number;
    hardFailure?: boolean;
}): RelayMigration {
    if (!input.toRelayId || input.toRelayId === input.fromRelayId) throw new Error("Migration requires a distinct target relay");
    const hardFailure = input.hardFailure ?? false;
    const dualPublishMs = hardFailure ? 0 : Math.max(0, input.dualPublishMs ?? 250);
    return {
        streamId: input.streamId,
        epoch: input.currentEpoch + 1,
        fromRelayId: input.fromRelayId,
        toRelayId: input.toRelayId,
        startedAt: input.now,
        dualPublishUntil: input.now + dualPublishMs,
        phase: hardFailure ? "committed" : dualPublishMs > 0 ? "dual-publish" : "committed",
        hardFailure
    };
}

export function migrationTargets(migration: RelayMigration, now: number) {
    if (migration.phase === "committed" || now >= migration.dualPublishUntil) return [migration.toRelayId];
    return [migration.fromRelayId, migration.toRelayId];
}

export function advanceRelayMigration(migration: RelayMigration, now: number): RelayMigration {
    if (migration.phase === "committed" || now < migration.dualPublishUntil) return migration;
    return { ...migration, phase: "committed" };
}

export function acceptsRouteEpoch(currentEpoch: number, packetEpoch: number) {
    return packetEpoch >= currentEpoch;
}
