import type { RelayCapabilities } from "./types.ts";

export type GameNetcodeMode = "deterministic-rollback" | "authoritative-rollback" | "authoritative-snapshot";
export type GameMessageKind = "input" | "snapshot" | "critical-event" | "bulk-state";

export interface GameSessionProfile {
    maxPlayers: number;
    deterministicSimulation: boolean;
    supportsRollback: boolean;
    competitive: boolean;
    persistentWorld: boolean;
    tickRate: number;
}

export interface GameNetcodePolicy {
    mode: GameNetcodeMode;
    rollbackFrames: number;
    clientPrediction: boolean;
    authoritativeSimulation: boolean;
    areaOfInterest: boolean;
}

export interface GameTransportLane {
    lane: number;
    reliability: "unreliable" | "reliable";
    ordered: boolean;
    priority: number;
    inputRedundancy: number;
}

export interface AuthorityDecision {
    eligible: boolean;
    reason: "trusted-authority" | "casual-player-host" | "forward-only" | "authority-unavailable";
}

export interface AdaptiveRecoveryInput {
    kind: "input" | "snapshot" | "audio" | "video";
    payloadBytes: number;
    lossRate: number;
    rttMs: number;
    deadlineMs: number;
}

export interface AdaptiveRecoveryPolicy {
    redundantCopies: number;
    fecParityPackets: number;
    retransmit: boolean;
}

export function selectGameNetcode(profile: GameSessionProfile): GameNetcodePolicy {
    if (profile.deterministicSimulation && profile.supportsRollback && profile.maxPlayers <= 16) {
        return {
            mode: "deterministic-rollback",
            rollbackFrames: Math.max(2, Math.min(12, Math.ceil(profile.tickRate * 0.15))),
            clientPrediction: true,
            authoritativeSimulation: profile.competitive,
            areaOfInterest: false
        };
    }
    if (!profile.persistentWorld && profile.maxPlayers <= 64) {
        return {
            mode: "authoritative-rollback",
            rollbackFrames: Math.max(2, Math.min(8, Math.ceil(profile.tickRate * 0.1))),
            clientPrediction: true,
            authoritativeSimulation: true,
            areaOfInterest: profile.maxPlayers > 24
        };
    }
    return {
        mode: "authoritative-snapshot",
        rollbackFrames: 0,
        clientPrediction: true,
        authoritativeSimulation: true,
        areaOfInterest: true
    };
}

export function gameTransportLane(kind: GameMessageKind): GameTransportLane {
    if (kind === "input") return { lane: 0, reliability: "unreliable", ordered: false, priority: 100, inputRedundancy: 3 };
    if (kind === "snapshot") return { lane: 1, reliability: "unreliable", ordered: false, priority: 80, inputRedundancy: 0 };
    if (kind === "critical-event") return { lane: 2, reliability: "reliable", ordered: true, priority: 60, inputRedundancy: 0 };
    return { lane: 3, reliability: "reliable", ordered: true, priority: 10, inputRedundancy: 0 };
}

export function evaluateGameAuthority(
    relay: RelayCapabilities,
    profile: Pick<GameSessionProfile, "competitive">,
    allowCasualPlayerHost = false
): AuthorityDecision {
    if (!relay.gameAuthority) return { eligible: false, reason: "authority-unavailable" };
    if (relay.authorityTrust === "managed" || relay.authorityTrust === "community") {
        return { eligible: true, reason: "trusted-authority" };
    }
    if (!profile.competitive && allowCasualPlayerHost) return { eligible: true, reason: "casual-player-host" };
    return { eligible: false, reason: "forward-only" };
}

export function adaptiveRecoveryPolicy(input: AdaptiveRecoveryInput): AdaptiveRecoveryPolicy {
    const loss = Math.max(0, Math.min(1, input.lossRate));
    const packets = Math.max(1, Math.ceil(input.payloadBytes / 1100));
    if (input.kind === "input") {
        return { redundantCopies: loss >= 0.08 ? 4 : loss >= 0.02 ? 3 : 2, fecParityPackets: 0, retransmit: false };
    }
    const retransmit = input.rttMs * 1.25 < input.deadlineMs;
    if (loss < 0.01) return { redundantCopies: 1, fecParityPackets: 0, retransmit };
    const smallPayloadBoost = packets <= 2 ? 1 : 0;
    const fecParityPackets = Math.min(packets, Math.max(1, Math.ceil(packets * loss * 1.5) + smallPayloadBoost));
    return { redundantCopies: 1, fecParityPackets, retransmit };
}
