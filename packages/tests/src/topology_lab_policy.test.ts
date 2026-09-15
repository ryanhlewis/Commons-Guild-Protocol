import { describe, expect, it } from "vitest";
import { CompactingQueue } from "../../../loadnet/topology/compacting-queue.ts";
import { RealtimeForwardQueue } from "../../../loadnet/topology/realtime-forward-queue.ts";
import { RealtimePacketWindow } from "../../../loadnet/topology/realtime-packet-window.ts";
import { federationCopiesForEpoch, recoveryRedundancyRatio } from "../../../loadnet/topology/recovery-redundancy.ts";
import { AccrualFailureDetector } from "../../../loadnet/topology/failure-detector.ts";
import { shouldInjectSyntheticBaselineDrop } from "../../../loadnet/topology/failure-injection.ts";
import {
    buildGameRoutePlans,
    planAuthorityHandoff,
    planAuthorityLeaseHandoff,
    relayForShard,
    spatialSubscriptions
} from "../../../loadnet/topology/game-sharding.ts";
import { adaptiveRecoveryPolicy, evaluateGameAuthority, gameTransportLane, selectGameNetcode } from "../../../loadnet/topology/game-networking.ts";
import { buildMediaRoutePlans, routeCost } from "../../../loadnet/topology/media-topology.ts";
import {
    compileMediaFastPath,
    prewarmedAudioPublications,
    selectMediaRoomMode,
    selectMediaRelayOverlay,
    selectReceiverAudioSources,
    selectVideoLayer
} from "../../../loadnet/topology/media-session.ts";
import { decodeTopologyPacket, deterministicRtpPayload, encodeTopologyPacket, verifyDeterministicRtpPayload } from "../../../loadnet/topology/packet-wire.ts";
import { assignParticipantsToRelays, evaluateRelay, selectRelayCandidates } from "../../../loadnet/topology/relay-capabilities.ts";
import { buildDemandDrivenRelayTree, relayTreeStats } from "../../../loadnet/topology/relay-overlay.ts";
import { selectRoomRelayCohort } from "../../../loadnet/topology/room-relay-cohort.ts";
import { repairRelayTree } from "../../../loadnet/topology/resilient-overlay.ts";
import {
    assignmentMovement,
    buildResilientRelayAssignments,
    remapRelayAssignments
} from "../../../loadnet/topology/resilient-placement.ts";
import { buildLabRelayCapabilities, buildParticipants, buildTopologyLabScenario } from "../../../loadnet/topology/scenario.ts";
import {
    acceptsRouteEpoch,
    advanceRelayMigration,
    beginRelayMigration,
    migrationTargets
} from "../../../loadnet/topology/topology-recovery.ts";
import type { ParticipantLocation, RelayCapabilities } from "../../../loadnet/topology/types.ts";

describe("topology lab relay capabilities", () => {
    const participant: ParticipantLocation = { participantId: 0, region: "region-0", x: 0, y: 0 };

    it("rejects user relays that cannot safely provide the requested SFU role", () => {
        const relay: RelayCapabilities = buildLabRelayCapabilities(1)[0];
        const invalid = {
            ...relay,
            udp: false,
            federation: false,
            opaqueE2eeForwarding: false
        };
        const evaluation = evaluateRelay(invalid, participant, {
            protocolVersion: "hollow-media/1",
            requireSfu: true,
            requireFederation: true,
            requireOpaqueE2ee: true
        });
        expect(evaluation.eligible).toBe(false);
        expect(evaluation.rejectionReasons).toEqual(expect.arrayContaining([
            "udp-unavailable",
            "federation-unavailable",
            "opaque-e2ee-unavailable"
        ]));
    });

    it("does not confuse an opaque user relay with trusted game authority", () => {
        const relay = buildLabRelayCapabilities(1)[0];
        expect(evaluateGameAuthority(relay, { competitive: true })).toEqual({ eligible: false, reason: "forward-only" });
        const managed = { ...relay, operator: "managed" as const, authorityTrust: "managed" as const };
        expect(evaluateGameAuthority(managed, { competitive: true })).toEqual({ eligible: true, reason: "trusted-authority" });
    });

    it("prefers a healthy low-latency user relay while preserving backups", () => {
        const relays = buildLabRelayCapabilities(4);
        relays[0].rttMs = 8;
        relays[1].rttMs = 30;
        relays[2].healthy = false;
        const selected = selectRelayCandidates(relays, participant, {
            protocolVersion: "hollow-media/1",
            requireSfu: true,
            requireFederation: true,
            requireOpaqueE2ee: true
        }, 2);
        expect(selected).toHaveLength(2);
        expect(selected[0].relay.relayId).toBe("media-relay-0");
        expect(selected.every((entry) => entry.relay.healthy)).toBe(true);
    });

    it("keeps excess regional relays warm until active capacity requires them", () => {
        const relays = buildLabRelayCapabilities(8);
        const smallAssignments = assignParticipantsToRelays(buildParticipants(1000, 1000), relays, {
            protocolVersion: "hollow-media/1",
            requireSfu: true,
            requireFederation: true,
            requireOpaqueE2ee: true
        });
        expect(new Set(smallAssignments).size).toBe(4);

        const constrained = buildLabRelayCapabilities(8).map((relay) => ({
            ...relay,
            capacity: { ...relay.capacity, maxParticipants: 160 }
        }));
        const largeAssignments = assignParticipantsToRelays(buildParticipants(1000, 1000), constrained, {
            protocolVersion: "hollow-media/1",
            requireSfu: true,
            requireFederation: true,
            requireOpaqueE2ee: true
        });
        expect(new Set(largeAssignments).size).toBe(8);
    });
});

describe("failure-aware topology control", () => {
    it("keeps synthetic baseline drops out of the resilient hard-failure path", () => {
        expect(shouldInjectSyntheticBaselineDrop("federated-sfu", 2, "relay-a", "relay-a")).toBe(true);
        expect(shouldInjectSyntheticBaselineDrop("resilient-sfu", 2, "relay-a", "relay-a")).toBe(false);
        expect(shouldInjectSyntheticBaselineDrop("federated-sfu", 3, "relay-a", "relay-a")).toBe(false);
    });

    it("uses adaptive suspicion before declaring a relay dead", () => {
        const detector = new AccrualFailureDetector({
            initialIntervalMs: 100,
            minimumStdDeviationMs: 10,
            acceptablePauseMs: 0,
            hardLeaseMs: 1_000
        });
        for (let now = 0; now <= 600; now += 100) detector.heartbeat("relay-a", now);
        expect(detector.assess("relay-a", 700).state).toBe("alive");
        expect(detector.assess("relay-a", 750).state).toBe("suspect");
        expect(detector.assess("relay-a", 1_600).state).toBe("dead");
        detector.heartbeat("relay-a", 1_610);
        expect(detector.assess("relay-a", 1_610).state).toBe("alive");
        detector.markDraining("relay-a");
        expect(detector.assess("relay-a", 1_620).state).toBe("draining");
    });

    it("keeps unaffected participants fixed when a relay disappears", () => {
        const participants = buildParticipants(1_000, 1_000);
        const relays = buildLabRelayCapabilities(8, 160);
        const plan = buildResilientRelayAssignments(participants, relays, {
            protocolVersion: "hollow-media/1",
            requireSfu: true,
            requireFederation: true,
            requireOpaqueE2ee: true
        }, 3);
        const failedRelay = plan.primaryByParticipant[0];
        const remapped = remapRelayAssignments(plan.candidatesByParticipant, new Set([failedRelay]));
        const movement = assignmentMovement(plan.primaryByParticipant, remapped.assignments);
        const originallyAffected = plan.primaryByParticipant.filter((relayId) => relayId === failedRelay).length;
        expect(remapped.changedParticipants).toBe(originallyAffected);
        expect(movement.changed).toBe(originallyAffected);
        expect(remapped.assignments).not.toContain(failedRelay);
    });

    it("reserves enough participant capacity for hard relay loss", () => {
        const participants = buildParticipants(1_000, 1_000);
        const relays = buildLabRelayCapabilities(8, 200);
        const plan = buildResilientRelayAssignments(participants, relays, {
            protocolVersion: "hollow-media/1",
            requireSfu: true,
            requireFederation: true,
            requireOpaqueE2ee: true
        }, 3);
        const failed = new Set([...new Set(plan.primaryByParticipant)].slice(0, 2));
        const capacity = new Map(relays.map((relay) => [relay.relayId, relay.capacity.maxParticipants]));
        const remapped = remapRelayAssignments(plan.candidatesByParticipant, failed, capacity);
        expect(Math.max(...remapped.loads.values())).toBeLessThanOrEqual(200);
        expect(plan.primaryByParticipant.every((relayId, participantId) =>
            failed.has(relayId) || remapped.assignments[participantId] === relayId
        )).toBe(true);

        const insufficient = new Map(relays.map((relay) => [relay.relayId, 160]));
        expect(() => remapRelayAssignments(plan.candidatesByParticipant, failed, insufficient)).toThrow(
            /No surviving relay capacity/
        );
    });

    it("keeps excess relays warm instead of adding them to every room tree", () => {
        const relays = buildLabRelayCapabilities(12, 250);
        const cohort = selectRoomRelayCohort(relays, 1_000, {
            failureReserve: 2,
            targetUtilization: 0.8
        });
        expect(cohort.active).toHaveLength(7);
        expect(cohort.standby).toHaveLength(5);
        expect(cohort.worstCaseSurvivorCapacity * cohort.targetUtilization).toBeGreaterThanOrEqual(1_000);
        expect(() => selectRoomRelayCohort(buildLabRelayCapabilities(8, 160), 1_000, {
            failureReserve: 2,
            targetUtilization: 0.85
        })).toThrow(/Insufficient relay capacity/);
    });

    it("keeps enough active relays for repair federation after failures", () => {
        const cohort = selectRoomRelayCohort(buildLabRelayCapabilities(6, 1_000), 256, {
            failureReserve: 2,
            minimumSurvivors: 2,
            targetUtilization: 0.85
        });
        expect(cohort.active).toHaveLength(4);
        expect(cohort.standby).toHaveLength(2);
    });

    it("repairs only broken relay-tree branches within bounded fanout", () => {
        const relays = buildLabRelayCapabilities(12);
        const primary = buildDemandDrivenRelayTree(
            relays[0].relayId,
            relays.slice(1).map((relay) => relay.relayId),
            relays,
            { maxChildren: 3, maxDepth: 6 }
        );
        const failedRelay = Object.entries(primary.childrenByRelay).find(([, children]) => children.length > 0)?.[0] ?? relays[1].relayId;
        const repaired = repairRelayTree(primary, new Set([failedRelay]), relays, { maxChildren: 3, maxDepth: 6 });
        const stats = relayTreeStats(repaired.overlay);
        expect(stats.relays).toBe(11);
        expect(stats.maxFanout).toBeLessThanOrEqual(3);
        expect(stats.maxDepth).toBeLessThanOrEqual(6);
        expect(repaired.changedParents).toBeLessThan(11);
    });

    it("dual-publishes planned moves and immediately fences hard failures", () => {
        const planned = beginRelayMigration({
            streamId: "audio:7",
            currentEpoch: 4,
            fromRelayId: "r1",
            toRelayId: "r2",
            now: 1_000,
            dualPublishMs: 200
        });
        expect(migrationTargets(planned, 1_100)).toEqual(["r1", "r2"]);
        expect(migrationTargets(advanceRelayMigration(planned, 1_250), 1_250)).toEqual(["r2"]);
        const failed = beginRelayMigration({
            streamId: "audio:7",
            currentEpoch: planned.epoch,
            fromRelayId: "r2",
            toRelayId: "r3",
            now: 1_300,
            hardFailure: true
        });
        expect(migrationTargets(failed, 1_300)).toEqual(["r3"]);
        expect(acceptsRouteEpoch(failed.epoch, failed.epoch - 1)).toBe(false);
    });
});

describe("media topology policy", () => {
    it("changes publisher cost from recipient-linear mesh to one upload per encoded frame", () => {
        const participants = 1000;
        const relayIds = ["relay-0", "relay-1", "relay-2", "relay-3"];
        const audioSources = Array.from({ length: 8 }, (_, index) => index);
        const videoSources = Array.from({ length: 8 }, (_, index) => index);
        const plans = buildMediaRoutePlans({
            participantCount: participants,
            relayIds,
            homeRelays: Array.from({ length: participants }, (_, index) => relayIds[index % relayIds.length]),
            audioSources,
            videoSources,
            audioTopK: 4,
            videoTiles: 4
        });
        const streams = [
            ...audioSources.map((sourceId) => ({ kind: "audio" as const, sourceId, frames: 10 })),
            ...videoSources.map((sourceId) => ({ kind: "video" as const, sourceId, frames: 5 }))
        ];
        const mesh = routeCost(plans["turn-mesh"], streams);
        const single = routeCost(plans["single-sfu"], streams);
        const federated = routeCost(plans["federated-sfu"], streams);
        expect(mesh.sourceUploads).toBe(mesh.clientDeliveries);
        expect(single.sourceUploads).toBe(120);
        expect(federated.sourceUploads).toBe(120);
        expect(federated.sourceUploads).toBeLessThan(mesh.sourceUploads / 100);
        expect(federated.federationCopies).toBeLessThanOrEqual(120 * (relayIds.length - 1));
        expect(federated.clientDeliveries).toBe(mesh.clientDeliveries);
        const cascaded = routeCost(plans["cascaded-sfu"], streams);
        expect(cascaded.sourceUploads).toBe(federated.sourceUploads);
        expect(cascaded.clientDeliveries).toBe(federated.clientDeliveries);
        expect(cascaded.federationCopies).toBe(federated.federationCopies);
    });

    it("builds a bounded demand-only cascade instead of source-relay fanout", () => {
        const relays = buildLabRelayCapabilities(12);
        const overlay = buildDemandDrivenRelayTree(
            relays[0].relayId,
            relays.slice(1).map((relay) => relay.relayId),
            relays,
            { maxChildren: 3, maxDepth: 5 }
        );
        const stats = relayTreeStats(overlay);
        expect(stats).toMatchObject({ relays: 12, edges: 11 });
        expect(stats.maxFanout).toBeLessThanOrEqual(3);
        expect(stats.maxDepth).toBeLessThanOrEqual(5);
        expect(overlay.childrenByRelay[relays[0].relayId].length).toBeLessThan(11);
    });
});

describe("media session policy", () => {
    it("uses direct, interactive SFU, and staged tree modes at distinct scales", () => {
        expect(selectMediaRoomMode({ participants: 2, simultaneousPublishers: 2, expectedAudience: 2, endToEndEncryption: false })).toBe("direct");
        expect(selectMediaRoomMode({ participants: 24, simultaneousPublishers: 8, expectedAudience: 24, endToEndEncryption: true })).toBe("interactive-sfu");
        expect(selectMediaRoomMode({ participants: 1000, simultaneousPublishers: 16, expectedAudience: 1000, endToEndEncryption: true })).toBe("stage-tree");
    });

    it("adds cascade hops only when direct source fanout exceeds its budget", () => {
        expect(selectMediaRelayOverlay({
            demandRelayCount: 4,
            maxSourceFanout: 4,
            estimatedExtraHopMs: 25,
            latencyBudgetMs: 150
        })).toBe("direct-federation");
        expect(selectMediaRelayOverlay({
            demandRelayCount: 12,
            maxSourceFanout: 4,
            estimatedExtraHopMs: 25,
            latencyBudgetMs: 150
        })).toBe("cascade");
    });

    it("selects loud active speakers while retaining muted DTX publications", () => {
        const sources = [
            { participantId: 1, score: 0.9 },
            { participantId: 2, score: 0.7, muted: true },
            { participantId: 3, score: 0.8 },
            { participantId: 4, score: 0.6 }
        ];
        expect(selectReceiverAudioSources(sources, {
            receiverId: 9,
            mode: "exclude",
            participantIds: [3],
            maxActiveSpeakers: 2
        })).toEqual([1, 4]);
        expect(prewarmedAudioPublications(sources)).toContainEqual({ participantId: 2, state: "dtx-warm" });
    });

    it("chooses an SVC layer from receiver viewport and bandwidth", () => {
        const layers = [
            { id: "low", width: 320, height: 180, maxBitrateKbps: 180, spatialLayer: 0, temporalLayer: 1 },
            { id: "mid", width: 640, height: 360, maxBitrateKbps: 600, spatialLayer: 1, temporalLayer: 2 },
            { id: "high", width: 1280, height: 720, maxBitrateKbps: 1800, spatialLayer: 2, temporalLayer: 2 }
        ];
        expect(selectVideoLayer(layers, {
            visible: true,
            viewportWidth: 640,
            viewportHeight: 360,
            availableBitrateKbps: 900,
            priority: 1
        })?.id).toBe("mid");
        expect(selectVideoLayer(layers, {
            visible: false,
            viewportWidth: 1280,
            viewportHeight: 720,
            availableBitrateKbps: 3000,
            priority: 1
        })).toBeUndefined();
    });

    it("compiles immutable deduplicated fast-path route snapshots", () => {
        const snapshot = compileMediaFastPath(7, [{
            streamId: "audio:1",
            nextRelayIds: ["r2", "r2", "r3"],
            localRecipientIds: [4, 4, 5]
        }]);
        expect(snapshot.routes["audio:1"].nextRelayIds).toEqual(["r2", "r3"]);
        expect(snapshot.routes["audio:1"].localRecipientIds).toEqual([4, 5]);
        expect(Object.isFrozen(snapshot.routes)).toBe(true);
    });
});

describe("spatial multiplayer policy", () => {
    it("limits updates to area-of-interest recipients instead of the whole room", () => {
        const participants = buildParticipants(1000, 1000);
        const relayIds = ["relay-0", "relay-1", "relay-2", "relay-3"];
        const plans = buildGameRoutePlans({ participants, relayIds, cellSize: 100, interestRadius: 80 });
        const hostRecipients = Object.values(plans["host-star-game"].subscriptions).reduce((sum, recipients) => sum + recipients.length, 0);
        const spatialRecipients = Object.values(plans["spatial-sharded-game"].subscriptions).reduce((sum, recipients) => sum + recipients.length, 0);
        expect(hostRecipients).toBe(999000);
        expect(spatialRecipients).toBeGreaterThan(0);
        expect(spatialRecipients).toBeLessThan(hostRecipients / 20);
    });

    it("uses prepare, dual-publish, and commit when authority changes", () => {
        const participant = { participantId: 7, region: "region-0", x: 99, y: 50 };
        let handoff = planAuthorityHandoff(participant, { x: 101, y: 50 }, ["relay-0", "relay-1", "relay-2", "relay-3"], 100);
        if (!handoff) {
            handoff = planAuthorityHandoff(participant, { x: 201, y: 50 }, ["relay-0", "relay-1", "relay-2", "relay-3"], 100);
        }
        expect(handoff).toBeDefined();
        expect(handoff?.fromRelay).not.toBe(handoff?.toRelay);
        expect(handoff?.phases).toEqual(["prepare", "dual-publish", "commit"]);
    });

    it("does not include the source in its own interest subscription", () => {
        const subscriptions = spatialSubscriptions(buildParticipants(64, 100), 50);
        for (const [key, recipients] of Object.entries(subscriptions)) {
            const source = Number(key.split(":")[1]);
            expect(recipients).not.toContain(source);
        }
    });

    it("enforces a nearest-first replication budget", () => {
        const participants = [
            { participantId: 0, region: "r", x: 0, y: 0 },
            { participantId: 1, region: "r", x: 1, y: 0 },
            { participantId: 2, region: "r", x: 2, y: 0 },
            { participantId: 3, region: "r", x: 3, y: 0 }
        ];
        expect(spatialSubscriptions(participants, 100, 2)["game:0"]).toEqual([1, 2]);
    });

    it("keeps most shard ownership stable when a relay is added", () => {
        const before = ["r0", "r1", "r2", "r3"];
        const after = [...before, "r4"];
        const shards = Array.from({ length: 2000 }, (_, index) => `${index % 100}:${Math.floor(index / 100)}`);
        const changed = shards.filter((shard) => relayForShard(shard, before) !== relayForShard(shard, after));
        expect(changed.length / shards.length).toBeGreaterThan(0.1);
        expect(changed.length / shards.length).toBeLessThan(0.35);
    });

    it("uses hysteresis and monotonic epochs for authority handoff", () => {
        const participant = { participantId: 7, region: "region-0", x: 95, y: 50 };
        const relays = ["relay-0", "relay-1", "relay-2", "relay-3"];
        expect(planAuthorityLeaseHandoff(participant, { x: 101, y: 50 }, relays, 100, 4, 1000)).toBeUndefined();
        const handoff = planAuthorityLeaseHandoff(participant, { x: 125, y: 50 }, relays, 100, 4, 1000);
        if (relayForShard("0:0", relays) !== relayForShard("1:0", relays)) {
            expect(handoff).toMatchObject({ previousEpoch: 4, nextEpoch: 5, leaseExpiresAt: 11000 });
        }
    });
});

describe("game netcode and transport policy", () => {
    it("selects bounded rollback only where the simulation supports it", () => {
        expect(selectGameNetcode({
            maxPlayers: 8,
            deterministicSimulation: true,
            supportsRollback: true,
            competitive: true,
            persistentWorld: false,
            tickRate: 60
        }).mode).toBe("deterministic-rollback");
        expect(selectGameNetcode({
            maxPlayers: 1000,
            deterministicSimulation: false,
            supportsRollback: false,
            competitive: false,
            persistentWorld: true,
            tickRate: 30
        })).toMatchObject({ mode: "authoritative-snapshot", areaOfInterest: true, rollbackFrames: 0 });
    });

    it("separates latency-sensitive datagrams from reliable bulk traffic", () => {
        expect(gameTransportLane("input")).toMatchObject({ lane: 0, reliability: "unreliable", inputRedundancy: 3 });
        expect(gameTransportLane("critical-event")).toMatchObject({ lane: 2, reliability: "reliable", ordered: true });
        expect(gameTransportLane("bulk-state").lane).not.toBe(gameTransportLane("critical-event").lane);
        expect(gameTransportLane("bulk-state").priority).toBeLessThan(gameTransportLane("snapshot").priority);
    });

    it("scales deadline-aware recovery with loss and frame size", () => {
        const small = adaptiveRecoveryPolicy({ kind: "video", payloadBytes: 900, lossRate: 0.05, rttMs: 80, deadlineMs: 50 });
        const large = adaptiveRecoveryPolicy({ kind: "video", payloadBytes: 9900, lossRate: 0.05, rttMs: 20, deadlineMs: 100 });
        expect(small).toMatchObject({ fecParityPackets: 1, retransmit: false });
        expect(large.retransmit).toBe(true);
        expect(small.fecParityPackets / 1).toBeGreaterThan(large.fecParityPackets / 9);
    });
});

describe("topology lab packet wire", () => {
    it("purges superseded session generations and route epochs without FIFO blocking", () => {
        const queue = new RealtimeForwardQueue<string>();
        for (let index = 0; index < 20_000; index += 1) {
            queue.enqueue(`old-${index}`, { scopeId: "room", generation: 1, epoch: 1 });
        }
        const generationAdvance = queue.enqueue("current", { scopeId: "room", generation: 2, epoch: 1 });
        expect(generationAdvance.droppedSuperseded).toBe(20_000);
        expect(queue.dequeue()).toBe("current");
        queue.enqueue("route-1", { scopeId: "room", generation: 2, epoch: 1 });
        const epochAdvance = queue.enqueue("route-2", { scopeId: "room", generation: 2, epoch: 2 });
        expect(epochAdvance.droppedSuperseded).toBe(1);
        expect(queue.dequeue()).toBe("route-2");
        expect(queue.enqueue("late", { scopeId: "room", generation: 1, epoch: 9 }).accepted).toBe(false);
    });

    it("drains large relay fanout queues in FIFO order without shifting the backing array", () => {
        const queue = new CompactingQueue<number>();
        for (let value = 0; value < 20_000; value += 1) queue.enqueue(value);
        for (let value = 0; value < 12_000; value += 1) expect(queue.dequeue()).toBe(value);
        for (let value = 20_000; value < 25_000; value += 1) queue.enqueue(value);
        expect(queue.length).toBe(13_000);
        for (let value = 12_000; value < 25_000; value += 1) expect(queue.dequeue()).toBe(value);
        expect(queue.length).toBe(0);
        expect(queue.dequeue()).toBeUndefined();
    });

    it("bounds repair-epoch redundancy and deduplicates before client fanout", () => {
        expect(federationCopiesForEpoch("resilient-sfu", 1, { routeEpoch: 2, recoveryCopies: 2 })).toBe(1);
        expect(federationCopiesForEpoch("resilient-sfu", 2, { routeEpoch: 2, recoveryCopies: 2 })).toBe(2);
        expect(federationCopiesForEpoch("federated-sfu", 2, { routeEpoch: 2, recoveryCopies: 2 })).toBe(1);
        expect(recoveryRedundancyRatio(100, 120)).toBeCloseTo(0.2);

        const window = new RealtimePacketWindow(2);
        expect(window.accept("audio:1:1")).toBe(true);
        expect(window.accept("audio:1:1")).toBe(false);
        expect(window.accept("audio:1:2")).toBe(true);
        expect(window.accept("audio:1:3")).toBe(true);
        expect(window.size).toBe(2);
        expect(window.accept("audio:1:1")).toBe(true);
    });

    it("preserves an opaque deterministic RTP payload exactly", () => {
        const payload = deterministicRtpPayload("run-1", "audio", 12, 44, 320);
        const encoded = encodeTopologyPacket({
            type: "upload",
            topology: "federated-sfu",
            streamKind: "audio",
            epoch: 3,
            sourceId: 12,
            sequence: 44,
            recipientId: 9,
            originRelayIndex: 2,
            sentAt: 123456789,
            payload
        });
        const decoded = decodeTopologyPacket(encoded);
        expect(decoded.payload.equals(payload)).toBe(true);
        expect(decoded).toMatchObject({
            type: "upload",
            topology: "federated-sfu",
            streamKind: "audio",
            epoch: 3,
            sourceId: 12,
            sequence: 44,
            recipientId: 9,
            originRelayIndex: 2,
            sentAt: 123456789
        });
        expect(verifyDeterministicRtpPayload("run-1", "audio", 12, 44, decoded.payload)).toBe(true);
    });

    it("builds replaceable media and game plans in one versioned scenario", () => {
        const scenario = buildTopologyLabScenario({
            runId: "scenario-test",
            users: 128,
            relays: 4,
            audioSources: 8,
            videoSources: 8,
            gameSources: 8,
            audioTopK: 4,
            videoTiles: 4,
            audioFrames: 4,
            videoFrames: 2,
            gameFrames: 1,
            worldSize: 1000,
            cellSize: 100,
            interestRadius: 80
        });
        expect(scenario.version).toBe(3);
        expect(scenario.architecture).toMatchObject({
            mediaMode: "stage-tree",
            mediaOverlay: "direct-federation",
            gameMode: "authoritative-snapshot",
            opaqueUserRelays: true
        });
        expect(scenario.relayIds).toHaveLength(4);
        expect(Object.keys(scenario.topologies)).toEqual(expect.arrayContaining([
            "turn-mesh",
            "single-sfu",
            "federated-sfu",
            "cascaded-sfu",
            "resilient-sfu",
            "host-star-game",
            "spatial-sharded-game"
        ]));
        expect(scenario.mediaSources.game).toHaveLength(8);
        expect(Object.keys(scenario.topologies["resilient-sfu"].subscriptions)).toEqual(
            expect.arrayContaining(["audio:0", "video:0", "game:0"])
        );
    });
});
