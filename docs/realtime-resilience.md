# Realtime Resilience Architecture

## Status

CGP now has a replaceable topology and failure-control layer plus a Docker data-plane lab. It validates relay selection, bounded forwarding, hard-failure repair, client churn, payload integrity, and SLO accounting. It does not turn independent LiveKit deployments into a federated production SFU. Hollow must still connect this policy to an SFU implementation that supports cross-node media piping or cascading.

The implementation is intentionally split across small modules under `loadnet/topology/`:

- `room-relay-cohort.ts`: selects the smallest failure-safe active cohort
- `resilient-placement.ts`: weighted rendezvous candidates and stable capacity-aware remapping
- `failure-detector.ts`: accrual suspicion, leases, draining, and explicit leave state
- `resilient-overlay.ts`: bounded repair of only failed tree branches
- `topology-recovery.ts`: planned dual-publish and hard-failure route epochs
- `recovery-redundancy.ts`: bounded repair-epoch packet protection
- `realtime-forward-queue.ts`: O(1) generation and route-epoch fencing
- `realtime-packet-window.ts`: bounded deduplication before fanout
- `resilient-media.ts`: shared audio/video/game route assembly

## Topology

1. Keep direct peer media for very small calls when policy and E2EE permit it.
2. Assign every participant an ordered primary plus backups using weighted rendezvous over capability, locality, headroom, and load.
3. Activate the smallest relay cohort satisfying:

   `target utilization * capacity after the worst F relay losses >= room participants`

4. For at most four demanded destination relays, send one copy directly from the source relay to each destination relay.
5. Above that source-fanout budget, use a demand-only tree with maximum fanout four. A source packet crosses relay boundaries once per demanded relay, not once per participant.
6. Select audio speakers and SVC video layers in the media data plane. Route game state by spatial area of interest. Audio, video, and game may share one transport/session while retaining independent priority and reliability policy.
7. Leave compatible excess relays warm for failover or assign them to other rooms. Adding every available relay to every room increases federation cost and failure surface.

For `N` receivers, `S` selected sources, and `R` active room relays, client delivery remains proportional to selected subscriptions. Direct SFU federation adds roughly `S * (R - 1)` inter-relay copies. A bounded tree keeps source-relay fanout constant but still has `R - 1` total tree edges. This is why the active cohort has a measurable optimum.

## Failure Path

- Planned drain: mark the relay draining, stop new placements, preconnect the target, dual-publish briefly, commit a new route epoch, then close the old path.
- Abrupt relay loss: accrual suspicion and a hard lease fence the failed relay; only participants whose primary disappeared select their next capacity-safe candidate.
- Repair epoch: duplicate the bounded number of publisher-edge and SFU-to-SFU packets, deduplicate before client fanout, then return to normal media protection. Production WebRTC should map this concept to Opus RED/in-band FEC, NACK/RTX, and transport-appropriate game reliability instead of blindly duplicating every video packet.
- Client loss: remove subscriptions and authority leases, suppress queued delivery, and rotate group key epochs when membership confidentiality requires it.
- Every queued forward carries a session generation and route epoch. Old work is discarded before it can block or leak into the repaired route.

Failure detection follows the separation used by SWIM, Lifeguard, and phi-accrual detectors: suspicion is adaptive, explicit graceful leave is distinct from failure, and local health degradation must not falsely evict healthy peers. Route repair is deterministic so control-plane convergence cannot create two long-lived forwarding graphs.

## Research Alignment

- [AsTree (NSDI 2025)](https://www.usenix.org/conference/nsdi25/presentation/meng) moves large-conference speaker selection into a cascading media plane and reports lower audio/video stalls without an all-pairs control bottleneck.
- [Scallop (2025)](https://www.cs.princeton.edu/~jrex/papers/sfu25.pdf) separates the fast packet path from slower conference control and demonstrates the scale available from a compact forwarding data plane. Hollow keeps this separation even though its current lab is software.
- [mediasoup scalability](https://mediasoup.org/documentation/v3/scalability/) uses multiple single-core routers and `pipeToRouter`-style inter-router media piping rather than one global process.
- [LiveKit distributed deployment](https://docs.livekit.io/transport/self-hosting/distributed/) uses load-aware, region-aware room placement and graceful node draining, but a self-hosted room remains on one node; abrupt node loss is therefore still a production gap for a federated Hollow room.
- [SFrame, RFC 9605](https://www.rfc-editor.org/rfc/rfc9605) and [MLS, RFC 9420](https://www.rfc-editor.org/rfc/rfc9420/) provide the standards basis for opaque media forwarding and group key evolution.
- [WebRTC SVC](https://www.w3.org/TR/webrtc-svc/) provides receiver-specific scalable video selection.
- [ICE restart, RFC 8839](https://www.rfc-editor.org/rfc/rfc8839.html) is the client transport recovery mechanism after path failure.

This is SOTA-aligned architecture, not evidence that Hollow globally outperforms every production SFU. That claim requires comparable multi-region hardware, codec, bitrate, and workload benchmarks against named systems.

## Measured Local Evidence

The capacity-safe release profile in `topology-lab-summary-1784103385659.json` ran 1,000 participants across eight 200-participant relays with an 85% target utilization, 40ms latency, 10ms jitter, and 1% packet loss. The controller hard-killed two relay containers and two multiplexed client-sink containers (125 departed participants).

- 245ms measured recovery
- 98.34% survivor delivery in the failover epoch and 99.02% after client churn
- 289ms recovered p95 latency
- zero corrupt, unexpected, stale-epoch, or duplicate client deliveries
- six surviving active relays, maximum load 172, minimum headroom 28
- three deterministic relay candidates, maximum overlay fanout four, maximum depth two

The all-pattern regression in `topology-lab-summary-1784104251094.json` ran 256 participants and passed TURN mesh, single SFU, direct federation, bounded cascade, host-star game, spatial game shards, and the resilient shared path at 100% delivery. After killing two of four relays and one client sink, the resilient path recovered in 240ms with a 93ms recovered p95 and zero stale or corrupt delivery.

These are reproducible local Docker results on one host. They prove the policy, failure machinery, byte integrity, and SLO gates under deterministic impairment; they do not substitute for multi-region browser, codec-quality, capacity, or cost comparisons.

## Production Gates

1. Implement actual inter-SFU RTP forwarding/cascading in the selected production SFU, preserving dependency descriptors, RTP sequence state, RTCP feedback, and E2EE metadata.
2. Connect relay catalog membership, health, capacity, and failure domains to the cohort and placement modules.
3. Extend Hollow's implemented terminal endpoint replacement into a consensus-backed federated room route so partitions cannot split a room and planned drain can dual-publish without exposing duplicate publications.
4. Replace lab recovery copies with codec- and transport-specific RED/FEC/RTX policy and congestion control.
5. Run real browser publishers/receivers across multiple hosts and regions, including TURN-only clients, VPNs, NAT rebinding, relay drains, abrupt process loss, host loss, and correlated region loss.
6. Gate release on recovered media quality, not packet delivery alone: audio concealment/ASR, freeze duration, keyframe recovery, lip sync, game correction rate, CPU, bandwidth, and cost.
