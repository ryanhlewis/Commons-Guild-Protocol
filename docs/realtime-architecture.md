# Realtime media and game architecture

Status: loadnet policy and packet-path prototype, July 2026.

This design keeps transport routing, media session policy, and game simulation policy separate. A relay can forward opaque packets without being trusted to decrypt media or decide authoritative game state.

## Quorum-certified SFU discovery

Large-room media discovery is anchored in the guild log with an
`SFU_AUTHORITY_SET` event. The guild owner signs the node IDs, continuity
cluster, authority/forward-only roles, route-signing keys, activation window,
and current CGP write-quorum policy. Relays reject the event unless that
policy exactly matches their configured write epoch.

After the write quorum votes, the relay attaches a
`cgp/write-certificate/1` proof to the event. The certificate commits to the
prior guild head and exact owner-signed proposal. Clients independently
verify the event ID, owner signature, majority policy, distinct relay votes,
and every vote signature before accepting an SFU descriptor.

Rotation uses monotonically increasing epochs. During `overlapUntil`, clients
may use both the previous and current authority sets; afterward the prior set
is rejected. Hollow relay discovery returns only the newest two proof events.
The media plane remains provider-neutral at the call boundary; the current
membership fields map to federated mediasoup node IDs and route keys.

## Media modes

| Mode | Intended room | Packet path |
| --- | --- | --- |
| `direct` | Up to four peers when E2EE policy permits exposing peer addresses | ICE-selected peer path, relay fallback |
| `interactive-sfu` | Small and medium many-to-many calls | One publisher upload, receiver-driven subscriptions, direct federation between demanded relays |
| `stage-tree` | Large rooms and events | One publisher upload, bounded demand-driven relay cascade, media-plane active-speaker selection |

Muted audio publications remain established with DTX. Unmute must not wait for signaling or overlay construction. Receivers select active speakers and visible video layers; a thousand participants do not imply a thousand decoded audio streams or video subscriptions.

The packet fast path consumes an immutable, epoch-versioned route snapshot. Membership, authorization, MLS epochs, relay selection, and statistics aggregation stay in the slow control path. The implementation is a software boundary inspired by [eXpressSFU](https://www.usenix.org/conference/nsdi26/presentation/tran), whose SmartNIC results show that cryptography, memory operations, and I/O dominate SFU processing latency. Hollow does not claim its hardware acceleration results.

The staged mode follows the demand-driven cascading and media-plane audio-selection findings in [AsTree](https://www.usenix.org/system/files/nsdi25-meng.pdf). Receiver audio include/exclude policies and loudest-speaker limits follow the production design published by [Jitsi](https://jitsi.org/blog/introducing-receiver-audio-subscriptions/). Cascade links must use ICE, DTLS, and SRTP and avoid unnecessary regional hops, as in [Jitsi Secure Octo](https://jitsi.org/blog/bridge-cascading-is-back/amp/).

Video uses simulcast or scalable video coding with receiver-side layer selection. With encrypted media, relays route using metadata such as the WebRTC dependency descriptor rather than frame contents. Relevant standards are [WebRTC SVC](https://www.w3.org/TR/webrtc-svc/), [SFrame](https://www.rfc-editor.org/rfc/rfc9605.html), and [MLS](https://www.rfc-editor.org/rfc/rfc9420.html).

Congestion control starts with a proven controller and records per-path telemetry. Learned policies may be shadow-evaluated offline, following the deployment discipline in [Mowgli](https://www.usenix.org/conference/nsdi25/presentation/agarwal); they must not learn unchecked in live calls. Loss recovery is deadline-aware and frame-size-aware rather than fixed-rate. [Tooth](https://www.usenix.org/system/files/nsdi25-an.pdf) shows why small frames need proportionally different FEC protection. Last-mile Wi-Fi droughts can still dominate tail latency even with good relay placement, as measured by [BLADE](https://www.usenix.org/conference/nsdi26/presentation/guo-fengqian), so client telemetry must distinguish access-network stalls from relay saturation.

## Game modes

| Mode | Intended game | Consistency |
| --- | --- | --- |
| `deterministic-rollback` | Small deterministic games | Input prediction and bounded rollback; trusted authority for competitive play |
| `authoritative-rollback` | Session games with moderate player counts | Server authority, client prediction, bounded reconciliation |
| `authoritative-snapshot` | Persistent or large worlds | Authoritative spatial shards, snapshot interpolation, area-of-interest replication |

Inputs, snapshots, critical events, and bulk state use separate transport lanes. Inputs are redundant unreliable datagrams; snapshots are sequenced unreliable datagrams; critical events and bulk transfer are reliable but isolated so bulk state cannot head-of-line block inputs. This matches the message and lane model in Valve's [GameNetworkingSockets](https://github.com/ValveSoftware/GameNetworkingSockets).

Rollback is explicitly bounded by game semantics and latency policy. The [OPODIS 2025 formal model](https://drops.dagstuhl.de/storage/00lipics/lipics-vol361-opodis2025/html/LIPIcs.OPODIS.2025.11/LIPIcs.OPODIS.2025.11.html) proves that perfect immersion and safety cannot both be guaranteed under adversarial unbounded delay. Hollow therefore rejects stale input beyond a declared window, records delay anomalies, and never treats a user forwarding relay as competitive authority.

Large worlds replicate only a nearest-first area of interest under a per-source budget. Shards use rendezvous ownership so relay membership changes move approximately `1 / newRelayCount` of shards instead of remapping the whole world. Authority crossing is `prepare -> dual-publish -> commit`, with boundary hysteresis and monotonically increasing lease epochs.

## Trust boundary

- `forward-only` user relays may perform opaque TURN/SFU/game packet forwarding.
- `community` authorities require an explicit server policy and operational identity.
- `managed` authorities may host competitive simulation.
- User-provided media relays require SFrame, MLS, DTLS/SRTP federation, version negotiation, capacity advertisement, and health checks.
- E2EE protects content, not traffic metadata. Relays still observe timing, packet sizes, and route membership.

## CGP emergency realtime lane

Relays may advertise an optional `webtransport-datagram` endpoint in `HELLO_OK`. Durable state, history, authorization, and discovery remain on WebSocket. A signed `SUB_TRANSIENT` installs only an expiring realtime subscription and cannot request snapshots.

- MTU-sized voice/game events use QUIC datagrams and are never retransmitted after their deadline.
- Oversized preview camera/screen events use independent unidirectional QUIC streams with deadline cancellation.
- Control datagrams are retried until acknowledged.
- Clients keep two relay paths and race the same signed transient event over both; event identity and message identity suppress duplicates.
- Relays apply the same membership, channel, expiry, signature, and backpressure checks to WebSocket and WebTransport ingress.
- WebSocket remains the final compatibility fallback. WebRTC RTP/SFU remains the primary call media plane.

## Required gates

The loadnet must reject a release when any tested topology has corrupt payloads, unexpected recipients, duplicate delivery, missing capability enforcement, excessive shard churn, unbounded cascade fanout, or delivery below the configured network-condition threshold. It must report per-hop relay ingress/egress and p50/p95/p99 latency separately for mesh, single SFU, direct federation, cascade, host-star game, and spatial game routes.

The topology lab validates forwarding and policy behavior. It is not a codec-quality, browser interoperability, SmartNIC, anti-cheat, or production capacity certification. Those need separate real WebRTC clients, objective media metrics, heterogeneous devices, and long-duration soak tests.
