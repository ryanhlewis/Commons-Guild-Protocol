# CGP Relay Rejection Rules

This document defines the normative rejection surface for interoperable CGP relays. A relay may add stricter policy through plugins, but it must not accept an event that violates these rules.

## Frame-Level Rejection

Relays must reject a frame when:

- the WebSocket payload is not a valid CGP frame array
- the frame kind is unknown
- required fields are missing or have the wrong type
- the encoded frame exceeds the relay maximum frame size
- a batch exceeds the relay maximum batch size
- a publish request repeats an already accepted client nonce or event identifier within the replay window

The relay should return `ERROR` with a stable machine-readable code and avoid echoing oversized or attacker-controlled payloads.

## Event Integrity Rejection

Relays must reject published events when:

- the author public key is malformed
- the event signature does not verify over the canonical unsigned publish payload
- the event body type is unknown or invalid for the target protocol version
- the body `guildId` is missing for a guild-scoped event
- the event would create a non-monotonic sequence number
- the previous hash does not match the current relay head for the guild
- the canonical event id does not match the accepted event fields
- the event timestamp is outside the relay's configured clock-skew window

Canonical event identity is computed from `seq`, `prevHash`, `createdAt`, `author`, and `body`. Runtime fields such as `id`, `signature`, relay metadata, delivery metadata, and transport envelope fields must not affect event identity.

## Authority Rejection

Relays must reject an event when the author lacks authority at the point in log history where the event would be appended.

Required checks include:

- non-members cannot publish to members-only guilds or private channels
- banned users cannot publish, create invites, alter roles, or read private state
- muted or timed-out users cannot send ordinary messages while the moderation action is active
- channel overwrites must be applied before guild-wide role permissions
- role hierarchy must prevent lower roles from editing, assigning, revoking, kicking, banning, or timing out higher/equal privileged roles
- only authorized users or app principals may create, edit, or delete channels, roles, webhooks, app installs, and guild settings
- message edits and deletes must be limited to the author or an authorized moderator
- webhook and app command responses must match an installed app principal and scoped permission grant
- private guild state, private channel state, and private relay heads must not be served to unauthorized readers

Client-side permission checks are only UX. Relay-side rejection is the authority boundary.

## State and History Rejection

Relays must reject state or history requests when:

- the requesting principal cannot read the requested guild/channel
- the requested cursor is malformed or outside the valid cursor namespace
- the requested page size exceeds the relay maximum
- the request asks for a private guild head or private channel history without proof of access

Relays should serve large lists only through bounded pages. Full member, channel, message, or read-state dumps are not required and should be avoided for large guilds.

## Native Relay Anti-Entropy

Relays may serve `GET_LOG_RANGE` to authorized readers and peer-repair tooling. A response should contain a bounded contiguous range after the caller's local sequence, the current relay head, and a range hash over returned event identifiers.

Clients and operators must reject a repair range when:

- the response is not signed/authenticated according to the normal read policy
- the first returned event does not follow the local head sequence
- the first returned event `prevHash` does not match the local head hash
- any returned event has an invalid signature, canonical id, sequence, or previous hash
- the final repaired head disagrees with the source relay's advertised end hash

When multiple peers are available, operators should first probe signed relay heads and only repair from a source whose head matches the configured canonical quorum. A local relay must refuse automatic repair when its existing head is ahead of the quorum or has a different hash at the quorum sequence, because that is a rollback or fork-resolution decision rather than ordinary catch-up.

This gives self-hosted relays a bitcoin-like anti-entropy path without requiring Kafka, NATS, Redis, or any other external bus.

## Relay-Head Rejection and Equivocation Detection

Clients and relays must reject relay heads when:

- the relay public key is malformed
- the head signature does not verify
- the head guild does not match the requested guild
- `headSeq` is lower than `checkpointSeq`
- checkpoint fields are internally inconsistent

Clients and relays should treat the following as equivocation evidence:

- two valid heads for the same guild and sequence with different `headHash`
- two valid heads from the same relay id for the same guild with different head values
- two valid heads with the same `checkpointSeq` and different `checkpointHash`

Operators should preserve conflicting heads for audit/export instead of overwriting them.

## Plugin Policy Rejection

Plugins may reject otherwise valid events for local policy. Examples include:

- rate-limit policy
- proof-of-work or proof-of-compression policy
- invite-abuse policy
- guardian recovery request-abuse policy
- media quota policy
- raid-mode policy
- jurisdictional or community moderation policy

Plugin rejections must be explicit policy failures. They must not mutate canonical event identity, sequence rules, hash-chain rules, or signature rules.

Relays that load untrusted plugin policy should prefer the sandboxed command plugin adapter. A
sandboxed plugin may return an explicit `ERROR` frame for rejected client frames, but it receives
only JSON hook inputs and cannot directly mutate relay storage or canonical sequencing state.

## Guardian Recovery Abuse Rejection

Guardian recovery is app-layer social recovery, not relay-side account custody. Relays may accept
opaque `org.cgp.recovery` objects, but they should reject recovery traffic when:

- a public recovery record contains plaintext guardian usernames, email addresses, phone numbers, or hashes of those identifiers
- a request omits the relay's required proof-of-work or anti-abuse token
- a handle, requester key, guardian hint, recovery topic, or source network exceeds the relay's recovery request budget
- repeated requests are not coalesced into an existing pending notification
- the target guardian has muted, blocked, or paused recovery notifications for that topic
- the object tries to store a private key, plaintext guardian share, password, email token, or relay-readable recovery secret

Recovery rejection responses should be generic. A relay should not reveal whether the account handle
exists, whether the guardian hint was correct, whether a guardian has approved, or which approvals
are still missing. Guardian identities belong in encrypted recovery policy or client UI, not in an
enumeration oracle.

## Recovery and Repair

Relays should provide operator tooling to:

- verify an append-only guild log from genesis to head
- export a guild log with verification metadata
- back up and restore relay databases from canonical guild logs
- stream JSONL backups/restores without materializing full guild histories
- repair a lagging relay from a peer relay using bounded contiguous log ranges
- repair a lagging relay from a canonical signed-head quorum when multiple peers are available
- rebuild derived indexes from the canonical log
- compare signed heads across relays
- preserve equivocation evidence for incident response
- recover from missed live pubsub frames by replaying retained guild-log envelopes after the local durable head sequence
- preserve retained pubsub log envelopes across pubsub restarts when the deployment advertises durable pubsub retention
- retry relay-to-pubsub publishes that were not acknowledged by the pubsub durability boundary
- use valid signed relay-head observations as catch-up hints, not as replacements for hash-chain verification

Derived indexes are cache/projection state. The append-only log is the recovery source of truth.

## Operator Readiness

Production relays should expose non-guild-specific operator endpoints for load balancers and monitoring:

- `/healthz` for process liveness
- `/readyz` for serving readiness
- `/metrics` for low-cardinality relay metrics such as connected clients, subscriptions, queued fanout, pubsub topics, hosted guild count, relay-head conflict count, and event-loop delay

These endpoints must not expose private guild history, private channel state, user payloads, or encrypted payload content.
