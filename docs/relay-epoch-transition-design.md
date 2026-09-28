# Relay epoch transition: implementation boundary and review proposal

Status: design only. No live membership transition is currently implemented.
Startup environment edits are not a protocol operation. The existing fixed
majority mechanism is a crash/partition safety fence, not Byzantine consensus;
a 2-of-3 pilot does not establish tolerance of one malicious voter.

A deployable transition needs all of the following, implemented and tested together:

1. An authenticated administrative authority binding for the relay set, distinct
   from an arbitrary community member or a relay's transport credentials. The
   current system has no universal community-to-relay administration binding.
2. A canonical transition record binding old/new complete policies, unique nonce,
   previous transition hash, expiry, and an authenticated inventory of exact guild
   heads. A per-guild head alone cannot retire a global policy safely for other guilds.
3. A joint phase requiring both old-policy and new-policy quorums over the same
   transition and head barrier. Neither one old quorum alone nor a smaller proposed
   threshold may authorize writes under the new set. Any configured threshold
   reduction requires its own explicit reviewed policy, not an availability fallback.
4. Durable per-identity transition fences: after acknowledging retirement an old
   node cannot resume the old epoch following a restart. The final transition record
   and all vote/term fences must survive backups and crash recovery.
5. New members must verify the old certified history and synchronize authority pins,
   state/indexes and the committed barrier before voting. Historical certificates
   remain verified against their authenticated historical policy; never manufacture
   new certificates over old events or treat a current member list as historical truth.
6. Reads, writes, replay, plugin publication and peer catch-up must all obey the same
   epoch state machine. Clients need an authenticated transition chain and an explicit
   minimum supported protocol version, not a mutable directory declaration.
7. A stalled joint phase remains unavailable when either required quorum is absent.
   A deterministic recovery/abort path must prove no finalized new-epoch write exists;
   independent timers or administrator edits cannot reset durable fences.

Required adversarial tests before shipping: simultaneous conflicting transitions;
old/new partitions during every phase; retired-node restart; stale snapshot restore;
replayed/expired transition; altered scope/catalog; equivalent-key aliases;
missing old or new signatures; partial new-member catch-up; plugin bypass; process
crash between durable fence and response; and multiple private guilds omitted from
an otherwise valid inventory. Model checking of the state machine should precede
production implementation. Until those gates pass, preserve the fixed epoch and
recover only exact current state with the original writer fenced.


## Bounded executable design evidence

`scripts/epoch-transition-model.ts` explores 266,240 combinations across overlapping
and disjoint 3-to-3 sets, vote subsets and crash/restart subsets. It checks joint
majorities, conflicting transition exclusion, durable retirement preventing a
remaining old majority, authenticated permission and synchronized barrier gates.
A negative control deliberately acknowledges votes without durable persistence,
then crashes; the model finds that conflicting transitions can both be certified.
`packages/tests/src/epoch_transition_model.test.ts` runs this model. The receipt is
`output/community-continuity/epoch-transition-model.json`.

This is an abstract finite safety model, not a production implementation or a
proof for arbitrary network histories. It assumes validated administrative/voter
signatures, an exact global head barrier, honest crash-fault voters, and correct
storage durability. It does not implement cryptographic authorization, distributed
barrier discovery, liveness, catch-up or client transition verification. Runtime
membership migration therefore remains an open gate.
