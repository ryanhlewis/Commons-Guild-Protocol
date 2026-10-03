# Community continuity qualification

Run from the repository root:

```sh
npm run ops:continuity
```

The command builds core/client, launches separate Node relay processes with separate
LevelDB directories, forcibly stops the founding relay, repairs an empty replacement
from two agreeing signed relay heads, and verifies the complete signed event log.
It also verifies insufficient witnesses fail, modified content fails signature/hash
verification, and a divergent local head is not overwritten. Finally a fresh client
reads and writes on the replacement after all original relays have stopped. The owner
then enrolls a new identity, which reads original history and publishes a new message.

JSON results go to `output/community-continuity/latest.json`. Pass a report path
directly to `tsx scripts/community-continuity-drill.ts PATH` after building for a
different destination. Each run preserves its synthetic stores under the reported
temporary evidence directory. Private keys are disposable and are not included in
the report. All child processes and clients are closed at completion or failure.

## Scope of the evidence

This is local process/storage continuity, not a claim of independent operators or
public-network qualification. A shared local pubsub hub remains available. Bootstrap
copies explicitly trust the synthetic founder. A fresh client reuses the owner's key;
this is not delegated-device recovery, invitation/bootstrap discovery, private-key recovery,
attachment preservation, or voice-call testing. Relay write quorum is not configured
in this drill. Successful replacement therefore does not demonstrate changing a live
write-quorum membership epoch.

## Trust and relay-set transition audit

`repair-from-quorum` now defaults to a strict majority of distinct configured relay
URLs: two of two, two of three, three of four. Explicit threshold overrides remain
operator decisions. Signed heads count distinct signing identities in core; multiple
URLs do not turn the same relay identity into multiple witnesses. The CLI's configured
URLs are its trust input; a valid signature alone does not prove that a signer is an
authorized operator. Do not source replacement URLs from an untrusted advertisement.

`RelayWriteQuorumCoordinator` uses a configured epoch and membership set with durable
vote fences keyed by epoch. Changing a configuration string is not an authenticated
community decision. Replacing a member or inventing a new epoch must not be treated
as an availability shortcut: independent configurations can accept inconsistent
histories even when each reaches its own numerical majority.

A production transition needs a protocol-reviewed certificate tying the old trusted
configuration, final agreed checkpoint, new membership and threshold, and authorization
to change membership together. Old-epoch finalization/fencing and new-epoch activation
must prevent both sides independently committing conflicting continuations. New and
recovering clients need a verifiable chain from their existing trust anchor; operators
must durably store the transition and reject unauthorized, stale, replayed, or
conflicting transitions. Disjoint memberships require explicit handoff, not a lowered
threshold. This mechanism is not claimed implemented by this drill.

The next external qualification requires independently administered machines, fresh
members and delegated-device recovery, private guild keys, retained attachment bytes,
replacement discovery after original bootstrap loss, and public-network calls. Record
each outcome separately; local success cannot stand in for these results.

## Minimal independent archive reader

`examples/independent-archive-reader.mjs` imports no CGP core, client, relay, canonical
JSON helper, or protocol verifier. It independently encodes canonical JSON and hashes
with Node's SHA-256. It shares the `@noble/secp256k1` cryptographic primitive dependency;
this is implementation separation, not a separate organization or independent audit.

```sh
node examples/independent-archive-reader.mjs archive.json EXPECTED_HEAD EXPECTED_COUNT
node examples/independent-archive-reader.mjs archive.json EXPECTED_HEAD EXPECTED_COUNT --read
```

Supply head/count from a trusted checkpoint, not from the untrusted archive itself.
The tool checks the genesis, guild scope, full sequence/hash chain, author signatures,
and externally expected final checkpoint. Without that checkpoint a valid truncated
prefix cannot be distinguished from complete history. `--read` prints verified raw
message events as JSON; it does not reconstruct edits/deletions or render HTML.
The continuity drill exports a real repaired store and invokes this reader against
the previously compared checkpoint. Separate tests use core-signed fixtures with
Unicode/numeric keys and reject modified content, sequence, predecessor, signature,
truncation, guild substitution, unsupported delegated authority, and wrong checkpoints.

Limits: full nonempty root-author-signed logs only, maximum 32 MiB CLI input and 100
JSON nesting levels. Delegated-device authorizations fail closed. This reader does not
validate stateful membership/permissions or voting certificates, decrypt private
messages, fetch attachment bytes, restore live authority, or prove checkpoint trust.
Signature validity is not a claim that every event was authorized by community policy.

## Local validation receipt (2026-09-28)

- Core/client TypeScript builds passed.
- Final complete suite with `--maxWorkers=2`: 287 tests passed, zero failures;
  machine-readable receipt: `output/community-continuity/full-suite.json`.
- Changed quorum/reader suites: 25 tests passed, including alternate compressed and
  uncompressed encodings of the same signing identity.
- `npm run security:scan` passed (dependency audit, package scripts and lockfile).
- `npm run conformance:relay` passed against its temporary local relay.
- Continuity drill passed all 11 checks; receipt: `output/community-continuity/latest.json`.

The initial default-parallel full suite had three timing failures in message/fork
event delivery and SFU minority catch-up; those files passed in isolation. The final
reduced-concurrency full-suite result does not assert the default-parallel timing
issue is fixed. No production service was changed and no external operator was qualified.

## Authorized ECHO cross-machine pilot (2026-09-28)

The isolated Windows ECHO plus local Windows controller pilot passed five checks:
native LevelDB startup, signed founder-history transfer to two pinned-checkpoint local
witnesses, two-head repair and independent archive verification, remote replacement
reads/new-member writes after original relays stop, and persistence after restarting
the remote replacement. All network listeners were loopback with strict-host-key SSH
forwarding; no production relay, directory, firewall or Kubo service was changed.

Machine ECHO ran founder PID 42264 on port 53234, replacement PID 40956 on port 62854,
then replacement restart PID 41552 on port 62871. Replacement/restart used the same
task-owned LevelDB directory. Each wrote a graceful stopped receipt; a subsequent
task-specific process query found zero remaining Node processes. The separate first
attempt failed because its detached child did not survive the SSH session; a later
query also found zero task-specific processes from that attempt. The successful
controller keeps SSH attached to the worker and has a ten-minute remote watchdog.

Evidence directory: `output/community-continuity/20260928-continuity-1790626323221/`
contains `report.json`, `remote-lifecycle.json`, verified `replacement.json`, and
`public-checkpoint.json`. Remote task directory:
`C:/Users/ECHO/hollow-roadmap-staging-20260928-continuity-1790626323221`.
Worker SHA-256: `26ebfc5ecadb5fb065a36efeabaa95f2b1c200eb9b3b35cb77c75a84f95e97c9`.

This remains one administrative operator. Both repair witnesses ran locally. The
remote replacement imported and revalidated the repaired archive; this is not a
claim of automatic remote relay repair, live voting-membership replacement, public
discovery, independent operators, media durability, calls or delegated-device recovery.

Only with authorization to use ECHO, its existing trusted SSH route and the sibling
Tauri repository's SSH helper, reproduce from this repo on Windows:

```sh
npm run build:protocol
node scripts/build-remote-continuity-package.cjs
node --import tsx scripts/echo-continuity-pilot.ts
```

The packaging step uses existing dependencies and installs nothing. The run creates
a unique task staging directory, copies the standalone relay and native LevelDB
dependency closure, performs the bounded pilot, and closes test workers/forwards.
It preserves synthetic stores and evidence rather than recursively deleting them.

## Public three-PC fixed-quorum pilot (2026-09-28)

The temporary pilot harness is `scripts/public-quorum-pilot.ts`, packaged with
`scripts/build-remote-continuity-package.cjs`. It uses distinct persistent relay
stores and secp256k1 identities on Cortop1, ECHO and Cortop3, a fixed 2-of-3 policy,
and temporary TLS WebSocket tunnels. The authenticated shared pubsub hub runs on
Cortop1; this is an explicit common availability dependency. Sequencer consensus
is disabled in this pilot, so this is not a live leader-election test. The machines
remain under one administrator, not three independently operated organizations.

`scripts/public-quorum-matrix.ts <client-window.json>` rotates every singleton
and surviving pair, then isolates all three simultaneously. Each denied write
must leave its signed head unchanged; every healing step requires all three
signed heads to agree. Resulting events must carry exact-policy valid certificates.
These healing assertions prove read convergence, not post-outage write liveness.
In write-quorum-only mode, different isolated proposals can leave incompatible
durable vote fences at the same head. Healing transport does not erase those
fences; exact retry only resumes the same proposal, not arbitrary conflicts.
The controller also restarts Cortop3 over its original store/key and verifies its
signed durable head. Temporary tunnels and processes are bounded and explicitly
stopped after the actual-app test window.

The pilot discovered a real replication boundary bug: configured quorum relays
accepted an author-signed genesis injected through replication without any quorum
votes. `quorum_ingress.test.ts` failed before the fix. Quorum-mode append now
attaches proof to every event, and replication verifies the exact configured policy
before incoming device authority or state mutations. Direct plugin bulk append is
blocked in quorum mode; no-quorum behavior is retained. The production operations
document describes the deliberate compatibility boundary for old uncertified logs.
The ingress matrix rejects missing/malformed/duplicate-vote/altered and wrong
member/epoch/threshold proofs; historical delegated proof tests distinguish valid
signed-event time from currently expired authority.

Newly issued tunnel names intermittently failed the PCs' default DNS resolvers.
The fixture-only resolver fallback registers exact issued hostnames and queries
1.1.1.1/8.8.8.8 on default lookup failure, preserving URL hostname, TLS SNI and
certificate checks. It changes no machine DNS configuration. Receipts record
failures and addresses; these tests do not prove seamless default-DNS discovery.
Earlier failed runs retain honest reports: initial quorum transport readiness,
then a false-ready client/restart timing issue, then unresolved default DNS. Their
successful intermediate protocol checks are not represented as complete passes.


### Narrower local completion evidence

After the distributed workflow was stopped, `scripts/local-quorum-pilot.ts
--client-window` completed a separate **local-only** run, `local-quorum-1790630041349`.
Six checks passed: each of three singleton/remaining-pair partitions, full isolation,
exact matching certified logs, and a LevelDB/relay-instance close and reopen using
the same key. This was one Node process on one PC; it was not an OS process restart.
The final receipt is `output/community-continuity/local-quorum-1790630041349/report.json`.
The real Hollow browser account export/fresh-device restore and signed accepted
publish subsequently passed against these loopback endpoints, with all three
relay HELLO pins checked (Svelte `.tmp/local-app-quorum-20260928`). The local runner
exited successfully and closed its listeners after the app finished. These local
results do not complete the public all-pairs, public application or public restart
gates. The seven prior distributed attempts retain their original outcomes and
were independently cleaned up by the parent/operator workflow.


### Authorized public retry completed

The subsequent user-authorized original workflow passed normal tool review and
completed as `20260928-quorum-1790631262324`. Its `report.json` records six passing
baseline/recovery checks, including an actual Cortop3 Node process restart
(PID 8376 to 7076) over the same LevelDB and identity. The replacement public TLS
endpoint returned the exact persisted signed head. `all-pairs.json` records all
three singleton/remaining-pair partitions, all-three-isolated rejection, healed
signed-head agreement, and exact-policy certificate verification.

The matrix runner had a separate cleanup defect after every protocol assertion
passed: removing an already absent isolation sentinel returned SSH exit 1 and
skipped closing client sockets. Its original `all-pairs-failure.json` is retained.
A read-only `matrix-heal-audit.json` confirmed every relay link was ready and healed;
the specific matrix controller PID 24980 was then stopped. The harness now makes
sentinel removal idempotent and closes sockets in a nested `finally`. This is not
represented as a clean exit of the original matrix runner.

The actual Hollow application also passed account export and fresh-browser device
recovery plus an accepted signed publish over these public endpoints, with all
three expected relay keys checked. Its sanitized receipts live in Svelte
`.tmp/public-app-quorum-normal-retry-20260928`. Browser TLS and default DNS were
used without overrides. The replacement tunnel initially failed DNS resolution;
the bounded longer readiness wait ultimately succeeded. Controller DNS diagnostics
are retained rather than claiming seamless discovery. The main controller exited
0 after explicitly stopping its workers/tunnels. Independent cleanup is recorded
by the parent operator workflow. No production source changed during this retry.

These results supersede the earlier incomplete public test gates only for this
bounded synthetic-data, fixed-2-of-3 pilot. They do not establish independent human
operators, removal of the shared pubsub hub, live membership transitions, or public
sequencer-election resilience.
