# Consensus v2 pilot

This is an opt-in protocol and relay pilot. Existing Hollow browser/native clients and existing guilds do **not** automatically move to it. The normal application's previously recorded recovery, MLS, media and call results are separate evidence; they do not qualify the new consensus path.

## Trust and safety model

The coordinator implements crash-fault Paxos with authenticated votes and a configured strict majority. It is **not Byzantine consensus**: signatures identify a voter but do not make a dishonest voter obey its durable promise. A compromised quorum, duplicated signing identity outside its exclusive durable store, or rollback of durable acceptor state violates the operating assumptions.

Provision a guild's `ConsensusPolicy` independently of relay discovery: epoch, canonical compressed voter keys, strict-majority threshold, administrator keys and administrative threshold. `HELLO`, a URL and a reachable server are not trust anchors. The `peers` map supplies transport addresses; relay RPC responses are checked against the configured signing identity. Use authenticated TLS for nonlocal transport, including protection of signed read requests and the private histories returned over that transport.

The durable state includes the certified history, highest promise, accepted proposal and proposer counter. A promise/vote is emitted only after persistence. Reopening the same LevelDB directory restores those fences; copying only the application event log is insufficient. A relay identity must have one exclusive storage domain. Preserve the full node state when backing up or replacing it; do not erase fences to restore availability.

Each prepare quorum determines the highest previously accepted value. A later proposal can therefore commit an earlier request rather than the caller's requested value. This is expected recovery behavior. Losing a response leaves the outcome unknown; fetch and verify certified history before deciding what to retry. `ConsensusV2Client` exposes `requestedCommitted` rather than treating every successful response as acceptance of its submitted value.

## Opt-in interfaces

`RelayServer` accepts `consensusV2` configuration with explicit per-guild trust roots, pinned peer addresses and a bounded RPC timeout. `ConsensusV2Client` provides `fetchHistory`, `submitEvent` and `submitTransition`; its read signer authorizes the exact correlated read payload. The client verifies the history against its caller-provided anchor and rejects prefix forks, rollback and activation rollback.

The wire entry points are `CONSENSUS_HISTORY`, `CONSENSUS_PUBLISH` and authenticated relay `CONSENSUS_RPC`. This is a separate transport from legacy `PUBLISH`; a configured v2 guild rejects legacy write/replication/plugin ingress. Legacy checkpoint/pruning paths cannot rewrite its certified prefix. Application permission checks and current device authority still apply to new proposals. Validating an authenticated proposal can durably advance device-authority pins even when the proposal does not commit; an ambiguous write outcome does not authorize rolling back those pins or resurrecting revoked authority. Historical replay uses event-time proofs and does not confer live permission on a revoked device.

Application event numbering starts with `seq: 0`. Consensus positions are separately numbered because policy transitions occupy consensus entries without adding application events. Clients must not confuse these two counters.

## Membership transitions and legacy migration

A v2 membership change needs administrator authorization, a committed transition under the old policy, and signed readiness from a strict majority of the new policy after they have validated and durably stored the prefix. The old epoch cannot append after its transition. A new epoch cannot append before activation. Epoch reuse, missing activation, divergent prefixes and rollback are rejected. This protocol does not make simultaneous loss of an old majority recoverable by unilateral administrator action.

Migrating an existing **legacy** guild is a different operation from changing membership of an already-v2 guild. The legacy bridge now has focused kernel and local real-relay qualification. Do not enable v2 over an existing application log merely by changing configuration: the relay refuses an unbridged log instead of overwriting it. Qualification must demonstrate an authorized, exact certified legacy prefix; durable retirement of all old voters; handling of a vote/proposal already in flight at the freeze boundary; and continuation without omitting any possibly chosen legacy value. The local qualification below establishes these boundaries for synthetic fixtures; it does not establish a production guild rollout. Every old voter must run freeze-aware software, including after restart; a downgraded binary that ignores its retirement tombstone is outside this safety assumption. An unavailable old voter or differing certified heads blocks this conservative migration.

## Reproducible acceptance and evidence

From the repository root, build the protocol packages and run focused tests:

```powershell
npm.cmd run build:protocol
npx vitest run --config packages/tests/vitest.config.ts packages/tests/src/consensus_v2.test.ts packages/tests/src/consensus_v2_client.test.ts packages/tests/src/consensus_v2_live.test.ts packages/tests/src/consensus_legacy_migration.test.ts packages/tests/src/consensus_legacy_live.test.ts
```

The kernel/client tests cover partial-accept recovery, durable promises, highest-accepted-value enforcement, shared-store coordinator serialization, administrative transitions, history substitution/forks/rollback and bounded client transport failure. An internal reviewer independently reran the kernel and client files: **10 tests passed** on 2026-09-28. That result predates the legacy bridge additions; rerun after those changes.

The final full protocol run passed **357 tests across 62 files** on 2026-09-28 (`npm.cmd test -- --maxWorkers=2`, 96.40 seconds). Core, client and relay builds passed. After the final typed migration-client fixture edit, the live migration file passed again (2.38 seconds of tests). The ignored local full-run receipt is `.tmp/consensus-v2-full-suite-20260928.log`.

The new-guild fixture passed with five real loopback WebSocket RelayServers and separate LevelDB stores. It covers private-history denial, legacy-write refusal, recovery after two accepted-but-uncommitted voters restart, and accurate acknowledgment when consensus chooses the earlier value. It moves ABC to BCD and then CDE, heals a reachable voter that missed activation, and lets a new voter bootstrap another voter that missed the entire transition after an old node closes. The new voter continues writing and certified history survives restart. Clients, servers and owned temporary stores are cleaned in `finally`. Loopback tests do not establish public-network reachability, independent operation or browser integration.

The legacy kernel and live migration files independently passed **5 tests** on 2026-09-28; after adding wire-level operator authorization assertions, the live file passed again. Its three loopback RelayServers use separate LevelDB stores and a shared local pubsub adapter. Setup imports a synthetic, cryptographically certified genesis through the real legacy replication validator; it does not claim a normal legacy composer publication. The test persists three distinct minority fences, restarts, freezes every voter, imports the bridge, and continues with a real WebSocket v2 client at application sequence 1. One node uses the exported typed client `freezeLegacy(request, expectedVoter)` and `importHistory(history)` over actual WebSocket frames, after explicitly fetching an empty v2 history; the bootstrap bridge requires separately pinned legacy trust. Raw signed requests from unauthorized operators are rejected. Restarting without v2 configuration still rejects a late legacy vote and a synthetically valid late certificate, preserves the original fence and retirement record, and leaves the log unchanged. The fixture closes servers/clients and removes only its owned temporary stores in `finally`. This is a local regression result, not a multi-PC deployment receipt.

## Three-PC public transport pilot

The bounded network pilot passed six assertions against product revision `37fddeaa14b31561db028d6ff4cad03e7d1e0922`, completing at 2026-09-29 00:36:01 UTC (September 28 locally). The [sanitized receipt](evidence/consensus-network-pilot-2026-09-28.json) records the bundled worker hash, topology, assertions and cleanup. Four synthetic voter keys used three owned Windows PCs: A and replacement D on CORTOP1, B on ECHO, C on CORTOP3. Both ABC and BCD span three PCs; this replaces a voter key, not an independent administrator.

The test used public WSS through temporary Cloudflare quick tunnels, verified TLS and authenticated RPC identities, committed a private genesis and rejected an outsider's signed read. A and B accepted an event without committing it; both processes and tunnels then restarted over their original LevelDB stores and keys. C's competing request recovered the exact earlier accepted event and correctly reported that its own request was not committed. An administrator-authorized ABC-to-BCD transition gathered old-policy commitment and new-policy readiness, rejected retired A, and accepted a new D write. A fixture partition isolated B and C's consensus RPC paths: D could not commit alone under the unchanged 2-of-3 threshold. Healing restored convergence and the same refused request then committed.

Reproduce only on the existing owned test hosts with their prerequisites:

```powershell
node scripts/build-consensus-network-package.cjs
npx tsx scripts/consensus-network-pilot.ts
```

The build reuses the Windows native dependency archive from `build-remote-continuity-package.cjs`. The controller uses strict existing SSH routes and foreground workers, loopback listeners, disposable quick tunnels and a 30-minute worker deadline. It installs no service, scheduled task, startup entry or named tunnel. New quick-tunnel DNS names initially produced reachability failures; the bounded retry and exact-host fixture DNS fallback retained the original hostname, SNI and TLS verification. Original DNS observations and failures remain in the ignored run directory.

All four final workers reported shutdown. A separate root-agent audit then confirmed all **12 historical worker/tunnel PIDs and six listener ports absent** across the three PCs, including pre-restart instances. The initial controller-only attempt failed on a local variable naming error before starting a worker; its failure receipt was retained. Original instance metadata accidentally overwrote the routing-host field with the listener address; the original evidence is preserved, and future controller output uses a separate `routeHost` field. Public receipt routing comes from the recorded machine/role, not that overwritten field.

This is a synthetic Node-client/protocol pilot. It does not establish browser integration, physical microphone/camera support, Byzantine safety, independent operators, public legacy-guild migration, sustained-load behavior or persistent deployment. The shared tunnel provider remains a correlated dependency. Earlier restrictions on persistent provisioning remain untouched.

## Remaining acceptance boundaries

- Extend legacy migration qualification to deployed operator topology, real nonempty historical guilds/device-authority transitions and failure during full-store recovery. Kernel tests cover an in-flight vote, conflicting partial fences, required exact majority-fenced payload and missing old voters; the local live test covers certified-prefix retirement/restart. A safe refusal remains preferable to an uncertified bridge.
- Migrate actual browser/native client flows explicitly, then repeat recovery, encrypted-room rejoin/history, media and call acceptance against the new path. The Node client alone does not establish those outcomes.
- Extend the bounded public transport result to independently administered operators. Separately qualify sustained load, resource limits, network-level packet loss and long-running operational recovery; the pilot partition is an explicit fixture RPC gate, not packet-level fault injection.
- Obtain external security review and independently administered operators. Multiple machines managed by the same account are not independent operators; internal agent review is not an external audit.
- Record trust-root distribution, signed release/update procedures, retention obligations and operator handoff. A quorum commit is not a media retention SLA or assurance that users can recover lost encryption keys.

Keep synthetic private keys, account exports, credentials and plaintext private histories out of committed receipts. Retain test revision, sanitized topology and thresholds, assertions, timings, failure artifacts and exact cleanup evidence. No outreach or external-human review is implied by this document.
