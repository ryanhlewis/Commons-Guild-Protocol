# Security and Fuzz Testing

CGP security validation currently has six layers:

1. Adversarial protocol tests in `packages/tests/src/security.test.ts` and `packages/tests/src/exploit.test.ts`.
2. Deterministic fuzz tests in `packages/tests/src/fuzz.test.ts`.
3. Supply-chain checks through `npm audit` and `npm run security:scan`.
4. Signed relay-head verification and equivocation detection in `packages/tests/src/relay_head.test.ts`.
5. Distributed/load testing through `npm run load:relay` and the `loadnet/` Docker harness.
6. CI-grade gates through `npm run ci:gate`.

## Commands

```bash
npm run security:scan
npm run conformance:relay
npm run test:fuzz
npm test -- --run
npm run load:relay -- smoke all
npm run ci:gate
npx tsx loadnet/verify-summary.ts
npm run ops:relay -- verify-log --db ./relay-db
npm run ops:relay -- repair-indexes --db ./relay-db
npm run ops:relay -- backup-db --db ./relay-db --output ./relay-backup.json
npm run ops:relay -- restore-db --db ./relay-db-restored --input ./relay-backup.json
npm run ops:relay -- backup-jsonl --db ./relay-db --output ./relay-backup.jsonl
npm run ops:relay -- restore-jsonl --db ./relay-db-restored --input ./relay-backup.jsonl
npm run ops:relay -- sync-from-relay --db ./relay-db --guild <guild-id> --relay ws://peer-relay:7447
npm run ops:relay -- repair-from-quorum --db ./relay-db --guild <guild-id> --relays ws://relay-a:7447,ws://relay-b:7447 --min-valid-heads 2 --min-canonical-count 2
curl http://localhost:7447/healthz
curl http://localhost:7447/readyz
curl http://localhost:7447/metrics
```

## Current Coverage

The adversarial and fuzz tests cover:

- malformed WebSocket frames
- unknown frame kinds
- invalid payload shapes
- oversized payload rejection
- invalid signatures
- unauthorized admin actions
- app/admin object permission checks
- banned-user publish rejection
- replayed publish frames
- concurrent publish sequence races
- split-brain/history-rewrite simulations
- canonical event-id recomputation with polluted `id` and `signature` fields
- randomized hash-chain mutation rejection
- directory registration signature/timestamp rejection
- directory Merkle proof tamper rejection
- LevelDB restart hydration for history, members, and message refs
- follower-relay backfill after sequencer shutdown
- follower-relay retained pubsub log replay after restart
- disk-retained pubsub log replay after pubsub hub restart
- pubsub publish ACK/retry so relays resend unaccepted log envelopes after hub reconnect
- head-gossip-triggered relay catch-up when a peer advertises a newer signed head
- signed relay heads from `GET_HEAD`
- gossiped signed relay heads from `GET_HEADS` and `RELAY_HEAD_GOSSIP`
- forged relay-head rejection
- same-sequence relay-head equivocation detection
- checkpoint sequence/hash disagreement detection
- private guild head access control
- LevelDB log verification, derived-index repair, guild-log export, bulk database backup/restore, and streaming JSONL backup/restore through `npm run ops:relay`
- native relay-to-relay anti-entropy repair through large contiguous `GET_LOG_RANGE` pages and `sync-from-relay`
- quorum-safe anti-entropy repair through `repair-from-quorum`, which refuses divergent local heads, invalid relay heads, and non-canonical source relays
- relay HTTP health, readiness, and Prometheus-style metrics endpoints
- normative relay rejection rules in `docs/protocol-rejection-rules.md`

## Supply Chain

`npm run security:scan` checks:

- `npm audit --json`
- suspicious lifecycle/package scripts
- package-lock resolved tarball hosts
- package-lock remote tarball integrity hashes

This is a heuristic supply-chain scan, not a proof that dependencies are malware-free. It is meant to catch common high-signal issues early and keep the dependency audit clean.

## CI Gate

`npm run ci:gate` runs:

- `npm run security:scan`
- `npm run conformance:relay`
- `npm run test:fuzz`
- `npm test -- --run`
- package builds for core/client/relay/directory
- local smoke load test with a JSON report
- load-report threshold verification

The gate fails if:

- `npm audit` finds vulnerabilities
- suspicious package lifecycle/download scripts appear
- lockfile tarballs resolve from unexpected hosts or lack integrity hashes
- the reference relay fails the HELLO/GET_HEAD/history/state conformance check
- fuzz/tests/builds fail
- smoke load delivery misses expected events
- smoke message append rate or fanout delivery rate drops under thresholds
- smoke RSS exceeds the configured memory ceiling

## Relay Authority

`docs/protocol-rejection-rules.md` is the implementation-neutral authority checklist for third-party relays. The conformance and security suites should grow against that document rather than against UI assumptions.

## Loadnet Findings

The Docker loadnet harness has already identified and driven fixes for:

- stale run-data between distributed runs
- Docker concurrent-build races
- live-only churn backfill gaps
- multi-writer sequence forks
- channel-topic non-contiguous replication
- follower checkpoint forks
- non-canonical event IDs caused by hashing mutable `id`/`signature` fields
- restarted relays missing log envelopes published while they were offline
- pubsub hub restarts losing retained envelopes before followers can replay them
- operator backup/restore exporting an incomplete hot log before the latest sequenced writes reached durable storage

The non-canonical event-id issue was fixed by making `computeEventId` hash only canonical unsigned event fields. Restart gaps were fixed by adding bounded retained pubsub log envelopes, disk-backed pubsub retention, replay subscriptions with `afterSeq`, durable startup resubscription, publish ACK/retry, and signed-head-driven catch-up.

Backup/restore gaps are covered by waiting for the canonical durable head before export and verifying the restored hash chain plus indexed message history. The JSONL backup path streams events instead of materializing full guild logs, so it is the preferred operator path for large relays. Native relay repair uses signed read requests, large contiguous log ranges, and local hash-chain verification before appending. `repair-from-quorum` composes signed head probes with range sync, so operators can recover from peers without manually choosing an unverified source. Restart and slow-consumer profiles are available through the Docker loadnet harness so restart recovery and backpressure failures are tested under actual container networking instead of only in-process mocks.
