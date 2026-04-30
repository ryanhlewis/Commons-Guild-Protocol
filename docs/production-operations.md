# CGP Production Operations

CGP relays are designed to be self-hosted append-only log replicas. A deployment can use a local process, a container cluster, or a serverless worker backed by durable storage. External buses such as Kafka, NATS, or Redis are optional deployment choices, not protocol requirements.

## Production Shape

- Partition by guild id. Keep all writes for a guild on one sequencer at a time.
- Replicate accepted log envelopes to follower relays for read fanout and backfill.
- Keep the append-only guild log as the source of truth. Derived indexes are rebuildable.
- Expose `/healthz`, `/readyz`, and `/metrics` on every relay.
- Keep retained pubsub/log envelopes on durable storage when the deployment uses pubsub.
- Use signed relay-head quorum before trusting a peer for disaster recovery.

## Placement

Recommended minimum production placement:

- 3 relay nodes across at least 2 failure domains.
- 1 active sequencer per hot guild partition.
- 2 or more follower relays that can serve history and state reads.
- Durable LevelDB/RocksDB-compatible storage for process relays, or an equivalent ordered key-value store for worker deployments.
- Periodic JSONL backups to independent object storage.

For very large guilds:

- shard clients by guild/channel subscription interest.
- route channel live fanout to channel-specific topics.
- keep guild-log replication separate from live fanout topics.
- use `GET_LOG_RANGE` only for repair/backfill beyond retained envelopes, not for every live event.

## SLO Baseline

The reference dashboard and alert rules live in `ops/`.

Initial service objectives:

- `/healthz` success: 99.99%
- `/readyz` success for serving relays: 99.9%
- event-loop p99 delay: under 250ms for 5-minute windows
- fanout queued frames: normally near zero, alert over 10k for 2 minutes
- relay-head conflict guilds: zero
- pubsub pending guilds: zero after startup warmup

Import:

```bash
promtool check rules ops/prometheus-rules.yml
```

Then import `ops/grafana-dashboard-cgp-relay.json` into Grafana with a Prometheus datasource.

## Disaster Recovery

Verify a local relay before making changes:

```bash
npm run ops:relay -- verify-log --db ./relay-db
npm run ops:relay -- repair-indexes --db ./relay-db
```

Stream a backup without loading full guild histories:

```bash
npm run ops:relay -- backup-jsonl --db ./relay-db --output ./backup/relay.jsonl
npm run ops:relay -- restore-jsonl --db ./relay-db-restored --input ./backup/relay.jsonl
```

Run the local operational drill before deploying relay changes. It exercises durable backup/restore,
health/readiness/metrics endpoints, failover repair paths, and sandboxed plugin rejection behavior:

```bash
npm run ops:drill
```

Compare signed heads across peers:

```bash
npm run ops:relay -- compare-heads --guild <guild-id> --relays ws://relay-a:7447,ws://relay-b:7447,ws://relay-c:7447 --min-valid-heads 2 --min-canonical-count 2
```

Repair a lagging relay from a quorum-selected canonical source:

```bash
npm run ops:relay -- repair-from-quorum --db ./relay-db --guild <guild-id> --relays ws://relay-a:7447,ws://relay-b:7447,ws://relay-c:7447 --min-valid-heads 2 --min-canonical-count 2 --limit 10000
```

Use direct peer sync only when the operator has already chosen the source:

```bash
npm run ops:relay -- sync-from-relay --db ./relay-db --guild <guild-id> --relay ws://peer-relay:7447 --limit 10000
```

Never silently repair over a local head that is ahead of quorum or divergent at the quorum sequence. Preserve the DB and signed head evidence for incident response.

## Plugin Isolation

Trusted built-in plugins may run in-process. Untrusted or community plugins should be wrapped with
`createSandboxedCommandPlugin`, which invokes a separate command per hook over the
`cgp.relay.sandboxed-plugin.v1` JSON protocol.

The sandbox adapter passes only sanitized hook arguments plus the relay public key. It does not pass
the relay store, live `WebSocket`, HTTP response object, or publish/broadcast functions. Operators
should still run the command under an OS/container sandbox when filesystem or network isolation is
required; the relay wrapper enforces process separation, bounded stdout/stderr, HTTP body limits, and
timeouts.

## Soak And Release Gates

Run local gates before release:

```bash
npm run ci:gate
npm run gate:brutal
```

Run distributed profiles before production rollout:

```bash
npm run gate:brutal -- --with-docker
```

For a long soak, run the longer Docker profiles under the target host type and retain `loadnet/results/*.json`:

```bash
npm run loadnet:run -- --profile soak-long
npm run loadnet:run -- --profile soak-chaos
```

Verify each summary with strict gates:

```bash
npx tsx loadnet/verify-summary.ts --summary loadnet/results/summary-<id>.json --min-delivery-ratio=1.0 --max-publisher-p99-ms=30000 --max-relay-head-lag=0
```

Do not promote a build if any relay returns an invalid signed head, if expected subscriber delivery fails after final backfill, if follower relays disagree on canonical head hash, if delivery ratio falls below `1.0`, or if publisher batch `p99` exceeds `30000ms`.

## Worker-Backed Relays

Worker deployments should preserve the same invariants:

- use an ordered durable key namespace equivalent to `guild:<guildId>:seq:<seq>`.
- keep a head key equivalent to `guild:<guildId>:head`.
- write derived member/message/channel projections transactionally with accepted events when possible.
- expose the same `GET_HEAD`, `GET_HEADS`, `GET_HISTORY`, `GET_STATE`, and `GET_LOG_RANGE` behavior.
- avoid full guild scans in hot request paths; use cursor pages and log ranges.

The protocol does not require a specific database vendor. The required property is deterministic ordered reads over guild sequence keys plus durable writes before a relay advertises a signed head.
