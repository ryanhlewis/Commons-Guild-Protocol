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

## SFU authority operations

Configure relay write quorum and sequencer consensus with the same epoch,
member keys, and threshold before certifying media nodes. A production guild
should use at least five relay voters with a 3-of-5 threshold when it needs to
survive one voter loss while retaining a majority.

The guild owner rotates currently advertised Hollow mediasoup nodes through
`rotateGuildSfuAuthorities()`. Run the new nodes before publishing the next
epoch, retain the prior nodes through `overlapUntil`, and remove their route
private keys only after the overlap ends. Do not shorten the overlap below the
catalog refresh interval plus expected clock skew.

Monitor failed `SFU_AUTHORITY_SET` publishes as control-plane incidents. A
2-of-5 partition is intentionally unavailable for rotation; it must never be
made available by lowering the threshold during the incident.

## Durable Write Quorum

Deployments that accept the same guild writes on multiple relays should enable the
epoch-bound write quorum. This prevents an isolated relay from silently accepting a
competing write even when a client bypasses Hollow's configured-relay majority check.

Every relay in an epoch must have:

- a stable `CGP_RELAY_PRIVATE_KEY_HEX` that survives process and host restarts.
- the exact same `CGP_RELAY_WRITE_QUORUM_CONFIG` membership and epoch.
- durable storage for the relay log and write-vote fences.
- connectivity to enough write-quorum members through pubsub.

Example three-relay configuration:

```bash
CGP_RELAY_WRITE_QUORUM_CONFIG='{"epoch":"prod-2026-07-1","members":["<relay-a-public-key>","<relay-b-public-key>","<relay-c-public-key>"],"requiredVotes":2,"voteTimeoutMs":2000}'
```

`requiredVotes` is clamped to at least a strict majority. A relay persists at most
one signed proposal vote per guild head and appends only after collecting the fixed
number of trusted member signatures. `PUBLISH_BATCH` is disabled while this mode is
active because an unvoted batch would bypass the fence.

An application event may request a portable durable proof by placing the
relay's exact `cgp/write-quorum/1` policy in its top-level `certifier` field.
The relay rejects a mismatched policy or an unavailable quorum and attaches a
`cgp/write-certificate/1` to the committed event. The certificate binds the
guild head, complete event body, author signature, epoch, and unique relay
votes, so an application can verify durability without trusting the HTTP
frontend that returned it. `SFU_AUTHORITY_SET` continues to require this proof
implicitly; certified `APP_OBJECT_UPSERT` records such as Hollow's private
Avera security log opt in explicitly.

The write quorum alone is a split-brain safety fence, not a leader-election
protocol. Enable the per-guild sequencer coordinator when clients can submit
competing durable proposals or automatic failover is required:

```bash
CGP_RELAY_SEQUENCER_CONFIG='{"epoch":"prod-2026-07-1","members":["<relay-a-public-key>","<relay-b-public-key>","<relay-c-public-key>"],"requiredVotes":2,"electionTimeoutMinMs":450,"electionTimeoutMaxMs":900,"heartbeatIntervalMs":100,"requestTimeoutMs":8000}'
```

The sequencer and write-quorum configurations must have exactly the same epoch,
ordered member keys, and vote threshold. Terms and votes are persisted in the
relay store. A signed majority certificate elects one leader per active guild;
the leader selects one durable request at a time, and the existing write quorum
still fences the selected append at its current guild head. Followers retain
unselected requests for failover and discard certified completed slots.

Hollow and other durable clients must send the same signed `PUBLISH` request to
at least the configured quorum of writer relays. This lets each independent
relay validate the request and issue its own write vote; relays do not treat a
single forwarding relay as a quorum.

Sequencer consensus applies only to durable guild-log events. `PUBLISH_TRANSIENT`,
WebTransport datagrams, WebRTC RTP/RTCP, SFU media, and game packet lanes do not
wait for this coordinator.

The built-in coordinator assumes authenticated, crash-fault relay members. It
survives one unavailable member in a 3-member/2-vote deployment, but it is not a
Byzantine protocol: a modified member that double-votes is outside this trust
model. Deployments that admit mutually untrusted voting relays need a reviewed
3f+1/2f+1 BFT consensus engine in front of the same durable append boundary.
Never weaken or clear persisted terms, votes, or write fences to regain
availability.

Use mirrored pubsub adapters when write availability must survive one pubsub outage:

```bash
CGP_RELAY_PUBSUB_URLS='wss://pubsub-a.example,wss://pubsub-b.example'
CGP_RELAY_PUBSUB_MODE='redundant'
```

The default multi-URL mode remains sharded. Redundant mode publishes and subscribes
on every configured hub, deduplicating at the relay event/proposal layer.

Treat a membership change as an explicit epoch migration. Bring up the new stable
keys and pubsub paths first, then deploy the same new epoch configuration to the
whole relay set. Never independently edit a member list in place: mismatched epochs
fail closed and can make the write quorum unavailable.

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

From Hollow's repository, run the browser/process split-brain gate after relay
membership, storage, or pubsub changes:

```bash
npm run test:relay-partition
```

It forms a real asymmetric 2/1 partition, checks Hollow's fixed configured quorum,
attempts a direct raw-client write against the minority, kills one mirrored pubsub
hub, and heals the relay. It then races two independent Chromium writers, kills the
deterministic first sequencer process, requires a replacement leader to commit
through the surviving quorum, restarts the killed relay on its existing LevelStore,
and requires exact matching history and head hashes on all three stores.

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

The maintained Cloudflare target lives in `packages/relay-cloudflare`. It is
separate from the Node relay so Worker compatibility constraints do not affect
the normal relay's `ws`/LevelDB/plugin hot path. Use Durable Object SQLite by
default, or configure D1 when an operator wants a shared SQL backing store.
