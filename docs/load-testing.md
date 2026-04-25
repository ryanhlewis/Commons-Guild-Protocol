# Relay load testing

Use the relay load harness to measure CGP relay storage and fanout behavior at target sizes before tuning. The harness is intentionally standalone so it can run without a browser client.

For distributed Docker runs with multiple relay nodes, latency, packet loss, and socket churn, use the loadnet harness in `loadnet/`.

## Commands

```powershell
npm run load:relay -- smoke all
npm run load:relay -- 100k storage 100000 1000000 0 0 10000 8 100 level
npm run load:relay -- 1m storage 1000000 2000000 0 0 10000 16 100 level
npm run load:relay -- 100k fanout 0 0 1000 1000 250 1 10 memory
npm run load:relay -- 100k fanout 0 0 1000 1000 250 1 10 memory -- --fanout-parse=count
npm run load:relay -- 100k fanout 0 0 1000 1000 250 1 10 memory -- --fanout-parse=count --relays=4
npm run load:relay -- 100k fanout 0 0 1000 1000 250 8 10 memory -- --fanout-parse=json --relays=4
npm run loadnet:compose -- --profile smoke
npm run loadnet:compose -- --profile smoke-binary-v1
npm run loadnet:compose -- --profile 10k
npm run loadnet:compose -- --profile soak
npm run loadnet:compose -- --profile restart
npm run loadnet:compose -- --profile pubsub-restart
npm run loadnet:compose -- --profile slow-consumers
docker build --pull=false -t cgp-loadnet:local -f loadnet/Dockerfile .
docker compose -f loadnet/docker-compose.generated.yml up -d
$collector = docker compose -f loadnet/docker-compose.generated.yml ps -q collector
docker wait $collector
docker compose -f loadnet/docker-compose.generated.yml logs collector
npx tsx loadnet/verify-summary.ts
docker compose -f loadnet/docker-compose.generated.yml down
```

Equivalent one-command runner:

```powershell
npm run loadnet:run -- --profile smoke
npm run loadnet:run -- --profile smoke-binary-v1
```

Argument order:

```text
profile scenario members messages subscribers publishMessages batchSize channels samplePages store
```

Profiles are labels only; explicit numeric arguments control the run. Use `storage` to benchmark LevelDB/index projections, `fanout` to benchmark websocket delivery, or `all` for both.

Useful flags:

| Flag | Purpose |
| --- | --- |
| `--fanout-parse=json` | Subscriber parses every delivered JSON frame. This approximates browser/client work. |
| `--fanout-parse=count` | Subscriber counts deliveries without JSON parsing. This isolates relay/network delivery cost. |
| `--wire-format=json` | Send websocket text JSON frames. This is the default compatibility path. |
| `--wire-format=binary-json` | Send websocket binary frames that still contain JSON-array frames. This isolates websocket text-vs-binary overhead. |
| `--wire-format=binary-v1` | Send compact binary frames with a fixed CGP header, one-byte frame-kind length, UTF-8 frame kind, and JSON payload bytes. This removes the outer JSON array parse from hot websocket paths while preserving JSON payload compatibility. |
| `--relays=N` | Start N local relays sharing a pubsub adapter and distribute subscribers across them. |

Docker loadnet profiles also support:

| Profile field | Purpose |
| --- | --- |
| `relayRestartEveryMs` | Host-side runner restarts one relay every interval while publishers/subscribers are still active. |
| `relayRestartCount` | Maximum relay restarts during the run. |
| `pubSubRestartEveryMs` | Host-side runner restarts the pubsub hub every interval while publishers/subscribers are still active. |
| `pubSubRestartCount` | Maximum pubsub hub restarts during the run. |
| `slowConsumerPercent` | Percentage of subscribers that intentionally delay live event processing. |
| `slowConsumerDelayMs` | Delay applied by slow subscribers before processing live event frames. |
| `wireFormat` | Optional profile-level websocket format: `json`, `binary-json`, or `binary-v1`. Applies to relay, client, and pubsub sockets in generated Docker loadnet runs. |
| `CGP_PUBSUB_RETAIN_ENVELOPES` | Per-topic retained pubsub log envelopes available for restarted relay catch-up. |
| `CGP_PUBSUB_WIRE_FORMAT` | Optional internal pubsub websocket format. Defaults to JSON and supports `binary-json` or `binary-v1` for high-throughput relay clusters. |
| `CGP_RELAY_PUBSUB_REPLAY_LIMIT` | Per-subscription cap on replayed pubsub envelopes requested by a relay. |


## Production Soak Profiles

The longer profiles in `loadnet/profiles` are for release confidence, not default CI. They are intentionally heavier than the baseline `smoke`, `1k`, `churn`, `restart`, `pubsub-restart`, and `slow-consumers` profiles.

| Profile | Duration | Stress pattern | Verification threshold |
| --- | --- | --- | --- |
| `soak-long` | 2h | WAN-like latency/loss with sustained subscriber churn. | `--min-delivery-ratio=1.0 --max-publisher-p99-ms=30000 --max-relay-head-lag=0` |
| `soak-chaos` | 3h | WAN-like latency/loss with sustained churn, forced relay restarts, forced pubsub restarts, and delayed consumers. | `--min-delivery-ratio=1.0 --max-publisher-p99-ms=30000 --max-relay-head-lag=0` |

Run them with:

```powershell
npm run loadnet:run -- --profile soak-long
npm run loadnet:run -- --profile soak-chaos
```

Verify the generated summary after the collector exits:

```powershell
npx tsx loadnet/verify-summary.ts --summary loadnet/results/summary-<id>.json --min-delivery-ratio=1.0 --max-publisher-p99-ms=30000 --max-relay-head-lag=0
```

Treat the run as failed if delivery is below `1.0`, if any relay-head conflict or signature error appears, if the collector does not finish, or if publisher batch `p99` exceeds `30000ms`.

## Current baseline on this workstation

These numbers were taken on April 20-21, 2026 with synthetic payload IDs and hash computation disabled so the run isolates store/index/fanout cost.

| Scenario | Workload | Result |
| --- | --- | --- |
| Storage | 100k members, 1M messages, 8 channels | 19.4k member appends/sec, 62.8k message appends/sec, 260MB RSS |
| Storage | 1M members, 2M messages, 16 channels | 14.1k member appends/sec, 44.7k message appends/sec, 265MB RSS |
| Fanout | 1 relay, 1,000 subscribers, 1,000 messages, JSON parse | 1M delivered events in 11.3s, about 88.6k delivered events/sec |
| Fanout | 1 relay, 1,000 subscribers, 1,000 messages, count-only | 1M delivered events in 6.3s, about 158.2k delivered events/sec |
| Fanout | 4 local relays, 1,000 subscribers, 1,000 messages, JSON parse | 1M delivered events in 7.1s, about 141.3k delivered events/sec |
| Fanout | 4 local relays, 1,000 subscribers, 1,000 messages, count-only | 1M delivered events in 5.3s, about 190.4k delivered events/sec |
| Fanout | 8 local relays, 1,000 subscribers, 1,000 messages, count-only | 1M delivered events in 5.4s, about 184.9k delivered events/sec |
| Fanout | 4 local relays, 8 channels, 1,000 channel-specific subscribers, 1,000 messages, JSON parse | 125k useful delivered events in 5.0s, about 25.0k useful delivered events/sec |
| Brutal gate storage | 100k members, 1M messages, 8 channels | 56.8k message appends/sec, 240MB RSS |
| Brutal gate fanout | 4 local relays, 1,000 subscribers, 1,000 messages, count-only | 1M delivered events in 9.7s, about 102.8k delivered events/sec |
| Docker loadnet smoke | 3 relays, 100 subscribers, 200 messages, 25ms latency/5ms jitter | 5,000/5,000 expected deliveries; every relay returned a valid signed head at seq 204 with the same head hash |
| Docker loadnet 1k | 4 relays, 1,000 subscribers, 2,000 messages, 40ms latency/10ms jitter/0.05% loss | 250,000/250,000 expected deliveries; every relay persisted seq 2008 with 250 messages/channel |
| Docker loadnet churn | 4 relays, 600 subscribers, 1,600 messages, 35ms latency/10ms jitter/0.02% loss/~1,370 reconnects | 120,000/120,000 expected deliveries; every relay persisted seq 1608 with 200 messages/channel |
| Docker loadnet restart | 4 relays, 200 subscribers, 2,000 messages, churn, packet loss, two forced relay restarts during active publishing | 50,000/50,000 expected deliveries; every relay returned a valid signed head at seq 2008 with the same head hash |
| Docker loadnet pubsub-restart | 4 relays, 200 subscribers, 1,600 messages, churn, packet loss, two forced pubsub hub restarts during active publishing | 40,000/40,000 expected deliveries; every relay returned a valid signed head at seq 1608 with the same head hash |
| Docker loadnet slow-consumers | 4 relays, 800 subscribers, 2,000 messages, 10% delayed consumers | 200,000/200,000 expected deliveries; every relay returned a valid signed head at seq 2008 with the same head hash |

Read-side samples stayed bounded in the 1M/2M run:

| Sample | Result |
| --- | --- |
| History tail pages | 100 pages in 114.1ms |
| Member page seeks | 100 pages in 65.2ms |
| Member search | 25 searches in 10.5ms |
| Random message refs | 100 reads in 10.0ms |

## Interpreting bottlenecks

- Storage writes are now the dominant bottleneck at million-scale local runs.
- Read paging, member search, and message-reference validation stayed bounded because they use indexed LevelDB projections instead of full-log scans.
- Channel history uses lightweight seq pointers rather than duplicating full event values into every channel index entry.
- Relay member search indexes identity, nickname, and roles in the hot membership projection. Bio-only full-text should live in a dedicated search index, not in the million-member membership write path.
- Fanout is inherently O(interested subscribers), so optimization should focus on partitioning, backpressure, batching, subscription-filter caching, and multi-process pubsub rather than pretending delivery can be O(1).
- Local fanout with JSON parsing measures subscriber-side websocket delivery and frame parse cost, not just relay send-loop cost.
- Count-only fanout shows the relay/network delivery ceiling before client parsing overhead.
- Local sharding helps both count-only delivery and JSON-parse delivery up to the point where websocket/subscriber overhead dominates. On this workstation, 4 local relay partitions beat 8.
- Channel-topic pubsub keeps channel-scoped events off guild-wide pubsub topics. In multi-channel runs, subscribers only receive useful channel traffic, so delivered-event totals are lower by design.
- Durable relay replication uses a separate guild log pubsub topic. Channel topics are optimized for live interested fanout; log topics are optimized for follower persistence and reconnect backfill.
- The websocket pubsub hub retains bounded log envelopes per topic and can persist them to `CGP_PUBSUB_RETAIN_DIR`. Restarted relays subscribe with `afterSeq` based on their local durable head, replay retained envelopes, and self-heal again when signed head gossip proves they are behind.
- Relay pubsub adapters keep unacknowledged publishes and resend them after reconnect. Pubsub ACK means the hub accepted the frame into its retention path, not merely parsed the websocket frame.
- If a relay is behind beyond retained pubsub windows, `GET_LOG_RANGE` and `npm run ops:relay -- sync-from-relay` provide a native anti-entropy repair path. Operators can repair in large contiguous pages and verify every event locally before append.
- If multiple peer relays are available, prefer `npm run ops:relay -- repair-from-quorum --db <db> --guild <guild> --relays <relay-a,relay-b>` so recovery chooses a source whose signed head matches the configured canonical quorum before requesting log ranges.
- Production load balancers and dashboards can use `/healthz`, `/readyz`, and `/metrics` on each relay. Metrics are intentionally low-cardinality so they remain usable under very large guild and subscriber counts.
- CGP event IDs are canonical over `seq`, `prevHash`, `createdAt`, `author`, and `body` only. Runtime `id` and `signature` fields must never affect event identity; otherwise follower relays reject replicated events.
- Distributed loadnet verification now queries signed `GET_HEAD` responses from every relay and fails on invalid signatures, request errors, or same-sequence head conflicts.
- `binary-json` is only a websocket frame-type variant. Use `binary-v1` to exercise the compact CGP frame envelope. Payload values remain JSON-compatible so existing relays and clients can support the fast path without introducing a second event schema.

## Useful variants

Measure event hashing cost:

```powershell
npm run load:relay -- 100k storage 100000 1000000 0 0 10000 8 100 level -- --compute-event-hashes
```

Keep the generated LevelDB directory for inspection:

```powershell
npm run load:relay -- 100k storage 100000 1000000 0 0 10000 8 100 level -- --keep-db
```

Write a JSON report:

```powershell
npx tsx scripts/relay-load-test.ts --profile=100k --scenario=storage --members=100000 --messages=1000000 --batch-size=10000 --channels=8 --sample-pages=100 --store=level --output=load-report.json
```

Verify a report against CI thresholds:

```powershell
npx tsx scripts/verify-load-report.ts --report=load-report.json
```

Run the full local gate:

```powershell
npm run ci:gate
```

Run the heavier local brutal gate:

```powershell
npm run gate:brutal
```

Include Docker churn/restart/slow-consumer distributed profiles in the brutal gate:

```powershell
npm run gate:brutal -- --with-docker
```

Run the conformance gate against the embedded reference relay:

```powershell
npm run conformance:relay
```

Run the same conformance gate against an already running relay:

```powershell
npm run conformance:relay -- --relay=ws://localhost:7447
```
