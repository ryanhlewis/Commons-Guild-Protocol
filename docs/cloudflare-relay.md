# Cloudflare Relay Target

CGP supports Cloudflare Workers through `@cgp/relay-cloudflare`.

The Worker target is a separate package so the normal Node relay is not slowed
down by Worker compatibility checks, Durable Object types, or Cloudflare
bundling constraints.

## Architecture

- `@cgp/relay` remains the full Node relay with LevelDB, pubsub fanout, Helia,
  media, static shard, push, mirror, and policy plugins.
- `@cgp/relay-cloudflare` is a Worker-native relay with:
  - `GET /relay` WebSocket upgrade.
  - Optional scoped WebSocket URLs for deterministic Durable Object sharding.
  - `RelayDO` Durable Object for socket hibernation and event sequencing.
  - Durable Object SQLite storage by default.
  - Optional D1 backing when configured by the operator.
  - Shared protocol primitives from `@cgp/core`.

## Local Development

```bash
npm install
npm run dev --workspace=@cgp/relay-cloudflare
```

Use a current Wrangler release. Older local `workerd` builds can warn and fall
back to their latest supported compatibility date during local development.

Connect clients to:

```text
ws://localhost:8787/relay
```

For Cloudflare deployments that need to avoid one global Durable Object, route
single-guild clients to a deterministic object:

```text
ws://localhost:8787/relay/guild/{guildId}
ws://localhost:8787/relay?guildId={guildId}
```

Operators can also use bucketed objects for regional or policy shards:

```text
ws://localhost:8787/relay/bucket/{bucketId}
ws://localhost:8787/relay?bucket={bucketId}
```

`/relay` remains as the compatibility endpoint for existing clients and tests.

## Production Deployment

```bash
npm run deploy --workspace=@cgp/relay-cloudflare
```

Set a stable relay signing key:

```bash
npx wrangler secret put CGP_RELAY_PRIVATE_KEY_HEX --cwd packages/relay-cloudflare
```

Without that secret, relay head signatures are ephemeral and unsuitable for
directory/quorum trust.

## Why This Is Separate

The Node relay imports Node-only dependencies such as `ws`, `http`, filesystem
storage, Helia/libp2p transports, `web-push`, process env configuration, and
child-process backed plugin paths. Importing that bundle into a Worker would
make Cloudflare builds fragile and would put Worker compatibility concerns on
the hot Node path.

The Worker package imports only `@cgp/core` plus a Worker-native store/adapter.
That keeps both deployments simple:

- Node relay: optimized server process.
- Cloudflare relay: Worker/DO/D1 deployment target.

## Scaling Model

Cloudflare Durable Objects are the right primitive for coordinated sockets and
strong event sequencing, but a single object should not carry the entire relay
fleet. The Worker therefore supports both modes:

- Compatibility mode: `/relay` maps to `global-relay`.
- Scaled mode: `/relay/guild/{guildId}` maps to one Durable Object per guild.
- Operator mode: `/relay/bucket/{bucketId}` maps to an explicit relay bucket.

The main Node relay does not import any of this code. Cloudflare-specific
routing, storage, and type constraints stay inside the Worker package.
