# @cgp/relay-cloudflare

Cloudflare Workers deployment target for CGP relays.

This package is intentionally separate from `@cgp/relay`. The Node relay keeps
its `ws`, LevelDB, Helia, and plugin hot path unchanged. This Worker target uses
Cloudflare-native request handling, Durable Objects, WebSocket hibernation, and
Durable Object SQLite by default.

## Run Locally

```bash
npm install
npm run dev --workspace=@cgp/relay-cloudflare
```

Use a current Wrangler release. Older local `workerd` builds may warn and fall
back to their latest supported compatibility date while still serving the relay.

The local relay endpoint is:

```text
ws://localhost:8787/relay
```

For deterministic Durable Object sharding, clients can connect to:

```text
ws://localhost:8787/relay/guild/{guildId}
ws://localhost:8787/relay/bucket/{bucketId}
```

`/relay` remains the compatibility endpoint. New Cloudflare-aware clients should
prefer the guild-scoped URL when a socket is only carrying one guild.

## Deploy

```bash
npm run deploy --workspace=@cgp/relay-cloudflare
```

For stable signed relay heads, configure a private key secret before production
deployment:

```bash
npx wrangler secret put CGP_RELAY_PRIVATE_KEY_HEX --cwd packages/relay-cloudflare
```

Generate the value as 32 random bytes encoded as 64 lowercase hex characters.
If this secret is omitted, the Worker generates an ephemeral key per Durable
Object instance, which is fine for local testing but not for production quorum.

## Storage

Default storage is Durable Object SQLite. That works without creating a D1
database and keeps sequencing local to the Durable Object.

To use D1 instead:

1. Create the database.

   ```bash
   npx wrangler d1 create cgp-relay-db
   ```

2. Add the returned `database_id` to `wrangler.jsonc`.
3. Set `CGP_RELAY_STORAGE` to `d1`.

Durable Object SQLite is the recommended default for a single Worker relay.
D1 is useful when an operator wants SQL visibility or external read replicas.

## Supported Frames

The Worker relay supports the core client frames:

- `HELLO`
- `SUB`
- `GET_HISTORY`
- `GET_LOG_RANGE`
- `GET_STATE`
- `GET_HEAD`
- `GET_HEADS`
- `GET_MEMBERS`
- `SEARCH`
- `PUBLISH`
- `PUBLISH_TRANSIENT`
- `PUBLISH_BATCH`

It does not load Node relay plugins. Worker-safe plugin support should be added
as explicit Cloudflare bindings, not by importing `@cgp/relay/src/plugins.ts`.
