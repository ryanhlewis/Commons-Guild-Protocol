# Commons Guild Protocol (CGP)

> **CGP** is a tokenless, forkable, guild-based community chat protocol inspired by Bitcoin’s data structures (hash chains, Merkle trees) but without PoW, mining, or per-message fees.

CGP’s focus:

- **Guilds (servers)** and **channels** as first-class objects.
- **Append-only, signed logs** per guild (like a mini ledger).
- **Forkable governance**: communities can revolt and fork a guild at any time.
- **Open relays** and thin clients (works in browsers, Node, workers).
- **No blockchain requirements** – but optional anchoring to Bitcoin or other L1s is supported.

This repo contains:

- `SPEC.md` – the protocol and data model.
- `packages/core` – TypeScript reference implementation of CGP primitives.
- `packages/relay` – a Node.js WebSocket relay for guild logs.
- `packages/relay-cloudflare` – a Cloudflare Workers/Durable Objects relay target.
- `packages/client` – a browser/Node client library.
- `packages/tests` – a Vitest-based test suite.

CGP is **not**:

- A cryptocurrency.
- A pay-to-chat system.
- A “messages on the blockchain” system.

Instead, it borrows the **ledger idea** and applies it to community governance, while keeping actual messages off-chain.

---

## Quick Overview

- **Identity**: public-key based (secp256k1). No phone numbers, no central accounts.
- **Guild log**: each guild has an append-only, hash-linked log of events:
  - structure events (create guild, channels, roles),
  - content events (messages, edits, deletes),
  - governance events (bans, forks, checkpoints).
- **Directory**: multiple independent operators maintain **transparency logs** (Merkle-tree based) mapping human-readable guild handles to guild IDs. Clients query several and require agreement.
- **Relays**: WebSocket servers that store and serve guild logs.
- **Clients**: thin web/Node clients that sync only the guilds they care about, with configurable history windows and ephemeral channels.

---

## Relay plugins and client extensions (optional)

Relays may load optional plugins to integrate with external systems. When enabled, a relay can advertise plugin metadata in `HELLO_OK` (see `SPEC.md`), including optional client extension information.

The reference relay also ships with optional/default policy plugins for operational concerns such as rate limits, encryption envelope policy, safety reports, and the portable `org.cgp.apps` namespace for app manifests, slash commands, webhooks, and self-declared agent profiles. These are relay policy surfaces over signed CGP events, not new core account types.

Relay implementations may optionally expose plugin-provided extension bundles over HTTP at:

```
http://<relay-host>/extensions/<clientExtension>/index.js
```

This hosting is optional; clients may also load extensions from absolute URLs.

The `cgp.static-shards` plugin supports bounded discovery for large game catalogs:

```http
GET /plugins/cgp.static-shards/catalog?limit=64
GET /plugins/cgp.static-shards/catalog?limit=64&cursor=<opaque-next-cursor>
GET /plugins/cgp.static-shards/catalog?id=<exact-game-id>
GET /plugins/cgp.static-shards/catalog?q=<search-terms>&limit=24
```

Paged responses use schema version 2 and include `total`, `releases`, and `page` (`count`, `hasMore`, and `nextCursor`). They expose one stable discovery slot and the latest release for each game; historical releases remain in the registry for immutable serving and rollback but do not consume discovery pages or inflate `total`. A repeated upload of identical `game@version` bytes is idempotent, while different bytes for an existing version are rejected; publishers must choose a new version instead of changing content behind a cacheable URL. Page limits are clamped to 1-256 and search results to 1-64. Exact lookup is O(1); search uses the relay's bounded token/prefix index rather than scanning the registry. Cursors are opaque to clients and remain stable when games publish newer releases or new games are appended. Calling `catalog` without paging parameters retains the version-1 full-registry response for compatibility. Registry writes use an append-only NDJSON journal and periodically create an atomic JSON snapshot (`CGP_STATIC_SHARD_REGISTRY_COMPACT_EVERY`, default 4096), so publishing a release does not rewrite the complete catalog.

Public HTTP uploads are self-authenticating. New uploads must include a `cgp/static-shard-publisher/1` proof by default, and the first accepted account public key owns that game ID on the relay. Every later version must be authorized by the same CGP account. A root key can sign directly, or `publisher.deviceAuthorization` can carry a root-authorized device certificate with the `publish` capability; the account key remains the owner in either case. Keep ownership with the publisher's recoverable CGP account rather than generating a new key per release:

```ts
import { getPublicKey, hashObject, sign } from "@cgp/core";
import {
  STATIC_SHARD_PUBLISHER_PROTOCOL,
  staticShardReleaseSigningPayload,
} from "@cgp/relay";

const publicKey = getPublicKey(publisherPrivateKey);
const unsigned = {
  ...release,
  publisher: { protocol: STATIC_SHARD_PUBLISHER_PROTOCOL, publicKey },
};
const signature = await sign(
  publisherPrivateKey,
  hashObject(staticShardReleaseSigningPayload(unsigned)),
);
const signedRelease = {
  ...unsigned,
  publisher: { ...unsigned.publisher, signature },
};
```

`POST /plugins/cgp.static-shards/upload` accepts the signed release plus its manifest/shard bytes without a relay credential. `POST /plugins/cgp.static-shards/ingest` is an operator-only outbound URL fetch and is closed unless `CGP_STATIC_SHARD_INGEST_TOKEN` is configured (Bearer or `X-CGP-Static-Shard-Token`) or the explicitly unsafe compatibility flag `CGP_STATIC_SHARD_ALLOW_UNAUTHENTICATED_INGEST=1` is set. Configured `CGP_STATIC_SHARD_SEED_URLS` remain operator-trusted so existing unsigned mirrors continue to load as `legacy-operator-seed` entries.

---

## Status

- **Spec:** draft 0.1.
- **Implementation:** intended to be TypeScript-first, browser- and Node-compatible.

---

## Technology choices (reference implementation)

We use well-maintained, audited libraries wherever possible:

- **Crypto**
  - `@noble/secp256k1` – pure JS/WASM secp256k1 implementation, small and fast.   
  - `@noble/hashes/sha256` – fast, audited SHA‑256.   

- **Merkle trees**
  - `merkletreejs` – standard Merkle trees with TS types, SHA‑256, proofs.   
  - (Optional) `sparse-merkle-tree` for directory logs if we want sparse key space.   

- **Serialization**
  - JSON + a canonical JSON serializer such as `fast-json-stable-stringify` or `safe-stable-stringify` for deterministic hashing.   
  - Optional: MessagePack/CBOR (`msgpackr` or `cbor-x`) for more compact frames later.   

- **Networking**
  - Relays: Node.js + `ws` for WebSocket server.   
  - Cloudflare target: Workers + Durable Objects via `@cgp/relay-cloudflare`.
  - Clients: browser WebSocket API + `ws` or Node’s built-in WebSocket client.   
  - P2P/fallback (optional): `js-libp2p` with WebRTC/WebSocket transports for browser-to-browser or browser↔Node connectivity.   

- **Storage**
  - Node: `level` or `better-sqlite3` as a simple KV/DB backend.
  - Cloudflare: Durable Object SQLite by default, optional D1.
  - Browser: IndexedDB via `idb`.

- **Testing**
  - `vitest` as a fast, TS-native test runner.   

---

## Monorepo layout

```text
.
├── README.md
├── SPEC.md
└── packages
    ├── core       # types, crypto, event hashing/validation, log logic
    ├── relay      # Node WebSocket relay implementation
    ├── relay-cloudflare # Cloudflare Workers/Durable Objects relay target
    ├── client     # browser/Node client SDK
    └── tests      # Vitest test suite
```

---

## Getting started (dev)

```bash
# clone
git clone https://github.com/your-org/cgp.git
cd cgp

# install deps
pnpm install

# run tests
pnpm test

# start a local relay
pnpm -C packages/relay dev

# run an example client
pnpm -C packages/client dev
```

See `SPEC.md` for protocol details.
