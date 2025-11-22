# Commons Guild Protocol (CGP)

> **CGP** is a tokenless, forkable, Discord-style chat protocol inspired by Bitcoin’s data structures (hash chains, Merkle trees) but without PoW, mining, or per-message fees.

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
  - Clients: browser WebSocket API + `ws` or Node’s built-in WebSocket client.   
  - P2P/fallback (optional): `js-libp2p` with WebRTC/WebSocket transports for browser-to-browser or browser↔Node connectivity.   

- **Storage**
  - Node: `level` or `better-sqlite3` as a simple KV/DB backend.
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
