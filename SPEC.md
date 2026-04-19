# Commons Guild Protocol (CGP) Specification (Draft 0.1)

## 1. Goals and non-goals

**Goals**

- Provide a **tokenless**, **PoW-less** protocol for Discord-style guilds:
  - guilds / channels / roles,
  - edits, deletes, ephemeral channels,
  - moderation & bans,
  - community-controlled forks.
- Use **hash-linked, signed logs** for tamper-evident history.
- Provide a **directory mechanism** so clients can find guilds in a tamper-evident way.
- Allow **multiple independent relays** and **optional P2P** transport.
- Be **implementable on web + Node** with minimal dependencies.

**Non-goals**

- Strong global consensus (no "single canonical world state").
- Strong delivery guarantees (no exactly-once; we aim for **eventual consistency** given honest relays).
- On-chain message storage. Only optional anchoring of small hashes to external L1s.
- Mandatory end-to-end encryption, key escrow, or relay-visible decryption.
- Strong metadata privacy without additional transports/plugins.

---

## 2. Cryptographic primitives

### 2.1 Keys

- Curve: **secp256k1** (same as Bitcoin and Nostr).
- Library (reference implementation): `@noble/secp256k1`.   
- Public key: 32-byte compressed key, encoded as:
  - binary: `Uint8Array(33)` in memory,  
  - over the wire: **hex** or **base64url** string (CGP-0.1 recommends hex).

### 2.2 Signatures

- Signature scheme: ECDSA or Schnorr over secp256k1 (reference impl uses noble's ECDSA first; Schnorr may be added later).
- Sign/verify functions live in `@cgp/core/crypto`.

### 2.3 Hashing

- Hash function: **SHA-256**.
- Library: `@noble/hashes/sha256`.   
- Used for:
  - event IDs: `event_id = SHA256(canonical_encode(event_without_id_sign))`
  - log `prev_hash`,
  - Merkle trees in directory logs.

### 2.4 Canonical encoding

To hash events deterministically we need canonical JSON:

- Use a stable stringify implementation such as `fast-json-stable-stringify`, `safe-stable-stringify`, or `json-stable-stringify`.   
- Requirements:
  - sort object keys lexicographically,
  - stable serialization of arrays, numbers, and strings,
  - no environment-dependent differences.

**Normative rule:**  
Implementations **must** use a deterministic encoding when computing hashes. The specific library is not mandated as long as it produces identical output for identical structures.

---

## 3. Data model

### 3.1 Identity

```ts
type PublicKeyHex = string;  // lowercase hex, 66 chars including "02"/"03" prefix
type SignatureHex = string;
type HashHex = string;       // lowercase hex
type GuildId = HashHex;      // stable collision-resistant guild identifier
type ChannelId = HashHex;    // stable collision-resistant channel identifier
type UserId = PublicKeyHex;  // identity == pubkey
```

* A **user** is any entity that controls a secp256k1 private key.
* A **guild** is defined by its first event (`GUILD_CREATE`); its `guildId` is the target log ID chosen
  by the creator. Implementations SHOULD derive it from a collision-resistant genesis preimage such as
  `(author, createdAt, random nonce, metadata)`, not from the final event that embeds `guildId` itself.
* A **channel** is defined by its `CHANNEL_CREATE` event; its `channelId` is stable within the guild and
  relays MUST reject duplicate channel IDs in the same guild.

### 3.2 Guild log

A guild log is a sequence of **events**:

```ts
interface GuildEventBodyBase {
  type: string;     // e.g. "GUILD_CREATE", "MESSAGE", ...
  guildId: GuildId; // target guild log
  // plus event-type-specific fields
}

interface GuildEvent {
  id: HashHex;                // SHA256 of canonical encoding of `unsigned`
  seq: number;                // monotonically increasing integer >= 0
  prevHash: HashHex | null;   // null for seq=0, otherwise hash of previous event
  createdAt: number;          // milliseconds since epoch (informational)
  author: UserId;             // public key of signer
  body: GuildEventBodyBase & Record<string, any>;
  signature: SignatureHex;    // signature over canonical encoding of {body, author, createdAt}
}
```

**Guild invariants (per log):**

* `seq` starts at 0 and increments by 1.
* For `seq = 0`, `prevHash = null`.
* For `seq > 0`, `prevHash = events[seq - 1].id`.
* `id` must equal `SHA256(canonicalEncode(unsignedEvent))` where `unsignedEvent` excludes `id` and `signature`.
* `signature` must verify under `author`.

Relays and clients **must** reject events that violate these invariants.

### 3.3 Event types (core)

Minimal set for CGP-0.1:

```ts
type EventBody =
  | GuildCreate
  | GuildMetadataUpdate
  | ChannelCreate
  | ChannelMetadataUpdate
  | RoleAssign
  | RoleRevoke
  | BanUser
  | UnbanUser
  | Message
  | EditMessage
  | DeleteMessage
  | AppObjectUpsert
  | AppObjectDelete
  | ForkFrom
  | Checkpoint
  | EphemeralPolicyUpdate;
```

Some key ones:

```ts
interface GuildCreate {
  type: "GUILD_CREATE";
  // guildId is the target log ID and MUST remain stable for every event in this guild.
  guildId: GuildId;
  name: string;
  description?: string;
  access?: "public" | "private";
  policies?: {
    // "public" allows any non-banned key to publish user-authored channel/app events.
    // "members" requires explicit membership even when the guild profile is public.
    posting?: "public" | "members";
  };
  flags?: {
    allowForksBy?: "any" | "mods" | "owner-only"; // advisory, not enforced by protocol
  };
}

interface ChannelCreate {
  type: "CHANNEL_CREATE";
  guildId: GuildId;
  channelId: ChannelId;
  name: string;
  kind: "text" | "voice" | "ephemeral-text";
  retention?: {
    // advisory (relays may enforce)
    mode: "infinite" | "rolling-window" | "ttl";
    days?: number;   // for rolling-window
    seconds?: number; // for ttl
  };
}

interface Message {
  type: "MESSAGE";
  guildId: GuildId;
  channelId: ChannelId;
  messageId: HashHex; // stable client-chosen id; SHOULD hash a collision-resistant message preimage
  content: string;    // UTF-8 text; later: support attachments
  replyTo?: HashHex;  // messageId of parent message
}

interface EditMessage {
  type: "EDIT_MESSAGE";
  guildId: GuildId;
  channelId: ChannelId;
  messageId: HashHex;
  newContent: string;
}

interface DeleteMessage {
  type: "DELETE_MESSAGE";
  guildId: GuildId;
  channelId: ChannelId;
  messageId: HashHex;
  reason?: string;
}

interface ForkFrom {
  type: "FORK_FROM";
  guildId: GuildId;       // new guild’s ID
  parentGuildId: GuildId;
  parentSeq: number;
  parentRootHash: HashHex; // hash of parent event at parentSeq
  note?: string;
}
```

Other governance events (roles, bans, checkpoints) follow similarly.

### 3.3.1 App-scoped objects

CGP core intentionally keeps channel history small: messages, edits, deletes, membership, roles, bans,
channels, and retention are first-class. Product features such as pins, read states, bookmarks,
third-party moderation annotations, and client-specific indexes SHOULD NOT be added as new core event
types unless they affect portable guild semantics.

Clients and relay plugins MAY instead use app-scoped object events:

```ts
interface AppObjectTarget {
  channelId?: ChannelId;
  messageId?: HashHex;
  userId?: UserId;
  // app/plugin-specific target keys
}

interface AppObjectUpsert {
  type: "APP_OBJECT_UPSERT";
  guildId: GuildId;
  namespace: string;  // reverse-DNS or relay/plugin namespace
  objectType: string; // app-defined kind, e.g. "message-pin"
  objectId: string;   // stable app-defined id inside namespace/objectType
  channelId?: ChannelId;
  target?: AppObjectTarget;
  value?: unknown;    // app-defined JSON value
}

interface AppObjectDelete {
  type: "APP_OBJECT_DELETE";
  guildId: GuildId;
  namespace: string;
  objectType: string;
  objectId: string;
  channelId?: ChannelId;
  target?: AppObjectTarget;
}
```

Relays MUST validate that the author can participate in the guild, that referenced channels exist, and
that referenced messages are live. Object semantics and finer permissions are owned by the namespace
and MAY be enforced by relay plugins or clients. Unknown namespaces must not change core guild state.

### 3.3.2 Admission, posting policy, and baseline anti-abuse

CGP identities are self-generated public keys. The base protocol is therefore not Sybil-resistant by
identity alone. Sybil resistance comes from guild and relay policy:

* `access: "private"` requires membership for reads and writes.
* `policies.posting: "members"` requires membership for user-authored publish events while still allowing
  a public guild profile or directory listing.
* `policies.posting: "public"` preserves open community behavior but must be paired with relay-side
  moderation, proof challenges, reputation, invite gates, or plugin policy for large public guilds.

Reference relays MUST reject writes from banned users and MUST enforce membership when `access` is private
or `policies.posting` is `members`. Sybil resistance, rate limiting, proof challenges, reputation,
captcha/WebAuthn gates, invite gates, and spam scoring are relay or guild policy, not canonical CGP state.
Relays MAY advertise those policies as plugins in `HELLO_OK.plugins`; these plugins do not change the core
event format and MUST NOT be represented as top-level core protocol fields.

The reference relay ships with a default plugin named `cgp.relay.rate-limit` that enforces per-socket,
per-author, and per-guild publish buckets. That plugin is a recommended operational default, not a
mandatory part of CGP-0.1. Compatible relays may disable it, replace it, or combine it with other policy
plugins as long as they still enforce the core authorization rules above.

### 3.4 Canonical state ("UTXO of messages")

For any guild, a **canonical view** of a channel at time `T` is computed by scanning the log up to `seq <= S` where `S` is last known, applying rules:

* A **live message** is one where:

  * there exists a `MESSAGE` event with `messageId = X`,
  * and there is no `DELETE_MESSAGE` with that `messageId` after it,
  * and (for ephemeral channels) it is not past TTL/retention window.
* Displayed content for `messageId` is from the last `EDIT_MESSAGE` (if any) before `S`.

This is analogous to a UTXO set:

* `MESSAGE` "mints" a message,
* `DELETE_MESSAGE` "spends" it,
* `EDIT_MESSAGE` updates its **view**, not its existence.

State derivation is local and deterministic given the log; clients are free to index/cache it.

---

## 4. Directory / discovery

### 4.1 Directory entry

Directory logs map a **guild handle** (like `@soulsborne/pvp`) to a guild descriptor:

```ts
type GuildHandle = string; // e.g. "soulsborne/pvp"

interface DirectoryValue {
  guildId: GuildId;
  guildPubkey: PublicKeyHex;
  lastSeenRootHash: HashHex; // latest known guild log hash at some seq
  metadata: {
    name: string;
    description?: string;
    tags?: string[];
    createdAt: number;
  };
}
```

### 4.2 Directory log structure

Each directory operator maintains a Merkle tree over `(handle, value)` pairs:

```ts
interface DirectoryEntry {
  handle: GuildHandle;
  value: DirectoryValue;
}

interface DirectoryLogSnapshot {
  rootHash: HashHex;
  size: number; // number of entries / operations
  createdAt: number;
  signature: SignatureHex; // signed by directory operator key
}
```

* Library: `merkletreejs` with SHA‑256 for Merkle roots and inclusion proofs.

**Operations:**

* `SET(handle, value)` – add or update an entry.
* `DELETE(handle)` – marks an entry as deleted (optional; some directories may be append-only with tombstones).

Clients can request:

* **Inclusion proof**: proof that `(handle, value)` is in the tree for a given `rootHash`.
* **Consistency proof**: proof that a later root extends an earlier root (no rewrite).

Clients **should** consult multiple directory operators and require:

* at least `M` of `K` to agree on `(handle → guildId, guildPubkey)`,
* monotonic roots over time.

### 4.3 Optional anchoring

Directory operators **may** periodically commit their `rootHash` to an external ledger (e.g. Bitcoin via OP_RETURN) as an additional timestamp / immutability anchor.

CGP does **not** require this; it is an optional hardening.

---

## 5. Wire protocol (relays)

CGP-0.1 defines a simple WebSocket-based relay protocol.

### 5.1 Transport

* Default: WebSocket over TLS (`wss://`).
* Frames are **JSON arrays** to keep parsing extremely simple (à la Nostr).

All frames:

```ts
// Over the wire (JSON array)
type Frame =
  | ["HELLO", HelloFrame]
  | ["HELLO_OK", HelloOkFrame]
  | ["ERROR", ErrorFrame]
  | ["EVENT", GuildEvent]             // server → client: new event
  | ["PUBLISH", PublishFrame]         // client -> server: submit pre-sequenced event
  | ["SUB", SubFrame]                 // client → server: subscribe
  | ["UNSUB", UnsubFrame]             // client → server: unsubscribe
  | ["SNAPSHOT", SnapshotFrame];      // server → client: initial events
```

```ts
interface PublishFrame {
  body: EventBody;
  author: UserId;
  createdAt: number;
  signature: SignatureHex; // sign(SHA256(canonicalEncode({ body, author, createdAt })))
}
```

### 5.2 HELLO handshake

On connection:

* Client sends:

```ts
interface HelloFrame {
  protocol: "cgp/0.1";
  clientName?: string;
  clientVersion?: string;
  auth?: {
    // optional: prove control of a key for privileged ops later
    user: UserId;
    challenge: string;       // random nonce from server OR pre-configured
    signature: SignatureHex; // sign(challenge)
  };
}
```

* Server replies:

```ts
interface HelloOkFrame {
  protocol: "cgp/0.1";
  relayName?: string;
  relayVersion?: string;
  features?: string[]; // e.g. ["ephemeral", "checkpoints", "directory-cache"]
  plugins?: RelayPluginDescriptor[];
}
```

If protocol version is unsupported, server sends:

```ts
interface ErrorFrame {
  code: string;   // e.g. "UNSUPPORTED_PROTOCOL"
  message: string;
}
```

### 5.3 Subscriptions

Clients subscribe to guild logs and/or channels:

```ts
interface SubFrame {
  subId: string;            // client-chosen subscription id
  guildId: GuildId;
  channels?: ChannelId[];   // optional filter
  fromSeq?: number;         // inclusive
  limit?: number;           // max events in initial snapshot
}
```

Server responds:

* A `SNAPSHOT` containing existing events that match:

```ts
interface SnapshotFrame {
  subId: string;
  guildId: GuildId;
  events: GuildEvent[];
  endSeq: number;         // highest seq included
}
```

* Then later `EVENT` frames whenever new events for that guild arrive:

```ts
// ["EVENT", GuildEvent]
```

Clients can cancel:

```ts
interface UnsubFrame {
  subId: string;
}
```

Servers **should** garbage-collect inactive subscriptions after a timeout.

### 5.4 Publishing events

To send a message / governance action:

1. Client fills a `GuildEvent` with:

   * `seq = null` (or omitted),
   * `prevHash = null` (or omitted),
   * `id = null` (or omitted).
2. Client signs the **event payload** excluding seq/prevHash/id (or signs a canonical “unsigned” representation – details in `@cgp/core`).
3. Client sends:

```ts
["PUBLISH", unsignedOrPartiallySignedEvent]
```

Server behavior:

* Atomically assigns:

  * `seq = lastSeq + 1`,
  * `prevHash = lastEvent.id` (or `null` for first),
* Recomputes `id = SHA256(canonicalEncode(unsignedEvent with seq/prevHash))`.
* Verifies the signature using `author`.
* If valid:

  * appends event to log,
  * broadcasts `["EVENT", fullEvent]` to subscribers.
* If invalid:

  * responds with `["ERROR", { code: "INVALID_EVENT", message: "..." }]` to sender.

**Note:** this makes relay responsible for sequencing. In P2P fallback mode, a temporary “sequencer” peer plays the same role.

---

### 5.5 Relay plugins (optional extension)

CGP core does not require relay plugins, but relays may advertise optional plugins for integration features.
If supported, `HELLO_OK` may include a `plugins` array:

```ts
interface RelayPluginDescriptor {
  name: string;
  metadata?: PluginMetadata;
  inputs?: PluginInputSchema[];
}

interface PluginMetadata {
  name: string;
  description?: string;
  icon?: string;
  version?: string;
  policy?: Record<string, unknown>; // plugin-defined policy/status metadata
  clientExtension?: string;
  clientExtensionDescription?: string;
  clientExtensionUrl?: string;
  clientExtensionRequiresBrowserExtension?: boolean;
  clientExtensionBrowserInstallUrl?: string;
  clientExtensionBrowserInstallLabel?: string;
  clientExtensionBrowserInstallHint?: string;
}

interface PluginInputSchema {
  name: string;
  type: "string" | "number" | "boolean" | "object";
  required: boolean;
  sensitive?: boolean;
  description?: string;
  placeholder?: string;
  scope?: "relay" | "client" | "both";
}
```

Clients may configure a plugin by sending:

```ts
["PLUGIN_CONFIG", { pluginName: string; config: any }]
```

Relays acknowledge with:

```ts
["PLUGIN_CONFIG_OK", { pluginName: string }]
```

or an `["ERROR", { code: "PLUGIN_NOT_FOUND" | "PLUGIN_CONFIG_ERROR", message: string }]`.

If `clientExtensionUrl` is relative, clients resolve it against the relay HTTP origin (if the relay serves extension bundles).
If `clientExtensionUrl` is absolute, clients fetch directly. HTTP hosting of extension bundles is optional and not part of the WebSocket protocol.
Relay implementations may also expose plugin-provided static assets under `/extensions/{clientExtension}/...` as a convenience.

Reserved reference plugin names:

* `cgp.relay.rate-limit`: relay-local publish buckets or equivalent anti-spam policy. This is a
  recommended reference plugin, not core CGP state.
* `cgp.security.encryption-policy`: relay-local policy that can require or reject encrypted MESSAGE
  envelopes for selected guilds/channels. This plugin must not receive plaintext keys and must not act
  as a key server.

---

## 6. P2P mode (optional extension)

In pure P2P mode (no relay reachable):

* Peers run a **libp2p node**:

  * use WebRTC/WebSocket transports for browser↔browser and browser↔Node connectivity.
* Use a libp2p protocol ID `"/cgp/0.1"`:

  * frames are exactly the same JSON arrays as the relay protocol,
  * but there is no central sequencer.

To avoid conflicting `seq` assignments:

* One peer is chosen as **temporary sequencer** (e.g., lowest key hash, or out-of-band).
* Other peers send PUBLISH-equivalent frames to the sequencer peer.
* Sequencer:

  * assigns `seq`/`prevHash`,
  * broadcasts events to others,
  * later can upload whole log to a relay when available.

Full P2P consensus, CRDTs, etc. can be added later; CGP-0.1 intentionally keeps P2P simple and best-effort.

---

## 7. Retention, ephemerality, and relay behavior

CGP doesn’t force hard rules but defines **recommended defaults**:

### 7.1 Channel retention policies

From `ChannelCreate.retention`:

* `mode: "infinite"`:

  * Relays *should* store full history until out of space.
* `mode: "rolling-window"` + `days`:

  * Relays *should* keep only last N days of events for this channel.
* `mode: "ttl"` + `seconds`:

  * Relays *should* drop messages older than TTL; clients should not expect them to be available.

Phones / thin clients:

* *may* keep smaller local windows (e.g. last 7 days) regardless of channel policy.

### 7.2 How long peers relay messages

Relays:

* Keep event logs on disk for as long as their operator wants, ideally honoring channel retention metadata.
* In-memory caches:

  * hold recent events (e.g. last few thousand per guild) for low-latency fan-out.
* For ephemeral relays (e.g. serverless functions):

  * they can act as **stateless frontends** reading/writing from a backing KV/DB (e.g. Cloudflare KV/D1).

Peers (clients):

* Are **not required** to relay for others.
* May optionally:

  * act as P2P "soft-relays" for small guilds where all members agree,
  * cache logs for offline use and share them with other clients during connection.

---

## 8. Guarantees

**Security / integrity**

* Any modification of a guild log’s history is **detectable** to any client that:

  * previously saw a `rootHash` or checkpoint event,
  * compares new logs and sees inconsistent `prevHash` or `id`s.
* Any directory re-write is detectable when clients:

  * receive conflicting Merkle roots or inclusion proofs from different directory operators.

**Consistency**

* Given at least one honest relay per guild and eventual connectivity, all honest clients will converge to the same log prefix.
* Conflicting histories (forks) are explicit and visible; clients can show them and let users choose.

**Privacy**

CGP-0.1 guarantees log integrity, not metadata privacy. By default, relays can observe:

* the network endpoint that connects to them,
* which guilds and channels a client subscribes to,
* event authors, event types, event sizes, timing, sequence numbers, and public guild/channel IDs,
* directory lookups made directly against a directory operator.

Payload confidentiality is an optional end-to-end client capability, not a relay requirement. Clients may
publish opaque ciphertext in message payloads or app-defined envelopes; relays MUST NOT need plaintext or
decryption keys to sequence or replicate those events. Relays MAY advertise an optional policy plugin such
as `cgp.security.encryption-policy` to require encrypted MESSAGE envelopes for selected guilds/channels,
or to reject encrypted payloads on relays that require plaintext moderation. Such a plugin verifies only
envelope shape and local relay policy; it is not a key server and does not make E2EE part of core relay
state.

Metadata privacy requires additional client/relay extensions. Recommended mitigations include TLS,
Tor/I2P/proxy transports, subscribing to broader guild snapshots and filtering locally, batching
subscriptions, padding event sizes, delaying publishes, pseudonymous per-guild keys, and private directory
lookup mechanisms. Strong metadata privacy via PIR, mixnets, or oblivious relays is intentionally out of
scope for CGP-0.1 because it changes latency, cost, and operational assumptions.
