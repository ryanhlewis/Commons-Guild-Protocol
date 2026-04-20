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
  | ["SNAPSHOT", SnapshotFrame]       // server -> client: initial events/history page
  | ["GET_STATE", StateRequestFrame]  // client -> server: canonical state read
  | ["GET_HISTORY", HistoryRequestFrame] // client -> server: paginated history read
  | ["SEARCH", SearchRequestFrame]    // client -> server: permission-filtered search
  | ["SEARCH_RESULTS", SearchResultsFrame]; // server -> client: search response
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
  kind: "text" | "voice" | "ephemeral-text" | string;
  categoryId?: string;
  description?: string;
  topic?: string;
  position?: number;
  permissionOverwrites?: PermissionOverwrite[];
  retention?: {
    // advisory (relays may enforce)
    mode: "infinite" | "rolling-window" | "ttl";
    days?: number;   // for rolling-window
    seconds?: number; // for ttl
  };
}

interface PermissionOverwrite {
  id: string;              // role id, guild id/@everyone, or user public key
  kind: "role" | "member";
  allow?: string[];
  deny?: string[];
}

interface Message {
  type: "MESSAGE";
  guildId: GuildId;
  channelId: ChannelId;
  messageId: HashHex; // stable client-chosen id; SHOULD hash a collision-resistant message preimage
  content: string;    // UTF-8 text or opaque ciphertext when encrypted=true
  replyTo?: HashHex;  // messageId of parent message
  attachments?: AttachmentRef[];
  iv?: string;         // present for encrypted envelopes
  encrypted?: boolean;
  external?: unknown;  // app-defined metadata; ignored by core state rules
}

interface AttachmentRef {
  id?: string;
  url?: string;        // HTTP(S), content-addressed URI, or app-defined opaque locator
  type?: "image" | "video" | "audio" | "file" | string;
  name?: string;
  mimeType?: string;
  size?: number;
  width?: number;
  height?: number;
  hash?: HashHex;      // hash of bytes when the publisher wants integrity binding
  encrypted?: boolean; // true when content/url is an opaque application-level envelope
  scheme?: string;
  iv?: string;
  content?: string;    // ciphertext or app-defined attachment envelope
  external?: unknown;
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
  guildId: GuildId;       // new guildâ€™s ID
  parentGuildId: GuildId;
  parentSeq: number;
  parentRootHash: HashHex; // hash of parent event at parentSeq
  note?: string;
}

interface Checkpoint {
  type: "CHECKPOINT";
  guildId: GuildId;
  rootHash: HashHex;          // hash(canonical(SerializableGuildState))
  seq: number;                // MUST match the enclosing GuildEvent.seq
  state: SerializableGuildState;
}
```

Other governance events (roles and bans) follow similarly. `CHECKPOINT` is relay-maintained:
clients MUST NOT publish it through `PUBLISH`. A checkpoint does not mutate guild state; it anchors
the current serialized state so relays and clients can rebuild from the checkpoint event plus the
tail events after it instead of replaying an entire log.

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

The reference namespace `org.cgp.apps` is reserved for portable app, bot, slash-command, and webhook
records. It is still an app-scoped namespace, not a new core user class:

* `app-manifest`: durable app metadata such as name, description, homepage, optional `endpoint`,
  optional `credentialRef`, and command summaries. Command summaries MAY include an `options` array
  using the same schema as `slash-command`.
* `slash-command`: a portable command registration. Command records MAY include an `endpoint` and
  `credentialRef` for relay-local or app-runtime command delivery. Command records MAY include an
  `options` array with up to 25 entries. Each option has a lowercase `name`, a `type` of `string`,
  `integer`, `number`, `boolean`, `user`, `channel`, `role`, or `mentionable`, optional
  `description`, optional `required`, optional `autocomplete`, and optional `choices`.
* `command-invocation`: a durable execution request for a registered slash command. Public channel
  responses SHOULD be normal `MESSAGE` events signed by the app/bot identity; private acknowledgements
  or "only visible to you" responses MAY use app objects and client-local rendering. Invocations MAY
  include an `options` object containing parsed scalar option values. The `cgp.apps.surface-policy`
  plugin SHOULD reject invocations with missing required options, invalid scalar types, or unsupported
  choices when the registered command declared an option schema.
* `command-response`: an optional app-scoped command receipt/response object for ephemeral or
  app-private responses. Relays SHOULD treat response visibility as advisory unless an application
  plugin enforces delivery rules. Bot runtimes MAY include `error: true` and a short `code` such as
  `INVALID_OPTIONS`, `COMMAND_ERROR`, or `COMMAND_TIMEOUT` for client-visible command failures.
* `webhook`: a webhook registration. Public log values SHOULD contain `credentialRef`, not plaintext
  secrets, tokens, passwords, or private keys.
* `agent-profile`: a self-declared profile for automated users or agents. Bots remain normal CGP
  identities that sign normal events; clients may display a bot badge when this record, member metadata,
  or the display name indicates automation.

Relays MAY advertise `cgp.apps.surface-policy` to validate these records and require role permissions
such as `manage_apps`, `manage_integrations`, or `manage_webhooks`. Relays SHOULD NOT grant bots any
implicit privilege merely because they declare themselves as bots or agents.

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

The reference relay ships with default plugins named `cgp.relay.rate-limit` and
`cgp.relay.abuse-controls`. These enforce publish buckets, message-size checks, mention flood checks,
duplicate-message throttles, and command invocation burst limits. They are recommended operational
defaults, not mandatory parts of CGP-0.1. Compatible relays may disable, replace, or combine them with
other policy plugins as long as they still enforce the core authorization rules above.

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

* Library: `merkletreejs` with SHAâ€‘256 for Merkle roots and inclusion proofs.

**Operations:**

* `SET(handle, value)` â€“ add or update an entry.
* `DELETE(handle)` â€“ marks an entry as deleted (optional; some directories may be append-only with tombstones).

Clients can request:

* **Inclusion proof**: proof that `(handle, value)` is in the tree for a given `rootHash`.
* **Consistency proof**: proof that a later root extends an earlier root (no rewrite).

Clients **should** consult multiple directory operators and require:

* at least `M` of `K` to agree on `(handle â†’ guildId, guildPubkey)`,
* monotonic roots over time.

### 4.3 Optional anchoring

Directory operators **may** periodically commit their `rootHash` to an external ledger (e.g. Bitcoin via OP_RETURN) as an additional timestamp / immutability anchor.

CGP does **not** require this; it is an optional hardening.

---

## 5. Wire protocol (relays)

CGP-0.1 defines a simple WebSocket-based relay protocol.

### 5.1 Transport

* Default: WebSocket over TLS (`wss://`).
* Frames are **JSON arrays** to keep parsing extremely simple (Ã  la Nostr).

All frames:

```ts
// Over the wire (JSON array)
type Frame =
  | ["HELLO", HelloFrame]
  | ["HELLO_OK", HelloOkFrame]
  | ["ERROR", ErrorFrame]
  | ["EVENT", GuildEvent]             // server â†’ client: new event
  | ["PUBLISH", PublishFrame]         // client -> server: submit pre-sequenced event
  | ["SUB", SubFrame]                 // client â†’ server: subscribe
  | ["UNSUB", UnsubFrame]             // client â†’ server: unsubscribe
  | ["SNAPSHOT", SnapshotFrame]       // server -> client: initial events/history page
  | ["GET_STATE", StateRequestFrame]  // client -> server: canonical state read
  | ["GET_HISTORY", HistoryRequestFrame] // client -> server: paginated history read
  | ["SEARCH", SearchRequestFrame]    // client -> server: permission-filtered search
  | ["SEARCH_RESULTS", SearchResultsFrame]; // server -> client: search response
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
  author?: PublicKeyHex;    // optional signed read identity
  createdAt?: number;
  signature?: SignatureHex; // signature over canonical({ kind: "SUB", payloadWithoutSignature })
}
```

Public guilds MAY serve public channel snapshots without read authentication. Private guilds and channels
hidden by permission overwrites require a valid signed read identity. Relays MUST filter `SNAPSHOT`,
`STATE`, `GET_HISTORY`, and live `EVENT` fan-out to the channels visible to the authenticated reader.
Banned or kicked users MAY receive the moderation event that removes them so clients can update local
state, but relays MUST NOT continue serving private guild history after removal.

Server responds:

* A `SNAPSHOT` containing existing events that match:

```ts
interface SnapshotFrame {
  subId: string;
  guildId: GuildId;
  events: GuildEvent[];
  endSeq: number;         // current relay log head
  endHash?: HashHex | null;
  oldestSeq?: number | null;
  newestSeq?: number | null;
  hasMore?: boolean;
  checkpointSeq?: number | null;
  checkpointHash?: HashHex | null;
}
```

For large logs, relays SHOULD return the latest valid `CHECKPOINT` event plus every tail event after
that checkpoint. Clients MUST verify the checkpoint `rootHash` against the embedded serialized state
before using it as a replay anchor. If no valid checkpoint is available, relays MAY fall back to a
genesis-based snapshot or a paginated `GET_HISTORY` response.

`GET_STATE`, `GET_HISTORY`, `GET_MEMBERS`, and `SEARCH` use the same optional signed read fields (`author`,
`createdAt`, `signature`) and the same visibility rules. Relays SHOULD reject stale signed read
requests to limit replay, while keeping the freshness window large enough for mobile clients with
mild clock skew. If no member plugin handles `GET_MEMBERS`, relays SHOULD derive the member list from
the canonical guild state rather than returning relay-local placeholder data.

Search is a relay read surface, not a new event type. Reference relays MAY implement it by scanning
the durable guild log and canonical state; production relays SHOULD maintain indexes. Search results
MUST be filtered by the same guild/channel visibility checks as `SNAPSHOT`, `STATE`, and
`GET_HISTORY`. Relays MUST NOT decrypt encrypted payloads for search; encrypted messages may only be
matched by visible metadata unless a client/plugin supplies its own encrypted-search index.

```ts
type SearchScope = "messages" | "channels" | "members" | "appObjects";

interface SearchRequestFrame {
  subId: string;
  guildId: GuildId;
  channelId?: ChannelId;
  query: string;
  scopes?: SearchScope[]; // default: all supported scopes
  beforeSeq?: number;
  afterSeq?: number;
  limit?: number;
  includeDeleted?: boolean;
  includeEncrypted?: boolean; // metadata only; never relay-side decryption
  includeEvent?: boolean;
  author?: PublicKeyHex;
  createdAt?: number;
  signature?: SignatureHex;
}

interface SearchResult {
  kind: "message" | "channel" | "member" | "appObject";
  guildId: GuildId;
  channelId?: ChannelId;
  messageId?: HashHex;
  userId?: UserId;
  objectId?: string;
  namespace?: string;
  objectType?: string;
  eventId?: HashHex;
  seq?: number;
  createdAt?: number;
  author?: UserId;
  encrypted?: boolean;
  preview?: string;
  content?: string; // optional current message projection; not part of the signed event
  event?: GuildEvent; // optional, omitted when includeEvent=false
}

interface SearchResultsFrame {
  subId: string;
  guildId: GuildId;
  channelId?: ChannelId;
  query: string;
  scopes: SearchScope[];
  results: SearchResult[];
  hasMore: boolean;
  oldestSeq?: number | null;
  newestSeq?: number | null;
  checkpointSeq?: number | null;
  checkpointHash?: HashHex | null;
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
2. Client signs the **event payload** excluding seq/prevHash/id (or signs a canonical â€œunsignedâ€ representation â€“ details in `@cgp/core`).
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

**Note:** this makes relay responsible for sequencing. In P2P fallback mode, a temporary â€œsequencerâ€ peer plays the same role.

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
* `cgp.relay.abuse-controls`: relay-local flood and abuse checks for message length, mentions,
  duplicates, and command invocation bursts. This is operational policy, not core CGP state.
* `cgp.apps.surface-policy`: relay-local validation and permission checks for the portable
  `org.cgp.apps` namespace (`app-manifest`, `slash-command`, `command-invocation`,
  `command-response`, `webhook`, and `agent-profile`).
  This plugin keeps bots/apps as normal signed CGP identities and app objects; it does not create a
  privileged bot account type.
* `cgp.apps.webhook-ingress`: optional HTTP ingress for registered `org.cgp.apps` webhook objects.
  Implementations SHOULD resolve `credentialRef` to relay-local secrets and MUST NOT store plaintext
  webhook tokens in the public guild log.
* `cgp.security.encryption-policy`: relay-local policy that can require or reject encrypted MESSAGE
  envelopes for selected guilds/channels. This plugin must not receive plaintext keys and must not act
  as a key server.

---

## 6. P2P mode (optional extension)

In pure P2P mode (no relay reachable):

* Peers run a **libp2p node**:

  * use WebRTC/WebSocket transports for browserâ†”browser and browserâ†”Node connectivity.
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

CGP doesnâ€™t force hard rules but defines **recommended defaults**:

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

* Any modification of a guild logâ€™s history is **detectable** to any client that:

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

Encrypted attachments use the same rule: `attachments[]` may contain plaintext blob references or opaque
client-defined encrypted envelopes. The relay validates only event shape and permissions; content hashes,
media keys, thumbnails, and attachment decryption UX are application-layer responsibilities unless a relay
advertises an optional media/security plugin.

Metadata privacy requires additional client/relay extensions. Recommended mitigations include TLS,
Tor/I2P/proxy transports, subscribing to broader guild snapshots and filtering locally, batching
subscriptions, padding event sizes, delaying publishes, pseudonymous per-guild keys, and private directory
lookup mechanisms. Strong metadata privacy via PIR, mixnets, or oblivious relays is intentionally out of
scope for CGP-0.1 because it changes latency, cost, and operational assumptions.
