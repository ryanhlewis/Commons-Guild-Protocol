# Hosting media for Hollow through CGP

CGP relays advertise services; the services can store and serve media on separate hosts. Hollow discovers GIF and avatar providers from the relay's `HELLO_OK` plugin metadata. Uploaded attachments use a separate storage placement interface. Adding a catalog URL does not enroll it in a global directory or guarantee retention.

Start with [Hollow's hosting guide](https://github.com/hollowchat/hollow-svelte/blob/HEAD/docs/media-hosting.md) in the corresponding Hollow checkout, or its public `/hosting.txt` after frontend deployment. It contains complete search payloads, a generic advertisement plugin, operator recipes and verification commands. Follow the checked-out source when a deployed guide differs.

## Choose the interface

| Service | Relay metadata / plugin | Operator responsibility |
| --- | --- | --- |
| GIFs, stickers, emoji | `metadata.expressionProvider` (built-in `cgp.expression.search`) | Search JSON, media/preview URLs, licenses, hashes, CORS and persistent files |
| VRM avatars | `metadata.hollowAvatarProvider` on a custom plugin | Search JSON, model/preview URLs and optional animations |
| Uploaded attachments | `cgp.media.storage`, with `metadata.policy.providers` | Placement policy plus a configured storage backend and retention |
| Real IPFS storage | `cgp.ipfs.helia` | Persistent block storage, gateway, and optionally reachable network peers |
| Ordinary storage with synthetic IPFS identifiers | `cgp.ipfs.faux` | Persistent local/object storage; disclose synthetic semantics |

`cgp.expression.search` advertises a search endpoint; it does not implement or proxy that endpoint. Configure `CGP_EXPRESSION_PROVIDER_ENDPOINT` and the provider's ID/label/types as implemented in [plugins.ts](../packages/relay/src/plugins.ts). There is no corresponding built-in DeVRM server: an avatar plugin advertises a separately operated provider.

For a custom plugin, export a default factory returning a relay plugin with `name` and `metadata`. Add its absolute module URL to `CGP_RELAY_PLUGINS`, preserving existing entries. Hollow's `scripts/media-hosting/advertise.mjs` implements this for GIF and avatar endpoints. The Cleric deployment's heartbeat registration is operator-specific; it is not a standard public CGP self-registration API.

## Storage is more than discovery

Enable `cgp.media.storage` together with a backend such as `cgp.ipfs.helia`. Set `CGP_MEDIA_PROVIDERS_JSON` to your provider policies, matching `ipfsBackendId` to the backend ID. Configure persistent `CGP_IPFS_HELIA_STORE_DIR` and a gateway that actually serves your stored content. Inspect `/plugins/cgp.media.storage/providers`; `/route` and `/upload` under that plugin implement placement and upload. The Hollow guide includes a local working recipe and explains what still needs hardening for public operation.

Helia local mode retains real IPFS data without promising public IPFS retrieval. Network mode needs reachable listen/announce addresses, routing and independent retrieval tests. An HTTP named tunnel exposes a gateway, not automatically a libp2p peer. A CID or successful upload receipt does not prove replication. Per-file limits do not constrain aggregate storage; apply admission controls, quotas and retention policies. Preserve Hollow's encryption envelope for private attachments.

For synthetic backends, read [faux IPFS backends](faux-ipfs-backends.md); do not present them as public IPFS pins. For gateway formats, networking and backend limits, inspect the actual implementation in [plugins.ts](../packages/relay/src/plugins.ts).

## Verify an operator contribution

Reconnect a Hollow client to your configured relay and check the advertised descriptor, then run Hollow's `scripts/media-hosting/verify.mjs`. Also verify actual GIF playback or VRM rendering, fetched hashes, CORS, pagination, unavailable-provider fallback and media retention after restart. Packed shard hosts need explicit `Range` tests: some return the complete object despite a range request.

For storage, upload a small test file through the real client, inspect its receipt/provider selection, restart the backend and retrieve it again. Public IPFS claims additionally require retrieval from an independent peer. Reference tests: [expression advertisement](../packages/tests/src/expression_provider.test.ts), [media storage](../packages/tests/src/media_storage.test.ts) and [Helia](../packages/tests/src/helia_ipfs.test.ts).
