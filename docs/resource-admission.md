# Relay storage and directory admission

Admission limits are operator policy, independent of CGP identity and membership. A public key is not proof of a unique person. Work challenges raise the cost of mass requests; aggregate limits bound what the operator accepts even when an attacker generates many keys. These controls do not eliminate distributed Sybil attacks or replace edge DDoS protection and community moderation.

## Static-shard relay defaults

| Limit | Default | Environment variable |
| --- | --- | --- |
| Stored static-shard bytes, including staging and existing files | 10 GiB | `CGP_STATIC_SHARD_STORE_MAX_BYTES` |
| Cumulative bytes per publisher root | 1 GiB | `CGP_STATIC_SHARD_PUBLISHER_MAX_BYTES` |
| Minimum free disk after a reservation | 512 MiB | `CGP_STATIC_SHARD_MIN_FREE_BYTES` |
| Concurrent upload requests | 2 | `CGP_STATIC_SHARD_MAX_CONCURRENT_UPLOADS` |
| Upload requests per IP/minute | 30 | `CGP_STATIC_SHARD_IP_REQUESTS_PER_MINUTE` |
| Upload requests per /24 IPv4 or /64 IPv6 network/minute | 90 | `CGP_STATIC_SHARD_NETWORK_REQUESTS_PER_MINUTE` |
| Upload requests globally/minute | 300 | `CGP_STATIC_SHARD_REQUESTS_PER_MINUTE` |
| Upload requests per authenticated publisher/minute | Network request limit | Same network variable |
| Work bits for a signed release | 16 | `CGP_STATIC_SHARD_WORK_BITS` |
| Global static-shard pin budget | 2 GiB | `CGP_STATIC_SHARD_PIN_MAX_BYTES` |
| Pin budget per publisher | 512 MiB | `CGP_STATIC_SHARD_PUBLISHER_PIN_MAX_BYTES` |
| Published retention before grace | 365 days | `CGP_STATIC_SHARD_RETENTION_DAYS` |

`GET /plugins/cgp.static-shards/status` exposes the selected limits. A configured private/test plugin may explicitly choose zero work bits. Public production defaults require work. Configure larger budgets deliberately for large game libraries; a pre-existing over-budget library remains readable but new writes fail closed.

The durable `admission-ledger.json` charges actual files, including failed partial writes. Reservations include the maximum allowed extraction size, not just compressed ZIP bytes. Mutations and cleanup are serialized. Disk exhaustion returns HTTP 507; request/concurrency admission returns 429. A request body must finish within 30 seconds. Disconnecting does not free the concurrency slot while its handler is still processing. Preserve this ledger along with the publisher ownership registry in backups.

Pending content and failed new uploads expire after 24 hours. Published, unpinned files expire after retention plus 90 days of grace. Cleanup runs at startup and hourly. Existing records receive a one-time migration retention period; restarting does not reset it. Cleanup removes served bytes, never the signed publisher ownership registry, guild history, account keys, or permissions. Re-uploading the original signed bytes remains subject to the original publisher binding. Changing content requires a new immutable version.

Fresh ownership authorization can renew hosting through `POST /plugins/cgp.static-shards/retain`. Sign `{kind:"cgp-static-shard-retention/1",id,version,releaseSha256,timestamp}` using the existing static-shard publisher signing envelope, include `publisher`, and submit within five minutes. Delegated devices require the publish capability and current authority. A different publisher, stale request, mismatched hash, or missing stored bytes is rejected. A new signed listing edit with a fresh timestamp also renews retention. Replaying an immutable release upload alone does not extend retention.

Public uploads do not automatically pin into IPFS. `CGP_STATIC_SHARD_PIN_PUBLISHERS` lists approved account root keys; `CGP_STATIC_SHARD_IPFS_PIN=1` additionally enables pinning. Approved uploads and operator URL imports still require a pin reservation. Pins remain charged and retained until the operator unpins/removes them; cleanup cannot safely claim a remote backend unpinned something without an acknowledgement. These budgets cover this plugin's static-shard bytes, not unrelated pins in a shared Kubo/Helia backend. Keep a separate backend volume quota and do not expose its administrative API publicly.

Trust forwarded `CF-Connecting-IP` only on an origin restricted to the trusted Cloudflare proxy: set `CGP_STATIC_SHARD_TRUST_PROXY=1` there. Otherwise socket addresses are used and spoofed forwarding headers are ignored. Normal desktop uploads solve the challenge automatically and wait/retry bounded 429 responses; older desktop builds must update to the admission-capable uploader. Completed staging remains resumable.

## Directory defaults

Each root may hold three live or grace-period handles. Reserved application names, including `guest`, cannot be claimed. New claims and revival after expiry require 18 work bits bound to the exact canonical signed registration. Renewals by the current owner do not need new work. Signatures, relay metadata, device generation/revocation continuity, and the five-minute timestamp window remain enforced.

Handles have a 365-day lease and 90-day owner recovery grace. They continue resolving during grace; another key cannot claim them until grace ends. Signed-in clients check on login/focus and hourly, and renew a verified handle in the final 90 days. Native clients can use an explicitly configured public directory; an offline native build does not invent successful directory registration. Legacy server records migrate from the fixed October 2, 2026 rollout anchor so independent directory peers agree on identical Merkle leaves.

Registration budgets are 30/IP, 90/network, 300/global and 30/verified-root per minute. The Cloudflare directory persists its minute budgets across Durable Object restarts. The reference Node service uses bounded in-memory minute budgets. Request bodies are capped at 32 KiB. Both implementations cap retained handle entries at 10,000 and 16 MiB of encoded metadata; new accepted registrations remove expired bindings, preserving authority pins. Authority history is capped at 50,000 roots and fails closed at capacity instead of evicting revocation history.

Reclaim changes only the handle-to-public-key binding and its new profile guild. Old accounts, messages, games, private membership and administrative roles stay with their original public key. Authority pins are stored separately and survive handle replacement and restart. Browser caches accept a new, quorum-verified owner only after their prior binding's reclaim deadline, and retain the old profile by its original key. A conflicting pin without enough expiry evidence fails closed. Sharing a public-key profile URL remains stable even if the username changes owners.

## Verification

Build core/client before testing. `packages/tests/src/admission.test.ts`, `directory_registration.test.ts`, `directory.test.ts` and `static_shard_seed.test.ts` cover bounded work, cumulative/restart accounting, failed writes, concurrent reservations, network aggregation, disk/pin limits, cleanup, owner-only renewal, alias limits, grace and signed uploads. Hollow's directory transport/proof/cache tests cover challenge retries, excessive work refusal, expiry and preservation of prior profile ownership. `scripts/check-production-directory.mjs` runs positive registrations against a local test directory; do not use it against production unless those writes are intentional.
