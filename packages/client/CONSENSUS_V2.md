# Explicitly anchored consensus client

`ConsensusV2Client` is an opt-in Node WebSocket client for a guild configured for consensus v2. It does not change `CgpClient`, automatically migrate an existing guild, or trust policy declarations received through `HELLO`.

```ts
import { ConsensusV2Client } from '@cgp/client';
import { hashObject, sign } from '@cgp/core';

const client = new ConsensusV2Client({
  relayUrl: 'wss://your-relay.example',
  guildId,
  anchorPolicy: independentlyVerifiedAnchor,
  // Required only if history contains an explicitly certified legacy bridge:
  // legacyTrust: independentlyVerifiedOldPolicyAndAdministrators,
  readSigner: async payload => ({
    author: accountPublicKey,
    signature: await sign(accountPrivateKey, hashObject(payload)),
  }),
});
try {
  const { history, verified } = await client.fetchHistory();
  // Build and sign the complete GuildEvent against verified.events' last app head.
  const result = await client.submitEvent(signedEvent);
  if (!result.requestedCommitted) {
    // Consensus recovered a prior accepted value. Inspect result.history;
    // rebuild/re-authorize a subsequent request instead of reporting success.
  }
} finally {
  client.close();
}
```

`submitTransition` accepts a fully administrator-signed `ConsensusTransition`; administrators sign `consensusTransitionPayload(transition)`. The result contains the certified history and current verified policy, including whether activation is still pending. Applications must not treat a transition proposal alone as an activated membership change.

Explicit migration operators can use `freezeLegacy(migrationRequest, expectedVoterPublicKey)` and `importHistory(certifiedHistory)`. Freezing is an irreversible old-voter write stop, not a read; invoke it only as part of an authorized migration with the full old-voter inventory available. The client requires explicit old trust, verifies administrator authorization before sending, and verifies the target voter's signed freeze receipt. Import validates the complete bridge/history before sending and requires the response to retain the imported certified prefix. These methods do not automatically gather missing voters or recover an unknown legacy proposal.

Retries of an event already present in verified history, including a legacy bridge's recovered event, return `requestedCommitted: true` without another proposal. The supplied event ID must match its computed content ID. Exact already-certified transition retries are also recognized.

Both reads and submissions carry a fresh UUID and a signature over `{protocol:'cgp/consensus-read/2', guildId, requestId, createdAt}`. A delegated signer may return `deviceAuthorization`; the relay enforces the read capability and guild access. Event and transition authorization remain separately verified. Replies use `CONSENSUS_RESULT` or `CONSENSUS_ERROR` with the same request/guild IDs.

Every successful result is verified against the explicit anchor, with the optional explicit old-policy trust required for legacy bridges. Previously verified commits cannot disappear or change, an accepted activation cannot disappear, and the migration base cannot change. Returned objects are copied so callers cannot mutate retained trust. These checks do not establish freshness against a newer history the client has never seen. Persist a verified checkpoint outside this in-memory session when restart-time rollback protection is required.

Requests are serialized per client. Timeouts mean the commit outcome is unknown; fetch certified history before retrying. The default timeout is 15 seconds (configurable from 100 ms to 60 seconds). Full history responses are currently bounded to 16 MiB; paginated certified history is not implemented.
