# Registration v2

New writes must sign `hashObject(directoryRegistrationPayload(handle, guildId, guildPubkey, timestamp, relays))` using the guild private key. Import the helper from `@cgp/core`. Handles must already be normalized. The payload includes protocol `cgp-directory-registration/2` and binds every stored routing field, including the ordered relay list.

Existing records remain readable and retain their owner across restarts. Updates must use the existing owner's key and a strictly newer timestamp. Identical retries within the five-minute timestamp window succeed without modifying the record. Legacy `REGISTER:...` signatures are rejected for writes. Key rotation and ownership transfers are not supported by this endpoint.

Deploy this directory version before its matching Hollow frontend. Configure the frontend's server-side `CGP_DIRECTORY_URLS` allowlist; see Hollow's `docs/directory-deployment.md`. Old clients can read existing records but must upgrade to create or change registrations.

Writes serialize the owner check and database update. Entries and their Merkle tree publish together only after persistence succeeds, so concurrent lookups return proofs for a single committed snapshot.
