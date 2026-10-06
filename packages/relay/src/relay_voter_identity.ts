/** Canonical SEC1 voter IDs prevent one signer occupying multiple quorum seats. */
export function normalizeRelayVoterMembers(members: string[]): string[] {
  const normalized = members.map((member) => {
    if (typeof member !== "string") {
      throw new Error("Relay voter members must be compressed secp256k1 public keys");
    }
    const publicKey = member.trim().toLowerCase();
    if (!/^(02|03)[0-9a-f]{64}$/.test(publicKey)) {
      throw new Error("Relay voter members must be compressed secp256k1 public keys");
    }
    return publicKey;
  });
  return [...new Set(normalized)];
}
