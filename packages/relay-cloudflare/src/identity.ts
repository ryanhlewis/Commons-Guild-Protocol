export type RelayIdentityStability = "configured" | "ephemeral";

export function relayIdentityStability(privateKeyHex?: string): RelayIdentityStability {
  return /^[a-fA-F0-9]{64}$/.test(privateKeyHex?.trim() ?? "")
    ? "configured"
    : "ephemeral";
}
