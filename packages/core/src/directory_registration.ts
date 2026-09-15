/** All mutable directory fields are covered by this versioned signature. */
export function directoryRegistrationPayload(handle: string, guildId: string, guildPubkey: string, timestamp: number, relays: string[] = []) {
    return { protocol: 'cgp-directory-registration/2', handle, guildId, guildPubkey: guildPubkey.toLowerCase(), timestamp, relays };
}
