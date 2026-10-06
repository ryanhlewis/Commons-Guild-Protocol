/** Cache only immutable cryptographic facts, never authority, expiry or policy decisions. */
export function createSignatureVerificationCache(
    verifyBytes: (publicKey: string, payloadHash: string, signature: string) => boolean,
    maximumEntries = 4096,
) {
    if (!Number.isSafeInteger(maximumEntries) || maximumEntries < 1 || maximumEntries > 4096) {
        throw new Error('Invalid signature verification cache bound');
    }
    const verified = new Set<string>();
    let hits = 0, misses = 0, evictions = 0;
    const check = (publicKey: string, payloadHash: string, signature: string): boolean => {
        // Fixed field lengths prevent ambiguous keys and bound memory even for hostile input.
        const cacheable = typeof publicKey === 'string' && /^(02|03)[0-9a-f]{64}$/i.test(publicKey)
            && typeof payloadHash === 'string' && /^[0-9a-f]{64}$/i.test(payloadHash)
            && typeof signature === 'string' && /^[0-9a-f]{128}$/i.test(signature);
        const key = cacheable ? publicKey + ':' + payloadHash + ':' + signature : undefined;
        if (key && verified.has(key)) { hits++; return true; }
        misses++;
        const valid = verifyBytes(publicKey, payloadHash, signature);
        if (valid && key) {
            verified.add(key);
            if (verified.size > maximumEntries) { verified.delete(verified.values().next().value!); evictions++; }
        }
        return valid;
    };
    return Object.assign(check, { statistics: () => ({hits, misses, evictions, entries:verified.size, maximumEntries}) });
}
