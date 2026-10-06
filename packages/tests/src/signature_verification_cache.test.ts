import { expect, test } from 'vitest';
import { generatePrivateKey, getPublicKey, sign, verify } from '@cgp/core';
import { createSignatureVerificationCache } from '../../core/src/signature_verification_cache';

test('identical valid cryptographic bytes are checked once; each changed signed field is verified and rejected', async () => {
    const key = generatePrivateKey(), publicKey = getPublicKey(key), hash = 'ab'.repeat(32);
    const signature = await sign(key, hash); let calls = 0;
    const cached = createSignatureVerificationCache((...fields) => { calls++; return verify(...fields); });
    expect(cached(publicKey, hash, signature)).toBe(true);
    for (let index = 0; index < 100; index++) expect(cached(publicKey, hash, signature)).toBe(true);
    expect(calls).toBe(1);
    expect(cached(getPublicKey(generatePrivateKey()), hash, signature)).toBe(false);
    expect(cached(publicKey, 'cd'.repeat(32), signature)).toBe(false);
    expect(cached(publicKey, hash, '00'.repeat(64))).toBe(false);
    expect(cached(publicKey, hash, '00'.repeat(64))).toBe(false);
    expect(calls).toBe(5); // Invalid proofs are never admitted into the cache.
    expect(cached.statistics()).toEqual({hits:100,misses:5,evictions:0,entries:1,maximumEntries:4096});
});

test('bounded cache evicts older exact proofs and does not cache malformed or unbounded inputs', async () => {
    const key = generatePrivateKey(), publicKey = getPublicKey(key); let calls = 0;
    const cached = createSignatureVerificationCache((...fields) => { calls++; return verify(...fields); }, 2);
    const proofs = await Promise.all(['ab', 'cd', 'ef'].map(async byte => ({hash:byte.repeat(32),signature:await sign(key,byte.repeat(32))})));
    for (const proof of proofs) expect(cached(publicKey, proof.hash, proof.signature)).toBe(true);
    expect(cached(publicKey, proofs[0].hash, proofs[0].signature)).toBe(true);
    expect(calls).toBe(4);
    expect(cached.statistics()).toEqual({hits:0,misses:4,evictions:2,entries:2,maximumEntries:2});
    expect(cached(publicKey, 'x'.repeat(100000), proofs[0].signature)).toBe(false);
    expect(() => createSignatureVerificationCache(() => true, 4097)).toThrow();
});
