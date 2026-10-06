import { describe, it, expect, vi } from 'vitest';
import { hashObject, getPublicKey, sign, verify, encrypt, decrypt, generateSymmetricKey } from '@cgp/core';

describe('browser crypto without Node Buffer', () => {
 it('preserves hash/signature bytes and encrypts/decrypts UTF-8', async () => {
  const key = new Uint8Array(32); key[31] = 1;
  const expected = hashObject({ text: 'hello 🌍' });
  vi.stubGlobal('Buffer', undefined);
  try {
   expect(hashObject({ text: 'hello 🌍' })).toBe(expected);
   const publicKey = getPublicKey(key);
   expect(publicKey).toBe('0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798');
   expect(verify(publicKey, expected, await sign(key, expected))).toBe(true);
   const encrypted = await encrypt(key, 'private 🌍');
   expect(await decrypt(key, encrypted.ciphertext, encrypted.iv)).toBe('private 🌍');
   expect(generateSymmetricKey()).toMatch(/^[a-f0-9]{64}$/);
  } finally { vi.unstubAllGlobals(); }
 });
});
