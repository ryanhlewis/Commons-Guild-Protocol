import { hashObject } from './crypto.js';

export const DIRECTORY_LEASE_MS = 365 * 86400000;
export const DIRECTORY_GRACE_MS = 90 * 86400000;
export const DIRECTORY_MAX_HANDLES = 3;
export const DIRECTORY_WORK_BITS = 18;
export const DIRECTORY_MAX_ENTRIES = 10000;
export const DIRECTORY_MAX_ENTRY_BYTES = 16 * 1024 * 1024;
export const DIRECTORY_MAX_AUTHORITY_ROOTS = 50000;
export const DIRECTORY_LEASE_MIGRATION_AT = Date.UTC(2026, 9, 2);
export const RESERVED_DIRECTORY_HANDLES = new Set(['guest', 'admin', 'administrator', 'root', 'system', 'hollow', 'api', 'www', 'login', 'signup', 'settings', 'messages', 'friends', 'community', 'play', 'import', 'download', 'agent']);

export function admissionNetworkKey(address: string) {
 const value = address.toLowerCase().replace(/^::ffff:/, '').split('%')[0];
 if (/^(\d{1,3}\.){3}\d{1,3}$/.test(value)) return value.split('.').slice(0, 3).join('.') + '.0/24';
 if (/^[a-f0-9:]+$/.test(value) && value.includes(':')) {
  const parts = value.split('::');
  const left = parts[0].split(':').filter(Boolean), right = (parts[1] ?? '').split(':').filter(Boolean);
  const expanded = parts.length === 2 ? [...left, ...Array(Math.max(0, 8-left.length-right.length)).fill('0'), ...right] : left;
  return expanded.slice(0, 4).map(part => part.padStart(4, '0')).join(':') + '::/64';
 }
 return 'unknown';
}

export async function solveAdmissionWork(task: string, bits: number, signal?: AbortSignal) {
 if (!Number.isInteger(bits) || bits < 0 || bits > 20) throw new Error('Admission work exceeds the client budget.');
 for (let counter = 0; counter < 2 ** 24; counter++) {
  if (counter % 2048 === 0) { signal?.throwIfAborted(); await new Promise(resolve => setTimeout(resolve, 0)); }
  const nonce = counter.toString(16);
  if (admissionWorkValid(task, nonce, bits)) return nonce;
 }
 throw new Error('Admission work budget exhausted; retry with a fresh task.');
}

/** Cheap to verify; binds the work to the exact signed task, not an arbitrary key. */
export function admissionWorkValid(task: string, nonce: unknown, bits: number) {
 if (!Number.isInteger(bits) || bits < 0 || bits > 24 || typeof nonce !== 'string' || !/^[a-f0-9]{1,16}$/.test(nonce)) return false;
 const hash = hashObject({ protocol: 'cgp-admission-work/1', task, nonce });
 const whole = Math.floor(bits / 4), extra = bits % 4;
 return hash.startsWith('0'.repeat(whole)) && (!extra || Number.parseInt(hash[whole], 16) < 2 ** (4 - extra));
}

export function handleLeaseState(entry: { leaseExpiresAt?: number; reclaimAfter?: number }, now = Date.now()) {
 if (entry.leaseExpiresAt === undefined && entry.reclaimAfter === undefined) return 'active'; // Legacy entries are migrated by their operator.
 if(!Number.isSafeInteger(entry.leaseExpiresAt)||!Number.isSafeInteger(entry.reclaimAfter)||entry.reclaimAfter!<entry.leaseExpiresAt!)return 'reclaimable';
 if (now < entry.leaseExpiresAt!) return 'active';
 return now < entry.reclaimAfter! ? 'grace' : 'reclaimable';
}
