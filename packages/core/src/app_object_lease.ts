import { hashObject } from "./crypto.js";
import type { AppObjectLease, AppObjectLeasePolicy, AppObjectUpsert, GuildEvent } from "./types.js";

export const APP_OBJECT_LEASE_PROTOCOL = "cgp/app-object-lease/1" as const;
export const APP_OBJECT_LEASE_MIN_DIFFICULTY_BITS = 12;
export const APP_OBJECT_LEASE_MAX_DIFFICULTY_BITS = 24;
export const APP_OBJECT_LEASE_MIN_DURATION_MS = 30_000;
export const APP_OBJECT_LEASE_MAX_DURATION_MS = 10 * 60_000;
export const APP_OBJECT_LEASE_MAX_CLOCK_SKEW_MS = 5 * 60_000;

function leadingZeroBits(hex: string) {
  let total = 0;
  for (const character of hex) {
    const value = Number.parseInt(character, 16);
    if (!Number.isFinite(value)) return -1;
    if (value === 0) {
      total += 4;
      continue;
    }
    if (value < 2) total += 3;
    else if (value < 4) total += 2;
    else if (value < 8) total += 1;
    return total;
  }
  return total;
}

export function appObjectLeaseChallenge(input: {
  lease: AppObjectLease;
  author: string;
  createdAt: number;
  guildId: string;
  namespace: string;
  objectType: string;
  objectId: string;
}) {
  return {
    protocol: APP_OBJECT_LEASE_PROTOCOL,
    difficultyBits: input.lease.difficultyBits,
    nonce: input.lease.nonce,
    author: input.author,
    createdAt: input.createdAt,
    guildId: input.guildId,
    namespace: input.namespace,
    objectType: input.objectType,
    objectId: input.objectId,
    expiresAt: input.lease.expiresAt,
  };
}

export function appObjectLeaseProofBits(input: Parameters<typeof appObjectLeaseChallenge>[0]) {
  return leadingZeroBits(hashObject(appObjectLeaseChallenge(input)));
}

export function matchingAppObjectLeasePolicy(
  policies: AppObjectLeasePolicy[] | undefined,
  body: Pick<AppObjectUpsert, "namespace" | "objectType">,
) {
  return policies?.find(
    (policy) =>
      policy?.namespace === body.namespace &&
      policy?.objectType === body.objectType,
  );
}

export function validateAppObjectLease(
  event: GuildEvent,
  policy?: AppObjectLeasePolicy,
  now = Date.now(),
) {
  const body = event.body as AppObjectUpsert;
  const lease = body.lease;
  if (!lease || lease.protocol !== APP_OBJECT_LEASE_PROTOCOL) {
    throw new Error("A valid exclusive app-object lease is required");
  }
  if (body.createOnly !== true) {
    throw new Error("Exclusive app-object leases require createOnly");
  }
  const durationMs = lease.expiresAt - event.createdAt;
  const minimumDifficulty = Math.max(
    APP_OBJECT_LEASE_MIN_DIFFICULTY_BITS,
    Math.floor(policy?.difficultyBits ?? APP_OBJECT_LEASE_MIN_DIFFICULTY_BITS),
  );
  const maximumDuration = Math.min(
    APP_OBJECT_LEASE_MAX_DURATION_MS,
    Math.floor(policy?.maxLeaseMs ?? APP_OBJECT_LEASE_MAX_DURATION_MS),
  );
  if (
    !Number.isSafeInteger(event.createdAt) ||
    event.createdAt < 1 ||
    event.createdAt > now + APP_OBJECT_LEASE_MAX_CLOCK_SKEW_MS ||
    !Number.isSafeInteger(lease.expiresAt) ||
    lease.expiresAt <= now ||
    durationMs < APP_OBJECT_LEASE_MIN_DURATION_MS ||
    durationMs > maximumDuration ||
    !Number.isSafeInteger(lease.difficultyBits) ||
    lease.difficultyBits < minimumDifficulty ||
    lease.difficultyBits > APP_OBJECT_LEASE_MAX_DIFFICULTY_BITS ||
    typeof lease.nonce !== "string" ||
    lease.nonce.length < 1 ||
    lease.nonce.length > 128
  ) {
    throw new Error("Exclusive app-object lease parameters are invalid");
  }
  if (
    appObjectLeaseProofBits({
      lease,
      author: event.author,
      createdAt: event.createdAt,
      guildId: body.guildId,
      namespace: body.namespace,
      objectType: body.objectType,
      objectId: body.objectId,
    }) < minimumDifficulty
  ) {
    throw new Error("Exclusive app-object lease proof-of-work is invalid");
  }
}
