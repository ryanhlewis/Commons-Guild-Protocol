import { describe, expect, it } from "vitest";
import {
  APP_OBJECT_LEASE_PROTOCOL,
  appObjectLeaseProofBits,
} from "@cgp/core";
import { computeEventId } from "@cgp/core";
import { applyEvent, createInitialState } from "@cgp/core";
import type { AppObjectUpsert, GuildCreate, GuildEvent } from "@cgp/core";
import { validateEvent } from "@cgp/core";

function event(
  seq: number,
  body: GuildCreate | AppObjectUpsert,
  createdAt = 1_750_000_000_000 + seq,
  author = "owner",
): GuildEvent {
  const value = {
    seq,
    prevHash: seq === 0 ? null : "prev_hash",
    createdAt,
    author,
    body,
    signature: "signature",
  } as GuildEvent;
  value.id = computeEventId(value);
  return value;
}

function leasedEvent(input: {
  seq: number;
  createdAt: number;
  expiresAt: number;
  author: string;
  body: Omit<AppObjectUpsert, "lease">;
}) {
  const lease = {
    protocol: APP_OBJECT_LEASE_PROTOCOL,
    expiresAt: input.expiresAt,
    difficultyBits: 12,
    nonce: "",
  };
  for (let counter = 0; counter < 1_000_000; counter += 1) {
    lease.nonce = `test:${counter}`;
    if (
      appObjectLeaseProofBits({
        lease,
        author: input.author,
        createdAt: input.createdAt,
        guildId: input.body.guildId,
        namespace: input.body.namespace,
        objectType: input.body.objectType,
        objectId: input.body.objectId,
      }) >= lease.difficultyBits
    ) {
      break;
    }
  }
  return event(
    input.seq,
    { ...input.body, lease },
    input.createdAt,
    input.author,
  );
}

describe("create-only app objects", () => {
  it("allows one canonical creator and rejects every later overwrite", () => {
    const guildId = "create-only-guild";
    let state = createInitialState(
      event(0, {
        type: "GUILD_CREATE",
        guildId,
        name: "Create-only test",
      }),
    );
    const first = event(1, {
      type: "APP_OBJECT_UPSERT",
      guildId,
      namespace: "org.example.claims",
      objectType: "claim",
      objectId: "resource-1",
      createOnly: true,
      value: { claimant: "first" },
    });
    expect(() => validateEvent(state, first)).not.toThrow();
    state = applyEvent(state, first);

    const competing = event(2, {
      type: "APP_OBJECT_UPSERT",
      guildId,
      namespace: "org.example.claims",
      objectType: "claim",
      objectId: "resource-1",
      createOnly: true,
      value: { claimant: "second" },
    });
    expect(() => validateEvent(state, competing)).toThrow(/already exists/);
  });

  it("retains normal upsert replacement semantics", () => {
    const guildId = "upsert-guild";
    let state = createInitialState(
      event(0, {
        type: "GUILD_CREATE",
        guildId,
        name: "Upsert test",
      }),
    );
    const first = event(1, {
      type: "APP_OBJECT_UPSERT",
      guildId,
      namespace: "org.example.objects",
      objectType: "setting",
      objectId: "theme",
      value: "dark",
    });
    state = applyEvent(state, first);
    const replacement = event(2, {
      type: "APP_OBJECT_UPSERT",
      guildId,
      namespace: "org.example.objects",
      objectType: "setting",
      objectId: "theme",
      value: "light",
    });
    expect(() => validateEvent(state, replacement)).not.toThrow();
  });

  it("enforces proof-backed leases and permits replacement only after expiry", () => {
    const now = 1_750_000_000_000;
    const guildId = "leased-object-guild";
    let state = createInitialState(
      event(0, {
        type: "GUILD_CREATE",
        guildId,
        name: "Leased object test",
        policies: {
          exclusiveAppObjects: [
            {
              namespace: "org.example.claims",
              objectType: "claim",
              difficultyBits: 12,
              maxLeaseMs: 60_000,
            },
          ],
        },
      }),
    );
    const body = {
      type: "APP_OBJECT_UPSERT",
      guildId,
      namespace: "org.example.claims",
      objectType: "claim",
      objectId: "resource-1",
      createOnly: true,
      value: { claimant: "first" },
    } satisfies Omit<AppObjectUpsert, "lease">;

    expect(() => validateEvent(state, event(1, body, now), { now })).toThrow(
      /lease is required/i,
    );

    const first = leasedEvent({
      seq: 1,
      createdAt: now,
      expiresAt: now + 60_000,
      author: "owner",
      body,
    });
    expect(() => validateEvent(state, first, { now })).not.toThrow();
    state = applyEvent(state, first);

    const early = leasedEvent({
      seq: 2,
      createdAt: now + 1_000,
      expiresAt: now + 61_000,
      author: "competitor",
      body: { ...body, value: { claimant: "competitor" } },
    });
    expect(() =>
      validateEvent(state, early, { now: now + 1_000 }),
    ).toThrow(/still active|already exists/i);

    const replacement = leasedEvent({
      seq: 2,
      createdAt: now + 60_000,
      expiresAt: now + 120_000,
      author: "competitor",
      body: { ...body, value: { claimant: "competitor" } },
    });
    expect(() =>
      validateEvent(state, replacement, { now: now + 60_000 }),
    ).not.toThrow();
  });
});
