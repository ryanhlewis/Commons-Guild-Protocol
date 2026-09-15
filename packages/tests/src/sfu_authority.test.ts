import { describe, expect, it } from "vitest";
import {
  activeSfuAuthoritySets,
  applyEvent,
  computeEventId,
  createInitialState,
  createRelayWriteCertificate,
  generatePrivateKey,
  getPublicKey,
  hashObject,
  relayWriteProposalId,
  sign,
  validateEvent,
  verifyRelayWriteCertificate,
  type GuildEvent,
  type RelayWriteProposal,
  type RelayWriteQuorumVote,
  type SfuAuthoritySet,
} from "@cgp/core";

function keyPair() {
  const privateKey = generatePrivateKey();
  return { privateKey, publicKey: getPublicKey(privateKey) };
}

async function signedEvent(
  privateKey: Uint8Array,
  author: string,
  seq: number,
  prevHash: string | null,
  createdAt: number,
  body: GuildEvent["body"],
): Promise<GuildEvent> {
  const signature = await sign(
    privateKey,
    hashObject({ body, author, createdAt }),
  );
  const event: GuildEvent = {
    id: "",
    seq,
    prevHash,
    createdAt,
    author,
    body,
    signature,
  };
  event.id = computeEventId(event);
  return event;
}

function authorityBody(
  guildId: string,
  policy: SfuAuthoritySet["certifier"],
  now: number,
  epoch = 1,
): SfuAuthoritySet {
  return {
    type: "SFU_AUTHORITY_SET",
    guildId,
    epoch,
    previousEpoch: epoch === 1 ? null : epoch - 1,
    notBefore: now,
    overlapUntil: now + 30_000,
    expiresAt: now + 3_600_000,
    certifier: policy,
    authorities: [
      {
        nodeId: `authority-${epoch}`,
        clusterId: "cluster-a",
        role: "authority",
        routeAuthorityPublicKey: keyPair().publicKey,
      },
      {
        nodeId: `forward-${epoch}`,
        clusterId: "cluster-a",
        role: "forward-only",
      },
    ],
  };
}

async function certify(
  event: GuildEvent,
  policy: SfuAuthoritySet["certifier"],
  relays: ReturnType<typeof keyPair>[],
  voteCount: number,
) {
  const proposal: RelayWriteProposal = {
    guildId: event.body.guildId,
    headSeq: event.seq - 1,
    headHash: event.prevHash,
    body: event.body,
    author: event.author,
    signature: event.signature,
    createdAt: event.createdAt,
  };
  const proposalId = relayWriteProposalId(policy.epoch, proposal);
  const votes: RelayWriteQuorumVote[] = [];
  for (const relay of relays.slice(0, voteCount)) {
    const unsigned = {
      protocol: "cgp/write-vote/1" as const,
      epoch: policy.epoch,
      relayPublicKey: relay.publicKey,
      guildId: event.body.guildId,
      headSeq: event.seq - 1,
      headHash: event.prevHash,
      proposalId,
      votedAt: event.createdAt,
    };
    votes.push({
      ...unsigned,
      signature: await sign(relay.privateKey, hashObject(unsigned)),
    });
  }
  event.writeCertificate = createRelayWriteCertificate(
    policy,
    proposal,
    votes,
  );
  return event;
}

describe("CGP-certified SFU authorities", () => {
  it("requires a 3-of-5 durable write certificate for the owner-authored set", async () => {
    const owner = keyPair();
    const relays = Array.from({ length: 5 }, keyPair);
    const policy = {
      protocol: "cgp/write-quorum/1" as const,
      epoch: "guild-consensus-7",
      members: relays.map((relay) => relay.publicKey),
      requiredVotes: 3,
    };
    const now = Date.now();
    const guildId = hashObject({ test: "sfu-authority", now });
    const genesis = await signedEvent(
      owner.privateKey,
      owner.publicKey,
      0,
      null,
      now - 1,
      { type: "GUILD_CREATE", guildId, name: "Authority test" },
    );
    const authority = await signedEvent(
      owner.privateKey,
      owner.publicKey,
      1,
      genesis.id,
      now,
      authorityBody(guildId, policy, now),
    );

    await certify(authority, policy, relays, 2);
    expect(
      verifyRelayWriteCertificate(authority, {
        expectedGuildId: guildId,
        expectedAuthor: owner.publicKey,
        requireBodyPolicy: true,
      }),
    ).toBe(false);

    await certify(authority, policy, relays, 3);
    expect(
      verifyRelayWriteCertificate(authority, {
        expectedGuildId: guildId,
        expectedAuthor: owner.publicKey,
        requireBodyPolicy: true,
      }),
    ).toBe(true);

    const state = createInitialState(genesis);
    expect(() => validateEvent(state, authority)).not.toThrow();
    const rotated = applyEvent(state, authority);
    expect(rotated.sfuAuthoritySets.map((set) => set.epoch)).toEqual([1]);
  });

  it("keeps only consecutive epochs and accepts the old epoch only during overlap", async () => {
    const owner = keyPair();
    const relays = Array.from({ length: 5 }, keyPair);
    const policy = {
      protocol: "cgp/write-quorum/1" as const,
      epoch: "guild-consensus-8",
      members: relays.map((relay) => relay.publicKey),
      requiredVotes: 3,
    };
    const now = Date.now();
    const guildId = hashObject({ test: "sfu-rotation", now });
    const genesis = await signedEvent(
      owner.privateKey,
      owner.publicKey,
      0,
      null,
      now - 2,
      { type: "GUILD_CREATE", guildId, name: "Rotation test" },
    );
    const first = await signedEvent(
      owner.privateKey,
      owner.publicKey,
      1,
      genesis.id,
      now - 1,
      authorityBody(guildId, policy, now - 1, 1),
    );
    let state = applyEvent(createInitialState(genesis), first);
    const secondBody = authorityBody(guildId, policy, now, 2);
    const second = await signedEvent(
      owner.privateKey,
      owner.publicKey,
      2,
      first.id,
      now,
      secondBody,
    );
    expect(() => validateEvent(state, second)).not.toThrow();
    state = applyEvent(state, second);
    expect(activeSfuAuthoritySets(state.sfuAuthoritySets, now + 1).map((set) => set.epoch)).toEqual([1, 2]);
    expect(activeSfuAuthoritySets(state.sfuAuthoritySets, secondBody.overlapUntil + 1).map((set) => set.epoch)).toEqual([2]);

    const skippedBody = { ...authorityBody(guildId, policy, now + 1, 4), previousEpoch: 2 };
    const skipped = await signedEvent(
      owner.privateKey,
      owner.publicKey,
      3,
      second.id,
      now + 1,
      skippedBody,
    );
    expect(() => validateEvent(state, skipped)).toThrow();
  });

  it("rejects certificate-member substitution and authority payload tampering", async () => {
    const owner = keyPair();
    const relays = Array.from({ length: 5 }, keyPair);
    const policy = {
      protocol: "cgp/write-quorum/1" as const,
      epoch: "guild-consensus-9",
      members: relays.map((relay) => relay.publicKey),
      requiredVotes: 3,
    };
    const now = Date.now();
    const guildId = hashObject({ test: "sfu-tamper", now });
    const event = await signedEvent(
      owner.privateKey,
      owner.publicKey,
      1,
      "previous-head",
      now,
      authorityBody(guildId, policy, now),
    );
    await certify(event, policy, relays, 3);
    expect(verifyRelayWriteCertificate(event, { requireBodyPolicy: true })).toBe(true);

    event.writeCertificate!.policy.members[0] = keyPair().publicKey;
    expect(verifyRelayWriteCertificate(event, { requireBodyPolicy: true })).toBe(false);
  });
});
