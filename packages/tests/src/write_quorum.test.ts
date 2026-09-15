import { describe, expect, it } from "vitest";
import { generatePrivateKey, getPublicKey } from "@cgp/core";
import {
  RelayWriteQuorumCoordinator,
  type RelayWriteProposal,
  type RelayWriteQuorumVote,
  type RelayWriteVoteTransport,
} from "@cgp/relay/src/write_quorum";
import { MemoryStore } from "@cgp/relay";

class VoteBus {
  private handlers = new Set<(vote: RelayWriteQuorumVote) => void>();
  private proposalHandlers = new Set<(proposal: RelayWriteProposal) => void>();

  transport(): RelayWriteVoteTransport {
    return {
      publish: (vote) => {
        for (const handler of this.handlers) {
          queueMicrotask(() => handler(vote));
        }
      },
      publishProposal: (proposal) => {
        for (const handler of this.proposalHandlers) {
          queueMicrotask(() => handler(proposal));
        }
      },
      subscribe: (handler, proposalHandler) => {
        this.handlers.add(handler);
        if (proposalHandler) this.proposalHandlers.add(proposalHandler);
        return () => {
          this.handlers.delete(handler);
          if (proposalHandler) this.proposalHandlers.delete(proposalHandler);
        };
      },
    };
  }
}

function identities(count: number) {
  return Array.from({ length: count }, () => {
    const privateKey = generatePrivateKey();
    return { privateKey, publicKey: getPublicKey(privateKey) };
  });
}

function proposal(content: string): RelayWriteProposal {
  return {
    guildId: "write-quorum-guild",
    headSeq: 4,
    headHash: "head-4",
    body: {
      type: "MESSAGE",
      guildId: "write-quorum-guild",
      channelId: "general",
      messageId: `message-${content}`,
      content,
    },
    author: "author",
    signature: `author-signature-${content}`,
    createdAt: 1_800_000_000_000,
    clientEventId: `client-${content}`,
  };
}

function coordinator(
  identity: ReturnType<typeof identities>[number],
  members: string[],
  store: MemoryStore,
  bus: VoteBus,
) {
  return new RelayWriteQuorumCoordinator(
    {
      epoch: "epoch-1",
      members,
      requiredVotes: 2,
      voteTimeoutMs: 100,
    },
    identity,
    store,
    bus.transport(),
  );
}

describe("relay write quorum coordinator", () => {
  it("authorizes a majority partition and rejects an isolated minority", async () => {
    const keys = identities(3);
    const members = keys.map((key) => key.publicKey);
    const majorityBus = new VoteBus();
    const minorityBus = new VoteBus();
    const majority = [
      coordinator(keys[0], members, new MemoryStore(), majorityBus),
      coordinator(keys[1], members, new MemoryStore(), majorityBus),
    ];
    const minority = coordinator(
      keys[2],
      members,
      new MemoryStore(),
      minorityBus,
    );
    await Promise.all([...majority.map((entry) => entry.start()), minority.start()]);

    const certificates = await Promise.all(
      majority.map((entry) => entry.authorize(proposal("majority"))),
    );
    expect(certificates.every((votes) => votes.length === 2)).toBe(true);
    await expect(minority.authorize(proposal("minority"))).rejects.toThrow(
      "1/2 votes",
    );

    await Promise.all([...majority.map((entry) => entry.close()), minority.close()]);
  });

  it("collects remote witness votes from one proposer broadcast", async () => {
    const keys = identities(3);
    const members = keys.map((key) => key.publicKey);
    const bus = new VoteBus();
    const coordinators = keys.map((key) => (
      coordinator(key, members, new MemoryStore(), bus)
    ));
    await Promise.all(coordinators.map((entry) => entry.start()));

    const votes = await coordinators[0].authorize(proposal("single-proposer"));
    expect(votes).toHaveLength(2);
    expect(new Set(votes.map((vote) => vote.relayPublicKey)).size).toBe(2);

    await Promise.all(coordinators.map((entry) => entry.close()));
  });

  it("durably refuses a competing proposal for the same guild head", async () => {
    const keys = identities(3);
    const members = keys.map((key) => key.publicKey);
    const store = new MemoryStore();
    const isolatedBus = new VoteBus();
    const first = coordinator(keys[0], members, store, isolatedBus);
    await first.start();
    await expect(first.authorize(proposal("first"))).rejects.toThrow("1/2 votes");
    await first.close();

    const restarted = coordinator(keys[0], members, store, isolatedBus);
    await restarted.start();
    await expect(restarted.authorize(proposal("competing"))).rejects.toThrow(
      "already voted",
    );
    await restarted.close();
  });

  it("serializes concurrent vote-fence mutations for competing proposals", async () => {
    const keys = identities(3);
    const members = keys.map((key) => key.publicKey);
    const backing = new MemoryStore();
    const delayedStore = {
      async getWriteVoteFence(key: string) {
        await new Promise((resolve) => setTimeout(resolve, 20));
        return backing.getWriteVoteFence(key);
      },
      async putWriteVoteFence(key: string, proposalId: string) {
        await new Promise((resolve) => setTimeout(resolve, 20));
        backing.putWriteVoteFence(key, proposalId);
      },
    };
    const isolatedBus = new VoteBus();
    const entry = new RelayWriteQuorumCoordinator(
      {
        epoch: "epoch-atomic-fence",
        members,
        requiredVotes: 2,
        voteTimeoutMs: 100,
      },
      keys[0],
      delayedStore,
      isolatedBus.transport(),
    );
    await entry.start();

    const outcomes = await Promise.allSettled([
      entry.authorize(proposal("concurrent-a")),
      entry.authorize(proposal("concurrent-b")),
    ]);
    const messages = outcomes.map((outcome) =>
      outcome.status === "rejected" ? String(outcome.reason?.message) : "accepted"
    );
    expect(messages.filter((message) => message.includes("already voted"))).toHaveLength(1);
    expect(messages.filter((message) => message.includes("1/2 votes"))).toHaveLength(1);
    await entry.close();
  });
});
