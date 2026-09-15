import { describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import { generatePrivateKey, getPublicKey } from "@cgp/core";
import {
  RelaySequencerConsensusCoordinator,
  type RelaySequencerMessage,
  type RelaySequencerTransport,
  type RelaySequencingRequest,
} from "@cgp/relay";
import { MemoryStore } from "@cgp/relay";
import { LevelStore } from "@cgp/relay/src/store_level";

class SequencerBus {
  private handlers = new Map<
    string,
    (message: RelaySequencerMessage) => void
  >();
  private isolated = new Set<string>();

  transport(nodeId: string): RelaySequencerTransport {
    return {
      publish: (message) => {
        for (const [targetId, handler] of this.handlers) {
          if (
            targetId !== nodeId &&
            (this.isolated.has(nodeId) || this.isolated.has(targetId))
          ) {
            continue;
          }
          queueMicrotask(() => handler(message));
        }
      },
      subscribe: (handler) => {
        this.handlers.set(nodeId, handler);
        return () => {
          this.handlers.delete(nodeId);
        };
      },
    };
  }

  isolate(nodeId: string) {
    this.isolated.add(nodeId);
  }

  heal(nodeId: string) {
    this.isolated.delete(nodeId);
  }
}

function identities(count: number) {
  return Array.from({ length: count }, () => {
    const privateKey = generatePrivateKey();
    return { privateKey, publicKey: getPublicKey(privateKey) };
  });
}

function request(label: string): RelaySequencingRequest {
  return {
    guildId: "sequencer-test-guild",
    body: {
      type: "MESSAGE",
      guildId: "sequencer-test-guild",
      channelId: "general",
      messageId: `message-${label}`,
      content: label,
    },
    author: "author",
    signature: `signature-${label}`,
    createdAt: 1_800_000_000_000,
    clientEventId: `client-${label}`,
  };
}

function createCluster(options: { redirectFollowers?: boolean } = {}) {
  const keys = identities(3);
  const members = keys.map((key) => key.publicKey);
  const bus = new SequencerBus();
  const stores = keys.map(() => new MemoryStore());
  const heads = keys.map(() => ({ headSeq: -1, headHash: null as string | null }));
  const createNode = (index: number) =>
    new RelaySequencerConsensusCoordinator(
      {
        epoch: "sequencer-epoch-1",
        members,
        requiredVotes: 2,
        electionTimeoutMinMs: 100,
        electionTimeoutMaxMs: 190,
        heartbeatIntervalMs: 25,
		requestTimeoutMs: 1_500,
        redirectFollowers: options.redirectFollowers,
      },
      keys[index],
      {
        getSequencerState: (key) => stores[index].getSequencerState(key),
        putSequencerState: (key, state) =>
          stores[index].putSequencerState(key, state),
      },
      bus.transport(keys[index].publicKey),
      () => heads[index],
      () => 0,
    );
  return {
    keys,
    members,
    bus,
    stores,
    heads,
    nodes: keys.map((_, index) => createNode(index)),
    createNode,
  };
}

async function closeAll(
  nodes: Array<RelaySequencerConsensusCoordinator | undefined>,
) {
  await Promise.all(nodes.map((node) => node?.close()));
}

describe("relay sequencer consensus", () => {
  it("redirects a follower immediately when another relay owns the lease", async () => {
    const cluster = createCluster({ redirectFollowers: true });
    await Promise.all(cluster.nodes.map((node) => node.start()));

    const leaderToken = await cluster.nodes[0].sequence(request("elect-leader"));
    await cluster.nodes[0].complete(leaderToken);
    const follower = cluster.nodes.find(
      (node) => node.diagnostics("sequencer-test-guild").role === "follower",
    );
    expect(follower).toBeDefined();

    const startedAt = Date.now();
    await expect(
      follower!.sequence(request("redirected")),
    ).rejects.toThrow(
      `SEQUENCER_REDIRECT ${leaderToken.leaderPublicKey}`,
    );
    expect(Date.now() - startedAt).toBeLessThan(100);
    await closeAll(cluster.nodes);
  });

  it("persists election terms in LevelDB and treats a missing state as empty", async () => {
    const dbPath = `./test-sequencer-state-${randomUUID()}`;
    let openStore: LevelStore | undefined;
    try {
      openStore = new LevelStore(dbPath);
      expect(await openStore.getSequencerState("missing")).toBeUndefined();
      await openStore.putSequencerState("guild-state", {
        term: 7,
        votedFor: "relay-key",
      });
      await openStore.close();

      openStore = new LevelStore(dbPath);
      expect(await openStore.getSequencerState("guild-state")).toEqual({
        term: 7,
        votedFor: "relay-key",
      });
      await openStore.close();
      openStore = undefined;
    } finally {
      await openStore?.close().catch(() => undefined);
      await fs.rm(dbPath, { recursive: true, force: true });
    }
  });

  it("elects one leader and gives every relay the same sequencing token", async () => {
    const cluster = createCluster();
    await Promise.all(cluster.nodes.map((node) => node.start()));

    const tokens = await Promise.all(
      cluster.nodes.map((node) => node.sequence(request("first"))),
    );
    expect(new Set(tokens.map((token) => token.term)).size).toBe(1);
    expect(new Set(tokens.map((token) => token.leaderPublicKey)).size).toBe(1);
    expect(new Set(tokens.map((token) => token.requestId)).size).toBe(1);
    expect(
      cluster.nodes.filter(
        (node) => node.diagnostics("sequencer-test-guild").role === "leader",
      ),
    ).toHaveLength(1);

    await Promise.all(
      cluster.nodes.map((node, index) => node.complete(tokens[index])),
    );
    await closeAll(cluster.nodes);
  });

  it("serializes competing writers and releases the next request after completion", async () => {
    const cluster = createCluster();
    await Promise.all(cluster.nodes.map((node) => node.start()));
    const warmTokens = await Promise.all(
      cluster.nodes.map((node) => node.sequence(request("warm"))),
    );
    await Promise.all(
      cluster.nodes.map((node, index) => node.complete(warmTokens[index])),
    );

    const firstPromises = cluster.nodes.map((node) =>
      node.sequence(request("competing-a")),
    );
    const secondPromises = cluster.nodes.map((node) =>
      node.sequence(request("competing-b")),
    );
    const firstTokens = await Promise.all(firstPromises);
    let secondResolved = false;
    void Promise.all(secondPromises).then(() => {
      secondResolved = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(secondResolved).toBe(false);

    await Promise.all(
      cluster.nodes.map((node, index) => node.complete(firstTokens[index])),
    );
    const secondTokens = await Promise.all(secondPromises);
    expect(new Set(firstTokens.map((token) => token.requestId)).size).toBe(1);
    expect(new Set(secondTokens.map((token) => token.requestId)).size).toBe(1);
    expect(secondTokens[0].requestId).not.toBe(firstTokens[0].requestId);

    await Promise.all(
      cluster.nodes.map((node, index) => node.complete(secondTokens[index])),
    );
    await closeAll(cluster.nodes);
  });

  it("keeps an uncommitted proposal active for an exact retry", async () => {
    const cluster = createCluster();
    await Promise.all(cluster.nodes.map((node) => node.start()));
    const firstRequest = request("retry-after-quorum-timeout");
    const firstTokens = await Promise.all(
      cluster.nodes.map((node) => node.sequence(firstRequest)),
    );
    const leaderIndex = cluster.nodes.findIndex(
      (node) => node.diagnostics("sequencer-test-guild").role === "leader",
    );
    expect(leaderIndex).toBeGreaterThanOrEqual(0);

    await cluster.nodes[leaderIndex].complete(firstTokens[leaderIndex], false);
    const blockedNext = cluster.nodes.map((node) =>
      node.sequence(request("must-wait-for-retry")),
    );
    let nextResolved = false;
    void Promise.all(blockedNext).then(() => {
      nextResolved = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(nextResolved).toBe(false);

    const retryToken = await cluster.nodes[leaderIndex].sequence(firstRequest);
    expect(retryToken).toEqual(firstTokens[leaderIndex]);
    await cluster.nodes[leaderIndex].complete(retryToken, true);
    const nextTokens = await Promise.all(blockedNext);
    expect(nextTokens[0].requestId).not.toBe(retryToken.requestId);
    await Promise.all(
      cluster.nodes.map((node, index) => node.complete(nextTokens[index])),
    );
    await closeAll(cluster.nodes);
  });

  it("releases a deterministically rejected proposal for the next request", async () => {
    const cluster = createCluster();
    await Promise.all(cluster.nodes.map((node) => node.start()));
    const rejectedTokens = await Promise.all(
      cluster.nodes.map((node) => node.sequence(request("invalid-request"))),
    );
    const leaderIndex = cluster.nodes.findIndex(
      (node) => node.diagnostics("sequencer-test-guild").role === "leader",
    );
    expect(leaderIndex).toBeGreaterThanOrEqual(0);

    await cluster.nodes[leaderIndex].complete(rejectedTokens[leaderIndex], true);
    const nextTokens = await Promise.all(
      cluster.nodes.map((node) => node.sequence(request("valid-after-rejection"))),
    );
    expect(nextTokens[0].requestId).not.toBe(rejectedTokens[0].requestId);
    await Promise.all(
      cluster.nodes.map((node, index) => node.complete(nextTokens[index])),
    );
    await closeAll(cluster.nodes);
  });

  it("fails over after leader loss and preserves the monotonic term on restart", async () => {
    const cluster = createCluster();
    await Promise.all(cluster.nodes.map((node) => node.start()));
    const firstTokens = await Promise.all(
      cluster.nodes.map((node) => node.sequence(request("before-failure"))),
    );
    await Promise.all(
      cluster.nodes.map((node, index) => node.complete(firstTokens[index])),
    );
    const oldLeader = cluster.nodes.findIndex(
      (node) => node.diagnostics("sequencer-test-guild").role === "leader",
    );
    expect(oldLeader).toBeGreaterThanOrEqual(0);
    await cluster.nodes[oldLeader].close();

    const survivors = cluster.nodes
      .map((node, index) => ({ node, index }))
      .filter(({ index }) => index !== oldLeader);
    const failoverTokens = await Promise.all(
      survivors.map(({ node }) => node.sequence(request("after-failure"))),
    ).catch((error) => {
      throw new Error(
        `failover sequencing failed: ${String(error)} ${JSON.stringify(
          survivors.map(({ node }) =>
            node.diagnostics("sequencer-test-guild"),
          ),
        )}`,
      );
    });
    expect(failoverTokens[0].term).toBeGreaterThan(firstTokens[0].term);
    expect(failoverTokens[0].leaderPublicKey).not.toBe(
      firstTokens[0].leaderPublicKey,
    );
    await Promise.all(
      survivors.map(({ node }, index) => node.complete(failoverTokens[index])),
    );

    const restarted = cluster.createNode(oldLeader);
    cluster.nodes[oldLeader] = restarted;
    await restarted.start();
    const recoveredTokens = await Promise.all(
      cluster.nodes.map((node) => node.sequence(request("recovered"))),
    ).catch((error) => {
      throw new Error(`recovered sequencing failed: ${String(error)}`);
    });
    expect(recoveredTokens[0].term).toBe(failoverTokens[0].term);
    await Promise.all(
      cluster.nodes.map((node, index) => node.complete(recoveredTokens[index])),
    );
    await closeAll(cluster.nodes);

    cluster.nodes = cluster.keys.map((_, index) => cluster.createNode(index));
    await Promise.all(cluster.nodes.map((node) => node.start()));
    const postRestartTokens = await Promise.all(
      cluster.nodes.map((node) => node.sequence(request("post-restart"))),
    ).catch((error) => {
      throw new Error(`post-restart sequencing failed: ${String(error)}`);
    });
    expect(postRestartTokens[0].term).toBeGreaterThan(failoverTokens[0].term);
    await Promise.all(
      cluster.nodes.map((node, index) =>
        node.complete(postRestartTokens[index]),
      ),
    );
    await closeAll(cluster.nodes);
  });

  it("continues with the next queued request when a leader dies mid-slot", async () => {
    const cluster = createCluster();
    await Promise.all(cluster.nodes.map((node) => node.start()));
    const firstPromises = cluster.nodes.map((node) =>
      node.sequence(request("active-before-leader-loss")),
    );
    const secondPromises = cluster.nodes.map((node) =>
      node.sequence(request("queued-before-leader-loss")),
    );
    const firstTokens = await Promise.all(firstPromises);
    const oldLeader = cluster.nodes.findIndex(
      (node) => node.diagnostics("sequencer-test-guild").role === "leader",
    );
    expect(oldLeader).toBeGreaterThanOrEqual(0);
    const abandonedLeaderRequest = secondPromises[oldLeader].catch(
      (error) => error,
    );
    await cluster.nodes[oldLeader].close();
    expect(await abandonedLeaderRequest).toBeInstanceOf(Error);

    const survivingSecondTokens = await Promise.all(
      secondPromises.filter((_, index) => index !== oldLeader),
    );
    expect(survivingSecondTokens[0].term).toBeGreaterThan(firstTokens[0].term);
    expect(survivingSecondTokens[0].requestId).not.toBe(
      firstTokens[0].requestId,
    );

    const survivors = cluster.nodes.filter((_, index) => index !== oldLeader);
    await Promise.all(
      survivors.map((node, index) =>
        node.complete(survivingSecondTokens[index]),
      ),
    );
    await closeAll(cluster.nodes);
  });

  it("allows a majority partition and rejects an isolated minority", async () => {
    const cluster = createCluster();
    await Promise.all(cluster.nodes.map((node) => node.start()));
    cluster.bus.isolate(cluster.keys[2].publicKey);

    const majorityTokens = await Promise.all(
      cluster.nodes
        .slice(0, 2)
        .map((node) => node.sequence(request("majority"))),
    );
    expect(majorityTokens[0].leaderPublicKey).toBe(
      majorityTokens[1].leaderPublicKey,
    );
    await expect(
      cluster.nodes[2].sequence(request("minority")),
    ).rejects.toThrow("sequencer unavailable");

    await Promise.all(
      cluster.nodes
        .slice(0, 2)
        .map((node, index) => node.complete(majorityTokens[index])),
    );
    cluster.bus.heal(cluster.keys[2].publicKey);
    await closeAll(cluster.nodes);
  });
});
