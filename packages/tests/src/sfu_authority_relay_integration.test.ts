import { describe, expect, it } from "vitest";
import {
  generatePrivateKey,
  getPublicKey,
  hashObject,
  verifyRelayWriteCertificate,
} from "@cgp/core";
import { CgpClient } from "@cgp/client";
import {
  MemoryStore,
  RelayServer,
  type RelayPubSubAdapter,
  type RelayPubSubEnvelope,
} from "@cgp/relay";

class PartitionHub {
  private handlers = new Map<
    string,
    Map<number, Set<(envelope: RelayPubSubEnvelope) => void>>
  >();
  private retained: Array<{ topic: string; envelope: RelayPubSubEnvelope }> = [];
  private groupByNode = new Map<number, number>();

  adapter(node: number): RelayPubSubAdapter {
    return {
      publish: (topic, envelope) => {
        this.retained.push({ topic, envelope });
        this.deliver(node, topic, envelope);
      },
      subscribe: (topic, handler) => {
        const byNode = this.handlers.get(topic) ?? new Map();
        const handlers = byNode.get(node) ?? new Set();
        handlers.add(handler);
        byNode.set(node, handlers);
        this.handlers.set(topic, byNode);
        return () => handlers.delete(handler);
      },
    };
  }

  partition(groups: number[][]) {
    this.groupByNode.clear();
    groups.forEach((nodes, group) =>
      nodes.forEach((node) => this.groupByNode.set(node, group)),
    );
  }

  heal() {
    this.groupByNode.clear();
    for (const { topic, envelope } of this.retained) {
      const byNode = this.handlers.get(topic);
      if (!byNode) continue;
      for (const handlers of byNode.values()) {
        for (const handler of handlers) queueMicrotask(() => handler(envelope));
      }
    }
  }

  private deliver(
    source: number,
    topic: string,
    envelope: RelayPubSubEnvelope,
  ) {
    const byNode = this.handlers.get(topic);
    if (!byNode) return;
    const sourceGroup = this.groupByNode.get(source);
    for (const [target, handlers] of byNode) {
      const targetGroup = this.groupByNode.get(target);
      if (
        sourceGroup !== undefined &&
        targetGroup !== undefined &&
        sourceGroup !== targetGroup
      ) continue;
      for (const handler of handlers) queueMicrotask(() => handler(envelope));
    }
  }
}

async function waitFor(
  predicate: () => boolean,
  description: string,
  timeoutMs = 5_000,
) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`Timed out waiting for ${description}`);
}

describe("five-relay SFU authority hard gate", () => {
  it("certifies explicitly quorum-bound application objects", async () => {
    const relayKeys = Array.from({ length: 3 }, generatePrivateKey);
    const members = relayKeys.map(getPublicKey);
    const stores = members.map(() => new MemoryStore());
    const hub = new PartitionHub();
    const writeQuorum = {
      epoch: "certified-app-object-epoch",
      members,
      requiredVotes: 2,
      voteTimeoutMs: 500,
    };
    const relays = relayKeys.map(
      (privateKey, index) =>
        new RelayServer(0, stores[index], [], {
          enableDefaultPlugins: false,
          instanceId: `certified-app-object-${index}`,
          relayPrivateKeyHex: Buffer.from(privateKey).toString("hex"),
          pubSubAdapter: hub.adapter(index),
          writeQuorum,
        }),
    );
    const ownerPrivateKey = generatePrivateKey();
    const ownerPublicKey = getPublicKey(ownerPrivateKey);
    const client = new CgpClient({
      relays: relays.map((relay) => `ws://localhost:${relay.getPort()}`),
      keyPair: { pub: ownerPublicKey, priv: ownerPrivateKey },
    });
    const guildId = hashObject({
      certifiedAppObject: true,
      nonce: Date.now(),
    });
    const certifier = {
      protocol: "cgp/write-quorum/1" as const,
      epoch: writeQuorum.epoch,
      members,
      requiredVotes: 2,
    };
    try {
      await client.connect();
      await client.publishReliable(
        { type: "GUILD_CREATE", guildId, name: "Certified app object" },
        { timeoutMs: 4_000 },
      );
      await waitFor(
        () => stores.every((store) => store.getLog(guildId).length === 1),
        "certified app genesis",
      );
      await client.publishReliable({
        type: "APP_OBJECT_UPSERT",
        guildId,
        namespace: "avera.game",
        objectType: "security-evidence-component.v1",
        objectId: "opaque-player.region-a",
        certifier,
        value: { sequence: 1 },
      } as any, { timeoutMs: 4_000 });
      await waitFor(
        () => stores.every((store) => store.getLog(guildId).length === 2),
        "certified app object on every relay",
      );
      const event = stores[0].getLog(guildId)[1];
      expect(verifyRelayWriteCertificate(event, {
        expectedGuildId: guildId,
        expectedAuthor: ownerPublicKey,
        requireBodyPolicy: true,
      })).toBe(true);
      expect(verifyRelayWriteCertificate({
        ...event,
        body: {
          ...event.body,
          value: { sequence: 2 },
        },
      }, {
        expectedGuildId: guildId,
        expectedAuthor: ownerPublicKey,
        requireBodyPolicy: true,
      })).toBe(false);
    } finally {
      client.close();
      await Promise.all(relays.map((relay) => relay.close()));
    }
  }, 10_000);

  it("rejects a 2-node partition, commits on 3 nodes, heals, and rotates after a relay death", async () => {
    const relayKeys = Array.from({ length: 5 }, generatePrivateKey);
    const members = relayKeys.map(getPublicKey);
    const stores = members.map(() => new MemoryStore());
    const hub = new PartitionHub();
    const writeQuorum = {
      epoch: "sfu-authority-hard-gate",
      members,
      requiredVotes: 3,
      voteTimeoutMs: 500,
    };
    const relays = relayKeys.map(
      (privateKey, index) =>
        new RelayServer(0, stores[index], [], {
          enableDefaultPlugins: false,
          instanceId: `sfu-authority-${index}`,
          relayPrivateKeyHex: Buffer.from(privateKey).toString("hex"),
          pubSubAdapter: hub.adapter(index),
          writeQuorum,
        }),
    );
    const ownerPrivateKey = generatePrivateKey();
    const ownerPublicKey = getPublicKey(ownerPrivateKey);
    const client = new CgpClient({
      relays: relays.map((relay) => `ws://localhost:${relay.getPort()}`),
      keyPair: { pub: ownerPublicKey, priv: ownerPrivateKey },
    });
    const guildId = hashObject({ gate: "five-relay-sfu-authority", nonce: Date.now() });
    const routeKeys = Array.from({ length: 3 }, () =>
      getPublicKey(generatePrivateKey()),
    );
    const policy = {
      protocol: "cgp/write-quorum/1" as const,
      epoch: writeQuorum.epoch,
      members,
      requiredVotes: 3,
    };
    const authorityBody = (epoch: number) => {
      const now = Date.now();
      return {
        type: "SFU_AUTHORITY_SET" as const,
        guildId,
        epoch,
        previousEpoch: epoch === 1 ? null : epoch - 1,
        notBefore: now,
        overlapUntil: now + 30_000,
        expiresAt: now + 3_600_000,
        certifier: policy,
        authorities: [
          {
            nodeId: `sfu-${epoch}`,
            clusterId: "hard-gate-cluster",
            role: "authority" as const,
            routeAuthorityPublicKey: routeKeys[(epoch - 1) % routeKeys.length],
          },
        ],
      };
    };

    try {
      await client.connect();
      await client.publishReliable(
        { type: "GUILD_CREATE", guildId, name: "SFU authority hard gate" },
        { timeoutMs: 4_000 },
      );
      await waitFor(
        () => stores.every((store) => store.getLog(guildId).length === 1),
        "genesis on five relays",
      );
      await client.publishReliable(authorityBody(1), { timeoutMs: 4_000 });
      await waitFor(
        () => stores.every((store) => store.getLog(guildId).length === 2),
        "epoch 1 on five relays",
      );
      expect(
        stores.every((store) =>
          verifyRelayWriteCertificate(store.getLog(guildId)[1], {
            expectedGuildId: guildId,
            expectedAuthor: ownerPublicKey,
            requireBodyPolicy: true,
          }),
        ),
      ).toBe(true);

      hub.partition([[0, 1], [2, 3, 4]]);
      await client.publishReliable(authorityBody(2), { timeoutMs: 4_000 });
      await waitFor(
        () => stores.slice(2).every((store) => store.getLog(guildId).length === 3),
        "epoch 2 on the 3-node majority",
      );
      expect(stores.slice(0, 2).map((store) => store.getLog(guildId).length)).toEqual([2, 2]);
      expect(
        stores.slice(2).every((store) =>
          verifyRelayWriteCertificate(store.getLog(guildId)[2], {
            expectedGuildId: guildId,
            expectedAuthor: ownerPublicKey,
            requireBodyPolicy: true,
          }),
        ),
      ).toBe(true);

      hub.heal();
      await waitFor(
        () => stores.every((store) => store.getLog(guildId).length === 3),
        "minority catch-up after healing",
      );
      await relays[4].close();
      await client.publishReliable(authorityBody(3), { timeoutMs: 4_000 });
      await waitFor(
        () => stores.slice(0, 4).every((store) => store.getLog(guildId).length === 4),
        "epoch 3 after one relay death",
      );
      expect(
        verifyRelayWriteCertificate(stores[0].getLog(guildId)[3], {
          expectedGuildId: guildId,
          expectedAuthor: ownerPublicKey,
          requireBodyPolicy: true,
        }),
      ).toBe(true);
    } finally {
      client.close();
      await Promise.all(relays.map((relay) => relay.close()));
    }
  }, 20_000);
});
