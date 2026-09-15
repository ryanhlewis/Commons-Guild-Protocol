import { describe, expect, it } from "vitest";
import { generatePrivateKey, getPublicKey, hashObject, sign } from "@cgp/core";
import { CgpClient } from "@cgp/client";
import {
  LocalRelayPubSubAdapter,
  MemoryStore,
  RelayServer,
  type RelayPubSubAdapter,
  type RelayPubSubEnvelope,
  type RelayPubSubSubscribeOptions,
  type RelaySequencerConsensusConfig,
  type RelayPluginContext,
  type RelayWriteQuorumConfig,
} from "@cgp/relay";

class DelayedGuildEventPubSubAdapter implements RelayPubSubAdapter {
  private inner = new LocalRelayPubSubAdapter();

  constructor(private delayMs: number) {}

  setDelay(delayMs: number) {
    this.delayMs = delayMs;
  }

  publish(topic: string, envelope: RelayPubSubEnvelope) {
    if (envelope.event || (envelope.events?.length ?? 0) > 0) {
      setTimeout(() => this.inner.publish(topic, envelope), this.delayMs);
      return;
    }
    this.inner.publish(topic, envelope);
  }

  subscribe(
    topic: string,
    handler: (envelope: RelayPubSubEnvelope) => void,
    options?: RelayPubSubSubscribeOptions,
  ) {
    return this.inner.subscribe(topic, handler, options);
  }

  isReady() {
    return true;
  }

  close() {
    this.inner.close();
  }
}

async function waitForConvergence(
  stores: MemoryStore[],
  guildId: string,
  eventCount: number,
  timeoutMs = 3_000,
) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const logs = stores.map((store) => store.getLog(guildId));
    if (logs.every((log) => log.length === eventCount)) {
      const eventIds = logs.map((log) => log.map((event) => event.id));
      if (eventIds.every((ids) => JSON.stringify(ids) === JSON.stringify(eventIds[0]))) {
        return logs;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(
    `Relays did not converge on ${eventCount} events: ${stores
      .map((store) => store.getLog(guildId).length)
      .join(",")}`,
  );
}

describe("relay sequencer integration", () => {
  it("retries the same selected proposal after witness catch-up misses one quorum deadline", async () => {
    const relayKeys = Array.from({ length: 2 }, () => generatePrivateKey());
    const members = relayKeys.map((key) => getPublicKey(key));
    const writeQuorum: RelayWriteQuorumConfig = {
      epoch: "retry-fenced-head-epoch",
      members,
      requiredVotes: 2,
      voteTimeoutMs: 100,
    };
    const sequencerConsensus: RelaySequencerConsensusConfig = {
      epoch: writeQuorum.epoch,
      members,
      requiredVotes: 2,
      electionTimeoutMinMs: 60,
      electionTimeoutMaxMs: 100,
      heartbeatIntervalMs: 20,
      requestTimeoutMs: 1_500,
    };
    const pubSub = new DelayedGuildEventPubSubAdapter(180);
    const stores = members.map(() => new MemoryStore());
    const contexts: RelayPluginContext[] = [];
    const relays = relayKeys.map((privateKey, index) => new RelayServer(
      0,
      stores[index],
      [{
        name: `retry-fenced-head-${index}`,
        onInit(context) {
          contexts[index] = context;
        },
      }],
      {
        enableDefaultPlugins: false,
        instanceId: `retry-fenced-head-${index}`,
        relayPrivateKeyHex: Buffer.from(privateKey).toString("hex"),
        pubSubAdapter: pubSub,
        writeQuorum,
        sequencerConsensus,
      },
    ));
    const ownerPrivateKey = generatePrivateKey();
    const ownerPublicKey = getPublicKey(ownerPrivateKey);
    const guildId = hashObject({ test: "retry-fenced-head", nonce: Date.now() });
    const signed = async (body: any, createdAt: number, clientEventId: string) => {
      const unsigned = { body, author: ownerPublicKey, createdAt };
      return {
        ...unsigned,
        signature: await sign(ownerPrivateKey, hashObject(unsigned)),
        clientEventId,
      };
    };

    try {
      const deadline = Date.now() + 1_000;
      while (contexts.length < relays.length && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      expect(contexts).toHaveLength(2);
      const create = await signed({
        type: "GUILD_CREATE",
        guildId,
        name: "Retry fenced head",
      }, Date.now(), "retry-fenced-create");
      await contexts[0].publishSignedEvent!(create);

      const channel = await signed({
        type: "CHANNEL_CREATE",
        guildId,
        channelId: "general",
        name: "general",
        kind: "text",
      }, Date.now() + 1, "retry-fenced-channel");
      await expect(contexts[0].publishSignedEvent!(channel)).rejects.toThrow(/1\/2 votes/i);
      await new Promise((resolve) => setTimeout(resolve, 220));
      pubSub.setDelay(0);
      const committed = await contexts[0].publishSignedEvent!(channel);
      expect(committed?.body).toMatchObject({ type: "CHANNEL_CREATE", channelId: "general" });
      await waitForConvergence(stores, guildId, 2);
    } finally {
      await Promise.all(relays.map((relay) => relay.close()));
      pubSub.close();
    }
  });

  it("waits for a lagging witness before voting on the next selected head", async () => {
    const relayKeys = Array.from({ length: 2 }, () => generatePrivateKey());
    const members = relayKeys.map((key) => getPublicKey(key));
    const writeQuorum: RelayWriteQuorumConfig = {
      epoch: "lagging-witness-epoch",
      members,
      requiredVotes: 2,
      voteTimeoutMs: 700,
    };
    const sequencerConsensus: RelaySequencerConsensusConfig = {
      epoch: writeQuorum.epoch,
      members,
      requiredVotes: 2,
      electionTimeoutMinMs: 60,
      electionTimeoutMaxMs: 100,
      heartbeatIntervalMs: 20,
      requestTimeoutMs: 1_500,
    };
    const pubSub = new DelayedGuildEventPubSubAdapter(150);
    const stores = members.map(() => new MemoryStore());
    const relays = relayKeys.map((privateKey, index) => new RelayServer(
      0,
      stores[index],
      [],
      {
        enableDefaultPlugins: false,
        instanceId: `lagging-witness-${index}`,
        relayPrivateKeyHex: Buffer.from(privateKey).toString("hex"),
        pubSubAdapter: pubSub,
        writeQuorum,
        sequencerConsensus,
      },
    ));
    const ownerPrivateKey = generatePrivateKey();
    const ownerPublicKey = getPublicKey(ownerPrivateKey);
    const client = new CgpClient({
      relays: [`ws://localhost:${relays[0].getPort()}`],
      keyPair: { pub: ownerPublicKey, priv: ownerPrivateKey },
    });
    const guildId = hashObject({ test: "lagging-witness", nonce: Date.now() });

    try {
      await client.connect();
      await client.publishReliable({
        type: "GUILD_CREATE",
        guildId,
        name: "Lagging witness",
      }, { clientEventId: "lagging-create", timeoutMs: 4_000 });
      // Do not wait for replication: the next quorum proposal deliberately
      // arrives while the second witness is still one durable event behind.
      await client.publishReliable({
        type: "CHANNEL_CREATE",
        guildId,
        channelId: "general",
        name: "general",
        kind: "text",
      }, { clientEventId: "lagging-channel", timeoutMs: 4_000 });
      const converged = await waitForConvergence(stores, guildId, 2);
      expect(converged[0].map((event) => event.id)).toEqual(
        converged[1].map((event) => event.id),
      );
    } finally {
      client.close();
      await Promise.all(relays.map((relay) => relay.close()));
      pubSub.close();
    }
  });

  it("requires every witness plugin to authorize a policy-sideband write", async () => {
    const relayKeys = Array.from({ length: 2 }, () => generatePrivateKey());
    const members = relayKeys.map((key) => getPublicKey(key));
    const writeQuorum: RelayWriteQuorumConfig = {
      epoch: "plugin-policy-epoch",
      members,
      requiredVotes: 2,
      voteTimeoutMs: 250,
    };
    const sequencerConsensus: RelaySequencerConsensusConfig = {
      epoch: writeQuorum.epoch,
      members,
      requiredVotes: 2,
      electionTimeoutMinMs: 60,
      electionTimeoutMaxMs: 100,
      heartbeatIntervalMs: 20,
      requestTimeoutMs: 1_000,
    };
    const pubSub = new LocalRelayPubSubAdapter();
    const stores = members.map(() => new MemoryStore());
    const contexts: RelayPluginContext[] = [];
    const witnessAllows = [true, false];
    let joiningAuthor = "";
    const relays = relayKeys.map((privateKey, index) => new RelayServer(
      0,
      stores[index],
      [{
        name: `policy-witness-${index}`,
        onInit(context) {
          contexts[index] = context;
        },
        onValidatePolicyAuthorizedEvent({ proposal }, context) {
          const body = proposal.body as any;
          return witnessAllows[index] &&
            proposal.authorizationMode === "plugin-policy" &&
            body?.type === "ROLE_ASSIGN" &&
            body.guildId === proposal.guildId &&
            body.roleId === "test-authority" &&
            body.userId === joiningAuthor &&
            proposal.author === joiningAuthor &&
            hashObject(body.certifier) === hashObject(context.writeQuorumPolicy);
        },
      }],
      {
        enableDefaultPlugins: false,
        instanceId: `policy-witness-${index}`,
        relayPrivateKeyHex: Buffer.from(privateKey).toString("hex"),
        pubSubAdapter: pubSub,
        writeQuorum,
        sequencerConsensus,
      },
    ));
    const ownerPrivateKey = generatePrivateKey();
    const ownerPublicKey = getPublicKey(ownerPrivateKey);
    const joiningPrivateKey = generatePrivateKey();
    joiningAuthor = getPublicKey(joiningPrivateKey);

    const publishToAll = async (body: any, clientEventId: string) => {
      const createdAt = Date.now();
      const unsigned = { body, author: ownerPublicKey, createdAt };
      const event = {
        ...unsigned,
        signature: await sign(ownerPrivateKey, hashObject(unsigned)),
        clientEventId,
      };
      return await Promise.all(
        contexts.map((context) => context.publishSignedEvent!(event)),
      );
    };

    try {
      const deadline = Date.now() + 1_000;
      while (contexts.length < relays.length && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      expect(contexts).toHaveLength(relays.length);

      const rejectedGuildId = hashObject({ test: "policy-rejected", nonce: Date.now() });
      await publishToAll({
        type: "GUILD_CREATE",
        guildId: rejectedGuildId,
        name: "Policy rejected",
      }, "policy-rejected-create");
      await waitForConvergence(stores, rejectedGuildId, 1);
      const rotationBody = {
        type: "ROLE_ASSIGN",
        guildId: rejectedGuildId,
        roleId: "test-authority",
        userId: joiningAuthor,
        certifier: contexts[0].writeQuorumPolicy,
      };
      const rotationCreatedAt = Date.now();
      const rotationUnsigned = {
        body: rotationBody,
        author: joiningAuthor,
        createdAt: rotationCreatedAt,
      };
      const rotationEvent = {
        ...rotationUnsigned,
        signature: await sign(joiningPrivateKey, hashObject(rotationUnsigned)),
        clientEventId: "policy-rotation",
      };
      await expect(
        contexts[0].publishPolicyAuthorizedEvent!(rotationEvent),
      ).rejects.toThrow(/1\/2 votes/i);
      expect(stores.every((store) => store.getLog(rejectedGuildId).length === 1)).toBe(true);

      witnessAllows[1] = true;
      const accepted = await contexts[0].publishPolicyAuthorizedEvent!(rotationEvent);
      expect(accepted?.writeCertificate?.votes).toHaveLength(2);
      await waitForConvergence(stores, rejectedGuildId, 2);
    } finally {
      await Promise.all(relays.map((relay) => relay.close()));
      pubSub.close();
    }
  });

  it("returns an idempotent event to every plugin fanout caller", async () => {
    const relayKeys = Array.from({ length: 3 }, () => generatePrivateKey());
    const members = relayKeys.map((key) => getPublicKey(key));
    const writeQuorum: RelayWriteQuorumConfig = {
      epoch: "plugin-fanout-epoch",
      members,
      requiredVotes: 2,
      voteTimeoutMs: 700,
    };
    const sequencerConsensus: RelaySequencerConsensusConfig = {
      epoch: writeQuorum.epoch,
      members,
      requiredVotes: 2,
      electionTimeoutMinMs: 100,
      electionTimeoutMaxMs: 190,
      heartbeatIntervalMs: 25,
      requestTimeoutMs: 1_500,
    };
    const pubSub = new LocalRelayPubSubAdapter();
    const stores = members.map(() => new MemoryStore());
    const contexts: RelayPluginContext[] = [];
    const relays = relayKeys.map(
      (privateKey, index) =>
        new RelayServer(0, stores[index], [{
          name: `fanout-${index}`,
          onInit(context) {
            contexts[index] = context;
          },
        }], {
          enableDefaultPlugins: false,
          instanceId: `plugin-fanout-${index}`,
          relayPrivateKeyHex: Buffer.from(privateKey).toString("hex"),
          pubSubAdapter: pubSub,
          writeQuorum,
          sequencerConsensus,
        }),
    );
    const authorityPrivateKey = generatePrivateKey();
    const authorityPublicKey = getPublicKey(authorityPrivateKey);
    const guildId = hashObject({ test: "plugin-fanout", nonce: Date.now() });

    try {
      const deadline = Date.now() + 1_000;
      while (contexts.length < relays.length && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      expect(contexts).toHaveLength(relays.length);
      const publish = async (body: any, createdAt: number, clientEventId: string) => {
        const unsigned = { body, author: authorityPublicKey, createdAt };
        const signature = await sign(authorityPrivateKey, hashObject(unsigned));
        return await Promise.all(contexts.map((context) => context.publishSignedEvent!({
          ...unsigned,
          signature,
          clientEventId,
        })));
      };
      const created = await publish({
        type: "GUILD_CREATE",
        guildId,
        name: "Plugin fanout",
      }, Date.now(), "plugin-fanout-create");
      expect(created.every((event) => event?.id === created[0]?.id)).toBe(true);
      await waitForConvergence(stores, guildId, 1);

      const updated = await publish({
        type: "GUILD_UPDATE",
        guildId,
        name: "Plugin fanout updated",
      }, Date.now() + 1, "plugin-fanout-update");
      expect(updated.every((event) => event?.id === updated[0]?.id)).toBe(true);
      await waitForConvergence(stores, guildId, 2);
    } finally {
      await Promise.all(relays.map((relay) => relay.close()));
      pubSub.close();
    }
  });

  it("orders competing websocket writers and fails over without a fork", async () => {
    const relayKeys = Array.from({ length: 3 }, () => generatePrivateKey());
    const members = relayKeys.map((key) => getPublicKey(key));
    const writeQuorum: RelayWriteQuorumConfig = {
      epoch: "relay-integration-epoch",
      members,
      requiredVotes: 2,
      voteTimeoutMs: 700,
    };
    const sequencerConsensus: RelaySequencerConsensusConfig = {
      epoch: writeQuorum.epoch,
      members,
      requiredVotes: 2,
      electionTimeoutMinMs: 100,
      electionTimeoutMaxMs: 190,
      heartbeatIntervalMs: 25,
      requestTimeoutMs: 1_500,
    };
    const pubSub = new LocalRelayPubSubAdapter();
    const stores = members.map(() => new MemoryStore());
    const relays = relayKeys.map(
      (privateKey, index) =>
        new RelayServer(0, stores[index], [], {
          enableDefaultPlugins: false,
          instanceId: `sequencer-integration-${index}`,
          relayPrivateKeyHex: Buffer.from(privateKey).toString("hex"),
          pubSubAdapter: pubSub,
          writeQuorum,
          sequencerConsensus,
        }),
    );
    const relayUrls = relays.map(
      (relay) => `ws://localhost:${relay.getPort()}`,
    );
    const ownerPrivateKey = generatePrivateKey();
    const ownerPublicKey = getPublicKey(ownerPrivateKey);
    const firstWriter = new CgpClient({
      relays: relayUrls,
      keyPair: { pub: ownerPublicKey, priv: ownerPrivateKey },
    });
    const secondWriter = new CgpClient({
      relays: relayUrls,
      keyPair: { pub: ownerPublicKey, priv: ownerPrivateKey },
    });
    const guildId = hashObject({
      test: "sequencer-relay-integration",
      nonce: Date.now(),
    });
    const channelId = "general";

    try {
      await Promise.all([firstWriter.connect(), secondWriter.connect()]);
      await firstWriter.publishReliable(
        {
          type: "GUILD_CREATE",
          guildId,
          name: "Sequencer integration",
        },
        { timeoutMs: 4_000 },
      );
      await waitForConvergence(stores, guildId, 1);
      await firstWriter.publishReliable(
        {
          type: "CHANNEL_CREATE",
          guildId,
          channelId,
          name: "general",
          kind: "text",
        },
        { timeoutMs: 4_000 },
      );
      await waitForConvergence(stores, guildId, 2);

      await expect(
        firstWriter.publishReliable(
          {
            type: "GUILD_CREATE",
            guildId,
            name: "Invalid duplicate genesis",
          },
          { clientEventId: "invalid-duplicate-genesis", timeoutMs: 4_000 },
        ),
      ).rejects.toThrow();
      await firstWriter.publishReliable(
        {
          type: "MESSAGE",
          guildId,
          channelId,
          messageId: "valid-after-rejection",
          content: "sequencer remained live",
        },
        { clientEventId: "valid-after-rejection", timeoutMs: 4_000 },
      );
      await waitForConvergence(stores, guildId, 3);

      await Promise.all([
        firstWriter.publishReliable(
          {
            type: "MESSAGE",
            guildId,
            channelId,
            messageId: "competing-a",
            content: "competing writer A",
          },
          { clientEventId: "competing-a", timeoutMs: 4_000 },
        ),
        secondWriter.publishReliable(
          {
            type: "MESSAGE",
            guildId,
            channelId,
            messageId: "competing-b",
            content: "competing writer B",
          },
          { clientEventId: "competing-b", timeoutMs: 4_000 },
        ),
      ]);
      const converged = await waitForConvergence(stores, guildId, 5);
      expect(
        converged[0].slice(3).map((event) => (event.body as any).messageId),
      ).toEqual(expect.arrayContaining(["competing-a", "competing-b"]));

      await relays[0].close();
      await firstWriter.publishReliable(
        {
          type: "MESSAGE",
          guildId,
          channelId,
          messageId: "after-leader-loss",
          content: "survived leader loss",
        },
        { clientEventId: "after-leader-loss", timeoutMs: 4_000 },
      );
      const survivorLogs = await waitForConvergence(
        stores.slice(1),
        guildId,
        6,
      );
      expect(survivorLogs[0][5]?.body).toMatchObject({
        type: "MESSAGE",
        messageId: "after-leader-loss",
      });
      expect(survivorLogs[0][5]?.id).toBe(survivorLogs[1][5]?.id);
    } finally {
      firstWriter.close();
      secondWriter.close();
      await Promise.all(relays.map((relay) => relay.close()));
      pubSub.close();
    }
  }, 15_000);
});
