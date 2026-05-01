import { afterEach, describe, expect, it } from "vitest";
import { createServer, Server } from "http";
import { rm } from "fs/promises";
import {
  generatePrivateKey,
  getPublicKey,
  hashObject,
  sign,
  type GuildEvent,
} from "@cgp/core";
import {
  createRelayPushPlugin,
  type RelayPluginContext,
} from "@cgp/relay/src/plugins";

const tempFiles: string[] = [];
const servers: Server[] = [];

afterEach(async () => {
  await Promise.all(
    servers
      .splice(0)
      .map(
        (server) =>
          new Promise<void>((resolve) => server.close(() => resolve())),
      ),
  );
  await Promise.all(
    tempFiles.splice(0).map((file) => rm(file, { force: true })),
  );
});

describe("relay push plugin", () => {
  it("accepts signed UnifiedPush registrations and posts wake payloads", async () => {
    const received: string[] = [];
    const endpoint = await listenForPushes(received);
    const registryPath = tempRegistry();
    const account = await createIdentity();
    const sender = await createIdentity();
    const plugin = createRelayPushPlugin({
      registryPath,
      publicRelayUrl: "ws://localhost:7447",
      deliveryTimeoutMs: 2000,
    });

    await plugin.onInit?.(ctx());
    await plugin.onConfig?.(
      {
        socket: {} as any,
        config: {
          protocol: "hollow.relay-push/1",
          action: "register",
          registration: await signedPayload(
            account,
            "cgp.relay.push.register",
            {
              version: 1,
              provider: "unifiedpush",
              endpointKind: "webpush",
              endpoint,
              deviceId: "phone-1",
              account: account.publicKey,
              wakeOnly: true,
              includePreviews: false,
              includeDirectMessages: true,
              requireRelayHead: true,
              guildIds: ["guild-a", "@me"],
            },
          ),
        },
      },
      ctx(),
    );

    await plugin.onEventsAppended?.(
      {
        events: [
          event(sender.publicKey, {
            type: "MESSAGE",
            guildId: "guild-a",
            channelId: "general",
            messageId: "m1",
            content: "secret text that should not be pushed",
          }),
        ],
      },
      ctx(),
    );

    expect(received.length).toBe(1);
    const payload = JSON.parse(received[0]);
    expect(payload.protocol).toBe("hollow.relay-push/1");
    expect(payload.kind).toBe("message");
    expect(payload.guildId).toBe("guild-a");
    expect(payload.channelId).toBe("general");
    expect(payload.messageId).toBe("m1");
    expect(payload.relayUrl).toBe("ws://localhost:7447");
    expect(JSON.stringify(payload)).not.toContain("secret text");
  });

  it("routes targeted call presence only to the invited account", async () => {
    const invitedReceived: string[] = [];
    const otherReceived: string[] = [];
    const invitedEndpoint = await listenForPushes(invitedReceived);
    const otherEndpoint = await listenForPushes(otherReceived);
    const registryPath = tempRegistry();
    const invited = await createIdentity();
    const other = await createIdentity();
    const sender = await createIdentity();
    const plugin = createRelayPushPlugin({
      registryPath,
      publicRelayUrl: "ws://localhost:7447",
      deliveryTimeoutMs: 2000,
    });

    await plugin.onInit?.(ctx());
    for (const [identity, endpoint] of [
      [invited, invitedEndpoint],
      [other, otherEndpoint],
    ] as const) {
      await plugin.onConfig?.(
        {
          socket: {} as any,
          config: {
            protocol: "hollow.relay-push/1",
            action: "register",
            registration: await signedPayload(
              identity,
              "cgp.relay.push.register",
              {
                version: 1,
                provider: "unifiedpush",
                endpointKind: "webpush",
                endpoint,
                deviceId: `phone-${identity.publicKey.slice(0, 8)}`,
                account: identity.publicKey,
                wakeOnly: true,
                includePreviews: false,
                includeDirectMessages: true,
                requireRelayHead: true,
                guildIds: ["unrelated-guild"],
              },
            ),
          },
        },
        ctx(),
      );
    }

    await plugin.onEventsAppended?.(
      {
        events: [
          event(sender.publicKey, {
            type: "CALL_EVENT",
            guildId: "guild-call",
            channelId: "voice-1",
            localServerId: "guild-call",
            payload: {
              kind: "presence",
              messageId: "call-presence-1",
              roomId: "server:guild-call:channel:voice-1",
              fromUserId: sender.publicKey,
              toUserId: invited.publicKey,
              timestamp: Date.now(),
            },
          }),
        ],
      },
      ctx(),
    );

    expect(invitedReceived.length).toBe(1);
    expect(otherReceived.length).toBe(0);
    const payload = JSON.parse(invitedReceived[0]);
    expect(payload.kind).toBe("incoming-call");
    expect(payload.callKind).toBe("presence");
    expect(payload.targetUserId).toBe(invited.publicKey);
    expect(payload.guildId).toBe("guild-call");
    expect(payload.channelId).toBe("voice-1");
  });

  it("rejects forged registrations", async () => {
    const registryPath = tempRegistry();
    const account = await createIdentity();
    const attacker = await createIdentity();
    const plugin = createRelayPushPlugin({ registryPath });
    await plugin.onInit?.(ctx());

    const forged = await signedPayload(attacker, "cgp.relay.push.register", {
      version: 1,
      provider: "unifiedpush",
      endpointKind: "webpush",
      endpoint: "http://127.0.0.1:1/push",
      deviceId: "phone-1",
      account: account.publicKey,
      wakeOnly: true,
      includePreviews: false,
      includeDirectMessages: true,
      requireRelayHead: true,
      guildIds: ["guild-a"],
    });

    await expect(
      plugin.onConfig?.(
        {
          socket: {} as any,
          config: {
            protocol: "hollow.relay-push/1",
            action: "register",
            registration: forged,
          },
        },
        ctx(),
      ),
    ).rejects.toThrow(/account must match/i);
  });
});

async function listenForPushes(received: string[]) {
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    req.on("end", () => {
      received.push(Buffer.concat(chunks).toString("utf8"));
      res.statusCode = 202;
      res.end("ok");
    });
  });
  servers.push(server);
  await new Promise<void>((resolve) =>
    server.listen(0, "127.0.0.1", () => resolve()),
  );
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("Failed to bind push test server");
  }
  return `http://127.0.0.1:${address.port}/push`;
}

function tempRegistry() {
  const file = `./.tmp/relay-push-test-${Date.now()}-${Math.random().toString(36).slice(2)}.json`;
  tempFiles.push(file);
  return file;
}

function ctx(): RelayPluginContext {
  return {
    relayPublicKey: "relay",
    store: {} as any,
    publishAsRelay: async () => undefined,
    broadcast: () => undefined,
    getLog: async () => [],
  };
}

async function createIdentity() {
  const privateKey = generatePrivateKey();
  return {
    privateKey,
    publicKey: getPublicKey(privateKey),
  };
}

async function signedPayload(
  identity: Awaited<ReturnType<typeof createIdentity>>,
  kind: string,
  payload: Record<string, unknown>,
) {
  const createdAt = Date.now();
  const unsignedPayload = {
    ...payload,
    author: identity.publicKey,
    createdAt,
  };
  const signature = await sign(
    identity.privateKey,
    hashObject({ kind, payload: unsignedPayload }),
  );
  return {
    ...unsignedPayload,
    signature,
  };
}

function event(author: string, body: Record<string, unknown>): GuildEvent {
  const createdAt = Date.now();
  const base = {
    seq: 7,
    prevHash: "prev",
    createdAt,
    author,
    body: body as any,
  };
  return {
    ...base,
    id: hashObject(base),
    signature: "sig",
  };
}
