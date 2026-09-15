import { afterAll, beforeAll, describe, expect, it } from "vitest";
import WebSocket from "ws";
import { MemoryStore, RelayServer } from "@cgp/relay";

type RelayFrame = [string, Record<string, any>];

function parseFrame(raw: WebSocket.RawData): RelayFrame {
  const value = JSON.parse(String(raw));
  if (!Array.isArray(value) || typeof value[0] !== "string") {
    throw new Error("Invalid relay frame");
  }
  return [value[0], value[1] && typeof value[1] === "object" ? value[1] : {}];
}

function waitForPort(relay: RelayServer) {
  return new Promise<number>((resolve, reject) => {
    const startedAt = Date.now();
    const timer = setInterval(() => {
      const port = relay.getPort();
      if (Number.isFinite(port) && port > 0) {
        clearInterval(timer);
        resolve(port);
        return;
      }
      if (Date.now() - startedAt > 5_000) {
        clearInterval(timer);
        reject(new Error("Timed out waiting for relay port"));
      }
    }, 25);
  });
}

function waitForFrame(socket: WebSocket, kind: string, timeoutMs = 5_000) {
  return new Promise<Record<string, any>>((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out waiting for ${kind}`));
    }, timeoutMs);
    const onMessage = (raw: WebSocket.RawData) => {
      const [receivedKind, body] = parseFrame(raw);
      if (receivedKind !== kind) {
        return;
      }
      cleanup();
      resolve(body);
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const cleanup = () => {
      clearTimeout(timeout);
      socket.off("message", onMessage);
      socket.off("error", onError);
    };
    socket.on("message", onMessage);
    socket.on("error", onError);
  });
}

async function openJoinedPeer(relayUrl: string, roomId: string, role: string) {
  const socket = new WebSocket(relayUrl);
  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
  const joined = waitForFrame(socket, "JOINED");
  socket.send(
    JSON.stringify([
      "JOIN",
      {
        roomId,
        metadata: {
          role,
          bridgeKind: "minecraft-web-client",
        },
      },
    ]),
  );
  return { socket, joined: await joined };
}

describe("Hollow room relay", () => {
  let relay: RelayServer | undefined;
  let port = 0;

  beforeAll(async () => {
    relay = new RelayServer(0, new MemoryStore());
    port = await waitForPort(relay);
  });

  afterAll(async () => {
    relay?.close();
  });

  it("advertises hollow-relay for Hollow game bridge discovery", async () => {
    const response = await fetch(
      `http://127.0.0.1:${port}/plugins/hollow-relay/relays`,
    );
    expect(response.ok).toBe(true);
    expect(response.headers.get("access-control-allow-origin")).toBe("*");

    const payload = await response.json();
    expect(payload.relays).toHaveLength(1);
    expect(payload.relays[0]).toMatchObject({
      provider: "cgp-relay",
      transport: "hollow-room",
      healthy: true,
      publicWsUrl: `ws://127.0.0.1:${port}`,
    });
  });

  it("routes Minecraft Web Client room peer traffic over WebSocket JOIN and DIRECT frames", async () => {
    const relayUrl = `ws://127.0.0.1:${port}`;
    const roomId = `minecraft-room-${Date.now()}`;
    const host = await openJoinedPeer(relayUrl, roomId, "host");
    const peer = await openJoinedPeer(relayUrl, roomId, "peer");

    try {
      expect(peer.joined.peers).toHaveLength(1);
      expect(peer.joined.peers[0]).toMatchObject({
        peerId: host.joined.peerId,
        metadata: {
          role: "host",
          bridgeKind: "minecraft-web-client",
        },
      });

      const direct = waitForFrame(host.socket, "DIRECT");
      peer.socket.send(
        JSON.stringify([
          "DIRECT",
          {
            roomId,
            toPeerId: host.joined.peerId,
            payload: Buffer.from("minecraft-packet").toString("base64"),
          },
        ]),
      );

      await expect(direct).resolves.toMatchObject({
        roomId,
        fromPeerId: peer.joined.peerId,
        payload: Buffer.from("minecraft-packet").toString("base64"),
      });
    } finally {
      host.socket.close();
      peer.socket.close();
    }
  });
});
