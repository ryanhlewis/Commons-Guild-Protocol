import { execFileSync } from "node:child_process";
import { createHash, X509Certificate } from "node:crypto";
import dgram from "node:dgram";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import http from "node:http";
import { tmpdir } from "node:os";
import path from "node:path";
import { CgpClient } from "@cgp/client";
import { CgpWebTransportDatagramClient } from "@cgp/client";
import { generatePrivateKey, getPublicKey } from "@cgp/core";
import {
  LocalRelayPubSubAdapter,
  RelayServer,
} from "@cgp/relay/src/server";
import { MemoryStore } from "@cgp/relay/src/store";

interface Observation {
  relayIndex: number;
  eventId: string;
  messageId: string;
  payloadHash: string;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor<T>(
  label: string,
  predicate: () => T | undefined | false | Promise<T | undefined | false>,
  timeoutMs = 8_000,
) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const value = await predicate();
    if (value) {
      return value;
    }
    await sleep(20);
  }
  throw new Error(`Timed out waiting for ${label}`);
}

async function reserveUdpPort() {
  const socket = dgram.createSocket("udp4");
  return await new Promise<number>((resolve, reject) => {
    socket.once("error", reject);
    socket.bind(0, "127.0.0.1", () => {
      const address = socket.address();
      socket.close(() => resolve(address.port));
    });
  });
}

function createCertificate(directory: string) {
  const certificatePath = path.join(directory, "cert.pem");
  const privateKeyPath = path.join(directory, "key.pem");
  execFileSync(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "ec",
      "-pkeyopt",
      "ec_paramgen_curve:prime256v1",
      "-nodes",
      "-keyout",
      privateKeyPath,
      "-out",
      certificatePath,
      "-days",
      "13",
      "-subj",
      "/CN=127.0.0.1",
      "-addext",
      "subjectAltName=IP:127.0.0.1",
      "-config",
      process.platform === "win32" ? "NUL" : "/dev/null",
    ],
    { stdio: "ignore" },
  );
  const certificate = readFileSync(certificatePath, "utf8");
  return {
    certificate,
    privateKey: readFileSync(privateKeyPath, "utf8"),
    hash: createHash("sha256")
      .update(new X509Certificate(certificate).raw)
      .digest("hex"),
  };
}

function payloadHash(event: any) {
  const payload = event?.body?.payload;
  if (typeof payload?.audioBase64 === "string") {
    return createHash("sha256")
      .update(Buffer.from(payload.audioBase64, "base64"))
      .digest("hex");
  }
  if (typeof payload?.frame === "string") {
    return createHash("sha256").update(payload.frame).digest("hex");
  }
  return createHash("sha256")
    .update(JSON.stringify(payload ?? null))
    .digest("hex");
}

async function main() {
  const tempDirectory = mkdtempSync(
    path.join(tmpdir(), "cgp-browser-webtransport-"),
  );
  const certificate = createCertificate(tempDirectory);
  const webTransportPorts = await Promise.all([
    reserveUdpPort(),
    reserveUdpPort(),
  ]);
  const ownerPrivateKey = generatePrivateKey();
  const keyPair = {
    priv: ownerPrivateKey,
    pub: getPublicKey(ownerPrivateKey),
  };
  const observerPrivateKey = generatePrivateKey();
  const observerKeyPair = {
    priv: observerPrivateKey,
    pub: getPublicKey(observerPrivateKey),
  };
  const pubSub = new LocalRelayPubSubAdapter();
  const relayStores = [new MemoryStore(), new MemoryStore()];
  const relays: RelayServer[] = [];
  const relayPorts: number[] = [];
  const startRelay = async (relayIndex: number) => {
    const webTransportPort = webTransportPorts[relayIndex];
    const relay = new RelayServer(0, relayStores[relayIndex], [], {
        enableDefaultPlugins: false,
        instanceId: `browser-webtransport-relay-${relayIndex}`,
        pubSubAdapter: pubSub,
        webTransport: {
          port: webTransportPort,
          host: "127.0.0.1",
          path: "/cgp/realtime",
          publicUrl: `https://127.0.0.1:${webTransportPort}/cgp/realtime`,
          certificate: certificate.certificate,
          privateKey: certificate.privateKey,
          advertiseCertificateHash: true,
        },
      });
    const relayPort = await waitFor(`relay ${relayIndex} WebSocket`, () => {
      const port = relay.getPort();
      return Number.isFinite(port) && port > 0 ? port : undefined;
    });
    await waitFor(`relay ${relayIndex} WebTransport`, () =>
      relay.isWebTransportReady() ? true : undefined,
    );
    relays[relayIndex] = relay;
    relayPorts[relayIndex] = relayPort;
  };

  await startRelay(0);
  const owner = new CgpClient({
    relays: [`ws://127.0.0.1:${relayPorts[0]}`],
    keyPair,
  });
  await owner.connect();
  const guildId = await owner.createGuild("Browser WebTransport Hard Gate");
  const channelId = await owner.createChannel(guildId, "voice", "voice");
  const primarySetupLog = await waitFor(
    "primary relay durable setup",
    async () => {
      const log = await relayStores[0].getLog(guildId);
      return log.length >= 2 ? log : undefined;
    },
  );
  const primarySetupEventIds = primarySetupLog.map((event) => event.id);
  const primarySetupEventCount = primarySetupEventIds.length;

  await startRelay(1);
  const coldReplicaLog = await relayStores[1].getLog(guildId);
  const coldReplicaEventCount = coldReplicaLog.length;
  if (coldReplicaEventCount !== 0) {
    throw new Error("Cold replica unexpectedly shared primary relay storage");
  }
  const replicaBootstrap = new CgpClient({
    relays: [`ws://127.0.0.1:${relayPorts[1]}`],
    keyPair,
  });
  await replicaBootstrap.connect();
  await replicaBootstrap.subscribe(guildId);
  const replicaBootstrapLog = await waitFor(
    "cold relay retained-log bootstrap",
    async () => {
      const log = await relayStores[1].getLog(guildId);
      return log.length >= primarySetupEventCount ? log : undefined;
    },
  );
  const replicaBootstrapEventIds = replicaBootstrapLog.map((event) => event.id);
  const replicaBootstrapEventCount = replicaBootstrapEventIds.length;
  if (
    primarySetupEventIds.join(":") !== replicaBootstrapEventIds.join(":")
  ) {
    throw new Error("Cold relay bootstrapped a non-canonical durable log");
  }

  const durableProbeMessageId = `durable-probe-${Date.now()}`;
  const durableProbeAck = await owner.publishReliable({
    type: "MESSAGE",
    guildId,
    channelId,
    messageId: durableProbeMessageId,
    content: "independent relay replication probe",
  });
  let lastDurableLogs = await Promise.all(
    relayStores.map((store) => store.getLog(guildId)),
  );
  let replicatedDurableLogs;
  try {
    replicatedDurableLogs = await waitFor(
      "live durable event replication",
      async () => {
        lastDurableLogs = await Promise.all(
          relayStores.map((store) => store.getLog(guildId)),
        );
        return lastDurableLogs.every(
          (log) =>
            log.length === primarySetupEventCount + 1 &&
            log.at(-1)?.id === durableProbeAck.eventId,
        )
          ? lastDurableLogs
          : undefined;
      },
    );
  } catch (error) {
    throw new Error(
      `${error instanceof Error ? error.message : String(error)}; ack=${JSON.stringify(durableProbeAck)} logs=${JSON.stringify(
        lastDurableLogs.map((log) =>
          log.map((event) => ({ seq: event.seq, id: event.id, type: event.body.type })),
        ),
      )}`,
    );
  }
  replicaBootstrap.close();
  const durabilityBaseline = {
    separateStores: relayStores[0] !== relayStores[1],
    coldReplicaStartedAtEvents: coldReplicaEventCount,
    bootstrapEvents: replicaBootstrapEventCount,
    liveReplicatedEventId: durableProbeAck.eventId,
    liveReplicatedSeq: durableProbeAck.seq,
    logsMatch:
      replicatedDurableLogs[0].map((event) => event.id).join(":") ===
      replicatedDurableLogs[1].map((event) => event.id).join(":"),
  };
  const observations: Observation[] = [];
  const observers = new Map<number, CgpWebTransportDatagramClient>();

  const connectObserver = async (relayIndex: number) => {
    await observers.get(relayIndex)?.close();
    const observer = new CgpWebTransportDatagramClient({
      url: `https://127.0.0.1:${webTransportPorts[relayIndex]}/cgp/realtime`,
      certificateHash: certificate.hash,
    });
    observer.on("event", (event: any) => {
      const messageId = event?.body?.payload?.messageId;
      if (typeof messageId !== "string") {
        return;
      }
      observations.push({
        relayIndex,
        eventId: typeof event.id === "string" ? event.id : "",
        messageId,
        payloadHash: payloadHash(event),
      });
    });
    await observer.connect();
    await observer.subscribeTransient(guildId, observerKeyPair, [channelId]);
    observers.set(relayIndex, observer);
  };
  await Promise.all(relays.map((_, index) => connectObserver(index)));

  let controlServer: http.Server;
  const sendJson = (
    response: http.ServerResponse,
    status: number,
    body: unknown,
  ) => {
    response.writeHead(status, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      "Cache-Control": "no-store",
      "Content-Type": "application/json; charset=utf-8",
    });
    response.end(JSON.stringify(body));
  };

  controlServer = http.createServer(async (request, response) => {
    if (request.method === "OPTIONS") {
      sendJson(response, 204, null);
      return;
    }
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    if (request.method === "GET" && url.pathname === "/observations") {
      const messageId = url.searchParams.get("messageId") ?? "";
      sendJson(response, 200, {
        observations: observations.filter(
          (entry) => !messageId || entry.messageId === messageId,
        ),
      });
      return;
    }
    if (request.method === "GET" && url.pathname === "/status") {
      sendJson(response, 200, {
        relays: relays.map((relay, relayIndex) => ({
          relayIndex,
          webTransportReady: relay.isWebTransportReady(),
        })),
      });
      return;
    }
    if (request.method === "GET" && url.pathname === "/durability") {
      const logs = await Promise.all(
        relayStores.map((store) => store.getLog(guildId)),
      );
      sendJson(response, 200, {
        ...durabilityBaseline,
        eventCounts: logs.map((log) => log.length),
        headEventIds: logs.map((log) => log.at(-1)?.id ?? null),
        logsMatch:
          logs[0].map((event) => event.id).join(":") ===
          logs[1].map((event) => event.id).join(":"),
      });
      return;
    }

    const match = /^\/relays\/(\d+)\/webtransport\/(stop|start)$/.exec(
      url.pathname,
    );
    if (request.method === "POST" && match) {
      const relayIndex = Number(match[1]);
      const relay = relays[relayIndex];
      if (!relay) {
        sendJson(response, 404, { ok: false, error: "Unknown relay" });
        return;
      }
      try {
        if (match[2] === "stop") {
          await relay.stopWebTransport();
          await observers.get(relayIndex)?.close();
          observers.delete(relayIndex);
        } else {
          await relay.startWebTransport();
          await connectObserver(relayIndex);
        }
        sendJson(response, 200, {
          ok: true,
          relayIndex,
          webTransportReady: relay.isWebTransportReady(),
        });
      } catch (error) {
        sendJson(response, 500, {
          ok: false,
          error: error instanceof Error ? error.message : String(error),
        });
      }
      return;
    }
    sendJson(response, 404, { ok: false, error: "Not found" });
  });
  await new Promise<void>((resolve, reject) => {
    controlServer.once("error", reject);
    controlServer.listen(0, "127.0.0.1", () => resolve());
  });
  const controlAddress = controlServer.address();
  if (!controlAddress || typeof controlAddress === "string") {
    throw new Error("Unable to resolve browser fixture control port");
  }

  let closing = false;
  const close = async () => {
    if (closing) {
      return;
    }
    closing = true;
    await Promise.allSettled(
      [...observers.values()].map((observer) => observer.close()),
    );
    owner.close();
    await Promise.allSettled(relays.map((relay) => relay.close()));
    pubSub.close();
    await new Promise<void>((resolve) => controlServer.close(() => resolve()));
    rmSync(tempDirectory, { recursive: true, force: true });
  };
  process.once("SIGINT", () => void close().finally(() => process.exit(0)));
  process.once("SIGTERM", () => void close().finally(() => process.exit(0)));
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (chunk: string) => {
    if (chunk.split(/\r?\n/).some((line) => line.trim() === "shutdown")) {
      void close().finally(() => process.exit(0));
    }
  });

  process.stdout.write(
    `${JSON.stringify({
      type: "ready",
      controlUrl: `http://127.0.0.1:${controlAddress.port}`,
      relays: relayPorts.map((port, relayIndex) => ({
        relayIndex,
        websocketUrl: `ws://127.0.0.1:${port}`,
        webTransportUrl: `https://127.0.0.1:${webTransportPorts[relayIndex]}/cgp/realtime`,
        certificateHash: certificate.hash,
      })),
      guildId,
      channelId,
      identity: {
        priv: Buffer.from(keyPair.priv).toString("hex"),
        pub: keyPair.pub,
      },
    })}\n`,
  );
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
