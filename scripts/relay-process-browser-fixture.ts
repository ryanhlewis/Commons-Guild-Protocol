import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import http from "node:http";
import net from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { CgpClient } from "@cgp/client";
import { generatePrivateKey, getPublicKey, hashObject } from "@cgp/core";
import { WebSocketPubSubHub } from "@cgp/relay/src/pubsub_ws";

interface RelayProcessState {
  relayIndex: number;
  port: number;
  dbPath: string;
  generation: number;
  child?: ChildProcessWithoutNullStreams;
  lastExit?: {
    at: number;
    code: number | null;
    signal: NodeJS.Signals | null;
  };
}

interface RelayInspection {
  relayIndex: number;
  generation: number;
  running: boolean;
  pid: number | null;
  dbPath: string;
  headSeq?: number;
  headHash?: string | null;
  messages?: Array<{
    messageId: string;
    content: string;
    eventId: string;
    seq: number;
  }>;
  error?: string;
}

const directory = path.dirname(fileURLToPath(import.meta.url));
const repositoryDirectory = path.resolve(directory, "..");
const tsxCli = path.join(repositoryDirectory, "node_modules", "tsx", "dist", "cli.mjs");
const workerPath = path.join(directory, "relay-process-worker.ts");

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor<T>(
  label: string,
  predicate: () => T | undefined | false | Promise<T | undefined | false>,
  timeoutMs = 15_000,
) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const value = await predicate();
    if (value) {
      return value;
    }
    await sleep(25);
  }
  throw new Error(`Timed out waiting for ${label}`);
}

async function reserveTcpPorts(count: number) {
  const servers: net.Server[] = [];
  const ports: number[] = [];
  try {
    for (let index = 0; index < count; index += 1) {
      const server = net.createServer();
      servers.push(server);
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", () => resolve());
      });
      const address = server.address();
      if (!address || typeof address === "string") {
        throw new Error("Unable to reserve a TCP port");
      }
      ports.push(address.port);
    }
    return ports;
  } finally {
    await Promise.all(
      servers.map(
        (server) =>
          new Promise<void>((resolve) => {
            server.close(() => resolve());
          }),
      ),
    );
  }
}

async function waitForTcpPort(port: number) {
  await waitFor(`TCP port ${port}`, async () => {
    return await new Promise<boolean>((resolve) => {
      const socket = net.createConnection({ host: "127.0.0.1", port });
      socket.once("connect", () => {
        socket.destroy();
        resolve(true);
      });
      socket.once("error", () => {
        socket.destroy();
        resolve(false);
      });
    });
  });
}

function relayIsRunning(state: RelayProcessState) {
  return Boolean(
    state.child &&
      state.child.exitCode === null &&
      state.child.signalCode === null &&
      !state.child.killed,
  );
}

function relayUrl(state: RelayProcessState) {
  return `ws://127.0.0.1:${state.port}`;
}

function messageDetails(events: any[]) {
  return events.flatMap((event) => {
    if (
      event?.body?.type !== "MESSAGE" ||
      typeof event.body.messageId !== "string" ||
      typeof event.body.content !== "string"
    ) {
      return [];
    }
    return [
      {
        messageId: event.body.messageId,
        content: event.body.content,
        eventId: String(event.id ?? ""),
        seq: Number(event.seq),
      },
    ];
  });
}

async function main() {
  const temporaryDirectory = mkdtempSync(
    path.join(tmpdir(), "cgp-relay-process-hard-gate-"),
  );
  const [pubSubPort, ...relayPorts] = await reserveTcpPorts(4);
  const pubSubUrl = `ws://127.0.0.1:${pubSubPort}`;
  const pubSubHub = new WebSocketPubSubHub(pubSubPort, {
    host: "127.0.0.1",
    retainDir: path.join(temporaryDirectory, "pubsub-retained"),
    retainEnvelopesPerTopic: 10_000,
  });
  await waitForTcpPort(pubSubPort);

  const relayStates: RelayProcessState[] = relayPorts.map((port, relayIndex) => ({
    relayIndex,
    port,
    dbPath: path.join(temporaryDirectory, `relay-${relayIndex}`),
    generation: 0,
  }));

  const startRelay = async (relayIndex: number) => {
    const state = relayStates[relayIndex];
    if (!state) {
      throw new Error(`Unknown relay ${relayIndex}`);
    }
    if (relayIsRunning(state)) {
      return {
        relayIndex,
        generation: state.generation,
        pid: state.child?.pid ?? null,
        startedAt: Date.now(),
      };
    }

    state.generation += 1;
    state.lastExit = undefined;
    const startedAt = Date.now();
    const child = spawn(process.execPath, [tsxCli, workerPath], {
      cwd: repositoryDirectory,
      env: {
        ...process.env,
        CGP_PROCESS_GATE_GENERATION: String(state.generation),
        CGP_PROCESS_GATE_RELAY_INDEX: String(relayIndex),
        CGP_RELAY_DB: state.dbPath,
        CGP_RELAY_PORT: String(state.port),
        CGP_RELAY_PUBSUB_URL: pubSubUrl,
      },
      stdio: ["pipe", "pipe", "pipe"],
      windowsHide: true,
    });
    state.child = child;
    child.once("exit", (code, signal) => {
      state.lastExit = { at: Date.now(), code, signal };
    });
    child.stderr.on("data", (chunk) => {
      process.stderr.write(`[relay-${relayIndex}] ${chunk.toString()}`);
    });

    await new Promise<void>((resolve, reject) => {
      let stdoutBuffer = "";
      const timeout = setTimeout(() => {
        cleanup();
        reject(new Error(`Timed out starting relay ${relayIndex}`));
      }, 20_000);
      const cleanup = () => {
        clearTimeout(timeout);
        child.stdout.off("data", onStdout);
        child.off("exit", onExit);
      };
      const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
        cleanup();
        reject(
          new Error(
            `Relay ${relayIndex} exited before readiness (code ${code}, signal ${signal})`,
          ),
        );
      };
      const onStdout = (chunk: Buffer) => {
        stdoutBuffer += chunk.toString();
        const lines = stdoutBuffer.split(/\r?\n/);
        stdoutBuffer = lines.pop() ?? "";
        for (const line of lines) {
          try {
            const payload = JSON.parse(line);
            if (
              payload?.type === "ready" &&
              payload.relayIndex === relayIndex &&
              payload.generation === state.generation
            ) {
              cleanup();
              resolve();
              return;
            }
          } catch {
            if (line.trim()) {
              process.stderr.write(`[relay-${relayIndex}] ${line}\n`);
            }
          }
        }
      };
      child.stdout.on("data", onStdout);
      child.once("exit", onExit);
    });
    await waitForTcpPort(state.port);
    return {
      relayIndex,
      generation: state.generation,
      pid: child.pid ?? null,
      startedAt,
      readyAt: Date.now(),
    };
  };

  const stopRelay = async (relayIndex: number, force: boolean) => {
    const state = relayStates[relayIndex];
    const child = state?.child;
    if (!state || !child || child.exitCode !== null || child.signalCode !== null) {
      return state?.lastExit;
    }

    const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
    if (force) {
      child.kill("SIGKILL");
    } else {
      child.stdin.write("shutdown\n");
    }
    await Promise.race([exited, sleep(4_000)]);
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
      await Promise.race([exited, sleep(2_000)]);
    }
    await sleep(50);
    return state.lastExit;
  };

  await Promise.all(relayStates.map((state) => startRelay(state.relayIndex)));

  const ownerPrivateKey = generatePrivateKey();
  const keyPair = {
    priv: ownerPrivateKey,
    pub: getPublicKey(ownerPrivateKey),
  };
  const inspectorPrivateKey = generatePrivateKey();
  const inspectorKeyPair = {
    priv: inspectorPrivateKey,
    pub: getPublicKey(inspectorPrivateKey),
  };
  const owner = new CgpClient({
    relays: relayStates.map(relayUrl),
    keyPair,
  });
  await owner.connect();
  const guildId = hashObject({
    type: "relay-process-hard-gate",
    id: randomUUID(),
  });
  const channelId = hashObject({
    type: "relay-process-hard-gate-channel",
    guildId,
    id: randomUUID(),
  });
  await owner.subscribe(guildId);
  await owner.publishReliable({
    type: "GUILD_CREATE",
    guildId,
    name: "Relay Process Hard Gate",
  });
  await owner.publishReliable({
    type: "CHANNEL_CREATE",
    guildId,
    channelId,
    name: "durability",
    kind: "text",
  });

  const inspectRelay = async (state: RelayProcessState): Promise<RelayInspection> => {
    const base = {
      relayIndex: state.relayIndex,
      generation: state.generation,
      running: relayIsRunning(state),
      pid: relayIsRunning(state) ? state.child?.pid ?? null : null,
      dbPath: state.dbPath,
    };
    if (!base.running) {
      return base;
    }

    const client = new CgpClient({
      relays: [relayUrl(state)],
      // The browser activates delegated device authority for the owner. A
      // separate signed reader keeps durability inspection independent of
      // that account's intentional direct-signature lockout.
      keyPair: inspectorKeyPair,
      connectTimeoutMs: 1_500,
    });
    try {
      await client.connect();
      const [head, history] = await Promise.all([
        client.getRelayHead(guildId, { timeoutMs: 1_500 }),
        client.getHistory({
          guildId,
          channelId,
          limit: 256,
        }),
      ]);
      return {
        ...base,
        headSeq: head.headSeq,
        headHash: head.headHash,
        messages: messageDetails(history.events),
      };
    } catch (error) {
      return {
        ...base,
        error: error instanceof Error ? error.message : String(error),
      };
    } finally {
      client.close();
    }
  };

  const inspectAllRelays = async () => {
    const relays = await Promise.all(relayStates.map(inspectRelay));
    const readable = relays.filter(
      (relay) =>
        relay.running &&
        !relay.error &&
        typeof relay.headSeq === "number" &&
        typeof relay.headHash === "string",
    );
    return {
      separateDatabasePaths:
        new Set(relayStates.map((state) => state.dbPath)).size === relayStates.length,
      relays,
      readableRelayCount: readable.length,
      matchingReadableHeads:
        readable.length > 0 &&
        new Set(relays.flatMap((relay) => relay.headHash ?? [])).size === 1 &&
        new Set(relays.flatMap((relay) => relay.headSeq ?? [])).size === 1,
    };
  };

  await waitFor("three matching LevelStore relay heads", async () => {
    const inspection = await inspectAllRelays();
    return inspection.readableRelayCount === 3 &&
      inspection.matchingReadableHeads &&
      inspection.relays.every((relay) => (relay.headSeq ?? -1) >= 1)
      ? inspection
      : undefined;
  });

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

  const controlServer = http.createServer(async (request, response) => {
    if (request.method === "OPTIONS") {
      sendJson(response, 204, null);
      return;
    }

    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    if (request.method === "GET" && url.pathname === "/status") {
      sendJson(response, 200, {
        relays: relayStates.map((state) => ({
          relayIndex: state.relayIndex,
          generation: state.generation,
          running: relayIsRunning(state),
          pid: relayIsRunning(state) ? state.child?.pid ?? null : null,
          lastExit: state.lastExit,
        })),
      });
      return;
    }
    if (request.method === "GET" && url.pathname === "/durability") {
      sendJson(response, 200, await inspectAllRelays());
      return;
    }

    const match = /^\/relays\/(\d+)\/(kill|restart)$/.exec(url.pathname);
    if (request.method === "POST" && match) {
      const relayIndex = Number(match[1]);
      const state = relayStates[relayIndex];
      if (!state) {
        sendJson(response, 404, { ok: false, error: "Unknown relay" });
        return;
      }
      try {
        if (match[2] === "kill") {
          const pid = state.child?.pid ?? null;
          const generation = state.generation;
          const requestedAt = Date.now();
          const lastExit = await stopRelay(relayIndex, true);
          sendJson(response, 200, {
            ok: true,
            relayIndex,
            generation,
            pid,
            requestedAt,
            exitedAt: lastExit?.at ?? Date.now(),
            signal: lastExit?.signal ?? "SIGKILL",
          });
        } else {
          sendJson(response, 200, {
            ok: true,
            ...(await startRelay(relayIndex)),
          });
        }
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
    throw new Error("Unable to resolve relay process control port");
  }

  let closing = false;
  const close = async () => {
    if (closing) {
      return;
    }
    closing = true;
    owner.close();
    await new Promise<void>((resolve) => controlServer.close(() => resolve()));
    await Promise.allSettled(
      relayStates.map((state) => stopRelay(state.relayIndex, false)),
    );
    await pubSubHub.close();
    rmSync(temporaryDirectory, {
      recursive: true,
      force: true,
      maxRetries: 5,
      retryDelay: 100,
    });
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
      relays: relayStates.map((state) => ({
        relayIndex: state.relayIndex,
        websocketUrl: relayUrl(state),
        dbPath: state.dbPath,
        generation: state.generation,
        pid: state.child?.pid ?? null,
      })),
      pubSubUrl,
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
