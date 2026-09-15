import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import http from "node:http";
import net from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { CgpClient } from "@cgp/client";
import {
  generatePrivateKey,
  getPublicKey,
  hashObject,
} from "@cgp/core";

interface ManagedChild {
  index: number;
  port: number;
  generation: number;
  child?: ChildProcessWithoutNullStreams;
  lastExit?: {
    at: number;
    code: number | null;
    signal: NodeJS.Signals | null;
  };
}

interface RelayChild extends ManagedChild {
  dbPath: string;
  privateKeyHex: string;
  publicKey: string;
  pubSubUrls: string[];
}

interface RelayInspection {
  relayIndex: number;
  generation: number;
  running: boolean;
  pid: number | null;
  dbPath: string;
  pubSubUrls: string[];
  headSeq?: number;
  headHash?: string;
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
const tsxCli = path.join(
  repositoryDirectory,
  "node_modules",
  "tsx",
  "dist",
  "cli.mjs",
);
const relayWorker = path.join(directory, "relay-process-worker.ts");
const hubWorker = path.join(directory, "pubsub-process-worker.ts");

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor<T>(
  label: string,
  predicate: () => T | undefined | false | Promise<T | undefined | false>,
  timeoutMs = 20_000,
) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const value = await predicate();
    if (value) {
      return value;
    }
    await sleep(30);
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
        throw new Error("Unable to reserve TCP port");
      }
      ports.push(address.port);
    }
    return ports;
  } finally {
    await Promise.all(
      servers.map(
        (server) =>
          new Promise<void>((resolve) => server.close(() => resolve())),
      ),
    );
  }
}

function isRunning(state: ManagedChild) {
  return Boolean(
    state.child &&
      state.child.exitCode === null &&
      state.child.signalCode === null &&
      !state.child.killed,
  );
}

function relayUrl(state: RelayChild) {
  return `ws://127.0.0.1:${state.port}`;
}

async function stopChild(state: ManagedChild, force: boolean) {
  const child = state.child;
  if (!child || child.exitCode !== null || child.signalCode !== null) {
    return state.lastExit;
  }
  const exited = new Promise<void>((resolve) =>
    child.once("exit", () => resolve()),
  );
  if (force) {
    child.kill("SIGKILL");
  } else {
    child.stdin.write("shutdown\n");
  }
  await Promise.race([exited, sleep(5_000)]);
  if (child.exitCode === null && child.signalCode === null) {
    child.kill("SIGKILL");
    await Promise.race([exited, sleep(2_000)]);
  }
  await sleep(50);
  return state.lastExit;
}

async function waitForWorkerReady(
  state: ManagedChild,
  child: ChildProcessWithoutNullStreams,
  kind: "relay" | "hub",
) {
  await new Promise<void>((resolve, reject) => {
    let buffer = "";
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out starting ${kind} ${state.index}`));
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
          `${kind} ${state.index} exited before readiness (${code}/${signal})`,
        ),
      );
    };
    const onStdout = (chunk: Buffer) => {
      buffer += chunk.toString();
      const lines = buffer.split(/\r?\n/);
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        try {
          const payload = JSON.parse(line);
          const expectedIndex =
            kind === "relay" ? payload.relayIndex : payload.hubIndex;
          if (
            payload?.type === "ready" &&
            expectedIndex === state.index &&
            payload.generation === state.generation
          ) {
            cleanup();
            resolve();
            return;
          }
        } catch {
          if (line.trim()) {
            process.stderr.write(`[${kind}-${state.index}] ${line}\n`);
          }
        }
      }
    };
    child.stdout.on("data", onStdout);
    child.once("exit", onExit);
  });
}

async function main() {
  const temporaryDirectory = mkdtempSync(
    path.join(tmpdir(), "cgp-relay-partition-hard-gate-"),
  );
  const ports = await reserveTcpPorts(6);
  const hubStates: ManagedChild[] = ports.slice(0, 3).map((port, index) => ({
    index,
    port,
    generation: 0,
  }));
  const hubUrls = hubStates.map((hub) => `ws://127.0.0.1:${hub.port}`);
  const relayKeyPairs = Array.from({ length: 3 }, () => {
    const privateKey = generatePrivateKey();
    return {
      privateKeyHex: Buffer.from(privateKey).toString("hex"),
      publicKey: getPublicKey(privateKey),
    };
  });
  const writeQuorum = {
    epoch: `partition-gate-${randomUUID()}`,
    members: relayKeyPairs.map((key) => key.publicKey),
    requiredVotes: 2,
    voteTimeoutMs: 700,
  };
  const sequencerConsensus = {
    epoch: writeQuorum.epoch,
    members: writeQuorum.members,
    requiredVotes: writeQuorum.requiredVotes,
    electionTimeoutMinMs: 180,
    electionTimeoutMaxMs: 360,
    heartbeatIntervalMs: 50,
    requestTimeoutMs: 1_500,
  };
  const relayStates: RelayChild[] = ports
    .slice(3)
    .map((port, index) => ({
      index,
      port,
      generation: 0,
      dbPath: path.join(temporaryDirectory, `relay-${index}`),
      privateKeyHex: relayKeyPairs[index].privateKeyHex,
      publicKey: relayKeyPairs[index].publicKey,
      pubSubUrls: [hubUrls[0], hubUrls[1]],
    }));

  const startHub = async (index: number) => {
    const state = hubStates[index];
    if (isRunning(state)) {
      return {
        index,
        generation: state.generation,
        pid: state.child?.pid ?? null,
        readyAt: Date.now(),
      };
    }
    state.generation += 1;
    const child = spawn(process.execPath, [tsxCli, hubWorker], {
      cwd: repositoryDirectory,
      env: {
        ...process.env,
        CGP_PROCESS_GATE_GENERATION: String(state.generation),
        CGP_PROCESS_GATE_HUB_INDEX: String(index),
        CGP_PUBSUB_PORT: String(state.port),
        CGP_PUBSUB_RETAIN_DIR: path.join(
          temporaryDirectory,
          `hub-${index}-retained`,
        ),
      },
      stdio: ["pipe", "pipe", "pipe"],
      windowsHide: true,
    });
    state.child = child;
    child.once("exit", (code, signal) => {
      state.lastExit = { at: Date.now(), code, signal };
    });
    child.stderr.on("data", (chunk) =>
      process.stderr.write(`[hub-${index}] ${chunk.toString()}`),
    );
    await waitForWorkerReady(state, child, "hub");
    return {
      index,
      generation: state.generation,
      pid: child.pid ?? null,
      readyAt: Date.now(),
    };
  };

  const startRelay = async (index: number) => {
    const state = relayStates[index];
    if (isRunning(state)) {
      return {
        index,
        generation: state.generation,
        pid: state.child?.pid ?? null,
        readyAt: Date.now(),
      };
    }
    state.generation += 1;
    const child = spawn(process.execPath, [tsxCli, relayWorker], {
      cwd: repositoryDirectory,
      env: {
        ...process.env,
        CGP_PROCESS_GATE_GENERATION: String(state.generation),
        CGP_PROCESS_GATE_RELAY_INDEX: String(index),
        CGP_RELAY_DB: state.dbPath,
        CGP_RELAY_PORT: String(state.port),
        CGP_RELAY_PRIVATE_KEY_HEX: state.privateKeyHex,
        CGP_RELAY_PUBSUB_URL: state.pubSubUrls[0],
        CGP_RELAY_PUBSUB_URLS: state.pubSubUrls.join(","),
        CGP_RELAY_WRITE_QUORUM_CONFIG: JSON.stringify(writeQuorum),
        CGP_RELAY_SEQUENCER_CONFIG: JSON.stringify(sequencerConsensus),
      },
      stdio: ["pipe", "pipe", "pipe"],
      windowsHide: true,
    });
    state.child = child;
    child.once("exit", (code, signal) => {
      state.lastExit = { at: Date.now(), code, signal };
    });
    child.stderr.on("data", (chunk) =>
      process.stderr.write(`[relay-${index}] ${chunk.toString()}`),
    );
    await waitForWorkerReady(state, child, "relay");
    return {
      index,
      generation: state.generation,
      pid: child.pid ?? null,
      readyAt: Date.now(),
    };
  };

  await Promise.all(hubStates.map((hub) => startHub(hub.index)));
  await Promise.all(relayStates.map((relay) => startRelay(relay.index)));

  const ownerPrivateKey = generatePrivateKey();
  const ownerKeyPair = {
    priv: ownerPrivateKey,
    pub: getPublicKey(ownerPrivateKey),
  };
  const inspectorPrivateKey = generatePrivateKey();
  const inspectorKeyPair = {
    priv: inspectorPrivateKey,
    pub: getPublicKey(inspectorPrivateKey),
  };
  const writerIdentities = Array.from({ length: 2 }, () => {
    const priv = generatePrivateKey();
    return {
      priv: Buffer.from(priv).toString("hex"),
      pub: getPublicKey(priv),
    };
  });
  const owner = new CgpClient({
    relays: relayStates.map(relayUrl),
    keyPair: ownerKeyPair,
  });
  await owner.connect();
  const guildId = hashObject({
    type: "relay-partition-hard-gate",
    nonce: randomUUID(),
  });
  const channelId = hashObject({
    type: "relay-partition-hard-gate-channel",
    guildId,
    nonce: randomUUID(),
  });
  await owner.subscribe(guildId);
  await owner.publishReliable({
    type: "GUILD_CREATE",
    guildId,
    name: "Relay Partition Hard Gate",
  });
  await owner.publishReliable({
    type: "CHANNEL_CREATE",
    guildId,
    channelId,
    name: "partition",
    kind: "text",
  });

  const inspectRelay = async (
    state: RelayChild,
  ): Promise<RelayInspection> => {
    const base = {
      relayIndex: state.index,
      generation: state.generation,
      running: isRunning(state),
      pid: isRunning(state) ? state.child?.pid ?? null : null,
      dbPath: state.dbPath,
      pubSubUrls: state.pubSubUrls,
    };
    if (!base.running) {
      return base;
    }
    const client = new CgpClient({
      relays: [relayUrl(state)],
      keyPair: inspectorKeyPair,
      connectTimeoutMs: 1_500,
    });
    try {
      await client.connect();
      const [head, history] = await Promise.all([
        client.getRelayHead(guildId, { timeoutMs: 1_500 }),
        client.getHistory({ guildId, channelId, limit: 256 }),
      ]);
      return {
        ...base,
        headSeq: head.headSeq,
        headHash: head.headHash,
        messages: history.events.flatMap((event: any) =>
          event?.body?.type === "MESSAGE"
            ? [
                {
                  messageId: String(event.body.messageId ?? ""),
                  content: String(event.body.content ?? ""),
                  eventId: String(event.id ?? ""),
                  seq: Number(event.seq),
                },
              ]
            : [],
        ),
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

  const inspectAll = async () => {
    const relays = await Promise.all(relayStates.map(inspectRelay));
    const readable = relays.filter(
      (relay) =>
        relay.running &&
        !relay.error &&
        typeof relay.headSeq === "number" &&
        typeof relay.headHash === "string",
    );
    return {
      relays,
      readableRelayCount: readable.length,
      matchingReadableHeads:
        readable.length > 0 &&
        new Set(readable.map((relay) => relay.headSeq)).size === 1 &&
        new Set(readable.map((relay) => relay.headHash)).size === 1,
      hubs: hubStates.map((hub) => ({
        hubIndex: hub.index,
        generation: hub.generation,
        running: isRunning(hub),
        pid: isRunning(hub) ? hub.child?.pid ?? null : null,
      })),
    };
  };

  await waitFor("initial write-quorum replicas", async () => {
    const inspection = await inspectAll();
    return inspection.readableRelayCount === 3 &&
      inspection.matchingReadableHeads &&
      inspection.relays.every((relay) => relay.headSeq === 1)
      ? inspection
      : undefined;
  });

  let partitioned = false;
  const partitionMinority = async () => {
    if (partitioned) {
      return;
    }
    await stopChild(relayStates[2], true);
    relayStates[2].pubSubUrls = [hubUrls[2]];
    await startRelay(2);
    partitioned = true;
  };
  const healMinority = async () => {
    if (!partitioned) {
      return;
    }
    await stopChild(relayStates[2], true);
    relayStates[2].pubSubUrls = [hubUrls[0], hubUrls[1]];
    await startRelay(2);
    partitioned = false;
  };

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
    try {
      if (request.method === "GET" && url.pathname === "/durability") {
        sendJson(response, 200, await inspectAll());
        return;
      }
      if (request.method === "POST" && url.pathname === "/partition") {
        const startedAt = Date.now();
        await partitionMinority();
        sendJson(response, 200, {
          ok: true,
          startedAt,
          completedAt: Date.now(),
          minorityRelayGeneration: relayStates[2].generation,
        });
        return;
      }
      if (request.method === "POST" && url.pathname === "/heal") {
        const startedAt = Date.now();
        await healMinority();
        sendJson(response, 200, {
          ok: true,
          startedAt,
          completedAt: Date.now(),
          minorityRelayGeneration: relayStates[2].generation,
        });
        return;
      }
      if (request.method === "POST" && url.pathname === "/raw-minority") {
        const before = await inspectRelay(relayStates[2]);
        const messageId = `raw-minority-${randomUUID()}`;
        const client = new CgpClient({
          relays: [relayUrl(relayStates[2])],
          keyPair: inspectorKeyPair,
          connectTimeoutMs: 1_500,
        });
        const startedAt = Date.now();
        let error = "";
        try {
          await client.connect();
          await client.publishReliable(
            {
              type: "MESSAGE",
              guildId,
              channelId,
              messageId,
              content: `must-not-commit-${messageId}`,
            },
            { timeoutMs: 3_000 },
          );
        } catch (caught) {
          error = caught instanceof Error ? caught.message : String(caught);
        } finally {
          client.close();
        }
        const after = await inspectRelay(relayStates[2]);
        sendJson(response, 200, {
          ok: Boolean(error) && before.headHash === after.headHash,
          messageId,
          error,
          elapsedMs: Date.now() - startedAt,
          beforeHeadSeq: before.headSeq,
          beforeHeadHash: before.headHash,
          afterHeadSeq: after.headSeq,
          afterHeadHash: after.headHash,
        });
        return;
      }
      const hubMatch = /^\/hubs\/(\d+)\/(kill|restart)$/.exec(url.pathname);
      if (request.method === "POST" && hubMatch) {
        const hubIndex = Number(hubMatch[1]);
        const hub = hubStates[hubIndex];
        if (!hub) {
          sendJson(response, 404, { ok: false, error: "Unknown hub" });
          return;
        }
        if (hubMatch[2] === "kill") {
          const pid = hub.child?.pid ?? null;
          const generation = hub.generation;
          const requestedAt = Date.now();
          const lastExit = await stopChild(hub, true);
          sendJson(response, 200, {
            ok: true,
            hubIndex,
            pid,
            generation,
            requestedAt,
            exitedAt: lastExit?.at ?? Date.now(),
          });
        } else {
          sendJson(response, 200, {
            ok: true,
            ...(await startHub(hubIndex)),
          });
        }
        return;
      }
      const relayMatch = /^\/relays\/(\d+)\/(kill|restart)$/.exec(
        url.pathname,
      );
      if (request.method === "POST" && relayMatch) {
        const relayIndex = Number(relayMatch[1]);
        const relay = relayStates[relayIndex];
        if (!relay) {
          sendJson(response, 404, { ok: false, error: "Unknown relay" });
          return;
        }
        if (relayMatch[2] === "kill") {
          const pid = relay.child?.pid ?? null;
          const generation = relay.generation;
          const requestedAt = Date.now();
          const lastExit = await stopChild(relay, true);
          sendJson(response, 200, {
            ok: true,
            relayIndex,
            pid,
            generation,
            requestedAt,
            exitedAt: lastExit?.at ?? Date.now(),
          });
        } else {
          sendJson(response, 200, {
            ok: true,
            ...(await startRelay(relayIndex)),
          });
        }
        return;
      }
      sendJson(response, 404, { ok: false, error: "Not found" });
    } catch (error) {
      sendJson(response, 500, {
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  });
  await new Promise<void>((resolve, reject) => {
    controlServer.once("error", reject);
    controlServer.listen(0, "127.0.0.1", () => resolve());
  });
  const controlAddress = controlServer.address();
  if (!controlAddress || typeof controlAddress === "string") {
    throw new Error("Unable to resolve partition fixture control port");
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
      relayStates.map((relay) => stopChild(relay, false)),
    );
    await Promise.allSettled(hubStates.map((hub) => stopChild(hub, false)));
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
      relays: relayStates.map((relay) => ({
        relayIndex: relay.index,
        websocketUrl: relayUrl(relay),
        publicKey: relay.publicKey,
      })),
      hubs: hubStates.map((hub) => ({
        hubIndex: hub.index,
        url: hubUrls[hub.index],
      })),
      writeQuorum: {
        epoch: writeQuorum.epoch,
        members: writeQuorum.members,
        requiredVotes: writeQuorum.requiredVotes,
      },
      sequencerConsensus,
      guildId,
      channelId,
      identity: {
        priv: Buffer.from(ownerKeyPair.priv).toString("hex"),
        pub: ownerKeyPair.pub,
      },
      writerIdentities,
    })}\n`,
  );
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
