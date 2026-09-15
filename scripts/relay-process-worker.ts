import {
  RedundantWebSocketRelayPubSubAdapter,
  WebSocketRelayPubSubAdapter,
} from "@cgp/relay/src/pubsub_ws";
import { RelayServer } from "@cgp/relay/src/server";

function requiredEnvironment(name: string) {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function positivePort(name: string) {
  const value = Number(requiredEnvironment(name));
  if (!Number.isSafeInteger(value) || value <= 0 || value > 65_535) {
    throw new Error(`${name} must be a valid TCP port`);
  }
  return value;
}

async function waitForRelayPort(relay: RelayServer, expectedPort: number) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < 10_000) {
    if (relay.getPort() === expectedPort) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Relay did not bind port ${expectedPort}`);
}

async function main() {
  const relayIndex = Number(process.env.CGP_PROCESS_GATE_RELAY_INDEX ?? "0");
  const generation = Number(process.env.CGP_PROCESS_GATE_GENERATION ?? "1");
  const port = positivePort("CGP_RELAY_PORT");
  const dbPath = requiredEnvironment("CGP_RELAY_DB");
  const pubSubUrls = (
    process.env.CGP_RELAY_PUBSUB_URLS ??
    requiredEnvironment("CGP_RELAY_PUBSUB_URL")
  )
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
  const pubSub =
    pubSubUrls.length > 1
      ? new RedundantWebSocketRelayPubSubAdapter(pubSubUrls)
      : new WebSocketRelayPubSubAdapter(pubSubUrls[0]);
  const relay = new RelayServer(port, dbPath, [], {
    enableDefaultPlugins: false,
    instanceId: `process-hard-gate-${relayIndex}-${generation}`,
    pubSubAdapter: pubSub,
  });

  await waitForRelayPort(relay, port);
  process.stdout.write(
    `${JSON.stringify({
      type: "ready",
      relayIndex,
      generation,
      pid: process.pid,
      port,
      dbPath,
    })}\n`,
  );

  let closing = false;
  const close = async (exitCode = 0) => {
    if (closing) {
      return;
    }
    closing = true;
    await relay.close();
    await pubSub.close();
    process.exit(exitCode);
  };

  process.once("SIGINT", () => void close());
  process.once("SIGTERM", () => void close());
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (chunk: string) => {
    if (chunk.split(/\r?\n/).some((line) => line.trim() === "shutdown")) {
      void close();
    }
  });
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
