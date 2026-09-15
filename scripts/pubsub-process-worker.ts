import net from "node:net";
import { WebSocketPubSubHub } from "@cgp/relay/src/pubsub_ws";

function requiredEnvironment(name: string) {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

async function waitForPort(port: number) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < 10_000) {
    const connected = await new Promise<boolean>((resolve) => {
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
    if (connected) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Pubsub hub did not bind port ${port}`);
}

async function main() {
  const hubIndex = Number(process.env.CGP_PROCESS_GATE_HUB_INDEX ?? "0");
  const generation = Number(process.env.CGP_PROCESS_GATE_GENERATION ?? "1");
  const port = Number(requiredEnvironment("CGP_PUBSUB_PORT"));
  const retainDir = requiredEnvironment("CGP_PUBSUB_RETAIN_DIR");
  const hub = new WebSocketPubSubHub(port, {
    host: "127.0.0.1",
    retainDir,
    retainEnvelopesPerTopic: 10_000,
  });
  await waitForPort(port);
  process.stdout.write(
    `${JSON.stringify({
      type: "ready",
      hubIndex,
      generation,
      pid: process.pid,
      port,
    })}\n`,
  );

  let closing = false;
  const close = async () => {
    if (closing) {
      return;
    }
    closing = true;
    await hub.close();
    process.exit(0);
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
