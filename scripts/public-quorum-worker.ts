import {allowFixtureHostname,fixtureDnsObservations} from "./fixture-dns.js";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { spawn, type ChildProcess } from "node:child_process";
import { RelayServer, type RelayPubSubAdapter, type RelayPubSubEnvelope, type RelayPubSubSubscribeOptions } from "@cgp/relay/src/server";
import { WebSocketPubSubHub, WebSocketRelayPubSubAdapter } from "@cgp/relay/src/pubsub_ws";
import { getPublicKey } from "@cgp/core";

async function main() {
  const directory = path.resolve(process.argv[2] || "");
  if (!directory.includes("hollow-roadmap-staging-") || !fs.existsSync(directory)) throw new Error("Task directory required");
  const config = JSON.parse(fs.readFileSync(path.join(directory, "config.json"), "utf8"));
  let tunnel: ChildProcess | undefined, relay: RelayServer | undefined, hub: WebSocketPubSubHub | undefined;
  let adapter: WebSocketRelayPubSubAdapter | undefined;
  const subscriptions = new Set<{ topic: string; handler: (event: RelayPubSubEnvelope) => void; options?: RelayPubSubSubscribeOptions }>();
  let isolated = false;
  function connectAdapter() {
    allowFixtureHostname(config.pubsubUrl);
    adapter = new WebSocketRelayPubSubAdapter(config.pubsubUrl, { authToken: config.pubsubToken });
    for (const sub of subscriptions) adapter.subscribe(sub.topic, sub.handler, sub.options);
  }
  const switchable: RelayPubSubAdapter = {
    publish(topic, event) { return adapter?.publish(topic, event); },
    subscribe(topic, handler, options) {
      const sub = { topic, handler, options }; subscriptions.add(sub);
      const remove = adapter?.subscribe(topic, handler, options);
      return () => { subscriptions.delete(sub); return remove?.(); };
    }
  };
  let port: number;
  if (config.role === "hub") {
    hub = new WebSocketPubSubHub(0, { host: "127.0.0.1", authToken: config.pubsubToken, retainDir: path.join(directory, "hub-retained"), retainEnvelopesPerTopic: 10000 });
    while (!(hub as any).wss.address()) await new Promise(r => setTimeout(r, 10));
    port = (hub as any).wss.address().port;
  } else {
    connectAdapter();
    relay = new RelayServer(0, path.join(directory, "leveldb"), [], {
      listenHost: "127.0.0.1", enableDefaultPlugins: false, sequencerConsensus: false,
      instanceId: config.label, relayPrivateKeyHex: config.privateKey,
      pubSubAdapter: switchable, writeQuorum: config.quorum,
    });
    while (!relay.getPort()) await new Promise(r => setTimeout(r, 10));
    port = relay.getPort();
  }
  const metadata = { machine: os.hostname(), pid: process.pid, port, host: "127.0.0.1", role: config.role,
    ...(config.role === "relay" ? { relayId: config.label, publicKey: getPublicKey(Buffer.from(config.privateKey, "hex")), quorum: config.quorum } : {}) };
  fs.writeFileSync(path.join(directory, "ready.json"), JSON.stringify(metadata));
  tunnel = spawn(config.cloudflared, ["tunnel", "--no-autoupdate", "--protocol", "http2", "--url", `http://127.0.0.1:${port}`, "--metrics", "127.0.0.1:0"], { windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
  let logs = "";
  const inspectTunnel = (chunk: Buffer) => {
    logs = (logs + chunk.toString()).slice(-24000);
    const url = logs.match(/https:\/\/[a-z0-9]+(?:-[a-z0-9]+){2,}\.trycloudflare\.com/);
    if (url) fs.writeFileSync(path.join(directory, "public-ready.json"), JSON.stringify({ ...metadata, tunnelPid: tunnel?.pid, httpsUrl: url[0], wssUrl: url[0].replace("https:", "wss:") }));
  };
  tunnel.stdout?.on("data", inspectTunnel); tunnel.stderr?.on("data", inspectTunnel);
  let closed = false;
  const close = async () => {
    if (closed) return; closed = true; clearInterval(watch); clearTimeout(deadline);
    tunnel?.kill(); await relay?.close(); await adapter?.close(); await hub?.close();
    fs.writeFileSync(path.join(directory, "stopped.json"), JSON.stringify({ machine: os.hostname(), pid: process.pid, tunnelPid: tunnel?.pid, stopped: true }));
    fs.writeFileSync(path.join(directory,"dns-observations.json"),JSON.stringify(fixtureDnsObservations(),null,2));
    process.exit(0);
  };
  const watch = setInterval(() => {
    if (fs.existsSync(path.join(directory, "stop"))) { void close(); return; }
    if (config.role !== "relay") {
      fs.writeFileSync(path.join(directory, "transport-state.json"), JSON.stringify({ clients: (hub as any).wss.clients.size, topics: (hub as any).subscriptions.size }));
      return;
    }
    fs.writeFileSync(path.join(directory, "transport-state.json"), JSON.stringify({ ready: adapter?.isReady() ?? false, isolated, socketState: (adapter as any)?.socket?.readyState ?? null }));
    const requested = fs.existsSync(path.join(directory, "isolate"));
    if (requested !== isolated) {
      isolated = requested;
      if (isolated) { const old = adapter; adapter = undefined; void old?.close(); }
      else connectAdapter();
      fs.writeFileSync(path.join(directory, "network-state.json"), JSON.stringify({ isolated, at: new Date().toISOString() }));
    }
    const inspect = path.join(directory, "inspect.json");
    if (fs.existsSync(inspect)) {
      try {
        const { guild } = JSON.parse(fs.readFileSync(inspect, "utf8"));
        void (relay as any).store.getLog(guild).then((events: any[]) => fs.writeFileSync(path.join(directory, "snapshot.json"), JSON.stringify({ guild, events })));
      } catch { /* Controller may be atomically replacing its request. */ }
    }
  }, 250);
  const deadline = setTimeout(() => void close(), 30 * 60 * 1000);
  tunnel.once("error", () => void close());
  process.once("SIGINT", () => void close()); process.once("SIGTERM", () => void close());
}
void main().catch(() => { console.error("Public quorum fixture failed"); process.exit(1); });
