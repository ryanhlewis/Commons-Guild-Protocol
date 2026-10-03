import fs from "node:fs";
import path from "node:path";
import { RelayServer } from "@cgp/relay/src/server";
import { LevelStore } from "@cgp/relay/src/store_level";
import { computeEventId, hashObject, verify } from "@cgp/core";

// Task-owned loopback fixture. No inherited production quorum/plugin configuration.
async function main() {
  const directory = path.resolve(process.argv[2] || "");
  if (!directory.includes("hollow-roadmap-staging-") || !fs.existsSync(directory)) throw new Error("Task directory required");
  const stage = process.argv[3];
  if (!/^(founder|replacement|replacement-restart)$/.test(stage)) throw new Error("Invalid fixture stage");
  const db = path.join(directory, `${stage.replace('-restart', '')}-db`);
  const archivePath = path.join(directory, "replacement.json");
  if (stage === "replacement") {
    const archive = JSON.parse(fs.readFileSync(archivePath, "utf8"));
    const expected = JSON.parse(fs.readFileSync(path.join(directory, "checkpoint.json"), "utf8"));
    if (archive.events.length !== expected.count || archive.events.at(-1)?.id !== expected.head) throw new Error("Checkpoint mismatch");
    const store = new LevelStore(db);
    try {
      await (store as any).db.open();
      let previous = null;
      for (const [index, event] of archive.events.entries()) {
        if (event.seq !== index || event.prevHash !== previous || event.body.guildId !== archive.guildId || computeEventId(event) !== event.id ||
          !verify(event.author, hashObject({ body: event.body, author: event.author, createdAt: event.createdAt }), event.signature)) throw new Error("Invalid imported signed log");
        previous = event.id;
      }
      await store.appendEvents(archive.guildId, archive.events);
    } finally { await store.close(); }
  }
  const relay = new RelayServer(0, db, [], { listenHost: "127.0.0.1", enableDefaultPlugins: false, writeQuorum: false, instanceId: `isolated-${stage}` });
  let closed = false;
  const shutdown = async () => {
    if (closed) return; closed = true;
    clearInterval(watcher); clearTimeout(deadline);
    await relay.close();
    fs.writeFileSync(path.join(directory, `${stage}-stopped.json`), JSON.stringify({ pid: process.pid, stopped: true }));
    process.exit(0);
  };
  const watcher = setInterval(() => { if (fs.existsSync(path.join(directory, `${stage}.stop`))) void shutdown(); }, 250);
  const deadline = setTimeout(() => void shutdown(), 10 * 60 * 1000);
  process.once("SIGINT", () => void shutdown()); process.once("SIGTERM", () => void shutdown());
  const started = Date.now();
  while (!relay.getPort()) {
    if (Date.now() - started > 10000) throw new Error("Bind timeout");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  fs.writeFileSync(path.join(directory, `${stage}-ready.json`), JSON.stringify({ pid: process.pid, port: relay.getPort(), host: "127.0.0.1", db }));
}
void main().catch(() => { console.error("Isolated continuity fixture failed"); process.exit(1); });
