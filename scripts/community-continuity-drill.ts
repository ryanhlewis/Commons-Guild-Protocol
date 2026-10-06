import assert from "node:assert/strict";
import { spawn, execFile, type ChildProcess } from "node:child_process";
import { promisify } from "node:util";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import net from "node:net";
import { CgpClient } from "@cgp/client";
import { generatePrivateKey, getPublicKey } from "@cgp/core";
import { LevelStore } from "@cgp/relay";
import { WebSocketPubSubHub } from "@cgp/relay/src/pubsub_ws";

// Real separate relay processes, separate persistent stores, synthetic identities.
// This deliberately tests data continuity, NOT live write-epoch reconfiguration.
const exec = promisify(execFile);
const root = process.cwd();
const directory = mkdtempSync(path.join(tmpdir(), "cgp-continuity-"));
const checks: string[] = [];
const children: ChildProcess[] = [];
const clients: CgpClient[] = [];
const privateKey = generatePrivateKey();
const keyPair = { priv: privateKey, pub: getPublicKey(privateKey) };
const auth = ["--private-key-hex", Buffer.from(privateKey).toString("hex")];
let hub: WebSocketPubSubHub | undefined;
async function port() {
  const server = net.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const value = (server.address() as net.AddressInfo).port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return value;
}
async function ops(args: string[], shouldPass = true) {
  try {
    const result = await exec(process.execPath, ["--import", "tsx", "scripts/relay-ops.ts", ...args], {
      cwd: root, windowsHide: true, timeout: 25000, maxBuffer: 2_000_000,
    });
    assert(shouldPass, "unsafe operation unexpectedly succeeded");
    return result.stdout;
  } catch (error: any) {
    // Never emit the child command: it contains a disposable signing key.
    if (shouldPass || !Number.isInteger(error.code) || error.code === 0) {
      const detail = String(error.stderr || `exit ${error.code ?? "unknown"}; signal ${error.signal ?? "none"}`)
        .replaceAll(Buffer.from(privateKey).toString("hex"), "[redacted]");
      throw new Error(`ops command ${args[0]} failed: ${detail}`);
    }
    return String(error.stderr || error.stdout);
  }
}
async function stop(child: ChildProcess) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  child.kill("SIGKILL");
  await exited;
}
async function start(db: string, pubsub: string) {
  const relayPort = await port();
  const child = spawn(process.execPath, ["--import", "tsx", "scripts/relay-process-worker.ts"], {
    cwd: root, windowsHide: true, stdio: ["pipe", "pipe", "pipe"],
    env: { ...process.env, CGP_RELAY_DB: db, CGP_RELAY_PORT: String(relayPort), CGP_RELAY_PUBSUB_URL: pubsub },
  });
  children.push(child);
  child.stderr?.resume();
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("relay readiness timeout")), 20000);
    let buffer = "";
    child.once("error", (error) => { clearTimeout(timer); reject(error); });
    child.once("exit", () => { clearTimeout(timer); reject(new Error("relay exited before ready")); });
    child.stdout?.on("data", (chunk) => {
      buffer += chunk.toString();
      if (buffer.includes('"type":"ready"')) { clearTimeout(timer); resolve(); }
    });
  });
  return { child, url: `ws://127.0.0.1:${relayPort}` };
}
async function client(url: string, identity = keyPair) {
  const value = new CgpClient({ relays: [url], keyPair: identity });
  clients.push(value);
  await value.connect();
  return value;
}
async function main() {
  const hubPort = await port();
  hub = new WebSocketPubSubHub(hubPort, { host: "127.0.0.1" });
  const pubsub = `ws://127.0.0.1:${hubPort}`;
  const dbs = [0, 1, 2, 3].map((n) => path.join(directory, `relay-${n}`));
  const founder = await start(dbs[0], pubsub);
  const owner = await client(founder.url);
  const guild = await owner.createGuild("Continuity drill");
  const channel = await owner.createChannel(guild, "general", "text");
  await owner.sendMessage(guild, channel, "before founder loss");
  // Establish the two witness copies before the founder departs. Explicit trust
  // is confined to this synthetic bootstrap; it is not a production trust policy.
  for (const db of dbs.slice(1, 3)) {
    await ops(["sync-from-relay", "--db", db, "--guild", guild, "--relay", founder.url, "--trust-source-head", ...auth]);
  }
  const a = await start(dbs[1], pubsub);
  const b = await start(dbs[2], pubsub);
  owner.close();
  await stop(founder.child);
  checks.push("founder process forcibly stopped; two separately persisted witnesses survive");
  const fresh = await client(a.url);
  const history = await fresh.getHistory({ guildId: guild, channelId: channel, limit: 100 });
  assert(history.events.some((event: any) => event.body.content === "before founder loss"));
  checks.push("fresh client with existing owner key reads history without founder");
  const repair = ["repair-from-quorum", "--db", dbs[3], "--guild", guild, "--min-valid-heads", "2", "--min-canonical-count", "2", ...auth];
  const insufficient = await ops([...repair, "--relays", a.url], false);
  assert.match(insufficient, /quorum failed/);
  checks.push("one witness cannot satisfy the unchanged two-witness repair threshold");
  const defaultThreshold = await ops(["repair-from-quorum", "--db", dbs[3], "--guild", guild,
    "--relays", `${a.url},${founder.url}`, "--timeout-ms", "500", ...auth], false);
  assert.match(defaultThreshold, /quorum failed: 1\/2 valid heads/);
  checks.push("default two-relay policy requires both witnesses when one is unavailable");
  await ops([...repair, "--relays", `${a.url},${b.url}`]);
  checks.push("empty replacement store repaired from two agreeing signed witness heads");
  fresh.close();
  await stop(a.child);
  await stop(b.child);
  const stores = dbs.slice(1).map((db) => new LevelStore(db));
  let log: any[];
  try {
    const logs = await Promise.all(stores.map((store) => store.getLog(guild)));
    assert.deepEqual(logs[2], logs[0]);
    assert.deepEqual(logs[2], logs[1]);
    log = logs[2];
  } finally { await Promise.all(stores.map((store) => store.close())); }
  await ops(["verify-log", "--db", dbs[3], "--guild", guild]);
  checks.push("replacement matches every signed event and passes full chain/signature verification");
  const archivePath = path.join(directory, "community-archive.json");
  await ops(["export-guild", "--db", dbs[3], "--guild", guild, "--output", archivePath]);
  const independent = await exec(process.execPath, ["examples/independent-archive-reader.mjs", archivePath,
    log!.at(-1).id, String(log!.length)], { cwd: root, windowsHide: true, timeout: 10000 });
  assert.equal(JSON.parse(independent.stdout).ok, true);
  checks.push("separate reader with no CGP imports verifies exported log against the agreed checkpoint");
  const tampered = structuredClone(log!);
  tampered[tampered.length - 1].body.content = "forged content";
  const badDb = path.join(directory, "tampered");
  const badStore = new LevelStore(badDb);
  try { await (badStore as any).db.open(); await badStore.appendEvents(guild, tampered); } finally { await badStore.close(); }
  const rejected = await ops(["verify-log", "--db", badDb, "--guild", guild], false);
  assert.match(rejected, /mismatch/);
  checks.push("modified archive content fails cryptographic verification");
  // Reopen the two known witnesses and attempt repair over a divergent local head.
  const witnessA = await start(dbs[1], pubsub);
  const witnessB = await start(dbs[2], pubsub);
  const divergentDb = path.join(directory, "divergent");
  const divergentStore = new LevelStore(divergentDb);
  const divergent = structuredClone(log!);
  divergent[divergent.length - 1].id = "f".repeat(64);
  try {
    await (divergentStore as any).db.open();
    await divergentStore.appendEvents(guild, divergent);
  } finally { await divergentStore.close(); }
  const conflict = await ops(["repair-from-quorum", "--db", divergentDb, "--guild", guild,
    "--relays", `${witnessA.url},${witnessB.url}`, "--min-valid-heads", "2", "--min-canonical-count", "2", ...auth], false);
  assert.match(conflict, /divergent head/);
  checks.push("repair refuses to overwrite a divergent local head");
  await stop(witnessA.child);
  await stop(witnessB.child);
  const replacement = await start(dbs[3], pubsub);
  const recovered = await client(replacement.url);
  await recovered.subscribe(guild);
  await recovered.sendMessage(guild, channel, "after every original relay stopped");
  const recoveredHistory = await recovered.getHistory({ guildId: guild, channelId: channel, limit: 100 });
  assert(recoveredHistory.events.some((event: any) => event.body.content === "after every original relay stopped"));
  checks.push("fresh client reads and writes on replacement after all original relays stop");
  const newcomerKey = generatePrivateKey();
  const newcomerIdentity = { priv: newcomerKey, pub: getPublicKey(newcomerKey) };
  await recovered.assignRole(guild, newcomerIdentity.pub, "member");
  const newcomer = await client(replacement.url, newcomerIdentity);
  await newcomer.subscribe(guild);
  await newcomer.sendMessage(guild, channel, "new member after replacement");
  const newcomerHistory = await newcomer.getHistory({ guildId: guild, channelId: channel, limit: 100 });
  assert(newcomerHistory.events.some((event: any) => event.author === newcomerIdentity.pub && event.body.content === "new member after replacement"));
  assert(newcomerHistory.events.some((event: any) => event.body.content === "before founder loss"));
  checks.push("new identity enrolled by surviving owner reads old history and publishes on replacement");
}
async function run() {
let failure: string | undefined;
try { await main(); } catch (error) { failure = error instanceof Error ? error.message : String(error); }
finally {
  clients.forEach((value) => value.close());
  await Promise.all(children.map(stop));
  await hub?.close();
}
const report = {
  format: "cgp.community-continuity-drill.v1", at: new Date().toISOString(), ok: !failure,
  checks, failure, evidenceDirectory: directory,
  boundaries: ["Local processes on one machine, not independent human operators", "Shared pubsub hub remains available", "Synthetic bootstrap explicitly trusts source", "Owner reuses existing key; no delegated-device or lost-key recovery claim", "New identity is owner-enrolled; invitation/bootstrap discovery is not tested", "No media bytes, calls, private-guild keys, or public network tested", "Write quorum is not configured; no safe live voting-epoch transition claim"],
};
const reportPath = path.resolve(process.argv[2] || "output/community-continuity/latest.json");
mkdirSync(path.dirname(reportPath), { recursive: true });
writeFileSync(reportPath, JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify(report, null, 2));
if (failure) process.exitCode = 1;
}
void run().catch(() => { console.error("Continuity drill cleanup/report failed"); process.exitCode = 1; });
