import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import net from "node:net";
import { spawn, execFile, type ChildProcess } from "node:child_process";
import { promisify } from "node:util";
import { CgpClient } from "@cgp/client";
import { generatePrivateKey, getPublicKey, verifyRelayHead } from "@cgp/core";
import { RelayServer } from "@cgp/relay/src/server";
import { LevelStore } from "@cgp/relay/src/store_level";
import { echoPowerShell, copyToEcho } from "../../hollow-tauri/scripts/ops/remote-ssh.mjs";

const execute = promisify(execFile);
const run = `20260928-continuity-${Date.now()}`;
const remote = `C:/Users/ECHO/hollow-roadmap-staging-${run}`;
const local = path.resolve(`output/community-continuity/${run}`);
fs.mkdirSync(local, { recursive: true });
const privateKey = generatePrivateKey();
const identity = { priv: privateKey, pub: getPublicKey(privateKey) };
const stages: string[] = [], forwards: ChildProcess[] = [], clients: CgpClient[] = [], relays: RelayServer[] = [];
const remoteSessions: ChildProcess[] = [];
const checks: string[] = [];
const auth = ["--private-key-hex", Buffer.from(privateKey).toString("hex")];
let failed: string | undefined;
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function ops(args: string[]) {
  try { await execute(process.execPath, ["--import", "tsx", "scripts/relay-ops.ts", ...args], { windowsHide: true, timeout: 25000 }); }
  catch { throw new Error(`Local ${args[0]} failed`); }
}
async function startRemote(stage: string) {
  stages.push(stage);
  // Keep the SSH session attached: Windows sshd may terminate detached children
  // when their launching session ends. No scheduled task or service is created.
  const encoded = Buffer.from(`Set-Location -LiteralPath '${remote}'; & 'C:/nvm4w/nodejs/node.exe' '${remote}/worker.cjs' '${remote}' '${stage}'`, "utf16le").toString("base64");
  const session = spawn("ssh", ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=8", "echo.hardesty.ai", `powershell -NoProfile -NonInteractive -EncodedCommand ${encoded}`], { windowsHide: true, stdio: "ignore" });
  remoteSessions.push(session);
  for (let attempt = 0; attempt < 15; attempt++) {
    const result = await echoPowerShell(`if(Test-Path -LiteralPath '${remote}/${stage}-ready.json'){Get-Content -LiteralPath '${remote}/${stage}-ready.json' -Raw}`);
    if (result.stdout.trim()) {
      const ready = JSON.parse(result.stdout.trim());
      assert.equal(ready.host, "127.0.0.1");
      return ready as { pid: number; port: number; db: string };
    }
    await sleep(250);
  }
  throw new Error(`Remote ${stage} readiness failed`);
}
async function stopRemote(stage: string) {
  await echoPowerShell(`Set-Content -LiteralPath '${remote}/${stage}.stop' -Value 'stop'`);
  for (let attempt = 0; attempt < 12; attempt++) {
    const result = await echoPowerShell(`if(Test-Path -LiteralPath '${remote}/${stage}-stopped.json'){Get-Content -LiteralPath '${remote}/${stage}-stopped.json' -Raw}`);
    if (result.stdout.trim()) { assert.equal(JSON.parse(result.stdout).stopped, true); return; }
    await sleep(250);
  }
  throw new Error(`Remote ${stage} cleanup not confirmed`);
}
async function forwarded(remotePort: number) {
  const server = net.createServer();
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as net.AddressInfo).port;
  await new Promise<void>(resolve => server.close(() => resolve()));
  const ssh = spawn("ssh", ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ExitOnForwardFailure=yes", "-o", "ConnectTimeout=8", "-N", "-L", `127.0.0.1:${port}:127.0.0.1:${remotePort}`, "echo.hardesty.ai"], { windowsHide: true, stdio: "ignore" });
  forwards.push(ssh);
  for (let attempt = 0; attempt < 100; attempt++) {
    if (ssh.exitCode !== null) throw new Error("Authorized loopback forward failed");
    const connected = await new Promise<boolean>(resolve => {
      const socket = net.connect(port, "127.0.0.1");
      socket.once("connect", () => { socket.destroy(); resolve(true); });
      socket.once("error", () => { socket.destroy(); resolve(false); });
    });
    if (connected) return `ws://127.0.0.1:${port}`;
    await sleep(100);
  }
  throw new Error("Loopback forward timeout");
}
async function connect(url: string, keys = identity) {
  const client = new CgpClient({ relays: [url], keyPair: keys }); clients.push(client); await client.connect(); return client;
}
async function localRelay(db: string) {
  const relay = new RelayServer(0, db, [], { enableDefaultPlugins: false, listenHost: "127.0.0.1", writeQuorum: false });
  relays.push(relay);
  while (!relay.getPort()) await sleep(10);
  return `ws://127.0.0.1:${relay.getPort()}`;
}
async function main() {
  await echoPowerShell(`New-Item -ItemType Directory -Path '${remote}' -ErrorAction Stop | Out-Null`);
  await copyToEcho(path.resolve("output/community-continuity/remote-package/worker.cjs"), `${remote}/worker.cjs`);
  await copyToEcho(path.resolve("output/community-continuity/remote-package/dependencies.zip"), `${remote}/dependencies.zip`);
  await echoPowerShell(`Expand-Archive -LiteralPath '${remote}/dependencies.zip' -DestinationPath '${remote}' -ErrorAction Stop; Set-Location -LiteralPath '${remote}'; & 'C:/nvm4w/nodejs/node.exe' -e "require('classic-level'); console.log('native-dependency-smoke-ok')"; if($LASTEXITCODE -ne 0){exit $LASTEXITCODE}`);
  checks.push("ECHO Node loads isolated Windows LevelDB dependency closure");
  const founder = await startRemote("founder");
  const founderUrl = await forwarded(founder.port);
  const owner = await connect(founderUrl);
  const guild = await owner.createGuild("SSH continuity pilot");
  const channel = await owner.createChannel(guild, "general", "text");
  await owner.sendMessage(guild, channel, "cross-machine original");
  const head = await owner.getRelayHead(guild);
  assert(verifyRelayHead(head));
  const witnesses = [path.join(local, "witness-a"), path.join(local, "witness-b")];
  for (const db of witnesses) await ops(["sync-from-relay", "--db", db, "--guild", guild, "--relay", founderUrl, "--expected-end-seq", String(head.headSeq), "--expected-end-hash", head.headHash!, ...auth]);
  owner.close(); await stopRemote("founder");
  checks.push("remote founder stored signed history, two local witnesses synchronized to pinned head, remote founder stopped");
  const urls = await Promise.all(witnesses.map(localRelay));
  const survivor = await connect(urls[0]);
  assert((await survivor.getHistory({ guildId: guild, channelId: channel, limit: 100 })).events.some((e: any) => e.body.content === "cross-machine original"));
  const repaired = path.join(local, "repaired");
  await ops(["repair-from-quorum", "--db", repaired, "--guild", guild, "--relays", urls.join(","), "--min-valid-heads", "2", "--min-canonical-count", "2", ...auth]);
  const archive = path.join(local, "replacement.json");
  await ops(["export-guild", "--db", repaired, "--guild", guild, "--output", archive]);
  const exported = JSON.parse(fs.readFileSync(archive, "utf8"));
  assert.equal(exported.events.at(-1).id, head.headHash);
  const checkpoint = path.join(local, "checkpoint.json");
  fs.writeFileSync(checkpoint, JSON.stringify({ head: head.headHash, count: head.headSeq + 1 }));
  await execute(process.execPath, ["examples/independent-archive-reader.mjs", archive, head.headHash!, String(head.headSeq + 1)], { windowsHide: true });
  checks.push("two signed local witnesses repair complete history; separate archive reader validates checkpoint");
  await copyToEcho(archive, `${remote}/replacement.json`);
  await copyToEcho(checkpoint, `${remote}/checkpoint.json`);
  const replacement = await startRemote("replacement");
  await Promise.all(relays.map(relay => relay.close())); relays.length = 0; survivor.close();
  const next = await connect(await forwarded(replacement.port));
  const newKey = generatePrivateKey(); const newIdentity = { priv: newKey, pub: getPublicKey(newKey) };
  await next.assignRole(guild, newIdentity.pub, "member");
  const member = await connect(await forwarded(replacement.port), newIdentity);
  await member.subscribe(guild); await member.sendMessage(guild, channel, "new member on remote replacement");
  const history = await member.getHistory({ guildId: guild, channelId: channel, limit: 100 });
  assert(history.events.some((e: any) => e.body.content === "cross-machine original"));
  assert(history.events.some((e: any) => e.body.content === "new member on remote replacement" && e.author === newIdentity.pub));
  member.close(); next.close(); await stopRemote("replacement");
  checks.push("remote replacement validates imported log and serves old history/new member writes after original relays stop");
  const restarted = await startRemote("replacement-restart");
  const fresh = await connect(await forwarded(restarted.port), newIdentity);
  const afterRestart = await fresh.getHistory({ guildId: guild, channelId: channel, limit: 100 });
  assert(afterRestart.events.some((e: any) => e.body.content === "new member on remote replacement"));
  checks.push("remote replacement restarts from persistent LevelDB and fresh participant reads new signed message");
  fs.writeFileSync(path.join(local, "public-checkpoint.json"), JSON.stringify({ guild, channel, head: head.headHash, count: head.headSeq + 1 }, null, 2));
}
async function runPilot() {
  try { await main(); } catch (error) { failed = error instanceof Error ? error.message : "pilot failed"; }
  finally {
    clients.forEach(client => client.close()); await Promise.all(relays.map(relay => relay.close().catch(() => undefined)));
    for (const stage of stages) { try { await stopRemote(stage); } catch { failed = `${failed || ""} cleanup unconfirmed for ${stage}`.trim(); } }
    for (const ssh of forwards) ssh.kill();
    for (const ssh of remoteSessions) ssh.kill();
  }
  const report = { ok: !failed, failed, at: new Date().toISOString(), remoteTaskDirectory: remote, checks,
    scope: ["ECHO and local Windows controller, one administrative operator", "Loopback SSH forwards; not public peer discovery or independent operators", "Two witness identities/stores share local machine", "No write-quorum epoch transition, media, calls or delegated recovery", "Transfer revalidated signed archive; not automatic remote repair service"], cleanup: !failed?.includes("cleanup unconfirmed") };
  fs.writeFileSync(path.join(local, "report.json"), JSON.stringify(report, null, 2)); console.log(JSON.stringify(report, null, 2));
  if (failed) process.exitCode = 1;
}
void runPilot();
