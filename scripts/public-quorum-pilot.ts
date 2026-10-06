import {allowFixtureHostname,fixtureDnsObservations} from "./fixture-dns.js";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { CgpClient } from "@cgp/client";
import { generatePrivateKey, getPublicKey, hashObject, assertRelayHeadQuorum, verifyRelayWriteCertificate } from "@cgp/core";
import { echoPowerShell, workerPowerShell, copyToEcho } from "../../hollow-tauri/scripts/ops/remote-ssh.mjs";

type Host = "local" | "echo" | "cortop3";
interface Fixture { host: Host; directory: string; label: string; config: Record<string, unknown>; ready?: any }
const runId = `20260928-quorum-${Date.now()}`;
const output = path.resolve(`output/community-continuity/${runId}`);
fs.mkdirSync(output, { recursive: true });
const echoStage = `C:/Users/ECHO/hollow-roadmap-staging-${runId}`;
const cortopStage = `C:/Users/PC/hollow-roadmap-staging-${runId}`;
const localStage = path.join(output, `hollow-roadmap-staging-${runId}`);
fs.mkdirSync(localStage, { recursive: true });
const baseSsh = ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=8"];
const processes: ChildProcess[] = [], clients: CgpClient[] = [], started: Fixture[] = [];
const checks: string[] = [];
const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const quote = (s: string) => `'${s.replaceAll("'", "''")}'`;
const encode = (s: string) => Buffer.from(s, "utf16le").toString("base64");
const bundle = path.resolve("output/community-continuity/remote-package/public-worker.cjs");
const deps = path.resolve("output/community-continuity/remote-package/dependencies.zip");
const keyPairs = Array.from({ length: 3 }, () => { const priv = generatePrivateKey(); return { priv, pub: getPublicKey(priv) }; });
const quorum = { epoch: runId, members: keyPairs.map(k => k.pub), requiredVotes: 2, voteTimeoutMs: 8000 };
const pubsubToken = randomBytes(32).toString("hex");
const cloudflared = { local: "C:/Users/cortanium/Downloads/asda/tools/cloudflared-2026.6.0-windows-amd64.exe", echo: "C:/Program Files (x86)/cloudflared/cloudflared.exe", cortop3: "C:/Users/PC/Downloads/cloudflared/cloudflared.exe" };
async function remote(host: Host, script: string) { return host === "echo" ? echoPowerShell(script) : workerPowerShell("cortop3", script); }
async function read(fixture: Fixture, name: string) {
  if (fixture.host === "local") { try { return JSON.parse(fs.readFileSync(path.join(fixture.directory, name), "utf8")); } catch { return undefined; } }
  const result = await remote(fixture.host, `if(Test-Path -LiteralPath ${quote(fixture.directory+'/'+name)}){Get-Content -LiteralPath ${quote(fixture.directory+'/'+name)} -Raw}`);
  try { return result.stdout.trim() ? JSON.parse(result.stdout.trim()) : undefined; } catch { return undefined; }
}
async function write(fixture: Fixture, name: string, value: string) {
  if (fixture.host === "local") fs.writeFileSync(path.join(fixture.directory, name), value);
  else await remote(fixture.host, `[IO.File]::WriteAllText(${quote(fixture.directory+'/'+name)},${quote(value)})`);
}
async function waitFor(label: string, predicate: () => Promise<boolean>, seconds = 45) {
  const end = Date.now()+seconds*1000;
  while (Date.now()<end) { if (await predicate()) return; await pause(300); }
  throw new Error(`Timed out: ${label}`);
}
async function prepareRemote() {
  await echoPowerShell(`New-Item -ItemType Directory -Path ${quote(echoStage)} -ErrorAction Stop | Out-Null`);
  await copyToEcho(bundle, `${echoStage}/public-worker.cjs`); await copyToEcho(deps, `${echoStage}/dependencies.zip`);
  await echoPowerShell(`Expand-Archive -LiteralPath ${quote(echoStage+'/dependencies.zip')} -DestinationPath ${quote(echoStage)} -ErrorAction Stop`);
  await workerPowerShell("cortop3", `New-Item -ItemType Directory -Path ${quote(cortopStage)} -ErrorAction Stop | Out-Null`);
  for (const filename of ["public-worker.cjs", "dependencies.zip"]) {
    await echoPowerShell(`& scp -F C:/Users/ECHO/.workers/config -o BatchMode=yes -o StrictHostKeyChecking=yes ${quote(echoStage+'/'+filename)} ${quote('cortop3:'+cortopStage+'/'+filename)}; exit $LASTEXITCODE`, { timeout: 180000 });
  }
  await workerPowerShell("cortop3", `Expand-Archive -LiteralPath ${quote(cortopStage+'/dependencies.zip')} -DestinationPath ${quote(cortopStage)} -ErrorAction Stop`, { timeout: 180000 });
}
async function start(fixture: Fixture) {
  if (fixture.host === "local") {
    fs.mkdirSync(fixture.directory, { recursive: true });
    await write(fixture, "config.json", JSON.stringify(fixture.config));
    const proc = spawn(process.execPath, [bundle, fixture.directory], { windowsHide: true, stdio: "ignore" }); processes.push(proc);
  } else {
    // Synthetic private config travels over SSH stdin/encoded command, never logs.
    await write(fixture, "config.json", JSON.stringify(fixture.config));
    const node = fixture.host === "echo" ? "C:/nvm4w/nodejs/node.exe" : "C:/Users/PC/hollow-roadmap-staging-20260926-v3/runtime/node.exe";
    const inner = `Set-Location -LiteralPath ${quote(fixture.directory)}; & ${quote(node)} ${quote(fixture.directory+'/public-worker.cjs')} ${quote(fixture.directory)}`;
    let script = inner;
    if (fixture.host === "cortop3") script = `& ssh -F C:/Users/ECHO/.workers/config -o BatchMode=yes -o StrictHostKeyChecking=yes cortop3 'powershell -NoProfile -NonInteractive -EncodedCommand ${encode(inner)}'; exit $LASTEXITCODE`;
    processes.push(spawn("ssh", [...baseSsh, "echo.hardesty.ai", `powershell -NoProfile -NonInteractive -EncodedCommand ${encode(script)}`], { windowsHide: true, stdio: "ignore" }));
  }
  if(!started.includes(fixture)) started.push(fixture);
  await waitFor(`${fixture.label} public tunnel`, async () => { fixture.ready = await read(fixture, "public-ready.json"); return !!fixture.ready; }, 100);
  assert.equal(fixture.ready.host, "127.0.0.1");
  console.log(JSON.stringify({ stage: "public-ready", label: fixture.label, ...fixture.ready }));
}
async function connect(url: string, identity: { pub: string; priv: Uint8Array }) {
  allowFixtureHostname(url);
  for(let attempt=0;attempt<60;attempt++) {
    const client = new CgpClient({ relays: [url], keyPair: identity }); clients.push(client);
    try { await client.connect(); if (!(client as any).sockets.some((s:any)=>s.readyState===1)) throw new Error("not open"); return client; } catch { client.close(); await pause(2000); }
  }
  throw new Error("Public relay WebSocket did not become reachable");
}
async function main() {
  await prepareRemote();
  const hub: Fixture = { host: "local", directory: path.join(localStage,"hub"), label: "hub", config: { role: "hub", pubsubToken, cloudflared: cloudflared.local } };
  await start(hub);
  const fixtures: Fixture[] = (["local","echo","cortop3"] as Host[]).map((host,index) => ({host,
    directory: host === "local" ? path.join(localStage,"relay") : host === "echo" ? echoStage : cortopStage,
    label: `quorum-${host}`, config: { role:"relay", label:`quorum-${host}`, privateKey:Buffer.from(keyPairs[index].priv).toString("hex"), quorum,
      pubsubUrl:hub.ready.wssUrl, pubsubToken, cloudflared:cloudflared[host] } }));
  for (const fixture of fixtures) await start(fixture);
  try {
    await waitFor("all pubsub transports ready",async()=> (await Promise.all(fixtures.map(f=>read(f,"transport-state.json")))).every(s=>s?.ready===true),120);
  } finally {
    const transports=await Promise.all([hub,...fixtures].map(async f=>({label:f.label,state:await read(f,"transport-state.json")})));
    fs.writeFileSync(path.join(output,"transport-readiness.json"),JSON.stringify(transports,null,2));
    console.log(JSON.stringify({stage:"transport-readiness",transports}));
  }
  const endpoints = fixtures.map((f,index) => ({host:f.host, machine:f.ready.machine, url:f.ready.wssUrl, relayId:f.ready.relayId, publicKey:keyPairs[index].pub, port:f.ready.port, pid:f.ready.pid, tunnelPid:f.ready.tunnelPid}));
  fs.writeFileSync(path.join(output,"endpoints.json"),JSON.stringify({quorum,endpoints},null,2));
  const privateKey = generatePrivateKey(), identity = {priv:privateKey,pub:getPublicKey(privateKey)};
  const peers = await Promise.all(fixtures.map(f=>connect(f.ready.wssUrl,identity)));
  const guild=hashObject({runId,owner:identity.pub}), channel=hashObject({guild,channel:"general"});
  await peers[0].publishReliable({type:"GUILD_CREATE",guildId:guild,name:"Public fixed quorum pilot"},{timeoutMs:25000});
  for(const peer of peers) await peer.subscribe(guild);
  await waitFor("genesis on three machines", async()=> (await Promise.all(peers.map(p=>p.getRelayHead(guild).catch(()=>null)))).every(h=>h?.headSeq===0));
  await peers[0].publishReliable({type:"CHANNEL_CREATE",guildId:guild,channelId:channel,name:"general",kind:"text"},{timeoutMs:25000});
  await peers[0].publishReliable({type:"MESSAGE",guildId:guild,channelId:channel,messageId:"baseline",content:"public quorum baseline"},{timeoutMs:25000});
  await waitFor("baseline replication",async()=> (await Promise.all(peers.map(p=>p.getRelayHead(guild).catch(()=>null)))).every(h=>h?.headSeq===2));
  checks.push("three physical PCs with distinct keys/stores commit and replicate baseline through public TLS WebSockets");
  const heads=await Promise.all(peers.map(p=>p.getRelayHead(guild)));
  assertRelayHeadQuorum(guild,heads,{minValidHeads:3,minCanonicalCount:3});
  assert.throws(()=>assertRelayHeadQuorum(guild,[heads[0],heads[0]],{minValidHeads:2,minCanonicalCount:2}));
  checks.push("signed head quorum agrees across PCs and repeated witness identity fails 2-head requirement");
  await write(fixtures[2],"isolate","test-owned pubsub partition");
  await waitFor("minority isolated",async()=> (await read(fixtures[2],"network-state.json"))?.isolated===true);
  await assert.rejects(peers[2].publishReliable({type:"MESSAGE",guildId:guild,channelId:channel,messageId:"minority-denied",content:"must not commit"},{timeoutMs:20000}),/quorum unavailable|1\/2 votes/i);
  assert.equal((await peers[2].getRelayHead(guild)).headSeq,2);
  await peers[0].publishReliable({type:"MESSAGE",guildId:guild,channelId:channel,messageId:"majority",content:"two-machine majority commits"},{timeoutMs:25000});
  await waitFor("majority commit",async()=> (await Promise.all(peers.slice(0,2).map(p=>p.getRelayHead(guild)))).every(h=>h.headSeq===3));
  assert.equal((await peers[2].getRelayHead(guild)).headSeq,2);
  checks.push("isolated CORTOP3 rejects single-voter publish with unchanged 2-of-3 policy while CORTOP1+ECHO commit");
  await remote(fixtures[2].host,`Remove-Item -LiteralPath ${quote(fixtures[2].directory+'/isolate')} -ErrorAction Stop`);
  await waitFor("minority catchup after healing",async()=> (await peers[2].getRelayHead(guild).catch(()=>null))?.headSeq===3,60);
  const healed=await Promise.all(peers.map(p=>p.getRelayHead(guild)));assertRelayHeadQuorum(guild,healed,{minValidHeads:3,minCanonicalCount:3});
  checks.push("healed minority replays certified committed history and all three signed heads converge");
  for(const fixture of fixtures) await write(fixture,"inspect.json",JSON.stringify({guild}));
  await waitFor("certified snapshots",async()=> (await read(fixtures[0],"snapshot.json"))?.events?.length===4);
  const snapshot=await read(fixtures[0],"snapshot.json");
  for(const event of snapshot.events){assert.equal(verifyRelayWriteCertificate(event),true);assert.equal(event.writeCertificate.policy.epoch,quorum.epoch);assert.deepEqual(event.writeCertificate.policy.members,quorum.members);assert.equal(event.writeCertificate.policy.requiredVotes,2);}
  const duplicated=structuredClone(snapshot.events.at(-1));duplicated.writeCertificate.votes=[duplicated.writeCertificate.votes[0],duplicated.writeCertificate.votes[0]];assert.equal(verifyRelayWriteCertificate(duplicated),false);
  const altered=structuredClone(snapshot.events.at(-1));altered.body.content="divergent";assert.equal(verifyRelayWriteCertificate(altered),false);
  fs.writeFileSync(path.join(output,"signed-events.json"),JSON.stringify(snapshot,null,2));
  checks.push("every event has valid fixed-epoch majority certificate; duplicate votes and modified divergent event fail verification");
  const stable={runId,quorum,endpoints,guild,channel,checks,control:{finishFile:path.join(output,"finish")},scope:["three PCs under one administrative operator","shared authenticated pubsub hub/tunnel remains a dependency","fixed membership only; no live epoch transition"]};
  fs.writeFileSync(path.join(output,"client-window.json"),JSON.stringify(stable,null,2));
  console.log(JSON.stringify({stage:"stable-client-window",...stable}));
  await waitFor("client validation finish signal",async()=>fs.existsSync(path.join(output,"finish")),18*60);
  // Restart the minority process over the identical LevelDB/key; its public tunnel changes.
  const restarted=fixtures[2],beforeRestart={...restarted.ready};
  peers[2].close();await write(restarted,"stop","restart");
  await waitFor("persistent minority stopped",async()=>!!(await read(restarted,"stopped.json")),30);
  await remote(restarted.host,`foreach($name in @('stop','stopped.json','public-ready.json','transport-state.json')){Remove-Item -LiteralPath (${quote(restarted.directory)}+'/'+$name) -ErrorAction SilentlyContinue}`);
  await start(restarted);
  peers[2]=await connect(restarted.ready.wssUrl,identity);
  await waitFor("persistent signed head recovered",async()=> (await peers[2].getRelayHead(guild).catch(()=>null))?.headHash===healed[0].headHash,60);
  assert.notEqual(restarted.ready.pid,beforeRestart.pid);
  endpoints[2]={host:restarted.host,machine:restarted.ready.machine,url:restarted.ready.wssUrl,relayId:restarted.ready.relayId,publicKey:keyPairs[2].pub,port:restarted.ready.port,pid:restarted.ready.pid,tunnelPid:restarted.ready.tunnelPid};
  fs.writeFileSync(path.join(output,"restart.json"),JSON.stringify({before:beforeRestart,after:restarted.ready,head:await peers[2].getRelayHead(guild)},null,2));
  checks.push("CORTOP3 process and tunnel restart preserves identical LevelDB certified head and relay identity");

}
async function run(){let failure:string|undefined;try{await main();}catch(error){failure=error instanceof Error?error.message:"public quorum pilot failed";}
 finally{clients.forEach(c=>c.close());for(const fixture of [...started].reverse()){try{await write(fixture,"stop","stop");await waitFor(`${fixture.label} cleanup`,async()=>!!(await read(fixture,"stopped.json")),25);}catch{failure=`${failure||""} cleanup unconfirmed:${fixture.label}`.trim();}}for(const proc of processes)proc.kill();}
 fs.writeFileSync(path.join(output,"controller-dns.json"),JSON.stringify(fixtureDnsObservations(),null,2));
 const report={runId,at:new Date().toISOString(),ok:!failure,failure,checks,quorum,workers:started.map(f=>({label:f.label,host:f.host,ready:f.ready})),scope:["three PCs; not independent administrators","public TLS tunnels with authenticated shared pubsub","fixed 2-of-3 epoch; no membership reconfiguration"]};
 fs.writeFileSync(path.join(output,"report.json"),JSON.stringify(report,null,2));console.log(JSON.stringify(report,null,2));if(failure)process.exitCode=1;
}
void run();
